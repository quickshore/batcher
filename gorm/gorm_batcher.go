package gorm

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/atlasgurus/batcher/batcher"
	"gorm.io/gorm"
)

// InsertBatcher is a GORM batcher for batch inserts
type InsertBatcher[T any] struct {
	db      *gorm.DB
	batcher *batcher.BatchProcessor[[]T]
}

// UpdateBatcher is a GORM batcher for batch updates
type UpdateBatcher[T any] struct {
	db      *gorm.DB
	batcher *batcher.BatchProcessor[[]UpdateItem[T]]
}

type UpdateItem[T any] struct {
	Item         T
	UpdateFields []string
}

// NewInsertBatcher creates a new GORM insert batcher
func NewInsertBatcher[T any](db *gorm.DB, maxBatchSize int, maxWaitTime time.Duration, ctx context.Context) *InsertBatcher[T] {
	return &InsertBatcher[T]{
		db:      db,
		batcher: batcher.NewBatchProcessor(maxBatchSize, maxWaitTime, ctx, batchInsert[T](db)),
	}
}

// NewUpdateBatcher creates a new GORM update batcher
func NewUpdateBatcher[T any](db *gorm.DB, maxBatchSize int, maxWaitTime time.Duration, ctx context.Context) *UpdateBatcher[T] {
	return &UpdateBatcher[T]{
		db:      db,
		batcher: batcher.NewBatchProcessor(maxBatchSize, maxWaitTime, ctx, batchUpdate[T](db)),
	}
}

// Insert submits one or more items for batch insertion
func (b *InsertBatcher[T]) Insert(items ...T) error {
	return b.batcher.SubmitAndWait(items)
}

// Update submits one or more items for batch update
func (b *UpdateBatcher[T]) Update(items []T, updateFields []string) error {
	updateItems := make([]UpdateItem[T], len(items))
	for i, item := range items {
		updateItems[i] = UpdateItem[T]{Item: item, UpdateFields: updateFields}
	}
	return b.batcher.SubmitAndWait(updateItems)
}

func batchInsert[T any](db *gorm.DB) func([][]T) error {
	return func(batches [][]T) error {
		if len(batches) == 0 {
			return nil
		}

		tx := db.Begin()
		if tx.Error != nil {
			return tx.Error
		}

		for _, batch := range batches {
			if err := tx.Create(batch).Error; err != nil {
				tx.Rollback()
				return err
			}
		}

		return tx.Commit().Error
	}
}

func batchUpdate[T any](db *gorm.DB) func([][]UpdateItem[T]) error {
	return func(batches [][]UpdateItem[T]) error {
		if len(batches) == 0 {
			return nil
		}

		tx := db.Begin()
		if tx.Error != nil {
			return tx.Error
		}

		for _, batch := range batches {
			for _, updateItem := range batch {
				item := updateItem.Item
				updateFields := updateItem.UpdateFields

				primaryKey, primaryKeyValue := getPrimaryKeyAndValue(item)
				if primaryKey == "" {
					tx.Rollback()
					return fmt.Errorf("primary key not found for item")
				}

				updateMap := make(map[string]interface{})
				v := reflect.ValueOf(item)
				t := v.Type()
				if t.Kind() == reflect.Ptr {
					v = v.Elem()
					t = v.Type()
				}

				for i := 0; i < t.NumField(); i++ {
					field := t.Field(i)
					if len(updateFields) == 0 || contains(updateFields, field.Name) {
						updateMap[field.Name] = v.Field(i).Interface()
					}
				}

				if err := tx.Model(new(T)).Where(primaryKey+" = ?", primaryKeyValue).Updates(updateMap).Error; err != nil {
					tx.Rollback()
					return err
				}
			}
		}

		return tx.Commit().Error
	}
}

// Helper function to check if a string is in a slice
func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

// getPrimaryKeyAndValue uses reflection to find the primary key field and its value
func getPrimaryKeyAndValue(item interface{}) (string, interface{}) {
	t := reflect.TypeOf(item)
	v := reflect.ValueOf(item)

	// If it's a pointer, get the underlying element
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
		v = v.Elem()
	}

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if tag := field.Tag.Get("gorm"); strings.Contains(tag, "primaryKey") {
			return field.Name, v.Field(i).Interface()
		}
	}

	return "", nil
}
