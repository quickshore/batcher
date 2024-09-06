package gorm

import (
	"context"
	"errors"
	"reflect"
	"time"

	"atlasgurus/batcher/batcher"
	"gorm.io/gorm"
)

// Batcher is a GORM batcher for batch inserts and updates
type Batcher struct {
	db            *gorm.DB
	insertBatcher *batcher.BatchProcessor[interface{}]
	updateBatcher *batcher.BatchProcessor[interface{}]
}

// NewBatcher creates a new GORM batcher
func NewBatcher(db *gorm.DB, maxBatchSize int, maxWaitTime time.Duration) *Batcher {
	ctx := context.Background()
	return &Batcher{
		db:            db,
		insertBatcher: batcher.NewBatchProcessor(maxBatchSize, maxWaitTime, ctx, batchInsert(db)),
		updateBatcher: batcher.NewBatchProcessor(maxBatchSize, maxWaitTime, ctx, batchUpdate(db)),
	}
}

// Insert submits an item for batch insertion
func (b *Batcher) Insert(item interface{}) error {
	return b.insertBatcher.SubmitAndWait(item)
}

// Update submits an item for batch update
func (b *Batcher) Update(item interface{}) error {
	return b.updateBatcher.SubmitAndWait(item)
}

func batchInsert(db *gorm.DB) func([]interface{}) error {
	return func(items []interface{}) error {
		if len(items) == 0 {
			return nil
		}

		// Create a slice of the correct type
		sliceType := reflect.SliceOf(reflect.TypeOf(items[0]))
		slice := reflect.MakeSlice(sliceType, len(items), len(items))

		// Copy items into the new slice
		for i, item := range items {
			slice.Index(i).Set(reflect.ValueOf(item))
		}

		return db.Create(slice.Interface()).Error
	}
}

func batchUpdate(db *gorm.DB) func([]interface{}) error {
	return func(items []interface{}) error {
		if len(items) == 0 {
			return nil
		}

		// Ensure all items are of the same type
		firstType := reflect.TypeOf(items[0])
		for _, item := range items {
			if reflect.TypeOf(item) != firstType {
				return errors.New("all items in a batch update must be of the same type")
			}
		}

		// Start a transaction
		tx := db.Begin()
		if tx.Error != nil {
			return tx.Error
		}

		for _, item := range items {
			if err := tx.Updates(item).Error; err != nil {
				tx.Rollback()
				return err
			}
		}

		return tx.Commit().Error
	}
}
