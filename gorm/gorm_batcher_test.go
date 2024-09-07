package gorm

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	gormv1 "github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mysql"
	_ "github.com/jinzhu/gorm/dialects/postgres"
	_ "github.com/jinzhu/gorm/dialects/sqlite"
	"github.com/stretchr/testify/assert"
	gormv2 "gorm.io/gorm"
)

type TestModel struct {
	ID    uint   `gorm:"primaryKey"`
	Name  string `gorm:"type:varchar(100)"`
	Value int    `gorm:"type:int"`
}

type CompositeKeyModel struct {
	ID1   int    `gorm:"primaryKey"`
	ID2   string `gorm:"primaryKey"`
	Name  string
	Value int
}

var (
	db      *gormv2.DB
	dialect string
)

func TestMain(m *testing.M) {
	// Get the DSN and dialect from environment variables
	dsn := os.Getenv("DSN")
	dialect = os.Getenv("DIALECT")
	if dsn == "" {
		panic("DSN environment variable must be set")
	}
	if dialect == "" {
		dialect = "mysql" // Default to MySQL if DIALECT is not set
	}

	var err error
	var v1DB *gormv1.DB

	// Open a GORM v1 connection based on the dialect
	switch dialect {
	case "mysql", "postgres", "sqlite3":
		v1DB, err = gormv1.Open(dialect, dsn)
	default:
		panic("Unsupported dialect: " + dialect)
	}

	if err != nil {
		panic(fmt.Sprintf("failed to connect database: %v", err))
	}

	// Convert to GORM v2
	db, err = GormV1ToV2Adapter(v1DB)
	if err != nil {
		panic(fmt.Sprintf("failed to convert GORM v1 to v2: %v", err))
	}

	// Migrate the schema
	err = db.AutoMigrate(&TestModel{}, &CompositeKeyModel{})
	if err != nil {
		panic(fmt.Sprintf("failed to migrate database: %v", err))
	}

	// Run the tests
	code := m.Run()

	// Clean up
	v1DB.Close()

	os.Exit(code)
}

func getDBProvider() DBProvider {
	return func() (*gormv2.DB, error) {
		return db, nil
	}
}

func TestInsertBatcher(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	batcher := NewInsertBatcher[*TestModel](getDBProvider(), 3, 100*time.Millisecond, ctx)

	// Clean up the table before the test
	db.Exec("DELETE FROM test_models")

	// Insert single items
	for i := 1; i <= 3; i++ {
		err := batcher.Insert(&TestModel{Name: fmt.Sprintf("Single %d", i), Value: i})
		assert.NoError(t, err)
	}

	// Insert multiple items at once
	multipleItems := []*TestModel{
		{Name: "Multiple 1", Value: 4},
		{Name: "Multiple 2", Value: 5},
	}
	err := batcher.Insert(multipleItems...)
	assert.NoError(t, err)

	// Check if all items were inserted
	var count int64
	db.Model(&TestModel{}).Count(&count)
	assert.Equal(t, int64(5), count)

	var insertedModels []TestModel
	db.Find(&insertedModels)
	assert.Len(t, insertedModels, 5)
	for i, model := range insertedModels {
		if i < 3 {
			assert.Equal(t, fmt.Sprintf("Single %d", i+1), model.Name)
			assert.Equal(t, i+1, model.Value)
		} else {
			assert.Equal(t, fmt.Sprintf("Multiple %d", i-2), model.Name)
			assert.Equal(t, i+1, model.Value)
		}
	}
}

func TestUpdateBatcher(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	batcher := NewUpdateBatcher[*TestModel](getDBProvider(), 3, 100*time.Millisecond, ctx)

	// Clean up the table before the test
	db.Exec("DELETE FROM test_models")

	// Insert some initial data
	initialModels := []*TestModel{
		{Name: "Test 1", Value: 10},
		{Name: "Test 2", Value: 20},
		{Name: "Test 3", Value: 30},
		{Name: "Test 4", Value: 40},
		{Name: "Test 5", Value: 50},
	}
	db.Create(&initialModels)

	// Update single items
	for i := 0; i < 3; i++ {
		initialModels[i].Value += 5
		err := batcher.Update([]*TestModel{initialModels[i]}, []string{"Value"})
		assert.NoError(t, err)
	}

	// Update multiple items at once
	for i := 3; i < 5; i++ {
		initialModels[i].Value += 10
	}
	err := batcher.Update([]*TestModel{initialModels[3], initialModels[4]}, []string{"Value"})
	assert.NoError(t, err)

	// Check if all items were updated correctly
	var updatedModels []TestModel
	db.Find(&updatedModels)
	assert.Len(t, updatedModels, 5)
	for i, model := range updatedModels {
		if i < 3 {
			assert.Equal(t, initialModels[i].Value, model.Value)
		} else {
			assert.Equal(t, initialModels[i].Value, model.Value)
		}
		assert.Equal(t, fmt.Sprintf("Test %d", i+1), model.Name)
	}
}

func TestConcurrentOperations(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	insertBatcher := NewInsertBatcher[*TestModel](getDBProvider(), 10, 100*time.Millisecond, ctx)
	updateBatcher := NewUpdateBatcher[*TestModel](getDBProvider(), 10, 100*time.Millisecond, ctx)

	// Clean up the table before the test
	db.Exec("DELETE FROM test_models")

	var wg sync.WaitGroup
	operationCount := 100

	// Concurrent inserts
	for i := 1; i <= operationCount; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			err := insertBatcher.Insert(&TestModel{Name: fmt.Sprintf("Test %d", i), Value: i})
			assert.NoError(t, err)
		}(i)
	}

	wg.Wait()

	// Check if all items were inserted
	var count int64
	db.Model(&TestModel{}).Count(&count)
	assert.Equal(t, int64(operationCount), count)

	// Concurrent updates
	var updatedModels []TestModel
	db.Find(&updatedModels)

	for i := range updatedModels {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			updatedModels[i].Value += 1000
			err := updateBatcher.Update([]*TestModel{&updatedModels[i]}, nil) // Update all fields
			assert.NoError(t, err)
		}(i)
	}

	wg.Wait()

	// Check if all items were updated
	db.Find(&updatedModels)
	assert.Len(t, updatedModels, operationCount)
	for _, model := range updatedModels {
		assert.True(t, model.Value > 1000, "Expected Value to be greater than 1000, got %d for ID %d", model.Value, model.ID)
	}
}

func TestUpdateBatcher_AllFields(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	batcher := NewUpdateBatcher[*TestModel](getDBProvider(), 3, 100*time.Millisecond, ctx)

	db.Exec("DELETE FROM test_models")

	initialModels := []TestModel{
		{Name: "Test 1", Value: 10},
		{Name: "Test 2", Value: 20},
		{Name: "Test 3", Value: 30},
	}
	db.Create(&initialModels)

	updatedModels := make([]*TestModel, len(initialModels))
	for i := range initialModels {
		updatedModels[i] = &TestModel{
			ID:    initialModels[i].ID,
			Name:  fmt.Sprintf("Updated %d", i+1),
			Value: initialModels[i].Value + 5,
		}
	}

	err := batcher.Update(updatedModels, nil) // Update all fields
	assert.NoError(t, err)

	var finalModels []TestModel
	db.Find(&finalModels)
	assert.Len(t, finalModels, 3)
	for i, model := range finalModels {
		fmt.Printf("Model after update: %+v\n", model)
		assert.Equal(t, fmt.Sprintf("Updated %d", i+1), model.Name)
		assert.Equal(t, initialModels[i].Value+5, model.Value)
	}
}

func TestUpdateBatcher_SpecificFields(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	batcher := NewUpdateBatcher[*TestModel](getDBProvider(), 3, 100*time.Millisecond, ctx)

	db.Exec("DELETE FROM test_models")

	initialModels := []TestModel{
		{Name: "Test 1", Value: 10},
		{Name: "Test 2", Value: 20},
		{Name: "Test 3", Value: 30},
	}
	db.Create(&initialModels)

	updatedModels := make([]*TestModel, len(initialModels))
	for i := range initialModels {
		updatedModels[i] = &TestModel{
			ID:    initialModels[i].ID,
			Name:  fmt.Sprintf("Should Not Update %d", i+1),
			Value: initialModels[i].Value + 10,
		}
	}

	err := batcher.Update(updatedModels, []string{"Value"})
	assert.NoError(t, err)

	var finalModels []TestModel
	db.Find(&finalModels)
	assert.Len(t, finalModels, 3)
	for i, model := range finalModels {
		fmt.Printf("Model after update: %+v\n", model)
		assert.Equal(t, fmt.Sprintf("Test %d", i+1), model.Name, "Name should not have been updated")
		assert.Equal(t, initialModels[i].Value+10, model.Value, "Value should have been updated")
	}
}

func TestUpdateBatcher_CompositeKey(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	batcher := NewUpdateBatcher[*CompositeKeyModel](getDBProvider(), 3, 100*time.Millisecond, ctx)

	db.Exec("DELETE FROM composite_key_models")

	initialModels := []CompositeKeyModel{
		{ID1: 1, ID2: "A", Name: "Test 1", Value: 10},
		{ID1: 1, ID2: "B", Name: "Test 2", Value: 20},
		{ID1: 2, ID2: "A", Name: "Test 3", Value: 30},
	}
	db.Create(&initialModels)

	updatedModels := make([]*CompositeKeyModel, len(initialModels))
	for i := range initialModels {
		updatedModels[i] = &CompositeKeyModel{
			ID1:   initialModels[i].ID1,
			ID2:   initialModels[i].ID2,
			Name:  fmt.Sprintf("Updated %d", i+1),
			Value: initialModels[i].Value + 5,
		}
	}

	err := batcher.Update(updatedModels, []string{"Name", "Value"})
	assert.NoError(t, err)

	var finalModels []CompositeKeyModel
	db.Find(&finalModels)
	assert.Len(t, finalModels, 3)
	for i, model := range finalModels {
		fmt.Printf("Model after update: %+v\n", model)
		assert.Equal(t, fmt.Sprintf("Updated %d", i+1), model.Name)
		assert.Equal(t, initialModels[i].Value+5, model.Value)
		assert.Equal(t, initialModels[i].ID1, model.ID1)
		assert.Equal(t, initialModels[i].ID2, model.ID2)
	}
}
