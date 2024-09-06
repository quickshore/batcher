package gorm

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

type TestModel struct {
	ID    uint   `gorm:"primaryKey"`
	Name  string `gorm:"type:varchar(100)"`
	Value int    `gorm:"type:int"`
}

var db *gorm.DB

func TestMain(m *testing.M) {
	// Get the DSN from environment variable
	dsn := os.Getenv("DSN")
	if dsn == "" {
		panic("DSN environment variable is not set")
	}

	var err error
	db, err = gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		panic("failed to connect database")
	}

	// Migrate the schema
	err = db.AutoMigrate(&TestModel{})
	if err != nil {
		panic("failed to migrate database")
	}

	// Run the tests
	code := m.Run()

	// Clean up
	sqlDB, _ := db.DB()
	_ = sqlDB.Close()

	os.Exit(code)
}

func TestBatcher_Insert(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	batcher := NewBatcher(db, 3, 100*time.Millisecond, ctx)

	// Clean up the table before the test
	db.Exec("DELETE FROM test_models")

	// Insert 5 items
	for i := 1; i <= 5; i++ {
		err := batcher.Insert(&TestModel{Name: fmt.Sprintf("Test %d", i), Value: i})
		assert.NoError(t, err)
	}

	time.Sleep(200 * time.Millisecond) // Wait for batches to be processed

	// Check if all items were inserted
	var count int64
	db.Model(&TestModel{}).Count(&count)
	assert.Equal(t, int64(5), count)
}

func TestBatcher_Update(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	batcher := NewBatcher(db, 3, 100*time.Millisecond, ctx)

	// Clean up the table before the test
	db.Exec("DELETE FROM test_models")

	// Insert some initial data
	initialModels := []TestModel{
		{Name: "Test 1", Value: 10},
		{Name: "Test 2", Value: 20},
		{Name: "Test 3", Value: 30},
	}
	db.Create(&initialModels)

	// Update the models
	for i := range initialModels {
		initialModels[i].Value += 5
		err := batcher.Update(&initialModels[i])
		assert.NoError(t, err)
	}

	time.Sleep(200 * time.Millisecond) // Wait for batches to be processed

	// Check if all items were updated
	var updatedModels []TestModel
	db.Find(&updatedModels)
	assert.Len(t, updatedModels, 3)
	for i, model := range updatedModels {
		assert.Equal(t, initialModels[i].Value, model.Value)
	}
}

func TestBatcher_ConcurrentOperations(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	batcher := NewBatcher(db, 10, 100*time.Millisecond, ctx)

	// Clean up the table before the test
	db.Exec("DELETE FROM test_models")

	var wg sync.WaitGroup
	operationCount := 100

	// Concurrent inserts
	for i := 1; i <= operationCount; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			err := batcher.Insert(&TestModel{Name: fmt.Sprintf("Test %d", i), Value: i})
			assert.NoError(t, err)
		}(i)
	}

	wg.Wait()
	time.Sleep(200 * time.Millisecond) // Wait for batches to be processed

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
			err := batcher.Update(&updatedModels[i])
			assert.NoError(t, err)
		}(i)
	}

	wg.Wait()
	time.Sleep(200 * time.Millisecond) // Wait for batches to be processed

	// Check if all items were updated
	db.Find(&updatedModels)
	assert.Len(t, updatedModels, operationCount)
	for _, model := range updatedModels {
		assert.True(t, model.Value > 1000, "Expected Value to be greater than 1000, got %d for ID %d", model.Value, model.ID)
	}
}

func TestBatcher_MixedOperations(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	batcher := NewBatcher(db, 3, 100*time.Millisecond, ctx)

	// Clean up the table before the test
	db.Exec("DELETE FROM test_models")

	// Insert some initial data
	initialModel := TestModel{Name: "Test 1", Value: 10}
	err := batcher.Insert(&initialModel)
	require.NoError(t, err)

	time.Sleep(200 * time.Millisecond) // Wait for batch to be processed

	// Fetch the inserted model
	var modelToUpdate TestModel
	db.First(&modelToUpdate, "name = ?", initialModel.Name)

	// Update the model
	modelToUpdate.Value = 20
	err = batcher.Update(&modelToUpdate)
	require.NoError(t, err)

	// Insert a new model
	newModel := TestModel{Name: "Test 2", Value: 30}
	err = batcher.Insert(&newModel)
	require.NoError(t, err)

	time.Sleep(200 * time.Millisecond) // Wait for batches to be processed

	// Check if all operations were performed correctly
	var models []TestModel
	db.Find(&models)
	assert.Len(t, models, 2)
	assert.Equal(t, "Test 1", models[0].Name)
	assert.Equal(t, 20, models[0].Value)
	assert.Equal(t, "Test 2", models[1].Name)
	assert.Equal(t, 30, models[1].Value)
}

func TestBatcher_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	batcher := NewBatcher(db, 3, 100*time.Millisecond, ctx)

	// Clean up the table before the test
	db.Exec("DELETE FROM test_models")

	// Insert some items
	for i := 1; i <= 5; i++ {
		err := batcher.Insert(&TestModel{Name: fmt.Sprintf("Test %d", i), Value: i})
		assert.NoError(t, err)
	}

	// Cancel the context
	cancel()

	// Try to insert after cancellation
	err := batcher.Insert(&TestModel{Name: "Should not be inserted", Value: 100})
	assert.Error(t, err)

	time.Sleep(200 * time.Millisecond) // Wait for any ongoing operations to complete

	// Check the number of inserted items
	var count int64
	db.Model(&TestModel{}).Count(&count)
	assert.True(t, count > 0 && count <= 5, "Expected between 1 and 5 items, got %d", count)
}
