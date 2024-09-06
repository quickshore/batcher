package gorm

import (
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
	batcher := NewBatcher(db, 3, 100*time.Millisecond)

	// Clean up the table before the test
	db.Exec("DELETE FROM test_models")

	var wg sync.WaitGroup
	for i := 1; i <= 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			err := batcher.Insert(&TestModel{Name: fmt.Sprintf("Test %d", i), Value: i})
			assert.NoError(t, err)
		}(i)
	}

	wg.Wait()

	// Check if all items were inserted
	var count int64
	db.Model(&TestModel{}).Count(&count)
	assert.Equal(t, int64(5), count)
}

func TestBatcher_Update(t *testing.T) {
	batcher := NewBatcher(db, 3, 100*time.Millisecond)

	// Clean up the table before the test
	db.Exec("DELETE FROM test_models")

	// Insert some initial data
	initialModels := []TestModel{
		{Name: "Test 1", Value: 10},
		{Name: "Test 2", Value: 20},
		{Name: "Test 3", Value: 30},
	}
	db.Create(&initialModels)

	var wg sync.WaitGroup
	for i := range initialModels {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			initialModels[i].Value += 5
			err := batcher.Update(&initialModels[i])
			assert.NoError(t, err)
		}(i)
	}

	wg.Wait()

	// Check if all items were updated
	var updatedModels []TestModel
	db.Find(&updatedModels)
	assert.Len(t, updatedModels, 3)
	for i, model := range updatedModels {
		assert.Equal(t, initialModels[i].Value, model.Value)
	}
}

func TestBatcher_EmptyBatch(t *testing.T) {
	_ = NewBatcher(db, 3, 100*time.Millisecond)

	// Clean up the table before the test
	db.Exec("DELETE FROM test_models")

	// No operations performed

	// Check that no items were inserted
	var count int64
	db.Model(&TestModel{}).Count(&count)
	assert.Equal(t, int64(0), count)
}

func TestBatcher_LargeBatch(t *testing.T) {
	batcher := NewBatcher(db, 1000, 1*time.Second)

	// Clean up the table before the test
	db.Exec("DELETE FROM test_models")

	var wg sync.WaitGroup
	for i := 1; i <= 2000; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			err := batcher.Insert(&TestModel{Name: fmt.Sprintf("Test %d", i), Value: i})
			assert.NoError(t, err)
		}(i)
	}

	wg.Wait()

	// Check if all items were inserted
	var count int64
	db.Model(&TestModel{}).Count(&count)
	assert.Equal(t, int64(2000), count)
}

func TestBatcher_ConcurrentOperations(t *testing.T) {
	batcher := NewBatcher(db, 10, 100*time.Millisecond)

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

	// Check if all items were inserted
	var count int64
	db.Model(&TestModel{}).Count(&count)
	assert.Equal(t, int64(operationCount), count)

	// Concurrent updates
	for i := 1; i <= operationCount; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			var model TestModel
			db.First(&model, i)
			model.Value += 1000
			err := batcher.Update(&model)
			assert.NoError(t, err)
		}(i)
	}

	wg.Wait()

	// Check if all items were updated
	var updatedModels []TestModel
	db.Find(&updatedModels)
	assert.Len(t, updatedModels, operationCount)
	for _, model := range updatedModels {
		assert.True(t, model.Value > 1000)
	}
}

func TestBatcher_MixedOperations(t *testing.T) {
	batcher := NewBatcher(db, 3, 100*time.Millisecond)

	// Clean up the table before the test
	db.Exec("DELETE FROM test_models")

	var wg sync.WaitGroup

	// Insert initial data
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := batcher.Insert(&TestModel{Name: "Test 1", Value: 10})
		require.NoError(t, err)
	}()

	wg.Wait()

	// Update and insert concurrently
	wg.Add(2)
	go func() {
		defer wg.Done()
		var model TestModel
		db.First(&model)
		model.Value = 20
		err := batcher.Update(&model)
		require.NoError(t, err)
	}()

	go func() {
		defer wg.Done()
		err := batcher.Insert(&TestModel{Name: "Test 2", Value: 30})
		require.NoError(t, err)
	}()

	wg.Wait()

	// Check if all operations were performed correctly
	var models []TestModel
	db.Find(&models)
	assert.Len(t, models, 2)
	assert.Equal(t, "Test 1", models[0].Name)
	assert.Equal(t, 20, models[0].Value)
	assert.Equal(t, "Test 2", models[1].Name)
	assert.Equal(t, 30, models[1].Value)
}
