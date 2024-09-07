package gorm

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func BenchmarkGORMBatcher(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Configuration
	numRoutines := 100
	operationsPerRoutine := 100
	maxBatchSize := 100
	maxWaitTime := 50 * time.Millisecond

	// Create batchers
	insertBatcher := NewInsertBatcher[*TestModel](getDBProvider(), maxBatchSize, maxWaitTime, ctx)
	updateBatcher := NewUpdateBatcher[*TestModel](getDBProvider(), maxBatchSize, maxWaitTime, ctx)

	// Clean up the table before the benchmark
	db.Exec("DELETE FROM test_models")

	// Prepare for benchmark
	var wg sync.WaitGroup
	startTime := time.Now()

	// Run benchmark
	b.ResetTimer()
	for i := 0; i < numRoutines; i++ {
		wg.Add(1)
		go func(routineID int) {
			defer wg.Done()
			for j := 0; j < operationsPerRoutine; j++ {
				if j%2 == 0 { // Even operations are inserts, odd are updates
					// Insert
					model := &TestModel{Name: fmt.Sprintf("Test %d-%d", routineID, j), Value: j}
					err := insertBatcher.Insert(model)
					assert.NoError(b, err)
				} else {
					// Update
					model := &TestModel{ID: uint((routineID*operationsPerRoutine + j + 1) / 2), Value: j * 10}
					err := updateBatcher.Update([]*TestModel{model}, []string{"Value"})
					assert.NoError(b, err)
				}
			}
		}(i)
	}

	wg.Wait()
	duration := time.Since(startTime)

	b.StopTimer()

	// Verify data integrity
	var count int64
	db.Model(&TestModel{}).Count(&count)
	expectedCount := int64(numRoutines * operationsPerRoutine / 2) // Half of operations are inserts

	var sumValue int64
	db.Model(&TestModel{}).Select("SUM(value)").Row().Scan(&sumValue)

	// Calculate expected sum
	expectedSum := int64(0)
	for i := 0; i < numRoutines; i++ {
		for j := 0; j < operationsPerRoutine; j++ {
			if j%2 == 0 {
				expectedSum += int64(j) // Initial insert value
			} else {
				expectedSum += int64(j * 10) // Updated value
			}
		}
	}

	// Print statistics and verification results
	fmt.Printf("Benchmark Statistics:\n")
	fmt.Printf("Total Duration: %v\n", duration)
	fmt.Printf("Total Operations: %d\n", numRoutines*operationsPerRoutine)
	fmt.Printf("Inserts: %d\n", numRoutines*operationsPerRoutine/2)
	fmt.Printf("Updates: %d\n", numRoutines*operationsPerRoutine/2)
	fmt.Printf("Operations per second: %.2f\n", float64(numRoutines*operationsPerRoutine)/duration.Seconds())
	fmt.Printf("Average time per operation: %v\n", duration/time.Duration(numRoutines*operationsPerRoutine))
	fmt.Printf("Total records in database: %d (Expected: %d)\n", count, expectedCount)
	fmt.Printf("Sum of all values in database: %d (Expected: %d)\n", sumValue, expectedSum)

	// Assertions
	assert.Equal(b, expectedCount, count, "Record count mismatch")
	assert.Equal(b, expectedSum, sumValue, "Sum of values mismatch")
}
