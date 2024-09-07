package gorm

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

func BenchmarkGORMBatcher(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Configuration
	numRoutines := 100
	operationsPerRoutine := 1000
	maxBatchSize := 100
	maxWaitTime := 5 * time.Millisecond

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
				if j%2 == 0 { // Even operations are inserts
					// Insert
					id := routineID*operationsPerRoutine + j/2 + 1
					model := &TestModel{ID: uint(id), Name: fmt.Sprintf("Test %d-%d", routineID, j), Value: j}
					err := insertBatcher.Insert(model)
					if err != nil {
						b.Logf("Insert error: %v", err)
					}
				} else { // Odd operations are updates
					// Update
					id := routineID*operationsPerRoutine + j/2 + 1
					model := &TestModel{ID: uint(id), Value: j * 10}
					err := updateBatcher.Update([]*TestModel{model}, []string{"Value"})
					if err != nil {
						b.Logf("Update error: %v", err)
					}
				}
			}
		}(i)
	}

	wg.Wait()
	duration := time.Since(startTime)

	b.StopTimer()

	// Allow time for any pending operations to complete
	time.Sleep(5 * time.Second)

	// Verify data integrity
	var count int64
	db.Model(&TestModel{}).Count(&count)
	expectedCount := int64(numRoutines * operationsPerRoutine / 2) // Half of operations are inserts

	var sumValue int64
	db.Model(&TestModel{}).Select("SUM(value)").Row().Scan(&sumValue)

	// Calculate expected sum
	expectedSum := int64(0)
	for i := 0; i < numRoutines*operationsPerRoutine/2; i++ {
		expectedSum += int64(i * 2 * 10) // Each record is updated to j * 10, where j is odd
	}

	// Print statistics and verification results
	b.Logf("Benchmark Statistics:")
	b.Logf("Total Duration: %v", duration)
	b.Logf("Total Operations: %d", numRoutines*operationsPerRoutine)
	b.Logf("Inserts: %d", numRoutines*operationsPerRoutine/2)
	b.Logf("Updates: %d", numRoutines*operationsPerRoutine/2)
	b.Logf("Operations per second: %.2f", float64(numRoutines*operationsPerRoutine)/duration.Seconds())
	b.Logf("Average time per operation: %v", duration/time.Duration(numRoutines*operationsPerRoutine))
	b.Logf("Total records in database: %d (Expected: %d)", count, expectedCount)
	b.Logf("Sum of all values in database: %d (Expected: %d)", sumValue, expectedSum)

	// Assertions
	if count != expectedCount {
		b.Errorf("Record count mismatch. Got: %d, Expected: %d", count, expectedCount)
	}
	if sumValue != expectedSum {
		b.Errorf("Sum of values mismatch. Got: %d, Expected: %d", sumValue, expectedSum)
	}

	// Print the first 10 and last 10 records for debugging
	var firstTenRecords, lastTenRecords []TestModel
	db.Limit(10).Order("id ASC").Find(&firstTenRecords)
	db.Limit(10).Order("id DESC").Find(&lastTenRecords)
	b.Logf("First 10 records:")
	for _, record := range firstTenRecords {
		b.Logf("ID: %d, Name: %s, Value: %d", record.ID, record.Name, record.Value)
	}
	b.Logf("Last 10 records:")
	for _, record := range lastTenRecords {
		b.Logf("ID: %d, Name: %s, Value: %d", record.ID, record.Name, record.Value)
	}
}
