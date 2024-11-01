package gorm

import (
	"context"
	"fmt"
	"gorm.io/gorm"
	"sync"
	"testing"
	"time"
)

type BenchTestModel struct {
	ID       uint   `gorm:"primary_key"`
	Name     string `gorm:"type:varchar(100)"`
	MyValue1 int    `gorm:"type:int"`
	MyValue2 int    `gorm:"type:int"`
}

func BenchmarkGORMBatcher(b *testing.B) {
	// Configuration
	const (
		numRoutines          = 100
		operationsPerRoutine = 99
		maxBatchSize         = 30
		maxWaitTime          = 1 * time.Millisecond
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	insertBatcher := NewInsertBatcher[*BenchTestModel](getDBProvider(), maxBatchSize, maxWaitTime, ctx)
	updateBatcher, err := NewUpdateBatcher[*BenchTestModel](getDBProvider(), maxBatchSize, maxWaitTime, ctx)
	if err != nil {
		b.Fatalf("Failed to create update batcher: %v", err)
	}
	selectBatcher, err := NewSelectBatcher[*BenchTestModel](getDBProvider(), maxBatchSize, maxWaitTime, ctx, []string{"id", "name", "my_value1", "my_value2"})
	if err != nil {
		b.Fatalf("Failed to create select batcher: %v", err)
	}

	err = db.AutoMigrate(&BenchTestModel{}, &CompositeKeyModel{})
	if err != nil {
		b.Fatalf("Failed to migrate database: %v", err)
	}

	metrics := newMetrics()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		db.Exec("DELETE FROM bench_test_models")

		var wg sync.WaitGroup
		iterationMetrics := newMetrics()

		start := time.Now()

		for j := 0; j < numRoutines; j++ {
			wg.Add(1)
			go func(routineID int) {
				defer wg.Done()
				for k := 0; k < operationsPerRoutine; k++ {
					operationType := k % 3 // 0: Insert, 1: Update, 2: Select
					id := routineID*operationsPerRoutine + k/3 + 1

					opStart := time.Now()
					var err error

					switch operationType {
					case 0: // Insert
						model := &BenchTestModel{ID: uint(id), Name: fmt.Sprintf("Test %d-%d", routineID, k), MyValue1: k}
						err = insertBatcher.Insert(model)
						iterationMetrics.recordOperation(&iterationMetrics.Insert, opStart, err)
					case 1: // Update
						model := &BenchTestModel{ID: uint(id), MyValue1: k * 10, MyValue2: k * 20}
						if routineID%2 == 0 {
							err = updateBatcher.Update([]*BenchTestModel{model}, []string{"my_value1"})
						} else {
							err = updateBatcher.Update([]*BenchTestModel{model}, []string{"my_value1", "my_value2"})
						}
						iterationMetrics.recordOperation(&iterationMetrics.Update, opStart, err)
					case 2: // Select
						_, err = selectBatcher.Select("id = ?", id)
						iterationMetrics.recordOperation(&iterationMetrics.Select, opStart, err)
					}
				}
			}(j)
		}

		wg.Wait()
		iterationDuration := time.Since(start)

		metrics.accumulateMetrics(iterationMetrics, iterationDuration)

		verifyDataIntegrity(b, db, numRoutines, operationsPerRoutine)
	}

	metrics.printMetrics(b.N)
}

func verifyDataIntegrity(b *testing.B, db *gorm.DB, numRoutines, operationsPerRoutine int) {
	var count int64
	db.Model(&BenchTestModel{}).Count(&count)
	expectedCount := int64(numRoutines * operationsPerRoutine / 3) // One-third of operations are inserts

	var sumValue1, sumValue2 int64
	db.Model(&BenchTestModel{}).Select("SUM(my_value1) as sum_value1, SUM(my_value2) as sum_value2").Row().Scan(&sumValue1, &sumValue2)

	expectedSum := int64(0)
	for routineID := 0; routineID < numRoutines; routineID++ {
		for j := 0; j < operationsPerRoutine; j++ {
			if j%3 == 1 { // Update operations
				expectedSum += int64(j * 10)
			}
		}
	}

	if count != expectedCount {
		b.Errorf("Record count mismatch. Got: %d, Expected: %d", count, expectedCount)
	}
	if sumValue1 != expectedSum {
		b.Errorf("Sum of MyValue1 mismatch. Got: %d, Expected: %d", sumValue1, expectedSum)
	}
	if sumValue2 != expectedSum {
		b.Errorf("Sum of MyValue2 mismatch. Got: %d, Expected: %d", sumValue2, expectedSum)
	}
}

type operationMetrics struct {
	Count    int64
	Errors   int64
	Duration time.Duration
}

type benchmarkMetrics struct {
	sync.Mutex
	Insert        operationMetrics
	Update        operationMetrics
	Select        operationMetrics
	TotalDuration time.Duration
}

func newMetrics() *benchmarkMetrics {
	return &benchmarkMetrics{}
}

func (m *benchmarkMetrics) recordOperation(op *operationMetrics, start time.Time, err error) {
	m.Lock()
	defer m.Unlock()
	op.Count++
	op.Duration += time.Since(start)
	if err != nil {
		op.Errors++
	}
}

func (m *benchmarkMetrics) accumulateMetrics(iteration *benchmarkMetrics, duration time.Duration) {
	m.Lock()
	defer m.Unlock()
	m.Insert.Count += iteration.Insert.Count
	m.Insert.Errors += iteration.Insert.Errors
	m.Insert.Duration += iteration.Insert.Duration
	m.Update.Count += iteration.Update.Count
	m.Update.Errors += iteration.Update.Errors
	m.Update.Duration += iteration.Update.Duration
	m.Select.Count += iteration.Select.Count
	m.Select.Errors += iteration.Select.Errors
	m.Select.Duration += iteration.Select.Duration
	m.TotalDuration += duration
}

func (m *benchmarkMetrics) printMetrics(iterations int) {
	fmt.Println("Benchmark Statistics:")
	fmt.Printf("Total Duration: %v\n", m.TotalDuration)
	fmt.Printf("Total Operations: %d\n", m.Insert.Count+m.Update.Count+m.Select.Count)

	fmt.Println("\nInsert Operations:")
	fmt.Printf("  Count: %d\n", m.Insert.Count)
	fmt.Printf("  Errors: %d\n", m.Insert.Errors)
	fmt.Printf("  Total Duration: %v\n", m.Insert.Duration)
	fmt.Printf("  Average Duration: %v\n", time.Duration(int64(m.Insert.Duration)/m.Insert.Count))

	fmt.Println("\nUpdate Operations:")
	fmt.Printf("  Count: %d\n", m.Update.Count)
	fmt.Printf("  Errors: %d\n", m.Update.Errors)
	fmt.Printf("  Total Duration: %v\n", m.Update.Duration)
	fmt.Printf("  Average Duration: %v\n", time.Duration(int64(m.Update.Duration)/m.Update.Count))

	fmt.Println("\nSelect Operations:")
	fmt.Printf("  Count: %d\n", m.Select.Count)
	fmt.Printf("  Errors: %d\n", m.Select.Errors)
	fmt.Printf("  Total Duration: %v\n", m.Select.Duration)
	fmt.Printf("  Average Duration: %v\n", time.Duration(int64(m.Select.Duration)/m.Select.Count))

	fmt.Println("\nOverall:")
	totalOps := float64(m.Insert.Count + m.Update.Count + m.Select.Count)
	fmt.Printf("  Operations per second: %.2f\n", totalOps/m.TotalDuration.Seconds())
	fmt.Printf("  Average time per operation: %v\n", time.Duration(int64(m.TotalDuration)/int64(totalOps)))

	fmt.Printf("\nNumber of iterations: %d\n", iterations)
	fmt.Printf("Average operations per iteration: %.2f\n", totalOps/float64(iterations))
}
