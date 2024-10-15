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

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		db.Exec("DELETE FROM bench_test_models")

		var wg sync.WaitGroup
		metrics := newMetrics()

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
						metrics.recordOperation(&metrics.Insert, opStart, err)
					case 1: // Update
						model := &BenchTestModel{ID: uint(id), MyValue1: k * 10, MyValue2: k * 20}
						if routineID%2 == 0 {
							err = updateBatcher.Update([]*BenchTestModel{model}, []string{"my_value1"})
						} else {
							err = updateBatcher.Update([]*BenchTestModel{model}, []string{"my_value1", "my_value2"})
						}
						metrics.recordOperation(&metrics.Update, opStart, err)
					case 2: // Select
						_, err = selectBatcher.Select("id = ?", id)
						metrics.recordOperation(&metrics.Select, opStart, err)
					}
				}
			}(j)
		}

		wg.Wait()
		totalDuration := time.Since(start)

		metrics.reportMetrics(b, totalDuration)

		verifyDataIntegrity(b, db, numRoutines, operationsPerRoutine)
	}
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
	Insert operationMetrics
	Update operationMetrics
	Select operationMetrics
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

func (m *benchmarkMetrics) reportMetrics(b *testing.B, totalDuration time.Duration) {
	reportOperationMetrics(b, "insert", &m.Insert, totalDuration)
	reportOperationMetrics(b, "update", &m.Update, totalDuration)
	reportOperationMetrics(b, "select", &m.Select, totalDuration)
}

func reportOperationMetrics(b *testing.B, opName string, op *operationMetrics, totalDuration time.Duration) {
	opsPerSec := float64(op.Count) / totalDuration.Seconds()
	avgDuration := float64(op.Duration) / float64(op.Count)

	b.ReportMetric(opsPerSec, opName+"s/sec")
	b.ReportMetric(avgDuration, "avg_"+opName+"_duration")
	b.ReportMetric(float64(op.Errors), opName+"_errors")
}
