# AtlasGurus Batcher

AtlasGurus Batcher is a Go module that provides efficient batching functionality for various operations, with a specific implementation for GORM database operations. It consists of two main packages and a utility adapter:

1. `batcher`: A generic batching package
2. `gorm`: A GORM-specific implementation using the batcher package
3. GORM v1 to v2 adapter: A utility for converting GORM v1 connections to v2

## Features

- Generic batching functionality with configurable batch size and wait time
- GORM-specific batching for both insert and update operations
- Support for both GORM v1 and v2
- Concurrent operation support
- Type-safe implementations using Go generics

## Installation

To install AtlasGurus Batcher, use `go get`:

```
go get atlasgurus/batcher
```

## Usage

### Generic Batcher Package

The `batcher` package provides a generic batching mechanism that can be used for any type of operation:

```go
import (
    "context"
    "time"
    "atlasgurus/batcher"
)

// Create a new batch processor
processor := batcher.NewBatchProcessor(
    100, // maxBatchSize
    5*time.Second, // maxWaitTime
    context.Background(),
    func(items []YourType) error {
        // Process the batch of items
        return nil
    },
)

// Submit items for processing
err := processor.SubmitAndWait(item)
if err != nil {
    // Handle error
}
```

### GORM Batcher Package

The `gorm` package provides GORM-specific batching for insert and update operations:

```go
import (
    "context"
    "time"
    "atlasgurus/batcher/gorm"
    "gorm.io/gorm"
)

// Assuming you have a GORM v2 DB instance
db, _ := gorm.Open(...)

// Create an insert batcher
insertBatcher := gorm.NewInsertBatcher[*YourModel](db, 100, 5*time.Second, context.Background())

// Use the insert batcher
err := insertBatcher.Insert(&YourModel{...})
if err != nil {
    // Handle error
}

// Create an update batcher
updateBatcher := gorm.NewUpdateBatcher[*YourModel](db, 100, 5*time.Second, context.Background(), []string{"FieldToUpdate"})

// Use the update batcher
err = updateBatcher.Update(&YourModel{...})
if err != nil {
    // Handle error
}
```

### GORM v1 to v2 Adapter

If you're using GORM v1, you can use the provided adapter to convert your v1 connection to v2:

```go
import (
    gormv1 "github.com/jinzhu/gorm"
    "atlasgurus/batcher/gorm"
)

// Open a GORM v1 connection
v1DB, _ := gormv1.Open("mysql", "connection_string")

// Convert to GORM v2
v2DB, _ := gorm.GormV1ToV2Adapter(v1DB)

// Now you can use v2DB with the GORM batcher or any other GORM v2 operations
```

## Configuration

### Generic Batcher

`NewBatchProcessor` takes the following parameters:

- `maxBatchSize`: The maximum number of items to include in a single batch
- `maxWaitTime`: The maximum time to wait before processing a batch
- `ctx`: A context for cancellation
- `processFn`: A function to process the batched items

### GORM Batcher

Both `NewInsertBatcher` and `NewUpdateBatcher` take the following parameters:

- `db`: A GORM v2 database instance
- `maxBatchSize`: The maximum number of items to include in a single batch
- `maxWaitTime`: The maximum time to wait before processing a batch
- `ctx`: A context for cancellation
- `updateFields` (for update batcher only): An optional list of fields to update

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.