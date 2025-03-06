# DataBuffer

A lightweight, generic Go library that provides buffered, concurrent data processing with configurable behavior.

## Overview

DataBuffer maintains a pool of worker goroutines, each with its own buffer. Workers collect data from an input channel and batch process it using a user-provided report function. The buffer is automatically flushed when either:

1. The buffer reaches its configured maximum size (`MaxBufferSize`)
2. A configurable time limit (`WorkerWait`) has elapsed

## Features

- Generic implementation (`[T any]`) to support any data type
- Configurable number of worker goroutines
- Automatic buffer management with size and time-based triggers
- Graceful error handling with optional hard limits
- Context-based cancellation support
- Structured logging with `log/slog`
- Functional options pattern for flexible configuration

## Bound vs Unbound

`MaxBufferSize` is the max size of the worker buffers before they will try to send items through the report function. The buffers will grow as large as necessary until they are successfully sent.

If `BufferHardLimit` is set to 0, the buffers are unbound and will grow indefinitely until the data is successfully sent off through the report function. If not set to 0, the minimum valid value is equal to `MaxBufferSize`. The default is `MaxBufferSize * 2`. If the buffer size reaches the value set for `BufferHardLimit`, then all items in the buffer will attempt to be sent through the report function, and be immediately dropped if the report function returns an error. If the data is critical, the report function should make sure to deal with any errors.

**NOTE:** The supplied report function needs to be concurrency safe.

## Default Configuration

DataBuffer comes with reasonable defaults:

| Option | Default Value |
|--------|---------------|
| `MaxBufferSize` | 64 |
| `BufferHardLimit` | MaxBufferSize * 2 (128) |
| `NumWorkers` | 2 |
| `WorkerWait` | 1 minute |
| `ChanBufferSize` | 4 * NumWorkers (8) |
| `Reporter` | DefaultReporter (no-op implementation) |
| `Logger` | Default slog.Logger |

## Example

```go
package main

import (
	"context"
	"time"

	"github.com/scottso/databuffer"
)

type reporter struct{}

// Must be concurrency safe
func (r reporter) Report(ctx context.Context, data []int) error {
	// ...Do stuff with data...
	return nil
}

func main() {
	r := reporter{}

	// Create a new DataBuffer with custom options
	dBuf, err := databuffer.New(
		databuffer.MaxBufferSize[int](1024),
		databuffer.BufferHardLimit[int](4096), 
		databuffer.NumWorkers[int](4),
		databuffer.WorkerWait[int](5*time.Minute),
		databuffer.SetReporter(r),  // Pass the reporter struct that implements Report
		databuffer.ChanBufferSize[int](16),
	)
	if err != nil {
		panic(err)
	}

	// Create a context that we can cancel
	ctx, cancel := context.WithCancel(context.Background())

	// Start the workers
	dBuf.Start(ctx)

	// Send some data
	go func() {
		for i := 0; i < 10000; i++ {
			dBuf.WorkerChan() <- []int{i}
		}
		cancel()
	}()

	<-ctx.Done()
}
```

## Configuration Options

DataBuffer uses the functional options pattern for configuration:

| Option | Description |
|--------|-------------|
| `MaxBufferSize[T](int)` | Maximum size before a buffer is flushed |
| `BufferHardLimit[T](int)` | Maximum size before dropping data on error |
| `NumWorkers[T](int)` | Number of worker goroutines |
| `WorkerWait[T](time.Duration)` | Maximum time before a buffer is flushed |
| `ChanBufferSize[T](int)` | Size of the input channel buffer |
| `SetReporter[T](Reporter[T])` | Interface that implements Report method |
| `Logger[T](*slog.Logger)` | Custom logger instance |

## Implementation Details

- Each worker has its own buffer that starts empty with a capacity of `MaxBufferSize`.
- Workers are staggered on startup to prevent all workers from flushing simultaneously.
- When context is cancelled, remaining data is flushed before shutdown.
- The first worker (ID 0) is responsible for closing the input channel on context cancellation.

## Error Handling

If the report function returns an error:
- For buffers below the hard limit, data is kept and will be retried.
- For buffers at or above the hard limit, data is dropped and a warning is logged.

For critical data, ensure your report function handles errors properly.