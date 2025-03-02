# databuffer

This is a simple data buffer that runs in background worker goroutines, one per worker. Each worker has it's own buffer that starts at `MaxBufferSize`. It takes a slice of any type as input from a channel and each worker batches the data up in for processing by a user provided report function. The workers will send/report their buffer data at a configurable time limit, or when the buffer is full, whichever happens first. Behavior is customizable with an options struct. The default options are fully functional.

You can retrieve the default values with the `GetDefaultOptions()` function.

## Bound vs Unbound

`MaxBufferSize` is the max size of the worker buffers before they will try to send items through the report function. The buffers will grow as large as necessary until they are successfully sent.

If `BufferHardLimit` is set to 0, the buffers are unbound and will grow indefinitely until the data is successfully sent off through the report function. If not set to 0, the minimum valid value is equal to `MaxBufferSize`. The default is `MaxBufferSize * 2`. If the buffer size reaches the value set for `BufferHardLimit`, then all items in the buffer will attempt to be sent through the report function, and be immediately dropped if the report function returns an error. If the data is critical, the report function should make sure to deal with any errors.

**NOTE:** The supplied report function needs to be concurrency safe.

## Example

```golang
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

	db, err := databuffer.New(
		databuffer.MaxBufferSize[int](1024),
		databuffer.BufferHardLimit[int](4096),
		databuffer.NumWorkers[int](4),
		databuffer.WorkerWait[int](5*time.Minute),
		databuffer.SetReporter(r.Report),
    databuffer.ChanBufferSize[int](16),
	)

	dBuf, err := databuffer.New(opts)
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	// start the workers
	dBuf.Start(ctx)

	go func() {
		for i := range 10000 {
			dBuf.WorkerChan() <- []int{i}
		}
		cancel()
	}()

	<-ctx.Done()
}
```
