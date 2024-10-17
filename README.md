# databuffer
This is a simple data buffer that runs in background worker goroutines, one per worker. Each worker has it's own buffer that starts at `Options.MaxBufferSize`. It takes a slice of any type as input from a channel and each worker batches the data up in for processing by a user provided report function.  The workers will send/report their buffer data at a configurable time limit, or when the buffer is full, whichever happens first. Behavior is customizable with an options struct.  The default options are fully functional.

You can retrieve the default values with the `GetDefaultOptions()` function.

## Bound vs Unbound
`Options.MaxBufferSize` is the max size of the worker buffers before they will try to send items through the report function.  The buffers will grow as large as necessary until they are successfully sent.

If `Options.BufferHardLimit` is set to 0, the buffers are unbound and will grow indefinitely until the data is successfully sent off through the report function. If not set to 0, the minimum valid value is equal to `Options.MaxBufferSize`. The default is `Options.MaxBufferSize * 2`. If the buffer size reaches the value set for `Options.BufferHardLimit`, then all items in the buffer will attempt to be sent through the report function, and be immediately dropped if the report function returns an error. If the data is critical, the report function should make sure to deal with any errors.

**NOTE:** The supplied report function needs to be concurrency safe.

## Example
```golang
type reporter struct {}

func (r reporter) Report(data []int) error {
	...Do stuff with data...
}

func main() {
	opts := databuffer.GetDefaultOptions[int]()
	opts.NumWorkers = 4
	opts.WorkerWait = 5 * time.Minute
	opts.MaxBufferSize = 1024
	opts.BufferHardLimit = 4096
	opts.Reporter =  reporter{}

  // Use a buffered channel
  opts.ChanBufferSize = 16

	dBuf, err := databuffer.New(opts)
	if err != nil {
		return err
	}

  ctx, cancel := context.WithCancel(context.Background())

  // start the workers
  dBuf.Start(ctx)

  go func() {
  	for i := range 10000 {
    	dBuf.WorkerChan() <- i
  	}
    cancel()
  }

  <-ctx.Done
}
```
