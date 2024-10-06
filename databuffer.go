package databuffer

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type Logger interface {
	Debug(s string)
	Info(s string)
	Warn(s string)
	Error(s string)
}

type Reporter[T any] interface {
	Report([]T) error
}

type DataBuffer[T any] struct {
	numWorkers    int
	maxBufferSize int
	workerWait    time.Duration
	log           Logger
	in            chan T
	startOnce     sync.Once
	Reporter[T]
}

// Start the workers.
func (b *DataBuffer[T]) Start(ctx context.Context) {
	b.startOnce.Do(func() {
		b.log.Info(fmt.Sprintf("Starting %d buffer workers", b.numWorkers))
		for i := range b.numWorkers {
			go b.worker(ctx, i)
		}
	})
}

func (b *DataBuffer[T]) WorkerChan() chan<- T {
	return b.in
}

// Flush and empy the buffer.
func (b *DataBuffer[T]) report(workerID int, buffer []T) []T {
	b.log.Debug(fmt.Sprintf("worker %d sending %d items", workerID, len(buffer)))

	if err := b.Report(buffer); err != nil {
		err = fmt.Errorf("worker %d error sending data: %w", workerID, err)
		b.log.Error(err.Error())

		// return the original buffer if we couldn't send
		return buffer
	}

	// return a new empty buffer if we sent them off successfully
	return make([]T, 0, b.maxBufferSize)
}

func (b *DataBuffer[T]) worker(ctx context.Context, workerID int) {
	buffer := make([]T, 0, b.maxBufferSize)
	ticker := time.Tick(b.workerWait)

workerLoop:
	for {
		select {
		case data, ok := <-b.in:
			buffer = append(buffer, data)
			if len(buffer) >= b.maxBufferSize {
				buffer = b.report(workerID, buffer)
			}
			if !ok {
				break workerLoop
			}
		case <-ticker:
			buffer = b.report(workerID, buffer)
		case <-ctx.Done():
			break workerLoop
		}
	}

	b.log.Debug(fmt.Sprintf("worker %d sending any remaining data and shutting down", workerID))
	b.report(workerID, buffer)
}

func New[T any](options ...Options[T]) (*DataBuffer[T], error) {
	opts := GetDefaultOptions[T]()
	if len(options) > 0 {
		opts = options[0]
	}

	opts, err := validateOptions(opts)
	if err != nil {
		return nil, err
	}

	ch := make(chan T)

	return &DataBuffer[T]{
		numWorkers:    opts.NumWorkers,
		maxBufferSize: opts.MaxBufferSize,
		workerWait:    opts.WorkerWait,
		in:            ch,
		log:           opts.Logger,
		Reporter:      opts.Reporter,
	}, nil
}
