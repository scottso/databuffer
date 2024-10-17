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
	// This method MUST be concurrent safe or panics will ensue
	Report([]T) error
}

type DataBuffer[T any] struct {
	numWorkers      int
	maxBufferSize   int
	bufferHardLimit int
	workerWait      time.Duration
	log             Logger
	in              chan []T
	startOnce       sync.Once
	Reporter[T]
}

// Start the workers.
func (b *DataBuffer[T]) Start(ctx context.Context) {
	b.startOnce.Do(func() {
		b.log.Info(fmt.Sprintf("Starting %d %T databuffer workers", b.numWorkers, *new(T)))
		for i := range b.numWorkers {
			go b.worker(ctx, i)
		}
	})
}

func (b *DataBuffer[T]) WorkerChan() chan<- []T {
	return b.in
}

// Flush and empy the buffer.
func (b *DataBuffer[T]) report(workerID int, buffer []T) []T {
	if len(buffer) == 0 {
		return buffer
	}

	b.log.Debug(fmt.Sprintf("%T databuffer worker %d sending %d items", *new(T), workerID, len(buffer)))

	if err := b.Report(buffer); err != nil {
		err = fmt.Errorf("%T databuffer worker %d error sending data: %w", *new(T), workerID, err)
		b.log.Error(err.Error())

		// return the original buffer if we couldn't send and we're less than the hard limit
		if len(buffer) <= b.bufferHardLimit || b.bufferHardLimit == 0 {
			return buffer
		}

		b.log.Warn(fmt.Sprintf("%T databuffer worker %d buffer hit hard limit; dropping %d items", *new(T), workerID, len(buffer)))
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
			buffer = append(buffer, data...)
			if len(buffer) >= b.maxBufferSize {
				buffer = b.report(workerID, buffer)
			}
			if !ok {
				break workerLoop
			}
		case <-ticker:
			b.log.Debug(fmt.Sprintf("%T databuffer worker %d wait ticker fired", *new(T), workerID))
			buffer = b.report(workerID, buffer)
		case <-ctx.Done():
			if workerID == 0 {
				close(b.in)
				break workerLoop
			}
		}
	}

	b.log.Debug(fmt.Sprintf("%T databuffer worker %d sending any remaining data and shutting down", *new(T), workerID))
	b.report(workerID, buffer)
}

func New[T any](options ...Options[T]) (*DataBuffer[T], error) {
	opts := GetDefaultOptions[T]()
	if len(options) > 0 {
		opts = options[0]
	}

	opts, err := ValidateOptions(opts)
	if err != nil {
		return nil, err
	}

	ch := make(chan []T, opts.ChanBufferSize)

	return &DataBuffer[T]{
		numWorkers:      opts.NumWorkers,
		maxBufferSize:   opts.MaxBufferSize,
		bufferHardLimit: opts.BufferHardLimit,
		workerWait:      opts.WorkerWait,
		in:              ch,
		log:             opts.Logger,
		Reporter:        opts.Reporter,
	}, nil
}
