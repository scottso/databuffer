package databuffer

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"
)

var ErrNilReporter = errors.New("reporter cannot be nil")

type Reporter[T any] interface {
	Report(context.Context, []T) error
}

type DataBuffer[T any] struct {
	numWorkers      int
	maxBufferSize   int
	bufferHardLimit int
	chanBufferSize  int
	workerWait      time.Duration
	logger          *slog.Logger
	in              chan []T
	startOnce       sync.Once
	Reporter[T]
}

// Start the workers.
func (b *DataBuffer[T]) Start(ctx context.Context) {
	b.startOnce.Do(func() {
		b.logger.Info("Starting databuffer workers", slog.Int("num_workers", b.numWorkers))
		for i := range b.numWorkers {
			go b.worker(ctx, i)
		}
	})
}

func (b *DataBuffer[T]) WorkerChan() chan<- []T {
	return b.in
}

// Flush and empy the buffer.
func (b *DataBuffer[T]) report(ctx context.Context, buffer []T) []T {
	if len(buffer) == 0 {
		return buffer
	}

	b.logger.DebugContext(ctx, "databuffer worker sending items")

	if err := b.Report(ctx, buffer); err != nil {
		b.logger.ErrorContext(ctx, "databuffer worker error sending data")

		// return the original buffer if we couldn't send and we're less than the hard limit
		if len(buffer) <= b.bufferHardLimit || b.bufferHardLimit == 0 {
			return buffer
		}

		b.logger.WarnContext(ctx, "databuffer worker buffer hit hard limit; dropping data")
	}

	// return a new empty buffer if we sent them off successfully
	return make([]T, 0, b.maxBufferSize)
}

func (b *DataBuffer[T]) worker(ctx context.Context, workerID int) {
	// Stagger worker startup so they don't all send at the same time.
	time.Sleep(time.Duration(workerID) * time.Second)

	buffer := make([]T, 0, b.maxBufferSize)
	ticker := time.Tick(b.workerWait)

workerLoop:
	for {
		select {
		case data, ok := <-b.in:
			buffer = append(buffer, data...)
			if len(buffer) >= b.maxBufferSize {
				buffer = b.report(ctx, buffer)
			}
			if !ok {
				break workerLoop
			}
		case <-ticker:
			b.logger.DebugContext(ctx, "databuffer worker wait ticker fired")
			buffer = b.report(ctx, buffer)
		case <-ctx.Done():
			if workerID == 0 {
				close(b.in)
				break workerLoop
			}
		}
	}

	b.logger.DebugContext(ctx, "databuffer worker sending any remaining data and shutting down")
	b.report(ctx, buffer)
}

func (b *DataBuffer[T]) options(opts ...Option[T]) {
	for _, opt := range opts {
		opt(b)
	}
}

func New[T any](options ...Option[T]) (*DataBuffer[T], error) {
	d := &DataBuffer[T]{
		numWorkers:      defaultNumWorkers,
		maxBufferSize:   defaultMaxBufferSize,
		bufferHardLimit: defaultBufferHardLimit,
		workerWait:      defaultWorkerWait,
		chanBufferSize:  defaultChannelBufferSize,
		Reporter:        &DefaultReporter[T]{},
		logger:          NewLogger[T](),
	}

	d.options(options...)

	// Check if Reporter is nil
	if d.Reporter == nil {
		return nil, ErrNilReporter
	}

	d.in = make(chan []T, d.chanBufferSize)

	return d, nil
}
