package databuffer

import (
	"context"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

type reporter[T any] func(context.Context, []T) error

type DataBuffer[T any] struct {
	numWorkers      int
	maxBufferSize   int
	bufferHardLimit int
	workerWait      time.Duration
	logger          zerolog.Logger
	in              chan []T
	startOnce       sync.Once
	reporter[T]
}

// Start the workers.
func (b *DataBuffer[T]) Start(ctx context.Context) {
	b.startOnce.Do(func() {
		b.logger.Info().Msgf("Starting %d %T databuffer workers", b.numWorkers, *new(T))
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

	logger := zerolog.Ctx(ctx)

	logger.Debug().Int("sent", len(buffer)).Msgf("%T databuffer worker sending items", *new(T))

	if err := b.reporter(ctx, buffer); err != nil {
		logger.Error().Err(err).Msgf("%T databuffer worker error sending data", *new(T))

		// return the original buffer if we couldn't send and we're less than the hard limit
		if len(buffer) <= b.bufferHardLimit || b.bufferHardLimit == 0 {
			return buffer
		}

		logger.Warn().
			Int("dropped", len(buffer)).
			Msgf("%T databuffer worker buffer hit hard limit; dropping data", *new(T))
	}

	// return a new empty buffer if we sent them off successfully
	return make([]T, 0, b.maxBufferSize)
}

func (b *DataBuffer[T]) worker(ctx context.Context, workerID int) {
	// Stagger worker startup so they don't all send at the same time.
	time.Sleep(time.Duration(workerID) * time.Second)

	buffer := make([]T, 0, b.maxBufferSize)
	ticker := time.Tick(b.workerWait)
	logger := b.logger.With().Ctx(ctx).Int("worker_id", workerID).Logger()
	ctx = logger.WithContext(ctx)

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
			logger.Debug().Msgf("%T databuffer worker wait ticker fired", *new(T))
			buffer = b.report(ctx, buffer)
		case <-ctx.Done():
			if workerID == 0 {
				close(b.in)
				break workerLoop
			}
		}
	}

	logger.Debug().
		Msgf("%T databuffer worker sending any remaining data and shutting down", *new(T))
	b.report(ctx, buffer)
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
		logger:          log.Logger,
		reporter:        opts.Reporter,
	}, nil
}
