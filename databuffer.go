package databuffer

import (
	"context"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

type Reporter[T any] interface {
	// This method MUST be concurrent safe or panics will ensue
	Report([]T) error
}

type DataBuffer[T any] struct {
	numWorkers      int
	maxBufferSize   int
	bufferHardLimit int
	workerWait      time.Duration
	logger          zerolog.Logger
	in              chan []T
	startOnce       sync.Once
	Reporter[T]
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
func (b *DataBuffer[T]) report(logger zerolog.Logger, buffer []T) []T {
	if len(buffer) == 0 {
		return buffer
	}

	logger.Debug().Int("sent", len(buffer)).Msgf("%T databuffer worker sending items", *new(T))

	if err := b.Report(buffer); err != nil {
		logger.Error().Err(err).Msgf("%T databuffer worker error sending data", *new(T))

		// return the original buffer if we couldn't send and we're less than the hard limit
		if len(buffer) <= b.bufferHardLimit || b.bufferHardLimit == 0 {
			return buffer
		}

		logger.Warn().Int("dropped", len(buffer)).Msgf("%T databuffer worker buffer hit hard limit; dropping data", *new(T))
	}

	// return a new empty buffer if we sent them off successfully
	return make([]T, 0, b.maxBufferSize)
}

func (b *DataBuffer[T]) worker(ctx context.Context, workerID int) {
	buffer := make([]T, 0, b.maxBufferSize)
	ticker := time.Tick(b.workerWait)
	logger := b.logger.With().Int("worker_id", workerID).Logger()

workerLoop:
	for {
		select {
		case data, ok := <-b.in:
			buffer = append(buffer, data...)
			if len(buffer) >= b.maxBufferSize {
				buffer = b.report(logger, buffer)
			}
			if !ok {
				break workerLoop
			}
		case <-ticker:
			logger.Debug().Msgf("%T databuffer worker wait ticker fired", *new(T))
			buffer = b.report(logger, buffer)
		case <-ctx.Done():
			if workerID == 0 {
				close(b.in)
				break workerLoop
			}
		}
	}

	logger.Debug().Msgf("%T databuffer worker sending any remaining data and shutting down", *new(T))
	b.report(logger, buffer)
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
		Reporter:        opts.Reporter,
	}, nil
}
