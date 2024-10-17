package databuffer

import (
	"errors"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"
)

const (
	defaultMaxBufferSize   = 64
	defaultBufferHardLimit = defaultMaxBufferSize * 2
	defaultNumWorkers      = 2
	defaultWorkerWait      = time.Minute
)

type Options[T any] struct {
	// The duration to wait before we force send the buffer no matter how many items.
	WorkerWait time.Duration
	// This is not bound and is a best effort size.  Once the buffers are larger
	// than this it will make best effort to send off the data, but will not drop
	// any and the buffer will continue to grow.
	MaxBufferSize int
	// This is the hard limit of the buffer size.  If set to 0 the buffers will
	// be unbound and grow until a successful report.  For values > 0 any buffer
	// that grows beyond this will have its contents dropped if it cannot report
	// successfully. Default is 2 * MaxBufferSize
	BufferHardLimit int
	// Number of worker goroutines to process incoming data. Default 2.
	NumWorkers int
	// Size of the worker channel buffer.  Defaults to unbuffered channel.
	ChanBufferSize int
	// This must be concurrency safe or panics will occur
	Reporter Reporter[T]
	Logger   Logger
}

type DefaultLogger[T any] struct{}

func (d DefaultLogger[T]) Debug(s string) {
	log.Debug().Msgf("%s", s)
}

func (d DefaultLogger[T]) Info(s string) {
	log.Info().Msgf("INFO %s", s)
}

func (d DefaultLogger[T]) Warn(s string) {
	log.Warn().Msgf("WARN %s", s)
}

func (d DefaultLogger[T]) Error(s string) {
	log.Error().Msgf("ERROR %s", s)
}

type DefaultReporter[T any] struct{}

func (d DefaultReporter[T]) Report(_ []T) error { return nil }

func GetDefaultOptions[T any]() Options[T] {
	return Options[T]{
		MaxBufferSize:   defaultMaxBufferSize,
		BufferHardLimit: defaultBufferHardLimit,
		NumWorkers:      defaultNumWorkers,
		WorkerWait:      defaultWorkerWait,
		Reporter:        DefaultReporter[T]{},
		Logger:          DefaultLogger[T]{},
	}
}

func ValidateOptions[T any](opts Options[T]) (Options[T], error) {
	if opts.Logger == nil {
		opts.Logger = &DefaultLogger[T]{}
		opts.Logger.Warn("databuffer logger option is nil; using default logger")
	}

	if opts.Reporter == nil {
		opts.Logger.Error("databuffer reporter option is nil")
		return opts, errors.New("databuffer reporter option is nil")
	}

	if opts.WorkerWait <= 0 {
		opts.Logger.Warn(fmt.Sprintf("invalid databuffer worker wait time is %s; setting to %s", opts.WorkerWait, defaultWorkerWait))
		opts.WorkerWait = time.Minute
	}

	if opts.MaxBufferSize <= 0 {
		opts.Logger.Warn(fmt.Sprintf("invalid data buffer size %d, setting to %d", opts.MaxBufferSize, defaultMaxBufferSize))
		opts.MaxBufferSize = defaultMaxBufferSize
	}

	if opts.BufferHardLimit < 0 || opts.BufferHardLimit > 0 && opts.BufferHardLimit < opts.MaxBufferSize {
		opts.Logger.Warn(fmt.Sprintf("buffer hard limit is less than max buffer size; setting to %d", defaultBufferHardLimit))
		opts.BufferHardLimit = defaultBufferHardLimit
	}

	if opts.NumWorkers < 1 {
		opts.Logger.Warn(fmt.Sprintf("invalid number of workers %d, setting to default of %d", opts.NumWorkers, defaultNumWorkers))
		opts.NumWorkers = defaultNumWorkers
	}

	return opts, nil
}
