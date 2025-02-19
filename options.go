package databuffer

import (
	"context"
	"errors"
	"log/slog"
	"time"
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
	// Logger
	Logger *slog.Logger
}

type DefaultReporter[T any] struct{}

func (d *DefaultReporter[T]) Report(context.Context, []T) error { return nil }

func GetDefaultOptions[T any]() Options[T] {
	return Options[T]{
		MaxBufferSize:   defaultMaxBufferSize,
		BufferHardLimit: defaultBufferHardLimit,
		NumWorkers:      defaultNumWorkers,
		WorkerWait:      defaultWorkerWait,
		Reporter:        &DefaultReporter[T]{},
		Logger:          NewLogger[T](),
	}
}

func ValidateOptions[T any](opts Options[T]) (Options[T], error) {
	if opts.Logger == nil {
		opts.Logger = NewLogger[T]()
	}

	if opts.Reporter == nil {
		err := errors.New("databuffer reporter method is nil")
		opts.Logger.Error("error validating options", slog.Any("error", err))
		return opts, err
	}

	if opts.WorkerWait <= 0 {
		opts.Logger.Warn(
			"invalid databuffer worker wait time; setting to default",
			slog.Duration("wanted", opts.WorkerWait),
			slog.String("used", defaultWorkerWait.String()),
		)
		opts.WorkerWait = defaultWorkerWait
	}

	if opts.MaxBufferSize <= 0 {
		opts.Logger.Warn(
			"invalid data buffer size; setting to default",
			slog.Int("wanted", opts.MaxBufferSize),
			slog.Int("used", defaultMaxBufferSize),
		)
		opts.MaxBufferSize = defaultMaxBufferSize
	}

	if opts.BufferHardLimit < 0 ||
		opts.BufferHardLimit > 0 && opts.BufferHardLimit < opts.MaxBufferSize {
		opts.Logger.Warn(
			"buffer hard limit is less than max buffer size; setting to default",
			slog.Int("used", defaultBufferHardLimit),
		)
		opts.BufferHardLimit = defaultBufferHardLimit
	}

	if opts.NumWorkers < 1 {
		opts.Logger.Warn(
			"invalid number of workers; setting to default",
			slog.Int("wanted", opts.NumWorkers),
			slog.Int("used", defaultNumWorkers),
		)
		opts.NumWorkers = defaultNumWorkers
	}

	return opts, nil
}
