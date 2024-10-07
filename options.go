package databuffer

import (
	"errors"
	"fmt"
	"log"
	"time"
)

const (
	defaultMaxBufferSize = 64
	defaultNumWorkers    = 2
	defaultWorkerWait    = time.Minute
)

type Options[T any] struct {
	WorkerWait    time.Duration
	MaxBufferSize int
	NumWorkers    int
	// This must be concurrency safe or panics will occur
	Reporter Reporter[T]
	Logger   Logger
}

type DefaultLogger[T any] struct{}

func (d DefaultLogger[T]) Debug(s string) {
	log.Printf("DEBUG %s", s)
}

func (d DefaultLogger[T]) Info(s string) {
	log.Printf("INFO %s", s)
}

func (d DefaultLogger[T]) Warn(s string) {
	log.Printf("WARN %s", s)
}

func (d DefaultLogger[T]) Error(s string) {
	log.Printf("ERROR %s", s)
}

type DefaultReporter[T any] struct{}

func (d DefaultReporter[T]) Report(_ []T) error { return nil }

func GetDefaultOptions[T any]() Options[T] {
	return Options[T]{
		MaxBufferSize: defaultMaxBufferSize,
		NumWorkers:    defaultNumWorkers,
		WorkerWait:    time.Minute,
		Reporter:      DefaultReporter[T]{},
		Logger:        DefaultLogger[T]{},
	}
}

func validateOptions[T any](opts Options[T]) (Options[T], error) {
	var err error

	if opts.Logger == nil {
		opts.Logger = &DefaultLogger[T]{}
		opts.Logger.Warn("databuffer logger option is nil; using default logger")
	}

	if opts.Reporter == nil {
		opts.Logger.Error("databuffer reporter option is nil")
		err = errors.New("databuffer reporter option is nil")
	}

	if opts.MaxBufferSize == 0 {
		opts.Logger.Warn(fmt.Sprintf("invalid data buffer size %d, setting to %d", opts.MaxBufferSize, defaultMaxBufferSize))
		opts.MaxBufferSize = defaultMaxBufferSize
	}

	if opts.NumWorkers < 1 {
		opts.Logger.Warn(fmt.Sprintf("invalid number of workers %d, setting to default of %d", opts.NumWorkers, defaultNumWorkers))
		opts.NumWorkers = defaultNumWorkers
	}

	return opts, err
}
