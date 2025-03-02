package databuffer

import (
	"context"
	"log/slog"
	"time"
)

const (
	defaultMaxBufferSize   = 64
	defaultBufferHardLimit = defaultMaxBufferSize * 2
	defaultNumWorkers      = 2
	defaultWorkerWait      = time.Minute
)

type option[T any] func(*DataBuffer[T])

func WorkerWait[T any](duration time.Duration) option[T] {
	return func(d *DataBuffer[T]) {
		if duration <= 0 {
			d.workerWait = defaultWorkerWait
			return
		}

		d.workerWait = duration
	}
}

func BufferHardLimit[T any](limit int) option[T] {
	return func(d *DataBuffer[T]) {
		if limit < 0 || limit > 0 && limit < d.maxBufferSize {
			d.bufferHardLimit = d.maxBufferSize
			return
		}
		d.bufferHardLimit = limit
	}
}

func MaxBufferSize[T any](size int) option[T] {
	return func(d *DataBuffer[T]) {
		if size <= 0 {
			d.maxBufferSize = defaultMaxBufferSize
			return
		}
		d.maxBufferSize = size
	}
}

func NumWorkers[T any](num int) option[T] {
	return func(d *DataBuffer[T]) {
		if num < 1 {
			d.numWorkers = defaultNumWorkers
			return
		}
		d.numWorkers = num
	}
}

func ChanBufferSize[T any](size int) option[T] {
	return func(d *DataBuffer[T]) {
		d.chanBufferSize = size
	}
}

func SetReporter[T any](r Reporter[T]) option[T] {
	return func(d *DataBuffer[T]) {
		d.Reporter = r
	}
}

func Logger[T any](l *slog.Logger) option[T] {
	return func(d *DataBuffer[T]) {
		d.logger = l
	}
}

type DefaultReporter[T any] struct{}

func (d *DefaultReporter[T]) Report(context.Context, []T) error { return nil }
