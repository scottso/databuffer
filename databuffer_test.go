package databuffer_test

import (
	"context"
	"math/rand/v2"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/scottso/databuffer"
	"github.com/stretchr/testify/require"
)

type RecorderReporter struct {
	results []string
	sync.RWMutex
}

func (r *RecorderReporter) Report(_ context.Context, data []string) error {
	func() {
		r.Lock()
		defer r.Unlock()
		r.results = append(r.results, data...)
	}()

	// emulate some network latency in sending data
	n := rand.Int64N(1000)
	time.Sleep(time.Duration(n) * time.Millisecond)

	return nil
}

func (r *RecorderReporter) GetResults() []string {
	r.RLock()
	defer r.RUnlock()
	return r.results
}

func init() {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.TimeOnly}).
		With().
		Caller().
		Logger()
}

func TestOptionsValidations(t *testing.T) {
	opts := databuffer.GetDefaultOptions[string]()
	opts.BufferHardLimit = -1
	opts, err := databuffer.ValidateOptions(opts)
	require.NoError(t, err)
	require.Equal(t, databuffer.GetDefaultOptions[string]().BufferHardLimit, opts.BufferHardLimit)

	// Make sure MaxBufferSize is set to a default sane value if the given one is nonsensical
	opts = databuffer.GetDefaultOptions[string]()
	opts.MaxBufferSize = 0
	opts, err = databuffer.ValidateOptions(opts)
	require.NoError(t, err)
	require.Equal(t, databuffer.GetDefaultOptions[string]().MaxBufferSize, opts.MaxBufferSize)

	// Make sure the number of workers is set to a default sane value if the given one is nonsensical
	opts = databuffer.GetDefaultOptions[string]()
	opts.NumWorkers = 0
	opts, err = databuffer.ValidateOptions(opts)
	require.NoError(t, err)
	require.Equal(t, databuffer.GetDefaultOptions[string]().NumWorkers, opts.NumWorkers)

	// Make sure we error if the Reporter function is not assigned
	opts = databuffer.GetDefaultOptions[string]()
	opts.Reporter = nil
	_, err = databuffer.ValidateOptions(opts)
	require.Error(t, err)

	// Make sure worker wait time before flushing is set to a default sane value if the given one is nonsensical
	opts = databuffer.GetDefaultOptions[string]()
	opts.WorkerWait = -1
	opts, err = databuffer.ValidateOptions(opts)
	require.NoError(t, err)
	require.Equal(t, databuffer.GetDefaultOptions[string]().WorkerWait, opts.WorkerWait)
}

func TestDataBuffer(t *testing.T) {
	const numStrings = 2001

	reporter := &RecorderReporter{}

	opts := databuffer.Options[string]{
		NumWorkers:    6,
		MaxBufferSize: 128,
		WorkerWait:    3 * time.Second,
		Reporter:      reporter,
	}

	dbuf, err := databuffer.New(opts)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dbuf.Start(ctx)

	go func() {
		for _, s := range GenerateRandomStrings(numStrings, 8) {
			dbuf.WorkerChan() <- []string{s}
			n := rand.Int64N(5)
			time.Sleep(time.Duration(n) * time.Millisecond)
		}
		cancel()
	}()

	<-ctx.Done()
	// Wait for workers to shut down
	time.Sleep(5 * time.Second)

	require.Len(t, reporter.GetResults(), numStrings)
}

func TestDataBufferSlices(t *testing.T) {
	const (
		numStrings     = 121
		numGenerations = 11
	)

	reporter := &RecorderReporter{}

	opts := databuffer.Options[string]{
		NumWorkers:     4,
		MaxBufferSize:  512,
		ChanBufferSize: 512 / 4,
		WorkerWait:     3 * time.Second,
		Reporter:       reporter,
	}

	dbuf, err := databuffer.New(opts)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dbuf.Start(ctx)

	go func() {
		for range numGenerations {
			dbuf.WorkerChan() <- GenerateRandomStrings(numStrings, 8)
			n := rand.Int64N(5)
			time.Sleep(time.Duration(n) * time.Millisecond)
		}
		cancel()
	}()

	<-ctx.Done()
	// Wait for workers to shut down
	time.Sleep(5 * time.Second)

	require.Len(t, reporter.GetResults(), numStrings*numGenerations)
}

func GenerateRandomStrings(num int, length int) []string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	seededRand := rand.New(rand.NewPCG(1, 2))
	randomStrings := make([]string, num)

	for i := range num {
		b := make([]byte, length)
		for j := range b {
			b[j] = charset[seededRand.IntN(len(charset))]
		}
		randomStrings[i] = string(b)
	}

	return randomStrings
}
