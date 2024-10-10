package databuffer_test

import (
	"context"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/scottso/databuffer"
	"github.com/stretchr/testify/require"
)

type RecorderReporter struct {
	results []string
	sync.RWMutex
}

func (r *RecorderReporter) Report(data []string) error {
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
	time.Sleep(1 * time.Second)

	require.Equal(t, numStrings, len(reporter.GetResults()))
}

func TestDataBufferSlices(t *testing.T) {
	const (
		numStrings     = 121
		numGenerations = 11
	)

	reporter := &RecorderReporter{}

	opts := databuffer.Options[string]{
		NumWorkers:    4,
		MaxBufferSize: 512,
		WorkerWait:    3 * time.Second,
		Reporter:      reporter,
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
	time.Sleep(1 * time.Second)

	require.Equal(t, numStrings*numGenerations, len(reporter.GetResults()))
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
