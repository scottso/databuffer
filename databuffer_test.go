package databuffer_test

import (
	"context"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/scottso/databuffer.go"
	"github.com/stretchr/testify/require"
)

type Reporter struct{}

func (r Reporter) Report(_ []string) error {
	// emulate some delay in sending data
	time.Sleep(time.Second)

	return nil
}

func TestDataBuffer(t *testing.T) {
	opts := databuffer.Options[string]{
		NumWorkers:    2,
		MaxBufferSize: 128,
		WorkerWait:    3 * time.Second,
		Reporter:      Reporter{},
	}

	ch := make(chan string)

	dbuf, err := databuffer.New(ch, opts)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()

	dbuf.Start(ctx)

	go func() {
		for _, s := range GenerateRandomStrings(1001, 8) {
			ch <- s
		}

		cancel()
	}()

	<-ctx.Done()
	time.Sleep(2 * time.Second)
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
