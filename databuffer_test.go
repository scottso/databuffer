package databuffer_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/scottso/databuffer"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// MockReporter simulates the Reporter interface
type MockReporter[T any] struct {
	mock.Mock
}

func (m *MockReporter[T]) Report(ctx context.Context, data []T) error {
	args := m.Called(ctx, data)
	return args.Error(0)
}

func TestNewDataBuffer(t *testing.T) {
	db, err := databuffer.New[int]()
	require.NoError(t, err)
	assert.NotNil(t, db)
}

func TestDataBufferReporting(t *testing.T) {
	mockReporter := new(MockReporter[int])
	mockReporter.On("Report", mock.Anything, mock.Anything).Return(nil)

	db, err := databuffer.New(databuffer.Options[int]{
		MaxBufferSize:   5,
		BufferHardLimit: 10,
		NumWorkers:      1,
		WorkerWait:      100 * time.Millisecond,
		Reporter:        mockReporter,
		Logger:          databuffer.NewLogger[int](),
	})

	require.NoError(t, err)
	assert.NotNil(t, db)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db.Start(ctx)

	db.WorkerChan() <- []int{1, 2, 3, 4, 5}

	// Give workers time to process
	time.Sleep(200 * time.Millisecond)

	mockReporter.AssertCalled(t, "Report", mock.Anything, []int{1, 2, 3, 4, 5})
}

func TestBufferLimits(t *testing.T) {
	mockReporter := new(MockReporter[int])
	mockReporter.On("Report", mock.Anything, mock.Anything).Return(errors.New("report failed"))

	db, err := databuffer.New(databuffer.Options[int]{
		MaxBufferSize:   3,
		BufferHardLimit: 5,
		NumWorkers:      1,
		WorkerWait:      2 * time.Second,
		Reporter:        mockReporter,
		Logger:          databuffer.NewLogger[int](),
	})

	require.NoError(t, err)
	assert.NotNil(t, db)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db.Start(ctx)

	db.WorkerChan() <- []int{1, 2, 3}
	db.WorkerChan() <- []int{4, 5}

	time.Sleep(200 * time.Millisecond)

	// Expect a single report with all 5 elements
	mockReporter.AssertCalled(t, "Report", mock.Anything, []int{1, 2, 3, 4, 5})
	mockReporter.AssertNumberOfCalls(t, "Report", 2)
}

func TestWorkerLifecycle(t *testing.T) {
	mockReporter := new(MockReporter[int])
	mockReporter.On("Report", mock.Anything, mock.Anything).Return(nil)

	db, err := databuffer.New(databuffer.Options[int]{
		MaxBufferSize: 3,
		NumWorkers:    2,
		WorkerWait:    50 * time.Millisecond,
		Reporter:      mockReporter,
		Logger:        databuffer.NewLogger[int](),
	})

	require.NoError(t, err)
	assert.NotNil(t, db)

	ctx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		db.Start(ctx)
	}()

	db.WorkerChan() <- []int{1, 2, 3}
	time.Sleep(200 * time.Millisecond)

	cancel()
	wg.Wait()

	mockReporter.AssertCalled(t, "Report", mock.Anything, []int{1, 2, 3})
}

func TestInvalidOptions(t *testing.T) {
	opts := databuffer.Options[int]{
		MaxBufferSize: -1,
		WorkerWait:    -1,
		NumWorkers:    0,
		Reporter:      nil,
		Logger:        nil,
	}

	_, err := databuffer.ValidateOptions(opts)
	assert.Error(t, err)
}
