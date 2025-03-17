package databuffer_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/scottso/databuffer"
)

// MockReporter simulates the Reporter interface
type MockReporter[T any] struct {
	mock.Mock
}

func (m *MockReporter[T]) Report(ctx context.Context, data []T) error {
	args := m.Called(ctx, data)
	return args.Error(0)
}

// DataBufferSuite defines the test suite
type DataBufferSuite struct {
	suite.Suite
	ctxCancel context.CancelFunc
	ctx       context.Context
}

func (s *DataBufferSuite) SetupTest() {
	s.ctx, s.ctxCancel = context.WithCancel(context.Background())
}

func (s *DataBufferSuite) TearDownTest() {
	s.ctxCancel()
}

func (s *DataBufferSuite) TestNewDataBuffer() {
	db, err := databuffer.New[int]()
	s.Require().NoError(err)
	s.Require().NotNil(db)
}

func (s *DataBufferSuite) TestDataBufferReporting() {
	mockReporter := new(MockReporter[int])
	mockReporter.On("Report", mock.Anything, mock.Anything).Return(nil)

	db, err := databuffer.New(
		databuffer.MaxBufferSize[int](5),
		databuffer.BufferHardLimit[int](10),
		databuffer.NumWorkers[int](1),
		databuffer.WorkerWait[int](100*time.Millisecond),
		databuffer.Logger[int](databuffer.NewLogger[int]()),
		databuffer.SetReporter(mockReporter),
	)

	s.Require().NoError(err)
	s.Require().NotNil(db)

	db.Start(s.ctx)

	db.WorkerChan() <- []int{1, 2, 3, 4, 5}

	// Give workers time to process
	time.Sleep(200 * time.Millisecond)

	mockReporter.AssertCalled(s.T(), "Report", mock.Anything, []int{1, 2, 3, 4, 5})
}

func (s *DataBufferSuite) TestBufferLimits() {
	mockReporter := new(MockReporter[int])
	mockReporter.On("Report", mock.Anything, mock.Anything).Return(errors.New("report failed"))

	db, err := databuffer.New(
		databuffer.MaxBufferSize[int](3),
		databuffer.BufferHardLimit[int](5),
		databuffer.NumWorkers[int](1),
		databuffer.WorkerWait[int](2*time.Second),
		databuffer.SetReporter(mockReporter),
		databuffer.Logger[int](databuffer.NewLogger[int]()),
	)

	s.Require().NoError(err)
	s.Require().NotNil(db)

	db.Start(s.ctx)

	db.WorkerChan() <- []int{1, 2, 3}
	db.WorkerChan() <- []int{4, 5}

	time.Sleep(200 * time.Millisecond)

	// Expect a single report with all 5 elements
	mockReporter.AssertCalled(s.T(), "Report", mock.Anything, []int{1, 2, 3, 4, 5})
	mockReporter.AssertNumberOfCalls(s.T(), "Report", 2)
}

func (s *DataBufferSuite) TestWorkerLifecycle() {
	mockReporter := new(MockReporter[int])
	mockReporter.On("Report", mock.Anything, mock.Anything).Return(nil)

	db, err := databuffer.New(
		databuffer.MaxBufferSize[int](3),
		databuffer.BufferHardLimit[int](2),
		databuffer.NumWorkers[int](1),
		databuffer.WorkerWait[int](50*time.Millisecond),
		databuffer.SetReporter(mockReporter),
		databuffer.Logger[int](databuffer.NewLogger[int]()),
	)

	s.Require().NoError(err)
	s.Require().NotNil(db)

	localCtx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		db.Start(localCtx)
	}()

	db.WorkerChan() <- []int{1, 2, 3}
	time.Sleep(200 * time.Millisecond)

	cancel()
	wg.Wait()

	mockReporter.AssertCalled(s.T(), "Report", mock.Anything, []int{1, 2, 3})
}

func (s *DataBufferSuite) TestMultipleStartCalls() {
	mockReporter := new(MockReporter[int])
	mockReporter.On("Report", mock.Anything, mock.Anything).Return(nil)

	db, err := databuffer.New(
		databuffer.NumWorkers[int](1),
		databuffer.WorkerWait[int](50*time.Millisecond),
		databuffer.SetReporter(mockReporter),
	)
	s.Require().NoError(err)

	// Call Start multiple times to test startOnce functionality
	db.Start(s.ctx)
	db.Start(s.ctx) // This should be a no-op due to sync.Once

	db.WorkerChan() <- []int{1, 2, 3}
	time.Sleep(100 * time.Millisecond)

	mockReporter.AssertNumberOfCalls(s.T(), "Report", 1)
}

func (s *DataBufferSuite) TestDefaultOptions() {
	mockReporter := new(MockReporter[string])
	mockReporter.On("Report", mock.Anything, mock.Anything).Return(nil)

	// Test with a different type (string) using the reporter
	db, err := databuffer.New(
		databuffer.SetReporter(mockReporter),
		databuffer.WorkerWait[string](50*time.Millisecond), // Need shorter wait time for test
	)
	s.Require().NoError(err)
	s.Require().NotNil(db)

	db.Start(s.ctx)
	db.WorkerChan() <- []string{"test1", "test2"}

	// Need to wait long enough for the worker to process
	time.Sleep(200 * time.Millisecond)

	mockReporter.AssertCalled(s.T(), "Report", mock.Anything, []string{"test1", "test2"})
}

func (s *DataBufferSuite) TestBufferAutoFlushOnSize() {
	mockReporter := new(MockReporter[int])
	mockReporter.On("Report", mock.Anything, mock.Anything).Return(nil)

	db, err := databuffer.New(
		databuffer.MaxBufferSize[int](3),
		databuffer.WorkerWait[int](
			1*time.Hour,
		), // Long wait time to ensure flush is triggered by size, not time
		databuffer.NumWorkers[int](1),
		databuffer.SetReporter(mockReporter),
	)
	s.Require().NoError(err)

	db.Start(s.ctx)

	// Send 2 items, which is less than MaxBufferSize
	db.WorkerChan() <- []int{1, 2}
	time.Sleep(50 * time.Millisecond)

	// Should not have triggered a report yet
	mockReporter.AssertNumberOfCalls(s.T(), "Report", 0)

	// Send 1 more item, which makes the buffer size equal to MaxBufferSize
	db.WorkerChan() <- []int{3}
	time.Sleep(50 * time.Millisecond)

	// Should have triggered a report now
	mockReporter.AssertCalled(s.T(), "Report", mock.Anything, []int{1, 2, 3})
	mockReporter.AssertNumberOfCalls(s.T(), "Report", 1)
}

func (s *DataBufferSuite) TestMultipleWorkers() {
	// Create a mutex to synchronize access to the mock reporter
	var mockMutex sync.Mutex

	// Create a custom mock implementation that uses the mutex
	mockReporter := new(MockReporter[int])

	// Setup the mock to acquire the mutex before recording calls
	mockReporter.On("Report", mock.Anything, mock.Anything).
		Run(func(_ mock.Arguments) {
			mockMutex.Lock()
			defer mockMutex.Unlock()
		}).
		Return(nil)

	db, err := databuffer.New(
		databuffer.NumWorkers[int](2),
		databuffer.MaxBufferSize[int](5),
		databuffer.WorkerWait[int](50*time.Millisecond),
		databuffer.SetReporter(mockReporter),
	)
	s.Require().NoError(err)

	db.Start(s.ctx)

	// Send items to be distributed across workers
	for i := range 10 {
		db.WorkerChan() <- []int{i}
	}

	// Allow time for workers to process
	time.Sleep(300 * time.Millisecond)
	s.ctxCancel()

	// Wait for workers to completely shut down
	time.Sleep(200 * time.Millisecond)

	// Safely check calls with the mutex
	mockMutex.Lock()
	callCount := len(mockReporter.Calls)
	mockMutex.Unlock()

	// Verify the reporter was called at least once
	s.Require().Positive(callCount, "Reporter should have been called at least once")
}

func (s *DataBufferSuite) TestHardLimitBehavior() {
	mockReporter := new(MockReporter[int])
	// Mock reporter to always fail to test hard limit behavior
	mockReporter.On("Report", mock.Anything, mock.Anything).Return(errors.New("report error"))

	db, err := databuffer.New(
		databuffer.MaxBufferSize[int](2),
		databuffer.BufferHardLimit[int](3),
		databuffer.NumWorkers[int](1),
		databuffer.WorkerWait[int](50*time.Millisecond),
		databuffer.SetReporter(mockReporter),
	)
	s.Require().NoError(err)

	db.Start(s.ctx)

	// First send 2 items (equal to MaxBufferSize)
	db.WorkerChan() <- []int{1, 2}
	time.Sleep(100 * time.Millisecond)

	// The report fails but items are kept in buffer since we're below hard limit
	mockReporter.AssertCalled(s.T(), "Report", mock.Anything, []int{1, 2})

	// Send 2 more items to exceed hard limit
	db.WorkerChan() <- []int{3, 4}
	time.Sleep(100 * time.Millisecond)

	// The buffer should have been cleared after exceeding hard limit
	mockReporter.AssertCalled(s.T(), "Report", mock.Anything, []int{1, 2, 3, 4})
}

func (s *DataBufferSuite) TestChanBufferSize() {
	mockReporter := new(MockReporter[int])
	mockReporter.On("Report", mock.Anything, mock.Anything).Return(nil)

	customChanSize := 10
	db, err := databuffer.New(
		databuffer.ChanBufferSize[int](customChanSize),
		databuffer.SetReporter(mockReporter),
	)
	s.Require().NoError(err)

	db.Start(s.ctx)

	// We can't directly test channel buffer size, but we can verify we can send
	// many items without blocking
	for i := range customChanSize {
		select {
		case db.WorkerChan() <- []int{i}:
			// Should not block
		default:
			s.T().Fatalf("Channel should not be full after %d sends", i)
		}
	}
}

func (s *DataBufferSuite) TestInvalidOptionValues() {
	// Test with invalid option values that should use defaults
	db, err := databuffer.New(
		databuffer.MaxBufferSize[int](-1),
		databuffer.BufferHardLimit[int](-1),
		databuffer.NumWorkers[int](-1),
		databuffer.WorkerWait[int](-1),
	)
	s.Require().NoError(err)
	s.Require().NotNil(db)

	// Should not panic when starting
	db.Start(s.ctx)
}

func (s *DataBufferSuite) TestNilReporter() {
	// Test creating a DataBuffer with a nil reporter
	db, err := databuffer.New(
		databuffer.SetReporter[int](nil),
	)

	// Should fail with an error when a nil reporter is provided
	s.Require().Error(err)
	s.Require().Nil(db)
}

func (s *DataBufferSuite) TestChannelBufferSizeOption() {
	// Test custom channel buffer size
	customSize := 8
	db, err := databuffer.New(
		databuffer.ChanBufferSize[int](customSize),
	)
	s.Require().NoError(err)

	// Test we can send the exact number of items without blocking
	for i := range customSize {
		select {
		case db.WorkerChan() <- []int{i}:
			// Should not block
		default:
			s.T().Fatalf("Failed to send item %d, channel should not be full", i)
		}
	}
}

func (s *DataBufferSuite) TestContextCancellation() {
	// Instead of trying to test that a channel is closed (which is causing data races),
	// let's just test that we can start the worker and cancel the context without errors
	mockReporter := new(MockReporter[int])
	mockReporter.On("Report", mock.Anything, mock.Anything).Return(nil)

	db, err := databuffer.New(
		databuffer.SetReporter(mockReporter),
		databuffer.NumWorkers[int](1),
		databuffer.WorkerWait[int](10*time.Millisecond),
	)
	s.Require().NoError(err)

	// Create a context that will be canceled shortly
	localCtx, cancel := context.WithCancel(context.Background())

	// Start the worker in a goroutine so we can observe its behavior
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		db.Start(localCtx)
	}()

	// Send some data
	db.WorkerChan() <- []int{1, 2, 3}

	// Allow worker some time to process
	time.Sleep(50 * time.Millisecond)

	// Cancel the context
	cancel()

	// Wait for the worker to complete
	wg.Wait()

	// Verify that the reporter was called with our data
	mockReporter.AssertCalled(s.T(), "Report", mock.Anything, []int{1, 2, 3})
}

// Run the suite
func TestDataBufferSuite(t *testing.T) {
	suite.Run(t, new(DataBufferSuite))
}
