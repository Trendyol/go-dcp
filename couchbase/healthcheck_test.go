package couchbase

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Trendyol/go-dcp/config"
	"github.com/Trendyol/go-dcp/logger"
	"github.com/Trendyol/go-dcp/models"
	"github.com/Trendyol/go-dcp/wrapper"
	"github.com/couchbase/gocbcore/v10"
)

func init() {
	logger.InitDefaultLogger("info")
}

func TestHealthCheck_Start_Stop(t *testing.T) {
	// Arrange
	cfg := &config.HealthCheck{
		Interval: 100 * time.Millisecond,
	}

	pingCh := make(chan struct{}, 3)

	mc := &mockClient{}
	mc.PingFunc = func() (*models.PingResult, error) {
		mc.PingCallCount++

		// Non-blocking send to avoid deadlocks if channel is full
		select {
		case pingCh <- struct{}{}:
		default:
		}

		return &models.PingResult{}, nil
	}

	sut := NewHealthCheck(cfg, mc)

	// Act
	sut.Start()

	expectedPings := 3

	// Assert

	// Use a timeout to prevent the test from hanging indefinitely
	timeout := time.After(2 * time.Second)

	for i := 0; i < expectedPings; i++ {
		select {
		case <-pingCh:
			// Ping was called, continue to wait for the next one
		case <-timeout:
			// Timeout occurred before receiving all expected Ping calls
			sut.Stop()
			t.Fatalf("Timed out waiting for %d Ping calls, only received %d", expectedPings, i)
		}
	}

	sut.Stop()

	// Assert
	if mc.PingCallCount < expectedPings {
		t.Fatalf("Ping should have been called at least %d times, but it was called %d times", expectedPings, mc.PingCallCount)
	}
}

func TestHealthCheck_PingFailure(t *testing.T) {
	// Arrange
	cfg := &config.HealthCheck{
		Interval: 100 * time.Millisecond,
	}

	mc := &mockClient{}

	pingCh := make(chan struct{}, 3)

	// Define the behavior for Ping: fail the first two times, then succeed.
	mc.PingFunc = func() (*models.PingResult, error) {
		mc.PingCallCount++

		select {
		case pingCh <- struct{}{}:
		default:
		}

		if mc.PingCallCount <= 2 {
			return nil, errors.New("ping failed")
		}
		return &models.PingResult{}, nil
	}

	sut := NewHealthCheck(cfg, mc)

	// Act
	sut.Start()

	expectedPings := 3

	// Use a timeout to prevent the test from hanging indefinitely
	timeout := time.After(10 * time.Second)

	// Wait for the expected number of Ping calls
	for i := 0; i < expectedPings; i++ {
		select {
		case <-pingCh:
			// Ping was called, continue to wait for the next one
		case <-timeout:
			// Timeout occurred before receiving all expected Ping calls
			sut.Stop()
			t.Fatalf("Timed out waiting for %d Ping calls, only received %d", expectedPings, i)
		}
	}

	// Stop the health check after receiving the expected number of Ping calls
	sut.Stop()

	// Assert
	if mc.PingCallCount < expectedPings {
		t.Fatalf("Ping should have been called at least %d times, but it was called %d times", expectedPings, mc.PingCallCount)
	}
}

func TestHealthCheck_PanicFailure(t *testing.T) {
	// Arrange
	cfg := &config.HealthCheck{
		Interval: 100 * time.Millisecond,
	}

	mc := &mockClient{}

	pingCh := make(chan struct{}, 5)

	mc.PingFunc = func() (*models.PingResult, error) {
		mc.PingCallCount++
		pingCh <- struct{}{}
		return nil, errors.New("ping failed")
	}

	sut := healthCheck{
		config: cfg,
		client: mc,
	}

	// Act
	sut.wg.Add(1)
	go func() {
		defer func() {
			if r := recover(); r == nil {
				t.Error("test should be panic!")
			}
		}()

		sut.run(context.Background())
	}()

	// Assert
	expectedPings := 5
	timeout := time.After(10 * time.Second)

	for i := 0; i < expectedPings; i++ {
		select {
		case <-pingCh:
		case <-timeout:
			t.Fatalf("Timed out waiting for Ping call %d", i+1)
		}
	}

	if mc.PingCallCount < expectedPings {
		t.Fatalf("Ping should have been called at least %d times, but it was called %d times", expectedPings, mc.PingCallCount)
	}
}

type mockClient struct {
	PingFunc      func() (*models.PingResult, error)
	PingCallCount int
}

var _ Client = (*mockClient)(nil)

func (m *mockClient) Ping() (*models.PingResult, error) {
	if m.PingFunc != nil {
		return m.PingFunc()
	}

	m.PingCallCount++
	return &models.PingResult{}, nil
}

func (m *mockClient) GetAgent() *gocbcore.Agent {
	panic("implement me")
}

func (m *mockClient) GetMetaAgent() *gocbcore.Agent {
	panic("implement me")
}

func (m *mockClient) Connect() error {
	panic("implement me")
}

func (m *mockClient) Close() {
	panic("implement me")
}

func (m *mockClient) DcpConnect(useExpiryOpcode bool, useChangeStreams bool) error {
	panic("implement me")
}

func (m *mockClient) DcpClose() {
	panic("implement me")
}

func (m *mockClient) GetVBucketSeqNos(awareCollection bool) (*wrapper.ConcurrentSwissMap[uint16, uint64], error) {
	panic("implement me")
}

func (m *mockClient) GetNumVBuckets() int {
	panic("implement me")
}

func (m *mockClient) GetFailOverLogs(vbID uint16) ([]gocbcore.FailoverEntry, error) {
	panic("implement me")
}

func (m *mockClient) OpenStream(vbID uint16, collectionIDs map[uint32]string, offset *models.Offset, observer Observer) error {
	panic("implement me")
}

func (m *mockClient) CloseStream(vbID uint16) error {
	panic("implement me")
}

func (m *mockClient) GetCollectionIDs(scopeName string, collectionNames []string) map[uint32]string {
	panic("implement me")
}

func (m *mockClient) GetAgentConfigSnapshot() (*gocbcore.ConfigSnapshot, error) {
	panic("implement me")
}

func (m *mockClient) GetDcpAgentConfigSnapshot() (*gocbcore.ConfigSnapshot, error) {
	panic("implement me")
}

func (m *mockClient) GetAgentQueues() []*models.AgentQueue {
	panic("implement me")
}
