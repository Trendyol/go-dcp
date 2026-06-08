package api

import (
	"errors"
	"net/http/httptest"
	"testing"

	"github.com/Trendyol/go-dcp/couchbase"
	"github.com/Trendyol/go-dcp/models"
	"github.com/Trendyol/go-dcp/stream"
	"github.com/Trendyol/go-dcp/wrapper"

	"github.com/gofiber/fiber/v2"
)

// fakeClient only implements Ping; the embedded interface satisfies the rest.
type fakeClient struct {
	couchbase.Client
	pingErr error
}

func (c *fakeClient) Ping() (*models.PingResult, error) {
	return &models.PingResult{}, c.pingErr
}

// fakeStream only implements what status() reads; the embedded interface
// satisfies the rest.
type fakeStream struct {
	stream.Stream
	open          bool
	assignedVbIDs int
	activeStreams int32
}

func (s *fakeStream) IsOpen() bool { return s.open }

func (s *fakeStream) GetOffsets() (
	*wrapper.ConcurrentSwissMap[uint16, *models.Offset],
	*wrapper.ConcurrentSwissMap[uint16, bool],
	bool,
) {
	offsets := wrapper.CreateConcurrentSwissMap[uint16, *models.Offset](uint64(s.assignedVbIDs))
	for i := 0; i < s.assignedVbIDs; i++ {
		offsets.Store(uint16(i), &models.Offset{})
	}
	return offsets, nil, false
}

func (s *fakeStream) GetMetric() (*stream.Metric, int32) {
	return &stream.Metric{}, s.activeStreams
}

func TestStatus(t *testing.T) {
	cases := []struct {
		name          string
		pingErr       error
		open          bool
		assignedVbIDs int
		activeStreams int32
		wantStatus    int
	}{
		{
			name:          "ok when all assigned streams are active",
			open:          true,
			assignedVbIDs: 4,
			activeStreams: 4,
			wantStatus:    fiber.StatusOK,
		},
		{
			name:          "unavailable when some streams are abandoned",
			open:          true,
			assignedVbIDs: 4,
			activeStreams: 2,
			wantStatus:    fiber.StatusServiceUnavailable,
		},
		{
			// During rebalance the stream is closed (IsOpen()==false) for the
			// whole close->reopen window, so the active-stream check is skipped
			// and the in-flight teardown never reports a false negative.
			name:          "ok during rebalance even with fewer active streams",
			open:          false,
			assignedVbIDs: 4,
			activeStreams: 0,
			wantStatus:    fiber.StatusOK,
		},
		{
			name:          "error when couchbase ping fails",
			pingErr:       errors.New("ping failed"),
			open:          true,
			assignedVbIDs: 4,
			activeStreams: 4,
			wantStatus:    fiber.StatusInternalServerError,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a := &api{
				client: &fakeClient{pingErr: tc.pingErr},
				stream: &fakeStream{
					open:          tc.open,
					assignedVbIDs: tc.assignedVbIDs,
					activeStreams: tc.activeStreams,
				},
			}

			app := fiber.New()
			app.Get("/status", a.status)

			resp, err := app.Test(httptest.NewRequest(fiber.MethodGet, "/status", nil))
			if err != nil {
				t.Fatalf("request failed: %v", err)
			}

			if resp.StatusCode != tc.wantStatus {
				t.Fatalf("got status %d, want %d", resp.StatusCode, tc.wantStatus)
			}
		})
	}
}
