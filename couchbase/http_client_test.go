package couchbase

import (
	"testing"

	"github.com/Trendyol/go-dcp/config"
)

func TestNewHTTPClient_MaxIdemponentCallAttempts(t *testing.T) {
	t.Run("configured value is copied", func(t *testing.T) {
		cfg := &config.Dcp{MaxIdemponentCallAttempts: 3}

		hc, ok := NewHTTPClient(cfg, nil).(*httpClient)
		if !ok {
			t.Fatal("expected *httpClient")
		}

		if hc.httpClient.MaxIdemponentCallAttempts != 3 {
			t.Errorf("got %d want %d", hc.httpClient.MaxIdemponentCallAttempts, 3)
		}
	})

	t.Run("zero falls back to 1", func(t *testing.T) {
		cfg := &config.Dcp{}

		hc, ok := NewHTTPClient(cfg, nil).(*httpClient)
		if !ok {
			t.Fatal("expected *httpClient")
		}

		if hc.httpClient.MaxIdemponentCallAttempts != 1 {
			t.Errorf("got %d want %d", hc.httpClient.MaxIdemponentCallAttempts, 1)
		}
	})
}
