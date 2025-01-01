package couchbase

import (
	"context"
	"github.com/Trendyol/go-dcp/config"
	"github.com/Trendyol/go-dcp/logger"
	"sync"
	"time"
)

type HealthCheck interface {
	Start()
	Stop()
}

type healthCheck struct {
	config     *config.HealthCheck
	client     Client
	cancelFunc context.CancelFunc
	wg         sync.WaitGroup
	startOnce  sync.Once
	stopOnce   sync.Once
}

func NewHealthCheck(config *config.HealthCheck, client Client) HealthCheck {
	return &healthCheck{
		config: config,
		client: client,
	}
}

func (h *healthCheck) Start() {
	h.startOnce.Do(func() {
		ctx, cancel := context.WithCancel(context.Background())
		h.cancelFunc = cancel
		h.wg.Add(1)
		go h.run(ctx)
	})
}

func (h *healthCheck) Stop() {
	h.stopOnce.Do(func() {
		if h.cancelFunc != nil {
			h.cancelFunc()
		}
		h.wg.Wait()
	})
}

func (h *healthCheck) run(ctx context.Context) {
	defer h.wg.Done()

	ticker := time.NewTicker(h.config.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Log.Info("Health check stopped.")
			return
		case <-ticker.C:
			h.performHealthCheck(ctx)
		}
	}
}

func (h *healthCheck) performHealthCheck(ctx context.Context) {
	const maxRetries = 5
	retryInterval := time.Second

	for attempt := 1; attempt <= maxRetries; attempt++ {
		_, err := h.client.Ping()
		if err == nil {
			logger.Log.Trace("healthcheck success")
			return
		}

		logger.Log.Warn("Health check attempt %d/%d failed: %v", attempt, maxRetries, err)

		if attempt < maxRetries {
			select {
			case <-ctx.Done():
				logger.Log.Info("Health check canceled during retry.")
				return
			case <-time.After(retryInterval):
				// Retry after waiting.
			}
		} else {
			logger.Log.Error("Health check failed after %d attempts: %v", maxRetries, err)
			panic(err)
		}
	}
}
