package couchbase

import (
	"time"

	"github.com/Trendyol/go-dcp/config"
	"github.com/Trendyol/go-dcp/logger"
)

type HealthCheck interface {
	Start(ch chan struct{})
	Stop()
}

type healthCheck struct {
	ticker *time.Ticker
	config *config.HealthCheck
	client Client
}

func (h *healthCheck) Start(ch chan struct{}) {
	h.ticker = time.NewTicker(h.config.Interval)

	go func() {
		for range h.ticker.C {
			if _, err := h.client.Ping(); err != nil {
				logger.Log.Error("health check failed: %v", err)
				h.ticker.Stop()
				ch <- struct{}{}
				break
			}
		}
	}()
}

func (h *healthCheck) Stop() {
	h.ticker.Stop()
}

func NewHealthCheck(config *config.HealthCheck, client Client) HealthCheck {
	return &healthCheck{
		config: config,
		client: client,
	}
}
