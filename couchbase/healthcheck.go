package couchbase

import (
	"time"

	"github.com/Trendyol/go-dcp/config"
	"github.com/Trendyol/go-dcp/logger"
)

type HealthCheck interface {
	Start()
	Stop()
}

type healthCheck struct {
	ticker *time.Ticker
	config *config.HealthCheck
	client Client
}

func (h *healthCheck) Start() {
	h.ticker = time.NewTicker(h.config.Interval)

	go func() {
		for range h.ticker.C {
			if _, err := h.client.Ping(); err != nil {
				logger.Log.Error("error while health check: %v", err)
				panic(err)
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
