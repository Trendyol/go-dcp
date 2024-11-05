package couchbase

import (
	"time"

	"github.com/Trendyol/go-dcp/logger"

	"github.com/Trendyol/go-dcp/config"
)

type HealthCheck interface {
	Start()
	Stop()
}

type healthCheck struct {
	config  *config.HealthCheck
	client  Client
	running bool
}

func (h *healthCheck) Start() {
	h.running = true

	go func() {
		for h.running {
			time.Sleep(h.config.Interval)

			retry := 5

			for {
				_, err := h.client.Ping()
				if err == nil {
					break
				} else {
					logger.Log.Warn("cannot health check, err: %v", err)
				}

				retry--
				if retry == 0 {
					logger.Log.Error("error while health check: %v", err)
					panic(err)
				}

				time.Sleep(time.Second)
			}
		}
	}()
}

func (h *healthCheck) Stop() {
	h.running = false
}

func NewHealthCheck(config *config.HealthCheck, client Client) HealthCheck {
	return &healthCheck{
		config: config,
		client: client,
	}
}
