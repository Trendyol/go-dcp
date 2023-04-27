package api

import (
	"fmt"

	godcpclient "github.com/Trendyol/go-dcp-client/config"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/gofiber/fiber/v2/middleware/pprof"

	"github.com/Trendyol/go-dcp-client/couchbase"
	"github.com/Trendyol/go-dcp-client/logger"
	"github.com/Trendyol/go-dcp-client/servicediscovery"
	"github.com/Trendyol/go-dcp-client/stream"

	"github.com/gofiber/fiber/v2"
)

type API interface {
	Listen()
	Shutdown()
}

type api struct {
	client           couchbase.Client
	stream           stream.Stream
	serviceDiscovery servicediscovery.ServiceDiscovery
	app              *fiber.App
	config           *godcpclient.Dcp
}

func (s *api) Listen() {
	logger.Log.Printf("api starting on port %d", s.config.API.Port)

	err := s.app.Listen(fmt.Sprintf(":%d", s.config.API.Port))

	if err != nil {
		logger.ErrorLog.Printf("api cannot start on port %d, err: %v", s.config.API.Port, err)
	} else {
		logger.Log.Printf("api stopped")
	}
}

func (s *api) Shutdown() {
	err := s.app.Shutdown()
	if err != nil {
		logger.ErrorLog.Printf("api cannot be shutdown, err: %v", err)
		panic(err)
	}
}

func (s *api) status(c *fiber.Ctx) error {
	if err := s.client.Ping(); err != nil {
		return err
	}

	return c.SendString("OK")
}

func (s *api) offset(c *fiber.Ctx) error {
	offsets, _, _ := s.stream.GetOffsets()
	return c.JSON(offsets)
}

func (s *api) rebalance(c *fiber.Ctx) error {
	s.stream.Rebalance()

	return c.SendString("OK")
}

func (s *api) followers(c *fiber.Ctx) error {
	if s.serviceDiscovery == nil {
		return c.SendString("service discovery is not enabled")
	}

	return c.JSON(s.serviceDiscovery.GetAll())
}

func NewAPI(config *godcpclient.Dcp,
	client couchbase.Client,
	stream stream.Stream,
	serviceDiscovery servicediscovery.ServiceDiscovery,
	vBucketDiscovery stream.VBucketDiscovery,
	metricCollectors ...prometheus.Collector,
) API {
	app := fiber.New(fiber.Config{DisableStartupMessage: true})

	api := &api{
		app:              app,
		config:           config,
		client:           client,
		stream:           stream,
		serviceDiscovery: serviceDiscovery,
	}

	metricMiddleware, err := NewMetricMiddleware(app, config, stream, client, vBucketDiscovery, metricCollectors...)

	if err == nil {
		app.Use(metricMiddleware)
	} else {
		logger.ErrorLog.Printf("metric middleware cannot be initialized: %v", err)
	}

	if config.Debug {
		app.Use(pprof.New())
		app.Get("/states/offset", api.offset)
		app.Get("/states/followers", api.followers)
	}

	if !config.HealthCheck.Disabled {
		app.Get("/status", api.status)
	}

	app.Get("/rebalance", api.rebalance)

	return api
}
