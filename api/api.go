package api

import (
	"fmt"
	"github.com/Trendyol/go-dcp/metric"
	"github.com/ansrivas/fiberprometheus/v2"
	"github.com/prometheus/client_golang/prometheus"

	dcp "github.com/Trendyol/go-dcp/config"

	"github.com/gofiber/fiber/v2/middleware/pprof"

	"github.com/Trendyol/go-dcp/couchbase"
	"github.com/Trendyol/go-dcp/logger"
	"github.com/Trendyol/go-dcp/servicediscovery"
	"github.com/Trendyol/go-dcp/stream"

	"github.com/gofiber/fiber/v2"
)

type API interface {
	Listen()
	Shutdown()
	UnregisterMetricCollectors()
}

type api struct {
	client           couchbase.Client
	stream           stream.Stream
	serviceDiscovery servicediscovery.ServiceDiscovery
	app              *fiber.App
	config           *dcp.Dcp
	registerer       *metric.Unregisterer
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

func (s *api) UnregisterMetricCollectors() {
	s.registerer.UnregisterAll()
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

func (s *api) registerMetricCollectors(collectors []prometheus.Collector) {
	s.registerer.RegisterAll(collectors)
}

func NewAPI(config *dcp.Dcp,
	client couchbase.Client,
	stream stream.Stream,
	serviceDiscovery servicediscovery.ServiceDiscovery,
	collectors []prometheus.Collector,
) API {
	app := fiber.New(fiber.Config{DisableStartupMessage: true})

	api := &api{
		app:              app,
		config:           config,
		client:           client,
		stream:           stream,
		serviceDiscovery: serviceDiscovery,
		registerer:       metric.WrapWithUnregisterer(prometheus.DefaultRegisterer),
	}

	api.registerer.RegisterAll(collectors)
	metricMiddleware, err := newMetricMiddleware(app, config)

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

func newMetricMiddleware(app *fiber.App,
	config *dcp.Dcp,
) (func(ctx *fiber.Ctx) error, error) {
	fiberPrometheus := fiberprometheus.New(config.Dcp.Group.Name)
	fiberPrometheus.RegisterAt(app, config.Metric.Path)

	logger.Log.Printf("metric middleware registered on path %s", config.Metric.Path)

	return fiberPrometheus.Middleware, nil
}
