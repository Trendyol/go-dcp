package api

import (
	"fmt"

	"github.com/Trendyol/go-dcp/membership"
	"github.com/Trendyol/go-dcp/models"

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
	registerer       *metric.Registerer
}

func (s *api) Listen() {
	logger.Log.Info("api starting on port %d", s.config.API.Port)

	err := s.app.Listen(fmt.Sprintf(":%d", s.config.API.Port))

	if err != nil {
		logger.Log.Error("api cannot start on port %d, err: %v", s.config.API.Port, err)
	} else {
		logger.Log.Info("api stopped")
	}
}

func (s *api) Shutdown() {
	err := s.app.Shutdown()
	if err != nil {
		logger.Log.Error("error while api cannot be shutdown, err: %v", err)
		panic(err)
	}
}

func (s *api) UnregisterMetricCollectors() {
	s.registerer.UnregisterAll()
}

func (s *api) status(c *fiber.Ctx) error {
	if _, err := s.client.Ping(); err != nil {
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

func (s *api) setInfo(c *fiber.Ctx) error {
	var req models.SetInfoRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).SendString("Invalid request body")
	}

	s.stream.SetInfo(&membership.Model{MemberNumber: req.MemberNumber, TotalMembers: req.TotalMembers})

	if s.stream.IsAlive() {
		s.stream.Rebalance()
	}

	return c.SendString("OK")
}

func (s *api) followers(c *fiber.Ctx) error {
	if s.serviceDiscovery == nil {
		return c.SendString("service discovery is not enabled")
	}

	return c.JSON(s.serviceDiscovery.GetAll())
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
		registerer:       metric.WrapWithRegisterer(prometheus.DefaultRegisterer),
	}

	err := api.registerer.RegisterAll(collectors)
	if err == nil {
		app.Use(newMetricMiddleware(app, config))
	} else {
		logger.Log.Error("metric middleware cannot be initialized: %v", err)
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
	app.Put("/membership/info", api.setInfo)

	return api
}

func newMetricMiddleware(app *fiber.App, config *dcp.Dcp) func(ctx *fiber.Ctx) error {
	fiberPrometheus := fiberprometheus.NewWithRegistry(prometheus.DefaultRegisterer, config.Dcp.Group.Name, "http", "", nil)
	fiberPrometheus.RegisterAt(app, config.Metric.Path)

	logger.Log.Info("metric middleware registered on path %s", config.Metric.Path)

	return fiberPrometheus.Middleware
}
