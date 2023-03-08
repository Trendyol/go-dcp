package godcpclient

import (
	"fmt"

	gDcp "github.com/Trendyol/go-dcp-client/dcp"

	"github.com/Trendyol/go-dcp-client/logger"

	"github.com/Trendyol/go-dcp-client/helpers"
	"github.com/Trendyol/go-dcp-client/servicediscovery"
	"github.com/gofiber/fiber/v2"
)

type API interface {
	Listen()
	Shutdown()
}

type api struct {
	client           gDcp.Client
	stream           Stream
	serviceDiscovery servicediscovery.ServiceDiscovery
	app              *fiber.App
	config           helpers.Config
}

func (s *api) Listen() {
	logger.Info("api starting on port %d", s.config.API.Port)

	err := s.app.Listen(fmt.Sprintf(":%d", s.config.API.Port))

	if err != nil {
		logger.Error(err, "api cannot start on port %d", s.config.API.Port)
	} else {
		logger.Debug("api stopped")
	}
}

func (s *api) Shutdown() {
	err := s.app.Shutdown()
	if err != nil {
		logger.Panic(err, "api cannot be shutdown")
	}
}

func (s *api) status(c *fiber.Ctx) error {
	_, err := s.client.Ping()
	if err != nil {
		return err
	}

	return c.SendString("OK")
}

func (s *api) offset(c *fiber.Ctx) error {
	return c.JSON(s.stream.GetOffsets())
}

func (s *api) rebalance(c *fiber.Ctx) error {
	s.stream.Rebalance()

	return c.SendString("OK")
}

func (s *api) followers(c *fiber.Ctx) error {
	return c.JSON(s.serviceDiscovery.GetAll())
}

func NewAPI(config helpers.Config, client gDcp.Client, stream Stream, serviceDiscovery servicediscovery.ServiceDiscovery) API {
	app := fiber.New(fiber.Config{DisableStartupMessage: true})

	metricMiddleware, err := NewMetricMiddleware(app, config, stream, client)

	if err == nil {
		app.Use(metricMiddleware)
	} else {
		logger.Error(err, "metric middleware cannot be initialized")
	}

	api := &api{
		app:              app,
		config:           config,
		client:           client,
		stream:           stream,
		serviceDiscovery: serviceDiscovery,
	}

	app.Get("/status", api.status)
	app.Get("/states/offset", api.offset)
	app.Get("/states/followers", api.followers)
	app.Get("/rebalance", api.rebalance)

	return api
}
