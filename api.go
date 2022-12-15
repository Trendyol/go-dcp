package godcpclient

import (
	"fmt"
	"log"

	"github.com/Trendyol/go-dcp-client/helpers"
	"github.com/Trendyol/go-dcp-client/servicediscovery"
	"github.com/gofiber/fiber/v2"
)

type API interface {
	Listen()
	Shutdown()
}

type api struct {
	app              *fiber.App
	config           helpers.Config
	client           Client
	stream           Stream
	serviceDiscovery servicediscovery.ServiceDiscovery
}

func (s *api) Listen() {
	go func() {
		log.Printf("api starting on port %d", s.config.API.Port)

		err := s.app.Listen(fmt.Sprintf(":%d", s.config.API.Port))

		if err != nil {
			log.Printf("api cannot start on port %d, error: %v", s.config.API.Port, err)
		} else {
			log.Printf("api stopped")
		}
	}()
}

func (s *api) Shutdown() {
	err := s.app.Shutdown()
	if err != nil {
		panic(err)
	}
}

func (s *api) status(c *fiber.Ctx) error {
	_, err := s.client.Ping()
	if err != nil {
		return err
	}

	return c.SendString("OK")
}

func (s *api) observerState(c *fiber.Ctx) error {
	return c.JSON(s.stream.GetObserver().GetState())
}

func (s *api) rebalance(c *fiber.Ctx) error {
	s.stream.Rebalance()

	return c.SendString("OK")
}

func (s *api) followers(c *fiber.Ctx) error {
	return c.JSON(s.serviceDiscovery.GetAll())
}

func NewAPI(config helpers.Config, client Client, stream Stream, serviceDiscovery servicediscovery.ServiceDiscovery) API {
	app := fiber.New(fiber.Config{DisableStartupMessage: true})

	metricMiddleware, err := NewMetricMiddleware(app, config, stream.GetObserver())

	if err != nil {
		app.Use(metricMiddleware)
	} else {
		log.Printf("metric middleware cannot be initialized, error: %v", err)
	}

	api := &api{
		app:              app,
		config:           config,
		client:           client,
		stream:           stream,
		serviceDiscovery: serviceDiscovery,
	}

	app.Get("/status", api.status)
	app.Get("/states/observer", api.observerState)
	app.Get("/states/followers", api.followers)
	app.Post("/rebalance", api.rebalance)

	return api
}
