package godcpclient

import (
	"fmt"
	"github.com/Trendyol/go-dcp-client/helpers"
	"github.com/Trendyol/go-dcp-client/servicediscovery"
	"github.com/gofiber/fiber/v2"
	"log"
)

type Api interface {
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
		log.Printf("api starting on port %d", s.config.Api.Port)

		err := s.app.Listen(fmt.Sprintf(":%d", s.config.Api.Port))

		if err != nil {
			panic(err)
		}

		log.Printf("api stopped")
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

func NewApi(config helpers.Config, client Client, stream Stream, serviceDiscovery servicediscovery.ServiceDiscovery) Api {
	app := fiber.New(fiber.Config{DisableStartupMessage: true})

	app.Use(NewMetricMiddleware(app, config, stream.GetObserver()))

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
