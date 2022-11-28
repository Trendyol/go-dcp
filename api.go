package godcpclient

import (
	"fmt"
	"github.com/gofiber/fiber/v2"
	"log"
)

type Api interface {
	Listen()
	Shutdown()
}

type api struct {
	app      *fiber.App
	config   Config
	observer Observer
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
	return c.SendString("OK")
}

func (s *api) observerState(c *fiber.Ctx) error {
	return c.JSON(s.observer.GetState())
}

func NewApi(config Config, observer Observer) Api {
	app := fiber.New(fiber.Config{DisableStartupMessage: true})

	app.Use(NewMetricMiddleware(app, config, observer))

	api := &api{
		app:      app,
		config:   config,
		observer: observer,
	}

	app.Get("/status", api.status)
	app.Get("/states/observer", api.observerState)

	return api
}
