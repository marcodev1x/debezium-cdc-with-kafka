package main

import (
	"context"
	"database/sql"
	"debezium-kafka/handlers"
	"debezium-kafka/internal"
	"debezium-kafka/services"
	"errors"
	"github.com/gofiber/fiber/v3"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
	"github.com/uptrace/bun/driver/pgdriver"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pgsql := sql.OpenDB(pgdriver.NewConnector(
		pgdriver.WithDSN("postgres://user:password@localhost:5432/dbname?sslmode=disable"),
	))
	db := bun.NewDB(pgsql, pgdialect.New())

	defer db.Close()

	service := services.NewService(db)

	broker := os.Getenv("KAFKA_BROKER")

	if broker == "" {
		broker = "localhost:9092"
	}

	subscriber := internal.NewSubscriber([]string{broker}, "debezium-kafka", service.Endpoints())
	subscribed := make(chan struct{})

	go func() {
		defer close(subscribed)

		if err := subscriber.Sub(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("sub: stopped. error=%v", err)
		}
	}()

	module := fiber.New()

	module.Get("/", func(c fiber.Ctx) error {
		return c.SendString("Hello, World!")
	})

	module.Post("/users", handlers.NewUserHandler(service).PostUser)

	if err := module.Listen(":3000", fiber.ListenConfig{GracefulContext: ctx}); err != nil {
		log.Printf("http: listen failed. error=%v", err)
		stop()
	}

	<-subscribed
}
