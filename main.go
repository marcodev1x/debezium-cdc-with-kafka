package main

import "github.com/gofiber/fiber/v3"

func main() {
	module := fiber.New()

	module.Get("/", func(c fiber.Ctx) error {
		return c.SendString("Hello, World!")
	})

	module.Listen(":3000")
}
