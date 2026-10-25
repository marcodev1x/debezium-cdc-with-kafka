package handlers

import (
	"debezium-kafka/services"
	"debezium-kafka/store"
	"errors"
	"github.com/gofiber/fiber/v3"
	"github.com/uptrace/bun/driver/pgdriver"
	"log"
	"strings"
	"time"
)

type CreateUserRequest struct {
	Name  string `json:"name"`
	Email string `json:"email"`
}

type UserResponse struct {
	ID        int64      `json:"id"`
	Name      string     `json:"name"`
	Email     string     `json:"email"`
	CreatedAt *time.Time `json:"created_at"`
}

type UserHandler struct {
	service services.ServiceInterface
}

func NewUserHandler(service services.ServiceInterface) *UserHandler {
	return &UserHandler{
		service: service,
	}
}

func (h UserHandler) PostUser(c fiber.Ctx) error {
	var body CreateUserRequest

	if err := c.Bind().Body(&body); err != nil {
		return fiber.NewError(fiber.StatusBadRequest, "invalid body")
	}

	body.Name = strings.TrimSpace(body.Name)
	body.Email = strings.TrimSpace(body.Email)

	if body.Name == "" || body.Email == "" {
		return fiber.NewError(fiber.StatusBadRequest, "name and email are required")
	}

	user := &store.User{
		Name:  body.Name,
		Email: body.Email,
	}

	if err := h.service.PostUser(c.Context(), user); err != nil {
		var pgErr pgdriver.Error

		if errors.As(err, &pgErr) && pgErr.Field('C') == "23505" {
			return fiber.NewError(fiber.StatusConflict, "email already registered")
		}

		log.Printf("http: post user failed. error=%v", err)

		return fiber.ErrInternalServerError
	}

	return c.Status(fiber.StatusCreated).JSON(UserResponse{
		ID:        user.ID,
		Name:      user.Name,
		Email:     user.Email,
		CreatedAt: user.CreatedAt,
	})
}
