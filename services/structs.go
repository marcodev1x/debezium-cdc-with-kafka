package services

import (
	"strconv"
	"time"
)

type EventType string

const (
	UserCreated EventType = "UserCreated"
)

type Event interface {
	AggregateType() string
	AggregateID() string
}

type UserEvent struct {
	ID        int64      `json:"id"`
	Name      string     `json:"name"`
	Email     string     `json:"email"`
	CreatedAt *time.Time `json:"created_at"`
}

func (e UserEvent) AggregateType() string {
	return "user"
}

func (e UserEvent) AggregateID() string {
	return strconv.FormatInt(e.ID, 10)
}
