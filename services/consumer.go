package services

import (
	"context"
	"debezium-kafka/internal"
	"encoding/json"
	"github.com/segmentio/kafka-go"
	"log"
)

const UserTopic = "outbox.event.user"

type Incoming[T Event] struct {
	ID    string
	Type  EventType
	Event T
}

func (s Service) Endpoints() internal.Endpoints {
	return internal.Endpoints{
		UserTopic: {
			Read: func(ctx context.Context, msg kafka.Message) (any, error) {
				return decode[UserEvent](msg)
			},
			// 1 thread mantém a ordem dos eventos de um mesmo usuário
			Threads:  1,
			Retries:  3,
			Provider: s.OnUserEvent,
		},
	}
}

func (s Service) OnUserEvent(ctx context.Context, msg any) (any, error) {
	in := msg.(Incoming[UserEvent])

	switch in.Type {
	case UserCreated:
		log.Printf("consumer: user created. EventID=%s, UserID=%d, Email=%s", in.ID, in.Event.ID, in.Event.Email)
	default:
		log.Printf("consumer: event ignored. EventID=%s, Type=%s", in.ID, in.Type)
	}

	return nil, nil
}

func decode[T Event](msg kafka.Message) (Incoming[T], error) {
	var in Incoming[T]

	for _, h := range msg.Headers {
		switch h.Key {
		case "id":
			in.ID = string(h.Value)
		case "eventType":
			in.Type = EventType(h.Value)
		}
	}

	err := json.Unmarshal(msg.Value, &in.Event)

	return in, err
}
