package services

import (
	"context"
	"debezium-kafka/internal"
	"debezium-kafka/store"
	"encoding/json"
	"github.com/google/uuid"
	"github.com/uptrace/bun"
)

type ServiceInterface interface {
	PostUser(ctx context.Context, payload *store.User) error
	Endpoints() internal.Endpoints
}

type Service struct {
	userRepository *store.UserRepository
	pub            *internal.Publisher
}

func NewService(db *bun.DB) ServiceInterface {
	return &Service{
		userRepository: store.NewUserRepository(db),
		pub:            internal.NewPublisher(),
	}
}

func (s Service) PostUser(ctx context.Context, payload *store.User) error {
	configuration := internal.InsertConfig{
		Tx: true,
		TxFuncs: []internal.TxFunc{
			func(ctx context.Context, tx bun.Tx) (any, error) {
				return s.CreateOutbox(ctx, tx, UserCreated, UserEvent{
					ID:        payload.ID,
					Name:      payload.Name,
					Email:     payload.Email,
					CreatedAt: payload.CreatedAt,
				})
			},
		},
	}

	err := s.userRepository.CreateUser(ctx, payload, &configuration)

	if err != nil {
		return err
	}

	return nil
}

func (s Service) CreateOutbox(ctx context.Context, tx bun.Tx, eventType EventType, event Event) (*store.UserOutbox, error) {
	payload, err := json.Marshal(event)

	if err != nil {
		return nil, err
	}

	outbox := &store.UserOutbox{
		ID:            uuid.New(),
		AggregateType: event.AggregateType(),
		AggregateID:   event.AggregateID(),
		Type:          string(eventType),
		Payload:       payload,
	}

	return outbox, s.userRepository.CreateOutbox(ctx, tx, outbox)
}
