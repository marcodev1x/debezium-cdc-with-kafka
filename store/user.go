package store

import (
	"context"
	"debezium-kafka/internal"
	"encoding/json"
	"github.com/google/uuid"
	"github.com/uptrace/bun"
	"time"
)

type User struct {
	bun.BaseModel `bun:"table:users,alias:u"`

	ID        int64      `bun:",pk,autoincrement"`
	Name      string     `bun:",notnull"`
	Email     string     `bun:",unique,notnull"`
	CreatedAt *time.Time `bun:",nullzero,notnull,default:current_timestamp"`
	UpdatedAt *time.Time `bun:",nullzero,notnull,default:current_timestamp"`
}

type UserOutbox struct {
	bun.BaseModel `bun:"table:users_outboxes,alias:uo"`

	ID            uuid.UUID       `bun:",pk,type:uuid"`
	AggregateType string          `bun:"aggregatetype,notnull"`
	AggregateID   string          `bun:"aggregateid,notnull"`
	Type          string          `bun:",notnull"`
	Payload       json.RawMessage `bun:",type:jsonb,notnull"`
	CreatedAt     *time.Time      `bun:",nullzero,notnull,default:current_timestamp"`
}

type UserRepository struct {
	sql *bun.DB
}

func NewUserRepository(sql *bun.DB) *UserRepository {
	return &UserRepository{
		sql: sql,
	}
}

func (u UserRepository) CreateUser(ctx context.Context, payload *User, config *internal.InsertConfig) error {
	err := u.sql.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		_, err := tx.
			NewInsert().
			Model(payload).
			Exec(ctx)

		if err != nil {
			return err
		}

		if config != nil && config.Tx {
			for _, fn := range config.TxFuncs {
				if _, err := fn(ctx, tx); err != nil {
					return err
				}
			}
		}

		return nil
	})

	return err
}

func (u UserRepository) CreateOutbox(ctx context.Context, db bun.IDB, payload *UserOutbox) error {
	_, err := db.
		NewInsert().
		Model(payload).
		Exec(ctx)

	return err
}
