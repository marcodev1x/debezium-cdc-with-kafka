package internal

import (
	"context"
	"github.com/uptrace/bun"
)

type TxFunc func(ctx context.Context, tx bun.Tx) (any, error)

type InsertConfig struct {
	Tx      bool
	TxFuncs []TxFunc
}
