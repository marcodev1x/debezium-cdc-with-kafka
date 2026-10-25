package internal

import (
	"context"
	"errors"
	"fmt"
	"github.com/segmentio/kafka-go"
	"log"
	"os"
	"sync"
	"time"
)

type Publisher struct {
	writer *kafka.Writer
}

type Subscriber struct {
	group   string
	eps     Endpoints
	readers map[string]*kafka.Reader
}

type Endpoints map[string]EventConstructor

type EventConstructor struct {
	Read     func(ctx context.Context, msg kafka.Message) (any, error)
	Threads  int
	Provider func(ctx context.Context, msg any) (interface{}, error)
	Retries  int
}

func NewPublisher() *Publisher {
	broker := os.Getenv("KAFKA_BROKER")

	return &Publisher{
		writer: &kafka.Writer{
			Addr:                   kafka.TCP(broker),
			AllowAutoTopicCreation: true,
			Compression:            kafka.Zstd,
			RequiredAcks:           kafka.RequireAll,
			MaxAttempts:            5,
			Logger:                 kafka.LoggerFunc(log.Printf),
			ErrorLogger:            kafka.LoggerFunc(log.Printf),
			Topic:                  "test",
			Balancer:               &kafka.LeastBytes{},
		},
	}
}

func (p *Publisher) Pub(ctx context.Context, messages ...kafka.Message) error {
	return p.writer.WriteMessages(ctx, messages...)
}

func (p *Publisher) Close() error {
	return p.writer.Close()
}

var ErrMalformed = errors.New("bus: malformed message")

func NewSubscriber(brokers []string, group string, eps Endpoints) *Subscriber {
	sbs := &Subscriber{
		group:   group,
		eps:     eps,
		readers: make(map[string]*kafka.Reader),
	}

	for topic := range eps {
		sbs.readers[topic] = kafka.NewReader(kafka.ReaderConfig{
			Brokers:               brokers,
			GroupID:               group,
			Topic:                 topic,
			WatchPartitionChanges: true,
			Logger:                kafka.LoggerFunc(log.Printf),
		})
	}

	return sbs
}

func (s *Subscriber) Sub(ctx context.Context) error {
	work := context.WithoutCancel(ctx)

	var wg sync.WaitGroup

	for topic, reader := range s.readers {
		wg.Add(1)

		go func(r *kafka.Reader, e EventConstructor) {
			defer wg.Done()

			type inflight struct {
				msg  kafka.Message
				done chan bool
			}

			order := make(chan inflight, max(e.Threads, 1)-1)

			wg.Go(func() {
				defer close(order)

				for failures := 0; ; {
					msg, err := r.FetchMessage(ctx)

					if err != nil {
						if ctx.Err() != nil {
							return
						}

						failures++
						log.Printf("sub: fetch failed. Topic=%s, failures=%d, error=%v", r.Config().Topic, failures, err)

						select {
						case <-ctx.Done():
							return
						case <-time.After(min(time.Second<<min(failures, 5), 30*time.Second)):
						}

						continue
					}

					flight := inflight{
						msg:  msg,
						done: make(chan bool, 1),
					}

					select {
					case <-ctx.Done():
						return
					case order <- flight:
					}

					wg.Go(func() {
						defer close(flight.done)

						var err error

						for tries := 0; tries <= e.Retries; tries++ {
							if tries > 0 {
								select {
								case <-ctx.Done():
									return
								case <-time.After(min(time.Second<<min(tries-1, 5), 30*time.Second)):
								}
							}

							if err = e.consume(work, flight.msg); err == nil || errors.Is(err, ErrMalformed) {
								break
							}
						}

						if err != nil {
							log.Printf("sub: process failed. Topic=%s, error=%v", r.Config().Topic, err)
						}

						flight.done <- true
					})
				}
			})

			halted := false

			for f := range order {
				if !<-f.done {
					halted = true
				}

				if halted {
					continue
				}

				if err := r.CommitMessages(ctx, f.msg); err != nil {
					log.Printf("sub: commit failed. Topic=%s, error=%v", r.Config().Topic, err)
				}
			}
		}(reader, s.eps[topic])
	}

	<-ctx.Done()
	wg.Wait()

	left := make(chan struct{})

	go func() {
		for _, r := range s.readers {
			wg.Go(func() {
				r.Close()
			})
		}

		wg.Wait()
		close(left)
	}()

	select {
	case <-left:
	case <-time.After(10 * time.Second):
		log.Printf("bus: readers still leaving group=%s after 10s, likely mid rebalance; exiting anyway", s.group)
	}

	return ctx.Err()
}

func (h EventConstructor) consume(ctx context.Context, msg kafka.Message) error {
	req, err := h.Read(ctx, msg)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrMalformed, err)
	}

	_, err = h.Provider(ctx, req)

	return err
}
