package execution

import (
	"context"
	"encoding/json"
	"log"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const NotificationStream = "stream:execution-events"
const notificationChannel = "execution:wake"

type Notification struct {
	ID      int64
	RunID   int64
	Seq     int64
	Topic   string
	Payload json.RawMessage
}

func (Notification) TableName() string { return "execution_outbox" }

// Delivery is at least once: a crash after the peer accepts but before the PG
// commit can redeliver the same (run_id,seq). Consumers must deduplicate by it.
func (s *Store) PublishOutbox(ctx context.Context, publish func(context.Context, Notification) error, limit int) (int, error) {
	if publish == nil || limit < 1 || limit > 128 {
		return 0, ErrInvalidInput
	}
	var rows []Notification
	err := s.transaction(ctx, func(tx *gorm.DB) error {
		if err := tx.Where("published_at IS NULL").Order("id").Limit(limit).Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).Find(&rows).Error; err != nil {
			return err
		}
		ids := make([]int64, 0, len(rows))
		for _, n := range rows {
			callCtx, cancel := context.WithTimeout(tx.Statement.Context, 2*time.Second)
			err := publish(callCtx, n)
			cancel()
			if err != nil {
				return err
			}
			ids = append(ids, n.ID)
		}
		if len(ids) == 0 {
			return nil
		}
		return tx.Model(&Notification{}).Where("id IN ?", ids).Update("published_at", gorm.Expr("clock_timestamp()")).Error
	})
	if err != nil {
		return 0, err
	}
	return len(rows), nil
}

func (s *Store) RunNotifications(ctx context.Context, rdb *redis.Client, notify func()) error {
	if rdb == nil || notify == nil {
		return ErrInvalidInput
	}
	// Pubsub is only a wake-up hint; the bounded stream carries audit hints.
	sub := rdb.Subscribe(ctx, notificationChannel)
	defer sub.Close()
	messages := sub.Channel(redis.WithChannelSize(1))
	publish := func(callCtx context.Context, n Notification) error {
		if err := rdb.XAdd(callCtx, &redis.XAddArgs{Stream: NotificationStream, MaxLen: 100000, Approx: true, Values: map[string]any{"run_id": n.RunID, "seq": n.Seq, "topic": n.Topic, "payload": string(n.Payload)}}).Err(); err != nil {
			return err
		}
		return rdb.Publish(callCtx, notificationChannel, strconv.FormatInt(n.RunID, 10)).Err()
	}
	timer := time.NewTimer(0)
	defer timer.Stop()
	retention := time.NewTicker(time.Hour)
	defer retention.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case _, ok := <-messages:
			if !ok {
				messages = nil
			} else {
				notify()
			}
		case <-timer.C:
			count, err := s.PublishOutbox(ctx, publish, 32)
			if err != nil && ctx.Err() == nil {
				log.Printf("execution outbox delivery retained for retry: %v", err)
			}
			delay := 500 * time.Millisecond
			if err == nil && count == 32 {
				delay = 0
			}
			// Full batches continue immediately. An empty queue or failed peer
			// backs off, while each bounded batch returns to cancellation/retention.
			timer.Reset(delay)
		case <-retention.C:
			if err := s.PruneEvents(ctx, 7*24*time.Hour, 1000); err != nil && ctx.Err() == nil {
				log.Printf("execution retention: %v", err)
			}
		}
	}
}
