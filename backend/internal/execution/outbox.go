package execution

import (
	"context"
	"encoding/json"
	"errors"
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
	count := 0
	for range limit {
		found := false
		err := s.transaction(ctx, func(tx *gorm.DB) error {
			var n Notification
			err := tx.Where("published_at IS NULL").Order("id").Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).Take(&n).Error
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return nil
			}
			if err != nil {
				return err
			}
			found = true
			callCtx, cancel := context.WithTimeout(tx.Statement.Context, 2*time.Second)
			defer cancel()
			if err = publish(callCtx, n); err != nil {
				return err
			}
			return tx.Model(&Notification{}).Where("id=?", n.ID).Update("published_at", gorm.Expr("NOW()")).Error
		})
		if err != nil {
			return count, err
		}
		if !found {
			break
		}
		count++
	}
	return count, nil
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
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
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
		case <-ticker.C:
			if _, err := s.PublishOutbox(ctx, publish, 32); err != nil && ctx.Err() == nil {
				log.Printf("execution outbox delivery retained for retry: %v", err)
			}
		case <-retention.C:
			if err := s.PruneEvents(ctx, 7*24*time.Hour, 1000); err != nil && ctx.Err() == nil {
				log.Printf("execution retention: %v", err)
			}
		}
	}
}
