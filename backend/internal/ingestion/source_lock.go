package ingestion

import (
	"context"
	"database/sql/driver"
	"fmt"
	"gorm.io/gorm"
	"sync"
	"time"
)

var sourcePools sync.Map

// Serialize remote writes and cleanup across processes, including canceled generations.
// The connection-scoped lock is automatically released if its worker process dies.
func LockSource(ctx context.Context, database *gorm.DB, docID int64) (func(), error) {
	db, err := database.DB()
	if err != nil {
		return nil, err
	}
	maximum := db.Stats().MaxOpenConnections
	if maximum == 1 {
		return nil, fmt.Errorf("source locking requires a PostgreSQL pool of at least two")
	}
	capacity := 8
	if maximum > 0 {
		capacity = min(capacity, max(1, maximum/2))
	}
	value, _ := sourcePools.LoadOrStore(db, make(chan struct{}, capacity))
	slots := value.(chan struct{})
	select {
	case slots <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		<-slots
		return nil, err
	}
	closeConn := func() { conn.Close(); <-slots }
	var key int64
	if err = conn.QueryRowContext(ctx, "SELECT hashtextextended(current_schema()||':doc:'||$1::text,0)", docID).Scan(&key); err != nil {
		closeConn()
		return nil, err
	}
	for {
		var locked bool
		if err = conn.QueryRowContext(ctx, "SELECT pg_try_advisory_lock($1)", key).Scan(&locked); err != nil {
			_ = conn.Raw(func(any) error { return driver.ErrBadConn })
			closeConn()
			return nil, err
		}
		if locked {
			return func() {
				release, stop := context.WithTimeout(context.Background(), 2*time.Second)
				defer stop()
				var unlocked bool
				if err := conn.QueryRowContext(release, "SELECT pg_advisory_unlock($1)", key).Scan(&unlocked); err != nil || !unlocked {
					// Never return a connection still holding a session lock to the pool.
					_ = conn.Raw(func(any) error { return driver.ErrBadConn })
				}
				closeConn()
			}, nil
		}
		select {
		case <-ctx.Done():
			closeConn()
			return nil, ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}
