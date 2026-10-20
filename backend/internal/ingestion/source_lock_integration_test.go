//go:build integration

package ingestion

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRemoteSourceLockOrdersCleanupAndPreservesPoolCapacity(t *testing.T) {
	_, db, _, _, doc := fixture(t)
	pool, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	pool.SetMaxOpenConns(2)
	first, err := LockSource(context.Background(), db, doc.ID)
	if err != nil {
		t.Fatal(err)
	}
	released := false
	defer func() {
		if !released {
			first()
		}
	}()
	blocked, stop := context.WithTimeout(context.Background(), 100*time.Millisecond)
	_, err = LockSource(blocked, db, doc.ID)
	stop()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("cleanup passed an in-flight source writer", err)
	}
	// The IO holder must leave capacity for heartbeats and fencing queries.
	query, stopQuery := context.WithTimeout(context.Background(), time.Second)
	defer stopQuery()
	if err = db.WithContext(query).Exec("SELECT 1").Error; err != nil {
		t.Fatal("source holder starved authoritative writes", err)
	}
	first()
	released = true
	next, err := LockSource(query, db, doc.ID)
	if err != nil {
		t.Fatal("source lock not released", err)
	}
	next()
}
