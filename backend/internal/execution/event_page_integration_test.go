//go:build integration

package execution

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type eventQueryCounter struct {
	logger.Interface
	count atomic.Int64
}

func (l *eventQueryCounter) Trace(context.Context, time.Time, func() (string, int64), error) {
	l.count.Add(1)
}
func TestEventPageReadsBoundedTerminalCursorWithOneSnapshotQuery(t *testing.T) {
	s, db, owner, wf := fixture(t)
	run := enqueue(t, s, owner, wf)
	active := claim(t, s, "event-page")
	if err := s.Finish(context.Background(), active, Succeeded, []byte(`{"result":"saved"}`), ""); err != nil {
		t.Fatal(err)
	}
	counter := &eventQueryCounter{Interface: logger.Default.LogMode(logger.Silent)}
	reader := NewStore(db.Session(&gorm.Session{Logger: counter}))
	page, err := reader.ReadEventPage(context.Background(), owner, run.ID, 0, 1)
	if err != nil || page.Status != Succeeded || len(page.Events) != 1 || page.NextSeq < 2 || counter.count.Load() != 1 {
		t.Fatal("bounded single-snapshot page", page, err, counter.count.Load())
	}
	cursor := page.Events[0].Seq
	page, err = reader.ReadEventPage(context.Background(), owner, run.ID, cursor, 128)
	if err != nil || len(page.Events) == 0 || page.Events[len(page.Events)-1].Seq != page.NextSeq || page.Events[len(page.Events)-1].Type != Succeeded {
		t.Fatal("lost terminal tail", page, err)
	}
	if _, err = reader.ReadEventPage(context.Background(), owner+999, run.ID, 0, 128); !errors.Is(err, ErrNotFound) {
		t.Fatal("foreign read", err)
	}
	if _, err = reader.ReadEventPage(context.Background(), owner, run.ID, page.NextSeq+1, 128); !errors.Is(err, ErrInvalidInput) {
		t.Fatal("future cursor", err)
	}
	if err = db.Exec("DELETE FROM execution_events WHERE run_id=?", run.ID).Error; err != nil {
		t.Fatal(err)
	}
	if _, err = reader.ReadEventPage(context.Background(), owner, run.ID, 0, 128); !errors.Is(err, ErrCursorExpired) {
		t.Fatal("expired cursor", err)
	}
	page, err = reader.ReadEventPage(context.Background(), owner, run.ID, page.NextSeq, 128)
	if err != nil || len(page.Events) != 0 || page.Status != Succeeded {
		t.Fatal("caught-up terminal cursor", page, err)
	}
}
