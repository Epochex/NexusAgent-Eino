// Package capacity provides PostgreSQL-authoritative admission. Workflow leases
// and interactive runs share one active view; provider calls have separate leases.
package capacity

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/chengpeng-cp/nexus-agent/internal/observability"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var ErrBusy = errors.New("shared capacity exhausted")
var ErrLeaseLost = errors.New("shared capacity lease lost")

type Policy struct {
	Scope            string `json:"scope"`
	ActiveLimit      int64  `json:"active_limit"`
	OwnerActiveLimit int64  `json:"owner_active_limit"`
	QueueLimit       int64  `json:"queue_limit"`
	OwnerQueueLimit  int64  `json:"owner_queue_limit"`
	LeaseMS          int64  `json:"lease_ms"`
	Version          int64  `json:"version"`
}
type Store struct{ db *gorm.DB }

func New(db *gorm.DB) *Store { return &Store{db: db} }
func (s *Store) Validate(parent context.Context) error {
	ctx, cancel := context.WithTimeout(parent, 3*time.Second)
	defer cancel()
	var count int64
	if err := s.db.WithContext(ctx).Table("execution_capacity_policies").Where("scope IN ?", []string{"run", "provider"}).Count(&count).Error; err != nil {
		return err
	}
	if count != 2 {
		return fmt.Errorf("shared capacity policies are missing")
	}
	_, err := RunCount(s.db.WithContext(ctx))
	return err
}

func lock(tx *gorm.DB, scope, resource string) error {
	return tx.Exec("SELECT pg_advisory_xact_lock(hashtextextended(current_schema()||':capacity:'||?||':'||?,0))", scope, resource).Error
}
func readPolicy(tx *gorm.DB, scope string) (Policy, error) {
	var p Policy
	err := tx.Table("execution_capacity_policies").Clauses(clause.Locking{Strength: "SHARE"}).Where("scope=?", scope).Take(&p).Error
	return p, err
}

// LockRun is used inside the workflow claim transaction, so deciding capacity
// and claiming the next ready run cannot race an interactive or another worker.
func LockRun(tx *gorm.DB) (Policy, error) {
	if err := lock(tx, "run", ""); err != nil {
		return Policy{}, err
	}
	return readPolicy(tx, "run")
}
func RunCount(tx *gorm.DB) (int64, error) {
	var n int64
	err := tx.Table("execution_active_capacity").Count(&n).Error
	return n, err
}

// LockQueue precedes idempotent lookup; CheckQueue runs only for NEW requests.
func LockQueue(tx *gorm.DB) error { return lock(tx, "queue", "") }
func CheckQueue(tx *gorm.DB, owner int64) error {
	p, err := readPolicy(tx, "run")
	if err != nil {
		return err
	}
	var counts struct{ Total, Own int64 }
	err = tx.Raw(`SELECT count(*) AS total,count(*) FILTER(WHERE owner_id=?) AS own
	 FROM workflow_runs WHERE definition IS NOT NULL AND deadline_at>clock_timestamp()
	 AND status NOT IN ('succeeded','failed','canceled')`, owner).Scan(&counts).Error
	if err != nil {
		return err
	}
	if counts.Total >= p.QueueLimit || counts.Own >= p.OwnerQueueLimit {
		observe("queue", "reject")
		return ErrBusy
	}
	return nil
}

type lease struct {
	store     *Store
	id, scope string
	ttl       time.Duration
}

type runLeaseKey struct{}

// FenceRun is the last write in the answer transaction, after the message INSERT
// (which may wait on locks). The opaque lease identity only comes from HoldRun.
// A heartbeat/context check alone cannot fence a paused executor's final write.
func FenceRun(ctx context.Context, tx *gorm.DB) error {
	id, _ := ctx.Value(runLeaseKey{}).(string)
	if id == "" {
		return nil // repository-only/unit users do not acquire interactive leases
	}
	result := tx.WithContext(ctx).Exec("UPDATE execution_capacity_leases SET expires_at=expires_at WHERE id=? AND scope='run' AND expires_at>clock_timestamp()", id)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return ErrLeaseLost
	}
	return nil
}

func (s *Store) acquire(parent context.Context, scope, resource string, owner int64) (*lease, error) {
	if s == nil || s.db == nil || owner < 0 || scope != "run" && scope != "provider" || scope == "run" && (owner == 0 || resource != "") || len(resource) > 128 || !utf8.ValidString(resource) {
		return nil, fmt.Errorf("invalid shared capacity request")
	}
	ctx, cancel := context.WithTimeout(parent, 3*time.Second)
	defer cancel()
	var result *lease
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := lock(tx, scope, resource); err != nil {
			return err
		}
		p, err := readPolicy(tx, scope)
		if err != nil {
			return err
		}
		// Dead leases do not reserve capacity and are bounded to the live pools.
		if err = tx.Exec("DELETE FROM execution_capacity_leases WHERE expires_at<=clock_timestamp()").Error; err != nil {
			return err
		}
		var counts struct{ Total, Own int64 }
		if scope == "run" {
			err = tx.Raw("SELECT count(*) AS total,count(*) FILTER(WHERE owner_id=?) AS own FROM execution_active_capacity", owner).Scan(&counts).Error
		} else {
			err = tx.Raw("SELECT count(*) AS total,count(*) FILTER(WHERE owner_id=?) AS own FROM execution_capacity_leases WHERE scope='provider' AND resource=? AND expires_at>clock_timestamp()", owner, resource).Scan(&counts).Error
		}
		if err != nil {
			return err
		}
		if counts.Total >= p.ActiveLimit || counts.Own >= p.OwnerActiveLimit {
			return ErrBusy
		}
		id := rand.Text()
		if err = tx.Exec("INSERT INTO execution_capacity_leases(id,scope,resource,owner_id,expires_at) VALUES(?,?,?,?,clock_timestamp()+?*interval '1 millisecond')", id, scope, resource, owner, p.LeaseMS).Error; err != nil {
			return err
		}
		result = &lease{store: s, id: id, scope: scope, ttl: time.Duration(p.LeaseMS) * time.Millisecond}
		return nil
	})
	if err == nil {
		observe(scope, "granted")
	} else if errors.Is(err, ErrBusy) {
		observe(scope, "reject")
	} else {
		observe(scope, "error")
	}
	return result, err
}
func (l *lease) renew(parent context.Context) error {
	ctx, cancel := context.WithTimeout(parent, 2*time.Second)
	defer cancel()
	result := l.store.db.WithContext(ctx).Table("execution_capacity_leases").Where("id=? AND expires_at>clock_timestamp()", l.id).Update("expires_at", gorm.Expr("clock_timestamp()+?*interval '1 millisecond'", l.ttl.Milliseconds()))
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return ErrLeaseLost
	}
	return nil
}
func (l *lease) hold(parent context.Context) (context.Context, func()) {
	ctx, cancel := context.WithCancelCause(parent)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(l.ttl / 3)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := l.renew(ctx); err != nil {
					observe(l.scope, "renew_lost")
					cancel(ErrLeaseLost)
					return
				}
			}
		}
	}()
	var once sync.Once
	release := func() {
		once.Do(func() {
			cancel(context.Canceled)
			<-done
			cleanup, stop := context.WithTimeout(context.WithoutCancel(parent), 2*time.Second)
			defer stop()
			if err := l.store.db.WithContext(cleanup).Exec("DELETE FROM execution_capacity_leases WHERE id=?", l.id).Error; err != nil {
				observe(l.scope, "release_error")
				log.Printf("capacity release failed; %s lease remains bounded by expiry", l.scope)
			}
		})
	}
	return ctx, release
}
func (s *Store) HoldRun(ctx context.Context, owner int64) (context.Context, func(), error) {
	l, err := s.acquire(ctx, "run", "", owner)
	if err != nil {
		return nil, nil, err
	}
	run, release := l.hold(ctx)
	return context.WithValue(run, runLeaseKey{}, l.id), release, nil
}
func (s *Store) HoldProvider(ctx context.Context, resource string, owner int64) (context.Context, func(), error) {
	l, err := s.acquire(ctx, "provider", resource, owner)
	if err != nil {
		return nil, nil, err
	}
	call, release := l.hold(ctx)
	return call, release, nil
}

func observe(scope, outcome string) {
	if m := observability.MetricsInstance(); m != nil {
		m.CapacityOperations.WithLabelValues(scope, outcome).Inc()
	}
}
