package capacity

import (
	"context"
	"errors"
	"time"

	"gorm.io/gorm"
)

var ErrInvalidPolicy = errors.New("invalid shared capacity policy")
var ErrPolicyConflict = errors.New("capacity policy version changed")

type Snapshot struct {
	Policies             []Policy  `json:"policies" gorm:"-"`
	ActiveRuns           int64     `json:"active_runs"`
	ActiveProviderCalls  int64     `json:"active_provider_calls"`
	OutstandingWorkflows int64     `json:"outstanding_workflows"`
	ObservedAt           time.Time `json:"observed_at"`
}

// Snapshot reads DB authority, rather than summing process-local counters.
// Counts are sampled in one SQL statement (including the database timestamp).
func (s *Store) Snapshot(parent context.Context) (*Snapshot, error) {
	ctx, cancel := context.WithTimeout(parent, 3*time.Second)
	defer cancel()
	result := &Snapshot{}
	var policies []Policy
	if err := s.db.WithContext(ctx).Table("execution_capacity_policies").Order("scope").Find(&policies).Error; err != nil {
		return nil, err
	}
	err := s.db.WithContext(ctx).Raw(`SELECT
	 (SELECT count(*) FROM execution_active_capacity) AS active_runs,
	 (SELECT count(*) FROM execution_capacity_leases WHERE scope='provider' AND expires_at>clock_timestamp()) AS active_provider_calls,
	 (SELECT count(*) FROM workflow_runs WHERE definition IS NOT NULL AND deadline_at>clock_timestamp() AND status NOT IN ('succeeded','failed','canceled')) AS outstanding_workflows,
	 clock_timestamp() AS observed_at`).Scan(result).Error
	result.Policies = policies
	return result, err
}

// UpdatePolicy serializes against admission's SHARE lock. A shrink never revokes
// existing work. Lease duration changes apply to newly acquired leases only.
func (s *Store) UpdatePolicy(parent context.Context, scope string, p Policy) (*Policy, error) {
	if scope != "run" && scope != "provider" || p.Version < 1 || p.ActiveLimit < 1 || p.ActiveLimit > 1024 || p.OwnerActiveLimit < 1 || p.OwnerActiveLimit > p.ActiveLimit || p.QueueLimit < 1 || p.QueueLimit > 100000 || p.OwnerQueueLimit < 1 || p.OwnerQueueLimit > p.QueueLimit || p.LeaseMS < 1000 || p.LeaseMS > 60000 {
		return nil, ErrInvalidPolicy
	}
	ctx, cancel := context.WithTimeout(parent, 3*time.Second)
	defer cancel()
	p.Scope = scope
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		result := tx.Table("execution_capacity_policies").Where("scope=? AND version=?", scope, p.Version).Updates(map[string]any{
			"active_limit": p.ActiveLimit, "owner_active_limit": p.OwnerActiveLimit, "queue_limit": p.QueueLimit, "owner_queue_limit": p.OwnerQueueLimit, "lease_ms": p.LeaseMS, "version": gorm.Expr("version+1"), "updated_at": gorm.Expr("clock_timestamp()"),
		})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrPolicyConflict
		}
		p.Version++
		return nil
	})
	return &p, err
}
