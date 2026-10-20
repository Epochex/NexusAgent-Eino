// Package execution implements PostgreSQL-authoritative durable work.
package execution

import (
	"errors"
	"gorm.io/datatypes"
	"time"
)

const (
	Queued         = "queued"
	Running        = "running"
	Waiting        = "waiting_approval"
	RetryWait      = "retry_wait"
	Succeeded      = "succeeded"
	Failed         = "failed"
	Canceled       = "canceled"
	NeedsAttention = "needs_attention"
	EngineVersion  = "eino-v0.9.22"
	GraphSchema    = 1
)

var (
	ErrLeaseLost       = errors.New("execution lease lost")
	ErrNotFound        = errors.New("execution not found")
	ErrConflict        = errors.New("execution state or arguments changed")
	ErrVersionMismatch = errors.New("checkpoint definition or runtime version mismatch")
	ErrCursorExpired   = errors.New("event cursor expired; fetch run snapshot")
	ErrInvalidInput    = errors.New("invalid execution input")
)

type Run struct {
	ID             int64          `json:"id"`
	OwnerID        int64          `json:"owner_id"`
	WorkflowID     int64          `json:"workflow_id"`
	Definition     datatypes.JSON `json:"-"`
	DefinitionHash string         `json:"definition_hash"`
	EngineVersion  string         `json:"engine_version"`
	GraphSchema    int            `json:"graph_schema"`
	Status         string         `json:"status"`
	Input          datatypes.JSON `json:"input"`
	Output         datatypes.JSON `json:"output,omitempty"`
	Error          string         `json:"error,omitempty"`
	StateVersion   int64          `json:"state_version"`
	Generation     int64          `json:"generation"`
	LeaseOwner     string         `json:"-"`
	LeaseUntil     *time.Time     `json:"-"`
	DeadlineAt     time.Time      `json:"deadline_at"`
	AvailableAt    time.Time      `json:"available_at"`
	PendingIDs     datatypes.JSON `json:"-"`
	NextSeq        int64          `json:"last_seq"`
	CreatedAt      time.Time      `json:"created_at"`
	StartedAt      *time.Time     `json:"started_at,omitempty"`
	FinishedAt     *time.Time     `json:"finished_at,omitempty"`
	Approvals      []Approval     `gorm:"-" json:"approvals,omitempty"`
}

func (Run) TableName() string { return "workflow_runs" }

type Approval struct {
	ID          int64      `json:"id"`
	RunID       int64      `json:"run_id"`
	InterruptID string     `json:"interrupt_id"`
	Path        string     `json:"path"`
	ArgsHash    string     `json:"args_hash"`
	Prompt      string     `json:"prompt"`
	Decision    string     `json:"decision"`
	DecidedBy   *int64     `json:"decided_by,omitempty"`
	DecidedAt   *time.Time `json:"decided_at,omitempty"`
	ExpiresAt   time.Time  `json:"expires_at"`
}

func (Approval) TableName() string { return "execution_approvals" }

type Event struct {
	RunID         int64          `json:"run_id"`
	Seq           int64          `json:"seq"`
	Type          string         `json:"type"`
	SchemaVersion int            `json:"schema_version"`
	Data          datatypes.JSON `json:"data"`
	CreatedAt     time.Time      `json:"time"`
}

func (Event) TableName() string { return "execution_events" }
func terminal(s string) bool {
	return s == Succeeded || s == Failed || s == Canceled
}
