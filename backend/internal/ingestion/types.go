// Package ingestion owns versioned document work and publication in PostgreSQL.
package ingestion

import (
	"context"
	"errors"
	"time"
)

const ProcessorVersion = "loader-v2/rune-v2/embed-v1"
const MaxSourceBytes = 20 << 20

var ErrLeaseLost = errors.New("document job lease lost")
var ErrNotFound = errors.New("document job/source not found")
var ErrConflict = errors.New("document generation changed")
var ErrInvalidInput = errors.New("invalid document job input")
var ErrPermanent = errors.New("document processing requires intervention")

func Permanent(err error) error { return errors.Join(ErrPermanent, err) }

type Job struct {
	ID               int64
	DocumentID       int64
	OwnerID          int64
	KBID             int64
	IndexVersion     int64
	EmbeddingVersion string
	ProcessorVersion string
	Kind             string
	Status           string
	Generation       int64
	Attempts         int
	MaxAttempts      int
	EpochAttempts    int
	RetryEpoch       int
	LeaseOwner       string
	LeaseUntil       *time.Time
	AvailableAt      time.Time
	DeadlineAt       time.Time
	Error            string
	CreatedAt        time.Time
	UpdatedAt        time.Time
}

func (Job) TableName() string { return "document_jobs" }

type Stats struct {
	ChunkCount, TokenCount int
	Warnings               []string
}
type jobKey struct{}

func WithJob(ctx context.Context, job *Job) context.Context {
	return context.WithValue(ctx, jobKey{}, job)
}
func JobFromContext(ctx context.Context) (*Job, bool) {
	job, ok := ctx.Value(jobKey{}).(*Job)
	return job, ok
}
