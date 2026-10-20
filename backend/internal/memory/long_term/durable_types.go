package long_term

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/chengpeng-cp/nexus-agent/internal/llm"
	"gorm.io/datatypes"
	"time"
	"unicode/utf8"
)

const MemoryProcessorVersion = "memory-v2/ops-v1"
const MemoryVectorSchemaVersion = "memory-v2"

type ModelResolver interface {
	EnsureMemoryProfile(context.Context, int64) (*Profile, error)
	CaptureMemoryExtractor(context.Context) (json.RawMessage, error)
	ResolveMemoryEmbedding(context.Context, *Profile) (*llm.ModelDescriptor, error)
	ResolveMemoryExtractor(context.Context, json.RawMessage) (*llm.ModelDescriptor, error)
}

var ErrMemoryLeaseLost = errors.New("memory job lease lost")
var ErrMemoryConflict = errors.New("memory revision or source changed")
var ErrMemoryNotFound = errors.New("memory source not found")
var ErrMemoryInvalid = errors.New("invalid memory request")
var ErrMemoryPermanent = errors.New("memory processing requires intervention")

func PermanentMemory(err error) error { return errors.Join(ErrMemoryPermanent, err) }

type Profile struct {
	UserID             int64          `gorm:"primaryKey" json:"user_id"`
	EmbeddingConfigID  int64          `json:"embedding_config_id"`
	EmbeddingVersion   string         `gorm:"primaryKey" json:"embedding_version"`
	EmbeddingDimension int            `json:"embedding_dimension"`
	EmbeddingSnapshot  datatypes.JSON `json:"-"`
	CreatedAt          time.Time      `json:"created_at"`
	Active             bool           `gorm:"default:true" json:"active"`
}

func (Profile) TableName() string { return "memory_profiles" }
func MemoryCollection(version string) string {
	if version == "" {
		return "memories"
	}
	if len(version) > 32 {
		version = version[:32]
	}
	return "mem_" + version
}

type Job struct {
	ID               int64          `gorm:"primaryKey" json:"id"`
	Kind             string         `json:"kind"`
	OwnerID          int64          `json:"-"`
	AgentID          *int64         `json:"agent_id"`
	SessionID        *string        `json:"session_id,omitempty"`
	ThroughMessageID int64          `json:"through_message_id,omitempty"`
	ExtractorVersion string         `json:"extractor_version,omitempty"`
	MemoryID         *int64         `json:"memory_id,omitempty"`
	MemoryRevision   int64          `json:"memory_revision,omitempty"`
	VectorAction     string         `json:"vector_action,omitempty"`
	EmbeddingVersion string         `json:"embedding_version"`
	ProcessorVersion string         `json:"processor_version"`
	Payload          datatypes.JSON `json:"-"`
	Decision         datatypes.JSON `json:"-"`
	Facts            datatypes.JSON `json:"-"`
	DecisionHash     string         `json:"-"`
	Warnings         datatypes.JSON `json:"warnings"`
	ParentJobID      *int64         `json:"parent_job_id,omitempty"`
	RebuildID        *int64         `json:"rebuild_id,omitempty"`
	Status           string         `json:"status"`
	Generation       int64          `json:"generation"`
	Attempts         int            `json:"attempts"`
	EpochAttempts    int            `json:"epoch_attempts"`
	RetryEpoch       int            `json:"retry_epoch"`
	MaxAttempts      int            `json:"max_attempts"`
	LeaseOwner       string         `json:"-"`
	LeaseUntil       *time.Time     `json:"lease_until,omitempty"`
	AvailableAt      time.Time      `json:"available_at"`
	DeadlineAt       time.Time      `json:"deadline_at"`
	CreatedAt        time.Time      `json:"created_at"`
	UpdatedAt        time.Time      `json:"updated_at"`
	Error            string         `json:"error,omitempty"`
}

func (Job) TableName() string { return "memory_jobs" }

type SourceMessage struct {
	ID            int64
	Role, Content string
}
type Source struct {
	Messages  []SourceMessage
	Extractor json.RawMessage
	Facts     []string `json:"-"`
}
type EnqueueInput struct {
	OwnerID            int64
	SessionID          string
	Force              bool
	Every, MaxAttempts int
	Extractor          json.RawMessage
}
type Operation struct {
	Op      string `json:"op"`
	ID      int64  `json:"id,omitempty"`
	Content string `json:"content,omitempty"`
}
type Decision struct {
	Ops        []Operation     `json:"ops"`
	Candidates map[int64]int64 `json:"candidates"`
}
type AppliedOperation struct {
	ID             int64
	JobID          int64
	OpIndex        int
	PayloadHash    string
	Op             string
	MemoryID       *int64
	ResultRevision int64
	CreatedAt      time.Time
}

func (AppliedOperation) TableName() string { return "memory_operations" }
func boundedMemory(message string) string {
	if len(message) <= 512 {
		return message
	}
	end := 512
	for end > 0 && !utf8.ValidString(message[:end]) {
		end--
	}
	return message[:end]
}
func objectJSON(raw []byte) bool {
	if len(raw) == 0 || len(raw) > 16384 {
		return false
	}
	var value map[string]any
	return json.Unmarshal(raw, &value) == nil && value != nil
}
func memoryNoticePayload(job *Job) ([]byte, error) {
	return json.Marshal(map[string]any{"job_id": job.ID, "kind": job.Kind, "generation": job.Generation, "status": job.Status, "memory_id": job.MemoryID, "memory_revision": job.MemoryRevision})
}
func noticeKey(job *Job, topic string) string {
	return fmt.Sprintf("memory:%d:%d:%s", job.ID, job.Generation, topic)
}
