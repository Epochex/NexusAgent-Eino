package a2a

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	sdk "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv/taskstore"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// TaskStore persists SDK task state and ownership. It never restarts an Agent
// after process loss; expired unfinished tasks become failed for inspection.
type TaskStore struct {
	db       *gorm.DB
	owner    func(context.Context) (int64, error)
	lifetime time.Duration
}
type storedTaskRow struct {
	ID                               string
	OwnerID                          int64
	ContextID, State                 string
	Version                          int64
	Task                             datatypes.JSON
	DeadlineAt, CreatedAt, UpdatedAt time.Time
}

func (storedTaskRow) TableName() string { return "a2a_tasks" }

var _ taskstore.Store = (*TaskStore)(nil)

func NewTaskStore(db *gorm.DB, owner func(context.Context) (int64, error), lifetime time.Duration) (*TaskStore, error) {
	if db == nil || owner == nil || lifetime < time.Second || lifetime > 5*time.Minute {
		return nil, fmt.Errorf("task store requires database, owner resolver and finite lifetime")
	}
	return &TaskStore{db: db, owner: owner, lifetime: lifetime}, nil
}
func (s *TaskStore) identity(ctx context.Context) (int64, error) {
	owner, err := s.owner(ctx)
	if err != nil {
		return 0, err
	}
	if owner < 1 {
		return 0, sdk.ErrUnauthenticated
	}
	return owner, nil
}
func validTaskState(state sdk.TaskState) bool {
	switch state {
	case sdk.TaskStateSubmitted, sdk.TaskStateWorking, sdk.TaskStateCompleted, sdk.TaskStateFailed, sdk.TaskStateCanceled, sdk.TaskStateRejected, sdk.TaskStateInputRequired, sdk.TaskStateAuthRequired:
		return true
	}
	return false
}
func encodeTask(task *sdk.Task) ([]byte, error) {
	if task == nil || task.ID == "" || len(task.ID) > 128 || task.ContextID == "" || len(task.ContextID) > 128 || !validTaskState(task.Status.State) || len(task.History) > 20 || len(task.Artifacts) > 32 {
		return nil, sdk.ErrInvalidParams
	}
	raw, err := json.Marshal(task)
	if err != nil || len(raw) > 250<<10 {
		return nil, fmt.Errorf("A2A task payload exceeds limit: %w", sdk.ErrInvalidParams)
	}
	return raw, nil
}
func decodeStored(row *storedTaskRow) (*taskstore.StoredTask, error) {
	var task sdk.Task
	if json.Unmarshal(row.Task, &task) != nil || string(task.ID) != row.ID || string(task.Status.State) != row.State {
		return nil, fmt.Errorf("stored A2A task is invalid")
	}
	return &taskstore.StoredTask{Task: &task, Version: taskstore.TaskVersion(row.Version), User: fmt.Sprint(row.OwnerID)}, nil
}
func (s *TaskStore) expire(ctx context.Context, owner int64) error {
	now := time.Now().UTC()
	status, _ := json.Marshal(sdk.TaskStatus{State: sdk.TaskStateFailed, Timestamp: &now, Message: sdk.NewMessage(sdk.MessageRoleAgent, sdk.NewTextPart("Execution expired or its process was interrupted; inspect effects before retrying."))})
	return s.db.WithContext(ctx).Exec("UPDATE a2a_tasks SET state=?,version=version+1,task=jsonb_set(task-'artifacts','{status}',?::jsonb),updated_at=now() WHERE owner_id=? AND deadline_at<=now() AND state IN ?", string(sdk.TaskStateFailed), string(status), owner, []string{string(sdk.TaskStateSubmitted), string(sdk.TaskStateWorking), string(sdk.TaskStateInputRequired), string(sdk.TaskStateAuthRequired)}).Error
}
func (s *TaskStore) Create(parent context.Context, task *sdk.Task) (taskstore.TaskVersion, error) {
	owner, err := s.identity(parent)
	if err != nil {
		return 0, err
	}
	raw, err := encodeTask(task)
	if err != nil {
		return 0, err
	}
	ctx, cancel := context.WithTimeout(parent, 3*time.Second)
	defer cancel()
	record := map[string]any{"id": string(task.ID), "owner_id": owner, "context_id": string(task.ContextID), "state": string(task.Status.State), "version": 1, "task": datatypes.JSON(raw), "deadline_at": gorm.Expr("now()+?*interval '1 millisecond'", s.lifetime.Milliseconds())}
	result := s.db.WithContext(ctx).Table("a2a_tasks").Clauses(clause.OnConflict{DoNothing: true}).Create(record)
	if result.Error != nil {
		return 0, result.Error
	}
	if result.RowsAffected != 1 {
		return 0, taskstore.ErrTaskAlreadyExists
	}
	return 1, nil
}
func (s *TaskStore) Update(parent context.Context, request *taskstore.UpdateRequest) (taskstore.TaskVersion, error) {
	if request == nil || request.PrevVersion < 1 {
		return 0, taskstore.ErrConcurrentModification
	}
	owner, err := s.identity(parent)
	if err != nil {
		return 0, err
	}
	raw, err := encodeTask(request.Task)
	if err != nil {
		return 0, err
	}
	ctx, cancel := context.WithTimeout(parent, 3*time.Second)
	defer cancel()
	if err = s.expire(ctx, owner); err != nil {
		return 0, err
	}
	var version taskstore.TaskVersion
	err = s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var row storedTaskRow
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id=? AND owner_id=?", string(request.Task.ID), owner).First(&row).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return sdk.ErrTaskNotFound
			}
			return err
		}
		if row.Version != int64(request.PrevVersion) {
			return taskstore.ErrConcurrentModification
		}
		if sdk.TaskState(row.State).Terminal() {
			if row.State != string(request.Task.Status.State) {
				return taskstore.ErrConcurrentModification
			}
			version = taskstore.TaskVersion(row.Version)
			return nil // terminal snapshots are immutable
		}
		if row.ContextID != string(request.Task.ContextID) {
			return sdk.ErrInvalidParams
		}
		// now() is the transaction start time and can predate a row-lock wait.
		// The fence must use the database clock at the actual write.
		result := tx.Model(&storedTaskRow{}).Where("id=? AND owner_id=? AND version=? AND deadline_at>clock_timestamp()", row.ID, owner, row.Version).Updates(map[string]any{"state": string(request.Task.Status.State), "task": datatypes.JSON(raw), "version": row.Version + 1, "updated_at": gorm.Expr("now()")})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return taskstore.ErrConcurrentModification
		}
		version = taskstore.TaskVersion(row.Version + 1)
		return nil
	})
	return version, err
}
func (s *TaskStore) Get(parent context.Context, id sdk.TaskID) (*taskstore.StoredTask, error) {
	if id == "" || len(id) > 128 {
		return nil, sdk.ErrTaskNotFound
	}
	owner, err := s.identity(parent)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(parent, 3*time.Second)
	defer cancel()
	if err = s.expire(ctx, owner); err != nil {
		return nil, err
	}
	var row storedTaskRow
	if err = s.db.WithContext(ctx).Where("id=? AND owner_id=?", string(id), owner).First(&row).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, sdk.ErrTaskNotFound
		}
		return nil, err
	}
	return decodeStored(&row)
}

type taskCursor struct {
	Updated time.Time `json:"updated"`
	ID      string    `json:"id"`
}

func (s *TaskStore) List(parent context.Context, request *sdk.ListTasksRequest) (*sdk.ListTasksResponse, error) {
	owner, err := s.identity(parent)
	if err != nil {
		return nil, err
	}
	if request == nil {
		request = &sdk.ListTasksRequest{}
	}
	if request.PageSize < 0 || request.PageSize > 100 || len(request.PageToken) > 512 || len(request.ContextID) > 128 || request.Status != sdk.TaskStateUnspecified && !validTaskState(request.Status) {
		return nil, sdk.ErrInvalidParams
	}
	limit := request.PageSize
	if limit == 0 || limit > 20 {
		limit = 20
	}
	history := 20
	if request.HistoryLength != nil {
		if *request.HistoryLength < 0 {
			return nil, sdk.ErrInvalidParams
		}
		history = min(20, *request.HistoryLength)
	}
	ctx, cancel := context.WithTimeout(parent, 3*time.Second)
	defer cancel()
	if err = s.expire(ctx, owner); err != nil {
		return nil, err
	}
	query := s.db.WithContext(ctx).Model(&storedTaskRow{}).Where("owner_id=?", owner)
	if request.ContextID != "" {
		query = query.Where("context_id=?", string(request.ContextID))
	}
	if request.Status != sdk.TaskStateUnspecified {
		query = query.Where("state=?", string(request.Status))
	}
	if request.StatusTimestampAfter != nil {
		query = query.Where("updated_at>?", *request.StatusTimestampAfter)
	}
	var total int64
	if err = query.Count(&total).Error; err != nil {
		return nil, err
	}
	if request.PageToken != "" {
		var cursor taskCursor
		raw, e := base64.RawURLEncoding.DecodeString(request.PageToken)
		if e != nil || json.Unmarshal(raw, &cursor) != nil || cursor.Updated.IsZero() || cursor.ID == "" || len(cursor.ID) > 128 {
			return nil, sdk.ErrInvalidParams
		}
		query = query.Where("(updated_at,id)<(?,?)", cursor.Updated, cursor.ID)
	}
	var rows []storedTaskRow
	if err = query.Order("updated_at DESC,id DESC").Limit(limit + 1).Find(&rows).Error; err != nil {
		return nil, err
	}
	next := ""
	if len(rows) > limit {
		rows = rows[:limit]
		last := rows[len(rows)-1]
		raw, _ := json.Marshal(taskCursor{Updated: last.UpdatedAt, ID: last.ID})
		next = base64.RawURLEncoding.EncodeToString(raw)
	}
	result := &sdk.ListTasksResponse{TotalSize: int(total), PageSize: limit, NextPageToken: next, Tasks: []*sdk.Task{}}
	for i := range rows {
		stored, e := decodeStored(&rows[i])
		if e != nil {
			return nil, e
		}
		task := stored.Task
		if !request.IncludeArtifacts {
			task.Artifacts = nil
		}
		if history < len(task.History) {
			task.History = task.History[len(task.History)-history:]
		}
		result.Tasks = append(result.Tasks, task)
	}
	if raw, e := json.Marshal(result); e != nil || len(raw) > 1<<20 {
		return nil, fmt.Errorf("reduce A2A task page size or history: %w", sdk.ErrInvalidParams)
	}
	return result, nil
}
