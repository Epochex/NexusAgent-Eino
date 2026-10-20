//go:build integration

package a2a

import (
	"context"
	"errors"
	"testing"
	"time"

	sdk "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
	"github.com/a2aproject/a2a-go/v2/a2asrv/taskstore"
	"github.com/chengpeng-cp/nexus-agent/internal/model"
	"github.com/chengpeng-cp/nexus-agent/internal/security"
	"github.com/chengpeng-cp/nexus-agent/internal/testutil/testpg"
	"gorm.io/gorm/clause"
)

func TestA2ATaskStoreOwnerCASAndExpiredExecution(t *testing.T) {
	db := testpg.Open(t)
	owner := model.User{Username: "a2a-owner", Email: "a2a-owner@test.invalid", PasswordHash: "test"}
	other := model.User{Username: "a2a-other", Email: "a2a-other@test.invalid", PasswordHash: "test"}
	if err := db.Create(&owner).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&other).Error; err != nil {
		t.Fatal(err)
	}
	store, err := NewTaskStore(db, func(ctx context.Context) (int64, error) {
		if id, ok := security.UserIDFromContext(ctx); ok {
			return id, nil
		}
		return 0, sdk.ErrUnauthenticated
	}, 150*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	ctx := security.WithUserID(context.Background(), owner.ID)
	foreign := security.WithUserID(context.Background(), other.ID)
	now := time.Now().UTC()
	task := &sdk.Task{ID: "task-cas", ContextID: "context-owner", Status: sdk.TaskStatus{State: sdk.TaskStateSubmitted, Timestamp: &now}}
	version, err := store.Create(ctx, task)
	if err != nil || version != 1 {
		t.Fatal("create", version, err)
	}
	if _, err = store.Get(foreign, task.ID); !errors.Is(err, sdk.ErrTaskNotFound) {
		t.Fatal("foreign task visible", err)
	}
	task.Status.State = sdk.TaskStateWorking
	next, err := store.Update(ctx, &taskstore.UpdateRequest{Task: task, PrevVersion: version})
	if err != nil || next != 2 {
		t.Fatal("working", next, err)
	}
	if _, err = store.Update(ctx, &taskstore.UpdateRequest{Task: task, PrevVersion: version}); !errors.Is(err, taskstore.ErrConcurrentModification) {
		t.Fatal("stale update accepted", err)
	}
	task.Status.State = sdk.TaskStateCanceled
	canceled, err := store.Update(ctx, &taskstore.UpdateRequest{Task: task, PrevVersion: next})
	if err != nil {
		t.Fatal(err)
	}
	task.Status.State = sdk.TaskStateCompleted
	if _, err = store.Update(ctx, &taskstore.UpdateRequest{Task: task, PrevVersion: canceled}); err == nil {
		t.Fatal("canceled task revived as completed")
	}
	lost := &sdk.Task{ID: "task-lost", ContextID: "context-owner", Status: sdk.TaskStatus{State: sdk.TaskStateWorking, Timestamp: &now}}
	if _, err = store.Create(ctx, lost); err != nil {
		t.Fatal(err)
	}
	if err = db.Exec("UPDATE a2a_tasks SET deadline_at=now()-interval '1 second' WHERE id=?", string(lost.ID)).Error; err != nil {
		t.Fatal(err)
	}
	expired, err := store.Get(ctx, lost.ID)
	if err != nil || expired.Task.Status.State != sdk.TaskStateFailed || expired.Version != 2 {
		t.Fatal("lost execution reported success or kept running", expired, err)
	}
	lost.Status.State = sdk.TaskStateCompleted
	if _, err = store.Update(ctx, &taskstore.UpdateRequest{Task: lost, PrevVersion: 1}); err == nil {
		t.Fatal("late orphan result published")
	}
	page, err := store.List(ctx, &sdk.ListTasksRequest{PageSize: 1})
	if err != nil || page.TotalSize != 2 || len(page.Tasks) != 1 || page.NextPageToken == "" {
		t.Fatal("owner pagination", page, err)
	}
	second, err := store.List(ctx, &sdk.ListTasksRequest{PageSize: 1, PageToken: page.NextPageToken})
	if err != nil || len(second.Tasks) != 1 || second.Tasks[0].ID == page.Tasks[0].ID {
		t.Fatal("pagination repeated/lost task", second, err)
	}
	hidden, err := store.List(foreign, &sdk.ListTasksRequest{})
	if err != nil || hidden.TotalSize != 0 || len(hidden.Tasks) != 0 {
		t.Fatal("foreign list exposed task", hidden, err)
	}
	handler := a2asrv.NewHandler(protocolExecutor{}, a2asrv.WithTaskStore(store))
	response, err := handler.SendMessage(ctx, &sdk.SendMessageRequest{Message: sdk.NewMessage(sdk.MessageRoleUser, sdk.NewTextPart("Inspect actual stored task"))})
	if err != nil {
		t.Fatal("official SDK could not persist execution", err)
	}
	saved, ok := response.(*sdk.Task)
	if !ok || saved.Status.State != sdk.TaskStateCompleted {
		t.Fatal("actual SDK task not completed", response)
	}
	stored, err := store.Get(ctx, saved.ID)
	if err != nil || stored.Task.Status.State != sdk.TaskStateCompleted || len(stored.Task.Artifacts) != 1 {
		t.Fatal("SDK completed task did not survive store reload", stored, err)
	}
	if _, err = handler.GetTask(foreign, &sdk.GetTaskRequest{ID: saved.ID}); !errors.Is(err, sdk.ErrTaskNotFound) {
		t.Fatal("foreign SDK query exposed actual task", err)
	}
}

func TestA2ATaskUpdateBlockedPastDeadlineCannotComplete(t *testing.T) {
	db := testpg.Open(t)
	owner := model.User{Username: "deadline-owner", Email: "deadline@test.invalid", PasswordHash: "test"}
	if err := db.Create(&owner).Error; err != nil {
		t.Fatal(err)
	}
	store, err := NewTaskStore(db, func(context.Context) (int64, error) { return owner.ID, nil }, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	task := &sdk.Task{ID: "deadline-race", ContextID: "deadline-context", Status: sdk.TaskStatus{State: sdk.TaskStateWorking}}
	v, err := store.Create(ctx, task)
	if err != nil {
		t.Fatal(err)
	}
	tx := db.Begin()
	defer tx.Rollback()
	var locked storedTaskRow
	if err = tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id=?", string(task.ID)).First(&locked).Error; err != nil {
		t.Fatal(err)
	}
	task.Status.State = sdk.TaskStateCompleted
	done := make(chan error, 1)
	go func() { _, err := store.Update(ctx, &taskstore.UpdateRequest{Task: task, PrevVersion: v}); done <- err }()
	time.Sleep(time.Until(locked.DeadlineAt) + 100*time.Millisecond)
	if err = tx.Commit().Error; err != nil {
		t.Fatal(err)
	}
	if err = <-done; err == nil {
		t.Fatal("task blocked past its deadline committed completion")
	}
	current, err := store.Get(ctx, task.ID)
	if err != nil || current.Task.Status.State != sdk.TaskStateFailed {
		t.Fatal("expired task did not fail", current, err)
	}
}
