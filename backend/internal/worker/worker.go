// Package worker executes leased PostgreSQL document jobs. Redis is a wake-up hint.
package worker

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chengpeng-cp/nexus-agent/internal/config"
	"github.com/chengpeng-cp/nexus-agent/internal/ingestion"
	"github.com/chengpeng-cp/nexus-agent/internal/model"
	"github.com/chengpeng-cp/nexus-agent/internal/rag/engine"
	"github.com/chengpeng-cp/nexus-agent/internal/rag/loader"
	"github.com/chengpeng-cp/nexus-agent/internal/repo"
	"github.com/chengpeng-cp/nexus-agent/internal/security"
	"github.com/minio/minio-go/v7"
	"github.com/redis/go-redis/v9"
)

const (
	StreamDocETL = "stream:doc-etl"
	StreamEval   = "stream:eval"
	DLQDocETL    = "stream:doc-etl:dlq"
	GroupDocETL  = "g-docetl"
	GroupEval    = "g-eval"
)

// Publisher 任务投递者（实现 service.StreamPublisher）。
type Publisher struct {
	rdb *redis.Client
}

// NewPublisher 构造。
func NewPublisher(rdb *redis.Client) *Publisher { return &Publisher{rdb: rdb} }

// PublishDocETL 投递文档 ETL 任务。
func (p *Publisher) PublishDocETL(ctx context.Context, docID int64) error {
	return p.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: StreamDocETL,
		Values: map[string]any{"doc_id": strconv.FormatInt(docID, 10)},
	}).Err()
}

// PublishEvalRun 投递评估任务。
func (p *Publisher) PublishEvalRun(ctx context.Context, runID int64) error {
	return p.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: StreamEval,
		Values: map[string]any{"run_id": strconv.FormatInt(runID, 10)},
	}).Err()
}

type Worker struct {
	repos   *repo.Repos
	engine  *engine.Engine
	minio   *minio.Client
	bucket  string
	cfg     config.Worker
	wake    chan struct{}
	running atomic.Bool
}

func New(_ *redis.Client, repos *repo.Repos, eng *engine.Engine, mc *minio.Client, bucket string, cfg *config.Worker) *Worker {
	w := &Worker{repos: repos, engine: eng, minio: mc, bucket: bucket, wake: make(chan struct{}, 1)}
	if cfg != nil {
		w.cfg = *cfg
	}
	return w
}
func (w *Worker) Notify() {
	select {
	case w.wake <- struct{}{}:
	default:
	}
}
func (w *Worker) Run(parent context.Context) error {
	if !w.running.CompareAndSwap(false, true) {
		return fmt.Errorf("document worker already running")
	}
	defer w.running.Store(false)
	if w.repos == nil || w.repos.DB == nil || w.engine == nil || w.minio == nil || w.bucket == "" {
		return fmt.Errorf("document worker dependencies missing")
	}
	concurrency := w.cfg.Concurrency
	if concurrency == 0 {
		concurrency = 4
	}
	if concurrency < 1 || concurrency > 16 {
		return fmt.Errorf("document concurrency must be 1..16")
	}
	hostname, _ := os.Hostname()
	owner := fmt.Sprintf("%s-%d-%s", hostname, os.Getpid(), rand.Text())
	store := ingestion.NewStore(w.repos.DB)
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	slots := make(chan struct{}, concurrency)
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	var jobs sync.WaitGroup
	w.Notify()
	for {
		select {
		case <-ctx.Done():
			drained := make(chan struct{})
			go func() { jobs.Wait(); close(drained) }()
			select {
			case <-drained:
				return nil
			case <-time.After(5 * time.Second):
				return fmt.Errorf("document worker shutdown exceeded 5s")
			}
		case <-ticker.C:
		case <-w.wake:
		}
		for ctx.Err() == nil && len(slots) < cap(slots) {
			slots <- struct{}{}
			job, err := store.Claim(ctx, owner, 15*time.Second)
			if err != nil || job == nil {
				<-slots
				if err != nil && ctx.Err() == nil {
					log.Printf("document claim: %v", err)
				}
				break
			}
			jobs.Add(1)
			go func() { defer jobs.Done(); defer func() { <-slots; w.Notify() }(); w.execute(ctx, store, job) }()
		}
	}
}
func (w *Worker) execute(parent context.Context, store *ingestion.Store, job *ingestion.Job) {
	deadline := minTime(job.DeadlineAt, time.Now().Add(5*time.Minute))
	ctx, cancel := context.WithDeadline(parent, deadline)
	// Completion updates its Job through GORM; renewal owns an immutable lease
	// identity rather than reading fields that completion may change.
	leaseIdentity := *job
	heartbeat := make(chan struct{})
	go func() {
		defer close(heartbeat)
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := store.Renew(ctx, &leaseIdentity, 15*time.Second); err != nil {
					cancel()
					return
				}
			}
		}
	}()
	err := w.process(ctx, store, job)
	cancel()
	<-heartbeat
	if err != nil && parent.Err() == nil && !errors.Is(err, ingestion.ErrLeaseLost) {
		final, stop := context.WithTimeout(context.Background(), 3*time.Second)
		defer stop()
		if persist := store.Fail(final, job, err); persist != nil && !errors.Is(persist, ingestion.ErrLeaseLost) {
			log.Printf("document job=%d finalize: %v", job.ID, persist)
		}
	}
}
func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

func (w *Worker) process(ctx context.Context, store *ingestion.Store, job *ingestion.Job) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = ingestion.Permanent(fmt.Errorf("document processor panic"))
		}
	}()
	unlock, err := ingestion.LockSource(ctx, w.repos.DB, job.DocumentID)
	if err != nil {
		return err
	}
	defer unlock()
	doc, kb, err := store.Source(ctx, job)
	if err != nil {
		return err
	}
	ctx = ingestion.WithJob(security.WithUserID(ctx, job.OwnerID), job)
	if job.Kind == "delete" {
		if err = w.engine.DeleteDocumentVectors(ctx, kb, doc.ID); err != nil {
			return err
		}
		if doc.ObjectKey != "" {
			if err = w.minio.RemoveObject(ctx, w.bucket, doc.ObjectKey, minio.RemoveObjectOptions{}); err != nil {
				return err
			}
		}
		return store.CompleteDelete(ctx, job)
	}
	if err = store.Stage(ctx, job, model.DocStatusParsing, nil); err != nil {
		return err
	}
	data, err := w.getObject(ctx, doc.ObjectKey)
	if err != nil {
		return fmt.Errorf("read document source: %w", err)
	}
	sum := sha256.Sum256(data)
	if hex.EncodeToString(sum[:]) != doc.ContentHash || int64(len(data)) != doc.FileSize {
		return ingestion.Permanent(fmt.Errorf("document source integrity mismatch"))
	}
	loaded, err := loader.Load(doc.Filename, data)
	if err != nil {
		return ingestion.Permanent(err)
	}
	if err = store.Stage(ctx, job, model.DocStatusChunking, loaded.Warnings); err != nil {
		return err
	}
	index := w.engine.WithIndexProgress(func(call context.Context) error { return store.Stage(call, job, model.DocStatusEmbedding, nil) })
	if err = index.IndexDocument(ctx, kb, doc, loaded); err != nil {
		return err
	}
	return store.Complete(ctx, job, ingestion.Stats{ChunkCount: doc.ChunkCount, TokenCount: doc.TokenCount, Warnings: loaded.Warnings})
}
func (w *Worker) getObject(ctx context.Context, key string) ([]byte, error) {
	object, err := w.minio.GetObject(ctx, w.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	defer object.Close()
	data, err := io.ReadAll(io.LimitReader(object, ingestion.MaxSourceBytes+1))
	if err != nil {
		return nil, err
	}
	if len(data) > ingestion.MaxSourceBytes {
		return nil, ingestion.Permanent(fmt.Errorf("source exceeds size bound"))
	}
	return data, nil
}
