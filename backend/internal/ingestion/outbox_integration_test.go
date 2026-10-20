//go:build integration

package ingestion_test

import (
	"context"
	"testing"

	"github.com/chengpeng-cp/nexus-agent/internal/execution"
	"github.com/chengpeng-cp/nexus-agent/internal/testutil/testpg"
)

func TestDocumentOutboxSharesPublisherWithoutWorkflowID(t *testing.T) {
	db := testpg.Open(t)
	if err := db.Exec(`INSERT INTO execution_outbox(topic,payload,dedup_key) VALUES('document.index','{"job_id":1}', 'document-test')`).Error; err != nil {
		t.Fatal(err)
	}
	var delivered execution.Notification
	count, err := execution.NewStore(db).PublishOutbox(context.Background(), func(_ context.Context, n execution.Notification) error { delivered = n; return nil }, 4)
	if err != nil || count != 1 || delivered.RunID != 0 || delivered.Seq != 0 || delivered.Topic != "document.index" || delivered.ID <= 0 {
		t.Fatalf("document notification cannot publish: %d %+v %v", count, delivered, err)
	}
}
