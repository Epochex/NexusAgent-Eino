//go:build integration && infrastructure

package vector

import (
	"context"
	"crypto/rand"
	"github.com/chengpeng-cp/nexus-agent/internal/config"
	"github.com/chengpeng-cp/nexus-agent/internal/infra"
	"os"
	"strings"
	"testing"
	"time"
)

func TestMilvusScopePreventsForeignAndUnpublishedCrowding(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cli, err := infra.NewMilvus(ctx, &config.Milvus{Addr: os.Getenv("NX_TEST_MILVUS_ADDR")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cli.Close() })
	store := NewMilvusStore(cli)
	collection := "nexus_it_" + strings.ToLower(rand.Text())
	space := strings.Repeat("a", 64)
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		if err := store.DropCollection(cleanup, collection); err != nil {
			t.Error(err)
		}
	})
	if err = store.EnsureCollection(ctx, collection, 3); err != nil {
		t.Fatal(err)
	}
	record := func(id, user, kb, doc, version int64, vector []float32) Record {
		return Record{ID: id, Vector: vector, Metadata: map[string]any{"user_id": user, "kb_id": kb, "doc_id": doc, "index_version": version, "embedding_version": space}}
	}
	records := []Record{record(1, 7, 1, 1, 2, []float32{0.8, 0.2, 0}), record(2, 7, 1, 2, 1, []float32{1, 0, 0}), record(3, 8, 1, 1, 2, []float32{1, 0, 0}), record(4, 7, 2, 1, 2, []float32{1, 0, 0}), record(5, 7, 1, 1, 1, []float32{1, 0, 0})}
	if err = store.Upsert(ctx, collection, records); err != nil {
		t.Fatal(err)
	}
	hits, err := store.SearchScoped(ctx, collection, []float32{1, 0, 0}, 1, Scope{UserID: 7, KBID: 1, EmbeddingVersion: space, DocumentVersions: map[int64]int64{1: 2}})
	if err != nil || len(hits) != 1 || hits[0].ID != 1 {
		t.Fatalf("unauthorized/unpublished candidate crowded legitimate top1: %+v %v", hits, err)
	}
	if _, err = store.SearchScoped(ctx, collection, []float32{1, 0, 0}, 1, Scope{UserID: 7, KBID: 1, EmbeddingVersion: space}); err != nil {
		t.Fatal(err)
	}
	if _, err = store.SearchScoped(ctx, collection, []float32{1, 0, 0}, 1, Scope{UserID: 0, KBID: 1, EmbeddingVersion: space, DocumentVersions: map[int64]int64{1: 2}}); err == nil {
		t.Error("unscoped search accepted")
	}
}
