//go:build integration && infrastructure

package vector

import (
	"context"
	"crypto/rand"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/chengpeng-cp/nexus-agent/internal/config"
	"github.com/chengpeng-cp/nexus-agent/internal/infra"
)

func TestMilvusUpsertAndDimensionContract(t *testing.T) {
	address := os.Getenv("NX_TEST_MILVUS_ADDR")
	if address == "" {
		t.Fatal("explicit NX_TEST_MILVUS_ADDR required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	client, err := infra.NewMilvus(ctx, &config.Milvus{Addr: address})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close() })
	store := NewMilvusStore(client)
	collection := "nexus_it_" + strings.ToLower(rand.Text())
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		if err := store.DropCollection(cleanup, collection); err != nil {
			t.Error("owned test collection cleanup: ", err)
		}
	})
	if err = store.EnsureCollection(ctx, collection, 3); err != nil {
		t.Fatal(err)
	}
	record := Record{ID: 1, Vector: []float32{1, 0, 0}, Metadata: map[string]any{"kb_id": int64(1), "user_id": int64(7), "doc_id": int64(1), "seq": int32(0), "page": int32(1), "index_version": int64(1), "embedding_version": strings.Repeat("a", 64), "content": "first"}}
	if err = store.Upsert(ctx, collection, []Record{record}); err != nil {
		t.Fatal(err)
	}
	record.Vector = []float32{0, 1, 0}
	record.Metadata["content"] = "replacement"
	if err = store.Upsert(ctx, collection, []Record{record}); err != nil {
		t.Fatal(err)
	}
	count, err := store.Count(ctx, collection)
	if err != nil || count != 1 {
		t.Errorf("retry duplicated logical vector count=%d err=%v", count, err)
	}
	hits, err := store.Search(ctx, collection, record.Vector, 4)
	if err != nil || len(hits) != 1 || hits[0].ID != 1 || hits[0].Score < 0.99 {
		t.Errorf("replacement not immediately readable %+v %v", hits, err)
	}
	if err = store.EnsureCollection(ctx, collection, 4); err == nil {
		t.Error("existing collection accepted a different dimension")
	}
}
