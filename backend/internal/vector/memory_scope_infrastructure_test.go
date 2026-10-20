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

func TestMemoryScopeExcludesOtherAgentsAndOldRevisions(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cli, err := infra.NewMilvus(ctx, &config.Milvus{Addr: os.Getenv("NX_TEST_MILVUS_ADDR")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cli.Close() })
	store := NewMilvusStore(cli)
	collection := "mem_it_" + strings.ToLower(rand.Text())
	space := strings.Repeat("b", 64)
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
	record := func(id, user, agent, revision int64, vec []float32) Record {
		return Record{ID: id, Vector: vec, Metadata: map[string]any{"user_id": user, "agent_id": agent, "revision": revision, "embedding_version": space}}
	}
	records := []Record{record(1, 7, 1, 2, []float32{0.8, 0.2, 0}), record(2, 7, 2, 1, []float32{1, 0, 0}), record(3, 8, 1, 2, []float32{1, 0, 0}), record(4, 7, 1, 1, []float32{1, 0, 0}), record(5, 7, 0, 1, []float32{0.7, 0.3, 0})}
	if err = store.Upsert(ctx, collection, records); err != nil {
		t.Fatal(err)
	}
	agent := int64(1)
	scope := Scope{UserID: 7, AgentID: &agent, EmbeddingVersion: space, MemoryRevisions: map[int64]int64{1: 2, 2: 1, 3: 2, 4: 2, 5: 1}}
	hits, err := store.SearchScoped(ctx, collection, []float32{1, 0, 0}, 1, scope)
	if err != nil || len(hits) != 1 || hits[0].ID != 1 {
		t.Fatalf("other-agent/owner/stale record crowded legitimate memory %+v %v", hits, err)
	}
	scope.AgentID = nil
	hits, err = store.SearchScoped(ctx, collection, []float32{1, 0, 0}, 3, scope)
	if err != nil || len(hits) != 1 || hits[0].ID != 5 {
		t.Fatalf("global scope included agent-private memories %+v %v", hits, err)
	}
}
