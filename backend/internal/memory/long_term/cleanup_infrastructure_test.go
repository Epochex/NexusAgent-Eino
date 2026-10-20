//go:build integration && infrastructure

package long_term

import (
	"context"
	"crypto/sha256"
	"fmt"
	"github.com/chengpeng-cp/nexus-agent/internal/config"
	"github.com/chengpeng-cp/nexus-agent/internal/infra"
	"github.com/chengpeng-cp/nexus-agent/internal/model"
	"github.com/chengpeng-cp/nexus-agent/internal/vector"
	"os"
	"testing"
	"time"
)

func TestMemorySupersededMigrationCleansPreviousSpaces(t *testing.T) {
	if os.Getenv("NX_TEST_MILVUS_ADDR") == "" {
		t.Skip("NX_TEST_MILVUS_ADDR required")
	}
	for _, returnToFirst := range []bool{false, true} {
		t.Run(fmt.Sprint("return_to_first_", returnToFirst), func(t *testing.T) {
			store, db, owner, _, base := memoryFixture(t)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			client, err := infra.NewMilvus(ctx, &config.Milvus{Addr: os.Getenv("NX_TEST_MILVUS_ADDR")})
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			vectors := vector.NewMilvusStore(client)
			profile := func(label string) *Profile {
				copy := *base
				copy.EmbeddingVersion = fmt.Sprintf("%x", sha256.Sum256([]byte(t.Name()+label)))
				return &copy
			}
			a, b, c := profile("a"), profile("b"), profile("c")
			for _, p := range []*Profile{a, b} {
				if err = vectors.EnsureCollection(ctx, MemoryCollection(p.EmbeddingVersion), 3); err != nil {
					t.Fatal(err)
				}
			}
			defer func() {
				for _, p := range []*Profile{a, b} {
					cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
					err := vectors.DropCollection(cleanup, MemoryCollection(p.EmbeddingVersion))
					stop()
					if err != nil {
						t.Error(err)
					}
				}
			}()
			if _, err = store.RebuildProfile(ctx, a, "initial"); err != nil {
				t.Fatal(err)
			}
			memory := &model.Memory{UserID: owner, Content: "stable fact", EmbeddingVersion: a.EmbeddingVersion, IndexStatus: "ready", Revision: 1, IndexedRevision: 1}
			if err = db.Create(memory).Error; err != nil {
				t.Fatal(err)
			}
			put := func(p *Profile, revision int64) {
				t.Helper()
				if err := vectors.Upsert(ctx, MemoryCollection(p.EmbeddingVersion), []vector.Record{{ID: memory.ID, Vector: []float32{1, 0, 0}, Metadata: map[string]any{"user_id": owner, "agent_id": int64(0), "revision": revision, "embedding_version": p.EmbeddingVersion}}}); err != nil {
					t.Fatal(err)
				}
			}
			put(a, 1)
			if _, err = store.RebuildProfile(ctx, b, "switch-b"); err != nil {
				t.Fatal(err)
			}
			stale := claimMemory(t, store, "stale-migrator")
			put(b, 2) // Remote effect occurred, completion has not committed.
			current := c
			if returnToFirst {
				current = a
			}
			if _, err = store.RebuildProfile(ctx, current, "switch-current"); err != nil {
				t.Fatal(err)
			}
			if returnToFirst {
				put(a, 3)
			}
			manager := &Manager{repo: store, vectors: vectors}
			if err = manager.processVector(ctx, stale); err != nil {
				t.Fatal(err)
			}
			aCount, err := vectors.Count(ctx, MemoryCollection(a.EmbeddingVersion))
			if err != nil {
				t.Fatal(err)
			}
			bCount, err := vectors.Count(ctx, MemoryCollection(b.EmbeddingVersion))
			if err != nil {
				t.Fatal(err)
			}
			wantA := int64(0)
			if returnToFirst {
				wantA = 1
			}
			if aCount != wantA || bCount != 0 {
				t.Fatalf("superseded migration orphaned/deleted vectors: A=%d want %d B=%d", aCount, wantA, bCount)
			}
		})
	}
}
