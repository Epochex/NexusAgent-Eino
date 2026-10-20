//go:build integration

package long_term

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func TestMemoryFactsCheckpointAndCanonicalSecretFreeSource(t *testing.T) {
	store, _, owner, session, _ := memoryFixture(t)
	memoryMessages(t, store.db, session.ID, 2)
	first, _, err := enqueueMemory(t, store, owner, session.ID, true)
	if err != nil {
		t.Fatal(err)
	}
	alternate := EnqueueInput{OwnerID: owner, SessionID: session.ID, Force: true, Every: 6, Extractor: json.RawMessage(`{"descriptor":{"APIKey":"private-fixture-value","ModelName":"test"},"config_id":1,"encrypted_key":"private-fixture-value"}`)}
	duplicate, created, err := store.Enqueue(context.Background(), alternate)
	if err != nil || created || duplicate.ID != first.ID {
		t.Fatal("semantic snapshot/source did not coalesce", err)
	}
	if strings.Contains(string(duplicate.Payload), "private-fixture-value") {
		t.Fatal("credential persisted in source snapshot")
	}
	job := claimMemory(t, store, "facts")
	if err = store.SaveFacts(context.Background(), job, []string{" one ", "one", "two"}); err != nil {
		t.Fatal(err)
	}
	source, _, err := store.Source(context.Background(), job)
	if err != nil || len(source.Facts) != 2 || source.Facts[0] != "one" {
		t.Fatal("facts phase checkpoint lost", err)
	}
	if err = store.SaveFacts(context.Background(), job, []string{"different"}); err == nil {
		t.Fatal("checkpoint facts changed under same source")
	}
}
func TestProfileRebuildMergesExactLegacyDuplicates(t *testing.T) {
	store, db, owner, _, profile := memoryFixture(t)
	for i := 0; i < 2; i++ {
		if err := db.Exec("INSERT INTO memories(user_id,content,vector_id) VALUES(?,'legacy identical fact','')", owner).Error; err != nil {
			t.Fatal(err)
		}
	}
	result, err := store.RebuildProfile(context.Background(), profile, "merge-legacy")
	if err != nil || result.Reindexed != 1 || result.Merged != 1 {
		t.Fatal("legacy duplicates blocked migration", result, err)
	}
	var active, deleted, jobs int64
	db.Table("memories").Where("user_id=? AND status=1", owner).Count(&active)
	db.Table("memories").Where("user_id=? AND status=3", owner).Count(&deleted)
	db.Model(&Job{}).Where("rebuild_id=?", result.ID).Count(&jobs)
	if active != 1 || deleted != 1 || jobs != 2 {
		t.Fatal("legacy merge/cleanup not atomic", active, deleted, jobs)
	}
}
