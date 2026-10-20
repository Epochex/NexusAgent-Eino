//go:build integration

package capacity_test

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/chengpeng-cp/nexus-agent/internal/execution"
	"github.com/golang-migrate/migrate/v4"
	migratepg "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

func TestCapacityMigrationRollbackAndReupgradePreservesLiveState(t *testing.T) {
	db, owners, flows := actors(t)
	if err := db.Exec("UPDATE execution_capacity_policies SET version=7,active_limit=3,owner_active_limit=2 WHERE scope='run'").Error; err != nil {
		t.Fatal(err)
	}
	store := execution.NewStore(db)
	if _, err := store.Enqueue(context.Background(), owners[0], flows[0], json.RawMessage(`{"dsl":"original"}`), nil, time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	run, err := store.Claim(context.Background(), "rollback-live-worker", time.Minute)
	if err != nil || run == nil {
		t.Fatal("claim", err)
	}
	if err = db.Exec("INSERT INTO execution_capacity_leases(id,scope,resource,owner_id,expires_at) VALUES('rollback-live-chat','run','',?,clock_timestamp()+interval '1 minute')", owners[1]).Error; err != nil {
		t.Fatal(err)
	}
	snapshot := func() string {
		var value string
		err := db.Raw(`SELECT jsonb_build_object(
 'policies',(SELECT jsonb_agg(to_jsonb(p) ORDER BY scope) FROM execution_capacity_policies p),
 'leases',(SELECT jsonb_agg(to_jsonb(l) ORDER BY id) FROM execution_capacity_leases l),
 'fairness',(SELECT jsonb_agg(to_jsonb(f) ORDER BY owner_id) FROM execution_owner_fairness f),
 'runs',(SELECT jsonb_agg(to_jsonb(r) ORDER BY id) FROM workflow_runs r))::text`).Scan(&value).Error
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
	before := snapshot()
	pool, _ := db.DB()
	driver, err := migratepg.WithInstance(pool, &migratepg.Config{})
	if err != nil {
		t.Fatal(err)
	}
	migrationPath, err := filepath.Abs("../../migrations")
	if err != nil {
		t.Fatal(err)
	}
	migrator, err := migrate.NewWithDatabaseInstance("file://"+filepath.ToSlash(migrationPath), "postgres", driver)
	if err != nil {
		t.Fatal(err)
	}
	defer migrator.Close()
	if err = migrator.Force(13); err != nil {
		t.Fatal(err)
	}
	if err = migrator.Steps(-1); err != nil {
		t.Fatal("rollback", err)
	}
	if got := snapshot(); got != before {
		t.Fatal("rollback changed live leases, policy, fairness or workflow state")
	}
	if err = migrator.Up(); err != nil {
		t.Fatal("re-upgrade retained compatible schema", err)
	}
	if got := snapshot(); got != before {
		t.Fatal("re-upgrade changed retained live state")
	}
	var active int64
	if err = db.Table("execution_active_capacity").Count(&active).Error; err != nil || active != 2 {
		t.Fatal("compatible active quota view", active, err)
	}
	version, dirty, err := migrator.Version()
	if err != nil || dirty || version != 13 {
		t.Fatal("migration version", version, dirty, err)
	}
}
