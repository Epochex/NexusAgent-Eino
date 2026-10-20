//go:build integration

// Package testpg owns isolated schemas in an explicitly selected test database.
package testpg

import (
	"crypto/rand"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func Open(t testing.TB) *gorm.DB {
	t.Helper()
	parsed, err := url.Parse(os.Getenv("NX_TEST_DATABASE_DSN"))
	if err != nil || parsed.Host == "" || !strings.HasSuffix(parsed.Path, "_test") {
		t.Fatal("explicit NX_TEST_DATABASE_DSN URL ending in _test required")
	}
	connect := func(dsn string) *gorm.DB {
		db, err := gorm.Open(postgres.New(postgres.Config{DSN: dsn, PreferSimpleProtocol: true}), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
		if err != nil {
			t.Fatal("cannot connect to test PostgreSQL")
		}
		return db
	}
	base := connect(parsed.String())
	name := "nexus_it_" + strings.ToLower(rand.Text())
	if err := base.Exec(fmt.Sprintf(`CREATE SCHEMA "%s"`, name)).Error; err != nil {
		t.Fatal(err)
	}
	query := parsed.Query()
	query.Set("search_path", name)
	parsed.RawQuery = query.Encode()
	db := connect(parsed.String())
	t.Cleanup(func() {
		sqlDB, _ := db.DB()
		_ = sqlDB.Close()
		if !strings.HasPrefix(name, "nexus_it_") {
			t.Fatal("invalid cleanup scope")
		}
		if err := base.Exec(fmt.Sprintf(`DROP SCHEMA "%s" CASCADE`, name)).Error; err != nil {
			t.Error(err)
		}
		sqlBase, _ := base.DB()
		_ = sqlBase.Close()
	})
	dir := "migrations"
	for i := 0; i < 6; i++ {
		if _, err := os.Stat(filepath.Join(dir, "001_init.up.sql")); err == nil {
			break
		}
		dir = filepath.Join("..", dir)
	}
	files, err := filepath.Glob(filepath.Join(dir, "*.up.sql"))
	if err != nil || len(files) == 0 {
		t.Fatal("migration sources unavailable")
	}
	for _, file := range files {
		if strings.HasPrefix(filepath.Base(file), "002_") {
			continue
		}
		sql, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		if err = db.Exec(string(sql)).Error; err != nil {
			t.Fatalf("migration %s: %v", filepath.Base(file), err)
		}
	}
	return db
}
