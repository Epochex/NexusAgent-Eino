// 数据库迁移工具：执行 migrations/*.sql 并种子管理员账号。
//
// 为什么独立成 cmd：服务进程保持无状态启动（main.go 不做 DDL），
// 迁移由 `make migrate`（本命令）显式执行 —— 生产可用 CI 卡点，教学可手动执行。
//
// 用法：
//
//	go run ./cmd/migrate          # 执行全部 up 迁移 + 种子
//	go run ./cmd/migrate -down 1  # 回滚最近 1 个版本（危险，教学演示用）
package main

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"flag"
	"fmt"
	"os"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres" // lib/pq 驱动 + schema_migrations 表
	_ "github.com/golang-migrate/migrate/v4/source/file"       // file:// 读取 migrations/ 目录
	"golang.org/x/crypto/bcrypt"

	"github.com/chengpeng-cp/nexus-agent/internal/config"
	"github.com/chengpeng-cp/nexus-agent/internal/infra"
)

func main() {
	down := flag.Int("down", 0, "回滚 N 个版本（默认 0 = 只做升级）")
	flag.Parse()

	if err := run(*down); err != nil {
		fmt.Fprintf(os.Stderr, "migrate failed: %v\n", err)
		os.Exit(1)
	}
}

func run(down int) error {
	// ① 复用统一配置加载（PG DSN 来自 config.yaml / NX_ 环境变量）
	cfg, err := config.Load("")
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	// ② 升级：file source 顺序执行 migrations/NNN_xxx.up.sql，
	//    已应用的记录在 schema_migrations 表（golang-migrate 自动维护，幂等）
	m, err := migrate.New("file://migrations", cfg.PG.DSN)
	if err != nil {
		return fmt.Errorf("new migrator: %w", err)
	}
	defer func() { _, _ = m.Close() }()

	if down > 0 {
		if err := m.Steps(-down); err != nil {
			return fmt.Errorf("step down %d: %w", down, err)
		}
		fmt.Printf("rolled back %d version(s)\n", down)
		return nil
	}
	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		return fmt.Errorf("up: %w", err)
	}
	fmt.Println("migrations applied")

	// ③ 种子管理员：users 表为空才插入（幂等，密码哈希在代码层生成，
	//    与 002_seed.up.sql 头部注释的设计一致）
	if err := seedAdmin(cfg.PG.DSN); err != nil {
		return fmt.Errorf("seed admin: %w", err)
	}
	return nil
}

// seedAdmin inserts the initial admin only when users is empty. It also rotates
// the legacy admin/admin123 credential on upgraded installations.
func seedAdmin(dsn string) error {
	db, err := infra.NewPG(disabledCtx(), &config.PG{DSN: dsn, MaxOpenConns: 2, MaxIdleConns: 1})
	if err != nil {
		return err
	}
	sqlDB, err := db.DB()
	if err != nil {
		return err
	}
	defer sqlDB.Close()

	var userCount int64
	if err := db.WithContext(disabledCtx()).Table("users").Count(&userCount).Error; err != nil {
		return err
	}
	if userCount > 0 {
		var legacyAdmin struct {
			ID           int64
			PasswordHash string `gorm:"column:password_hash"`
		}
		if err := db.WithContext(disabledCtx()).Table("users").Select("id, password_hash").Where("username = ?", "admin").Take(&legacyAdmin).Error; err != nil {
			fmt.Println("users table not empty, skip seed")
			return nil
		}
		if bcrypt.CompareHashAndPassword([]byte(legacyAdmin.PasswordHash), []byte("admin123")) != nil {
			fmt.Println("users table not empty, skip seed")
			return nil
		}

		password, generated, err := bootstrapPassword()
		if err != nil {
			return err
		}
		hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
		if err != nil {
			return err
		}
		res := db.WithContext(disabledCtx()).Exec("UPDATE users SET password_hash = ?, updated_at = NOW() WHERE id = ?", string(hash), legacyAdmin.ID)
		if res.Error != nil {
			return res.Error
		}
		if res.RowsAffected == 0 {
			return fmt.Errorf("legacy admin password changed concurrently; rerun migration")
		}
		printBootstrapCredential(password, generated, "rotated legacy admin password")
		return nil
	}

	password, generatedPassword, err := bootstrapPassword()
	if err != nil {
		return err
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return err
	}
	// 唯一条记录插入：存在任何用户则跳过（与种子 SQL 的幂等约定一致）
	res := db.WithContext(disabledCtx()).Exec(`
		INSERT INTO users (username, email, password_hash, role, status)
		SELECT 'admin', 'admin@nexus.local', ?, 'admin', 1
		WHERE NOT EXISTS (SELECT 1 FROM users)`, string(hash))
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected > 0 {
		printBootstrapCredential(password, generatedPassword, "seeded admin user")
	} else {
		fmt.Println("users table not empty, skip seed")
	}
	return nil
}

func bootstrapPassword() (password string, generated bool, err error) {
	password = os.Getenv("NX_SEED_ADMIN_PASSWORD")
	generated = password == ""
	if generated {
		raw := make([]byte, 24)
		if _, err = rand.Read(raw); err != nil {
			return "", false, fmt.Errorf("generate bootstrap password: %w", err)
		}
		password = base64.RawURLEncoding.EncodeToString(raw)
	}
	if len(password) < 16 {
		return "", false, fmt.Errorf("NX_SEED_ADMIN_PASSWORD must contain at least 16 characters")
	}
	return password, generated, nil
}

func printBootstrapCredential(password string, generated bool, action string) {
	if generated {
		fmt.Printf("%s: admin / %s (请立即保存并在首次登录后修改密码)\n", action, password)
	} else {
		fmt.Printf("%s: admin (使用 NX_SEED_ADMIN_PASSWORD 设置的密码登录)\n", action)
	}
}

// disabledCtx 迁移场景不需要可取消上下文，给一个空实现保持 infra.NewPG 签名。
func disabledCtx() (ctx context.Context) {
	return context.Background()
}
