package tool

import (
	"context"
	"github.com/chengpeng-cp/nexus-agent/internal/security"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestFileOpsRejectsSiblingTraversalAndOversizedWrites(t *testing.T) {
	parent := t.TempDir()
	base := filepath.Join(parent, "files")
	sibling := filepath.Join(parent, "files-other")
	if err := os.MkdirAll(base, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(sibling, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sibling, "secret.txt"), []byte("outside"), 0600); err != nil {
		t.Fatal(err)
	}
	cfg := FileOpsConfig{BaseDir: base, MaxSize: 16}
	if _, err := runFileOps(cfg, map[string]any{"operation": "read", "path": "../files-other/secret.txt"}); err == nil {
		t.Fatal("prefix sibling traversal accepted")
	}
	if _, err := runFileOps(cfg, map[string]any{"operation": "save", "path": "large.txt", "content": strings.Repeat("x", 17)}); err == nil {
		t.Fatal("oversized write accepted")
	}
	if _, err := os.Stat(filepath.Join(base, "large.txt")); !os.IsNotExist(err) {
		t.Fatal("oversized write left an effect", err)
	}
}
func TestFileOpsRejectsSymlinkEscapeAndPreservesExternalHardLink(t *testing.T) {
	parent := t.TempDir()
	base := filepath.Join(parent, "files")
	if err := os.Mkdir(base, 0700); err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(parent, "outside.txt")
	if err := os.WriteFile(outside, []byte("original"), 0600); err != nil {
		t.Fatal(err)
	}
	cfg := FileOpsConfig{BaseDir: base, MaxSize: 64}
	if err := os.Symlink(outside, filepath.Join(base, "link.txt")); err == nil {
		if _, err = runFileOps(cfg, map[string]any{"operation": "read", "path": "link.txt"}); err == nil {
			t.Error("read followed external symlink")
		}
	} else {
		t.Log("OS did not allow creating test symlink", err)
	}
	if err := os.Link(outside, filepath.Join(base, "hard.txt")); err != nil {
		t.Fatal(err)
	}
	if _, err := runFileOps(cfg, map[string]any{"operation": "save", "path": "hard.txt", "content": "replacement"}); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(outside)
	if err != nil || string(raw) != "original" {
		t.Fatal("save modified external hardlinked file", string(raw), err)
	}
}
func TestRegisteredFileOpsSeparatesOwnersAndRequiresAuthentication(t *testing.T) {
	reg := NewRegistry()
	RegisterFiles(reg, FileOpsConfig{BaseDir: t.TempDir(), MaxSize: 64})
	handler, _ := reg.Get("file_ops")
	save := map[string]any{"operation": "save", "path": "memo.txt", "content": "owner-one"}
	if _, err := handler(context.Background(), save, ""); err == nil {
		t.Fatal("anonymous filesystem mutation accepted")
	}
	if _, err := handler(security.WithUserID(context.Background(), 1), save, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := handler(security.WithUserID(context.Background(), 2), map[string]any{"operation": "read", "path": "memo.txt"}, ""); err == nil {
		t.Fatal("other owner file visible")
	}
	value, err := handler(security.WithUserID(context.Background(), 1), map[string]any{"operation": "read", "path": "memo.txt"}, "")
	if err != nil || value != "owner-one" {
		t.Fatal("owner cannot read own file", value, err)
	}
}
