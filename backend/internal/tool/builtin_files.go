// File tools operate within a per-owner directory using descriptor-based roots.
package tool

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"github.com/chengpeng-cp/nexus-agent/internal/security"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"unicode/utf8"
)

type FileOpsConfig struct {
	BaseDir string
	MaxSize int
}

func RegisterFiles(reg Registry, cfg FileOpsConfig) {
	reg.Register("file_ops", func(ctx context.Context, args map[string]any, _ string) (string, error) {
		owner, ok := security.UserIDFromContext(ctx)
		if !ok || owner < 1 {
			return "", fmt.Errorf("authenticated file owner required")
		}
		if err := ctx.Err(); err != nil {
			return "", err
		}
		base, err := openFileRoot(cfg.BaseDir)
		if err != nil {
			return "", err
		}
		defer base.Close()
		name := strconv.FormatInt(owner, 10)
		if err = base.Mkdir(name, 0700); err != nil && !errors.Is(err, fs.ErrExist) {
			return "", err
		}
		info, err := base.Lstat(name)
		if err != nil {
			return "", err
		}
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return "", fmt.Errorf("invalid owner directory")
		}
		root, err := base.OpenRoot(name)
		if err != nil {
			return "", err
		}
		defer root.Close()
		return runRootFileOps(root, cfg.MaxSize, args)
	})
}
func openFileRoot(base string) (*os.Root, error) {
	if base == "" {
		return nil, fmt.Errorf("authorized file directory is required")
	}
	if err := os.MkdirAll(base, 0700); err != nil {
		return nil, err
	}
	return os.OpenRoot(base)
}
func runFileOps(cfg FileOpsConfig, args map[string]any) (string, error) {
	root, err := openFileRoot(cfg.BaseDir)
	if err != nil {
		return "", err
	}
	defer root.Close()
	return runRootFileOps(root, cfg.MaxSize, args)
}
func runRootFileOps(root *os.Root, maximum int, args map[string]any) (string, error) {
	if maximum == 0 {
		maximum = 1 << 20
	}
	if maximum < 1 || maximum > 10<<20 {
		return "", fmt.Errorf("file size limit must be 1..10MiB")
	}
	operation, _ := args["operation"].(string)
	path, _ := args["path"].(string)
	if path == "" || len(path) > 1024 || !utf8.ValidString(path) || !filepath.IsLocal(path) || filepath.Clean(path) == "." {
		return "", fmt.Errorf("path must be a local file within the authorized directory")
	}
	switch operation {
	case "read":
		entry, err := root.Lstat(path)
		if err != nil {
			return "", err
		}
		if !entry.Mode().IsRegular() {
			return "", fmt.Errorf("only regular text files are supported")
		}
		file, err := root.Open(path)
		if err != nil {
			return "", fmt.Errorf("read file: %w", err)
		}
		defer file.Close()
		info, err := file.Stat()
		if err != nil {
			return "", err
		}
		if !info.Mode().IsRegular() {
			return "", fmt.Errorf("only regular text files are supported")
		}
		if info.Size() > int64(maximum) {
			return "", fmt.Errorf("file exceeds read limit")
		}
		data, err := io.ReadAll(io.LimitReader(file, int64(maximum)+1))
		if err != nil {
			return "", err
		}
		if len(data) > maximum {
			return "", fmt.Errorf("file exceeds read limit")
		}
		return string(data), nil
	case "save":
		content, _ := args["content"].(string)
		if len(content) > maximum || !utf8.ValidString(content) {
			return "", fmt.Errorf("content exceeds file limit or is not UTF-8")
		}
		if err := root.MkdirAll(filepath.Dir(path), 0700); err != nil {
			return "", err
		}
		temporary := filepath.Join(filepath.Dir(path), ".nexus-"+rand.Text()+".tmp")
		file, err := root.OpenFile(temporary, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			return "", err
		}
		defer root.Remove(temporary)
		_, writeErr := file.WriteString(content)
		if writeErr == nil {
			writeErr = file.Sync()
		}
		closeErr := file.Close()
		if writeErr != nil {
			return "", writeErr
		}
		if closeErr != nil {
			return "", closeErr
		}
		// Replacing the directory entry avoids following a destination symlink or
		// truncating an external inode through a pre-existing hard link.
		if err = root.Rename(temporary, path); err != nil {
			return "", fmt.Errorf("save file: %w", err)
		}
		return fmt.Sprintf("{\"saved\":%q,\"bytes\":%d}", path, len(content)), nil
	default:
		return "", fmt.Errorf("operation must be read/save")
	}
}
