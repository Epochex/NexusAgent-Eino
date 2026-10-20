package main

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/chengpeng-cp/nexus-agent/internal/rag/loader"
	"github.com/chengpeng-cp/nexus-agent/internal/testutil/pdfsample"
)

const parserTestToken = "public-parser-fixture-token-32-characters"

func TestActualPDFParserChildAndPrivateProtocol(t *testing.T) {
	name := "docparser"
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	binary := filepath.Join(t.TempDir(), name)
	build := exec.Command("go", "build", "-o", binary, ".")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("actual parser build %v %s", err, output)
	}
	peer := httptest.NewServer(parserHandler(binary, parserTestToken, 2*time.Second))
	defer peer.Close()
	client, err := loader.NewPDFClient(peer.URL, parserTestToken)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	doc, err := client.Load(context.Background(), pdfsample.Simple("isolated PDF accepted"))
	if err != nil || len(doc.Pages) != 1 || doc.Pages[0].Page != 1 || !strings.Contains(doc.Pages[0].Content, "isolated PDF accepted") {
		t.Fatal("actual parser lost text/citation", doc, err)
	}
	res, err := http.Post(peer.URL+"/parse", "application/pdf", bytes.NewReader(pdfsample.Simple("secret fixture")))
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, res.Body)
	res.Body.Close()
	if res.StatusCode != 401 {
		t.Fatal("unauthenticated parser request accepted", res.StatusCode)
	}
	if _, err = client.Load(context.Background(), []byte("%PDF-invalid")); err == nil {
		t.Fatal("malformed PDF reported ready")
	}
}

func TestPDFParserKillsHungChildAndReleasesSlot(t *testing.T) {
	dir := t.TempDir()
	filename := filepath.Join(dir, "hang.go")
	if err := os.WriteFile(filename, []byte("package main\nfunc main(){for{}}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	name := "hang"
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	binary := filepath.Join(dir, name)
	if output, err := exec.Command("go", "build", "-o", binary, filename).CombinedOutput(); err != nil {
		t.Fatalf("controlled hung child build %v %s", err, output)
	}
	peer := httptest.NewServer(parserHandler(binary, parserTestToken, 150*time.Millisecond))
	defer peer.Close()
	for range 2 {
		req, _ := http.NewRequest("POST", peer.URL+"/parse", bytes.NewReader([]byte("%PDF-hung-child")))
		req.Header.Set("Authorization", "Bearer "+parserTestToken)
		client := &http.Client{Timeout: 2 * time.Second}
		start := time.Now()
		res, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		client.CloseIdleConnections()
		if res.StatusCode != 504 || time.Since(start) > time.Second {
			t.Fatalf("hung parse was unbounded or retained its slot: status=%d elapsed=%s", res.StatusCode, time.Since(start))
		}
	}
}
