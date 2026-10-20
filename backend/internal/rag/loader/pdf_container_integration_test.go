//go:build integration && infrastructure

package loader

import (
	"compress/gzip"
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/chengpeng-cp/nexus-agent/internal/testutil/pdfsample"
)

func TestPDFParserContainerFailureAndRecovery(t *testing.T) {
	endpoint := os.Getenv("NX_TEST_PDF_PARSER_URL")
	if endpoint == "" {
		t.Skip("NX_TEST_PDF_PARSER_URL required for the dedicated constrained parser container")
	}
	client, err := NewPDFClient(endpoint, os.Getenv("NX_TEST_PDF_PARSER_TOKEN"))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()
	verify := func() error {
		doc, e := client.Load(ctx, pdfsample.Simple("isolated parser recovery verified"))
		if e == nil && (len(doc.Pages) != 1 || doc.Pages[0].Page != 1 || !strings.Contains(doc.Pages[0].Content, "isolated parser recovery verified")) {
			return errors.New("real PDF text or page citation missing")
		}
		return e
	}
	if err = verify(); err != nil {
		t.Fatal("actual parser unavailable before fault", err)
	}
	// Prepare the committed small fixture before starting the fault clock.
	// Compressing 768MiB while race instrumentation is active measures generator
	// CPU and can exhaust the test deadline before the parser is ever called.
	file, err := os.Open("testdata/pdf-literal-768m.pdf.gz")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	gz, err := gzip.NewReader(file)
	if err != nil {
		t.Fatal(err)
	}
	defer gz.Close()
	compressed, err := io.ReadAll(io.LimitReader(gz, MaxDocumentBytes+1))
	if err != nil || len(compressed) > MaxDocumentBytes {
		t.Fatal("invalid bounded test fixture", err)
	}
	t.Logf("hostile PDF: compressed_input=%d bytes; uncompressed literal=768MiB", len(compressed))
	start := time.Now()
	if _, err = client.Load(ctx, compressed); !errors.Is(err, ErrPDFUnavailable) {
		t.Fatalf("resource fault was silently accepted or misclassified: %v", err)
	}
	if time.Since(start) > 17*time.Second {
		t.Fatal("parser fault exceeded its request/child deadline")
	}
	for until := time.Now().Add(20 * time.Second); ; {
		err = verify()
		if err == nil {
			break
		}
		if time.Now().After(until) {
			t.Fatal("parser could not recover after bounded child/container failure", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Logf("actual parser resource failure and subsequent valid PDF recovery: elapsed=%s", time.Since(start))
}
