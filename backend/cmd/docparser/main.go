// docparser belongs in its own resource-limited container, never in the API or
// worker container. The HTTP parent supervises one disposable PDF child.
package main

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/chengpeng-cp/nexus-agent/internal/rag/loader"
)

type boundedBuffer struct {
	bytes.Buffer
	limit int
}

func (b *boundedBuffer) Write(p []byte) (int, error) {
	if len(p) > b.limit-b.Len() {
		return 0, fmt.Errorf("parser output bound exceeded")
	}
	return b.Buffer.Write(p)
}

func parse() int {
	data, err := io.ReadAll(io.LimitReader(os.Stdin, loader.MaxDocumentBytes+1))
	if err != nil || len(data) > loader.MaxDocumentBytes {
		return 3
	}
	doc, err := loader.Load("document.pdf", data)
	if err != nil {
		return 3
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetEscapeHTML(false)
	if encoder.Encode(doc) != nil {
		return 1
	}
	return 0
}

func parserHandler(binary, token string, timeout time.Duration) http.Handler {
	mux := http.NewServeMux()
	slot := make(chan struct{}, 1)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			w.WriteHeader(405)
			return
		}
		w.WriteHeader(200)
	})
	mux.HandleFunc("/parse", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			w.WriteHeader(405)
			return
		}
		if subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+token)) != 1 {
			http.Error(w, "unauthorized", 401)
			return
		}
		select {
		case slot <- struct{}{}:
			defer func() { <-slot }()
		default:
			w.Header().Set("Retry-After", "1")
			http.Error(w, "parser busy", 429)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), timeout)
		defer cancel()
		_ = http.NewResponseController(w).SetReadDeadline(time.Now().Add(timeout))
		r.Body = http.MaxBytesReader(w, r.Body, loader.MaxDocumentBytes)
		data, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "invalid or oversized document", 413)
			return
		}
		if !bytes.HasPrefix(data, []byte("%PDF-")) {
			http.Error(w, "invalid PDF", 422)
			return
		}
		cmd := exec.CommandContext(ctx, binary, "-parse")
		// No application credentials or caller-controlled filenames/flags enter
		// the child. GOMEMLIMIT is a soft tuning hint; Docker is the hard bound.
		cmd.Env = []string{"GOMAXPROCS=1", "GOMEMLIMIT=256MiB"}
		cmd.Stdin = bytes.NewReader(data)
		output := &boundedBuffer{limit: loader.MaxPDFResponseBytes}
		cmd.Stdout = output
		cmd.Stderr = io.Discard
		cmd.WaitDelay = time.Second
		prepareChild(cmd)
		err = cmd.Run()
		if ctx.Err() != nil {
			http.Error(w, "parse deadline exceeded or canceled", 504)
			return
		}
		if err != nil {
			var exit *exec.ExitError
			if errors.As(err, &exit) && exit.ExitCode() == 3 {
				http.Error(w, "PDF text extraction failed; OCR was not performed", 422)
				return
			}
			http.Error(w, "parser process failed", 503)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(output.Bytes())
	})
	return mux
}

func main() {
	child := flag.Bool("parse", false, "supervised child mode; stdin PDF, stdout JSON")
	healthcheck := flag.Bool("healthcheck", false, "probe the private local listener")
	listen := flag.String("listen", ":8095", "private parser listener")
	flag.Parse()
	if *healthcheck {
		client := &http.Client{Timeout: time.Second}
		res, err := client.Get("http://127.0.0.1:8095/healthz")
		if err != nil {
			os.Exit(1)
		}
		res.Body.Close()
		if res.StatusCode != 200 {
			os.Exit(1)
		}
		return
	}
	if *child {
		os.Exit(parse())
	}
	token := os.Getenv("NX_PDF_PARSER_TOKEN")
	if len(token) < 32 || len(token) > 4096 || strings.ContainsAny(token, "\r\n\x00") {
		fmt.Fprintln(os.Stderr, "NX_PDF_PARSER_TOKEN must contain 32..4096 characters")
		os.Exit(1)
	}
	binary, err := os.Executable()
	if err != nil {
		fmt.Fprintln(os.Stderr, "cannot locate parser executable")
		os.Exit(1)
	}
	root, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	srv := &http.Server{Addr: *listen, Handler: parserHandler(binary, token, 10*time.Second), ReadHeaderTimeout: 3 * time.Second, ReadTimeout: 12 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 15 * time.Second, MaxHeaderBytes: 16 << 10, BaseContext: func(net.Listener) context.Context { return root }}
	done := make(chan error, 1)
	go func() { done <- srv.ListenAndServe() }()
	select {
	case <-root.Done():
	case err = <-done:
		if !errors.Is(err, http.ErrServerClosed) {
			fmt.Fprintln(os.Stderr, "parser listener failed")
			os.Exit(1)
		}
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if srv.Shutdown(ctx) != nil {
		_ = srv.Close()
	}
}
