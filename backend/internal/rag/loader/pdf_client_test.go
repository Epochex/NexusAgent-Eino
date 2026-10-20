package loader

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestPDFParserClientRequiresExplicitConfigurationAndBoundedOutput(t *testing.T) {
	for _, tc := range []struct{ endpoint, token string }{{"http://user:secret@localhost", "token"}, {"http://localhost?token=x", "token"}, {"http://localhost", ""}, {"", "token"}} {
		if _, err := NewPDFClient(tc.endpoint, tc.token); err == nil {
			t.Error("unsafe/partial parser configuration accepted")
		}
	}
	disabled, err := NewPDFClient("", "")
	if err != nil || disabled != nil {
		t.Fatal("empty configuration should disable PDF")
	}
	if _, err = disabled.Load(context.Background(), []byte("%PDF-fixture")); !errors.Is(err, ErrPDFNotConfigured) {
		t.Fatal("unconfigured PDF did not fail closed", err)
	}
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/parse" || r.Method != "POST" || r.Header.Get("Authorization") != "Bearer public-fixture-token" {
			t.Error("parser protocol/authentication changed")
		}
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(LoadedDoc{Pages: []PageContent{{Page: 2, Content: "actual text"}}, Warnings: []string{"page 1 requires OCR"}})
	}))
	defer peer.Close()
	client, err := NewPDFClient(peer.URL, "public-fixture-token")
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	doc, err := client.Load(context.Background(), []byte("%PDF-fixture"))
	if err != nil || len(doc.Pages) != 1 || doc.Pages[0].Page != 2 || len(doc.Warnings) != 1 {
		t.Fatal("parser lost citations/warnings", doc, err)
	}
	for _, raw := range []string{`{"Pages":[]}`, `{"Pages":[{"Page":0,"Content":"bad page"}]}`, `{"Pages":[{"Page":1,"Content":"a"},{"Page":1,"Content":"duplicate"}]}`, `{"Pages":[{"Page":1,"Content":""}]}`, `{"Pages":[{"Page":1,"Content":"text"}],"unknown":true}`} {
		t.Run(raw, func(t *testing.T) {
			bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				io.WriteString(w, raw)
			}))
			defer bad.Close()
			client, err := NewPDFClient(bad.URL, "public-fixture-token")
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			if _, err = client.Load(context.Background(), []byte("%PDF-fixture")); err == nil {
				t.Fatal("invalid parser response accepted")
			}
		})
	}
}

func TestPDFParserClientCancellationAndRedirectNeverForwardCredentials(t *testing.T) {
	entered, stopped := make(chan struct{}), make(chan struct{})
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		close(entered)
		<-r.Context().Done()
		close(stopped)
	}))
	defer peer.Close()
	client, err := NewPDFClient(peer.URL, "public-fixture-token")
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, e := client.Load(ctx, []byte("%PDF-fixture")); done <- e }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("parser request did not start")
	}
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Error("canceled parse succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("parser cancellation did not return")
	}
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("parser HTTP still running")
	}
	foreign := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { t.Error("redirect escaped configured parser origin") }))
	defer foreign.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, foreign.URL, http.StatusTemporaryRedirect)
	}))
	defer redirect.Close()
	client, err = NewPDFClient(redirect.URL, "public-fixture-token")
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if _, err = client.Load(context.Background(), []byte("%PDF-fixture")); err == nil {
		t.Fatal("parser redirected silently")
	}
}
