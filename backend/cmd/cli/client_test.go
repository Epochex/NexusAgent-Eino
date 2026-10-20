package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestCLIContinuesSessionWithExtendedSSEFields(t *testing.T) {
	var calls atomic.Int32
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/auth/login" {
			fmt.Fprint(w, `{"code":0,"data":{"access_token":"test-token"}}`)
			return
		}
		if r.Header.Get("Authorization") != "Bearer test-token" {
			t.Error("missing auth header")
		}
		var request map[string]any
		json.NewDecoder(r.Body).Decode(&request)
		if calls.Add(1) == 2 && request["session_id"] != "session-1" {
			t.Errorf("second request lost conversation: %+v", request)
		}
		fmt.Fprint(w, "event: meta\ndata: {\"session_id\":\"session-1\",\"run_id\":\"run\",\"seq\":1,\"time\":\"2026-10-20T00:00:00Z\"}\n\n: keepalive\n\nevent: delta\ndata: {\"content\":\"ok\",\"run_id\":\"run\",\"seq\":2}\n\nevent: done\ndata: {\"finish_reason\":\"stop\",\"run_id\":\"run\",\"seq\":3}\n\n")
	}))
	defer s.Close()
	c := NewClient(s.URL)
	if err := c.Login("test", "ephemeral-test-password"); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := c.Chat("hello"); err != nil {
			t.Fatal(err)
		}
	}
}
func TestCLIReportsSSEFailure(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "event: error\ndata: {\"code\":50001,\"message\":\"model failed\"}\n\n")
	}))
	defer s.Close()
	if err := NewClient(s.URL).Chat("hello"); err == nil {
		t.Fatal("SSE failure returned success")
	}
}

func TestCLIRejectsTruncatedStream(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "event: delta\ndata: {\"content\":\"partial\"}\n\n")
	}))
	defer s.Close()
	if err := NewClient(s.URL).Chat("hello"); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("truncated stream accepted: %v", err)
	}
}
