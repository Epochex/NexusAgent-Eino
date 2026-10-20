// Package llmstub provides isolated HTTP servers for model protocol tests.
package llmstub

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func New(t testing.TB, handler http.HandlerFunc) *httptest.Server {
	t.Helper()
	s := httptest.NewServer(handler)
	t.Cleanup(s.Close)
	return s
}
