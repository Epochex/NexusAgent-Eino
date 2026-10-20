package security

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestOutboundResponseHeaderBudget(t *testing.T) {
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		timer := time.NewTimer(150 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-r.Context().Done():
			return
		case <-timer.C:
			w.Write([]byte("ready"))
		}
	}))
	defer peer.Close()
	short, err := NewOutboundClient(OutboundPolicy{PrivateOrigins: []string{peer.URL}, Timeout: time.Second, ResponseHeaderTimeout: 30 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer short.CloseIdleConnections()
	if response, err := short.Get(peer.URL); err == nil {
		response.Body.Close()
		t.Fatal("response header deadline ignored")
	}
	long, err := NewOutboundClient(OutboundPolicy{PrivateOrigins: []string{peer.URL}, Timeout: time.Second, ResponseHeaderTimeout: 500 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer long.CloseIdleConnections()
	response, err := long.Get(peer.URL)
	if err != nil {
		t.Fatal("explicit longer header budget not honored", err)
	}
	response.Body.Close()
	if _, err = NewOutboundClient(OutboundPolicy{Timeout: time.Second, ResponseHeaderTimeout: 2 * time.Second}); err == nil {
		t.Fatal("header wait exceeded total request budget")
	}
}
