package a2a

import (
	"context"
	"errors"
	"testing"

	sdk "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
	"github.com/chengpeng-cp/nexus-agent/internal/security"
)

func TestA2AVersionNegotiation(t *testing.T) {
	for _, tc := range []struct {
		name     string
		headers  map[string][]string
		rejected bool
	}{
		{"missing uses default", nil, false},
		{"empty uses default", map[string][]string{"A2A-Version": {""}}, false},
		{"supported", map[string][]string{"A2A-Version": {string(sdk.Version)}}, false},
		{"unknown", map[string][]string{"A2A-Version": {"99.0"}}, true},
		{"case insensitive", map[string][]string{"a2a-version": {"99.0"}}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := security.WithUserID(context.Background(), 1)
			ctx, call := a2asrv.NewCallContext(ctx, a2asrv.NewServiceParams(tc.headers))
			_, _, err := (serverIdentity{}).Before(ctx, call, &a2asrv.Request{})
			if tc.rejected && !errors.Is(err, sdk.ErrVersionNotSupported) {
				t.Fatalf("got %v, want unsupported version", err)
			}
			if !tc.rejected && err != nil {
				t.Fatal(err)
			}
		})
	}
}
