package main

import (
	"strings"
	"testing"
)

func TestStreamCountsContentNotMetadataAndRequiresTerminal(t *testing.T) {
	for _, tc := range []struct {
		body             string
		tokens           int
		terminal, failed bool
	}{
		{"event: meta\ndata: {\"content\":\"metadata\"}\n\nevent: delta\ndata: {\"content\":\"answer\"}\n\nevent: done\ndata: {}\n\n", 1, true, false},
		{"event: error\ndata: {\"message\":\"failed\"}\n\n", 0, true, true},
		{"event: meta\ndata: {}\n\n", 0, false, false},
	} {
		var tokens int
		result, err := readEvents(strings.NewReader(tc.body), func(name string, data []byte) {
			if isContentDelta(name, data) {
				tokens++
			}
		})
		if err != nil || tokens != tc.tokens || result.Terminal != tc.terminal || result.Failed != tc.failed {
			t.Fatalf("incorrect stream accounting: %+v tokens=%d err=%v", result, tokens, err)
		}
	}
}
