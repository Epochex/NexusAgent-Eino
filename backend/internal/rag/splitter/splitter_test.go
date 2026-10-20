package splitter

import (
	"strings"
	"testing"
	"unicode/utf8"
)

func TestSplitterBoundsAndCoverage(t *testing.T) {
	for name, text := range map[string]string{
		"no_separator":        strings.Repeat("界", 1024),
		"single_long_segment": strings.Repeat("x", 512) + ". tail",
		"buffer_order":        "prefix. " + strings.Repeat("界", 300) + "。suffix",
		"headings":            "# 第一节\n" + strings.Repeat("内容 words. ", 60) + "\n# 第二节\n" + strings.Repeat("结论。", 40),
	} {
		t.Run(name, func(t *testing.T) {
			parts := New(64, 8).Split(text, 7)
			if len(parts) < 2 {
				t.Fatalf("long content not split: %d", len(parts))
			}
			var rebuilt strings.Builder
			for i, part := range parts {
				runes := []rune(part.Content)
				if len(runes) > 64 || len(runes) == 0 || !utf8.ValidString(part.Content) || part.Page != 7 || part.Seq != i {
					t.Fatalf("invalid chunk %d size=%d page=%d seq=%d", i, len(runes), part.Page, part.Seq)
				}
				if i == 0 {
					rebuilt.WriteString(part.Content)
				} else {
					prior := []rune(parts[i-1].Content)
					if len(runes) < 8 || string(prior[len(prior)-8:]) != string(runes[:8]) {
						t.Fatalf("overlap mismatch at %d", i)
					}
					rebuilt.WriteString(string(runes[8:]))
				}
			}
			if rebuilt.String() != strings.TrimSpace(text) {
				t.Fatal("text lost, duplicated or reordered")
			}
		})
	}
}
func TestSplitterEmptyAndTiny(t *testing.T) {
	if got := New(1, 0).Split(" \n ", 1); len(got) != 0 {
		t.Fatal("whitespace chunks")
	}
	got := New(1, 0).Split("甲乙丙", 2)
	if len(got) != 3 {
		t.Fatalf("tiny chunk size failed %+v", got)
	}
}

func TestSplitBoundedRejectsExcessChunks(t *testing.T) {
	if _, err := New(64, 8).SplitBounded(strings.Repeat("x", 10000), 1, 10); err == nil {
		t.Fatal("chunk budget was not enforced")
	}
}
