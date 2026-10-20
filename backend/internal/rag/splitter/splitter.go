// Package splitter makes bounded UTF-8 chunks with exact overlapping context.
// Separator preference never changes the order or drops buffered content.
package splitter

import (
	"fmt"
	"strings"
	"unicode/utf8"
)

type Splitter struct {
	ChunkSize    int
	ChunkOverlap int
}

func New(size, overlap int) *Splitter {
	if size <= 0 {
		size = 512
	}
	if overlap < 0 || overlap >= size {
		overlap = size / 8
	}
	return &Splitter{ChunkSize: size, ChunkOverlap: overlap}
}

type Chunk struct {
	Seq     int
	Content string
	Page    int
}

func (s *Splitter) Split(content string, page int) []Chunk {
	chunks, _ := s.split(content, page, 0)
	return chunks
}
func (s *Splitter) SplitBounded(content string, page, maxChunks int) ([]Chunk, error) {
	if maxChunks < 1 {
		return nil, fmt.Errorf("chunk budget exhausted")
	}
	return s.split(content, page, maxChunks)
}
func (s *Splitter) split(content string, page, maxChunks int) ([]Chunk, error) {
	cfg := New(s.ChunkSize, s.ChunkOverlap)
	runes := []rune(strings.TrimSpace(content))
	if len(runes) == 0 {
		return nil, nil
	}
	capacity := len(runes)/(cfg.ChunkSize-cfg.ChunkOverlap) + 1
	if maxChunks > 0 {
		if (len(runes)+cfg.ChunkSize-1)/cfg.ChunkSize > maxChunks {
			return nil, fmt.Errorf("document exceeds chunk budget")
		}
		capacity = min(capacity, maxChunks)
	}
	chunks := make([]Chunk, 0, capacity)
	for start := 0; start < len(runes); {
		if maxChunks > 0 && len(chunks) >= maxChunks {
			return nil, fmt.Errorf("document exceeds chunk budget")
		}
		end := min(start+cfg.ChunkSize, len(runes))
		if end < len(runes) {
			window := string(runes[start:end])
			minimum := max(cfg.ChunkOverlap+1, cfg.ChunkSize/2)
			for _, sep := range []string{"\n#", "\n\n", "\n", "。", ".", " "} {
				at := strings.LastIndex(window, sep)
				if at < 0 {
					continue
				}
				cut := utf8.RuneCountInString(window[:at]) + utf8.RuneCountInString(sep)
				if sep == "\n#" {
					cut--
				} // keep the next heading with its following text
				if cut >= minimum && cut < end-start {
					end = start + cut
					break
				}
			}
		}
		chunks = append(chunks, Chunk{Seq: len(chunks), Content: string(runes[start:end]), Page: page})
		if end == len(runes) {
			break
		}
		// end-start is always greater than overlap: every iteration makes progress.
		start = end - cfg.ChunkOverlap
	}
	return chunks, nil
}
