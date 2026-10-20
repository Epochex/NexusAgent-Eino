package vector

import (
	"context"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

const MaxScopeRecords = 1000

// Scope contains authoritative published generations, never a caller SQL string.
// Empty published sets match nothing rather than removing the filter.
type Scope struct {
	UserID, KBID     int64
	EmbeddingVersion string
	DocumentVersions map[int64]int64
	AgentID          *int64
	MemoryRevisions  map[int64]int64
}
type ScopedStore interface {
	SearchScoped(context.Context, string, []float32, int, Scope) ([]SearchHit, error)
}

func (s Scope) expression() (string, bool, error) {
	decoded, err := hex.DecodeString(s.EmbeddingVersion)
	if s.UserID <= 0 || err != nil || len(decoded) != 32 {
		return "", false, fmt.Errorf("valid owner and embedding space required")
	}
	expr := fmt.Sprintf("user_id == %d and embedding_version == %s", s.UserID, strconv.Quote(s.EmbeddingVersion))
	versions := s.DocumentVersions
	idField := "doc_id"
	versionField := "index_version"
	if s.KBID > 0 {
		expr += fmt.Sprintf(" and kb_id == %d", s.KBID)
	} else {
		if s.KBID < 0 {
			return "", false, fmt.Errorf("invalid knowledge base scope")
		}
		agent := int64(0)
		if s.AgentID != nil {
			agent = *s.AgentID
			if agent < 0 {
				return "", false, fmt.Errorf("invalid agent scope")
			}
		}
		expr += fmt.Sprintf(" and (agent_id == 0 or agent_id == %d)", agent)
		versions = s.MemoryRevisions
		idField = "id"
		versionField = "revision"
	}
	if len(versions) > MaxScopeRecords {
		return "", false, fmt.Errorf("published search scope exceeds %d records", MaxScopeRecords)
	}
	if len(versions) == 0 {
		return "", true, nil
	}
	// Group equal revisions to keep the scalar filter compact and deterministic.
	grouped := map[int64][]int64{}
	for id, version := range versions {
		if id < 1 || version < 1 {
			return "", false, fmt.Errorf("invalid published generation")
		}
		grouped[version] = append(grouped[version], id)
	}
	keys := make([]int64, 0, len(grouped))
	for version := range grouped {
		keys = append(keys, version)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	clauses := make([]string, 0, len(keys))
	for _, version := range keys {
		ids := grouped[version]
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		clauses = append(clauses, fmt.Sprintf("(%s == %d and %s in %s)", versionField, version, idField, fmtInt64Slice(ids)))
	}
	return expr + " and (" + strings.Join(clauses, " or ") + ")", false, nil
}
