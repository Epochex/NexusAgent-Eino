package long_term

import (
	"bytes"
	"context"
	"encoding/json"
	"gorm.io/gorm"
	"io"
	"strings"
	"unicode/utf8"
)

func canonicalMemoryModel(raw []byte) ([]byte, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value map[string]any
	if decoder.Decode(&value) != nil || value == nil || decoder.Decode(new(any)) != io.EOF {
		return nil, ErrMemoryInvalid
	}
	var clean func(any)
	clean = func(value any) {
		switch item := value.(type) {
		case map[string]any:
			for key, nested := range item {
				normalized := strings.ToLower(strings.ReplaceAll(key, "_", ""))
				if normalized == "apikey" || normalized == "encryptedkey" {
					delete(item, key)
				} else {
					clean(nested)
				}
			}
		case []any:
			for _, nested := range item {
				clean(nested)
			}
		}
	}
	clean(value)
	return json.Marshal(value)
}
func normalizeFacts(input []string) ([]string, error) {
	if len(input) > 16 {
		return nil, ErrMemoryInvalid
	}
	result := make([]string, 0, len(input))
	seen := map[string]bool{}
	for _, fact := range input {
		fact = strings.TrimSpace(fact)
		if fact == "" || len(fact) > 1024 || !utf8.ValidString(fact) {
			return nil, ErrMemoryInvalid
		}
		if !seen[fact] {
			seen[fact] = true
			result = append(result, fact)
		}
	}
	return result, nil
}
func (s *Store) SaveFacts(ctx context.Context, job *Job, input []string) error {
	facts, err := normalizeFacts(input)
	if err != nil {
		return PermanentMemory(err)
	}
	raw, err := json.Marshal(facts)
	if err != nil {
		return err
	}
	return s.withLease(ctx, job, func(tx *gorm.DB, current *Job) error {
		if current.Kind != "extract" {
			return ErrMemoryInvalid
		}
		if len(current.Facts) > 0 {
			var saved []string
			if json.Unmarshal(current.Facts, &saved) != nil {
				return PermanentMemory(ErrMemoryInvalid)
			}
			if len(saved) != len(facts) {
				return ErrMemoryConflict
			}
			for i := range saved {
				if saved[i] != facts[i] {
					return ErrMemoryConflict
				}
			}
			return nil
		}
		return tx.Model(&Job{}).Where("id=?", current.ID).Update("facts", string(raw)).Error
	})
}
