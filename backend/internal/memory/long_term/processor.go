package long_term

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/chengpeng-cp/nexus-agent/internal/ingestion"
	"github.com/chengpeng-cp/nexus-agent/internal/llm"
	"github.com/chengpeng-cp/nexus-agent/internal/model"
	"github.com/chengpeng-cp/nexus-agent/internal/security"
	"github.com/chengpeng-cp/nexus-agent/internal/vector"
)

const factsPrompt = "MEMORY_FACTS_V2\n仅从用户明确表达的信息抽取可长期使用的偏好、背景和约束。不要把助手推测或工具输出当成用户事实，不保存密码、密钥等认证信息。最多16条，每条简短独立陈述。只返回JSON字符串数组，无内容返回[]。"
const operationsPrompt = "MEMORY_OPS_V2\n根据新事实与已有记忆决定ADD、UPDATE、DELETE、NOOP。UPDATE和DELETE只能使用已有记忆的ID，同一ID最多修改一次。ADD必须包含实际要保存的content且不带id。不要跨条目按位置关联，只按事实内容和明确ID匹配。只返回JSON数组，格式[{\"op\":\"ADD\",\"content\":\"...\"},{\"op\":\"UPDATE\",\"id\":1,\"content\":\"...\"},{\"op\":\"DELETE\",\"id\":2}]。"

func decodeMemoryArray(text string, value any) error {
	text = strings.TrimSpace(text)
	if strings.HasPrefix(text, "\x60\x60\x60json") && strings.HasSuffix(text, "\x60\x60\x60") {
		text = strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(text, "\x60\x60\x60json"), "\x60\x60\x60"))
	}
	if len(text) > 65536 || !strings.HasPrefix(text, "[") || !strings.HasSuffix(text, "]") {
		return PermanentMemory(fmt.Errorf("memory model must return one bounded JSON array"))
	}
	decoder := json.NewDecoder(strings.NewReader(text))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return PermanentMemory(fmt.Errorf("invalid structured memory response"))
	}
	if decoder.Decode(new(any)) != io.EOF {
		return PermanentMemory(ErrMemoryInvalid)
	}
	return nil
}
func (m *Manager) chat(ctx context.Context, descriptor *llm.ModelDescriptor, system string, input any) (string, error) {
	if descriptor == nil {
		return "", ErrMemoryInvalid
	}
	raw, err := json.Marshal(input)
	if err != nil {
		return "", err
	}
	window := descriptor.ContextWindow
	if window <= 0 {
		window = 8192
	}
	reserve := llm.EstimateTokens(system) + llm.EstimateTokens(string(raw)) + 64
	output := descriptor.MaxTokens
	if output <= 0 {
		output = 2048
	}
	output = min(output, 2048, window-reserve)
	if output < 256 {
		return "", PermanentMemory(fmt.Errorf("memory input exceeds configured model context"))
	}
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	response, _, err := m.gateway.Chat(ctx, descriptor, nil, &llm.ChatRequest{Messages: []llm.Message{{Role: llm.RoleSystem, Content: system}, {Role: llm.RoleUser, Content: string(raw)}}, Temperature: 0.1, MaxTokens: output})
	if err != nil {
		return "", err
	}
	if response == nil || response.FinishReason == "length" || len(response.Content) > 65536 {
		return "", PermanentMemory(fmt.Errorf("incomplete or oversized memory response"))
	}
	return response.Content, nil
}
func (m *Manager) Process(ctx context.Context, job *Job) error {
	if !m.valid() || job == nil {
		return ErrMemoryInvalid
	}
	ctx = security.WithUserID(ctx, job.OwnerID)
	if job.Kind == "vector" {
		return m.processVector(ctx, job)
	}
	if job.Kind != "extract" {
		return PermanentMemory(ErrMemoryInvalid)
	}
	source, decision, err := m.repo.Source(ctx, job)
	if err != nil {
		return err
	}
	if decision != nil {
		return m.repo.Apply(ctx, job)
	}
	profile, err := m.repo.Profile(ctx, job.OwnerID)
	if err != nil {
		return err
	}
	if profile.EmbeddingVersion != job.EmbeddingVersion {
		return ErrMemoryConflict
	}
	descriptor, err := m.models.ResolveMemoryExtractor(ctx, source.Extractor)
	if err != nil {
		return err
	}
	facts := source.Facts
	if facts == nil {
		// Source IDs remain in the private checkpoint; model input needs only roles/text.
		messages := make([]llm.Message, 0, len(source.Messages))
		for _, message := range source.Messages {
			messages = append(messages, llm.Message{Role: message.Role, Content: message.Content})
		}
		content, err := m.chat(ctx, descriptor, factsPrompt, messages)
		if err != nil {
			return err
		}
		var extracted []string
		if err = decodeMemoryArray(content, &extracted); err != nil {
			return err
		}
		facts, err = normalizeFacts(extracted)
		if err != nil {
			return PermanentMemory(err)
		}
		if err = m.repo.SaveFacts(ctx, job, facts); err != nil {
			return err
		}
	}
	plan := Decision{Candidates: map[int64]int64{}}
	if len(facts) > 0 {
		candidates, err := m.candidates(ctx, job, profile, facts)
		if err != nil {
			return err
		}
		if len(candidates) == 0 {
			for _, fact := range facts {
				plan.Ops = append(plan.Ops, Operation{Op: "ADD", Content: fact})
			}
		} else {
			type candidate struct {
				ID      int64  `json:"id"`
				Content string `json:"content"`
			}
			existing := make([]candidate, 0, len(candidates))
			allowed := map[int64]int64{}
			for _, memory := range candidates {
				existing = append(existing, candidate{memory.ID, memory.Content})
				allowed[memory.ID] = memory.Revision
			}
			content, err := m.chat(ctx, descriptor, operationsPrompt, map[string]any{"existing_memories": existing, "new_facts": facts})
			if err != nil {
				return err
			}
			if err = decodeMemoryArray(content, &plan.Ops); err != nil {
				return err
			}
			for _, op := range plan.Ops {
				if op.ID != 0 {
					revision, exists := allowed[op.ID]
					if !exists {
						return PermanentMemory(fmt.Errorf("memory model referenced an unauthorized candidate"))
					}
					plan.Candidates[op.ID] = revision
				}
			}
		}
	}
	if err = m.repo.SaveDecision(ctx, job, plan); err != nil {
		return err
	}
	return m.repo.Apply(ctx, job)
}
func (m *Manager) candidates(ctx context.Context, job *Job, profile *Profile, facts []string) ([]model.Memory, error) {
	scope, err := m.repo.ReadyScope(ctx, job.OwnerID, job.AgentID, profile)
	if err != nil {
		return nil, err
	}
	if len(scope.MemoryRevisions) == 0 {
		return nil, nil
	}
	ctx, cancel := context.WithTimeout(ctx, 12*time.Second)
	defer cancel()
	descriptor, err := m.models.ResolveMemoryEmbedding(ctx, profile)
	if err != nil {
		return nil, err
	}
	for _, fact := range facts {
		if descriptor.ContextWindow > 0 && llm.EstimateTokens(fact) > descriptor.ContextWindow {
			return nil, PermanentMemory(ErrMemoryInvalid)
		}
	}
	values, err := m.gateway.Embed(ctx, descriptor, facts)
	if err != nil {
		return nil, err
	}
	if len(values) != len(facts) {
		return nil, fmt.Errorf("memory fact embedding count mismatch")
	}
	scoped, ok := m.vectors.(vector.ScopedStore)
	if !ok {
		return nil, fmt.Errorf("scoped vector search required")
	}
	type search struct {
		hits []vector.SearchHit
		err  error
	}
	results := make([]search, len(values))
	for _, value := range values {
		if len(value) != profile.EmbeddingDimension {
			return nil, fmt.Errorf("memory fact embedding dimension mismatch")
		}
	}
	slots := make(chan struct{}, 4)
	var work sync.WaitGroup
	for i, value := range values {
		work.Add(1)
		go func(index int, value []float32) {
			defer work.Done()
			select {
			case slots <- struct{}{}:
				defer func() { <-slots }()
			case <-ctx.Done():
				results[index].err = ctx.Err()
				return
			}
			results[index].hits, results[index].err = scoped.SearchScoped(ctx, MemoryCollection(profile.EmbeddingVersion), value, 5, scope)
		}(i, value)
	}
	work.Wait()
	scores := map[int64]float32{}
	for _, result := range results {
		if result.err != nil {
			return nil, result.err
		}
		for _, hit := range result.hits {
			if hit.Score >= m.cfg.MinimumSimilarity && hit.Score > scores[hit.ID] {
				scores[hit.ID] = hit.Score
			}
		}
	}
	ids := make([]int64, 0, len(scores))
	for id := range scores {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		if scores[ids[i]] == scores[ids[j]] {
			return ids[i] < ids[j]
		}
		return scores[ids[i]] > scores[ids[j]]
	})
	if len(ids) > 32 {
		ids = ids[:32]
	}
	rows, err := m.repo.ReadyByIDs(ctx, job.OwnerID, job.AgentID, profile, ids)
	if err != nil {
		return nil, err
	}
	byID := map[int64]model.Memory{}
	for _, row := range rows {
		if scope.MemoryRevisions[row.ID] == row.Revision {
			byID[row.ID] = row
		}
	}
	result := make([]model.Memory, 0, len(rows))
	size := 0
	for _, id := range ids {
		if memory, ok := byID[id]; ok {
			if size+len(memory.Content) > 16384 {
				continue
			}
			size += len(memory.Content)
			result = append(result, memory)
		}
	}
	return result, nil
}
func (m *Manager) processVector(parent context.Context, job *Job) error {
	if job.MemoryID == nil {
		return PermanentMemory(ErrMemoryInvalid)
	}
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	unlock, err := ingestion.LockResource(ctx, m.repo.db, "memory", *job.MemoryID)
	if err != nil {
		return err
	}
	defer unlock()
	memory, profile, live, err := m.repo.VectorSource(ctx, job)
	if err != nil {
		return err
	}
	var migration struct {
		CleanupPrevious bool   `json:"cleanup_previous"`
		PreviousSpace   string `json:"previous_space"`
	}
	if json.Unmarshal(job.Payload, &migration) != nil {
		return PermanentMemory(ErrMemoryInvalid)
	}
	if migration.CleanupPrevious {
		if migration.PreviousSpace != "" && !validMemorySpace(migration.PreviousSpace) {
			return PermanentMemory(ErrMemoryInvalid)
		}
		// A superseded A->B migration still owns cleanup of A; the later
		// B->C job cannot infer that ancestry. Never delete a space that the
		// current active memory has switched back to. The resource lock keeps
		// this cleanup ordered before a newer vector writer.
		if !(memory.Status == 1 && memory.EmbeddingVersion == migration.PreviousSpace) {
			if err = m.vectors.Delete(ctx, MemoryCollection(migration.PreviousSpace), []int64{memory.ID}); err != nil {
				return err
			}
		}
	}
	if !live {
		if !(memory.Status == 1 && memory.EmbeddingVersion == job.EmbeddingVersion) {
			if err = m.vectors.Delete(ctx, MemoryCollection(job.EmbeddingVersion), []int64{memory.ID}); err != nil {
				return err
			}
		}
		return m.repo.CompleteVector(ctx, job)
	}
	if job.VectorAction == "delete" {
		if err = m.vectors.Delete(ctx, MemoryCollection(job.EmbeddingVersion), []int64{memory.ID}); err != nil {
			return err
		}
		return m.repo.CompleteVector(ctx, job)
	}
	if profile == nil {
		return ErrMemoryConflict
	}
	if err = m.vectors.EnsureCollection(ctx, MemoryCollection(profile.EmbeddingVersion), profile.EmbeddingDimension); err != nil {
		return err
	}
	descriptor, err := m.models.ResolveMemoryEmbedding(ctx, profile)
	if err != nil {
		return err
	}
	if descriptor.ContextWindow > 0 && llm.EstimateTokens(memory.Content) > descriptor.ContextWindow {
		return PermanentMemory(fmt.Errorf("memory exceeds embedding context"))
	}
	values, err := m.gateway.Embed(ctx, descriptor, []string{memory.Content})
	if err != nil {
		return err
	}
	if len(values) != 1 || len(values[0]) != profile.EmbeddingDimension {
		return fmt.Errorf("memory vector dimension mismatch")
	}
	agentID := int64(0)
	if memory.AgentID != nil {
		agentID = *memory.AgentID
	}
	record := vector.Record{ID: memory.ID, Vector: values[0], Metadata: map[string]any{"user_id": memory.UserID, "agent_id": agentID, "revision": memory.Revision, "embedding_version": memory.EmbeddingVersion}}
	if err = m.vectors.Upsert(ctx, MemoryCollection(profile.EmbeddingVersion), []vector.Record{record}); err != nil {
		return err
	}
	return m.repo.CompleteVector(ctx, job)
}
