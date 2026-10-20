package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptrace"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chengpeng-cp/nexus-agent/internal/capacity"
	"github.com/chengpeng-cp/nexus-agent/internal/model"
	"gorm.io/gorm"
)

type stageConfig struct {
	Name             string `json:"name"`
	SSE, Slots, Rate int
	Duration         time.Duration
	Idle             time.Duration
}
type stageReport struct {
	Config                          stageConfig `json:"config"`
	Prefix                          string      `json:"prefix"`
	Started, Finished               time.Time
	Arrivals                        arrivalSnapshot         `json:"arrivals"`
	Wire                            int64                   `json:"actual_wire_requests"`
	WirePerSecond                   []int64                 `json:"wire_per_second"`
	WireAfterWindow                 int64                   `json:"wire_after_offer_window"`
	Outcomes                        map[string]int          `json:"http_outcomes"`
	Errors                          map[string]int          `json:"bounded_errors"`
	Latency                         map[string]distribution `json:"latency"`
	Database                        *databaseAudit          `json:"database"`
	PeerMismatches                  int                     `json:"peer_completed_run_mismatches"`
	Peer                            any                     `json:"controlled_peer"`
	SSEOpened, SSEFailures, SSEPeak int64
	Samples                         []resourceSample `json:"resource_samples"`
	Passed                          bool             `json:"passed"`
	Gaps                            []string         `json:"gaps"`
}
type resourceSample struct {
	At                  time.Time
	Phase               string
	SSE                 int64
	Server, Worker      map[string]any
	Outbox, Outstanding int64
	Docker              []map[string]string `json:"docker_stats,omitempty"`
	Error               string              `json:"error,omitempty"`
}
type stageStats struct {
	sync.Mutex
	outcomes, errors map[string]int
	latency          map[string][]float64
	acknowledged     map[int64]bool
}

func (s *stageStats) latencyValue(name string, value float64) {
	s.Lock()
	defer s.Unlock()
	s.latency[name] = append(s.latency[name], value)
}
func (s *stageStats) outcome(name string, err error) {
	s.Lock()
	defer s.Unlock()
	s.outcomes[name]++
	if err != nil && len(s.errors) < 32 {
		s.errors[err.Error()]++
	}
}
func writeReport(path string, value any) error {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	temp := path + ".tmp"
	if err = os.WriteFile(temp, raw, 0600); err != nil {
		return err
	}
	return os.Rename(temp, path)
}
func getJSON(ctx context.Context, client *http.Client, url string, out any) error {
	req, _ := http.NewRequestWithContext(ctx, "GET", url, nil)
	res, err := client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		return fmt.Errorf("observation status %d", res.StatusCode)
	}
	return json.NewDecoder(io.LimitReader(res.Body, 32<<20)).Decode(out)
}
func updateStageCapacity(ctx context.Context, api *apiClient, token string, slots int) error {
	var snapshot capacity.Snapshot
	if err := api.call(ctx, "GET", "/admin/capacity", token, nil, &snapshot); err != nil {
		return err
	}
	for _, p := range snapshot.Policies {
		p.ActiveLimit = int64(slots)
		p.OwnerActiveLimit = 4
		if p.OwnerActiveLimit > p.ActiveLimit {
			p.OwnerActiveLimit = p.ActiveLimit
		}
		if err := api.call(ctx, "PUT", "/admin/capacity/"+p.Scope, token, p, nil); err != nil {
			return err
		}
	}
	return nil
}
func seedRun(ctx context.Context, api *apiClient, actor loadActor, key string) (model.WorkflowRun, error) {
	var run model.WorkflowRun
	err := api.call(ctx, "POST", "/workflows/run", actor.Token, map[string]any{"workflow_id": actor.WaitFlowID, "input": map[string]any{}, "request_key": key}, &run)
	return run, err
}
func waitApproval(ctx context.Context, api *apiClient, actor loadActor, id int64) (model.WorkflowRun, error) {
	for {
		var run model.WorkflowRun
		if err := api.call(ctx, "GET", fmt.Sprintf("/workflows/runs/%d", id), actor.Token, nil, &run); err != nil {
			return run, err
		}
		if run.Status == "waiting_approval" && len(run.Approvals) > 0 {
			return run, nil
		}
		select {
		case <-ctx.Done():
			return run, ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}
func runStage(parent context.Context, fixturePath, out string, cfg stageConfig) error {
	if cfg.Name == "" || cfg.SSE < 32 || cfg.SSE > 500 || cfg.Slots < 1 || cfg.Rate < 1 || cfg.Duration <= 0 {
		return fmt.Errorf("invalid stage configuration")
	}
	f, err := readFixture(fixturePath)
	if err != nil {
		return err
	}
	if len(f.Actors) != 16 {
		return fmt.Errorf("bootstrap fixture first")
	}
	api := newAPIClient(f.API)
	api.http.Transport.(*http.Transport).MaxConnsPerHost = 2048
	api.http.Transport.(*http.Transport).MaxIdleConnsPerHost = 1024
	defer api.http.CloseIdleConnections()
	// Prevent credentials from a previous retained test schema silently selecting
	// another actor whose numeric ID happens to match.
	var me struct{ Username string }
	if err = api.call(parent, "GET", "/me", f.Actors[0].Token, nil, &me); err != nil {
		return err
	}
	if me.Username != f.Actors[0].Username {
		return fmt.Errorf("fixture/API schema mismatch")
	}
	db, err := fixtureDB(parent, f.Schema)
	if err != nil {
		return err
	}
	sqlDB, _ := db.DB()
	defer sqlDB.Close()
	if err = updateStageCapacity(parent, api, f.Actors[0].Token, cfg.Slots); err != nil {
		return err
	}
	setup, cancelSetup := context.WithTimeout(parent, 3*time.Minute)
	defer cancelSetup()
	prefix := "load-" + f.SuiteID + "-" + cfg.Name + "-"
	if !auditPrefixPattern.MatchString(prefix) {
		return fmt.Errorf("invalid stage name")
	}
	var existing int64
	db.WithContext(setup).Table("workflow_runs").Where("request_key LIKE ?", prefix+"%").Count(&existing)
	if existing != 0 {
		return fmt.Errorf("stage namespace already used")
	}
	seedCount := cfg.SSE + 2*(int(cfg.Duration/(10*time.Second))+2)
	seeds := make([]streamTarget, seedCount)
	for i := range seeds {
		actor := f.Actors[i%len(f.Actors)]
		run, e := seedRun(setup, api, actor, fmt.Sprintf("seed-%s-%s-%d", f.SuiteID, cfg.Name, i))
		if e != nil {
			return e
		}
		seeds[i] = streamTarget{id: run.ID, actor: actor}
	}
	// Establish actual waiting jobs before timing; do not manufacture fake SSE.
	for _, seed := range seeds {
		if _, err = waitApproval(setup, api, seed.actor, seed.id); err != nil {
			return err
		}
	}
	stats := &stageStats{outcomes: map[string]int{}, errors: map[string]int{}, latency: map[string][]float64{}, acknowledged: map[int64]bool{}}
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	streamCtx, stopStreams := context.WithCancel(ctx)
	defer stopStreams()
	pool := newStreamPool(streamCtx, api, seeds[:cfg.SSE], stats.latencyValue)
	for i := range pool.slots {
		if err = pool.change(i, &pool.seeds[i], false); err != nil {
			return err
		}
	}
	ready := time.Now().Add(20 * time.Second)
	for pool.active.Load() != int64(cfg.SSE) && time.Now().Before(ready) {
		time.Sleep(20 * time.Millisecond)
	}
	if pool.active.Load() != int64(cfg.SSE) {
		return fmt.Errorf("SSE fixture failed to establish requested connections: %d/%d", pool.active.Load(), cfg.SSE)
	}
	report := &stageReport{Config: cfg, Prefix: prefix, Started: time.Now().UTC()}
	resourceFile, err := os.OpenFile(out+".resources.jsonl", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer resourceFile.Close()
	var wire, chatActive atomic.Int64
	var wireAfter atomic.Int64
	wireBuckets := make([]atomic.Int64, int((cfg.Duration+time.Second-1)/time.Second))
	var samplesMu sync.Mutex
	sampleDone := make(chan struct{})
	sampleCtx, stopSamples := context.WithCancel(ctx)
	defer stopSamples()
	phase := atomic.Value{}
	phase.Store("load")
	go func() {
		defer close(sampleDone)
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			sample := collectResources(sampleCtx, api.http, db, pool.active.Load()+chatActive.Load(), phase.Load().(string))
			if sampleCtx.Err() != nil {
				return
			}
			samplesMu.Lock()
			report.Samples = append(report.Samples, sample)
			samplesMu.Unlock()
			_ = json.NewEncoder(resourceFile).Encode(sample)
			_ = resourceFile.Sync()
			select {
			case <-sampleCtx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	controlCtx, stopControls := context.WithCancel(ctx)
	defer stopControls()
	controlsDone := make(chan struct{})
	go func() { defer close(controlsDone); controlLoop(controlCtx, api, seeds[cfg.SSE:], stats) }()
	// Keep request deadlines independent of the offer window so accepted work can
	// drain. The scheduler never waits for any response or a client slot.
	arrival, err := scheduleArrivals(ctx, cfg.Rate, cfg.Duration, 1024, func(parent context.Context, index int64, scheduled time.Time) error {
		callCtx, stop := context.WithTimeout(parent, 90*time.Second)
		defer stop()
		nonce := fmt.Sprintf("%s%08d", prefix, index)
		actor := f.Actors[index%int64(len(f.Actors))]
		trace := &httptrace.ClientTrace{WroteRequest: func(info httptrace.WroteRequestInfo) {
			if info.Err == nil {
				wire.Add(1)
				elapsed := time.Since(report.Started)
				bucket := int(elapsed / time.Second)
				if bucket >= 0 && bucket < len(wireBuckets) {
					wireBuckets[bucket].Add(1)
				} else {
					wireAfter.Add(1)
				}
				stats.latencyValue("wire_schedule_lag", float64(time.Since(scheduled))/1e6)
			}
		}}
		callCtx = httptrace.WithClientTrace(callCtx, trace)
		if index%5 == 4 {
			slot := int(index % 16)
			if e := pool.change(slot, nil, true); e != nil {
				stats.outcome("client_sse_slot_error", e)
				return e
			}
			defer pool.resume(slot)
			payload, _ := json.Marshal(map[string]any{"agent_id": actor.AgentID, "content": "nonce=" + nonce})
			req, _ := http.NewRequestWithContext(callCtx, "POST", api.base+"/api/v1/chat/stream", bytes.NewReader(payload))
			req.Header.Set("Authorization", "Bearer "+actor.Token)
			req.Header.Set("Content-Type", "application/json")
			res, e := api.http.Do(req)
			if e != nil {
				stats.outcome("chat_transport_unknown", e)
				return e
			}
			defer res.Body.Close()
			if res.StatusCode != 200 {
				e = &apiFailure{Status: res.StatusCode, Path: "/chat/stream"}
				if res.StatusCode == 429 {
					stats.outcome("chat_rejected", nil)
					return nil
				}
				stats.outcome("chat_http_error", e)
				return e
			}
			chatActive.Add(1)
			defer chatActive.Add(-1)
			stats.outcome("chat_accepted", nil)
			first := false
			result, e := readEvents(res.Body, func(name string, data []byte) {
				if !first && isContentDelta(name, data) {
					first = true
					stats.latencyValue("chat_first_token", float64(time.Since(scheduled))/1e6)
				}
			})
			stats.latencyValue("chat_end_to_end", float64(time.Since(scheduled))/1e6)
			if e != nil || !result.Terminal {
				stats.outcome("chat_missing_terminal", e)
				return fmt.Errorf("chat missing terminal")
			}
			if result.Failed {
				stats.outcome("chat_failed", nil)
				return fmt.Errorf("chat error terminal")
			}
			stats.outcome("chat_done", nil)
			return nil
		}
		var run model.WorkflowRun
		e := api.call(callCtx, "POST", "/workflows/run", actor.Token, map[string]any{"workflow_id": actor.FlowID, "input": map[string]any{"nonce": nonce}, "request_key": nonce}, &run)
		stats.latencyValue("workflow_submit", float64(time.Since(scheduled))/1e6)
		if e != nil {
			var failure *apiFailure
			if errors.As(e, &failure) && failure.Status == 429 {
				stats.outcome("workflow_rejected", nil)
				return nil
			}
			stats.outcome("workflow_transport_or_http_unknown", e)
			return e
		}
		stats.Lock()
		stats.acknowledged[run.ID] = true
		stats.Unlock()
		stats.outcome("workflow_accepted", nil)
		slot := 16 + int(index%int64(cfg.SSE-16))
		if e = pool.change(slot, &streamTarget{id: run.ID, actor: actor, primary: true, scheduled: scheduled}, false); e != nil {
			stats.outcome("workflow_subscription_error", e)
		}
		return e
	})
	if err != nil {
		return err
	}
	stopControls()
	<-controlsDone
	phase.Store("drain")
	drainCtx, stopDrain := context.WithTimeout(ctx, 2*time.Minute)
	err = arrival.wait(drainCtx)
	stopDrain()
	if err != nil {
		report.Gaps = append(report.Gaps, "client callbacks failed to drain within 2m")
	}
	deadline := time.Now().Add(90 * time.Second)
	for {
		stats.Lock()
		ack := make(map[int64]bool, len(stats.acknowledged))
		for id, v := range stats.acknowledged {
			ack[id] = v
		}
		stats.Unlock()
		report.Database, err = auditDatabase(ctx, db, prefix, ack)
		if err != nil {
			return err
		}
		pending := 0
		for state, n := range report.Database.WorkflowStates {
			if state != "succeeded" && state != "failed" && state != "canceled" && state != "needs_attention" {
				pending += n
			}
		}
		if pending == 0 || time.Now().After(deadline) {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	stopStreams()
	for _, slot := range pool.slots {
		slot.mu.Lock()
		if slot.cancel != nil {
			slot.cancel()
		}
		done := slot.done
		slot.mu.Unlock()
		if done != nil {
			<-done
		}
	}
	// Cancel only this stage's deliberately waiting fixtures after subscribers
	// close. These control calls are outside the measured load/control samples.
	for _, seed := range seeds {
		_ = api.call(ctx, "POST", fmt.Sprintf("/workflows/runs/%d/cancel", seed.id), seed.actor.Token, map[string]any{}, nil)
	}
	if cfg.Idle > 0 {
		phase.Store("idle")
		timer := time.NewTimer(cfg.Idle)
		select {
		case <-ctx.Done():
			timer.Stop()
		case <-timer.C:
		}
	}
	stopSamples()
	<-sampleDone
	report.Finished = time.Now().UTC()
	report.Arrivals = arrival.snapshot()
	report.Wire = wire.Load()
	for i := range wireBuckets {
		report.WirePerSecond = append(report.WirePerSecond, wireBuckets[i].Load())
	}
	report.WireAfterWindow = wireAfter.Load()
	report.SSEOpened = pool.opened.Load()
	report.SSEFailures = pool.failures.Load()
	report.SSEPeak = pool.peak.Load()
	report.Outcomes = stats.outcomes
	report.Errors = stats.errors
	report.Latency = map[string]distribution{}
	for name, values := range stats.latency {
		report.Latency[name] = summarize(values)
	}
	var peer struct {
		Nonces map[string]peerNonce `json:"nonces"`
	}
	obsCtx, obsCancel := context.WithTimeout(ctx, 10*time.Second)
	err = getJSON(obsCtx, api.http, f.PeerAPI+"/stats?prefix="+prefix, &peer)
	obsCancel()
	if err != nil {
		report.Gaps = append(report.Gaps, "controlled peer observation unavailable")
	} else {
		report.Peer = peer
		for _, nonce := range report.Database.CompletedNonces {
			p := peer.Nonces[nonce]
			if p.Models != 2 || p.ModelCompleted != 2 || p.Tools != 1 || p.ToolCompleted != 1 {
				report.PeerMismatches++
			}
		}
	}
	assessStage(report)
	if err = writeReport(out, report); err != nil {
		return err
	}
	fmt.Printf("stage=%s offered=%d workflow=%d chat=%d passed=%t report=%s\n", cfg.Name, report.Wire, report.Database.WorkflowTotal, report.Database.ChatTotal, report.Passed, out)
	return nil // a failed target is a recorded experimental result, not a hidden runner crash
}
func collectResources(parent context.Context, client *http.Client, db *gorm.DB, sse int64, phase string) resourceSample {
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	r := resourceSample{At: time.Now().UTC(), Phase: phase, SSE: sse}
	if e := getJSON(ctx, client, "http://127.0.0.1:16060/runtime", &r.Server); e != nil {
		r.Error = "server diagnostics unavailable"
	}
	if e := getJSON(ctx, client, "http://127.0.0.1:16061/runtime", &r.Worker); e != nil {
		r.Error += "; worker diagnostics unavailable"
	}
	if db.WithContext(ctx).Table("execution_outbox").Where("published_at IS NULL").Count(&r.Outbox).Error != nil {
		r.Error += "; PG observation unavailable"
	}
	_ = db.WithContext(ctx).Table("workflow_runs").Where("definition IS NOT NULL AND status NOT IN ('succeeded','failed','canceled')").Count(&r.Outstanding).Error
	output, err := exec.CommandContext(ctx, "docker", "stats", "--no-stream", "--format", "{{json .}}", "nexus-eino-capacity-server-1", "nexus-eino-capacity-worker-1", "nexus-eino-capacity-peer-1", "nexus-eino-acceptance-postgres-1", "nexus-eino-acceptance-redis-1", "nexus-eino-acceptance-milvus-1", "nexus-eino-acceptance-minio-1", "nexus-eino-acceptance-docparser-1", "nexus-eino-acceptance-etcd-1", "nexus-eino-acceptance-milvus-minio-1").Output()
	if err != nil {
		if parent.Err() == nil {
			r.Error += "; Docker CPU/memory observation unavailable"
		}
	} else {
		for _, line := range strings.Split(strings.TrimSpace(string(output)), "\n") {
			var row map[string]string
			if json.Unmarshal([]byte(line), &row) == nil {
				r.Docker = append(r.Docker, row)
			}
		}
	}
	return r
}
func controlLoop(ctx context.Context, api *apiClient, seeds []streamTarget, stats *stageStats) {
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	i := 0
	control := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		if len(seeds) == 0 {
			return
		}
		target := seeds[min(control, len(seeds)-1)]
		start := time.Now()
		requestCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err := api.call(requestCtx, "GET", fmt.Sprintf("/workflows/runs/%d", target.id), target.actor.Token, nil, nil)
		cancel()
		if err != nil && ctx.Err() != nil {
			return
		}
		stats.latencyValue("control_query", float64(time.Since(start))/1e6)
		if err != nil && ctx.Err() == nil {
			stats.outcome("control_error", err)
		} else if err == nil {
			stats.outcome("control_query_ok", nil)
		}
		i++
		if i%10 != 0 || control+1 >= len(seeds) {
			continue
		}
		for _, approve := range []bool{true, false} {
			target = seeds[control]
			control++
			requestCtx, cancel = context.WithTimeout(ctx, 5*time.Second)
			start = time.Now()
			name := "control_cancel"
			if approve {
				var run model.WorkflowRun
				err = api.call(requestCtx, "GET", fmt.Sprintf("/workflows/runs/%d", target.id), target.actor.Token, nil, &run)
				if err == nil && len(run.Approvals) > 0 {
					a := run.Approvals[0]
					start = time.Now()
					err = api.call(requestCtx, "POST", fmt.Sprintf("/workflows/runs/%d/approve", target.id), target.actor.Token, map[string]any{"interrupt_id": a.InterruptID, "args_hash": a.ArgsHash, "approved": true}, nil)
				} else if err == nil {
					err = fmt.Errorf("control approval missing")
				}
				name = "control_approve"
			} else {
				err = api.call(requestCtx, "POST", fmt.Sprintf("/workflows/runs/%d/cancel", target.id), target.actor.Token, map[string]any{}, nil)
			}
			cancel()
			stats.latencyValue(name, float64(time.Since(start))/1e6)
			if err != nil && ctx.Err() == nil {
				stats.outcome("control_error", err)
			} else if err == nil {
				stats.outcome(name+"_ok", nil)
			}
		}
	}
}
func assessStage(r *stageReport) {
	if r.WireAfterWindow > 0 {
		r.Gaps = append(r.Gaps, "actual HTTP offers occurred after the scheduled arrival window")
	}
	if r.Outcomes["workflow_rejected"]+r.Outcomes["chat_rejected"] > 0 {
		r.Gaps = append(r.Gaps, "admission rejected offered traffic; this stage does not sustain the full offered rate")
	}
	if r.Wire != r.Arrivals.Target || r.Arrivals.SkippedBusy+r.Arrivals.SkippedLate+r.Arrivals.NotScheduled != 0 {
		r.Gaps = append(r.Gaps, "open arrival delivery below target")
	}
	if r.PeerMismatches > 0 || r.Database.InvalidWorkflowResults+r.Database.InvalidChatResults+r.Database.DuplicateChatNonces > 0 {
		r.Gaps = append(r.Gaps, "invalid/duplicate results or incorrect model/tool execution count")
	}
	if r.Database.WorkflowTotal+r.Database.ChatTotal != int(r.Wire)-r.Outcomes["workflow_rejected"]-r.Outcomes["chat_rejected"] {
		r.Gaps = append(r.Gaps, "wire/DB/rejection accounting mismatch")
	}
	failed := r.Database.WorkflowTotal - r.Database.WorkflowStates["succeeded"] + r.Database.ChatTotal - r.Database.ChatStates["done"]
	if float64(failed)/float64(max(1, r.Wire)) >= .001 {
		r.Gaps = append(r.Gaps, "valid request completion error rate >= 0.1%")
	}
	if r.Outcomes["control_error"] > 0 || r.SSEFailures > 0 {
		r.Gaps = append(r.Gaps, "control or SSE observation failed")
	}
	for _, name := range []string{"control_query", "control_approve", "control_cancel"} {
		d := r.Latency[name]
		if d.Count == 0 || d.P95 >= 150 || d.P99 >= 500 {
			r.Gaps = append(r.Gaps, name+" missing or above latency target")
		}
	}
	r.Passed = len(r.Gaps) == 0
}
