# Turn-aware model context reduction

## Problem and implementation

The Eino runner already bounded individual tool output and rejected an oversized
model input. A valid multi-turn conversation could therefore stop even when
discarding an old, completed turn would leave sufficient room for the current task.

`compactInputHistory` validates the original message protocol before removing
anything. It removes the oldest complete user turns only until the existing
estimated input budget is met. All system messages, the prefix before the first
user, and the latest user turn remain. A parallel tool call and its results are
removed together with their user turn. Tool definitions and reasoning use the
same accounting as admission checks. Protected content which still exceeds the
budget is rejected; malformed old history cannot be hidden through removal.

The policy runs before initial admission and in the real Eino v0.9.22 ADK
`BeforeModelRewriteState` hook, including subsequent tool iterations. The hook
returns a copied ADK working state with a replacement message slice; it does not
delete caller history or persistent checkpoints. Progress events expose removal
counts and estimated input sizes without message contents.

## Design alternatives

| Concern | Route A | Route B | Choice and trade-off |
| --- | --- | --- | --- |
| Model loop | Eino ADK middleware | Handwritten ReAct loop | Extend the installed ADK so stream, tool and lifecycle behavior retain existing coverage. A handwritten loop offers control but duplicates protocol machinery. |
| Historical context | Deterministic whole-turn eviction | Model-generated summaries | Eviction has no additional inference latency or summary hallucination. It loses old semantic content, so it is not suitable for tasks requiring every old fact without an external store. |
| Long-running state | Existing PostgreSQL run/step/attempt records and fenced workers | Durable workflow orchestration such as Temporal | Keep the existing authoritative state design. Neither route alone makes external non-idempotent effects exactly once; those need separate operation/result reconciliation. |
| Tool contract | Application permissions, policy versions and result records | SDK schema validation alone | Schema validation establishes argument shape; application checks remain necessary for authorization and replay policy. |
| Admission capacity | Shared database quota | Per-process semaphore | The shared quota covers multiple workers; local semaphores still bound within-run tool concurrency. Coordination adds database traffic. |
| Verification | Deterministic protocol peers and state assertions | Real-model task evaluation | This change uses the first route for reproducible failure and correctness checks. Semantic task quality needs a separate real-model evaluation. |

Primary references consulted on 2026-10-31:

- [Anthropic: Effective harnesses for long-running agents](https://www.anthropic.com/research/effective-harnesses-for-long-running-agents)
- [Anthropic: Effective context engineering for AI agents](https://www.anthropic.com/research/effective-context-engineering-for-ai-agents)
- [LangGraph persistence](https://docs.langchain.com/oss/python/langgraph/persistence)
- Installed Eino v0.9.22 `adk/chatmodel.go` and `adk/wrappers.go` for the actual rewrite-state contract.

## Verification performed

Environment: Windows, Go 1.26.3, installed Eino v0.9.22. The existing runner
baseline passed `go test ./internal/agent/runtime ./internal/agent/einoadapter`.

Before implementation, the new first-request and historical parallel-tool tests
failed with budget rejection. The tool-iteration test made the first tool call,
then terminated on budget instead of reaching the second model response.

After implementation:

```
go test ./internal/agent/runtime ./internal/agent/einoadapter -count=1
go test ./... -count=1
go test -race ./internal/agent/runtime ./internal/agent/einoadapter -count=1
```

All passed. The full command was run from `backend` and covers packages included
by the normal local test build; it is not evidence that infrastructure integration
tests requiring Docker or special flags ran. An independent source review also
ran the new focused tests successfully and found no correctness issue.

The new regressions inspect requests received by a real localhost HTTP server
through the installed Eino runner. They check system/current-task retention,
whole parallel-tool-round removal, minimum necessary eviction, caller-history
immutability, tool-schema accounting, malformed-history rejection, and reduction
after a real SDK tool iteration.

## Limits

- The model peer returns controlled responses. No real LLM task-success, factual
  retention, provider billing, or context-window accuracy improvement is claimed.
- Counts are the existing heuristic estimates, not an exact provider tokenizer.
- Old turns are discarded rather than summarized; applications must retain facts
  needed later in task state or retrieve them explicitly.
- This policy covers the Eino ADK runner, not checkpoint compaction for durable
  YAML graph executions.
- Docker/database failure-injection suites were not re-run for this change.
- Event metadata is exercised by the loop but not independently asserted in all
  combinations; no event-count accuracy benchmark is claimed.
