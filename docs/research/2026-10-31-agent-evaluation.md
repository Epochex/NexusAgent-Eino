# Agent Harness task evaluation

## Purpose and scope

`backend/internal/agent/evaluation` evaluates a fresh production `runtime.Runner`
per task. `backend/cmd/agenteval` exercises that package through the real Eino ADK,
HTTP model gateway, tool-schema adapter and event stream. The bundled local HTTP
peer is deliberately deterministic; it is **controlled integration**, not a model
capability benchmark, SWE-bench run, business task success claim or quality gain.

The fixture peer reads document requests, invokes a document tool, then returns
its JSON result. The degraded peer corrupts the side-effect recovery policy.
The independent JSONL expected fields are used only by the evaluator; they are
not sent to the Runner or HTTP peer. The toy documents are integration fixtures,
not a claim that retrieval relevance or complex development tasks were evaluated.

## Run

From `backend`, with Go 1.26 and the existing module cache:

```sh
go test ./internal/agent/evaluation ./cmd/agenteval -count=1
go test -race ./internal/agent/evaluation ./cmd/agenteval -count=1
go run ./cmd/agenteval --demo --variant baseline --revision YOUR_REV --output ../tmp/agenteval/baseline
go run ./cmd/agenteval --demo --variant degraded --revision YOUR_REV --output ../tmp/agenteval/degraded
go run ./cmd/agenteval --compare-before ../tmp/agenteval/baseline/report.json --compare-after ../tmp/agenteval/degraded/report.json
```

`--suite path.jsonl` replaces the embedded suite, but the CLI remains explicitly
demo-only. To evaluate a real model or application, call `evaluation.Evaluate`
with that application's actual Runner factory, ToolSource and model descriptor;
do not relabel this controlled peer as a real model. The CLI makes no external
model calls and does not consume credentials from the environment.

## Evaluation contract

- Each JSONL task has a unique ID, a family, split (`train`, `dev`, `test`), model
  messages, tool bindings and independently authored `expected.final/tools`.
  Reject duplicate IDs, unknown fields, malformed JSON and the same family
  crossing splits before creating a Runner. Family labels are curator supplied;
  this check cannot detect semantically duplicated tasks with different labels.
  No split is selected implicitly: all supplied tasks execute.
- Fingerprint the complete task definitions, including oracles, with SHA256.
  Code revision, variant, scope and model name accompany every report. Task
  order and message order are significant. Keys are canonicalized by Go JSON;
  numeric spellings such as `1` and `1.0` remain distinct in exact scoring.
- Deep-copy only input messages and tool bindings into each RunInput. A fresh
  Runner prevents state/counter reuse across tasks; a fresh Run/Session ID is
  namespaced by the unique task ID. Real persisted backends should add an
  experiment namespace before using these IDs across separate invocations.
- A pass requires exactly one terminal with `finish_reason=stop` (cancellation,
  truncation and filtering are failures), exact JSON final output,
  expected tool-name/argument multiplicities and one successful result for
  every unique call ID. Tools may execute concurrently: compare multisets and
  join call/result IDs after collection, since Eino callback results can arrive
  before iterator call frames. Do not infer tool order from callback timing.
- All supplied tasks remain in the denominator, including initialization errors,
  protocol failures, timeouts and parent cancellation. Per-task deadlines and
  event caps bound collection. Runner.Run must return promptly and obey context;
  an arbitrary malicious/blocking implementation cannot be killed by a Go
  context. Run untrusted code only through the existing sandbox tool.
- Full observed events and failure reasons are preserved in JSON. HTML is
  escaped and links the human summary to the trace file. Reports contain prompts
  and tool output: keep them private and review/redact before sharing. Model
  descriptors/API keys and expected fields are not serialized as metadata;
  this does not scrub secrets that an application itself emits in tool text.
- Token totals come from the runtime's aggregate usage frame. Missing or all-zero
  usage stays unknown; fixture usage is synthetic. Cost stays unknown unless
  explicitly versioned finite prices are supplied to the evaluation API. Such
  cost is an estimate, not provider billing. No token estimate is inferred from
  text length. Failed tasks retain observed usage instead of being excluded.
- Comparisons require identical suite fingerprints, denominators and task IDs.
  Report improvements and regressions separately, with paired wall-time deltas;
  do not claim reliable latency improvements from these short synthetic runs.

## Engineering choices

1. A reusable Runner adapter rather than a hosted evaluation service keeps this
   repository's own execution path and independent oracle in view; it adds no
   production dependency or provider bill. It lacks hosted annotation/workflows.
2. Exact JSON and deterministic tool evidence rather than LLM-as-judge make
   regression decisions reproducible; they cannot grade open-ended code quality.
   Real development tasks require separate executable tests or human rubrics.
3. Local JSON/HTML traces rather than a new telemetry backend make experiments
   inspectable offline. Existing Langfuse/OTel remains the live tracing system.
4. Test-family isolation reduces direct leakage; chronology, source overlap and
   near-duplicate detection still require curated real-world task data.

## Sources consulted

- Eino ADK: https://www.cloudwego.io/docs/eino/core_modules/eino_adk/
- LangGraph persistence: https://docs.langchain.com/oss/python/langgraph/persistence
- SWE-bench executable evaluation: https://www.swebench.com/SWE-bench/guides/evaluation/
- Langfuse evaluation: https://langfuse.com/docs/evaluation/overview

These sources informed design choices; they are not evidence that this repository
has run their benchmarks or implemented every described capability.
