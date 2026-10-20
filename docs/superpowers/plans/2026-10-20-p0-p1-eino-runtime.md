# P0/P1 Eino 执行内核 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 使原平台通过 Eino 执行普通 Agent 与专家 Agent，修复模型流错误/额度生命周期，形成可取消、有权限边界、有界资源和正确终态的首个可运行版本。

**Architecture:** 保留 Hertz、service、现有模型网关与工具执行器；增加不可变 Eino 模型适配器、受控工具适配器和实现原 Runner 接口的 EinoRunner。先修好流式网关，再迁移执行入口和事件生命周期；持久工作流、检查点、事件重放及跨 worker 配额在 P2/P4 独立交付，不在本阶段声称完成。

**Tech Stack:** Go >= 1.26.0、Hertz v0.10.6、Eino v0.9.22、PostgreSQL、既有 React/TypeScript 前端；本阶段不引入 eino-ext 或额外模型 SDK。

**Spec:** `docs/superpowers/specs/2026-10-20-nexus-agent-eino-design.md`，用户于 2026-10-20 确认采用。

## Global Constraints

- 保留 Agent、RAG、Host/Expert、YAML 工作流、HITL、MCP、A2A、记忆及观测能力；不另换订单等行业场景。
- Eino 主模块首先锁定 `v0.9.22`，以对应 tag 源码核对接口；不引用浮动 main。常规模型消息路径满足本轮需求，不要求 Agentic Beta。
- Go 首先保持上游 `1.26.0` 下限，CI 与本地实际工具链版本必须记录。
- 维持模块化单体 server 与可扩展 worker；生产路径统一到一个 Eino Agent runtime，不长期维护两套 ReAct。`agent/naive` 保留教学用途。
- 只有完成相应门槛才能称该模块经过生产工程验证；实际生产运行规模需要真实部署证据。
- 参考负载目标为 server+worker 合计 4 vCPU/8 GiB、200 SSE 订阅、32 执行槽、每秒 10 个新运行，持续 30 分钟；完整容量验证属于 P4，P1 只记录微基准及资源回收，不冒称已达到该目标。
- 原 Go module/import 路径在 P0/P1 保留，避免与行为迁移混入全仓路径重写；远端仓库和上游归属已通过 origin/upstream、README、UPSTREAM.md 区分。
- 本阶段敏感工具没有持久审批时必须拒绝执行；不得因为尚未实施 P2 就绕过审批。现有 YAML 工作流的完整工具策略与持久审批属于 P2，整个平台在此之前不标记为生产完成。

## Review Focus

- 上游在输出部分文本后断流：不得变成正常完成或切换备用模型重复输出；Task 2、Task 5 覆盖。
- 两个用户同时运行不同工具集：不可变绑定不得串用；Task 3、Task 4 覆盖。
- 客户端不再消费、关闭连接或取消：模型体、转发协程、额度必须在有界时间内释放；Task 2、Task 5、Task 7 覆盖。
- Provider 的 usage 为累计值且工具调用分片/多调用交错：不能重复统计或损坏调用参数；Task 2、Task 3、Task 5 覆盖。
- 消息持久化失败、Host 专家失败及模型空回答：不能发出误导成功终态；Task 5、Task 6、Task 7 覆盖。

## 文件与依赖地图

新增 `backend/internal/agent/einoadapter/`，只依赖 llm/tool/security 和 Eino；runtime 引用它，adapter 不反向依赖 runtime。runtime 中新增 runner/events/limits 文件，host 仍只依赖原 Runner。service 负责消息持久化，api 负责连接取消，不把数据库塞入 adapter。

测试桩使用标准库 `httptest` 实际 HTTP/SSE；关键消息落库测试使用真实 PostgreSQL。回归文件保留在所属包，通用模型桩放 `backend/internal/testutil/llmstub/`。所有实现任务先写失败测试，运行确认失败，再实现和验证；不以大量与实现同构的 mock 测试充数。

---

### Task 1: 建立可重复构建与上游基线

**Files:**
- Create: `.github/workflows/ci.yml`
- Create: `docs/research/2026-10-20-baseline.md`
- Modify: `.gitignore`（开放 research；忽略 bin、覆盖率、node_modules、临时基准结果与本地配置）
- Modify: `README.md`（实际运行命令与状态）
- Modify only if baseline requires: `backend/go.mod`, `backend/go.sum` 或阻塞编译的源码；每项独立记录原因。

**Interfaces:**
- Consumes: 上游 `go.mod`、`frontend/package-lock.json`、server/worker/cli/migrate 入口。
- Produces: 可运行的 Go/npm 环境、无外部模型密钥的 CI、成功/失败明确的基线记录。

- [ ] **Step 1: 运行并记录原始基线。** 检查本机可用 Go/Node/Docker；Go 不存在时在仓库外安装官方 Go 1.26.0 工具链，核对官方 SHA256，避免覆写系统 PATH。启动已有 Docker Desktop 并检查 engine；不删除现有容器/卷。执行 `go version`、`go test ./... -timeout 120s`、`go build ./cmd/server ./cmd/worker ./cmd/cli ./cmd/migrate`（backend），及 `npm ci`、`npm run typecheck`、`npm run build`（frontend）。保留首个失败而非先静默升级依赖。
- [ ] **Step 2: 修复实际阻塞并复测。** 仅处理上述命令暴露的构建/依赖问题；若第三方版本不可取得，记录官方可用版本、API 差异和最小兼容改动。不得直接把所有依赖升级到 latest。
- [ ] **Step 3: 编写 CI 与基线记录。** Linux runner 配置 Go 1.26.0、Node 22；`go vet ./...`、`go test -race ./... -timeout 180s`、四入口 build、前端 npm ci/typecheck/build。锁文件必须保持可复现。需要真实服务的测试用 `integration` tag 区分，并在 Task 7 增加真实 PostgreSQL job；不得默认 skip 掉现有纯单测。
- [ ] **Step 4: 验证和提交。** 重跑实际适用命令，`git diff --check`；提交 `build: establish reproducible upstream baseline`。基线文档分别列出已跑、失败和未跑，不能把 CI 文件存在当成 CI 通过。

### Task 2: 修复模型流错误、连接与并发额度生命周期

**Files:**
- Modify: `backend/internal/llm/{types.go,openai_compatible.go,gateway.go}`
- Create: `backend/internal/llm/{stream_test.go,gateway_stream_test.go}`
- Create: `backend/internal/testutil/llmstub/server.go`

**Interfaces:**
- Consumes: 既有 `Provider.ChatStream`、`Gateway.ChatStream` 和 `Semaphore`。
- Produces: `StreamEvent.Err error`（内部错误，不对 JSON 序列化）；现有 Chat/ChatStream 对外签名不变。新增 `func (g *Gateway) guardStream(ctx context.Context, desc *ModelDescriptor, req *ChatRequest) (<-chan StreamEvent, error)`，拥有从上游请求开始到流关闭/取消的额度与观测生命周期。
- Produces test utility: `func New(t testing.TB, handler http.HandlerFunc) *httptest.Server`，通过调用方 handler 描述真实 SSE/错误，不定义业务专用脚本语言。

- [ ] **Step 1: 写失败测试。** `TestStreamReportsUnexpectedEOF`（已输出文本但无合法 finish 即 EOF，收到 error）；`TestStreamPropagatesUpstreamError`（200 响应中的 error 帧）；`TestStreamToolCallsByIndex`（index 0/2 的交错参数能稳定聚合，无丢调用）；`TestStreamRejectsOversizedFrame`（超过 512 KiB 帧明确报错）；`TestStreamUsageNotDuplicated`（尾 usage 正确）；`TestGatewayStreamHoldsPermit`（额度 1，第一个流未结束时第二个不能进入服务端）；`TestGatewayStreamCancelReleasesPermit`；`TestGatewayDoesNotFallbackAfterPartialOutput`；`TestProviderConcurrentClientInitialization`（并发请求下 race 不报错）。
- [ ] **Step 2: 验证红灯。** backend 执行 `go test ./internal/llm -run 'Test(Stream|Gateway|Provider)' -count=1 -timeout 30s`；编译缺 Err 或断流被当成功均属于预期失败，记录实际结果。
- [ ] **Step 3: 实现错误协议及完整流保护。** 注释/空 SSE 行可忽略，非法 data JSON、error 帧、scanner.Err、缺结束语义 EOF 必须报错；已有合法 finish 后 EOF 可以结束。ctx 取消可停止所有发送并关闭响应体。共享并复用线程安全 HTTP client/transport；请求级 timeout 由 ctx 控制。模型并发额度在整个流期间持有，结束只释放一次；span/熔断/latency 统计流的最终结果。初始请求失败可按原策略 fallback，任何已输出流错误不重放。`spanOnClose` 不得留下无 ctx 退出分支的发送。
- [ ] **Step 4: 验证绿灯。** `go test -race ./internal/llm -count=1 -timeout 60s`；测试用同步屏障而非靠长 sleep 判断并发。
- [ ] **Step 5: 提交。** `fix: preserve model stream failures and permit lifetime`。

### Task 3: 实现不可变的 Eino 模型与消息适配

**Files:**
- Modify: `backend/go.mod`, `backend/go.sum`
- Create: `backend/internal/agent/einoadapter/{model.go,message.go,model_test.go,message_test.go}`

**Interfaces:**
- Consumes: `GatewayClient`：`Chat(context.Context,*llm.ModelDescriptor,*llm.ModelDescriptor,*llm.ChatRequest)(*llm.ChatResponse,*llm.ModelDescriptor,error)` 与同参数 `ChatStream(...)(<-chan llm.StreamEvent,*llm.ModelDescriptor,error)`。
- Produces: `NewChatModel(g GatewayClient, primary, fallback *llm.ModelDescriptor, stepTimeout time.Duration) (model.ToolCallingChatModel, error)`。
- Produces: `ToMessages([]llm.Message) ([]*schema.Message,error)` 与 `FromMessage(*schema.Message) (llm.Message,error)`；模型实现 tag 中的 Generate、Stream、WithTools，并支持 ADK 使用的 `model.WithTools` call option。

- [ ] **Step 1: 写失败测试并锁版。** 添加 Eino v0.9.22；`TestMessageRoundTripToolCalls` 检查 role/content/reasoning/name/call ID/args；`TestWithToolsIsImmutable` 并行绑定不同工具集不能互相覆盖；`TestOptionsCopyDescriptors` 调用选项不修改共享 primary/fallback；`TestModelStreamPreservesErrors` 将 Task 2 Err 转成 StreamReader.Recv error；`TestModelStreamCloseCancelsUpstream`；`TestModelUsesFallbackIdentity` 用量标记实际模型。未支持的多模态、stop/tool-choice 选项明确拒绝，不能静默忽略。
- [ ] **Step 2: 验证红灯。** `go test ./internal/agent/einoadapter -count=1` 观察未实现接口/转换的失败。
- [ ] **Step 3: 实现适配。** WithTools 返回新实例并拷贝 schema/配置，不能修改接收者；Stream 用 `schema.Pipe` 桥接且消费者关闭时取消子 ctx。完整 ToolCalls 只转发一次，保留 index/ID 及 usage/finish。Generate/Stream 共同执行单步 timeout。模型请求中的模型名、采样和输出上限由不可变副本承载；不额外启用 ADK retry/failover，避免与网关双重重试。
- [ ] **Step 4: 验证绿灯。** `go test -race ./internal/agent/einoadapter ./internal/llm -count=1 -timeout 60s`；`go mod tidy` 后检查变更没有浮动版本或未经使用的 eino-ext。
- [ ] **Step 5: 提交。** `feat: adapt model gateway to Eino immutable chat models`。

### Task 4: 工具适配的允许列表、参数与敏感操作边界

**Files:**
- Create: `backend/internal/agent/einoadapter/{tools.go,tools_test.go}`
- Modify: `backend/internal/tool/{executor.go,executor_impl.go}`（新增 context-aware ListContext，保留 List 兼容现有调用）
- Create: `backend/internal/tool/executor_context_test.go`
- Modify: `backend/internal/llm/types.go`（ToolDef 新增 `Dangerous bool`，仅为内部执行 metadata，不发送为模型工具 Schema 字段）
- Modify: `backend/go.mod`, `backend/go.sum`（参数校验锁定 `github.com/santhosh-tekuri/jsonschema/v6 v6.0.3`）

**Interfaces:**
- Produces: `ToolSource` interface：`ListContext(context.Context,string)([]llm.ToolDef,error)`、现有 `Invoke(context.Context,string,string,string) tool.ToolResult`。
- Produces: `BuildTools(ctx context.Context, source ToolSource, bindings string, dangerous map[string]bool, timeout time.Duration, maxOutputBytes int) ([]einotool.BaseTool,error)`；`ListContext` 必须由既有 Executor 暴露并贯穿 repo 查询。
- Tools 在本阶段只允许绑定中明确授权的、非危险工具执行；危险集合由服务端解析，模型无权设置。`ProdExecutor.ListContext` 将权威 `model.Tool.IsDangerous` 写入 `llm.ToolDef.Dangerous`；BuildTools 使用权威标记与 caller map 的并集。定义缺失/查询失败拒绝，不能仅依靠 caller 提供的 map。

- [ ] **Step 1: 写失败测试。** `TestToolRejectsUnboundName`（不能因 registry 有 handler 而执行未绑定工具）；`TestToolSchemaRejectsInvalidArgs`（required/type/additionalProperties）；`TestToolDangerousFailsClosed`（无审批时 handler 调用数为 0，map 为空但权威定义危险也拒绝）；`TestToolLimitAndTimeout`（输出超过 64 KiB 返回明确截断标记，超时分类正确）；`TestToolBindingsIsolatedAcrossRuns`；`TestListContextCancellation`（repo 收到已取消 ctx，不能使用 Background 绕过）。
- [ ] **Step 2: 验证红灯。** `go test ./internal/agent/einoadapter ./internal/tool -count=1`。
- [ ] **Step 3: 实现。** BuildTools 只产出允许列表工具，JSON Schema 在调用前校验；服务端权威危险标记附在绑定快照，拒绝执行时返回可识别的 approval-required 错误观察，不调用 handler。明确 Schema 方言，未知/不支持的规则或远程 $ref 拒绝装载，不进行任意网络解析。错误观察不终止所有可纠正工具调用，权限/审批失败不能重试成允许。ListContext 由兼容 List 委派；旧 Workflow executor 调用在此阶段不加“空绑定允许所有”的后门。
- [ ] **Step 4: 验证绿灯。** `go test -race ./internal/agent/einoadapter ./internal/tool -count=1 -timeout 60s`；原有工作流 build 保持通过。
- [ ] **Step 5: 提交。** `feat: enforce tool boundaries in Eino execution`。

### Task 5: EinoRunner、完整输入预算与有界事件流

**Files:**
- Create: `backend/internal/agent/runtime/{eino_runner.go,eino_events.go,limits.go,eino_runner_test.go,eino_events_test.go,limits_test.go}`
- Modify: `backend/internal/agent/runtime/runner.go`（只扩充 RunInput；暂保留旧入口以保证本任务独立可构建，Task 6 切换调用方后删除旧实现）
- Modify: `backend/internal/agent/event.go`

**Interfaces:**
- Produces: `EinoConfig {StepTimeout,RunTimeout,SendTimeout time.Duration; InputTokenBudget,MaxOutputBytes,EventBuffer,MaxConcurrentTools int}`；默认 EventBuffer=128、SendTimeout=2s、MaxOutputBytes=65536、MaxConcurrentTools=4、InputTokenBudget=128000。
- Produces: `NewEinoRunner(g einoadapter.GatewayClient, source einoadapter.ToolSource, cfg EinoConfig) (*EinoRunner,error)`，实现原 `Run(context.Context,*RunInput)(<-chan agent.Event,error)`。
- Extend: `RunInput.RunID string`；`agent.Event.RunID string`、`Seq uint64`、`At time.Time`，不改既有 Type/Data 字段含义。
- Produces internal helper: `validateInputBudget(messages []*schema.Message, tools []*schema.ToolInfo, budget int) error`；`eventSink` 内部串行化事件，`Send(context.Context,agent.Event) error`、`Close()`。完整串行标识在 service/host 汇总出口重新赋值，不能把多个专家内部 seq 原样混成同一序列。

- [ ] **Step 1: 写失败测试。** `TestEinoRunnerReActRoundTrip`（第一次模型请求工具，第二次能看到配对结果，最终答案/usage/done）；`TestEinoRunnerIterationLimit`（恰好达到模型调用上限即失败）；`TestEinoRunnerEmptySuccessfulAnswer`（合法 stop 的空内容不误判迭代耗尽）；`TestEinoRunnerUpstreamPartialFailure`（部分文本后 error，不能 done）；`TestEinoRunnerInputNotMutated`；`TestBudgetIncludesSystemRAGAndTools`；`TestBudgetKeepsToolPairs`；`TestEinoEventsSlowConsumerTerminates`（不消费时 2s 内触发取消并关闭）；`TestEinoEventsNoTerminalDropWhenConsumerResumes`；`TestEinoRunnerCancellationClosesModel`；`TestEinoRunnerToolConcurrencyBound`（超过4个工具时最多4个实际执行）。
- [ ] **Step 2: 验证红灯。** `go test ./internal/agent/runtime -run 'Test(Eino|Budget)' -count=1 -timeout 30s`。
- [ ] **Step 3: 实现。** 构建 ADK ChatModelAgent，Instruction 不重复拼接服务端 system；模型输入的 tool 配对和预算每一轮重新检查。预算用既有估算器加完整 tool/schema/角色开销，标记为估算而非供应商精确 tokenizer；超限先明确拒绝，P3 再加摘要策略，不能截掉半个工具对。工具并发使用有 ctx 的共享 limiter 与 Eino 单运行限制；不产生无限 goroutine。
- [ ] **Step 4: 实现事件桥与终态。** 遍历 ADK iterator，对每个 MessageStream 完整消费/关闭；模型流中的文本和 reasoning 分别映射，工具调用/结果及 usage 各一次，记录实际模型步数。逻辑运行只有一个终态；ctx canceled 映射 done 的 `finish_reason=canceled`，deadline/模型/迭代错误映射 error。可用消费者下 terminal 可靠发送；慢消费者被关闭时由 service 的持久消息状态承担结果可查性，不声称 P1 已有持久事件重放。
- [ ] **Step 5: 验证与提交。** `go test -race ./internal/agent/... -count=1 -timeout 90s`；记录资源退出的同步信号，避免以一次 goroutine 数值断言替代生命周期验证。提交 `feat: run agents through bounded Eino ADK runtime`。

### Task 6: 接通普通 Agent、Host 专家与现有入口

**Files:**
- Modify: `backend/internal/service/chatsvc.go`
- Modify: `backend/internal/agent/host/host.go`
- Modify: `backend/internal/agent/runtime/runner.go`（保留 Runner/RunInput，删除已无调用的手写 ReactRunner）
- Modify: `backend/internal/agent/runtime/planner.go`（无生产调用的 planner 接收者迁到 EinoRunner，保留功能，不宣称 Plan-and-Execute 已接通）
- Modify: `backend/internal/config/config.go`, `backend/configs/config.yaml`
- Create: `backend/internal/agent/host/host_test.go`
- Create: `backend/internal/service/chat_runner_test.go`

**Interfaces:**
- Consumes: Task 5 `NewEinoRunner`，Host 原 `Expert.Runner`/`DirectRunner` 不改签名。
- Produces: 配置 `agent.input_token_budget=128000`、`event_buffer=128`、`send_timeout=2s`、`max_tool_output_bytes=65536`、`max_concurrent_tools=4`。配置缺省注入这些值，显式负值/非法值启动失败。
- Service 所有普通、专家和直答入口改用 EinoRunner；Host 不复制修改共享 RunInput，专家危险集合由专家自己的绑定解析，不能复用 Host 的集合。

- [ ] **Step 1: 写失败测试。** `TestHostExpertUsesOwnToolsAndDangerousFlags`、`TestHostDoesNotMergeAfterExpertFailure`、`TestHostForwardsOneTerminal`（merge 在成功 done 前，失败不 merge）、`TestHostCancellationPropagates`、`TestChatSelectsEinoForEveryPath`；配置默认/非法值测试。保持无匹配专家的直答与历史权限边界。
- [ ] **Step 2: 验证红灯。** `go test ./internal/agent/host ./internal/service ./internal/config -count=1 -timeout 60s`。
- [ ] **Step 3: 接线与兼容。** 更新所有 NewReactRunner 生产引用；Host 输出有界发送，失败立即结束并关闭/取消专家流；只在成功时 merge 后发送最终 done。planner 方法保留但不用它伪装已经有规划执行。不要引入第二个 ADK Host 以改变既有单专家路由语义。
- [ ] **Step 4: 验证绿灯与路径检查。** `go test -race ./internal/agent/... ./internal/service ./internal/config -count=1 -timeout 90s`；`rg -n 'NewReactRunner|ReactRunner' backend` 不应留下生产引用（文档/naive 教学说明可保留）；`go build ./cmd/server ./cmd/worker ./cmd/cli ./cmd/migrate`。
- [ ] **Step 5: 提交。** `feat: migrate chat and expert execution to Eino`。

### Task 7: SSE 取消、终态与真实消息落库

**Files:**
- Modify: `backend/internal/service/chatsvc.go`（必要时提取 pump 到 `chat_stream.go`，保持包和公开接口不变）
- Modify: `backend/internal/api/handler_chat.go`
- Create: `backend/internal/service/chat_stream_integration_test.go`（integration tag）
- Create: `backend/internal/api/chat_stream_test.go`
- Modify: `.github/workflows/ci.yml`（PostgreSQL 集成 job）

**Interfaces:**
- Consumes: 原 `ChatService.Stream(ctx,userID,*ChatInput)`、`model.MsgStatusDone/Stopped/Failed`、Task 5 事件。
- Produces: API 在建立流前创建可取消子 ctx，在写失败或读端关闭时 cancel；SSE 格式保留 event/data，Data 增加 run_id/seq/time 字段供前端可选使用。P1 无 Last-Event-ID 或事件重放接口。
- Service 持久化最终消息使用 `context.WithTimeout(context.WithoutCancel(ctx),3*time.Second)`；只在持久化成功后转发成功终态。错误持久化失败也记录明确错误，不能发成功。统计、标题、记忆扩展失败单列观测，不改变已提交消息事实。

- [ ] **Step 1: 写失败测试。** 使用真实 PG 验证成功落 Done、模型错误落 Failed、用户取消落 Stopped、取消仍能在3秒内完成或明确失败；`TestChatNoDoneBeforeMessageCommit` 在阻塞事务时验证 done 尚未到达；`TestChatPersistenceFailureIsNotSuccess`；`TestChatSlowConsumerPersistsActualTermination`。API 用真实 Hertz listener/HTTP 客户端断流，验证模型服务端 ctx 退出及 SSE 事件字段兼容，不能只测试 writeSSE 字符串。
- [ ] **Step 2: 验证红灯。** 在专用测试数据库应用迁移，运行 `go test -tags=integration ./internal/service -run TestChat -count=1 -timeout 90s`；API 测试 `go test ./internal/api -run TestChat -count=1 -timeout 30s`。测试数据库通过 `NX_TEST_DATABASE_DSN` 环境注入，不在命令参数和日志输出密码；不得指向用户已有数据库。
- [ ] **Step 3: 实现。** pump 累积消息并保留终态，等待运行结束后有界持久化，再发送 terminal。缓冲满触发取消并落失败/停止状态，不静默丢内容后仍写成功；用户真实取消写 Stopped。最后汇总出口赋予递增 seq，防止 Host 内部事件序号冲突。handler 不让 pooled RequestContext 逃入 goroutine，提前拷贝必要字段；关闭响应流时取消上游。
- [ ] **Step 4: 验证绿灯。** 重跑上述集成与 API 测试，再 `go test -race ./... -timeout 180s`。Linux CI 提供独立 PostgreSQL service 与迁移；测试清理只删除自己创建的数据，不运行 `compose down -v`。
- [ ] **Step 5: 提交。** `fix: persist truthful chat outcomes and cancel disconnected streams`。

### Task 8: 微基准、闭环演示与阶段验收

**Files:**
- Create: `backend/internal/agent/runtime/eino_benchmark_test.go`
- Create: `docs/research/2026-10-20-p1-validation.md`
- Modify: `README.md`

**Interfaces:**
- Consumes: Task 1–7；真实现有登录/Agent/知识库/聊天接口。
- Produces: `BenchmarkEinoRunnerText`、`BenchmarkEinoRunnerToolRoundTrip`，明确桩延迟、数据量、并发度、alloc/op 和平台开销，微基准不等于全平台容量。

- [ ] **Step 1: 运行功能与兼容回归。** 普通 Agent 流式回答、一次工具调用闭环、Host 专家路由、模型失败、取消和敏感工具拒绝；通过真实 HTTP 使用既有前端/CLI 解析事件。RAG/记忆/工作流等未改造模块只记录实际兼容结果，不能把旧实现未完成的能力写成通过。
- [ ] **Step 2: 运行基准和生命周期验证。** `go test ./internal/agent/runtime -run '^$' -bench BenchmarkEino -benchmem -count=3`；取消/慢消费者用例重复100次并采集退出、连接、goroutine/堆趋势。若有实际新泄漏先修复再测；没有同口径旧基线不得写提升百分比。
- [ ] **Step 3: 全阶段验证。** `go vet ./...`、`go test -race ./... -timeout 180s`、真实 PG 集成、四入口 build、前端 typecheck/build、`git diff --check`；检查 GitHub CI 的真实运行结论。Windows 无法跑 race 的环境以 Linux 实测补齐，不能只删除 race 参数。
- [ ] **Step 4: 更新交付说明并提交。** README 明确 P1 已实现项、测试方式及 P2/P3/P4 未完成项；记录硬件、Go/Eino/数据库版本、命令和结果。提交 `test: validate Eino runtime lifecycle and compatibility`。
- [ ] **Step 5: 全分支审查和必要修复。** 依 verification-before-completion 与 requesting-code-review 流程，审查错误传播、授权边界、取消和资源退出。用户选择原生执行时最后使用一个独立审查者，选择子代理执行时按每任务独立审查。修复实际问题后只重跑受影响及必要全量检查，然后推送可验证结果。

## 阶段边界

P1 不是整个生产化项目完成。下一阶段 P2 必须实现持久工作流版本、Run/Step/Attempt、PG CheckPointStore、审批 CAS/outbox、worker 租约与副作用核对；P3 闭合 ETL/RAG/记忆/MCP/A2A；P4 验证共享配额和整体容量。未完成的模块始终在 README 和验收报告中如实标示。
