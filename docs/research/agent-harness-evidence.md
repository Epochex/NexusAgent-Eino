# Agent Harness：实现与验证索引

此页将具体问题、代码入口、验证方法与结论范围关联，便于复现和技术追问。
这些改进是在本仓库既有 Eino 二次开发运行时上完成；上游来源见
[UPSTREAM.md](../../UPSTREAM.md)。受控协议测试与真实模型能力评测分开记录。

## 1. 长会话的完整轮次裁剪

**问题：** 历史对话超过模型输入预算时，即使删除已完成的旧轮次即可容纳当前任务，
原运行时仍会直接拒绝。按单消息裁剪则可能留下孤立工具回执或丢失系统消息。

**实现：** 先验证完整原始协议，再按最旧完整用户轮次淘汰历史；保留系统消息、
首个用户前缀及最新用户轮次，工具调用与全部回执随所属轮次一起移除。
消息、推理和工具 Schema 统一使用现有预算估计；受保护内容超限仍拒绝。
首次 admission 和后续 Eino `BeforeModelRewriteState` 使用同一策略；
复制模型工作状态而不删除原始会话或持久 checkpoint。

**代码与证据：**

- [裁剪逻辑](../../backend/internal/agent/runtime/context_history.go)
- [生产 Eino Runner 接入](../../backend/internal/agent/runtime/eino_runner.go)
- [边界与实际 SDK HTTP 回合回归](../../backend/internal/agent/runtime/context_history_test.go)
- [技术取舍、红绿测试与限制](2026-10-31-turn-aware-context-validation.md)

**可支持的结论：** 已实现协议安全的模型输入历史裁剪，减少可通过删除旧轮次解决的
预算拒绝。旧语义信息会丢失；未证明真实模型长程任务成功率或事实保留率提升，
预算估计不是供应商精确 tokenizer，也不是持久图 checkpoint 压缩。

## 2. 独立预期驱动的任务评测与版本对比

**问题：** 仅有单元测试、延迟基准或“最终文本看起来正确”，无法统一判断工具是否
实际按预期调用、失败是否被排除、版本变更是否引入退化。

**实现：** 新增可复用 Go evaluation 包和 `agenteval` CLI，直接接入生产 Runner。
JSONL 保存模型可见输入和独立预期，只有输入传给被测运行时；执行前拒绝重复任务
及跨 train/dev/test 的同族泄漏。成功要求精确 JSON 结果、工具名/参数多重集、
唯一调用 ID 对应的成功回执，以及唯一 `finish_reason=stop` 终态。
Eino 并发工具回调可能先于调用迭代事件到达，因此收齐轨迹后按 ID 配对，
不把到达顺序当作调用顺序。

超时、初始化错误和协议失败均留在分母；按任务时限和事件数限制收集。
完整观察轨迹、任务集 SHA256、代码版本、时延和 Token 写入 JSON/HTML。
未知用量和成本保留未知，只有提供明确版本价格时估算费用；失败任务保留已观察用量。
版本比较要求相同任务指纹与任务集合，分别记录改善、退化和逐任务耗时差。

**代码与证据：**

- [评测引擎](../../backend/internal/agent/evaluation/evaluation.go)
- [报告与成对版本比较](../../backend/internal/agent/evaluation/report.go)
- [超时、取消、用量、工具证据及泄漏回归](../../backend/internal/agent/evaluation/evaluation_test.go)
- [实际 Eino SDK / HTTP 网关 / 工具 CLI](../../backend/cmd/agenteval/main.go)
- [集成及故意退化版本回归](../../backend/cmd/agenteval/main_test.go)
- [运行命令、接口契约和限制](2026-10-31-agent-evaluation.md)

**验证：** 2026-11-01（Asia/Shanghai），Go 1.26 固定 Docker 镜像、Eino v0.9.22：

```sh
cd backend
go test ./... -p=4 -count=1 -timeout=120s
go test -race ./internal/agent/evaluation ./cmd/agenteval ./internal/agent/runtime ./internal/agent/einoadapter -count=1
go vet ./...
```

全部通过。实际 CLI 运行了三个受控集成任务：基线 3/3，故意破坏处置策略的版本 2/3，
相同任务指纹下检出 1 项退化、0 项改善。此计数只证明集成和判定器能检出预设错误；
模型响应及用量是固定对端生成，不是三个独立真实研发任务的质量成绩。
原始报告位于被忽略的 `tmp/agenteval/`，避免默认公开应用输入与工具输出。

**可支持的结论：** 已开发并联调独立预期、工具轨迹及版本退化验证工具，
通过真实 SDK、网关与工具回合验证程序行为。尚未完成真实研发任务集的语义质量评测；
没有运行 SWE-bench，不把受控样例结果写成模型质量、业务收益或生产 SLA。

## 3. SSE 取消与进程退出

针对 SIGTERM 后流式连接偶发未释放，复现生产者先关闭管道、异步取消回调尚未运行的
窗口。读取端同步检查取消状态并设置过期写截止时间，使 Hertz 的最终分块写入失败，
由框架释放连接；正常 EOF 语义不变。使用保留客户端空闲连接池的真实 TCP 回归验证，
完整修复在 race 检测下连续运行 20 次通过，未放宽原进程退出断言。
详见[失败记录、框架语义与修复验证](2026-11-01-sse-cancellation-drain.md)。

## 4. 使用这些证据

技术描述应对应上面的具体机制和代码。引用某次结果时同时说明任务范围、
固定对端/真实模型、分母和代码版本；PR 创建、CI 通过、合并是三个独立状态。
发布后可使用 PR 的 Files changed 与 commit permalink 指向实际实现，避免只引用仓库首页。
新真实模型或业务实验应另建记录，保留负结果，不覆盖这些受控实验的原始范围。
