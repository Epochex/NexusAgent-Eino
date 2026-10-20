# P0/P1 执行内核验证

日期：2026-10-20。范围：首阶段 Eino 执行内核、模型/工具边界、聊天流生命周期和真实消息落库。总体项目的 P2/P3/P4 尚未交付，不能从本报告推导整个平台已经完成生产验收。

## 实现范围

普通 Agent、Host 专家和直答统一使用 Eino ADK v0.9.22；原 Runner 契约保留，生产手写 ReAct 已移除，naive 教学版保留。模型与工具绑定独立于并发请求；流中的传输、协议和供应商错误明确传播，供应商额度覆盖完整流生命周期。

Agent 工具只从绑定列表构建，调用前验证 JSON Schema 并刷新权威定义。已禁用、新增危险标记或 Schema 已变化的工具拒绝继续执行。危险工具在持久审批接入前返回 approval_required，handler 不执行。YAML 工作流的审批和工具策略属于 P2，此处不能解释为已完成全平台统一授权。

默认资源约束：输入估算预算 128000 tokens；单步最多 32 个工具调用、同时执行 4 个；工具观察最多 64 KiB（UTF-8 安全截断）；Agent 总生成内容最多 1 MiB；底层 SSE 单帧 512 KiB、单次模型流累计 4 MiB；事件缓冲 128、发送等待 2 秒。预算是本地估算，不宣称等同于供应商精确 tokenizer。

消息以 Done/Failed/Stopped 区分成功、失败和中断。服务端把终态保留到 CreateMessage 提交后；数据库失败返回错误，不能先报成功。收尾使用独立的 3 秒上下文，客户端取消不抹掉最终记录。流携带 run_id/seq/time；P1 通过 session 与 assistant_message_id 查询持久结果，没有持久 Run 或断点事件重放 API。

## 真实验证与故障条件

- Windows/amd64：Go 1.26.0 全包测试、vet、server/worker/cli/migrate 构建；Node v24.14.1 的前端 typecheck/build。
- Linux/amd64 Docker：Go 1.26.0，全包 race；PostgreSQL 16 的真实迁移和集成测试。每个测试创建独立 schema，清理范围限制为自己创建的 schema，测试数据库名必须以 `_test` 结尾。
- 审查修复后的固定源码最终矩阵：Linux 全包 race 的顶层用例 68 个通过、0 失败；带 integration tag 的 `-run TestChat` 命令有 10 个顶层用例通过、0 失败（包含 1 个普通入口选择用例，不能把 10 个都称为 PG 用例）。同轮 vet、四入口构建及 Windows 全包/前端检查均通过。
- HTTP：真实 Hertz listener 与标准 HTTP 客户端，模型使用本地 HTTP/SSE 桩。验证正常流、未知终止、部分输出后错误、非法帧、过大响应、交错工具调用、并发额度、取消、慢消费者和客户端断开。
- PostgreSQL：用表锁阻塞消息提交，确认 done 没有提前发出；用 CHECK 约束拒绝助手消息，确认没有虚假成功；阻塞超过期限验证最终写库有界；验证取消后的 Stopped、失败后的 Failed 及外部用户会话拒绝。
- Host/CLI：专家采用自己的工具与危险标记；失败不聚合，merge 在成功终态前；CLI 延续会话 ID，识别 SSE error 和缺失终态，兼容扩展字段与 comment 心跳。
- 轨迹：验证切片扩容后完成状态不丢失、并发步骤和快照隔离，以及工具耗时覆盖实际执行。

Windows 下 Hertz connstate 不支持远端关闭感知，使用 250 ms comment 心跳唤醒底层写操作检测断连；Linux 开启原生关闭感知并使用 15 秒心跳。两个平台均运行实际断连测试。

独立全分支审查发现两项 Important，已补回归并修复：最终模型步骤为空白/只有 reasoning/工具调用前有文字而最终为空时，返回模型空响应错误（50001），不再报告 done、成功 trace 或 Host merge；真实 PostgreSQL 验证其落 Failed。TCP 停读回归保持连接打开并停止读取，缩小 socket 缓冲并发送 3 MiB 桩输出：旧实现超过 4 秒仍不释放连接；新实现每个网络 chunk 更新 2 秒写期限，超时退出 Hertz body writer，关闭 Pipe、取消模型并结束根 span。另测模型正常停顿 2.2 秒后继续完成，确保该期限不限制正常推理时长。

server 显式使用 Hertz standard transport，使生产入口与两平台真实 TCP 测试使用同一套期限/取消语义；取消通过 socket deadline 打断 Flush，连接 Close 由 Hertz 管理。P4 可按同口径负载比较其他 transport，不能假定更换网络库即获得性能收益。[实施裁决与审查范围](2026-10-20-p1-decisions.md) 保留接口取舍和未验收边界。

## 微基准

环境：Linux/amd64，Go 1.26.0，CPU 为 Intel Core i5-13400F，Docker CPU 配额 4、内存上限 4 GiB，GOMAXPROCS=4。本地 HTTP 模型桩无人工延迟；不访问真实 LLM、PostgreSQL、Milvus、MinIO 或 Redis。每个基准重复 3 次，下面取各列中位数。

| 基准 | ns/op | ms/op | B/op | allocs/op |
| --- | ---: | ---: | ---: | ---: |
| 文本执行 | 330130 | 0.330 | 150711 | 872 |
| 模型→工具→模型 | 960402 | 0.960 | 362046 | 2680 |

测量包含 Eino 构造/运行、适配、事件处理和本地 HTTP；工具往返包含 Schema 与策略检查。没有同口径上游基准，因此不报告性能提升百分比。完整平台的开放到达率负载、共享额度、长时间压力和饱和容量属于 P4。

```sh
go test ./internal/agent/runtime -run '^$' -bench BenchmarkEino -benchmem -count=3
go test -race ./internal/agent/runtime -run 'TestEinoRunnerCancellationClosesModel|TestEinoEventsSlowConsumerTerminates' -count=100 -timeout 120s
go test -race ./internal/agent/runtime -run TestEinoRunnerLifecycleResources -count=1 -v
```

取消和慢消费者两项用例各重复 100 次通过。额外的复用执行器测试在 5 次预热后分 3 轮执行 102 次取消；每轮完成并 GC 后的观测如下：

| 已完成次数 | goroutines | 活跃模型连接 | HeapAlloc (bytes) |
| --- | ---: | ---: | ---: |
| 34 | 3 | 0 | 2224872 |
| 68 | 3 | 0 | 2227640 |
| 102 | 3 | 0 | 2305720 |

这些短周期结果验证本次取消路径的回收行为；不替代长时间内存稳定性、全平台负载或线上可用性验证。

## 复核命令与范围

```sh
go vet ./...
go test -race ./... -count=1 -timeout 180s
# 显式配置 NX_TEST_DATABASE_DSN，必须指向独立的 *_test 数据库
go test -race -tags=integration ./internal/service -run TestChat -count=1 -timeout 120s
go build ./cmd/server ./cmd/worker ./cmd/cli ./cmd/migrate
```

前端执行 `npm ci`、`npm run typecheck`、`npm run build`。GitHub CI 固定 Node 22、本地为 Node 24。[修复提交 bd75fe8 的实际 CI](https://github.com/Epochex/NexusAgent-Eino/actions/runs/39375939678) 已全部通过：frontend 15 秒、backend 1 分 23 秒；backend 包含 vet、全包 race、真实 PostgreSQL 集成和四入口构建。这是实际运行结论，不是以 workflow 文件存在代替验证。

尚未验证真实模型正确率或外部工具副作用，未做整个平台的 RAG/MCP/A2A/记忆闭环验收；相关旧模块只完成本次构建兼容检查。并发审批、检查点、worker 租约、outbox、索引幂等、长期记忆一致性与全平台容量继续按总体设计交付。
