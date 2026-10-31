# NexusAgent-Eino

Go + Eino 的 Agent 应用平台，面向需要接入企业知识、业务工具和远程 Agent 的应用开发者。用户在控制台配置 Agent、知识库和工作流；平台负责身份权限、模型与工具调用、异步执行、审批恢复和结果追踪。

基于 [chengpeng-cp/nexus-agent](https://github.com/chengpeng-cp/nexus-agent) 改造，保留上游历史和教学实现；本仓库的独立工作是将生产路径接入实际 Eino ADK/Graph，并补齐可验证的执行、数据和集成边界。来源及许可情况见 [UPSTREAM.md](UPSTREAM.md)。

**实施状态：P0–P4 工程实现与本地验收已完成；[最终验收与审查记录](docs/research/2026-10-21-release-validation.md)。首轮 [8 次真实模型关键链路验收](docs/research/2026-10-21-real-model-validation.md)后，追加 [100 次真实请求扩展验收](docs/research/2026-10-21-real-model-extended-validation.md)，发现并修复跨轮工具证据丢失。现已在 RTX5060 上完成[真实 Qwen3 embedding 的400题检索与 RAG 验收](docs/research/2026-10-21-local-embedding-validation.md)，保留混合检索退化与无答案候选问题；代表性业务质量与供应商容量仍需另测。**

[完整开发记录](docs/research/development-journal.md)用第一人称说明选题、源码核对、实现、失败、取舍与验证，注明上游来源和 AI 辅助范围。

## 已实现的主流程

| 能力 | 实现与边界 | 验证证据 |
|---|---|---|
| Agent 对话与 Host/Expert | Eino ADK v0.9.22；实际 Function Calling、工具执行和最终答案 SSE；模型、工具绑定与上下文预算 | [执行内核](docs/research/2026-10-20-p1-validation.md)、[产品权限](docs/research/2026-10-20-p3-validation.md) |
| Harness 上下文与任务评测 | 完整用户轮次裁剪、系统消息与工具回执保护；独立预期核验答案及工具轨迹、失败分母、Token 与版本退化对比 | [实现与验证索引](docs/research/agent-harness-evidence.md)、[评测工具](docs/research/2026-10-31-agent-evaluation.md) |
| 持久工作流 | YAML 编译为 Eino Graph；发布快照、PG checkpoint、租约与 generation fencing、请求幂等、审批、取消、事件重放 | [故障实验](docs/research/2026-10-20-p2-validation.md)、[操作手册](docs/research/workflow-runtime-runbook.md) |
| 文档知识库 | MinIO 文档版本、异步 ETL、有界分块、Milvus 版本索引；PDF 使用独立容器解析服务 | [文档验证](docs/research/2026-10-20-p3-documents-validation.md)、[解析隔离](docs/research/2026-10-20-p4-runtime-hardening.md) |
| 混合检索 | 在 ANN 查询前限制所有者/知识库/版本；PG 文本召回、稠密召回、RRF、可选 rerank、显式降级 | [固定 100 条评测](docs/research/2026-10-20-p3-retrieval-validation.md) |
| 长期记忆 | 对话事实提取、PG 事实与向量版本、可重试任务、删除与重建；来源与所有者过滤 | [记忆验证](docs/research/2026-10-20-p3-memory-validation.md) |
| MCP | 官方 Go SDK；发现、Schema、加密凭据、显式用户 ACL、工具策略、实际调用、配置变更失效 | [MCP 验证](docs/research/2026-10-20-p3-mcp-validation.md)、[运维](docs/research/mcp-runtime-runbook.md) |
| A2A | 官方 Go SDK v2；入站 JWT 与 PG 任务、出站持久委派和幂等键；远端未知结果保留核对，不自动重新发送 | [A2A 验证](docs/research/2026-10-20-p3-a2a-validation.md)、[运维](docs/research/a2a-runtime-runbook.md) |
| 多进程容量 | 工作流和 Chat 共用 PG 活跃额度；模型调用按供应商 origin 共用额度；用户限额、公平领取、有界排队 | [共享额度验证](docs/research/2026-10-20-p4-shared-admission-validation.md) |

危险外部操作按策略进入持久审批；调用后结果未提交而进程退出时，不能假定外部未执行。非幂等操作进入人工核对，只有只读或具备真实幂等保障的调用允许自动重试。Redis 通知采用 outbox 至少一次投递，PG 是执行状态权威。

检索先以受控32维 embedding 验证机制，现补充实际 Qwen3-Embedding-0.6B：中文100题与 SciFact500文档/300题子集，测密集、ILIKE/TF-IDF、等权RRF。SciFact Hit@3 分别为86.33%/31.00%/71.00%，混合并未优于密集；该子集不等于完整榜单或业务质量承诺。[代码执行工具](docs/research/code-execution-runbook.md)默认关闭，已单独验证真实容器资源边界、Python/shell/Go、超时与 OOM 恢复；生产需专用 TLS Docker 节点。其并发限制是每进程本地上限，不计入关闭沙箱时的容量基准。

## 本地启动

要求 Go 1.26、Node.js 22.12+、Docker Compose。下列配置面向开发环境；生产环境须使用独立凭据、私有诊断监听和经过授权的出站目标。

1. 在 `backend` 下运行 `make compose-core`，启动 PostgreSQL、Redis、Milvus 和 MinIO。
2. 通过环境变量或私有配置设置 `NX_PG__DSN`、`NX_JWT__SECRET` 和 `NX_SECRET_KEY`。AES 主密钥使用 32 字节，JWT 密钥至少 32 字符且与 AES 密钥不同。不要提交密钥。
3. 在 `backend` 下运行 `go run ./cmd/migrate`。可用 `NX_SEED_ADMIN_PASSWORD` 指定初始管理员密码；未指定时迁移工具仅在创建账号时显示一次随机密码。
4. 分别运行 `go run ./cmd/server` 和 `go run ./cmd/worker`。仅工作流执行可用 `go run ./cmd/worker -queues execution`，此模式只要求 PG，不负责数据队列与通知投递。
5. 在 `frontend` 下运行 `npm ci`、`npm run dev`，打开 `http://localhost:5173`。登录后在管理页配置 OpenAI 兼容模型供应商与模型，再配置工具和 Agent。

PDF 上传要求配置 `NX_PDF_PARSER__ENDPOINT` 和 `NX_PDF_PARSER__TOKEN`，并部署 [私有解析服务](backend/deployments/docker-compose.docparser.yaml)。API 与文档 Worker 需同时加入解析服务的私有网络；不发布解析端口，不挂载宿主数据或 Docker socket。未配置时 PDF 上传明确失败，TXT/DOCX 不依赖该服务。

内网 MCP/A2A 必须显式加入 `NX_OUTBOUND__PRIVATE_ORIGINS`；HTTP 工具和搜索服务另有目的地授权规则，见 [工具访问边界](docs/research/tool-access-boundaries.md)。不要将所有私有地址统一放行。

## 验证与容量复现

```powershell
# Windows PowerShell，仓库根目录；真实基础设施 + Linux race + 前端构建
./scripts/verify-p3.ps1 -ModuleCache C:\Users\lin\go\pkg\mod

# 独立容量 schema、实际 API/Worker/MCP、开放到达率负载
./scripts/run-capacity.ps1 -Suite Pilot
# 全部 36 档 + 30 分钟基准 + 三次排空/恢复观察
./scripts/run-capacity.ps1 -Suite All
```

容量部署将 API 与 Worker 各限制为 2 CPU/4 GiB，模型桩、负载发生器及基础设施单列。模型桩每次响应等待 250ms，MCP 工具等待 100ms；主要请求中 80% 为工作流两次模型与一次工具，20% 为实际 Eino Agent 工具循环。报告分别记录计划到达、实际写出、拒绝、接收、最终状态、数据库独立核对、首 token、控制接口延迟、CPU、内存、GC、连接与 outbox 积压。等待审批的 SSE、执行中的订阅与重新连接间隙会反映在实际采样中。

实测基准：200 SSE / 32 共享槽位 / 10 新请求每秒持续 30 分钟，18000 次实际请求全部成功；36 档中 30 档支撑完整到达率，低槽位高负载的拒绝边界保留。详情、全部原始数据与基础设施成本见[容量报告](docs/research/2026-10-21-p4-capacity-validation.md)，部署步骤见[发布/回滚手册](docs/research/deployment-runbook.md)。

生成凭据仅保存在忽略目录下的 fixture 文件；不要上传该文件。受控负载结果只说明对应硬件、配置和工作负载的行为。详细设计和验收约束见 [生产化设计](docs/superpowers/specs/2026-10-20-nexus-agent-eino-design.md)。

## 目录与运行边界

- `backend/cmd/server`、`worker`、`cli`、`migrate`：API、异步执行、终端会话和显式迁移入口。
- `backend/cmd/docparser`：私有 PDF 解析进程；`cmd/loadtest`：独立验收工具。
- `backend/internal/agent/runtime`：Eino 生产执行路径；`agent/naive`：上游教学实现。
- `backend/internal/execution`：持久状态、审批、操作记录、checkpoint、事件与 outbox。
- `frontend`：React/TypeScript 控制台；`docs/research`：验收证据与操作手册。

共享容量和权限不能约束远端系统在网络断开后仍可能进行的外部操作；A2A 的未知结果和危险工具的人工核对属于显式产品状态。模型熔断是进程内状态，共享限额由 PG 管理。生产部署需将 PostgreSQL、Redis、对象存储、向量库及备份恢复单独纳入运维，不能用单机 Compose 测试等同线上 SLA。
