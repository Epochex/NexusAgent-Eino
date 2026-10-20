# 最终发布验收与审查（2026-10-21）

本次交付包含完整的 P2/P3/P4 工程改造，功能证据按下表索引。P0/P1 已在 main；新增内容仍保留上游历史及许可边界。最初验收未配置真实模型；随后用户授权有限额度，已完成[8 次真实模型关键链路验收](2026-10-21-real-model-validation.md)，保留实际 token、逐次延迟和审批恢复证据。

## 验收索引

| 范围 | 验证内容 | 证据 |
|---|---|---|
| Eino 生产执行 | ADK Function Calling、Host/Expert、Graph 工作流、真实工具与最终 SSE | [P1](2026-10-20-p1-validation.md)、[产品权限](2026-10-20-p3-validation.md) |
| 持久执行 | 实际进程强杀、租约/代际围栏、审批、取消、未知写入、请求幂等、checkpoint、事件/outbox | [P2](2026-10-20-p2-validation.md) |
| 文档与检索 | PG/MinIO/Milvus 实际失败/重试/版本发布/引用/删除；固定100题与越权预过滤 | [文档](2026-10-20-p3-documents-validation.md)、[检索](2026-10-20-p3-retrieval-validation.md) |
| 长期记忆 | 实际 Worker 崩溃、稳定向量ID、重建/迁移/删除、浏览器失败任务重试 | [记忆](2026-10-20-p3-memory-validation.md) |
| MCP/A2A | 官方 SDK、实际发现/调用/委派/取消/重启核对、权限与加密快照 | [MCP](2026-10-20-p3-mcp-validation.md)、[A2A](2026-10-20-p3-a2a-validation.md) |
| 共享额度 | 真实多进程全局/用户/provider限额、公平领取、额度缩小和失租取消 | [P4准入](2026-10-20-p4-shared-admission-validation.md) |
| 资源隔离 | 独立 PDF 容器 OOM 与恢复；真实代码容器 Python/shell/Go、内核限额、超时、输出与成功审计 | [隔离与运行时](2026-10-20-p4-runtime-hardening.md) |
| 实测容量 | 36档、30分钟18000请求、三次排空、500 SSE复测、Worker-kill | [容量及全部原始数据](2026-10-21-p4-capacity-validation.md) |
| 真实模型 | Host/Expert→MCP→最终答案、审批同意/拒绝与恢复不重放、上传文档问答/拒答/越权；8次实际供应商请求 | [逐次原始结果与费用估算](2026-10-21-real-model-validation.md) |

## 最终完整检查

最终代码59da984下，`scripts/verify-p3.ps1 -ModuleCache C:\Users\lin\go\pkg\mod -SkipInfrastructureStartup -Checks Backend` 退出0：`go vet ./...`、全部单元race、真实基础设施/进程integration+infrastructure race、独立PDF故障race以及server/worker/cli/migrate/docparser/loadtest六入口构建全部通过。

真实基础设施包耗时：execution 269.711s、service 208.076s、api 24.265s、repo 6.664s、ingestion 21.025s、vector 5.158s、memory 29.789s、tool 3.037s、a2a 7.686s、capacity 7.240s、loadtest 3.712s；隔离PDF loader 3.208s。包含固定100条检索、真实进程恢复、新增到期操作回归，未增加业务超时来通过检查。

首次 main CI [39474013284](https://github.com/Epochex/NexusAgent-Eino/actions/runs/39474013284) 暴露冷启动 runner 的缓存前置条件：setup-go 未命中缓存时 `/home/runner/go/pkg/mod` 不存在，Docker bind mount 在任何基础设施测试启动前退出125。修复为挂载前显式创建 Go 模块缓存目录，未跳过测试或放宽阈值。实际CI日志为RED，准备后的空缓存双网络容器能启动Go1.26.0为本地GREEN；Windows Docker Desktop 自动创建缺失共享目录，不能用其旧行为冒充Linux失败复现。后续mainCI仍为必须通过的最终gate。

主分支远端CI需以[实际Actions运行](https://github.com/Epochex/NexusAgent-Eino/actions?query=branch%3Amain)与提交SHA为准；本地通过不代替远端结果。

独立原生代码执行 gate：`go vet ./cmd/loadtest ./internal/tool ./internal/model`；真实 PG 与 Docker 下 `go test -race -tags integration ./cmd/loadtest ./internal/tool -count=1 -timeout=120s` 通过（4.920s / 23.339s）。Linux CI 的无 Docker socket 测试容器不替代这层测试。

前端最终 `npm ci`、`npm run typecheck`、`npm run build` 通过，npm 报告0漏洞。真实浏览器重新登录，选择 capacity-expert，观察实际 Eino→mcp_1_inspect→最终答案与两步调用链；工作流记录的 llm→tool→llm 和冻结发布/事件详情正常。沿用受控 peer，不是实际模型。截图：[对话](../images/p4-release-chat.png)、[工作流记录滚动视图](../images/p4-release-workflow.png)。浏览器运行期间仅发现 favicon.ico 404；关闭验证服务后产生的连接错误不计作应用仍在运行时的结果。

## 一次独立整分支审查与修复

遵循已采用的原生执行流程，最后仅调用一位全新上下文的 gpt-6-astra/high 审查者，范围 `1729851..11b1090`，只读审查共享准入、执行/未知写入、SSE/outbox、模型与工具快照、权限、检索/记忆、MCP/A2A、沙箱/PDF、负载与发布路径。结论为2项Important、0项Critical，无另列Minor；两项均由作者以失败测试先行修复于 `59da984`，没有二次审查。

| 问题 | 实际 RED | 修复与 GREEN |
|---|---|---|
| 领取过期运行绕过未知操作收尾 | 真实 PG 中 manual/readonly 操作及attempt仍为running，缺失operation_unknown，7.033s失败 | 领取和定时清理复用同一持锁事务处理；四种入口/策略、迟到提交、事件/outbox、取消/未知重用等回归13.002s通过 |
| migrate镜像没有SQL资源 | 实际TARGET=migrate镜像启动报file://migrations目录不存在 | 复制SQL；真实镜像在新隔离schema全量升级、重复升级、13→12→13和政策版本/限额逐行对比均通过 |

## 实际发布镜像

固定 Go builder digest；scratch runtime 使用65532:65532、CA/时区、配置和迁移资源，data/files属于应用用户。源码 `59da9844e7d2645641d9732b7a40a5baa2c1a540`：

- Server manifest `sha256:c284507ccbc4412847f860c45d384bfec75bedb4fb555cc83b17bfd4d9dadd9c`。
- Migrate manifest `sha256:5f93d629e6a22a72d78cc92defc4d5758ca5597b3133eb452aab9796cb93a53e`。

Server镜像实际以非root、只读根文件系统、受限可写tmp/files挂载启动；/healthz、真实登录、Eino→MCP→SSE done成功，并独立核对PG仅有1条成功assistant答案。不能用这一镜像冒充容量测试冻结二进制：容量实测为d8925c0，后续59da984只修复过期领取的操作收尾及镜像资源，可选代码审计亦单独验证；未重写原始测量版本或哈希。

迁移回退只验证013，未授权连续down其它数据迁移；P3旧进程不能与共享额度版本混跑。具体步骤见[发布回滚手册](deployment-runbook.md)。镜像未上传到容器Registry，代码与Dockerfile可复建。

## 未覆盖与决策

大样本真实模型回答质量、真实 embedding 语义质量、供应商吞吐、跨主机SLA、无限期稳定性、生产专用TLS Docker节点独立性及实际备份恢复，均未获得本轮证据。真实模型补充验收已记录8次请求的usage与价格估算，不能把小样本当作普遍准确率或容量证明。受控32维embedding的100题结果仅验证检索机制和权限。

[实施决策与代价](2026-10-21-p4-implementation-decisions.md)逐项保留所有Ruling及审查未判断项。延后小问题：favicon.ico缺失；没有其它已确认未修复的Important/Critical。此前自动审批只对本地清理返回`blocked by policy`，未提供具体规则；没有将该结果归因于某个未经证实的路径/命令原因，也未重试原清理。临时资料、功能分支、验收容器和本地日志保留，发布代码不依赖删除它们。
