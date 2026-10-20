# A2A 运行、身份与故障核对

## 协议与部署

使用官方 a2a-go/v2 v2.6.0，接入当前 A2A 1.0/1.0.1 JSON-RPC。发现地址为 `/.well-known/agent-card.json`，RPC 为 `/a2a`；不再使用旧的手写 wire types。名片只声明已实现的文本消息、任务查询、分页和取消。流式订阅、推送、文件输入、任务续写和 REST/gRPC transport 未启用。协议依据见 [A2A 1.0.1](https://a2a-protocol.org/v1.0.1/specification/) 与 [官方 Go SDK](https://github.com/a2aproject/a2a-go/releases/tag/v2.6.0)。

先将数据库迁移到 012，保留数据库备份及平台凭据加密主密钥，然后部署同版本 server/worker。迁移会停用旧 A2A 配置，标记为 requires_upgrade；管理员重新同步名片并手动启用。down 保留加密字段和任务审计表，不会还原旧匿名服务行为；回滚应用必须兼容这些安全字段。

```yaml
a2a:
  enabled: true
  bind_addr: 127.0.0.1:8082
  public_url: https://agent.example.com
  max_concurrency: 8
```

生产环境必须填写对外 origin；默认只绑定 loopback。反向代理须将名片和 RPC 转发到此监听器，保留 Authorization。RPC 要求平台有效的 Bearer JWT，并重新查询用户状态；名片可以公开读取。JWT 对应的真实用户传入本地 Chat/Eino，并作为会话、任务所有者；没有 system user 0 代执行。

监听失败会使启动失败，运行中监听异常会触发服务退出。A2A 根 context 停止 Agent，关闭流程等待 SDK 的执行收尾回调覆盖终态提交；等待结束后主程序才退出。强杀进程无法执行收尾：已经完成的任务仍可由新进程查询，未完成任务在 run timeout 加 30 秒收尾窗口后按失败查询；不会自动重跑模型或声明断点续写。主程序只保留一个信号处理者，通过 Hertz Run 启动 API，避免 Spin 与主程序重复关闭导致 SIGTERM 退出码错误。

## 管理与出站委派

管理接口每次读取 PostgreSQL 中的现有角色/状态，只有当前有效管理员可以操作。鉴权头使用 AES-GCM 信封保存，只返回 headers_present，不回显明文或密文。空 headers 保留已有凭据，clear_headers 显式清除；config_version 用于编辑 CAS。目标或凭据变化会清除已固定的名片并停用，必须先同步再手动启用。健康检查不会暗中接受新名片或重新启用服务。

出站默认仅公网；内部服务由运营者在 `outbound.private_origins` 精确许可 scheme/host/port。解析、实际拨号和重定向均验证，不继承系统代理；始终禁止元数据/链路本地目标。名片不能把调用或凭据引向另一个 origin，也不能降级到未支持的 transport。当前每次管理/委派使用有限生命周期客户端并 Close，尚未建立跨操作的 A2A 连接池。

`POST /api/v1/admin/a2a/agents/:id/tasks` 要求 text、request_key，可设 return_immediately。前端默认异步接受，保留请求号用于“核对原请求”。数据库先预留唯一的 `(owner, agent, request_key)`，再发送远端请求；相同请求号与相同正文只返回已有记录，变更正文返回 409。该去重范围不代表远端支持 exactly-once。

网络中断或响应丢失返回 202/unknown，不自动重发。若已经取得远端任务号，可以查询或取消；若还没有任务号，使用记录号/消息号到远端核对，不能把 unknown 当成“未执行”。重新生成请求号属于新的执行，应先检查原请求的实际效果。新配置不会改写旧委派的绑定快照；核对使用冻结名片、端点及加密凭据，但停用/删除和当前运营者出站策略仍可阻止网络访问。

管理配置可由管理员共享，委派历史只属于发起用户。知道其他管理员的本地记录号或远端任务号不授予查询/取消权。列表先按所有者筛选再分页；历史 DTO 不含私有绑定快照和凭据。

## 有限预算与状态

| 对象 | 当前上限/行为 |
| --- | --- |
| 入站执行 | 单进程默认 8，单用户 2；run timeout 不超过 120 秒 |
| 入站任务 | PostgreSQL 持久化、版本 CAS、终态不可复活、写入瞬间数据库时钟 deadline fence |
| 出站新委派 | PostgreSQL 事务共享预留，未过期记录全局 32 / 单用户 8，150 秒容量窗口 |
| 管理目录 | 最多 128 个未归档 Agent；所有者历史 10,000 条后拒绝新任务，不自动删除去重记录 |
| 核对请求 | 单记录跨进程刷新间隔 5 秒、取消间隔 10 秒，单次网络 5 秒 |
| 有效载荷 | 文本输入 32 KiB、完整输出 64 KiB、名片 256 KiB、wire response 1 MiB |

出站容量窗口限制平台在途请求，不保证远端任务在窗口后停止；远端仍须有自己的执行期限/额度。入站目前是进程局部额度，集群共享执行准入属于 P4，不能将此版本称作集群全局限流。历史保留、连接复用和压力容量也在 P4 继续验证。

submitted/working 不等于 completed；失败、取消、拒绝、input_required、auth_required 均保留。完整回答必须正常结束且非空，部分文本、坏 SSE、超限、异常 EOF 和取消不能变为完成。客户端校验 JSON-RPC version、请求响应 ID 和 result/error 互斥，拒绝错配响应。终态更新受 CAS 保护，迟到的模型输出不能覆盖取消。

## 验证入口

`go test -race -tags integration ./internal/a2a ./internal/service ./internal/api -run TestA2A -count=1` 使用真实测试 PostgreSQL；实际 server 进程强杀/重启测试另需 integration+infrastructure、Redis、MinIO、Milvus。详见 [验收记录](2026-10-20-p3-a2a-validation.md)。可选 `NX_A2A_BROWSER=1` 启动仅 loopback 的短期浏览器测试夹具；夹具的公开测试密码与控制端点不会编译进生产服务。
