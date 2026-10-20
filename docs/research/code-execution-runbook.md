# 可选代码执行工具

`code_exec` 默认关闭，普通 Agent/MCP/RAG 不要求 Docker 控制权限。启用前必须单独部署和验收执行运行时；生产 API/Worker 不应连接业务宿主机的 Docker socket。

## 镜像与部署

`backend/deployments/Dockerfile.sandbox` 从固定 Go/Python 基础镜像构建，提供 Python、shell、Go 和进程内 `timeout` 看门狗。先在专用执行节点构建、扫描、预加载镜像；生产发布到自己的镜像仓库，以不可变 digest 配置 `NX_SANDBOX__IMAGE`。运行请求不会拉取镜像。

```powershell
# 开发验收镜像，仓库根目录；镜像 ID 只在当前 daemon 有效
 docker build -f backend/deployments/Dockerfile.sandbox -t nexus-eino/sandbox:local backend
 docker inspect nexus-eino/sandbox:local --format '{{.Id}}'
```

配置 `NX_SANDBOX__ENABLED=true`、`NX_SANDBOX__IMAGE`、`NX_SANDBOX__MEMORY`、`NX_SANDBOX__CPUS`、`NX_SANDBOX__TIMEOUT`、`NX_SANDBOX__POOL_SIZE`。内存范围 64MiB–2GiB，CPU 0.1–4，时限 100ms–1min，每进程 1–8 个槽。默认关闭；启用后的无效配置会阻止启动。

`prod/production` 模式额外要求 `DOCKER_HOST=tcp://...`、`DOCKER_TLS_VERIFY=1`、`DOCKER_CERT_PATH`，由 Docker SDK 使用客户端证书。TLS 配置校验不能证明目标 daemon 是专用节点，这一点由部署者确认；这些凭据拥有该 daemon 的控制权限，须留在私有配置中。验收使用本地测试 daemon，不等于已验证生产 TLS 部署。

## 实际边界

- 容器 UID/GID 65532，根文件系统只读，无宿主数据挂载、无 Docker socket、环境变量使用固定白名单。
- `network=none`、drop ALL capabilities、no-new-privileges；内存与 swap 总额相同、CPU 配额、64 PID、256 文件描述符和 32MiB 单文件限制。
- `/tmp` 为 128MiB 临时文件系统。为支持 Go 编译后的程序，它明确允许执行；容器退出后移除。Go 禁止工具链下载和模块代理下载。
- stdout/stderr 持续读取，各仅保留 64KiB 加截断标记；Docker 日志另设 256KiB/1 文件轮转。非 UTF-8 字节移除后返回文本。
- Docker 请求有时限；容器内 `timeout` 也限制解释器执行时间。取消或超时后尝试强制移除本次命名容器，清理失败明确报错，不能作为执行成功返回。
- 槽位上限是**每个 API/Worker 进程**的本地限制；总体运行仍受 PG 共享运行额度约束。执行节点资源需按副本数和每进程槽位另算，未包含在关闭沙箱的 API/Worker 容量基准中。
- 代码执行审计只保存源代码 hash 与有界输出；审计提交失败明确返回错误。危险工具的权限与持久审批仍由统一工具执行层控制。

这些机制没有将容器解释为独立虚拟机，也不提供持久用户文件系统。第三方 Go 依赖必须事先包含在受信镜像中，运行时不联网安装。

## 独立验收

原生 Go 进程需能连接明确拥有的测试 daemon，使用已加载镜像 digest：

```powershell
$env:NX_TEST_SANDBOX_IMAGE='<上面 inspect 得到的不可变镜像 ID>'
# NX_TEST_DATABASE_DSN 使用名称以 _test 结尾的专用 PostgreSQL
cd backend
go test -race -tags integration ./internal/tool -run 'TestSandbox|TestCodeExecAudit' -count=1 -timeout=120s -v
```

测试实际读取容器内的 UID、根挂载标志、CapEff、NoNewPrivs、cgroup 内存/swap/CPU/PID 值，并执行大输出、OOM、取消、并发拒绝、CPU 超时、shell/Python/Go 运行及审计数据库失败。未显式提供测试镜像的 CI 会跳过这组独立 Docker 控制测试，不冒充已运行。