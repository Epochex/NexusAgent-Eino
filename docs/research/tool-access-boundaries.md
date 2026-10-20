# 内置工具访问边界

`file_ops` 的参数仍为 `operation/path/content`，但文件归属已经改变：每个登录用户使用 `data/files/<user_id>/` 下的独立目录。路径是相对该目录的文件名，不再代表平台共用目录或系统绝对路径。旧的共用文件不会自动迁入任意用户目录，需由管理员确认归属后迁移。

读取只支持普通文件，拒绝符号链接和超限文件；实际读取也有字节上限。保存先校验 UTF-8 与大小，再在授权目录内创建临时文件并原子替换目录项，避免覆盖目标符号链接或通过已有硬链接截断外部文件。Go `os.Root` 负责目录描述符和路径穿越边界。此能力不替代主机隔离：管理员不应把设备文件、特殊挂载或其他用户数据放入授权目录。

`db_query` 接受固定报表，不再接受 `sql`：

```json
{"report":"recent_runs","limit":20}
```

支持 `recent_runs`、`pending_approvals`、`usage_summary`，仅查询当前用户的数据。查询使用固定 SQL、参数绑定、只读事务、语句/锁等待时限和输出上限。迁移 010 更新工具 Schema；旧的任意 SQL 工作流需要编辑并重新发布，不能通过旧审批继续执行。若需要外部业务库的分析能力，应增加专门授权的数据源和报表，不把平台数据库权限交给模型。

新增出站 HTTP 客户端提供精确内部 origin 许可、DNS 解析后数值地址直连、元数据/链路本地地址拒绝、跨 origin 重定向拒绝、请求与响应大小限制。带操作键的 POST 不会因连接复用和响应丢失被 Go HTTP 传输隐式重发。MCP 已接入并完成协议对端/真实进程故障验证，操作规则见 [MCP 运行手册](mcp-runtime-runbook.md)。A2A、网络工具及模型网关需要各自接入与验收，不能从 MCP 的结果推断所有出站路径已受保护。

验证：Windows `go vet ./...` 与全包单测通过；Linux 4 CPU / 4 GiB 下 security/tool race 与真实 PG 报表测试通过，既有真实 worker 进程故障恢复回归通过（51.453 秒），server/worker/cli/migrate 构建通过。测试实际复现并阻止目录名前缀越界、外部符号链接读取、硬链接写穿、匿名/跨用户访问和任意 SQL；丢失响应的 POST 回归从两次远端效果变为一次并向调用方返回错误。

## 网络搜索与网页抓取

`web_search` 固定访问运营者配置的 `search.searxng_url`，该固定 origin 的内部许可仅供搜索使用。`http_client` 使用独立的 `search.http_domains` 域名白名单和 `search.http_private_origins` 精确内部 origin 许可，不继承搜索服务或 MCP 的访问许可。白名单空时不注册 `http_client`；搜索地址空时不注册 `web_search`。

```yaml
search:
  searxng_url: http://127.0.0.1:8081/search
  http_domains: [go.dev, pkg.go.dev]
  http_private_origins: []
```

域名规则不含协议、端口、通配符和路径，允许自身及其子域，忽略大小写/末尾点。私网访问同时需要命中域名规则和精确 origin 许可。DNS 与数值地址校验、元数据拒绝和重定向规则由共享出站客户端执行；相同域名的不同端口也属于不同 origin，不能借重定向扩大范围。

抓取只接受 GET，可设置 Accept、Accept-Language、User-Agent，不能让模型附加 Authorization/Cookie/代理等鉴权头。搜索响应上限 512 KiB，网页响应上限 256 KiB；HTTP 失败、响应截断、过限和非法 UTF-8 都返回错误。正文输出明确截短，搜索仅保留有限条合法 HTTP(S) 结果；上游缺失 results 数组不能假装“无结果”。客户端在注册时创建并复用连接池，退出时关闭空闲连接，调用者取消/超时会传播给上游。

新增测试实际复现旧实现私网访问成功、HTTP 500 作为内容返回、响应中途断开和超限被忽略；修正后均返回错误。额外验证固定搜索内部许可不泄漏给用户抓取、跨 origin 重定向不触达目标、明确许可后的内容可读、鉴权头被拒绝，以及调用方超时中止请求。A2A 与模型网关的出站边界仍单独验收。

本次网络工具接入后的 Windows 全包 vet/单测/构建通过；Linux 4 CPU / 4 GiB tool/security race 通过（1.185s/1.028s），实际工作流及 MCP worker 进程故障回归通过（76.278s），全部四个命令构建成功。未把包级耗时作为请求延迟或吞吐结论。
