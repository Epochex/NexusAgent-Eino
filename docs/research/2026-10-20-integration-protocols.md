# MCP/A2A 协议选型核查

核查日期：2026-10-20。这里记录集成任务的依据，不代表这些协议路径已经验收完成。

MCP 当前规范为 2026-07-28，已引入无状态生命周期；旧的 initialize/initialized 握手仍用于 2025-11-25 及更早版本。官方 Go SDK 同时支持新版本发现和旧握手回退，因此计划用固定版本 v1.8.0 替代项目内的简化 JSON-RPC/SSE 客户端，覆盖并发、分页及协议状态。平台仍负责权限、密钥、资源限制和执行重试语义。[官方生命周期说明](https://go.sdk.modelcontextprotocol.io/protocol/)、[v1.8.0 发布页](https://github.com/modelcontextprotocol/go-sdk/releases/tag/v1.8.0)。

A2A 已发布 1.0.1 规范。当前源码的简化 status 字符串、旧名片路径和 message/send 包装不能据此声明兼容。核实可用的官方 Go SDK 为 github.com/a2aproject/a2a-go/v2 v2.6.0（模块要求 Go1.26，本项目已使用该工具链）。后续优先复用 SDK 的数据与传输实现，另行验证登录身份、任务归属、取消、错误和持久化；只宣告实测的传输与能力。[A2A 1.0.1 规范](https://a2a-protocol.org/v1.0.1/specification/)、[Go SDK v2.6.0](https://github.com/a2aproject/a2a-go/releases/tag/v2.6.0)。
