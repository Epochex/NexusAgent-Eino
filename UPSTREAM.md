# 上游来源与本项目改造

- 上游项目：[chengpeng-cp/nexus-agent](https://github.com/chengpeng-cp/nexus-agent)。
- 导入基线：`5c7b81ee35492e35bca61881f061cafb3c65b363`，2026-10-19。
- 本仓库：[Epochex/NexusAgent-Eino](https://github.com/Epochex/NexusAgent-Eino)。
- 保留上游 Git 历史、作者和原有源码归属；本项目的工程化改造通过后续独立提交体现，不将上游能力表述为本项目原创。
- 导入快照未包含 LICENSE 文件；本项目未为上游代码另外授予许可。

导入基线的实际执行路径为 Hertz + 自研 ReAct/工作流。本仓库已将普通 Agent、Host/Expert 与直答接入 Eino ADK，并将 YAML 工作流编译为实际 Eino Graph；同时保留上游教学实现。改造范围、验证与仍未验收的边界，以本仓库 README 和 docs/research 的实际证据为准。
