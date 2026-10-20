# 真实模型关键链路验收（2026-10-21）

目标是用少量真实请求确认模型协议、工具结果和应用持久状态能够连起来。实际使用 DeepSeek `deepseek-flash`，共8次供应商请求，全部 HTTP 200；没有用真实模型做压力测试，也未为补充协议回归重复这8次请求。[逐次原始结果](2026-10-21-real-model-results.json)保留usage、客户端观测与操作记录，不包含凭据、请求正文或推理正文。

## 结果

| 链路 | 实际断言/独立核对 | 供应商请求 |
|---|---|---:|
| Host→Expert→MCP→答案 | 实际官方 MCP SDK 工具返回提示词中没有的随机标记；模型答案包含该标记；工具执行1次；SSE收到done；PG恰好保存1条成功assistant消息 | 3 |
| 持久工作流：审批同意 | 实际 Eino Graph先完成LLM节点，危险file_ops自动挂起；审批前文件不存在；停止并重建Worker实例后审批，同一运行恢复并实际写入所有者目录；后置模型读取工具输出；前置LLM attempt仍为1，恢复仅新增1次模型请求 | 2 |
| 持久工作流：审批拒绝 | 同request_key重复提交返回同run；拒绝后canceled，无文件副作用，无后续模型请求 | 1 |
| 文档→检索→真实模型 | 认证HTTP上传TXT，经实际MinIO/PG异步Worker/Milvus发布ready版本；答案为17分钟，引用对应文件、文档、chunk及页码；PG答案提交且SSE正常完成 | 1 |
| 无答案与越权 | 文档没有GPU预算，模型返回NOT_PROVIDED；另一个用户访问该知识库返回404且供应商请求数不变 | 1 |

审批恢复测试重建的是同一Go测试进程内的Worker实例，不能表述为本次真实模型经历OS进程强杀。此前真实进程故障、未知操作与租约围栏证据仍见[P2](2026-10-20-p2-validation.md)。实际文件工具为内置file_ops，MCP样本为只读工具，二者分别验证。

客户端指标从实际HTTP请求发出计时，首内容不包括reasoning或工具事件：

| 对话 | 首内容 | 收到最终结果 |
|---|---:|---:|
| Host/Expert/MCP | 2715.10ms | 2784.93ms |
| 有依据文档问答 | 917.23ms | 971.23ms |
| 无答案拒答 | 1291.09ms | 1358.04ms |

这是3个单次观测，不计算p95/p99，不外推供应商吞吐、一般回答准确率或线上SLA。文档embedding是受控32维固定向量，只证明真实存储/检索/引用与实际生成连通，不能证明语义召回质量；引用元数据在原始结果中逐项核对。

## 用量与额度保护

供应商usage合计：输入1327 tokens，输出396 tokens（其中reasoning316），输入缓存命中128。按已核对的[官方价格](https://api-docs.deepseek.com/zh-cn/quick_start/pricing/)，采用峰时输入全未命中2元/百万、输出8元/百万的保守估算：

`1327 × 2 / 1,000,000 + 396 × 8 / 1,000,000 = 0.005822元`。

实际非峰时并计缓存的估算为0.00278556元。以上是根据usage和价格的估算，不是供应商账单明细；两位小数的余额未变化不能说明免费。

测试专用loopback relay串行转发真实请求/响应，只在内存替换Authorization；限定模型、输入16KiB、32条消息、单次最多1024输出tokens、最多24次请求和0.8元保守预算。请求发出前写入本地预留账本，未取到完整usage的请求保留预留；SHA256指纹阻止同正文被自动重试而重复计费。该relay仅用于本次显式授权验收，不是产品的通用计费系统或供应商账单硬限额。实际在8次后停止。

凭据文件及账本位于忽略目录，实际模型密钥不进入应用数据库、仓库或报告。CI默认不编译realmodel测试，普通验证不会触发付费请求。

## 协议回归与执行证据

真实验收使用基线`a922e65`加本次测试fixture，命令退出0，race通过，耗时16.505s：

```powershell
# 需先设置基础设施地址、私有NX_REAL_KEY_FILE与NX_REAL_OUTPUT目录
$env:NX_REAL_SPEND = 'ALLOW'
go test -race -tags 'integration infrastructure realmodel' ./internal/api -run '^TestRealModelCriticalEndToEnd$' -count=1 -timeout=10m
```

请勿为重现日志直接重复执行：真实调用会消耗额度，原账本会拒绝重复付费请求。后续代码核对发现assistant工具调用历史序列化漏传reasoning_content；本次真实供应商仍接受请求，但[官方多轮工具协议](https://api-docs.deepseek.com/guides/thinking_mode/)要求保留该字段。已在同一序列化分支补齐，并由严格本地HTTP协议peer验证非流式与流式实际请求：修复前两项HTTP400失败，修复后通过。没有为这项修复重新消耗真实模型额度，真实8次结果与修复后的本地回归证据分别记录。

零付费验证：`go test -race ./internal/llm ./internal/agent/einoadapter ./internal/agent/runtime -count=1 -timeout=3m`全部通过（2.896s / 2.750s / 3.612s）；预算防线单测只访问本地httptest服务，验证非法模型/输出/输入、重复请求、预算、重建账本和错误脱敏。真实模型测试必须同时满足build tags和NX_REAL_SPEND=ALLOW才会执行。

提交前补充执行`go vet ./...`和`go test -race ./... -count=1 -timeout=180s`，均退出0；预算单测重新运行通过（2.563s）。全量单测未带realmodel标签，未调用供应商。已有实际基础设施故障/容量证据保持原冻结版本，不将本次小样本替代那些实验。
