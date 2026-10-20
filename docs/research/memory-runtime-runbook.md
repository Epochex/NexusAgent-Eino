# 长期记忆执行与修复

长期记忆以 PostgreSQL 为事实和任务状态的权威来源。Milvus 是可以重建的投影；Redis 仅提供通知，不决定任务是否完成。默认完整 worker 消费 `execution,doc-etl,memory`；可用 `worker -queues memory -memory-concurrency 2` 独立运行记忆队列。此模式需要 PostgreSQL、Milvus 和配置好的模型服务，不需要 MinIO 或代码执行容器。

## 可观察的完成条件

`POST /api/v1/memories/extract` 返回 202 和持久化任务。相同会话消息边界、向量空间和抽取模型配置合并为同一个任务。自动抽取按新提交的用户/助手消息数计算阈值，配置 `agent.memory_extract_every` 为 1..20，0 关闭。

抽取任务 `done` 只说明事实操作与后续向量任务已原子提交。记忆必须同时满足 `status=1`、`index_status=ready`、`indexed_revision=revision`，且属于当前激活的模型空间，才会被召回。管理页分别显示抽取和向量同步任务，避免把队列接收当作记忆生效。

管理接口：

| 操作 | 接口 |
| --- | --- |
| 我的记忆 | `GET /api/v1/memories` |
| 我的任务 / 任务及后续投影统计 | `GET /api/v1/memories/jobs` / `GET /api/v1/memories/jobs/:id` |
| 重试失败任务 | `POST /api/v1/memories/jobs/:id/retry` |
| 当前模型空间 / 可选模型 | `GET /api/v1/memories/profile` / `GET /api/v1/memories/embedding-models` |
| 显式重建 | `POST /api/v1/memories/rebuild` |
| 重建投影统计 | `GET /api/v1/memories/rebuilds/:id` |
| 归档 / 删除 | `POST /api/v1/memories/:id/archive` / `DELETE /api/v1/memories/:id` |

任务、重建和记忆均按登录用户隔离。任务响应不包含消息原文、模型配置快照、凭据、内部事实或决策检查点。

## 失败与重试

默认每个任务每轮最多尝试三次，续租 15 秒；进程退出后由替代 worker 在租约到期后接管。任务有 24 小时处理期限，单次处理最多五分钟。瞬时错误进入 `retry_wait`；结构化输出无效、版本冲突等需要干预的错误直接进入 `failed`。显式重试重置本轮预算并保留累计尝试数。

事实抽取和最终操作决策分别保存检查点。普通瞬时重试复用已保存阶段；显式重试失败抽取任务会重新考虑事实与当前候选版本。ADD 使用稳定操作键；UPDATE 增加 revision 并按实际新内容重新向量化；DELETE/归档立即撤销召回资格，并排队清理远端。远端 Upsert 成功但 PG 提交前崩溃时，重复执行同一主键，不增加逻辑向量条数。

向量清理失败不会重新使记忆可召回。先恢复 Milvus，再重试对应失败任务。已被较新 revision 替代的任务不可强行重试；使用最新任务或显式重建。

## 模型变更与旧数据

首次抽取固定用户的向量模型设置、维度和空间版本。日常修改模型配置不会混用不同空间。仅允许从仍启用、绑定关系和服务地址一致的配置获取轮换后的密钥；地址或模型空间迁移需要显式重建。

在管理页选择“重建记忆索引”，或提交：

```json
{"embedding_config_id": 12, "request_key": "an-owner-unique-request-key"}
```

保留该请求键用于网络失败后的重试。同一请求键与目标空间重复提交返回同一重建记录；不同空间返回冲突。事务先固定新空间、提升所有有效记忆 revision 并排队；内容保留，同范围完全重复事实合并，旧空间向量通过任务清理。同步期间对应记忆暂停召回。单用户最多重建 1000 条有效记忆，超限需先归档；不会静默截断。

旧索引中的记忆标记为 `needs_reindex`，不因升级自动获得“已就绪”资格。旧模型空间的配置记录和空集合保留，便于审计与后续明确的保留策略，不宣称完成了物理存储回收。

## 验证边界

受控 HTTP 模型对端用于验证协议、事务、失败恢复、向量更新与权限隔离。它不证明真实模型的事实抽取准确率、记忆质量、延迟或费用；真实模型服务尚未配置，相关验收未执行。
