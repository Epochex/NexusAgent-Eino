# 本地真实 embedding 与 RAG 验收（2026-10-21）

使用实际RTX5060和固定Qwen3-Embedding-0.6B权重，替换此前受控向量来测语义检索。本次完成两组数据、三条路线，共1200条路线级检查（其中30条权限拒绝不进入检索）。实际上传/异步Worker/RAG/DS生成另做定向验收。以下是有限样本的原始基线，不是所有业务语料的质量承诺。

## 模型、硬件与边界

- 模型：`Qwen/Qwen3-Embedding-0.6B`，revision `97b0c614be4d77ee51c0cef4e5f07c00f9eb65b3`，Apache-2.0；[官方模型卡](https://huggingface.co/Qwen/Qwen3-Embedding-0.6B)。权重与tokenizer SHA256见[锁定清单](../../scripts/embedding/artifact-lock.json)，启动前再次核对。
- 实机：NVIDIA GeForce RTX5060，8151MiB显存，初查空闲5773MiB；系统内存约31.8GiB。Torch2.8.0+cu128确认包含sm_120且实际CUDA推理通过，不是只检查cuda.is_available。
- Transformers4.57.6；FP16 CUDA模型，左填充、末token池化、关闭KV cache、归一化FP32输出1024维。本轮不加query instruction，沿用生产接口对文档/query使用相同模型的契约；不声称测过指令增益或真实cross-encoder rerank。
- Loopback私有Bearer服务；单次1–4条、每条最多512实际tokens、正文64KiB、一个GPU batch在途。长输入明确拒绝而非静默截断；CPU模式需显式选择。它是验收服务，不是公网高可用模型平台。
- [探针](2026-10-21-local-embedding-probe.json)：batch1短文本、batch4×512tokens均返回有限的单位范数1024维向量。最初探针实际编码为513，修正输入构造后重测，未放宽服务上限。
- [运行统计](2026-10-21-local-embedding-runtime.json)：累计1501个本地embedding批次、2973条输入、218769输入tokens、0推理失败；其中纯检索评测1496批次/2965条/216627tokens，探针2批次/5条/2055tokens，RAG追加3批次/3条/87tokens。没有向量化API费用。
- 峰值PyTorch分配1,465,709,568字节（约1.37GiB），保留1,516,240,896字节（约1.41GiB）。这是PyTorch计量，不包含全部桌面、驱动及CUDA上下文占用。[进程样本](2026-10-21-local-embedding-process.json)记录工作集峰值约2.86GiB、CPU累计112.39秒；torch计算线程设2，不能把它解释成整个进程只有2个OS线程。
- 已停止本次模型进程，GPU全局使用量观察从3828MiB降到2245MiB；模型、环境与证据保留在忽略目录。全局采样包含其它桌面进程，不将差值当成精确模型分配量。

## 数据与口径

中文组沿用项目既有20段短运维说明、100题：80有答案、10无答案、10权限负例。SciFact组使用[BEIR官方分发](https://github.com/beir-cellar/beir)与[数据集卡](https://huggingface.co/datasets/BeIR/scifact)的test qrels，500篇文档、300题；原始全文5183篇。按qrels保留所有测试正例，再按固定SHA256顺序补足500篇，**不是完整SciFact排行榜设置**。数据卡标注CC-BY-SA-4.0；下载文本不入仓库，公开[来源/子集哈希](2026-10-21-local-embedding-dataset.json)与准备脚本，原项目见[AllenAI SciFact](https://github.com/allenai/scifact)。

文档按现有splitter的512字符/64字符重叠分块；这与服务的512 tokenizer tokens是两种单位。中文20分块，SciFact2159分块。每批按实际tokenizer检查，没有对测试文本静默裁切。

索引fixture执行真实PG任务/发布、Go分块与网关、GPU embedding和Milvus写入，文本来源直接是固定语料；不能把它冒充认证上传/异步ETL全链路。后者由下方独立RAG验收证明。

配置在运行前固定：DenseTopK25、SparseTopK25、FusedTopN15、最终3分块、RRF k60、密集原始分数阈值0.25，不加载reranker。未用本轮测试标签选阈值、调整融合权重或选择更好看的重复运行。

按生产返回的前三个**分块**映射到文档评分，重复文档不增加召回。Hit@3表示至少命中一个相关文档；Recall@3是该查询找回的不同相关文档数/全部标注文档数；MRR@3按首次相关分块排名计算，未命中为0。对多相关文档不能把Hit写成Recall。SciFact的“相关”包括支持/反驳的相关资料，不是医学结论正确性或生成答案准确率。

## 实际结果

| 数据/路线 | Hit@3 | Recall@3 | MRR@3 | 检索总延迟p95 |
|---|---:|---:|---:|---:|
| 中文/密集向量 | 0.9625 | 0.9625 | 0.8854 | 617.33ms |
| 中文/ILIKE-TF-IDF | 1.0000 | 1.0000 | 0.9854 | 9.60ms |
| 中文/混合RRF | 1.0000 | 1.0000 | 0.9417 | 646.19ms |
| SciFact子集/密集向量 | 0.8633 | 0.8447 | 0.8183 | 589.56ms |
| SciFact子集/ILIKE-TF-IDF | 0.3100 | 0.2934 | 0.2856 | 104.67ms |
| SciFact子集/混合RRF | 0.7100 | 0.6711 | 0.4822 | 635.33ms |

中文密集/混合各有8/10无答案查询仍返回候选，关键词为0/10。这里不是LLM幻觉率，而是检索返回了不应直接当作答案依据的候选。原先适用于受控特征的阈值不能直接当作真实模型的语义拒答保证。需要独立开发集校准，并验证生成端拒答；本轮保留该缺口。

所有路线的协议错误、引用内容/文件对应错误为0；中文权限负例每路线0/10泄漏。每条普通检索的mode均与指定路线一致，degraded为空，没有把失败降级后的结果标成混合增益。SciFact无无答案/权限题，不能用其0/0外推此类能力。

混合在SciFact子集退化，51个查询密集命中而混合未命中。例如query49的相关文档5953485在密集前三分块全部命中，关键词偏向13964633/1606628，等权RRF后的前三分块全部来自13964633。该结果说明弱路线参与融合与重复分块占位会影响结果，不能认为“加上RRF就必然更好”。本轮不据此调完参数后再复用这300题宣称提升。

原始逐查询ID、相关ID、排名、mode、降级和时长：[中文100](2026-10-21-local-embedding-chinese100.json)、[SciFact500子集](2026-10-21-local-embedding-scifact500.json)。JSON包含测试源文件和服务代码SHA256。实际race测试退出0，532.411s；中文105.76s、SciFact424.21s。通过表示执行/协议/隔离断言成立，不表示质量全达某个业务SLA。

## 实际RAG与新增DS费用

通过真实JWT注册/登录，HTTP上传TXT→MinIO→异步文档Worker→本地Qwen embedding→Milvus ready→Go检索→实际DeepSeek→SSE done→PG答案提交。新增2次生成请求，有依据回答17分钟并引用正确文档/分块/第1页；无依据GPU预算返回NOT_PROVIDED；另一个用户读取知识库返回404且没有新增模型请求。

两次客户端首内容1359.87/1449.72ms，总时长1430.72/1529.00ms，仅两个单次观测，不计算其p95。实际新增3次本地embedding批次，证明不是继续走旧的常量向量分支。[定向原始结果](2026-10-21-local-embedding-rag.json)

DS继续使用此前扩展轮的同一私有账本，计数从100增到102，未清零。新增输入376、输出109tokens，按已核对峰时输入全未命中2元/百万、输出8元/百万保守估算0.001624元；扩展轮累计估算0.2588元，仍低于4.8元防线和用户5元授权。原8次独立首轮不混入该账本。普通CI不读取密钥，也不执行这些付费用例。

## 复现与剩余范围

见[本地服务与命令](../../scripts/embedding/README.md)、[实施计划](../superpowers/plans/2026-10-21-local-embedding-validation.md)。原始语料、模型、venv、CUDA下载和私有凭据均未上传；数据与权重固定hash，资源和版本随结果保留。

尚未验证：更大候选语料、客户真实中文资料、独立阈值校准集、query instruction增益、融合权重/文档多样性策略、真实cross-encoder rerank、模型高并发服务与跨机SLA。既有容量实验的受控模型和冻结源码口径保持不变，不用此处质量实验替代容量结论。
