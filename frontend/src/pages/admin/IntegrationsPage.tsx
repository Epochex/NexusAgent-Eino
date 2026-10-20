/**
 * 集成中心 —— 「MCP Server」与「A2A 远程 Agent」两个区块上下排列。
 *
 * 职责：
 * 1. MCP Server：版本化配置、加密凭据、授权用户、同步工具与调用策略审阅；
 * 2. A2A 远程 Agent：注册（后端先拉名片验活）、健康检查、任务委派与删除；
 * 3. MCP 只读调试受后端策略约束，工具清单使用独立分页接口。
 */
import { useEffect, useRef, useState } from 'react';
import {
  checkA2AAgent,
  checkMCPServer,
  createMCPServer,
  deleteA2AAgent,
  deleteMCPServer,
  listA2AAgents,
  listMCPServers,
  registerA2AAgent,
  sendA2ATask,
  syncMCPServer,
  updateMCPServer,
  migrateMCPCredentials,
} from '@/api/admin';
import { MCPToolsModal } from './MCPToolsModal';
import { ConfirmDialog } from '@/components/ui/ConfirmDialog';
import { Modal } from '@/components/ui/Modal';
import {
  Badge,
  Button,
  EmptyState,
  Input,
  PageHeader,
  Select,
  Spinner,
  StatusBadge,
  Textarea,
} from '@/components/ui/primitives';
import { relativeTime, truncate } from '@/lib/format';
import { staggerEnter } from '@/lib/gsap';
import { errMsg } from '@/lib/request';
import { toast } from '@/stores/ui';
import type { A2AAgent, MCPServer } from '@/types/admin';

/* ---- 常量与工具 ---- */

/** 传输方式 → 徽标色调（未知传输回退灰色） */
const TRANSPORT_TONE: Record<string, 'lime' | 'teal' | 'violet'> = {
  streamable_http: 'lime',
  stdio: 'teal',
  sse: 'violet',
};

/** 传输方式选项（与后端 MCPServer.transport 枚举一致） */
const TRANSPORTS = ['streamable_http'] as const;

/** 解析 JSON 文本：空串回退默认值，非法返回 null（由调用方 toast 阻断提交） */
function parseJson<T>(text: string, fallback: T): T | null {
  const t = text.trim();
  if (!t) return fallback;
  try {
    return JSON.parse(t) as T;
  } catch {
    return null;
  }
}

/** 结果输出统一截断，避免超长响应撑爆弹窗 */
const clip = (s: string) => truncate(s, 2000);

/* ---- 表单草稿类型 ---- */

/** MCP 表单草稿：授权用户与鉴权头在提交时解析校验。 */
interface McpDraft {
  name: string;
  transport: string;
  url: string;
  enabled: boolean;
  allowedUserIDs: string;
  clearHeaders: boolean;
  headers: string;
}

const EMPTY_MCP: McpDraft = {
  name: '',
  transport: 'streamable_http',
  url: '',
  enabled: true,
  allowedUserIDs: '[]',
  clearHeaders: false,
  headers: '{}',
};

export function IntegrationsPage() {
  /* ---- 列表数据与全局状态 ---- */
  const [servers, setServers] = useState<MCPServer[]>([]);
  const [agents, setAgents] = useState<A2AAgent[]>([]);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [deleting, setDeleting] = useState(false);
  /** 行内动作（同步 / 健康检查）进行中的目标 id，用于禁用对应按钮 */
  const [busyId, setBusyId] = useState<number | null>(null);

  /* ---- MCP 表单 ---- */
  const [mcpOpen, setMcpOpen] = useState(false);
  const [editingMcp, setEditingMcp] = useState<MCPServer | null>(null);
  const [mcpDraft, setMcpDraft] = useState<McpDraft>(EMPTY_MCP);

  const [toolsServer, setToolsServer] = useState<MCPServer | null>(null);

  /* ---- A2A 注册 ---- */
  const [registerOpen, setRegisterOpen] = useState(false);
  const [regDraft, setRegDraft] = useState({ base_url: '', name: '' });

  /* ---- A2A 委派任务 ---- */
  const [taskAgent, setTaskAgent] = useState<A2AAgent | null>(null);
  const [taskText, setTaskText] = useState('');
  const [taskResult, setTaskResult] = useState('');
  const [taskRunning, setTaskRunning] = useState(false);

  /** 删除目标：kind 区分区块，label 用于确认文案 */
  const [delTarget, setDelTarget] = useState<{
    kind: 'mcp' | 'a2a';
    id: number;
    label: string;
  } | null>(null);

  const rootRef = useRef<HTMLDivElement>(null);

  /* ---- 数据加载 ---- */

  /** 并行拉取 MCP Server + A2A Agent；写操作成功后复用整体刷新 */
  const load = async () => {
    setLoading(true);
    try {
      const [serverList, agentList] = await Promise.all([listMCPServers(), listA2AAgents()]);
      setServers(serverList);
      setAgents(agentList);
    } catch (e) {
      toast.err(errMsg(e));
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    void load();
  }, []);

  /** 首屏数据就绪后两个区块交错入场（仅首次，后续刷新不重播） */
  const playedRef = useRef(false);
  useEffect(() => {
    if (loading || playedRef.current) return;
    playedRef.current = true;
    const sections = rootRef.current?.querySelectorAll('section');
    if (sections && sections.length > 0) staggerEnter(Array.from(sections));
  }, [loading]);

  /* ---- MCP：新增 / 编辑 ---- */

  const patchMcp = (patch: Partial<McpDraft>) => setMcpDraft((d) => ({ ...d, ...patch }));

  const openMcpCreate = () => {
    setEditingMcp(null);
    setMcpDraft(EMPTY_MCP);
    setMcpOpen(true);
  };

  const openMcpEdit = (s: MCPServer) => {
    setEditingMcp(s);
    // JSON 字段序列化为缩进文本便于编辑；结构异常时回退默认值
    setMcpDraft({
      name: s.name,
      transport: s.transport,
      url: s.url,
      enabled: s.enabled,
      allowedUserIDs: JSON.stringify(s.allowed_user_ids ?? []),
      clearHeaders: false,
      headers: '{}',
    });
    setMcpOpen(true);
  };

  const submitMcp = async () => {
    if (!mcpDraft.name.trim()) {
      toast.err('名称为必填项');
      return;
    }
    const allowedUserIDs = parseJson<number[]>(mcpDraft.allowedUserIDs, []);
    if (!Array.isArray(allowedUserIDs) || allowedUserIDs.some(id => !Number.isSafeInteger(id) || id < 1)) {
      toast.err('授权用户 ID 必须为正整数数组'); return;
    }
    const headers = parseJson<Record<string, string>>(mcpDraft.headers, {});
    if (!headers || Array.isArray(headers) || typeof headers !== 'object' || Object.values(headers).some(v => typeof v !== 'string')) {
      toast.err('headers 不是合法的 JSON 对象');
      return;
    }
    setSaving(true);
    try {
      const payload: Partial<MCPServer> = {
        name: mcpDraft.name.trim(),
        transport: mcpDraft.transport,
        url: mcpDraft.url.trim(),
        config_version: editingMcp?.config_version,
        enabled: mcpDraft.enabled,
        allowed_user_ids: allowedUserIDs,
        clear_headers: mcpDraft.clearHeaders,
        headers,
      };
      if (editingMcp) {
        await updateMCPServer(editingMcp.id, payload);
        toast.ok('MCP Server 已更新');
      } else {
        await createMCPServer(payload);
        toast.ok('MCP Server 已创建');
      }
      setMcpOpen(false);
      await load();
    } catch (e) {
      toast.err(errMsg(e));
    } finally {
      setSaving(false);
    }
  };

  /* ---- MCP：行内动作 ---- */

  /** 同步工具清单：toast 展示同步数量后刷新列表 */
  const handleSync = async (s: MCPServer) => {
    setBusyId(s.id);
    try {
      const res = await syncMCPServer(s.id);
      toast.ok(`已同步 ${res.length} 个工具，新工具需审阅调用策略`);
      await load();
    } catch (e) {
      toast.err(errMsg(e));
    } finally {
      setBusyId(null);
    }
  };

  const handleMcpHealth = async (s: MCPServer) => {
    setBusyId(s.id);
    try {
      const res = await checkMCPServer(s.id);
      toast.ok(`健康检查完成：${res.status}`);
      await load();
    } catch (e) {
      toast.err(errMsg(e));
    } finally {
      setBusyId(null);
    }
  };

  const handleMigrate = async (s: MCPServer) => {
    setBusyId(s.id);
    try { await migrateMCPCredentials(s.id); toast.ok('旧配置已加密迁移'); await load(); }
    catch (e) { toast.err(errMsg(e)); } finally { setBusyId(null); }
  };

  /* ---- A2A：注册 / 健康检查 / 委派任务 ---- */

  const submitRegister = async () => {
    if (!regDraft.base_url.trim()) {
      toast.err('Base URL 为必填项');
      return;
    }
    setSaving(true);
    try {
      // name 留空时不传，由后端注册时从 Agent Card 名片取
      const payload: { base_url: string; name?: string } = { base_url: regDraft.base_url.trim() };
      const name = regDraft.name.trim();
      if (name) payload.name = name;
      await registerA2AAgent(payload);
      toast.ok('远程 Agent 已注册');
      setRegisterOpen(false);
      setRegDraft({ base_url: '', name: '' });
      await load();
    } catch (e) {
      toast.err(errMsg(e));
    } finally {
      setSaving(false);
    }
  };

  const handleAgentHealth = async (a: A2AAgent) => {
    setBusyId(a.id);
    try {
      const res = await checkA2AAgent(a.id);
      toast.ok(`健康检查完成：${res.status}`);
      await load();
    } catch (e) {
      toast.err(errMsg(e));
    } finally {
      setBusyId(null);
    }
  };

  const openTask = (a: A2AAgent) => {
    setTaskAgent(a);
    setTaskText('');
    setTaskResult('');
  };

  const submitTask = async () => {
    if (!taskAgent) return;
    if (!taskText.trim()) {
      toast.err('请输入任务内容');
      return;
    }
    setTaskRunning(true);
    try {
      const res = await sendA2ATask(taskAgent.id, taskText.trim());
      setTaskResult(clip(JSON.stringify(res ?? null, null, 2)));
    } catch (e) {
      toast.err(errMsg(e));
    } finally {
      setTaskRunning(false);
    }
  };

  /* ---- 删除 ---- */

  const confirmDelete = async () => {
    if (!delTarget) return;
    setDeleting(true);
    try {
      if (delTarget.kind === 'mcp') await deleteMCPServer(delTarget.id);
      else await deleteA2AAgent(delTarget.id);
      toast.ok('删除成功');
      setDelTarget(null);
      await load();
    } catch (e) {
      toast.err(errMsg(e));
    } finally {
      setDeleting(false);
    }
  };

  return (
    <div ref={rootRef} className="page-container">
      <PageHeader
        title="集成中心"
        desc="接入外部能力：MCP 工具服务与 A2A 远程智能体，支持健康检查与在线调试"
      />

      {loading ? (
        <div className="flex justify-center py-24">
          <Spinner className="h-6 w-6 text-lime" />
        </div>
      ) : (
        <div className="space-y-6">
          {/* 区块一：MCP Server */}
          <section className="panel overflow-hidden">
            <div className="flex items-center justify-between border-b border-smoke/60 px-4 py-3">
              <h2 className="section-title">MCP Server</h2>
              <Button onClick={openMcpCreate}>＋ 新增 MCP Server</Button>
            </div>
            {servers.length === 0 ? (
              <EmptyState
                title="暂无 MCP Server"
                hint="接入 MCP 工具服务后，Agent 即可调用其暴露的工具"
                action={<Button onClick={openMcpCreate}>新增 MCP Server</Button>}
              />
            ) : (
              <div className="overflow-x-auto">
                <table className="w-full text-left text-sm">
                  <thead>
                    <tr className="border-b border-smoke/60 text-xs text-ash">
                      <th className="px-4 py-2.5 font-medium">名称</th>
                      <th className="px-4 py-2.5 font-medium">传输</th>
                      <th className="px-4 py-2.5 font-medium">端点</th>
                      <th className="px-4 py-2.5 font-medium">状态</th>
                      <th className="px-4 py-2.5 font-medium">工具数</th>
                      <th className="px-4 py-2.5 font-medium">最近健康检查</th>
                      <th className="px-4 py-2.5 text-right font-medium">操作</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-smoke/40">
                    {servers.map((s) => (
                      <tr key={s.id} className="transition-colors hover:bg-graphite/40">
                        <td className="px-4 py-2.5 text-bone">{s.name}{!s.enabled && <span className="ml-2 text-ash">已停用</span>}{s.credentials_pending_migration && <span className="ml-2 text-coral">凭据待迁移</span>}</td>
                        <td className="px-4 py-2.5">
                          <Badge tone={TRANSPORT_TONE[s.transport] ?? 'gray'}>{s.transport}</Badge>
                        </td>
                        {/* stdio 展示 command，远程传输展示 url */}
                        <td className="px-4 py-2.5">
                          <span
                            className="block max-w-[220px] truncate font-mono text-xs text-fog"
                            title={s.command || s.url}
                          >
                            {s.command || s.url || '—'}
                          </span>
                        </td>
                        <td className="px-4 py-2.5">
                          <StatusBadge status={s.status} />
                        </td>
                        <td className="px-4 py-2.5 font-mono text-xs text-fog">
                          {s.tool_count}
                        </td>
                        <td className="px-4 py-2.5 text-xs text-fog">
                          {relativeTime(s.last_health_at) || '—'}
                        </td>
                        <td className="px-4 py-2.5">
                          <div className="flex justify-end gap-3 whitespace-nowrap text-xs">
                            {s.credentials_pending_migration && <button disabled={busyId === s.id} className="text-coral" onClick={() => void handleMigrate(s)}>加密迁移</button>}
                            <button
                              disabled={busyId === s.id}
                              className="text-ash transition-colors hover:text-lime disabled:opacity-40"
                              onClick={() => void handleSync(s)}
                            >
                              同步工具
                            </button>
                            <button
                              disabled={busyId === s.id}
                              className="text-ash transition-colors hover:text-lime disabled:opacity-40"
                              onClick={() => void handleMcpHealth(s)}
                            >
                              健康检查
                            </button>
                            <button
                              className="text-ash transition-colors hover:text-lime"
                              onClick={() => setToolsServer(s)}
                            >
                              工具与策略
                            </button>
                            <button
                              className="text-ash transition-colors hover:text-lime"
                              onClick={() => openMcpEdit(s)}
                            >
                              编辑
                            </button>
                            <button
                              className="text-ash transition-colors hover:text-coral"
                              onClick={() => setDelTarget({ kind: 'mcp', id: s.id, label: s.name })}
                            >
                              删除
                            </button>
                          </div>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </section>

          {/* 区块二：A2A 远程 Agent */}
          <section className="panel overflow-hidden">
            <div className="flex items-center justify-between border-b border-smoke/60 px-4 py-3">
              <h2 className="section-title">A2A 远程 Agent</h2>
              <Button onClick={() => setRegisterOpen(true)}>＋ 注册远程 Agent</Button>
            </div>
            {agents.length === 0 ? (
              <EmptyState
                title="暂无远程 Agent"
                hint="注册 A2A 远程 Agent 后，可将任务委派给其协作完成"
                action={<Button onClick={() => setRegisterOpen(true)}>注册远程 Agent</Button>}
              />
            ) : (
              <div className="overflow-x-auto">
                <table className="w-full text-left text-sm">
                  <thead>
                    <tr className="border-b border-smoke/60 text-xs text-ash">
                      <th className="px-4 py-2.5 font-medium">名称</th>
                      <th className="px-4 py-2.5 font-medium">Base URL</th>
                      <th className="px-4 py-2.5 font-medium">状态</th>
                      <th className="px-4 py-2.5 font-medium">名片摘要</th>
                      <th className="px-4 py-2.5 font-medium">最近健康检查</th>
                      <th className="px-4 py-2.5 text-right font-medium">操作</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-smoke/40">
                    {agents.map((a) => (
                      <tr key={a.id} className="transition-colors hover:bg-graphite/40">
                        <td className="px-4 py-2.5 text-bone">{a.name}</td>
                        <td className="px-4 py-2.5">
                          <span
                            className="block max-w-[220px] truncate font-mono text-xs text-fog"
                            title={a.base_url}
                          >
                            {a.base_url}
                          </span>
                        </td>
                        <td className="px-4 py-2.5">
                          <StatusBadge status={a.status} />
                        </td>
                        <td className="px-4 py-2.5">
                          {/* card 名片结构宽容处理：缺失或字段缺失时降级展示 */}
                          {a.card ? (
                            <div className="min-w-0">
                              <p className="max-w-[240px] truncate text-xs text-mist">
                                {(a.card as { name?: string }).name || '未命名'}
                              </p>
                              <p className="max-w-[240px] truncate text-xs text-ash">
                                {(a.card as { description?: string }).description ?? ''}
                              </p>
                            </div>
                          ) : (
                            <span className="text-xs text-ash">—</span>
                          )}
                        </td>
                        <td className="px-4 py-2.5 text-xs text-fog">
                          {relativeTime(a.last_health_at) || '—'}
                        </td>
                        <td className="px-4 py-2.5">
                          <div className="flex justify-end gap-3 whitespace-nowrap text-xs">
                            <button
                              disabled={busyId === a.id}
                              className="text-ash transition-colors hover:text-lime disabled:opacity-40"
                              onClick={() => void handleAgentHealth(a)}
                            >
                              健康检查
                            </button>
                            <button
                              className="text-ash transition-colors hover:text-lime"
                              onClick={() => openTask(a)}
                            >
                              委派任务
                            </button>
                            <button
                              className="text-ash transition-colors hover:text-coral"
                              onClick={() => setDelTarget({ kind: 'a2a', id: a.id, label: a.name })}
                            >
                              删除
                            </button>
                          </div>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </section>
        </div>
      )}

      {/* 新增 / 编辑 MCP Server */}
      <Modal
        open={mcpOpen}
        title={editingMcp ? '编辑 MCP Server' : '新增 MCP Server'}
        onClose={() => setMcpOpen(false)}
        width="max-w-2xl"
        footer={
          <>
            <Button variant="ghost" onClick={() => setMcpOpen(false)}>
              取消
            </Button>
            <Button loading={saving} onClick={() => void submitMcp()}>
              保存
            </Button>
          </>
        }
      >
        <div className="space-y-3">
          <div className="grid grid-cols-2 gap-3">
            <div>
              <label className="field-label">名称 *</label>
              <Input
                value={mcpDraft.name}
                placeholder="如 filesystem"
                onChange={(e) => patchMcp({ name: e.target.value })}
              />
            </div>
            <div>
              <label className="field-label">传输方式</label>
              <Select
                value={mcpDraft.transport}
                onChange={(e) => patchMcp({ transport: e.target.value })}
              >
                {TRANSPORTS.map((t) => (
                  <option key={t} value={t}>
                    {t}
                  </option>
                ))}
              </Select>
            </div>
          </div>
          <div>
            <label className="field-label">URL（Streamable HTTP）</label>
            <Input
              value={mcpDraft.url}
              placeholder="如 https://mcp.example.com/mcp"
              onChange={(e) => patchMcp({ url: e.target.value })}
            />
          </div>
          <div>
            <label className="field-label">授权用户 ID（JSON 数组，空数组仅管理员）</label>
            <Input value={mcpDraft.allowedUserIDs} onChange={e => patchMcp({allowedUserIDs: e.target.value})} />
          </div>
          <label className="flex items-center gap-2 text-sm"><input type="checkbox" checked={mcpDraft.enabled} onChange={e => patchMcp({enabled: e.target.checked})} />启用服务</label>
          <div>
            <label className="field-label">鉴权头（JSON 对象，留空或 {'{}'} 保留已有凭据）</label>
            <Textarea rows={3} className="font-mono" value={mcpDraft.headers} onChange={e => patchMcp({headers: e.target.value})} />
            <p className="mt-1 text-xs text-ash">{editingMcp?.headers_configured ? '已配置加密凭据；原文不回显。更换凭据后需重新审阅工具。' : '凭据加密保存，禁止填入 URL。'}</p>
          </div>
          {editingMcp && <label className="flex items-center gap-2 text-sm"><input type="checkbox" checked={mcpDraft.clearHeaders} onChange={e => patchMcp({clearHeaders: e.target.checked})} />明确清除已有鉴权头</label>}
        </div>
      </Modal>

      {toolsServer && <MCPToolsModal server={toolsServer} onClose={() => setToolsServer(null)} />}

      {/* 注册远程 Agent */}
      <Modal
        open={registerOpen}
        title="注册远程 Agent"
        onClose={() => setRegisterOpen(false)}
        footer={
          <>
            <Button variant="ghost" onClick={() => setRegisterOpen(false)}>
              取消
            </Button>
            <Button loading={saving} onClick={() => void submitRegister()}>
              注册
            </Button>
          </>
        }
      >
        <div className="space-y-3">
          <div>
            <label className="field-label">Base URL *</label>
            <Input
              value={regDraft.base_url}
              placeholder="如 https://agent.example.com"
              onChange={(e) => setRegDraft((d) => ({ ...d, base_url: e.target.value }))}
            />
          </div>
          <div>
            <label className="field-label">名称（可选）</label>
            <Input
              value={regDraft.name}
              placeholder="留空则自动取名片名称；注册时会拉取 /.well-known/agent.json 验活"
              onChange={(e) => setRegDraft((d) => ({ ...d, name: e.target.value }))}
            />
          </div>
        </div>
      </Modal>

      {/* A2A 委派任务 */}
      <Modal
        open={taskAgent !== null}
        title={`委派任务 · ${taskAgent?.name ?? ''}`}
        onClose={() => setTaskAgent(null)}
        width="max-w-xl"
        footer={
          <>
            <Button variant="ghost" onClick={() => setTaskAgent(null)}>
              关闭
            </Button>
            <Button loading={taskRunning} onClick={() => void submitTask()}>
              发送任务
            </Button>
          </>
        }
      >
        <div className="space-y-3">
          <div>
            <label className="field-label">任务内容</label>
            <Textarea
              rows={4}
              value={taskText}
              placeholder="用自然语言描述要委派给该 Agent 的任务"
              onChange={(e) => setTaskText(e.target.value)}
            />
          </div>
          {taskResult && (
            <div>
              <label className="field-label">执行结果</label>
              <pre className="code-block max-h-64 whitespace-pre-wrap">{taskResult}</pre>
            </div>
          )}
        </div>
      </Modal>

      {/* 删除二次确认 */}
      <ConfirmDialog
        open={delTarget !== null}
        title={delTarget?.kind === 'mcp' ? '删除 MCP Server' : '删除远程 Agent'}
        content={
          delTarget
            ? `确认删除「${delTarget.label}」？该操作不可恢复。`
            : ''
        }
        danger
        confirmText="删除"
        loading={deleting}
        onCancel={() => setDelTarget(null)}
        onConfirm={() => void confirmDelete()}
      />
    </div>
  );
}
