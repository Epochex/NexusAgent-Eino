/**
 * 长期记忆管理页 —— 记忆分页列表 + 归档 / 删除 + 手动触发记忆抽取。
 *
 * 职责：
 * 1. 分页展示长期记忆：内容、范围、重要度、命中统计、状态与时间线；
 * 2. 生效中的记忆可归档，任意记忆可删除（ConfirmDialog 二次确认）；
 * 3. 支持指定会话手动触发一次长期记忆抽取。
 */
import { useCallback, useEffect, useRef, useState } from 'react';
import { clsx } from 'clsx';
import {
  Badge,
  Button,
  EmptyState,
  Input,
  PageHeader,
  Spinner,
} from '@/components/ui/primitives';
import { Modal } from '@/components/ui/Modal';
import { ConfirmDialog } from '@/components/ui/ConfirmDialog';
import { archiveMemory, deleteMemory, extractMemory, listMemories, listMemoryJobs, retryMemoryJob, getMemoryProfile, memoryEmbeddingOptions, rebuildMemories, getMemoryRebuild } from '@/api/memory';
import type { Memory, MemoryJob, MemoryProfile, MemoryEmbeddingOption, MemoryRebuild } from '@/types/memory';
import { fullTime, relativeTime } from '@/lib/format';
import { errMsg } from '@/lib/request';
import { toast } from '@/stores/ui';
import { staggerEnter } from '@/lib/gsap';

const PAGE_SIZE = 20;
const JOB_LABEL: Record<MemoryJob['status'], string> = {queued:'等待处理',running:'处理中',retry_wait:'等待重试',done:'已完成',failed:'失败',superseded:'已被新版本替代',canceled:'已取消'};
const SYNC_LABEL: Record<Memory['index_status'], string> = {needs_reindex:'待重建',pending:'同步中',retry_wait:'同步重试中',failed:'同步失败',ready:'可召回',removed:'向量已清理'};

/** 记忆范围 → Badge（user → teal / agent → violet） */
const SCOPE_BADGE: Record<Memory['scope'], { text: string; tone: 'teal' | 'violet' }> = {
  user: { text: '用户', tone: 'teal' },
  agent: { text: 'Agent', tone: 'violet' },
};

/** 重要度 1-10 → 5 段小方块（每格代表 2 级）+ 数字 */
function ImportanceBar({ value }: { value: number }) {
  const filled = Math.min(5, Math.max(0, Math.ceil(value / 2)));
  return (
    <span className="inline-flex items-center gap-1.5">
      <span className="flex gap-0.5">
        {[1, 2, 3, 4, 5].map((i) => (
          <span
            key={i}
            className={clsx('h-1.5 w-2.5 rounded-sm', i <= filled ? 'bg-pulse' : 'bg-smoke')}
          />
        ))}
      </span>
      <span className="text-xs text-ash">{value}</span>
    </span>
  );
}

export function MemoriesPage() {
  const [list, setList] = useState<Memory[]>([]);
  const [total, setTotal] = useState(0);
  const [page, setPage] = useState(1);
  const [loading, setLoading] = useState(true);
  const [extractOpen, setExtractOpen] = useState(false);
  const [sessionId, setSessionId] = useState('');
  const [extracting, setExtracting] = useState(false);
  const [archivingId, setArchivingId] = useState<number | null>(null);
  const [delTarget, setDelTarget] = useState<Memory | null>(null);
  const tbodyRef = useRef<HTMLTableSectionElement>(null);
  const [jobs, setJobs] = useState<MemoryJob[]>([]);
  const [jobPage, setJobPage] = useState(1);
  const [jobTotal, setJobTotal] = useState(0);
  const [profile, setProfile] = useState<MemoryProfile | null>(null);
  const [rebuildOpen, setRebuildOpen] = useState(false);
  const [options, setOptions] = useState<MemoryEmbeddingOption[]>([]);
  const [embeddingId, setEmbeddingId] = useState(0);
  const [rebuilding, setRebuilding] = useState(false);
  const [rebuild, setRebuild] = useState<MemoryRebuild | null>(null);
  const rebuildKey = useRef('');
  const [retryingId, setRetryingId] = useState<number | null>(null);
  const loadSeq = useRef(0);

  const totalPages = Math.max(1, Math.ceil(total / PAGE_SIZE));

  /** 拉取指定页记忆 */
  const load = useCallback(async (p: number, quiet = false) => {
    const seq = ++loadSeq.current;
    if (!quiet) setLoading(true);
    try {
      const data = await listMemories({ page: p, page_size: PAGE_SIZE });
      if (seq !== loadSeq.current) return;
      setList(data.list);
      setTotal(data.total);
    } catch (e) {
      toast.err(errMsg(e));
    } finally {
      if (seq === loadSeq.current) setLoading(false);
    }
  }, []);

  const loadJobs = useCallback(async () => {
    const [data, activeProfile] = await Promise.all([listMemoryJobs({page:jobPage,page_size:PAGE_SIZE}), getMemoryProfile()]);
    setJobs(data.list ?? []); setJobTotal(data.total); setProfile(activeProfile);
  }, [jobPage]);
  useEffect(() => { void loadJobs().catch(e => toast.err(errMsg(e))); }, [loadJobs]);
  const hasWork = jobs.some(j => ['queued','running','retry_wait'].includes(j.status)) || list.some(m => ['pending','retry_wait'].includes(m.index_status)) || (rebuild !== null && rebuild.vector_pending === undefined) || (rebuild?.vector_pending ?? 0) > 0;
  useEffect(() => {
    if (!hasWork) return;
    let busy = false, stopped = false;
    const timer = window.setInterval(async () => {
      if (busy || stopped) return; busy = true;
      try {
        await Promise.all([load(page, true), loadJobs()]);
        if (rebuild) { const status = await getMemoryRebuild(rebuild.id); if (!stopped) setRebuild(status); }
      } catch (e) { if (!stopped) toast.err(errMsg(e)); }
      finally { busy = false; }
    }, 2000);
    return () => { stopped = true; window.clearInterval(timer); };
  }, [hasWork, page, load, loadJobs, rebuild]);

  const openRebuild = async () => {
    try { const choices = await memoryEmbeddingOptions(); setOptions(choices); setEmbeddingId(profile?.embedding_config_id ?? choices[0]?.id ?? 0); rebuildKey.current = crypto.randomUUID(); setRebuildOpen(true); }
    catch(e) { toast.err(errMsg(e)); }
  };
  const doRebuild = async () => {
    if (!embeddingId) return; setRebuilding(true);
    try { const result = await rebuildMemories(embeddingId, rebuildKey.current); setRebuild(result); setRebuildOpen(false); toast.ok(`已提交重建 ${result.reindexed} 条记忆，合并 ${result.merged} 条重复事实`); await Promise.all([load(page), loadJobs()]); }
    catch(e) { toast.err(errMsg(e)); } finally { setRebuilding(false); }
  };
  const retryJob = async (id: number) => {
    setRetryingId(id);
    try { await retryMemoryJob(id); toast.ok('已重新入队'); await Promise.all([load(page,true), loadJobs()]); }
    catch(e) { toast.err(errMsg(e)); } finally { setRetryingId(null); }
  };

  // 翻页 / 增删后的刷新统一走这里
  useEffect(() => {
    void load(page);
  }, [load, page]);

  // 列表数据变化 → 行级交错入场（GSAP）
  useEffect(() => {
    const els = tbodyRef.current ? Array.from(tbodyRef.current.children) : [];
    if (els.length === 0) return;
    const tween = staggerEnter(els);
    return () => {
      tween.kill();
    };
  }, [list]);

  /** 归档（仅生效中的记忆可操作：status 1 → 2） */
  const doArchive = async (m: Memory) => {
    setArchivingId(m.id);
    try {
      await archiveMemory(m.id);
      toast.ok('记忆已归档');
      await Promise.all([load(page), loadJobs()]);
    } catch (e) {
      toast.err(errMsg(e));
    } finally {
      setArchivingId(null);
    }
  };

  /** 删除：当前页删空且非首页时回退一页（触发 useEffect 重新拉取） */
  const confirmDelete = async () => {
    if (!delTarget) return;
    try {
      await deleteMemory(delTarget.id);
      toast.ok('记忆已删除');
      setDelTarget(null);
      if (list.length === 1 && page > 1) {
        setPage(page - 1);
        await loadJobs();
      } else {
        await Promise.all([load(page), loadJobs()]);
      }
    } catch (e) {
      toast.err(errMsg(e));
    }
  };

  /** 手动触发一次长期记忆抽取 */
  const doExtract = async () => {
    const sid = sessionId.trim();
    if (!sid) {
      toast.err('请填写会话 ID');
      return;
    }
    setExtracting(true);
    try {
      const job = await extractMemory({ session_id: sid });
      toast.ok(`抽取任务 #${job.id} 已提交，向量同步后才可召回`);
      setExtractOpen(false);
      setSessionId('');
      setPage(1); setJobPage(1);
      await Promise.all([load(1), loadJobs()]);
    } catch (e) {
      toast.err(errMsg(e));
    } finally {
      setExtracting(false);
    }
  };

  return (
    <div className="page-container">
      <PageHeader
        title="长期记忆"
        desc="跨会话沉淀的用户与 Agent 记忆，可归档或删除"
        action={<div className="flex gap-2"><Button variant="ghost" onClick={() => void openRebuild()}>重建记忆索引</Button><Button onClick={() => setExtractOpen(true)}>记住一次对话</Button></div>}
      />

      <div className="mb-4 text-xs text-ash">{profile ? `当前模型 #${profile.embedding_config_id} · ${profile.embedding_dimension} 维 · ${profile.embedding_version.slice(0,12)}` : '尚无固定模型空间。旧记忆需要显式重建后才能召回。'}{rebuild && <span className="ml-3">重建 #{rebuild.id}：已同步 {rebuild.vector_done ?? 0} / {rebuild.reindexed + rebuild.merged}，待处理 {rebuild.vector_pending ?? '查询中'}，失败 {rebuild.vector_failed ?? 0}，被新版本替代 {rebuild.vector_superseded ?? 0}</span>}</div>

      {loading ? (
        <div className="flex justify-center py-20">
          <Spinner className="h-6 w-6 text-lime" />
        </div>
      ) : list.length === 0 ? (
        <EmptyState
          title="暂无长期记忆"
          hint="与 Agent 对话积累的记忆会出现在这里，也可点击右上角手动触发抽取"
        />
      ) : (
        <>
          {/* 记忆列表：窄屏横向滚动 */}
          <div className="panel overflow-hidden">
            <div className="overflow-x-auto">
              <table className="w-full text-left text-sm">
                <thead>
                  <tr className="border-b border-smoke/60 text-xs tracking-wider text-ash">
                    <th className="px-4 py-3 font-medium">记忆内容</th>
                    <th className="px-4 py-3 font-medium">范围</th>
                    <th className="px-4 py-3 font-medium">重要度</th>
                    <th className="px-4 py-3 font-medium">命中</th>
                    <th className="px-4 py-3 font-medium">最近命中</th>
                    <th className="px-4 py-3 font-medium">状态</th>
                    <th className="px-4 py-3 font-medium">创建时间</th>
                    <th className="px-4 py-3 text-right font-medium">操作</th>
                  </tr>
                </thead>
                <tbody ref={tbodyRef}>
                  {list.map((m) => (
                    <tr
                      key={m.id}
                      className="border-b border-smoke/40 align-top transition-colors last:border-0 hover:bg-carbon/50"
                    >
                      {/* content 全文展示 */}
                      <td className="max-w-md whitespace-pre-wrap break-words px-4 py-3 text-mist">
                        {m.content}
                      </td>
                      <td className="px-4 py-3">
                        <Badge tone={SCOPE_BADGE[m.scope].tone}>
                          {SCOPE_BADGE[m.scope].text}
                        </Badge>
                      </td>
                      <td className="px-4 py-3">
                        <ImportanceBar value={m.importance} />
                      </td>
                      <td className="px-4 py-3 text-xs text-fog">{m.hit_count}</td>
                      <td className="px-4 py-3 text-xs text-fog">
                        {relativeTime(m.last_hit_at) || '—'}
                      </td>
                      {/* 状态：1 生效 / 2 已归档 */}
                      <td className="px-4 py-3">
                        {m.status === 1 ? (
                          <Badge tone={m.index_status === 'ready' && m.indexed_revision === m.revision ? 'pulse' : m.index_status === 'failed' ? 'coral' : 'gray'}>{SYNC_LABEL[m.index_status] ?? m.index_status} · v{m.revision}</Badge>
                        ) : (
                          <Badge tone="gray">已归档</Badge>
                        )}
                      </td>
                      <td className="px-4 py-3 text-xs text-fog">{fullTime(m.created_at)}</td>
                      <td className="px-4 py-3">
                        <div className="flex justify-end gap-2">
                          {m.status === 1 && (
                            <Button
                              variant="ghost"
                              className="px-2.5 py-1 text-xs"
                              loading={archivingId === m.id}
                              onClick={() => void doArchive(m)}
                            >
                              归档
                            </Button>
                          )}
                          <Button
                            variant="danger"
                            className="px-2.5 py-1 text-xs"
                            onClick={() => setDelTarget(m)}
                          >
                            删除
                          </Button>
                        </div>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>

          {/* 分页条 */}
          <div className="mt-4 flex items-center justify-between text-xs text-ash">
            <span>共 {total} 条</span>
            <div className="flex items-center gap-3">
              <Button
                variant="ghost"
                className="px-3 py-1 text-xs"
                disabled={page <= 1 || loading}
                onClick={() => setPage((p) => p - 1)}
              >
                上一页
              </Button>
              <span>
                第 {page} / {totalPages} 页
              </span>
              <Button
                variant="ghost"
                className="px-3 py-1 text-xs"
                disabled={page >= totalPages || loading}
                onClick={() => setPage((p) => p + 1)}
              >
                下一页
              </Button>
            </div>
          </div>
        </>
      )}

      <section className="panel mt-6 p-4">
        <div className="mb-3 flex items-center justify-between"><h2 className="text-sm font-medium">记忆处理任务</h2><Button variant="ghost" className="text-xs" onClick={() => void Promise.all([load(page,true),loadJobs()]).catch(e=>toast.err(errMsg(e)))}>刷新</Button></div>
        <p className="mb-3 text-xs text-ash">抽取完成表示事实和同步任务已提交；只有记忆显示“可召回”才会参与对话。失败任务可在修复模型服务后重试。</p>
        {jobs.length === 0 ? <p className="text-xs text-ash">暂无处理任务</p> : <div className="overflow-x-auto"><table className="w-full text-left text-xs"><thead><tr className="text-ash"><th className="py-2">任务</th><th>对象</th><th>状态 / 尝试</th><th>说明</th><th /></tr></thead><tbody>{jobs.map(j=><tr key={j.id} className="border-t border-smoke/50"><td className="py-3">#{j.id} {j.kind==='extract'?'事实抽取':j.vector_action==='delete'?'向量清理':'向量同步'}</td><td>{j.memory_id ? `记忆 #${j.memory_id} v${j.memory_revision}` : j.session_id?.slice(0,12)}</td><td>{j.kind==='extract' && j.status==='done'?'事实已提交':JOB_LABEL[j.status]} · {j.epoch_attempts}/{j.max_attempts}（累计 {j.attempts}）</td><td className="max-w-md break-words py-2 text-ash">{j.error || j.warnings?.join('；') || '—'}</td><td>{j.status==='failed'&&<Button variant="ghost" className="text-xs" loading={retryingId===j.id} onClick={()=>void retryJob(j.id)}>重试</Button>}</td></tr>)}</tbody></table></div>}
        <div className="mt-3 flex items-center justify-end gap-3 text-xs text-ash"><span>共 {jobTotal} 个任务</span><Button variant="ghost" disabled={jobPage<=1} onClick={()=>setJobPage(p=>p-1)}>上一页</Button><span>{jobPage}</span><Button variant="ghost" disabled={jobPage*PAGE_SIZE>=jobTotal} onClick={()=>setJobPage(p=>p+1)}>下一页</Button></div>
      </section>

      <Modal open={rebuildOpen} title="重建记忆索引" onClose={()=>setRebuildOpen(false)} width="max-w-md" footer={<><Button variant="ghost" onClick={()=>setRebuildOpen(false)}>取消</Button><Button loading={rebuilding} disabled={!embeddingId} onClick={()=>void doRebuild()}>确认重建</Button></>}>
        <p className="mb-4 text-sm text-fog">保留有效记忆内容并重新向量化，可迁移旧索引或更换模型。重建期间这些记忆暂停召回；同范围的完全重复事实会合并。失败可重试。</p>
        <label className="field-label" htmlFor="memory-embedding">目标向量模型</label><select id="memory-embedding" className="input w-full bg-carbon p-2 text-sm" value={embeddingId} onChange={e=>{setEmbeddingId(Number(e.target.value));rebuildKey.current=crypto.randomUUID();}}>{options.map(o=><option key={o.id} value={o.id}>{o.alias} · {o.dimension} 维</option>)}</select>
        {options.length===0&&<p className="mt-2 text-xs text-ash">请先由管理员配置可用的向量模型。</p>}
      </Modal>

      {/* 手动抽取 Modal */}
      <Modal
        open={extractOpen}
        title="记住一次对话"
        onClose={() => setExtractOpen(false)}
        width="max-w-md"
        footer={
          <>
            <Button variant="ghost" onClick={() => setExtractOpen(false)}>
              取消
            </Button>
            <Button loading={extracting} onClick={() => void doExtract()}>
              开始抽取
            </Button>
          </>
        }
      >
        <form
          onSubmit={(e) => {
            e.preventDefault();
            void doExtract();
          }}
        >
          <label className="field-label">会话 ID *</label>
          <Input
            value={sessionId}
            onChange={(e) => setSessionId(e.target.value)}
            placeholder="输入要抽取记忆的会话 ID"
          />
          <p className="mt-1 text-xs text-ash">
            将对指定会话执行长期记忆抽取，结果稍后出现在列表中
          </p>
        </form>
      </Modal>

      {/* 删除二次确认 */}
      <ConfirmDialog
        open={delTarget !== null}
        title="删除记忆"
        content="删除后该条长期记忆不可恢复，确认删除？"
        danger
        confirmText="删除"
        onCancel={() => setDelTarget(null)}
        onConfirm={() => void confirmDelete()}
      />
    </div>
  );
}
