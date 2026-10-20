import { useEffect, useState } from 'react';
import { callMCPTool, listMCPTools, reviewMCPTool } from '@/api/admin';
import { Modal } from '@/components/ui/Modal';
import { Button, Select, Textarea, Spinner } from '@/components/ui/primitives';
import { errMsg } from '@/lib/request';
import { toast } from '@/stores/ui';
import type { MCPServer } from '@/types/admin';
import type { Tool } from '@/types/agent';

export function MCPToolsModal({server, onClose}: {server: MCPServer; onClose: () => void}) {
  const [rows,setRows] = useState<Tool[]>([]);
  const [total,setTotal] = useState(0);
  const [page,setPage] = useState(1);
  const [loading,setLoading] = useState(true);
  const [selected,setSelected] = useState<Tool | null>(null);
  const [policy,setPolicy] = useState<Tool['replay_policy']>('manual');
  const [dangerous,setDangerous] = useState(true);
  const [evidence,setEvidence] = useState('');
  const [args,setArgs] = useState('{}');
  const [output,setOutput] = useState('');
  const [busy,setBusy] = useState(false);
  const [reload,setReload] = useState(0);
  useEffect(() => {
    let active = true; setLoading(true); setSelected(null);
    listMCPTools(server.id,page).then(result => {if(active){setRows(result.list);setTotal(result.total);}})
      .catch(error => {if(active){setRows([]);toast.err(errMsg(error));}})
      .finally(() => {if(active)setLoading(false);});
    return () => {active = false;};
  },[server.id,page,reload]);
  const select = (item: Tool) => {
    setSelected(item);setPolicy(item.replay_policy);setDangerous(item.is_dangerous);
    setEvidence(item.replay_evidence ?? '');setArgs('{}');setOutput('');
  };
  const save = async () => {
    if(!selected)return;
    setBusy(true);
    try {
      const row = await reviewMCPTool(server.id,selected.id,{
        binding_version:selected.binding_version,replay_policy:policy,is_dangerous:dangerous,evidence,
      });
      setRows(old => old.map(item => item.id === row.id ? row : item));select(row);
      toast.ok('调用与重试策略已更新，已有旧授权将失效');
    } catch(error) {toast.err(errMsg(error));setReload(n => n+1);}
    finally {setBusy(false);}
  };
  const debug = async () => {
    if(!selected)return;
    let parsed: unknown;
    try {parsed=JSON.parse(args);} catch {toast.err('参数必须为 JSON 对象');return;}
    if(!parsed || typeof parsed !== 'object' || Array.isArray(parsed)){toast.err('参数必须为 JSON 对象');return;}
    setBusy(true);setOutput('');
    try {
      const result=await callMCPTool(server.id,selected.mcp_name ?? selected.name,parsed as Record<string,unknown>);
      setOutput(typeof result.output === 'string' ? result.output : JSON.stringify(result.output,null,2));
    } catch(error){toast.err(errMsg(error));} finally {setBusy(false);}
  };
  const canDebug = server.enabled && selected?.replay_policy === 'readonly' && !selected.is_dangerous && !!selected.policy_reviewed_at;
  return <Modal open title={`工具与策略 · ${server.name}`} onClose={onClose} width="max-w-4xl"
    footer={<Button variant="ghost" onClick={onClose}>关闭</Button>}>
    <p className="mb-3 text-xs text-fog">新工具默认需审批且不自动重试。只读或幂等策略必须由管理员核查远端实现并记录依据；写操作通过工作流审批执行。</p>
    <div className="grid gap-4 md:grid-cols-[220px_1fr]">
      <div>
        {loading ? <Spinner /> : rows.length === 0 ? <p className="text-sm text-ash">暂无活动工具，请先同步。</p> :
          <div className="max-h-96 space-y-1 overflow-y-auto">{rows.map(item => <button key={item.id} disabled={busy}
            className={`block w-full rounded border px-2 py-2 text-left text-xs ${selected?.id === item.id ? 'border-lime text-lime' : 'border-smoke text-fog'}`}
            onClick={() => select(item)}>
            <span className="block truncate font-mono">{item.mcp_name ?? item.name}</span>
            <span className="text-ash">{item.replay_policy} · {item.is_dangerous ? '需审批' : '免审批'}</span>
          </button>)}</div>}
        <div className="mt-3 flex items-center justify-between text-xs">
          <Button variant="ghost" disabled={page===1 || busy} onClick={() => setPage(p => p-1)}>上一页</Button>
          <span>{page} / {Math.max(1,Math.ceil(total/100))}</span>
          <Button variant="ghost" disabled={page*100>=total || busy} onClick={() => setPage(p => p+1)}>下一页</Button>
        </div>
      </div>
      {selected ? <div className="min-w-0 space-y-3">
        <p className="break-all text-sm text-bone">{selected.description || selected.name}</p>
        <details className="text-xs text-fog"><summary>参数定义</summary><pre className="code-block max-h-48 overflow-auto whitespace-pre-wrap">{JSON.stringify(selected.parameters,null,2)}</pre></details>
        <div>
          <label className="field-label">重试策略</label>
          <Select disabled={busy} value={policy} onChange={e => {const value=e.target.value as Tool['replay_policy'];setPolicy(value);setDangerous(value!=='readonly');}}>
            <option value="manual">manual · 结果未知时人工核对</option>
            <option value="readonly">readonly · 已确认无副作用</option>
            <option value="idempotent">idempotent · 远端保证操作号幂等</option>
          </Select>
        </div>
        <label className="flex items-center gap-2 text-sm"><input type="checkbox" checked={dangerous} disabled={busy || policy!=='readonly'} onChange={e => setDangerous(e.target.checked)} />执行前需人工审批</label>
        <div>
          <label className="field-label">审阅依据（只读 / 幂等必填，8..512 字节）</label>
          <Textarea rows={2} value={evidence} disabled={busy} onChange={e => setEvidence(e.target.value)} placeholder="记录核查的接口行为、幂等键语义或文档版本" />
          {selected.policy_reviewed_at && <p className="mt-1 text-xs text-ash">最近审阅：用户 {selected.policy_reviewed_by} · {new Date(selected.policy_reviewed_at).toLocaleString()}</p>}
        </div>
        <Button loading={busy} onClick={() => void save()}>保存审阅策略</Button>
        <div className="border-t border-smoke pt-3">
          <label className="field-label">只读调试参数（JSON）</label>
          <Textarea rows={3} value={args} onChange={e => setArgs(e.target.value)} disabled={busy || !canDebug} />
          <p className="my-2 text-xs text-ash">仅已审阅、免审批的只读工具可调试。保存策略后生效。</p>
          <Button disabled={!canDebug || busy} onClick={() => void debug()}>执行只读调试</Button>
          {output && <pre className="code-block mt-2 max-h-64 overflow-auto whitespace-pre-wrap">{output}</pre>}
        </div>
      </div> : <p className="text-sm text-ash">选择工具查看定义与调用策略。</p>}
    </div>
  </Modal>;
}

