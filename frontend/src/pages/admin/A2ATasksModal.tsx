import { useEffect, useState } from 'react';
import { cancelA2ATask, getA2ATask, listA2ATasks, sendA2ATask } from '@/api/admin';
import { Modal } from '@/components/ui/Modal';
import { Button, Spinner, StatusBadge, Textarea } from '@/components/ui/primitives';
import { relativeTime } from '@/lib/format';
import { errMsg } from '@/lib/request';
import { toast } from '@/stores/ui';
import type { A2AAgent, A2ATask } from '@/types/admin';

const pending = (task?: A2ATask | null) => !!task && ['submitted','working','dispatching'].includes(task.state);

export function A2ATasksModal({agent,onClose}:{agent:A2AAgent;onClose:()=>void}) {
  const [rows,setRows] = useState<A2ATask[]>([]);
  const [total,setTotal] = useState(0);
  const [page,setPage] = useState(1);
  const [revision,setRevision] = useState(0);
  const [loading,setLoading] = useState(true);
  const [busy,setBusy] = useState(false);
  const [draft,setDraft] = useState('');
  const [lastRequest,setLastRequest] = useState<{key:string;text:string}|null>(null);
  const [selected,setSelected] = useState<A2ATask|null>(null);
  const [pollError,setPollError] = useState('');

  const accept = (incoming:A2ATask,force=false) => {
	setSelected(previous => previous?.id===incoming.id && previous.version>incoming.version ? previous : force ? incoming : previous?.id===incoming.id ? incoming : previous);
    setRows(previous => previous.map(item => item.id===incoming.id && incoming.version>=item.version ? incoming : item));
  };
  useEffect(() => {
    let active=true;setLoading(true);
    listA2ATasks(agent.id,page).then(result => {
      if(!active)return;
      setRows(previous => result.list.map(item => {
        const current=previous.find(row => row.id===item.id);
        return current && current.version>item.version ? current : item;
      }));
      setTotal(result.total);
    }).catch(error => {if(active)toast.err(errMsg(error));}).finally(() => {if(active)setLoading(false);});
    return () => {active=false;};
  },[agent.id,page,revision]);

  const selectedId=selected?.id;
  const shouldPoll=pending(selected) && selected?.can_refresh;
  useEffect(() => {
    if(!selectedId || !shouldPoll || busy)return;
    let active=true,inFlight=false;
    const controller=new AbortController();
    const poll=async () => {
      if(inFlight)return;
      inFlight=true;
      try {
        const row=await getA2ATask(selectedId,true,controller.signal);
        if(active){accept(row);setPollError('');}
      }catch(error){if(active && !controller.signal.aborted)setPollError(errMsg(error));}
      finally {inFlight=false;}
    };
    void poll();
    const interval=window.setInterval(() => void poll(),5000);
    return () => {active=false;controller.abort();window.clearInterval(interval);};
  },[selectedId,shouldPoll,busy]);

  const send = async (retry=false) => {
    const request=retry ? lastRequest : {key:crypto.randomUUID(),text:draft.trim()};
    if(!request || !request.text)return;
    setLastRequest(request);setBusy(true);setPollError('');
    try {
      const row=await sendA2ATask(agent.id,request.text,request.key);
      accept(row,true);setPage(1);setRevision(n => n+1);
    }catch(error){toast.err(errMsg(error));}
    finally {setBusy(false);}
  };
  const refresh = async () => {
    if(!selected)return;setBusy(true);
    try {accept(await getA2ATask(selected.id,true));setPollError('');}
    catch(error){setPollError(errMsg(error));}finally{setBusy(false);}
  };
  const cancel = async () => {
    if(!selected)return;setBusy(true);
    try {accept(await cancelA2ATask(selected.id));setRevision(n => n+1);}
    catch(error){toast.err(errMsg(error));}finally{setBusy(false);}
  };
  const newTask=() => {setLastRequest(null);setDraft('');setSelected(null);setPollError('');};

  return <Modal open title={`委派与记录 · ${agent.name}`} onClose={onClose} width="max-w-4xl"
    footer={<Button variant="ghost" onClick={onClose}>关闭</Button>}>
    <div className="grid gap-4 md:grid-cols-[220px_1fr]">
      <div className="min-w-0">
        <p className="mb-2 text-xs text-fog">我的委派记录</p>
        {loading ? <Spinner /> : rows.length===0 ? <p className="text-xs text-ash">暂无记录</p> :
          <div className="max-h-96 space-y-1 overflow-y-auto">{rows.map(row => <button key={row.id} disabled={busy}
            className={`block w-full rounded border p-2 text-left ${selected?.id===row.id ? 'border-lime' : 'border-smoke'}`}
            onClick={() => {accept(row,true);setPollError('');}}>
            <StatusBadge status={row.state} />
            <span className="mt-1 block truncate font-mono text-xs text-fog">{row.id}</span>
            <span className="text-xs text-ash">{relativeTime(row.updated_at)}</span>
          </button>)}</div>}
        <div className="mt-3 flex items-center justify-between text-xs">
          <Button variant="ghost" disabled={page===1 || busy} onClick={() => setPage(p => p-1)}>上一页</Button>
          <span>{page} / {Math.max(1,Math.ceil(total/20))}</span>
          <Button variant="ghost" disabled={page*20>=total || busy} onClick={() => setPage(p => p+1)}>下一页</Button>
        </div>
      </div>
      <div className="min-w-0 space-y-3">
        <div>
          <label className="field-label">任务内容</label>
          <Textarea rows={3} maxLength={8192} value={draft} disabled={busy || !!lastRequest}
            placeholder="用自然语言描述委派任务" onChange={e => setDraft(e.target.value)} />
          <p className="mt-1 text-xs text-ash">发送后保留请求号；网络异常时可核对原请求，避免重复执行。</p>
        </div>
        <div className="flex flex-wrap gap-2">
          <Button disabled={!agent.enabled || busy || !!lastRequest || !draft.trim()} onClick={() => void send()}>发送任务</Button>
          {lastRequest && <Button variant="ghost" disabled={busy} onClick={() => void send(true)}>核对原请求</Button>}
          <Button variant="ghost" disabled={busy} onClick={newTask}>新任务</Button>
        </div>
        {!agent.enabled && <p className="text-xs text-coral">Agent 已停用，可查看已有记录。</p>}
        {selected && <div className="space-y-3 border-t border-smoke pt-3">
          <div className="flex items-center justify-between gap-2">
            <StatusBadge status={selected.state} />
            <span className="text-xs text-ash">{relativeTime(selected.updated_at)}</span>
          </div>
          {selected.error && <p className="text-sm text-coral">{selected.error}</p>}
          {pollError && <p className="text-xs text-coral">状态刷新失败：{pollError}</p>}
          {selected.state==='unknown' && !selected.task_id && <p className="text-xs text-fog">尚无已确认的远端任务号，请按核对信息检查远端实际记录。</p>}
          {selected.text && <pre className="code-block max-h-72 overflow-auto whitespace-pre-wrap">{selected.text}</pre>}
          <div className="flex gap-2">
            <Button variant="ghost" disabled={busy || !selected.can_refresh || !agent.enabled} onClick={() => void refresh()}>刷新状态</Button>
            <Button variant="danger" disabled={busy || !selected.can_cancel || !agent.enabled} onClick={() => void cancel()}>取消远端任务</Button>
          </div>
          {pending(selected) && selected.can_refresh && <p className="text-xs text-ash">每 5 秒核对远端状态；关闭页面不会取消已接受的委派。</p>}
          <details className="text-xs text-fog">
            <summary>核对信息</summary>
            <p className="mt-2 break-all">请求号：{selected.request_key}</p>
            <p className="break-all">记录号 / 消息号：{selected.id}</p>
            <p className="break-all">远端任务号：{selected.task_id || '尚未确认'}</p>
          </details>
        </div>}
      </div>
    </div>
  </Modal>;
}

