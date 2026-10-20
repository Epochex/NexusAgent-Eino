/**
 * 工作流接口 —— 对应 router.go 工作流分组。
 * DSL 为 YAML 文本；提交返回 202；HITL 精确选择待审批节点。
 */
import { http } from '@/lib/request';
import type { Paged, PageQuery } from '@/types/api';
import type { Workflow, WorkflowRun, WorkflowOperation } from '@/types/workflow';

/** 工作流列表 */
export const listWorkflows = (q?: PageQuery) =>
  http.get<Paged<Workflow>>('/workflows', q);

/** 工作流详情 */
export const getWorkflow = (id: number) => http.get<Workflow>(`/workflows/${id}`);

/** 创建工作流（name/description/dsl） */
export const createWorkflow = (req: Partial<Workflow>) =>
  http.post<Workflow>('/workflows', req);

/** 更新工作流 */
export const updateWorkflow = (id: number, req: Partial<Workflow>) =>
  http.put<Workflow>(`/workflows/${id}`, req);

/** 删除工作流 */
export const deleteWorkflow = (id: number) => http.delete(`/workflows/${id}`);

/** DSL 静态校验：{dsl} → {valid:true}；不合法抛 ApiError(message 为校验错误) */
export const validateWorkflow = (dsl: string) =>
  http.post<{ valid: boolean }>('/workflows/validate', { dsl });

/** 持久化提交：网络重试复用同一 requestKey，返回可查询的 run。 */
export const runWorkflow = (workflowId: number, input: Record<string, unknown>, requestKey: string) =>
  http.post<WorkflowRun>('/workflows/run', {
    workflow_id: workflowId,
    input,
    request_key: requestKey,
  });

/** 执行记录列表（可按 workflow_id 过滤） */
export const listRuns = (q?: PageQuery & { workflow_id?: number }) =>
  http.get<Paged<WorkflowRun>>('/workflows/runs', q);

/** 执行详情（含 steps 轨迹） */
export const getRun = (id: number) => http.get<WorkflowRun>(`/workflows/runs/${id}`);

/** HITL 审批：通过/驳回 */
export const approveRun = (id: number, approved: boolean, interruptId: string, argsHash: string, comment?: string) =>
  http.post<WorkflowRun>(`/workflows/runs/${id}/approve`, {
    approved,
    interrupt_id: interruptId,
    args_hash: argsHash,
    comment: comment ?? '',
  });

export const cancelRun = (id: number) => http.post<WorkflowRun>(`/workflows/runs/${id}/cancel`, {});
export const getRunOperations = (id: number) => http.get<WorkflowOperation[]>(`/workflows/runs/${id}/operations`);
export const reconcileRun = (id: number, operation: WorkflowOperation, output: unknown, evidence: string) =>
  http.post<WorkflowRun>(`/workflows/runs/${id}/reconcile`, {
    operation_id: operation.id, args_hash: operation.args_hash, output, evidence,
  });
