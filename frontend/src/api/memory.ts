/**
 * 记忆接口 —— 对应 router.go 记忆分组。
 */
import { http } from '@/lib/request';
import type { Paged, PageQuery } from '@/types/api';
import type { ExtractMemoryReq, Memory, MemoryJob, MemoryProfile, MemoryEmbeddingOption, MemoryRebuild } from '@/types/memory';

/** 记忆列表（分页） */
export const listMemories = (q?: PageQuery) =>
  http.get<Paged<Memory>>('/memories', q);

/** 删除记忆 */
export const deleteMemory = (id: number) => http.delete(`/memories/${id}`);

/** 归档记忆（status 1 生效 → 2 归档） */
export const archiveMemory = (id: number) =>
  http.post(`/memories/${id}/archive`);

/** 手动抽取："记住这次对话"（对指定会话跑长期记忆抽取） */
export const extractMemory = (req: ExtractMemoryReq) =>
  http.post<MemoryJob>('/memories/extract', req);

export const listMemoryJobs = (q?: PageQuery) => http.get<Paged<MemoryJob>>('/memories/jobs', q);
export const retryMemoryJob = (id: number) => http.post(`/memories/jobs/${id}/retry`);
export const getMemoryProfile = () => http.get<MemoryProfile | null>('/memories/profile');
export const memoryEmbeddingOptions = () => http.get<MemoryEmbeddingOption[]>('/memories/embedding-models');
export const rebuildMemories = (embedding_config_id: number, request_key: string) => http.post<MemoryRebuild>('/memories/rebuild', { embedding_config_id, request_key });
export const getMemoryRebuild = (id: number) => http.get<MemoryRebuild>(`/memories/rebuilds/${id}`);
