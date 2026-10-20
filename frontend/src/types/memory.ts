/**
 * 记忆域类型 —— 对应 model/memory.go。
 */

/** 长期记忆 */
export interface Memory {
  id: number;
  user_id: number;
  agent_id?: number | null;
  content: string;
  vector_id: string;
  /** user / agent */
  scope: 'user' | 'agent';
  /** 1-10 重要度 */
  importance: number;
  hit_count: number;
  last_hit_at?: string | null;
  /** 1 生效 / 2 归档 */
  status: 1 | 2;
  revision: number;
  indexed_revision: number;
  index_status: 'needs_reindex' | 'pending' | 'retry_wait' | 'failed' | 'ready' | 'removed';
  embedding_version: string;
  created_at: string;
  updated_at: string;
}

/** 手动抽取记忆请求（POST /memories/extract） */
export interface ExtractMemoryReq {
  session_id: string;
}

export interface MemoryJob {
 id: number;
 kind: 'extract' | 'vector';
 status: 'queued' | 'running' | 'retry_wait' | 'done' | 'failed' | 'superseded' | 'canceled';
 session_id?: string;
 memory_id?: number;
 memory_revision?: number;
 vector_action?: 'upsert' | 'delete';
 parent_job_id?: number;
 rebuild_id?: number;
 attempts: number;
 epoch_attempts: number;
 max_attempts: number;
 error?: string;
 warnings?: string[];
 updated_at: string;
}
export interface MemoryProfile {
 embedding_config_id: number;
 embedding_version: string;
 embedding_dimension: number;
}
export interface MemoryEmbeddingOption { id: number; alias: string; dimension: number }
export interface MemoryRebuild {
 id: number;
 reindexed: number;
 merged: number;
 embedding_version: string;
 vector_pending?: number;
 vector_failed?: number;
 vector_done?: number;
 vector_superseded?: number;
}
