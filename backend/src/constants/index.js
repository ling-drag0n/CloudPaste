/**
 * 常量定义文件
 */

// 数据库表名常量
export const DbTables = {
  ADMINS: "admins", // 管理员表
  ADMIN_TOKENS: "admin_tokens", // 管理员令牌表
  PASTES: "pastes", // 文本表
  PASTE_TAGS: "paste_tags", // 文本标签定义表
  PASTE_TAG_ASSIGNMENTS: "paste_tag_assignments", // 文本与标签关联表
  API_KEYS: "api_keys", // API密钥表
  STORAGE_CONFIGS: "storage_configs", // 通用"存储配置表"
  PRINCIPAL_STORAGE_ACL: "principal_storage_acl", // 主体 -> 存储配置 ACL 表
  FILES: "files", // 文件表
  FILE_PASSWORDS: "file_passwords", // 文件明文密码表
  SYSTEM_SETTINGS: "system_settings", // 系统设置表
  PASTE_PASSWORDS: "paste_passwords", // 文本密码表
  STORAGE_MOUNTS: "storage_mounts", // 存储挂载表
  FS_META: "fs_meta", // 目录 Meta 配置表
  FS_SEARCH_INDEX_ENTRIES: "fs_search_index_entries", // FS 搜索索引（条目表，派生数据）
  FS_SEARCH_INDEX_STATE: "fs_search_index_state", // FS 搜索索引（状态表，派生数据）
  FS_SEARCH_INDEX_FTS: "fs_search_index_fts", // FS 搜索索引（FTS5 虚表，派生数据）
  FS_SEARCH_INDEX_DIRTY: "fs_search_index_dirty", // FS 搜索索引（dirty 队列表，派生数据）
  METRICS_CACHE: "metrics_cache", // 通用指标缓存表（快照/用量/配额等派生数据）
  TASKS: "tasks", // 任务编排表
  SCHEMA_MIGRATIONS: "schema_migrations", // 迁移历史表（用于记录 schema 版本）
  SCHEDULED_JOBS: "scheduled_jobs", // 后台调度作业表
  SCHEDULED_JOB_RUNS: "scheduled_job_runs", // 后台调度作业运行日志表
  UPLOAD_SESSIONS: "upload_sessions", // 通用上传会话表（前端分片/断点续传）
  UPLOAD_PARTS: "upload_parts", // 上传分片明细表（临时账本，一片一行）
  VFS_NODES: "vfs_nodes", // 虚拟目录树索引表（长期目录树/条目）
};

// 默认的最大上传大小（MB）
export const DEFAULT_MAX_UPLOAD_SIZE_MB = 100;

// API状态码常量
export const ApiStatus = {
  SUCCESS: 200,
  CREATED: 201,
  ACCEPTED: 202, // 部分成功，用于批量操作中有部分失败的情况
  BAD_REQUEST: 400,
  UNAUTHORIZED: 401,
  FORBIDDEN: 403,
  NOT_FOUND: 404,
  CONFLICT: 409,
  GONE: 410,
  INTERNAL_ERROR: 500,
};

//文件类型常量
export const FILE_TYPES = {
  UNKNOWN: 0, // 未知文件
  FOLDER: 1, // 文件夹
  VIDEO: 2, // 视频文件
  AUDIO: 3, // 音频文件
  TEXT: 4, // 文本文件
  IMAGE: 5, // 图片文件
  OFFICE: 6, // Office文档
  DOCUMENT: 7, // 文档文件
};

// 文件类型名称映射
export const FILE_TYPE_NAMES = {
  [FILE_TYPES.UNKNOWN]: "unknown",
  [FILE_TYPES.FOLDER]: "folder",
  [FILE_TYPES.VIDEO]: "video",
  [FILE_TYPES.AUDIO]: "audio",
  [FILE_TYPES.TEXT]: "text",
  [FILE_TYPES.IMAGE]: "image",
  [FILE_TYPES.OFFICE]: "office",
  [FILE_TYPES.DOCUMENT]: "document",
};

// S3提供商类型常量
export const S3ProviderTypes = {
  R2: "Cloudflare R2",
  B2: "Backblaze B2",
  AWS: "AWS S3",
  ALIYUN_OSS: "Aliyun OSS",
  OTHER: "Other S3 Compatible",
};

// 统一的用户类型常量（供服务层/能力层使用）
export const UserType = {
  ADMIN: "admin",
  API_KEY: "apiKey",
  ANONYMOUS: "anonymous",
};
