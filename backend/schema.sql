-- CloudPaste D1数据库初始化
-- 这个文件用于创建所有必需的表结构
-- 运行方式: wrangler d1 execute cloudpaste-db --file=./schema.sql

-- 删除已有表（如果需要重新创建）
DROP TABLE IF EXISTS files;
DROP TABLE IF EXISTS storage_configs;
DROP TABLE IF EXISTS api_keys;
DROP TABLE IF EXISTS admin_tokens;
DROP TABLE IF EXISTS admins;
DROP TABLE IF EXISTS paste_tag_assignments;
DROP TABLE IF EXISTS paste_tags;
DROP TABLE IF EXISTS pastes;
DROP TABLE IF EXISTS file_passwords;
DROP TABLE IF EXISTS paste_passwords;
DROP TABLE IF EXISTS tasks;
DROP TABLE IF EXISTS fs_meta;
DROP TABLE IF EXISTS storage_mounts;
DROP TABLE IF EXISTS system_settings;
DROP TABLE IF EXISTS schema_migrations;
DROP TABLE IF EXISTS scheduled_jobs;
DROP TABLE IF EXISTS upload_sessions;
DROP TABLE IF EXISTS scheduled_job_runs;
DROP TABLE IF EXISTS vfs_nodes;
DROP TABLE IF EXISTS upload_parts;
DROP TABLE IF EXISTS fs_search_index_entries;
DROP TABLE IF EXISTS fs_search_index_state;
DROP TABLE IF EXISTS fs_search_index_dirty;
DROP TABLE IF EXISTS fs_search_index_fts;
DROP TABLE IF EXISTS metrics_cache;

-- 创建pastes表 - 存储文本分享数据
CREATE TABLE pastes (
  id TEXT PRIMARY KEY,
  slug TEXT UNIQUE NOT NULL,
  content TEXT NOT NULL,
  title TEXT,
  remark TEXT,
  password TEXT,
  expires_at DATETIME,
  max_views INTEGER,
  views INTEGER DEFAULT 0,  
  is_public BOOLEAN NOT NULL DEFAULT 1,
  created_by TEXT,                     -- 创建者标识（管理员ID或API密钥ID）
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- 创建索引
CREATE INDEX idx_pastes_slug ON pastes(slug);
CREATE INDEX idx_pastes_created_at ON pastes(created_at DESC);
CREATE INDEX idx_pastes_created_by ON pastes(created_by);    -- 添加创建者索引
CREATE INDEX idx_pastes_is_public ON pastes(is_public);

-- 文本标签定义与多对多关联
CREATE TABLE paste_tags (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL COLLATE NOCASE UNIQUE,
  color TEXT NOT NULL,
  sort_order INTEGER NOT NULL DEFAULT 0,
  created_by TEXT,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE paste_tag_assignments (
  paste_id TEXT NOT NULL,
  tag_id TEXT NOT NULL,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (paste_id, tag_id),
  FOREIGN KEY (paste_id) REFERENCES pastes(id) ON DELETE CASCADE,
  FOREIGN KEY (tag_id) REFERENCES paste_tags(id) ON DELETE CASCADE
);

CREATE INDEX idx_paste_tags_sort ON paste_tags(sort_order, name);
CREATE INDEX idx_paste_tag_assignments_tag ON paste_tag_assignments(tag_id, paste_id);

-- 默认文本标签（同名标签存在时不会覆盖）
INSERT OR IGNORE INTO paste_tags (id, name, color, sort_order, created_by) VALUES
  ('preset-tag-red', '红色', '#EF4444', 0, NULL),
  ('preset-tag-yellow', '黄色', '#FACC15', 1, NULL),
  ('preset-tag-green', '绿色', '#22C55E', 2, NULL),
  ('preset-tag-important', '重要', '#A855F7', 3, NULL),
  ('preset-tag-personal', '个人', '#3B82F6', 4, NULL);

-- 创建admins表 - 存储管理员信息
CREATE TABLE admins (
  id TEXT PRIMARY KEY,
  username TEXT UNIQUE NOT NULL,
  password TEXT NOT NULL,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- 创建admin_tokens表 - 存储管理员认证令牌
CREATE TABLE admin_tokens (
  token TEXT PRIMARY KEY,
  admin_id TEXT NOT NULL,
  expires_at DATETIME NOT NULL,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (admin_id) REFERENCES admins(id) ON DELETE CASCADE
);

-- 创建api_keys表 - 存储API密钥（位标志权限系统）
CREATE TABLE api_keys (
  id TEXT PRIMARY KEY,
  name TEXT UNIQUE NOT NULL,
  key TEXT UNIQUE NOT NULL,
  permissions INTEGER DEFAULT 0,        -- 位标志权限（替代布尔字段）
  role TEXT DEFAULT 'GENERAL',          -- 用户角色：GUEST/GENERAL/ADMIN
  basic_path TEXT DEFAULT '/',
  is_enable BOOLEAN DEFAULT 0,         -- 启用状态：0=禁用，1=启用（所有密钥默认禁用，需手动开启）
  last_used DATETIME,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  expires_at DATETIME NOT NULL
);

-- 创建 storage_configs 表 - 聚合（驱动私有配置存入 config_json）
CREATE TABLE storage_configs (
  id TEXT PRIMARY KEY,                 -- 配置唯一标识
  name TEXT NOT NULL,                  -- 配置名称
  storage_type TEXT NOT NULL,          -- 存储类型（S3/WebDAV/…）
  admin_id TEXT,                       -- 归属管理员（允许NULL以兼容/预留）
  is_public INTEGER NOT NULL DEFAULT 0,
  is_default INTEGER NOT NULL DEFAULT 0,
  remark TEXT,                         -- 备注说明
  url_proxy TEXT,                      -- 代理入口 URL（例如 Worker/CDN 反代入口）
  status TEXT NOT NULL DEFAULT 'ENABLED',
  config_json TEXT NOT NULL,           -- 驱动私有配置（JSON，敏感字段加密后存入）
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  last_used DATETIME
);
-- 索引与唯一约束
CREATE INDEX idx_storage_admin ON storage_configs(admin_id);
CREATE INDEX idx_storage_type ON storage_configs(storage_type);
CREATE INDEX idx_storage_public ON storage_configs(is_public);
CREATE UNIQUE INDEX idx_default_per_admin ON storage_configs(admin_id) WHERE is_default = 1;

-- 存储 ACL 表：主体 -> 存储配置访问白名单
CREATE TABLE principal_storage_acl (
  subject_type TEXT NOT NULL,           -- 主体类型：API_KEY/USER/ROLE 等
  subject_id TEXT NOT NULL,             -- 主体ID：api_keys.id / users.id 等
  storage_config_id TEXT NOT NULL,      -- 被允许访问的 storage_configs.id
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (subject_type, subject_id, storage_config_id)
);
CREATE INDEX idx_psa_storage_config_id ON principal_storage_acl(storage_config_id);

-- 创建files表 - 存储已上传文件的元数据（支持多存储类型）
CREATE TABLE files (
  id TEXT PRIMARY KEY,
  slug TEXT UNIQUE NOT NULL,           -- 保持现有slug机制
  filename TEXT NOT NULL,              -- 原始文件名

  -- 存储引用（支持多存储类型）
  storage_config_id TEXT NOT NULL,     -- 存储配置ID（S3/WebDAV/Local等）
  storage_type TEXT NOT NULL,          -- 存储类型标识
  storage_path TEXT NOT NULL,          -- 存储中的实际路径
  file_path TEXT,                      -- 文件系统路径（可选，用于从文件系统创建的分享）

  -- 文件元数据
  mimetype TEXT NOT NULL,              -- 文件MIME类型
  size INTEGER NOT NULL,               -- 文件大小
  etag TEXT,                           -- 文件ETag（完整性验证和缓存）

  -- 分享控制
  remark TEXT,                         -- 文件备注
  password TEXT,                       -- 密码保护
  expires_at DATETIME,                 -- 过期时间
  max_views INTEGER,                   -- 最大查看次数
  views INTEGER DEFAULT 0,             -- 当前查看次数
  use_proxy BOOLEAN DEFAULT 0,         -- 是否使用代理访问

  -- 元数据
  created_by TEXT,                     -- 创建者
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- 添加索引提升查询性能
CREATE INDEX idx_files_slug ON files(slug);
CREATE INDEX idx_files_storage_config_id ON files(storage_config_id);
CREATE INDEX idx_files_storage_type ON files(storage_type);
CREATE INDEX idx_files_file_path ON files(file_path);
CREATE INDEX idx_files_created_at ON files(created_at);
CREATE INDEX idx_files_expires_at ON files(expires_at);

CREATE TABLE fs_meta (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  path TEXT NOT NULL,                  -- 虚拟路径，如 "/", "/public", "/private/docs"

  header_markdown TEXT NULL,           -- 顶部 README markdown 内容（inline）
  header_inherit BOOLEAN NOT NULL DEFAULT 0,

  footer_markdown TEXT NULL,           -- 底部 README markdown 内容（inline）
  footer_inherit BOOLEAN NOT NULL DEFAULT 0,

  hide_patterns TEXT NULL,             -- JSON 数组字符串，如 ["^README\\.md$", "^top\\.md$"]
  hide_inherit BOOLEAN NOT NULL DEFAULT 0,

  password TEXT NULL,                  -- 目录访问密码（明文，为空表示未设置）
  password_inherit BOOLEAN NOT NULL DEFAULT 0,

  extra JSON NULL,                     -- 预留扩展字段

  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_fs_meta_path ON fs_meta(path);

-- ================================
-- FS 搜索索引（派生数据，可重建）
-- - entries/state/dirty 为普通表，可迁移
-- - fts 为 FTS5 虚表，作为加速结构，不参与 export/import（建议通过重建恢复）
-- ================================

CREATE TABLE fs_search_index_entries (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  mount_id TEXT NOT NULL,
  fs_path TEXT NOT NULL,
  name TEXT NOT NULL,
  is_dir BOOLEAN NOT NULL DEFAULT 0,
  size INTEGER NOT NULL DEFAULT 0,
  modified_ms INTEGER NOT NULL DEFAULT 0,
  mimetype TEXT,
  index_run_id TEXT,
  updated_at_ms INTEGER NOT NULL DEFAULT 0,
  UNIQUE (mount_id, fs_path)
);

CREATE TABLE fs_search_index_state (
  mount_id TEXT PRIMARY KEY,
  status TEXT NOT NULL,
  last_indexed_ms INTEGER,
  updated_at_ms INTEGER NOT NULL,
  last_error TEXT
);

CREATE TABLE fs_search_index_dirty (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  mount_id TEXT NOT NULL,
  fs_path TEXT NOT NULL,
  op TEXT NOT NULL,
  created_at_ms INTEGER NOT NULL,
  dedupe_key TEXT NOT NULL UNIQUE
);

CREATE VIRTUAL TABLE fs_search_index_fts
USING fts5(
  name,
  fs_path,
  tokenize='trigram',
  content='fs_search_index_entries',
  content_rowid='id'
);

CREATE TRIGGER fs_search_index_ai
AFTER INSERT ON fs_search_index_entries
BEGIN
  INSERT INTO fs_search_index_fts(rowid, name, fs_path)
  VALUES (new.id, new.name, new.fs_path);
END;

CREATE TRIGGER fs_search_index_ad
AFTER DELETE ON fs_search_index_entries
BEGIN
  INSERT INTO fs_search_index_fts(fs_search_index_fts, rowid, name, fs_path)
  VALUES ('delete', old.id, old.name, old.fs_path);
END;

CREATE TRIGGER fs_search_index_au
AFTER UPDATE ON fs_search_index_entries
BEGIN
  INSERT INTO fs_search_index_fts(fs_search_index_fts, rowid, name, fs_path)
  VALUES ('delete', old.id, old.name, old.fs_path);
  INSERT INTO fs_search_index_fts(rowid, name, fs_path)
  VALUES (new.id, new.name, new.fs_path);
END;

CREATE INDEX idx_fs_search_entries_mount_path ON fs_search_index_entries(mount_id, fs_path);
CREATE INDEX idx_fs_search_entries_mount_modified ON fs_search_index_entries(mount_id, modified_ms DESC, id DESC);
CREATE INDEX idx_fs_search_entries_modified ON fs_search_index_entries(modified_ms DESC, id DESC);
CREATE INDEX idx_fs_search_entries_mount_run ON fs_search_index_entries(mount_id, index_run_id);
CREATE INDEX idx_fs_search_dirty_mount ON fs_search_index_dirty(mount_id, created_at_ms ASC);

-- 创建file_passwords表 - 存储文件密码
CREATE TABLE file_passwords (
  file_id TEXT PRIMARY KEY,
  plain_password TEXT NOT NULL,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (file_id) REFERENCES files(id) ON DELETE CASCADE
);

-- 创建paste_passwords表 - 存储文本密码
CREATE TABLE paste_passwords (
  paste_id TEXT PRIMARY KEY,
  plain_password TEXT NOT NULL,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (paste_id) REFERENCES pastes(id) ON DELETE CASCADE
);

-- 创建system_settings表 - 存储系统设置
CREATE TABLE system_settings (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL,
  description TEXT,
  type TEXT DEFAULT 'string',              -- 设置值类型：string, number, bool, select
  group_id INTEGER DEFAULT 1,              -- 分组ID：1=全局设置, 3=WebDAV设置
  options TEXT,                            -- JSON格式的选项配置（用于select类型）
  sort_order INTEGER DEFAULT 0,            -- 在分组内的排序顺序
  flags INTEGER DEFAULT 0,                
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- 创建schema_migrations表 - 记录迁移执行历史（内部使用）
CREATE TABLE schema_migrations (
  id TEXT PRIMARY KEY,
  applied_at TEXT NOT NULL
);

-- 创建storage_mounts表 - 存储挂载配置
CREATE TABLE storage_mounts (
  id TEXT PRIMARY KEY,                  -- 唯一标识
  name TEXT NOT NULL,                   -- 挂载点名称
  storage_type TEXT NOT NULL,           -- 存储类型(S3, WebDAV等)
  storage_config_id TEXT,               -- 关联的通用存储配置ID（storage_configs.id）
  mount_path TEXT NOT NULL,             -- 挂载路径，如 /photos
  remark TEXT,                          -- 备注说明
  is_active BOOLEAN DEFAULT 1,          -- 是否启用
  created_by TEXT NOT NULL,             -- 创建者标识
  sort_order INTEGER DEFAULT 0,         -- 显示排序顺序
  cache_ttl INTEGER DEFAULT 300,        -- 缓存时间(秒)，提高性能
  web_proxy BOOLEAN DEFAULT 0,          -- 网页预览、下载和直接链接是否通过中转
  webdav_policy TEXT DEFAULT '302_redirect', -- WebDAV策略：'302_redirect' 或 'native_proxy'
  enable_sign BOOLEAN DEFAULT 0,        -- 是否启用代理签名
  sign_expires INTEGER DEFAULT NULL,    -- 签名过期时间（秒），NULL表示使用全局设置
  enable_folder_summary_compute BOOLEAN DEFAULT 0, -- 是否启用“目录大小/时间”递归计算（兜底用）
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  last_used DATETIME                    -- 最后使用时间
);

-- 创建索引
CREATE INDEX idx_storage_mounts_mount_path ON storage_mounts(mount_path);
CREATE INDEX idx_storage_mounts_storage_config_id ON storage_mounts(storage_config_id);
CREATE INDEX idx_storage_mounts_created_by ON storage_mounts(created_by);
CREATE INDEX idx_storage_mounts_is_active ON storage_mounts(is_active);
CREATE INDEX idx_storage_mounts_sort_order ON storage_mounts(sort_order);


-- 通用上传会话表：管理各存储驱动的前端分片/断点续传会话
CREATE TABLE upload_sessions (
  id TEXT PRIMARY KEY,                   -- 内部会话ID（例如 upl_xxx），供前后端统一引用

  -- 主体与目标信息
  user_id TEXT NOT NULL,                 -- 用户或 API Key 标识
  user_type TEXT NOT NULL,               -- 'admin' | 'apikey' | 其他
  storage_type TEXT NOT NULL,            -- 存储类型：S3 / ONEDRIVE / WEBDAV / LOCAL 等
  storage_config_id TEXT NOT NULL,       -- 关联的存储配置 ID（storage_configs.id）
  mount_id TEXT,                         -- 关联挂载 ID（storage_mounts.id，可为空）
  fs_path TEXT NOT NULL,                 -- FS 视图路径，如 /onedrive/path/file.ext 或 /s3/bucket/key
  source TEXT NOT NULL,                  -- 会话来源：FS / SHARE / OTHER

  -- 文件级元数据
  file_name TEXT NOT NULL,               -- 文件名
  file_size INTEGER NOT NULL,            -- 总大小（字节）
  mime_type TEXT,                        -- MIME 类型（可选）
  checksum TEXT,                         -- 校验和（可选，例如 SHA-256）

  -- 文件指纹（用于跨驱动/跨会话识别同一逻辑文件）
  fingerprint_algo TEXT,                 -- 指纹算法标识，例如 meta-v1 / sha256 等
  fingerprint_value TEXT,                -- 指纹值（算法自定义，可为元数据拼接或哈希）

  -- 策略与进度
  strategy TEXT NOT NULL,                -- 分片策略：per_part_url / single_session / ...
  part_size INTEGER NOT NULL,            -- 分片大小（字节）
  total_parts INTEGER NOT NULL,          -- 预估分片总数
  bytes_uploaded INTEGER NOT NULL DEFAULT 0,  -- 已上传字节数（对于 single_session 可由 nextExpectedRanges 推导）
  uploaded_parts INTEGER NOT NULL DEFAULT 0,  -- 已确认的分片数量（对于 per_part_url 有意义）
  next_expected_range TEXT,              -- 对于 single_session：Graph nextExpectedRanges[0]，如 \"5242880-35799947\"

  -- provider 会话信息（驱动私有）
  provider_upload_id TEXT,               -- 例如 S3 UploadId / 其他云 provider upload id
  provider_upload_url TEXT,              -- 例如 OneDrive uploadSession.uploadUrl
  provider_meta TEXT,                    -- JSON 扩展字段（驱动私有）

  -- 会话状态与错误
  status TEXT NOT NULL,                  -- initiated / uploading / completed / aborted / expired / error
  error_code TEXT,
  error_message TEXT,

  -- 生命周期
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  expires_at DATETIME                    -- 会话过期时间（例如 Graph expirationDateTime）
);

CREATE INDEX idx_upload_sessions_user ON upload_sessions(user_id, user_type);
CREATE INDEX idx_upload_sessions_storage ON upload_sessions(storage_type, storage_config_id);
CREATE INDEX idx_upload_sessions_mount_path ON upload_sessions(mount_id, fs_path);
CREATE INDEX idx_upload_sessions_status ON upload_sessions(status, updated_at DESC);
CREATE INDEX idx_upload_sessions_source ON upload_sessions(source);
CREATE INDEX idx_upload_sessions_fingerprint ON upload_sessions(fingerprint_value);

-- ================================
-- VFS 索引（长期数据）
-- - 用于让“无目录树”的内容后端也能在 FS UI 里展示为目录树
-- - root 约定：root 本身不占记录；root 下子节点使用 parent_id = ''（空字符串）
-- ================================

CREATE TABLE vfs_nodes (
  id TEXT PRIMARY KEY,                   -- 唯一标识（例如 vfs_xxx）

  -- 多用户隔离预留
  owner_type TEXT NOT NULL,              -- admin / apikey / user（预留）
  owner_id TEXT NOT NULL,                -- 具体主体 id（字符串）

  -- 归属作用域（用于“无目录树后端”的虚拟目录树真相）
  scope_type TEXT NOT NULL,              -- mount | storage_config
  scope_id TEXT NOT NULL,                -- storage_mounts.id / storage_configs.id

  -- 目录树结构
  parent_id TEXT NOT NULL DEFAULT '',    -- root 下子节点：''；非 root：父节点 id
  name TEXT NOT NULL,                    -- 节点名（文件名/文件夹名）
  node_type TEXT NOT NULL,               -- dir | file | link

  -- 展示/元信息
  mime_type TEXT,
  size INTEGER,
  hash_algo TEXT,
  hash_value TEXT,
  status TEXT NOT NULL DEFAULT 'active', -- active | deleted（软删除预留）

  -- 内容后端定位
  storage_type TEXT NOT NULL,            -- 内容后端类型（TELEGRAM / URL / S3 ...）
  content_ref TEXT,                      -- JSON：描述如何获取内容（TG manifest / URL 等）

  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,

  UNIQUE (owner_type, owner_id, scope_type, scope_id, parent_id, name)
);

CREATE INDEX idx_vfs_nodes_scope ON vfs_nodes(owner_type, owner_id, scope_type, scope_id, parent_id);
CREATE INDEX idx_vfs_nodes_scope_id ON vfs_nodes(scope_type, scope_id);

-- ================================
-- 分片上传明细（临时数据：complete 后删除）
-- - 一片一行，避免并发写入互相覆盖
-- ================================

CREATE TABLE upload_parts (
  id TEXT PRIMARY KEY,                   -- 唯一标识（例如 uplp_xxx）
  upload_id TEXT NOT NULL,               -- upload_sessions.id
  part_no INTEGER NOT NULL,              -- 分片编号（从 1 开始）

  byte_start INTEGER,                    -- 可选：该片在整文件中的起始偏移
  byte_end INTEGER,                      -- 可选：该片在整文件中的结束偏移（包含）
  size INTEGER NOT NULL,                 -- 分片大小（字节）

  checksum_algo TEXT,
  checksum TEXT,

  storage_type TEXT NOT NULL,            -- 内容后端类型（TELEGRAM / URL / ...）
  provider_part_id TEXT,                 -- 可选：回执 id（如 ETag / messageId / fileId）
  provider_meta TEXT,                    -- JSON：回执扩展字段（如 TG messageId/fileId/channelId）

  status TEXT NOT NULL DEFAULT 'uploaded', -- uploaded | error
  error_code TEXT,
  error_message TEXT,

  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,

  UNIQUE (upload_id, part_no)
);

CREATE INDEX idx_upload_parts_upload_part_no ON upload_parts(upload_id, part_no);
CREATE INDEX idx_upload_parts_updated_at ON upload_parts(updated_at);


CREATE TABLE tasks (
  -- 核心标识
  task_id TEXT PRIMARY KEY,
  task_type TEXT NOT NULL,           -- 'copy' | 'upload' | 'download' | 'delete' | 'archive'

  -- 通用状态
  status TEXT NOT NULL,              -- 'pending' | 'running' | 'completed' | 'partial' | 'failed' | 'cancelled'

  -- 任务负载（JSON格式）
  payload TEXT NOT NULL,             -- JSON: { items: [...], options: {...} }

  -- 统计信息（JSON格式）
  stats TEXT NOT NULL DEFAULT '{}',  -- JSON: { totalItems, processedItems, successCount, failedCount, skippedCount }

  -- 错误信息（可选）
  error_message TEXT,

  -- 用户信息
  user_id TEXT NOT NULL,
  user_type TEXT NOT NULL,           -- 'admin' | 'apikey'

  -- 触发来源（用于任务列表展示/区分）
  trigger_type TEXT NOT NULL DEFAULT 'manual', -- 'manual' | 'scheduled'
  trigger_ref TEXT,                              -- 可选：来源引用（如 scheduled handlerId / 页面标识）

  -- Workflows 关联（仅 Workers 运行时使用，可选）
  workflow_instance_id TEXT,

  -- 时间戳（Unix timestamp in milliseconds）
  created_at INTEGER NOT NULL,
  started_at INTEGER,
  updated_at INTEGER NOT NULL,
  finished_at INTEGER
);

CREATE INDEX IF NOT EXISTS idx_tasks_status_created ON tasks (status, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_tasks_type_status ON tasks (task_type, status);
CREATE INDEX IF NOT EXISTS idx_tasks_user ON tasks (user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_tasks_workflow ON tasks (workflow_instance_id) WHERE workflow_instance_id IS NOT NULL;

CREATE TABLE scheduled_jobs (
  task_id              TEXT PRIMARY KEY,              -- 作业唯一标识（job ID），例如 'cleanup_upload_sessions_default'
  handler_id           TEXT,                          -- 任务类型ID（Handler ID），例如 'cleanup_upload_sessions'
  name                 TEXT,                          -- 作业名称（展示用，可自定义）
  description          TEXT,                          -- 作业描述/备注
  enabled              INTEGER NOT NULL,              -- 1=启用, 0=禁用

  schedule_type        TEXT NOT NULL DEFAULT 'interval', -- 调度类型：interval | cron
  interval_sec         INTEGER,                       -- interval 模式下的执行间隔(秒)
  cron_expression      TEXT,                          -- cron 模式下的表达式，例如 \"0 2 * * *\"

  run_count            INTEGER NOT NULL DEFAULT 0,    -- 累计执行次数（成功/失败均计入）
  failure_count        INTEGER NOT NULL DEFAULT 0,    -- 累计失败次数

  last_run_status      TEXT,                          -- 最近一次执行状态：success/failure/skipped
  last_run_started_at  DATETIME,                      -- 最近一次执行开始时间
  last_run_finished_at DATETIME,                      -- 最近一次执行结束时间

  next_run_after       DATETIME,                      -- 最早允许再次执行的时间
  lock_until           DATETIME,                      -- 锁过期时间，用于多实例互斥

  config_json          TEXT NOT NULL DEFAULT '{}',
  created_at           DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at           DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_scheduled_jobs_next_run ON scheduled_jobs (enabled, next_run_after);

-- 后台调度作业运行日志表：记录每次执行结果
CREATE TABLE scheduled_job_runs (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  task_id       TEXT NOT NULL,                  -- 对应 scheduled_jobs.task_id
  status        TEXT NOT NULL,                  -- 'success' | 'failure' | 'skipped'
  trigger_type  TEXT NOT NULL DEFAULT 'auto',   -- 触发类型：'auto' | 'manual'

  scheduled_at  DATETIME,                       -- 计划执行时间（可选）
  started_at    DATETIME NOT NULL,              -- 本次执行开始时间
  finished_at   DATETIME,                       -- 本次执行结束时间
  duration_ms   INTEGER,                        -- 执行耗时(毫秒)

  summary       TEXT,                           -- 简要摘要，如影响行数等
  error_message TEXT,                           -- 失败时的错误信息
  details_json  TEXT,                           -- 可选的结构化详情(JSON)

  created_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_scheduled_job_runs_task_started
  ON scheduled_job_runs (task_id, started_at DESC);

-- 通用指标缓存表：存储各种统计指标的缓存数据
CREATE TABLE metrics_cache (
  scope_type TEXT NOT NULL,              -- 作用域类型，如 'global'、'storage'、'user' 等
  scope_id TEXT NOT NULL,                -- 作用域标识，如具体的存储ID或用户ID
  metric_key TEXT NOT NULL,              -- 指标键名

  value_num INTEGER,                     -- 数值类型的指标值
  value_text TEXT,                       -- 文本类型的指标值
  value_json_text TEXT,                  -- JSON格式的复杂指标值

  snapshot_at_ms INTEGER,                -- 快照时间戳（毫秒）
  updated_at_ms INTEGER NOT NULL,        -- 最后更新时间戳（毫秒）

  error_message TEXT,                    -- 错误信息（如果指标计算失败）

  PRIMARY KEY (scope_type, scope_id, metric_key)
);

-- 通用指标缓存索引：按作用域类型和ID查询
CREATE INDEX idx_metrics_cache_scope ON metrics_cache(scope_type, scope_id);
