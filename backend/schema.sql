-- CloudPaste D1数据库初始化
-- 这个文件用于创建所有必需的表结构
-- 运行方式: wrangler d1 execute cloudpaste-db --file=./schema.sql

-- 删除已有表（如果需要重新创建）
DROP TABLE IF EXISTS files;
DROP TABLE IF EXISTS s3_configs;
DROP TABLE IF EXISTS api_keys;
DROP TABLE IF EXISTS admin_tokens;
DROP TABLE IF EXISTS admins;
DROP TABLE IF EXISTS pastes;
DROP TABLE IF EXISTS file_passwords;
DROP TABLE IF EXISTS paste_passwords;

-- 创建pastes表 - 存储文本分享数据
CREATE TABLE pastes (
  id TEXT PRIMARY KEY,
  slug TEXT UNIQUE NOT NULL,
  content TEXT NOT NULL,
  remark TEXT,
  password TEXT,
  expires_at DATETIME,
  max_views INTEGER,
  views INTEGER DEFAULT 0,  
  created_by TEXT,                     -- 创建者标识（管理员ID或API密钥ID）
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- 创建索引
CREATE INDEX idx_pastes_slug ON pastes(slug);
CREATE INDEX idx_pastes_created_at ON pastes(created_at DESC);
CREATE INDEX idx_pastes_created_by ON pastes(created_by);    -- 添加创建者索引



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
  is_guest BOOLEAN DEFAULT 0,           -- 是否为访客（免密访问）
  last_used DATETIME,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  expires_at DATETIME NOT NULL
);

-- 创建s3_configs表 - 存储S3配置信息
CREATE TABLE s3_configs (
  id TEXT PRIMARY KEY,                 -- 配置唯一标识
  name TEXT NOT NULL,                  -- 配置名称（用户友好的名称，如"我的项目备份B2"）
  provider_type TEXT NOT NULL,         -- 提供商类型（Cloudflare R2, Backblaze B2, AWS S3, 其他）
  endpoint_url TEXT NOT NULL,          -- S3 API端点URL
  bucket_name TEXT NOT NULL,           -- 存储桶名称
  region TEXT,                         -- 存储桶区域（对AWS S3必须，对其他可选）
  access_key_id TEXT NOT NULL,         -- 访问密钥ID（加密存储）
  secret_access_key TEXT NOT NULL,     -- 秘密访问密钥（加密存储）
  path_style BOOLEAN DEFAULT 0,        -- 是否使用路径样式访问（1=是，0=否）
  default_folder TEXT DEFAULT '',      -- 默认上传文件夹路径
  is_public BOOLEAN DEFAULT 0,         -- 是否为公开允许API密钥用户使用
  is_default BOOLEAN DEFAULT 0,       -- 是否为默认配置
  total_storage_bytes BIGINT,          -- 存储桶总容量（字节数），用于计算使用百分比，NULL表示使用默认值
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  last_used DATETIME,                  -- 最后使用时间
  admin_id TEXT,                       -- 关联的管理员ID
  custom_host TEXT,                    -- 自定义域名/CDN域名
  signature_expires_in INTEGER DEFAULT 3600, -- 签名有效期（秒）
  FOREIGN KEY (admin_id) REFERENCES admins(id) ON DELETE CASCADE
);

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

-- 创建storage_mounts表 - 存储挂载配置
CREATE TABLE storage_mounts (
  id TEXT PRIMARY KEY,                  -- 唯一标识
  name TEXT NOT NULL,                   -- 挂载点名称
  storage_type TEXT NOT NULL,           -- 存储类型(S3, WebDAV等)
  storage_config_id TEXT,               -- 关联的存储配置ID (对S3类型，关联s3_configs表)
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

-- 创建初始管理员账户
-- 默认账户: admin/admin123
-- 注意: 这是SHA-256哈希后的密码，实际部署时应更改
INSERT INTO admins (id, username, password)
VALUES (
  '00000000-0000-0000-0000-000000000000',
  'admin',
  '240be518fabd2724ddb6f04eeb1da5967448d7e831c08c8fa822809f74c720a9'  -- SHA-256('admin123')
);


-- 创建示例文本分享（可选，仅用于测试）
INSERT INTO pastes (id, slug, content, remark, created_at)
VALUES (
  '11111111-1111-1111-1111-111111111111',
  'welcome',
  '# 欢迎使用 CloudPaste！\n\n这是一个简单的文本分享平台，您可以在这里创建和分享文本内容。\n\n## 功能特性\n\n- 创建文本分享\n- 密码保护\n- 阅读次数限制\n- 过期时间设置\n\n祝您使用愉快！',
  '欢迎信息',
  CURRENT_TIMESTAMP
);

-- 插入示例S3配置（加密密钥仅作示例，实际应用中应当由系统加密存储）
INSERT INTO s3_configs (
  id,
  name,
  provider_type,
  endpoint_url,
  bucket_name,
  region,
  access_key_id,
  secret_access_key,
  path_style,
  default_folder,
  admin_id,
  custom_host,
  signature_expires_in
) VALUES (
  '22222222-2222-2222-2222-222222222222',
  'Cloudflare R2存储',
  'Cloudflare R2',
  'https://account-id.r2.cloudflarestorage.com',
  'my-cloudpaste-bucket',
  'auto',
  'encrypted:access-key-id-placeholder',
  'encrypted:secret-access-key-placeholder',
  0,
  'uploads/',
  '00000000-0000-0000-0000-000000000000',
  NULL,
  3600
);