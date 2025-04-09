/**
 * 常量定义文件
 */

// 数据库表名常量
export const DbTables = {
  ADMINS: "admins", // 管理员表
  ADMIN_TOKENS: "admin_tokens", // 管理员令牌表
  PASTES: "pastes", // 文本表
  API_KEYS: "api_keys", // API密钥表
  S3_CONFIGS: "s3_configs", // S3配置表
  FILES: "files", // 文件表
  FILE_PASSWORDS: "file_passwords", // 文件明文密码表
  SYSTEM_SETTINGS: "system_settings", // 系统设置表
  PASTE_PASSWORDS: "paste_passwords", // 文本密码表
};

// 默认的最大上传大小（MB）
export const DEFAULT_MAX_UPLOAD_SIZE_MB = 100;

// API状态码常量
export const ApiStatus = {
  SUCCESS: 200,
  CREATED: 201,
  BAD_REQUEST: 400,
  UNAUTHORIZED: 401,
  FORBIDDEN: 403,
  NOT_FOUND: 404,
  CONFLICT: 409,
  GONE: 410,
  INTERNAL_ERROR: 500,
};

// S3提供商类型常量
export const S3ProviderTypes = {
  R2: "Cloudflare R2",
  B2: "Backblaze B2",
  AWS: "AWS S3",
  OTHER: "Other S3 Compatible",
  WEBDAV: "WebDAV Storage",
};
