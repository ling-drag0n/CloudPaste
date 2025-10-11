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
  STORAGE_MOUNTS: "storage_mounts", // 存储挂载表
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

// alist风格的文件类型常量
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
