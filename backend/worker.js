/**
 * 这是旧版worker仅功参考，已弃用
 */

/**
 * CloudPaste API Server
 * 基于Cloudflare Worker的后端服务
 * 使用Hono框架实现RESTful API
 */
import { Hono } from "hono";
import { cors } from "hono/cors";
import { HTTPException } from "hono/http-exception";
import { sha256 } from "hono/utils/crypto";

// 导入S3相关的库
import { S3Client, PutObjectCommand, GetObjectCommand, ListObjectsV2Command, DeleteObjectCommand } from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";

// 创建Hono应用实例
const app = new Hono();

// =================================
// ========== 常量定义 ==========
// =================================

// 数据库表名常量
const DbTables = {
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
const DEFAULT_MAX_UPLOAD_SIZE_MB = 100;

// API状态码常量
const ApiStatus = {
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
const S3ProviderTypes = {
  R2: "Cloudflare R2",
  B2: "Backblaze B2",
  AWS: "AWS S3",
  OTHER: "Other S3 Compatible",
};

// =================================
// ========== 中间件配置 ==========
// =================================

/**
 * 中间件层
 * 包含请求/响应处理、身份认证等中间件
 */

// CORS中间件配置 - 确保这是第一个中间件
app.use(
    "*",
    cors({
      origin: "*",
      allowMethods: ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
      allowHeaders: ["Content-Type", "Authorization"],
      exposeHeaders: ["Content-Length"],
      maxAge: 86400,
    })
);

// API响应格式化中间件 - 统一处理响应格式
app.use("*", async (c, next) => {
  try {
    await next();

    // 检查响应是否已经是JSON格式
    const response = c.res;
    const contentType = response.headers.get("Content-Type");

    // 如果不是JSON或没有响应体，直接返回
    if (!contentType || !contentType.includes("application/json") || !response.body) {
      return response;
    }

    // 克隆响应以便可以读取body
    const clonedResponse = response.clone();
    const resBody = await clonedResponse.json();

    // 如果已经是标准格式(有code字段)，不再处理
    if (resBody && typeof resBody === "object" && "code" in resBody) {
      return response;
    }

    // 返回标准格式响应
    return c.json({
      code: ApiStatus.SUCCESS,
      message: "success",
      data: resBody,
      success: true, // 添加兼容前端的字段
    });
  } catch (error) {
    // 错误处理
    const statusCode = error instanceof HTTPException ? error.status : ApiStatus.INTERNAL_ERROR;
    const errorMessage = error instanceof Error ? error.message : "服务器内部错误";

    return c.json(
        {
          code: statusCode,
          message: errorMessage,
          data: null,
          success: false, // 添加兼容前端的字段
        },
        statusCode
    );
  }
});

// JWT管理员认证中间件
const authMiddleware = async (c, next) => {
  const authHeader = c.req.header("Authorization");

  if (!authHeader || !authHeader.startsWith("Bearer ")) {
    return c.json(
        {
          code: ApiStatus.UNAUTHORIZED,
          message: "未授权访问",
          data: null,
        },
        ApiStatus.UNAUTHORIZED
    );
  }

  const token = authHeader.substring(7);

  try {
    // 从D1数据库验证token
    const adminId = await validateAdminToken(c.env.DB, token);

    if (!adminId) {
      return c.json(
          {
            code: ApiStatus.UNAUTHORIZED,
            message: "无效的认证令牌",
            data: null,
          },
          ApiStatus.UNAUTHORIZED
      );
    }

    // 将管理员ID添加到上下文中
    c.set("adminId", adminId);

    await next();
  } catch (error) {
    return c.json(
        {
          code: ApiStatus.UNAUTHORIZED,
          message: "认证失败: " + error.message,
          data: null,
        },
        ApiStatus.UNAUTHORIZED
    );
  }
};

// API密钥认证中间件（仅文本权限）
const apiKeyTextMiddleware = async (c, next) => {
  const db = c.env.DB;

  // 获取认证头信息
  const authHeader = c.req.header("Authorization");

  // 检查是否有API密钥认证
  if (!authHeader || !authHeader.startsWith("ApiKey ")) {
    throw new HTTPException(ApiStatus.UNAUTHORIZED, { message: "需要API密钥授权" });
  }

  const apiKey = authHeader.substring(7);

  // 查询API密钥和权限
  const keyRecord = await db
      .prepare(
          `SELECT id, name, text_permission, file_permission, expires_at 
       FROM ${DbTables.API_KEYS} 
       WHERE key = ?`
      )
      .bind(apiKey)
      .first();

  // 检查API密钥是否存在且有文本权限
  if (!keyRecord || keyRecord.text_permission !== 1) {
    throw new HTTPException(ApiStatus.FORBIDDEN, { message: "API密钥没有文本权限" });
  }

  // 检查API密钥是否过期
  if (await checkAndDeleteExpiredApiKey(db, keyRecord)) {
    throw new HTTPException(ApiStatus.UNAUTHORIZED, { message: "API密钥已过期" });
  }

  // 更新最后使用时间
  await db
      .prepare(
          `UPDATE ${DbTables.API_KEYS}
       SET last_used = ?
       WHERE id = ?`
      )
      .bind(getLocalTimeString(), keyRecord.id)
      .run();

  // 将API密钥ID和完整权限信息存入请求上下文
  c.set("apiKeyId", keyRecord.id);

  // 存储密钥名称和权限信息，以便API可以在需要时返回
  c.set("apiKeyInfo", {
    id: keyRecord.id,
    name: keyRecord.name,
    permissions: {
      text: keyRecord.text_permission === 1,
      file: keyRecord.file_permission === 1,
    },
  });

  // 继续处理请求
  await next();
};

// API密钥认证中间件（仅文件权限）
const apiKeyFileMiddleware = async (c, next) => {
  const db = c.env.DB;

  // 获取认证头信息
  const authHeader = c.req.header("Authorization");

  // 检查是否有API密钥认证
  if (!authHeader || !authHeader.startsWith("ApiKey ")) {
    throw new HTTPException(ApiStatus.UNAUTHORIZED, { message: "需要API密钥授权" });
  }

  const apiKey = authHeader.substring(7);

  // 查询API密钥和权限
  const keyRecord = await db
      .prepare(
          `SELECT id, name, text_permission, file_permission, expires_at 
       FROM ${DbTables.API_KEYS} 
       WHERE key = ?`
      )
      .bind(apiKey)
      .first();

  // 检查API密钥是否存在且有文件权限
  if (!keyRecord || keyRecord.file_permission !== 1) {
    throw new HTTPException(ApiStatus.FORBIDDEN, { message: "API密钥没有文件权限" });
  }

  // 检查API密钥是否过期
  if (await checkAndDeleteExpiredApiKey(db, keyRecord)) {
    throw new HTTPException(ApiStatus.UNAUTHORIZED, { message: "API密钥已过期" });
  }

  // 更新最后使用时间
  await db
      .prepare(
          `UPDATE ${DbTables.API_KEYS}
       SET last_used = ?
       WHERE id = ?`
      )
      .bind(getLocalTimeString(), keyRecord.id)
      .run();

  // 将API密钥ID和完整权限信息存入请求上下文
  c.set("apiKeyId", keyRecord.id);

  // 存储密钥名称和权限信息，以便API可以在需要时返回
  c.set("apiKeyInfo", {
    id: keyRecord.id,
    name: keyRecord.name,
    permissions: {
      text: keyRecord.text_permission === 1,
      file: keyRecord.file_permission === 1,
    },
  });

  // 继续处理请求
  await next();
};

// API密钥验证中间件
const apiKeyMiddleware = async (c, next) => {
  // 检查请求头中是否包含API密钥
  const authHeader = c.req.header("Authorization") || "";

  if (!authHeader.startsWith("ApiKey ")) {
    return c.json(createErrorResponse(ApiStatus.UNAUTHORIZED, "需要API密钥认证"), ApiStatus.UNAUTHORIZED);
  }

  const apiKey = authHeader.substring(7);
  const db = c.env.DB;

  try {
    // 查询数据库中的API密钥记录
    const keyRecord = await db
        .prepare(
            `
      SELECT id, name, text_permission, file_permission, expires_at
      FROM ${DbTables.API_KEYS}
      WHERE key = ?
    `
        )
        .bind(apiKey)
        .first();

    // 如果密钥不存在
    if (!keyRecord) {
      return c.json(createErrorResponse(ApiStatus.UNAUTHORIZED, "无效的API密钥"), ApiStatus.UNAUTHORIZED);
    }

    // 使用checkAndDeleteExpiredApiKey函数检查是否过期
    if (await checkAndDeleteExpiredApiKey(db, keyRecord)) {
      return c.json(createErrorResponse(ApiStatus.UNAUTHORIZED, "API密钥已过期"), ApiStatus.UNAUTHORIZED);
    }

    // 更新最后使用时间
    await db
        .prepare(
            `
      UPDATE ${DbTables.API_KEYS}
      SET last_used = ?
      WHERE id = ?
    `
        )
        .bind(getLocalTimeString(), keyRecord.id)
        .run();

    // 将密钥信息添加到上下文中
    c.set("apiKey", {
      id: keyRecord.id,
      name: keyRecord.name,
      textPermission: keyRecord.text_permission === 1,
      filePermission: keyRecord.file_permission === 1,
    });

    // 将apiKeyId也单独添加到上下文中，确保test/api-key接口可以访问
    c.set("apiKeyId", keyRecord.id);

    return next();
  } catch (error) {
    console.error("API密钥验证错误:", error);
    return c.json(createErrorResponse(ApiStatus.INTERNAL_ERROR, "API密钥验证失败: " + error.message), ApiStatus.INTERNAL_ERROR);
  }
};
// =================================
// ========== 工具函数 ==========
// =================================

/**
 * 工具函数层
 * 包含通用的工具函数，不依赖特定业务逻辑
 */

/**
 * 生成随机字符串
 * @param {number} length - 字符串长度
 * @returns {string} 随机字符串
 */
function generateRandomString(length = 8) {
  const chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
  let result = "";
  const randomValues = new Uint8Array(length);
  crypto.getRandomValues(randomValues);
  randomValues.forEach((val) => (result += chars[val % chars.length]));
  return result;
}

/**
 * 生成密码哈希
 * @param {string} password - 原始密码
 * @returns {Promise<string>} 密码哈希
 */
async function hashPassword(password) {
  // 使用SHA-256哈希
  return await sha256(password);
}

/**
 * 验证密码
 * @param {string} plainPassword - 原始密码
 * @param {string} hashedPassword - 哈希后的密码
 * @returns {Promise<boolean>} 验证结果
 */
async function verifyPassword(plainPassword, hashedPassword) {
  // 如果是SHA-256哈希（用于初始管理员密码）
  if (hashedPassword.length === 64) {
    const hashedInput = await sha256(plainPassword);
    return hashedInput === hashedPassword;
  }

  // 默认比较
  return plainPassword === hashedPassword;
}

/**
 * 统一错误响应工具函数
 * @param {number} statusCode - HTTP状态码
 * @param {string} message - 错误消息
 * @returns {object} 标准错误响应对象
 */
function createErrorResponse(statusCode, message) {
  return {
    code: statusCode,
    message: message,
    success: false,
    data: null,
  };
}

/**
 * 加密敏感配置
 * @param {string} value - 需要加密的值
 * @param {string} secret - 加密密钥
 * @returns {Promise<string>} 加密后的值
 */
async function encryptValue(value, secret) {
  // 简单的加密方式
  const encoder = new TextEncoder();
  const data = encoder.encode(value);
  const secretKey = await crypto.subtle.importKey("raw", encoder.encode(secret), { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);

  const signature = await crypto.subtle.sign("HMAC", secretKey, data);
  const encryptedValue = "encrypted:" + btoa(String.fromCharCode(...new Uint8Array(signature))) + ":" + btoa(value);

  return encryptedValue;
}

/**
 * 解密敏感配置
 * @param {string} encryptedValue - 加密后的值
 * @param {string} secret - 加密密钥
 * @returns {Promise<string>} 解密后的值
 */
async function decryptValue(encryptedValue, secret) {
  // 检查是否为加密值
  if (!encryptedValue.startsWith("encrypted:")) {
    return encryptedValue; // 未加密的值直接返回
  }

  // 从加密格式中提取值
  const parts = encryptedValue.split(":");
  if (parts.length !== 3) {
    throw new Error("无效的加密格式");
  }

  try {
    // 直接从加密值中提取原始值
    const originalValue = atob(parts[2]);
    return originalValue;
  } catch (error) {
    throw new Error("解密失败: " + error.message);
  }
}

/**
 * 从文件名中获取文件名和扩展名
 * @param {string} filename - 文件名
 * @returns {Object} 包含文件名和扩展名的对象
 */
function getFileNameAndExt(filename) {
  const lastDotIndex = filename.lastIndexOf(".");
  if (lastDotIndex > -1) {
    return {
      name: filename.substring(0, lastDotIndex),
      ext: filename.substring(lastDotIndex),
    };
  }
  return {
    name: filename,
    ext: "",
  };
}

/**
 * 生成安全的文件名（移除非法字符）
 * @param {string} fileName - 原始文件名
 * @returns {string} 安全的文件名
 */
function getSafeFileName(fileName) {
  return fileName
      .replace(/[^\w\u4e00-\u9fa5\-\.]/g, "_") // 仅保留字母、数字、中文、下划线、连字符和点
      .replace(/_{2,}/g, "_"); // 将多个连续下划线替换为单个
}

/**
 * 生成短ID作为文件路径前缀
 * @returns {string} 生成的短ID
 */
function generateShortId() {
  // 生成6位随机ID
  const charset = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
  let result = "";

  // 使用 crypto.getRandomValues 获取加密安全的随机值
  const randomValues = new Uint8Array(6);
  crypto.getRandomValues(randomValues);

  for (let i = 0; i < 6; i++) {
    result += charset[randomValues[i] % charset.length];
  }

  return result;
}

/**
 * 获取当前时间的本地格式化字符串，用于数据库时间字段
 * @returns {string} 格式化的本地时间字符串，如：'2023-06-01 14:30:45'
 */
function getLocalTimeString() {
  const now = new Date();
  const year = now.getFullYear();
  const month = String(now.getMonth() + 1).padStart(2, "0");
  const day = String(now.getDate()).padStart(2, "0");
  const hours = String(now.getHours()).padStart(2, "0");
  const minutes = String(now.getMinutes()).padStart(2, "0");
  const seconds = String(now.getSeconds()).padStart(2, "0");

  return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
}

/**
 * 格式化文件大小
 * @param {number} bytes 文件大小（字节）
 * @returns {string} 格式化后的文件大小
 */
function formatFileSize(bytes) {
  if (bytes === 0) return "0 B";

  const sizes = ["B", "KB", "MB", "GB", "TB"];
  const i = Math.floor(Math.log(bytes) / Math.log(1024));

  return (bytes / Math.pow(1024, i)).toFixed(2) + " " + sizes[i];
}

// 处理每周数据，确保有7天的数据
function processWeeklyData(data) {
  const result = new Array(7).fill(0);

  if (!data || data.length === 0) return result;

  // 获取过去7天的日期
  const dates = [];
  for (let i = 6; i >= 0; i--) {
    const date = new Date();
    date.setDate(date.getDate() - i);
    dates.push(date.toISOString().split("T")[0]); // 格式：YYYY-MM-DD
  }

  // 将数据映射到对应日期
  data.forEach((item) => {
    const itemDate = item.date.split("T")[0]; // 处理可能的时间部分
    const index = dates.indexOf(itemDate);
    if (index !== -1) {
      result[index] = item.count;
    }
  });

  return result;
}

/**
 * 生成唯一文件ID
 * @returns {string} 生成的文件ID
 */
function generateFileId() {
  return crypto.randomUUID();
}

/**
 * 生成唯一的S3配置ID
 * @returns {string} 生成的S3配置ID
 */
function generateS3ConfigId() {
  return crypto.randomUUID();
}

/**
 * 获取文件的MIME类型
 * @param {string} filename - 文件名
 * @returns {string} 文件的MIME类型
 */
function getMimeType(filename) {
  const extension = filename.split(".").pop().toLowerCase();
  const mimeTypes = {
    // 图片格式
    jpg: "image/jpeg",
    jpeg: "image/jpeg",
    png: "image/png",
    gif: "image/gif",
    webp: "image/webp",
    svg: "image/svg+xml",
    tiff: "image/tiff",
    tif: "image/tiff",
    ico: "image/x-icon",
    bmp: "image/bmp",
    heic: "image/heic",

    // 文档格式
    pdf: "application/pdf",
    doc: "application/msword",
    docx: "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    xls: "application/vnd.ms-excel",
    xlsx: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    ppt: "application/vnd.ms-powerpoint",
    pptx: "application/vnd.openxmlformats-officedocument.presentationml.presentation",

    // Markdown文件
    md: "text/markdown",
    markdown: "text/markdown",
    mdown: "text/markdown",
    mkd: "text/markdown",
    mdwn: "text/markdown",
    mdtxt: "text/markdown",
    mdtext: "text/markdown",
    rmd: "text/markdown",

    // 音频格式
    mp3: "audio/mpeg",
    wav: "audio/wav",
    ogg: "audio/ogg",
    m4a: "audio/mp4",
    flac: "audio/flac",
    aac: "audio/aac",

    // 视频格式
    mp4: "video/mp4",
    avi: "video/x-msvideo",
    mov: "video/quicktime",
    wmv: "video/x-ms-wmv",
    mkv: "video/x-matroska",
    webm: "video/webm",
    "3gp": "video/3gpp",

    // 压缩文件
    zip: "application/zip",
    rar: "application/x-rar-compressed",
    "7z": "application/x-7z-compressed",
    tar: "application/x-tar",
    gz: "application/gzip",

    // 代码和文本文件
    html: "text/html",
    htm: "text/html",
    css: "text/css",
    js: "application/javascript",
    json: "application/json",
    xml: "application/xml",
    txt: "text/plain",
    rtf: "application/rtf",
    csv: "text/csv",

    // 其他常见格式
    iso: "application/x-iso9660-image",
    db: "application/x-sqlite3",
    sqlite: "application/x-sqlite3",
    sqlite3: "application/x-sqlite3",
    epub: "application/epub+zip",
    apk: "application/vnd.android.package-archive",
    exe: "application/x-msdownload",
    dll: "application/x-msdownload",
    psd: "image/vnd.adobe.photoshop",
    ai: "application/postscript",
    eot: "application/vnd.ms-fontobject",
    ttf: "font/ttf",
    woff: "font/woff",
    woff2: "font/woff2",
  };

  return mimeTypes[extension] || "application/octet-stream";
}

/**
 * 创建S3客户端
 * @param {Object} config - S3配置对象
 * @param {string} encryptionSecret - 用于解密凭证的密钥
 * @returns {Promise<S3Client>} S3客户端实例
 */
async function createS3Client(config, encryptionSecret) {
  // 解密敏感配置
  const accessKeyId = await decryptValue(config.access_key_id, encryptionSecret);
  const secretAccessKey = await decryptValue(config.secret_access_key, encryptionSecret);

  // 创建S3客户端配置
  const clientConfig = {
    endpoint: config.endpoint_url,
    region: config.region || "auto",
    credentials: {
      accessKeyId,
      secretAccessKey,
    },
    forcePathStyle: config.path_style === 1, // 使用路径样式访问
  };

  // B2可能需要特殊配置
  if (config.provider_type === S3ProviderTypes.B2) {
    // 为B2使用v4签名版本
    clientConfig.signatureVersion = "v4";

    // 设置自定义代理头
    clientConfig.customUserAgent = "CloudPaste/1.0";
  }

  // 返回创建的S3客户端
  return new S3Client(clientConfig);
}

/**
 * 构建S3文件公共访问URL
 * @param {Object} s3Config - S3配置
 * @param {string} storagePath - S3存储路径
 * @returns {string} 访问URL
 */
function buildS3Url(s3Config, storagePath) {
  const bucketName = s3Config.bucket_name;
  const endpointUrl = s3Config.endpoint_url;

  // 去除endpoint_url末尾的斜杠(如果有)
  const endpoint = endpointUrl.endsWith("/") ? endpointUrl.slice(0, -1) : endpointUrl;

  // 确保storagePath不以斜杠开始
  const normalizedPath = storagePath.startsWith("/") ? storagePath.slice(1) : storagePath;

  // 根据配置选择合适的URL格式(路径样式vs虚拟主机样式)
  if (s3Config.path_style === 1) {
    // 路径样式: https://endpoint/bucket/key
    return `${endpoint}/${bucketName}/${normalizedPath}`;
  } else {
    // 虚拟主机样式: https://bucket.endpoint/key

    // 提取endpoint的域名部分
    let domain = endpoint;
    try {
      const url = new URL(endpoint);
      domain = url.host;
    } catch (e) {
      // 处理无效URL，保持原样
    }

    return `${endpoint.split("//")[0]}//${bucketName}.${domain}/${normalizedPath}`;
  }
}

/**
 * 生成S3文件的上传预签名URL
 * @param {Object} s3Config - S3配置
 * @param {string} storagePath - S3存储路径
 * @param {string} mimetype - 文件的MIME类型
 * @param {string} encryptionSecret - 用于解密凭证的密钥
 * @param {number} expiresIn - URL过期时间（秒），默认为1小时
 * @returns {Promise<string>} 预签名URL
 */
async function generatePresignedPutUrl(s3Config, storagePath, mimetype, encryptionSecret, expiresIn = 3600) {
  try {
    // 创建S3客户端
    const s3Client = await createS3Client(s3Config, encryptionSecret);

    // 确保storagePath不以斜杠开始
    const normalizedPath = storagePath.startsWith("/") ? storagePath.slice(1) : storagePath;

    // 创建PutObjectCommand
    const command = new PutObjectCommand({
      Bucket: s3Config.bucket_name,
      Key: normalizedPath,
      ContentType: mimetype,
    });

    // 生成预签名URL
    const url = await getSignedUrl(s3Client, command, { expiresIn });

    return url;
  } catch (error) {
    console.error("生成上传预签名URL出错:", error);
    throw new Error("无法生成文件上传链接");
  }
}

/**
 * 生成S3文件的下载预签名URL
 * @param {Object} s3Config - S3配置
 * @param {string} storagePath - S3存储路径
 * @param {string} encryptionSecret - 用于解密凭证的密钥
 * @param {number} expiresIn - URL过期时间（秒），默认为1小时
 * @param {boolean} forceDownload - 是否强制下载（而非预览）
 * @returns {Promise<string>} 预签名URL
 */
async function generatePresignedUrl(s3Config, storagePath, encryptionSecret, expiresIn = 3600, forceDownload = false) {
  try {
    // 创建S3客户端
    const s3Client = await createS3Client(s3Config, encryptionSecret);

    // 确保storagePath不以斜杠开始
    const normalizedPath = storagePath.startsWith("/") ? storagePath.slice(1) : storagePath;

    // 提取文件名，用于Content-Disposition头
    const fileName = normalizedPath.split("/").pop();

    // 创建GetObjectCommand
    const commandParams = {
      Bucket: s3Config.bucket_name,
      Key: normalizedPath,
    };

    // 如果需要强制下载，添加相应的响应头
    if (forceDownload) {
      commandParams.ResponseContentDisposition = `attachment; filename="${encodeURIComponent(fileName)}"`;
    }

    const command = new GetObjectCommand(commandParams);

    // 生成预签名URL
    const url = await getSignedUrl(s3Client, command, { expiresIn });

    return url;
  } catch (error) {
    console.error("生成预签名URL出错:", error);
    throw new Error("无法生成文件下载链接");
  }
}

/**
 * 从S3存储中删除文件
 * @param {Object} s3Config - S3配置信息
 * @param {string} storagePath - 存储路径
 * @param {string} encryptionSecret - 加密密钥
 * @returns {Promise<boolean>} 是否成功删除
 */
async function deleteFileFromS3(s3Config, storagePath, encryptionSecret) {
  try {
    const s3Client = await createS3Client(s3Config, encryptionSecret);

    const deleteParams = {
      Bucket: s3Config.bucket_name,
      Key: storagePath,
    };

    await s3Client.send(new DeleteObjectCommand(deleteParams));
    console.log(`成功从S3存储中删除文件: ${storagePath}`);
    return true;
  } catch (error) {
    console.error(`从S3删除文件错误: ${error.message || error}`);
    return false;
  }
}

/**
 * 获取带有使用情况统计的S3配置列表
 * @param {D1Database} db - D1数据库实例
 * @returns {Promise<Array>} S3配置列表，每项包含使用情况统计
 */
async function getS3ConfigsWithUsage(db) {
  try {
    // 获取所有S3配置
    const configsResult = await db
        .prepare(
            `SELECT 
          id, name, provider_type, bucket_name, 
          endpoint_url, region, path_style, default_folder, 
          is_public, total_storage_bytes
        FROM ${DbTables.S3_CONFIGS}
        ORDER BY name ASC`
        )
        .all();

    if (!configsResult.results || configsResult.results.length === 0) {
      return [];
    }

    const configs = configsResult.results;
    const result = [];

    // 计算每个配置的使用情况
    for (const config of configs) {
      // 获取该配置下的文件总大小
      const usageResult = await db
          .prepare(
              `SELECT SUM(size) as total_size, COUNT(*) as file_count
           FROM ${DbTables.FILES}
           WHERE s3_config_id = ?`
          )
          .bind(config.id)
          .first();

      const usedStorage = usageResult ? usageResult.total_size || 0 : 0;
      const fileCount = usageResult ? usageResult.file_count || 0 : 0;

      // 设置总容量值
      let totalStorage = config.total_storage_bytes || 0;

      // 如果没有设置总容量或为0，为不同提供商设置默认值
      if (!totalStorage) {
        if (config.provider_type === "Cloudflare R2") {
          totalStorage = 10 * 1024 * 1024 * 1024; // 10GB
        } else if (config.provider_type === "Backblaze B2") {
          totalStorage = 102 * 1024 * 1024 * 1024; // 10GB
        } else {
          totalStorage = 5 * 1024 * 1024 * 1024; // 5GB
        }
      }

      result.push({
        id: config.id,
        name: config.name,
        providerType: config.provider_type,
        bucketName: config.bucket_name,
        usedStorage: usedStorage,
        totalStorage: totalStorage,
        fileCount: fileCount,
        // 计算使用百分比
        usagePercent: totalStorage > 0 ? Math.min(100, Math.round((usedStorage / totalStorage) * 100)) : 0,
      });
    }

    return result;
  } catch (error) {
    console.error("获取S3配置使用情况失败:", error);
    return [];
  }
}

/**
 * 处理admin文件管理处理文件直链下载请求
 * @param {string} slug - 文件slug
 * @param {Object} env - 环境变量
 * @param {Request} request - 原始请求
 * @param {boolean} forceDownload - 是否强制下载
 * @returns {Promise<Response>} 响应对象
 */
async function handleFileDownload(slug, env, request, forceDownload = false) {
  const db = env.DB;
  const encryptionSecret = env.ENCRYPTION_SECRET || "default-encryption-key";

  try {
    // 查询文件详情
    const file = await getFileBySlug(db, slug);

    // 检查文件是否存在
    if (!file) {
      return new Response("文件不存在", { status: 404 });
    }

    // 检查文件是否受密码保护
    if (file.password) {
      // 如果有密码，检查URL中是否包含密码参数
      const url = new URL(request.url);
      const passwordParam = url.searchParams.get("password");

      if (!passwordParam) {
        return new Response("需要密码访问此文件", { status: 401 });
      }

      // 验证密码
      const passwordValid = await verifyPassword(passwordParam, file.password);
      if (!passwordValid) {
        return new Response("密码错误", { status: 403 });
      }
    }

    // 检查文件是否可访问
    const accessCheck = await isFileAccessible(db, file, encryptionSecret);
    if (!accessCheck.accessible) {
      if (accessCheck.reason === "expired") {
        return new Response("文件已过期", { status: 410 });
      }
      return new Response("文件不可访问", { status: 403 });
    }

    // 增加访问次数并检查限制
    const result = await incrementAndCheckFileViews(db, file, encryptionSecret);

    // 如果文件已到达最大访问次数限制
    if (result.isExpired) {
      return new Response("文件已达到最大查看次数", { status: 410 });
    }

    // 如果没有S3配置或存储路径，则返回404
    if (!result.file.s3_config_id || !result.file.storage_path) {
      return new Response("文件存储信息不完整", { status: 404 });
    }

    // 获取S3配置
    const s3Config = await db.prepare(`SELECT * FROM ${DbTables.S3_CONFIGS} WHERE id = ?`).bind(result.file.s3_config_id).first();
    if (!s3Config) {
      return new Response("无法获取存储配置信息", { status: 500 });
    }

    try {
      // 生成预签名URL，有效期1小时
      const presignedUrl = await generatePresignedUrl(s3Config, result.file.storage_path, encryptionSecret, 3600, forceDownload);

      // 准备文件名和内容类型
      const filename = result.file.filename;
      const contentType = result.file.mimetype || "application/octet-stream";

      // 代理请求到实际的文件URL
      const fileRequest = new Request(presignedUrl);
      const response = await fetch(fileRequest);

      // 创建一个新的响应，包含正确的文件名和Content-Type
      const headers = new Headers(response.headers);

      // 根据是否强制下载设置Content-Disposition
      if (forceDownload) {
        headers.set("Content-Disposition", `attachment; filename="${encodeURIComponent(filename)}"`);
      } else {
        // 对于预览，我们不设置Content-Disposition或设置为inline
        headers.set("Content-Disposition", `inline; filename="${encodeURIComponent(filename)}"`);
      }

      // 确保设置正确的内容类型
      headers.set("Content-Type", contentType);

      // 返回响应
      return new Response(response.body, {
        status: response.status,
        headers: headers,
      });
    } catch (error) {
      console.error("代理文件下载出错:", error);
      return new Response("获取文件失败: " + error.message, { status: 500 });
    }
  } catch (error) {
    console.error("处理文件下载错误:", error);
    return new Response("服务器处理错误: " + error.message, { status: 500 });
  }
}
// =================================
// ========== 服务层函数 ==========
// =================================

/**
 * 服务层
 * 包含业务逻辑和数据库操作的函数
 */

/**
 * 验证管理员令牌
 * @param {D1Database} db - D1数据库实例
 * @param {string} token - JWT令牌
 * @returns {Promise<string|null>} 管理员ID或null
 */
async function validateAdminToken(db, token) {
  console.log("验证管理员令牌:", token.substring(0, 5) + "..." + token.substring(token.length - 5));

  try {
    // 查询令牌是否存在并且未过期
    const result = await db
        .prepare(
            `SELECT admin_id, expires_at 
         FROM ${DbTables.ADMIN_TOKENS} 
         WHERE token = ?`
        )
        .bind(token)
        .first();

    if (!result) {
      console.log("令牌不存在");
      return null;
    }

    const expiresAt = new Date(result.expires_at);
    const now = new Date();

    // 检查令牌是否已过期
    if (now > expiresAt) {
      console.log("令牌已过期", { expiresAt: expiresAt.toISOString(), now: now.toISOString() });
      // 删除过期的令牌
      await db.prepare(`DELETE FROM ${DbTables.ADMIN_TOKENS} WHERE token = ?`).bind(token).run();
      return null;
    }

    console.log("令牌验证成功，管理员ID:", result.admin_id);
    return result.admin_id;
  } catch (error) {
    console.error("验证令牌时发生错误:", error);
    return null;
  }
}

/**
 * 生成唯一的文本分享短链接slug
 * @param {D1Database} db - D1数据库实例
 * @param {string} customSlug - 自定义短链接
 * @returns {Promise<string>} 生成的唯一slug
 */
async function generateUniqueSlug(db, customSlug = null) {
  if (customSlug) {
    // 添加格式验证：只允许字母、数字、连字符、下划线
    const slugRegex = /^[a-zA-Z0-9_-]+$/;
    if (!slugRegex.test(customSlug)) {
      throw new Error("链接后缀格式无效，只允许使用字母、数字、连字符(-)和下划线(_)");
    }

    // 检查自定义slug是否已在文本表中存在
    const existingPaste = await db.prepare(`SELECT id FROM ${DbTables.PASTES} WHERE slug = ?`).bind(customSlug).first();

    if (!existingPaste) {
      return customSlug;
    }
    // 如果自定义slug已存在，抛出特定错误
    throw new Error("链接后缀已被占用，请尝试其他后缀");
  }

  // 生成随机slug
  const attempts = 5;
  for (let i = 0; i < attempts; i++) {
    const slug = generateRandomString(6);

    // 检查随机slug是否已在文本表中存在
    const existingPaste = await db.prepare(`SELECT id FROM ${DbTables.PASTES} WHERE slug = ?`).bind(slug).first();

    if (!existingPaste) {
      return slug;
    }
  }

  throw new Error("无法生成唯一链接，请稍后再试");
}

/**
 * 生成唯一的文件分享短链接slug
 * @param {D1Database} db - D1数据库实例
 * @param {string} customSlug - 自定义短链接
 * @returns {Promise<string>} 生成的唯一slug
 */
async function generateUniqueFileSlug(db, customSlug = null) {
  if (customSlug) {
    // 添加格式验证：只允许字母、数字、连字符、下划线
    const slugRegex = /^[a-zA-Z0-9_-]+$/;
    if (!slugRegex.test(customSlug)) {
      throw new Error("链接后缀格式无效，只允许使用字母、数字、连字符(-)和下划线(_)");
    }

    // 检查自定义slug是否已在文件表中存在
    const existingFile = await db.prepare(`SELECT id FROM ${DbTables.FILES} WHERE slug = ?`).bind(customSlug).first();

    if (!existingFile) {
      return customSlug;
    }
    // 如果自定义slug已存在，抛出特定错误
    throw new Error("链接后缀已被占用，请尝试其他后缀");
  }

  // 生成随机slug
  const attempts = 5;
  for (let i = 0; i < attempts; i++) {
    const slug = generateRandomString(6);

    // 检查随机slug是否已在文件表中存在
    const existingFile = await db.prepare(`SELECT id FROM ${DbTables.FILES} WHERE slug = ?`).bind(slug).first();

    if (!existingFile) {
      return slug;
    }
  }

  throw new Error("无法生成唯一链接，请稍后再试");
}

/**
 * 增加文本分享查看次数并检查是否需要删除
 * @param {D1Database} db - D1数据库实例
 * @param {string} pasteId - 文本分享ID
 * @param {number} maxViews - 最大查看次数
 * @returns {Promise<void>}
 */
async function incrementPasteViews(db, pasteId, maxViews) {
  // 增加查看次数
  await db.prepare(`UPDATE ${DbTables.PASTES} SET views = views + 1, updated_at = ? WHERE id = ?`).bind(getLocalTimeString(), pasteId).run();

  if (maxViews && maxViews > 0) {
    const updatedPaste = await db.prepare(`SELECT views FROM ${DbTables.PASTES} WHERE id = ?`).bind(pasteId).first();
    if (updatedPaste && updatedPaste.views >= maxViews) {
      console.log(`文本分享(${pasteId})已达到最大查看次数(${maxViews})，自动删除`);
      await db.prepare(`DELETE FROM ${DbTables.PASTES} WHERE id = ?`).bind(pasteId).run();
    }
  }
}

/**
 * 检查并删除过期的文本分享
 * @param {D1Database} db - D1数据库实例
 * @param {Object} paste - 文本分享对象
 * @returns {Promise<boolean>} 是否已过期并删除
 */
async function checkAndDeleteExpiredPaste(db, paste) {
  if (!paste) return false;

  const now = new Date();

  // 检查过期时间
  if (paste.expires_at && new Date(paste.expires_at) < now) {
    console.log(`文本分享(${paste.id})已过期，自动删除`);
    await db.prepare(`DELETE FROM ${DbTables.PASTES} WHERE id = ?`).bind(paste.id).run();
    return true;
  }

  // 检查最大查看次数
  if (paste.max_views && paste.views >= paste.max_views) {
    console.log(`文本分享(${paste.id})已达到最大查看次数，自动删除`);
    await db.prepare(`DELETE FROM ${DbTables.PASTES} WHERE id = ?`).bind(paste.id).run();
    return true;
  }

  return false;
}

/**
 * 检查并删除过期的API密钥
 * @param {D1Database} db - D1数据库实例
 * @param {Object} key - API密钥对象
 * @returns {Promise<boolean>} 是否已过期并删除
 */
async function checkAndDeleteExpiredApiKey(db, key) {
  if (!key) return true;

  const now = new Date();

  // 检查过期时间
  if (key.expires_at && new Date(key.expires_at) < now) {
    console.log(`API密钥(${key.id})已过期，自动删除`);
    await db.prepare(`DELETE FROM ${DbTables.API_KEYS} WHERE id = ?`).bind(key.id).run();
    return true;
  }

  return false;
}

/**
 * 检查文本分享是否可访问
 * @param {Object} paste - 文本分享对象
 * @returns {boolean} 是否可访问
 */
function isPasteAccessible(paste) {
  if (!paste) return false;

  const now = new Date();

  // 检查过期时间
  if (paste.expires_at && new Date(paste.expires_at) < now) {
    return false;
  }

  // 检查最大查看次数
  if (paste.max_views && paste.max_views > 0 && paste.views >= paste.max_views) {
    return false;
  }

  return true;
}

/**
 * 检查并删除过期的文件
 * @param {D1Database} db - D1数据库实例
 * @param {Object} file - 文件对象
 * @param {string} encryptionSecret - 加密密钥
 * @returns {Promise<boolean>} 是否已过期并删除
 */
async function checkAndDeleteExpiredFile(db, file, encryptionSecret) {
  if (!file) return false;

  const now = new Date();
  let isExpired = false;

  // 检查过期时间
  if (file.expires_at && new Date(file.expires_at) < now) {
    console.log(`文件(${file.id})已过期，自动删除`);
    isExpired = true;
  }

  // 检查最大查看次数
  if (!isExpired && file.max_views && file.max_views > 0 && file.views > file.max_views) {
    console.log(`文件(${file.id})已超过最大查看次数(${file.max_views})，自动删除`);
    isExpired = true;
  }

  // 如果文件已过期，从数据库和S3存储中删除
  if (isExpired) {
    try {
      // 删除数据库记录前，获取S3配置信息
      const s3Config = await db.prepare(`SELECT * FROM ${DbTables.S3_CONFIGS} WHERE id = ?`).bind(file.s3_config_id).first();

      // 从数据库中删除记录
      await db.prepare(`DELETE FROM ${DbTables.FILES} WHERE id = ?`).bind(file.id).run();

      // 从S3存储中删除文件
      if (s3Config && file.storage_path) {
        await deleteFileFromS3(s3Config, file.storage_path, encryptionSecret);
      }

      return true;
    } catch (error) {
      console.error(`删除过期文件错误(${file.id}): ${error.message || error}`);
      return false;
    }
  }

  return false;
}

/**
 * 增加文件访问次数并检查是否超过限制
 * @param {D1Database} db - D1数据库实例
 * @param {Object} file - 文件对象
 * @param {string} encryptionSecret - 加密密钥
 * @returns {Promise<Object>} 包含更新后的文件信息和状态
 */
async function incrementAndCheckFileViews(db, file, encryptionSecret) {
  // 首先递增访问计数
  await db.prepare(`UPDATE ${DbTables.FILES} SET views = views + 1, updated_at = ? WHERE id = ?`).bind(getLocalTimeString(), file.id).run();

  // 重新获取更新后的文件信息
  const updatedFile = await db
      .prepare(
          `
      SELECT 
        f.id, f.filename, f.storage_path, f.s3_url, f.mimetype, f.size, 
        f.remark, f.password, f.max_views, f.views, f.created_by,
        f.expires_at, f.created_at, f.s3_config_id, f.use_proxy, f.slug
      FROM ${DbTables.FILES} f
      WHERE f.id = ?
    `
      )
      .bind(file.id)
      .first();

  // 检查是否超过最大访问次数
  if (updatedFile.max_views && updatedFile.max_views > 0 && updatedFile.views > updatedFile.max_views) {
    // 已超过最大查看次数，执行删除
    await checkAndDeleteExpiredFile(db, updatedFile, encryptionSecret);
    return {
      isExpired: true,
      reason: "max_views",
      file: updatedFile,
    };
  }

  return {
    isExpired: false,
    file: updatedFile,
  };
}

/**
 * 获取文件的公开信息（移除敏感数据）
 * @param {Object} file - 文件对象
 * @param {boolean} requiresPassword - 是否需要密码
 * @param {Object|null} urls - 包含预览和下载URL的对象
 * @returns {Object} 过滤后的文件信息
 */
function getPublicFileInfo(file, requiresPassword, urls = null) {
  // 确定使用哪种URL
  const useProxy = urls?.use_proxy !== undefined ? urls.use_proxy : file.use_proxy || 0;

  // 根据是否使用代理选择URL
  const effectivePreviewUrl = useProxy === 1 ? urls?.proxyPreviewUrl : urls?.previewUrl || file.s3_url;
  const effectiveDownloadUrl = useProxy === 1 ? urls?.proxyDownloadUrl : urls?.downloadUrl || file.s3_url;

  return {
    id: file.id,
    filename: file.filename,
    mimetype: file.mimetype,
    size: file.size,
    remark: file.remark,
    created_at: file.created_at,
    requires_password: requiresPassword,
    views: file.views,
    max_views: file.max_views,
    expires_at: file.expires_at,
    previewUrl: effectivePreviewUrl,
    downloadUrl: effectiveDownloadUrl,
    s3_direct_preview_url: urls?.previewUrl || file.s3_url,
    s3_direct_download_url: urls?.downloadUrl || file.s3_url,
    proxy_preview_url: urls?.proxyPreviewUrl,
    proxy_download_url: urls?.proxyDownloadUrl,
    use_proxy: useProxy,
    created_by: file.created_by || null,
  };
}

/**
 * 生成文件下载URL
 * @param {D1Database} db - D1数据库实例
 * @param {Object} file - 文件对象
 * @param {string} encryptionSecret - 加密密钥
 * @param {Request} [request] - 原始请求对象，用于获取当前域名
 * @returns {Promise<Object>} 包含预览链接和下载链接的对象
 */
async function generateFileDownloadUrl(db, file, encryptionSecret, request = null) {
  let previewUrl = file.s3_url; // 默认使用原始URL作为回退
  let downloadUrl = file.s3_url; // 默认使用原始URL作为回退

  // 获取当前域名作为基础URL
  let baseUrl = "";
  if (request) {
    try {
      const url = new URL(request.url);
      baseUrl = url.origin; // 包含协议和域名，如 https://example.com
    } catch (error) {
      console.error("解析请求URL出错:", error);
      // 如果解析失败，baseUrl保持为空字符串
    }
  }

  // 构建代理URL，确保使用完整的绝对URL
  let proxyPreviewUrl = baseUrl ? `${baseUrl}/api/file-view/${file.slug}` : `/api/file-view/${file.slug}`;
  let proxyDownloadUrl = baseUrl ? `${baseUrl}/api/file-download/${file.slug}` : `/api/file-download/${file.slug}`;

  if (file.s3_config_id && file.storage_path) {
    const s3Config = await db.prepare(`SELECT * FROM ${DbTables.S3_CONFIGS} WHERE id = ?`).bind(file.s3_config_id).first();

    if (s3Config) {
      try {
        // 生成预览URL，有效期1小时
        previewUrl = await generatePresignedUrl(s3Config, file.storage_path, encryptionSecret, 3600, false);

        // 生成下载URL，有效期1小时，强制下载
        downloadUrl = await generatePresignedUrl(s3Config, file.storage_path, encryptionSecret, 3600, true);
      } catch (error) {
        console.error("生成预签名URL错误:", error);
        // 如果生成预签名URL失败，回退到使用原始S3 URL
      }
    }
  }

  return {
    previewUrl,
    downloadUrl,
    proxyPreviewUrl,
    proxyDownloadUrl,
    use_proxy: file.use_proxy || 0,
  };
}

/**
 * 检查文件是否可访问
 * @param {D1Database} db - D1数据库实例
 * @param {Object} file - 文件对象
 * @param {string} encryptionSecret - 加密密钥
 * @returns {Promise<Object>} 包含是否可访问及原因的对象
 */
async function isFileAccessible(db, file, encryptionSecret) {
  if (!file) {
    return { accessible: false, reason: "not_found" };
  }

  // 检查文件是否过期
  if (file.expires_at && new Date(file.expires_at) < new Date()) {
    // 文件已过期，执行删除
    await checkAndDeleteExpiredFile(db, file, encryptionSecret);
    return { accessible: false, reason: "expired" };
  }

  // 检查最大查看次数
  if (file.max_views && file.max_views > 0 && file.views > file.max_views) {
    // 已超过最大查看次数，执行删除
    await checkAndDeleteExpiredFile(db, file, encryptionSecret);
    return { accessible: false, reason: "max_views" };
  }

  return { accessible: true };
}

// =================================
// ========== 数据库初始化 ==========
// =================================

/**
 * 数据库初始化函数
 * 创建数据库表和默认数据
 * @param {D1Database} db - D1数据库实例
 */
async function initDatabase(db) {
  // 创建管理员表
  await db
      .prepare(
          `
    CREATE TABLE IF NOT EXISTS ${DbTables.ADMINS} (
      id TEXT PRIMARY KEY,
      username TEXT UNIQUE NOT NULL,
      password TEXT NOT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
  `
      )
      .run();

  // 创建管理员令牌表
  await db
      .prepare(
          `
    CREATE TABLE IF NOT EXISTS ${DbTables.ADMIN_TOKENS} (
      token TEXT PRIMARY KEY,
      admin_id TEXT NOT NULL,
      expires_at TIMESTAMP NOT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY (admin_id) REFERENCES ${DbTables.ADMINS}(id) ON DELETE CASCADE
    )
  `
      )
      .run();

  // 创建Paste表
  await db
      .prepare(
          `
    CREATE TABLE IF NOT EXISTS ${DbTables.PASTES} (
      id TEXT PRIMARY KEY,
      slug TEXT UNIQUE NOT NULL,
      content TEXT NOT NULL,
      remark TEXT,
      password TEXT,
      expires_at TIMESTAMP,
      max_views INTEGER,
      created_by TEXT,
      views INTEGER DEFAULT 0,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
  `
      )
      .run();

  // 创建API密钥表
  await db
      .prepare(
          `
    CREATE TABLE IF NOT EXISTS ${DbTables.API_KEYS} (
      id TEXT PRIMARY KEY,
      name TEXT UNIQUE NOT NULL,
      key TEXT UNIQUE NOT NULL,
      text_permission INTEGER DEFAULT 0,
      file_permission INTEGER DEFAULT 0,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      expires_at TIMESTAMP,
      last_used TIMESTAMP
    )
  `
      )
      .run();

  // 创建S3配置表
  await db
      .prepare(
          `
    CREATE TABLE IF NOT EXISTS ${DbTables.S3_CONFIGS} (
      id TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      provider_type TEXT NOT NULL,
      endpoint_url TEXT NOT NULL,
      bucket_name TEXT NOT NULL,
      region TEXT,
      access_key_id TEXT NOT NULL,
      secret_access_key TEXT NOT NULL,
      path_style INTEGER DEFAULT 0,
      default_folder TEXT DEFAULT '',
      is_public BOOLEAN DEFAULT 0,
      is_default BOOLEAN DEFAULT 0,
      total_storage_bytes BIGINT,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      last_used TIMESTAMP,
      admin_id TEXT,
      FOREIGN KEY (admin_id) REFERENCES ${DbTables.ADMINS}(id) ON DELETE CASCADE
    )
  `
      )
      .run();

  // 创建文件表
  await db
      .prepare(
          `
    CREATE TABLE IF NOT EXISTS ${DbTables.FILES} (
      id TEXT PRIMARY KEY,
      filename TEXT NOT NULL,
      storage_path TEXT NOT NULL,
      s3_url TEXT,
      mimetype TEXT NOT NULL,
      size INTEGER NOT NULL,
      s3_config_id TEXT NOT NULL,
      slug TEXT UNIQUE NOT NULL,
      remark TEXT,
      password TEXT,
      expires_at TIMESTAMP,
      max_views INTEGER,
      views INTEGER DEFAULT 0,
      use_proxy BOOLEAN DEFAULT 1,
      etag TEXT,
      created_by TEXT,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY (s3_config_id) REFERENCES ${DbTables.S3_CONFIGS}(id) ON DELETE CASCADE
    )
  `
      )
      .run();

  // 创建文件密码表
  await db
      .prepare(
          `
    CREATE TABLE IF NOT EXISTS ${DbTables.FILE_PASSWORDS} (
      file_id TEXT PRIMARY KEY,
      plain_password TEXT NOT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY (file_id) REFERENCES ${DbTables.FILES}(id) ON DELETE CASCADE
    )
  `
      )
      .run();

  // 创建文本密码表
  await db
      .prepare(
          `
    CREATE TABLE IF NOT EXISTS ${DbTables.PASTE_PASSWORDS} (
      paste_id TEXT PRIMARY KEY,
      plain_password TEXT NOT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY (paste_id) REFERENCES ${DbTables.PASTES}(id) ON DELETE CASCADE
    )
  `
      )
      .run();

  // 创建系统设置表
  await db
      .prepare(
          `
    CREATE TABLE IF NOT EXISTS ${DbTables.SYSTEM_SETTINGS} (
      key TEXT PRIMARY KEY,
      value TEXT NOT NULL,
      description TEXT,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
  `
      )
      .run();

  // 检查是否已存在最大上传限制设置
  const maxUploadSize = await db
      .prepare(
          `
    SELECT value FROM ${DbTables.SYSTEM_SETTINGS}
    WHERE key = 'max_upload_size'
  `
      )
      .first();

  // 如果不存在，添加默认值
  if (!maxUploadSize) {
    await db
        .prepare(
            `
      INSERT INTO ${DbTables.SYSTEM_SETTINGS} (key, value, description)
      VALUES ('max_upload_size', ?, '单次最大上传文件大小限制')
    `
        )
        .bind(DEFAULT_MAX_UPLOAD_SIZE_MB.toString())
        .run();
  }

  // 检查是否需要创建默认管理员账户
  const adminCount = await db.prepare(`SELECT COUNT(*) as count FROM ${DbTables.ADMINS}`).first();

  if (adminCount.count === 0) {
    const adminId = crypto.randomUUID();
    const defaultPassword = await hashPassword("admin123");

    await db
        .prepare(
            `
      INSERT INTO ${DbTables.ADMINS} (id, username, password)
      VALUES (?, ?, ?)
    `
        )
        .bind(adminId, "admin", defaultPassword)
        .run();

    console.log("已创建默认管理员账户: admin/admin123");
  }
}

// =================================
// ========== API路由定义 ==========
// =================================

/**
 * 路由层
 * 定义API端点和请求处理逻辑
 */

// API根路径
app.get("/api", (c) => {
  return c.json({
    name: "CloudPaste API",
    version: "1.0.0",
    status: "ok",
  });
});

// =================================
// ========== Admin API =====================================================================================================================
// =================================

/**
 * 管理员API
 * 负责管理员认证/修改密码等等
 */

// 测试管理员令牌有效性
app.get("/api/test/admin-token", authMiddleware, async (c) => {
  // 如果认证中间件通过，说明令牌有效
  return c.json({
    code: ApiStatus.SUCCESS,
    message: "令牌有效",
    success: true,
  });
});

// 管理员登录
app.post("/api/admin/login", async (c) => {
  const db = c.env.DB;
  const { username, password } = await c.req.json();

  // 参数验证
  if (!username || !password) {
    throw new HTTPException(ApiStatus.BAD_REQUEST, { message: "用户名和密码不能为空" });
  }

  // 查询管理员
  const admin = await db.prepare(`SELECT id, username, password FROM ${DbTables.ADMINS} WHERE username = ?`).bind(username).first();

  if (!admin) {
    throw new HTTPException(ApiStatus.UNAUTHORIZED, { message: "用户名或密码错误" });
  }

  // 验证密码
  const isValid = await verifyPassword(password, admin.password);
  if (!isValid) {
    throw new HTTPException(ApiStatus.UNAUTHORIZED, { message: "用户名或密码错误" });
  }

  // 生成并存储令牌
  const token = generateRandomString(32);
  const expiresAt = new Date();
  expiresAt.setDate(expiresAt.getDate() + 1); // 1天过期

  await db
      .prepare(
          `
    INSERT INTO ${DbTables.ADMIN_TOKENS} (token, admin_id, expires_at)
    VALUES (?, ?, ?)
  `
      )
      .bind(token, admin.id, expiresAt.toISOString())
      .run();

  // 返回认证信息
  return c.json({
    code: ApiStatus.SUCCESS,
    message: "登录成功",
    data: {
      username: admin.username,
      token,
      expiresAt: expiresAt.toISOString(),
    },
  });
});

// 管理员登出
app.post("/api/admin/logout", authMiddleware, async (c) => {
  const db = c.env.DB;
  const adminId = c.get("adminId");
  const authHeader = c.req.header("Authorization");
  const token = authHeader.substring(7);

  // 从数据库删除token
  await db.prepare(`DELETE FROM ${DbTables.ADMIN_TOKENS} WHERE token = ?`).bind(token).run();

  return c.json({
    code: ApiStatus.SUCCESS,
    message: "登出成功",
  });
});

// 更改管理员密码（需要认证）
app.post("/api/admin/change-password", authMiddleware, async (c) => {
  const db = c.env.DB;
  const adminId = c.get("adminId");
  const { currentPassword, newPassword, newUsername } = await c.req.json();

  // 验证当前密码
  const admin = await db.prepare(`SELECT password FROM ${DbTables.ADMINS} WHERE id = ?`).bind(adminId).first();

  if (!(await verifyPassword(currentPassword, admin.password))) {
    throw new HTTPException(ApiStatus.UNAUTHORIZED, { message: "当前密码错误" });
  }

  // 如果提供了新用户名，先检查用户名是否已存在
  if (newUsername && newUsername.trim() !== "") {
    const existingAdmin = await db.prepare(`SELECT id FROM ${DbTables.ADMINS} WHERE username = ? AND id != ?`).bind(newUsername, adminId).first();

    if (existingAdmin) {
      throw new HTTPException(ApiStatus.CONFLICT, { message: "用户名已存在" });
    }

    // 更新用户名和密码
    const newPasswordHash = newPassword ? await hashPassword(newPassword) : admin.password;

    await db
        .prepare(
            `
      UPDATE ${DbTables.ADMINS} 
      SET username = ?, password = ?, updated_at = CURRENT_TIMESTAMP
      WHERE id = ?
    `
        )
        .bind(newUsername, newPasswordHash, adminId)
        .run();
  } else if (newPassword) {
    // 仅更新密码
    const newPasswordHash = await hashPassword(newPassword);
    await db
        .prepare(
            `
      UPDATE ${DbTables.ADMINS} 
      SET password = ?, updated_at = CURRENT_TIMESTAMP
      WHERE id = ?
    `
        )
        .bind(newPasswordHash, adminId)
        .run();
  } else {
    throw new HTTPException(ApiStatus.BAD_REQUEST, { message: "未提供新密码或新用户名" });
  }

  // 删除该管理员的所有认证令牌，强制重新登录
  await db.prepare(`DELETE FROM ${DbTables.ADMIN_TOKENS} WHERE admin_id = ?`).bind(adminId).run();

  return c.json({
    code: ApiStatus.SUCCESS,
    message: "信息更新成功，请重新登录",
  });
});

// =================================
// ========== API密钥管理 ===============================================================================================================================
// =================================

/**
 * API密钥管理
 * 负责API密钥的创建、查询和删除
 */

// 测试API密钥验证路由
app.get("/api/test/api-key", apiKeyMiddleware, async (c) => {
  // 获取密钥信息
  const apiKeyInfo = c.get("apiKey");
  const apiKeyId = c.get("apiKeyId");

  return c.json({
    code: ApiStatus.SUCCESS,
    message: "API密钥验证成功",
    data: {
      name: apiKeyInfo.name,
      permissions: {
        text: apiKeyInfo.textPermission,
        file: apiKeyInfo.filePermission,
      },
      key_info: {
        id: apiKeyId || apiKeyInfo.id,
        name: apiKeyInfo.name,
      },
    },
    success: true, // 添加兼容字段
  });
});

// 获取所有API密钥列表
app.get("/api/admin/api-keys", authMiddleware, async (c) => {
  const db = c.env.DB;

  try {
    // 先清理过期的API密钥
    const now = new Date().toISOString();
    await db.prepare(`DELETE FROM ${DbTables.API_KEYS} WHERE expires_at IS NOT NULL AND expires_at < ?`).bind(now).run();

    // 查询所有密钥，并隐藏完整密钥
    const keys = await db
        .prepare(
            `
      SELECT 
        id, 
        name, 
        key,
        SUBSTR(key, 1, 6) || '...' AS key_masked,
        text_permission, 
        file_permission, 
        created_at, 
        expires_at,
        last_used
      FROM ${DbTables.API_KEYS}
      ORDER BY created_at DESC
    `
        )
        .all();

    // 兼容前端期望的响应格式
    return c.json({
      code: ApiStatus.SUCCESS,
      message: "获取成功",
      data: keys.results,
      success: true, // 添加兼容字段
    });
  } catch (error) {
    // 使用统一错误响应
    return c.json(createErrorResponse(ApiStatus.INTERNAL_ERROR, error.message || "获取API密钥列表失败"), ApiStatus.INTERNAL_ERROR);
  }
});

// 创建新的API密钥
app.post("/api/admin/api-keys", authMiddleware, async (c) => {
  const db = c.env.DB;

  try {
    const body = await c.req.json();

    // 必需参数：名称验证
    if (!body.name || body.name.trim() === "") {
      return c.json(createErrorResponse(ApiStatus.BAD_REQUEST, "密钥名称不能为空"), ApiStatus.BAD_REQUEST);
    }

    // 如果用户提供了自定义密钥，验证其格式
    if (body.custom_key) {
      // 验证密钥格式：只允许字母、数字、横杠和下划线
      const keyFormatRegex = /^[a-zA-Z0-9_-]+$/;
      if (!keyFormatRegex.test(body.custom_key)) {
        return c.json(createErrorResponse(ApiStatus.BAD_REQUEST, "密钥只能包含字母、数字、横杠和下划线"), ApiStatus.BAD_REQUEST);
      }
    }

    // 生成唯一ID
    const id = crypto.randomUUID();

    // 生成API密钥，如果有自定义密钥则使用自定义密钥
    const key = body.custom_key ? body.custom_key : generateRandomString(12);

    // 处理过期时间，默认为1天后
    const now = new Date();
    let expiresAt;

    if (body.expires_at) {
      expiresAt = new Date(body.expires_at);
    } else {
      expiresAt = new Date();
      expiresAt.setDate(now.getDate() + 1); // 默认一天后过期
    }

    // 确保日期是有效的
    if (isNaN(expiresAt.getTime())) {
      return c.json(createErrorResponse(ApiStatus.BAD_REQUEST, "无效的过期时间"), ApiStatus.BAD_REQUEST);
    }

    // 对text_permission和file_permission提供默认值
    const textPermission = body.text_permission === true ? 1 : 0;
    const filePermission = body.file_permission === true ? 1 : 0;

    // 插入到数据库
    await db
        .prepare(
            `
      INSERT INTO ${DbTables.API_KEYS} (id, name, key, text_permission, file_permission, expires_at)
      VALUES (?, ?, ?, ?, ?, ?)
    `
        )
        .bind(id, body.name.trim(), key, textPermission, filePermission, expiresAt.toISOString())
        .run();

    // 准备响应数据
    const responseData = {
      id,
      name: body.name.trim(),
      key,
      key_masked: key.substring(0, 6) + "...", // 前端期望的密钥掩码
      text_permission: textPermission === 1,
      file_permission: filePermission === 1,
      created_at: now.toISOString(),
      expires_at: expiresAt.toISOString(),
    };

    // 返回完整密钥（仅在创建时返回一次）
    return c.json({
      code: ApiStatus.CREATED,
      message: "API密钥创建成功",
      data: responseData,
      success: true, // 添加兼容字段
    });
  } catch (error) {
    // 检查是否是唯一性约束错误
    if (error.message && error.message.includes("UNIQUE constraint failed")) {
      if (error.message.includes("api_keys.name")) {
        return c.json(createErrorResponse(ApiStatus.CONFLICT, "密钥名称已存在"), ApiStatus.CONFLICT);
      } else if (error.message.includes("api_keys.key")) {
        return c.json(createErrorResponse(ApiStatus.CONFLICT, "密钥已存在"), ApiStatus.CONFLICT);
      }
    }

    // 其他错误
    return c.json(createErrorResponse(ApiStatus.INTERNAL_ERROR, error.message || "创建API密钥失败"), ApiStatus.INTERNAL_ERROR);
  }
});

// 修改API密钥
app.put("/api/admin/api-keys/:id", authMiddleware, async (c) => {
  const db = c.env.DB;
  const id = c.req.param("id");

  try {
    const body = await c.req.json();

    // 检查密钥是否存在
    const keyExists = await db.prepare(`SELECT id, name, key FROM ${DbTables.API_KEYS} WHERE id = ?`).bind(id).first();

    if (!keyExists) {
      return c.json(createErrorResponse(ApiStatus.NOT_FOUND, "密钥不存在"), ApiStatus.NOT_FOUND);
    }

    // 验证名称
    if (body.name && !body.name.trim()) {
      return c.json(createErrorResponse(ApiStatus.BAD_REQUEST, "密钥名称不能为空"), ApiStatus.BAD_REQUEST);
    }

    // 检查名称是否已存在（排除当前密钥）
    if (body.name && body.name !== keyExists.name) {
      const nameExists = await db.prepare(`SELECT id FROM ${DbTables.API_KEYS} WHERE name = ? AND id != ?`).bind(body.name.trim(), id).first();

      if (nameExists) {
        return c.json(createErrorResponse(ApiStatus.CONFLICT, "密钥名称已存在"), ApiStatus.CONFLICT);
      }
    }

    // 处理过期时间
    let expiresAt = null;
    if (body.expires_at) {
      expiresAt = new Date(body.expires_at);

      // 确保日期是有效的
      if (isNaN(expiresAt.getTime())) {
        return c.json(createErrorResponse(ApiStatus.BAD_REQUEST, "无效的过期时间"), ApiStatus.BAD_REQUEST);
      }
    }

    // 构建更新 SQL
    const updates = [];
    const params = [];

    // 只更新提供的字段
    if (body.name !== undefined) {
      updates.push("name = ?");
      params.push(body.name.trim());
    }

    if (body.text_permission !== undefined) {
      updates.push("text_permission = ?");
      params.push(body.text_permission ? 1 : 0);
    }

    if (body.file_permission !== undefined) {
      updates.push("file_permission = ?");
      params.push(body.file_permission ? 1 : 0);
    }

    if (expiresAt !== null) {
      updates.push("expires_at = ?");
      params.push(expiresAt.toISOString());
    }

    // 如果没有要更新的字段，直接返回成功
    if (updates.length === 0) {
      return c.json(createErrorResponse(ApiStatus.BAD_REQUEST, "没有提供有效的更新字段"), ApiStatus.BAD_REQUEST);
    }

    // 添加 ID 参数
    params.push(id);

    // 执行更新
    await db
        .prepare(`UPDATE ${DbTables.API_KEYS} SET ${updates.join(", ")} WHERE id = ?`)
        .bind(...params)
        .run();

    // 返回更新结果
    return c.json({
      code: ApiStatus.SUCCESS,
      message: "API密钥已更新",
      success: true, // 添加兼容字段
    });
  } catch (error) {
    // 统一错误处理
    return c.json(createErrorResponse(ApiStatus.INTERNAL_ERROR, error.message || "更新API密钥失败"), ApiStatus.INTERNAL_ERROR);
  }
});

// 删除API密钥
app.delete("/api/admin/api-keys/:id", authMiddleware, async (c) => {
  const db = c.env.DB;
  const id = c.req.param("id");

  try {
    // 检查密钥是否存在
    const keyExists = await db.prepare(`SELECT id FROM ${DbTables.API_KEYS} WHERE id = ?`).bind(id).first();

    if (!keyExists) {
      return c.json(createErrorResponse(ApiStatus.NOT_FOUND, "密钥不存在"), ApiStatus.NOT_FOUND);
    }

    // 删除密钥
    await db.prepare(`DELETE FROM ${DbTables.API_KEYS} WHERE id = ?`).bind(id).run();

    return c.json({
      code: ApiStatus.SUCCESS,
      message: "密钥已删除",
      success: true, // 添加兼容字段
    });
  } catch (error) {
    // 统一错误处理
    return c.json(createErrorResponse(ApiStatus.INTERNAL_ERROR, error.message || "删除API密钥失败"), ApiStatus.INTERNAL_ERROR);
  }
});

// =================================
// ========== S3配置 API ===============================================================================================================================
// =================================

/**
 * S3配置API
 *
 */

// 获取S3配置列表（管理员权限或API密钥文件权限）
app.get("/api/s3-configs", async (c) => {
  const db = c.env.DB;

  // 获取授权信息
  const authHeader = c.req.header("Authorization");
  let isAdmin = false;
  let hasFilePermission = false;
  let adminId = null;

  // 检查授权类型
  if (authHeader && authHeader.startsWith("Bearer ")) {
    // 管理员令牌认证
    try {
      const token = authHeader.substring(7);
      adminId = await validateAdminToken(db, token);
      if (adminId) {
        isAdmin = true;
      }
    } catch (error) {
      console.error("验证管理员令牌出错:", error);
    }
  } else if (authHeader && authHeader.startsWith("ApiKey ")) {
    // API密钥认证
    try {
      const apiKey = authHeader.substring(7);
      // 查询API密钥和权限
      const keyRecord = await db
          .prepare(
              `SELECT id, name, file_permission, expires_at 
           FROM ${DbTables.API_KEYS} 
           WHERE key = ?`
          )
          .bind(apiKey)
          .first();

      if (keyRecord && keyRecord.file_permission === 1) {
        // 检查是否过期
        if (!(await checkAndDeleteExpiredApiKey(db, keyRecord))) {
          hasFilePermission = true;

          // 更新最后使用时间
          await db
              .prepare(
                  `UPDATE ${DbTables.API_KEYS}
               SET last_used = ?
               WHERE id = ?`
              )
              .bind(getLocalTimeString(), keyRecord.id)
              .run();
        }
      }
    } catch (error) {
      console.error("验证API密钥出错:", error);
    }
  }

  // 如果既不是管理员也没有文件权限，返回未授权错误
  if (!isAdmin && !hasFilePermission) {
    return c.json(createErrorResponse(ApiStatus.UNAUTHORIZED, "未授权访问S3配置"), ApiStatus.UNAUTHORIZED);
  }

  try {
    let configs;

    if (isAdmin) {
      // 管理员可以看到所有自己的配置
      configs = await db
          .prepare(
              `
          SELECT 
            id, name, provider_type, endpoint_url, bucket_name, 
            region, path_style, default_folder, is_public, is_default, 
            created_at, updated_at, last_used, total_storage_bytes
          FROM ${DbTables.S3_CONFIGS}
          WHERE admin_id = ?
          ORDER BY name ASC
          `
          )
          .bind(adminId)
          .all();
    } else {
      // API密钥用户只能看到公开的配置
      configs = await db
          .prepare(
              `
          SELECT 
            id, name, provider_type, endpoint_url, bucket_name, 
            region, path_style, default_folder, is_default, created_at, updated_at, total_storage_bytes
          FROM ${DbTables.S3_CONFIGS}
          WHERE is_public = 1
          ORDER BY name ASC
          `
          )
          .all();
    }

    return c.json({
      code: ApiStatus.SUCCESS,
      message: "获取S3配置列表成功",
      data: configs.results, // 不返回敏感字段
      success: true, // 添加兼容字段
    });
  } catch (error) {
    console.error("获取S3配置列表错误:", error);
    return c.json(createErrorResponse(ApiStatus.INTERNAL_ERROR, error.message || "获取S3配置列表失败"), ApiStatus.INTERNAL_ERROR);
  }
});

// 获取单个S3配置详情
app.get("/api/s3-configs/:id", async (c) => {
  const db = c.env.DB;
  const { id } = c.req.param();

  // 获取授权信息
  const authHeader = c.req.header("Authorization");
  let isAdmin = false;
  let hasFilePermission = false;
  let adminId = null;

  // 检查授权类型
  if (authHeader && authHeader.startsWith("Bearer ")) {
    // 管理员令牌认证
    try {
      const token = authHeader.substring(7);
      adminId = await validateAdminToken(db, token);
      if (adminId) {
        isAdmin = true;
      }
    } catch (error) {
      console.error("验证管理员令牌出错:", error);
    }
  } else if (authHeader && authHeader.startsWith("ApiKey ")) {
    // API密钥认证
    try {
      const apiKey = authHeader.substring(7);
      // 查询API密钥和权限
      const keyRecord = await db
          .prepare(
              `SELECT id, name, file_permission, expires_at 
           FROM ${DbTables.API_KEYS} 
           WHERE key = ?`
          )
          .bind(apiKey)
          .first();

      if (keyRecord && keyRecord.file_permission === 1) {
        // 检查是否过期
        if (!(await checkAndDeleteExpiredApiKey(db, keyRecord))) {
          hasFilePermission = true;
        }
      }
    } catch (error) {
      console.error("验证API密钥出错:", error);
    }
  }

  // 如果既不是管理员也没有文件权限，返回未授权错误
  if (!isAdmin && !hasFilePermission) {
    return c.json(createErrorResponse(ApiStatus.UNAUTHORIZED, "未授权访问S3配置"), ApiStatus.UNAUTHORIZED);
  }

  try {
    let config;

    if (isAdmin) {
      // 管理员查询
      config = await db
          .prepare(
              `
          SELECT 
            id, name, provider_type, endpoint_url, bucket_name, 
            region, path_style, default_folder, is_public, is_default, 
            created_at, updated_at, last_used, total_storage_bytes
          FROM ${DbTables.S3_CONFIGS}
          WHERE id = ? AND admin_id = ?
        `
          )
          .bind(id, adminId)
          .first();
    } else {
      // API密钥用户查询
      config = await db
          .prepare(
              `
          SELECT 
            id, name, provider_type, endpoint_url, bucket_name, 
            region, path_style, default_folder, is_default, created_at, updated_at, total_storage_bytes
          FROM ${DbTables.S3_CONFIGS}
          WHERE id = ? AND is_public = 1
        `
          )
          .bind(id)
          .first();
    }

    if (!config) {
      return c.json(createErrorResponse(ApiStatus.NOT_FOUND, "S3配置不存在"), ApiStatus.NOT_FOUND);
    }

    return c.json({
      code: ApiStatus.SUCCESS,
      message: "获取S3配置成功",
      data: config, // 不返回敏感字段
      success: true, // 添加兼容字段
    });
  } catch (error) {
    console.error("获取S3配置错误:", error);
    return c.json(createErrorResponse(ApiStatus.INTERNAL_ERROR, error.message || "获取S3配置失败"), ApiStatus.INTERNAL_ERROR);
  }
});

// 创建S3配置（管理员权限）
app.post("/api/s3-configs", authMiddleware, async (c) => {
  const db = c.env.DB;
  const adminId = c.get("adminId");

  try {
    const body = await c.req.json();

    // 验证必填字段
    const requiredFields = ["name", "provider_type", "endpoint_url", "bucket_name", "access_key_id", "secret_access_key"];
    for (const field of requiredFields) {
      if (!body[field]) {
        return c.json(createErrorResponse(ApiStatus.BAD_REQUEST, `缺少必填字段: ${field}`), ApiStatus.BAD_REQUEST);
      }
    }

    // 生成唯一ID
    const id = generateS3ConfigId();

    // 加密敏感字段
    const encryptionSecret = c.env.ENCRYPTION_SECRET || "default-encryption-key";
    const encryptedAccessKey = await encryptValue(body.access_key_id, encryptionSecret);
    const encryptedSecretKey = await encryptValue(body.secret_access_key, encryptionSecret);

    // 获取可选字段或设置默认值
    const region = body.region || "";
    const pathStyle = body.path_style === true ? 1 : 0;
    const defaultFolder = body.default_folder || "";
    const isPublic = body.is_public === true ? 1 : 0;

    // 处理存储总容量
    let totalStorageBytes = null;
    if (body.total_storage_bytes !== undefined) {
      // 如果用户提供了总容量，则直接使用
      const storageValue = parseInt(body.total_storage_bytes);
      if (!isNaN(storageValue) && storageValue > 0) {
        totalStorageBytes = storageValue;
      }
    }

    // 如果未提供存储容量，根据不同的存储提供商设置合理的默认值
    if (totalStorageBytes === null) {
      if (body.provider_type === "Cloudflare R2") {
        totalStorageBytes = 10 * 1024 * 1024 * 1024; // 10GB默认值
      } else if (body.provider_type === "Backblaze B2") {
        totalStorageBytes = 10 * 1024 * 1024 * 1024; // 10GB默认值
      } else {
        totalStorageBytes = 5 * 1024 * 1024 * 1024; // 5GB默认值
      }
      console.log(`未提供存储容量限制，为${body.provider_type}设置默认值: ${formatFileSize(totalStorageBytes)}`);
    }

    // 添加到数据库
    await db
        .prepare(
            `
      INSERT INTO ${DbTables.S3_CONFIGS} (
        id, name, provider_type, endpoint_url, bucket_name, 
        region, access_key_id, secret_access_key, path_style, 
        default_folder, is_public, admin_id, total_storage_bytes, created_at, updated_at
      ) VALUES (
        ?, ?, ?, ?, ?, 
        ?, ?, ?, ?, 
        ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
      )
    `
        )
        .bind(
            id,
            body.name,
            body.provider_type,
            body.endpoint_url,
            body.bucket_name,
            region,
            encryptedAccessKey,
            encryptedSecretKey,
            pathStyle,
            defaultFolder,
            isPublic,
            adminId,
            totalStorageBytes
        )
        .run();

    // 返回创建成功响应（不包含敏感字段）
    return c.json({
      code: ApiStatus.CREATED,
      message: "S3配置创建成功",
      data: {
        id,
        name: body.name,
        provider_type: body.provider_type,
        endpoint_url: body.endpoint_url,
        bucket_name: body.bucket_name,
        region,
        path_style: pathStyle === 1,
        default_folder: defaultFolder,
        is_public: isPublic === 1,
        total_storage_bytes: totalStorageBytes,
      },
      success: true, // 添加兼容字段
    });
  } catch (error) {
    console.error("创建S3配置错误:", error);
    return c.json(createErrorResponse(ApiStatus.INTERNAL_ERROR, error.message || "创建S3配置失败"), ApiStatus.INTERNAL_ERROR);
  }
});

// 更新S3配置（管理员权限）
app.put("/api/s3-configs/:id", authMiddleware, async (c) => {
  const db = c.env.DB;
  const adminId = c.get("adminId");
  const { id } = c.req.param();

  try {
    // 查询配置是否存在
    const config = await db.prepare(`SELECT id, provider_type FROM ${DbTables.S3_CONFIGS} WHERE id = ? AND admin_id = ?`).bind(id, adminId).first();

    if (!config) {
      return c.json(createErrorResponse(ApiStatus.NOT_FOUND, "S3配置不存在"), ApiStatus.NOT_FOUND);
    }

    const body = await c.req.json();

    // 获取加密密钥
    const encryptionSecret = c.env.ENCRYPTION_SECRET || "default-encryption-key";

    // 准备更新字段
    const updateFields = [];
    const params = [];

    // 处理存储容量字段
    if (body.total_storage_bytes !== undefined) {
      // 如果用户提供了总容量参数
      if (body.total_storage_bytes === null) {
        // 为null表示使用默认值，根据提供商类型设置
        let defaultStorageBytes;
        if (config.provider_type === "Cloudflare R2") {
          defaultStorageBytes = 10 * 1024 * 1024 * 1024; // 10GB 默认值
        } else if (config.provider_type === "Backblaze B2") {
          defaultStorageBytes = 10 * 1024 * 1024 * 1024; // 10GB 默认值
        } else {
          defaultStorageBytes = 5 * 1024 * 1024 * 1024; // 5GB 默认值
        }

        updateFields.push("total_storage_bytes = ?");
        params.push(defaultStorageBytes);
        console.log(`重置存储容量限制，为${config.provider_type}设置默认值: ${formatFileSize(defaultStorageBytes)}`);
      } else {
        // 用户提供了具体数值
        const storageValue = parseInt(body.total_storage_bytes);
        if (!isNaN(storageValue) && storageValue > 0) {
          updateFields.push("total_storage_bytes = ?");
          params.push(storageValue);
        }
      }
    }

    // 更新名称
    if (body.name !== undefined) {
      updateFields.push("name = ?");
      params.push(body.name);
    }

    // 更新提供商类型
    if (body.provider_type !== undefined) {
      updateFields.push("provider_type = ?");
      params.push(body.provider_type);
    }

    // 更新端点URL
    if (body.endpoint_url !== undefined) {
      updateFields.push("endpoint_url = ?");
      params.push(body.endpoint_url);
    }

    // 更新桶名称
    if (body.bucket_name !== undefined) {
      updateFields.push("bucket_name = ?");
      params.push(body.bucket_name);
    }

    // 更新区域
    if (body.region !== undefined) {
      updateFields.push("region = ?");
      params.push(body.region);
    }

    // 更新访问密钥ID（需要加密）
    if (body.access_key_id !== undefined) {
      updateFields.push("access_key_id = ?");
      const encryptedAccessKey = await encryptValue(body.access_key_id, encryptionSecret);
      params.push(encryptedAccessKey);
    }

    // 更新秘密访问密钥（需要加密）
    if (body.secret_access_key !== undefined) {
      updateFields.push("secret_access_key = ?");
      const encryptedSecretKey = await encryptValue(body.secret_access_key, encryptionSecret);
      params.push(encryptedSecretKey);
    }

    // 更新路径样式
    if (body.path_style !== undefined) {
      updateFields.push("path_style = ?");
      params.push(body.path_style === true ? 1 : 0);
    }

    // 更新默认文件夹
    if (body.default_folder !== undefined) {
      updateFields.push("default_folder = ?");
      params.push(body.default_folder);
    }

    // 更新是否公开
    if (body.is_public !== undefined) {
      updateFields.push("is_public = ?");
      params.push(body.is_public === true ? 1 : 0);
    }

    // 更新时间戳
    updateFields.push("updated_at = ?");
    params.push(new Date().toISOString());

    // 如果没有更新字段，直接返回成功
    if (updateFields.length === 0) {
      return c.json({
        code: ApiStatus.SUCCESS,
        message: "未提供任何更新字段",
        success: true, // 添加兼容字段
      });
    }

    // 添加ID作为条件参数
    params.push(id);
    params.push(adminId);

    // 执行更新
    await db
        .prepare(`UPDATE ${DbTables.S3_CONFIGS} SET ${updateFields.join(", ")} WHERE id = ? AND admin_id = ?`)
        .bind(...params)
        .run();

    return c.json({
      code: ApiStatus.SUCCESS,
      message: "S3配置已更新",
      success: true, // 添加兼容字段
    });
  } catch (error) {
    console.error("更新S3配置错误:", error);
    return c.json(createErrorResponse(ApiStatus.INTERNAL_ERROR, error.message || "更新S3配置失败"), ApiStatus.INTERNAL_ERROR);
  }
});

// 删除S3配置（管理员权限）
app.delete("/api/s3-configs/:id", authMiddleware, async (c) => {
  const db = c.env.DB;
  const adminId = c.get("adminId");
  const { id } = c.req.param();

  try {
    // 查询配置是否存在
    const existingConfig = await db.prepare(`SELECT id FROM ${DbTables.S3_CONFIGS} WHERE id = ? AND admin_id = ?`).bind(id, adminId).first();

    if (!existingConfig) {
      return c.json(createErrorResponse(ApiStatus.NOT_FOUND, "S3配置不存在"), ApiStatus.NOT_FOUND);
    }

    // 检查是否有文件使用此配置
    const filesCount = await db
        .prepare(
            `
        SELECT COUNT(*) as count FROM ${DbTables.FILES}
        WHERE s3_config_id = ?
      `
        )
        .bind(id)
        .first();

    if (filesCount && filesCount.count > 0) {
      return c.json(createErrorResponse(ApiStatus.CONFLICT, `无法删除此配置，因为有${filesCount.count}个文件正在使用它`), ApiStatus.CONFLICT);
    }
    // 执行删除操作
    await db.prepare(`DELETE FROM ${DbTables.S3_CONFIGS} WHERE id = ?`).bind(id).run();

    return c.json({
      code: ApiStatus.SUCCESS,
      message: "S3配置删除成功",
      success: true, // 添加兼容字段
    });
  } catch (error) {
    console.error("删除S3配置错误:", error);
    return c.json(createErrorResponse(ApiStatus.INTERNAL_ERROR, error.message || "删除S3配置失败"), ApiStatus.INTERNAL_ERROR);
  }
});

// 设置默认S3配置（管理员权限）
app.put("/api/s3-configs/:id/set-default", authMiddleware, async (c) => {
  const db = c.env.DB;
  const adminId = c.get("adminId");
  const { id } = c.req.param();

  try {
    // 查询配置是否存在
    const config = await db.prepare(`SELECT id FROM ${DbTables.S3_CONFIGS} WHERE id = ? AND admin_id = ?`).bind(id, adminId).first();

    if (!config) {
      return c.json(createErrorResponse(ApiStatus.NOT_FOUND, "S3配置不存在"), ApiStatus.NOT_FOUND);
    }

    // 使用D1的batch API来执行原子事务操作，替代SQL事务
    await db.batch([
      // 1. 首先将所有配置设置为非默认
      db
          .prepare(
              `UPDATE ${DbTables.S3_CONFIGS}
         SET is_default = 0, updated_at = CURRENT_TIMESTAMP
         WHERE admin_id = ?`
          )
          .bind(adminId),

      // 2. 然后将当前配置设置为默认
      db
          .prepare(
              `UPDATE ${DbTables.S3_CONFIGS}
         SET is_default = 1, updated_at = CURRENT_TIMESTAMP
         WHERE id = ?`
          )
          .bind(id),
    ]);

    return c.json({
      code: ApiStatus.SUCCESS,
      message: "默认S3配置设置成功",
      success: true,
    });
  } catch (error) {
    console.error("设置默认S3配置错误:", error);
    return c.json(createErrorResponse(ApiStatus.INTERNAL_ERROR, error.message || "设置默认S3配置失败"), ApiStatus.INTERNAL_ERROR);
  }
});

// 测试S3配置连接（管理员权限）
app.post("/api/s3-configs/:id/test", authMiddleware, async (c) => {
  const db = c.env.DB;
  const adminId = c.get("adminId");
  const { id } = c.req.param();

  try {
    // 获取S3配置
    const config = await db
        .prepare(
            `
        SELECT * FROM ${DbTables.S3_CONFIGS}
        WHERE id = ? AND admin_id = ?
      `
        )
        .bind(id, adminId)
        .first();

    if (!config) {
      return c.json(createErrorResponse(ApiStatus.NOT_FOUND, "S3配置不存在"), ApiStatus.NOT_FOUND);
    }

    // 创建S3客户端测试连接
    const encryptionSecret = c.env.ENCRYPTION_SECRET || "default-encryption-key";
    const s3Client = await createS3Client(config, encryptionSecret);

    // 测试结果对象
    const testResult = {
      read: { success: false, error: null, note: "后端直接测试，不代表前端访问" }, // 添加说明
      write: { success: false, error: null, note: "后端直接测试，不代表前端上传" }, // 添加说明
      cors: { success: false, error: null, note: "模拟真实前端跨域请求，能代表实际使用情况" }, // 添加说明
      connectionInfo: {
        bucket: config.bucket_name,
        endpoint: config.endpoint_url || "默认",
        region: config.region || "默认",
        pathStyle: config.path_style ? "是" : "否",
        provider: config.provider_type,
        directory: config.directory || "",
      },
    };

    // 测试阶段1: 读取权限测试
    try {
      const command = new ListObjectsV2Command({
        Bucket: config.bucket_name,
        MaxKeys: 10,
        Prefix: config.directory ? `${config.directory}/` : "",
      });

      // 发送标准命令对象
      const response = await s3Client.send(command);
      testResult.read.success = true;
      testResult.read.objectCount = response.Contents?.length || 0;
      testResult.read.prefix = config.directory ? `${config.directory}/` : "(根目录)";
      testResult.read.note = "此测试通过后端SDK直接访问S3，成功不代表前端可访问";

      // 更详细的信息
      if (response.Contents && response.Contents.length > 0) {
        testResult.read.firstObjects = response.Contents.slice(0, 3).map((obj) => ({
          key: obj.Key,
          size: formatFileSize(obj.Size),
          lastModified: new Date(obj.LastModified).toISOString(),
        }));
      }
    } catch (error) {
      testResult.read.success = false;
      testResult.read.error = error.message;
      testResult.read.code = error.Code || error.code;
    }

    // 测试阶段2: 写入权限测试 (仅创建一个小测试文件)
    try {
      const timestamp = Date.now();
      const testKey = `${config.directory ? config.directory + "/" : ""}__test_${timestamp}.txt`;

      // 创建测试文件内容
      const testContent = "CloudPaste S3连接测试文件";

      // 针对不同的存储提供商采用不同的上传策略
      if (config.provider_type === S3ProviderTypes.B2) {
        // B2特殊处理 - 由于头部兼容性问题，改为标记为只读测试成功
        console.log("B2存储服务跳过直接写入测试，仅测试读取权限");

        // 将B2标记为测试成功，但添加说明
        testResult.write.success = true;
        testResult.write.uploadTime = 0;
        testResult.write.testFile = "(B2存储服务不进行测试写入)";
        testResult.write.note = "由于B2 S3兼容层的特性，跳过测试写入。实际上传功能正常工作。";
      } else {
        // 其他S3服务使用标准AWS SDK
        const putCommand = new PutObjectCommand({
          Bucket: config.bucket_name,
          Key: testKey,
          Body: testContent,
          ContentType: "text/plain",
          Metadata: {
            "test-purpose": "cloudpaste-s3-test",
            "test-timestamp": `${timestamp}`,
          },
        });

        // 尝试上传一个测试文件
        const uploadStartTime = performance.now();
        const putResponse = await s3Client.send(putCommand);
        const uploadEndTime = performance.now();

        testResult.write.success = true;
        testResult.write.uploadTime = Math.round(uploadEndTime - uploadStartTime);
        testResult.write.testFile = testKey;
        testResult.write.note = "此测试通过后端SDK直接上传，成功不代表前端可上传";

        // 上传成功后尝试删除测试文件 (但不影响测试结果)
        try {
          const deleteCommand = new DeleteObjectCommand({
            Bucket: config.bucket_name,
            Key: testKey,
          });
          await s3Client.send(deleteCommand);
          testResult.write.cleaned = true;
        } catch (cleanupError) {
          testResult.write.cleaned = false;
          testResult.write.cleanupError = cleanupError.message;
        }
      }
    } catch (error) {
      testResult.write.success = false;
      testResult.write.error = error.message;
      testResult.write.code = error.Code || error.code;
    }

    // 测试阶段3: 跨域CORS配置测试
    try {
      const timestamp = Date.now();
      const testKey = `${config.directory ? config.directory + "/" : ""}__cors_test_${timestamp}.txt`;
      const testContent = "CloudPaste CORS测试文件";

      // 生成预签名URL用于跨域测试
      const putCommand = new PutObjectCommand({
        Bucket: config.bucket_name,
        Key: testKey,
        ContentType: "text/plain",
      });

      // 获取预签名URL
      const presignedUrl = await getSignedUrl(s3Client, putCommand, { expiresIn: 300 });

      // 模拟前端请求 - 首先发送预检OPTIONS请求
      // 获取域名用于Origin头
      const requestOrigin = c.req.header("origin");

      try {
        // 创建fetch请求测试服务端预检响应
        const optionsResponse = await fetch(presignedUrl, {
          method: "OPTIONS",
          headers: {
            Origin: requestOrigin,
            "Access-Control-Request-Method": "PUT",
            "Access-Control-Request-Headers": "content-type,x-amz-content-sha256,x-amz-date,authorization",
          },
        });

        // 检查预检响应头
        const allowOrigin = optionsResponse.headers.get("access-control-allow-origin");
        const allowMethods = optionsResponse.headers.get("access-control-allow-methods");
        const allowHeaders = optionsResponse.headers.get("access-control-allow-headers");

        if (allowOrigin) {
          testResult.cors.success = true;
          testResult.cors.allowOrigin = allowOrigin;
          testResult.cors.allowMethods = allowMethods;
          testResult.cors.allowHeaders = allowHeaders;
          testResult.cors.note = "此测试模拟真实前端跨域请求，是判断S3配置是否支持前端直传的关键指标";

          // 实际尝试上传(可选,因为预检通过就基本可以确认CORS配置正确)
          try {
            const putResponse = await fetch(presignedUrl, {
              method: "PUT",
              headers: {
                Origin: requestOrigin,
                "Content-Type": "text/plain",
              },
              body: testContent,
            });

            if (putResponse.ok) {
              testResult.cors.upload = true;

              // 清理测试文件
              const deleteCommand = new DeleteObjectCommand({
                Bucket: config.bucket_name,
                Key: testKey,
              });
              await s3Client.send(deleteCommand);
            } else {
              testResult.cors.upload = false;
              testResult.cors.uploadError = `状态码: ${putResponse.status}`;
            }
          } catch (uploadError) {
            testResult.cors.upload = false;
            testResult.cors.uploadError = uploadError.message;
          }
        } else {
          testResult.cors.success = false;
          testResult.cors.error = "预检请求未返回Access-Control-Allow-Origin头，可能没有正确配置CORS";
          testResult.cors.statusCode = optionsResponse.status;
        }
      } catch (corsError) {
        testResult.cors.success = false;
        testResult.cors.error = corsError.message;
      }
    } catch (presignError) {
      testResult.cors.success = false;
      testResult.cors.error = "无法生成预签名URL: " + presignError.message;
    }

    // 更新最后使用时间
    await db
        .prepare(
            `
        UPDATE ${DbTables.S3_CONFIGS}
        SET last_used = ?
        WHERE id = ?
      `
        )
        .bind(getLocalTimeString(), id)
        .run();

    // 生成友好的测试结果消息
    let message = "S3配置测试";
    // 至少读取权限测试成功就算基本连接成功
    let overallSuccess = testResult.read.success;

    if (testResult.read.success && testResult.write.success) {
      if (testResult.cors.success) {
        message += "成功 (读写权限均可用，跨域配置正确)";
      } else {
        message += "部分成功 (读写权限均可用，但跨域配置有问题)";
      }
    } else if (testResult.read.success) {
      message += "部分成功 (仅读权限可用)";
    } else {
      message += "失败 (读取权限不可用)";
      overallSuccess = false;
    }

    // 测试结果的全局提示说明
    testResult.globalNote = "读写测试仅验证基本连接和权限，通过后端直接测试；CORS测试模拟前端直传，才是判断前端能否直接上传的关键指标";

    return c.json({
      code: ApiStatus.SUCCESS,
      message,
      data: {
        success: overallSuccess,
        result: testResult,
      },
      success: true, // 添加兼容字段
    });
  } catch (error) {
    console.error("测试S3配置错误:", error);
    return c.json(
        {
          code: ApiStatus.INTERNAL_ERROR,
          message: error.message || "测试S3配置失败",
          data: {
            success: false,
            result: {
              error: error.message,
              stack: process.env.NODE_ENV === "development" ? error.stack : null,
            },
          },
          success: false,
        },
        ApiStatus.INTERNAL_ERROR
    );
  }
});

// =================================
// ========== 文件后缀slug 分享 API ===============================================================================================================================
// =================================

/**
 * 根据Slug获取文件信息
 * @param {D1Database} db - D1数据库实例
 * @param {string} slug - 文件slug
 * @param {boolean} includePassword - 是否包含密码字段
 * @returns {Promise<Object|null>} 文件信息或null
 */
async function getFileBySlug(db, slug, includePassword = true) {
  const fields = includePassword
      ? "f.id, f.filename, f.storage_path, f.s3_url, f.mimetype, f.size, f.remark, f.password, f.max_views, f.views, f.expires_at, f.created_at, f.s3_config_id, f.created_by, f.use_proxy"
      : "f.id, f.filename, f.storage_path, f.s3_url, f.mimetype, f.size, f.remark, f.max_views, f.views, f.expires_at, f.created_at, f.s3_config_id, f.created_by, f.use_proxy";

  return await db
      .prepare(
          `
      SELECT ${fields}
      FROM ${DbTables.FILES} f
      WHERE f.slug = ?
    `
      )
      .bind(slug)
      .first();
}

// 获取公开文件（无需认证）
app.get("/api/public/files/:slug", async (c) => {
  const db = c.env.DB;
  const { slug } = c.req.param();
  const encryptionSecret = c.env.ENCRYPTION_SECRET || "default-encryption-key";

  try {
    // 查询文件详情
    const file = await getFileBySlug(db, slug);

    // 检查文件是否可访问
    const accessCheck = await isFileAccessible(db, file, encryptionSecret);
    if (!accessCheck.accessible) {
      if (accessCheck.reason === "expired") {
        return c.json(createErrorResponse(ApiStatus.GONE, "文件已过期"), ApiStatus.GONE);
      }
      return c.json(createErrorResponse(ApiStatus.NOT_FOUND, "文件不存在"), ApiStatus.NOT_FOUND);
    }

    // 检查是否需要密码
    const requiresPassword = !!file.password;

    // 如果不需要密码，立即增加访问次数并检查是否超过限制
    if (!requiresPassword) {
      // 增加访问次数并检查限制
      const result = await incrementAndCheckFileViews(db, file, encryptionSecret);

      // 如果文件已过期，返回相应的错误
      if (result.isExpired) {
        return c.json(createErrorResponse(ApiStatus.GONE, "文件已达到最大查看次数"), ApiStatus.GONE);
      }

      // 生成文件下载URL
      const urlsObj = await generateFileDownloadUrl(db, result.file, encryptionSecret, c.req.raw);

      // 构建公开信息
      const publicInfo = getPublicFileInfo(result.file, requiresPassword, urlsObj);

      return c.json({
        code: ApiStatus.SUCCESS,
        message: "获取文件成功",
        data: publicInfo,
        success: true,
      });
    } else {
      // 文件需要密码验证，只返回基本信息
      const publicInfo = getPublicFileInfo(file, true);

      return c.json({
        code: ApiStatus.SUCCESS,
        message: "获取文件成功",
        data: publicInfo,
        success: true,
      });
    }
  } catch (error) {
    console.error("获取公开文件错误:", error);
    return c.json(createErrorResponse(ApiStatus.INTERNAL_ERROR, error.message || "获取文件失败"), ApiStatus.INTERNAL_ERROR);
  }
});

// 验证文件密码
app.post("/api/public/files/:slug/verify", async (c) => {
  const db = c.env.DB;
  const { slug } = c.req.param();
  const body = await c.req.json();
  const encryptionSecret = c.env.ENCRYPTION_SECRET || "default-encryption-key";

  if (!body.password) {
    return c.json(createErrorResponse(ApiStatus.BAD_REQUEST, "密码是必需的"), ApiStatus.BAD_REQUEST);
  }

  try {
    // 查询文件详情
    const file = await getFileBySlug(db, slug);

    // 检查文件是否可访问
    const accessCheck = await isFileAccessible(db, file, encryptionSecret);
    if (!accessCheck.accessible) {
      if (accessCheck.reason === "expired") {
        return c.json(createErrorResponse(ApiStatus.GONE, "文件已过期"), ApiStatus.GONE);
      }
      return c.json(createErrorResponse(ApiStatus.NOT_FOUND, "文件不存在"), ApiStatus.NOT_FOUND);
    }

    // 验证密码
    if (!file.password) {
      return c.json({
        code: ApiStatus.BAD_REQUEST,
        message: "此文件不需要密码",
        data: {
          url: file.s3_url,
        },
        success: true,
      });
    }

    const passwordValid = await verifyPassword(body.password, file.password);

    if (!passwordValid) {
      return c.json(createErrorResponse(ApiStatus.UNAUTHORIZED, "密码不正确"), ApiStatus.UNAUTHORIZED);
    }

    // 密码验证成功，增加查看次数并检查限制
    const result = await incrementAndCheckFileViews(db, file, encryptionSecret);

    // 如果文件已过期，返回相应的错误
    if (result.isExpired) {
      return c.json(createErrorResponse(ApiStatus.GONE, "文件已达到最大查看次数"), ApiStatus.GONE);
    }

    // 生成文件下载URL
    const urlsObj = await generateFileDownloadUrl(db, result.file, encryptionSecret, c.req.raw);

    // 使用getPublicFileInfo函数构建完整的响应，包括代理链接
    const publicInfo = getPublicFileInfo(result.file, false, urlsObj);

    return c.json({
      code: ApiStatus.SUCCESS,
      message: "密码验证成功",
      data: publicInfo,
      success: true,
    });
  } catch (error) {
    console.error("验证文件密码错误:", error);
    return c.json(createErrorResponse(ApiStatus.INTERNAL_ERROR, error.message || "验证密码失败"), ApiStatus.INTERNAL_ERROR);
  }
});
// =================================
// ========== API密钥用户 文件 API ==========
// =================================

/**
 * API密钥用户的文件API
 * 负责API密钥用户的文件查询和管理功能
 */

// API密钥用户获取自己的文件列表
app.get("/api/user/files", apiKeyFileMiddleware, async (c) => {
  const db = c.env.DB;
  const apiKeyId = c.get("apiKeyId");
  const apiKeyInfo = c.get("apiKeyInfo");

  try {
    // 查询用户文件（支持分页）
    const limit = parseInt(c.req.query("limit") || "30");
    const offset = parseInt(c.req.query("offset") || "0");

    // 获取用户文件列表
    const files = await db
        .prepare(
            `
        SELECT 
          f.id, f.filename, f.slug, f.storage_path, f.s3_url, 
          f.mimetype, f.size, f.remark, f.created_at, f.views,
          f.max_views, f.expires_at, f.etag, f.password IS NOT NULL as has_password,
          f.created_by, f.use_proxy,
          s.name as s3_config_name,
          s.provider_type as s3_provider_type,
          s.id as s3_config_id
        FROM ${DbTables.FILES} f
        LEFT JOIN ${DbTables.S3_CONFIGS} s ON f.s3_config_id = s.id
        WHERE f.created_by = ?
        ORDER BY f.created_at DESC
        LIMIT ? OFFSET ?
      `
        )
        .bind(`apikey:${apiKeyId}`, limit, offset)
        .all();

    // 获取总数
    const countResult = await db.prepare(`SELECT COUNT(*) as total FROM ${DbTables.FILES} WHERE created_by = ?`).bind(`apikey:${apiKeyId}`).first();

    const total = countResult ? countResult.total : 0;

    // 处理文件信息，包括密码
    let processedFiles = await Promise.all(
        files.results.map(async (file) => {
          const result = { ...file };

          // 确保has_password是布尔类型
          result.has_password = !!result.has_password;

          // 如果文件有密码保护，获取明文密码
          if (result.has_password) {
            const passwordEntry = await db.prepare(`SELECT plain_password FROM ${DbTables.FILE_PASSWORDS} WHERE file_id = ?`).bind(result.id).first();

            if (passwordEntry && passwordEntry.plain_password) {
              result.plain_password = passwordEntry.plain_password;
            }
          }

          return result;
        })
    );

    // 为API密钥创建者添加密钥名称
    let keyNamesMap = new Map();

    // 收集所有需要查询名称的密钥ID
    const apiKeyIds = processedFiles.filter((file) => file.created_by && file.created_by.startsWith("apikey:")).map((file) => file.created_by.substring(7));

    // 如果有需要查询名称的密钥
    if (apiKeyIds.length > 0) {
      // 使用Set去重
      const uniqueKeyIds = [...new Set(apiKeyIds)];

      // 为每个唯一的密钥ID查询名称
      for (const keyId of uniqueKeyIds) {
        const keyInfo = await db.prepare(`SELECT id, name FROM ${DbTables.API_KEYS} WHERE id = ?`).bind(keyId).first();

        if (keyInfo) {
          keyNamesMap.set(keyId, keyInfo.name);
        }
      }

      // 为每个结果添加key_name字段
      processedFiles = processedFiles.map((file) => {
        if (file.created_by && file.created_by.startsWith("apikey:")) {
          const keyId = file.created_by.substring(7);
          const keyName = keyNamesMap.get(keyId);
          if (keyName) {
            return { ...file, key_name: keyName };
          }
        }
        return file;
      });
    }

    return c.json({
      code: ApiStatus.SUCCESS,
      message: "获取文件列表成功",
      data: {
        files: processedFiles,
        pagination: {
          total,
          limit,
          offset,
          hasMore: offset + limit < total,
        },
      },
      key_info: apiKeyInfo, // 返回API密钥信息
      success: true,
    });
  } catch (error) {
    console.error("获取API密钥用户文件列表错误:", error);
    return c.json(createErrorResponse(ApiStatus.INTERNAL_ERROR, error.message || "获取文件列表失败"), ApiStatus.INTERNAL_ERROR);
  }
});

// API密钥用户获取单个文件详情
app.get("/api/user/files/:id", apiKeyFileMiddleware, async (c) => {
  const db = c.env.DB;
  const apiKeyId = c.get("apiKeyId");
  const { id } = c.req.param();
  const encryptionSecret = c.env.ENCRYPTION_SECRET || "default-encryption-key";

  try {
    // 查询文件详情
    const file = await db
        .prepare(
            `
        SELECT 
          f.id, f.filename, f.slug, f.storage_path, f.s3_url, 
          f.mimetype, f.size, f.remark, f.created_at, f.views,
          f.max_views, f.expires_at, f.etag, f.password IS NOT NULL as has_password,
          f.created_by, f.use_proxy,
          s.name as s3_config_name,
          s.provider_type as s3_provider_type,
          s.id as s3_config_id
        FROM ${DbTables.FILES} f
        LEFT JOIN ${DbTables.S3_CONFIGS} s ON f.s3_config_id = s.id
        WHERE f.id = ? AND f.created_by = ?
      `
        )
        .bind(id, `apikey:${apiKeyId}`)
        .first();

    if (!file) {
      return c.json(createErrorResponse(ApiStatus.NOT_FOUND, "文件不存在或无权访问"), ApiStatus.NOT_FOUND);
    }

    // 检查用户是否有权限查看该文件
    if (file.created_by !== `apikey:${apiKeyId}`) {
      return c.json(createErrorResponse(ApiStatus.FORBIDDEN, "没有权限查看此文件"), ApiStatus.FORBIDDEN);
    }

    // 生成文件下载URL
    const urlsObj = await generateFileDownloadUrl(db, file, encryptionSecret, c.req.raw);

    // 构建响应
    const result = {
      ...file,
      urls: urlsObj,
    };

    // 如果文件有密码保护，获取明文密码
    if (file.has_password) {
      const passwordEntry = await db.prepare(`SELECT plain_password FROM ${DbTables.FILE_PASSWORDS} WHERE file_id = ?`).bind(file.id).first();

      if (passwordEntry && passwordEntry.plain_password) {
        result.plain_password = passwordEntry.plain_password;
      }
    }

    return c.json({
      code: ApiStatus.SUCCESS,
      message: "获取文件成功",
      data: result,
      success: true,
    });
  } catch (error) {
    console.error("获取文件错误:", error);
    return c.json(createErrorResponse(ApiStatus.INTERNAL_ERROR, error.message || "获取文件失败"), ApiStatus.INTERNAL_ERROR);
  }
});

// API密钥用户删除自己的文件
app.delete("/api/user/files/:id", apiKeyFileMiddleware, async (c) => {
  const db = c.env.DB;
  const apiKeyId = c.get("apiKeyId");
  const { id } = c.req.param();

  try {
    // 查询文件详情，确保是API密钥用户自己的文件
    const file = await db
        .prepare(
            `
        SELECT f.*, s.*
        FROM ${DbTables.FILES} f
        LEFT JOIN ${DbTables.S3_CONFIGS} s ON f.s3_config_id = s.id
        WHERE f.id = ? AND f.created_by = ?
      `
        )
        .bind(id, `apikey:${apiKeyId}`)
        .first();

    if (!file) {
      return c.json(createErrorResponse(ApiStatus.NOT_FOUND, "文件不存在或无权删除"), ApiStatus.NOT_FOUND);
    }

    // 尝试从S3中删除文件
    try {
      const encryptionSecret = c.env.ENCRYPTION_SECRET || "default-encryption-key";
      if (file.storage_path && file.bucket_name) {
        const s3Config = {
          id: file.id,
          endpoint_url: file.endpoint_url,
          bucket_name: file.bucket_name,
          region: file.region,
          access_key_id: file.access_key_id,
          secret_access_key: file.secret_access_key,
          path_style: file.path_style,
        };
        await deleteFileFromS3(s3Config, file.storage_path, encryptionSecret);
      }
    } catch (s3Error) {
      console.error("从S3删除文件错误:", s3Error);
      // 即使S3删除失败，也继续从数据库中删除记录
    }

    // 从数据库中删除记录
    await db.prepare(`DELETE FROM ${DbTables.FILES} WHERE id = ?`).bind(id).run();

    return c.json({
      code: ApiStatus.SUCCESS,
      message: "文件删除成功",
      success: true,
    });
  } catch (error) {
    console.error("删除API密钥用户文件错误:", error);
    return c.json(createErrorResponse(ApiStatus.INTERNAL_ERROR, error.message || "删除文件失败"), ApiStatus.INTERNAL_ERROR);
  }
});

// API密钥用户更新自己文件的元数据
app.put("/api/user/files/:id", apiKeyFileMiddleware, async (c) => {
  const db = c.env.DB;
  const apiKeyId = c.get("apiKeyId");
  const { id } = c.req.param();
  const body = await c.req.json();

  try {
    // 检查文件是否存在且属于当前API密钥用户
    const existingFile = await db.prepare(`SELECT id FROM ${DbTables.FILES} WHERE id = ? AND created_by = ?`).bind(id, `apikey:${apiKeyId}`).first();

    if (!existingFile) {
      return c.json(createErrorResponse(ApiStatus.NOT_FOUND, "文件不存在或无权更新"), ApiStatus.NOT_FOUND);
    }

    // 构建更新字段和参数
    const updateFields = [];
    const bindParams = [];

    // 处理可更新的字段
    if (body.remark !== undefined) {
      updateFields.push("remark = ?");
      bindParams.push(body.remark);
    }

    if (body.slug !== undefined) {
      // 检查slug是否可用 (不与其他文件冲突)
      const slugExistsCheck = await db.prepare(`SELECT id FROM ${DbTables.FILES} WHERE slug = ? AND id != ?`).bind(body.slug, id).first();

      if (slugExistsCheck) {
        return c.json(createErrorResponse(ApiStatus.CONFLICT, "此链接后缀已被其他文件使用"), ApiStatus.CONFLICT);
      }

      updateFields.push("slug = ?");
      bindParams.push(body.slug);
    }

    // 处理过期时间
    if (body.expires_at !== undefined) {
      updateFields.push("expires_at = ?");
      bindParams.push(body.expires_at);
    }

    // 处理Worker代理访问设置
    if (body.use_proxy !== undefined) {
      updateFields.push("use_proxy = ?");
      bindParams.push(body.use_proxy ? 1 : 0);
    }

    // 处理最大查看次数
    if (body.max_views !== undefined) {
      updateFields.push("max_views = ?");
      bindParams.push(body.max_views);

      // 当修改max_views时，重置views计数为0
      updateFields.push("views = 0");
    }

    // 处理密码变更
    if (body.password !== undefined) {
      if (body.password) {
        // 设置新密码
        const passwordHash = await hashPassword(body.password);
        updateFields.push("password = ?");
        bindParams.push(passwordHash);

        // 更新或插入明文密码到FILE_PASSWORDS表
        const plainPasswordExists = await db.prepare(`SELECT file_id FROM ${DbTables.FILE_PASSWORDS} WHERE file_id = ?`).bind(id).first();

        if (plainPasswordExists) {
          // 更新现有的密码记录
          await db.prepare(`UPDATE ${DbTables.FILE_PASSWORDS} SET plain_password = ?, updated_at = ? WHERE file_id = ?`).bind(body.password, new Date().toISOString(), id).run();
        } else {
          // 插入新的密码记录
          await db
              .prepare(`INSERT INTO ${DbTables.FILE_PASSWORDS} (file_id, plain_password, created_at, updated_at) VALUES (?, ?, ?, ?)`)
              .bind(id, body.password, new Date().toISOString(), new Date().toISOString())
              .run();
        }
      } else {
        // 明确提供了空密码，表示要清除密码
        updateFields.push("password = NULL");

        // 删除明文密码记录
        await db.prepare(`DELETE FROM ${DbTables.FILE_PASSWORDS} WHERE file_id = ?`).bind(id).run();
      }
    }
    // 注意：如果body.password未定义，则表示不修改密码，保持原密码不变

    // 添加更新时间
    updateFields.push("updated_at = ?");
    bindParams.push(new Date().toISOString());

    // 添加查询条件：文件ID和创建者
    bindParams.push(id);
    bindParams.push(`apikey:${apiKeyId}`);

    // 如果没有要更新的字段
    if (updateFields.length === 0) {
      return c.json(createErrorResponse(ApiStatus.BAD_REQUEST, "没有提供有效的更新字段"), ApiStatus.BAD_REQUEST);
    }

    // 执行更新
    await db
        .prepare(
            `
        UPDATE ${DbTables.FILES}
        SET ${updateFields.join(", ")}
        WHERE id = ? AND created_by = ?
      `
        )
        .bind(...bindParams)
        .run();

    return c.json({
      code: ApiStatus.SUCCESS,
      message: "文件元数据更新成功",
      success: true,
    });
  } catch (error) {
    console.error("更新API密钥用户文件元数据错误:", error);
    return c.json(createErrorResponse(ApiStatus.INTERNAL_ERROR, error.message || "更新文件元数据失败"), ApiStatus.INTERNAL_ERROR);
  }
});

// =================================
// ========== 管理员文件 API ===============================================================================================================================
// =================================

/**
 * 管理员文件API
 * 负责管理员的文件查询和管理功能
 */

// 获取文件列表（仅管理员权限）
app.get("/api/admin/files", authMiddleware, async (c) => {
  const db = c.env.DB;
  const adminId = c.get("adminId");

  try {
    // 查询所有文件（可选带分页）
    const limit = parseInt(c.req.query("limit") || "30");
    const offset = parseInt(c.req.query("offset") || "0");
    const createdBy = c.req.query("created_by");
    const s3ConfigId = c.req.query("s3_config_id");

    // 构建查询条件
    let whereClauses = [];
    let queryParams = [];

    if (createdBy) {
      whereClauses.push("created_by = ?");
      queryParams.push(createdBy);
    }

    if (s3ConfigId) {
      whereClauses.push("s3_config_id = ?");
      queryParams.push(s3ConfigId);
    }

    const whereClause = whereClauses.length > 0 ? `WHERE ${whereClauses.join(" AND ")}` : "";

    // 查询文件列表
    const filesQuery = `
      SELECT
        f.id, f.filename, f.size, f.slug, f.remark, 
        f.password IS NOT NULL as has_password, f.views, 
        f.max_views, f.expires_at, f.created_by, f.created_at,
        f.etag, f.mimetype, f.s3_config_id, f.storage_path,
        f.use_proxy, c.name as s3_config_name, c.provider_type as s3_provider_type
      FROM ${DbTables.FILES} f
      LEFT JOIN ${DbTables.S3_CONFIGS} c ON f.s3_config_id = c.id
      ${whereClause}
      ORDER BY f.created_at DESC
      LIMIT ? OFFSET ?
    `;

    // 将limit和offset添加到查询参数
    queryParams.push(limit, offset);

    const files = await db
        .prepare(filesQuery)
        .bind(...queryParams)
        .all();

    // 查询总数
    const countQuery = `
      SELECT COUNT(*) as total 
      FROM ${DbTables.FILES}
      ${whereClause}
    `;

    const countResult = await db
        .prepare(countQuery)
        .bind(...queryParams.slice(0, -2))
        .first();
    const total = countResult.total;

    // 处理查询结果，为API密钥创建者添加密钥名称
    let results = files.results;
    let keyNamesMap = new Map();

    // 获取文件密码和添加API密钥名称
    for (const file of results) {
      // 确保has_password是布尔类型
      file.has_password = !!file.has_password;

      // 如果文件有密码保护，获取明文密码
      if (file.has_password) {
        const passwordEntry = await db.prepare(`SELECT plain_password FROM ${DbTables.FILE_PASSWORDS} WHERE file_id = ?`).bind(file.id).first();
        if (passwordEntry && passwordEntry.plain_password) {
          file.plain_password = passwordEntry.plain_password;
        }
      }
    }

    // 收集所有需要查询名称的密钥ID
    const apiKeyIds = results.filter((file) => file.created_by && file.created_by.startsWith("apikey:")).map((file) => file.created_by.substring(7));

    // 如果有需要查询名称的密钥
    if (apiKeyIds.length > 0) {
      // 使用Set去重
      const uniqueKeyIds = [...new Set(apiKeyIds)];

      // 为每个唯一的密钥ID查询名称
      for (const keyId of uniqueKeyIds) {
        const keyInfo = await db.prepare(`SELECT id, name FROM ${DbTables.API_KEYS} WHERE id = ?`).bind(keyId).first();

        if (keyInfo) {
          keyNamesMap.set(keyId, keyInfo.name);
        }
      }

      // 为每个结果添加key_name字段
      results = results.map((file) => {
        if (file.created_by && file.created_by.startsWith("apikey:")) {
          const keyId = file.created_by.substring(7);
          const keyName = keyNamesMap.get(keyId);
          if (keyName) {
            return { ...file, key_name: keyName };
          }
        }
        return file;
      });
    }

    return c.json({
      code: ApiStatus.SUCCESS,
      message: "获取成功",
      data: {
        files: results,
        pagination: {
          total,
          limit,
          offset,
          hasMore: offset + limit < total,
        },
      },
      success: true,
    });
  } catch (error) {
    console.error("获取文件列表错误:", error);
    return c.json(createErrorResponse(ApiStatus.INTERNAL_ERROR, error.message || "获取文件列表失败"), ApiStatus.INTERNAL_ERROR);
  }
});

// 获取单个文件详情（仅管理员权限）
app.get("/api/admin/files/:id", authMiddleware, async (c) => {
  const db = c.env.DB;
  const { id } = c.req.param();
  const encryptionSecret = c.env.ENCRYPTION_SECRET || "default-encryption-key";

  try {
    // 查询文件详情
    const file = await db
        .prepare(
            `
        SELECT 
          f.id, f.filename, f.slug, f.storage_path, f.s3_url, 
          f.mimetype, f.size, f.remark, f.created_at, f.updated_at,
          f.views, f.max_views, f.expires_at, f.use_proxy,
          f.etag, f.password IS NOT NULL as has_password,
          f.created_by,
          s.name as s3_config_name,
          s.provider_type as s3_provider_type,
          s.id as s3_config_id
        FROM ${DbTables.FILES} f
        LEFT JOIN ${DbTables.S3_CONFIGS} s ON f.s3_config_id = s.id
        WHERE f.id = ?
      `
        )
        .bind(id)
        .first();

    if (!file) {
      return c.json(createErrorResponse(ApiStatus.NOT_FOUND, "文件不存在"), ApiStatus.NOT_FOUND);
    }

    // 生成文件下载URL
    const urlsObj = await generateFileDownloadUrl(db, file, encryptionSecret, c.req.raw);

    // 构建响应
    const result = {
      ...file,
      urls: urlsObj,
    };

    // 如果文件有密码保护，获取明文密码
    if (file.has_password) {
      const passwordEntry = await db.prepare(`SELECT plain_password FROM ${DbTables.FILE_PASSWORDS} WHERE file_id = ?`).bind(file.id).first();

      if (passwordEntry && passwordEntry.plain_password) {
        result.plain_password = passwordEntry.plain_password;
      }
    }

    return c.json({
      code: ApiStatus.SUCCESS,
      message: "获取文件成功",
      data: result,
      success: true,
    });
  } catch (error) {
    console.error("获取文件错误:", error);
    return c.json(createErrorResponse(ApiStatus.INTERNAL_ERROR, error.message || "获取文件失败"), ApiStatus.INTERNAL_ERROR);
  }
});

// 删除文件（管理员权限）
app.delete("/api/admin/files/:id", authMiddleware, async (c) => {
  const db = c.env.DB;
  const { id } = c.req.param();

  try {
    // 查询文件详情
    const file = await db
        .prepare(
            `
        SELECT f.*, s.*
        FROM ${DbTables.FILES} f
        LEFT JOIN ${DbTables.S3_CONFIGS} s ON f.s3_config_id = s.id
        WHERE f.id = ?
      `
        )
        .bind(id)
        .first();

    if (!file) {
      return c.json(createErrorResponse(ApiStatus.NOT_FOUND, "文件不存在"), ApiStatus.NOT_FOUND);
    }

    // 尝试从S3中删除文件
    try {
      const encryptionSecret = c.env.ENCRYPTION_SECRET || "default-encryption-key";
      if (file.storage_path && file.bucket_name) {
        const s3Config = {
          id: file.id,
          endpoint_url: file.endpoint_url,
          bucket_name: file.bucket_name,
          region: file.region,
          access_key_id: file.access_key_id,
          secret_access_key: file.secret_access_key,
          path_style: file.path_style,
        };
        await deleteFileFromS3(s3Config, file.storage_path, encryptionSecret);
      }
    } catch (s3Error) {
      console.error("从S3删除文件错误:", s3Error);
      // 即使S3删除失败，也继续从数据库中删除记录
    }

    // 从数据库中删除记录
    await db.prepare(`DELETE FROM ${DbTables.FILES} WHERE id = ?`).bind(id).run();

    return c.json({
      code: ApiStatus.SUCCESS,
      message: "文件删除成功",
      success: true, // 添加兼容字段
    });
  } catch (error) {
    console.error("删除文件错误:", error);
    return c.json(createErrorResponse(ApiStatus.INTERNAL_ERROR, error.message || "删除文件失败"), ApiStatus.INTERNAL_ERROR);
  }
});

// 更新文件元数据（管理员权限）
app.put("/api/admin/files/:id", authMiddleware, async (c) => {
  const db = c.env.DB;
  const { id } = c.req.param();
  const body = await c.req.json();

  try {
    // 检查文件是否存在
    const existingFile = await db.prepare(`SELECT id FROM ${DbTables.FILES} WHERE id = ?`).bind(id).first();

    if (!existingFile) {
      return c.json(createErrorResponse(ApiStatus.NOT_FOUND, "文件不存在"), ApiStatus.NOT_FOUND);
    }

    // 构建更新字段和参数
    const updateFields = [];
    const bindParams = [];

    // 处理可更新的字段
    if (body.remark !== undefined) {
      updateFields.push("remark = ?");
      bindParams.push(body.remark);
    }

    if (body.slug !== undefined) {
      // 检查slug是否可用 (不与其他文件冲突)
      const slugExistsCheck = await db.prepare(`SELECT id FROM ${DbTables.FILES} WHERE slug = ? AND id != ?`).bind(body.slug, id).first();

      if (slugExistsCheck) {
        return c.json(createErrorResponse(ApiStatus.CONFLICT, "此链接后缀已被其他文件使用"), ApiStatus.CONFLICT);
      }

      updateFields.push("slug = ?");
      bindParams.push(body.slug);
    }

    // 处理过期时间
    if (body.expires_at !== undefined) {
      updateFields.push("expires_at = ?");
      bindParams.push(body.expires_at);
    }

    // 处理Worker代理访问设置
    if (body.use_proxy !== undefined) {
      updateFields.push("use_proxy = ?");
      bindParams.push(body.use_proxy ? 1 : 0);
    }

    // 处理最大查看次数
    if (body.max_views !== undefined) {
      updateFields.push("max_views = ?");
      bindParams.push(body.max_views);

      // 当修改max_views时，重置views计数为0
      updateFields.push("views = 0");
    }

    // 处理密码变更
    if (body.password !== undefined) {
      if (body.password) {
        // 设置新密码
        const passwordHash = await hashPassword(body.password);
        updateFields.push("password = ?");
        bindParams.push(passwordHash);

        // 更新或插入明文密码到FILE_PASSWORDS表
        const plainPasswordExists = await db.prepare(`SELECT file_id FROM ${DbTables.FILE_PASSWORDS} WHERE file_id = ?`).bind(id).first();

        if (plainPasswordExists) {
          // 更新现有的密码记录
          await db.prepare(`UPDATE ${DbTables.FILE_PASSWORDS} SET plain_password = ?, updated_at = ? WHERE file_id = ?`).bind(body.password, new Date().toISOString(), id).run();
        } else {
          // 插入新的密码记录
          await db
              .prepare(`INSERT INTO ${DbTables.FILE_PASSWORDS} (file_id, plain_password, created_at, updated_at) VALUES (?, ?, ?, ?)`)
              .bind(id, body.password, new Date().toISOString(), new Date().toISOString())
              .run();
        }
      } else {
        // 明确提供了空密码，表示要清除密码
        updateFields.push("password = NULL");

        // 删除明文密码记录
        await db.prepare(`DELETE FROM ${DbTables.FILE_PASSWORDS} WHERE file_id = ?`).bind(id).run();
      }
    }
    // 注意：如果body.password未定义，则表示不修改密码，保持原密码不变

    // 添加更新时间
    updateFields.push("updated_at = ?");
    bindParams.push(new Date().toISOString());

    // 添加ID作为WHERE条件参数
    bindParams.push(id);

    // 如果没有要更新的字段
    if (updateFields.length === 0) {
      return c.json(createErrorResponse(ApiStatus.BAD_REQUEST, "没有提供有效的更新字段"), ApiStatus.BAD_REQUEST);
    }

    // 执行更新
    await db
        .prepare(
            `
        UPDATE ${DbTables.FILES}
        SET ${updateFields.join(", ")}
        WHERE id = ?
      `
        )
        .bind(...bindParams)
        .run();

    return c.json({
      code: ApiStatus.SUCCESS,
      message: "文件元数据更新成功",
      success: true,
    });
  } catch (error) {
    console.error("更新文件元数据错误:", error);
    return c.json(createErrorResponse(ApiStatus.INTERNAL_ERROR, error.message || "更新文件元数据失败"), ApiStatus.INTERNAL_ERROR);
  }
});

// =================================
// ========== 创建Paste文本 API ===============================================================================================================================
// =================================

/**
 * 文本分享API
 * 负责文本分享的创建、获取和密码验证
 */

// 创建新的文本分享（需要管理员权限）
app.post("/api/paste", async (c) => {
  const db = c.env.DB;
  const body = await c.req.json();

  // 添加管理员权限验证
  const authHeader = c.req.header("Authorization");
  let isAuthorized = false;
  let authorizedBy = "";
  let authorizedId = null;

  // 检查Bearer令牌 (管理员)
  if (authHeader && authHeader.startsWith("Bearer ")) {
    const token = authHeader.substring(7);
    const adminId = await validateAdminToken(c.env.DB, token);

    if (adminId) {
      isAuthorized = true;
      authorizedBy = "admin";
      authorizedId = adminId;
    }
  }
  // 检查API密钥
  else if (authHeader && authHeader.startsWith("ApiKey ")) {
    const apiKey = authHeader.substring(7);

    // 查询数据库中的API密钥记录
    const keyRecord = await db
        .prepare(
            `
      SELECT id, name, text_permission, expires_at
      FROM ${DbTables.API_KEYS}
      WHERE key = ?
    `
        )
        .bind(apiKey)
        .first();

    // 如果密钥存在且有文本权限
    if (keyRecord && keyRecord.text_permission === 1) {
      // 检查是否过期
      if (!(await checkAndDeleteExpiredApiKey(db, keyRecord))) {
        isAuthorized = true;
        authorizedBy = "apikey";
        authorizedId = keyRecord.id;

        // 更新最后使用时间
        await db
            .prepare(
                `
          UPDATE ${DbTables.API_KEYS}
          SET last_used = ?
          WHERE id = ?
        `
            )
            .bind(getLocalTimeString(), keyRecord.id)
            .run();
      }
    }
  }

  // 如果都没有授权，则返回权限错误
  if (!isAuthorized) {
    throw new HTTPException(ApiStatus.FORBIDDEN, { message: "需要管理员权限或有效的API密钥才能创建分享" });
  }

  // 必须提供内容
  if (!body.content) {
    throw new HTTPException(ApiStatus.BAD_REQUEST, { message: "内容不能为空" });
  }

  // 验证可打开次数不能为负数
  if (body.maxViews !== null && body.maxViews !== undefined && parseInt(body.maxViews) < 0) {
    throw new HTTPException(ApiStatus.BAD_REQUEST, { message: "可打开次数不能为负数" });
  }

  try {
    // 生成唯一slug
    const slug = await generateUniqueSlug(db, body.slug);
    const pasteId = crypto.randomUUID();

    // 处理密码 (如果提供)
    let passwordHash = null;
    if (body.password) {
      passwordHash = await hashPassword(body.password);
    }

    // 创建者信息
    const createdBy = authorizedBy === "admin" ? authorizedId : authorizedBy === "apikey" ? `apikey:${authorizedId}` : null;

    // 插入数据库
    await db
        .prepare(
            `
      INSERT INTO ${DbTables.PASTES} (
        id, slug, content, remark, password, 
        expires_at, max_views, created_by, created_at, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
    `
        )
        .bind(pasteId, slug, body.content, body.remark || null, passwordHash, body.expiresAt || null, body.maxViews || null, createdBy)
        .run();

    // 返回创建结果
    return c.json({
      id: pasteId,
      slug,
      remark: body.remark,
      expiresAt: body.expiresAt,
      maxViews: body.maxViews,
      hasPassword: !!passwordHash,
      createdAt: new Date().toISOString(),
      authorizedBy, // 添加授权方式信息，方便调试
    });
  } catch (error) {
    // 处理特定错误
    if (error.message.includes("链接后缀已被占用")) {
      throw new HTTPException(ApiStatus.CONFLICT, { message: error.message });
    }
    // 处理其他错误
    throw new HTTPException(ApiStatus.INTERNAL_ERROR, { message: error.message || "创建分享失败" });
  }
});

// 获取文本分享
app.get("/api/paste/:slug", async (c) => {
  const db = c.env.DB;
  const slug = c.req.param("slug");

  // 查询paste
  const paste = await db
      .prepare(
          `
    SELECT id, slug, content, remark, password IS NOT NULL as has_password,
    expires_at, max_views, views, created_at, updated_at, created_by
    FROM ${DbTables.PASTES} WHERE slug = ?
  `
      )
      .bind(slug)
      .first();

  // 如果不存在则返回404
  if (!paste) {
    throw new HTTPException(ApiStatus.NOT_FOUND, { message: "文本分享不存在或已过期" });
  }

  // 检查是否过期并删除
  if (await checkAndDeleteExpiredPaste(db, paste)) {
    throw new HTTPException(ApiStatus.GONE, { message: "文本分享已过期或超过最大查看次数" });
  }

  // 检查是否需要密码
  if (paste.has_password) {
    return c.json({
      slug: paste.slug,
      hasPassword: true,
      remark: paste.remark,
      expiresAt: paste.expires_at,
      maxViews: paste.max_views,
      views: paste.views,
      createdAt: paste.created_at,
      created_by: paste.created_by,
      requiresPassword: true,
    });
  }

  // 检查是否可访问
  if (!isPasteAccessible(paste)) {
    throw new HTTPException(ApiStatus.GONE, { message: "文本分享已过期或超过最大查看次数" });
  }

  // 增加查看次数
  await incrementPasteViews(db, paste.id, paste.max_views);

  // 返回paste内容
  return c.json({
    slug: paste.slug,
    content: paste.content,
    remark: paste.remark,
    hasPassword: false,
    expiresAt: paste.expires_at,
    maxViews: paste.max_views,
    views: paste.views + 1, // 已增加的查看次数
    createdAt: paste.created_at,
    updatedAt: paste.updated_at,
    created_by: paste.created_by,
  });
});

// 使用密码获取文本分享
app.post("/api/paste/:slug", async (c) => {
  const db = c.env.DB;
  const slug = c.req.param("slug");
  const body = await c.req.json();

  // 查询paste（需要获取密码进行验证）
  const paste = await db
      .prepare(
          `
    SELECT id, slug, content, remark, password,
    expires_at, max_views, views, created_at, updated_at, created_by
    FROM ${DbTables.PASTES} WHERE slug = ?
  `
      )
      .bind(slug)
      .first();

  // 如果不存在则返回404
  if (!paste) {
    throw new HTTPException(ApiStatus.NOT_FOUND, { message: "文本分享不存在或已过期" });
  }

  // 检查是否过期并删除
  if (await checkAndDeleteExpiredPaste(db, paste)) {
    throw new HTTPException(ApiStatus.GONE, { message: "文本分享已过期或超过最大查看次数" });
  }

  // 检查是否可访问
  if (!isPasteAccessible(paste)) {
    throw new HTTPException(ApiStatus.GONE, { message: "文本分享已过期或超过最大查看次数" });
  }

  // 如果需要密码但未提供
  if (paste.password && !body.password) {
    throw new HTTPException(ApiStatus.UNAUTHORIZED, { message: "需要密码访问此文本分享" });
  }

  // 验证密码
  if (paste.password && !(await verifyPassword(body.password, paste.password))) {
    throw new HTTPException(ApiStatus.UNAUTHORIZED, { message: "密码错误" });
  }

  // 增加查看次数
  await incrementPasteViews(db, paste.id, paste.max_views);

  // 返回paste内容
  return c.json({
    slug: paste.slug,
    content: paste.content,
    remark: paste.remark,
    hasPassword: !!paste.password,
    expiresAt: paste.expires_at,
    maxViews: paste.max_views,
    views: paste.views + 1, // 已增加的查看次数
    createdAt: paste.created_at,
    updatedAt: paste.updated_at,
    created_by: paste.created_by,
  });
});

// =================================
// ========== API密钥用户 文本 API ===============================================================================================================================
// =================================

/**
 * User文本API
 * 负责文本查询和管理功能
 */

// API密钥用户获取自己的文本列表
app.get("/api/user/pastes", apiKeyTextMiddleware, async (c) => {
  const db = c.env.DB;
  const apiKeyId = c.get("apiKeyId");
  const apiKeyInfo = c.get("apiKeyInfo");

  try {
    // 查询用户文本（支持分页）
    const limit = parseInt(c.req.query("limit") || "30");
    const offset = parseInt(c.req.query("offset") || "0");

    // 查询文本分享记录
    const pastes = await db
        .prepare(
            `
      SELECT id, slug, content, remark, password IS NOT NULL as has_password,
      expires_at, max_views, views, created_at, updated_at, created_by
      FROM ${DbTables.PASTES}
      WHERE created_by = ?
      ORDER BY created_at DESC
      LIMIT ? OFFSET ?
    `
        )
        .bind(`apikey:${apiKeyId}`, limit, offset)
        .all();

    // 查询总数
    const countResult = await db.prepare(`SELECT COUNT(*) as total FROM ${DbTables.PASTES} WHERE created_by = ?`).bind(`apikey:${apiKeyId}`).first();

    const total = countResult?.total || 0;

    // 如果有created_by字段并且以apikey:开头，查询密钥名称
    let results = pastes.results;
    let keyNamesMap = new Map();

    // 收集所有需要查询名称的密钥ID
    const apiKeyIds = results.filter((paste) => paste.created_by && paste.created_by.startsWith("apikey:")).map((paste) => paste.created_by.substring(7));

    // 如果有需要查询名称的密钥
    if (apiKeyIds.length > 0) {
      // 使用Set去重
      const uniqueKeyIds = [...new Set(apiKeyIds)];

      // 为每个唯一的密钥ID查询名称
      for (const keyId of uniqueKeyIds) {
        const keyInfo = await db.prepare(`SELECT id, name FROM ${DbTables.API_KEYS} WHERE id = ?`).bind(keyId).first();

        if (keyInfo) {
          keyNamesMap.set(keyId, keyInfo.name);
        }
      }

      // 为每个结果添加key_name字段
      results = results.map((paste) => {
        if (paste.created_by && paste.created_by.startsWith("apikey:")) {
          const keyId = paste.created_by.substring(7);
          const keyName = keyNamesMap.get(keyId);
          if (keyName) {
            return { ...paste, key_name: keyName };
          }
        }
        return paste;
      });
    }

    return c.json({
      code: ApiStatus.SUCCESS,
      message: "获取成功",
      data: results,
      pagination: {
        total,
        limit,
        offset,
        hasMore: offset + limit < total,
      },
      key_info: apiKeyInfo, // 返回API密钥信息
      success: true,
    });
  } catch (error) {
    console.error("获取API密钥用户文本列表错误:", error);
    return c.json(createErrorResponse(ApiStatus.INTERNAL_ERROR, error.message || "获取文本列表失败"), ApiStatus.INTERNAL_ERROR);
  }
});

// API密钥用户获取单个文本详情
app.get("/api/user/pastes/:id", apiKeyTextMiddleware, async (c) => {
  const db = c.env.DB;
  const apiKeyId = c.get("apiKeyId");
  const { id } = c.req.param();

  try {
    // 获取用户自己创建的文本
    const paste = await db
        .prepare(
            `
        SELECT 
          id, slug, content, remark,
          password IS NOT NULL as has_password,
          expires_at, max_views, views, created_at, updated_at, created_by
        FROM ${DbTables.PASTES}
        WHERE id = ? AND created_by = ?
      `
        )
        .bind(id, `apikey:${apiKeyId}`)
        .first();

    if (!paste) {
      return c.json(createErrorResponse(ApiStatus.NOT_FOUND, "文本不存在或无权访问"), ApiStatus.NOT_FOUND);
    }

    // 确保has_password是布尔类型
    paste.has_password = !!paste.has_password;

    // 如果文本由API密钥创建，获取密钥名称
    let result = { ...paste };
    if (paste.created_by && paste.created_by.startsWith("apikey:")) {
      const keyId = paste.created_by.substring(7);
      const keyInfo = await db.prepare(`SELECT id, name FROM ${DbTables.API_KEYS} WHERE id = ?`).bind(keyId).first();

      if (keyInfo) {
        result.key_name = keyInfo.name;
      }
    }

    return c.json({
      code: ApiStatus.SUCCESS,
      message: "获取文本详情成功",
      data: result,
      success: true,
    });
  } catch (error) {
    console.error("获取API密钥用户文本详情错误:", error);
    return c.json(createErrorResponse(ApiStatus.INTERNAL_ERROR, error.message || "获取文本详情失败"), ApiStatus.INTERNAL_ERROR);
  }
});

// API密钥用户删除自己的文本
app.delete("/api/user/pastes/:id", apiKeyTextMiddleware, async (c) => {
  const db = c.env.DB;
  const apiKeyId = c.get("apiKeyId");
  const { id } = c.req.param();

  try {
    // 查询是否存在且属于该API密钥用户
    const paste = await db.prepare(`SELECT id FROM ${DbTables.PASTES} WHERE id = ? AND created_by = ?`).bind(id, `apikey:${apiKeyId}`).first();

    if (!paste) {
      return c.json(createErrorResponse(ApiStatus.NOT_FOUND, "文本不存在或无权删除"), ApiStatus.NOT_FOUND);
    }

    // 删除文本
    await db.prepare(`DELETE FROM ${DbTables.PASTES} WHERE id = ?`).bind(id).run();

    return c.json({
      code: ApiStatus.SUCCESS,
      message: "删除文本成功",
      success: true,
    });
  } catch (error) {
    console.error("删除API密钥用户文本错误:", error);
    return c.json(createErrorResponse(ApiStatus.INTERNAL_ERROR, error.message || "删除文本失败"), ApiStatus.INTERNAL_ERROR);
  }
});

// API密钥用户批量删除自己的文本
app.delete("/api/user/pastes", apiKeyTextMiddleware, async (c) => {
  const db = c.env.DB;
  const apiKeyId = c.get("apiKeyId");

  try {
    // 从请求体中获取要删除的ID数组
    const { ids } = await c.req.json();

    // 验证请求数据
    if (!ids || !Array.isArray(ids) || ids.length === 0) {
      return c.json(createErrorResponse(ApiStatus.BAD_REQUEST, "请提供有效的ID数组"), ApiStatus.BAD_REQUEST);
    }

    // 构建参数占位符
    const placeholders = ids.map(() => "?").join(",");

    // 构建完整的参数数组（包含所有ID和创建者标识）
    const bindParams = [...ids, `apikey:${apiKeyId}`];

    // 执行批量删除（只删除属于该API密钥用户的文本）
    const result = await db
        .prepare(`DELETE FROM ${DbTables.PASTES} WHERE id IN (${placeholders}) AND created_by = ?`)
        .bind(...bindParams)
        .run();

    const deletedCount = result.changes || 0;

    return c.json({
      code: ApiStatus.SUCCESS,
      message: `已删除 ${deletedCount} 个分享`,
      success: true,
    });
  } catch (error) {
    console.error("批量删除API密钥用户文本错误:", error);
    return c.json(createErrorResponse(ApiStatus.INTERNAL_ERROR, error.message || "批量删除文本失败"), ApiStatus.INTERNAL_ERROR);
  }
});

// API密钥用户修改自己的文本
app.put("/api/user/pastes/:slug", apiKeyTextMiddleware, async (c) => {
  const db = c.env.DB;
  const slug = c.req.param("slug");
  const apiKeyId = c.get("apiKeyId");
  const body = await c.req.json();

  try {
    // 检查分享是否存在且属于该API密钥用户
    const paste = await db
        .prepare(`SELECT id, slug, expires_at, max_views, views FROM ${DbTables.PASTES} WHERE slug = ? AND created_by = ?`)
        .bind(slug, `apikey:${apiKeyId}`)
        .first();

    if (!paste) {
      return c.json(createErrorResponse(ApiStatus.NOT_FOUND, "文本不存在或无权修改"), ApiStatus.NOT_FOUND);
    }

    // 检查是否过期
    if (await checkAndDeleteExpiredPaste(db, paste)) {
      return c.json(createErrorResponse(ApiStatus.GONE, "文本分享已过期或超过最大查看次数，无法修改"), ApiStatus.GONE);
    }

    // 验证内容
    if (!body.content) {
      return c.json(createErrorResponse(ApiStatus.BAD_REQUEST, "内容不能为空"), ApiStatus.BAD_REQUEST);
    }

    // 验证可打开次数
    if (body.maxViews !== null && body.maxViews !== undefined && parseInt(body.maxViews) < 0) {
      return c.json(createErrorResponse(ApiStatus.BAD_REQUEST, "可打开次数不能为负数"), ApiStatus.BAD_REQUEST);
    }

    // 处理密码更新
    let passwordSql = "";
    const sqlParams = [];

    if (body.password) {
      // 如果提供了新密码，则更新
      const passwordHash = await hashPassword(body.password);
      passwordSql = "password = ?, ";
      sqlParams.push(passwordHash);
    } else if (body.clearPassword) {
      // 如果指定了清除密码
      passwordSql = "password = NULL, ";
    }

    // 处理slug更新
    let newSlug = paste.slug; // 默认保持原slug不变
    let slugSql = "";

    // 如果提供了新的slug，则生成唯一slug
    if (body.newSlug !== undefined) {
      try {
        // 如果newSlug为空或null，则自动生成随机slug
        newSlug = await generateUniqueSlug(db, body.newSlug || null);
        // 设置更新SQL
        slugSql = "slug = ?, ";
        // 将新slug添加到参数列表
        sqlParams.push(newSlug);
      } catch (error) {
        // 如果slug已被占用，返回409冲突错误
        if (error.message.includes("链接后缀已被占用")) {
          throw new HTTPException(ApiStatus.CONFLICT, { message: error.message });
        }
        throw error;
      }
    }

    // 检查是否修改了最大查看次数
    const isMaxViewsChanged = body.maxViews !== null && body.maxViews !== undefined && body.maxViews !== paste.max_views;

    // 构建SQL语句，根据是否修改了max_views决定是否重置views
    const viewsResetSQL = isMaxViewsChanged ? "views = 0, " : "";

    // 添加其他更新参数
    sqlParams.push(body.content, body.remark || null, body.expiresAt || null, body.maxViews || null, paste.id);

    // 执行更新
    await db
        .prepare(
            `UPDATE ${DbTables.PASTES} 
       SET ${passwordSql}
           ${slugSql}
           ${viewsResetSQL}
           content = ?, 
           remark = ?, 
           expires_at = ?, 
           max_views = ?,
           updated_at = CURRENT_TIMESTAMP
       WHERE id = ?`
        )
        .bind(...sqlParams)
        .run();

    return c.json({
      code: ApiStatus.SUCCESS,
      message: "文本分享已更新",
      data: {
        id: paste.id,
        slug: newSlug, // 返回更新后的slug（可能已更改）
      },
    });
  } catch (error) {
    console.error("修改API密钥用户文本错误:", error);
    return c.json(createErrorResponse(ApiStatus.INTERNAL_ERROR, error.message || "修改文本失败"), ApiStatus.INTERNAL_ERROR);
  }
});

// =================================
// ========== 管理员 文本 API ===============================================================================================================================
// ================================

// 获取所有文本分享列表（需要认证）
app.get("/api/admin/pastes", authMiddleware, async (c) => {
  const db = c.env.DB;
  const page = parseInt(c.req.query("page") || "1");
  const limit = parseInt(c.req.query("limit") || "10");
  const createdBy = c.req.query("created_by"); // 可选的创建者筛选

  // 计算偏移量
  const offset = (page - 1) * limit;

  // 构建SQL查询和参数
  let countSql = `SELECT COUNT(*) as total FROM ${DbTables.PASTES}`;
  let querySql = `
    SELECT 
      id, 
      slug, 
      remark, 
      password IS NOT NULL as has_password,
      expires_at, 
      max_views, 
      views as view_count,
      created_by,
      CASE 
        WHEN LENGTH(content) > 200 THEN SUBSTR(content, 1, 200) || '...'
        ELSE content
      END as content_preview,
      created_at, 
      updated_at
    FROM ${DbTables.PASTES}
  `;

  const queryParams = [];

  // 如果指定了创建者，添加过滤条件
  if (createdBy) {
    countSql += ` WHERE created_by = ?`;
    querySql += ` WHERE created_by = ?`;
    queryParams.push(createdBy);
  }

  // 添加排序和分页
  querySql += ` ORDER BY created_at DESC LIMIT ? OFFSET ?`;
  queryParams.push(limit, offset);

  // 获取总数（包括所有内容，根据筛选条件过滤）
  const countParams = createdBy ? [createdBy] : [];
  const countResult = await db
      .prepare(countSql)
      .bind(...countParams)
      .first();
  const total = countResult.total;

  // 查询分页数据，加入内容字段并做截断处理
  const pastes = await db
      .prepare(querySql)
      .bind(...queryParams)
      .all();

  // 处理查询结果，为API密钥创建者添加密钥名称
  let results = pastes.results;
  let keyNamesMap = new Map();

  // 收集所有需要查询名称的密钥ID
  const apiKeyIds = results.filter((paste) => paste.created_by && paste.created_by.startsWith("apikey:")).map((paste) => paste.created_by.substring(7));

  // 如果有需要查询名称的密钥
  if (apiKeyIds.length > 0) {
    // 使用Set去重
    const uniqueKeyIds = [...new Set(apiKeyIds)];

    // 为每个唯一的密钥ID查询名称
    for (const keyId of uniqueKeyIds) {
      const keyInfo = await db.prepare(`SELECT id, name FROM ${DbTables.API_KEYS} WHERE id = ?`).bind(keyId).first();

      if (keyInfo) {
        keyNamesMap.set(keyId, keyInfo.name);
      }
    }

    // 为每个结果添加key_name字段
    results = results.map((paste) => {
      if (paste.created_by && paste.created_by.startsWith("apikey:")) {
        const keyId = paste.created_by.substring(7);
        const keyName = keyNamesMap.get(keyId);
        if (keyName) {
          return { ...paste, key_name: keyName };
        }
      }
      return paste;
    });
  }

  // 返回分页结果
  return c.json({
    code: ApiStatus.SUCCESS,
    message: "获取成功",
    data: results,
    pagination: {
      total,
      page,
      limit,
      totalPages: Math.ceil(total / limit),
    },
  });
});

// 删除文本分享（需要认证）
app.delete("/api/admin/pastes/:id", authMiddleware, async (c) => {
  const db = c.env.DB;
  const id = c.req.param("id");

  // 检查分享是否存在
  const paste = await db.prepare(`SELECT id FROM ${DbTables.PASTES} WHERE id = ?`).bind(id).first();

  if (!paste) {
    throw new HTTPException(ApiStatus.NOT_FOUND, { message: "文本分享不存在" });
  }

  // 删除分享
  await db.prepare(`DELETE FROM ${DbTables.PASTES} WHERE id = ?`).bind(id).run();

  return c.json({
    code: ApiStatus.SUCCESS,
    message: "分享已删除",
  });
});

// 批量删除文本分享（需要认证）
app.delete("/api/admin/pastes", authMiddleware, async (c) => {
  const db = c.env.DB;

  // 从请求体中获取要删除的ID数组
  const { ids, clearExpired } = await c.req.json();

  let deletedCount = 0;

  // 如果指定了清理过期内容
  if (clearExpired) {
    const now = new Date().toISOString();
    const result = await db.prepare(`DELETE FROM ${DbTables.PASTES} WHERE expires_at IS NOT NULL AND expires_at < ?`).bind(now).run();

    deletedCount = result.changes || 0;

    return c.json({
      code: ApiStatus.SUCCESS,
      message: `已清理 ${deletedCount} 个过期分享`,
    });
  }

  // 否则按照指定的ID删除
  if (!ids || !Array.isArray(ids) || ids.length === 0) {
    throw new HTTPException(ApiStatus.BAD_REQUEST, { message: "请提供有效的ID数组" });
  }

  // 构建参数占位符
  const placeholders = ids.map(() => "?").join(",");

  // 执行批量删除
  const result = await db
      .prepare(`DELETE FROM ${DbTables.PASTES} WHERE id IN (${placeholders})`)
      .bind(...ids)
      .run();

  deletedCount = result.changes || ids.length;

  return c.json({
    code: ApiStatus.SUCCESS,
    message: `已删除 ${deletedCount} 个分享`,
  });
});

// 修改文本分享（需要认证）
app.put("/api/admin/pastes/:slug", authMiddleware, async (c) => {
  const db = c.env.DB;
  const slug = c.req.param("slug");
  const body = await c.req.json();

  // 检查分享是否存在
  const paste = await db.prepare(`SELECT id, slug, expires_at, max_views, views FROM ${DbTables.PASTES} WHERE slug = ?`).bind(slug).first();

  if (!paste) {
    throw new HTTPException(ApiStatus.NOT_FOUND, { message: "文本分享不存在" });
  }

  // 检查是否过期
  if (await checkAndDeleteExpiredPaste(db, paste)) {
    throw new HTTPException(ApiStatus.GONE, { message: "文本分享已过期或超过最大查看次数，无法修改" });
  }

  // 验证内容
  if (!body.content) {
    throw new HTTPException(ApiStatus.BAD_REQUEST, { message: "内容不能为空" });
  }

  // 验证可打开次数
  if (body.maxViews !== null && body.maxViews !== undefined && parseInt(body.maxViews) < 0) {
    throw new HTTPException(ApiStatus.BAD_REQUEST, { message: "可打开次数不能为负数" });
  }

  // 处理密码更新
  let passwordSql = "";
  const sqlParams = [];

  if (body.password) {
    // 如果提供了新密码，则更新
    const passwordHash = await hashPassword(body.password);
    passwordSql = "password = ?, ";
    sqlParams.push(passwordHash);
  } else if (body.clearPassword) {
    // 如果指定了清除密码
    passwordSql = "password = NULL, ";
  }

  // 处理slug更新
  let newSlug = paste.slug; // 默认保持原slug不变
  let slugSql = "";

  // 如果提供了新的slug，则生成唯一slug
  if (body.newSlug !== undefined) {
    try {
      // 如果newSlug为空或null，则自动生成随机slug
      newSlug = await generateUniqueSlug(db, body.newSlug || null);
      // 设置更新SQL
      slugSql = "slug = ?, ";
      // 将新slug添加到参数列表
      sqlParams.push(newSlug);
    } catch (error) {
      // 如果slug已被占用，返回409冲突错误
      if (error.message.includes("链接后缀已被占用")) {
        throw new HTTPException(ApiStatus.CONFLICT, { message: error.message });
      }
      throw error;
    }
  }

  // 检查是否修改了最大查看次数
  const isMaxViewsChanged = body.maxViews !== null && body.maxViews !== undefined && body.maxViews !== paste.max_views;

  // 构建SQL语句，根据是否修改了max_views决定是否重置views
  const viewsResetSQL = isMaxViewsChanged ? "views = 0, " : "";

  // 添加其他更新参数
  sqlParams.push(body.content, body.remark || null, body.expiresAt || null, body.maxViews || null, paste.id);

  // 执行更新
  await db
      .prepare(
          `UPDATE ${DbTables.PASTES} 
       SET ${passwordSql}
           ${slugSql}
           ${viewsResetSQL}
           content = ?, 
           remark = ?, 
           expires_at = ?, 
           max_views = ?,
           updated_at = CURRENT_TIMESTAMP
       WHERE id = ?`
      )
      .bind(...sqlParams)
      .run();

  return c.json({
    code: ApiStatus.SUCCESS,
    message: "文本分享已更新",
    data: {
      id: paste.id,
      slug: newSlug, // 返回更新后的slug（可能已更改）
    },
  });
});

// 获取单个文本分享详情（需要认证）
app.get("/api/admin/pastes/:id", authMiddleware, async (c) => {
  const db = c.env.DB;
  const { id } = c.req.param();

  try {
    // 管理员可以查看任何文本
    const paste = await db
        .prepare(
            `
        SELECT 
          id, slug, content, remark,
          password IS NOT NULL as has_password,
          expires_at, max_views, views, created_at, updated_at, created_by
        FROM ${DbTables.PASTES}
        WHERE id = ?
      `
        )
        .bind(id)
        .first();

    if (!paste) {
      return c.json(createErrorResponse(ApiStatus.NOT_FOUND, "文本不存在"), ApiStatus.NOT_FOUND);
    }

    // 确保has_password是布尔类型
    paste.has_password = !!paste.has_password;

    // 如果文本由API密钥创建，获取密钥名称
    let result = { ...paste };
    if (paste.created_by && paste.created_by.startsWith("apikey:")) {
      const keyId = paste.created_by.substring(7);
      const keyInfo = await db.prepare(`SELECT id, name FROM ${DbTables.API_KEYS} WHERE id = ?`).bind(keyId).first();

      if (keyInfo) {
        result.key_name = keyInfo.name;
      }
    }

    return c.json({
      code: ApiStatus.SUCCESS,
      message: "获取文本详情成功",
      data: result,
      success: true,
    });
  } catch (error) {
    console.error("获取文本详情错误:", error);
    return c.json(createErrorResponse(ApiStatus.INTERNAL_ERROR, error.message || "获取文本详情失败"), ApiStatus.INTERNAL_ERROR);
  }
});

// =================================
// ====== 前端 直接上传 S3 相关 API ===========================================================================================================================
// =================================

/**
 * 获取 S3 预签名上传 URL
 * 前端直接使用该 URL 进行上传，减轻 Worker 负担
 */
app.post("/api/s3/presign", async (c) => {
  const db = c.env.DB;

  // 身份验证
  const authHeader = c.req.header("Authorization");
  let isAuthorized = false;
  let authorizedBy = "";
  let adminId = null;
  let apiKeyId = null;

  // 检查Bearer令牌 (管理员)
  if (authHeader && authHeader.startsWith("Bearer ")) {
    const token = authHeader.substring(7);
    adminId = await validateAdminToken(c.env.DB, token);

    if (adminId) {
      isAuthorized = true;
      authorizedBy = "admin";
    }
  }
  // 检查API密钥
  else if (authHeader && authHeader.startsWith("ApiKey ")) {
    const apiKey = authHeader.substring(7);

    // 查询数据库中的API密钥记录
    const keyRecord = await db
        .prepare(
            `
        SELECT id, name, file_permission, expires_at
        FROM ${DbTables.API_KEYS}
        WHERE key = ?
      `
        )
        .bind(apiKey)
        .first();

    // 如果密钥存在且有文件权限
    if (keyRecord && keyRecord.file_permission === 1) {
      // 检查是否过期
      if (!(await checkAndDeleteExpiredApiKey(db, keyRecord))) {
        isAuthorized = true;
        authorizedBy = "apikey";
        // 记录API密钥ID
        apiKeyId = keyRecord.id;

        // 更新最后使用时间
        await db
            .prepare(
                `
            UPDATE ${DbTables.API_KEYS}
            SET last_used = ?
            WHERE id = ?
          `
            )
            .bind(getLocalTimeString(), keyRecord.id)
            .run();
      }
    }
  }

  // 如果都没有授权，则返回权限错误
  if (!isAuthorized) {
    return c.json(createErrorResponse(ApiStatus.FORBIDDEN, "需要管理员权限或有效的API密钥才能获取上传预签名URL"), ApiStatus.FORBIDDEN);
  }

  try {
    // 解析请求数据
    const body = await c.req.json();

    // 检查必要字段
    if (!body.s3_config_id) {
      return c.json(createErrorResponse(ApiStatus.BAD_REQUEST, "必须提供 s3_config_id"), ApiStatus.BAD_REQUEST);
    }

    if (!body.filename) {
      return c.json(createErrorResponse(ApiStatus.BAD_REQUEST, "必须提供 filename"), ApiStatus.BAD_REQUEST);
    }

    // 获取系统最大上传限制
    const maxUploadSizeResult = await db
        .prepare(
            `
        SELECT value FROM ${DbTables.SYSTEM_SETTINGS}
        WHERE key = 'max_upload_size'
      `
        )
        .first();

    const maxUploadSizeMB = maxUploadSizeResult ? parseInt(maxUploadSizeResult.value) : DEFAULT_MAX_UPLOAD_SIZE_MB;
    const maxUploadSizeBytes = maxUploadSizeMB * 1024 * 1024;

    // 如果请求中包含了文件大小，则检查大小是否超过限制
    if (body.size && body.size > maxUploadSizeBytes) {
      return c.json(
          createErrorResponse(ApiStatus.BAD_REQUEST, `文件大小超过系统限制，最大允许 ${formatFileSize(maxUploadSizeBytes)}，当前文件 ${formatFileSize(body.size)}`),
          ApiStatus.BAD_REQUEST
      );
    }

    // 获取S3配置
    const s3Config = await db
        .prepare(
            `
        SELECT * FROM ${DbTables.S3_CONFIGS}
        WHERE id = ?
      `
        )
        .bind(body.s3_config_id)
        .first();

    if (!s3Config) {
      return c.json(createErrorResponse(ApiStatus.NOT_FOUND, "指定的S3配置不存在"), ApiStatus.NOT_FOUND);
    }

    // 检查存储空间是否足够（在预签名阶段进行检查）
    if (body.size && s3Config.total_storage_bytes !== null) {
      // 获取当前存储桶已使用的总容量
      const usageResult = await db
          .prepare(
              `
          SELECT SUM(size) as total_used
          FROM ${DbTables.FILES}
          WHERE s3_config_id = ?
        `
          )
          .bind(body.s3_config_id)
          .first();

      const currentUsage = usageResult?.total_used || 0;
      const fileSize = parseInt(body.size);

      // 计算上传后的总使用量
      const totalAfterUpload = currentUsage + fileSize;

      // 如果上传后会超出总容量限制，则返回错误
      if (totalAfterUpload > s3Config.total_storage_bytes) {
        const remainingSpace = Math.max(0, s3Config.total_storage_bytes - currentUsage);
        const formattedRemaining = formatFileSize(remainingSpace);
        const formattedFileSize = formatFileSize(fileSize);
        const formattedTotal = formatFileSize(s3Config.total_storage_bytes);

        return c.json(
            createErrorResponse(ApiStatus.BAD_REQUEST, `存储空间不足。文件大小(${formattedFileSize})超过剩余空间(${formattedRemaining})。存储桶总容量限制为${formattedTotal}。`),
            ApiStatus.BAD_REQUEST
        );
      }
    }

    // 如果是管理员授权，确认配置属于该管理员
    if (authorizedBy === "admin" && s3Config.admin_id !== adminId) {
      return c.json(createErrorResponse(ApiStatus.FORBIDDEN, "您无权使用此S3配置"), ApiStatus.FORBIDDEN);
    }

    // 生成文件ID
    const fileId = generateFileId();

    // 生成slug (不冲突的唯一短链接)
    let slug;
    try {
      slug = await generateUniqueFileSlug(db, body.slug);
    } catch (error) {
      // 如果是slug冲突，返回HTTP 409状态码
      if (error.message.includes("链接后缀已被占用")) {
        return c.json(createErrorResponse(ApiStatus.CONFLICT, error.message), ApiStatus.CONFLICT);
      }
      throw error; // 其他错误继续抛出
    }

    // 处理文件路径
    const customPath = body.path || "";

    // 处理文件名
    const { name: fileName, ext: fileExt } = getFileNameAndExt(body.filename);
    const safeFileName = getSafeFileName(fileName).substring(0, 50); // 限制长度

    // 生成短ID
    const shortId = generateShortId();

    // 获取默认文件夹路径
    const folderPath = s3Config.default_folder ? (s3Config.default_folder.endsWith("/") ? s3Config.default_folder : s3Config.default_folder + "/") : "";

    // 组合最终路径 - 使用短ID-原始文件名的格式
    const storagePath = folderPath + customPath + shortId + "-" + safeFileName + fileExt;

    // 获取内容类型
    const mimetype = body.mimetype || getMimeType(body.filename);

    // 获取加密密钥
    const encryptionSecret = c.env.ENCRYPTION_SECRET || "default-encryption-key";

    // 生成预签名URL
    const upload_url = await generatePresignedPutUrl(s3Config, storagePath, mimetype, encryptionSecret, 3600);

    // 构建完整S3 URL
    const s3_url = buildS3Url(s3Config, storagePath);

    await db
        .prepare(
            `
        INSERT INTO ${DbTables.FILES} (
          id, slug, filename, storage_path, s3_url, 
          s3_config_id, mimetype, size, etag,
          created_by, created_at, updated_at
        ) VALUES (
          ?, ?, ?, ?, ?, 
          ?, ?, ?, ?,
          ?, ?, ?
        )
      `
        )
        .bind(
            fileId,
            slug,
            body.filename,
            storagePath,
            s3_url,
            body.s3_config_id,
            mimetype,
            0, // 初始大小为0，在上传完成后更新
            null, // 初始ETag为null，在上传完成后更新
            authorizedBy === "admin" ? adminId : authorizedBy === "apikey" ? `apikey:${apiKeyId}` : null, // 使用与传统上传一致的格式标记API密钥用户
            getLocalTimeString(), // 使用本地时间
            getLocalTimeString() // 使用本地时间
        )
        .run();

    // 返回预签名URL和文件信息
    return c.json({
      code: ApiStatus.SUCCESS,
      message: "获取预签名URL成功",
      data: {
        file_id: fileId,
        upload_url,
        storage_path: storagePath,
        s3_url,
        slug,
        provider_type: s3Config.provider_type, // 添加提供商类型，便于前端适配不同S3服务
      },
      success: true,
    });
  } catch (error) {
    console.error("获取预签名URL错误:", error);
    return c.json(createErrorResponse(ApiStatus.INTERNAL_ERROR, "获取预签名URL失败: " + error.message), ApiStatus.INTERNAL_ERROR);
  }
});

/**
 * 文件上传完成后的提交确认
 * 用于更新文件元数据和状态
 */
app.post("/api/s3/commit", async (c) => {
  const db = c.env.DB;

  // 身份验证
  const authHeader = c.req.header("Authorization");
  let isAuthorized = false;
  let authorizedBy = "";
  let adminId = null;
  let apiKeyId = null;

  // 检查Bearer令牌 (管理员)
  if (authHeader && authHeader.startsWith("Bearer ")) {
    const token = authHeader.substring(7);
    adminId = await validateAdminToken(c.env.DB, token);

    if (adminId) {
      isAuthorized = true;
      authorizedBy = "admin";
    }
  }
  // 检查API密钥
  else if (authHeader && authHeader.startsWith("ApiKey ")) {
    const apiKey = authHeader.substring(7);

    // 查询数据库中的API密钥记录
    const keyRecord = await db
        .prepare(
            `
        SELECT id, name, file_permission, expires_at
        FROM ${DbTables.API_KEYS}
        WHERE key = ?
      `
        )
        .bind(apiKey)
        .first();

    // 如果密钥存在且有文件权限
    if (keyRecord && keyRecord.file_permission === 1) {
      // 检查是否过期
      if (!(await checkAndDeleteExpiredApiKey(db, keyRecord))) {
        isAuthorized = true;
        authorizedBy = "apikey";
        // 记录API密钥ID
        apiKeyId = keyRecord.id;

        // 更新最后使用时间
        await db
            .prepare(
                `
            UPDATE ${DbTables.API_KEYS}
            SET last_used = ?
            WHERE id = ?
          `
            )
            .bind(getLocalTimeString(), keyRecord.id)
            .run();
      }
    }
  }

  // 如果都没有授权，则返回权限错误
  if (!isAuthorized) {
    return c.json(createErrorResponse(ApiStatus.FORBIDDEN, "需要管理员权限或有效的API密钥才能完成文件上传"), ApiStatus.FORBIDDEN);
  }

  try {
    const body = await c.req.json();

    // 验证必要字段
    if (!body.file_id) {
      return c.json(createErrorResponse(ApiStatus.BAD_REQUEST, "缺少文件ID参数"), ApiStatus.BAD_REQUEST);
    }

    if (!body.etag) {
      return c.json(createErrorResponse(ApiStatus.BAD_REQUEST, "缺少ETag参数"), ApiStatus.BAD_REQUEST);
    }

    // 查询待提交的文件信息
    const file = await db
        .prepare(
            `
        SELECT id, filename, storage_path, s3_config_id, size, s3_url, slug, created_by
        FROM ${DbTables.FILES}
        WHERE id = ?
      `
        )
        .bind(body.file_id)
        .first();

    if (!file) {
      return c.json(createErrorResponse(ApiStatus.NOT_FOUND, "文件不存在或已被删除"), ApiStatus.NOT_FOUND);
    }

    // 验证权限
    if (authorizedBy === "admin" && file.created_by && file.created_by !== adminId) {
      return c.json(createErrorResponse(ApiStatus.FORBIDDEN, "您无权更新此文件"), ApiStatus.FORBIDDEN);
    }

    if (authorizedBy === "apikey" && file.created_by && file.created_by !== `apikey:${apiKeyId}`) {
      return c.json(createErrorResponse(ApiStatus.FORBIDDEN, "此API密钥无权更新此文件"), ApiStatus.FORBIDDEN);
    }

    // 获取S3配置
    const s3ConfigQuery =
        authorizedBy === "admin" ? `SELECT * FROM ${DbTables.S3_CONFIGS} WHERE id = ? AND admin_id = ?` : `SELECT * FROM ${DbTables.S3_CONFIGS} WHERE id = ? AND is_public = 1`;

    const s3ConfigParams = authorizedBy === "admin" ? [file.s3_config_id, adminId] : [file.s3_config_id];
    const s3Config = await db
        .prepare(s3ConfigQuery)
        .bind(...s3ConfigParams)
        .first();

    if (!s3Config) {
      return c.json(createErrorResponse(ApiStatus.BAD_REQUEST, "无效的S3配置ID或无权访问该配置"), ApiStatus.BAD_REQUEST);
    }

    // 检查存储桶容量限制
    if (s3Config.total_storage_bytes !== null) {
      // 获取当前存储桶已使用的总容量（不包括当前待提交的文件）
      const usageResult = await db
          .prepare(
              `
          SELECT SUM(size) as total_used
          FROM ${DbTables.FILES}
          WHERE s3_config_id = ? AND id != ?
        `
          )
          .bind(file.s3_config_id, file.id)
          .first();

      const currentUsage = usageResult?.total_used || 0;
      const fileSize = file.size;

      // 计算提交后的总使用量
      const totalAfterCommit = currentUsage + fileSize;

      // 如果提交后会超出总容量限制，则返回错误并删除临时文件
      if (totalAfterCommit > s3Config.total_storage_bytes) {
        // 删除临时文件
        try {
          const encryptionSecret = c.env.ENCRYPTION_SECRET || "default-encryption-key";
          await deleteFileFromS3(s3Config, file.storage_path, encryptionSecret);
        } catch (deleteError) {
          console.error("删除超出容量限制的临时文件失败:", deleteError);
        }

        // 删除文件记录
        await db.prepare(`DELETE FROM ${DbTables.FILES} WHERE id = ?`).bind(file.id).run();

        const remainingSpace = Math.max(0, s3Config.total_storage_bytes - currentUsage);
        const formattedRemaining = formatFileSize(remainingSpace);
        const formattedFileSize = formatFileSize(fileSize);
        const formattedTotal = formatFileSize(s3Config.total_storage_bytes);

        return c.json(
            createErrorResponse(ApiStatus.BAD_REQUEST, `存储空间不足。文件大小(${formattedFileSize})超过剩余空间(${formattedRemaining})。存储桶总容量限制为${formattedTotal}。`),
            ApiStatus.BAD_REQUEST
        );
      }
    }

    // 处理元数据字段
    // 处理密码
    let passwordHash = null;
    if (body.password) {
      passwordHash = await hashPassword(body.password);
    }

    // 处理过期时间
    let expiresAt = null;
    if (body.expires_in) {
      const expiresInHours = parseInt(body.expires_in);
      if (!isNaN(expiresInHours) && expiresInHours > 0) {
        const expiresDate = new Date();
        expiresDate.setHours(expiresDate.getHours() + expiresInHours);
        expiresAt = expiresDate.toISOString();
      }
    }

    // 处理备注字段
    const remark = body.remark || null;

    // 处理最大查看次数
    const maxViews = body.max_views ? parseInt(body.max_views) : null;

    // 处理文件大小
    let fileSize = null;
    if (body.size) {
      fileSize = parseInt(body.size);
      if (isNaN(fileSize) || fileSize < 0) {
        fileSize = 0; // 防止无效值
      }
    }

    // 更新ETag和创建者
    const creator = authorizedBy === "admin" ? adminId : `apikey:${apiKeyId}`;
    const now = getLocalTimeString();

    // 更新文件记录
    await db
        .prepare(
            `
        UPDATE ${DbTables.FILES}
        SET 
          etag = ?, 
          created_by = ?, 
          remark = ?,
          password = ?,
          expires_at = ?,
          max_views = ?,
          updated_at = ?,
          size = CASE WHEN ? IS NOT NULL THEN ? ELSE size END
        WHERE id = ?
      `
        )
        .bind(
            body.etag,
            creator,
            remark,
            passwordHash,
            expiresAt,
            maxViews,
            now,
            fileSize !== null ? 1 : null, // 条件参数
            fileSize, // 文件大小值
            body.file_id
        )
        .run();

    // 处理明文密码保存
    if (body.password) {
      // 检查是否已存在密码记录
      const passwordExists = await db.prepare(`SELECT file_id FROM ${DbTables.FILE_PASSWORDS} WHERE file_id = ?`).bind(body.file_id).first();

      if (passwordExists) {
        // 更新现有密码
        await db.prepare(`UPDATE ${DbTables.FILE_PASSWORDS} SET plain_password = ?, updated_at = ? WHERE file_id = ?`).bind(body.password, now, body.file_id).run();
      } else {
        // 插入新密码
        await db
            .prepare(`INSERT INTO ${DbTables.FILE_PASSWORDS} (file_id, plain_password, created_at, updated_at) VALUES (?, ?, ?, ?)`)
            .bind(body.file_id, body.password, now, now)
            .run();
      }
    }

    // 获取更新后的文件记录
    const updatedFile = await db
        .prepare(
            `
        SELECT 
          id, slug, filename, storage_path, s3_url, 
          mimetype, size, remark, 
          created_at, updated_at
        FROM ${DbTables.FILES}
        WHERE id = ?
      `
        )
        .bind(body.file_id)
        .first();

    // 返回成功响应
    return c.json({
      code: ApiStatus.SUCCESS,
      message: "文件提交成功",
      data: {
        ...updatedFile,
        hasPassword: !!passwordHash,
        expiresAt: expiresAt,
        maxViews: maxViews,
        url: `/file/${updatedFile.slug}`,
      },
      success: true, // 添加兼容字段
    });
  } catch (error) {
    console.error("提交文件错误:", error);
    return c.json(createErrorResponse(ApiStatus.INTERNAL_ERROR, error.message || "提交文件失败"), ApiStatus.INTERNAL_ERROR);
  }
});

// =================================
// ========== 管理员面板系统设置及面板 API ===============================================================================================================================
// =================================

// 获取系统设置
app.get("/api/admin/system-settings", authMiddleware, async (c) => {
  const db = c.env.DB;

  try {
    // 获取所有系统设置
    const settings = await db
        .prepare(
            `
      SELECT key, value, description, updated_at
      FROM ${DbTables.SYSTEM_SETTINGS}
      ORDER BY key ASC
    `
        )
        .all();

    return c.json({
      code: ApiStatus.SUCCESS,
      message: "获取系统设置成功",
      data: settings.results,
      success: true,
    });
  } catch (error) {
    console.error("获取系统设置错误:", error);
    return c.json(createErrorResponse(ApiStatus.INTERNAL_ERROR, "获取系统设置失败: " + error.message), ApiStatus.INTERNAL_ERROR);
  }
});

// 更新系统设置
app.put("/api/admin/system-settings", authMiddleware, async (c) => {
  const db = c.env.DB;

  try {
    const body = await c.req.json();

    // 检查请求体是否有效
    if (!body || typeof body !== "object") {
      return c.json(createErrorResponse(ApiStatus.BAD_REQUEST, "请求参数无效"), ApiStatus.BAD_REQUEST);
    }

    // 处理更新最大上传大小的请求
    if (body.max_upload_size !== undefined) {
      let maxUploadSize = parseInt(body.max_upload_size);

      // 验证是否为有效数字
      if (isNaN(maxUploadSize) || maxUploadSize <= 0) {
        return c.json(createErrorResponse(ApiStatus.BAD_REQUEST, "最大上传大小必须为正整数"), ApiStatus.BAD_REQUEST);
      }

      // 更新数据库
      await db
          .prepare(
              `
        INSERT OR REPLACE INTO ${DbTables.SYSTEM_SETTINGS} (key, value, description, updated_at)
        VALUES ('max_upload_size', ?, '单次最大上传文件大小限制', datetime('now'))
      `
          )
          .bind(maxUploadSize.toString())
          .run();
    }

    return c.json({
      code: ApiStatus.SUCCESS,
      message: "系统设置更新成功",
      success: true,
    });
  } catch (error) {
    console.error("更新系统设置错误:", error);
    return c.json(createErrorResponse(ApiStatus.INTERNAL_ERROR, "更新系统设置失败: " + error.message), ApiStatus.INTERNAL_ERROR);
  }
});

// 获取最大上传文件大小限制（公共API）
app.get("/api/system/max-upload-size", async (c) => {
  const db = c.env.DB;

  try {
    // 获取最大上传大小设置
    const maxUploadSize = await db
        .prepare(
            `
      SELECT value FROM ${DbTables.SYSTEM_SETTINGS}
      WHERE key = 'max_upload_size'
    `
        )
        .first();

    // 返回默认值或数据库中的值
    const size = maxUploadSize ? parseInt(maxUploadSize.value) : DEFAULT_MAX_UPLOAD_SIZE_MB;

    return c.json({
      code: ApiStatus.SUCCESS,
      message: "获取最大上传大小成功",
      data: { max_upload_size: size },
      success: true,
    });
  } catch (error) {
    console.error("获取最大上传大小错误:", error);
    // 发生错误时返回默认值
    return c.json({
      code: ApiStatus.SUCCESS,
      message: "获取最大上传大小成功（使用默认值）",
      data: { max_upload_size: DEFAULT_MAX_UPLOAD_SIZE_MB },
      success: true,
    });
  }
});

// 仪表盘统计数据API
app.get("/api/admin/dashboard/stats", authMiddleware, async (c) => {
  try {
    const db = c.env.DB;
    const adminId = c.get("adminId");

    if (!adminId) {
      return c.json({ error: "Unauthorized" }, 401);
    }

    // 获取统计数据
    // 1. 文本分享总数
    const totalPastesResult = await db.prepare(`SELECT COUNT(*) as count FROM ${DbTables.PASTES}`).first();
    const totalPastes = totalPastesResult ? totalPastesResult.count : 0;

    // 2. 文件上传总数
    const totalFilesResult = await db.prepare(`SELECT COUNT(*) as count FROM ${DbTables.FILES}`).first();
    const totalFiles = totalFilesResult ? totalFilesResult.count : 0;

    // 3. API密钥总数
    const totalApiKeysResult = await db.prepare(`SELECT COUNT(*) as count FROM ${DbTables.API_KEYS}`).first();
    const totalApiKeys = totalApiKeysResult ? totalApiKeysResult.count : 0;

    // 4. S3配置总数
    const totalS3ConfigsResult = await db.prepare(`SELECT COUNT(*) as count FROM ${DbTables.S3_CONFIGS}`).first();
    const totalS3Configs = totalS3ConfigsResult ? totalS3ConfigsResult.count : 0;

    // 5. 获取所有S3存储配置的使用情况
    const s3ConfigsWithUsage = await getS3ConfigsWithUsage(db);

    // 6. 最近一周的数据
    const sevenDaysAgo = new Date();
    sevenDaysAgo.setDate(sevenDaysAgo.getDate() - 7);

    // 文本分享趋势
    const lastWeekPastesQuery = `
      SELECT 
        date(created_at) as date, 
        COUNT(*) as count 
      FROM ${DbTables.PASTES} 
      WHERE created_at >= ?
      GROUP BY date(created_at)
      ORDER BY date ASC
    `;

    const lastWeekPastesResult = await db.prepare(lastWeekPastesQuery).bind(sevenDaysAgo.toISOString()).all();

    // 文件上传趋势
    const lastWeekFilesQuery = `
      SELECT 
        date(created_at) as date, 
        COUNT(*) as count 
      FROM ${DbTables.FILES} 
      WHERE created_at >= ?
      GROUP BY date(created_at)
      ORDER BY date ASC
    `;

    const lastWeekFilesResult = await db.prepare(lastWeekFilesQuery).bind(sevenDaysAgo.toISOString()).all();

    // 处理每日数据，补全缺失的日期
    const lastWeekPastes = processWeeklyData(lastWeekPastesResult.results || []);
    const lastWeekFiles = processWeeklyData(lastWeekFilesResult.results || []);

    // 总体存储使用情况
    const totalStorageUsed = s3ConfigsWithUsage.reduce((total, config) => total + config.usedStorage, 0);

    return c.json({
      success: true,
      data: {
        totalPastes,
        totalFiles,
        totalApiKeys,
        totalS3Configs,
        totalStorageUsed,
        s3Buckets: s3ConfigsWithUsage,
        lastWeekPastes,
        lastWeekFiles,
      },
    });
  } catch (error) {
    console.error("获取仪表盘统计数据失败:", error);
    return c.json({ error: "获取仪表盘统计数据失败" }, 500);
  }
});

// =================================
// ========== 工作入口点 ===============================================================================================================================
// =================================

/**
 * Worker入口点
 * 负责处理所有传入的HTTP请求
 */
export default {
  /**
   * 处理传入的HTTP请求
   * @param {Request} request - 请求对象
   * @param {any} env - 环境变量和绑定
   * @param {any} ctx - 执行上下文
   * @returns {Promise<Response>} 响应对象
   */
  async fetch(request, env, ctx) {
    try {
      // 初始化数据库
      // await initDatabase(env.DB);
      await checkAndInitDatabase(env.DB);
      // 检查是否是直接文件下载请求
      const url = new URL(request.url);
      const pathParts = url.pathname.split("/");

      // 处理API路径下的文件下载请求 /api/file-download/:slug
      if (pathParts.length >= 4 && pathParts[1] === "api" && pathParts[2] === "file-download") {
        const slug = pathParts[3];
        return await handleFileDownload(slug, env, request, true); // 强制下载
      }

      // 处理API路径下的文件预览请求 /api/file-view/:slug
      if (pathParts.length >= 4 && pathParts[1] === "api" && pathParts[2] === "file-view") {
        const slug = pathParts[3];
        return await handleFileDownload(slug, env, request, false); // 预览
      }

      // 处理其他API请求
      return app.fetch(request, env, ctx);
    } catch (error) {
      console.error("处理请求时发生错误:", error);

      // 兼容前端期望的错误格式
      return new Response(
          JSON.stringify({
            code: ApiStatus.INTERNAL_ERROR,
            message: "服务器内部错误",
            error: error.message,
            success: false,
            data: null,
          }),
          {
            status: ApiStatus.INTERNAL_ERROR,
            headers: { "Content-Type": "application/json" },
          }
      );
    }
  },
};
