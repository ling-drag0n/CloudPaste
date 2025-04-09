import { DbTables } from "../constants";
import { ApiStatus, S3ProviderTypes } from "../constants";
import { createErrorResponse, generateFileId, generateShortId, getSafeFileName, getFileNameAndExt, formatFileSize, getLocalTimeString } from "../utils/common";
import { getMimeType } from "../utils/fileUtils";
import { generatePresignedPutUrl, buildS3Url, deleteFileFromS3 } from "../utils/s3Utils";
import { validateAdminToken } from "../services/adminService";
import { checkAndDeleteExpiredApiKey } from "../services/apiKeyService";
import { hashPassword } from "../utils/crypto";
import { Hono } from "hono";
import { authMiddleware } from "../middlewares";
import { generateWebDAVPutUrl, createWebDAVDirectory } from "../utils/webdavUtils";
import htpaw from "htpaw";

// 默认最大上传限制（MB）
const DEFAULT_MAX_UPLOAD_SIZE_MB = 50;

/**
 * 生成唯一的文件slug
 * @param {D1Database} db - D1数据库实例
 * @param {string} customSlug - 自定义slug
 * @returns {Promise<string>} 生成的唯一slug
 */
async function generateUniqueFileSlug(db, customSlug = null) {
  // 如果提供了自定义slug，验证其格式并检查是否已存在
  if (customSlug) {
    // 验证slug格式：只允许字母、数字、横杠和下划线
    const slugFormatRegex = /^[a-zA-Z0-9_-]+$/;
    if (!slugFormatRegex.test(customSlug)) {
      throw new Error("链接后缀格式无效，只能使用字母、数字、下划线和横杠");
    }

    // 检查slug是否已存在
    const existingFile = await db.prepare(`SELECT id FROM ${DbTables.FILES} WHERE slug = ?`).bind(customSlug).first();
    if (existingFile) {
      throw new Error("链接后缀已被占用，请使用其他链接后缀");
    }

    return customSlug;
  }

  // 生成随机slug (6个字符)
  let attempts = 0;
  const maxAttempts = 10;
  while (attempts < maxAttempts) {
    const randomSlug = generateShortId();

    // 检查是否已存在
    const existingFile = await db.prepare(`SELECT id FROM ${DbTables.FILES} WHERE slug = ?`).bind(randomSlug).first();
    if (!existingFile) {
      return randomSlug;
    }

    attempts++;
  }

  throw new Error("无法生成唯一链接后缀，请稍后再试");
}

/**
 * 注册S3文件上传相关API路由
 * @param {Object} app - Hono应用实例
 */
export function registerS3UploadRoutes(app) {
  // 获取预签名上传URL
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

  // 文件上传完成后的提交确认
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
        const fileSize = parseInt(body.size || 0);

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
            createErrorResponse(
              ApiStatus.BAD_REQUEST,
              `存储空间不足。文件大小(${formattedFileSize})超过剩余空间(${formattedRemaining})。存储桶总容量限制为${formattedTotal}。文件已被删除。`
            ),
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
      return c.json(createErrorResponse(ApiStatus.INTERNAL_ERROR, "提交文件失败: " + error.message), ApiStatus.INTERNAL_ERROR);
    }
  });

  // 生成WebDAV上传链接
  app.post("/api/webdav-upload", async (c) => {
    try {
      const db = c.env.DB;
      const encryptionSecret = c.env.ENCRYPTION_SECRET || "default-encryption-key";
      
      // 获取请求体
      const body = await c.req.json();
      
      // 提取必要参数
      const s3ConfigId = body.s3_config_id;
      const filename = body.filename;
      const fileSize = parseInt(body.size || "0");
      const mimetype = body.mimetype || "application/octet-stream";
      
      if (!s3ConfigId || !filename) {
        return c.json(createErrorResponse(ApiStatus.BAD_REQUEST, "缺少必要参数"), ApiStatus.BAD_REQUEST);
      }
      
      // 获取S3配置
      const s3Config = await db
        .prepare(`SELECT * FROM ${DbTables.S3_CONFIGS} WHERE id = ?`)
        .bind(s3ConfigId)
        .first();
      
      if (!s3Config) {
        return c.json(createErrorResponse(ApiStatus.NOT_FOUND, "存储配置不存在"), ApiStatus.NOT_FOUND);
      }
      
      // 检查是否为WebDAV类型
      if (s3Config.provider_type !== S3ProviderTypes.WEBDAV) {
        return c.json(createErrorResponse(ApiStatus.BAD_REQUEST, "该接口仅支持WebDAV存储"), ApiStatus.BAD_REQUEST);
      }
      
      // 生成文件存储路径
      const folderPrefix = s3Config.default_folder ? (s3Config.default_folder.endsWith("/") ? s3Config.default_folder : s3Config.default_folder + "/") : "";
      const timestamp = Date.now();
      const storagePath = `${folderPrefix}${timestamp}_${filename}`;
      
      // 确保目录存在
      if (folderPrefix) {
        await createWebDAVDirectory(s3Config, folderPrefix, encryptionSecret);
      }
      
      // 生成WebDAV上传URL
      const webdavInfo = await generateWebDAVPutUrl(s3Config, storagePath, encryptionSecret);
      
      // 使用htpaw库创建认证头
      const authHeader = htpaw.basicAuth(webdavInfo.auth.username, webdavInfo.auth.password);
      
      return c.json({
        code: ApiStatus.SUCCESS,
        success: true,
        message: "生成WebDAV上传URL成功",
        data: {
          url: webdavInfo.url,
          method: webdavInfo.method,
          headers: {
            'Authorization': authHeader,
            'Content-Type': mimetype
          },
          storage_path: storagePath,
          s3_config_id: s3ConfigId
        }
      });
    } catch (error) {
      console.error("生成WebDAV上传URL出错:", error);
      return c.json(
        createErrorResponse(ApiStatus.INTERNAL_ERROR, "生成WebDAV上传URL失败: " + error.message),
        ApiStatus.INTERNAL_ERROR
      );
    }
  });
}
