import { DbTables } from "../constants";
import { ApiStatus, S3ProviderTypes } from "../constants";
import { createErrorResponse, generateFileId, generateShortId, getSafeFileName, getFileNameAndExt, formatFileSize, getLocalTimeString } from "../utils/common";
import { getMimeType } from "../utils/fileUtils";
import { generatePresignedPutUrl, buildS3Url, deleteFileFromS3 } from "../utils/s3Utils";
import { uploadFileToWebDAV, buildWebDAVUrl } from "../utils/webdavUtils";
import { validateAdminToken } from "../services/adminService";
import { checkAndDeleteExpiredApiKey } from "../services/apiKeyService";
import { hashPassword } from "../utils/crypto";

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
            createErrorResponse(ApiStatus.BAD_REQUEST, `存储空间不足，剩余 ${formattedRemaining}，文件大小 ${formattedFileSize}，总容量 ${formattedTotal}`),
            ApiStatus.BAD_REQUEST
          );
        }
      }

      // 获取加密密钥
      const encryptionSecret = c.env.ENCRYPTION_SECRET || "default-encryption-key";

      // 安全处理文件名（移除路径信息等）
      const safeFilename = getSafeFileName(body.filename);
      const { fileNameWithoutExt, extension } = getFileNameAndExt(safeFilename);

      // 确定mime类型
      const mimeType = body.mimetype || getMimeType(safeFilename);

      // 构建存储路径
      const timestamp = Date.now();
      const storageFolder = s3Config.default_folder ? (s3Config.default_folder.endsWith("/") ? s3Config.default_folder : s3Config.default_folder + "/") : "";
      const storageDateFolder = `${new Date().getFullYear()}/${String(new Date().getMonth() + 1).padStart(2, "0")}/`;
      const storageFileName = `${fileNameWithoutExt}-${timestamp}${extension ? "." + extension : ""}`;
      const storagePath = `${storageFolder}${storageDateFolder}${storageFileName}`;
      
      // 根据不同的存储提供商类型处理上传
      let uploadInfo;
      let directUpload = false;
      
      if (s3Config.provider_type === S3ProviderTypes.WEBDAV) {
        // WebDAV需要通过Worker直接上传，不支持客户端直接上传
        directUpload = true;
        
        // 构建WebDAV URL
        const webdavUrl = buildWebDAVUrl(s3Config, storagePath);
        
        uploadInfo = {
          provider: "webdav",
          uploadUrl: "/api/s3/webdav-upload", // 使用Worker代理上传端点
          storagePath,
          s3_config_id: s3Config.id,
          fields: {
            filename: safeFilename,
            mimetype: mimeType
          }
        };
      } else {
        // 生成S3预签名URL用于直接上传
        try {
          const presignedUrl = await generatePresignedPutUrl(s3Config, storagePath, mimeType, encryptionSecret);
          
          uploadInfo = {
            provider: s3Config.provider_type,
            uploadUrl: presignedUrl,
            storagePath,
            fields: null, // S3预签名URL不需要额外字段
          };
        } catch (presignError) {
          console.error("生成预签名URL错误:", presignError);
          return c.json(createErrorResponse(ApiStatus.INTERNAL_ERROR, "生成上传URL失败: " + presignError.message), ApiStatus.INTERNAL_ERROR);
        }
      }

      // 生成唯一文件标识和URL用于访问
      const fileId = generateFileId();
      const fileSlug = await generateUniqueFileSlug(db, body.custom_slug);

      // 计算过期时间（如果提供）
      let expiresAt = null;
      if (body.expires_days) {
        const daysToExpire = parseInt(body.expires_days);
        if (!isNaN(daysToExpire) && daysToExpire > 0) {
          const expiryDate = new Date();
          expiryDate.setDate(expiryDate.getDate() + daysToExpire);
          expiresAt = expiryDate.toISOString();
        }
      }

      // 处理提供的密码
      let passwordHash = null;
      let plainPassword = null;
      if (body.password && body.password.trim().length > 0) {
        passwordHash = await hashPassword(body.password);
        plainPassword = body.password; // 保存明文密码，稍后插入到file_passwords表
      }

      // 创建文件记录
      const fileEntry = {
        id: fileId,
        filename: safeFilename,
        storage_path: storagePath,
        s3_url: null, // 将在上传完成后更新
        mimetype: mimeType,
        size: body.size || 0, // 如果未提供大小，则初始为0
        s3_config_id: s3Config.id,
        slug: fileSlug,
        remark: body.remark || null,
        password: passwordHash,
        expires_at: expiresAt,
        max_views: body.max_views ? parseInt(body.max_views) : null,
        views: 0,
        etag: null, // 将在上传完成后更新
        created_by: authorizedBy === "admin" ? adminId : apiKeyId, // 使用授权者ID
        use_proxy: body.use_proxy !== false ? 1 : 0, // 默认使用代理
      };

      // 将文件记录添加到数据库
      await db
        .prepare(
          `
          INSERT INTO ${DbTables.FILES} (
            id, filename, storage_path, s3_url, mimetype, 
            size, s3_config_id, slug, remark, password, 
            expires_at, max_views, views, etag, created_by,
            use_proxy, created_at, updated_at
          ) 
          VALUES (
            ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?,
            ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
          )
        `
        )
        .bind(
          fileEntry.id,
          fileEntry.filename,
          fileEntry.storage_path,
          fileEntry.s3_url,
          fileEntry.mimetype,
          fileEntry.size,
          fileEntry.s3_config_id,
          fileEntry.slug,
          fileEntry.remark,
          fileEntry.password,
          fileEntry.expires_at,
          fileEntry.max_views,
          fileEntry.views,
          fileEntry.etag,
          fileEntry.created_by,
          fileEntry.use_proxy
        )
        .run();

      // 如果提供了密码，保存明文密码到file_passwords表（用于管理员查看）
      if (plainPassword) {
        await db
          .prepare(
            `
            INSERT INTO ${DbTables.FILE_PASSWORDS} (
              file_id, plain_password, created_at, updated_at
            )
            VALUES (?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
          `
          )
          .bind(fileId, plainPassword)
          .run();
      }

      // 更新S3配置的使用时间
      await db
        .prepare(
          `
          UPDATE ${DbTables.S3_CONFIGS}
          SET last_used = ?
          WHERE id = ?
        `
        )
        .bind(getLocalTimeString(), s3Config.id)
        .run();

      // 构建文件访问URL
      const origin = new URL(c.req.url).origin;
      const fileViewUrl = `${origin}/view/${fileSlug}`;
      const fileDownloadUrl = `${origin}/d/${fileSlug}`;

      return c.json({
        code: ApiStatus.SUCCESS,
        message: "已获取预签名上传URL",
        data: {
          upload: uploadInfo,
          file: {
            id: fileId,
            slug: fileSlug,
            filename: safeFilename,
            mimetype: mimeType,
            size: body.size || 0,
            storage_path: storagePath,
            direct_upload: directUpload,
            view_url: fileViewUrl,
            download_url: fileDownloadUrl,
            created_at: getLocalTimeString(),
          },
        },
        success: true, // 兼容字段
      });
    } catch (error) {
      console.error("获取预签名URL错误:", error);
      return c.json(createErrorResponse(ApiStatus.INTERNAL_ERROR, "获取预签名URL失败: " + error.message), ApiStatus.INTERNAL_ERROR);
    }
  });

  // WebDAV上传端点 - 用于处理WebDAV直接上传
  app.post("/api/s3/webdav-upload", async (c) => {
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
        }
      }
    }

    // 如果都没有授权，则返回权限错误
    if (!isAuthorized) {
      return c.json(createErrorResponse(ApiStatus.FORBIDDEN, "需要管理员权限或有效的API密钥才能上传文件"), ApiStatus.FORBIDDEN);
    }
    
    try {
      // 处理表单数据
      const formData = await c.req.formData();
      const file = formData.get("file");
      const s3ConfigId = formData.get("s3_config_id");
      const storagePath = formData.get("storagePath");
      
      if (!file || !s3ConfigId || !storagePath) {
        return c.json(
          createErrorResponse(ApiStatus.BAD_REQUEST, "缺少必需的上传参数"),
          ApiStatus.BAD_REQUEST
        );
      }
      
      // 获取S3配置
      const s3Config = await db
        .prepare(`SELECT * FROM ${DbTables.S3_CONFIGS} WHERE id = ?`)
        .bind(s3ConfigId)
        .first();
      
      if (!s3Config) {
        return c.json(
          createErrorResponse(ApiStatus.NOT_FOUND, "指定的存储配置不存在"),
          ApiStatus.NOT_FOUND
        );
      }
      
      // 检查是否为WebDAV类型
      if (s3Config.provider_type !== S3ProviderTypes.WEBDAV) {
        return c.json(
          createErrorResponse(ApiStatus.BAD_REQUEST, "此端点仅支持WebDAV存储类型"),
          ApiStatus.BAD_REQUEST
        );
      }
      
      // 获取文件内容
      const fileContent = await file.arrayBuffer();
      const fileSize = fileContent.byteLength;
      
      // 获取加密密钥
      const encryptionSecret = c.env.ENCRYPTION_SECRET || "default-encryption-key";
      
      // 上传到WebDAV
      await uploadFileToWebDAV(s3Config, storagePath, fileContent, encryptionSecret);
      
      // 更新文件记录
      await db
        .prepare(
          `
          UPDATE ${DbTables.FILES}
          SET size = ?, updated_at = CURRENT_TIMESTAMP
          WHERE s3_config_id = ? AND storage_path = ?
        `
        )
        .bind(fileSize, s3ConfigId, storagePath)
        .run();
      
      return c.json({
        code: ApiStatus.SUCCESS,
        message: "文件成功上传到WebDAV",
        data: {
          size: fileSize,
          storagePath: storagePath
        },
        success: true
      });
    } catch (error) {
      console.error("WebDAV上传错误:", error);
      return c.json(
        createErrorResponse(ApiStatus.INTERNAL_ERROR, "WebDAV上传失败: " + error.message),
        ApiStatus.INTERNAL_ERROR
      );
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
}
