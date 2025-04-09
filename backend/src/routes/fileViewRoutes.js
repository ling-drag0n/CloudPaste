import { DbTables, S3ProviderTypes } from "../constants";
import { verifyPassword } from "../utils/crypto";
import { generatePresignedUrl, deleteFileFromS3 } from "../utils/s3Utils";
import { downloadFileFromWebDAV, deleteFileFromWebDAV } from "../utils/webdavUtils";

/**
 * 从数据库获取文件信息
 * @param {D1Database} db - D1数据库实例
 * @param {string} slug - 文件的slug
 * @param {boolean} includePassword - 是否包含密码
 * @returns {Promise<Object|null>} 文件信息或null
 */
async function getFileBySlug(db, slug, includePassword = true) {
  const fields = includePassword
      ? "f.id, f.filename, f.storage_path, f.s3_url, f.mimetype, f.size, f.remark, f.password, f.max_views, f.views, f.expires_at, f.created_at, f.s3_config_id, f.created_by, f.use_proxy, f.slug"
      : "f.id, f.filename, f.storage_path, f.s3_url, f.mimetype, f.size, f.remark, f.max_views, f.views, f.expires_at, f.created_at, f.s3_config_id, f.created_by, f.use_proxy, f.slug";

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

/**
 * 检查并删除过期文件
 * @param {D1Database} db - D1数据库实例
 * @param {Object} file - 文件对象
 * @param {string} encryptionSecret - 加密密钥
 * @returns {Promise<boolean>} 是否已删除
 */
async function checkAndDeleteExpiredFile(db, file, encryptionSecret) {
  try {
    if (!file) return false;

    let isExpired = false;
    const now = new Date();

    // 检查是否过期
    if (file.expires_at && new Date(file.expires_at) < now) {
      isExpired = true;
    }

    // 检查是否超过最大查看次数
    if (file.max_views && file.max_views > 0 && file.views > file.max_views) {
      isExpired = true;
    }

    // 如果已过期，尝试删除
    if (isExpired) {
      // 如果有S3配置，尝试从S3删除
      if (file.s3_config_id && file.storage_path) {
        const s3Config = await db.prepare(`SELECT * FROM ${DbTables.S3_CONFIGS} WHERE id = ?`).bind(file.s3_config_id).first();
        if (s3Config) {
          try {
            // 根据不同的提供商类型使用不同的删除方法
            if (s3Config.provider_type === S3ProviderTypes.WEBDAV) {
              // WebDAV删除
              await deleteFileFromWebDAV(s3Config, file.storage_path, encryptionSecret);
            } else {
              // S3删除
              await deleteFileFromS3(s3Config, file.storage_path, encryptionSecret);
            }
          } catch (error) {
            console.error("从存储中删除过期文件失败:", error);
            // 即使存储删除失败，仍继续数据库删除
          }
        }
      }

      // 从数据库删除文件记录
      await db.prepare(`DELETE FROM ${DbTables.FILES} WHERE id = ?`).bind(file.id).run();

      console.log(`文件(${file.id})已过期或超过最大查看次数，已删除`);
      return true;
    }

    return false;
  } catch (error) {
    console.error("检查和删除过期文件出错:", error);
    return false;
  }
}

/**
 * 增加文件查看次数并检查是否超过限制
 * @param {D1Database} db - D1数据库实例
 * @param {Object} file - 文件对象
 * @param {string} encryptionSecret - 加密密钥
 * @returns {Promise<Object>} 包含更新后的文件信息和状态
 */
async function incrementAndCheckFileViews(db, file, encryptionSecret) {
  // 首先递增访问计数
  await db.prepare(`UPDATE ${DbTables.FILES} SET views = views + 1, updated_at = ? WHERE id = ?`).bind(new Date().toISOString(), file.id).run();

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
 * 处理文件下载请求
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

    // 获取S3存储配置
    const s3Config = await db.prepare(`SELECT * FROM ${DbTables.S3_CONFIGS} WHERE id = ?`).bind(file.s3_config_id).first();
    if (!s3Config) {
      return new Response("文件存储配置不存在", { status: 500 });
    }

    // 增加查看计数并检查是否超过限制
    const viewResult = await incrementAndCheckFileViews(db, file, encryptionSecret);
    if (viewResult.isExpired) {
      return new Response("文件已达到最大查看次数", { status: 410 });
    }

    // 根据不同的存储提供商类型处理下载
    if (s3Config.provider_type === S3ProviderTypes.WEBDAV) {
      // WebDAV下载
      try {
        // 从WebDAV获取文件内容
        const { data, stats } = await downloadFileFromWebDAV(s3Config, file.storage_path, encryptionSecret);
        
        // 准备响应头
        const headers = {
          "Content-Type": file.mimetype || "application/octet-stream",
          "Content-Length": stats.size.toString(),
        };
        
        // 根据是否强制下载设置Content-Disposition
        if (forceDownload) {
          headers["Content-Disposition"] = `attachment; filename="${encodeURIComponent(file.filename)}"`;
        } else {
          headers["Content-Disposition"] = `inline; filename="${encodeURIComponent(file.filename)}"`;
        }
        
        // 返回文件内容
        return new Response(data, {
          status: 200,
          headers: headers,
        });
      } catch (error) {
        console.error("WebDAV下载错误:", error);
        return new Response(`从WebDAV下载文件失败: ${error.message}`, { status: 500 });
      }
    } else {
      // S3下载
      try {
        // 生成预签名URL用于直接下载
        const presignedUrl = await generatePresignedUrl(s3Config, file.storage_path, encryptionSecret, 3600, forceDownload);
        
        // 直接重定向到预签名URL
        return Response.redirect(presignedUrl, 302);
      } catch (error) {
        console.error("生成S3预签名URL错误:", error);
        return new Response(`生成S3下载URL失败: ${error.message}`, { status: 500 });
      }
    }
  } catch (error) {
    console.error("文件下载处理错误:", error);
    return new Response(`处理文件下载时出错: ${error.message}`, { status: 500 });
  }
}

/**
 * 注册文件查看/下载路由
 * @param {Object} app - Hono应用实例
 */
export function registerFileViewRoutes(app) {
  // 处理API路径下的文件下载请求 /api/file-download/:slug
  app.get("/api/file-download/:slug", async (c) => {
    const slug = c.req.param("slug");
    return await handleFileDownload(slug, c.env, c.req.raw, true); // 强制下载
  });

  // 处理API路径下的文件预览请求 /api/file-view/:slug
  app.get("/api/file-view/:slug", async (c) => {
    const slug = c.req.param("slug");
    return await handleFileDownload(slug, c.env, c.req.raw, false); // 预览
  });
}

// 导出handleFileDownload函数和checkAndDeleteExpiredFile函数
export { handleFileDownload, checkAndDeleteExpiredFile };
