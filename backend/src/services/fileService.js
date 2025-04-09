import { DbTables, S3ProviderTypes } from "../constants";
import { generatePresignedUrl } from "../utils/s3Utils";
import { generateWebDAVUrl } from "../utils/webdavUtils";
import { decryptValue } from "../utils/crypto";

/**
 * 根据slug获取文件
 * @param {D1Database} db - D1数据库实例
 * @param {string} slug - 文件的slug
 * @returns {Promise<Object>} 文件对象
 * @throws {Error} 如果文件不存在
 */
export async function getFileBySlug(db, slug) {
  if (!slug) {
    throw new Error("缺少文件slug参数");
  }

  const file = await db
    .prepare(
      `
      SELECT f.*, s.endpoint_url, s.bucket_name, s.region, s.access_key_id, s.secret_access_key, s.path_style
      FROM ${DbTables.FILES} f
      LEFT JOIN ${DbTables.S3_CONFIGS} s ON f.s3_config_id = s.id
      WHERE f.slug = ?
    `
    )
    .bind(slug)
    .first();

  if (!file) {
    throw new Error("文件不存在");
  }

  return file;
}

/**
 * 检查文件是否可访问
 * @param {D1Database} db - D1数据库实例
 * @param {Object} file - 文件对象
 * @param {string} encryptionSecret - 加密密钥
 * @returns {Promise<Object>} 包含accessible和reason的对象
 */
export async function isFileAccessible(db, file, encryptionSecret) {
  if (!file) {
    return { accessible: false, reason: "not_found" };
  }

  // 检查文件是否已过期
  if (file.expires_at) {
    const expiryDate = new Date(file.expires_at);
    const now = new Date();
    if (now > expiryDate) {
      return { accessible: false, reason: "expired" };
    }
  }

  // 检查文件访问次数是否超过限制
  if (file.max_views !== null && file.max_views > 0) {
    if (file.views > file.max_views) {
      return { accessible: false, reason: "expired" };
    }
  }

  return { accessible: true };
}

/**
 * 增加文件查看次数并检查是否超过限制
 * @param {D1Database} db - D1数据库实例
 * @param {Object} file - 文件对象
 * @param {string} encryptionSecret - 加密密钥
 * @returns {Promise<Object>} 包含isExpired和file的对象
 */
export async function incrementAndCheckFileViews(db, file, encryptionSecret) {
  // 增加views计数
  await db.prepare(`UPDATE ${DbTables.FILES} SET views = views + 1 WHERE id = ?`).bind(file.id).run();

  // 重新获取文件信息，包括更新后的views计数
  const updatedFile = await db
    .prepare(
      `
      SELECT f.*, s.endpoint_url, s.bucket_name, s.region, s.access_key_id, s.secret_access_key, s.path_style
      FROM ${DbTables.FILES} f
      LEFT JOIN ${DbTables.S3_CONFIGS} s ON f.s3_config_id = s.id
      WHERE f.id = ?
    `
    )
    .bind(file.id)
    .first();

  // 检查是否达到最大查看次数限制
  if (updatedFile.max_views !== null && updatedFile.max_views > 0 && updatedFile.views > updatedFile.max_views) {
    return { isExpired: true, file: updatedFile };
  }

  return { isExpired: false, file: updatedFile };
}

/**
 * 生成文件下载URL
 * @param {D1Database} db - D1数据库实例
 * @param {Object} file - 文件对象
 * @param {string} encryptionSecret - 加密密钥
 * @param {Request} request - 原始请求对象，用于获取当前域名
 * @returns {Promise<Object>} 包含预览链接和下载链接的对象
 */
export async function generateFileDownloadUrl(db, file, encryptionSecret, request = null) {
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
        // WebDAV存储需要特殊处理
        if (s3Config.provider_type === S3ProviderTypes.WEBDAV) {
          // 对于WebDAV存储，强制使用代理URL，因为需要在服务器端处理认证
          previewUrl = proxyPreviewUrl;
          downloadUrl = proxyDownloadUrl;
        } else {
          // 生成预览URL，有效期1小时
          previewUrl = await generatePresignedUrl(s3Config, file.storage_path, encryptionSecret, 3600, false);

          // 生成下载URL，有效期1小时，强制下载
          downloadUrl = await generatePresignedUrl(s3Config, file.storage_path, encryptionSecret, 3600, true);
        }
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
    use_proxy: file.use_proxy || (file.s3_config_id && s3Config?.provider_type === S3ProviderTypes.WEBDAV ? 1 : 0),
  };
}

/**
 * 获取文件的公开信息
 * @param {Object} file - 文件对象
 * @param {boolean} requiresPassword - 是否需要密码
 * @param {Object} urlsObj - URL对象
 * @returns {Object} 公开文件信息
 */
export function getPublicFileInfo(file, requiresPassword, urlsObj = null) {
  // 确定使用哪种URL
  const useProxy = urlsObj?.use_proxy !== undefined ? urlsObj.use_proxy : file.use_proxy || 0;

  // 根据是否使用代理选择URL
  const effectivePreviewUrl = useProxy === 1 ? urlsObj?.proxyPreviewUrl : urlsObj?.previewUrl || file.s3_url;
  const effectiveDownloadUrl = useProxy === 1 ? urlsObj?.proxyDownloadUrl : urlsObj?.downloadUrl || file.s3_url;

  return {
    id: file.id,
    slug: file.slug,
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
    s3_direct_preview_url: urlsObj?.previewUrl || file.s3_url,
    s3_direct_download_url: urlsObj?.downloadUrl || file.s3_url,
    proxy_preview_url: urlsObj?.proxyPreviewUrl,
    proxy_download_url: urlsObj?.proxyDownloadUrl,
    use_proxy: useProxy,
    created_by: file.created_by || null,
  };
}
