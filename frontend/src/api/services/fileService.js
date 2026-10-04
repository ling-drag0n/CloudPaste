/**
 * 文件管理服务API
 * 统一管理所有文件相关的API调用，包括上传、下载、管理等
 */

import { get, post, put, del } from "../client";

/******************************************************************************
 * 文件上传相关API
 ******************************************************************************/

/**
 * 获取上传预签名URL
 * @param {Object} options - 上传选项
 * @param {string} options.storage_config_id - 存储配置ID
 * @param {string} options.filename - 文件名
 * @param {string} options.mimetype - 文件的MIME类型
 * @param {string} [options.path] - 自定义存储路径
 * @param {string} [options.slug] - 自定义短链接
 * @param {number} [options.size] - 文件大小（字节）
 * @returns {Promise<Object>} 包含预签名URL的响应
 */
export async function getUploadPresignedUrl(options) {
  try {
    const sha256 = typeof options?.sha256 === "string" && options.sha256 ? options.sha256 : null;
    const data = {
      filename: options.filename,
      fileSize: options.size ?? options.fileSize ?? options.filesize,
      contentType: options.mimetype,
      path: options.path || null,
      storage_config_id: options.storage_config_id || null,
      sha256,
    };

    return await post("share/presign", data);
  } catch (error) {
    if (error.message && error.message.includes("链接后缀已被占用")) {
      throw new Error("链接后缀已被占用，请尝试其他后缀");
    }

    throw new Error(`获取预签名URL失败: ${error.message}`);
  }
}

/**
 * 完成文件上传（更新文件元数据）
 * @param {Object} data - 上传完成后的元数据
 * @returns {Promise<Object>} 更新响应
 */
export async function completeFileUpload(data) {
  // 新协议优先：key + storage_config_id；兼容旧 path（虚拟路径）
  const payload = {
    key: data.key || undefined,
    storage_config_id: data.storage_config_id || undefined,
    path: data.path || undefined,
    filename: data.filename,
    size: Number(data.size ?? data.fileSize ?? 0),
    etag: data.etag,
    sha256: typeof data?.sha256 === "string" && data.sha256 ? data.sha256 : null,
    slug: data.slug,
    remark: data.remark,
    password: data.password,
    expires_in: data.expires_in ?? data.expiresIn,
    max_views: data.max_views ?? data.maxViews,
    use_proxy: data.use_proxy ?? data.useProxy,
    original_filename: data.original_filename ?? data.originalFilename ?? false,
  };

  return await post("share/commit", payload);
}

/**
 * 通过通用分享上传接口上传单个文件并创建分享记录（ObjectStore 多存储直传）
 * @param {Object} options
 * @param {File|Blob} options.file - 要上传的文件
 * @param {string|number} options.storage_config_id - 存储配置ID
 * @param {string} [options.path] - 目标目录（可选）
 * @param {string} [options.slug] - 自定义链接后缀（可选）
 * @param {string} [options.remark] - 备注（可选）
 * @param {string} [options.password] - 密码（可选）
 * @param {number|string} [options.expires_in] - 过期时间（小时，可选）
 * @param {number|string} [options.max_views] - 最大下载次数（可选）
 * @param {boolean} [options.use_proxy] - 是否通过代理下载（可选）
 * @param {boolean} [options.original_filename] - 是否使用原始文件名（可选）
 * @returns {Promise<Object>} 后端统一响应对象 { success, message, data }
 */
export async function uploadShareFile(options) {
  const {
    file,
    storage_config_id,
    path = "",
    slug = "",
    remark = "",
    password = "",
    expires_in = "0",
    max_views = 0,
    use_proxy = undefined,
    original_filename = undefined,
  } = options || {};

  if (!file) {
    throw new Error("缺少上传文件");
  }
  if (!storage_config_id) {
    throw new Error("缺少存储配置ID");
  }

  const formData = new FormData();
  formData.append("file", file);
  formData.append("storage_config_id", String(storage_config_id));

  if (path) formData.append("path", String(path));
  if (slug) formData.append("slug", String(slug));
  if (remark) formData.append("remark", String(remark));
  if (password) formData.append("password", String(password));
  if (expires_in != null) formData.append("expires_in", String(expires_in));
  if (max_views != null) formData.append("max_views", String(max_views));
  if (use_proxy !== undefined) formData.append("use_proxy", use_proxy ? "true" : "false");
  if (original_filename !== undefined) {
    formData.append("original_filename", original_filename ? "true" : "false");
  }

  return post("share/upload", formData);
}


/******************************************************************************
 * 统一文件管理API
 ******************************************************************************/

/**
 * 获取文件列表（统一接口，自动根据认证信息处理）
 * @param {number} limit - 每页条数
 * @param {number} offset - 偏移量
 * @param {Object} options - 额外查询选项（管理员可用）
 * @returns {Promise<Object>} 文件列表响应
 */
export async function getFiles(limit = 50, offset = 0, options = {}) {
  const params = { limit, offset, ...options };
  return await get("files", { params });
}

/**
 * 获取单个文件详情（统一接口，自动根据认证信息处理）
 * @param {string} id - 文件ID
 * @returns {Promise<Object>} 文件详情响应
 */
export async function getFile(id, options = {}) {
  const params = {};
  if (options.includeLinks) {
    params.include = "links";
  }
  const hasParams = Object.keys(params).length > 0;
  return await get(`files/${id}`, hasParams ? { params } : undefined);
}

/**
 * 更新文件元数据（统一接口，自动根据认证信息处理）
 * @param {string} id - 文件ID
 * @param {Object} metadata - 更新的文件元数据
 * @returns {Promise<Object>} 更新响应
 */
export async function updateFile(id, metadata) {
  return await put(`files/${id}`, metadata);
}

/**
 * 批量删除文件（统一接口，自动根据认证信息处理）
 * @param {Array<string>} ids - 文件ID数组
 * @param {string} deleteMode - 删除模式：'record_only' 仅删除记录，'both' 同时删除文件（默认）
 * @returns {Promise<Object>} 批量删除响应
 */
export async function batchDeleteFiles(ids, deleteMode = "both") {
  return await del(`files/batch-delete`, { ids, delete_mode: deleteMode });
}

/******************************************************************************
 * 公共文件访问API
 ******************************************************************************/

/**
 * 获取公开文件信息
 * @param {string} slug - 文件短链接
 * @returns {Promise<Object>} 文件信息响应
 */
export async function getPublicFile(slug) {
  return await get(`share/get/${slug}`);
}

/**
 * 验证文件密码
 * @param {string} slug - 文件短链接
 * @param {string} password - 文件密码
 * @returns {Promise<Object>} 验证响应
 */
export async function verifyFilePassword(slug, password) {
  return await post(`share/verify/${slug}`, { password });
}

/******************************************************************************
 * 文件夹分享（公开只读）API
 ******************************************************************************/

/**
 * 获取公开文件夹分享元信息
 * @param {string} slug - 分享短链接
 * @returns {Promise<Object>} 元信息响应（无密码时包含根目录 items）
 */
export async function getPublicFolder(slug) {
  return await get(`share/folder/${slug}`);
}

/**
 * 验证文件夹分享密码（成功后返回根目录 items）
 * @param {string} slug - 分享短链接
 * @param {string} password - 访问密码
 * @returns {Promise<Object>} 验证响应
 */
export async function verifyFolderPassword(slug, password) {
  return await post(`share/folder/verify/${slug}`, { password });
}

/**
 * 获取文件夹分享某个子目录的列表
 * @param {string} slug - 分享短链接
 * @param {{path?:string, password?:string}} [options]
 * @returns {Promise<Object>} 目录列表响应
 */
export async function getPublicFolderList(slug, options = {}) {
  const params = {};
  if (options.path) params.path = options.path;
  if (options.password) params.password = options.password;
  return await get(`share/folder/${slug}/list`, { params });
}
