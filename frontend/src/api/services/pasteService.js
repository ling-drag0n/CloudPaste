/**
 * 文本分享服务API
 * 统一管理所有文本分享相关的API调用，包括管理员和API密钥用户的操作
 */

import { get, post, put, del } from "../client";
import { API_BASE_URL } from "../config";

/******************************************************************************
 * 公共文本分享API（无需认证）
 ******************************************************************************/

/**
 * 获取文本分享
 * @param {string} slug - 文本分享链接后缀
 * @param {string} [password] - 访问密码（如果需要）
 * @returns {Promise<Object>} 文本分享数据
 */
export function getPaste(slug, password = null) {
  const endpoint = `/paste/${slug}`;

  // 如果提供了密码，则以POST方式提交
  if (password) {
    return post(endpoint, { password });
  }

  return get(endpoint);
}

/**
 * 获取文本分享的原始内容链接
 * @param {string} slug - 文本分享链接后缀
 * @param {string} [password] - 访问密码（如果需要）
 * @returns {string} 原始文本链接
 */
export function getRawPasteUrl(slug, password = null) {
  // 使用API_BASE_URL常量，不使用API_PREFIX前缀
  const baseUrl = `${API_BASE_URL}/api/raw/${slug}`;

  // 如果提供了密码，添加到URL参数中
  if (password) {
    return `${baseUrl}?password=${encodeURIComponent(password)}`;
  }

  return baseUrl;
}

/******************************************************************************
 * 统一文本分享API
 ******************************************************************************/

/**
 * 创建新的文本分享（管理员或API密钥用户）
 * @param {Object} pasteData - 文本分享数据
 * @param {string} pasteData.content - 分享内容
 * @param {string} [pasteData.slug] - 自定义链接后缀（可选）
 * @param {string} [pasteData.remark] - 备注信息（可选）
 * @param {string} [pasteData.expires_at] - 过期时间，ISO格式（可选）
 * @param {string} [pasteData.password] - 访问密码（可选）
 * @param {number} [pasteData.max_views] - 最大查看次数（可选）
 * @returns {Promise<Object>} 创建结果
 */
export function createPaste(pasteData) {
  // 使用post方法，client.js会自动处理认证头部
  return post("/paste", pasteData);
}

/**
 * 获取文本分享列表（统一接口，自动根据认证信息处理）
 * @param {number} [page=1] - 页码（管理员使用）
 * @param {number} [limit=10] - 每页数量
 * @param {number} [offset=0] - 偏移量（API密钥用户使用）
 * @param {Object} options - 额外查询选项（管理员可用）
 * @returns {Promise<Object>} 文本分享列表
 */
export function getPastes(page = 1, limit = 10, offset = 0, options = {}) {
  // 构建查询参数
  const params = new URLSearchParams();

  // 管理员使用page参数，API密钥用户使用limit和offset
  if (page > 1) {
    params.append("page", page.toString());
  }
  params.append("limit", limit.toString());
  if (offset > 0) {
    params.append("offset", offset.toString());
  }

  // 添加额外的查询选项（管理员可用）
  Object.keys(options).forEach((key) => {
    if (options[key] !== undefined && options[key] !== null) {
      params.append(key, options[key].toString());
    }
  });

  const queryString = params.toString();
  return get(`/pastes${queryString ? "?" + queryString : ""}`);
}

/**
 * 获取单个文本分享详情（统一接口，自动根据认证信息处理）
 * @param {string} id - 文本分享ID
 * @returns {Promise<Object>} 文本分享详情
 */
export function getPasteById(id) {
  return get(`/pastes/${id}`);
}

/**
 * 更新文本分享（统一接口，自动根据认证信息处理）
 * @param {string} slug - 文本分享的唯一标识
 * @param {Object} data - 更新的数据
 * @param {string} [data.content] - 新的文本内容
 * @param {string} [data.password] - 新的访问密码
 * @param {boolean} [data.clearPassword] - 是否清除密码
 * @param {string} [data.remark] - 新的备注
 * @param {string} [data.expires_at] - 新的过期时间
 * @param {number} [data.max_views] - 新的最大查看次数
 * @param {string} [data.newSlug] - 新的链接后缀，为空则自动生成
 * @returns {Promise<ApiResponse>} - API响应
 */
export function updatePaste(slug, data) {
  return put(`/pastes/${slug}`, data);
}

/**
 * 批量删除文本分享（统一接口，自动根据认证信息处理）
 * @param {string[]} ids - 文本分享ID数组
 * @returns {Promise<Object>} 删除结果
 */
export function batchDeletePastes(ids) {
  return del("/pastes/batch-delete", { ids });
}

/**
 * 清理所有过期的文本分享（仅管理员）
 * @returns {Promise<Object>} 清理结果
 */
export function clearExpiredPastes() {
  return post("/pastes/clear-expired", { clearExpired: true });
}
