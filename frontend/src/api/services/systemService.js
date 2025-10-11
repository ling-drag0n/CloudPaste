/**
 * 系统管理服务API
 * 统一管理所有系统相关的API调用，包括系统设置、仪表盘统计等
 */

import { get, post, put } from "../client";

/******************************************************************************
 * 仪表盘统计API
 ******************************************************************************/

/**
 * 获取仪表盘统计数据
 * @returns {Promise<Object>} 包含文本和文件数量及存储空间使用情况的统计数据
 */
export function getDashboardStats() {
  return get("/admin/dashboard/stats");
}

/******************************************************************************
 * 系统信息API
 ******************************************************************************/

/**
 * 获取系统最大上传大小限制
 * @returns {Promise<number>} 最大上传大小(MB)
 */
export async function getMaxUploadSize() {
  try {
    const response = await get("system/max-upload-size");
    if (response && response.data && response.data.max_upload_size) {
      return response.data.max_upload_size;
    }
    return 100; // 默认值
  } catch (error) {
    console.error("获取最大上传大小失败:", error);
    return 100; // 出错时返回默认值
  }
}

/******************************************************************************
 * 分组设置管理API
 ******************************************************************************/

/**
 * 按分组获取设置项
 * @param {number} groupId - 分组ID
 * @param {boolean} includeMetadata - 是否包含元数据
 * @returns {Promise<Object>} 分组设置响应
 */
export function getSettingsByGroup(groupId, includeMetadata = true) {
  const params = new URLSearchParams();
  params.append("group", groupId.toString());
  if (!includeMetadata) {
    params.append("metadata", "false");
  }
  return get(`/admin/settings?${params.toString()}`);
}

/**
 * 获取所有分组的设置项
 * @param {boolean} includeSystemGroup - 是否包含系统内部分组
 * @returns {Promise<Object>} 所有分组设置响应
 */
export function getAllSettingsByGroups(includeSystemGroup = false) {
  const params = new URLSearchParams();
  if (includeSystemGroup) {
    params.append("includeSystem", "true");
  }
  return get(`/admin/settings?${params.toString()}`);
}

/**
 * 获取分组列表和统计信息
 * @returns {Promise<Object>} 分组信息响应
 */
export function getGroupsInfo() {
  return get("/admin/settings/groups");
}

/**
 * 获取设置项元数据
 * @param {string} key - 设置键名
 * @returns {Promise<Object>} 设置元数据响应
 */
export function getSettingMetadata(key) {
  return get(`/admin/settings/metadata?key=${encodeURIComponent(key)}`);
}

/**
 * 按分组批量更新设置
 * @param {number} groupId - 分组ID
 * @param {Object} settings - 设置键值对
 * @param {boolean} validateType - 是否进行类型验证
 * @returns {Promise<Object>} 更新结果
 */
export function updateGroupSettings(groupId, settings, validateType = true) {
  const params = validateType ? "" : "?validate=false";
  return put(`/admin/settings/group/${groupId}${params}`, settings);
}

/******************************************************************************
 * 系统维护API
 ******************************************************************************/

/**
 * 清理所有过期的文本分享（仅管理员）
 * @returns {Promise<Object>} 清理结果
 */
export function clearExpiredPastes() {
  return post("/pastes/clear-expired", { clearExpired: true });
}

/**
 * 清理目录缓存（管理员）
 * @param {Object} options - 清理选项
 * @param {string} [options.mountId] - 要清理的挂载点ID
 * @param {string} [options.s3ConfigId] - S3配置ID
 * @returns {Promise<Object>} 清理结果
 */
export function clearCacheAdmin(options = {}) {
  return post("/admin/cache/clear", options);
}

/**
 * 清理目录缓存（API密钥用户）
 * @param {Object} options - 清理选项
 * @param {string} [options.mountId] - 要清理的挂载点ID
 * @param {string} [options.s3ConfigId] - S3配置ID
 * @returns {Promise<Object>} 清理结果
 */
export function clearCacheUser(options = {}) {
  return post("/user/cache/clear", options);
}

/**
 * 获取缓存统计信息
 * @returns {Promise<Object>} 缓存统计数据
 */
export function getCacheStats() {
  return get("/admin/cache/stats");
}

/**
 * 系统健康检查
 * @returns {Promise<Object>} 健康检查结果
 */
export function healthCheck() {
  return get("/health");
}

/**
 * 获取系统版本信息
 * @returns {Promise<Object>} 版本信息
 */
export function getVersionInfo() {
  return get("/version");
}
