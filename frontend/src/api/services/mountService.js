/**
 * 挂载点管理服务API
 * 统一管理所有挂载点相关的API调用，包括管理员和API密钥用户的操作
 */

import { get, post, put, del } from "../client";

/******************************************************************************
 * 管理员挂载点管理API
 ******************************************************************************/

/**
 * 获取挂载点列表（统一API）
 * 根据用户权限自动返回相应数据：
 * - 管理员：所有挂载点（包括禁用的）
 * - API密钥用户：有权限的活跃挂载点
 * @returns {Promise<Object>} 挂载点列表响应对象
 */
export async function getMountsList() {
  return get("/mount/list");
}

/**
 * 创建新挂载点（仅管理员）
 * @param {Object} mountData 挂载点数据
 * @returns {Promise<Object>} 创建结果响应对象
 */
export async function createMount(mountData) {
  return post("/mount/create", mountData);
}

/**
 * 更新挂载点（仅管理员）
 * @param {string} id 挂载点ID
 * @param {Object} mountData 挂载点更新数据
 * @returns {Promise<Object>} 更新结果响应对象
 */
export async function updateMount(id, mountData) {
  return put(`/mount/${id}`, mountData);
}

/**
 * 删除挂载点（仅管理员）
 * @param {string} id 挂载点ID
 * @returns {Promise<Object>} 删除结果响应对象
 */
export async function deleteMount(id) {
  return del(`/mount/${id}`);
}

// 兼容性导出 - 保持向后兼容

