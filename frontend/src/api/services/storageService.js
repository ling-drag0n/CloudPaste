/**
 * 存储配置服务API
 * 统一管理所有S3存储配置相关的API调用
 */

import { get, post, put, del } from "../client";

/******************************************************************************
 * S3存储配置管理API
 ******************************************************************************/

/**
 * 获取所有S3存储配置（支持分页）
 * @param {Object} params - 查询参数
 * @param {number} [params.page] - 页码
 * @param {number} [params.limit] - 每页数量
 * @returns {Promise<Object>} S3配置列表和分页信息
 */
export function getAllS3Configs(params = {}) {
  const queryParams = new URLSearchParams();

  if (params.page) {
    queryParams.append("page", params.page.toString());
  }
  if (params.limit) {
    queryParams.append("limit", params.limit.toString());
  }

  const queryString = queryParams.toString();
  return get(`/s3-configs${queryString ? `?${queryString}` : ""}`);
}

/**
 * 获取单个S3存储配置详情
 * @param {string} id - 配置ID
 * @returns {Promise<Object>} S3配置详情
 */
export function getS3Config(id) {
  return get(`/s3-configs/${id}`);
}

/**
 * 创建S3存储配置
 * @param {Object} config - S3配置信息
 * @param {string} config.name - 配置名称
 * @param {string} config.provider_type - 提供商类型，如'Cloudflare R2'
 * @param {string} config.endpoint_url - S3 API端点URL
 * @param {string} config.bucket_name - 存储桶名称
 * @param {string} [config.region] - 存储桶区域
 * @param {string} config.access_key_id - 访问密钥ID
 * @param {string} config.secret_access_key - 秘密访问密钥
 * @param {boolean} [config.path_style] - 是否使用路径样式访问
 * @param {string} [config.default_folder] - 默认上传文件夹路径
 * @param {boolean} [config.is_public] - 是否允许API密钥访问（默认为false）
 * @returns {Promise<Object>} 创建结果
 */
export function createS3Config(config) {
  return post("/s3-configs", config);
}

/**
 * 更新S3存储配置
 * @param {string} id - 配置ID
 * @param {Object} config - 要更新的配置信息
 * @returns {Promise<Object>} 更新结果
 */
export function updateS3Config(id, config) {
  return put(`/s3-configs/${id}`, config);
}

/**
 * 删除S3存储配置
 * @param {string} id - 要删除的配置ID
 * @returns {Promise<Object>} 删除结果
 */
export function deleteS3Config(id) {
  return del(`/s3-configs/${id}`);
}

/**
 * 设置默认S3存储配置
 * @param {string} id - 要设置为默认的配置ID
 * @returns {Promise<Object>} 设置结果
 */
export function setDefaultS3Config(id) {
  return put(`/s3-configs/${id}/set-default`);
}

/**
 * 测试S3存储配置连接
 * @param {string} id - 配置ID
 * @returns {Promise<Object>} 测试结果
 */
export function testS3Config(id) {
  return post(`/s3-configs/${id}/test`);
}

// 兼容性导出 - 保持向后兼容
export const getS3Configs = getAllS3Configs;
