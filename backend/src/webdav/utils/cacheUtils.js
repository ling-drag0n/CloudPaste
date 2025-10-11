/**
 * WebDAV缓存工具函数
 * 用于在WebDAV操作后清理目录缓存
 */
import { clearDirectoryCache } from "../../cache/index.js";

/**
 * 在WebDAV操作后清理相关的目录缓存
 * @param {D1Database} db - D1数据库实例
 * @param {string} s3SubPath - S3中的子路径
 * @param {Object} s3Config - S3配置对象
 * @param {boolean} isDirectory - 是否为目录操作
 * @param {string} mountId - 挂载点ID（可选，如果提供则直接使用）
 */
export async function clearCacheAfterWebDAVOperation(db, s3SubPath, s3Config, isDirectory = false, mountId = null) {
  try {
    if (!db || !s3Config || !s3Config.id) {
      console.warn("无法清理缓存，参数不完整");
      return;
    }

    // 如果提供了挂载点ID，直接清理该挂载点的缓存
    if (mountId) {
      const clearedCount = await clearDirectoryCache({ mountId });
      console.log(`WebDAV操作后缓存已清理 - 挂载点ID: ${mountId}, 路径: ${s3SubPath}, 清理项: ${clearedCount}`);
      return;
    }

    // 否则通过S3配置ID查找并清理所有相关挂载点的缓存
    const clearedCount = await clearDirectoryCache({ db, s3ConfigId: s3Config.id });
    console.log(`WebDAV操作后缓存已清理 - S3配置ID: ${s3Config.id}, 路径: ${s3SubPath}, 清理项: ${clearedCount}`);
  } catch (error) {
    // 记录错误但不抛出，避免影响主要操作
    console.error("WebDAV缓存清理错误:", error);
  }
}
