/**
 * 目录缓存管理器 - 基于BaseCache的目录列表缓存实现
 * 提供目录列表的缓存功能，用于提高频繁访问目录的性能
 */
import { BaseCache } from "./BaseCache.js";
import { clearS3UrlCache } from "./S3UrlCache.js";
import { clearSearchCache } from "./SearchCache.js";
import { normalizePath } from "../storage/fs/utils/PathResolver.js";

class DirectoryCacheManager extends BaseCache {
  /**
   * 构造函数
   * @param {Object} options - 配置选项
   * @param {number} options.maxItems - 最大缓存项数量，默认为500
   * @param {number} options.maxMemoryMB - 最大内存使用量(MB)，默认为100
   * @param {number} options.prunePercentage - 清理时删除的缓存项百分比，默认为20%
   */
  constructor(options = {}) {
    super({
      maxItems: options.maxItems || 300, 
      prunePercentage: options.prunePercentage || 20,
      defaultTtl: 300, 
      name: "DirectoryCache",
      ...options,
    });
  }

  /**
   * 生成安全的缓存键 - 重写基类方法
   * @param {string} mountId - 挂载点ID
   * @param {string} path - 目录路径
   * @returns {string} - 缓存键
   */
  generateKey(mountId, path) {
    // 规范化路径：确保目录路径的一致性
    // 对于目录缓存，统一将路径规范化为目录格式（以/结尾）
    const normalizedPath = normalizePath(path, true);

    // 使用 Base64 编码路径，避免特殊字符问题
    const encodedPath = Buffer.from(normalizedPath).toString("base64");
    return `${mountId}:${encodedPath}`;
  }

  /**
   * 获取缓存的目录列表 - 使用基类方法
   * @param {string} mountId - 挂载点ID
   * @param {string} path - 目录路径
   * @returns {Object|null} - 缓存的目录列表，如果缓存未命中则返回null
   */
  get(mountId, path) {
    return super.get(mountId, path);
  }

  /**
   * 设置目录列表缓存 - 保持原有参数顺序，内部调用基类方法
   * @param {string} mountId - 挂载点ID
   * @param {string} path - 目录路径
   * @param {Object} data - 要缓存的目录列表数据
   * @param {number} ttlSeconds - 缓存的生存时间（秒）
   */
  set(mountId, path, data, ttlSeconds) {
    // 保持原有参数顺序，内部调用基类方法时调整参数顺序
    super.set(data, ttlSeconds, mountId, path);
  }

  /**
   * 使指定目录的缓存失效 - 使用基类方法
   * @param {string} mountId - 挂载点ID
   * @param {string} path - 目录路径
   * @returns {boolean} - 如果缓存项存在并被删除则返回true，否则返回false
   */
  invalidate(mountId, path) {
    const deleted = super.invalidate(mountId, path);
    if (deleted) {
      console.log(`目录缓存已失效 - 挂载点:${mountId}, 路径:${path}`);
    }
    return deleted;
  }

  /**
   * 使指定路径及其所有父路径的缓存失效
   * 例如: 对于路径 /a/b/c，会使 /a/b/c、/a/b 和 /a 的缓存失效
   * @param {string} mountId - 挂载点ID
   * @param {string} path - 目录路径
   * @returns {number} - 被删除的缓存项数量
   */
  invalidatePathAndAncestors(mountId, path) {
    // 确保路径格式标准化
    const normalizedPath = path.startsWith("/") ? path : `/${path}`;
    let currentPath = normalizedPath;
    let count = 0;

    // 清除当前路径的缓存
    if (this.invalidate(mountId, currentPath)) {
      count++;
    }

    // 逐级向上清除父路径的缓存
    while (currentPath !== "/" && currentPath.includes("/")) {
      // 获取父路径
      currentPath = currentPath.substring(0, currentPath.lastIndexOf("/"));
      if (currentPath === "") currentPath = "/";

      // 清除父路径的缓存
      if (this.invalidate(mountId, currentPath)) {
        count++;
      }

      // 如果已经到达根路径，停止循环
      if (currentPath === "/") break;
    }

    if (count > 0) {
      console.log(`路径及父路径缓存已失效 - 挂载点:${mountId}, 路径:${path}, 删除项:${count}`);
    }

    return count;
  }

  /**
   * 清理指定挂载点的所有缓存
   * @param {string} mountId - 挂载点ID
   * @returns {number} 清理的缓存项数量
   */
  invalidateMount(mountId) {
    let clearedCount = 0;

    // 遍历所有缓存项，删除匹配挂载点的项
    for (const [key, item] of this.cache.entries()) {
      // 缓存键格式：mountId:encodedPath
      if (key.startsWith(`${mountId}:`)) {
        this.cache.delete(key);
        clearedCount++;
      }
    }

    if (clearedCount > 0) {
      this.stats.invalidations += clearedCount;
      console.log(`挂载点缓存已失效 - 挂载点:${mountId}, 删除项:${clearedCount}`);
    }

    return clearedCount;
  }

  // prune() 和 getStats() 方法已由基类提供，无需重复实现
}

// 创建单例实例
const directoryCacheManager = new DirectoryCacheManager();

/**
 * 统一的缓存清理函数 - 可根据挂载点ID或S3配置ID清理缓存
 * @param {Object} options - 清理选项
 * @param {string} [options.mountId] - 要清理的挂载点ID
 * @param {D1Database} [options.db] - 数据库连接（当使用s3ConfigId时必需）
 * @param {string} [options.s3ConfigId] - S3配置ID，将清理所有关联的挂载点
 * @returns {Promise<number>} 清除的缓存项数量
 */
export async function clearDirectoryCache(options = {}) {
  const { mountId, db, s3ConfigId } = options;
  let totalCleared = 0;

  try {
    // 场景1: 直接提供挂载点ID - 清理单个挂载点
    if (mountId) {
      const clearedCount = directoryCacheManager.invalidateMount(mountId);
      totalCleared += clearedCount;

      // 同时清理该挂载点的搜索缓存
      let searchCleared = 0;
      try {
        searchCleared = clearSearchCache({ mountId });
        totalCleared += searchCleared;
      } catch (error) {
        console.warn("清理搜索缓存失败:", error);
      }

      console.log(`已清理挂载点 ${mountId} 的缓存，目录缓存: ${clearedCount} 项，搜索缓存: ${searchCleared} 项`);
    }

    // 场景2: 提供S3配置ID - 查找并清理所有关联挂载点
    if (db && s3ConfigId) {
      // 获取与S3配置相关的所有挂载点
      const mounts = await db
        .prepare(
          `SELECT m.id
           FROM storage_mounts m
           WHERE m.storage_type = 'S3' AND m.storage_config_id = ?`
        )
        .bind(s3ConfigId)
        .all();

      if (!mounts?.results?.length) {
        console.log(`未找到与S3配置 ${s3ConfigId} 关联的挂载点`);
      } else {
        // 清理每个关联挂载点的缓存
        let searchTotalCleared = 0;
        for (const mount of mounts.results) {
          const clearedCount = directoryCacheManager.invalidateMount(mount.id);
          totalCleared += clearedCount;

          // 同时清理该挂载点的搜索缓存
          try {
            const searchCleared = clearSearchCache({ mountId: mount.id });
            totalCleared += searchCleared;
            searchTotalCleared += searchCleared;
          } catch (error) {
            console.warn(`清理挂载点 ${mount.id} 搜索缓存失败:`, error);
          }
        }

        if (totalCleared > 0) {
          console.log(`已清理 ${mounts.results.length} 个挂载点的缓存，目录缓存: ${totalCleared - searchTotalCleared} 项，搜索缓存: ${searchTotalCleared} 项`);
        }
      }

      // 清理S3URL缓存
      try {
        const s3UrlCleared = await clearS3UrlCache({ s3ConfigId });
        totalCleared += s3UrlCleared;
        console.log(`已清理S3配置 ${s3ConfigId} 的URL缓存，共 ${s3UrlCleared} 项`);
      } catch (error) {
        console.warn("清理S3URL缓存失败:", error);
      }
    }

    // 如果没有提供有效参数，清理所有缓存
    if (!mountId && !s3ConfigId) {
      const dirCleared = directoryCacheManager.invalidateAll();
      totalCleared += dirCleared;

      let s3UrlCleared = 0;
      try {
        s3UrlCleared = await clearS3UrlCache();
        totalCleared += s3UrlCleared;
      } catch (error) {
        console.warn("清理S3URL缓存失败:", error);
      }

      let searchCleared = 0;
      try {
        searchCleared = clearSearchCache();
        totalCleared += searchCleared;
      } catch (error) {
        console.warn("清理搜索缓存失败:", error);
      }

      console.log(`已清理所有缓存：目录缓存 ${dirCleared} 项，URL缓存 ${s3UrlCleared} 项，搜索缓存 ${searchCleared} 项`);
    }

    return totalCleared;
  } catch (error) {
    console.error("清理缓存时出错:", error);
    return 0;
  }
}

// 导出单例实例和类 (单例用于实际应用，类用于测试和特殊场景)
export { directoryCacheManager, DirectoryCacheManager };
