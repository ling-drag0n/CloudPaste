/**
 * S3预签名URL缓存管理器
 * 用于缓存S3预签名URL
 */
import { BaseCache } from "./BaseCache.js";

class S3UrlCacheManager extends BaseCache {
  /**
   * 构造函数
   * @param {Object} options - 配置选项
   */
  constructor(options = {}) {
    super({
      maxItems: options.maxItems || 1000,
      prunePercentage: options.prunePercentage || 20,
      defaultTtl: options.defaultTtl || 3600,
      name: "S3UrlCache",
      ...options,
    });

    // S3UrlCache特有的配置
    this.s3Config = {
      customHostTtl: options.customHostTtl || 86400 * 7, // 自定义域名TTL(7天)
    };
  }

  /**
   * 生成缓存键
   * @param {string} s3ConfigId - S3配置ID
   * @param {string} storagePath - 存储路径
   * @param {boolean} forceDownload - 是否强制下载
   * @param {string} userType - 用户类型(admin/apiKey)
   * @param {string} userId - 用户ID
   * @returns {string} 缓存键
   */
  generateKey(s3ConfigId, storagePath, forceDownload, userType, userId) {
    // 参数验证
    if (!s3ConfigId || !storagePath || !userType || !userId) {
      throw new Error(`缓存键生成失败：缺少必要参数 s3ConfigId=${s3ConfigId}, storagePath=${storagePath}, userType=${userType}, userId=${userId}`);
    }

    // 多租户隔离：不同用户类型和ID的缓存分离
    const userScope = `${userType}:${userId}`;
    const downloadFlag = forceDownload ? "dl" : "pv"; // download/preview
    const encodedPath = Buffer.from(storagePath).toString("base64");
    return `s3url:${s3ConfigId}:${userScope}:${downloadFlag}:${encodedPath}`;
  }

  /**
   * 生成缓存键
   * @deprecated 使用 generateKey 方法
   */
  generateCacheKey(s3ConfigId, storagePath, forceDownload, userType, userId) {
    return this.generateKey(s3ConfigId, storagePath, forceDownload, userType, userId);
  }

  /**
   * 获取缓存的预签名URL - 重写基类方法，保持错误处理和返回URL
   * @param {string} s3ConfigId - S3配置ID
   * @param {string} storagePath - 存储路径
   * @param {boolean} forceDownload - 是否强制下载
   * @param {string} userType - 用户类型
   * @param {string} userId - 用户ID
   * @returns {string|null} 缓存的URL或null
   */
  get(s3ConfigId, storagePath, forceDownload, userType, userId) {
    try {
      const key = this.generateKey(s3ConfigId, storagePath, forceDownload, userType, userId);
      const cacheItem = this.cache.get(key);

      if (!cacheItem) {
        this.stats.misses++;
        return null;
      }

      // 检查缓存是否过期
      if (Date.now() > cacheItem.expiresAt) {
        this.cache.delete(key);
        this.stats.expired++;
        return null;
      }

      // LRU策略：更新最后访问时间
      cacheItem.lastAccessed = Date.now();
      this.cache.set(key, cacheItem);

      this.stats.hits++;
      // S3UrlCache特殊逻辑：返回url字段而不是data字段
      return cacheItem.url;
    } catch (error) {
      console.warn("S3URL缓存获取失败:", error.message);
      this.stats.misses++;
      return null;
    }
  }

  /**
   * 设置预签名URL缓存 - 完全重写，保持原有数据结构和智能TTL计算
   * @param {string} s3ConfigId - S3配置ID
   * @param {string} storagePath - 存储路径
   * @param {boolean} forceDownload - 是否强制下载
   * @param {string} userType - 用户类型
   * @param {string} userId - 用户ID
   * @param {string} url - 预签名URL
   * @param {Object} s3Config - S3配置对象
   */
  set(s3ConfigId, storagePath, forceDownload, userType, userId, url, s3Config) {
    try {
      const key = this.generateKey(s3ConfigId, storagePath, forceDownload, userType, userId);
      const now = Date.now();

      // S3UrlCache特殊逻辑：智能TTL计算
      let ttl;
      // 判断是否为自定义域名且非强制下载（即预览模式）
      const isCustomHostPreview = s3Config.custom_host && !forceDownload;

      if (isCustomHostPreview) {
        // 自定义域名预览：长期缓存(7天)，因为不会过期
        ttl = this.s3Config.customHostTtl;
      } else {
        // 预签名URL或自定义域名强制下载（包含查询参数）：使用S3配置的过期时间，但留10%缓冲时间
        const configTtl = s3Config.signature_expires_in || this.config.defaultTtl;
        ttl = Math.floor(configTtl * 0.9); // 90%的有效期，避免边界过期
      }

      const expiresAt = now + ttl * 1000;

      // 检查缓存项数量限制，必要时清理
      this.checkSizeAndPrune();

      // S3UrlCache特殊数据结构：保持原有结构，直接存储url字段
      this.cache.set(key, {
        url,
        expiresAt,
        lastAccessed: now,
        s3ConfigId,
        userType,
        userId,
        isCustomHost: isCustomHostPreview,
      });
    } catch (error) {
      console.warn("S3URL缓存设置失败:", error.message);
      // 缓存失败不应该影响主流程，静默处理
    }
  }

  /**
   * 清理指定S3配置的所有缓存
   * @param {string} s3ConfigId - S3配置ID
   * @returns {number} 清理的缓存项数量
   */
  invalidateS3Config(s3ConfigId) {
    let clearedCount = 0;

    for (const [key, item] of this.cache.entries()) {
      if (item.s3ConfigId === s3ConfigId) {
        this.cache.delete(key);
        clearedCount++;
      }
    }

    this.stats.invalidations += clearedCount;
    return clearedCount;
  }

  /**
   * 清理指定用户的所有缓存
   * @param {string} userType - 用户类型
   * @param {string} userId - 用户ID
   * @returns {number} 清理的缓存项数量
   */
  invalidateUser(userType, userId) {
    let clearedCount = 0;

    for (const [key, item] of this.cache.entries()) {
      if (item.userType === userType && item.userId === userId) {
        this.cache.delete(key);
        clearedCount++;
      }
    }

    this.stats.invalidations += clearedCount;
    return clearedCount;
  }
}

// 创建单例实例
const s3UrlCacheManager = new S3UrlCacheManager();

/**
 * 统一的S3URL缓存清理函数
 * @param {Object} options - 清理选项
 * @param {string} [options.s3ConfigId] - S3配置ID
 * @param {string} [options.userType] - 用户类型
 * @param {string} [options.userId] - 用户ID
 * @returns {number} 清除的缓存项数量
 */
export async function clearS3UrlCache(options = {}) {
  const { s3ConfigId, userType, userId } = options;
  let totalCleared = 0;

  try {
    if (s3ConfigId) {
      totalCleared = s3UrlCacheManager.invalidateS3Config(s3ConfigId);
      console.log(`已清理S3配置 ${s3ConfigId} 的URL缓存，共 ${totalCleared} 项`);
    } else if (userType && userId) {
      totalCleared = s3UrlCacheManager.invalidateUser(userType, userId);
      console.log(`已清理用户 ${userType}:${userId} 的URL缓存，共 ${totalCleared} 项`);
    } else {
      totalCleared = s3UrlCacheManager.invalidateAll();
      console.log(`已清理所有S3URL缓存，共 ${totalCleared} 项`);
    }

    return totalCleared;
  } catch (error) {
    console.error("清理S3URL缓存时出错:", error);
    return 0;
  }
}

// 导出单例实例和类
export { s3UrlCacheManager, S3UrlCacheManager };
