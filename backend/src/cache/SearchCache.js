/**
 * 搜索缓存管理器
 * 提供搜索结果的缓存功能，用于提高频繁搜索的性能
 */
import crypto from "crypto";
import { BaseCache } from "./BaseCache.js";

class SearchCacheManager extends BaseCache {
  /**
   * 构造函数
   * @param {Object} options - 配置选项
   * @param {number} options.maxItems - 最大缓存项数量，默认为200
   * @param {number} options.prunePercentage - 清理时删除的缓存项百分比，默认为20%
   * @param {number} options.defaultTtl - 默认缓存时间（秒），默认为600秒（5分钟）
   */
  constructor(options = {}) {
    super({
      maxItems: options.maxItems || 200, 
      prunePercentage: options.prunePercentage || 20,
      defaultTtl: options.defaultTtl || 600, 
      name: "SearchCache",
      ...options,
    });
  }

  /**
   * 生成搜索缓存键 - 重写基类方法
   * @param {string} query - 搜索查询字符串
   * @param {Object} searchParams - 搜索参数对象
   * @param {string} userType - 用户类型 (admin/apiKey)
   * @param {string|Object} userIdOrInfo - 用户ID或API密钥信息
   * @returns {string} - 缓存键
   */
  generateKey(query, searchParams, userType, userIdOrInfo) {
    // 构建缓存键的组成部分
    const keyComponents = {
      query: query.toLowerCase().trim(),
      scope: searchParams.scope || "global",
      mountId: searchParams.mountId || "",
      path: searchParams.path || "",
      filters: searchParams.filters || {},
      userType: userType,
    };

    // 根据用户类型添加用户标识
    if (userType === "admin") {
      keyComponents.userId = userIdOrInfo;
    } else if (userType === "apiKey") {
      // 对于API密钥用户，使用basicPath作为权限标识
      keyComponents.basicPath = userIdOrInfo.basicPath || "";
      keyComponents.apiKeyId = userIdOrInfo.id || "";
    }

    // 将对象序列化并生成哈希
    const keyString = JSON.stringify(keyComponents, Object.keys(keyComponents).sort());
    const hash = crypto.createHash("sha256").update(keyString).digest("hex");

    // 返回带前缀的缓存键
    return `search:${hash.substring(0, 16)}`;
  }

  /**
   * 生成搜索缓存键 - 兼容性方法
   * @deprecated 使用 generateKey 方法
   */
  generateSearchKey(query, searchParams, userType, userIdOrInfo) {
    return this.generateKey(query, searchParams, userType, userIdOrInfo);
  }

  /**
   * 获取缓存的搜索结果 - 使用基类方法
   * @param {string} query - 搜索查询字符串
   * @param {Object} searchParams - 搜索参数对象
   * @param {string} userType - 用户类型
   * @param {string|Object} userIdOrInfo - 用户ID或API密钥信息
   * @returns {Object|null} - 缓存的搜索结果，如果缓存未命中则返回null
   */
  get(query, searchParams, userType, userIdOrInfo) {
    return super.get(query, searchParams, userType, userIdOrInfo);
  }

  /**
   * 设置搜索结果缓存 - 保持原有参数顺序，内部调用基类方法
   * @param {string} query - 搜索查询字符串
   * @param {Object} searchParams - 搜索参数对象
   * @param {string} userType - 用户类型
   * @param {string|Object} userIdOrInfo - 用户ID或API密钥信息
   * @param {Object} data - 要缓存的搜索结果数据
   * @param {number} ttlSeconds - 缓存的生存时间（秒），可选
   */
  set(query, searchParams, userType, userIdOrInfo, data, ttlSeconds = null) {
    // 保持原有参数顺序，内部调用基类方法时调整参数顺序
    super.set(data, ttlSeconds, query, searchParams, userType, userIdOrInfo);
  }

  /**
   * 获取额外的缓存数据 - 重写基类方法
   * @param {string} query - 搜索查询字符串
   * @param {Object} searchParams - 搜索参数对象
   * @param {string} userType - 用户类型
   * @param {string|Object} userIdOrInfo - 用户ID或API密钥信息
   * @returns {Object} 额外的缓存数据
   */
  getAdditionalCacheData(query, searchParams, userType, userIdOrInfo) {
    const additionalData = {
      query: query,
      searchParams: searchParams,
      userType: userType,
    };

    // 根据用户类型添加用户标识字段，用于invalidateUser方法
    if (userType === "admin") {
      additionalData.userId = typeof userIdOrInfo === "string" ? userIdOrInfo : userIdOrInfo.id;
    } else if (userType === "apiKey") {
      additionalData.basicPath = userIdOrInfo.basicPath || "";
      additionalData.apiKeyId = userIdOrInfo.id || "";
    }

    return additionalData;
  }

  /**
   * 使指定挂载点的所有搜索缓存失效
   * @param {string} mountId - 挂载点ID
   * @returns {number} - 被删除的缓存项数量
   */
  invalidateMount(mountId) {
    let count = 0;
    const keysToDelete = [];

    // 找出所有涉及该挂载点的搜索缓存
    for (const [key, cacheItem] of this.cache.entries()) {
      const { searchParams } = cacheItem;

      // 检查是否涉及该挂载点
      if (searchParams.mountId === mountId || (searchParams.scope === "global" && !searchParams.mountId)) {
        keysToDelete.push(key);
      }
    }

    // 删除找到的所有键
    for (const key of keysToDelete) {
      this.cache.delete(key);
      count++;
    }

    if (count > 0) {
      this.stats.invalidations += count;
      console.log(`搜索缓存已失效 - 挂载点:${mountId}, 删除项:${count}`);
    }

    return count;
  }

  /**
   * 使所有搜索缓存失效
   * @returns {number} - 被删除的缓存项数量
   */
  invalidateAll() {
    const count = this.cache.size;
    this.cache.clear();

    if (count > 0) {
      this.stats.invalidations += count;
      console.log(`所有搜索缓存已失效 - 删除项:${count}`);
    }

    return count;
  }

  /**
   * 使指定用户的所有搜索缓存失效
   * @param {string} userType - 用户类型
   * @param {string|Object} userIdOrInfo - 用户ID或API密钥信息
   * @returns {number} - 被删除的缓存项数量
   */
  invalidateUser(userType, userIdOrInfo) {
    let count = 0;
    const keysToDelete = [];

    // 找出所有属于该用户的搜索缓存
    for (const [key, cacheItem] of this.cache.entries()) {
      if (cacheItem.userType === userType) {
        let shouldDelete = false;

        if (userType === "admin") {
          // 对于管理员，直接比较用户ID
          const adminId = typeof userIdOrInfo === "string" ? userIdOrInfo : userIdOrInfo.id;
          // 正确访问：检查额外数据中的用户信息
          if (cacheItem.userId === adminId) {
            shouldDelete = true;
          }
        } else if (userType === "apiKey") {
          // 对于API密钥用户，比较basicPath和apiKeyId
          const apiKeyInfo = userIdOrInfo;
          // 正确访问：检查额外数据中的API密钥信息
          if (cacheItem.basicPath === apiKeyInfo.basicPath && cacheItem.apiKeyId === apiKeyInfo.id) {
            shouldDelete = true;
          }
        }

        if (shouldDelete) {
          keysToDelete.push(key);
        }
      }
    }

    // 删除找到的所有键
    for (const key of keysToDelete) {
      this.cache.delete(key);
      count++;
    }

    if (count > 0) {
      this.stats.invalidations += count;
      console.log(`用户搜索缓存已失效 - 用户类型:${userType}, 删除项:${count}`);
    }

    return count;
  }
}

// 创建单例实例
const searchCacheManager = new SearchCacheManager();

/**
 * 统一的搜索缓存清理函数
 * @param {Object} options - 清理选项
 * @param {string} [options.mountId] - 要清理的挂载点ID
 * @param {string} [options.userType] - 用户类型
 * @param {string|Object} [options.userIdOrInfo] - 用户ID或API密钥信息
 * @returns {number} 清除的缓存项数量
 */
export function clearSearchCache(options = {}) {
  const { mountId, userType, userIdOrInfo } = options;
  let totalCleared = 0;

  try {
    // 场景1: 清理指定挂载点的搜索缓存
    if (mountId) {
      const clearedCount = searchCacheManager.invalidateMount(mountId);
      console.log(`SearchCache: 已清理挂载点 ${mountId} 的搜索缓存，共 ${clearedCount} 项`);
      totalCleared += clearedCount;
    }

    // 场景2: 清理指定用户的搜索缓存
    if (userType && userIdOrInfo) {
      const clearedCount = searchCacheManager.invalidateUser(userType, userIdOrInfo);
      console.log(`SearchCache:已清理用户 ${userType} 的搜索缓存，共 ${clearedCount} 项`);
      totalCleared += clearedCount;
    }

    // 场景3: 清理所有搜索缓存
    if (!mountId && !userType) {
      const clearedCount = searchCacheManager.invalidateAll();
      totalCleared += clearedCount;
      console.log(`SearchCache:已清理所有搜索缓存，共 ${clearedCount} 项`);
    }

    return totalCleared;
  } catch (error) {
    console.error("清理搜索缓存时出错:", error);
    return 0;
  }
}

// 导出单例实例和类
export { searchCacheManager, SearchCacheManager };
