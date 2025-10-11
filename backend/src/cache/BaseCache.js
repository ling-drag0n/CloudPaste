/**
 * 基础缓存管理器
 * 提供通用的缓存功能
 */

export class BaseCache {
  /**
   * 构造函数
   * @param {Object} options - 配置选项
   * @param {number} options.maxItems - 最大缓存项数量
   * @param {number} options.prunePercentage - 清理时删除的缓存项百分比
   * @param {number} options.defaultTtl - 默认缓存时间（秒）
   * @param {string} options.name - 缓存名称（用于日志）
   */
  constructor(options = {}) {
    // 默认配置
    this.config = {
      maxItems: options.maxItems || 500,
      prunePercentage: options.prunePercentage || 20,
      defaultTtl: options.defaultTtl || 300,
      name: options.name || "BaseCache",
    };

    // 缓存存储
    this.cache = new Map();

    // 统计信息
    this.stats = {
      hits: 0,
      misses: 0,
      expired: 0,
      invalidations: 0,
      pruned: 0,
    };
  }

  /**
   * 生成缓存键 - 抽象方法，子类必须实现
   * @param {...any} params - 缓存键参数
   * @returns {string} 缓存键
   */
  generateKey(...params) {
    throw new Error("generateKey方法必须在子类中实现");
  }

  /**
   * 获取缓存项
   * @param {...any} params - 缓存键参数
   * @returns {any|null} 缓存的数据，如果缓存未命中则返回null
   */
  get(...params) {
    const key = this.generateKey(...params);
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
    this.cache.set(key, cacheItem); // 重新设置以更新Map中的顺序

    this.stats.hits++;
    return cacheItem.data;
  }

  /**
   * 设置缓存项
   * @param {any} data - 要缓存的数据
   * @param {number} ttlSeconds - 缓存的生存时间（秒），可选
   * @param {...any} params - 缓存键参数
   */
  set(data, ttlSeconds = null, ...params) {
    const key = this.generateKey(...params);
    const now = Date.now();
    const ttl = ttlSeconds || this.config.defaultTtl;
    const expiresAt = now + ttl * 1000;

    // 更新缓存
    this.cache.set(key, {
      data,
      expiresAt,
      lastAccessed: now,
      ...this.getAdditionalCacheData(...params), // 子类可以添加额外数据
    });

    // 检查是否需要清理缓存
    if (this.cache.size > this.config.maxItems) {
      this.prune();
    }
  }

  /**
   * 获取额外的缓存数据 - 子类可以重写
   * @param {...any} params - 缓存键参数
   * @returns {Object} 额外的缓存数据
   */
  getAdditionalCacheData(...params) {
    return {};
  }

  /**
   * 使指定键的缓存失效
   * @param {...any} params - 缓存键参数
   * @returns {boolean} 是否成功删除
   */
  invalidate(...params) {
    const key = this.generateKey(...params);
    const deleted = this.cache.delete(key);
    if (deleted) {
      this.stats.invalidations++;
    }
    return deleted;
  }

  /**
   * 清理所有缓存
   * @returns {number} 清理的缓存项数量
   */
  invalidateAll() {
    const clearedCount = this.cache.size;
    this.cache.clear();
    this.stats.invalidations += clearedCount;
    return clearedCount;
  }

  /**
   * 清理过期的缓存项或过多的缓存项
   */
  prune() {
    const now = Date.now();
    const entries = [...this.cache.entries()];
    let prunedCount = 0;

    // 找出已过期的项目
    const expiredEntries = entries.filter(([_, item]) => now > item.expiresAt);

    // 如果有足够的过期项目，直接清理它们
    if (expiredEntries.length >= Math.ceil((entries.length * this.config.prunePercentage) / 100)) {
      for (const [key] of expiredEntries) {
        this.cache.delete(key);
        prunedCount++;
      }
    } else {
      // 否则，使用LRU策略：按最后访问时间排序并删除最久未访问的项目
      entries.sort((a, b) => a[1].lastAccessed - b[1].lastAccessed);

      const toDelete = Math.ceil((entries.length * this.config.prunePercentage) / 100);
      for (let i = 0; i < toDelete; i++) {
        if (i < entries.length) {
          const [key] = entries[i];
          this.cache.delete(key);
          prunedCount++;
        }
      }
    }

    // 更新统计信息
    if (prunedCount > 0) {
      this.stats.pruned += prunedCount;
      console.log(`${this.config.name}缓存清理完成 - 删除项:${prunedCount}`);
    }
  }

  /**
   * 获取缓存统计信息
   * @returns {Object} 缓存统计数据，包括命中率和大小
   */
  getStats() {
    return {
      ...this.stats,
      cacheSize: this.cache.size,
      hitRate: this.stats.hits / (this.stats.hits + this.stats.misses) || 0,
      name: this.config.name,
    };
  }

  /**
   * 检查缓存大小并在必要时清理
   */
  checkSizeAndPrune() {
    if (this.cache.size > this.config.maxItems) {
      this.prune();
    }
  }

  /**
   * 获取缓存大小
   * @returns {number} 当前缓存项数量
   */
  size() {
    return this.cache.size;
  }

  /**
   * 检查缓存是否为空
   * @returns {boolean} 缓存是否为空
   */
  isEmpty() {
    return this.cache.size === 0;
  }

  /**
   * 获取所有缓存键
   * @returns {Array<string>} 所有缓存键的数组
   */
  keys() {
    return Array.from(this.cache.keys());
  }

  /**
   * 检查指定键是否存在于缓存中
   * @param {...any} params - 缓存键参数
   * @returns {boolean} 键是否存在
   */
  has(...params) {
    const key = this.generateKey(...params);
    const cacheItem = this.cache.get(key);

    if (!cacheItem) {
      return false;
    }

    // 检查是否过期
    if (Date.now() > cacheItem.expiresAt) {
      this.cache.delete(key);
      this.stats.expired++;
      return false;
    }

    return true;
  }
}

/**
 * 缓存工厂函数 - 用于创建特定类型的缓存实例
 * @param {string} type - 缓存类型
 * @param {Object} options - 配置选项
 * @returns {BaseCache} 缓存实例
 */
export function createCache(type, options = {}) {
  const defaultConfigs = {
    directory: { maxItems: 300, defaultTtl: 300, name: "DirectoryCache" }, 
    search: { maxItems: 200, defaultTtl: 600, name: "SearchCache" }, 
    s3url: { maxItems: 1000, defaultTtl: 3600, name: "S3UrlCache" }, 
  };

  const config = { ...defaultConfigs[type], ...options };
  return new BaseCache(config);
}
