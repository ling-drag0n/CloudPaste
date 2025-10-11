/**
 * LRU (Least Recently Used) 缓存实现
 * 用于优化 Office 预览 URL 缓存策略
 */
export class LRUCache {
  constructor(maxSize = 50) {
    this.maxSize = maxSize;
    this.cache = new Map();
  }

  /**
   * 获取缓存项
   * @param {string} key - 缓存键
   * @returns {any} 缓存值，如果不存在或已过期则返回 null
   */
  get(key) {
    if (!this.cache.has(key)) {
      return null;
    }

    const item = this.cache.get(key);
    
    // 检查是否过期
    if (item.expiry && Date.now() > item.expiry) {
      this.cache.delete(key);
      return null;
    }

    // 移动到最后（最近使用）
    this.cache.delete(key);
    this.cache.set(key, item);
    
    return item.value;
  }

  /**
   * 设置缓存项
   * @param {string} key - 缓存键
   * @param {any} value - 缓存值
   * @param {number} ttl - 生存时间（毫秒），可选
   */
  set(key, value, ttl = null) {
    // 如果已存在，先删除
    if (this.cache.has(key)) {
      this.cache.delete(key);
    }
    // 如果达到最大容量，删除最旧的项
    else if (this.cache.size >= this.maxSize) {
      const firstKey = this.cache.keys().next().value;
      this.cache.delete(firstKey);
    }

    const item = {
      value,
      timestamp: Date.now(),
      expiry: ttl ? Date.now() + ttl : null
    };

    this.cache.set(key, item);
  }

  /**
   * 检查是否存在缓存项
   * @param {string} key - 缓存键
   * @returns {boolean}
   */
  has(key) {
    return this.get(key) !== null;
  }

  /**
   * 删除缓存项
   * @param {string} key - 缓存键
   * @returns {boolean} 是否成功删除
   */
  delete(key) {
    return this.cache.delete(key);
  }

  /**
   * 清空缓存
   */
  clear() {
    this.cache.clear();
  }

  /**
   * 获取缓存大小
   * @returns {number}
   */
  size() {
    return this.cache.size;
  }

  /**
   * 清理过期项
   * @returns {number} 清理的项目数量
   */
  cleanup() {
    let cleanedCount = 0;
    const now = Date.now();
    
    for (const [key, item] of this.cache.entries()) {
      if (item.expiry && now > item.expiry) {
        this.cache.delete(key);
        cleanedCount++;
      }
    }
    
    return cleanedCount;
  }

  /**
   * 获取缓存统计信息
   * @returns {Object} 统计信息
   */
  getStats() {
    const now = Date.now();
    let expiredCount = 0;
    let totalAge = 0;
    
    for (const [key, item] of this.cache.entries()) {
      if (item.expiry && now > item.expiry) {
        expiredCount++;
      }
      totalAge += now - item.timestamp;
    }
    
    return {
      size: this.cache.size,
      maxSize: this.maxSize,
      expiredCount,
      averageAge: this.cache.size > 0 ? Math.round(totalAge / this.cache.size) : 0,
      utilizationRate: Math.round((this.cache.size / this.maxSize) * 100)
    };
  }
}

// 创建全局 Office 预览缓存实例
export const officePreviewCache = new LRUCache(30); // 最多缓存 30 个 Office 预览 URL

// 定期清理过期缓存（每 5 分钟）
setInterval(() => {
  const cleaned = officePreviewCache.cleanup();
  if (cleaned > 0) {
    console.log(`清理了 ${cleaned} 个过期的 Office 预览缓存项`);
  }
}, 5 * 60 * 1000);
