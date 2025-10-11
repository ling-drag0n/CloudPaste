/**
 * 预览设置缓存工具
 * 文件类型检测缓存机制
 */

import { RepositoryFactory } from "../repositories/index.js";
import { SETTING_GROUPS } from "../constants/settings.js";

/**
 * 预览设置缓存类
 * 提供O(1)复杂度的文件类型查询性能
 */
export class PreviewSettingsCache {
  constructor() {
    this.cache = new Map();
    this.typeCache = new Map();
    this.lastUpdate = null;
    this.ttl = Infinity;
    this._isLoaded = false;
  }

  /**
   * 获取文件类型（基于扩展名）
   * @param {string} extension - 文件扩展名（不含点）
   * @param {D1Database} db - 数据库实例（可选）
   * @returns {Promise<string>} 文件类型 (text|audio|video|image|office|document|unknown)
   */
  async getFileType(extension, db = null) {
    await this.ensureLoaded(db);
    return this.typeCache.get(extension.toLowerCase()) || "unknown";
  }

  /**
   * 检查缓存是否已加载
   * @returns {boolean}
   */
  isLoaded() {
    return this._isLoaded && this.lastUpdate; // 永不过期，只检查是否已加载
  }

  /**
   * 确保缓存已加载
   * @param {D1Database} db - 数据库实例（可选）
   */
  async ensureLoaded(db = null) {
    if (!this.isLoaded()) {
      await this.refresh(db);
    }
  }

  /**
   * 刷新缓存（从数据库重新加载预览设置）
   * @param {D1Database} db - 数据库实例（可选，用于外部调用）
   */
  async refresh(db = null) {
    try {
      // 如果没有传入db，尝试从全局获取（在实际使用中需要传入）
      if (!db) {
        console.warn("PreviewSettingsCache.refresh: 没有提供数据库实例，跳过刷新");
        return;
      }

      const repositoryFactory = new RepositoryFactory(db);
      const systemRepository = repositoryFactory.getSystemRepository();

      // 获取预览设置分组的所有设置
      const previewSettings = await systemRepository.getSettingsByGroup(SETTING_GROUPS.PREVIEW, false);

      // 清空现有缓存
      this.cache.clear();
      this.typeCache.clear();

      // 重建缓存
      for (const setting of previewSettings) {
        this.cache.set(setting.key, setting.value);

        // 解析扩展名列表并建立映射
        if (setting.key.endsWith("_types")) {
          const typeCategory = this.extractTypeCategory(setting.key);
          this.parseAndCacheExtensions(typeCategory, setting.value);
        }
      }

      this.lastUpdate = Date.now();
      this._isLoaded = true;

      console.log(`预览设置缓存已刷新，共缓存 ${this.typeCache.size} 个扩展名映射`);
    } catch (error) {
      console.error("刷新预览设置缓存失败:", error);
      // 刷新失败时保持旧缓存，避免服务中断
    }
  }

  /**
   * 从设置键名提取类型分类
   * @param {string} settingKey - 设置键名 (如 preview_text_types)
   * @returns {string} 类型分类 (如 text)
   */
  extractTypeCategory(settingKey) {
    const match = settingKey.match(/^preview_(.+)_types$/);
    return match ? match[1] : "unknown";
  }

  /**
   * 解析扩展名字符串并缓存映射关系
   * @param {string} typeCategory - 类型分类 (text|audio|video|image|office|document)
   * @param {string} extensionString - 逗号分隔的扩展名字符串
   */
  parseAndCacheExtensions(typeCategory, extensionString) {
    if (!extensionString || typeof extensionString !== "string") {
      return;
    }

    const extensions = extensionString
      .split(",")
      .map((ext) => ext.trim().toLowerCase())
      .filter((ext) => ext.length > 0 && /^[a-z0-9]+$/.test(ext));

    for (const extension of extensions) {
      this.typeCache.set(extension, typeCategory);
    }

    console.log(`缓存 ${typeCategory} 类型扩展名: ${extensions.join(", ")} (共${extensions.length}个)`);
  }

  /**
   * 获取原始设置值
   * @param {string} key - 设置键名
   * @returns {string|null} 设置值
   */
  getSetting(key) {
    return this.cache.get(key) || null;
  }

  /**
   * 获取所有支持的扩展名（按类型分组）
   * @returns {Object} 按类型分组的扩展名对象
   */
  getAllSupportedExtensions() {
    const result = {};
    for (const [extension, type] of this.typeCache.entries()) {
      if (!result[type]) {
        result[type] = [];
      }
      result[type].push(extension);
    }
    return result;
  }

  /**
   * 清除缓存
   */
  clear() {
    this.cache.clear();
    this.typeCache.clear();
    this.lastUpdate = null;
    this._isLoaded = false;
  }

  /**
   * 获取缓存统计信息
   * @returns {Object} 缓存统计
   */
  getStats() {
    return {
      isLoaded: this._isLoaded,
      lastUpdate: this.lastUpdate,
      settingsCount: this.cache.size,
      extensionMappings: this.typeCache.size,
      ttl: "永不过期",
      age: this.lastUpdate ? Date.now() - this.lastUpdate : null,
      cacheStrategy: "永不过期 + 主动刷新",
    };
  }
}

// 创建全局单例实例
const previewSettingsCacheInstance = new PreviewSettingsCache();

// 默认导出单例实例
export default previewSettingsCacheInstance;

// 命名导出
export const previewSettingsCache = previewSettingsCacheInstance;
