/**
 * 系统设置Repository类
 * 负责系统设置相关的数据访问操作
 */

import { BaseRepository } from "./BaseRepository.js";
import { DbTables } from "../constants/index.js";
import { SETTING_GROUPS, SETTING_TYPES, SETTING_FLAGS, validateSettingValue, convertSettingValue } from "../constants/settings.js";

export class SystemRepository extends BaseRepository {
  /**
   * 获取系统统计数据
   * @returns {Promise<Object>} 统计数据
   */
  async getDashboardStats() {
    // 获取各种数据的统计
    const stats = {};

    // 文本分享总数
    stats.totalPastes = await this.count(DbTables.PASTES);

    // 文件上传总数
    stats.totalFiles = await this.count(DbTables.FILES);

    // API密钥总数
    stats.totalApiKeys = await this.count(DbTables.API_KEYS);

    // S3配置总数
    stats.totalS3Configs = await this.count(DbTables.S3_CONFIGS);

    return stats;
  }

  /**
   * 获取最近一周的数据趋势
   * @returns {Promise<Object>} 趋势数据
   */
  async getWeeklyTrends() {
    const sevenDaysAgo = new Date();
    sevenDaysAgo.setDate(sevenDaysAgo.getDate() - 7);
    const sevenDaysAgoISO = sevenDaysAgo.toISOString();

    // 文本分享趋势
    const pastesQuery = `
      SELECT
        date(created_at) as date,
        COUNT(*) as count
      FROM ${DbTables.PASTES}
      WHERE created_at >= ?
      GROUP BY date(created_at)
      ORDER BY date ASC
    `;
    const pastesResult = await this.query(pastesQuery, [sevenDaysAgoISO]);

    // 文件上传趋势
    const filesQuery = `
      SELECT
        date(created_at) as date,
        COUNT(*) as count
      FROM ${DbTables.FILES}
      WHERE created_at >= ?
      GROUP BY date(created_at)
      ORDER BY date ASC
    `;
    const filesResult = await this.query(filesQuery, [sevenDaysAgoISO]);

    return {
      pastes: pastesResult.results || [],
      files: filesResult.results || [],
    };
  }

  /**
   * 获取存储使用情况统计
   * @returns {Promise<Object>} 存储统计
   */
  async getStorageStats() {
    // 获取所有S3配置的使用情况
    const s3ConfigsQuery = `
      SELECT
        s.id, s.name, s.provider_type, s.total_storage_bytes,
        COUNT(f.id) as file_count,
        COALESCE(SUM(f.size), 0) as total_size
      FROM ${DbTables.S3_CONFIGS} s
      LEFT JOIN ${DbTables.FILES} f ON s.id = f.storage_config_id AND f.storage_type = 'S3'
      GROUP BY s.id, s.name, s.provider_type, s.total_storage_bytes
      ORDER BY s.name ASC
    `;

    const s3ConfigsResult = await this.query(s3ConfigsQuery);
    const s3Configs = s3ConfigsResult.results || [];

    // 计算总存储使用量
    const totalStorageUsed = s3Configs.reduce((total, config) => total + (config.total_size || 0), 0);

    // 转换数据格式
    const s3Buckets = s3Configs.map((config) => {
      const usedStorage = config.total_size || 0;
      const totalStorage = config.total_storage_bytes || 0;
      const usagePercent = totalStorage > 0 ? Math.min(100, Math.round((usedStorage / totalStorage) * 100)) : 0;

      return {
        ...config,
        usedStorage,
        totalStorage,
        fileCount: config.file_count || 0,
        usagePercent,
        providerType: config.provider_type,
      };
    });

    return {
      totalStorageUsed,
      s3Buckets,
    };
  }

  /**
   * 清理过期数据
   * 使用各个Repository的专门方法，避免重复SQL逻辑
   * @returns {Promise<Object>} 清理结果
   */
  async cleanupExpiredData() {
    const now = new Date();
    let totalCleaned = 0;

    // 使用PasteRepository的专门方法清理过期文本分享
    const PasteRepository = require("./PasteRepository");
    const pasteRepo = new PasteRepository(this.db);

    const expiredPastesResult = await pasteRepo.deleteExpired(now);
    const pastesCount = expiredPastesResult.deletedCount;
    totalCleaned += pastesCount;

    // 使用ApiKeyRepository的专门方法清理过期API密钥
    const ApiKeyRepository = require("./ApiKeyRepository");
    const apiKeyRepo = new ApiKeyRepository(this.db);

    const expiredApiKeysResult = await apiKeyRepo.deleteExpired(now);
    const apiKeysCount = expiredApiKeysResult.deletedCount;
    totalCleaned += apiKeysCount;

    // 使用PasteRepository的专门方法清理超限文本分享
    const overLimitResult = await pasteRepo.deleteOverViewLimit();
    const overLimitCount = overLimitResult.deletedCount;
    totalCleaned += overLimitCount;

    return {
      totalCleaned,
      details: {
        expiredPastes: pastesCount,
        expiredApiKeys: apiKeysCount,
        overLimitPastes: overLimitCount,
      },
      message: `清理完成，共清理${totalCleaned}条过期数据`,
    };
  }

  /**
   * 获取代理签名全局配置
   * @returns {Promise<Object>} 全局配置
   */
  async getProxySignConfig() {
    const signAllSetting = await this.getSettingMetadata("proxy_sign_all");
    const expiresSetting = await this.getSettingMetadata("proxy_sign_expires");

    return {
      signAll: signAllSetting?.value === "true",
      expires: parseInt(expiresSetting?.value) || 0,
    };
  }

  /**
   * 更新代理签名全局配置
   * @param {Object} config - 配置对象
   * @param {boolean} config.signAll - 是否签名所有
   * @param {number} config.expires - 过期时间（秒）
   * @returns {Promise<Object>} 操作结果
   */
  async updateProxySignConfig(config) {
    const { signAll, expires } = config;

    // 验证参数
    if (typeof signAll !== "boolean") {
      throw new Error("signAll 必须是布尔值");
    }

    if (typeof expires !== "number" || expires < 0) {
      throw new Error("expires 必须是非负数");
    }

    // 使用新的分组更新机制（代理签名设置属于全局设置组，group_id = 1）
    const settings = {
      proxy_sign_all: signAll.toString(),
      proxy_sign_expires: expires.toString(),
    };

    return await this.updateGroupSettings(1, settings, { validateType: true });
  }

  // ==================== 分组和类型化设置管理方法 ====================

  /**
   * 按分组获取设置项
   * @param {number} groupId - 分组ID
   * @param {boolean} includeMetadata - 是否包含元数据
   * @returns {Promise<Array>} 设置项列表
   */
  async getSettingsByGroup(groupId, includeMetadata = true) {
    const orderBy = includeMetadata ? "sort_order ASC, key ASC" : "key ASC";

    if (includeMetadata) {
      // 返回完整的元数据
      return await this.findMany(DbTables.SYSTEM_SETTINGS, { group_id: groupId }, { orderBy });
    } else {
      // 只返回key-value对
      const settings = await this.findMany(DbTables.SYSTEM_SETTINGS, { group_id: groupId }, { orderBy });
      return settings.map((setting) => ({
        key: setting.key,
        value: setting.value,
      }));
    }
  }

  /**
   * 获取所有分组的设置项
   * @param {boolean} includeSystemGroup - 是否包含系统内部分组
   * @returns {Promise<Object>} 按分组组织的设置项
   */
  async getAllSettingsByGroups(includeSystemGroup = false) {
    const whereClause = includeSystemGroup ? {} : { group_id: { "!=": SETTING_GROUPS.SYSTEM } };
    const allSettings = await this.findMany(DbTables.SYSTEM_SETTINGS, whereClause, { orderBy: "group_id ASC, sort_order ASC, key ASC" });

    // 按分组组织数据
    const groupedSettings = {};
    for (const setting of allSettings) {
      const groupId = setting.group_id || SETTING_GROUPS.GLOBAL;
      if (!groupedSettings[groupId]) {
        groupedSettings[groupId] = [];
      }
      groupedSettings[groupId].push(setting);
    }

    return groupedSettings;
  }

  /**
   * 获取设置项的元数据
   * @param {string} key - 设置键名
   * @returns {Promise<Object|null>} 设置项元数据
   */
  async getSettingMetadata(key) {
    if (!key) return null;
    return await this.findOne(DbTables.SYSTEM_SETTINGS, { key });
  }

  /**
   * 批量更新分组设置（支持类型验证）
   * @param {number} groupId - 分组ID
   * @param {Object} settings - 设置键值对
   * @param {Object} options - 选项
   * @returns {Promise<Object>} 操作结果
   */
  async updateGroupSettings(groupId, settings, options = {}) {
    const { validateType = true } = options;
    const keys = Object.keys(settings);
    let updatedCount = 0;
    const errors = [];

    // 验证所有设置项都属于指定分组
    for (const key of keys) {
      const metadata = await this.getSettingMetadata(key);
      if (!metadata) {
        errors.push({ key, error: `设置项不存在: ${key}` });
        continue;
      }

      if (metadata.group_id !== groupId) {
        errors.push({ key, error: `设置项 ${key} 不属于分组 ${groupId}` });
        continue;
      }

      try {
        let finalValue = settings[key];

        // 类型验证
        if (validateType && metadata.type) {
          if (!validateSettingValue(key, finalValue, metadata.type)) {
            throw new Error(`设置值无效: ${key} = ${finalValue}`);
          }
        }

        // 类型转换
        if (metadata.type) {
          finalValue = convertSettingValue(finalValue, metadata.type);
        }

        // 更新设置
        await this.execute(
          `UPDATE ${DbTables.SYSTEM_SETTINGS}
           SET value = ?, updated_at = CURRENT_TIMESTAMP
           WHERE key = ?`,
          [finalValue.toString(), key]
        );

        updatedCount++;
      } catch (error) {
        errors.push({ key, error: error.message });
      }
    }

    return {
      groupId,
      updatedCount,
      totalCount: keys.length,
      errors,
      success: errors.length === 0,
      message: errors.length === 0 ? `成功更新分组${groupId}的${updatedCount}个设置` : `更新了${updatedCount}个设置，${errors.length}个失败`,
    };
  }

  /**
   * 获取分组列表和统计信息
   * @returns {Promise<Array>} 分组信息列表
   */
  async getGroupsInfo() {
    const groupStats = await this.query(`
      SELECT
        group_id,
        COUNT(*) as setting_count,
        COUNT(CASE WHEN flags = ${SETTING_FLAGS.READONLY} THEN 1 END) as readonly_count
      FROM ${DbTables.SYSTEM_SETTINGS}
      WHERE group_id != ${SETTING_GROUPS.SYSTEM}
      GROUP BY group_id
      ORDER BY group_id ASC
    `);

    return (groupStats.results || []).map((stat) => ({
      id: stat.group_id,
      name: this.getGroupName(stat.group_id),
      settingCount: stat.setting_count,
      readonlyCount: stat.readonly_count,
    }));
  }

  /**
   * 获取分组名称
   * @param {number} groupId - 分组ID
   * @returns {string} 分组名称
   */
  getGroupName(groupId) {
    const groupNames = {
      [SETTING_GROUPS.GLOBAL]: "全局设置",
      [SETTING_GROUPS.WEBDAV]: "WebDAV设置",
      [SETTING_GROUPS.SYSTEM]: "系统设置",
      [SETTING_GROUPS.PREVIEW]: "预览设置",
      [SETTING_GROUPS.SITE]: "站点设置",
    };
    return groupNames[groupId] || `未知分组(${groupId})`;
  }
}
