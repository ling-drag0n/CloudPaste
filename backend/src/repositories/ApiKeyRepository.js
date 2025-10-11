/**
 * API密钥Repository类
 * 负责API密钥相关的数据访问操作
 */

import { BaseRepository } from "./BaseRepository.js";
import { DbTables } from "../constants/index.js";
import { Permission, PermissionChecker } from "../constants/permissions.js";

export class ApiKeyRepository extends BaseRepository {
  /**
   * 根据密钥查找API密钥
   * @param {string} key - API密钥
   * @returns {Promise<Object|null>} API密钥对象或null
   */
  async findByKey(key) {
    if (!key) return null;

    return await this.findOne(DbTables.API_KEYS, { key });
  }

  /**
   * 根据ID查找API密钥
   * @param {string} keyId - API密钥ID
   * @returns {Promise<Object|null>} API密钥对象或null
   */
  async findById(keyId) {
    return await super.findById(DbTables.API_KEYS, keyId);
  }

  /**
   * 获取所有API密钥列表
   * @param {Object} options - 查询选项
   * @returns {Promise<Array>} API密钥列表
   */
  async findAll(options = {}) {
    const { orderBy = "created_at DESC", limit, offset } = options;

    return await this.findMany(DbTables.API_KEYS, {}, { orderBy, limit, offset });
  }

  /**
   * 创建API密钥
   * @param {Object} keyData - API密钥数据
   * @returns {Promise<Object>} 创建结果
   */
  async createApiKey(keyData) {
    // 确保包含必要的时间戳
    const dataWithTimestamp = {
      ...keyData,
      created_at: keyData.created_at || new Date().toISOString(),
    };

    return await this.create(DbTables.API_KEYS, dataWithTimestamp);
  }

  /**
   * 更新API密钥信息
   * @param {string} keyId - API密钥ID
   * @param {Object} updateData - 更新数据
   * @returns {Promise<Object>} 更新结果
   */
  async updateApiKey(keyId, updateData) {
    return await this.update(DbTables.API_KEYS, keyId, updateData);
  }

  /**
   * 更新API密钥最后使用时间
   * @param {string} keyId - API密钥ID
   * @returns {Promise<Object>} 更新结果
   */
  async updateLastUsed(keyId) {
    return await this.execute(`UPDATE ${DbTables.API_KEYS} SET last_used = CURRENT_TIMESTAMP WHERE id = ?`, [keyId]);
  }

  /**
   * 删除API密钥
   * @param {string} keyId - API密钥ID
   * @returns {Promise<Object>} 删除结果
   */
  async deleteApiKey(keyId) {
    return await this.delete(DbTables.API_KEYS, keyId);
  }

  /**
   * 检查密钥是否已存在
   * @param {string} key - API密钥
   * @param {string} excludeId - 排除的密钥ID（用于更新时检查）
   * @returns {Promise<boolean>} 是否存在
   */
  async existsByKey(key, excludeId = null) {
    if (excludeId) {
      const result = await this.queryFirst(`SELECT id FROM ${DbTables.API_KEYS} WHERE key = ? AND id != ?`, [key, excludeId]);
      return !!result;
    } else {
      return await this.exists(DbTables.API_KEYS, { key });
    }
  }

  /**
   * 检查名称是否已存在
   * @param {string} name - 密钥名称
   * @param {string} excludeId - 排除的密钥ID（用于更新时检查）
   * @returns {Promise<boolean>} 是否存在
   */
  async existsByName(name, excludeId = null) {
    if (excludeId) {
      const result = await this.queryFirst(`SELECT id FROM ${DbTables.API_KEYS} WHERE name = ? AND id != ?`, [name, excludeId]);
      return !!result;
    } else {
      return await this.exists(DbTables.API_KEYS, { name });
    }
  }

  /**
   * 删除过期的API密钥
   * @param {Date} currentTime - 当前时间
   * @returns {Promise<Object>} 删除结果
   */
  async deleteExpired(currentTime = new Date()) {
    const result = await this.execute(
      `DELETE FROM ${DbTables.API_KEYS} 
       WHERE expires_at IS NOT NULL AND expires_at < ?`,
      [currentTime.toISOString()]
    );

    return {
      deletedCount: result.meta?.changes || 0,
      message: `已删除${result.meta?.changes || 0}个过期API密钥`,
    };
  }

  /**
   * 查找过期的API密钥
   * @param {Date} currentTime - 当前时间
   * @returns {Promise<Array>} 过期密钥列表
   */
  async findExpired(currentTime = new Date()) {
    const result = await this.query(
      `SELECT * FROM ${DbTables.API_KEYS} 
       WHERE expires_at IS NOT NULL AND expires_at < ?`,
      [currentTime.toISOString()]
    );

    return result.results || [];
  }

  /**
   * 获取API密钥统计信息
   * @returns {Promise<Object>} 统计信息
   */
  async getStatistics() {
    const total = await this.count(DbTables.API_KEYS);

    // 获取有效密钥数量（未过期）
    const now = new Date().toISOString();
    const valid = await this.queryFirst(
      `SELECT COUNT(*) as count FROM ${DbTables.API_KEYS} 
       WHERE expires_at IS NULL OR expires_at > ?`,
      [now]
    );

    // 获取所有密钥数据，在应用层统计权限
    const allKeys = await this.findAll();
    const permissionStats = {
      // 基础权限
      text: 0,
      file_share: 0,

      // 挂载页权限
      mount_view: 0,
      mount_upload: 0,
      mount_copy: 0,
      mount_rename: 0,
      mount_delete: 0,

      // WebDAV权限
      webdav_read: 0,
      webdav_manage: 0,
    };

    // 使用位标志权限系统进行统计
    allKeys.forEach((key) => {
      const permissions = key.permissions || 0;

      // 基础权限统计
      if (PermissionChecker.hasPermission(permissions, Permission.TEXT)) {
        permissionStats.text++;
      }
      if (PermissionChecker.hasPermission(permissions, Permission.FILE_SHARE)) {
        permissionStats.file_share++;
      }

      // 挂载页权限统计
      if (PermissionChecker.hasPermission(permissions, Permission.MOUNT_VIEW)) {
        permissionStats.mount_view++;
      }
      if (PermissionChecker.hasPermission(permissions, Permission.MOUNT_UPLOAD)) {
        permissionStats.mount_upload++;
      }
      if (PermissionChecker.hasPermission(permissions, Permission.MOUNT_COPY)) {
        permissionStats.mount_copy++;
      }
      if (PermissionChecker.hasPermission(permissions, Permission.MOUNT_RENAME)) {
        permissionStats.mount_rename++;
      }
      if (PermissionChecker.hasPermission(permissions, Permission.MOUNT_DELETE)) {
        permissionStats.mount_delete++;
      }

      // WebDAV权限统计
      if (PermissionChecker.hasPermission(permissions, Permission.WEBDAV_READ)) {
        permissionStats.webdav_read++;
      }
      if (PermissionChecker.hasPermission(permissions, Permission.WEBDAV_MANAGE)) {
        permissionStats.webdav_manage++;
      }
    });

    return {
      total,
      valid: valid?.count || 0,
      expired: total - (valid?.count || 0),
      permissions: permissionStats,
    };
  }
}
