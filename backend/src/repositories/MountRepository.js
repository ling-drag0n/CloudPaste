/**
 * 挂载点Repository类
 * 负责挂载点相关的数据访问操作
 */

import { BaseRepository } from "./BaseRepository.js";
import { DbTables } from "../constants/index.js";

export class MountRepository extends BaseRepository {
  /**
   * 根据挂载路径查找挂载点
   * @param {string} mountPath - 挂载路径
   * @returns {Promise<Object|null>} 挂载点对象或null
   */
  async findByMountPath(mountPath) {
    if (!mountPath) return null;

    return await this.findOne(DbTables.STORAGE_MOUNTS, { mount_path: mountPath });
  }

  /**
   * 根据管理员ID获取挂载点列表
   * @param {string} adminId - 管理员ID
   * @param {boolean} includeInactive - 是否包含禁用的挂载点
   * @returns {Promise<Array>} 挂载点列表
   */
  async findByAdmin(adminId, includeInactive = false) {
    const conditions = { created_by: adminId };
    if (!includeInactive) {
      conditions.is_active = 1;
    }

    return await this.findMany(DbTables.STORAGE_MOUNTS, conditions, { orderBy: "sort_order ASC, name ASC" });
  }

  /**
   * 获取所有挂载点列表（管理员专用）
   * @param {boolean} includeInactive - 是否包含禁用的挂载点
   * @returns {Promise<Array>} 所有挂载点列表
   */
  async findAll(includeInactive = true) {
    const conditions = includeInactive ? {} : { is_active: 1 };

    return await this.findMany(DbTables.STORAGE_MOUNTS, conditions, { orderBy: "sort_order ASC, name ASC" });
  }

  /**
   * 根据ID获取挂载点
   * @param {string} mountId - 挂载点ID
   * @returns {Promise<Object|null>} 挂载点对象或null
   */
  async findById(mountId) {
    return await super.findById(DbTables.STORAGE_MOUNTS, mountId);
  }

  /**
   * 根据ID和管理员ID获取挂载点（权限检查）
   * @param {string} mountId - 挂载点ID
   * @param {string} adminId - 管理员ID
   * @returns {Promise<Object|null>} 挂载点对象或null
   */
  async findByIdAndAdmin(mountId, adminId) {
    return await this.findOne(DbTables.STORAGE_MOUNTS, {
      id: mountId,
      created_by: adminId,
    });
  }

  /**
   * 根据ID和API密钥用户获取激活的挂载点
   * @param {string} mountId - 挂载点ID
   * @param {string} apiKeyId - API密钥ID
   * @returns {Promise<Object|null>} 挂载点对象或null
   */
  async findByIdAndApiKey(mountId, apiKeyId) {
    return await this.findOne(DbTables.STORAGE_MOUNTS, {
      id: mountId,
      created_by: apiKeyId,
      is_active: 1,
    });
  }

  /**
   * 根据存储配置ID查找挂载点
   * @param {string} storageConfigId - 存储配置ID
   * @param {string} storageType - 存储类型
   * @returns {Promise<Array>} 挂载点列表
   */
  async findByStorageConfig(storageConfigId, storageType) {
    return await this.findMany(DbTables.STORAGE_MOUNTS, {
      storage_config_id: storageConfigId,
      storage_type: storageType,
    });
  }

  /**
   * 创建挂载点
   * @param {Object} mountData - 挂载点数据
   * @returns {Promise<Object>} 创建结果
   */
  async createMount(mountData) {
    // 确保包含必要的时间戳
    const dataWithTimestamp = {
      ...mountData,
      created_at: mountData.created_at || new Date().toISOString(),
      updated_at: mountData.updated_at || new Date().toISOString(),
    };

    return await this.create(DbTables.STORAGE_MOUNTS, dataWithTimestamp);
  }

  /**
   * 更新挂载点
   * @param {string} mountId - 挂载点ID
   * @param {Object} updateData - 更新数据
   * @returns {Promise<Object>} 更新结果
   */
  async updateMount(mountId, updateData) {
    // 自动更新修改时间
    const dataWithTimestamp = {
      ...updateData,
      updated_at: new Date().toISOString(),
    };

    return await this.update(DbTables.STORAGE_MOUNTS, mountId, dataWithTimestamp);
  }

  /**
   * 更新挂载点最后使用时间
   * @param {string} mountId - 挂载点ID
   * @returns {Promise<Object>} 更新结果
   */
  async updateLastUsed(mountId) {
    return await this.execute(`UPDATE ${DbTables.STORAGE_MOUNTS} SET last_used = CURRENT_TIMESTAMP WHERE id = ?`, [mountId]);
  }

  /**
   * 删除挂载点
   * @param {string} mountId - 挂载点ID
   * @returns {Promise<Object>} 删除结果
   */
  async deleteMount(mountId) {
    return await this.delete(DbTables.STORAGE_MOUNTS, mountId);
  }

  /**
   * 检查挂载路径是否已存在
   * @param {string} mountPath - 挂载路径
   * @param {string} excludeId - 排除的挂载点ID（用于更新时检查）
   * @returns {Promise<boolean>} 是否存在
   */
  async existsByMountPath(mountPath, excludeId = null) {
    if (excludeId) {
      const result = await this.queryFirst(`SELECT id FROM ${DbTables.STORAGE_MOUNTS} WHERE mount_path = ? AND id != ?`, [mountPath, excludeId]);
      return !!result;
    } else {
      return await this.exists(DbTables.STORAGE_MOUNTS, { mount_path: mountPath });
    }
  }

  /**
   * 批量更新挂载点状态
   * @param {Array<string>} mountIds - 挂载点ID数组
   * @param {number} isActive - 激活状态 (0或1)
   * @returns {Promise<Object>} 更新结果
   */
  async batchUpdateStatus(mountIds, isActive) {
    if (!mountIds || mountIds.length === 0) {
      return { updatedCount: 0, message: "没有要更新的挂载点" };
    }

    const placeholders = mountIds.map(() => "?").join(",");
    const result = await this.execute(
      `UPDATE ${DbTables.STORAGE_MOUNTS} 
       SET is_active = ?, updated_at = CURRENT_TIMESTAMP 
       WHERE id IN (${placeholders})`,
      [isActive, ...mountIds]
    );

    return {
      updatedCount: result.meta?.changes || 0,
      message: `已更新${result.meta?.changes || 0}个挂载点状态`,
    };
  }

  /**
   * 获取挂载点统计信息
   * @param {string} userId - 用户ID（可选）
   * @returns {Promise<Object>} 统计信息
   */
  async getStatistics(userId = null) {
    const conditions = userId ? { created_by: userId } : {};

    const total = await this.count(DbTables.STORAGE_MOUNTS, conditions);
    const active = await this.count(DbTables.STORAGE_MOUNTS, {
      ...conditions,
      is_active: 1,
    });

    // 按存储类型统计
    let sql = `
      SELECT storage_type, COUNT(*) as count 
      FROM ${DbTables.STORAGE_MOUNTS}
    `;

    const params = [];
    if (userId) {
      sql += ` WHERE created_by = ?`;
      params.push(userId);
    }

    sql += ` GROUP BY storage_type`;

    const typeStats = await this.query(sql, params);

    return {
      total,
      active,
      inactive: total - active,
      byType: typeStats.results || [],
    };
  }
}
