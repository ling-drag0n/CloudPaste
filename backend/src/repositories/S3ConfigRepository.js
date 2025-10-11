/**
 * S3配置Repository类
 * 负责S3配置相关的数据访问操作
 * 职责：纯粹的S3配置数据访问，不包含业务逻辑
 */

import { BaseRepository } from "./BaseRepository.js";
import { DbTables } from "../constants/index.js";

export class S3ConfigRepository extends BaseRepository {
  /**
   * 根据ID获取S3配置
   * @param {string} configId - 配置ID
   * @returns {Promise<Object|null>} S3配置对象或null
   */
  async findById(configId) {
    return await super.findById(DbTables.S3_CONFIGS, configId);
  }

  /**
   * 根据管理员ID获取S3配置列表
   * @param {string} adminId - 管理员ID
   * @returns {Promise<Array>} S3配置列表
   */
  async findByAdmin(adminId) {
    return await this.findMany(DbTables.S3_CONFIGS, { admin_id: adminId }, { orderBy: "name ASC" });
  }

  /**
   * 根据管理员ID获取S3配置列表（分页）
   * @param {string} adminId - 管理员ID
   * @param {Object} options - 查询选项
   * @param {number} [options.page=1] - 页码
   * @param {number} [options.limit=10] - 每页数量
   * @returns {Promise<Object>} 包含配置列表和分页信息的对象
   */
  async findByAdminWithPagination(adminId, options = {}) {
    const { page = 1, limit = 10 } = options;
    const offset = (page - 1) * limit;

    // 获取总数
    const total = await this.count(DbTables.S3_CONFIGS, { admin_id: adminId });

    // 获取分页数据
    const configs = await this.findMany(DbTables.S3_CONFIGS, { admin_id: adminId }, { orderBy: "name ASC", limit, offset });

    return {
      configs,
      total,
      page,
      limit,
      totalPages: Math.ceil(total / limit),
    };
  }

  /**
   * 根据提供商类型获取S3配置
   * @param {string} providerType - 提供商类型
   * @param {string} adminId - 管理员ID（可选）
   * @returns {Promise<Array>} S3配置列表
   */
  async findByProviderType(providerType, adminId = null) {
    const conditions = { provider_type: providerType };
    if (adminId) {
      conditions.admin_id = adminId;
    }

    return await this.findMany(DbTables.S3_CONFIGS, conditions, { orderBy: "name ASC" });
  }

  /**
   * 创建S3配置
   * @param {Object} configData - S3配置数据
   * @returns {Promise<Object>} 创建结果
   */
  async createConfig(configData) {
    // 确保包含必要的时间戳
    const dataWithTimestamp = {
      ...configData,
      created_at: configData.created_at || new Date().toISOString(),
      updated_at: configData.updated_at || new Date().toISOString(),
    };

    return await this.create(DbTables.S3_CONFIGS, dataWithTimestamp);
  }

  /**
   * 更新S3配置
   * @param {string} configId - 配置ID
   * @param {Object} updateData - 更新数据
   * @returns {Promise<Object>} 更新结果
   */
  async updateConfig(configId, updateData) {
    // 自动更新修改时间
    const dataWithTimestamp = {
      ...updateData,
      updated_at: new Date().toISOString(),
    };

    return await this.update(DbTables.S3_CONFIGS, configId, dataWithTimestamp);
  }

  /**
   * 更新S3配置最后使用时间
   * @param {string} configId - 配置ID
   * @returns {Promise<Object>} 更新结果
   */
  async updateLastUsed(configId) {
    return await this.execute(`UPDATE ${DbTables.S3_CONFIGS} SET last_used = CURRENT_TIMESTAMP WHERE id = ?`, [configId]);
  }

  /**
   * 删除S3配置
   * @param {string} configId - 配置ID
   * @returns {Promise<Object>} 删除结果
   */
  async deleteConfig(configId) {
    return await this.delete(DbTables.S3_CONFIGS, configId);
  }

  /**
   * 检查配置名称是否已存在
   * @param {string} name - 配置名称
   * @param {string} adminId - 管理员ID
   * @param {string} excludeId - 排除的配置ID（用于更新时检查）
   * @returns {Promise<boolean>} 是否存在
   */
  async existsByName(name, adminId, excludeId = null) {
    if (excludeId) {
      const result = await this.queryFirst(`SELECT id FROM ${DbTables.S3_CONFIGS} WHERE name = ? AND admin_id = ? AND id != ?`, [name, adminId, excludeId]);
      return !!result;
    } else {
      return await this.exists(DbTables.S3_CONFIGS, {
        name: name,
        admin_id: adminId,
      });
    }
  }

  /**
   * 获取用户的S3配置数量
   * @param {string} adminId - 管理员ID
   * @returns {Promise<number>} 配置数量
   */
  async countByAdmin(adminId) {
    return await this.count(DbTables.S3_CONFIGS, { admin_id: adminId });
  }

  /**
   * 获取所有S3配置（管理员专用）
   * @returns {Promise<Array>} 所有S3配置列表
   */
  async findAll() {
    return await this.findMany(DbTables.S3_CONFIGS, {}, { orderBy: "admin_id ASC, name ASC" });
  }

  /**
   * 根据端点URL查找配置
   * @param {string} endpointUrl - 端点URL
   * @param {string} adminId - 管理员ID
   * @returns {Promise<Array>} S3配置列表
   */
  async findByEndpoint(endpointUrl, adminId) {
    return await this.findMany(DbTables.S3_CONFIGS, {
      endpoint_url: endpointUrl,
      admin_id: adminId,
    });
  }

  /**
   * 根据存储桶名称查找配置
   * @param {string} bucketName - 存储桶名称
   * @param {string} adminId - 管理员ID
   * @returns {Promise<Array>} S3配置列表
   */
  async findByBucket(bucketName, adminId) {
    return await this.findMany(DbTables.S3_CONFIGS, {
      bucket_name: bucketName,
      admin_id: adminId,
    });
  }

  /**
   * 获取S3配置统计信息
   * @param {string} adminId - 管理员ID（可选）
   * @returns {Promise<Object>} 统计信息
   */
  async getStatistics(adminId = null) {
    const conditions = adminId ? { admin_id: adminId } : {};

    const total = await this.count(DbTables.S3_CONFIGS, conditions);

    // 按提供商类型统计
    let sql = `
      SELECT provider_type, COUNT(*) as count 
      FROM ${DbTables.S3_CONFIGS}
    `;

    const params = [];
    if (adminId) {
      sql += ` WHERE admin_id = ?`;
      params.push(adminId);
    }

    sql += ` GROUP BY provider_type`;

    const providerStats = await this.query(sql, params);

    return {
      total,
      byProvider: providerStats.results || [],
    };
  }

  /**
   * 查找最近使用的S3配置
   * @param {string} adminId - 管理员ID
   * @param {number} limit - 限制数量
   * @returns {Promise<Array>} S3配置列表
   */
  async findRecentlyUsed(adminId, limit = 10) {
    const result = await this.query(
      `
      SELECT * FROM ${DbTables.S3_CONFIGS}
      WHERE admin_id = ? AND last_used IS NOT NULL
      ORDER BY last_used DESC
      LIMIT ?
      `,
      [adminId, limit]
    );

    return result.results || [];
  }

  /**
   * 批量删除S3配置
   * @param {Array<string>} configIds - 配置ID数组
   * @returns {Promise<Object>} 删除结果
   */
  async batchDelete(configIds) {
    if (!configIds || configIds.length === 0) {
      return { deletedCount: 0, message: "没有要删除的配置" };
    }

    const placeholders = configIds.map(() => "?").join(",");
    const result = await this.execute(`DELETE FROM ${DbTables.S3_CONFIGS} WHERE id IN (${placeholders})`, configIds);

    return {
      deletedCount: result.meta?.changes || 0,
      message: `已删除${result.meta?.changes || 0}个S3配置`,
    };
  }

  /**
   * 根据区域查找S3配置
   * @param {string} region - 区域
   * @param {string} adminId - 管理员ID
   * @returns {Promise<Array>} S3配置列表
   */
  async findByRegion(region, adminId) {
    return await this.findMany(DbTables.S3_CONFIGS, {
      region: region,
      admin_id: adminId,
    });
  }

  /**
   * 根据ID和管理员ID获取S3配置
   * @param {string} configId - 配置ID
   * @param {string} adminId - 管理员ID
   * @returns {Promise<Object|null>} S3配置对象或null
   */
  async findByIdAndAdmin(configId, adminId) {
    return await this.findOne(DbTables.S3_CONFIGS, {
      id: configId,
      admin_id: adminId,
    });
  }

  /**
   * 根据ID和管理员ID获取S3配置（包含敏感字段）
   * @param {string} configId - 配置ID
   * @param {string} adminId - 管理员ID
   * @returns {Promise<Object|null>} S3配置对象或null
   */
  async findByIdAndAdminWithSecrets(configId, adminId) {
    // 这个方法需要返回包含access_key_id和secret_access_key的完整配置
    return await this.queryFirst(`SELECT * FROM ${DbTables.S3_CONFIGS} WHERE id = ? AND admin_id = ?`, [configId, adminId]);
  }

  /**
   * 获取公开的S3配置列表
   * @returns {Promise<Array>} 公开的S3配置列表
   */
  async findPublic() {
    return await this.findMany(DbTables.S3_CONFIGS, { is_public: 1 }, { orderBy: "name ASC" });
  }

  /**
   * 根据ID获取公开的S3配置
   * @param {string} configId - 配置ID
   * @returns {Promise<Object|null>} S3配置对象或null
   */
  async findPublicById(configId) {
    return await this.findOne(DbTables.S3_CONFIGS, {
      id: configId,
      is_public: 1,
    });
  }

  /**
   * 设置默认S3配置
   * @param {string} configId - 配置ID
   * @param {string} adminId - 管理员ID
   * @returns {Promise<void>}
   */
  async setAsDefault(configId, adminId) {
    // 使用事务操作：先将所有配置设为非默认，再设置指定配置为默认
    await this.db.batch([
      this.db.prepare(`UPDATE ${DbTables.S3_CONFIGS} SET is_default = 0, updated_at = CURRENT_TIMESTAMP WHERE admin_id = ?`).bind(adminId),
      this.db.prepare(`UPDATE ${DbTables.S3_CONFIGS} SET is_default = 1, updated_at = CURRENT_TIMESTAMP WHERE id = ?`).bind(configId),
    ]);
  }

  /**
   * 获取带使用情况的S3配置列表
   * @returns {Promise<Array>} S3配置列表
   */
  async findAllWithUsage() {
    // 获取所有S3配置
    const queryResult = await this.query(
      `SELECT id, name, provider_type, endpoint_url, bucket_name, region, path_style, default_folder,
              is_public, is_default, created_at, updated_at, last_used, total_storage_bytes, admin_id,
              custom_host, signature_expires_in
       FROM ${DbTables.S3_CONFIGS}
       ORDER BY name ASC`
    );
    const configs = queryResult.results || [];

    // 为每个配置查询使用情况
    const result = [];
    for (const config of configs) {
      const usage = await this.queryFirst(`SELECT COUNT(*) as file_count, SUM(size) as total_size FROM ${DbTables.FILES} WHERE storage_type = ? AND storage_config_id = ?`, [
        "S3",
        config.id,
      ]);

      result.push({
        ...config,
        usage: {
          file_count: usage?.file_count || 0,
          total_size: usage?.total_size || 0,
        },
      });
    }

    return result;
  }
}
