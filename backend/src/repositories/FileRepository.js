/**
 * 文件Repository类
 * 负责文件相关的数据访问操作
 */

import { BaseRepository } from "./BaseRepository.js";
import { DbTables } from "../constants/index.js";

export class FileRepository extends BaseRepository {
  /**
   * 根据ID查找文件
   * @param {string} fileId - 文件ID
   * @returns {Promise<Object|null>} 文件对象或null
   */
  async findById(fileId) {
    return await super.findById(DbTables.FILES, fileId);
  }

  /**
   * 根据slug查找文件
   * @param {string} slug - 文件slug
   * @returns {Promise<Object|null>} 文件对象或null
   */
  async findBySlug(slug) {
    if (!slug) return null;

    return await this.queryFirst(`SELECT * FROM ${DbTables.FILES} WHERE slug = ?`, [slug]);
  }

  /**
   * 根据slug查找文件并关联存储配置
   * @param {string} slug - 文件slug
   * @returns {Promise<Object|null>} 包含存储配置的文件对象或null
   */
  async findBySlugWithStorageConfig(slug) {
    if (!slug) return null;

    return await this.queryFirst(
      `
      SELECT
        f.*,
        CASE
          WHEN f.storage_type = 'S3' THEN s.endpoint_url
          ELSE NULL
        END as endpoint_url,
        CASE
          WHEN f.storage_type = 'S3' THEN s.bucket_name
          ELSE NULL
        END as bucket_name,
        CASE
          WHEN f.storage_type = 'S3' THEN s.region
          ELSE NULL
        END as region,
        CASE
          WHEN f.storage_type = 'S3' THEN s.access_key_id
          ELSE NULL
        END as access_key_id,
        CASE
          WHEN f.storage_type = 'S3' THEN s.secret_access_key
          ELSE NULL
        END as secret_access_key,
        CASE
          WHEN f.storage_type = 'S3' THEN s.path_style
          ELSE NULL
        END as path_style,
        CASE
          WHEN f.storage_type = 'S3' THEN s.provider_type
          ELSE NULL
        END as storage_provider_type,
        CASE
          WHEN f.storage_type = 'S3' THEN s.name
          ELSE NULL
        END as storage_config_name
      FROM ${DbTables.FILES} f
      LEFT JOIN ${DbTables.S3_CONFIGS} s ON f.storage_type = 'S3' AND f.storage_config_id = s.id
      WHERE f.slug = ?
      `,
      [slug]
    );
  }

  /**
   * 根据ID查找文件并关联存储配置
   * @param {string} fileId - 文件ID
   * @returns {Promise<Object|null>} 包含存储配置的文件对象或null
   */
  async findByIdWithStorageConfig(fileId) {
    if (!fileId) return null;

    return await this.queryFirst(
      `
      SELECT
        f.*,
        CASE
          WHEN f.storage_type = 'S3' THEN s.endpoint_url
          ELSE NULL
        END as endpoint_url,
        CASE
          WHEN f.storage_type = 'S3' THEN s.bucket_name
          ELSE NULL
        END as bucket_name,
        CASE
          WHEN f.storage_type = 'S3' THEN s.region
          ELSE NULL
        END as region,
        CASE
          WHEN f.storage_type = 'S3' THEN s.access_key_id
          ELSE NULL
        END as access_key_id,
        CASE
          WHEN f.storage_type = 'S3' THEN s.secret_access_key
          ELSE NULL
        END as secret_access_key,
        CASE
          WHEN f.storage_type = 'S3' THEN s.path_style
          ELSE NULL
        END as path_style,
        CASE
          WHEN f.storage_type = 'S3' THEN s.provider_type
          ELSE NULL
        END as storage_provider_type,
        CASE
          WHEN f.storage_type = 'S3' THEN s.name
          ELSE NULL
        END as storage_config_name
      FROM ${DbTables.FILES} f
      LEFT JOIN ${DbTables.S3_CONFIGS} s ON f.storage_type = 'S3' AND f.storage_config_id = s.id
      WHERE f.id = ?
      `,
      [fileId]
    );
  }

  /**
   * 创建文件记录
   * @param {Object} fileData - 文件数据
   * @returns {Promise<Object>} 创建结果
   */
  async createFile(fileData) {
    // 确保包含必要的时间戳
    const dataWithTimestamp = {
      ...fileData,
      created_at: fileData.created_at || new Date().toISOString(),
      updated_at: fileData.updated_at || new Date().toISOString(),
    };

    return await this.create(DbTables.FILES, dataWithTimestamp);
  }

  /**
   * 更新文件记录
   * @param {string} fileId - 文件ID
   * @param {Object} updateData - 更新数据
   * @returns {Promise<Object>} 更新结果
   */
  async updateFile(fileId, updateData) {
    // 自动更新修改时间
    const dataWithTimestamp = {
      ...updateData,
      updated_at: new Date().toISOString(),
    };

    return await this.update(DbTables.FILES, fileId, dataWithTimestamp);
  }

  /**
   * 增加文件查看次数
   * @param {string} fileId - 文件ID
   * @returns {Promise<Object>} 更新结果
   */
  async incrementViews(fileId) {
    return await this.execute(`UPDATE ${DbTables.FILES} SET views = views + 1, updated_at = CURRENT_TIMESTAMP WHERE id = ?`, [fileId]);
  }

  /**
   * 删除文件记录
   * @param {string} fileId - 文件ID
   * @returns {Promise<Object>} 删除结果
   */
  async deleteFile(fileId) {
    return await this.delete(DbTables.FILES, fileId);
  }

  /**
   * 根据存储路径删除文件记录
   * @param {string} storageConfigId - 存储配置ID
   * @param {string} storagePath - 存储路径
   * @param {string} storageType - 存储类型
   * @returns {Promise<Object>} 删除结果
   */
  async deleteByStorageConfigPath(storageConfigId, storagePath, storageType) {
    if (!storageConfigId || !storagePath || !storageType) {
      return { deletedCount: 0, message: "缺少必要参数" };
    }

    const result = await this.execute(`DELETE FROM ${DbTables.FILES} WHERE storage_config_id = ? AND storage_path = ? AND storage_type = ?`, [
      storageConfigId,
      storagePath,
      storageType,
    ]);

    return {
      deletedCount: result.meta?.changes || 0,
      message: `已删除${result.meta?.changes || 0}条文件记录`,
    };
  }

  /**
   * 根据存储配置ID查找文件
   * @param {string} storageConfigId - 存储配置ID
   * @param {string} storageType - 存储类型
   * @param {Object} options - 查询选项
   * @returns {Promise<Array>} 文件列表
   */
  async findByStorageConfigId(storageConfigId, storageType, options = {}) {
    if (!storageType) {
      throw new Error("存储类型参数是必需的");
    }
    return await this.findMany(
      DbTables.FILES,
      {
        storage_config_id: storageConfigId,
        storage_type: storageType,
      },
      options
    );
  }

  /**
   * 根据创建者查找文件
   * @param {string} createdBy - 创建者标识
   * @param {Object} options - 查询选项
   * @returns {Promise<Array>} 文件列表
   */
  async findByCreator(createdBy, options = {}) {
    return await this.findMany(DbTables.FILES, { created_by: createdBy }, options);
  }

  /**
   * 根据ID和创建者查找文件
   * @param {string} fileId - 文件ID
   * @param {string} createdBy - 创建者标识
   * @returns {Promise<Object|null>} 文件对象或null
   */
  async findByIdAndCreator(fileId, createdBy) {
    if (!fileId || !createdBy) return null;

    return await this.queryFirst(`SELECT * FROM ${DbTables.FILES} WHERE id = ? AND created_by = ?`, [fileId, createdBy]);
  }

  /**
   * 统计文件总大小
   * @param {Object} conditions - 统计条件
   * @returns {Promise<number>} 总大小（字节）
   */
  async getTotalSize(conditions = {}) {
    const fields = Object.keys(conditions);
    const values = Object.values(conditions);

    let sql = `SELECT COALESCE(SUM(size), 0) as total_size FROM ${DbTables.FILES}`;

    if (fields.length > 0) {
      const whereClause = fields.map((field) => `${field} = ?`).join(" AND ");
      sql += ` WHERE ${whereClause}`;
    }

    const result = await this.queryFirst(sql, values);
    return result?.total_size || 0;
  }

  /**
   * 获取指定存储配置的总使用量（排除指定文件）
   * @param {string} storageConfigId - 存储配置ID
   * @param {string} excludeFileId - 要排除的文件ID
   * @param {string} storageType - 存储类型
   * @returns {Promise<Object>} 包含total_used字段的对象
   */
  async getTotalSizeByStorageConfigExcludingFile(storageConfigId, excludeFileId, storageType) {
    if (!storageType) {
      throw new Error("存储类型参数是必需的");
    }
    const result = await this.queryFirst(`SELECT COALESCE(SUM(size), 0) as total_used FROM ${DbTables.FILES} WHERE storage_type = ? AND storage_config_id = ? AND id != ?`, [
      storageType,
      storageConfigId,
      excludeFileId,
    ]);
    return result;
  }

  /**
   * 批量删除文件记录
   * @param {Array<string>} fileIds - 文件ID数组
   * @returns {Promise<Object>} 删除结果
   */
  async batchDelete(fileIds) {
    if (!fileIds || fileIds.length === 0) {
      return { deletedCount: 0, message: "没有要删除的文件" };
    }

    const placeholders = fileIds.map(() => "?").join(",");
    const result = await this.execute(`DELETE FROM ${DbTables.FILES} WHERE id IN (${placeholders})`, fileIds);

    return {
      deletedCount: result.meta?.changes || 0,
      message: `已删除${result.meta?.changes || 0}条文件记录`,
    };
  }

  /**
   * 检查文件是否存在
   * @param {string} slug - 文件slug
   * @returns {Promise<boolean>} 是否存在
   */
  async existsBySlug(slug) {
    return await this.exists(DbTables.FILES, { slug });
  }

  /**
   * 根据slug查找文件（排除指定ID）
   * @param {string} slug - 文件slug
   * @param {string} excludeId - 要排除的文件ID
   * @returns {Promise<Object|null>} 文件对象或null
   */
  async findBySlugExcludingId(slug, excludeId) {
    return await this.queryFirst(`SELECT id FROM ${DbTables.FILES} WHERE slug = ? AND id != ?`, [slug, excludeId]);
  }

  /**
   * 检查存储路径是否存在
   * @param {string} storageConfigId - 存储配置ID
   * @param {string} storagePath - 存储路径
   * @param {string} storageType - 存储类型（必需参数）
   * @returns {Promise<boolean>} 是否存在
   */
  async existsByStoragePath(storageConfigId, storagePath, storageType) {
    if (!storageType) {
      throw new Error("存储类型参数是必需的");
    }
    return await this.exists(DbTables.FILES, {
      storage_config_id: storageConfigId,
      storage_path: storagePath,
      storage_type: storageType,
    });
  }

  /**
   * 根据存储路径查找文件
   * @param {string} storageConfigId - 存储配置ID
   * @param {string} storagePath - 存储路径
   * @param {string} storageType - 存储类型（必需参数）
   * @returns {Promise<Object|null>} 文件对象或null
   */
  async findByStoragePath(storageConfigId, storagePath, storageType) {
    if (!storageType) {
      throw new Error("存储类型参数是必需的");
    }
    return await this.findOne(DbTables.FILES, {
      storage_config_id: storageConfigId,
      storage_path: storagePath,
      storage_type: storageType,
    });
  }

  /**
   * 统计使用指定存储配置的文件数量
   * @param {string} storageConfigId - 存储配置ID
   * @param {string} storageType - 存储类型
   * @returns {Promise<number>} 文件数量
   */
  async countByStorageConfigId(storageConfigId, storageType) {
    if (!storageType) {
      throw new Error("存储类型参数是必需的");
    }
    return await super.count(DbTables.FILES, {
      storage_config_id: storageConfigId,
      storage_type: storageType,
    });
  }

  /**
   * 查找多个文件并关联存储配置
   * @param {Object} conditions - 查询条件
   * @param {Object} options - 查询选项
   * @returns {Promise<Array>} 包含存储配置的文件列表
   */
  async findManyWithStorageConfig(conditions = {}, options = {}) {
    const { orderBy = "created_at DESC", limit, offset } = options;

    // 构建WHERE条件
    const fields = Object.keys(conditions);
    const values = Object.values(conditions);

    let sql = `
      SELECT
        f.id, f.filename, f.slug, f.storage_path, f.storage_config_id,
        f.storage_type, f.file_path, f.mimetype, f.size, f.remark,
        f.created_at, f.views, f.max_views, f.expires_at, f.etag,
        f.password, f.created_by, f.use_proxy,
        CASE
          WHEN f.storage_type = 'S3' THEN s.name
          ELSE NULL
        END as storage_config_name,
        CASE
          WHEN f.storage_type = 'S3' THEN s.provider_type
          ELSE NULL
        END as storage_provider_type,
        CASE
          WHEN f.storage_type = 'S3' THEN s.id
          ELSE NULL
        END as storage_config_detail_id,
        ak.name as key_name
      FROM ${DbTables.FILES} f
      LEFT JOIN ${DbTables.S3_CONFIGS} s ON f.storage_type = 'S3' AND f.storage_config_id = s.id
      LEFT JOIN ${DbTables.API_KEYS} ak ON f.created_by LIKE 'apikey:%' AND ak.id = SUBSTR(f.created_by, 8)
    `;

    if (fields.length > 0) {
      const whereClause = fields.map((field) => `f.${field} = ?`).join(" AND ");
      sql += ` WHERE ${whereClause}`;
    }

    sql += ` ORDER BY f.${orderBy}`;

    if (limit) {
      sql += ` LIMIT ${limit}`;
      if (offset) {
        sql += ` OFFSET ${offset}`;
      }
    }

    const queryResult = await this.query(sql, values);
    return queryResult.results || [];
  }

  /**
   * 统计文件数量（支持条件）
   * @param {Object} conditions - 查询条件
   * @returns {Promise<number>} 文件数量
   */
  async count(conditions = {}) {
    return await super.count(DbTables.FILES, conditions);
  }

  // ==================== 密码管理方法 ====================

  /**
   * 创建文件明文密码记录
   * @param {string} fileId - 文件ID
   * @param {string} plainPassword - 明文密码
   * @returns {Promise<Object>} 创建结果
   */
  async createFilePasswordRecord(fileId, plainPassword) {
    return await this.execute(
      `INSERT INTO ${DbTables.FILE_PASSWORDS} (file_id, plain_password, created_at, updated_at)
       VALUES (?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,
      [fileId, plainPassword]
    );
  }

  /**
   * 更新文件的明文密码
   * @param {string} fileId - 文件ID
   * @param {string} plainPassword - 新的明文密码
   * @returns {Promise<Object>} 更新结果
   */
  async updateFilePasswordRecord(fileId, plainPassword) {
    return await this.execute(
      `UPDATE ${DbTables.FILE_PASSWORDS}
       SET plain_password = ?, updated_at = CURRENT_TIMESTAMP
       WHERE file_id = ?`,
      [plainPassword, fileId]
    );
  }

  /**
   * 删除文件的密码记录
   * @param {string} fileId - 文件ID
   * @returns {Promise<Object>} 删除结果
   */
  async deleteFilePasswordRecord(fileId) {
    return await this.execute(`DELETE FROM ${DbTables.FILE_PASSWORDS} WHERE file_id = ?`, [fileId]);
  }

  /**
   * 检查文件是否有密码记录
   * @param {string} fileId - 文件ID
   * @returns {Promise<boolean>} 是否存在密码记录
   */
  async hasFilePasswordRecord(fileId) {
    const result = await this.queryFirst(`SELECT file_id FROM ${DbTables.FILE_PASSWORDS} WHERE file_id = ?`, [fileId]);

    return !!result;
  }

  /**
   * 创建或更新文件密码记录
   * @param {string} fileId - 文件ID
   * @param {string} plainPassword - 明文密码
   * @returns {Promise<Object>} 操作结果
   */
  async upsertFilePasswordRecord(fileId, plainPassword) {
    const exists = await this.hasFilePasswordRecord(fileId);

    if (exists) {
      return await this.updateFilePasswordRecord(fileId, plainPassword);
    } else {
      return await this.createFilePasswordRecord(fileId, plainPassword);
    }
  }

  /**
   * 获取文件密码信息
   * @param {string} fileId - 文件ID
   * @returns {Promise<Object|null>} 密码信息对象，包含plain_password字段
   */
  async getFilePassword(fileId) {
    return await this.queryFirst(`SELECT plain_password FROM ${DbTables.FILE_PASSWORDS} WHERE file_id = ?`, [fileId]);
  }

  // ==================== 搜索方法 ====================

  /**
   * 搜索文件并关联存储配置
   * @param {string} searchTerm - 搜索关键词
   * @param {Object} options - 搜索选项
   * @param {string} options.createdBy - 创建者筛选
   * @param {number} options.limit - 每页条数
   * @param {number} options.offset - 偏移量
   * @returns {Promise<Object>} 搜索结果和分页信息
   */
  async searchWithStorageConfig(searchTerm, options = {}) {
    const { createdBy, limit = 30, offset = 0 } = options;

    // 构建搜索条件 - 搜索文件名、链接标识、备注
    const searchPattern = `%${searchTerm}%`;
    const searchConditions = ["f.filename LIKE ?", "f.slug LIKE ?", "f.remark LIKE ?"];

    let whereClause = `(${searchConditions.join(" OR ")})`;
    let params = [searchPattern, searchPattern, searchPattern];

    // 添加创建者筛选
    if (createdBy) {
      whereClause += " AND f.created_by = ?";
      params.push(createdBy);
    }

    // 查询数据
    const sql = `
      SELECT
        f.id, f.filename, f.slug, f.storage_path, f.storage_config_id,
        f.storage_type, f.file_path, f.mimetype, f.size, f.remark,
        f.created_at, f.views, f.max_views, f.expires_at, f.etag,
        f.password, f.created_by, f.use_proxy,
        CASE
          WHEN f.storage_type = 'S3' THEN s.name
          ELSE NULL
        END as storage_config_name,
        CASE
          WHEN f.storage_type = 'S3' THEN s.provider_type
          ELSE NULL
        END as storage_provider_type,
        CASE
          WHEN f.storage_type = 'S3' THEN s.id
          ELSE NULL
        END as storage_config_detail_id,
        ak.name as key_name
      FROM ${DbTables.FILES} f
      LEFT JOIN ${DbTables.S3_CONFIGS} s ON f.storage_type = 'S3' AND f.storage_config_id = s.id
      LEFT JOIN ${DbTables.API_KEYS} ak ON f.created_by LIKE 'apikey:%' AND ak.id = SUBSTR(f.created_by, 8)
      WHERE ${whereClause}
      ORDER BY f.created_at DESC
      LIMIT ? OFFSET ?
    `;

    params.push(limit, offset);
    const queryResult = await this.query(sql, params);
    const files = queryResult.results || [];

    // 获取搜索结果总数
    const countSql = `
      SELECT COUNT(*) as total
      FROM ${DbTables.FILES} f
      WHERE ${whereClause}
    `;
    const countParams = params.slice(0, -2); // 移除limit和offset参数
    const countResult = await this.queryFirst(countSql, countParams);
    const total = countResult?.total || 0;

    return {
      files,
      pagination: {
        total,
        limit,
        offset,
        hasMore: offset + limit < total,
      },
    };
  }
}
