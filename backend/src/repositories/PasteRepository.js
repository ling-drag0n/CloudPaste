/**
 * 文本分享Repository类
 * 负责文本分享相关的数据访问操作
 */

import { BaseRepository } from "./BaseRepository.js";
import { DbTables } from "../constants/index.js";

export class PasteRepository extends BaseRepository {
  /**
   * 根据slug查找文本分享
   * @param {string} slug - 唯一标识
   * @returns {Promise<Object|null>} 文本分享对象或null
   */
  async findBySlug(slug) {
    if (!slug) return null;

    return await this.findOne(DbTables.PASTES, { slug });
  }

  /**
   * 根据ID查找文本分享
   * @param {string} pasteId - 文本分享ID
   * @returns {Promise<Object|null>} 文本分享对象或null
   */
  async findById(pasteId) {
    return await super.findById(DbTables.PASTES, pasteId);
  }

  /**
   * 获取所有文本分享列表（分页）
   * @param {Object} options - 查询选项
   * @returns {Promise<Object>} 包含结果和分页信息的对象
   */
  async findAllWithPagination(options = {}) {
    const { page = 1, limit = 10, created_by = null, orderBy = "created_at DESC" } = options;
    const offset = (page - 1) * limit;

    // 构建查询条件
    const conditions = {};
    if (created_by) {
      conditions.created_by = created_by;
    }

    // 获取总数
    const total = await this.count(DbTables.PASTES, conditions);

    // 获取数据
    const results = await this.findMany(DbTables.PASTES, conditions, { orderBy, limit, offset });

    return {
      results,
      pagination: {
        page,
        limit,
        total,
        totalPages: Math.ceil(total / limit),
        hasNext: page * limit < total,
        hasPrev: page > 1,
      },
    };
  }

  /**
   * 根据创建者获取文本分享列表
   * @param {string} created_by - 创建者标识
   * @param {Object} options - 查询选项
   * @returns {Promise<Array>} 文本分享列表
   */
  async findByCreator(created_by, options = {}) {
    const { orderBy = "created_at DESC", limit, offset } = options;

    return await this.findMany(DbTables.PASTES, { created_by: created_by }, { orderBy, limit, offset });
  }

  /**
   * 根据ID和创建者查找文本分享
   * @param {string} pasteId - 文本分享ID
   * @param {string} created_by - 创建者标识
   * @returns {Promise<Object|null>} 文本分享对象或null
   */
  async findByIdAndCreator(pasteId, created_by) {
    if (!pasteId || !created_by) return null;

    return await this.findOne(DbTables.PASTES, {
      id: pasteId,
      created_by: created_by,
    });
  }

  /**
   * 创建文本分享
   * @param {Object} pasteData - 文本分享数据
   * @returns {Promise<Object>} 创建结果
   */
  async createPaste(pasteData) {
    // 确保包含必要的时间戳
    const dataWithTimestamp = {
      ...pasteData,
      created_at: pasteData.created_at || new Date().toISOString(),
      updated_at: pasteData.updated_at || new Date().toISOString(),
    };

    return await this.create(DbTables.PASTES, dataWithTimestamp);
  }

  /**
   * 更新文本分享
   * @param {string} pasteId - 文本分享ID
   * @param {Object} updateData - 更新数据
   * @returns {Promise<Object>} 更新结果
   */
  async updatePaste(pasteId, updateData) {
    // 自动更新修改时间
    const dataWithTimestamp = {
      ...updateData,
      updated_at: new Date().toISOString(),
    };

    return await this.update(DbTables.PASTES, pasteId, dataWithTimestamp);
  }

  /**
   * 增加文本分享查看次数
   * @param {string} pasteId - 文本分享ID
   * @returns {Promise<Object>} 更新结果
   */
  async incrementViews(pasteId) {
    return await this.execute(`UPDATE ${DbTables.PASTES} SET views = views + 1, updated_at = CURRENT_TIMESTAMP WHERE id = ?`, [pasteId]);
  }

  /**
   * 删除文本分享
   * @param {string} pasteId - 文本分享ID
   * @returns {Promise<Object>} 删除结果
   */
  async deletePaste(pasteId) {
    return await this.delete(DbTables.PASTES, pasteId);
  }

  /**
   * 批量删除文本分享
   * @param {Array<string>} pasteIds - 文本分享ID数组
   * @returns {Promise<Object>} 删除结果
   */
  async batchDelete(pasteIds) {
    if (!pasteIds || pasteIds.length === 0) {
      return { deletedCount: 0, message: "没有要删除的文本分享" };
    }

    const placeholders = pasteIds.map(() => "?").join(",");
    const result = await this.execute(`DELETE FROM ${DbTables.PASTES} WHERE id IN (${placeholders})`, pasteIds);

    return {
      deletedCount: result.meta?.changes || 0,
      message: `已删除${result.meta?.changes || 0}个文本分享`,
    };
  }

  /**
   * 删除过期的文本分享
   * @param {Date} currentTime - 当前时间
   * @returns {Promise<Object>} 删除结果
   */
  async deleteExpired(currentTime = new Date()) {
    const result = await this.execute(
      `DELETE FROM ${DbTables.PASTES} 
       WHERE expires_at IS NOT NULL AND expires_at < ?`,
      [currentTime.toISOString()]
    );

    return {
      deletedCount: result.meta?.changes || 0,
      message: `已删除${result.meta?.changes || 0}个过期文本分享`,
    };
  }

  /**
   * 删除超过最大查看次数的文本分享
   * @returns {Promise<Object>} 删除结果
   */
  async deleteOverViewLimit() {
    const result = await this.execute(
      `DELETE FROM ${DbTables.PASTES} 
       WHERE max_views IS NOT NULL AND max_views > 0 AND views >= max_views`
    );

    return {
      deletedCount: result.meta?.changes || 0,
      message: `已删除${result.meta?.changes || 0}个超限文本分享`,
    };
  }

  /**
   * 查找过期文本分享
   * @param {Date} currentTime - 当前时间
   * @returns {Promise<Array>} 过期文本分享列表
   */
  async findExpired(currentTime = new Date()) {
    const result = await this.query(
      `SELECT * FROM ${DbTables.PASTES} 
       WHERE expires_at IS NOT NULL AND expires_at < ?`,
      [currentTime.toISOString()]
    );

    return result.results || [];
  }

  /**
   * 查找超过最大查看次数的文本分享
   * @returns {Promise<Array>} 超限文本分享列表
   */
  async findOverViewLimit() {
    const result = await this.query(
      `SELECT * FROM ${DbTables.PASTES} 
       WHERE max_views IS NOT NULL AND max_views > 0 AND views >= max_views`
    );

    return result.results || [];
  }

  /**
   * 检查slug是否已存在
   * @param {string} slug - 唯一标识
   * @param {string} excludeId - 排除的文本分享ID（用于更新时检查）
   * @returns {Promise<boolean>} 是否存在
   */
  async existsBySlug(slug, excludeId = null) {
    if (excludeId) {
      const result = await this.queryFirst(`SELECT id FROM ${DbTables.PASTES} WHERE slug = ? AND id != ?`, [slug, excludeId]);
      return !!result;
    } else {
      return await this.exists(DbTables.PASTES, { slug });
    }
  }

  /**
   * 获取文本分享统计信息
   * @param {string} created_by - 创建者标识（可选）
   * @returns {Promise<Object>} 统计信息
   */
  async getStatistics(created_by = null) {
    const conditions = created_by ? { created_by: created_by } : {};

    const total = await this.count(DbTables.PASTES, conditions);

    // 获取最近一周的数据
    const sevenDaysAgo = new Date();
    sevenDaysAgo.setDate(sevenDaysAgo.getDate() - 7);

    let sql = `SELECT COUNT(*) as count FROM ${DbTables.PASTES} WHERE created_at >= ?`;
    const params = [sevenDaysAgo.toISOString()];

    if (created_by) {
      sql += ` AND created_by = ?`;
      params.push(created_by);
    }

    const recentCount = await this.queryFirst(sql, params);

    return {
      total,
      recentWeek: recentCount?.count || 0,
    };
  }

  // ==================== 密码管理方法 ====================

  /**
   * 创建文本分享明文密码记录
   * @param {string} pasteId - 文本分享ID
   * @param {string} plainPassword - 明文密码
   * @returns {Promise<Object>} 创建结果
   */
  async createPasswordRecord(pasteId, plainPassword) {
    return await this.execute(
      `INSERT INTO ${DbTables.PASTE_PASSWORDS} (paste_id, plain_password, created_at, updated_at)
       VALUES (?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,
      [pasteId, plainPassword]
    );
  }

  /**
   * 查找文本分享的明文密码
   * @param {string} pasteId - 文本分享ID
   * @returns {Promise<string|null>} 明文密码或null
   */
  async findPasswordByPasteId(pasteId) {
    const result = await this.queryFirst(`SELECT plain_password FROM ${DbTables.PASTE_PASSWORDS} WHERE paste_id = ?`, [pasteId]);

    return result?.plain_password || null;
  }

  /**
   * 更新文本分享的明文密码
   * @param {string} pasteId - 文本分享ID
   * @param {string} plainPassword - 新的明文密码
   * @returns {Promise<Object>} 更新结果
   */
  async updatePasswordRecord(pasteId, plainPassword) {
    return await this.execute(
      `UPDATE ${DbTables.PASTE_PASSWORDS}
       SET plain_password = ?, updated_at = CURRENT_TIMESTAMP
       WHERE paste_id = ?`,
      [plainPassword, pasteId]
    );
  }

  /**
   * 删除文本分享的密码记录
   * @param {string} pasteId - 文本分享ID
   * @returns {Promise<Object>} 删除结果
   */
  async deletePasswordRecord(pasteId) {
    return await this.execute(`DELETE FROM ${DbTables.PASTE_PASSWORDS} WHERE paste_id = ?`, [pasteId]);
  }

  /**
   * 检查文本分享是否有密码记录
   * @param {string} pasteId - 文本分享ID
   * @returns {Promise<boolean>} 是否存在密码记录
   */
  async hasPasswordRecord(pasteId) {
    const result = await this.queryFirst(`SELECT paste_id FROM ${DbTables.PASTE_PASSWORDS} WHERE paste_id = ?`, [pasteId]);

    return !!result;
  }

  /**
   * 创建或更新密码记录
   * @param {string} pasteId - 文本分享ID
   * @param {string} plainPassword - 明文密码
   * @returns {Promise<Object>} 操作结果
   */
  async upsertPasswordRecord(pasteId, plainPassword) {
    const exists = await this.hasPasswordRecord(pasteId);

    if (exists) {
      return await this.updatePasswordRecord(pasteId, plainPassword);
    } else {
      return await this.createPasswordRecord(pasteId, plainPassword);
    }
  }

  // ==================== 复合操作方法 ====================

  /**
   * 创建文本分享（包含密码处理）
   * @param {Object} pasteData - 文本分享数据
   * @param {string} plainPassword - 明文密码（可选）
   * @returns {Promise<Object>} 创建结果
   */
  async createPasteWithPassword(pasteData, plainPassword = null) {
    // 确保包含必要的时间戳
    const dataWithTimestamp = {
      ...pasteData,
      created_at: pasteData.created_at || new Date().toISOString(),
      updated_at: pasteData.updated_at || new Date().toISOString(),
    };

    // 创建文本分享记录
    const createResult = await this.create(DbTables.PASTES, dataWithTimestamp);

    // 如果提供了明文密码，创建密码记录
    if (plainPassword) {
      await this.createPasswordRecord(pasteData.id, plainPassword);
    }

    return createResult;
  }

  /**
   * 查找文本分享（包含密码信息）
   * @param {string} slug - 唯一标识
   * @param {boolean} includePassword - 是否包含明文密码
   * @returns {Promise<Object|null>} 文本分享对象（包含密码信息）或null
   */
  async findBySlugWithPassword(slug, includePassword = false) {
    const paste = await this.findBySlug(slug);

    if (!paste) return null;

    // 如果需要包含密码且文本有密码
    if (includePassword && paste.password) {
      const plainPassword = await this.findPasswordByPasteId(paste.id);
      return {
        ...paste,
        plain_password: plainPassword,
      };
    }

    return paste;
  }

  /**
   * 查找文本分享（包含密码信息）
   * @param {string} pasteId - 文本分享ID
   * @param {boolean} includePassword - 是否包含明文密码
   * @returns {Promise<Object|null>} 文本分享对象（包含密码信息）或null
   */
  async findByIdWithPassword(pasteId, includePassword = false) {
    const paste = await this.findById(pasteId);

    if (!paste) return null;

    // 如果需要包含密码且文本有密码
    if (includePassword && paste.password) {
      const plainPassword = await this.findPasswordByPasteId(paste.id);
      return {
        ...paste,
        plain_password: plainPassword,
      };
    }

    return paste;
  }

  /**
   * 更新文本分享（包含密码处理）
   * @param {string} pasteId - 文本分享ID
   * @param {Object} updateData - 更新数据
   * @param {string} newPlainPassword - 新的明文密码（可选）
   * @param {boolean} clearPassword - 是否清除密码
   * @returns {Promise<Object>} 更新结果
   */
  async updatePasteWithPassword(pasteId, updateData, newPlainPassword = null, clearPassword = false) {
    // 更新文本分享记录
    const updateResult = await this.updatePaste(pasteId, updateData);

    // 处理密码更新
    if (clearPassword) {
      // 清除密码
      await this.deletePasswordRecord(pasteId);
    } else if (newPlainPassword) {
      // 更新或创建密码记录
      await this.upsertPasswordRecord(pasteId, newPlainPassword);
    }

    return updateResult;
  }

  // ==================== 管理员专用查询方法 ====================

  /**
   * 获取管理员文本分享列表（包含内容预览和格式化字段）
   * @param {Object} options - 查询选项
   * @returns {Promise<Object>} 包含结果和分页信息的对象
   */
  async findAllForAdmin(options = {}) {
    const { page = 1, limit = 10, offset = null, created_by = null, search = null } = options;

    // 支持两种分页模式：page模式和offset模式
    const actualOffset = offset !== null ? offset : (page - 1) * limit;

    // 构建查询条件
    let whereClause = "";
    let countWhereClause = "";
    const queryParams = [];
    const countParams = [];

    const conditions = [];

    // 添加搜索条件 - 搜索链接标识、备注、内容
    if (search && search.trim()) {
      const searchPattern = `%${search.trim()}%`;
      conditions.push("(slug LIKE ? OR remark LIKE ? OR content LIKE ?)");
      queryParams.push(searchPattern, searchPattern, searchPattern);
      countParams.push(searchPattern, searchPattern, searchPattern);
    }

    // 添加创建者筛选
    if (created_by) {
      conditions.push("created_by = ?");
      queryParams.push(created_by);
      countParams.push(created_by);
    }

    // 构建WHERE子句
    if (conditions.length > 0) {
      whereClause = " WHERE " + conditions.join(" AND ");
      countWhereClause = " WHERE " + conditions.join(" AND ");
    }

    // 获取总数
    const countSql = `SELECT COUNT(*) as total FROM ${DbTables.PASTES}${countWhereClause}`;
    const countResult = await this.queryFirst(countSql, countParams);
    const total = countResult?.total || 0;

    // 查询数据（包含格式化字段）
    const querySql = `
      SELECT
        id,
        slug,
        remark,
        password IS NOT NULL as has_password,
        expires_at,
        max_views,
        views as view_count,
        created_by,
        CASE
          WHEN LENGTH(content) > 200 THEN SUBSTR(content, 1, 200) || '...'
          ELSE content
        END as content_preview,
        created_at,
        updated_at
      FROM ${DbTables.PASTES}${whereClause}
      ORDER BY created_at DESC
      LIMIT ? OFFSET ?
    `;

    queryParams.push(limit, actualOffset);
    const queryResult = await this.query(querySql, queryParams);
    const results = queryResult.results || [];

    return {
      results,
      pagination: {
        total,
        limit,
        offset: actualOffset,
        hasMore: actualOffset + limit < total,
        // 保持向后兼容
        page: Math.floor(actualOffset / limit) + 1,
        totalPages: Math.ceil(total / limit),
      },
    };
  }

  /**
   * 获取用户文本分享列表（包含完整内容）
   * @param {string} created_by - 创建者标识
   * @param {Object} options - 查询选项
   * @returns {Promise<Object>} 包含结果和分页信息的对象
   */
  async findByCreatorWithPagination(created_by, options = {}) {
    const { limit = 30, offset = 0, search = null } = options;

    // 构建查询条件
    let whereClause = "WHERE created_by = ?";
    let params = [created_by];

    // 添加搜索条件 - 搜索链接标识、备注、内容
    if (search && search.trim()) {
      const searchPattern = `%${search.trim()}%`;
      whereClause += " AND (slug LIKE ? OR remark LIKE ? OR content LIKE ?)";
      params.push(searchPattern, searchPattern, searchPattern);
    }

    // 查询数据
    const querySql = `
      SELECT id, slug, content, remark, password IS NOT NULL as has_password,
      expires_at, max_views, views, created_at, updated_at, created_by
      FROM ${DbTables.PASTES}
      ${whereClause}
      ORDER BY created_at DESC
      LIMIT ? OFFSET ?
    `;

    params.push(limit, offset);
    const queryResult = await this.query(querySql, params);
    const results = queryResult.results || [];

    // 获取总数（包含搜索条件）
    const countSql = `SELECT COUNT(*) as total FROM ${DbTables.PASTES} ${whereClause}`;
    const countParams = params.slice(0, -2); // 移除limit和offset参数
    const countResult = await this.queryFirst(countSql, countParams);
    const total = countResult?.total || 0;

    return {
      results,
      pagination: {
        total,
        limit,
        offset,
        hasMore: offset + limit < total,
      },
    };
  }

  // ==================== 更新专用方法 ====================

  /**
   * 根据条件查找文本分享（用于更新前的权限检查）
   * @param {string} slug - 唯一标识
   * @param {string} created_by - 创建者标识（可选，用于权限检查）
   * @returns {Promise<Object|null>} 文本分享对象或null
   */
  async findBySlugForUpdate(slug, created_by = null) {
    let sql = `SELECT id, slug, expires_at, max_views, views FROM ${DbTables.PASTES} WHERE slug = ?`;
    const params = [slug];

    if (created_by) {
      sql += ` AND created_by = ?`;
      params.push(created_by);
    }

    return await this.queryFirst(sql, params);
  }

  /**
   * 更新文本分享（支持复杂的字段更新）
   * @param {string} pasteId - 文本分享ID
   * @param {Object} updateData - 更新数据
   * @param {Object} options - 更新选项
   * @returns {Promise<Object>} 更新结果
   */
  async updatePasteComplex(pasteId, updateData, options = {}) {
    const { newSlug = null, passwordHash = null, clearPassword = false, resetViews = false, newViewsValue = 0 } = options;

    // 构建动态 SQL
    const updateFields = [];
    const params = [];

    // 处理密码更新
    if (clearPassword) {
      updateFields.push("password = NULL");
    } else if (passwordHash) {
      updateFields.push("password = ?");
      params.push(passwordHash);
    }

    // 处理 slug 更新
    if (newSlug) {
      updateFields.push("slug = ?");
      params.push(newSlug);
    }

    // 处理基本字段更新
    if (updateData.content !== undefined) {
      updateFields.push("content = ?");
      params.push(updateData.content);
    }

    if (updateData.remark !== undefined) {
      updateFields.push("remark = ?");
      params.push(updateData.remark || null);
    }

    if (updateData.expires_at !== undefined) {
      updateFields.push("expires_at = ?");
      params.push(updateData.expires_at || null);
    }

    if (updateData.max_views !== undefined) {
      updateFields.push("max_views = ?");
      params.push(updateData.max_views || null);
    }

    if (resetViews) {
      updateFields.push("views = ?");
      params.push(newViewsValue);
    }

    // 总是更新修改时间
    updateFields.push("updated_at = CURRENT_TIMESTAMP");

    // 添加 WHERE 条件的参数
    params.push(pasteId);

    const sql = `
      UPDATE ${DbTables.PASTES}
      SET ${updateFields.join(", ")}
      WHERE id = ?
    `;

    return await this.execute(sql, params);
  }

  // ==================== 批量删除专用方法 ====================

  /**
   * 批量删除指定创建者的文本分享
   * @param {Array<string>} pasteIds - 文本分享ID数组
   * @param {string} created_by - 创建者标识
   * @returns {Promise<Object>} 删除结果
   */
  async batchDeleteByCreator(pasteIds, created_by) {
    if (!pasteIds || pasteIds.length === 0) {
      return { deletedCount: 0, message: "没有要删除的文本分享" };
    }

    const placeholders = pasteIds.map(() => "?").join(",");
    const params = [...pasteIds, created_by];

    const result = await this.execute(`DELETE FROM ${DbTables.PASTES} WHERE id IN (${placeholders}) AND created_by = ?`, params);

    return {
      deletedCount: result.meta?.changes || 0,
      message: `已删除${result.meta?.changes || 0}个文本分享`,
    };
  }
}
