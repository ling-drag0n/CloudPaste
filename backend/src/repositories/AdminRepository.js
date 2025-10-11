/**
 * 管理员Repository类
 * 负责管理员相关的数据访问操作
 */

import { BaseRepository } from "./BaseRepository.js";
import { DbTables } from "../constants/index.js";

export class AdminRepository extends BaseRepository {
  /**
   * 根据用户名查找管理员
   * @param {string} username - 用户名
   * @returns {Promise<Object|null>} 管理员对象或null
   */
  async findByUsername(username) {
    if (!username) return null;

    return await this.findOne(DbTables.ADMINS, { username });
  }

  /**
   * 根据ID查找管理员
   * @param {string} adminId - 管理员ID
   * @returns {Promise<Object|null>} 管理员对象或null
   */
  async findById(adminId) {
    return await super.findById(DbTables.ADMINS, adminId);
  }

  /**
   * 创建管理员
   * @param {Object} adminData - 管理员数据
   * @returns {Promise<Object>} 创建结果
   */
  async createAdmin(adminData) {
    // 确保包含必要的时间戳
    const dataWithTimestamp = {
      ...adminData,
      created_at: adminData.created_at || new Date().toISOString(),
      updated_at: adminData.updated_at || new Date().toISOString(),
    };

    return await this.create(DbTables.ADMINS, dataWithTimestamp);
  }

  /**
   * 更新管理员信息
   * @param {string} adminId - 管理员ID
   * @param {Object} updateData - 更新数据
   * @returns {Promise<Object>} 更新结果
   */
  async updateAdmin(adminId, updateData) {
    // 自动更新修改时间
    const dataWithTimestamp = {
      ...updateData,
      updated_at: new Date().toISOString(),
    };

    return await this.update(DbTables.ADMINS, adminId, dataWithTimestamp);
  }

  /**
   * 更新管理员最后登录时间
   * 注意：admins表中没有last_login字段，此方法仅更新updated_at字段作为登录记录
   * @param {string} adminId - 管理员ID
   * @returns {Promise<Object>} 更新结果
   */
  async updateLastLogin(adminId) {
    return await this.execute(`UPDATE ${DbTables.ADMINS} SET updated_at = CURRENT_TIMESTAMP WHERE id = ?`, [adminId]);
  }

  /**
   * 检查用户名是否已存在
   * @param {string} username - 用户名
   * @param {string} excludeId - 排除的管理员ID（用于更新时检查）
   * @returns {Promise<boolean>} 是否存在
   */
  async existsByUsername(username, excludeId = null) {
    if (excludeId) {
      const result = await this.queryFirst(`SELECT id FROM ${DbTables.ADMINS} WHERE username = ? AND id != ?`, [username, excludeId]);
      return !!result;
    } else {
      return await this.exists(DbTables.ADMINS, { username });
    }
  }

  /**
   * 获取管理员统计信息
   * @returns {Promise<Object>} 统计信息
   */
  async getStatistics() {
    const total = await super.count(DbTables.ADMINS);

    // 获取最近活跃的管理员数量（最近30天有更新的）
    // 注意：由于admins表没有last_login字段，使用updated_at字段作为活跃度指标
    const thirtyDaysAgo = new Date();
    thirtyDaysAgo.setDate(thirtyDaysAgo.getDate() - 30);

    const recentActive = await this.queryFirst(
      `SELECT COUNT(*) as count FROM ${DbTables.ADMINS}
       WHERE updated_at IS NOT NULL AND updated_at >= ?`,
      [thirtyDaysAgo.toISOString()]
    );

    return {
      total,
      recentActive: recentActive?.count || 0,
    };
  }

  /**
   * 获取所有管理员列表
   * @param {Object} options - 查询选项
   * @returns {Promise<Array>} 管理员列表
   */
  async findAll(options = {}) {
    const { orderBy = "created_at DESC", limit, offset } = options;

    return await this.findMany(DbTables.ADMINS, {}, { orderBy, limit, offset });
  }

  /**
   * 删除管理员
   * @param {string} adminId - 管理员ID
   * @returns {Promise<Object>} 删除结果
   */
  async deleteAdmin(adminId) {
    return await this.delete(DbTables.ADMINS, adminId);
  }

  // ==================== 管理员令牌管理方法 ====================

  /**
   * 根据令牌值查找令牌信息
   * @param {string} token - 令牌值
   * @returns {Promise<Object|null>} 令牌对象或null
   */
  async findTokenByValue(token) {
    if (!token) return null;

    return await this.queryFirst(
      `SELECT admin_id, expires_at, created_at
       FROM ${DbTables.ADMIN_TOKENS}
       WHERE token = ?`,
      [token]
    );
  }

  /**
   * 验证令牌（包含过期检查和自动清理）
   * @param {string} token - 令牌值
   * @returns {Promise<string|null>} 管理员ID或null
   */
  async validateToken(token) {
    if (!token) return null;

    try {
      const tokenInfo = await this.findTokenByValue(token);

      if (!tokenInfo) {
        return null;
      }

      const expiresAt = new Date(tokenInfo.expires_at);
      const now = new Date();

      // 检查令牌是否已过期
      if (now > expiresAt) {
        // 删除过期的令牌
        await this.deleteToken(token);
        return null;
      }

      return tokenInfo.admin_id;
    } catch (error) {
      console.error("验证令牌时发生错误:", error);
      return null;
    }
  }

  /**
   * 创建管理员令牌
   * @param {string} adminId - 管理员ID
   * @param {string} token - 令牌值
   * @param {Date|string} expiresAt - 过期时间
   * @returns {Promise<Object>} 创建结果
   */
  async createToken(adminId, token, expiresAt) {
    const expiresAtISO = expiresAt instanceof Date ? expiresAt.toISOString() : expiresAt;

    return await this.execute(
      `INSERT INTO ${DbTables.ADMIN_TOKENS} (token, admin_id, expires_at, created_at)
       VALUES (?, ?, ?, CURRENT_TIMESTAMP)`,
      [token, adminId, expiresAtISO]
    );
  }

  /**
   * 删除指定令牌
   * @param {string} token - 令牌值
   * @returns {Promise<Object>} 删除结果
   */
  async deleteToken(token) {
    return await this.execute(`DELETE FROM ${DbTables.ADMIN_TOKENS} WHERE token = ?`, [token]);
  }

  /**
   * 删除管理员的所有令牌
   * @param {string} adminId - 管理员ID
   * @returns {Promise<Object>} 删除结果
   */
  async deleteTokensByAdminId(adminId) {
    return await this.execute(`DELETE FROM ${DbTables.ADMIN_TOKENS} WHERE admin_id = ?`, [adminId]);
  }

  /**
   * 删除所有过期令牌
   * @param {Date} currentTime - 当前时间
   * @returns {Promise<Object>} 删除结果
   */
  async deleteExpiredTokens(currentTime = new Date()) {
    const result = await this.execute(`DELETE FROM ${DbTables.ADMIN_TOKENS} WHERE expires_at < ?`, [currentTime.toISOString()]);

    return {
      deletedCount: result.meta?.changes || 0,
      message: `已删除${result.meta?.changes || 0}个过期令牌`,
    };
  }

  /**
   * 清理指定管理员的过期令牌
   * @param {string} adminId - 管理员ID
   * @param {Date} currentTime - 当前时间
   * @returns {Promise<Object>} 删除结果
   */
  async cleanupTokensForAdmin(adminId, currentTime = new Date()) {
    const result = await this.execute(
      `DELETE FROM ${DbTables.ADMIN_TOKENS}
       WHERE admin_id = ? AND expires_at < ?`,
      [adminId, currentTime.toISOString()]
    );

    return {
      deletedCount: result.meta?.changes || 0,
      message: `已删除管理员${adminId}的${result.meta?.changes || 0}个过期令牌`,
    };
  }

  /**
   * 获取管理员的令牌列表（可选择是否包含过期token）
   * @param {string} adminId - 管理员ID
   * @param {Object} options - 查询选项
   * @param {boolean} options.includeExpired - 是否包含过期token，默认false
   * @param {string} options.orderBy - 排序方式，默认'created_at ASC'
   * @returns {Promise<Array>} 令牌列表
   */
  async getTokensForAdmin(adminId, options = {}) {
    const { includeExpired = false, orderBy = "created_at ASC" } = options;

    let sql = `SELECT token, created_at, expires_at FROM ${DbTables.ADMIN_TOKENS} WHERE admin_id = ?`;
    const params = [adminId];

    // 如果不包含过期token，添加过期时间条件
    if (!includeExpired) {
      const now = new Date().toISOString();
      sql += ` AND expires_at > ?`;
      params.push(now);
    }

    sql += ` ORDER BY ${orderBy}`;

    const result = await this.query(sql, params);
    return result.results || [];
  }

  /**
   * 获取管理员的所有有效令牌（向后兼容方法）
   * @param {string} adminId - 管理员ID
   * @returns {Promise<Array>} 有效令牌列表
   */
  async getValidTokensForAdmin(adminId) {
    return await this.getTokensForAdmin(adminId, { includeExpired: false, orderBy: "created_at ASC" });
  }

  /**
   * 获取管理员的所有令牌（包括过期的）
   * @param {string} adminId - 管理员ID
   * @returns {Promise<Array>} 所有令牌列表
   */
  async getAllTokensForAdmin(adminId) {
    return await this.getTokensForAdmin(adminId, { includeExpired: true, orderBy: "created_at DESC" });
  }

  /**
   * 获取管理员的令牌统计信息（基于getValidTokensForAdmin避免重复查询）
   * @param {string} adminId - 管理员ID
   * @returns {Promise<Object>} 令牌统计信息
   */
  async getTokenStatistics(adminId) {
    // 获取总令牌数
    const total = await super.count(DbTables.ADMIN_TOKENS, { admin_id: adminId });

    // 获取有效令牌数（复用getValidTokensForAdmin方法）
    const validTokens = await this.getValidTokensForAdmin(adminId);
    const valid = validTokens.length;

    return {
      total,
      valid,
      expired: total - valid,
    };
  }
}
