/**
 * 基础Repository类
 * 提供通用的数据访问方法，所有具体Repository继承此类
 * 职责：纯粹的数据访问，不包含业务逻辑
 */

export class BaseRepository {
  /**
   * 构造函数
   * @param {D1Database} db - D1数据库实例
   */
  constructor(db) {
    this.db = db;
  }

  /**
   * 根据ID查找记录
   * @param {string} table - 表名
   * @param {string} id - 记录ID
   * @returns {Promise<Object|null>} 记录对象或null
   */
  async findById(table, id) {
    if (!id) return null;
    
    return await this.db
      .prepare(`SELECT * FROM ${table} WHERE id = ?`)
      .bind(id)
      .first();
  }

  /**
   * 根据条件查找单条记录
   * @param {string} table - 表名
   * @param {Object} conditions - 查询条件 {field: value}
   * @returns {Promise<Object|null>} 记录对象或null
   */
  async findOne(table, conditions) {
    const fields = Object.keys(conditions);
    const values = Object.values(conditions);
    
    if (fields.length === 0) return null;
    
    const whereClause = fields.map(field => `${field} = ?`).join(' AND ');
    
    return await this.db
      .prepare(`SELECT * FROM ${table} WHERE ${whereClause}`)
      .bind(...values)
      .first();
  }

  /**
   * 根据条件查找多条记录
   * @param {string} table - 表名
   * @param {Object} conditions - 查询条件 {field: value}
   * @param {Object} options - 查询选项 {orderBy, limit, offset}
   * @returns {Promise<Array>} 记录数组
   */
  async findMany(table, conditions = {}, options = {}) {
    const fields = Object.keys(conditions);
    const values = Object.values(conditions);
    
    let sql = `SELECT * FROM ${table}`;
    
    if (fields.length > 0) {
      const whereClause = fields.map(field => `${field} = ?`).join(' AND ');
      sql += ` WHERE ${whereClause}`;
    }
    
    if (options.orderBy) {
      sql += ` ORDER BY ${options.orderBy}`;
    }
    
    if (options.limit) {
      sql += ` LIMIT ${options.limit}`;
      if (options.offset) {
        sql += ` OFFSET ${options.offset}`;
      }
    }
    
    const result = await this.db
      .prepare(sql)
      .bind(...values)
      .all();
      
    return result.results || [];
  }

  /**
   * 创建记录
   * @param {string} table - 表名
   * @param {Object} data - 数据对象
   * @returns {Promise<Object>} 执行结果
   */
  async create(table, data) {
    const fields = Object.keys(data);
    const values = Object.values(data);
    
    if (fields.length === 0) {
      throw new Error('创建记录时数据不能为空');
    }
    
    const fieldsClause = fields.join(', ');
    const placeholders = fields.map(() => '?').join(', ');
    
    return await this.db
      .prepare(`INSERT INTO ${table} (${fieldsClause}) VALUES (${placeholders})`)
      .bind(...values)
      .run();
  }

  /**
   * 更新记录
   * @param {string} table - 表名
   * @param {string} id - 记录ID
   * @param {Object} data - 更新数据
   * @returns {Promise<Object>} 执行结果
   */
  async update(table, id, data) {
    const fields = Object.keys(data);
    const values = Object.values(data);
    
    if (fields.length === 0) {
      throw new Error('更新记录时数据不能为空');
    }
    
    const setClause = fields.map(field => `${field} = ?`).join(', ');
    
    return await this.db
      .prepare(`UPDATE ${table} SET ${setClause} WHERE id = ?`)
      .bind(...values, id)
      .run();
  }

  /**
   * 删除记录
   * @param {string} table - 表名
   * @param {string} id - 记录ID
   * @returns {Promise<Object>} 执行结果
   */
  async delete(table, id) {
    return await this.db
      .prepare(`DELETE FROM ${table} WHERE id = ?`)
      .bind(id)
      .run();
  }

  /**
   * 根据条件删除记录
   * @param {string} table - 表名
   * @param {Object} conditions - 删除条件
   * @returns {Promise<Object>} 执行结果
   */
  async deleteWhere(table, conditions) {
    const fields = Object.keys(conditions);
    const values = Object.values(conditions);
    
    if (fields.length === 0) {
      throw new Error('删除记录时条件不能为空');
    }
    
    const whereClause = fields.map(field => `${field} = ?`).join(' AND ');
    
    return await this.db
      .prepare(`DELETE FROM ${table} WHERE ${whereClause}`)
      .bind(...values)
      .run();
  }

  /**
   * 计算记录数量
   * @param {string} table - 表名
   * @param {Object} conditions - 查询条件
   * @returns {Promise<number>} 记录数量
   */
  async count(table, conditions = {}) {
    const fields = Object.keys(conditions);
    const values = Object.values(conditions);
    
    let sql = `SELECT COUNT(*) as count FROM ${table}`;
    
    if (fields.length > 0) {
      const whereClause = fields.map(field => `${field} = ?`).join(' AND ');
      sql += ` WHERE ${whereClause}`;
    }
    
    const result = await this.db
      .prepare(sql)
      .bind(...values)
      .first();
      
    return result?.count || 0;
  }

  /**
   * 检查记录是否存在
   * @param {string} table - 表名
   * @param {Object} conditions - 查询条件
   * @returns {Promise<boolean>} 是否存在
   */
  async exists(table, conditions) {
    const count = await this.count(table, conditions);
    return count > 0;
  }

  /**
   * 执行自定义SQL查询
   * @param {string} sql - SQL语句
   * @param {Array} params - 参数数组
   * @returns {Promise<Object>} 查询结果
   */
  async query(sql, params = []) {
    return await this.db
      .prepare(sql)
      .bind(...params)
      .all();
  }

  /**
   * 执行自定义SQL查询（单条记录）
   * @param {string} sql - SQL语句
   * @param {Array} params - 参数数组
   * @returns {Promise<Object|null>} 查询结果
   */
  async queryFirst(sql, params = []) {
    return await this.db
      .prepare(sql)
      .bind(...params)
      .first();
  }

  /**
   * 执行自定义SQL（增删改）
   * @param {string} sql - SQL语句
   * @param {Array} params - 参数数组
   * @returns {Promise<Object>} 执行结果
   */
  async execute(sql, params = []) {
    return await this.db
      .prepare(sql)
      .bind(...params)
      .run();
  }
}
