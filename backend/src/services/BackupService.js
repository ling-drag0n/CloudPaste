import crypto from "crypto";
import { DbTables } from "../constants/index.js";

/**
 * 数据备份与还原服务
 * 提供数据库的备份和还原功能
 */
export class BackupService {
  constructor(db) {
    this.db = db;

    // 模块与数据表的映射关系
    this.moduleTableMapping = {
      text_management: ["pastes", "paste_passwords"],
      file_management: ["files", "file_passwords"],
      mount_management: ["storage_mounts"],
      storage_config: ["s3_configs"],
      key_management: ["api_keys"],
      account_management: ["admins", "admin_tokens"],
      system_settings: ["system_settings"],
    };

    // 表的依赖关系（用于确定导入顺序）
    // 基于实际的外键约束关系和应用层依赖关系
    this.tableDependencies = {
      paste_passwords: ["pastes"],
      file_passwords: ["files"],
      admin_tokens: ["admins"],
      s3_configs: ["admins"], // s3_configs.admin_id -> admins.id
      storage_mounts: ["s3_configs"], // storage_mounts.storage_config_id -> s3_configs.id
    };
  }

  /**
   * 创建备份
   * @param {Object} options - 备份选项
   * @returns {Object} 备份数据
   */
  async createBackup(options = {}) {
    const { backup_type = "full", selected_modules = [] } = options;

    let tables;
    let finalModules = selected_modules;
    let dependencies = []; // 在函数作用域定义

    if (backup_type === "full") {
      // 完整备份 - 所有表
      tables = Object.values(DbTables);
    } else if (backup_type === "modules") {
      // 检查并自动包含依赖模块
      dependencies = this.getModuleDependencies(selected_modules);
      if (dependencies.length > 0) {
        finalModules = [...new Set([...selected_modules, ...dependencies])];
        console.log(`[BackupService] 检测到跨模块依赖，自动包含模块: ${dependencies.join(", ")}`);
      }

      // 模块备份 - 根据选中的模块确定表
      tables = this.getTablesFromModules(finalModules);
    } else {
      throw new Error("不支持的备份类型");
    }

    // 导出数据
    const data = await this.exportTables(tables);

    // 生成元数据
    const metadata = {
      version: "1.0",
      timestamp: new Date().toISOString(),
      backup_type,
      selected_modules: backup_type === "modules" ? selected_modules : null,
      included_modules: backup_type === "modules" ? finalModules : null, // 记录实际包含的模块
      auto_included_dependencies: backup_type === "modules" ? dependencies : null, // 记录自动包含的依赖
      tables: Object.keys(data).reduce((acc, table) => {
        acc[table] = data[table].length;
        return acc;
      }, {}),
      total_records: Object.values(data).reduce((sum, arr) => sum + arr.length, 0),
      checksum: this.generateChecksum(data),
    };

    return {
      metadata,
      data,
    };
  }

  /**
   * 获取模块的依赖模块
   * @param {Array} selectedModules - 选中的模块列表
   * @returns {Array} 依赖的模块列表
   */
  getModuleDependencies(selectedModules) {
    const dependencies = new Set();

    // 定义模块间的依赖关系
    const moduleDependencies = {
      mount_management: ["storage_config"], // 挂载管理依赖S3配置管理
      file_management: ["storage_config"], // 文件管理可能依赖存储配置（通过storage_config_id）
    };

    for (const module of selectedModules) {
      if (moduleDependencies[module]) {
        moduleDependencies[module].forEach((dep) => {
          // 只有当依赖模块未被选中时才添加
          if (!selectedModules.includes(dep)) {
            dependencies.add(dep);
          }
        });
      }
    }

    return Array.from(dependencies);
  }

  /**
   * 还原备份
   * @param {Object} backupData - 备份数据
   * @param {Object} options - 还原选项
   * @param {string} options.mode - 还原模式 ('overwrite' | 'merge')
   * @param {string} options.currentAdminId - 当前管理员ID（合并模式下用于映射admin_id）
   * @param {boolean} options.skipIntegrityCheck - 是否跳过数据完整性检查
   * @param {boolean} options.preserveTimestamps - 是否保留原始时间戳
   * @returns {Object} 还原结果
   */
  async restoreBackup(backupData, options = {}) {
    const { mode = "overwrite", currentAdminId, skipIntegrityCheck = false, preserveTimestamps = false } = options;

    // 验证备份数据
    this.validateBackupData(backupData);

    let { data } = backupData;

    // 进行 admin_id 映射（覆盖和合并模式）
    if (currentAdminId && (mode === "merge" || mode === "overwrite")) {
      data = this.mapAdminIds(data, currentAdminId);
    }

    const tables = Object.keys(data);

    // 验证表是否存在
    await this.validateTablesExist(tables);

    // 数据完整性检查
    if (!skipIntegrityCheck) {
      const integrityIssues = await this.validateDataIntegrity(data);
      if (integrityIssues.length > 0) {
        console.warn(`[BackupService] 发现 ${integrityIssues.length} 个数据完整性问题:`);
        integrityIssues.forEach((issue) => console.warn(`  - ${issue.message}`));

        // 在严格模式下可以选择抛出错误
        // if (options.strictIntegrityCheck) {
        //   throw new Error(`数据完整性检查失败: ${integrityIssues.map(i => i.message).join('; ')}`);
        // }
      }
    }

    // 按依赖关系排序表
    const orderedTables = this.sortTablesByDependency(tables);

    // 记录每个表的预期记录数和语句映射
    const expectedResults = {};
    const statementTableMap = []; // 记录每个语句对应的表名

    try {
      // 收集所有需要执行的语句
      const statements = [];

      // 使用D1的延迟外键约束功能
      statements.push(this.db.prepare("PRAGMA defer_foreign_keys = ON"));
      statementTableMap.push("_pragma"); // 标记为系统语句

      // 在覆盖模式下，按正确顺序删除数据
      if (mode === "overwrite") {
        // 按依赖关系的逆序删除（子表先删除）
        const reversedTables = [...orderedTables].reverse();
        for (const tableName of reversedTables) {
          statements.push(this.db.prepare(`DELETE FROM ${tableName}`));
          statementTableMap.push(`_delete_${tableName}`); // 标记为删除语句
        }
      }

      // 按依赖关系顺序插入数据
      for (const tableName of orderedTables) {
        const tableData = data[tableName];
        expectedResults[tableName] = {
          expected: tableData ? tableData.length : 0,
          statementIndices: [], // 记录该表对应的语句索引
        };

        if (tableData && tableData.length > 0) {
          // 准备插入语句
          for (const record of tableData) {
            // 处理时间戳字段
            const processedRecord = this.processTimestampFields(record, tableName, {
              preserveTimestamps,
              updateTimestamps: mode === "merge", // 合并模式下更新时间戳
            });

            const fields = Object.keys(processedRecord);
            const values = Object.values(processedRecord);
            const placeholders = fields.map(() => "?").join(", ");

            let sql;
            if (mode === "overwrite") {
              // 覆盖模式使用INSERT（表已清空）
              sql = `INSERT INTO ${tableName} (${fields.join(", ")}) VALUES (${placeholders})`;
            } else {
              // 合并模式使用INSERT OR IGNORE（避免冲突）
              sql = `INSERT OR IGNORE INTO ${tableName} (${fields.join(", ")}) VALUES (${placeholders})`;
            }

            const statementIndex = statements.length;
            statements.push(this.db.prepare(sql).bind(...values));
            statementTableMap.push(tableName);
            expectedResults[tableName].statementIndices.push(statementIndex);
          }
        }
      }

      // 在batch结束时恢复外键约束检查（可选，因为事务结束时会自动恢复）
      statements.push(this.db.prepare("PRAGMA defer_foreign_keys = OFF"));
      statementTableMap.push("_pragma"); // 标记为系统语句

      // 使用batch方法原子性执行所有语句
      let batchResults = [];
      if (statements.length > 0) {
        batchResults = await this.db.batch(statements);
      }

      // 分析batch执行结果，计算实际的成功/失败统计
      const results = this.analyzeBatchResults(batchResults, statementTableMap, expectedResults);

      return {
        restored_tables: Object.keys(results),
        total_records: Object.values(results).reduce((sum, r) => sum + r.success, 0),
        results,
      };
    } catch (error) {
      console.error("还原备份失败:", error);
      throw new Error(`还原备份失败: ${error.message}`);
    }
  }

  /**
   * 分析batch执行结果，计算实际的成功/失败统计
   * @param {Array} batchResults - batch执行结果数组
   * @param {Array} statementTableMap - 语句与表名的映射数组
   * @param {Object} expectedResults - 预期结果统计
   * @returns {Object} 实际结果统计
   */
  analyzeBatchResults(batchResults, statementTableMap, expectedResults) {
    const results = {};

    // 初始化结果统计
    for (const tableName of Object.keys(expectedResults)) {
      results[tableName] = {
        success: 0, // 实际插入的记录数
        ignored: 0, // 被忽略的重复记录数（仅合并模式）
        failed: 0, // 插入失败的记录数
        expected: expectedResults[tableName].expected,
      };
    }

    // 分析每个语句的执行结果
    for (let i = 0; i < batchResults.length; i++) {
      const result = batchResults[i];
      const tableName = statementTableMap[i];

      // 跳过系统语句（PRAGMA等）
      if (tableName.startsWith("_")) {
        continue;
      }

      // 检查语句执行是否成功
      if (result && result.success !== false) {
        // 对于INSERT语句，检查changes字段
        const changes = result.meta?.changes || result.changes || 0;
        if (changes > 0) {
          // 实际插入了记录
          results[tableName].success += changes;
        } else {
          // INSERT OR IGNORE被忽略（记录已存在，主键冲突）
          results[tableName].ignored += 1;
        }
      } else {
        // 语句执行失败
        results[tableName].failed += 1;
      }
    }

    return results;
  }

  /**
   * 验证数据完整性
   * @param {Object} data - 备份数据
   * @returns {Array} 完整性问题列表
   */
  async validateDataIntegrity(data) {
    const issues = [];

    // 检查 storage_mounts 的依赖
    if (data.storage_mounts) {
      for (const mount of data.storage_mounts) {
        if (mount.storage_config_id && mount.storage_type === "S3") {
          // 检查对应的 s3_configs 是否存在于备份数据中
          const hasS3Config = data.s3_configs?.some((config) => config.id === mount.storage_config_id);
          if (!hasS3Config) {
            // 检查数据库中是否存在
            try {
              const existingConfig = await this.db.prepare(`SELECT id FROM s3_configs WHERE id = ?`).bind(mount.storage_config_id).first();

              if (!existingConfig) {
                issues.push({
                  type: "missing_dependency",
                  table: "storage_mounts",
                  record_id: mount.id,
                  record_name: mount.name,
                  dependency_table: "s3_configs",
                  dependency_id: mount.storage_config_id,
                  message: `挂载点 "${mount.name}" 依赖的S3配置 "${mount.storage_config_id}" 不存在`,
                });
              }
            } catch (error) {
              console.warn(`[BackupService] 检查S3配置依赖时出错: ${error.message}`);
            }
          }
        }
      }
    }

    // 检查 file_passwords 的依赖
    if (data.file_passwords) {
      for (const filePassword of data.file_passwords) {
        const hasFile = data.files?.some((file) => file.id === filePassword.file_id);
        if (!hasFile) {
          try {
            const existingFile = await this.db.prepare(`SELECT id FROM files WHERE id = ?`).bind(filePassword.file_id).first();

            if (!existingFile) {
              issues.push({
                type: "missing_dependency",
                table: "file_passwords",
                record_id: filePassword.file_id,
                dependency_table: "files",
                dependency_id: filePassword.file_id,
                message: `文件密码记录依赖的文件 "${filePassword.file_id}" 不存在`,
              });
            }
          } catch (error) {
            console.warn(`[BackupService] 检查文件依赖时出错: ${error.message}`);
          }
        }
      }
    }

    // 检查 paste_passwords 的依赖
    if (data.paste_passwords) {
      for (const pastePassword of data.paste_passwords) {
        const hasPaste = data.pastes?.some((paste) => paste.id === pastePassword.paste_id);
        if (!hasPaste) {
          try {
            const existingPaste = await this.db.prepare(`SELECT id FROM pastes WHERE id = ?`).bind(pastePassword.paste_id).first();

            if (!existingPaste) {
              issues.push({
                type: "missing_dependency",
                table: "paste_passwords",
                record_id: pastePassword.paste_id,
                dependency_table: "pastes",
                dependency_id: pastePassword.paste_id,
                message: `文本密码记录依赖的文本 "${pastePassword.paste_id}" 不存在`,
              });
            }
          } catch (error) {
            console.warn(`[BackupService] 检查文本依赖时出错: ${error.message}`);
          }
        }
      }
    }

    return issues;
  }

  /**
   * 处理时间戳字段
   * @param {Object} record - 记录对象
   * @param {string} tableName - 表名
   * @param {Object} options - 处理选项
   * @returns {Object} 处理后的记录
   */
  processTimestampFields(record, tableName, options = {}) {
    const { preserveTimestamps = false, updateTimestamps = true } = options;
    const processedRecord = { ...record };

    if (!preserveTimestamps) {
      const now = new Date().toISOString();

      // 在合并模式下更新 updated_at 字段
      if (updateTimestamps && processedRecord.updated_at) {
        processedRecord.updated_at = now;
      }

      // 可选：为新插入的记录更新 created_at（通常不建议）
      // if (processedRecord.created_at && options.updateCreatedAt) {
      //   processedRecord.created_at = now;
      // }
    }

    return processedRecord;
  }

  /**
   * 在合并模式下映射 admin_id 到当前管理员
   * @param {Object} data - 备份数据
   * @param {string} currentAdminId - 当前管理员ID
   * @returns {Object} 映射后的数据
   */
  mapAdminIds(data, currentAdminId) {
    const mappedData = { ...data };

    console.log(`[BackupService] 映射 admin_id 到当前管理员 ${currentAdminId}`);

    // 处理 s3_configs 表
    if (mappedData.s3_configs) {
      const originalCount = mappedData.s3_configs.length;
      mappedData.s3_configs = mappedData.s3_configs.map((record) => ({
        ...record,
        admin_id: currentAdminId,
      }));
      console.log(`[BackupService] 映射 s3_configs 表：${originalCount} 条记录的 admin_id 已更新`);
    }

    // 处理 storage_mounts 表
    if (mappedData.storage_mounts) {
      const originalCount = mappedData.storage_mounts.length;
      mappedData.storage_mounts = mappedData.storage_mounts.map((record) => ({
        ...record,
        created_by: currentAdminId,
      }));
      console.log(`[BackupService] 映射 storage_mounts 表：${originalCount} 条记录的 created_by 已更新`);
    }

    // 处理 files 表
    if (mappedData.files) {
      const originalCount = mappedData.files.length;
      mappedData.files = mappedData.files.map((record) => ({
        ...record,
        created_by: currentAdminId,
      }));
      console.log(`[BackupService] 映射 files 表：${originalCount} 条记录的 created_by 已更新`);
    }

    // 处理 pastes 表
    if (mappedData.pastes) {
      const originalCount = mappedData.pastes.length;
      mappedData.pastes = mappedData.pastes.map((record) => ({
        ...record,
        created_by: currentAdminId,
      }));
      console.log(`[BackupService] 映射 pastes 表：${originalCount} 条记录的 created_by 已更新`);
    }

    // 注意：不处理 api_keys 表，因为API密钥是独立的用户身份
    // 注意：不处理 admin_tokens 表，因为令牌应该跟随对应的管理员

    return mappedData;
  }

  /**
   * 获取模块信息（包含记录数统计）
   * @returns {Object} 模块信息
   */
  async getModulesInfo() {
    const modules = {};

    for (const [moduleKey, tables] of Object.entries(this.moduleTableMapping)) {
      let totalRecords = 0;

      for (const tableName of tables) {
        try {
          const result = await this.db.prepare(`SELECT COUNT(*) as count FROM ${tableName}`).first();
          totalRecords += result.count || 0;
        } catch (error) {
          console.error(`获取表 ${tableName} 记录数失败:`, error);
          // 表不存在或查询失败时，记录数为0
          totalRecords += 0;
        }
      }

      modules[moduleKey] = {
        name: this.getModuleDisplayName(moduleKey),
        tables: tables,
        record_count: totalRecords,
        description: this.getModuleDescription(moduleKey),
      };
    }

    return modules;
  }

  /**
   * 根据模块获取对应的表
   * @param {Array} modules - 模块列表
   * @returns {Array} 表列表
   */
  getTablesFromModules(modules) {
    const tables = new Set();
    modules.forEach((module) => {
      if (this.moduleTableMapping[module]) {
        this.moduleTableMapping[module].forEach((table) => {
          tables.add(table);
        });
      }
    });
    return Array.from(tables);
  }

  /**
   * 导出指定表的数据
   * @param {Array} tableNames - 表名列表
   * @returns {Object} 导出的数据
   */
  async exportTables(tableNames) {
    const result = {};

    // 按依赖关系排序表
    const orderedTables = this.sortTablesByDependency(tableNames);

    for (const tableName of orderedTables) {
      try {
        const data = await this.db.prepare(`SELECT * FROM ${tableName}`).all();
        result[tableName] = data.results || [];
        console.log(`导出表 ${tableName}: ${result[tableName].length} 条记录`);
      } catch (error) {
        console.error(`导出表 ${tableName} 失败:`, error);
        result[tableName] = [];
      }
    }

    return result;
  }

  /**
   * 按依赖关系排序表
   * @param {Array} tables - 表列表
   * @returns {Array} 排序后的表列表
   */
  sortTablesByDependency(tables) {
    const sorted = [];
    const remaining = [...tables];

    while (remaining.length > 0) {
      let found = false;

      for (let i = 0; i < remaining.length; i++) {
        const table = remaining[i];
        const deps = this.tableDependencies[table] || [];

        // 检查依赖是否都已处理或不在待处理列表中
        if (deps.every((dep) => sorted.includes(dep) || !tables.includes(dep))) {
          sorted.push(table);
          remaining.splice(i, 1);
          found = true;
          break;
        }
      }

      // 如果没有找到可处理的表，说明有循环依赖，直接添加剩余的表
      if (!found) {
        sorted.push(...remaining);
        break;
      }
    }

    return sorted;
  }

  /**
   * 验证备份数据
   * @param {Object} backupData - 备份数据
   */
  validateBackupData(backupData) {
    if (!backupData || typeof backupData !== "object") {
      throw new Error("无效的备份数据格式");
    }

    if (!backupData.metadata || !backupData.data) {
      throw new Error("备份数据缺少必要的字段");
    }

    if (!backupData.metadata.version || !backupData.metadata.timestamp) {
      throw new Error("备份元数据不完整");
    }

    // 验证校验和
    const calculatedChecksum = this.generateChecksum(backupData.data);
    if (backupData.metadata.checksum !== calculatedChecksum) {
      throw new Error("备份数据校验失败，文件可能已损坏");
    }
  }

  /**
   * 验证表是否存在
   * @param {Array} tables - 表名列表
   */
  async validateTablesExist(tables) {
    const validTables = Object.values(DbTables);

    for (const table of tables) {
      if (!validTables.includes(table)) {
        throw new Error(`不支持的数据表: ${table}`);
      }
    }
  }

  /**
   * 获取模块显示名称
   * @param {string} moduleKey - 模块键
   * @returns {string} 显示名称
   */
  getModuleDisplayName(moduleKey) {
    const names = {
      text_management: "文本管理",
      file_management: "文件管理",
      mount_management: "挂载管理",
      storage_config: "S3配置管理",
      key_management: "密钥管理",
      account_management: "账号管理",
      system_settings: "系统设置",
    };
    return names[moduleKey] || moduleKey;
  }

  /**
   * 获取模块描述
   * @param {string} moduleKey - 模块键
   * @returns {string} 模块描述
   */
  getModuleDescription(moduleKey) {
    const descriptions = {
      text_management: "文本分享数据和密码",
      file_management: "文件分享数据和密码",
      mount_management: "存储挂载点配置",
      storage_config: "S3存储配置信息",
      key_management: "API密钥管理",
      account_management: "管理员账号和令牌",
      system_settings: "系统全局设置",
    };
    return descriptions[moduleKey] || "";
  }

  /**
   * 深度排序对象键（递归处理嵌套对象和数组）
   * @param {any} obj - 要排序的对象
   * @returns {any} 排序后的对象
   */
  deepSortKeys(obj) {
    if (obj === null || typeof obj !== "object") {
      return obj;
    }

    if (Array.isArray(obj)) {
      return obj.map((item) => this.deepSortKeys(item));
    }

    const sortedObj = {};
    const sortedKeys = Object.keys(obj).sort();

    for (const key of sortedKeys) {
      sortedObj[key] = this.deepSortKeys(obj[key]);
    }

    return sortedObj;
  }

  /**
   * 生成数据校验和（使用稳定的序列化算法）
   * @param {Object} data - 数据对象
   * @returns {string} 校验和
   */
  generateChecksum(data) {
    // 递归排序所有对象键，确保相同数据产生相同校验和
    const sortedData = this.deepSortKeys(data);
    const dataString = JSON.stringify(sortedData);
    return crypto.createHash("sha256").update(dataString).digest("hex").substring(0, 16);
  }
}
