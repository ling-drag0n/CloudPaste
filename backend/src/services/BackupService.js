import crypto from "crypto";
import { DbTables } from "../constants/index.js";
import { ValidationError, RepositoryError } from "../http/errors.js";
import { StorageConfigUtils } from "../storage/utils/StorageConfigUtils.js";
import { createDbRuntime } from "../db/runtime.js";

/**
 * 数据备份与还原服务
 * 提供数据库的备份和还原功能
 */
export class BackupService {
  constructor(db, env = {}) {
    this.db = db;
    this.dialect = createDbRuntime({ db, env }).dialect;

    // 模块与数据表的映射关系
    this.moduleTableMapping = {
      text_management: ["pastes", "paste_passwords", "paste_tags", "paste_tag_assignments"],
      file_management: ["files", "file_passwords"],
      vfs_management: ["vfs_nodes"],
      mount_management: ["storage_mounts"],
      storage_config: ["storage_configs", "principal_storage_acl"],
      key_management: ["api_keys"],
      account_management: ["admins", "admin_tokens"],
      system_settings: ["system_settings"],
      fs_meta_management: ["fs_meta"],
      task_management: ["tasks", "scheduled_jobs", "scheduled_job_runs"],
      upload_sessions: ["upload_sessions"],
    };

    // 表的依赖关系（用于确定导入顺序）
    // 基于实际的外键约束关系和应用层依赖关系
    this.tableDependencies = {
      paste_passwords: ["pastes"],
      paste_tag_assignments: ["pastes", "paste_tags"],
      file_passwords: ["files"],
      admin_tokens: ["admins"],
      storage_configs: ["admins"], // storage_configs.admin_id -> admins.id
      storage_mounts: ["storage_configs"], // storage_mounts.storage_config_id -> storage_configs.id
      vfs_nodes: ["storage_configs"],
      tasks: ["api_keys"], // tasks.user_id -> api_keys.id (当 user_type='apikey' 时)
      principal_storage_acl: ["api_keys", "storage_configs"],
      scheduled_job_runs: ["scheduled_jobs"],
      upload_sessions: ["storage_configs", "storage_mounts"],
    };
  }

  async getExistingTableSet() {
    try {
      const existingTables = await this.db.prepare(`SELECT name FROM sqlite_master WHERE type='table'`).all();
      const rows = existingTables?.results || [];
      return new Set(rows.map((t) => t?.name).filter(Boolean));
    } catch (error) {
      console.warn(`[BackupService] 读取 sqlite_master 失败，将跳过“表存在性”检查: ${error.message}`);
      return null;
    }
  }

  async getTableColumnSet(tableName) {
    try {
      const info = await this.db.prepare(`PRAGMA table_info(${tableName})`).all();
      const rows = info?.results || [];
      return new Set(rows.map((r) => r?.name).filter(Boolean));
    } catch (error) {
      console.warn(`[BackupService] 读取表字段失败（可忽略）: table=${tableName}, error=${error.message}`);
      return null;
    }
  }

  collectBackupColumnsSample(records, sampleLimit = 50) {
    const list = Array.isArray(records) ? records : [];
    const cols = new Set();
    const n = Math.min(sampleLimit, list.length);
    for (let i = 0; i < n; i++) {
      const row = list[i];
      if (!row || typeof row !== "object") continue;
      for (const k of Object.keys(row)) {
        cols.add(k);
      }
    }
    return cols;
  }

  estimateInsertStatementCount(tableName, records, options = {}) {
    const { mode = "overwrite" } = options;
    const list = Array.isArray(records) ? records : [];
    if (list.length === 0) return 0;

    const dialectName = this.dialect?.name || "unknown";
    const canBulkInsert = dialectName === "sqlite";

    // 估算列数：用前 N 条记录做采样（预检查只需要估算，不需要精确到每一条）
    const sampleCols = this.collectBackupColumnsSample(list, 50);
    const columnsCount = Math.max(1, sampleCols.size);

    if (!canBulkInsert) {
      // 非 sqlite：按“每行一条语句”保守估算
      return list.length;
    }

    // D1/SQLite 的变量上限在不同环境里可能不同（D1 报错：too many SQL variables）
    // 为了稳：统一用一个更保守的上限，宁可多跑几条语句，也不要导入时直接 500。
    const MAX_BIND_VARS = 80;
    const maxRowsPerStatement = Math.max(1, Math.floor(MAX_BIND_VARS / columnsCount));

    // merge/overwrite 都可以走多行插入（merge 用 INSERT OR IGNORE）
    // 这里仅估算语句数量
    const statements = Math.ceil(list.length / maxRowsPerStatement);
    return Math.max(1, statements);
  }

  /**
   * FS 搜索索引表属于派生数据：不参与备份/恢复事实来源，默认通过重建恢复
   * @returns {Set<string>}
   */
  getDerivedIndexTables() {
    return new Set([
      DbTables.FS_SEARCH_INDEX_ENTRIES,
      DbTables.FS_SEARCH_INDEX_STATE,
      DbTables.FS_SEARCH_INDEX_DIRTY,
      DbTables.FS_SEARCH_INDEX_FTS,
      DbTables.UPLOAD_PARTS,
    ].filter(Boolean));
  }

  /**
   * 还原后强制将 FS 搜索索引标记为“需重建”
   * - D1 的 FTS5 虚表不适合 export/import；索引属于派生数据，必须可重建
   */
  async markFsSearchIndexNotReadyAfterRestore() {
    const derived = this.getDerivedIndexTables();
    if (!derived.size) return;

    try {
      const statements = [];
      for (const tableName of derived) {
        try {
          statements.push(this.db.prepare(`DELETE FROM ${tableName}`));
        } catch (error) {
          // 旧 schema 或未创建索引表时：prepare 阶段就可能失败
          console.warn(`[BackupService] 清理 FS 搜索索引表失败（可忽略）: ${error.message}`);
        }
      }

      if (statements.length === 0) return;
      await this.db.batch(statements);
    } catch (error) {
      // 旧 schema 或未创建索引表时可能失败；保持“尽力而为”
      console.warn(`[BackupService] 清理 FS 搜索索引表失败（可忽略）: ${error.message}`);
    }
  }

  async getSchemaVersionForBackup() {
    try {
      const resp = await this.db.prepare(`SELECT id FROM schema_migrations`).all();
      const rows = resp?.results || [];
      const ids = rows.map((r) => r?.id).filter(Boolean);

      let maxVersion = null;
      for (const id of ids) {
        const match = String(id).match(/^app-v(\d{2})$/);
        if (!match) continue;
        const parsed = Number.parseInt(match[1], 10);
        if (!Number.isFinite(parsed)) continue;
        maxVersion = maxVersion === null ? parsed : Math.max(maxVersion, parsed);
      }

      return maxVersion === null ? null : String(maxVersion);
    } catch (error) {
      console.warn(`[BackupService] 读取 schema_migrations 失败: ${error.message}`);
      return null;
    }
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
    let dependencies = []; // 在函数作用域定义（包含静态依赖 + 动态依赖）

    if (backup_type === "full") {
      // 完整备份 - 所有表
      const derived = this.getDerivedIndexTables();
      tables = Object.values(DbTables).filter((t) => !derived.has(t));
    } else if (backup_type === "modules") {
      // 检查并自动包含依赖模块
      dependencies = this.getModuleDependencies(selected_modules);
      const dynamicDependencies = [];

      // 如果只备份 vfs_nodes 不备份 storage_mounts，恢复后 scope_id 会“对不上”，目录树会变成孤儿数据
      // 当选择了 vfs_management 且数据库里存在 scope_type=mount 的 vfs_nodes 时，自动包含 mount_management
      if (selected_modules.includes("vfs_management") && !selected_modules.includes("mount_management")) {
        try {
          const result = await this.db.prepare(`SELECT COUNT(*) as count FROM vfs_nodes WHERE scope_type = 'mount'`).first();
          const count = Number(result?.count || 0);
          if (count > 0) {
            dynamicDependencies.push("mount_management");
            console.log(`[BackupService] 检测到 vfs_nodes.scope_type=mount 的历史数据（${count} 条），自动包含模块: mount_management`);
          }
        } catch (error) {
          // 旧 schema 或表不存在时：忽略即可
          console.warn(`[BackupService] 检测 vfs_nodes.scope_type=mount 失败（可忽略）: ${error.message}`);
        }
      }

      dependencies = [...new Set([...dependencies, ...dynamicDependencies])];
      if (dependencies.length > 0) {
        finalModules = [...new Set([...selected_modules, ...dependencies])];
        console.log(`[BackupService] 检测到跨模块依赖，自动包含模块: ${dependencies.join(", ")}`);
      }

      // 模块备份 - 根据选中的模块确定表
      tables = this.getTablesFromModules(finalModules);
    } else {
      throw new ValidationError("不支持的备份类型");
    }

    // 导出数据
    const data = await this.exportTables(tables);

    const schemaVersion = await this.getSchemaVersionForBackup();

    // 生成元数据
    const metadata = {
      version: "1.0",
      timestamp: new Date().toISOString(),
      backup_type,
      schema_version: schemaVersion,
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
        vfs_management: ["storage_config"], // 目录树索引依赖存储配置（scope_type=storage_config 时）
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

    if (mode !== "overwrite" && mode !== "merge") {
      throw new ValidationError(`不支持的还原模式: ${mode}`);
    }

    // 验证备份数据
    this.validateBackupData(backupData);

    let { data } = backupData;

    // 进行 admin_id 映射（仅合并模式需要挂接到现有管理员）
    if (currentAdminId && mode === "merge") {
      data = this.mapAdminIds(data, currentAdminId);
    }

    const tables = Object.keys(data);

    // 验证表是否存在
    await this.validateTablesExist(tables);

    // 关键前置校验：确保目标库“真的有这些表/字段”
    // - 目的：避免 overwrite 先清表，后续才发现缺表/缺字段导致半成品
    const preview = await this.previewRestoreBackup(backupData, {
      mode,
      currentAdminId,
      // 这里仅做 schema 级别检查，不做跨表依赖完整性检查（由下方的 validateDataIntegrity 负责）
      skipIntegrityCheck: true,
      preserveTimestamps,
    });
    const hardErrors = Array.isArray(preview?.issues) ? preview.issues.filter((i) => i?.level === "error") : [];
    if (hardErrors.length > 0) {
      const first = hardErrors[0];
      throw new ValidationError(`还原前置检查失败：${first?.message || "目标库结构不匹配"}`);
    }

    // 数据完整性检查
    // - 注意：这是“提醒级别”的检查（默认不会阻断 restore），但会把问题返回给前端用于展示“操作日志”
    let integrityIssues = [];
    if (!skipIntegrityCheck) {
      integrityIssues = await this.validateDataIntegrity(data);
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
    const statementInsertRowCounts = []; // 记录每个插入语句包含的行数（用于统计 ignored/failed）

    try {
      // 收集所有需要执行的语句
      const statements = [];

      // 使用D1的延迟外键约束功能
      statements.push(this.db.prepare("PRAGMA defer_foreign_keys = ON"));
      statementTableMap.push("_pragma"); // 标记为系统语句
      statementInsertRowCounts.push(0);

      // 在覆盖模式下，按正确顺序删除数据
      if (mode === "overwrite") {
        // 按依赖关系的逆序删除（子表先删除）
        const reversedTables = [...orderedTables].reverse();
        for (const tableName of reversedTables) {
          statements.push(this.db.prepare(`DELETE FROM ${tableName}`));
          statementTableMap.push(`_delete_${tableName}`); // 标记为删除语句
          statementInsertRowCounts.push(0);
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
          const insertPlans = this.buildInsertStatementsForTable(tableName, tableData, {
            mode,
            preserveTimestamps,
          });

          for (const plan of insertPlans) {
            const statementIndex = statements.length;
            statements.push(plan.statement);
            statementTableMap.push(tableName);
            statementInsertRowCounts.push(plan.rowCount);
            expectedResults[tableName].statementIndices.push(statementIndex);
          }
        }
      }

      // 在batch结束时恢复外键约束检查（可选，因为事务结束时会自动恢复）
      statements.push(this.db.prepare("PRAGMA defer_foreign_keys = OFF"));
      statementTableMap.push("_pragma"); // 标记为系统语句
      statementInsertRowCounts.push(0);

      // 分批执行（避免单次 batch 过大导致失败）
      // 注意：多次 batch 可能不具备“全局原子性”，但可以显著提升大备份的可恢复性。
      const MAX_STATEMENTS_PER_BATCH = 80;
      let batchResults = [];
      if (statements.length > 0) {
        for (let i = 0; i < statements.length; i += MAX_STATEMENTS_PER_BATCH) {
          const chunk = statements.slice(i, i + MAX_STATEMENTS_PER_BATCH);
          // D1 batch 会返回与 chunk 同长度的结果数组
          // 将其拼接后，索引仍然与 statementTableMap/statementInsertRowCounts 对齐
          // eslint-disable-next-line no-await-in-loop
          const chunkResults = await this.db.batch(chunk);
          batchResults.push(...(chunkResults || []));
        }
      }

      // 分析batch执行结果，计算实际的成功/失败统计
      const results = this.analyzeBatchResults(batchResults, statementTableMap, statementInsertRowCounts, expectedResults, { mode });

      // 还原后：索引类派生数据一律视为无效，强制清空并要求重建
      await this.markFsSearchIndexNotReadyAfterRestore();

      return {
        restored_tables: Object.keys(results),
        total_records: Object.values(results).reduce((sum, r) => sum + r.success, 0),
        results,
        integrityIssues,
      };
    } catch (error) {
      console.error("还原备份失败:", error);
      throw new RepositoryError(`还原备份失败: ${error.message}`);
    }
  }

  /**
   * 仅做“预检查/预估”，不写入数据库
   * - 用于在真正 restore 前，告诉用户：会影响哪些表/大概会跑多少语句/有哪些明显不匹配
   * @param {Object} backupData
   * @param {{ mode?: "overwrite"|"merge", currentAdminId?: string, skipIntegrityCheck?: boolean, preserveTimestamps?: boolean }} options
   */
  async previewRestoreBackup(backupData, options = {}) {
    const { mode = "overwrite", currentAdminId, skipIntegrityCheck = false } = options;

    if (mode !== "overwrite" && mode !== "merge") {
      throw new ValidationError(`不支持的还原模式: ${mode}`);
    }

    this.validateBackupData(backupData);

    let { data } = backupData;
    if (currentAdminId && mode === "merge") {
      data = this.mapAdminIds(data, currentAdminId);
    }

    const tables = Object.keys(data);
    await this.validateTablesExist(tables);

    const existingTables = await this.getExistingTableSet();

    const issues = [];
    const tablePlans = {};

    // 完整性检查（只产出问题列表，不在预览阶段打印大量 warn）
    let integrityIssues = [];
    if (!skipIntegrityCheck) {
      integrityIssues = await this.validateDataIntegrity(data);
    }

    // 按依赖排序（便于用户理解插入顺序）
    const orderedTables = this.sortTablesByDependency(tables);
    const deleteOrder = mode === "overwrite" ? [...orderedTables].reverse() : [];

    let totalRecords = 0;
    let estimatedInsertStatements = 0;

    for (const tableName of orderedTables) {
      const records = Array.isArray(data[tableName]) ? data[tableName] : [];
      const recordCount = records.length;
      totalRecords += recordCount;

      const insertStatements = this.estimateInsertStatementCount(tableName, records, { mode });
      estimatedInsertStatements += insertStatements;

      const sampleCols = this.collectBackupColumnsSample(records, 50);
      const dbCols = await this.getTableColumnSet(tableName);

      const missingTable = existingTables ? !existingTables.has(tableName) : null;
      if (missingTable === true) {
        issues.push({
          level: "error",
          table: tableName,
          code: "TABLE_NOT_FOUND",
          message: `数据库中缺少表：${tableName}（请先初始化/迁移数据库结构）`,
        });
      }

      if (dbCols && sampleCols.size > 0) {
        const extraCols = [];
        for (const c of sampleCols) {
          if (!dbCols.has(c)) extraCols.push(c);
        }
        if (extraCols.length > 0) {
          issues.push({
            level: "error",
            table: tableName,
            code: "COLUMN_MISMATCH",
            message: `备份数据包含数据库不存在的字段：${extraCols.join(", ")}`,
          });
        }
      }

      tablePlans[tableName] = {
        records: recordCount,
        estimatedInsertStatements: insertStatements,
        sampleColumns: Array.from(sampleCols).sort(),
      };
    }

    const PRAGMA_STATEMENTS = 2; // defer_foreign_keys ON/OFF
    const estimatedDeleteStatements = mode === "overwrite" ? deleteOrder.length : 0;
    const MAX_STATEMENTS_PER_BATCH = 80;
    const estimatedTotalStatements = PRAGMA_STATEMENTS + estimatedDeleteStatements + estimatedInsertStatements;
    const estimatedBatches = Math.ceil(estimatedTotalStatements / MAX_STATEMENTS_PER_BATCH);

    const notes = [];
    notes.push("这是“预检查”结果：不会写入数据库。");
    notes.push("若导入到不同的 ENCRYPTION_SECRET 环境，部分加密配置可能无法解密（会表现为配置存在但不可用）。");
    if (mode === "overwrite") {
      notes.push("overwrite 会清空目标库中对应表的数据，再重新导入；建议先做一次全量备份留底。");
    } else {
      notes.push("merge 会尽量插入不存在的记录（主键冲突会忽略）；不保证把旧数据“更新成新值”。");
    }

    return {
      mode,
      backup: {
        backup_type: backupData?.metadata?.backup_type || null,
        timestamp: backupData?.metadata?.timestamp || null,
        schema_version: backupData?.metadata?.schema_version || null,
        total_records: totalRecords,
      },
      plan: {
        tables,
        orderedTables,
        deleteOrder,
        tablePlans,
        estimated: {
          totalRecords,
          insertStatements: estimatedInsertStatements,
          deleteStatements: estimatedDeleteStatements,
          totalStatements: estimatedTotalStatements,
          batches: estimatedBatches,
          maxStatementsPerBatch: MAX_STATEMENTS_PER_BATCH,
        },
      },
      issues,
      integrityIssues,
      notes,
    };
  }

  /**
   * 分析batch执行结果，计算实际的成功/失败统计
   * @param {Array} batchResults - batch执行结果数组
   * @param {Array} statementTableMap - 语句与表名的映射数组
   * @param {Object} expectedResults - 预期结果统计
   * @returns {Object} 实际结果统计
   */
  analyzeBatchResults(batchResults, statementTableMap, statementInsertRowCounts, expectedResults, options = {}) {
    const { mode = "overwrite" } = options;
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
      const expectedRows = Array.isArray(statementInsertRowCounts) ? (statementInsertRowCounts[i] || 0) : 0;

      // 跳过系统语句（PRAGMA等）
      if (tableName.startsWith("_")) {
        continue;
      }

      // 检查语句执行是否成功
      if (result && result.success !== false) {
        // 对于INSERT语句，检查changes字段
        const changes = result.meta?.changes || result.changes || 0;
        results[tableName].success += changes;

        if (expectedRows > changes) {
          if (mode === "merge") {
            results[tableName].ignored += expectedRows - changes;
          } else {
            // overwrite 下：changes < expectedRows 通常意味着语句未按预期写入（可能是字段/约束问题）
            results[tableName].failed += expectedRows - changes;
          }
        }
      } else {
        // 语句执行失败
        results[tableName].failed += expectedRows > 0 ? expectedRows : 1;
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
        if (mount.storage_config_id) {
          // 通用检查：备份数据中是否包含此存储配置记录
          const hasConfig = data.storage_configs?.some((config) => config.id === mount.storage_config_id);
          if (!hasConfig) {
            try {
              const exists = await StorageConfigUtils.configExists(this.db, mount.storage_type || "S3", mount.storage_config_id);
              if (!exists) {
                issues.push({
                  type: "missing_dependency",
                  table: "storage_mounts",
                  record_id: mount.id,
                  record_name: mount.name,
                  dependency_table: "storage_configs",
                  dependency_id: mount.storage_config_id,
                  message: `挂载点 "${mount.name}" 依赖的存储配置 "${mount.storage_config_id}" 不存在`,
                });
              }
            } catch (error) {
              console.warn(`[BackupService] 检查存储配置依赖时出错: ${error.message}`);
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
      const nowIso = new Date().toISOString();
      const nowMs = Date.now();

      // 在合并模式下更新 updated_at 字段
      if (updateTimestamps && processedRecord.updated_at !== undefined && processedRecord.updated_at !== null) {
        // tasks.updated_at 是 INTEGER（毫秒），不能写成 ISO 字符串
        if (tableName === DbTables.TASKS || tableName === "tasks") {
          processedRecord.updated_at = nowMs;
        } else {
          processedRecord.updated_at = nowIso;
        }
      }

      // 可选：为新插入的记录更新 created_at（通常不建议）
      // if (processedRecord.created_at && options.updateCreatedAt) {
      //   processedRecord.created_at = nowIso;
      // }
    }

    return processedRecord;
  }

  /**
   * 为单张表构建插入语句列表（尽量合并为“多行插入”，减少语句数量）
   * - overwrite：INSERT INTO ... VALUES (...),(...)\n
   * - merge：SQLite/D1 使用 INSERT OR IGNORE
   * @param {string} tableName
   * @param {Array<Object>} records
   * @param {{ mode: "overwrite"|"merge", preserveTimestamps?: boolean }} options
   * @returns {Array<{ statement: any, rowCount: number }>}
   */
  buildInsertStatementsForTable(tableName, records, options = {}) {
    const { mode = "overwrite", preserveTimestamps = false } = options;

    const list = Array.isArray(records) ? records : [];
    if (list.length === 0) return [];

    // 仅对 sqlite/d1 做“多行插入”优化；其它方言按原始单行语句走
    const dialectName = this.dialect?.name || "unknown";
    const canBulkInsert = dialectName === "sqlite";

    // 统一收集列（取 union），并排序保证稳定性
    const colSet = new Set();
    for (const r of list) {
      if (!r || typeof r !== "object") continue;
      for (const k of Object.keys(r)) {
        colSet.add(k);
      }
    }
    const columns = Array.from(colSet).sort();
    if (columns.length === 0) return [];

    // 处理记录（时间戳字段）
    const processed = list.map((r) =>
      this.processTimestampFields(r, tableName, { preserveTimestamps, updateTimestamps: mode === "merge" }),
    );

    if (!canBulkInsert) {
      // 回退：每行一个语句
      const out = [];
      for (const row of processed) {
        const fields = Object.keys(row);
        const values = Object.values(row);
        const sql =
          mode === "overwrite"
            ? `INSERT INTO ${tableName} (${fields.join(", ")}) VALUES (${fields.map(() => "?").join(", ")})`
            : this.dialect.buildInsertIgnoreSql({ table: tableName, columns: fields });
        out.push({ statement: this.db.prepare(sql).bind(...values), rowCount: 1 });
      }
      return out;
    }

    // D1/SQLite 的变量上限在不同环境里可能不同（D1 报错：too many SQL variables）
    // 为了稳：统一用一个更保守的上限，宁可多跑几条语句，也不要导入时直接失败。
    const MAX_BIND_VARS = 80;
    const maxRowsPerStatement = Math.max(1, Math.floor(MAX_BIND_VARS / columns.length));

    const statements = [];
    for (let i = 0; i < processed.length; i += maxRowsPerStatement) {
      const chunk = processed.slice(i, i + maxRowsPerStatement);
      const rowCount = chunk.length;

      const rowPlaceholders = `(${columns.map(() => "?").join(", ")})`;
      const valuesClause = Array.from({ length: rowCount }).map(() => rowPlaceholders).join(", ");

      const baseSql =
        mode === "merge"
          ? `INSERT OR IGNORE INTO ${tableName} (${columns.join(", ")}) VALUES ${valuesClause}`
          : `INSERT INTO ${tableName} (${columns.join(", ")}) VALUES ${valuesClause}`;

      const binds = [];
      for (const row of chunk) {
        for (const col of columns) {
          binds.push(Object.prototype.hasOwnProperty.call(row, col) ? row[col] : null);
        }
      }

      statements.push({
        statement: this.db.prepare(baseSql).bind(...binds),
        rowCount,
      });
    }

    return statements;
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

    // 处理 storage_configs 表
    if (mappedData.storage_configs) {
      const originalCount = mappedData.storage_configs.length;
      mappedData.storage_configs = mappedData.storage_configs.map((record) => ({
        ...record,
        admin_id: currentAdminId,
      }));
      console.log(`[BackupService] 映射 storage_configs 表：${originalCount} 条记录的 admin_id 已更新`);
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

    if (mappedData.paste_tags) {
      const originalCount = mappedData.paste_tags.length;
      mappedData.paste_tags = mappedData.paste_tags.map((record) => ({
        ...record,
        created_by: currentAdminId,
      }));
      console.log(`[BackupService] 映射 paste_tags 表：${originalCount} 条记录的 created_by 已更新`);
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
      throw new ValidationError("无效的备份数据格式");
    }

    if (!backupData.metadata || !backupData.data) {
      throw new ValidationError("备份数据缺少必要的字段");
    }

    if (!backupData.metadata.version || !backupData.metadata.timestamp) {
      throw new ValidationError("备份元数据不完整");
    }

    // 验证校验和
    const calculatedChecksum = this.generateChecksum(backupData.data);
    if (backupData.metadata.checksum !== calculatedChecksum) {
      throw new ValidationError("备份数据校验失败，文件可能已损坏");
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
        throw new ValidationError(`不支持的数据表: ${table}`);
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
      storage_config: "存储配置",
      fs_meta_management: "元信息管理",
      key_management: "密钥管理",
      account_management: "账号管理",
      system_settings: "系统设置",
      task_management: "任务与定时任务",
      upload_sessions: "上传会话",
      vfs_management: "目录树索引（VFS）",
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
      storage_config: "存储配置和访问控制",
      key_management: "API密钥管理",
      account_management: "管理员账号和令牌",
      system_settings: "系统全局设置",
      fs_meta_management: "目录元信息配置",
      task_management: "通用任务队列 + 后台定时任务（jobs/runs）",
      upload_sessions: "上传/分片/断点续传会话（可选迁移数据）",
      vfs_management: "虚拟目录树索引。建议与“存储配置”“挂载管理”一起备份",
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
