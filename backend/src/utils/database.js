import { DbTables } from "../constants/index.js";
import crypto from "crypto";

// ==================== 表结构定义 ====================

/**
 * 创建文本分享相关表
 * @param {D1Database} db - D1数据库实例
 */
async function createPasteTables(db) {
  console.log("创建文本分享相关表...");

  // 创建pastes表 - 存储文本分享数据
  await db
    .prepare(
      `
      CREATE TABLE IF NOT EXISTS ${DbTables.PASTES} (
        id TEXT PRIMARY KEY,
        slug TEXT UNIQUE NOT NULL,
        content TEXT NOT NULL,
        remark TEXT,
        password TEXT,
        expires_at DATETIME,
        max_views INTEGER,
        views INTEGER DEFAULT 0,
        created_by TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
      )
    `
    )
    .run();

  // 创建文本密码表
  await db
    .prepare(
      `
      CREATE TABLE IF NOT EXISTS ${DbTables.PASTE_PASSWORDS} (
        paste_id TEXT PRIMARY KEY,
        plain_password TEXT NOT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (paste_id) REFERENCES ${DbTables.PASTES}(id) ON DELETE CASCADE
      )
    `
    )
    .run();
}

/**
 * 创建管理员相关表
 * @param {D1Database} db - D1数据库实例
 */
async function createAdminTables(db) {
  console.log("创建管理员相关表...");

  // 创建admins表 - 存储管理员信息
  await db
    .prepare(
      `
      CREATE TABLE IF NOT EXISTS ${DbTables.ADMINS} (
        id TEXT PRIMARY KEY,
        username TEXT UNIQUE NOT NULL,
        password TEXT NOT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
      )
    `
    )
    .run();

  // 创建admin_tokens表 - 存储管理员认证令牌
  await db
    .prepare(
      `
      CREATE TABLE IF NOT EXISTS ${DbTables.ADMIN_TOKENS} (
        token TEXT PRIMARY KEY,
        admin_id TEXT NOT NULL,
        expires_at DATETIME NOT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (admin_id) REFERENCES ${DbTables.ADMINS}(id) ON DELETE CASCADE
      )
    `
    )
    .run();

  // 创建api_keys表 - 存储API密钥（位标志权限系统）
  await db
    .prepare(
      `
      CREATE TABLE IF NOT EXISTS ${DbTables.API_KEYS} (
        id TEXT PRIMARY KEY,
        name TEXT UNIQUE NOT NULL,
        key TEXT UNIQUE NOT NULL,
        permissions INTEGER DEFAULT 0,
        role TEXT DEFAULT 'GENERAL',
        basic_path TEXT DEFAULT '/',
        is_guest BOOLEAN DEFAULT 0,
        last_used DATETIME,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        expires_at DATETIME NOT NULL
      )
    `
    )
    .run();
}

/**
 * 创建存储相关表
 * @param {D1Database} db - D1数据库实例
 */
async function createStorageTables(db) {
  console.log("创建存储相关表...");

  // 创建s3_configs表 - 存储S3配置信息
  await db
    .prepare(
      `
      CREATE TABLE IF NOT EXISTS ${DbTables.S3_CONFIGS} (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        provider_type TEXT NOT NULL,
        endpoint_url TEXT NOT NULL,
        bucket_name TEXT NOT NULL,
        region TEXT,
        access_key_id TEXT NOT NULL,
        secret_access_key TEXT NOT NULL,
        path_style BOOLEAN DEFAULT 0,
        default_folder TEXT DEFAULT '',
        is_public BOOLEAN DEFAULT 0,
        is_default BOOLEAN DEFAULT 0,
        total_storage_bytes BIGINT,
        custom_host TEXT,
        signature_expires_in INTEGER DEFAULT 3600,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        last_used DATETIME,
        admin_id TEXT,
        FOREIGN KEY (admin_id) REFERENCES ${DbTables.ADMINS}(id) ON DELETE CASCADE
      )
    `
    )
    .run();

  // 创建storage_mounts表 - 存储挂载配置
  await db
    .prepare(
      `
      CREATE TABLE IF NOT EXISTS ${DbTables.STORAGE_MOUNTS} (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        storage_type TEXT NOT NULL,
        storage_config_id TEXT,
        mount_path TEXT NOT NULL,
        remark TEXT,
        is_active BOOLEAN DEFAULT 1,
        created_by TEXT NOT NULL,
        sort_order INTEGER DEFAULT 0,
        cache_ttl INTEGER DEFAULT 300,
        web_proxy BOOLEAN DEFAULT 0,
        webdav_policy TEXT DEFAULT '302_redirect',
        enable_sign BOOLEAN DEFAULT 0,
        sign_expires INTEGER DEFAULT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        last_used DATETIME
      )
    `
    )
    .run();
}

/**
 * 创建文件相关表
 * @param {D1Database} db - D1数据库实例
 */
async function createFileTables(db) {
  console.log("创建文件相关表...");

  // 创建files表 - 存储已上传文件的元数据（支持多存储类型）
  await db
    .prepare(
      `
      CREATE TABLE IF NOT EXISTS ${DbTables.FILES} (
        id TEXT PRIMARY KEY,
        slug TEXT UNIQUE NOT NULL,
        filename TEXT NOT NULL,

        -- 存储引用（支持多存储类型）
        storage_config_id TEXT NOT NULL,
        storage_type TEXT NOT NULL,
        storage_path TEXT NOT NULL,
        file_path TEXT,

        -- 文件元数据
        mimetype TEXT NOT NULL,
        size INTEGER NOT NULL,
        etag TEXT,

        -- 分享控制（保持现有功能）
        remark TEXT,
        password TEXT,
        expires_at DATETIME,
        max_views INTEGER,
        views INTEGER DEFAULT 0,
        use_proxy BOOLEAN DEFAULT 0,

        -- 元数据
        created_by TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
      )
    `
    )
    .run();

  // 创建file_passwords表 - 存储文件密码
  await db
    .prepare(
      `
      CREATE TABLE IF NOT EXISTS ${DbTables.FILE_PASSWORDS} (
        file_id TEXT PRIMARY KEY,
        plain_password TEXT NOT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (file_id) REFERENCES ${DbTables.FILES}(id) ON DELETE CASCADE
      )
    `
    )
    .run();
}

/**
 * 创建系统设置表
 * @param {D1Database} db - D1数据库实例
 */
async function createSystemTables(db) {
  console.log("创建系统设置表...");

  // 创建system_settings表 - 存储系统设置
  await db
    .prepare(
      `
      CREATE TABLE IF NOT EXISTS ${DbTables.SYSTEM_SETTINGS} (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL,
        description TEXT,
        type TEXT DEFAULT 'string',
        group_id INTEGER DEFAULT 1,
        options TEXT,
        sort_order INTEGER DEFAULT 0,
        flags INTEGER DEFAULT 0,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
      )
    `
    )
    .run();
}

// ==================== 索引创建 ====================

/**
 * 创建所有表的索引
 * @param {D1Database} db - D1数据库实例
 */
async function createIndexes(db) {
  console.log("创建数据库索引...");

  // pastes表索引
  await db.prepare(`CREATE INDEX IF NOT EXISTS idx_pastes_slug ON ${DbTables.PASTES}(slug)`).run();
  await db.prepare(`CREATE INDEX IF NOT EXISTS idx_pastes_created_at ON ${DbTables.PASTES}(created_at DESC)`).run();
  await db.prepare(`CREATE INDEX IF NOT EXISTS idx_pastes_created_by ON ${DbTables.PASTES}(created_by)`).run();

  // api_keys表索引
  await db.prepare(`CREATE INDEX IF NOT EXISTS idx_api_keys_key ON ${DbTables.API_KEYS}(key)`).run();
  await db.prepare(`CREATE INDEX IF NOT EXISTS idx_api_keys_role ON ${DbTables.API_KEYS}(role)`).run();
  await db.prepare(`CREATE INDEX IF NOT EXISTS idx_api_keys_permissions ON ${DbTables.API_KEYS}(permissions)`).run();
  await db.prepare(`CREATE INDEX IF NOT EXISTS idx_api_keys_expires_at ON ${DbTables.API_KEYS}(expires_at)`).run();

  // files表索引
  await db.prepare(`CREATE INDEX IF NOT EXISTS idx_files_slug ON ${DbTables.FILES}(slug)`).run();
  await db.prepare(`CREATE INDEX IF NOT EXISTS idx_files_storage_config_id ON ${DbTables.FILES}(storage_config_id)`).run();
  await db.prepare(`CREATE INDEX IF NOT EXISTS idx_files_storage_type ON ${DbTables.FILES}(storage_type)`).run();
  await db.prepare(`CREATE INDEX IF NOT EXISTS idx_files_file_path ON ${DbTables.FILES}(file_path)`).run();
  await db.prepare(`CREATE INDEX IF NOT EXISTS idx_files_created_at ON ${DbTables.FILES}(created_at)`).run();
  await db.prepare(`CREATE INDEX IF NOT EXISTS idx_files_expires_at ON ${DbTables.FILES}(expires_at)`).run();

  // storage_mounts表索引
  await db.prepare(`CREATE INDEX IF NOT EXISTS idx_storage_mounts_mount_path ON ${DbTables.STORAGE_MOUNTS}(mount_path)`).run();
  await db.prepare(`CREATE INDEX IF NOT EXISTS idx_storage_mounts_storage_config_id ON ${DbTables.STORAGE_MOUNTS}(storage_config_id)`).run();
  await db.prepare(`CREATE INDEX IF NOT EXISTS idx_storage_mounts_created_by ON ${DbTables.STORAGE_MOUNTS}(created_by)`).run();
  await db.prepare(`CREATE INDEX IF NOT EXISTS idx_storage_mounts_is_active ON ${DbTables.STORAGE_MOUNTS}(is_active)`).run();
  await db.prepare(`CREATE INDEX IF NOT EXISTS idx_storage_mounts_sort_order ON ${DbTables.STORAGE_MOUNTS}(sort_order)`).run();
}

// ==================== 默认数据初始化 ====================

/**
 * 初始化系统默认设置
 * @param {D1Database} db - D1数据库实例
 */
async function initDefaultSettings(db) {
  console.log("初始化系统默认设置...");

  const defaultSettings = [
    {
      key: "max_upload_size",
      value: "100",
      description: "单次最大上传文件大小限制(MB)",
      type: "number",
      group_id: 1,
      sort_order: 1,
      flags: 0,
    },
    {
      key: "webdav_upload_mode",
      value: "direct",
      description: "WebDAV客户端的上传模式选择。",
      type: "select",
      group_id: 3,
      options: JSON.stringify([
        { value: "direct", label: "直接上传" },
        { value: "multipart", label: "分片上传" },
      ]),
      sort_order: 1,
      flags: 0,
    },
    {
      key: "proxy_sign_all",
      value: "true",
      description: "是否对所有文件访问请求进行代理签名。",
      type: "bool",
      group_id: 1,
      sort_order: 2,
      flags: 0,
    },
    {
      key: "proxy_sign_expires",
      value: "0",
      description: "代理签名的过期时间（秒），0表示永不过期。",
      type: "number",
      group_id: 1,
      sort_order: 3,
      flags: 0,
    },
  ];

  for (const setting of defaultSettings) {
    const existing = await db.prepare(`SELECT value FROM ${DbTables.SYSTEM_SETTINGS} WHERE key = ?`).bind(setting.key).first();

    if (!existing) {
      await db
        .prepare(
          `INSERT INTO ${DbTables.SYSTEM_SETTINGS} (key, value, description, type, group_id, options, sort_order, flags)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
        )
        .bind(setting.key, setting.value, setting.description, setting.type, setting.group_id, setting.options || null, setting.sort_order, setting.flags)
        .run();
    }
  }
}

/**
 * 创建默认管理员账户
 * @param {D1Database} db - D1数据库实例
 */
async function createDefaultAdmin(db) {
  console.log("检查默认管理员账户...");

  const adminCount = await db.prepare(`SELECT COUNT(*) as count FROM ${DbTables.ADMINS}`).first();

  if (adminCount.count === 0) {
    const adminId = crypto.randomUUID();
    // 密码"admin123"的SHA-256哈希
    const defaultPassword = "240be518fabd2724ddb6f04eeb1da5967448d7e831c08c8fa822809f74c720a9";

    await db
      .prepare(
        `INSERT INTO ${DbTables.ADMINS} (id, username, password)
         VALUES (?, ?, ?)`
      )
      .bind(adminId, "admin", defaultPassword)
      .run();

    console.log("已创建默认管理员账户: admin/admin123");
  }
}

// ==================== 主初始化函数 ====================

/**
 * 初始化数据库表结构
 * @param {D1Database} db - D1数据库实例
 */
export async function initDatabase(db) {
  console.log("开始初始化数据库表结构...");

  // 创建所有表
  await createPasteTables(db);
  await createAdminTables(db);
  await createStorageTables(db);
  await createFileTables(db);
  await createSystemTables(db);

  // 创建索引
  await createIndexes(db);

  // 初始化完整的默认设置
  await initDefaultSettings(db); // 基础设置 (4个)
  await addPreviewSettings(db); // 预览设置 (6个)
  await addSiteSettings(db); // 站点设置 (5个)
  await addCustomContentSettings(db); // 自定义内容设置 (2个)
  await addFileNamingStrategySetting(db); // 文件命名策略设置 (1个)
  await addDefaultProxySetting(db); // 默认代理设置 (1个)

  await createDefaultAdmin(db);

  console.log("数据库初始化完成");
}

// ==================== 数据库迁移 ====================

/**
 * 添加表字段的通用函数
 * @param {D1Database} db - D1数据库实例
 * @param {string} tableName - 表名
 * @param {string} fieldName - 字段名
 * @param {string} fieldDefinition - 字段定义
 */
async function addTableField(db, tableName, fieldName, fieldDefinition) {
  try {
    const columnInfo = await db.prepare(`PRAGMA table_info(${tableName})`).all();
    const fieldExists = columnInfo.results.some((column) => column.name === fieldName);

    if (!fieldExists) {
      await db.prepare(`ALTER TABLE ${tableName} ADD COLUMN ${fieldDefinition}`).run();
      console.log(`成功添加${fieldName}字段到${tableName}表`);
    } else {
      console.log(`${tableName}表已存在${fieldName}字段，跳过添加`);
    }
  } catch (error) {
    console.error(`添加${fieldName}字段到${tableName}表时出错:`, error);
    console.log(`将继续执行迁移过程，但请手动检查${tableName}表结构`);
  }
}

/**
 * 执行数据库迁移,处理表结构变更
 * @param {D1Database} db - D1数据库实例
 * @param {number} currentVersion - 当前数据库版本
 * @param {number} targetVersion - 目标数据库版本
 */
async function migrateDatabase(db, currentVersion, targetVersion) {
  console.log(`开始数据库迁移,当前版本:${currentVersion},目标版本:${targetVersion}`);

  // 按版本号顺序执行迁移
  for (let version = currentVersion + 1; version <= targetVersion; version++) {
    console.log(`执行版本${version}的迁移...`);

    try {
      await executeMigrationForVersion(db, version);
      await recordMigration(db, version);
    } catch (error) {
      console.error(`版本${version}迁移失败:`, error);
      console.log("将继续执行后续迁移，但请手动检查数据库结构");
    }
  }

  console.log("数据库迁移完成");
  await cleanupOldMigrationRecords(db, targetVersion);
}

/**
 * 执行特定版本的迁移
 * @param {D1Database} db - D1数据库实例
 * @param {number} version - 迁移版本号
 */
async function executeMigrationForVersion(db, version) {
  switch (version) {
    case 1:
    case 2:
    case 3:
      // 早期版本，跳过
      break;

    case 4:
      // 版本4：为API_KEYS表添加挂载权限字段
      await addTableField(db, DbTables.API_KEYS, "mount_permission", "mount_permission BOOLEAN DEFAULT 0");
      break;

    case 5:
      // 版本5：为API_KEYS表添加basic_path字段
      await addTableField(db, DbTables.API_KEYS, "basic_path", "basic_path TEXT DEFAULT '/'");
      break;

    case 6:
      // 版本6：为S3_CONFIGS表添加自定义域名和签名时效相关字段
      await addTableField(db, DbTables.S3_CONFIGS, "custom_host", "custom_host TEXT");
      await addTableField(db, DbTables.S3_CONFIGS, "signature_expires_in", "signature_expires_in INTEGER DEFAULT 3600");
      break;

    case 7:
      // 版本7：尝试删除S3_CONFIGS表中的custom_host_signature字段
      await removeTableField(db, DbTables.S3_CONFIGS, "custom_host_signature");
      break;

    case 8:
      // 版本8：为storage_mounts表添加web_proxy和webdav_policy字段
      await addTableField(db, DbTables.STORAGE_MOUNTS, "web_proxy", "web_proxy BOOLEAN DEFAULT 0");
      await addTableField(db, DbTables.STORAGE_MOUNTS, "webdav_policy", "webdav_policy TEXT DEFAULT '302_redirect'");
      break;

    case 9:
      // 版本9：为files表添加多存储类型支持字段
      await migrateFilesTableToMultiStorage(db);
      break;

    case 10:
      // 版本10：位标志权限系统迁移
      await migrateToBitFlagPermissions(db);
      break;

    case 11:
      // 版本11：为storage_mounts表添加代理签名相关字段
      await addTableField(db, DbTables.STORAGE_MOUNTS, "enable_sign", "enable_sign BOOLEAN DEFAULT 0");
      await addTableField(db, DbTables.STORAGE_MOUNTS, "sign_expires", "sign_expires INTEGER DEFAULT NULL");
      break;

    case 12:
      // 版本12：系统设置架构重构
      await migrateSystemSettingsStructure(db);
      break;

    case 13:
      // 版本13：添加预览设置默认值
      await addPreviewSettings(db);
      break;

    case 14:
      // 版本14：修改files表的use_proxy默认值
      await migrateFilesUseProxyDefault(db);
      break;

    case 15:
      // 版本15：添加文件命名策略系统设置
      await addFileNamingStrategySetting(db);
      break;

    case 16:
      // 版本16：添加站点设置分组和公告栏设置
      await addSiteSettings(db);
      break;

    case 17:
      // 版本17：添加自定义头部和body设置
      await addCustomContentSettings(db);
      break;

    default:
      console.log(`未知的迁移版本: ${version}`);
      break;
  }
}

/**
 * 删除表字段的通用函数
 * @param {D1Database} db - D1数据库实例
 * @param {string} tableName - 表名
 * @param {string} fieldName - 字段名
 */
async function removeTableField(db, tableName, fieldName) {
  try {
    const columnInfo = await db.prepare(`PRAGMA table_info(${tableName})`).all();
    const fieldExists = columnInfo.results.some((column) => column.name === fieldName);

    if (fieldExists) {
      console.log(`检测到${fieldName}字段，尝试使用现代SQLite语法删除...`);
      await db.prepare(`ALTER TABLE ${tableName} DROP COLUMN ${fieldName}`).run();
      console.log(`${fieldName}字段删除成功`);
    } else {
      console.log(`${fieldName}字段不存在，跳过删除`);
    }
  } catch (error) {
    console.log(`现代SQLite语法删除失败，可能是版本不支持: ${error.message}`);
    console.log("该字段将在代码中被忽略，数据库结构保持不变以确保安全");
  }
}

/**
 * 记录迁移历史
 * @param {D1Database} db - D1数据库实例
 * @param {number} version - 迁移版本号
 */
async function recordMigration(db, version) {
  const now = new Date().toISOString();
  const migrationKey = `migration_${version}`;

  const existingMigration = await db.prepare(`SELECT key FROM ${DbTables.SYSTEM_SETTINGS} WHERE key = ?`).bind(migrationKey).first();

  if (!existingMigration) {
    // 根据版本决定使用哪种INSERT语句
    if (version >= 12) {
      // 版本12及以后：使用新的表结构（包含type, group_id等字段）
      await db
        .prepare(
          `INSERT INTO ${DbTables.SYSTEM_SETTINGS} (key, value, description, type, group_id, sort_order, flags, updated_at)
           VALUES (?, ?, ?, 'string', 99, 999, 1, ?)`
        )
        .bind(migrationKey, "completed", `Version ${version} migration completed`, now)
        .run();
    } else {
      // 版本12之前：使用旧的表结构（只有基本字段）
      await db
        .prepare(
          `INSERT INTO ${DbTables.SYSTEM_SETTINGS} (key, value, description, updated_at)
           VALUES (?, ?, ?, ?)`
        )
        .bind(migrationKey, "completed", `Version ${version} migration completed`, now)
        .run();
    }
  } else {
    console.log(`迁移记录 ${migrationKey} 已存在，跳过插入`);
  }
}

// ==================== 具体迁移函数 ====================

/**
 * 迁移files表到多存储类型支持
 * @param {D1Database} db - D1数据库实例
 */
async function migrateFilesTableToMultiStorage(db) {
  console.log("开始迁移files表到多存储类型支持...");

  try {
    // 检查当前表结构
    const columnInfo = await db.prepare(`PRAGMA table_info(${DbTables.FILES})`).all();
    const existingColumns = new Set(columnInfo.results.map((col) => col.name));

    const hasOldField = existingColumns.has("s3_config_id");
    const hasNewField = existingColumns.has("storage_config_id");

    console.log(`表结构检查: s3_config_id=${hasOldField}, storage_config_id=${hasNewField}`);

    // 添加新字段（如果不存在）
    if (!hasNewField) {
      console.log("添加新字段...");
      await addTableField(db, DbTables.FILES, "storage_config_id", "storage_config_id TEXT");
      await addTableField(db, DbTables.FILES, "storage_type", "storage_type TEXT");
      await addTableField(db, DbTables.FILES, "file_path", "file_path TEXT");
    }

    // 如果有旧字段，先迁移数据
    if (hasOldField) {
      console.log("开始迁移数据...");

      // 迁移数据
      const updateResult = await db
        .prepare(
          `UPDATE ${DbTables.FILES}
           SET storage_config_id = s3_config_id, storage_type = 'S3'
           WHERE s3_config_id IS NOT NULL
             AND (storage_config_id IS NULL OR storage_type IS NULL)`
        )
        .run();

      console.log(`成功迁移 ${updateResult.changes || 0} 条files记录`);
    }

    // 无论如何都重建表，确保最终结构正确
    console.log("重建表结构，确保最终结构正确...");
    await rebuildFilesTable(db);

    console.log("files表迁移完成");
  } catch (error) {
    console.error("迁移files表时出错:", error);
    throw error;
  }
}

/**
 * 重建files表，删除旧字段
 * @param {D1Database} db - D1数据库实例
 */
async function rebuildFilesTable(db) {
  console.log("开始重建files表结构...");

  // 创建新表结构
  await db
    .prepare(
      `CREATE TABLE ${DbTables.FILES}_new (
        id TEXT PRIMARY KEY,
        slug TEXT UNIQUE NOT NULL,
        filename TEXT NOT NULL,
        storage_config_id TEXT NOT NULL,
        storage_type TEXT NOT NULL,
        storage_path TEXT NOT NULL,
        file_path TEXT,
        mimetype TEXT NOT NULL,
        size INTEGER NOT NULL,
        etag TEXT,
        remark TEXT,
        password TEXT,
        expires_at DATETIME,
        max_views INTEGER,
        views INTEGER DEFAULT 0,
        use_proxy BOOLEAN DEFAULT 0,
        created_by TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
      )`
    )
    .run();

  // 复制数据到新表
  await db
    .prepare(
      `INSERT INTO ${DbTables.FILES}_new
       SELECT id, slug, filename, storage_config_id, storage_type, storage_path, file_path,
              mimetype, size, etag, remark, password, expires_at, max_views, views, use_proxy,
              created_by, created_at, updated_at
       FROM ${DbTables.FILES}
       WHERE storage_config_id IS NOT NULL AND storage_config_id != ''`
    )
    .run();

  // 删除旧表
  await db.prepare(`DROP TABLE ${DbTables.FILES}`).run();

  // 重命名新表
  await db.prepare(`ALTER TABLE ${DbTables.FILES}_new RENAME TO ${DbTables.FILES}`).run();

  // 重新创建索引
  await db.prepare(`CREATE INDEX IF NOT EXISTS idx_files_slug ON ${DbTables.FILES}(slug)`).run();
  await db.prepare(`CREATE INDEX IF NOT EXISTS idx_files_storage_config_id ON ${DbTables.FILES}(storage_config_id)`).run();
  await db.prepare(`CREATE INDEX IF NOT EXISTS idx_files_storage_type ON ${DbTables.FILES}(storage_type)`).run();
  await db.prepare(`CREATE INDEX IF NOT EXISTS idx_files_file_path ON ${DbTables.FILES}(file_path)`).run();
  await db.prepare(`CREATE INDEX IF NOT EXISTS idx_files_created_at ON ${DbTables.FILES}(created_at)`).run();
  await db.prepare(`CREATE INDEX IF NOT EXISTS idx_files_expires_at ON ${DbTables.FILES}(expires_at)`).run();

  console.log("成功重建files表结构");
}

/**
 * 位标志权限系统迁移
 * @param {D1Database} db - D1数据库实例
 */
async function migrateToBitFlagPermissions(db) {
  console.log("开始位标志权限系统迁移...");

  try {
    // 备份现有数据
    const existingKeys = await db.prepare(`SELECT * FROM ${DbTables.API_KEYS}`).all();
    console.log(`找到 ${existingKeys.results?.length || 0} 条现有API密钥记录`);

    // 检查新字段是否已存在
    const columnInfo = await db.prepare(`PRAGMA table_info(${DbTables.API_KEYS})`).all();
    const existingColumns = new Set(columnInfo.results.map((col) => col.name));

    if (!existingColumns.has("permissions") || !existingColumns.has("role") || !existingColumns.has("is_guest")) {
      console.log("检测到需要完整的表结构迁移");

      // 创建新表结构并迁移数据
      await db
        .prepare(
          `CREATE TABLE ${DbTables.API_KEYS}_new (
          id TEXT PRIMARY KEY,
          name TEXT UNIQUE NOT NULL,
          key TEXT UNIQUE NOT NULL,
          permissions INTEGER DEFAULT 0,
          role TEXT DEFAULT 'GENERAL',
          basic_path TEXT DEFAULT '/',
          is_guest BOOLEAN DEFAULT 0,
          last_used DATETIME,
          created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
          expires_at DATETIME NOT NULL
        )`
        )
        .run();

      // 迁移数据
      if (existingKeys.results && existingKeys.results.length > 0) {
        for (const keyRecord of existingKeys.results) {
          let permissions = 0;

          // 转换布尔权限为位标志权限
          if (keyRecord.text_permission === 1) permissions |= 1;
          if (keyRecord.file_permission === 1) permissions |= 2;
          if (keyRecord.mount_permission === 1) permissions |= 256 | 512 | 1024 | 2048 | 4096;

          const role = permissions === 256 ? "GUEST" : "GENERAL";

          await db
            .prepare(
              `INSERT INTO ${DbTables.API_KEYS}_new
             (id, name, key, permissions, role, basic_path, is_guest, last_used, created_at, expires_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
            )
            .bind(
              keyRecord.id,
              keyRecord.name,
              keyRecord.key,
              permissions,
              role,
              keyRecord.basic_path || "/",
              role === "GUEST" ? 1 : 0,
              keyRecord.last_used,
              keyRecord.created_at,
              keyRecord.expires_at
            )
            .run();
        }
      }

      // 替换旧表
      await db.prepare(`DROP TABLE ${DbTables.API_KEYS}`).run();
      await db.prepare(`ALTER TABLE ${DbTables.API_KEYS}_new RENAME TO ${DbTables.API_KEYS}`).run();

      console.log("api_keys表结构迁移完成");
    }
  } catch (error) {
    console.error("位标志权限系统迁移失败:", error);
  }
}

/**
 * 系统设置架构重构迁移
 * @param {D1Database} db - D1数据库实例
 */
async function migrateSystemSettingsStructure(db) {
  console.log("开始系统设置架构重构迁移...");

  const newFields = [
    { name: "type", sql: "ALTER TABLE system_settings ADD COLUMN type TEXT DEFAULT 'text'" },
    { name: "group_id", sql: "ALTER TABLE system_settings ADD COLUMN group_id INTEGER DEFAULT 1" },
    { name: "options", sql: "ALTER TABLE system_settings ADD COLUMN options TEXT" },
    { name: "sort_order", sql: "ALTER TABLE system_settings ADD COLUMN sort_order INTEGER DEFAULT 0" },
    { name: "flags", sql: "ALTER TABLE system_settings ADD COLUMN flags INTEGER DEFAULT 0" },
  ];

  const columnInfo = await db.prepare(`PRAGMA table_info(${DbTables.SYSTEM_SETTINGS})`).all();
  const existingColumns = new Set(columnInfo.results.map((col) => col.name));

  for (const field of newFields) {
    if (!existingColumns.has(field.name)) {
      try {
        await db.prepare(field.sql).run();
        console.log(`成功添加字段: ${field.name}`);
      } catch (error) {
        console.error(`添加字段 ${field.name} 失败:`, error);
      }
    }
  }
}

/**
 * 添加预览设置
 * @param {D1Database} db - D1数据库实例
 */
async function addPreviewSettings(db) {
  console.log("开始添加预览设置默认值...");

  const previewSettings = [
    {
      key: "preview_text_types",
      value:
        "txt,htm,html,xml,java,properties,sql,js,md,json,conf,ini,vue,php,py,bat,yml,yaml,go,sh,c,cpp,h,hpp,tsx,vtt,srt,ass,rs,lrc,dockerfile,makefile,gitignore,license,readme",
      description: "支持预览的文本文件扩展名，用逗号分隔",
      type: "textarea",
      group_id: 2,
      sort_order: 1,
      flags: 0,
    },
    {
      key: "preview_audio_types",
      value: "mp3,flac,ogg,m4a,wav,opus,wma",
      description: "支持预览的音频文件扩展名，用逗号分隔",
      type: "textarea",
      group_id: 2,
      sort_order: 2,
      flags: 0,
    },
    {
      key: "preview_video_types",
      value: "mp4,mkv,avi,mov,rmvb,webm,flv,m3u8,ts,m2ts",
      description: "支持预览的视频文件扩展名，用逗号分隔",
      type: "textarea",
      group_id: 2,
      sort_order: 3,
      flags: 0,
    },
    {
      key: "preview_image_types",
      value: "jpg,tiff,jpeg,png,gif,bmp,svg,ico,swf,webp,avif",
      description: "支持预览的图片文件扩展名，用逗号分隔",
      type: "textarea",
      group_id: 2,
      sort_order: 4,
      flags: 0,
    },
    {
      key: "preview_office_types",
      value: "doc,docx,xls,xlsx,ppt,pptx,rtf",
      description: "支持预览的Office文档扩展名（需要在线转换），用逗号分隔",
      type: "textarea",
      group_id: 2,
      sort_order: 5,
      flags: 0,
    },
    {
      key: "preview_document_types",
      value: "pdf",
      description: "支持预览的文档文件扩展名（可直接预览），用逗号分隔",
      type: "textarea",
      group_id: 2,
      sort_order: 6,
      flags: 0,
    },
  ];

  for (const setting of previewSettings) {
    const existing = await db.prepare(`SELECT key FROM ${DbTables.SYSTEM_SETTINGS} WHERE key = ?`).bind(setting.key).first();
    if (!existing) {
      await db
        .prepare(
          `INSERT INTO ${DbTables.SYSTEM_SETTINGS} (key, value, description, type, group_id, sort_order, flags, updated_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)`
        )
        .bind(setting.key, setting.value, setting.description, setting.type, setting.group_id, setting.sort_order, setting.flags)
        .run();
    }
  }
}

/**
 * 修改files表的use_proxy默认值
 * @param {D1Database} db - D1数据库实例
 */
async function migrateFilesUseProxyDefault(db) {
  console.log("开始修改files表的use_proxy默认值...");
  // 由于SQLite限制，这里只是记录迁移，实际的默认值在表创建时已设置
  console.log("files表use_proxy默认值已在表创建时设置为0");
}

/**
 * 添加文件命名策略设置
 * @param {D1Database} db - D1数据库实例
 */
async function addFileNamingStrategySetting(db) {
  console.log("开始添加文件命名策略系统设置...");

  const existing = await db.prepare(`SELECT key FROM ${DbTables.SYSTEM_SETTINGS} WHERE key = ?`).bind("file_naming_strategy").first();
  if (!existing) {
    const options = JSON.stringify([
      { value: "overwrite", label: "覆盖模式" },
      { value: "random_suffix", label: "随机后缀模式" },
    ]);

    await db
      .prepare(
        `INSERT INTO ${DbTables.SYSTEM_SETTINGS} (key, value, description, type, group_id, options, sort_order, flags, updated_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)`
      )
      .bind("file_naming_strategy", "overwrite", "文件命名策略：覆盖模式使用原始文件名（可能冲突），随机后缀模式避免冲突且保持文件名可读性。", "select", 1, options, 4, 0)
      .run();
    console.log("成功添加文件命名策略设置");
  }
}

/**
 * 添加默认代理设置
 * @param {D1Database} db - D1数据库实例
 */
async function addDefaultProxySetting(db) {
  console.log("开始添加默认代理设置...");

  const existing = await db.prepare(`SELECT key FROM ${DbTables.SYSTEM_SETTINGS} WHERE key = ?`).bind("default_use_proxy").first();
  if (!existing) {
    await db
      .prepare(
        `INSERT INTO ${DbTables.SYSTEM_SETTINGS} (key, value, description, type, group_id, sort_order, flags, updated_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)`
      )
      .bind("default_use_proxy", "false", "文件管理的默认代理设置。启用后新上传文件默认使用Worker代理，禁用后默认使用直链。", "bool", 1, 5, 0)
      .run();
  }
}

/**
 * 添加站点设置
 * @param {D1Database} db - D1数据库实例
 */
async function addSiteSettings(db) {
  console.log("开始添加站点设置分组和公告栏设置...");

  const siteSettings = [
    {
      key: "site_title",
      value: "CloudPaste",
      description: "站点标题，显示在浏览器标签页和页面标题中",
      type: "text",
      group_id: 4,
      sort_order: 1,
      flags: 0,
    },
    {
      key: "site_favicon_url",
      value: "",
      description: "站点图标URL，支持https链接或base64格式，留空使用默认图标",
      type: "text",
      group_id: 4,
      sort_order: 2,
      flags: 0,
    },
    {
      key: "site_announcement_enabled",
      value: "false",
      description: "是否在首页显示公告栏",
      type: "bool",
      group_id: 4,
      sort_order: 3,
      flags: 0,
    },
    {
      key: "site_announcement_content",
      value: "",
      description: "公告内容，支持 Markdown 格式",
      type: "textarea",
      group_id: 4,
      sort_order: 4,
      flags: 0,
    },
    {
      key: "site_footer_markdown",
      value: "© 2025 CloudPaste. 保留所有权利。",
      description: "页脚内容，支持 Markdown 格式，留空则不显示页脚",
      type: "textarea",
      group_id: 4,
      sort_order: 5,
      flags: 0,
    },
  ];

  for (const setting of siteSettings) {
    const existing = await db.prepare(`SELECT key FROM ${DbTables.SYSTEM_SETTINGS} WHERE key = ?`).bind(setting.key).first();
    if (!existing) {
      await db
        .prepare(
          `INSERT INTO ${DbTables.SYSTEM_SETTINGS} (key, value, description, type, group_id, options, sort_order, flags, updated_at)
         VALUES (?, ?, ?, ?, ?, NULL, ?, ?, CURRENT_TIMESTAMP)`
        )
        .bind(setting.key, setting.value, setting.description, setting.type, setting.group_id, setting.sort_order, setting.flags)
        .run();
      console.log(`成功添加站点设置: ${setting.key}`);
    }
  }
}

/**
 * 添加自定义头部和body设置
 * @param {D1Database} db - D1数据库实例
 */
async function addCustomContentSettings(db) {
  console.log("开始添加自定义头部和body设置...");

  const customContentSettings = [
    {
      key: "site_custom_head",
      value: "",
      description: "在此处设置的任何内容都会自动放置在网页头部的开头",
      type: "textarea",
      group_id: 4,
      sort_order: 6,
      flags: 0,
    },
    {
      key: "site_custom_body",
      value: "",
      description: "在此处设置的任何内容都会自动放置在网页正文的末尾",
      type: "textarea",
      group_id: 4,
      sort_order: 7,
      flags: 0,
    },
  ];

  for (const setting of customContentSettings) {
    const existing = await db.prepare(`SELECT key FROM ${DbTables.SYSTEM_SETTINGS} WHERE key = ?`).bind(setting.key).first();
    if (!existing) {
      await db
        .prepare(
          `INSERT INTO ${DbTables.SYSTEM_SETTINGS} (key, value, description, type, group_id, options, sort_order, flags, updated_at)
         VALUES (?, ?, ?, ?, ?, NULL, ?, ?, CURRENT_TIMESTAMP)`
        )
        .bind(setting.key, setting.value, setting.description, setting.type, setting.group_id, setting.sort_order, setting.flags)
        .run();
      console.log(`成功添加自定义内容设置: ${setting.key}`);
    } else {
      console.log(`自定义内容设置 ${setting.key} 已存在，跳过添加`);
    }
  }

  console.log("自定义头部和body设置添加完成");
}

// ==================== 清理函数 ====================

/**
 * 清理旧的迁移记录，只保留最近的几个版本
 * @param {D1Database} db - D1数据库实例
 * @param {number} currentVersion - 当前版本
 */
async function cleanupOldMigrationRecords(db, currentVersion) {
  try {
    console.log("开始清理旧的迁移记录...");

    // 保留最近的5个版本，删除更早的迁移记录
    const keepVersions = 5;
    const deleteBeforeVersion = currentVersion - keepVersions;

    if (deleteBeforeVersion > 0) {
      const deleteResult = await db
        .prepare(
          `DELETE FROM ${DbTables.SYSTEM_SETTINGS}
           WHERE key LIKE 'migration_%'
             AND CAST(SUBSTR(key, 11) AS INTEGER) < ?`
        )
        .bind(deleteBeforeVersion)
        .run();

      console.log(`清理了 ${deleteResult.changes || 0} 条旧的迁移记录`);
    }

    console.log("迁移记录清理完成");
  } catch (error) {
    console.error("清理迁移记录时出错:", error);
  }
}

// ==================== 导出函数 ====================

/**
 * 检查数据库是否需要初始化或迁移
 * @param {D1Database} db - D1数据库实例
 * @returns {Promise<boolean>} 是否执行了初始化
 */
export async function checkAndInitDatabase(db) {
  try {
    console.log("检查数据库状态...");

    // 获取所有现有表
    const existingTables = await db.prepare(`SELECT name FROM sqlite_master WHERE type='table'`).all();
    const tableSet = new Set(existingTables.results.map((table) => table.name));

    // 检查每个表是否存在，不存在则创建
    let needsTablesCreation = false;

    // 检查所有必需的表
    const requiredTables = [
      DbTables.PASTES,
      DbTables.PASTE_PASSWORDS,
      DbTables.ADMINS,
      DbTables.ADMIN_TOKENS,
      DbTables.API_KEYS,
      DbTables.S3_CONFIGS,
      DbTables.FILES,
      DbTables.FILE_PASSWORDS,
      DbTables.SYSTEM_SETTINGS,
      DbTables.STORAGE_MOUNTS,
    ];

    for (const tableName of requiredTables) {
      if (!tableSet.has(tableName)) {
        console.log(`${tableName}表不存在，需要创建`);
        needsTablesCreation = true;
        break;
      }
    }

    // 如果有表不存在，执行表初始化
    if (needsTablesCreation) {
      console.log("检测到缺少表，执行表创建...");
      await initDatabase(db);
    }

    // 检查数据库版本
    const versionSetting = await db.prepare(`SELECT value FROM ${DbTables.SYSTEM_SETTINGS} WHERE key = 'schema_version'`).first();

    const currentVersion = versionSetting ? parseInt(versionSetting.value) : 0;
    const targetVersion = 17; // 当前最新版本

    if (currentVersion < targetVersion) {
      console.log(`需要更新数据库结构，当前版本:${currentVersion}，目标版本:${targetVersion}`);

      if (currentVersion === 0 && !needsTablesCreation) {
        // 如果版本为0但表已存在，表示是旧数据库，执行完整初始化确保所有表创建
        await initDatabase(db);
      } else if (currentVersion > 0) {
        // 执行迁移脚本
        await migrateDatabase(db, currentVersion, targetVersion);
      }

      // 更新版本号
      const now = new Date().toISOString();
      const existingVersion = await db.prepare(`SELECT value FROM ${DbTables.SYSTEM_SETTINGS} WHERE key='schema_version'`).first();
      if (existingVersion) {
        await db
          .prepare(
            `UPDATE ${DbTables.SYSTEM_SETTINGS}
             SET value = ?, updated_at = ?
             WHERE key = 'schema_version'`
          )
          .bind(targetVersion.toString(), now)
          .run();
      } else {
        await db
          .prepare(
            `INSERT INTO ${DbTables.SYSTEM_SETTINGS} (key, value, description, type, group_id, sort_order, flags, updated_at)
             VALUES ('schema_version', ?, '数据库Schema版本号', 'string', 99, 1, 1, ?)`
          )
          .bind(targetVersion.toString(), now)
          .run();
      }
    }

    // 检查初始化标记
    if (tableSet.has(DbTables.SYSTEM_SETTINGS)) {
      const initFlag = await db.prepare(`SELECT value FROM ${DbTables.SYSTEM_SETTINGS} WHERE key='db_initialized'`).first();

      if (!initFlag) {
        // 没有初始化标记，设置标记
        const now = new Date().toISOString();
        try {
          await db
            .prepare(
              `INSERT INTO ${DbTables.SYSTEM_SETTINGS} (key, value, description, type, group_id, sort_order, flags, updated_at)
               VALUES ('db_initialized', ?, '数据库初始化完成标记', 'bool', 99, 2, 1, ?)`
            )
            .bind("true", now)
            .run();
          console.log("设置数据库初始化标记");
        } catch (insertError) {
          // 如果插入失败（可能是因为记录已存在），检查是否确实存在
          const recheckFlag = await db.prepare(`SELECT value FROM ${DbTables.SYSTEM_SETTINGS} WHERE key='db_initialized'`).first();
          if (recheckFlag) {
            console.log("数据库初始化标记已存在，跳过设置");
          } else {
            console.error("设置数据库初始化标记失败:", insertError);
            throw insertError;
          }
        }
      } else {
        console.log("数据库已初始化，跳过初始化标记设置");
      }
    }

    return needsTablesCreation || currentVersion < targetVersion;
  } catch (error) {
    console.error("检查数据库状态出错，执行初始化:", error);
    await initDatabase(db);
    return true;
  }
}
