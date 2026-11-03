import { DbTables } from "../../../../constants/index.js";
import { LegacyDbTables } from "./legacyKeys.js";
import { StorageFactory } from "../../../../storage/factory/StorageFactory.js";
import { toBool } from "../../../../utils/environmentUtils.js";
import {
  createFsMetaTables,
  createFsSearchIndexTables,
  createScheduledJobRunsTables,
  createScheduledJobsTables,
  createTasksTables,
  createUploadPartsTables,
  createUploadSessionsTables,
  createVfsTables,
  createMetricsCacheTables,
  createPasteTagTables,
} from "./schema.js";
import {
  addCustomContentSettings,
  addFileNamingStrategySetting,
  addPreviewSettings,
  resetPreviewProvidersDefaults,
  addSiteSettings,
  createDefaultGuestApiKey,
  initializeDefaultPasteTags,
} from "./seed.js";

/**
 * SQLite/D1 迁移辅助函数（legacy）
 *
 */

function getBooleanFieldNamesFromStorageSchema(storageType) {
  if (!storageType) return [];
  const meta = StorageFactory.getTypeMetadata(storageType);
  const fields = meta?.configSchema?.fields;
  if (!Array.isArray(fields)) return [];
  return fields
    .filter((f) => f && typeof f === "object" && typeof f.name === "string" && (f.type === "boolean" || f.type === "bool"))
    .map((f) => f.name)
    .filter(Boolean);
}

function coerceConfigJsonBooleans(storageType, configJsonObj) {
  if (!configJsonObj || typeof configJsonObj !== "object") {
    return { changed: false, next: configJsonObj };
  }
  const boolKeys = getBooleanFieldNamesFromStorageSchema(storageType);
  if (!boolKeys.length) {
    return { changed: false, next: configJsonObj };
  }

  let changed = false;
  for (const key of boolKeys) {
    if (!Object.prototype.hasOwnProperty.call(configJsonObj, key)) continue;
    const raw = configJsonObj[key];
    if (raw === undefined || raw === null) continue;
    const nextVal = toBool(raw, false) ? 1 : 0;
    if (configJsonObj[key] !== nextVal) {
      configJsonObj[key] = nextVal;
      changed = true;
    }
  }

  return { changed, next: configJsonObj };
}

async function normalizeStorageConfigsBooleanFields(db) {
  console.log("版本34：开始归一化 storage_configs.config_json 中的布尔字段（统一为 0/1）...");

  let rows = [];
  try {
    const res = await db
      .prepare(`SELECT id, storage_type, config_json FROM ${DbTables.STORAGE_CONFIGS} ORDER BY updated_at DESC`)
      .all();
    rows = Array.isArray(res?.results) ? res.results : [];
  } catch (e) {
    console.warn("版本34：读取 storage_configs 失败，跳过布尔字段归一化：", e?.message || e);
    return { total: 0, updated: 0, failed: 0, skipped: 0 };
  }

  let updated = 0;
  let skipped = 0;
  let failed = 0;

  for (const row of rows) {
    const id = row?.id ? String(row.id) : "";
    const storageType = row?.storage_type ? String(row.storage_type) : "";
    const rawJson = row?.config_json;
    if (!id || !storageType || !rawJson) {
      skipped += 1;
      continue;
    }

    let cfgObj;
    try {
      cfgObj = typeof rawJson === "string" ? JSON.parse(rawJson) : rawJson;
    } catch {
      failed += 1;
      continue;
    }

    const { changed, next } = coerceConfigJsonBooleans(storageType, cfgObj);
    if (!changed) {
      skipped += 1;
      continue;
    }

    try {
      await db
        .prepare(`UPDATE ${DbTables.STORAGE_CONFIGS} SET config_json = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?`)
        .bind(JSON.stringify(next), id)
        .run();
      updated += 1;
    } catch (e) {
      failed += 1;
      console.warn("版本34：归一化 storage_config 失败：", { id, storageType, error: e?.message || e });
    }
  }

  console.log("版本34：storage_configs 布尔字段归一化完成：", { total: rows.length, updated, skipped, failed });
  return { total: rows.length, updated, skipped, failed };
}

export async function addTableField(db, tableName, fieldName, fieldDefinition) {
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

export async function removeTableField(db, tableName, fieldName) {
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

export async function migrateFilesTableToMultiStorage(db) {
  console.log("开始迁移files表到多存储类型支持...");

  // 注意：该迁移原实现会重建 files 表（对存量数据有影响）。
  // 这里保留原逻辑，仅在版本升级路径中调用。
  try {
    const columnInfo = await db.prepare(`PRAGMA table_info(${DbTables.FILES})`).all();
    const existingColumns = new Set(columnInfo.results.map((col) => col.name));

    const hasOldField = existingColumns.has("s3_config_id");
    const hasNewField = existingColumns.has("storage_config_id");

    console.log(`表结构检查: s3_config_id=${hasOldField}, storage_config_id=${hasNewField}`);

    if (!hasNewField) {
      console.log("添加新字段...");
      await addTableField(db, DbTables.FILES, "storage_config_id", "storage_config_id TEXT");
      await addTableField(db, DbTables.FILES, "storage_type", "storage_type TEXT");
      await addTableField(db, DbTables.FILES, "file_path", "file_path TEXT");
    }

    if (hasOldField) {
      console.log("开始迁移数据...");

      const updateResult = await db
        .prepare(
          `UPDATE ${DbTables.FILES}
           SET storage_config_id = s3_config_id, storage_type = 'S3'
           WHERE s3_config_id IS NOT NULL
             AND (storage_config_id IS NULL OR storage_type IS NULL)`,
        )
        .run();

      console.log(`成功迁移 ${updateResult.changes || 0} 条files记录`);
    }

    console.log("重建表结构，确保最终结构正确...");
    await rebuildFilesTable(db);

    console.log("files表迁移完成");
  } catch (error) {
    console.error("迁移files表时出错:", error);
    throw error;
  }
}

export async function rebuildFilesTable(db) {
  console.log("开始重建files表结构...");

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
      )`,
    )
    .run();

  await db
    .prepare(
      `INSERT INTO ${DbTables.FILES}_new
       SELECT id, slug, filename, storage_config_id, storage_type, storage_path, file_path,
              mimetype, size, etag, remark, password, expires_at, max_views, views, use_proxy,
              created_by, created_at, updated_at
       FROM ${DbTables.FILES}
       WHERE storage_config_id IS NOT NULL AND storage_config_id != ''`,
    )
    .run();

  await db.prepare(`DROP TABLE ${DbTables.FILES}`).run();
  await db.prepare(`ALTER TABLE ${DbTables.FILES}_new RENAME TO ${DbTables.FILES}`).run();

  await db.prepare(`CREATE INDEX IF NOT EXISTS idx_files_slug ON ${DbTables.FILES}(slug)`).run();
  await db.prepare(`CREATE INDEX IF NOT EXISTS idx_files_storage_config_id ON ${DbTables.FILES}(storage_config_id)`).run();
  await db.prepare(`CREATE INDEX IF NOT EXISTS idx_files_storage_type ON ${DbTables.FILES}(storage_type)`).run();
  await db.prepare(`CREATE INDEX IF NOT EXISTS idx_files_file_path ON ${DbTables.FILES}(file_path)`).run();
  await db.prepare(`CREATE INDEX IF NOT EXISTS idx_files_created_at ON ${DbTables.FILES}(created_at)`).run();
  await db.prepare(`CREATE INDEX IF NOT EXISTS idx_files_expires_at ON ${DbTables.FILES}(expires_at)`).run();

  console.log("成功重建files表结构");
}

export async function migrateToBitFlagPermissions(db) {
  console.log("开始位标志权限系统迁移...");

  try {
    const existingKeys = await db.prepare(`SELECT * FROM ${DbTables.API_KEYS}`).all();
    console.log(`找到 ${existingKeys.results?.length || 0} 条现有API密钥记录`);

    const columnInfo = await db.prepare(`PRAGMA table_info(${DbTables.API_KEYS})`).all();
    const existingColumns = new Set(columnInfo.results.map((col) => col.name));

    if (
      !existingColumns.has("permissions") ||
      !existingColumns.has("role") ||
      (!existingColumns.has("is_enable") && !existingColumns.has("is_guest"))
    ) {
      console.log("检测到需要完整的表结构迁移");

      await db
        .prepare(
          `CREATE TABLE ${DbTables.API_KEYS}_new (
          id TEXT PRIMARY KEY,
          name TEXT UNIQUE NOT NULL,
          key TEXT UNIQUE NOT NULL,
          permissions INTEGER DEFAULT 0,
          role TEXT DEFAULT 'GENERAL',
          basic_path TEXT DEFAULT '/',
          is_enable BOOLEAN DEFAULT 0,
          last_used DATETIME,
          created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
          expires_at DATETIME NOT NULL
        )`,
        )
        .run();

      if (existingKeys.results && existingKeys.results.length > 0) {
        for (const keyRecord of existingKeys.results) {
          let permissions = 0;

          if (keyRecord.text_permission === 1) permissions |= 1;
          if (keyRecord.file_permission === 1) permissions |= 2;
          if (keyRecord.mount_permission === 1) permissions |= 256 | 512 | 1024 | 2048 | 4096;

          const role = permissions === 256 ? "GUEST" : "GENERAL";

          await db
            .prepare(
              `INSERT INTO ${DbTables.API_KEYS}_new
             (id, name, key, permissions, role, basic_path, is_enable, last_used, created_at, expires_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
            )
            .bind(
              keyRecord.id,
              keyRecord.name,
              keyRecord.key,
              permissions,
              role,
              keyRecord.basic_path || "/",
              0,
              keyRecord.last_used,
              keyRecord.created_at,
              keyRecord.expires_at,
            )
            .run();
        }
      }

      await db.prepare(`DROP TABLE ${DbTables.API_KEYS}`).run();
      await db.prepare(`ALTER TABLE ${DbTables.API_KEYS}_new RENAME TO ${DbTables.API_KEYS}`).run();

      console.log("api_keys表结构迁移完成");
    }
  } catch (error) {
    console.error("位标志权限系统迁移失败:", error);
  }
}

export async function migrateSystemSettingsStructure(db) {
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

export async function migrateWebDavUploadModeToSingleChunked(db) {
  console.log("开始迁移 webdav_upload_mode 设置到 single/chunked...");

  try {
    const row = await db.prepare(`SELECT key, value, options FROM ${DbTables.SYSTEM_SETTINGS} WHERE key = ?`).bind("webdav_upload_mode").first();

    if (!row) {
      console.log("未找到 webdav_upload_mode 设置，跳过迁移");
      return;
    }

    let value = row.value;
    if (value === "direct" || value === "stream") {
      value = "single";
    } else if (value === "multipart") {
      value = "chunked";
    }

    const options = JSON.stringify([
      { value: "chunked", label: "流式上传" },
      { value: "single", label: "单次上传" },
    ]);

    const now = new Date().toISOString();

    await db
      .prepare(
        `UPDATE ${DbTables.SYSTEM_SETTINGS}
         SET value = ?, options = ?, updated_at = ?
         WHERE key = 'webdav_upload_mode'`,
      )
      .bind(value, options, now)
      .run();

    console.log("webdav_upload_mode 设置迁移完成:", value);
  } catch (error) {
    console.error("迁移 webdav_upload_mode 设置失败:", error);
  }
}

export async function normalizeWebDavUploadModeLabels(db) {
  try {
    const row = await db.prepare(`SELECT key, value FROM ${DbTables.SYSTEM_SETTINGS} WHERE key = ?`).bind("webdav_upload_mode").first();

    if (!row) {
      console.log("normalizeWebDavUploadModeLabels: 未找到 webdav_upload_mode 设置，跳过更新");
      return;
    }

    const options = JSON.stringify([
      { value: "chunked", label: "流式上传" },
      { value: "single", label: "单次上传" },
    ]);

    const description = "WebDAV 客户端上传模式。流式上传大文件，单次上传适合小文件或兼容性场景。";
    const now = new Date().toISOString();

    await db
      .prepare(
        `UPDATE ${DbTables.SYSTEM_SETTINGS}
         SET description = ?, options = ?, updated_at = ?
         WHERE key = 'webdav_upload_mode'`,
      )
      .bind(description, options, now)
      .run();

    console.log("normalizeWebDavUploadModeLabels: 已更新 webdav_upload_mode 显示配置");
  } catch (error) {
    console.error("normalizeWebDavUploadModeLabels: 更新 webdav_upload_mode 显示配置失败:", error);
  }
}

export async function migrateFilesUseProxyDefault(db) {
  console.log("开始修改files表的use_proxy默认值...");
  console.log("files表use_proxy默认值已在表创建时设置为0");
}

export async function migrateApiKeysIsGuestToIsEnable(db) {
  console.log("开始迁移 api_keys 表的 is_guest -> is_enable...");

  const columnInfo = await db.prepare(`PRAGMA table_info(${DbTables.API_KEYS})`).all();
  const existingColumns = new Set(columnInfo.results.map((col) => col.name));

  const hasIsGuest = existingColumns.has("is_guest");
  const hasIsEnable = existingColumns.has("is_enable");

  if (!hasIsGuest && !hasIsEnable) {
    console.log("api_keys 表不存在 is_guest / is_enable 字段，跳过迁移");
    return;
  }

  if (hasIsGuest && !hasIsEnable) {
    try {
      console.log("检测到仅存在 is_guest，尝试重命名为 is_enable...");
      await db.prepare(`ALTER TABLE ${DbTables.API_KEYS} RENAME COLUMN is_guest TO is_enable`).run();
      console.log("成功将 is_guest 列重命名为 is_enable");
      return;
    } catch (error) {
      console.warn("重命名 is_guest -> is_enable 失败，将使用添加列 + 复制数据方案：", error);

      await db.prepare(`ALTER TABLE ${DbTables.API_KEYS} ADD COLUMN is_enable BOOLEAN DEFAULT 0`).run();
      await db.prepare(`UPDATE ${DbTables.API_KEYS} SET is_enable = COALESCE(is_guest, 0)`).run();

      try {
        await removeTableField(db, DbTables.API_KEYS, "is_guest");
      } catch (dropError) {
        console.warn("删除 is_guest 列失败，将保留旧列但在代码中忽略：", dropError);
      }
      return;
    }
  }

  if (hasIsGuest && hasIsEnable) {
    console.log("检测到同时存在 is_guest 和 is_enable，使用 is_guest 覆盖 is_enable...");
    await db
      .prepare(`UPDATE ${DbTables.API_KEYS} SET is_enable = COALESCE(is_guest, is_enable, 0)`)
      .run()
      .catch((error) => {
        console.error("同步 is_guest 到 is_enable 时出错：", error);
      });

    try {
      await removeTableField(db, DbTables.API_KEYS, "is_guest");
    } catch (dropError) {
      console.warn("删除 is_guest 列失败，将保留旧列但在代码中忽略：", dropError);
    }
    return;
  }

  if (!hasIsGuest && hasIsEnable) {
    console.log("api_keys 已使用 is_enable 列，不需要迁移 is_guest");
  }
}

export async function runLegacyMigrationByVersion(db, version) {
  switch (version) {
    case 1:
    case 2:
    case 3:
      break;

    case 4:
      await addTableField(db, DbTables.API_KEYS, "mount_permission", "mount_permission BOOLEAN DEFAULT 0");
      break;

    case 5:
      await addTableField(db, DbTables.API_KEYS, "basic_path", "basic_path TEXT DEFAULT '/'");
      break;

    case 6:
      await addTableField(db, LegacyDbTables.S3_CONFIGS, "custom_host", "custom_host TEXT");
      await addTableField(db, LegacyDbTables.S3_CONFIGS, "signature_expires_in", "signature_expires_in INTEGER DEFAULT 3600");
      break;

    case 7:
      await removeTableField(db, LegacyDbTables.S3_CONFIGS, "custom_host_signature");
      break;

    case 8:
      await addTableField(db, DbTables.STORAGE_MOUNTS, "web_proxy", "web_proxy BOOLEAN DEFAULT 0");
      await addTableField(db, DbTables.STORAGE_MOUNTS, "webdav_policy", "webdav_policy TEXT DEFAULT '302_redirect'");
      break;

    case 9:
      await migrateFilesTableToMultiStorage(db);
      break;

    case 10:
      await migrateToBitFlagPermissions(db);
      break;

    case 11:
      await addTableField(db, DbTables.STORAGE_MOUNTS, "enable_sign", "enable_sign BOOLEAN DEFAULT 0");
      await addTableField(db, DbTables.STORAGE_MOUNTS, "sign_expires", "sign_expires INTEGER DEFAULT NULL");
      break;

    case 12:
      await migrateSystemSettingsStructure(db);
      break;

    case 13:
      await addPreviewSettings(db);
      break;

    case 14:
      await migrateFilesUseProxyDefault(db);
      break;

    case 15:
      await addFileNamingStrategySetting(db);
      break;

    case 16:
      await addSiteSettings(db);
      break;

    case 17:
      await addCustomContentSettings(db);
      break;

    case 18: {
      console.log("版本18：创建 storage_configs 表并迁移 s3_configs 数据...");

      await db
        .prepare(
          `
          CREATE TABLE IF NOT EXISTS ${DbTables.STORAGE_CONFIGS} (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            storage_type TEXT NOT NULL,
            admin_id TEXT,
            is_public INTEGER NOT NULL DEFAULT 0,
            is_default INTEGER NOT NULL DEFAULT 0,
            remark TEXT,
            status TEXT NOT NULL DEFAULT 'ENABLED',
            config_json TEXT NOT NULL,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            last_used DATETIME
          )
        `,
        )
        .run();

      await db.prepare(`CREATE INDEX IF NOT EXISTS idx_storage_admin ON ${DbTables.STORAGE_CONFIGS}(admin_id)`).run();
      await db.prepare(`CREATE INDEX IF NOT EXISTS idx_storage_type ON ${DbTables.STORAGE_CONFIGS}(storage_type)`).run();
      await db.prepare(`CREATE INDEX IF NOT EXISTS idx_storage_public ON ${DbTables.STORAGE_CONFIGS}(is_public)`).run();
      await db
        .prepare(
          `CREATE UNIQUE INDEX IF NOT EXISTS idx_default_per_admin
           ON ${DbTables.STORAGE_CONFIGS}(admin_id)
           WHERE is_default = 1`,
        )
        .run();

      await db
        .prepare(
          `
          INSERT OR IGNORE INTO ${DbTables.STORAGE_CONFIGS} (
            id, name, storage_type, admin_id, is_public, is_default, remark, status,
            config_json, created_at, updated_at, last_used
          )
          SELECT
            s.id,
            s.name,
            'S3' AS storage_type,
            s.admin_id,
            COALESCE(s.is_public, 0),
            COALESCE(s.is_default, 0),
            NULL AS remark,
            'ENABLED' AS status,
            json_object(
              'provider_type', s.provider_type,
              'endpoint_url', s.endpoint_url,
              'bucket_name', s.bucket_name,
              'region', s.region,
              'path_style', s.path_style,
              'default_folder', s.default_folder,
              'custom_host', s.custom_host,
              'signature_expires_in', s.signature_expires_in,
              'total_storage_bytes', s.total_storage_bytes,
              'access_key_id', s.access_key_id,
              'secret_access_key', s.secret_access_key
            ) AS config_json,
            s.created_at,
            s.updated_at,
            s.last_used
          FROM ${LegacyDbTables.S3_CONFIGS} s
          WHERE NOT EXISTS (
            SELECT 1 FROM ${DbTables.STORAGE_CONFIGS} t WHERE t.id = s.id
          )
        `,
        )
        .run()
        .catch((error) => {
          // 兼容：若旧库中不存在 s3_configs（或已清理），跳过数据迁移但保留新表结构。
          // 这能避免“全新库/已升级库”在误触发旧迁移时直接失败。
          console.warn("版本18：迁移 s3_configs -> storage_configs 失败，将跳过数据迁移：", error?.message || error);
        });

      console.log("版本18：storage_configs 表与数据迁移完成。");
      break;
    }

    case 19:
      console.log("版本19：检查并创建 principal_storage_acl 表...");

      await db
        .prepare(
          `
          CREATE TABLE IF NOT EXISTS ${DbTables.PRINCIPAL_STORAGE_ACL} (
            subject_type TEXT NOT NULL,
            subject_id TEXT NOT NULL,
            storage_config_id TEXT NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (subject_type, subject_id, storage_config_id)
          )
        `,
        )
        .run();

      await db.prepare(`CREATE INDEX IF NOT EXISTS idx_psa_storage_config_id ON ${DbTables.PRINCIPAL_STORAGE_ACL}(storage_config_id)`).run();

      console.log("版本19：principal_storage_acl 表检查/创建完成。");
      break;

    case 20:
      console.log("版本20：检查并迁移 api_keys.is_guest -> is_enable...");
      await migrateApiKeysIsGuestToIsEnable(db);
      console.log("版本20：api_keys 启用位(is_enable) 迁移完成。");

      console.log("版本20：检查并创建默认游客 API 密钥...");
      await createDefaultGuestApiKey(db);
      console.log("版本20：默认游客 API 密钥检查/创建完成。");
      break;

    case 21:
      console.log("版本21：为 pastes 表添加 title 和 is_public 字段...");
      await addTableField(db, DbTables.PASTES, "title", "title TEXT");
      await addTableField(db, DbTables.PASTES, "is_public", "is_public BOOLEAN NOT NULL DEFAULT 1");
      await db.prepare(`CREATE INDEX IF NOT EXISTS idx_pastes_is_public ON ${DbTables.PASTES}(is_public)`).run();
      console.log("版本21：pastes 表 title / is_public 字段与索引检查/创建完成。");

      console.log("版本21：检查并创建 fs_meta 目录 Meta 表...");
      await createFsMetaTables(db);
      console.log("版本21：fs_meta 表及其索引检查/创建完成。");
      break;

    case 22:
      console.log("版本22：迁移 webdav_upload_mode 设置到 single/chunked...");
      await migrateWebDavUploadModeToSingleChunked(db);
      break;

    case 23:
      console.log("版本23：检查并补充 preview_providers 预览规则配置...");
      await addPreviewSettings(db);
      console.log("版本23：preview_providers 配置检查/创建完成。");
      console.log("版本23：更新 webdav_upload_mode 显示选项为“流式上传/单次上传”...");
      await normalizeWebDavUploadModeLabels(db);
      console.log("版本23：webdav_upload_mode 选项更新完成。");
      break;

    case 24:
      console.log("版本24：为 storage_configs 表添加 url_proxy 字段...");
      await addTableField(db, DbTables.STORAGE_CONFIGS, "url_proxy", "url_proxy TEXT");
      console.log("版本24：storage_configs.url_proxy 字段检查/创建完成。");
      break;

    case 25:
      console.log("版本25：检查并创建 tasks 表...");
      await createTasksTables(db);
      console.log("版本25：tasks 表及索引创建完成。");
      break;

    case 26:
      console.log("版本26：检查并创建 upload_sessions 表...");
      await createUploadSessionsTables(db);
      console.log("版本26：upload_sessions 表及索引检查/创建完成。");
      break;

    case 27:
      console.log("版本27：检查并创建 scheduled_jobs，scheduled_job_runs 表...");
      await createScheduledJobsTables(db);
      await createScheduledJobRunsTables(db);
      console.log("版本27：scheduled_jobs，scheduled_job_runs 表及索引检查/创建完成。");
      break;

    case 28:
      console.log("版本28：检查并创建 FS 搜索索引表（FTS5 trigram）...");
      await createFsSearchIndexTables(db);
      console.log("版本28：FS 搜索索引表检查/创建完成。");
      break;

    case 29:
      console.log("版本29：为 tasks 表添加 trigger_type / trigger_ref 字段...");
      await addTableField(db, DbTables.TASKS, "trigger_type", "trigger_type TEXT NOT NULL DEFAULT 'manual'");
      await addTableField(db, DbTables.TASKS, "trigger_ref", "trigger_ref TEXT");
      console.log("版本29：tasks.trigger_type / trigger_ref 字段检查/创建完成。");
      break;

    case 30:
      console.log("版本30：为 storage_mounts 表添加 enable_folder_summary_compute 字段...");
      await addTableField(
        db,
        DbTables.STORAGE_MOUNTS,
        "enable_folder_summary_compute",
        "enable_folder_summary_compute BOOLEAN DEFAULT 0",
      );
      console.log("版本30：storage_mounts.enable_folder_summary_compute 字段检查/创建完成。");
      break;

    case 31:
      console.log("版本31：重置 preview_providers 默认规则...");
      await resetPreviewProvidersDefaults(db);
      console.log("版本31：preview_providers 默认规则重置完成。");
      break;

    case 32:
      console.log("版本32：检查并创建 vfs_nodes 与 upload_parts 表...");
      await createVfsTables(db);
      await createUploadPartsTables(db);
      console.log("版本32：vfs_nodes 与 upload_parts 表检查/创建完成。");
      break;

    case 33:
      console.log("版本33：统一 upload_sessions.status 状态值（active -> initiated/uploading）...");
      try {
        const result = await db
          .prepare(
            `UPDATE ${DbTables.UPLOAD_SESSIONS}
             SET status = CASE
               WHEN (bytes_uploaded > 0)
                 OR (uploaded_parts > 0)
                 OR (next_expected_range IS NOT NULL AND next_expected_range != '')
               THEN 'uploading'
               ELSE 'initiated'
             END
             WHERE status = 'active'`,
          )
          .run();

        console.log("版本33：upload_sessions.status 迁移完成", {
          changes: result?.changes ?? result?.meta?.changes ?? 0,
        });
      } catch (error) {
        console.warn("版本33：upload_sessions.status 迁移失败（可忽略，将由新代码覆盖旧状态）:", error?.message || error);
      }
      break;

    case 34: {
      console.log("版本34：新增 metrics_cache（用量快照缓存）+ 默认快照刷新任务...");
      try {
        await createMetricsCacheTables(db);
      } catch (e) {
        console.warn("版本34：创建 metrics_cache 失败（可忽略，后续会再次尝试）:", e?.message || e);
      }
      try {
        const intervalSec = 6 * 60 * 60;
        const nextRunAfterIso = new Date(Date.now() + intervalSec * 1000).toISOString();
        await db
          .prepare(
            `
            INSERT INTO ${DbTables.SCHEDULED_JOBS} (task_id, handler_id, name, description, enabled, schedule_type, interval_sec, next_run_after, config_json)
            SELECT ?, ?, ?, ?, 1, 'interval', ?, ?, ?
            WHERE NOT EXISTS (
              SELECT 1 FROM ${DbTables.SCHEDULED_JOBS} WHERE task_id = ?
            )
          `,
          )
          .bind(
            "refresh_storage_usage_snapshots",
            "refresh_storage_usage_snapshots",
            "刷新存储用量快照（默认）",
            "定期刷新存储用量数据（已用/总量）。用于上传容量限制判断与管理端展示。",
            intervalSec,
            nextRunAfterIso,
            JSON.stringify({ maxItems: 50, maxConcurrency: 1 }),
            "refresh_storage_usage_snapshots",
          )
          .run();
      } catch (e) {
          console.warn("版本34：写入默认 refresh_storage_usage_snapshots 任务失败（可忽略）:", e?.message || e);
        }

      // 归一化 storage_configs.config_json 中的布尔字段（统一为 0/1）
      try {
        await normalizeStorageConfigsBooleanFields(db);
      } catch (e) {
        console.warn("版本34：归一化 storage_configs 布尔字段失败（可忽略，将由后续保存配置逐步修复）：", e?.message || e);
      }

      break;
    }

    case 35:
      console.log("版本35：初始化文本标签功能...");
      await createPasteTagTables(db);
      await initializeDefaultPasteTags(db);
      console.log("版本35：文本标签功能初始化完成。");
      break;

    default:
      console.log(`未知的迁移版本: ${version}`);
      break;
  }
}

export default {
  addTableField,
  removeTableField,
  migrateFilesTableToMultiStorage,
  rebuildFilesTable,
  migrateToBitFlagPermissions,
  migrateSystemSettingsStructure,
  migrateWebDavUploadModeToSingleChunked,
  normalizeWebDavUploadModeLabels,
  migrateFilesUseProxyDefault,
  migrateApiKeysIsGuestToIsEnable,
  runLegacyMigrationByVersion,
};
