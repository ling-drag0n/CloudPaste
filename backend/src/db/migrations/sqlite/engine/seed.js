import crypto from "crypto";
import { DbTables } from "../../../../constants/index.js";

export const DEFAULT_PASTE_TAGS = [
  { id: "preset-tag-red", name: "红色", color: "#EF4444" },
  { id: "preset-tag-yellow", name: "黄色", color: "#FACC15" },
  { id: "preset-tag-green", name: "绿色", color: "#22C55E" },
  { id: "preset-tag-important", name: "重要", color: "#A855F7", legacyColors: ["#9CA3AF", "#6B7280"] },
  { id: "preset-tag-personal", name: "个人", color: "#3B82F6", legacyColors: ["#FFFFFF"] },
];

export async function initializeDefaultPasteTags(db) {
  console.log("初始化默认文本标签...");

  const presetIds = DEFAULT_PASTE_TAGS.map((tag) => tag.id);
  const placeholders = presetIds.map(() => "?").join(", ");
  const maxCustomOrder = await db
    .prepare(
      `SELECT COALESCE(MAX(sort_order), -1) AS max_order
       FROM ${DbTables.PASTE_TAGS}
       WHERE id NOT IN (${placeholders})`,
    )
    .bind(...presetIds)
    .first();
  const startOrder = Number(maxCustomOrder?.max_order ?? -1) + 1;

  for (const [index, tag] of DEFAULT_PASTE_TAGS.entries()) {
    const sortOrder = startOrder + index;

    await db
      .prepare(
        `INSERT OR IGNORE INTO ${DbTables.PASTE_TAGS} (id, name, color, sort_order, created_by)
         VALUES (?, ?, ?, ?, NULL)`,
      )
      .bind(tag.id, tag.name, tag.color, sortOrder)
      .run();

    await db
      .prepare(
        `UPDATE ${DbTables.PASTE_TAGS}
         SET sort_order = ?, updated_at = CURRENT_TIMESTAMP
         WHERE id = ? AND sort_order != ?`,
      )
      .bind(sortOrder, tag.id, sortOrder)
      .run();

    if (tag.legacyColors?.length) {
      const legacyPlaceholders = tag.legacyColors.map(() => "?").join(", ");
      await db
        .prepare(
          `UPDATE ${DbTables.PASTE_TAGS}
           SET color = ?, updated_at = CURRENT_TIMESTAMP
           WHERE id = ? AND color IN (${legacyPlaceholders})`,
        )
        .bind(tag.color, tag.id, ...tag.legacyColors)
        .run();
    }
  }
}

/**
 * SQLite/D1 系统设置与默认数据（engine）
 *
 */

export async function initDefaultSettings(db) {
  console.log("初始化系统默认设置...");

  // 为 cleanup_upload_sessions 任务写入默认调度配置（若不存在）
  const cleanupIntervalSec = 24 * 60 * 60;
  const firstCleanupNextRunIso = new Date(Date.now() + cleanupIntervalSec * 1000).toISOString();
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
      "cleanup_upload_sessions",
      "cleanup_upload_sessions",
      "清理分片上传会话（默认）",
      "定期清理本地分片上传会话记录，保持活跃列表干净。",
      cleanupIntervalSec,
      firstCleanupNextRunIso,
      JSON.stringify({ keepDays: 30, activeGraceHours: 24 }),
      "cleanup_upload_sessions",
    )
    .run();

  // 为 refresh_storage_usage_snapshots 写入默认调度配置（若不存在）
  const refreshIntervalSec = 6 * 60 * 60;
  const firstRefreshNextRunIso = new Date(Date.now() + refreshIntervalSec * 1000).toISOString();
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
      refreshIntervalSec,
      firstRefreshNextRunIso,
      JSON.stringify({ maxItems: 50, maxConcurrency: 1 }),
      "refresh_storage_usage_snapshots",
    )
    .run();

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
      value: "chunked",
      description: "WebDAV 客户端上传模式。流式上传大文件，单次上传适合小文件或兼容性场景。",
      type: "select",
      group_id: 3,
      options: JSON.stringify([
        { value: "chunked", label: "流式上传" },
        { value: "single", label: "单次上传" },
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
    // INSERT OR IGNORE：已存在就忽略
    await db
      .prepare(
        `INSERT OR IGNORE INTO ${DbTables.SYSTEM_SETTINGS} (key, value, description, type, group_id, options, sort_order, flags)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
      )
      .bind(
        setting.key,
        setting.value,
        setting.description,
        setting.type,
        setting.group_id,
        setting.options || null,
        setting.sort_order,
        setting.flags,
      )
      .run();
  }
}

export async function addPreviewSettings(db) {
  console.log("开始添加预览设置默认值...");

  const previewSettings = [
    {
      key: "preview_text_types",
      value:
        "txt,htm,html,xml,java,properties,sql,js,md,json,conf,ini,vue,php,py,bat,yml,yaml,go,sh,c,cpp,h,hpp,tsx,vtt,srt,ass,rs,lrc,gitignore",
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
      key: "preview_providers",
      value: JSON.stringify(
        [
          // 无后缀文件（README/LICENSE/Dockerfile/Makefile 等）兜底
          {
            id: "noext-text",
            priority: 0,
            match: { regex: "/^(readme|license|dockerfile|makefile)$/i" },
            previewKey: "text",
            providers: {},
          },
          {
            id: "office-openxml",
            priority: 0,
            match: { ext: ["docx", "xlsx", "pptx"] },
            previewKey: "office",
            providers: {
              native: "native",
              microsoft: { urlTemplate: "https://view.officeapps.live.com/op/view.aspx?src=$e_url" },
              google: { urlTemplate: "https://docs.google.com/viewer?url=$e_url&embedded=true" },
            },
          },
          {
            id: "office-legacy",
            priority: 0,
            match: { ext: ["doc", "xls", "ppt", "rtf"] },
            previewKey: "office",
            providers: {
              microsoft: { urlTemplate: "https://view.officeapps.live.com/op/view.aspx?src=$e_url" },
              google: { urlTemplate: "https://docs.google.com/viewer?url=$e_url&embedded=true" },
            },
          },
          {
            id: "pdf",
            priority: 0,
            match: { ext: ["pdf"] },
            previewKey: "pdf",
            providers: {
              native: "native",
            },
          },
          {
            id: "epub",
            priority: 0,
            match: { ext: ["epub", "mobi", "azw3", "azw", "fb2", "cbz"] },
            previewKey: "epub",
            providers: {
              native: "native",
            },
          },
          {
            id: "archive",
            priority: 0,
            match: {
              ext: ["zip", "rar", "7z", "tar", "gz", "bz2", "xz", "tgz", "tbz", "tbz2", "txz", "cpio", "iso", "cab", "xar", "ar", "a", "mtree"],
            },
            previewKey: "archive",
            providers: {},
          },
        ],
        null,
        2,
      ),
      description: "预览规则配置（JSON 数组）：定义匹配条件、预览类型与 URL 模板",
      type: "textarea",
      group_id: 2,
      sort_order: 5,
      flags: 0,
    },
  ];

  for (const setting of previewSettings) {
    await db
      .prepare(
        `INSERT OR IGNORE INTO ${DbTables.SYSTEM_SETTINGS} (key, value, description, type, group_id, sort_order, flags, updated_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)`,
      )
      .bind(setting.key, setting.value, setting.description, setting.type, setting.group_id, setting.sort_order, setting.flags)
      .run();
  }
}

export async function resetPreviewProvidersDefaults(db) {
  console.log("开始重置 preview_providers 默认规则...");

  const previewProvidersValue = JSON.stringify(
    [
      // 无后缀文件（README/LICENSE/Dockerfile/Makefile 等）兜底
      {
        id: "noext-text",
        priority: 0,
        match: { regex: "/^(readme|license|dockerfile|makefile)$/i" },
        previewKey: "text",
        providers: {},
      },
      {
        id: "office-openxml",
        priority: 0,
        match: { ext: ["docx", "xlsx", "pptx"] },
        previewKey: "office",
        providers: {
          native: "native",
          microsoft: { urlTemplate: "https://view.officeapps.live.com/op/view.aspx?src=$e_url" },
          google: { urlTemplate: "https://docs.google.com/viewer?url=$e_url&embedded=true" },
        },
      },
      {
        id: "office-legacy",
        priority: 0,
        match: { ext: ["doc", "xls", "ppt", "rtf"] },
        previewKey: "office",
        providers: {
          microsoft: { urlTemplate: "https://view.officeapps.live.com/op/view.aspx?src=$e_url" },
          google: { urlTemplate: "https://docs.google.com/viewer?url=$e_url&embedded=true" },
        },
      },
      {
        id: "pdf",
        priority: 0,
        match: { ext: ["pdf"] },
        previewKey: "pdf",
        providers: {
          native: "native",
        },
      },
      {
        id: "epub",
        priority: 0,
        match: { ext: ["epub", "mobi", "azw3", "azw", "fb2", "cbz"] },
        previewKey: "epub",
        providers: {
          native: "native",
        },
      },
      {
        id: "archive",
        priority: 0,
        match: {
          // 压缩包/归档文件：走本地“在线解压预览”组件
          // 注：后端只取最后一个扩展名（例如 foo.tar.gz -> gz），因此把 tgz/tbz/txz 也单独列出来
          ext: ["zip", "rar", "7z", "tar", "gz", "bz2", "xz", "tgz", "tbz", "tbz2", "txz", "cpio", "iso", "cab", "xar", "ar", "a", "mtree"],
        },
        previewKey: "archive",
        providers: {},
      },
    ],
    null,
    2,
  );

  const description = "预览规则配置（JSON 数组）：定义匹配条件、预览类型与 URL 模板";
  await db
    .prepare(
      `INSERT INTO ${DbTables.SYSTEM_SETTINGS} (key, value, description, type, group_id, sort_order, flags, updated_at)
       VALUES (?, ?, ?, 'textarea', 2, 5, 0, CURRENT_TIMESTAMP)
       ON CONFLICT(key) DO UPDATE SET
         value = excluded.value,
         description = excluded.description,
         type = 'textarea',
         group_id = 2,
         sort_order = 5,
         flags = 0,
         updated_at = CURRENT_TIMESTAMP`,
    )
    .bind("preview_providers", previewProvidersValue, description)
    .run();

  const obsoleteKeys = [
    "preview_office_types",
    "preview_document_types",
    "preview_document_apps",
    "preview_iframe_templates",
  ];
  const placeholders = obsoleteKeys.map(() => "?").join(", ");
  await db
    .prepare(`DELETE FROM ${DbTables.SYSTEM_SETTINGS} WHERE key IN (${placeholders})`)
    .bind(...obsoleteKeys)
    .run();
}

export async function addFileNamingStrategySetting(db) {
  console.log("开始添加文件命名策略系统设置...");

  const options = JSON.stringify([
    { value: "overwrite", label: "覆盖模式" },
    { value: "random_suffix", label: "随机后缀模式" },
  ]);

  await db
    .prepare(
      `INSERT OR IGNORE INTO ${DbTables.SYSTEM_SETTINGS} (key, value, description, type, group_id, options, sort_order, flags, updated_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)`,
    )
    .bind(
      "file_naming_strategy",
      "overwrite",
      "文件命名策略：覆盖模式使用原始文件名（可能冲突），随机后缀模式避免冲突且保持文件名可读性。",
      "select",
      1,
      options,
      4,
      0,
    )
    .run();
}

export async function addDefaultProxySetting(db) {
  console.log("开始添加默认代理设置...");

  await db
    .prepare(
      `INSERT OR IGNORE INTO ${DbTables.SYSTEM_SETTINGS} (key, value, description, type, group_id, sort_order, flags, updated_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)`,
    )
    .bind(
      "default_use_proxy",
      "false",
      "文件管理的默认代理设置。启用后新上传文件默认使用Worker代理，禁用后默认使用直链。",
      "bool",
      1,
      5,
      0,
    )
    .run();
}

export async function addSiteSettings(db) {
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
    await db
      .prepare(
        `INSERT OR IGNORE INTO ${DbTables.SYSTEM_SETTINGS} (key, value, description, type, group_id, options, sort_order, flags, updated_at)
         VALUES (?, ?, ?, ?, ?, NULL, ?, ?, CURRENT_TIMESTAMP)`,
      )
      .bind(setting.key, setting.value, setting.description, setting.type, setting.group_id, setting.sort_order, setting.flags)
      .run();
  }
}

export async function addCustomContentSettings(db) {
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
    await db
      .prepare(
        `INSERT OR IGNORE INTO ${DbTables.SYSTEM_SETTINGS} (key, value, description, type, group_id, options, sort_order, flags, updated_at)
         VALUES (?, ?, ?, ?, ?, NULL, ?, ?, CURRENT_TIMESTAMP)`,
      )
      .bind(setting.key, setting.value, setting.description, setting.type, setting.group_id, setting.sort_order, setting.flags)
      .run();
  }

  console.log("自定义头部和body设置添加完成");
}

/**
 * SQLite/D1 种子数据（legacy）
 *
 * 来源：历史初始化实现（原 `backend/src/utils/database.js`，现已迁移到 db/migrations/sqlite/engine）
 */
export async function createDefaultAdmin(db) {
  console.log("检查默认管理员账户...");

  // Workers 冷启动并发下，用 INSERT OR IGNORE 避免 UNIQUE(username) 竞态导致 500
  const adminId = crypto.randomUUID();
  // 密码 "admin123" 的 SHA-256 哈希
  const defaultPassword = "240be518fabd2724ddb6f04eeb1da5967448d7e831c08c8fa822809f74c720a9";

  await db
    .prepare(
      `INSERT OR IGNORE INTO ${DbTables.ADMINS} (id, username, password)
       VALUES (?, ?, ?)`,
    )
    .bind(adminId, "admin", defaultPassword)
    .run();
}

export async function createDefaultGuestApiKey(db) {
  console.log("检查默认游客 API 密钥...");

  const id = crypto.randomUUID();
  const key = "guest";
  const expiresAt = new Date("9999-12-31T23:59:59Z").toISOString();

  await db
    .prepare(
      `INSERT OR IGNORE INTO ${DbTables.API_KEYS} (id, name, key, permissions, role, basic_path, is_enable, expires_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
    )
    .bind(id, "guest", key, 0, "GUEST", "/", 0, expiresAt)
    .run();
}

export default {
  initDefaultSettings,
  addPreviewSettings,
  addSiteSettings,
  addCustomContentSettings,
  addFileNamingStrategySetting,
  addDefaultProxySetting,
  initializeDefaultPasteTags,
  createDefaultAdmin,
  createDefaultGuestApiKey,
};
