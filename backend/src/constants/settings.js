/**
 * 系统设置相关常量定义
 *
 */

/**
 * 设置分组常量
 *
 */
export const SETTING_GROUPS = {
  GLOBAL: 1, // 全局设置
  PREVIEW: 2, // 预览设置
  WEBDAV: 3, // WebDAV设置
  SITE: 4, // 站点设置
  SYSTEM: 99, // 系统内部设置（不在前端显示）
};

/**
 * 设置类型常量
 * 支持不同的输入组件类型
 */
export const SETTING_TYPES = {
  TEXT: "text", // 文本输入框
  NUMBER: "number", // 数字输入框
  BOOL: "bool", // 开关组件
  SELECT: "select", // 下拉选择框
  TEXTAREA: "textarea", // 多行文本框
};

/**
 * 设置权限标识常量
 * 使用位标志控制设置项的访问权限
 */
export const SETTING_FLAGS = {
  PUBLIC: 0, // 公开可见
  PRIVATE: 1, // 需要管理员权限
  READONLY: 2, // 只读
  DEPRECATED: 3, // 已废弃
};

/**
 * 分组显示名称映射
 * 用于前端界面显示
 */
export const SETTING_GROUP_NAMES = {
  [SETTING_GROUPS.GLOBAL]: "全局设置",
  [SETTING_GROUPS.PREVIEW]: "预览设置",
  [SETTING_GROUPS.WEBDAV]: "WebDAV设置",
  [SETTING_GROUPS.SITE]: "站点设置",
  [SETTING_GROUPS.SYSTEM]: "系统设置",
};

/**
 * 设置项默认配置
 * 定义各个设置项的元数据
 */
export const DEFAULT_SETTINGS = {
  // 全局设置组
  max_upload_size: {
    key: "max_upload_size",
    type: SETTING_TYPES.NUMBER,
    group_id: SETTING_GROUPS.GLOBAL,
    help: "单次上传文件的最大大小限制(MB)，建议根据服务器性能设置。",
    options: null,
    sort_order: 1,
    flag: SETTING_FLAGS.PUBLIC,
    default_value: "100",
  },

  proxy_sign_all: {
    key: "proxy_sign_all",
    type: SETTING_TYPES.BOOL,
    group_id: SETTING_GROUPS.GLOBAL,
    help: "开启后所有代理访问都需要签名验证，提升安全性。",
    options: null,
    sort_order: 2,
    flag: SETTING_FLAGS.PUBLIC,
    default_value: "false",
  },

  proxy_sign_expires: {
    key: "proxy_sign_expires",
    type: SETTING_TYPES.NUMBER,
    group_id: SETTING_GROUPS.GLOBAL,
    help: "代理签名的过期时间（秒），0表示永不过期。",
    options: null,
    sort_order: 3,
    flag: SETTING_FLAGS.PUBLIC,
    default_value: "0",
  },

  file_naming_strategy: {
    key: "file_naming_strategy",
    type: SETTING_TYPES.SELECT,
    group_id: SETTING_GROUPS.GLOBAL,
    help: "文件命名策略：覆盖模式使用原始文件名（可能冲突），随机后缀模式避免冲突且保持文件名可读性。",
    options: JSON.stringify([
      { value: "overwrite", label: "覆盖模式" },
      { value: "random_suffix", label: "随机后缀模式" },
    ]),
    sort_order: 4,
    flag: SETTING_FLAGS.PUBLIC,
    default_value: "overwrite",
  },

  default_use_proxy: {
    key: "default_use_proxy",
    type: SETTING_TYPES.BOOL,
    group_id: SETTING_GROUPS.GLOBAL,
    help: "新文件的默认代理设置。启用后新文件默认使用Worker代理，禁用后默认使用直链。",
    options: null,
    sort_order: 5,
    flag: SETTING_FLAGS.PUBLIC,
    default_value: "false",
  },

  file_transfer_encryption: {
    key: "file_transfer_encryption",
    type: SETTING_TYPES.BOOL,
    group_id: SETTING_GROUPS.GLOBAL,
    help: "服务端传输加密：开启后新上传的分享文件将以 AES-256-GCM 加密存储，下载时代理解密。加密文件强制走代理访问；预签名直传与断点续传暂不支持，将自动回退。",
    options: null,
    sort_order: 6,
    flag: SETTING_FLAGS.PUBLIC,
    default_value: "false",
  },

  force_https: {
    key: "force_https",
    type: SETTING_TYPES.BOOL,
    group_id: SETTING_GROUPS.GLOBAL,
    help: "强制 HTTPS：开启后所有 HTTP 请求将 308 跳转到 HTTPS 并附加 HSTS 头。Docker/自建部署建议开启（Workers 平台已默认提供 HTTPS）。",
    options: null,
    sort_order: 7,
    flag: SETTING_FLAGS.PUBLIC,
    default_value: "false",
  },

  // 预览设置组
  preview_text_types: {
    key: "preview_text_types",
    type: SETTING_TYPES.TEXTAREA,
    group_id: SETTING_GROUPS.PREVIEW,
    help: "支持预览的文本文件扩展名，用逗号分隔",
    options: null,
    sort_order: 1,
    flag: SETTING_FLAGS.PUBLIC,
    default_value:
      "txt,htm,html,xml,java,properties,sql,js,md,json,conf,ini,vue,php,py,bat,yml,yaml,go,sh,c,cpp,h,hpp,tsx,vtt,srt,ass,rs,lrc,gitignore",
  },

  preview_audio_types: {
    key: "preview_audio_types",
    type: SETTING_TYPES.TEXTAREA,
    group_id: SETTING_GROUPS.PREVIEW,
    help: "支持预览的音频文件扩展名，用逗号分隔",
    options: null,
    sort_order: 2,
    flag: SETTING_FLAGS.PUBLIC,
    default_value: "mp3,flac,ogg,m4a,wav,opus,wma",
  },

  preview_video_types: {
    key: "preview_video_types",
    type: SETTING_TYPES.TEXTAREA,
    group_id: SETTING_GROUPS.PREVIEW,
    help: "支持预览的视频文件扩展名，用逗号分隔",
    options: null,
    sort_order: 3,
    flag: SETTING_FLAGS.PUBLIC,
    default_value: "mp4,mkv,avi,mov,rmvb,webm,flv,m3u8,ts,m2ts",
  },

  preview_image_types: {
    key: "preview_image_types",
    type: SETTING_TYPES.TEXTAREA,
    group_id: SETTING_GROUPS.PREVIEW,
    help: "支持预览的图片文件扩展名，用逗号分隔",
    options: null,
    sort_order: 4,
    flag: SETTING_FLAGS.PUBLIC,
    default_value: "jpg,tiff,jpeg,png,gif,bmp,svg,ico,swf,webp,avif,heic,heif",
  },

  preview_providers: {
    key: "preview_providers",
    type: SETTING_TYPES.TEXTAREA,
    group_id: SETTING_GROUPS.PREVIEW,
    help: "预览规则配置（JSON 数组），统一定义匹配条件、预览类型与可用预览器 URL 模板",
    options: null,
    sort_order: 5,
    flag: SETTING_FLAGS.PUBLIC,
    default_value: JSON.stringify(
      [
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
  },

  // WebDAV设置组
  webdav_upload_mode: {
    key: "webdav_upload_mode",
    type: SETTING_TYPES.SELECT,
    group_id: SETTING_GROUPS.WEBDAV,
    help: "WebDAV 客户端上传模式。流式上传大文件，单次上传适合小文件或兼容性场景。",
    options: JSON.stringify([
      { value: "chunked", label: "流式上传" },
      { value: "single", label: "单次上传" },
    ]),
    sort_order: 1,
    flag: SETTING_FLAGS.PUBLIC,
    default_value: "chunked",
  },

  // 站点设置组
  site_title: {
    key: "site_title",
    type: SETTING_TYPES.TEXT,
    group_id: SETTING_GROUPS.SITE,
    help: "站点标题，显示在浏览器标签页和页面标题中",
    options: null,
    sort_order: 1,
    flag: SETTING_FLAGS.PUBLIC,
    default_value: "CloudPaste",
  },

  site_favicon_url: {
    key: "site_favicon_url",
    type: SETTING_TYPES.TEXT,
    group_id: SETTING_GROUPS.SITE,
    help: "站点图标URL，支持https链接或base64格式，留空使用默认图标",
    options: null,
    sort_order: 2,
    flag: SETTING_FLAGS.PUBLIC,
    default_value: "",
  },

  site_announcement_enabled: {
    key: "site_announcement_enabled",
    type: SETTING_TYPES.BOOL,
    group_id: SETTING_GROUPS.SITE,
    help: "是否在首页显示公告栏",
    options: null,
    sort_order: 3,
    flag: SETTING_FLAGS.PUBLIC,
    default_value: "false",
  },

  site_announcement_content: {
    key: "site_announcement_content",
    type: SETTING_TYPES.TEXTAREA,
    group_id: SETTING_GROUPS.SITE,
    help: "公告内容，支持 Markdown 格式",
    options: null,
    sort_order: 4,
    flag: SETTING_FLAGS.PUBLIC,
    default_value: "",
  },

  site_footer_markdown: {
    key: "site_footer_markdown",
    type: SETTING_TYPES.TEXTAREA,
    group_id: SETTING_GROUPS.SITE,
    help: "页脚内容，支持 Markdown 格式，留空则不显示页脚",
    options: null,
    sort_order: 5,
    flag: SETTING_FLAGS.PUBLIC,
    default_value: "© 2025 CloudPaste. 保留所有权利。",
  },

  site_custom_head: {
    key: "site_custom_head",
    type: SETTING_TYPES.TEXTAREA,
    group_id: SETTING_GROUPS.SITE,
    help: "在此处设置的任何内容都会自动放置在网页头部的开头",
    options: null,
    sort_order: 6,
    flag: SETTING_FLAGS.PUBLIC,
    default_value: "",
  },

  site_custom_body: {
    key: "site_custom_body",
    type: SETTING_TYPES.TEXTAREA,
    group_id: SETTING_GROUPS.SITE,
    help: "在此处设置的任何内容都会自动放置在网页正文的末尾",
    options: null,
    sort_order: 7,
    flag: SETTING_FLAGS.PUBLIC,
    default_value: "",
  },

  // 前台入口开关
  // - enabled=true：显示入口 + 允许访问路由
  // - enabled=false：隐藏入口 + 访问时前端自动跳转到其它默认页
  site_home_editor_enabled: {
    key: "site_home_editor_enabled",
    type: SETTING_TYPES.BOOL,
    group_id: SETTING_GROUPS.SITE,
    help: "是否启用首页编辑器（/）。关闭后：首页入口不显示，访问 / 会自动跳转。",
    options: null,
    sort_order: 8,
    flag: SETTING_FLAGS.PUBLIC,
    default_value: "true",
  },

  site_upload_page_enabled: {
    key: "site_upload_page_enabled",
    type: SETTING_TYPES.BOOL,
    group_id: SETTING_GROUPS.SITE,
    help: "是否启用文件上传页面（/upload）。关闭后：上传入口不显示，访问 /upload 会自动跳转。",
    options: null,
    sort_order: 9,
    flag: SETTING_FLAGS.PUBLIC,
    default_value: "true",
  },

  site_mount_explorer_enabled: {
    key: "site_mount_explorer_enabled",
    type: SETTING_TYPES.BOOL,
    group_id: SETTING_GROUPS.SITE,
    help: "是否启用挂载浏览页面（/mount-explorer）。关闭后：挂载入口不显示，访问该页面会自动跳转。",
    options: null,
    sort_order: 10,
    flag: SETTING_FLAGS.PUBLIC,
    default_value: "true",
  },

  // 系统内部设置（不在前端显示）
  db_initialized: {
    key: "db_initialized",
    type: SETTING_TYPES.BOOL,
    group_id: SETTING_GROUPS.SYSTEM,
    help: "数据库初始化状态标记，系统内部使用。",
    options: null,
    sort_order: 1,
    flag: SETTING_FLAGS.READONLY,
    default_value: "false",
  },

  schema_version: {
    key: "schema_version",
    type: SETTING_TYPES.NUMBER,
    group_id: SETTING_GROUPS.SYSTEM,
    help: "数据库架构版本号，系统内部使用。",
    options: null,
    sort_order: 2,
    flag: SETTING_FLAGS.READONLY,
    default_value: "1",
  },
};

/**
 * 验证设置值的辅助函数
 * @param {string} key - 设置键名
 * @param {any} value - 设置值
 * @param {string} type - 设置类型
 * @returns {boolean} 验证结果
 */
export function validateSettingValue(key, value, type) {
  switch (type) {
    case SETTING_TYPES.NUMBER:
      const num = Number(value);
      if (isNaN(num)) return false;

      // 特定设置项的范围验证
      if (key === "max_upload_size") {
        return num > 0; // 非负数
      }
      if (key === "proxy_sign_expires") {
        return num >= 0; // 非负数
      }
      return true;

    case SETTING_TYPES.BOOL:
      return value === "true" || value === "false" || value === true || value === false;

    case SETTING_TYPES.SELECT:
      if (key === "webdav_upload_mode") {
        return ["single", "chunked"].includes(value);
      }
      return true;

    case SETTING_TYPES.TEXT:
    case SETTING_TYPES.TEXTAREA:
      if (typeof value !== "string") return false;

      // 自定义头部和body的长度限制
      if (key === "site_custom_head" || key === "site_custom_body") {
        return value.length <= 100000; // 100KB限制
      }

      // 预览设置的特殊验证
      if (key.startsWith("preview_") && key.endsWith("_types")) {
        // 验证扩展名列表格式：逗号分隔，只包含字母数字和点
        const extensions = value.split(",").map((ext) => ext.trim().toLowerCase());
        return extensions.every((ext) => /^[a-z0-9]+$/.test(ext));
      }

      return true;

    default:
      return true;
  }
}

/**
 * 转换设置值为正确的类型
 * @param {any} value - 原始值
 * @param {string} type - 目标类型
 * @returns {any} 转换后的值
 */
export function convertSettingValue(value, type) {
  switch (type) {
    case SETTING_TYPES.NUMBER:
      return Number(value);
    case SETTING_TYPES.BOOL:
      return value === "true" || value === true;
    case SETTING_TYPES.TEXT:
    case SETTING_TYPES.TEXTAREA:
    case SETTING_TYPES.SELECT:
      return String(value);
    default:
      return value;
  }
}
