/**
 * 安全相关系统设置读取（带进程内 TTL 缓存）
 *
 * 涉及每次请求的判定（传输加密开关、强制 HTTPS），直接读库会增加延迟，
 * 这里用模块级 Map 做 30s TTL 缓存（Workers isolate / Docker 单进程均生效）。
 */

const CACHE_TTL_MS = 30 * 1000;
const cache = new Map();

async function readBoolSetting(db, key, defaultValue = false) {
  const cached = cache.get(key);
  if (cached && Date.now() < cached.expires) {
    return cached.value;
  }
  let value = defaultValue;
  try {
    // 动态导入避免循环依赖（同 common.js 的做法）
    const { getSettingMetadata } = await import("../services/systemService.js");
    const setting = await getSettingMetadata(db, key);
    if (setting && setting.value !== undefined && setting.value !== null) {
      value = setting.value === true || setting.value === "true";
    }
  } catch (error) {
    console.warn(`读取系统设置 ${key} 失败，使用默认值 ${defaultValue}:`, error?.message || error);
  }
  cache.set(key, { value, expires: Date.now() + CACHE_TTL_MS });
  return value;
}

/**
 * 是否启用服务端文件传输加密（对新上传的分享文件生效）
 */
export function isFileTransferEncryptionEnabled(db) {
  return readBoolSetting(db, "file_transfer_encryption", false);
}

/**
 * 是否强制 HTTPS（http 请求 308 跳转 + HSTS）
 */
export function isForceHttpsEnabled(db) {
  return readBoolSetting(db, "force_https", false);
}

/** 供测试或设置更新后立即生效使用 */
export function clearSecuritySettingsCache() {
  cache.clear();
}
