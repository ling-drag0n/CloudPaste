/**
 * WebDAV认证缓存工具
 * 为Windows WebDAV客户端提供认证信息缓存，解决映射网络驱动器的认证问题
 */

// 内存缓存，存储认证信息
// 键格式: IP地址 + '|' + 用户代理的哈希
// 值格式: {authInfo: {...}, timestamp: 时间戳}
const authCache = new Map();

// 缓存配置
const CACHE_CONFIG = {
  EXPIRATION: 30 * 60 * 1000, // 30分钟过期时间
  MAX_SIZE: 1000, // 最大缓存项数量，防止内存泄漏
  CLEANUP_INTERVAL: 5 * 60 * 1000, // 5分钟清理间隔
  MAX_USER_AGENT_LENGTH: 500, // 最大UserAgent长度
};

// 清理定时器
let cleanupTimer = null;

/**
 * 判断是否为WebDAV客户端
 * @param {string} userAgent - 用户代理字符串
 * @returns {boolean} 是否为WebDAV客户端
 */
function isWebDAVClient(userAgent) {
  // 输入验证
  if (!userAgent || typeof userAgent !== "string") {
    return false;
  }

  // 限制UserAgent长度，防止性能攻击
  if (userAgent.length > CACHE_CONFIG.MAX_USER_AGENT_LENGTH) {
    console.warn(`WebDAV客户端检测: UserAgent过长 (${userAgent.length}字符)`);
    return false;
  }

  // WebDAV客户端识别模式
  const webdavPatterns = [
    /Microsoft-WebDAV-MiniRedir/i, // Windows WebDAV
    /Windows.*WebDAV/i, // Windows WebDAV变体
    /WebDAVFS/i, // WebDAV文件系统
    /Cyberduck/i, // Cyberduck客户端
    /WinSCP/i, // WinSCP客户端
    /Dart\/.*dart:io/i, // Dart WebDAV客户端 (AuthPass等)
    /davfs2/i, // Linux davfs2
    /rclone/i, // rclone工具
    /cadaver/i, // cadaver命令行客户端
    /BitKinex/i, // BitKinex客户端
    /WebDrive/i, // WebDrive客户端
    /gvfs/i, // GNOME虚拟文件系统
    /(Darwin|Mac).*WebDAV/i, // macOS WebDAV
    /(Darwin|Mac).*Finder/i, // macOS Finder
    /WebDAVLib/i, // WebDAV库
  ];

  // 使用some方法提高性能，找到匹配即返回
  return webdavPatterns.some((pattern) => pattern.test(userAgent));
}

/**
 * 生成安全的缓存键
 * 使用FNV-1a哈希算法
 * @param {string} clientIp - 客户端IP
 * @param {string} userAgent - 用户代理
 * @returns {string} 缓存键
 */
function generateCacheKey(clientIp, userAgent) {
  // 输入验证
  if (!clientIp || !userAgent || typeof clientIp !== "string" || typeof userAgent !== "string") {
    throw new Error("Invalid input: clientIp and userAgent must be non-empty strings");
  }

  // 限制输入长度，防止性能攻击
  const sanitizedIp = clientIp.substring(0, 45); // IPv6最大长度
  const sanitizedUA = userAgent.substring(0, CACHE_CONFIG.MAX_USER_AGENT_LENGTH);

  // 改进的FNV-1a哈希算法
  function fnv1aHash(str) {
    let hash = 2166136261; // FNV offset basis
    for (let i = 0; i < str.length; i++) {
      hash ^= str.charCodeAt(i);
      hash = (hash * 16777619) >>> 0; // FNV prime, 无符号右移确保32位
    }
    return hash;
  }

  const combinedString = `${sanitizedIp}:${sanitizedUA}`;
  const hash = fnv1aHash(combinedString);

  return `webdav_${sanitizedIp.replace(/[^a-zA-Z0-9]/g, "_")}_${hash}`;
}

/**
 * 存储认证信息到缓存
 * @param {string} clientIp - 客户端IP
 * @param {string} userAgent - 用户代理
 * @param {Object} authInfo - 认证信息
 */
export function storeAuthInfo(clientIp, userAgent, authInfo) {
  try {
    // 输入验证
    if (!authInfo || typeof authInfo !== "object") {
      console.warn("WebDAV认证缓存: 无效的认证信息");
      return;
    }

    // 为所有WebDAV客户端缓存认证信息
    if (!isWebDAVClient(userAgent)) {
      return;
    }

    // 检查缓存大小限制，防止内存泄漏
    if (authCache.size >= CACHE_CONFIG.MAX_SIZE) {
      console.warn(`WebDAV认证缓存: 达到最大大小限制 (${CACHE_CONFIG.MAX_SIZE})，执行清理`);
      performLRUCleanup();
    }

    const key = generateCacheKey(clientIp, userAgent);
    authCache.set(key, {
      authInfo: authInfo,
      timestamp: Date.now(),
      lastAccessed: Date.now(), // 添加最后访问时间，支持LRU
    });

    // 安全日志（清理敏感信息）
    const safeIp = clientIp.substring(0, 8) + "***";
    const safeUA = userAgent.substring(0, 15) + "...";
    console.log(`WebDAV认证缓存: 已存储 (${safeIp}, ${safeUA})`);

    // 启动定时清理（如果尚未启动）
    startPeriodicCleanup();
  } catch (error) {
    console.error("WebDAV认证缓存存储失败:", error.message);
    // 缓存失败不应该影响主流程，静默处理
  }
}

/**
 * 从缓存中获取认证信息
 * 添加了错误处理、LRU更新和安全日志
 * @param {string} clientIp - 客户端IP
 * @param {string} userAgent - 用户代理
 * @returns {Object|null} 认证信息或null
 */
export function getAuthInfo(clientIp, userAgent) {
  try {
    // 为所有WebDAV客户端使用认证缓存
    if (!isWebDAVClient(userAgent)) {
      return null;
    }

    const key = generateCacheKey(clientIp, userAgent);
    const cached = authCache.get(key);

    // 检查缓存是否存在且未过期
    if (cached && Date.now() - cached.timestamp < CACHE_CONFIG.EXPIRATION) {
      // 更新最后访问时间（LRU策略）
      cached.lastAccessed = Date.now();
      authCache.set(key, cached);

      // 安全日志（清理敏感信息）
      const safeIp = clientIp.substring(0, 8) + "***";
      const safeUA = userAgent.substring(0, 15) + "...";
      console.log(`WebDAV认证缓存: 命中 (${safeIp}, ${safeUA})`);

      return cached.authInfo;
    }

    // 如果过期，删除缓存
    if (cached) {
      const safeIp = clientIp.substring(0, 8) + "***";
      const safeUA = userAgent.substring(0, 15) + "...";
      console.log(`WebDAV认证缓存: 已过期 (${safeIp}, ${safeUA})`);
      authCache.delete(key);
    }

    return null;
  } catch (error) {
    console.error("WebDAV认证缓存获取失败:", error.message);
    return null; // 缓存失败时返回null，让系统进行正常认证
  }
}

/**
 * 执行LRU清理，移除最少使用的缓存项
 * 当缓存达到最大大小时调用
 */
function performLRUCleanup() {
  const entries = Array.from(authCache.entries());

  // 按最后访问时间排序，最久未访问的在前
  entries.sort((a, b) => a[1].lastAccessed - b[1].lastAccessed);

  // 删除最久未访问的25%的项
  const deleteCount = Math.floor(entries.length * 0.25);
  let deletedCount = 0;

  for (let i = 0; i < deleteCount && i < entries.length; i++) {
    authCache.delete(entries[i][0]);
    deletedCount++;
  }

  console.log(`WebDAV认证缓存: LRU清理完成，删除 ${deletedCount} 项`);
}

/**
 * 启动定期清理机制
 */
function startPeriodicCleanup() {
  // 如果定时器已经启动，不重复启动
  if (cleanupTimer) {
    return;
  }

  cleanupTimer = setInterval(() => {
    const now = Date.now();
    let expiredCount = 0;

    // 清理过期项
    for (const [key, value] of authCache.entries()) {
      if (now - value.timestamp > CACHE_CONFIG.EXPIRATION) {
        authCache.delete(key);
        expiredCount++;
      }
    }

    if (expiredCount > 0) {
      console.log(`WebDAV认证缓存: 定期清理完成，删除 ${expiredCount} 个过期项`);
    }

    // 如果缓存为空，停止定时器
    if (authCache.size === 0) {
      clearInterval(cleanupTimer);
      cleanupTimer = null;
      console.log("WebDAV认证缓存: 缓存为空，停止定期清理");
    }
  }, CACHE_CONFIG.CLEANUP_INTERVAL);

  console.log("WebDAV认证缓存: 启动定期清理机制");
}

/**
 * 获取详细的缓存统计信息
 * @returns {Object} 缓存统计
 */
export function getCacheStats() {
  const now = Date.now();
  let expiredCount = 0;
  let oldestTimestamp = now;
  let newestTimestamp = 0;

  // 统计过期项和时间范围
  for (const [, value] of authCache.entries()) {
    if (now - value.timestamp > CACHE_CONFIG.EXPIRATION) {
      expiredCount++;
    }
    if (value.timestamp < oldestTimestamp) {
      oldestTimestamp = value.timestamp;
    }
    if (value.timestamp > newestTimestamp) {
      newestTimestamp = value.timestamp;
    }
  }

  return {
    size: authCache.size,
    maxSize: CACHE_CONFIG.MAX_SIZE,
    expirationMinutes: CACHE_CONFIG.EXPIRATION / (60 * 1000),
    cleanupIntervalMinutes: CACHE_CONFIG.CLEANUP_INTERVAL / (60 * 1000),
    expiredCount: expiredCount,
    utilizationPercent: Math.round((authCache.size / CACHE_CONFIG.MAX_SIZE) * 100),
    oldestEntryAge: authCache.size > 0 ? Math.round((now - oldestTimestamp) / 1000) : 0,
    newestEntryAge: authCache.size > 0 ? Math.round((now - newestTimestamp) / 1000) : 0,
    isCleanupActive: cleanupTimer !== null,
  };
}

/**
 * 手动清理所有过期缓存项
 * 提供管理接口
 * @returns {number} 清理的项数
 */
export function clearExpiredCache() {
  const now = Date.now();
  let expiredCount = 0;

  for (const [key, value] of authCache.entries()) {
    if (now - value.timestamp > CACHE_CONFIG.EXPIRATION) {
      authCache.delete(key);
      expiredCount++;
    }
  }

  console.log(`WebDAV认证缓存: 手动清理完成，删除 ${expiredCount} 个过期项`);
  return expiredCount;
}

/**
 * 清空所有缓存
 * 提供管理接口，用于紧急情况
 * @returns {number} 清理的项数
 */
export function clearAllCache() {
  const size = authCache.size;
  authCache.clear();

  // 停止定时器
  if (cleanupTimer) {
    clearInterval(cleanupTimer);
    cleanupTimer = null;
  }

  console.log(`WebDAV认证缓存: 已清空所有缓存，删除 ${size} 项`);
  return size;
}

// 导出客户端类型检测函数，供其他模块使用
export { isWebDAVClient };

// 进程退出时清理定时器
if (typeof process !== "undefined") {
  process.on("exit", () => {
    if (cleanupTimer) {
      clearInterval(cleanupTimer);
    }
  });
}
