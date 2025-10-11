/**
 * 代理功能相关常量配置
 * 统一管理代理功能的配置参数，避免硬编码
 */

/**
 * 代理路由配置
 */
export const PROXY_CONFIG = {
  // 代理路由前缀
  ROUTE_PREFIX: "/api/p",

  // 代理用户类型标识
  USER_TYPE: "proxy",

  // 默认WebDAV策略
  DEFAULT_WEBDAV_POLICY: "302_redirect",

  // 支持的WebDAV策略
  WEBDAV_POLICIES: {
    REDIRECT: "302_redirect",
    NATIVE_PROXY: "native_proxy",
  },

  // 签名相关配置
  SIGN_PARAM: "sign", // 签名参数名
  TIMESTAMP_PARAM: "ts", // 时间戳参数名
};

/**
 * 代理安全配置
 */
export const PROXY_SECURITY = {
  // 最大路径长度
  MAX_PATH_LENGTH: 2048,

  // 禁止的路径模式
  FORBIDDEN_PATTERNS: [
    "..", // 路径遍历
    "\\", // 反斜杠
    "\0", // 空字节
  ],

  // URL解码错误消息
  DECODE_ERROR_MESSAGE: "无效的路径格式",
};

/**
 * 构建代理URL
 * @param {string} path - 文件路径
 * @param {boolean} download - 是否为下载模式
 * @returns {string} 代理URL
 */
export function buildProxyPath(path, download = false) {
  const basePath = `${PROXY_CONFIG.ROUTE_PREFIX}${path}`;
  return download ? `${basePath}?download=true` : basePath;
}

/**
 * 构建完整的代理URL
 * @param {Request} request - 请求对象
 * @param {string} path - 文件路径
 * @param {boolean} download - 是否为下载模式
 * @returns {string} 完整的代理URL
 */
export function buildFullProxyUrl(request, path, download = false) {
  if (!request) {
    return buildProxyPath(path, download);
  }

  try {
    const url = new URL(request.url);
    const proxyPath = buildProxyPath(path, download);
    return `${url.protocol}//${url.host}${proxyPath}`;
  } catch (error) {
    console.warn("构建完整代理URL失败:", error);
    return buildProxyPath(path, download);
  }
}

/**
 * 构建带签名的代理URL
 * @param {Request} request - 请求对象
 * @param {string} path - 文件路径
 * @param {Object} options - 选项
 * @returns {string} 带签名的代理URL
 */
export function buildSignedProxyUrl(request, path, options = {}) {
  const { download = false, signature, requestTimestamp, needsSignature = true } = options;

  // 如果不需要签名，返回普通URL
  if (!needsSignature || !signature) {
    return buildFullProxyUrl(request, path, download);
  }

  const baseUrl = buildFullProxyUrl(request, path, download);
  const url = new URL(baseUrl);

  // 添加签名参数
  url.searchParams.set(PROXY_CONFIG.SIGN_PARAM, signature);

  // 预览时添加时间戳参数
  if (!download && requestTimestamp) {
    url.searchParams.set(PROXY_CONFIG.TIMESTAMP_PARAM, requestTimestamp);
  }

  return url.toString();
}

/**
 * 安全解码URL路径
 * @param {string} encodedPath - 编码的路径
 * @returns {string} 解码后的路径
 * @throws {Error} 路径格式无效或包含危险字符
 */
export function safeDecodeProxyPath(encodedPath) {
  try {
    const decoded = decodeURIComponent(encodedPath);

    // 检查危险字符
    for (const pattern of PROXY_SECURITY.FORBIDDEN_PATTERNS) {
      if (decoded.includes(pattern)) {
        throw new Error("路径包含禁止的字符");
      }
    }

    // 检查路径长度
    if (decoded.length > PROXY_SECURITY.MAX_PATH_LENGTH) {
      throw new Error("路径长度超出限制");
    }

    return decoded;
  } catch (error) {
    throw new Error(PROXY_SECURITY.DECODE_ERROR_MESSAGE);
  }
}
