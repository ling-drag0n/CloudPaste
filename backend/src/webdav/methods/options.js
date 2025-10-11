import { createWebDAVErrorResponse } from "../utils/errorUtils.js";
import { getStandardWebDAVHeaders } from "../utils/headerUtils.js";
import { WEBDAV_CONFIG } from "../auth/config/WebDAVConfig.js";

/**
 * 处理WebDAV OPTIONS请求 - WebDAV能力发现
 * @param {Object} c - Hono上下文
 * @param {string} path - 请求路径
 * @param {string|Object} userId - 用户ID或信息（OPTIONS请求可能为undefined）
 * @param {string} userType - 用户类型（OPTIONS请求可能为undefined）
 * @param {D1Database} db - 数据库实例
 * @returns {Response} HTTP响应
 */
export async function handleOptions(c, path, userId, userType, db) {
  try {
    // WebDAV能力发现请求处理
    return await handleWebDAVOptionsRequest(c, path, userType);
  } catch (error) {
    console.error("WebDAV OPTIONS请求处理失败:", error);
    return createWebDAVErrorResponse("WebDAV OPTIONS请求处理失败", 500, false);
  }
}

/**
 * 处理WebDAV OPTIONS请求
 * @param {Object} c - Hono上下文
 * @param {string} path - 请求路径
 * @param {string} userType - 用户类型（可能为undefined）
 * @returns {Response} WebDAV OPTIONS响应
 */
function handleWebDAVOptionsRequest(c, path, userType) {
  // 构建静态的WebDAV方法列表
  const allowedMethods = buildStaticAllowMethods();

  // 构建DAV合规级别
  const davLevel = buildDAVComplianceLevel();

  // 获取客户端信息
  const clientInfo = detectClientInfo(c);

  // 构建响应头
  const headers = buildWebDAVResponseHeaders(allowedMethods, davLevel, clientInfo);

  // 记录日志
  logOptionsRequest(c, path, userType, davLevel, allowedMethods);

  return new Response(null, {
    status: 200,
    headers: headers,
  });
}

/**
 * 构建静态的WebDAV方法列表
 * 返回WebDAVConfig中定义的所有支持方法
 * @returns {string[]} 允许的方法列表
 */
function buildStaticAllowMethods() {
  // 返回WebDAV配置中定义的所有支持方法
  return [...WEBDAV_CONFIG.METHODS];
}

/**
 * 构建DAV合规级别 - 简化版本
 * @returns {string} DAV合规级别字符串
 */
function buildDAVComplianceLevel() {
  // CloudPaste支持WebDAV Class 1和Class 2
  // Class 1: 基础WebDAV功能（PROPFIND, GET, PUT, DELETE, MKCOL, COPY, MOVE）
  // Class 2: 锁定功能（LOCK, UNLOCK）
  // Class 3: 属性修改（PROPPATCH）当前返回405，不支持
  return "1, 2";
}

/**
 * 检测客户端信息
 * @param {Object} c - Hono上下文
 * @returns {Object} 客户端信息
 */
function detectClientInfo(c) {
  const userAgent = c.req.header("User-Agent") || "";

  return {
    isWindows: userAgent.includes("Microsoft") || userAgent.includes("Windows"),
    isMac: userAgent.includes("Darwin") || userAgent.includes("Mac"),
    isOffice: userAgent.includes("Microsoft Office") || userAgent.includes("Word") || userAgent.includes("Excel"),
    isWebDAVClient: userAgent.includes("WebDAV") || userAgent.includes("DAV"),
    userAgent: userAgent,
  };
}

/**
 * 构建WebDAV响应头
 * @param {string[]} allowedMethods - 允许的方法列表
 * @param {string} davLevel - DAV合规级别
 * @param {Object} clientInfo - 客户端信息
 * @returns {Object} 响应头对象
 */
function buildWebDAVResponseHeaders(allowedMethods, davLevel, clientInfo) {
  // 客户端特定头
  const clientSpecificHeaders = {};
  if (clientInfo.isWindows) {
    clientSpecificHeaders["MS-Author-Via"] = "DAV";
  }
  if (clientInfo.isMac) {
    clientSpecificHeaders["X-DAV-Powered-By"] = "CloudPaste";
  }

  // 使用统一的WebDAV头部管理工具，覆盖默认的Allow头
  return getStandardWebDAVHeaders({
    customHeaders: {
      DAV: davLevel,
      Allow: allowedMethods.join(", "),
      Public: allowedMethods.join(", "),
      "Content-Length": "0",
      "Content-Type": "text/plain",
      Server: "CloudPaste-WebDAV/1.0",
      "Accept-Ranges": "bytes",
      "Cache-Control": "no-cache, no-store, must-revalidate",
      Pragma: "no-cache",
      Expires: "0",
      "X-Content-Type-Options": "nosniff",
      "X-Frame-Options": "DENY",
      ...clientSpecificHeaders,
    },
  });
}

/**
 * 记录OPTIONS请求日志
 * @param {Object} c - Hono上下文
 * @param {string} path - 请求路径
 * @param {string} userType - 用户类型（可能为undefined）
 * @param {string} davLevel - DAV合规级别
 * @param {string[]} allowedMethods - 允许的方法列表
 */
function logOptionsRequest(c, path, userType, davLevel, allowedMethods) {
  const userAgent = c.req.header("User-Agent") || "Unknown";
  const authStatus = userType ? `认证用户(${userType})` : "未认证访问";
  console.log(`WebDAV OPTIONS请求 - 路径: ${path}, 认证状态: ${authStatus}, DAV级别: ${davLevel}, 方法: ${allowedMethods.length}个, 客户端: ${userAgent.substring(0, 50)}`);
}
