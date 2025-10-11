/**
 * 处理WebDAV LOCK请求
 * 创建或刷新文件/目录的锁定
 * 遵循RFC 4918标准，支持exclusive write locks
 */

import { lockManager } from "../utils/LockManager.js";
import { parseLockXML, parseTimeoutHeader, parseDepthHeader, buildLockResponseXML, hasLockConflict } from "../utils/lockUtils.js";
import { handleWebDAVError, createWebDAVErrorResponse } from "../utils/errorUtils.js";
import { getStandardWebDAVHeaders } from "../utils/headerUtils.js";
import { HTTPException } from "hono/http-exception";
import { ApiStatus } from "../../constants/index.js";

/**
 * 处理LOCK请求
 * @param {Object} c - Hono上下文
 * @param {string} path - 请求路径
 * @param {string|Object} userId - 用户ID或信息
 * @param {string} userType - 用户类型
 * @param {D1Database} db - 数据库实例
 * @returns {Response} HTTP响应
 */
export async function handleLock(c, path, userId, userType, db) {
  try {
    console.log(`WebDAV LOCK 请求 - 路径: ${path}, 用户类型: ${userType}`);

    // 获取请求头
    const ifHeader = c.req.header("If");
    const timeoutHeader = c.req.header("Timeout");
    const depthHeader = c.req.header("Depth");

    // 解析超时和深度
    const timeoutSeconds = parseTimeoutHeader(timeoutHeader);
    const depth = parseDepthHeader(depthHeader);

    // 确定锁定所有者
    let owner = "unknown";
    if (userType === "admin") {
      owner = `admin:${userId}`;
    } else if (userType === "apiKey" && typeof userId === "object") {
      owner = `apiKey:${userId.name || userId.id}`;
    }

    // 获取请求体
    let requestBody = "";
    try {
      requestBody = await c.req.text();
    } catch (error) {
      console.error("LOCK请求体读取失败:", error);
      return createWebDAVErrorResponse("无法读取请求体", 400);
    }

    console.log("LOCK请求体:", requestBody);

    // 检查是否为锁定刷新请求
    // RFC 4918: 如果有If头且没有请求体，则为刷新请求
    if (ifHeader && (!requestBody || requestBody.trim() === "")) {
      return await handleLockRefresh(c, path, ifHeader, timeoutSeconds);
    }

    // 解析XML请求体
    let lockRequest;
    try {
      lockRequest = parseLockXML(requestBody);
    } catch (error) {
      console.error("LOCK请求解析失败:", error);
      return createWebDAVErrorResponse("无效的LOCK请求格式", 400);
    }

    console.log("解析的LOCK请求:", lockRequest);

    // 检查现有锁定
    const existingLock = lockManager.getLockByPath(path);
    if (existingLock) {
      // 检查锁定冲突
      if (hasLockConflict(existingLock, lockRequest.scope)) {
        console.log(`LOCK冲突 - 路径: ${path}, 现有锁定: ${existingLock.scope} by ${existingLock.owner}`);
        return createWebDAVErrorResponse("资源已被锁定", 423);
      }
    }

    // 创建新锁定
    let lockInfo;
    try {
      lockInfo = lockManager.createLock(path, lockRequest.owner || owner, timeoutSeconds, depth, lockRequest.scope, lockRequest.type);
    } catch (error) {
      console.error("创建锁定失败:", error);
      if (error instanceof HTTPException) {
        return createWebDAVErrorResponse(error.message, error.status);
      }
      return createWebDAVErrorResponse("创建锁定失败", 500);
    }

    // 构建响应XML
    const responseXML = buildLockResponseXML(path, lockInfo);

    console.log(`LOCK成功 - 路径: ${path}, 令牌: ${lockInfo.token}, 超时: ${timeoutSeconds}秒`);

    return new Response(responseXML, {
      status: 200,
      headers: getStandardWebDAVHeaders({
        customHeaders: {
          "Content-Type": "application/xml; charset=utf-8",
          "Lock-Token": `<${lockInfo.token}>`,
          DAV: "1, 2",
        },
      }),
    });
  } catch (error) {
    console.error("处理LOCK失败:", error);
    return handleWebDAVError("LOCK", error);
  }
}

/**
 * 处理锁定刷新请求
 * @param {Object} c - Hono上下文
 * @param {string} path - 请求路径
 * @param {string} ifHeader - If头的值
 * @param {number} timeoutSeconds - 超时时间
 * @returns {Response} HTTP响应
 */
async function handleLockRefresh(c, path, ifHeader, timeoutSeconds) {
  console.log(`LOCK刷新请求 - 路径: ${path}, If头: ${ifHeader}`);

  // 从If头中提取锁令牌
  const tokenMatch = ifHeader.match(/<([^>]+)>/);
  if (!tokenMatch) {
    return createWebDAVErrorResponse("无效的If头格式", 400);
  }

  const token = tokenMatch[1];

  // 刷新锁定
  const refreshedLock = lockManager.refreshLock(token, timeoutSeconds);
  if (!refreshedLock) {
    return createWebDAVErrorResponse("锁定不存在或已过期", 412);
  }

  // 验证路径匹配
  if (refreshedLock.path !== path) {
    return createWebDAVErrorResponse("锁定路径不匹配", 409);
  }

  // 构建响应XML
  const responseXML = buildLockResponseXML(path, refreshedLock);

  console.log(`LOCK刷新成功 - 路径: ${path}, 令牌: ${token}, 新超时: ${timeoutSeconds}秒`);

  return new Response(responseXML, {
    status: 200,
    headers: {
      "Content-Type": "application/xml; charset=utf-8",
      "Lock-Token": `<${refreshedLock.token}>`,
      DAV: "1, 2",
    },
  });
}
