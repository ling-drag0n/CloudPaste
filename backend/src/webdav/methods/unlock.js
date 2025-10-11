/**
 * 处理WebDAV UNLOCK请求
 * 删除文件/目录的锁定
 */

import { lockManager } from "../utils/LockManager.js";
import { parseLockTokenHeader } from "../utils/lockUtils.js";
import { handleWebDAVError, createWebDAVErrorResponse } from "../utils/errorUtils.js";
import { getStandardWebDAVHeaders } from "../utils/headerUtils.js";

/**
 * 处理UNLOCK请求
 * @param {Object} c - Hono上下文
 * @param {string} path - 请求路径
 * @param {string|Object} userId - 用户ID或信息
 * @param {string} userType - 用户类型
 * @param {D1Database} db - 数据库实例
 * @returns {Response} HTTP响应
 */
export async function handleUnlock(c, path, userId, userType, db) {
  try {
    console.log(`WebDAV UNLOCK 请求 - 路径: ${path}, 用户类型: ${userType}`);

    // 获取Lock-Token头
    const lockTokenHeader = c.req.header("Lock-Token");
    if (!lockTokenHeader) {
      console.log(`UNLOCK失败 - 路径: ${path}, 缺少Lock-Token头`);
      return createWebDAVErrorResponse("缺少Lock-Token头", 400);
    }

    // 解析锁令牌
    const token = parseLockTokenHeader(lockTokenHeader);
    if (!token) {
      console.log(`UNLOCK失败 - 路径: ${path}, 无效的Lock-Token格式: ${lockTokenHeader}`);
      return createWebDAVErrorResponse("无效的Lock-Token格式", 400);
    }

    console.log(`UNLOCK请求 - 路径: ${path}, 令牌: ${token}`);

    // 直接通过令牌获取锁定信息（更健壮的方式）
    const lockInfo = lockManager.getLockByToken(token);
    if (!lockInfo) {
      console.log(`UNLOCK失败 - 令牌: ${token}, 锁定不存在或已过期`);
      return createWebDAVErrorResponse("锁定不存在或已过期", 409);
    }

    // 验证路径匹配
    if (lockInfo.path !== path) {
      console.log(`UNLOCK警告 - 路径不完全匹配: 锁定路径 ${lockInfo.path}, 请求路径 ${path}`);
      // 注意：不返回错误，因为令牌是权威的身份证明
    }

    // 验证所有权（可选的安全检查）
    let expectedOwner = "unknown";
    if (userType === "admin") {
      expectedOwner = `admin:${userId}`;
    } else if (userType === "apiKey" && typeof userId === "object") {
      expectedOwner = `apiKey:${userId.name || userId.id}`;
    }

    // 注意：这里不强制验证所有权，因为锁令牌本身就是权限证明
    // 但可以记录日志用于审计
    if (lockInfo.owner !== expectedOwner) {
      console.log(`UNLOCK警告 - 路径: ${path}, 所有者不匹配: 锁定所有者 ${lockInfo.owner}, 请求者 ${expectedOwner}`);
    }

    // 删除锁定
    const unlocked = lockManager.unlock(token);
    if (!unlocked) {
      console.log(`UNLOCK失败 - 路径: ${path}, 删除锁定失败`);
      return createWebDAVErrorResponse("删除锁定失败", 500);
    }

    console.log(`UNLOCK成功 - 路径: ${path}, 令牌: ${token}`);

    // 返回204 No Content
    return new Response(null, {
      status: 204,
      headers: getStandardWebDAVHeaders({
        customHeaders: {
          DAV: "1, 2",
        },
      }),
    });
  } catch (error) {
    console.error("处理UNLOCK失败:", error);
    return handleWebDAVError("UNLOCK", error);
  }
}
