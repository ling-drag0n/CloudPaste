/**
 * WebDAV服务入口文件
 *
 */

import { handlePropfind } from "./methods/propfind.js";
import { handleOptions } from "./methods/options.js";
import { handlePut } from "./methods/put.js";
import { handleGet } from "./methods/get.js";
import { handleDelete } from "./methods/delete.js";
import { handleMkcol } from "./methods/mkcol.js";
import { handleMove } from "./methods/move.js";
import { handleCopy } from "./methods/copy.js";
import { handleLock } from "./methods/lock.js";
import { handleUnlock } from "./methods/unlock.js";
import { handleProppatch } from "./methods/proppatch.js";
import { HTTPException } from "hono/http-exception";
import { ApiStatus } from "../constants/index.js";
import { createWebDAVErrorResponse } from "./utils/errorUtils.js";
import { getStandardWebDAVHeaders } from "./utils/headerUtils.js";
import { WebDAVAuth } from "./auth/core/WebDAVAuth.js";
import { processWebDAVPath } from "./utils/webdavUtils.js";

/**
 * 安全的路径处理
 * 防止路径遍历攻击和其他安全漏洞
 */
function processPath(rawPath) {
  return processWebDAVPath(rawPath, true); // 抛出异常模式
}

/**
 * WebDAV统一认证中间件
 *
 * @param {Object} c - Hono上下文
 * @param {Function} next - 下一个中间件
 */
export const webdavAuthMiddleware = (c, next) => {
  const webdavAuth = new WebDAVAuth(c.env.DB);
  const middleware = webdavAuth.createMiddleware();
  return middleware(c, next);
};

/**
 * WebDAV主处理函数
 *
 *
 * @param {Object} c - Hono上下文
 * @returns {Response} HTTP响应
 */
export async function handleWebDAV(c) {
  const method = c.req.method;
  const url = new URL(c.req.url);

  // 获取认证上下文
  const userId = c.get("userId");
  const userType = c.get("userType");

  // 路径处理
  const rawPath = url.pathname;
  let path;

  try {
    path = processPath(rawPath);
  } catch (error) {
    if (error instanceof HTTPException) {
      return new Response(error.message, {
        status: error.status,
        headers: { "Content-Type": "text/plain" },
      });
    }
    throw error;
  }

  // 获取数据库实例
  const db = c.env.DB;

  try {
    // 记录请求日志
    console.log(`WebDAV请求: ${method} ${path}, 用户类型: ${userType}`);

    // 方法分发 - 基于主流WebDAV服务器的方法处理模式
    let response;
    switch (method) {
      case "OPTIONS":
        response = await handleOptions(c, path, userId, userType, db);
        break;
      case "PROPFIND":
        response = await handlePropfind(c, path, userId, userType, db);
        break;
      case "GET":
      case "HEAD":
        response = await handleGet(c, path, userId, userType, db);
        break;
      case "PUT":
        response = await handlePut(c, path, userId, userType, db);
        break;
      case "DELETE":
        response = await handleDelete(c, path, userId, userType, db);
        break;
      case "MKCOL":
        response = await handleMkcol(c, path, userId, userType, db);
        break;
      case "MOVE":
        response = await handleMove(c, path, userId, userType, db);
        break;
      case "COPY":
        response = await handleCopy(c, path, userId, userType, db);
        break;
      case "LOCK":
        response = await handleLock(c, path, userId, userType, db);
        break;
      case "UNLOCK":
        response = await handleUnlock(c, path, userId, userType, db);
        break;
      case "PROPPATCH":
        response = await handleProppatch(c, path, userId, userType, db);
        break;
      default:
        console.warn(`WebDAV不支持的方法: ${method}`);
        throw new HTTPException(ApiStatus.METHOD_NOT_ALLOWED, {
          message: `Method ${method} not supported`,
        });
    }

    // 记录响应日志
    console.log(`WebDAV响应: ${method} ${path}, 状态码: ${response.status}`);
    return response;
  } catch (error) {
    console.error(`WebDAV处理错误: ${method} ${path}`, error);

    // 标准化错误处理
    if (error instanceof HTTPException) {
      return new Response(error.message, {
        status: error.status,
        headers: getStandardWebDAVHeaders({
          customHeaders: { "Content-Type": "text/plain" },
        }),
      });
    }

    // 通用错误响应
    return createWebDAVErrorResponse("Internal Server Error", 500);
  }
}
