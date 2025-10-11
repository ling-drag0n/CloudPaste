/**
 * WebDAV路由定义
 */
import { Hono } from "hono";
import { webdavAuthMiddleware, handleWebDAV } from "../webdav/index.js";
import { WEBDAV_BASE_PATH } from "../webdav/auth/config/WebDAVConfig.js";
import { ApiStatus } from "../constants/index.js";
import { createErrorResponse } from "../utils/common.js";

// 创建WebDAV路由处理程序
const webdavRoutes = new Hono();

// WebDAV认证和权限检查
webdavRoutes.use(WEBDAV_BASE_PATH, webdavAuthMiddleware);
webdavRoutes.use(`${WEBDAV_BASE_PATH}/*`, webdavAuthMiddleware);

// 明确定义各种WebDAV方法的处理函数，避免使用all通配符
const webdavMethods = ["GET", "PUT", "DELETE", "OPTIONS", "PROPFIND", "PROPPATCH", "MKCOL", "COPY", "MOVE", "LOCK", "UNLOCK", "HEAD"];

// 注册WebDAV路由处理器
webdavMethods.forEach((method) => {
  webdavRoutes.on(method, WEBDAV_BASE_PATH, async (c) => {
    try {
      return await handleWebDAV(c);
    } catch (error) {
      console.error(`WebDAV ${method} 请求处理错误:`, error);
      return c.json(createErrorResponse(ApiStatus.INTERNAL_ERROR, "WebDAV服务处理错误", error.message), ApiStatus.INTERNAL_ERROR);
    }
  });
});

// 处理WebDAV子路径的请求
webdavMethods.forEach((method) => {
  webdavRoutes.on(method, `${WEBDAV_BASE_PATH}/*`, async (c) => {
    try {
      return await handleWebDAV(c);
    } catch (error) {
      console.error(`WebDAV ${method} 请求处理错误:`, error);
      return c.json(createErrorResponse(ApiStatus.INTERNAL_ERROR, "WebDAV服务处理错误", error.message), ApiStatus.INTERNAL_ERROR);
    }
  });
});

export default webdavRoutes;
