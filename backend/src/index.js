import { Hono } from "hono";
import { cors } from "hono/cors";
import { HTTPException } from "hono/http-exception";
import { logger } from "hono/logger";
import adminRoutes from "./routes/adminRoutes.js";
import apiKeyRoutes from "./routes/apiKeyRoutes.js";
import { backupRoutes } from "./routes/backupRoutes.js";

import s3ConfigRoutes from "./routes/s3ConfigRoutes.js";
import systemRoutes from "./routes/systemRoutes.js";
import mountRoutes from "./routes/mountRoutes.js";
import webdavRoutes from "./routes/webdavRoutes.js";
import fsRoutes from "./routes/fsRoutes.js";
import { DbTables, ApiStatus } from "./constants/index.js";
import { createErrorResponse } from "./utils/common.js";
import filesRoutes from "./routes/filesRoutes.js";
import pastesRoutes from "./routes/pastesRoutes.js";
import s3UploadRoutes from "./routes/s3UploadRoutes.js";
import fileViewRoutes from "./routes/fileViewRoutes.js";
import urlUploadRoutes from "./routes/urlUploadRoutes.js";
import { fsProxyRoutes } from "./routes/fsProxyRoutes.js";
import { authGateway } from "./middlewares/authGatewayMiddleware.js";

// 创建一个Hono应用实例
const app = new Hono();

// 注册中间件
app.use("*", logger());
// 导入WebDAV配置
import { WEBDAV_BASE_PATH } from "./webdav/auth/config/WebDAVConfig.js";

// 统一CORS中间件
app.use("*", async (c, next) => {
  const isWebDAVPath = c.req.path === WEBDAV_BASE_PATH || c.req.path.startsWith(WEBDAV_BASE_PATH + "/");
  const isRootPath = c.req.path === "/";

  if (c.req.method === "OPTIONS" && (isWebDAVPath || isRootPath)) {
    // WebDAV OPTIONS请求和根路径OPTIONS请求跳过CORS自动处理
    console.log("WebDAV OPTIONS请求:", c.req.method, c.req.path);
    await next();
    return;
  } else {
    // 其他请求使用标准CORS处理
    const corsMiddleware = cors({
      origin: (origin) => {
        return origin || "*";
      },
      allowHeaders: [
        "Content-Type",
        "Authorization",
        "X-API-KEY",
        "Depth",
        "Destination",
        "Overwrite",
        "If-Match",
        "If-None-Match",
        "If-Modified-Since",
        "If-Unmodified-Since",
        "Lock-Token",
        "Content-Length",
        "X-Requested-With",
      ],
      allowMethods: ["GET", "POST", "PUT", "DELETE", "OPTIONS", "PROPFIND", "PROPPATCH", "MKCOL", "COPY", "MOVE", "LOCK", "UNLOCK", "HEAD"],
      exposeHeaders: ["ETag", "Content-Length", "Content-Disposition", "Content-Range", "Accept-Ranges"],
      maxAge: 86400,
      credentials: true,
    });

    return await corsMiddleware(c, next);
  }
});

// 根路径WebDAV OPTIONS兼容性处理器
// 为1Panel等客户端提供WebDAV能力发现支持
// 必须在其他路由注册之前，确保优先匹配
app.options("/", (c) => {
  // 返回标准WebDAV能力声明，与/dav路径保持一致
  const headers = {
    Allow: "OPTIONS, PROPFIND, GET, HEAD, PUT, DELETE, MKCOL, COPY, MOVE, LOCK, UNLOCK, PROPPATCH",
    DAV: "1, 2",
    "MS-Author-Via": "DAV",
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "OPTIONS, PROPFIND, GET, HEAD, PUT, DELETE, MKCOL, COPY, MOVE, LOCK, UNLOCK, PROPPATCH",
    "Access-Control-Allow-Headers": "Authorization, Content-Type, Depth, Destination, If, Lock-Token, Overwrite, X-Custom-Auth-Key",
    "Access-Control-Expose-Headers": "DAV, Lock-Token, MS-Author-Via",
    "Access-Control-Max-Age": "86400",
  };

  console.log("根路径WebDAV OPTIONS请求 - 客户端兼容性支持");
  return new Response("", { status: 200, headers });
});

// 注册路由
app.route("/", adminRoutes);
app.route("/", apiKeyRoutes);
app.route("/", backupRoutes);
app.route("/", fileViewRoutes);
app.route("/", filesRoutes);
app.route("/", pastesRoutes);
app.route("/", s3UploadRoutes);
app.route("/", urlUploadRoutes);
app.route("/", s3ConfigRoutes);
app.route("/", systemRoutes);
app.route("/", mountRoutes);
app.route("/", webdavRoutes);
app.route("/", fsRoutes);
app.route("/", fsProxyRoutes);

// 健康检查路由
app.get("/api/health", (c) => {
  return c.json({
    status: "ok",
    timestamp: new Date().toISOString(),
  });
});

// 全局错误处理
app.onError((err, c) => {
  console.error(`[错误] ${err.message}`, err.stack);

  if (err instanceof HTTPException) {
    const status = err.status || ApiStatus.INTERNAL_ERROR;
    const message = err.message || "服务器内部错误";
    return c.json(createErrorResponse(status, message), status);
  }

  return c.json(createErrorResponse(ApiStatus.INTERNAL_ERROR, "服务器内部错误"), ApiStatus.INTERNAL_ERROR);
});

// 404路由处理
app.notFound((c) => {
  return c.json(createErrorResponse(ApiStatus.NOT_FOUND, "未找到请求的资源"), ApiStatus.NOT_FOUND);
});

// 将应用导出为默认值
export default app;
