// docker-server.js - Express服务器实现
// 用于在Docker环境中运行的Express服务器，提供与Cloudflare Workers兼容的API接口

// 核心依赖
import express from "express";
import sqlite3 from "sqlite3";
import { open } from "sqlite";
import path from "path";
import fs from "fs";
import { fileURLToPath } from "url";
import methodOverride from "method-override";
import os from "os"; // 添加临时目录支持
import crypto from "crypto"; // 添加用于生成随机文件名

// 项目依赖
import { checkAndInitDatabase } from "./src/utils/database.js";
import app from "./src/index.js";
import { ApiStatus } from "./src/constants/index.js";

import { getWebDAVConfig } from "./src/webdav/auth/config/WebDAVConfig.js";

// ES模块兼容性处理：获取__dirname
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// 日志级别常量
const LOG_LEVELS = {
  ERROR: 0,
  WARN: 1,
  INFO: 2,
  DEBUG: 3,
};

// 当前日志级别，可通过环境变量设置
const CURRENT_LOG_LEVEL = process.env.LOG_LEVEL ? parseInt(process.env.LOG_LEVEL) : LOG_LEVELS.INFO;

/**
 * 统一的日志函数
 * @param {string} level - 日志级别 ('error', 'warn', 'info', 'debug')
 * @param {string} message - 日志消息
 * @param {Object} [data] - 附加数据对象
 */
function logMessage(level, message, data = null) {
  const logLevel = LOG_LEVELS[level.toUpperCase()];
  if (logLevel <= CURRENT_LOG_LEVEL) {
    if (data) {
      console[level.toLowerCase()](message, data);
    } else {
      console[level.toLowerCase()](message);
    }
  }
}

// ==========================================
// SQLite适配器类 - 提供与Cloudflare D1数据库兼容的接口
// ==========================================

/**
 * SQLite适配器类 - 提供与Cloudflare D1数据库兼容的接口
 * 用于在Docker环境中模拟D1数据库的行为
 */
class SQLiteAdapter {
  constructor(dbPath) {
    this.dbPath = dbPath;
    this.db = null;
  }

  async init() {
    logMessage("info", `初始化SQLite数据库: ${this.dbPath}`);
    this.db = await open({
      filename: this.dbPath,
      driver: sqlite3.Database,
    });

    // 启用外键约束，确保数据完整性
    await this.db.exec("PRAGMA foreign_keys = ON;");
    return this;
  }

  // 模拟D1的prepare方法，提供与Cloudflare D1兼容的接口
  prepare(sql) {
    return {
      sql,
      params: [],
      _db: this.db,

      bind(...args) {
        this.params = args;
        return this;
      },

      async run() {
        try {
          await this._db.run(this.sql, ...this.params);
          return { success: true };
        } catch (error) {
          logMessage("error", "SQL执行错误:", { error, sql: this.sql, params: this.params });
          throw error;
        }
      },

      async all() {
        try {
          const results = await this._db.all(this.sql, ...this.params);
          return { results };
        } catch (error) {
          logMessage("error", "SQL查询错误:", { error, sql: this.sql, params: this.params });
          throw error;
        }
      },

      async first() {
        try {
          return await this._db.get(this.sql, ...this.params);
        } catch (error) {
          logMessage("error", "SQL查询错误:", { error, sql: this.sql, params: this.params });
          throw error;
        }
      },
    };
  }

  // batch方法
  async batch(statements) {
    logMessage("info", `执行批处理操作，共${statements.length}条语句`);

    // 开始事务
    try {
      // 开始事务
      await this.db.exec("BEGIN TRANSACTION");

      // 执行所有语句
      const results = [];
      for (const statement of statements) {
        if (typeof statement === "string") {
          // 如果是纯SQL字符串
          const result = await this.db.exec(statement);
          results.push({ success: true, result });
        } else if (statement.sql && Array.isArray(statement.params)) {
          // 如果是{sql, params}格式
          const result = await this.db.run(statement.sql, ...statement.params);
          results.push({ success: true, result });
        } else if (statement.sql && typeof statement.params === "undefined") {
          // 只有SQL没有参数
          const result = await this.db.run(statement.sql);
          results.push({ success: true, result });
        } else {
          // 处理预处理语句的情况
          const stmt = this.prepare(statement.text || statement.sql);
          if (statement.params) {
            stmt.bind(...statement.params);
          }
          const result = await stmt.run();
          results.push(result);
        }
      }

      // 提交事务
      await this.db.exec("COMMIT");

      return results;
    } catch (error) {
      // 发生错误，回滚事务
      logMessage("error", "批处理执行错误，回滚事务:", { error });
      try {
        await this.db.exec("ROLLBACK");
      } catch (rollbackError) {
        logMessage("error", "事务回滚失败:", { rollbackError });
      }
      throw error;
    }
  }

  // 直接执行SQL语句的方法
  async exec(sql) {
    try {
      return await this.db.exec(sql);
    } catch (error) {
      logMessage("error", "SQL执行错误:", { error, sql });
      throw error;
    }
  }
}

// 创建SQLite适配器实例的工厂函数
function createSQLiteAdapter(dbPath) {
  return new SQLiteAdapter(dbPath);
}

/**
 * 统一的错误响应处理函数
 * @param {Error} error - 错误对象
 * @param {number} status - HTTP状态码
 * @param {string} defaultMessage - 默认错误消息
 */
function createErrorResponse(error, status = ApiStatus.INTERNAL_ERROR, defaultMessage = "服务器内部错误") {
  // 生成唯一错误ID用于日志跟踪
  const errorId = Date.now().toString(36) + Math.random().toString(36).substr(2, 5);

  // 记录详细错误信息但过滤敏感数据
  const sanitizedErrorMessage = error.message ? error.message.replace(/key|password|token|secret|auth/gi, (match) => "*".repeat(match.length)) : defaultMessage;

  // 在日志中包含错误ID方便后续追踪
  logMessage("error", `[${errorId}] 服务器错误:`, {
    status,
    message: sanitizedErrorMessage,
    stack: error.stack ? error.stack.split("\n").slice(0, 3).join("\n") : null,
  });

  // 对外部响应隐藏技术细节
  return {
    code: status,
    message: defaultMessage,
    errorId: errorId, // 包含错误ID便于用户报告问题
    success: false,
    data: null,
  };
}

// Express应用程序设置
const server = express();
const PORT = process.env.PORT || 8787;

// 数据目录和数据库设置
const dataDir = process.env.DATA_DIR || path.join(__dirname, "data");
if (!fs.existsSync(dataDir)) {
  fs.mkdirSync(dataDir, { recursive: true });
}

const dbPath = path.join(dataDir, "cloudpaste.db");
logMessage("info", `数据库文件路径: ${dbPath}`);

// 初始化SQLite适配器
const sqliteAdapter = createSQLiteAdapter(dbPath);
let isDbInitialized = false;

// ==========================================
// WebDAV统一认证系统配置
// ==========================================

// 获取WebDAV配置
const webdavConfig = getWebDAVConfig();

// CORS 由 Hono 层统一处理；此处不再配置 Express 侧 CORS（避免重复）

// ==========================================
// 中间件和服务器配置
// ==========================================

// 明确告知Express处理WebDAV方法
webdavConfig.METHODS.forEach((method) => {
  server[method.toLowerCase()] = function (path, ...handlers) {
    return server.route(path).all(function (req, res, next) {
      if (req.method === method) {
        return next();
      }
      next("route");
    }, ...handlers);
  };
});

// 为WebDAV方法添加直接路由，确保它们能被正确处理
webdavConfig.METHODS.forEach((method) => {
  server[method.toLowerCase()]("/dav*", (req, res, next) => {
    logMessage("debug", `直接WebDAV路由处理: ${method} ${req.path}`);
    next();
  });
});

// ==========================================
// 中间件配置（按功能分组）
// ==========================================

// 1. 基础中间件 - HTTP方法处理（CORS 由 Hono 统一）
// ==========================================
server.use(methodOverride("X-HTTP-Method-Override"));
server.use(methodOverride("X-HTTP-Method"));
server.use(methodOverride("X-Method-Override"));
server.disable("x-powered-by");

// 2. 请求体处理中间件
// ==========================================
// 处理multipart/form-data请求体的中间件
server.use((req, res, next) => {
  if (req.method === "POST" && req.headers["content-type"] && req.headers["content-type"].includes("multipart/form-data")) {
    logMessage("debug", `检测到multipart/form-data请求: ${req.path}，使用流式处理和临时文件存储`);

    // 创建随机临时文件名
    const tempFileName = `upload-${crypto.randomBytes(16).toString("hex")}`;
    const tempFilePath = path.join(os.tmpdir(), tempFileName);

    // 创建写入流
    const fileStream = fs.createWriteStream(tempFilePath);

    // 通过管道将请求流导入文件
    req.pipe(fileStream);

    // 保存临时文件路径，供后续处理使用
    req.tempFilePath = tempFilePath;

    // 处理文件写入完成
    fileStream.on("finish", () => {
      logMessage("debug", `multipart请求数据已保存到临时文件: ${tempFilePath}`);
      next();
    });

    // 处理错误
    fileStream.on("error", (err) => {
      logMessage("error", `保存multipart请求数据到临时文件失败:`, { error: err });
      // 尝试清理失败的临时文件
      try {
        fs.unlinkSync(tempFilePath);
      } catch (cleanupErr) {
        // 忽略清理错误
      }
      next(err);
    });

    // 处理请求结束，确保资源清理
    res.on("finish", () => {
      // 响应结束后清理临时文件
      if (req.tempFilePath) {
        logMessage("debug", `请求处理完成，清理临时文件: ${req.tempFilePath}`);
        try {
          fs.unlinkSync(req.tempFilePath);
          req.tempFilePath = null;
        } catch (cleanupErr) {
          logMessage("error", `清理临时文件失败:`, { error: cleanupErr, path: req.tempFilePath });
        }
      }
    });

    // 处理错误中断
    req.on("error", (err) => {
      logMessage("error", `multipart请求处理错误:`, { error: err });
      fileStream.end();
      next(err);
    });
  } else {
    // 非multipart请求，直接传递给下一个中间件
    next();
  }
});

// 处理原始请求体（XML、二进制等）
server.use(
    express.raw({
      type: ["application/xml", "text/xml", "application/octet-stream"],
      limit: "5gb",
      verify: (req, res, buf, encoding) => {
        // 对于WebDAV方法，特别是MKCOL，记录详细信息以便调试
        if ((req.method === "MKCOL" || req.method === "PUT") && buf && buf.length > 10 * 1024 * 1024) {
          logMessage("debug", `大型WebDAV ${req.method} 请求体:`, {
            contentType: req.headers["content-type"],
            size: buf ? buf.length : 0,
          });
        }

        // 安全检查：检测潜在的异常XML或二进制内容
        if (buf && req.path.startsWith("/dav") && (req.headers["content-type"] || "").includes("xml") && buf.length > 0) {
          // 检查是否为有效的XML开头标记，简单验证
          const xmlStart = buf.slice(0, Math.min(50, buf.length)).toString();
          if (!xmlStart.trim().startsWith("<?xml") && !xmlStart.trim().startsWith("<")) {
            logMessage("warn", `可疑的XML请求体: ${req.method} ${req.path} - 内容不以XML标记开头`, {
              contentType: req.headers["content-type"],
              bodyPreview: xmlStart.replace(/[\x00-\x1F\x7F-\xFF]/g, ".").substring(0, 30),
            });
          }
        }
      },
    })
);

// 处理请求体大小限制错误
server.use((err, req, res, next) => {
  if (err.type === "entity.too.large") {
    logMessage("error", `请求体过大错误:`, {
      method: req.method,
      path: req.path,
      contentLength: req.headers["content-length"] || "未知",
      limit: err.limit,
    });
    return res.status(413).json({
      error: "请求体过大",
      message: `上传内容超过限制 (${err.limit})`,
      maxSize: err.limit,
    });
  }

  // 处理multipart/form-data解析错误
  if (
      err.message &&
      (err.message.includes("Unexpected end of form") || err.message.includes("Unexpected end of multipart data") || err.message.includes("Multipart: Boundary not found"))
  ) {
    logMessage("error", `Multipart解析错误:`, {
      method: req.method,
      path: req.path,
      contentType: req.headers["content-type"] || "未知",
      error: err.message,
    });
    return res.status(400).json({
      error: "无效的表单数据",
      message: "无法解析multipart/form-data请求，请检查表单格式是否正确",
      detail: err.message,
    });
  }

  // 处理内容类型错误
  if (err.status === 415 || (err.message && err.message.includes("content type"))) {
    logMessage("error", `内容类型错误:`, {
      method: req.method,
      path: req.path,
      contentType: req.headers["content-type"] || "未知",
    });
    return res.status(415).json({
      error: "不支持的内容类型",
      message: `服务器无法处理请求的内容类型 ${req.headers["content-type"] || "未指定"}`,
    });
  }

  next(err);
});

// 处理表单数据
server.use(
    express.urlencoded({
      extended: true,
      limit: "5gb",
    })
);

// 处理JSON请求体
server.use(
    express.json({
      type: ["application/json", "application/json; charset=utf-8", "+json", "*/json"],
      limit: "5gb",
    })
);

// 3. WebDAV专用中间件
// ==========================================
// WebDAV请求日志记录
server.use((req, res, next) => {
  // 仅记录关键WebDAV操作，减少不必要的日志
  if (["MKCOL", "COPY", "MOVE", "DELETE", "PUT"].includes(req.method) && (req.path === "/dav" || req.path.startsWith("/dav/"))) {
    logMessage("debug", `关键WebDAV请求: ${req.method} ${req.path}`);
  }

  next();
});

// WebDAV请求日志记录 - 认证由Hono层处理
server.use("/dav", (req, res, next) => {
  // 明确设置允许的方法
  res.setHeader("Allow", webdavConfig.METHODS.join(","));

  // 记录WebDAV请求信息
  logMessage("info", `WebDAV请求: ${req.method} ${req.path}`, {
    contentType: req.headers["content-type"] || "无",
    contentLength: req.headers["content-length"] || "无",
  });

  // 直接传递给下一个中间件，认证由Hono层的webdavAuthMiddleware处理
  next();
});

// 4. 数据库初始化中间件
// ==========================================
server.use(async (req, res, next) => {
  try {
    if (!isDbInitialized) {
      logMessage("info", "首次请求，检查数据库状态...");
      isDbInitialized = true;
      try {
        await sqliteAdapter.init();
        await checkAndInitDatabase(sqliteAdapter);
      } catch (error) {
        logMessage("error", "数据库初始化出错:", { error });
      }
    }

    if (!process.env.ENCRYPTION_SECRET) {
      throw new Error("ENCRYPTION_SECRET 未设置，请在Docker环境变量中配置安全密钥");
    }

    req.env = {
      DB: sqliteAdapter,
      ENCRYPTION_SECRET: process.env.ENCRYPTION_SECRET,
    };

    next();
  } catch (error) {
    logMessage("error", "请求处理中间件错误:", { error });
    res.status(ApiStatus.INTERNAL_ERROR).json(createErrorResponse(error));
  }
});

// ==========================================
// 路由处理
// ==========================================

// 通配符路由 - 处理所有其他API请求
server.use("*", async (req, res) => {
  try {
    const response = await app.fetch(createAdaptedRequest(req), req.env, {});
    await convertWorkerResponseToExpress(response, res);
  } catch (error) {
    // 使用更安全的错误记录和响应生成
    const status = error.status && typeof error.status === "number" ? error.status : ApiStatus.INTERNAL_ERROR;
    res.status(status).json(createErrorResponse(error, status));
  }
});

// ==========================================
// 工具函数
// ==========================================

/**
 * 工具函数：创建适配的Request对象
 * 将Express请求转换为Cloudflare Workers兼容的Request对象
 */
function createAdaptedRequest(expressReq) {
  // 正确处理协议：优先使用X-Forwarded-Proto头部，回退到连接信息
  const protocol = expressReq.headers["x-forwarded-proto"] || (expressReq.connection && expressReq.connection.encrypted ? "https" : "http");

  // 调试日志：记录协议检测结果
  if (expressReq.path.includes("/api/file-")) {
    logMessage("debug", `协议检测 - Path: ${expressReq.path}, X-Forwarded-Proto: ${expressReq.headers["x-forwarded-proto"]}, 最终协议: ${protocol}`);
  }

  const url = new URL(expressReq.originalUrl, `${protocol}://${expressReq.headers.host || "localhost"}`);

  // 获取请求体内容
  let body = undefined;
  if (["POST", "PUT", "PATCH", "PROPFIND", "PROPPATCH", "MKCOL", "COPY", "MOVE", "DELETE"].includes(expressReq.method)) {
    // 检查请求体的类型和内容
    let contentType = expressReq.headers["content-type"] || "";

    // 特殊处理multipart/form-data请求
    if (contentType.includes("multipart/form-data")) {
      if (expressReq.tempFilePath && fs.existsSync(expressReq.tempFilePath)) {
        logMessage("debug", `处理multipart/form-data请求: ${expressReq.path}，使用流式读取临时文件: ${expressReq.tempFilePath}`);
        // 使用流式读取替代一次性加载全部内容到内存
        body = fs.createReadStream(expressReq.tempFilePath);
      } else if (expressReq.rawBody) {
        // 兼容模式 - 如果有rawBody但没有tempFilePath（向后兼容）
        logMessage("debug", `处理multipart/form-data请求: ${expressReq.path}，使用原始请求体（兼容模式），大小: ${expressReq.rawBody.length} 字节`);
        body = expressReq.rawBody;
      }
    }
    // 对于WebDAV请求特殊处理
    else if (expressReq.path.startsWith("/dav")) {
      // 确认Content-Type字段存在，如果不存在则设置一个默认值
      if (!contentType) {
        if (expressReq.method === "MKCOL") {
          // 为MKCOL设置默认的Content-Type
          contentType = "application/octet-stream";
          logMessage("debug", `WebDAV请求: 添加默认Content-Type (${contentType}) 到 ${expressReq.method} 请求`);
        }
      }
    }

    // MKCOL请求特殊处理: 即使有请求体也允许处理
    if (expressReq.method === "MKCOL") {
      // 对于MKCOL，如果有请求体就记录但不严格要求特定格式
      if (expressReq.body) {
        logMessage("debug", `MKCOL请求包含请求体，内容类型: ${contentType}, 请求体类型: ${typeof expressReq.body}`);
        // 对于MKCOL，我们总是设置一个空字符串作为请求体
        // 这样可以避免API处理逻辑中的415错误
        body = "";

        // 安全增强：检查请求体大小，防止DOS攻击
        if (Buffer.isBuffer(expressReq.body) && expressReq.body.length > 1024) {
          logMessage("warn", `MKCOL请求包含异常大的请求体 (${expressReq.body.length} 字节)，可能是客户端错误或恶意请求`);
        }
      }
    }
    // 正常处理其他请求类型
    else if (!body) {
      // 只有在没有设置body的情况下才处理
      // 如果是JSON请求且已经被解析
      if ((contentType.includes("application/json") || contentType.includes("json")) && expressReq.body && typeof expressReq.body === "object") {
        body = JSON.stringify(expressReq.body);
      }
      // 如果是XML或二进制数据，使用Buffer
      else if (
          (contentType.includes("application/xml") || contentType.includes("text/xml") || contentType.includes("application/octet-stream")) &&
          Buffer.isBuffer(expressReq.body)
      ) {
        body = expressReq.body;
      }
      // 针对form-urlencoded类型的处理
      else if (contentType.includes("application/x-www-form-urlencoded") && expressReq.body && typeof expressReq.body === "object") {
        // 将表单数据转换为字符串
        const formData = new URLSearchParams();
        for (const key in expressReq.body) {
          formData.append(key, expressReq.body[key]);
        }
        body = formData.toString();
      }
      // 如果是其他类型的请求体，如果有原始数据就使用
      else if (expressReq.body) {
        if (Buffer.isBuffer(expressReq.body)) {
          body = expressReq.body;
        } else if (typeof expressReq.body === "string") {
          body = expressReq.body;
        } else {
          // 尝试将其他类型转换为字符串
          try {
            body = JSON.stringify(expressReq.body);
          } catch (e) {
            logMessage("warn", `无法将请求体转换为JSON字符串: ${e.message}`);
            body = String(expressReq.body);
          }
        }
      }
    }
  }

  const requestInit = {
    method: expressReq.method,
    headers: expressReq.headers,
  };

  // 只有在有请求体时才添加body参数
  if (body !== undefined) {
    requestInit.body = body;
    // 添加duplex选项以符合Node.js新版本要求
    requestInit.duplex = "half";
  }

  return new Request(url, requestInit);
}

/**
 * 工具函数：将Worker Response转换为Express响应
 * 处理不同类型的响应（JSON、二进制、XML等）
 */
async function convertWorkerResponseToExpress(workerResponse, expressRes) {
  expressRes.status(workerResponse.status);

  workerResponse.headers.forEach((value, key) => {
    expressRes.set(key, value);
  });

  if (workerResponse.body) {
    const contentType = workerResponse.headers.get("content-type") || "";

    // 处理不同类型的响应
    if (contentType.includes("application/json")) {
      // JSON响应
      const jsonData = await workerResponse.json();
      expressRes.json(jsonData);
    } else if (contentType.includes("application/xml") || contentType.includes("text/xml")) {
      // XML响应，常见于WebDAV请求
      const text = await workerResponse.text();
      expressRes.type(contentType).send(text);
    } else if (contentType.includes("text/")) {
      // 文本响应
      const text = await workerResponse.text();
      expressRes.type(contentType).send(text);
    } else {
      // 二进制响应
      const buffer = await workerResponse.arrayBuffer();
      expressRes.send(Buffer.from(buffer));
    }
  } else {
    expressRes.end();
  }
}

// 启动服务器
server.listen(PORT, "0.0.0.0", () => {
  logMessage("info", `CloudPaste后端服务运行在 http://0.0.0.0:${PORT}`);

  // Web.config文件支持WebDAV方法
  try {
    const webConfigPath = path.join(__dirname, "Web.config");
    const webConfigContent = `<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <system.webServer>
    <modules>
      <remove name="WebDAVModule" />
    </modules>
    <handlers>
      <remove name="WebDAV" />
    </handlers>
    <validation validateIntegratedModeConfiguration="false" />
    <security>
      <requestFiltering>
        <verbs>
          <add verb="OPTIONS" allowed="true" />
          <add verb="PROPFIND" allowed="true" />
          <add verb="PROPPATCH" allowed="true" />
          <add verb="MKCOL" allowed="true" />
          <add verb="COPY" allowed="true" />
          <add verb="MOVE" allowed="true" />
          <add verb="DELETE" allowed="true" />
          <add verb="PUT" allowed="true" />
          <add verb="LOCK" allowed="true" />
          <add verb="UNLOCK" allowed="true" />
        </verbs>
      </requestFiltering>
    </security>
  </system.webServer>
</configuration>`;

    fs.writeFileSync(webConfigPath, webConfigContent);
    logMessage("info", `已创建Web.config文件以支持WebDAV方法: ${webConfigPath}`);
  } catch (error) {
    logMessage("warn", "创建Web.config文件失败:", { message: error.message });
  }

  // 启动内存使用监控
  startMemoryMonitoring();
});

/**
 * 内存使用监控和管理函数
 * 定期记录内存使用情况并尝试清理
 */
function startMemoryMonitoring(interval = 1200000) {
  // 简单读取容器内存使用情况
  const getContainerMemory = () => {
    try {
      const fs = require("fs");
      // 尝试读取cgroup内存使用（优先v2，回退v1）
      let usage = null,
          limit = null;

      // cgroup v2
      if (fs.existsSync("/sys/fs/cgroup/memory.current")) {
        usage = parseInt(fs.readFileSync("/sys/fs/cgroup/memory.current", "utf8"));
        const maxContent = fs.readFileSync("/sys/fs/cgroup/memory.max", "utf8").trim();
        if (maxContent !== "max") limit = parseInt(maxContent);
      }
      // cgroup v1
      else if (fs.existsSync("/sys/fs/cgroup/memory/memory.usage_in_bytes")) {
        usage = parseInt(fs.readFileSync("/sys/fs/cgroup/memory/memory.usage_in_bytes", "utf8"));
        const limitValue = parseInt(fs.readFileSync("/sys/fs/cgroup/memory/memory.limit_in_bytes", "utf8"));
        if (limitValue < Number.MAX_SAFE_INTEGER) limit = limitValue;
      }

      return usage && limit ? { usage, limit } : null;
    } catch (error) {
      return null; // 静默失败，不在容器中或无权限
    }
  };

  // 记录内存使用情况
  const logMemoryUsage = () => {
    const memUsage = process.memoryUsage();
    const containerMem = getContainerMemory();

    const memoryInfo = {
      rss: `${Math.round(memUsage.rss / 1024 / 1024)} MB`, // 常驻集大小
      heapTotal: `${Math.round(memUsage.heapTotal / 1024 / 1024)} MB`, // 总堆内存
      heapUsed: `${Math.round(memUsage.heapUsed / 1024 / 1024)} MB`, // 已用堆内存
      external: `${Math.round(memUsage.external / 1024 / 1024)} MB`, // 外部内存
      arrayBuffers: memUsage.arrayBuffers ? `${Math.round(memUsage.arrayBuffers / 1024 / 1024)} MB` : "N/A", // Buffer内存
    };

    // 如果在容器中，添加容器内存信息
    if (containerMem) {
      memoryInfo.container = `${Math.round(containerMem.usage / 1024 / 1024)} MB / ${Math.round(containerMem.limit / 1024 / 1024)} MB`;
      memoryInfo.containerUsage = `${Math.round((containerMem.usage / containerMem.limit) * 100)}%`;
    }

    logMessage("info", "内存使用情况:", memoryInfo);

    // 智能垃圾回收：优先使用容器内存，回退到进程内存
    let shouldGC = false;
    if (containerMem) {
      // 容器内存使用率超过85%
      shouldGC = containerMem.usage / containerMem.limit > 0.85;
    } else {
      // 回退到原有逻辑
      shouldGC = memUsage.heapUsed / memUsage.heapTotal > 0.85 || memUsage.external > 50 * 1024 * 1024 || (memUsage.arrayBuffers && memUsage.arrayBuffers > 50 * 1024 * 1024);
    }

    if (global.gc && shouldGC) {
      logMessage("info", "检测到内存使用较高，尝试手动垃圾回收");
      global.gc();
    }
  };

  // 初始记录
  logMemoryUsage();

  // 设置定时器定期记录
  const intervalId = setInterval(() => {
    logMemoryUsage();
  }, interval);

  // 防止定时器阻止进程退出
  process.on("exit", () => {
    clearInterval(intervalId);
  });

  return {
    stop: () => clearInterval(intervalId),
    logNow: () => logMemoryUsage(),
  };
}
