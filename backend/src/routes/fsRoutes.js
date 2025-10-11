/**
 * 统一文件系统API路由
 * 统一 /api/fs/* 路由，内部处理管理员和API密钥用户认证
 */
import { Hono } from "hono";
import { authGateway } from "../middlewares/authGatewayMiddleware.js";
import { createErrorResponse, generateFileId } from "../utils/common.js";
import { ApiStatus } from "../constants/index.js";
import { HTTPException } from "hono/http-exception";
import { MountManager } from "../storage/managers/MountManager.js";
import { FileSystem } from "../storage/fs/FileSystem.js";
import { getMimeTypeFromFilename } from "../utils/fileUtils.js";
import { clearDirectoryCache } from "../cache/index.js";
import { getS3ConfigByIdForAdmin, getPublicS3ConfigById } from "../services/s3ConfigService.js";
import { getVirtualDirectoryListing, isVirtualPath } from "../storage/fs/utils/VirtualDirectory.js";
import { RepositoryFactory } from "../repositories/index.js";

// 创建文件系统路由处理程序
const fsRoutes = new Hono();

/**
 * 设置CORS标头
 * @param {HonoContext} c - Hono上下文
 */
function setCorsHeaders(c) {
  // 获取请求的origin并返回相同的值作为Access-Control-Allow-Origin
  // 这是为了支持credentials的情况下正确处理CORS
  const origin = c.req.header("Origin");
  c.header("Access-Control-Allow-Origin", origin || "*");

  c.header("Access-Control-Allow-Headers", "Content-Type, Content-Length, Authorization, X-Requested-With, Range");
  c.header("Access-Control-Expose-Headers", "ETag, Content-Length, Content-Disposition");
  c.header("Access-Control-Allow-Credentials", "true");
  c.header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS");

  // 对于预览和下载请求，添加适当的缓存时间
  if (c.req.path.includes("/preview") || c.req.path.includes("/download")) {
    c.header("Access-Control-Max-Age", "3600"); // 1小时
  }
}

/**
 * 基本路径权限检查
 */
function checkBasicPathPermission(basicPath, requestPath) {
  if (!basicPath || !requestPath) {
    return false;
  }

  // 标准化路径
  const normalizeBasicPath = basicPath === "/" ? "/" : basicPath.replace(/\/+$/, "");
  const normalizeRequestPath = requestPath.replace(/\/+$/, "") || "/";

  // 如果基本路径是根路径，允许所有访问
  if (normalizeBasicPath === "/") {
    return true;
  }

  // 检查请求路径是否在基本路径范围内
  return normalizeRequestPath === normalizeBasicPath || normalizeRequestPath.startsWith(normalizeBasicPath + "/");
}

/**
 * 统一的文件系统认证中间件
 * 处理管理员和API密钥用户的认证，并进行路径权限检查
 */
const unifiedFsAuthMiddleware = async (c, next) => {
  const authResult = authGateway.utils.getAuthResult(c);

  if (!authResult || !authResult.isAuthenticated) {
    throw new HTTPException(ApiStatus.UNAUTHORIZED, { message: "需要认证访问" });
  }

  if (authResult.isAdmin()) {
    // 管理员用户
    c.set("userInfo", {
      type: "admin",
      id: authResult.getUserId(),
      hasFullAccess: true,
    });
  } else {
    // API密钥用户 - 进行路径权限检查
    const apiKeyInfo = authResult.keyInfo;

    // 对于需要路径的操作，进行权限检查
    // 注意：这里我们需要延迟到路由处理器中进行路径检查，因为路径可能在请求体中
    c.set("userInfo", {
      type: "apiKey",
      info: apiKeyInfo,
      hasFullAccess: false,
    });
  }

  await next();
};

/**
 * 统一的路径权限检查函数
 * @param {Object} userInfo - 用户信息对象
 * @param {string|Array<string>} paths - 要检查的路径（单个路径或路径数组）
 * @param {string} permissionType - 权限类型 ('operation' 或 'navigation')
 * @param {Object} c - Hono上下文（用于获取认证服务）
 * @returns {boolean} 是否有权限
 */
const checkPathPermission = (userInfo, paths, permissionType = "operation", c = null) => {
  if (userInfo.hasFullAccess) {
    return true; // 管理员拥有所有权限
  }

  // 支持单个路径或路径数组
  const pathsToCheck = Array.isArray(paths) ? paths : [paths];

  for (const path of pathsToCheck) {
    let hasPermission = false;

    if (permissionType === "navigation") {
      // 导航权限检查
      hasPermission = authGateway.utils.checkPathPermissionForNavigation(userInfo.info.basicPath, path);
    } else if (c) {
      // 操作权限检查（有上下文）
      hasPermission = authGateway.utils.checkPathPermissionForOperation(c, userInfo.info.basicPath, path);
    } else {
      // 基本权限检查（无上下文）
      hasPermission = checkBasicPathPermission(userInfo.info.basicPath, path);
    }

    if (!hasPermission) {
      return false;
    }
  }

  return true;
};

/**
 * 获取服务层调用参数
 * @param {Object} userInfo - 用户信息对象
 * @returns {Object} 服务层参数
 */
const getServiceParams = (userInfo) => {
  if (userInfo.type === "admin") {
    return { userIdOrInfo: userInfo.id, userType: "admin" };
  } else {
    return { userIdOrInfo: userInfo.info, userType: "apiKey" };
  }
};

/**
 * 获取创建者标识
 * @param {Object} userInfo - 用户信息对象
 * @returns {string} 创建者标识
 */
const getCreatedBy = (userInfo) => {
  if (userInfo.type === "admin") {
    return userInfo.id;
  } else {
    return `apikey:${userInfo.info.id}`;
  }
};

/**
 * 根据用户类型获取S3配置
 * @param {D1Database} db - 数据库实例
 * @param {string} configId - 配置ID
 * @param {string|Object} userIdOrInfo - 用户ID或API密钥信息
 * @param {string} userType - 用户类型
 * @param {string} encryptionSecret - 加密密钥
 * @returns {Promise<Object>} S3配置对象
 */
const getS3ConfigByUserType = async (db, configId, userIdOrInfo, userType, encryptionSecret) => {
  if (userType === "admin") {
    return await getS3ConfigByIdForAdmin(db, configId, userIdOrInfo);
  } else {
    return await getPublicS3ConfigById(db, configId);
  }
};

// ================ OPTIONS 请求处理 ================

// 处理预览和下载接口的OPTIONS请求
fsRoutes.options("/api/fs/preview", (c) => {
  setCorsHeaders(c);
  return c.text("", 204); // No Content
});

fsRoutes.options("/api/fs/download", (c) => {
  setCorsHeaders(c);
  return c.text("", 204); // No Content
});

// OPTIONS处理 - 分片上传相关，专门处理预检请求
fsRoutes.options("/api/fs/multipart/:action", (c) => {
  setCorsHeaders(c);
  c.header("Access-Control-Allow-Methods", "OPTIONS, POST");
  c.header("Access-Control-Max-Age", "86400");
  return c.text("", 204);
});

// 专门处理OPTIONS请求 - 分片上传
fsRoutes.options("/api/fs/multipart/part", (c) => {
  setCorsHeaders(c);
  c.header("Access-Control-Allow-Methods", "OPTIONS, POST");
  c.header("Access-Control-Max-Age", "86400"); // 24小时缓存预检响应
  return c.text("", 204); // No Content
});

// ================ 基础文件系统操作 ================

// 列出目录内容 - 需要挂载页查看权限
fsRoutes.get("/api/fs/list", authGateway.requireMount(), unifiedFsAuthMiddleware, async (c) => {
  const db = c.env.DB;
  const path = c.req.query("path") || "/";
  const refresh = c.req.query("refresh") === "true";
  const userInfo = c.get("userInfo");
  const { userIdOrInfo, userType } = getServiceParams(userInfo);
  const encryptionSecret = c.env.ENCRYPTION_SECRET;

  // 调试日志：记录refresh参数
  if (refresh) {
    console.log("[后端路由] 收到强制刷新请求:", { path, refresh });
  }

  try {
    // 对于API密钥用户，检查请求路径是否在基本路径权限范围内
    if (userType === "apiKey") {
      // 特殊处理：允许访问从根路径到基本路径的所有父级路径，以便用户能够导航
      if (!checkPathPermission(userInfo, path, "navigation")) {
        throw new HTTPException(ApiStatus.FORBIDDEN, { message: "没有权限访问此路径" });
      }
    }

    // 获取用户可访问的挂载点列表 - 使用统一的挂载点获取方法
    const mounts = await authGateway.utils.getAccessibleMounts(db, userIdOrInfo, userType);

    // 检查是否为虚拟路径（根路径或中间虚拟目录）
    if (isVirtualPath(path, mounts)) {
      // 处理虚拟目录，返回挂载点列表
      const basicPath = userType === "apiKey" ? userIdOrInfo.basicPath : null;
      const result = await getVirtualDirectoryListing(mounts, path, basicPath);

      return c.json({
        code: ApiStatus.SUCCESS,
        message: "获取目录列表成功",
        data: result,
        success: true,
      });
    }

    // 处理实际挂载点路径，使用FileSystem抽象层
    const mountManager = new MountManager(db, encryptionSecret);
    const fileSystem = new FileSystem(mountManager);

    // 调用FileSystem的listDirectory方法，传递refresh选项
    const result = await fileSystem.listDirectory(path, userIdOrInfo, userType, { refresh });

    return c.json({
      code: ApiStatus.SUCCESS,
      message: "获取目录列表成功",
      data: result,
      success: true,
    });
  } catch (error) {
    console.error("获取目录列表错误:", error);
    if (error instanceof HTTPException) {
      return c.json(createErrorResponse(error.status, error.message), error.status);
    }
    return c.json(createErrorResponse(ApiStatus.INTERNAL_ERROR, error.message || "获取目录列表失败"), ApiStatus.INTERNAL_ERROR);
  }
});

// 获取文件信息 - 需要挂载页查看权限
fsRoutes.get("/api/fs/get", authGateway.requireMount(), unifiedFsAuthMiddleware, async (c) => {
  const db = c.env.DB;
  const path = c.req.query("path");
  const userInfo = c.get("userInfo");
  const { userIdOrInfo, userType } = getServiceParams(userInfo);
  const encryptionSecret = c.env.ENCRYPTION_SECRET;

  if (!path) {
    return c.json(createErrorResponse(ApiStatus.BAD_REQUEST, "请提供文件路径"), ApiStatus.BAD_REQUEST);
  }

  // 检查路径权限（仅对API密钥用户）
  if (!checkPathPermission(userInfo, path)) {
    return c.json(createErrorResponse(ApiStatus.FORBIDDEN, "没有权限访问此路径"), ApiStatus.FORBIDDEN);
  }

  try {
    // 使用FileSystem抽象层
    const mountManager = new MountManager(db, encryptionSecret);
    const fileSystem = new FileSystem(mountManager);

    // 调用FileSystem的getFileInfo方法
    const result = await fileSystem.getFileInfo(path, userIdOrInfo, userType, c.req.raw);

    return c.json({
      code: ApiStatus.SUCCESS,
      message: "获取文件信息成功",
      data: result,
      success: true,
    });
  } catch (error) {
    console.error("获取文件信息错误:", error);
    if (error instanceof HTTPException) {
      return c.json(createErrorResponse(error.status, error.message), error.status);
    }
    return c.json(createErrorResponse(ApiStatus.INTERNAL_ERROR, error.message || "获取文件信息失败"), ApiStatus.INTERNAL_ERROR);
  }
});

// 下载文件 - 需要挂载页查看权限
fsRoutes.get("/api/fs/download", authGateway.requireMount(), unifiedFsAuthMiddleware, async (c) => {
  const db = c.env.DB;
  const path = c.req.query("path");
  const userInfo = c.get("userInfo");
  const { userIdOrInfo, userType } = getServiceParams(userInfo);
  const encryptionSecret = c.env.ENCRYPTION_SECRET;

  if (!path) {
    setCorsHeaders(c);
    return c.json(createErrorResponse(ApiStatus.BAD_REQUEST, "请提供文件路径"), ApiStatus.BAD_REQUEST);
  }

  // 检查路径权限（仅对API密钥用户）
  if (!checkPathPermission(userInfo, path)) {
    return c.json(createErrorResponse(ApiStatus.FORBIDDEN, "没有权限访问此路径"), ApiStatus.FORBIDDEN);
  }

  try {
    // 使用FileSystem抽象层
    const mountManager = new MountManager(db, encryptionSecret);
    const fileSystem = new FileSystem(mountManager);

    // 调用FileSystem的downloadFile方法
    const response = await fileSystem.downloadFile(path, null, c.req.raw, userIdOrInfo, userType);

    setCorsHeaders(c);
    return response;
  } catch (error) {
    console.error("下载文件错误:", error);
    setCorsHeaders(c);
    if (error instanceof HTTPException) {
      return c.json(createErrorResponse(error.status, error.message), error.status);
    }
    return c.json(createErrorResponse(ApiStatus.INTERNAL_ERROR, error.message || "下载文件失败"), ApiStatus.INTERNAL_ERROR);
  }
});

// 创建目录 - 需要挂载页上传权限
fsRoutes.post("/api/fs/mkdir", authGateway.requireMountUpload(), unifiedFsAuthMiddleware, async (c) => {
  const db = c.env.DB;
  const userInfo = c.get("userInfo");
  const { userIdOrInfo, userType } = getServiceParams(userInfo);
  const encryptionSecret = c.env.ENCRYPTION_SECRET;
  const body = await c.req.json();
  const path = body.path;

  if (!path) {
    return c.json(createErrorResponse(ApiStatus.BAD_REQUEST, "请提供目录路径"), ApiStatus.BAD_REQUEST);
  }

  // 检查路径权限（仅对API密钥用户）
  if (!checkPathPermission(userInfo, path)) {
    return c.json(createErrorResponse(ApiStatus.FORBIDDEN, "没有权限在此路径创建目录"), ApiStatus.FORBIDDEN);
  }

  try {
    // 使用FileSystem抽象层
    const mountManager = new MountManager(db, encryptionSecret);
    const fileSystem = new FileSystem(mountManager);

    // 调用FileSystem的createDirectory方法
    await fileSystem.createDirectory(path, userIdOrInfo, userType);

    return c.json({
      code: ApiStatus.SUCCESS,
      message: "目录创建成功",
      success: true,
    });
  } catch (error) {
    console.error("创建目录错误:", error);
    if (error instanceof HTTPException) {
      return c.json(createErrorResponse(error.status, error.message), error.status);
    }
    return c.json(createErrorResponse(ApiStatus.INTERNAL_ERROR, error.message || "创建目录失败"), ApiStatus.INTERNAL_ERROR);
  }
});

// 上传文件 - 需要挂载页上传权限
fsRoutes.post("/api/fs/upload", authGateway.requireMountUpload(), unifiedFsAuthMiddleware, async (c) => {
  const db = c.env.DB;
  const userInfo = c.get("userInfo");
  const { userIdOrInfo, userType } = getServiceParams(userInfo);
  const encryptionSecret = c.env.ENCRYPTION_SECRET;

  try {
    const formData = await c.req.formData();
    const file = formData.get("file");
    const path = formData.get("path");
    const useMultipart = formData.get("use_multipart") === "true";

    if (!file || !path) {
      return c.json(createErrorResponse(ApiStatus.BAD_REQUEST, "请提供文件和路径"), ApiStatus.BAD_REQUEST);
    }

    // 检查路径权限（仅对API密钥用户）
    if (!checkPathPermission(userInfo, path)) {
      return c.json(createErrorResponse(ApiStatus.FORBIDDEN, "没有权限在此路径上传文件"), ApiStatus.FORBIDDEN);
    }

    // 使用FileSystem抽象层
    const mountManager = new MountManager(db, encryptionSecret);
    const fileSystem = new FileSystem(mountManager);

    // 调用FileSystem的uploadFile方法
    const result = await fileSystem.uploadFile(path, file, userIdOrInfo, userType, {
      useMultipart,
    });

    // 如果是分片上传，返回相关信息
    if (result.useMultipart) {
      return c.json({
        code: ApiStatus.SUCCESS,
        message: "需要使用分片上传",
        data: result,
        success: true,
      });
    }

    // 常规上传成功
    return c.json({
      code: ApiStatus.SUCCESS,
      message: "文件上传成功",
      data: result,
      success: true,
    });
  } catch (error) {
    console.error("上传文件错误:", error);
    if (error instanceof HTTPException) {
      return c.json(createErrorResponse(error.status, error.message), error.status);
    }
    return c.json(createErrorResponse(ApiStatus.INTERNAL_ERROR, error.message || "上传文件失败"), ApiStatus.INTERNAL_ERROR);
  }
});

// 重命名文件或目录 - 需要挂载页重命名权限
fsRoutes.post("/api/fs/rename", authGateway.requireMountRename(), unifiedFsAuthMiddleware, async (c) => {
  const db = c.env.DB;
  const userInfo = c.get("userInfo");
  const { userIdOrInfo, userType } = getServiceParams(userInfo);
  const encryptionSecret = c.env.ENCRYPTION_SECRET;
  const body = await c.req.json();
  const oldPath = body.oldPath;
  const newPath = body.newPath;

  if (!oldPath || !newPath) {
    return c.json(createErrorResponse(ApiStatus.BAD_REQUEST, "请提供原路径和新路径"), ApiStatus.BAD_REQUEST);
  }

  // 检查路径权限（仅对API密钥用户）
  if (!checkPathPermission(userInfo, oldPath) || !checkPathPermission(userInfo, newPath)) {
    return c.json(createErrorResponse(ApiStatus.FORBIDDEN, "没有权限重命名此路径的文件"), ApiStatus.FORBIDDEN);
  }

  try {
    // 使用FileSystem抽象层
    const mountManager = new MountManager(db, encryptionSecret);
    const fileSystem = new FileSystem(mountManager);

    // 调用FileSystem的renameItem方法
    await fileSystem.renameItem(oldPath, newPath, userIdOrInfo, userType);

    // 清理缓存 - 需要获取mount信息
    try {
      const { mount } = await mountManager.getDriverByPath(oldPath, userIdOrInfo, userType);
      const { mount: newMount } = await mountManager.getDriverByPath(newPath, userIdOrInfo, userType);

      await clearDirectoryCache({ mountId: mount.id });
      if (newMount.id !== mount.id) {
        await clearDirectoryCache({ mountId: newMount.id });
      }
      console.log(`重命名操作完成后缓存已刷新：${oldPath} -> ${newPath}`);
    } catch (cacheError) {
      console.warn(`执行缓存清理时出错: ${cacheError.message}`);
    }

    return c.json({
      code: ApiStatus.SUCCESS,
      message: "重命名成功",
      success: true,
    });
  } catch (error) {
    console.error("重命名错误:", error);
    if (error instanceof HTTPException) {
      return c.json(createErrorResponse(error.status, error.message), error.status);
    }
    return c.json(createErrorResponse(ApiStatus.INTERNAL_ERROR, error.message || "重命名失败"), ApiStatus.INTERNAL_ERROR);
  }
});

// 批量删除文件或目录 - 需要挂载页删除权限
fsRoutes.delete("/api/fs/batch-remove", authGateway.requireMountDelete(), unifiedFsAuthMiddleware, async (c) => {
  const db = c.env.DB;
  const userInfo = c.get("userInfo");
  const { userIdOrInfo, userType } = getServiceParams(userInfo);
  const encryptionSecret = c.env.ENCRYPTION_SECRET;
  const body = await c.req.json();
  const paths = body.paths;

  if (!paths || !Array.isArray(paths) || paths.length === 0) {
    return c.json(createErrorResponse(ApiStatus.BAD_REQUEST, "请提供有效的路径数组"), ApiStatus.BAD_REQUEST);
  }

  // 检查所有路径的操作权限（仅对API密钥用户）
  if (!userInfo.hasFullAccess) {
    for (const path of paths) {
      if (!checkPathPermission(userInfo, path)) {
        return c.json(createErrorResponse(ApiStatus.FORBIDDEN, `没有权限删除路径: ${path}`), ApiStatus.FORBIDDEN);
      }
    }
  }

  try {
    // 使用FileSystem抽象层处理批量删除
    const mountManager = new MountManager(db, encryptionSecret);
    const fileSystem = new FileSystem(mountManager);

    // 调用FileSystem的batchRemoveItems方法
    const result = await fileSystem.batchRemoveItems(paths, userIdOrInfo, userType);

    // 收集所有挂载点ID用于缓存清理
    const mountIds = new Set();
    for (const path of paths) {
      try {
        const { mount } = await mountManager.getDriverByPath(path, userIdOrInfo, userType);
        mountIds.add(mount.id);
      } catch (error) {
        console.warn(`获取路径挂载信息失败: ${path}`, error);
      }
    }

    // 清理缓存
    try {
      for (const mountId of mountIds) {
        await clearDirectoryCache({ mountId });
      }
      console.log(`批量删除操作完成后缓存已刷新：${mountIds.size} 个挂载点`);
    } catch (cacheError) {
      console.warn(`执行缓存清理时出错: ${cacheError.message}`);
    }

    return c.json({
      code: ApiStatus.SUCCESS,
      message: "批量删除完成",
      data: result,
      success: true,
    });
  } catch (error) {
    console.error("批量删除错误:", error);
    if (error instanceof HTTPException) {
      return c.json(createErrorResponse(error.status, error.message), error.status);
    }
    return c.json(createErrorResponse(ApiStatus.INTERNAL_ERROR, error.message || "批量删除失败"), ApiStatus.INTERNAL_ERROR);
  }
});

// 获取文件直链(预签名URL) - 需要挂载页查看权限
fsRoutes.get("/api/fs/file-link", authGateway.requireMount(), unifiedFsAuthMiddleware, async (c) => {
  const db = c.env.DB;
  const path = c.req.query("path");
  const userInfo = c.get("userInfo");
  const { userIdOrInfo, userType } = getServiceParams(userInfo);
  const encryptionSecret = c.env.ENCRYPTION_SECRET;
  // 如果前端传入null或空值，则使用S3配置的默认签名时间，否则使用传入的值
  const expiresInParam = c.req.query("expires_in");
  const expiresIn = expiresInParam && expiresInParam !== "null" ? parseInt(expiresInParam) : null;
  const forceDownload = c.req.query("force_download") === "true";

  if (!path) {
    return c.json(createErrorResponse(ApiStatus.BAD_REQUEST, "请提供文件路径"), ApiStatus.BAD_REQUEST);
  }

  try {
    // 使用FileSystem抽象层
    const mountManager = new MountManager(db, encryptionSecret);
    const fileSystem = new FileSystem(mountManager);

    // 调用FileSystem的generatePresignedUrl方法
    const result = await fileSystem.generatePresignedUrl(path, userIdOrInfo, userType, {
      operation: "download",
      userType,
      userId: userType === "admin" ? userIdOrInfo : userIdOrInfo.id,
      expiresIn,
      forceDownload,
    });

    return c.json({
      code: ApiStatus.SUCCESS,
      message: "获取文件直链成功",
      data: result,
      success: true,
    });
  } catch (error) {
    console.error("获取文件直链错误:", error);
    if (error instanceof HTTPException) {
      return c.json(createErrorResponse(error.status, error.message), error.status);
    }
    return c.json(createErrorResponse(ApiStatus.INTERNAL_ERROR, error.message || "获取文件直链失败"), ApiStatus.INTERNAL_ERROR);
  }
});

// ================ 前端分片上传相关路由 ================

// ================ 前端分片上传相关路由 ================

// 初始化前端分片上传（生成预签名URL列表） - 需要挂载页上传权限
fsRoutes.post("/api/fs/multipart/init", authGateway.requireMountUpload(), unifiedFsAuthMiddleware, async (c) => {
  try {
    setCorsHeaders(c);

    // 获取数据库和加密密钥
    const db = c.env.DB;
    const encryptionSecret = c.env.ENCRYPTION_SECRET;

    // 从上下文获取用户信息
    const userInfo = c.get("userInfo");
    if (!userInfo) {
      return c.json(createErrorResponse(ApiStatus.UNAUTHORIZED, "未授权访问"), ApiStatus.UNAUTHORIZED);
    }

    const userIdOrInfo = userInfo.type === "admin" ? userInfo.id : userInfo.info;
    const userType = userInfo.type === "admin" ? "admin" : "apiKey";

    // 获取请求参数
    const body = await c.req.json();
    const { path, fileName, fileSize, partSize = 5 * 1024 * 1024, partCount } = body;

    if (!path || !fileName) {
      return c.json(createErrorResponse(ApiStatus.BAD_REQUEST, "缺少必要参数"), ApiStatus.BAD_REQUEST);
    }

    // 使用FileSystem抽象层
    const mountManager = new MountManager(db, encryptionSecret);
    const fileSystem = new FileSystem(mountManager);

    // 调用FileSystem的initializeFrontendMultipartUpload方法
    const result = await fileSystem.initializeFrontendMultipartUpload(path, fileName, fileSize, userIdOrInfo, userType, partSize, partCount);

    return c.json({
      code: ApiStatus.SUCCESS,
      message: "前端分片上传初始化成功",
      data: result,
      success: true,
    });
  } catch (error) {
    console.error("初始化前端分片上传错误:", error);
    setCorsHeaders(c);
    if (error instanceof HTTPException) {
      return c.json(createErrorResponse(error.status, error.message), error.status);
    }
    return c.json(createErrorResponse(ApiStatus.INTERNAL_ERROR, error.message || "初始化前端分片上传失败"), ApiStatus.INTERNAL_ERROR);
  }
});

// 完成前端分片上传 - 需要挂载页上传权限
fsRoutes.post("/api/fs/multipart/complete", authGateway.requireMountUpload(), unifiedFsAuthMiddleware, async (c) => {
  try {
    setCorsHeaders(c);

    // 获取数据库和加密密钥
    const db = c.env.DB;
    const encryptionSecret = c.env.ENCRYPTION_SECRET;

    // 从上下文获取用户信息
    const userInfo = c.get("userInfo");
    if (!userInfo) {
      return c.json(createErrorResponse(ApiStatus.UNAUTHORIZED, "未授权访问"), ApiStatus.UNAUTHORIZED);
    }

    const userIdOrInfo = userInfo.type === "admin" ? userInfo.id : userInfo.info;
    const userType = userInfo.type === "admin" ? "admin" : "apiKey";

    // 获取请求参数
    const body = await c.req.json();
    const { path, uploadId, parts, fileName, fileSize } = body;

    if (!path || !uploadId || !Array.isArray(parts) || parts.length === 0) {
      return c.json(createErrorResponse(ApiStatus.BAD_REQUEST, "缺少必要参数"), ApiStatus.BAD_REQUEST);
    }

    // 使用FileSystem抽象层
    const mountManager = new MountManager(db, encryptionSecret);
    const fileSystem = new FileSystem(mountManager);

    // 调用FileSystem的completeFrontendMultipartUpload方法
    const result = await fileSystem.completeFrontendMultipartUpload(path, uploadId, parts, fileName, fileSize, userIdOrInfo, userType);

    return c.json({
      code: ApiStatus.SUCCESS,
      message: "前端分片上传完成",
      data: result,
      success: true,
    });
  } catch (error) {
    console.error("完成前端分片上传错误:", error);
    setCorsHeaders(c);
    if (error instanceof HTTPException) {
      return c.json(createErrorResponse(error.status, error.message), error.status);
    }
    return c.json(createErrorResponse(ApiStatus.INTERNAL_ERROR, error.message || "完成前端分片上传失败"), ApiStatus.INTERNAL_ERROR);
  }
});

// 中止前端分片上传 - 需要挂载页上传权限
fsRoutes.post("/api/fs/multipart/abort", authGateway.requireMountUpload(), unifiedFsAuthMiddleware, async (c) => {
  try {
    setCorsHeaders(c);

    // 获取数据库和加密密钥
    const db = c.env.DB;
    const encryptionSecret = c.env.ENCRYPTION_SECRET;

    // 从上下文获取用户信息
    const userInfo = c.get("userInfo");
    if (!userInfo) {
      return c.json(createErrorResponse(ApiStatus.UNAUTHORIZED, "未授权访问"), ApiStatus.UNAUTHORIZED);
    }

    const userIdOrInfo = userInfo.type === "admin" ? userInfo.id : userInfo.info;
    const userType = userInfo.type === "admin" ? "admin" : "apiKey";

    // 获取请求参数
    const body = await c.req.json();
    const { path, uploadId, fileName } = body;

    if (!path || !uploadId || !fileName) {
      return c.json(createErrorResponse(ApiStatus.BAD_REQUEST, "缺少必要参数"), ApiStatus.BAD_REQUEST);
    }

    // 使用FileSystem抽象层
    const mountManager = new MountManager(db, encryptionSecret);
    const fileSystem = new FileSystem(mountManager);

    // 调用FileSystem的abortFrontendMultipartUpload方法
    const result = await fileSystem.abortFrontendMultipartUpload(path, uploadId, fileName, userIdOrInfo, userType);

    return c.json({
      code: ApiStatus.SUCCESS,
      message: "前端分片上传已中止",
      data: result,
      success: true,
    });
  } catch (error) {
    console.error("中止前端分片上传错误:", error);
    setCorsHeaders(c);
    if (error instanceof HTTPException) {
      return c.json(createErrorResponse(error.status, error.message), error.status);
    }
    return c.json(createErrorResponse(ApiStatus.INTERNAL_ERROR, error.message || "中止前端分片上传失败"), ApiStatus.INTERNAL_ERROR);
  }
});

// ===============断点续传==========================

// 列出进行中的分片上传 - 需要挂载页上传权限
fsRoutes.post("/api/fs/multipart/list-uploads", authGateway.requireMountUpload(), unifiedFsAuthMiddleware, async (c) => {
  try {
    setCorsHeaders(c);

    // 获取数据库和加密密钥
    const db = c.env.DB;
    const encryptionSecret = c.env.ENCRYPTION_SECRET;

    // 从上下文获取用户信息
    const userInfo = c.get("userInfo");
    if (!userInfo) {
      return c.json(createErrorResponse(ApiStatus.UNAUTHORIZED, "未授权访问"), ApiStatus.UNAUTHORIZED);
    }

    const userIdOrInfo = userInfo.type === "admin" ? userInfo.id : userInfo.info;
    const userType = userInfo.type === "admin" ? "admin" : "apiKey";

    // 获取请求参数
    const body = await c.req.json();
    const { path = "" } = body;

    // 使用FileSystem抽象层
    const mountManager = new MountManager(db, encryptionSecret);
    const fileSystem = new FileSystem(mountManager);

    // 调用FileSystem的listMultipartUploads方法
    const result = await fileSystem.listMultipartUploads(path, userIdOrInfo, userType);

    return c.json({
      code: ApiStatus.SUCCESS,
      message: "列出进行中的分片上传成功",
      data: result,
      success: true,
    });
  } catch (error) {
    console.error("列出进行中的分片上传错误:", error);
    setCorsHeaders(c);
    if (error instanceof HTTPException) {
      return c.json(createErrorResponse(error.status, error.message), error.status);
    }
    return c.json(createErrorResponse(ApiStatus.INTERNAL_ERROR, error.message || "列出进行中的分片上传失败"), ApiStatus.INTERNAL_ERROR);
  }
});

// 列出已上传的分片 - 需要挂载页上传权限
fsRoutes.post("/api/fs/multipart/list-parts", authGateway.requireMountUpload(), unifiedFsAuthMiddleware, async (c) => {
  try {
    setCorsHeaders(c);

    // 获取数据库和加密密钥
    const db = c.env.DB;
    const encryptionSecret = c.env.ENCRYPTION_SECRET;

    // 从上下文获取用户信息
    const userInfo = c.get("userInfo");
    if (!userInfo) {
      return c.json(createErrorResponse(ApiStatus.UNAUTHORIZED, "未授权访问"), ApiStatus.UNAUTHORIZED);
    }

    const userIdOrInfo = userInfo.type === "admin" ? userInfo.id : userInfo.info;
    const userType = userInfo.type === "admin" ? "admin" : "apiKey";

    // 获取请求参数
    const body = await c.req.json();
    const { path, uploadId, fileName } = body;

    if (!path || !uploadId || !fileName) {
      return c.json(createErrorResponse(ApiStatus.BAD_REQUEST, "缺少必要参数"), ApiStatus.BAD_REQUEST);
    }

    // 使用FileSystem抽象层
    const mountManager = new MountManager(db, encryptionSecret);
    const fileSystem = new FileSystem(mountManager);

    // 调用FileSystem的listMultipartParts方法
    const result = await fileSystem.listMultipartParts(path, uploadId, fileName, userIdOrInfo, userType);

    return c.json({
      code: ApiStatus.SUCCESS,
      message: "列出已上传的分片成功",
      data: result,
      success: true,
    });
  } catch (error) {
    console.error("列出已上传的分片错误:", error);
    setCorsHeaders(c);
    if (error instanceof HTTPException) {
      return c.json(createErrorResponse(error.status, error.message), error.status);
    }
    return c.json(createErrorResponse(ApiStatus.INTERNAL_ERROR, error.message || "列出已上传的分片失败"), ApiStatus.INTERNAL_ERROR);
  }
});

// 为现有上传刷新预签名URL - 需要挂载页上传权限
fsRoutes.post("/api/fs/multipart/refresh-urls", authGateway.requireMountUpload(), unifiedFsAuthMiddleware, async (c) => {
  try {
    setCorsHeaders(c);

    // 获取数据库和加密密钥
    const db = c.env.DB;
    const encryptionSecret = c.env.ENCRYPTION_SECRET;

    // 从上下文获取用户信息
    const userInfo = c.get("userInfo");
    if (!userInfo) {
      return c.json(createErrorResponse(ApiStatus.UNAUTHORIZED, "未授权访问"), ApiStatus.UNAUTHORIZED);
    }

    const userIdOrInfo = userInfo.type === "admin" ? userInfo.id : userInfo.info;
    const userType = userInfo.type === "admin" ? "admin" : "apiKey";

    // 获取请求参数
    const body = await c.req.json();
    const { path, uploadId, partNumbers } = body;

    if (!path || !uploadId || !Array.isArray(partNumbers) || partNumbers.length === 0) {
      return c.json(createErrorResponse(ApiStatus.BAD_REQUEST, "缺少必要参数"), ApiStatus.BAD_REQUEST);
    }

    // 使用FileSystem抽象层
    const mountManager = new MountManager(db, encryptionSecret);
    const fileSystem = new FileSystem(mountManager);

    // 调用FileSystem的refreshMultipartUrls方法
    const result = await fileSystem.refreshMultipartUrls(path, uploadId, partNumbers, userIdOrInfo, userType);

    return c.json({
      code: ApiStatus.SUCCESS,
      message: "刷新分片上传预签名URL成功",
      data: result,
      success: true,
    });
  } catch (error) {
    console.error("刷新分片上传预签名URL错误:", error);
    setCorsHeaders(c);
    if (error instanceof HTTPException) {
      return c.json(createErrorResponse(error.status, error.message), error.status);
    }
    return c.json(createErrorResponse(ApiStatus.INTERNAL_ERROR, error.message || "刷新分片上传预签名URL失败"), ApiStatus.INTERNAL_ERROR);
  }
});

// ================ 预签名URL相关路由 ================

// 获取预签名上传URL - 需要挂载页上传权限
fsRoutes.post("/api/fs/presign", authGateway.requireMountUpload(), unifiedFsAuthMiddleware, async (c) => {
  try {
    // 获取必要的上下文
    const db = c.env.DB;
    const userInfo = c.get("userInfo");
    const { userIdOrInfo, userType } = getServiceParams(userInfo);
    const encryptionSecret = c.env.ENCRYPTION_SECRET || "default-encryption-key";

    // 解析请求数据
    const body = await c.req.json();
    const path = body.path;
    const fileName = body.fileName;
    const contentType = body.contentType || "application/octet-stream";
    const fileSize = body.fileSize || 0;

    if (!path || !fileName) {
      return c.json(createErrorResponse(ApiStatus.BAD_REQUEST, "请提供上传路径和文件名"), ApiStatus.BAD_REQUEST);
    }

    // 检查操作权限（仅对API密钥用户）
    const tempTargetPath = path.endsWith("/") ? path + fileName : path + "/" + fileName;
    if (!checkPathPermission(userInfo, tempTargetPath)) {
      return c.json(createErrorResponse(ApiStatus.FORBIDDEN, "没有权限在此路径上传文件"), ApiStatus.FORBIDDEN);
    }

    // 使用 MountManager 获取存储驱动
    const mountManager = new MountManager(db, encryptionSecret);
    const { driver, mount, subPath } = await mountManager.getDriverByPath(path, userIdOrInfo, userType);

    if (!mount || mount.storage_type !== "S3") {
      return c.json(createErrorResponse(ApiStatus.BAD_REQUEST, "当前路径不支持预签名URL上传"), ApiStatus.BAD_REQUEST);
    }

    // 权限检查已在 MountManager.getDriverByPath 中处理

    // 构建完整的目标路径
    const targetPath = path.endsWith("/") ? path + fileName : path + "/" + fileName;

    console.log(`生成预签名URL，路径: ${targetPath}`);

    // 使用FileSystem抽象层生成预签名上传URL
    const fileSystem = new FileSystem(mountManager);
    const result = await fileSystem.generatePresignedUrl(targetPath, userIdOrInfo, userType, {
      operation: "upload",
      fileName,
      fileSize,
    });

    // 生成文件ID，用于后续提交更新
    const fileId = generateFileId();

    return c.json({
      code: ApiStatus.SUCCESS,
      message: "获取预签名URL成功",
      data: {
        presignedUrl: result.uploadUrl,
        fileId,
        s3Path: result.s3Path,
        s3Url: result.s3Url,
        mountId: mount.id,
        s3ConfigId: mount.storage_config_id,
        targetPath,
        contentType: result.contentType,
      },
      success: true,
    });
  } catch (error) {
    console.error("获取预签名URL错误:", error);
    if (error instanceof HTTPException) {
      return c.json(createErrorResponse(error.status, error.message), error.status);
    }
    return c.json(createErrorResponse(ApiStatus.INTERNAL_ERROR, error.message || "获取预签名URL失败"), ApiStatus.INTERNAL_ERROR);
  }
});

// 从文件系统创建分享链接 - 需要挂载页查看权限
fsRoutes.post("/api/fs/create-share", authGateway.requireMount(), unifiedFsAuthMiddleware, async (c) => {
  try {
    const db = c.env.DB;
    const encryptionSecret = c.env.ENCRYPTION_SECRET || "default-encryption-key";
    const userInfo = c.get("userInfo");
    const { userIdOrInfo, userType } = getServiceParams(userInfo);

    // 解析请求数据
    const body = await c.req.json();
    const { path } = body;

    if (!path) {
      return c.json(createErrorResponse(ApiStatus.BAD_REQUEST, "文件路径不能为空"), ApiStatus.BAD_REQUEST);
    }

    // 创建FileShareService实例
    const { FileShareService } = await import("../services/fileShareService.js");
    const { RepositoryFactory } = await import("../repositories/index.js");

    const repositoryFactory = new RepositoryFactory(db);
    const fileShareService = new FileShareService(db, repositoryFactory, encryptionSecret);

    // 从文件系统创建分享
    const result = await fileShareService.createShareFromFileSystem(path, userIdOrInfo, userType);

    return c.json({
      code: ApiStatus.SUCCESS,
      message: "分享创建成功",
      data: result,
      success: true,
    });
  } catch (error) {
    console.error("创建分享失败:", error);
    return c.json(createErrorResponse(ApiStatus.INTERNAL_ERROR, error.message || "创建分享失败"), ApiStatus.INTERNAL_ERROR);
  }
});

// 提交预签名URL上传完成 - 需要挂载页上传权限
fsRoutes.post("/api/fs/presign/commit", authGateway.requireMountUpload(), unifiedFsAuthMiddleware, async (c) => {
  try {
    // 解析请求数据
    const body = await c.req.json();
    const targetPath = body.targetPath;
    const mountId = body.mountId;
    const fileSize = body.fileSize || 0;

    if (!targetPath || !mountId) {
      return c.json(createErrorResponse(ApiStatus.BAD_REQUEST, "请提供完整的上传信息"), ApiStatus.BAD_REQUEST);
    }

    // 提取文件名
    const fileName = targetPath.split("/").filter(Boolean).pop();

    // fs系统不再创建files表记录，只做纯粹的文件操作
    console.log(`预签名上传完成: ${fileName}, 大小: ${fileSize}字节`);

    // 执行缓存清理
    try {
      await clearDirectoryCache({ mountId: mountId });
      console.log(`预签名上传完成后缓存已刷新：挂载点=${mountId}, 文件=${fileName}`);
    } catch (cacheError) {
      console.warn(`执行缓存清理时出错: ${cacheError.message}`);
      // 缓存清理失败不应影响整体操作
    }

    return c.json({
      code: ApiStatus.SUCCESS,
      message: "文件上传完成",
      data: {
        fileName,
        targetPath,
        fileSize,
      },
      success: true,
    });
  } catch (error) {
    console.error("提交预签名上传完成错误:", error);
    if (error instanceof HTTPException) {
      return c.json(createErrorResponse(error.status, error.message), error.status);
    }
    return c.json(createErrorResponse(ApiStatus.INTERNAL_ERROR, error.message || "提交预签名上传完成失败"), ApiStatus.INTERNAL_ERROR);
  }
});

// 更新文件内容 - 需要挂载页上传权限（更新相当于重新上传）
fsRoutes.post("/api/fs/update", authGateway.requireMountUpload(), unifiedFsAuthMiddleware, async (c) => {
  const db = c.env.DB;
  const userInfo = c.get("userInfo");
  const { userIdOrInfo, userType } = getServiceParams(userInfo);
  const encryptionSecret = c.env.ENCRYPTION_SECRET;
  const body = await c.req.json();
  const path = body.path;
  const content = body.content;

  if (!path || content === undefined) {
    return c.json(createErrorResponse(ApiStatus.BAD_REQUEST, "请提供文件路径和内容"), ApiStatus.BAD_REQUEST);
  }

  // 检查路径权限（仅对API密钥用户）
  if (!checkPathPermission(userInfo, path)) {
    return c.json(createErrorResponse(ApiStatus.FORBIDDEN, "没有权限更新此路径的文件"), ApiStatus.FORBIDDEN);
  }

  try {
    // 使用FileSystem抽象层
    const mountManager = new MountManager(db, encryptionSecret);
    const fileSystem = new FileSystem(mountManager);

    // 调用FileSystem的updateFile方法
    const result = await fileSystem.updateFile(path, content, userIdOrInfo, userType);

    // 清理缓存 - 需要获取mount信息
    try {
      const { mount } = await mountManager.getDriverByPath(path, userIdOrInfo, userType);
      await clearDirectoryCache({ mountId: mount.id });
      console.log(`更新文件操作完成后缓存已刷新：挂载点=${mount.id}, 路径=${path}`);
    } catch (cacheError) {
      console.warn(`执行缓存清理时出错: ${cacheError.message}`);
    }

    return c.json({
      code: ApiStatus.SUCCESS,
      message: "文件更新成功",
      data: result,
      success: true,
    });
  } catch (error) {
    console.error("更新文件错误:", error);
    if (error instanceof HTTPException) {
      return c.json(createErrorResponse(error.status, error.message), error.status);
    }
    return c.json(createErrorResponse(ApiStatus.INTERNAL_ERROR, error.message || "更新文件失败"), ApiStatus.INTERNAL_ERROR);
  }
});

// 批量复制文件或目录 - 需要挂载页复制权限
fsRoutes.post("/api/fs/batch-copy", authGateway.requireMountCopy(), unifiedFsAuthMiddleware, async (c) => {
  const db = c.env.DB;
  const userInfo = c.get("userInfo");
  const { userIdOrInfo, userType } = getServiceParams(userInfo);
  const encryptionSecret = c.env.ENCRYPTION_SECRET;
  const body = await c.req.json();
  const items = body.items;
  const skipExisting = body.skipExisting !== false; // 默认为true

  if (!items || !Array.isArray(items) || items.length === 0) {
    return c.json(createErrorResponse(ApiStatus.BAD_REQUEST, "请提供有效的复制项数组"), ApiStatus.BAD_REQUEST);
  }

  // 检查所有源路径和目标路径的操作权限（仅对API密钥用户）
  if (!userInfo.hasFullAccess) {
    for (const item of items) {
      if (!checkPathPermission(userInfo, item.sourcePath)) {
        return c.json(createErrorResponse(ApiStatus.FORBIDDEN, `没有权限访问源路径: ${item.sourcePath}`), ApiStatus.FORBIDDEN);
      }
      if (!checkPathPermission(userInfo, item.targetPath)) {
        return c.json(createErrorResponse(ApiStatus.FORBIDDEN, `没有权限访问目标路径: ${item.targetPath}`), ApiStatus.FORBIDDEN);
      }
    }
  }

  try {
    // 使用FileSystem抽象层处理批量复制
    const mountManager = new MountManager(db, encryptionSecret);
    const fileSystem = new FileSystem(mountManager);

    // 准备复制项，添加skipExisting选项
    const copyItems = items.map((item) => ({
      ...item,
      skipExisting,
    }));

    // 调用FileSystem的batchCopyItems方法
    const result = await fileSystem.batchCopyItems(copyItems, userIdOrInfo, userType);

    // 收集所有源路径和目标路径的挂载点ID用于缓存清理
    const sourceMountIds = new Set();
    const targetMountIds = new Set();
    for (const item of items) {
      try {
        // 收集源路径挂载点
        const { mount: sourceMount } = await mountManager.getDriverByPath(item.sourcePath, userIdOrInfo, userType);
        sourceMountIds.add(sourceMount.id);

        // 收集目标路径挂载点
        const { mount: targetMount } = await mountManager.getDriverByPath(item.targetPath, userIdOrInfo, userType);
        targetMountIds.add(targetMount.id);
      } catch (error) {
        console.warn(`获取路径挂载信息失败: ${item.sourcePath} -> ${item.targetPath}`, error);
      }
    }

    // 合并所有需要清理缓存的挂载点
    const allMountIds = new Set([...sourceMountIds, ...targetMountIds]);

    // 清理所有相关路径的缓存
    try {
      for (const mountId of allMountIds) {
        await clearDirectoryCache({ mountId });
      }
      console.log(`批量复制操作完成后缓存已刷新：源挂载点=${sourceMountIds.size}个，目标挂载点=${targetMountIds.size}个，总计=${allMountIds.size}个`);
    } catch (cacheError) {
      console.warn(`执行缓存清理时出错: ${cacheError.message}`);
    }

    // 从FileSystem返回的结果中提取信息
    const totalSuccess = result.success || 0;
    const totalSkipped = result.skipped || 0;
    const totalFailed = (result.failed && result.failed.length) || 0;
    const allDetails = result.details || [];
    const allFailedItems = result.failed || [];
    const hasCrossStorageOperations = result.hasCrossStorageOperations || false;
    const crossStorageResults = result.crossStorageResults || [];

    // 检查是否有跨存储复制操作
    if (hasCrossStorageOperations) {
      // 跨存储复制也需要清理缓存
      console.log(`跨存储复制操作，仍需清理缓存：源挂载点=${sourceMountIds.size}个，目标挂载点=${targetMountIds.size}个`);

      return c.json({
        code: ApiStatus.SUCCESS,
        message: "FILE_COPY_SUCCESS", // 标准消息标识，让前端统一生成消息
        data: {
          crossStorage: true,
          requiresClientSideCopy: true,
          standardCopyResults: {
            success: totalSuccess,
            skipped: totalSkipped,
            failed: totalFailed,
          },
          crossStorageResults: crossStorageResults,
          failed: allFailedItems,
          details: allDetails,
        },
        success: true,
      });
    }

    // 返回标准复制结果
    return c.json({
      code: ApiStatus.SUCCESS,
      message: "FILE_COPY_SUCCESS",
      data: {
        crossStorage: false,
        success: totalSuccess,
        skipped: totalSkipped,
        failed: totalFailed,
        details: allDetails,
      },
      success: true,
    });
  } catch (error) {
    console.error("批量复制错误:", error);
    if (error instanceof HTTPException) {
      return c.json(createErrorResponse(error.status, error.message), error.status);
    }
    return c.json(createErrorResponse(ApiStatus.INTERNAL_ERROR, error.message || "批量复制失败"), ApiStatus.INTERNAL_ERROR);
  }
});

// 提交批量跨存储复制完成 - 需要挂载页复制权限
fsRoutes.post("/api/fs/batch-copy-commit", authGateway.requireMountCopy(), unifiedFsAuthMiddleware, async (c) => {
  const db = c.env.DB;
  const userInfo = c.get("userInfo");
  const { userIdOrInfo, userType } = getServiceParams(userInfo);
  const body = await c.req.json();
  const { targetMountId, files } = body;

  if (!targetMountId || !Array.isArray(files) || files.length === 0) {
    return c.json(createErrorResponse(ApiStatus.BAD_REQUEST, "请提供有效的目标挂载点ID和文件列表"), ApiStatus.BAD_REQUEST);
  }

  try {
    // 获取挂载点信息
    const repositoryFactory = new RepositoryFactory(db);
    const mountRepository = repositoryFactory.getMountRepository();
    const mount = await mountRepository.findById(targetMountId);
    if (!mount) {
      return c.json(createErrorResponse(ApiStatus.NOT_FOUND, "目标挂载点不存在"), ApiStatus.NOT_FOUND);
    }

    // 获取S3配置
    const s3Config = await getS3ConfigByUserType(db, mount.storage_config_id, userIdOrInfo, userType, c.env.ENCRYPTION_SECRET);
    if (!s3Config) {
      return c.json(createErrorResponse(ApiStatus.NOT_FOUND, "存储配置不存在"), ApiStatus.NOT_FOUND);
    }

    // 用于存储结果
    const results = {
      success: [],
      failed: [],
    };

    // 处理每个文件
    for (const file of files) {
      try {
        const { targetPath, s3Path, contentType, fileSize, etag } = file;

        if (!targetPath || !s3Path) {
          results.failed.push({
            targetPath: targetPath || "未指定",
            error: "目标路径或S3路径不能为空",
          });
          continue;
        }

        // 提取文件名
        const fileName = targetPath.split("/").filter(Boolean).pop();

        results.success.push({
          targetPath,
          fileName,
        });
      } catch (fileError) {
        console.error("处理单个文件复制提交时出错:", fileError);
        results.failed.push({
          targetPath: file.targetPath || "未知路径",
          error: fileError.message || "处理文件时出错",
        });
      }
    }

    // 执行缓存清理 - 使用统一的clearCache函数
    try {
      await clearDirectoryCache({ mountId: mount.id });
      console.log(`批量复制完成后缓存已刷新：挂载点=${mount.id}, 共处理了${results.success.length}个文件`);
    } catch (cacheError) {
      console.warn(`执行缓存清理时出错: ${cacheError.message}`);
      // 缓存清理失败不应影响整体操作
    }

    // 根据结果判断整体是否成功
    const hasFailures = results.failed.length > 0;
    const hasSuccess = results.success.length > 0;

    // 如果有失败且没有任何成功的项目，则认为完全失败
    const overallSuccess = hasSuccess;

    return c.json({
      code: overallSuccess ? ApiStatus.SUCCESS : ApiStatus.ACCEPTED,
      message: "FILE_COPY_SUCCESS", // 标准消息标识，让前端统一生成消息
      data: {
        ...results,
        crossStorage: true, // 标记为跨存储复制提交
      },
      success: overallSuccess,
    });
  } catch (error) {
    console.error("提交批量复制完成错误:", error);
    if (error instanceof HTTPException) {
      return c.json(createErrorResponse(error.status, error.message), error.status);
    }
    return c.json(createErrorResponse(ApiStatus.INTERNAL_ERROR, error.message || "提交批量复制完成失败"), ApiStatus.INTERNAL_ERROR);
  }
});

/**
 * 提取搜索参数
 * @param {Record<string, string>} queryParams - 查询参数对象
 * @returns {Object} 搜索参数对象
 */
function extractSearchParams(queryParams) {
  const query = queryParams.q || "";
  const scope = queryParams.scope || "global"; // global, mount, directory
  const mountId = queryParams.mount_id || "";
  const path = queryParams.path || "";
  const limit = parseInt(queryParams.limit) || 50;
  const offset = parseInt(queryParams.offset) || 0;

  return {
    query,
    scope,
    mountId,
    path,
    limit: Math.min(limit, 200), // 限制最大返回数量
    offset: Math.max(offset, 0),
  };
}

// 搜索文件 - 需要挂载页查看权限
fsRoutes.get("/api/fs/search", authGateway.requireMount(), unifiedFsAuthMiddleware, async (c) => {
  const db = c.env.DB;
  const searchParams = extractSearchParams(c.req.query());
  const userInfo = c.get("userInfo");
  const { userIdOrInfo, userType } = getServiceParams(userInfo);

  // 参数验证
  if (!searchParams.query || searchParams.query.trim().length < 2) {
    return c.json(createErrorResponse(ApiStatus.BAD_REQUEST, "搜索查询至少需要2个字符"), ApiStatus.BAD_REQUEST);
  }

  try {
    // 使用FileSystem统一接口进行搜索
    const mountManager = new MountManager(db);
    const fileSystem = new FileSystem(mountManager);

    const result = await fileSystem.searchFiles(searchParams.query, searchParams, userIdOrInfo, userType);

    return c.json({
      code: ApiStatus.SUCCESS,
      message: "搜索完成",
      data: result,
      success: true,
    });
  } catch (error) {
    console.error("搜索文件错误:", error);
    if (error instanceof HTTPException) {
      return c.json(createErrorResponse(error.status, error.message), error.status);
    }
    return c.json(createErrorResponse(ApiStatus.INTERNAL_ERROR, error.message || "搜索文件失败"), ApiStatus.INTERNAL_ERROR);
  }
});

export default fsRoutes;
