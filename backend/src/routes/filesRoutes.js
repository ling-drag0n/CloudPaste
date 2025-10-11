import { Hono } from "hono";
import { ApiStatus } from "../constants/index.js";
import { createErrorResponse } from "../utils/common.js";
import { deleteFileFromS3 } from "../utils/s3Utils.js";
import { verifyPassword } from "../utils/crypto.js";
import {
  generateFileDownloadUrl,
  getAdminFileList,
  getAdminFileDetail,
  getUserFileList,
  getUserFileDetail,
  getFileBySlug,
  isFileAccessible,
  incrementAndCheckFileViews,
  getPublicFileInfo,
  updateFile,
} from "../services/fileService.js";
import { clearDirectoryCache } from "../cache/index.js";
import { authGateway } from "../middlewares/authGatewayMiddleware.js";
import { RepositoryFactory } from "../repositories/index.js";

const app = new Hono();

/**
 * 统一文件路由
 * 支持管理员和API密钥用户的文件管理功能
 */

/**
 * 创建统一的认证中间件，支持admin和apiKey两种认证方式
 */
function createUnifiedAuthMiddleware() {
  return async (c, next) => {
    // 检查Authorization头
    const authHeader = c.req.header("Authorization");
    if (!authHeader) {
      return c.json(createErrorResponse(ApiStatus.UNAUTHORIZED, "需要认证"), ApiStatus.UNAUTHORIZED);
    }

    let isAuthenticated = false;

    // 尝试admin认证
    try {
      await authGateway.requireAdmin()(c, async () => {
        // admin认证成功，设置用户信息
        c.set("userType", "admin");
        c.set("userId", authGateway.utils.getUserId(c));
        isAuthenticated = true;
      });

      if (isAuthenticated) {
        return await next();
      }
    } catch (adminError) {
      // admin认证失败，继续尝试apiKey认证
    }

    // 尝试apiKey认证
    try {
      await authGateway.requireFile()(c, async () => {
        // apiKey认证成功，设置用户信息
        c.set("userType", "apikey");
        c.set("userId", authGateway.utils.getUserId(c));
        c.set("apiKeyInfo", authGateway.utils.getApiKeyInfo(c));
        isAuthenticated = true;
      });

      if (isAuthenticated) {
        return await next();
      }
    } catch (apiKeyError) {
      // 两种认证都失败
    }

    // 如果到这里说明认证失败
    return c.json(createErrorResponse(ApiStatus.UNAUTHORIZED, "需要管理员权限或有效的API密钥"), ApiStatus.UNAUTHORIZED);
  };
}

// ==================== 公共文件接口（无需认证） ====================

// 获取公开文件（无需认证）
app.get("/api/public/files/:slug", async (c) => {
  const db = c.env.DB;
  const { slug } = c.req.param();
  const encryptionSecret = c.env.ENCRYPTION_SECRET || "default-encryption-key";

  try {
    // 查询文件详情
    const file = await getFileBySlug(db, slug);

    // 检查文件是否可访问
    const accessCheck = await isFileAccessible(db, file, encryptionSecret);
    if (!accessCheck.accessible) {
      if (accessCheck.reason === "expired") {
        return c.json(createErrorResponse(ApiStatus.GONE, "文件已过期"), ApiStatus.GONE);
      }
      return c.json(createErrorResponse(ApiStatus.NOT_FOUND, "文件不存在"), ApiStatus.NOT_FOUND);
    }

    // 检查是否需要密码
    const requiresPassword = !!file.password;

    // 如果不需要密码，立即增加访问次数并检查是否超过限制
    if (!requiresPassword) {
      // 增加访问次数并检查限制
      const result = await incrementAndCheckFileViews(db, file, encryptionSecret);

      // 如果文件已过期，返回相应的错误
      if (result.isExpired) {
        // 确保文件被删除
        try {
          // 再次检查文件是否仍然存在
          const repositoryFactory = new RepositoryFactory(db);
          const fileRepository = repositoryFactory.getFileRepository();
          const fileStillExists = await fileRepository.findById(file.id);
          if (fileStillExists) {
            console.log(`文件(${file.id})达到最大访问次数但未被删除，再次尝试删除...`);
            // 导入并使用 checkAndDeleteExpiredFile 函数
            const { checkAndDeleteExpiredFile } = await import("../services/fileViewService.js");
            await checkAndDeleteExpiredFile(db, result.file, encryptionSecret);
          }
        } catch (error) {
          console.error(`尝试再次删除文件(${file.id})时出错:`, error);
        }
        return c.json(createErrorResponse(ApiStatus.GONE, "文件已达到最大查看次数"), ApiStatus.GONE);
      }

      // 生成文件下载URL
      const urlsObj = await generateFileDownloadUrl(db, result.file, encryptionSecret, c.req.raw);

      // 构建公开信息
      const publicInfo = await getPublicFileInfo(result.file, requiresPassword, urlsObj);

      return c.json({
        code: ApiStatus.SUCCESS,
        message: "获取文件成功",
        data: publicInfo,
        success: true,
      });
    } else {
      // 文件需要密码验证，只返回基本信息
      const publicInfo = await getPublicFileInfo(file, true);

      return c.json({
        code: ApiStatus.SUCCESS,
        message: "获取文件成功",
        data: publicInfo,
        success: true,
      });
    }
  } catch (error) {
    console.error("获取公开文件错误:", error);
    return c.json(createErrorResponse(ApiStatus.INTERNAL_ERROR, error.message || "获取文件失败"), ApiStatus.INTERNAL_ERROR);
  }
});

// 验证文件密码
app.post("/api/public/files/:slug/verify", async (c) => {
  const db = c.env.DB;
  const { slug } = c.req.param();
  const body = await c.req.json();
  const encryptionSecret = c.env.ENCRYPTION_SECRET || "default-encryption-key";

  if (!body.password) {
    return c.json(createErrorResponse(ApiStatus.BAD_REQUEST, "密码是必需的"), ApiStatus.BAD_REQUEST);
  }

  try {
    // 查询文件详情
    const file = await getFileBySlug(db, slug);

    // 检查文件是否可访问
    const accessCheck = await isFileAccessible(db, file, encryptionSecret);
    if (!accessCheck.accessible) {
      if (accessCheck.reason === "expired") {
        return c.json(createErrorResponse(ApiStatus.GONE, "文件已过期"), ApiStatus.GONE);
      }
      return c.json(createErrorResponse(ApiStatus.NOT_FOUND, "文件不存在"), ApiStatus.NOT_FOUND);
    }

    // 验证密码
    if (!file.password) {
      return c.json({
        code: ApiStatus.BAD_REQUEST,
        message: "此文件不需要密码",
        data: {
          url: file.s3_url,
        },
        success: true,
      });
    }

    const passwordValid = await verifyPassword(body.password, file.password);

    if (!passwordValid) {
      return c.json(createErrorResponse(ApiStatus.UNAUTHORIZED, "密码不正确"), ApiStatus.UNAUTHORIZED);
    }

    // 密码验证成功，增加查看次数并检查限制
    const result = await incrementAndCheckFileViews(db, file, encryptionSecret);

    // 如果文件已过期，返回相应的错误
    if (result.isExpired) {
      // 确保文件被删除
      try {
        // 再次检查文件是否仍然存在
        const repositoryFactory = new RepositoryFactory(db);
        const fileRepository = repositoryFactory.getFileRepository();
        const fileStillExists = await fileRepository.findById(file.id);
        if (fileStillExists) {
          console.log(`文件(${file.id})达到最大访问次数但未被删除，再次尝试删除...`);
          // 导入并使用 checkAndDeleteExpiredFile 函数
          const { checkAndDeleteExpiredFile } = await import("../services/fileViewService.js");
          await checkAndDeleteExpiredFile(db, result.file, encryptionSecret);
        }
      } catch (error) {
        console.error(`尝试再次删除文件(${file.id})时出错:`, error);
      }
      return c.json(createErrorResponse(ApiStatus.GONE, "文件已达到最大查看次数"), ApiStatus.GONE);
    }

    // 生成文件下载URL
    const urlsObj = await generateFileDownloadUrl(db, result.file, encryptionSecret, c.req.raw);

    // 获取明文密码（与管理员接口保持一致）
    let fileWithPassword = result.file;
    if (result.file.password) {
      // 使用 RepositoryFactory 获取明文密码
      const repositoryFactory = new RepositoryFactory(db);
      const fileRepository = repositoryFactory.getFileRepository();
      const passwordInfo = await fileRepository.getFilePassword(result.file.id);
      if (passwordInfo && passwordInfo.plain_password) {
        fileWithPassword = {
          ...result.file,
          plain_password: passwordInfo.plain_password,
        };
      }
    }

    // 使用getPublicFileInfo函数构建完整的响应，包括代理链接和type字段
    const publicInfo = await getPublicFileInfo(fileWithPassword, false, urlsObj);

    return c.json({
      code: ApiStatus.SUCCESS,
      message: "密码验证成功",
      data: publicInfo,
      success: true,
    });
  } catch (error) {
    console.error("验证文件密码错误:", error);
    return c.json(createErrorResponse(ApiStatus.INTERNAL_ERROR, error.message || "验证密码失败"), ApiStatus.INTERNAL_ERROR);
  }
});

// ==================== 统一认证文件接口 ====================

const unifiedAuth = createUnifiedAuthMiddleware();

// 获取文件列表（统一接口）
app.get("/api/files", unifiedAuth, async (c) => {
  const db = c.env.DB;
  const userType = c.get("userType");
  const userId = c.get("userId");
  const apiKeyInfo = c.get("apiKeyInfo");

  try {
    let result;

    if (userType === "admin") {
      // 管理员：获取查询参数
      const limit = parseInt(c.req.query("limit") || "30");
      const offset = parseInt(c.req.query("offset") || "0");
      const search = c.req.query("search");
      const createdBy = c.req.query("created_by");

      // 构建查询选项
      const options = { limit, offset };
      if (search) options.search = search;
      if (createdBy) options.createdBy = createdBy;

      // 使用管理员服务获取文件列表
      result = await getAdminFileList(db, options);
    } else {
      // API密钥用户：获取查询参数
      const limit = parseInt(c.req.query("limit") || "30");
      const offset = parseInt(c.req.query("offset") || "0");
      const search = c.req.query("search");

      // 构建查询选项
      const options = { limit, offset };
      if (search) options.search = search;

      // 使用用户服务获取文件列表
      result = await getUserFileList(db, userId, options);
    }

    const response = {
      code: ApiStatus.SUCCESS,
      message: "获取文件列表成功",
      data: result,
      success: true,
    };

    // API密钥用户需要返回密钥信息
    if (userType === "apikey") {
      response.key_info = apiKeyInfo;
    }

    return c.json(response);
  } catch (error) {
    console.error("获取文件列表错误:", error);
    return c.json(createErrorResponse(ApiStatus.INTERNAL_ERROR, error.message || "获取文件列表失败"), ApiStatus.INTERNAL_ERROR);
  }
});

// 获取单个文件详情（统一接口）
app.get("/api/files/:id", unifiedAuth, async (c) => {
  const db = c.env.DB;
  const userType = c.get("userType");
  const userId = c.get("userId");
  const { id } = c.req.param();
  const encryptionSecret = c.env.ENCRYPTION_SECRET || "default-encryption-key";

  try {
    let result;

    if (userType === "admin") {
      // 管理员：可以获取任何文件的详情
      result = await getAdminFileDetail(db, id, encryptionSecret, c.req.raw);
    } else {
      // API密钥用户：只能获取自己文件的详情
      result = await getUserFileDetail(db, id, userId, encryptionSecret, c.req.raw);
    }

    return c.json({
      code: ApiStatus.SUCCESS,
      message: "获取文件成功",
      data: result,
      success: true,
    });
  } catch (error) {
    console.error("获取文件错误:", error);
    return c.json(createErrorResponse(ApiStatus.INTERNAL_ERROR, error.message || "获取文件失败"), ApiStatus.INTERNAL_ERROR);
  }
});

// 批量删除文件（统一接口）
app.delete("/api/files/batch-delete", unifiedAuth, async (c) => {
  const db = c.env.DB;
  const userType = c.get("userType");
  const userId = c.get("userId");
  const body = await c.req.json();
  const ids = body.ids;
  const deleteMode = body.delete_mode || "both"; // 默认为同时删除

  if (!ids || !Array.isArray(ids) || ids.length === 0) {
    return c.json(createErrorResponse(ApiStatus.BAD_REQUEST, "请提供有效的文件ID数组"), ApiStatus.BAD_REQUEST);
  }

  if (!["record_only", "both"].includes(deleteMode)) {
    return c.json(createErrorResponse(ApiStatus.BAD_REQUEST, "删除模式必须是 'record_only' 或 'both'"), ApiStatus.BAD_REQUEST);
  }

  // 结果统计
  const result = {
    success: 0,
    failed: [],
  };

  const encryptionSecret = c.env.ENCRYPTION_SECRET || "default-encryption-key";
  const s3ConfigIds = new Set(); // 收集需要清理缓存的S3配置ID

  // 使用 Repository 获取文件信息
  const repositoryFactory = new RepositoryFactory(db);
  const fileRepository = repositoryFactory.getFileRepository();

  for (const id of ids) {
    try {
      let file;

      if (userType === "admin") {
        // 管理员：可以删除任何文件
        file = await fileRepository.findByIdWithStorageConfig(id);
        if (!file) {
          result.failed.push({
            id: id,
            error: "文件不存在",
          });
          continue;
        }
      } else {
        // API密钥用户：只能删除自己的文件
        file = await fileRepository.findByIdAndCreator(id, `apikey:${userId}`);
        if (!file) {
          result.failed.push({
            id: id,
            error: "文件不存在或无权删除",
          });
          continue;
        }
      }

      // 收集S3配置ID用于缓存清理（仅对S3存储类型）
      if (file.storage_type === "S3" && file.storage_config_id) {
        s3ConfigIds.add(file.storage_config_id);
      }

      // 根据删除模式和文件来源进行不同的处理
      try {
        // 如果是来自文件系统的文件且选择同时删除
        if (deleteMode === "both" && file.file_path) {
          // 删除文件系统中的原文件
          const { MountManager } = await import("../storage/managers/MountManager.js");
          const { FileSystem } = await import("../storage/fs/FileSystem.js");

          const mountManager = new MountManager(db, encryptionSecret);
          const fileSystem = new FileSystem(mountManager);

          try {
            await fileSystem.batchRemoveItems([file.file_path], userType === "admin" ? userId : `apikey:${userId}`, userType);
          } catch (fsError) {
            console.error(`删除文件系统文件错误 (ID: ${id}):`, fsError);
            // 文件系统删除失败，记录错误但继续处理
          }
        }

        // 删除分享存储中的文件（如果存在且需要删除）
        if (deleteMode === "both" && file.storage_path && file.bucket_name) {
          const s3Config = {
            id: file.id,
            endpoint_url: file.endpoint_url,
            bucket_name: file.bucket_name,
            region: file.region,
            access_key_id: file.access_key_id,
            secret_access_key: file.secret_access_key,
            path_style: file.path_style,
          };
          await deleteFileFromS3(s3Config, file.storage_path, encryptionSecret);
        }
      } catch (deleteError) {
        console.error(`删除文件错误 (ID: ${id}):`, deleteError);
        // 即使删除失败，也继续从数据库中删除记录
      }

      // 使用 FileRepository 删除记录和关联的密码记录
      if (userType === "admin") {
        // 管理员：删除密码记录
        await fileRepository.deleteFilePasswordRecord(id);
      }
      // 删除文件记录
      await fileRepository.deleteFile(id);

      result.success++;
    } catch (error) {
      console.error(`删除文件失败 (ID: ${id}):`, error);
      result.failed.push({
        id: id,
        error: error.message || "删除失败",
      });
    }
  }

  // 清除与文件相关的缓存
  try {
    for (const s3ConfigId of s3ConfigIds) {
      await clearDirectoryCache({ db, s3ConfigId });
    }
    console.log(`批量删除操作完成后缓存已刷新：${s3ConfigIds.size} 个S3配置`);
  } catch (cacheError) {
    console.warn(`执行缓存清理时出错: ${cacheError.message}`);
  }

  return c.json({
    code: ApiStatus.SUCCESS,
    message: `批量删除完成，成功: ${result.success}，失败: ${result.failed.length}`,
    data: result,
    success: true,
  });
});

// 更新文件元数据（统一接口）
app.put("/api/files/:id", unifiedAuth, async (c) => {
  const db = c.env.DB;
  const userType = c.get("userType");
  const userId = c.get("userId");
  const { id } = c.req.param();
  const body = await c.req.json();

  try {
    // 调用服务层处理更新逻辑
    const result = await updateFile(db, id, body, { userType, userId });

    return c.json({
      code: ApiStatus.SUCCESS,
      message: result.message,
      success: true,
    });
  } catch (error) {
    console.error("更新文件元数据错误:", error);

    // 处理 HTTPException
    if (error.status) {
      return c.json(createErrorResponse(error.status, error.message), error.status);
    }

    return c.json(createErrorResponse(ApiStatus.INTERNAL_ERROR, error.message || "更新文件元数据失败"), ApiStatus.INTERNAL_ERROR);
  }
});

export default app;
