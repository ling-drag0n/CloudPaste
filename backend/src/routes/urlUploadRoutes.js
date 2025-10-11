/**
 * URL上传路由
 * 处理URL验证、元信息获取和代理URL内容的路由
 */
import { Hono } from "hono";
import { DbTables } from "../constants/index.js";
import { ApiStatus } from "../constants/index.js";
import { createErrorResponse } from "../utils/common.js";
import { deleteFileFromS3 } from "../utils/s3Utils.js";
import { clearDirectoryCache } from "../cache/index.js";
import { authGateway } from "../middlewares/authGatewayMiddleware.js";
import { RepositoryFactory } from "../repositories/index.js";
import { FileShareService } from "../services/fileShareService.js";

const app = new Hono();

// API路由：验证URL并获取文件元信息
app.post("/api/url/info", async (c) => {
  const db = c.env.DB;

  try {
    const body = await c.req.json();

    // 验证URL参数是否存在
    if (!body.url) {
      return c.json(createErrorResponse(ApiStatus.BAD_REQUEST, "缺少URL参数"), ApiStatus.BAD_REQUEST);
    }

    // 获取加密密钥
    const encryptionSecret = c.env.ENCRYPTION_SECRET || "default-encryption-key";

    // 使用FileShareService
    const shareService = new FileShareService(db, encryptionSecret);

    // 验证URL并获取文件元信息
    const metadata = await shareService.validateUrlMetadata(body.url);

    // 返回成功响应
    return c.json({
      code: ApiStatus.SUCCESS,
      message: "URL验证成功",
      data: metadata,
      success: true,
    });
  } catch (error) {
    console.error("URL验证错误:", error);

    // 确定适当的状态码
    let statusCode = ApiStatus.INTERNAL_ERROR;
    if (error.message.includes("无效的URL") || error.message.includes("仅支持HTTP")) {
      statusCode = ApiStatus.BAD_REQUEST;
    } else if (error.message.includes("无法访问")) {
      statusCode = ApiStatus.BAD_REQUEST;
    }

    return c.json(createErrorResponse(statusCode, error.message), statusCode);
  }
});

// API路由：代理URL内容（用于不支持CORS的资源）
app.get("/api/url/proxy", async (c) => {
  try {
    // 从查询参数获取URL
    const url = c.req.query("url");

    if (!url) {
      return c.json(createErrorResponse(ApiStatus.BAD_REQUEST, "缺少URL参数"), ApiStatus.BAD_REQUEST);
    }

    // 获取加密密钥
    const encryptionSecret = c.env.ENCRYPTION_SECRET || "default-encryption-key";

    // 使用FileShareService
    const shareService = new FileShareService(db, encryptionSecret);

    // 代理URL内容
    const response = await shareService.proxyUrlContent(url);

    // 直接返回响应流
    return response;
  } catch (error) {
    console.error("代理URL内容错误:", error);

    // 确定适当的状态码
    let statusCode = ApiStatus.INTERNAL_ERROR;
    if (error.message.includes("无效的URL") || error.message.includes("仅支持HTTP")) {
      statusCode = ApiStatus.BAD_REQUEST;
    } else if (error.message.includes("源服务器返回错误状态码")) {
      statusCode = ApiStatus.BAD_REQUEST;
    }

    return c.json(createErrorResponse(statusCode, error.message), statusCode);
  }
});

// API路由：为URL上传准备预签名URL和文件记录
app.post("/api/url/presign", authGateway.requireFile(), async (c) => {
  const db = c.env.DB;

  try {
    // 获取认证信息
    const userId = authGateway.utils.getUserId(c);
    const authType = authGateway.utils.getAuthType(c);

    const body = await c.req.json();

    // 验证必要参数
    if (!body.url) {
      return c.json(createErrorResponse(ApiStatus.BAD_REQUEST, "缺少URL参数"), ApiStatus.BAD_REQUEST);
    }

    if (!body.s3_config_id) {
      return c.json(createErrorResponse(ApiStatus.BAD_REQUEST, "缺少S3配置ID参数"), ApiStatus.BAD_REQUEST);
    }

    // 获取加密密钥
    const encryptionSecret = c.env.ENCRYPTION_SECRET || "default-encryption-key";

    // 使用FileShareService - 业务服务层处理所有业务逻辑
    const shareService = new FileShareService(db, encryptionSecret);

    // 获取URL元信息
    let metadata;

    // 如果客户端已经提供了元信息，则使用客户端提供的信息
    if (body.metadata && body.metadata.filename && body.metadata.contentType) {
      metadata = body.metadata;
      metadata.url = body.url; // 确保URL字段存在
    } else {
      // 否则获取URL元信息
      metadata = await shareService.validateUrlMetadata(body.url);
    }

    // 如果客户端提供了自定义文件名，则使用客户端提供的文件名
    if (body.filename) {
      metadata.filename = body.filename;
    }

    // 准备额外选项
    const options = {
      storageConfigId: body.s3_config_id,
      slug: body.slug || null,
      remark: body.remark || null,
      customPath: body.path || null,
      password: body.password || null,
      expires_in: body.expires_in || null,
      max_views: body.max_views || null,
      use_proxy: body.use_proxy,
    };

    // 创建URL上传
    const uploadInfo = await shareService.createUrlUpload(body.url, metadata, userId, authType, options);

    // 返回成功响应
    return c.json({
      code: ApiStatus.SUCCESS,
      message: "URL上传准备就绪",
      data: uploadInfo,
      success: true,
    });
  } catch (error) {
    console.error("URL上传准备错误:", error);

    // 确定适当的状态码
    let statusCode = ApiStatus.INTERNAL_ERROR;
    if (error.message.includes("无效的URL") || error.message.includes("仅支持HTTP")) {
      statusCode = ApiStatus.BAD_REQUEST;
    } else if (error.message.includes("无法访问")) {
      statusCode = ApiStatus.BAD_REQUEST;
    } else if (error.message.includes("S3配置不存在")) {
      statusCode = ApiStatus.NOT_FOUND;
    }

    return c.json(createErrorResponse(statusCode, error.message), statusCode);
  }
});

// API路由：URL上传完成后的提交确认
app.post("/api/url/commit", authGateway.requireFile(), async (c) => {
  const db = c.env.DB;

  try {
    // 获取认证信息
    const userId = authGateway.utils.getUserId(c);
    const authType = authGateway.utils.getAuthType(c);

    const body = await c.req.json();

    // 验证必要字段
    if (!body.file_id) {
      return c.json(createErrorResponse(ApiStatus.BAD_REQUEST, "缺少文件ID参数"), ApiStatus.BAD_REQUEST);
    }

    // ETag参数是可选的，某些S3兼容服务（如又拍云）可能由于CORS限制无法返回ETag
    // 如果没有ETag，我们仍然允许提交，但会记录警告
    if (!body.etag) {
      console.warn(`URL上传提交时未提供ETag: ${body.file_id}，可能是由于CORS限制导致前端无法获取ETag响应头`);
    }

    // 获取加密密钥
    const encryptionSecret = c.env.ENCRYPTION_SECRET || "default-encryption-key";

    // 使用FileShareService - 业务服务层处理所有业务逻辑
    const shareService = new FileShareService(db, encryptionSecret);

    // 准备上传结果数据
    const uploadResult = {
      etag: body.etag || null,
      size: body.size ? parseInt(body.size) : null,
      // 可选的元数据更新
      slug: body.slug || null,
      remark: body.remark || null,
      password: body.password || null,
      expires_in: body.expires_in || null,
      max_views: body.max_views || null,
    };

    // 委托给FileShareService处理提交逻辑
    const result = await shareService.commitUpload(body.file_id, uploadResult, userId, authType);

    // 返回成功响应
    return c.json({
      code: ApiStatus.SUCCESS,
      message: "文件提交成功",
      data: result,
      success: true,
    });
  } catch (error) {
    console.error("提交文件错误:", error);
    return c.json(createErrorResponse(ApiStatus.INTERNAL_ERROR, "提交文件失败: " + error.message), ApiStatus.INTERNAL_ERROR);
  }
});

// API路由：初始化分片上传流程
app.post("/api/url/multipart/init", authGateway.requireFile(), async (c) => {
  const db = c.env.DB;

  // 获取认证信息
  const isAdmin = authGateway.utils.isAdmin(c);
  const userId = authGateway.utils.getUserId(c);
  const authType = authGateway.utils.getAuthType(c);

  try {
    const body = await c.req.json();

    // 验证必要参数
    if (!body.url) {
      return c.json(createErrorResponse(ApiStatus.BAD_REQUEST, "缺少URL参数"), ApiStatus.BAD_REQUEST);
    }

    if (!body.s3_config_id) {
      return c.json(createErrorResponse(ApiStatus.BAD_REQUEST, "缺少S3配置ID参数"), ApiStatus.BAD_REQUEST);
    }

    // 验证S3配置ID
    const repositoryFactory = new RepositoryFactory(db);
    const s3ConfigRepository = repositoryFactory.getS3ConfigRepository();

    let s3Config;
    if (isAdmin) {
      s3Config = await s3ConfigRepository.findByIdAndAdmin(body.s3_config_id, userId);
    } else {
      s3Config = await s3ConfigRepository.findPublicById(body.s3_config_id);
    }

    if (!s3Config) {
      return c.json(createErrorResponse(ApiStatus.NOT_FOUND, "指定的S3配置不存在或无权访问"), ApiStatus.NOT_FOUND);
    }

    // 获取加密密钥
    const encryptionSecret = c.env.ENCRYPTION_SECRET || "default-encryption-key";

    // 使用FileShareService - 业务服务层处理所有业务逻辑
    const shareService = new FileShareService(db, encryptionSecret);

    // 获取URL元信息
    let metadata;

    // 如果客户端已经提供了元信息，则使用客户端提供的信息
    if (body.metadata && body.metadata.filename && body.metadata.contentType) {
      metadata = body.metadata;
      metadata.url = body.url; // 确保URL字段存在
    } else {
      // 否则获取URL元信息
      metadata = await shareService.validateUrlMetadata(body.url);
    }

    // 如果客户端提供了自定义文件名，则使用客户端提供的文件名
    if (body.filename) {
      metadata.filename = body.filename;
    }

    // 准备额外选项
    const options = {
      storageConfigId: body.s3_config_id,
      slug: body.slug || null,
      remark: body.remark || null,
      customPath: body.path || null,
      password: body.password || null,
      expires_in: body.expires_in || null,
      max_views: body.max_views || null,
      partSize: body.part_size || null,
      partCount: body.part_count || null,
      override: body.override || false, // 用户选择的覆盖设置
      use_proxy: body.use_proxy, // 用户选择的代理设置
    };

    // 初始化URL分片上传
    const result = await shareService.initializeUrlMultipartUpload(body.url, metadata, userId, authType, options);

    // 返回成功响应
    return c.json({
      code: ApiStatus.SUCCESS,
      message: "分片上传初始化成功",
      data: result,
      success: true,
    });
  } catch (error) {
    console.error("初始化分片上传错误:", error);

    // 确定适当的状态码
    let statusCode = ApiStatus.INTERNAL_ERROR;
    if (error.message.includes("无效的URL") || error.message.includes("仅支持HTTP")) {
      statusCode = ApiStatus.BAD_REQUEST;
    } else if (error.message.includes("无法访问")) {
      statusCode = ApiStatus.BAD_REQUEST;
    } else if (error.message.includes("S3配置不存在")) {
      statusCode = ApiStatus.NOT_FOUND;
    } else if (error.message.includes("自定义链接")) {
      statusCode = ApiStatus.BAD_REQUEST;
    }

    return c.json(createErrorResponse(statusCode, error.message), statusCode);
  }
});

// API路由：完成分片上传流程
app.post("/api/url/multipart/complete", authGateway.requireFile(), async (c) => {
  const db = c.env.DB;

  // 获取认证信息
  const isAdmin = authGateway.utils.isAdmin(c);
  const userId = authGateway.utils.getUserId(c);
  const authType = authGateway.utils.getAuthType(c);

  try {
    const body = await c.req.json();

    // 验证必要参数
    if (!body.file_id) {
      return c.json(createErrorResponse(ApiStatus.BAD_REQUEST, "缺少文件ID参数"), ApiStatus.BAD_REQUEST);
    }

    if (!body.parts) {
      return c.json(createErrorResponse(ApiStatus.BAD_REQUEST, "缺少分片列表参数"), ApiStatus.BAD_REQUEST);
    }

    if (!body.upload_id) {
      return c.json(createErrorResponse(ApiStatus.BAD_REQUEST, "缺少上传ID参数"), ApiStatus.BAD_REQUEST);
    }

    // 查询文件记录
    const repositoryFactory = new RepositoryFactory(db);
    const fileRepository = repositoryFactory.getFileRepository();
    const file = await fileRepository.findById(body.file_id);

    if (!file) {
      return c.json(createErrorResponse(ApiStatus.NOT_FOUND, "文件不存在或已被删除"), ApiStatus.NOT_FOUND);
    }

    // 验证权限
    if (isAdmin && file.created_by && file.created_by !== userId) {
      return c.json(createErrorResponse(ApiStatus.FORBIDDEN, "您无权完成此文件的上传"), ApiStatus.FORBIDDEN);
    }

    if (authType === "apikey" && file.created_by && file.created_by !== `apikey:${userId}`) {
      return c.json(createErrorResponse(ApiStatus.FORBIDDEN, "此API密钥无权完成此文件的上传"), ApiStatus.FORBIDDEN);
    }

    // 获取加密密钥
    const encryptionSecret = c.env.ENCRYPTION_SECRET || "default-encryption-key";

    // 使用FileShareService - 业务服务层处理所有业务逻辑
    const shareService = new FileShareService(db, encryptionSecret);

    // 完成URL分片上传
    const result = await shareService.completeUrlMultipartUpload(body.file_id, body.upload_id, body.parts, userId, authType);

    // 返回成功响应
    return c.json({
      code: ApiStatus.SUCCESS,
      message: "分片上传已完成",
      data: result,
      success: true,
    });
  } catch (error) {
    console.error("完成分片上传错误:", error);

    // 确定适当的状态码
    let statusCode = ApiStatus.INTERNAL_ERROR;

    if (error.message.includes("文件不存在")) {
      statusCode = ApiStatus.NOT_FOUND;
    } else if (error.message.includes("无效的分片信息")) {
      statusCode = ApiStatus.BAD_REQUEST;
    }

    return c.json(createErrorResponse(statusCode, error.message), statusCode);
  }
});

// API路由：终止分片上传流程
app.post("/api/url/multipart/abort", authGateway.requireFile(), async (c) => {
  const db = c.env.DB;

  // 获取认证信息
  const isAdmin = authGateway.utils.isAdmin(c);
  const userId = authGateway.utils.getUserId(c);
  const authType = authGateway.utils.getAuthType(c);

  try {
    const body = await c.req.json();

    // 验证必要参数
    if (!body.file_id) {
      return c.json(createErrorResponse(ApiStatus.BAD_REQUEST, "缺少文件ID参数"), ApiStatus.BAD_REQUEST);
    }

    if (!body.upload_id) {
      return c.json(createErrorResponse(ApiStatus.BAD_REQUEST, "缺少上传ID参数"), ApiStatus.BAD_REQUEST);
    }

    // 查询文件记录
    const repositoryFactory = new RepositoryFactory(db);
    const fileRepository = repositoryFactory.getFileRepository();
    const file = await fileRepository.findById(body.file_id);

    if (!file) {
      return c.json(createErrorResponse(ApiStatus.NOT_FOUND, "文件不存在或已被删除"), ApiStatus.NOT_FOUND);
    }

    // 验证权限
    if (isAdmin && file.created_by && file.created_by !== userId) {
      return c.json(createErrorResponse(ApiStatus.FORBIDDEN, "您无权终止此文件的上传"), ApiStatus.FORBIDDEN);
    }

    if (authType === "apikey" && file.created_by && file.created_by !== `apikey:${userId}`) {
      return c.json(createErrorResponse(ApiStatus.FORBIDDEN, "此API密钥无权终止此文件的上传"), ApiStatus.FORBIDDEN);
    }

    // 获取加密密钥
    const encryptionSecret = c.env.ENCRYPTION_SECRET || "default-encryption-key";

    // 使用FileShareService - 业务服务层处理所有业务逻辑
    const shareService = new FileShareService(db, encryptionSecret);

    // 中止URL分片上传
    const result = await shareService.abortUrlMultipartUpload(body.file_id, body.upload_id, userId, authType);

    // 返回成功响应
    return c.json({
      code: ApiStatus.SUCCESS,
      message: "分片上传已终止",
      data: result,
      success: true,
    });
  } catch (error) {
    console.error("终止分片上传错误:", error);

    // 确定适当的状态码
    let statusCode = ApiStatus.INTERNAL_ERROR;

    if (error.message.includes("文件不存在")) {
      statusCode = ApiStatus.NOT_FOUND;
    }

    return c.json(createErrorResponse(statusCode, error.message), statusCode);
  }
});

// API路由：取消URL上传并删除文件记录
app.post("/api/url/cancel", authGateway.requireFile(), async (c) => {
  const db = c.env.DB;

  // 使用新的权限工具获取用户信息
  const isAdmin = authGateway.utils.isAdmin(c);
  const userId = authGateway.utils.getUserId(c);
  const authType = authGateway.utils.getAuthType(c);

  let authorizedBy = "";
  let adminId = null;
  let apiKeyId = null;

  if (isAdmin) {
    authorizedBy = "admin";
    adminId = userId;
  } else if (authType === "apikey") {
    authorizedBy = "apikey";
    apiKeyId = userId;
  } else {
    return c.json(createErrorResponse(ApiStatus.FORBIDDEN, "需要管理员权限或有效的API密钥才能取消URL上传"), ApiStatus.FORBIDDEN);
  }

  try {
    const body = await c.req.json();

    // 验证必要参数
    if (!body.file_id) {
      return c.json(createErrorResponse(ApiStatus.BAD_REQUEST, "缺少文件ID参数"), ApiStatus.BAD_REQUEST);
    }

    // 查询文件记录
    const repositoryFactory = new RepositoryFactory(db);
    const fileRepository = repositoryFactory.getFileRepository();
    const file = await fileRepository.findById(body.file_id);

    if (!file) {
      return c.json(createErrorResponse(ApiStatus.NOT_FOUND, "文件不存在或已被删除"), ApiStatus.NOT_FOUND);
    }

    // 验证权限
    if (authorizedBy === "admin" && file.created_by && file.created_by !== adminId) {
      return c.json(createErrorResponse(ApiStatus.FORBIDDEN, "您无权取消此文件的上传"), ApiStatus.FORBIDDEN);
    }

    if (authorizedBy === "apikey" && file.created_by && file.created_by !== `apikey:${apiKeyId}`) {
      return c.json(createErrorResponse(ApiStatus.FORBIDDEN, "此API密钥无权取消此文件的上传"), ApiStatus.FORBIDDEN);
    }

    // 获取S3配置
    const s3ConfigRepository = repositoryFactory.getS3ConfigRepository();
    let s3Config = null;
    if (file.storage_type === "S3") {
      s3Config = await s3ConfigRepository.findById(file.storage_config_id);
    }

    if (file.storage_type === "S3" && !s3Config) {
      // 如果S3配置不存在，仍然尝试删除文件记录
      console.warn(`找不到S3配置(ID=${file.storage_config_id})，仅删除文件记录`);
    } else if (file.storage_type === "S3" && s3Config) {
      // 尝试从S3删除文件
      try {
        const encryptionSecret = c.env.ENCRYPTION_SECRET || "default-encryption-key";
        await deleteFileFromS3(s3Config, file.storage_path, encryptionSecret);
        console.log(`已从S3删除文件: ${file.storage_path}`);
      } catch (s3Error) {
        // 如果S3删除失败，记录错误但继续删除数据库记录
        console.error(`从S3删除文件失败: ${s3Error.message}`);
      }
    }

    // 删除文件密码记录（如果存在）
    await fileRepository.deleteFilePasswordRecord(file.id);

    // 删除文件记录
    await fileRepository.deleteFile(file.id);

    // 清除与文件相关的缓存 - 使用统一的clearDirectoryCache函数
    try {
      await clearDirectoryCache({ db, s3ConfigId: file.s3_config_id });
    } catch (cacheError) {
      console.warn(`清除文件缓存失败: ${cacheError.message}`);
    }

    // 返回成功响应
    return c.json({
      code: ApiStatus.SUCCESS,
      message: "URL上传已成功取消",
      data: {
        file_id: file.id,
        status: "cancelled",
        message: "文件记录已被删除",
      },
      success: true,
    });
  } catch (error) {
    console.error("取消URL上传错误:", error);

    // 确定适当的状态码
    let statusCode = ApiStatus.INTERNAL_ERROR;
    if (error.message.includes("文件不存在")) {
      statusCode = ApiStatus.NOT_FOUND;
    }

    return c.json(createErrorResponse(statusCode, "取消URL上传失败: " + error.message), statusCode);
  }
});

export default app;
