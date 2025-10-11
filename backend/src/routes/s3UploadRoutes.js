import { Hono } from "hono";
import { ApiStatus } from "../constants/index.js";
import { createErrorResponse } from "../utils/common.js";
import { authGateway } from "../middlewares/authGatewayMiddleware.js";
import { FileShareService } from "../services/fileShareService.js";

const app = new Hono();

// 获取预签名上传URL
app.post("/api/s3/presign", authGateway.requireFile(), async (c) => {
  const db = c.env.DB;

  try {
    // 获取认证信息
    const userId = authGateway.utils.getUserId(c);
    const authType = authGateway.utils.getAuthType(c);

    // 解析请求数据
    const body = await c.req.json();

    // 基本参数验证
    if (!body.s3_config_id) {
      return c.json(createErrorResponse(ApiStatus.BAD_REQUEST, "必须提供 s3_config_id"), ApiStatus.BAD_REQUEST);
    }

    if (!body.filename) {
      return c.json(createErrorResponse(ApiStatus.BAD_REQUEST, "必须提供 filename"), ApiStatus.BAD_REQUEST);
    }

    // 使用FileShareService - 业务服务层处理所有业务逻辑
    const shareService = new FileShareService(db, c.env.ENCRYPTION_SECRET || "default-encryption-key");

    const result = await shareService.createPresignedUpload(body.s3_config_id, body.filename, userId, authType, {
      fileSize: body.size,
      slug: body.slug,
      override: body.override,
      customPath: body.path,
      remark: body.remark,
      password: body.password,
      expires_in: body.expires_in,
      max_views: body.max_views,
      use_proxy: body.use_proxy,
    });

    // 返回预签名URL和文件信息
    return c.json({
      code: ApiStatus.SUCCESS,
      message: "获取预签名URL成功",
      data: result,
      success: true,
    });
  } catch (error) {
    console.error("获取预签名URL错误:", error);
    return c.json(createErrorResponse(ApiStatus.INTERNAL_ERROR, "获取预签名URL失败: " + error.message), ApiStatus.INTERNAL_ERROR);
  }
});

// 文件上传完成后的提交确认
app.post("/api/s3/commit", authGateway.requireFile(), async (c) => {
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
      console.warn(`文件提交时未提供ETag: ${body.file_id}，可能是由于CORS限制导致前端无法获取ETag响应头`);
    }

    // 使用FileShareService - 业务服务层处理所有业务逻辑
    const shareService = new FileShareService(db, c.env.ENCRYPTION_SECRET || "default-encryption-key");

    const result = await shareService.commitUpload(
      body.file_id,
      {
        size: body.size,
        etag: body.etag,
      },
      userId,
      authType
    );

    // 注意：业务参数（password, remark, expires_in等）应该在presign阶段处理
    // commit阶段只负责更新上传结果（size, etag）
    // 如果前端在commit阶段传递了这些参数，记录警告但不处理
    if (body.password || body.expires_in || body.max_views || body.remark) {
      console.warn(
        `commit阶段收到业务参数，这些参数应该在presign阶段处理: ${JSON.stringify({
          hasPassword: !!body.password,
          hasExpiresIn: !!body.expires_in,
          hasMaxViews: !!body.max_views,
          hasRemark: !!body.remark,
        })}`
      );
    }

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

// 直接上传文件接口
app.put("/api/upload-direct/:filename", authGateway.requireFile(), async (c) => {
  try {
    const db = c.env.DB;
    const filename = c.req.param("filename");

    // 获取认证信息
    const userId = authGateway.utils.getUserId(c);
    const authType = authGateway.utils.getAuthType(c);

    // 获取文件内容
    const fileContent = await c.req.arrayBuffer();
    const fileSize = fileContent.byteLength;

    // 解析所有查询参数
    const params = {
      s3_config_id: c.req.query("s3_config_id"),
      slug: c.req.query("slug"),
      path: c.req.query("path") || "",
      remark: c.req.query("remark") || "",
      password: c.req.query("password"),
      expires_in: c.req.query("expires_in") ? parseInt(c.req.query("expires_in")) : 0,
      max_views: c.req.query("max_views") ? parseInt(c.req.query("max_views")) : 0,
      override: c.req.query("override") === "true",
      original_filename: c.req.query("original_filename") === "true",
      use_proxy: c.req.query("use_proxy") !== "0" ? 1 : 0,
    };

    // 使用FileShareService完成核心上传工作
    const shareService = new FileShareService(db, c.env.ENCRYPTION_SECRET || "default-encryption-key");
    const result = await shareService.uploadDirectComplete(filename, fileContent, fileSize, userId, authType, params);

    // 在路由层构建响应数据
    const { fileRecord, config } = result;
    const { generatePresignedUrl } = await import("../utils/s3Utils.js");
    const { getMimeTypeFromFilename } = await import("../utils/fileUtils.js");

    const contentType = getMimeTypeFromFilename(filename);
    const isAdmin = authType === "admin";

    // 生成预签名URL
    const encryptionSecret = c.env.ENCRYPTION_SECRET || "default-encryption-key";
    const previewDirectUrl = await generatePresignedUrl(config, fileRecord.storage_path, encryptionSecret, null, false, contentType, { enableCache: false });
    const downloadDirectUrl = await generatePresignedUrl(config, fileRecord.storage_path, encryptionSecret, null, true, contentType, { enableCache: false });

    // 构建API路径URL
    const baseUrl = c.req.url.split("/api/")[0];
    const previewProxyUrl = `${baseUrl}/api/file-view/${fileRecord.slug}`;
    const downloadProxyUrl = `${baseUrl}/api/file-download/${fileRecord.slug}`;

    // 如果有密码，添加密码参数
    const previewProxyUrlWithPassword = params.password ? `${previewProxyUrl}?password=${encodeURIComponent(params.password)}` : previewProxyUrl;
    const downloadProxyUrlWithPassword = params.password ? `${downloadProxyUrl}?password=${encodeURIComponent(params.password)}` : downloadProxyUrl;

    // 计算过期时间
    let expiresAt = null;
    if (params.expires_in > 0) {
      const expiryDate = new Date();
      expiryDate.setHours(expiryDate.getHours() + params.expires_in);
      expiresAt = expiryDate.toISOString();
    }

    return c.json({
      code: ApiStatus.SUCCESS,
      message: "文件上传成功",
      data: {
        id: fileRecord.id,
        slug: fileRecord.slug,
        filename,
        mimetype: contentType,
        size: fileSize,
        remark: params.remark,
        created_at: new Date().toISOString(),
        requires_password: !!params.password,
        views: 0,
        max_views: params.max_views > 0 ? params.max_views : null,
        expires_at: expiresAt,
        // 访问URL - 根据use_proxy设置选择
        previewUrl: params.use_proxy ? previewProxyUrlWithPassword : previewDirectUrl,
        downloadUrl: params.use_proxy ? downloadProxyUrlWithPassword : downloadDirectUrl,
        // 直接S3访问URL (预签名)
        s3_direct_preview_url: previewDirectUrl,
        s3_direct_download_url: downloadDirectUrl,
        // 代理访问URL (通过服务器)
        proxy_preview_url: previewProxyUrlWithPassword,
        proxy_download_url: downloadProxyUrlWithPassword,
        // 其他信息
        use_proxy: params.use_proxy,
        created_by: isAdmin ? userId : `apikey:${userId}`,
        used_original_filename: params.original_filename,
      },
      success: true,
    });
  } catch (error) {
    console.error("直接上传文件错误:", error);

    // 处理特定错误类型
    if (error.message.includes("链接后缀已被占用")) {
      return c.json(createErrorResponse(ApiStatus.CONFLICT, error.message), ApiStatus.CONFLICT);
    }
    if (error.message.includes("API密钥用户只能使用公开")) {
      return c.json(createErrorResponse(ApiStatus.FORBIDDEN, error.message), ApiStatus.FORBIDDEN);
    }
    if (error.message.includes("没有可用的S3配置")) {
      return c.json(createErrorResponse(ApiStatus.BAD_REQUEST, error.message), ApiStatus.BAD_REQUEST);
    }

    return c.json(createErrorResponse(ApiStatus.INTERNAL_ERROR, "上传文件失败: " + error.message), ApiStatus.INTERNAL_ERROR);
  }
});

export default app;
