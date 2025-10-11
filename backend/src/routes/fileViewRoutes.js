/**
 * 文件查看路由
 * 处理文件分享的查看、下载、预览功能
 */
import { Hono } from "hono";
import { RepositoryFactory } from "../repositories/index.js";
import { verifyPassword } from "../utils/crypto.js";
import { generatePresignedUrl } from "../utils/s3Utils.js";
import { isOfficeFile } from "../utils/fileUtils.js";
import { handleFileDownload } from "../services/fileViewService.js";
import { getFileBySlug, isFileAccessible } from "../services/fileService.js";

const app = new Hono();

// ==========================================
// 路由处理器
// ==========================================

// 处理API路径下的文件下载请求 /api/file-download/:slug
app.get("/api/file-download/:slug", async (c) => {
  const slug = c.req.param("slug");
  return await handleFileDownload(slug, c.env, c.req.raw, true); // 强制下载
});

// 处理API路径下的文件预览请求 /api/file-view/:slug
app.get("/api/file-view/:slug", async (c) => {
  const slug = c.req.param("slug");
  return await handleFileDownload(slug, c.env, c.req.raw, false); // 预览
});

// 处理Office文件直接预览URL请求 /api/office-preview/:slug
app.get("/api/office-preview/:slug", async (c) => {
  const slug = c.req.param("slug");
  const db = c.env.DB;
  const encryptionSecret = c.env.ENCRYPTION_SECRET || "default-encryption-key";

  try {
    // 查询文件详情
    const file = await getFileBySlug(db, slug, encryptionSecret);

    // 检查文件是否存在
    if (!file) {
      return c.json({ error: "文件不存在" }, 404);
    }

    // 检查文件是否受密码保护
    if (file.password) {
      // 如果有密码，检查URL中是否包含密码参数
      const url = new URL(c.req.url);
      const passwordParam = url.searchParams.get("password");

      if (!passwordParam) {
        return c.json({ error: "需要密码访问此文件" }, 401);
      }

      // 验证密码
      const passwordValid = await verifyPassword(passwordParam, file.password);
      if (!passwordValid) {
        return c.json({ error: "密码错误" }, 401);
      }
    }

    // 检查文件是否可访问
    const accessCheck = await isFileAccessible(db, file, encryptionSecret);
    if (!accessCheck.accessible) {
      if (accessCheck.reason === "expired") {
        return c.json({ error: "文件已过期" }, 410);
      }
      return c.json({ error: "文件不可访问" }, 403);
    }

    // 检查文件是否为Office文件
    const isOffice = isOfficeFile(file.mimetype, file.filename);
    if (!isOffice) {
      return c.json({ error: "不是Office文件类型" }, 400);
    }

    // 检查文件存储信息
    if (!file.storage_config_id || !file.storage_path || !file.storage_type) {
      return c.json({ error: "文件存储信息不完整" }, 404);
    }

    // 检查存储类型
    if (file.storage_type !== "S3") {
      return c.json({ error: "暂不支持此存储类型的Office预览" }, 501);
    }

    // 获取S3配置
    const repositoryFactory = new RepositoryFactory(db);
    const s3ConfigRepository = repositoryFactory.getS3ConfigRepository();
    const s3Config = await s3ConfigRepository.findById(file.storage_config_id);
    if (!s3Config) {
      return c.json({ error: "无法获取存储配置信息" }, 500);
    }

    // 计算访问次数（暂不增加计数器，因为这只是获取URL）
    // 但需要考虑已有的访问次数
    if (file.max_views && file.max_views > 0 && file.views >= file.max_views) {
      return c.json({ error: "文件已达到最大查看次数" }, 410);
    }

    try {
      // Office预览使用S3配置的默认时效
      // 生成临时预签名URL，适用于Office预览
      // 注意：Office预览没有用户上下文，禁用缓存避免权限泄露
      const presignedUrl = await generatePresignedUrl(s3Config, file.storage_path, encryptionSecret, null, false, file.mimetype, { enableCache: false });

      // 返回直接访问URL
      return c.json({
        url: presignedUrl,
        filename: file.filename,
        mimetype: file.mimetype,
        expires_in: s3Config.signature_expires_in || 3600,
        is_temporary: true,
      });
    } catch (error) {
      console.error("生成Office预览URL出错:", error);
      return c.json({ error: "生成预览URL失败: " + error.message }, 500);
    }
  } catch (error) {
    console.error("处理Office预览URL请求错误:", error);
    return c.json({ error: "服务器处理错误: " + error.message }, 500);
  }
});

export default app;
