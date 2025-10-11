/**
 * 文件查看服务层
 * 负责文件分享查看、下载、预览相关的业务逻辑
 */

import { RepositoryFactory } from "../repositories/index.js";
import { verifyPassword } from "../utils/crypto.js";
import { generatePresignedUrl, deleteFileFromS3 } from "../utils/s3Utils.js";
import { getEffectiveMimeType, getContentTypeAndDisposition } from "../utils/fileUtils.js";
import { getFileBySlug, isFileAccessible } from "./fileService.js";

/**
 * 文件查看服务类
 */
export class FileViewService {
  /**
   * 构造函数
   * @param {D1Database} db - 数据库实例
   * @param {string} encryptionSecret - 加密密钥
   */
  constructor(db, encryptionSecret) {
    this.db = db;
    this.encryptionSecret = encryptionSecret;
  }

  /**
   * 增加文件查看次数并检查是否超过限制
   * @param {Object} file - 文件对象
   * @returns {Promise<Object>} 包含更新后的文件信息和状态
   */
  async incrementAndCheckFileViews(file) {
    // 使用 FileRepository 递增访问计数
    const repositoryFactory = new RepositoryFactory(this.db);
    const fileRepository = repositoryFactory.getFileRepository();

    await fileRepository.incrementViews(file.id);

    // 重新获取更新后的文件信息（包含存储配置）
    const updatedFile = await fileRepository.findByIdWithStorageConfig(file.id);

    // 检查是否超过最大访问次数
    if (updatedFile.max_views && updatedFile.max_views > 0 && updatedFile.views > updatedFile.max_views) {
      // 已超过最大查看次数，执行删除
      await this.checkAndDeleteExpiredFile(updatedFile);
      return {
        isExpired: true,
        reason: "max_views",
        file: updatedFile,
      };
    }

    return {
      isExpired: false,
      file: updatedFile,
    };
  }

  /**
   * 检查并删除过期文件
   * @param {Object} file - 文件对象
   */
  async checkAndDeleteExpiredFile(file) {
    try {
      console.log(`开始删除过期文件: ${file.id}`);

      // 从S3删除文件
      if (file.storage_path && file.storage_config_id) {
        const repositoryFactory = new RepositoryFactory(this.db);
        const s3ConfigRepository = repositoryFactory.getS3ConfigRepository();
        const s3Config = await s3ConfigRepository.findById(file.storage_config_id);

        if (s3Config) {
          await deleteFileFromS3(s3Config, file.storage_path, this.encryptionSecret);
          console.log(`已从S3删除文件: ${file.storage_path}`);
        }
      }

      // 从数据库删除文件记录
      const repositoryFactory = new RepositoryFactory(this.db);
      const fileRepository = repositoryFactory.getFileRepository();
      await fileRepository.deleteFile(file.id);

      console.log(`已从数据库删除文件记录: ${file.id}`);
    } catch (error) {
      console.error(`删除过期文件失败 (${file.id}):`, error);
      throw error;
    }
  }

  /**
   * 处理文件下载请求
   * @param {string} slug - 文件slug
   * @param {Request} request - 原始请求
   * @param {boolean} forceDownload - 是否强制下载
   * @returns {Promise<Response>} 响应对象
   */
  async handleFileDownload(slug, request, forceDownload = false) {
    try {
      // 查询文件详情
      const file = await getFileBySlug(this.db, slug, this.encryptionSecret);

      // 检查文件是否存在
      if (!file) {
        return new Response("文件不存在", { status: 404 });
      }

      // 检查文件是否受密码保护
      if (file.password) {
        // 如果有密码，检查URL中是否包含密码参数
        const url = new URL(request.url);
        const passwordParam = url.searchParams.get("password");

        if (!passwordParam) {
          return new Response("需要密码访问此文件", { status: 401 });
        }

        // 验证密码
        const passwordValid = await verifyPassword(passwordParam, file.password);
        if (!passwordValid) {
          return new Response("密码错误", { status: 401 });
        }
      }

      // 检查文件是否可访问
      const accessCheck = await isFileAccessible(this.db, file, this.encryptionSecret);
      if (!accessCheck.accessible) {
        if (accessCheck.reason === "expired") {
          return new Response("文件已过期", { status: 410 });
        }
        return new Response("文件不可访问", { status: 403 });
      }

      // 文件预览和下载端点默认不增加访问计数
      let result = { isExpired: false, file };

      // 如果文件已到达最大访问次数限制
      if (result.isExpired) {
        console.log(`文件(${file.id})已达到最大查看次数，准备删除...`);
        try {
          // 使用 FileRepository 再次检查文件是否被成功删除
          const repositoryFactory = new RepositoryFactory(this.db);
          const fileRepository = repositoryFactory.getFileRepository();

          const fileStillExists = await fileRepository.findById(file.id);
          if (fileStillExists) {
            console.log(`文件(${file.id})仍然存在，再次尝试删除...`);
            await this.checkAndDeleteExpiredFile(result.file);
          }
        } catch (error) {
          console.error(`尝试再次删除文件(${file.id})时出错:`, error);
        }
        return new Response("文件已达到最大查看次数", { status: 410 });
      }

      // 检查文件存储信息
      if (!result.file.storage_config_id || !result.file.storage_path || !result.file.storage_type) {
        return new Response("文件存储信息不完整", { status: 404 });
      }

      // 检查存储类型
      if (result.file.storage_type !== "S3") {
        return new Response("暂不支持此存储类型的文件下载", { status: 501 });
      }

      // 获取S3配置
      const repositoryFactory = new RepositoryFactory(this.db);
      const s3ConfigRepository = repositoryFactory.getS3ConfigRepository();
      const s3Config = await s3ConfigRepository.findById(result.file.storage_config_id);
      if (!s3Config) {
        return new Response("无法获取存储配置信息", { status: 500 });
      }

      // 获取文件的MIME类型
      const contentType = getEffectiveMimeType(result.file.mimetype, result.file.filename);

      // 生成预签名URL，使用S3配置的默认时效，传递MIME类型以确保正确的Content-Type
      // 注意：文件分享页面没有用户上下文，禁用缓存避免权限泄露
      const presignedUrl = await generatePresignedUrl(s3Config, result.file.storage_path, this.encryptionSecret, null, forceDownload, contentType, { enableCache: false });

      //处理Range请求
      const rangeHeader = request.headers.get("Range");
      const fileRequestHeaders = {};

      // 如果有Range请求，转发给S3
      if (rangeHeader) {
        fileRequestHeaders["Range"] = rangeHeader;
        console.log(`🎬 代理Range请求: ${rangeHeader}`);
      }

      // 代理请求到实际的文件URL
      const fileRequest = new Request(presignedUrl, {
        headers: fileRequestHeaders,
      });

      const fileResponse = await fetch(fileRequest);

      if (!fileResponse.ok) {
        console.error(`获取文件失败: ${fileResponse.status} ${fileResponse.statusText}`);
        return new Response("获取文件失败", { status: fileResponse.status });
      }

      // 获取内容类型和处置方式
      const { contentType: finalContentType, contentDisposition } = getContentTypeAndDisposition(result.file.filename, result.file.mimetype, { forceDownload: forceDownload });

      // 创建响应头
      const responseHeaders = new Headers();

      // 设置内容类型
      responseHeaders.set("Content-Type", finalContentType);

      // 设置内容处置
      responseHeaders.set("Content-Disposition", contentDisposition);

      // 复制原始响应的其他相关头部
      const headersToProxy = ["Content-Length", "Content-Range", "Accept-Ranges", "Last-Modified", "ETag", "Cache-Control"];
      headersToProxy.forEach((header) => {
        const value = fileResponse.headers.get(header);
        if (value) {
          responseHeaders.set(header, value);
        }
      });

      // 设置CORS头部
      responseHeaders.set("Access-Control-Allow-Origin", "*");
      responseHeaders.set("Access-Control-Allow-Methods", "GET, HEAD, OPTIONS");
      responseHeaders.set("Access-Control-Allow-Headers", "Range, Content-Type");
      responseHeaders.set("Access-Control-Expose-Headers", "Content-Length, Content-Range, Accept-Ranges");

      // 返回代理响应
      return new Response(fileResponse.body, {
        status: fileResponse.status,
        statusText: fileResponse.statusText,
        headers: responseHeaders,
      });
    } catch (error) {
      console.error("代理文件下载出错:", error);
      return new Response("获取文件失败: " + error.message, { status: 500 });
    }
  }
}

// 导出便捷函数供路由使用
export async function handleFileDownload(slug, env, request, forceDownload = false) {
  const service = new FileViewService(env.DB, env.ENCRYPTION_SECRET || "default-encryption-key");
  return await service.handleFileDownload(slug, request, forceDownload);
}

export async function checkAndDeleteExpiredFile(db, file, encryptionSecret) {
  const service = new FileViewService(db, encryptionSecret);
  return await service.checkAndDeleteExpiredFile(file);
}
