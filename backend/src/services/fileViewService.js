/**
 * 文件查看服务层
 * 负责文件分享查看、下载、预览相关的业务逻辑
 */

import { ensureRepositoryFactory } from "../utils/repositories.js";
import { verifyPassword } from "../utils/crypto.js";
import { verifyShareAccessToken } from "../utils/shareAccessToken.js";
import { parseEncryptionMeta } from "../utils/fileEncryption.js";
import { getEffectiveMimeType, getContentTypeAndDisposition } from "../utils/fileUtils.js";
import { getFileBySlug, isFileAccessible } from "./fileService.js";
import { ObjectStore } from "../storage/object/ObjectStore.js";
import { StorageStreaming, STREAMING_CHANNELS } from "../storage/streaming/index.js";
import { StorageFactory } from "../storage/factory/StorageFactory.js";
import { FILE_TYPES, UserType } from "../constants/index.js";

/**
 * 文件查看服务类
 */
export class FileViewService {
  /**
   * 构造函数
   * @param {D1Database} db - 数据库实例
   * @param {string} encryptionSecret - 加密密钥
   */
  constructor(db, encryptionSecret, repositoryFactory = null) {
    this.db = db;
    this.encryptionSecret = encryptionSecret;
    this.repositoryFactory = ensureRepositoryFactory(db, repositoryFactory);
  }

  /**
   * 检查并删除过期文件
   * @param {Object} file - 文件对象
   */
  async checkAndDeleteExpiredFile(file) {
    try {
      console.log(`开始删除过期文件: ${file.id}`);

      // 通过 ObjectStore 按存储路径删除对象
      //
      // 1) “上传即分享”（share upload / storage-first）：file_path 通常为空，这类过期应删除真实存储对象
      // 2) “从文件系统创建分享”（fs -> share）：file_path 有值，表示引用的是网盘里的真实文件
      //    这种场景下“分享过期”只应该删除分享记录，不应该删除真实网盘文件。
      const shouldDeleteStorageObject = !file.file_path;

      if (shouldDeleteStorageObject && file.storage_path && file.storage_config_id && file.storage_type) {
        try {
          const objectStore = new ObjectStore(this.db, this.encryptionSecret, this.repositoryFactory);
          await objectStore.deleteByStoragePath(file.storage_config_id, file.storage_path, { db: this.db });
          console.log(`已从存储删除文件: ${file.storage_path}`);
        } catch (e) {
        console.warn("删除存储对象失败（已忽略以完成记录删除）:", e?.message || e);
        }
      }

      // 从数据库删除文件记录
      const fileRepository = this.repositoryFactory.getFileRepository();
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
  async handleFileDownload(slug, request, forceDownload = false, options = {}) {
    try {
      // 查询文件详情
      const file = await getFileBySlug(this.db, slug, this.encryptionSecret);

      // 检查文件是否存在
      if (!file) {
        return new Response("文件不存在", { status: 404 });
      }

      // 检查文件是否受密码保护
      if (file.password) {
        const url = new URL(request.url);
        // 优先校验短时效访问令牌（?at=，由密码验证接口签发，避免密码进 URL）
        let authorized = false;
        const atToken = url.searchParams.get("at");
        if (atToken) {
          authorized = await verifyShareAccessToken(this.encryptionSecret, atToken, { type: "file", slug });
        }

        // 兼容旧链接：?password= 明文参数
        if (!authorized) {
          const passwordParam = url.searchParams.get("password");
          if (!passwordParam) {
            return new Response("需要密码访问此文件", { status: 401 });
          }
          authorized = await verifyPassword(passwordParam, file.password);
        }

        if (!authorized) {
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
          const fileRepository = this.repositoryFactory.getFileRepository();

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

      const fileRecord = result.file;
      const useProxyFlag = fileRecord.use_proxy ?? 0;
      // 加密文件（server/e2e）一律强制本地代理：server 模式由代理层解密，e2e 模式保证前端同源取密文
      const encryptionMeta = parseEncryptionMeta(fileRecord.encryption_meta || null);
      const forceProxy = (options && options.forceProxy === true) || !!encryptionMeta;

      // 文本类预览优先走本地代理，以避免直链 CORS 与内容类型差异
      const isInline = !forceDownload;
      const isTextLike =
        fileRecord.type === FILE_TYPES.TEXT ||
        (fileRecord.mimetype && fileRecord.mimetype.startsWith("text/"));

      // 抽取本地代理下载逻辑，便于在直链失败时复用
      // 使用 StorageStreaming 层统一处理
      const proxyDownload = async () => {
        const parseOwnerFromCreatedBy = (createdBy) => {
          const raw = typeof createdBy === "string" ? createdBy.trim() : "";
          if (!raw || raw === "anonymous") return null;
          if (raw.startsWith("apikey:")) {
            const id = raw.slice("apikey:".length).trim();
            if (!id) return null;
            return { ownerType: UserType.API_KEY, ownerId: id };
          }
          // 默认视为 admin 创建的分享
          return { ownerType: UserType.ADMIN, ownerId: raw };
        };

        const owner = parseOwnerFromCreatedBy(fileRecord.created_by);

        // 处理 Range 请求
        const rangeHeader = request.headers.get("Range");
        if (rangeHeader) {
          console.log(`分享下载 - 代理 Range 请求: ${rangeHeader}`);
        }

        // 使用 StorageStreaming 层统一处理内容访问
        const streaming = new StorageStreaming({
          mountManager: null, // 存储路径模式不需要 mountManager
          storageFactory: StorageFactory,
          encryptionSecret: this.encryptionSecret,
        });

        const response = await streaming.createResponse({
          path: fileRecord.storage_path,
          channel: STREAMING_CHANNELS.SHARE,
          storageConfigId: fileRecord.storage_config_id,
          rangeHeader,
          request,
          db: this.db,
          repositoryFactory: this.repositoryFactory,
          encryptionMeta: encryptionMeta && encryptionMeta.mode === "server" ? encryptionMeta : null,
          ...(owner ? owner : null),
        });

        // 基于文件记录重新计算 Content-Type / Content-Disposition，保持分享层一致性
        const { contentType: finalContentType, contentDisposition } = getContentTypeAndDisposition(
          fileRecord.filename,
          fileRecord.mimetype,
          { forceDownload }
        );

        // 更新响应头
        response.headers.set("Content-Type", finalContentType);
        response.headers.set("Content-Disposition", contentDisposition);

        // 设置CORS头部
        response.headers.set("Access-Control-Allow-Origin", "*");
        response.headers.set("Access-Control-Allow-Methods", "GET, HEAD, OPTIONS");
        response.headers.set("Access-Control-Allow-Headers", "Range, Content-Type");
        response.headers.set("Access-Control-Expose-Headers", "Content-Length, Content-Range, Accept-Ranges");

        return response;
      };

      // forceProxy=true 时，强制只走本地代理（share 的 /api/s 与 /api/share/content）
      if (forceProxy) {
        return await proxyDownload();
      }

      // 文本类 inline 预览，无论 use_proxy 配置如何，都优先走本地代理访问
      if (isInline && isTextLike) {
        return await proxyDownload();
      }

      // use_proxy = 1 时，走本地代理访问
      if (useProxyFlag === 1) {
        return await proxyDownload();
      }

      // use_proxy != 1 时，优先尝试直链：S3 custom_host 优先，其次驱动 DirectLink 能力（例如预签名 URL）
      let directUrl = null;
      try {
        const objectStore = new ObjectStore(this.db, this.encryptionSecret, this.repositoryFactory);
        const links = await objectStore.generateLinksByStoragePath(fileRecord.storage_config_id, fileRecord.storage_path, {
          forceDownload,
        });
        directUrl = links?.download?.url || links?.preview?.url || null;
      } catch (e) {
        console.error("生成存储直链失败:", e);
      }

      if (directUrl) {
        const redirectHeaders = new Headers();
        redirectHeaders.set("Location", directUrl);

        return new Response(null, {
          status: 302,
          headers: redirectHeaders,
        });
      }

      // 直链不可用时回退为本地代理访问，避免 501，保证“反代访问”场景下始终可用
      return await proxyDownload();
    } catch (error) {
      console.error("代理文件下载出错:", error);
      return new Response("获取文件失败: " + error.message, { status: 500 });
    }
  }
}

// 导出便捷函数供路由使用
export async function handleFileDownload(
  slug,
  db,
  encryptionSecret,
  request,
  forceDownload = false,
  repositoryFactory = null,
  options = {},
) {
  const service = new FileViewService(db, encryptionSecret, repositoryFactory);
  return service.handleFileDownload(slug, request, forceDownload, options);
}

export async function checkAndDeleteExpiredFile(db, file, encryptionSecret, repositoryFactory = null) {
  const service = new FileViewService(db, encryptionSecret, repositoryFactory);
  return await service.checkAndDeleteExpiredFile(file);
}
