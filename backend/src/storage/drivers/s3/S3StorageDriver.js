/**
 * S3存储驱动实现协调各个操作模块提供统一的存储接口
 * 实现所有能力接口
 */

import { BaseDriver } from "../../interfaces/capabilities/BaseDriver.js";
import { CAPABILITIES } from "../../interfaces/capabilities/index.js";
import { HTTPException } from "hono/http-exception";
import { ApiStatus } from "../../../constants/index.js";
import { createS3Client } from "../../../utils/s3Utils.js";
import { normalizeS3SubPath, isCompleteFilePath } from "./utils/S3PathUtils.js";
import { updateMountLastUsed, findMountPointByPath } from "../../fs/utils/MountResolver.js";
import { buildFullProxyUrl, buildSignedProxyUrl } from "../../../constants/proxy.js";
import { ProxySignatureService } from "../../../services/ProxySignatureService.js";

// 导入各个操作模块
import { S3FileOperations } from "./operations/S3FileOperations.js";
import { S3DirectoryOperations } from "./operations/S3DirectoryOperations.js";
import { S3BatchOperations } from "./operations/S3BatchOperations.js";
import { S3UploadOperations } from "./operations/S3UploadOperations.js";
import { S3SearchOperations } from "./operations/S3SearchOperations.js";

export class S3StorageDriver extends BaseDriver {
  /**
   * 构造函数
   * @param {Object} config - S3配置对象
   * @param {string} encryptionSecret - 加密密钥
   */
  constructor(config, encryptionSecret) {
    super(config);
    this.type = "S3";
    this.encryptionSecret = encryptionSecret;
    this.s3Client = null;

    // S3存储驱动支持所有能力
    this.capabilities = [
      CAPABILITIES.READER, // 读取能力：list, get, getInfo
      CAPABILITIES.WRITER, // 写入能力：put, mkdir, remove
      CAPABILITIES.PRESIGNED, // 预签名URL能力：generatePresignedUrl
      CAPABILITIES.MULTIPART, // 分片上传能力：multipart upload
      CAPABILITIES.ATOMIC, // 原子操作能力：rename, copy
      CAPABILITIES.PROXY, // 代理能力：generateProxyUrl
    ];

    // 操作模块实例
    this.fileOps = null;
    this.directoryOps = null;
    this.batchOps = null;
    this.uploadOps = null;
    this.backendMultipartOps = null;
    this.searchOps = null;
  }

  /**
   * 初始化S3存储驱动
   * @returns {Promise<void>}
   */
  async initialize() {
    try {
      // 创建S3客户端
      this.s3Client = await createS3Client(this.config, this.encryptionSecret);

      // 初始化各个操作模块
      this.fileOps = new S3FileOperations(this.s3Client, this.config, this.encryptionSecret, this);
      this.directoryOps = new S3DirectoryOperations(this.s3Client, this.config, this.encryptionSecret);
      this.batchOps = new S3BatchOperations(this.s3Client, this.config, this.encryptionSecret);
      this.uploadOps = new S3UploadOperations(this.s3Client, this.config, this.encryptionSecret);
      // this.backendMultipartOps = new S3BackendMultipartOperations(this.s3Client, this.config, this.encryptionSecret); // 已废弃，使用前端分片上传
      this.searchOps = new S3SearchOperations(this.s3Client, this.config, this.encryptionSecret);

      this.initialized = true;
      console.log(`S3存储驱动初始化成功: ${this.config.name} (${this.config.provider_type})`);
    } catch (error) {
      console.error("S3存储驱动初始化失败:", error);
      throw new HTTPException(ApiStatus.INTERNAL_ERROR, {
        message: `S3存储驱动初始化失败: ${error.message}`,
      });
    }
  }

  /**
   * 列出目录内容
   * @param {string} path - 目录路径
   * @param {Object} options - 选项参数
   * @returns {Promise<Object>} 目录内容
   */
  async listDirectory(path, options = {}) {
    this._ensureInitialized();

    const { mount, subPath, db } = options;

    // 规范化S3子路径
    const s3SubPath = normalizeS3SubPath(subPath, true);

    // 更新挂载点的最后使用时间
    if (db && mount.id) {
      await updateMountLastUsed(db, mount.id);
    }

    // 委托给目录操作模块，传递所有选项参数
    return await this.directoryOps.listDirectory(s3SubPath, {
      mount,
      subPath, // 使用正确的子路径用于缓存键生成
      path,
      db,
      ...options,
    });
  }

  /**
   * 获取文件信息
   * @param {string} path - 文件路径
   * @param {Object} options - 选项参数
   * @returns {Promise<Object>} 文件信息
   */
  async getFileInfo(path, options = {}) {
    this._ensureInitialized();

    const { mount, subPath, db, userType, userId, request } = options;

    // 规范化S3子路径
    const s3SubPath = normalizeS3SubPath(subPath, false);

    // 更新挂载点的最后使用时间
    if (db && mount.id) {
      await updateMountLastUsed(db, mount.id);
    }

    // 特殊处理：当s3SubPath为空字符串时（访问挂载点根目录），直接作为目录处理，跳过文件检查
    // 因为S3对象Key不能为空字符串，所以空字符串永远不可能是有效的文件
    if (s3SubPath === "") {
      console.log(`getFileInfo - 检测到挂载点根目录访问，直接作为目录处理: ${path}`);
      return await this.directoryOps.getDirectoryInfo(s3SubPath, {
        mount,
        path,
      });
    }

    try {
      // 首先尝试作为文件获取信息
      return await this.fileOps.getFileInfo(s3SubPath, {
        mount,
        path,
        userType,
        userId,
        request,
        db,
      });
    } catch (error) {
      if (error.status === ApiStatus.NOT_FOUND) {
        // 如果文件不存在，尝试作为目录处理
        try {
          return await this.directoryOps.getDirectoryInfo(s3SubPath, {
            mount,
            path,
          });
        } catch (dirError) {
          // 如果目录也不存在，抛出原始错误
          throw error;
        }
      }
      throw error;
    }
  }

  /**
   * 下载文件
   * @param {string} path - 文件路径
   * @param {Object} options - 选项参数
   * @returns {Promise<Response>} 文件内容响应
   */
  async downloadFile(path, options = {}) {
    this._ensureInitialized();

    const { mount, subPath, db, request } = options;

    // 规范化S3子路径
    const s3SubPath = normalizeS3SubPath(subPath, false);

    // 更新挂载点的最后使用时间
    if (db && mount.id) {
      await updateMountLastUsed(db, mount.id);
    }

    // 提取文件名
    const fileName = path.split("/").filter(Boolean).pop() || "file";

    // 委托给文件操作模块
    return await this.fileOps.downloadFile(s3SubPath, fileName, request);
  }

  /**
   * 上传文件
   * @param {string} path - 目标路径
   * @param {File} file - 文件对象
   * @param {Object} options - 选项参数
   * @returns {Promise<Object>} 上传结果
   */
  async uploadFile(path, file, options = {}) {
    this._ensureInitialized();

    const { mount, subPath, db, userIdOrInfo, userType, useMultipart = true } = options;

    // 规范化S3子路径
    const s3SubPath = normalizeS3SubPath(subPath, false);

    if (useMultipart) {
      // 使用分片上传
      return await this.uploadOps.initializeFrontendMultipartUpload(s3SubPath, {
        fileName: file.name,
        fileSize: file.size,
        mount,
        db,
        userIdOrInfo,
        userType,
      });
    } else {
      // 使用直接上传
      return await this.uploadOps.uploadFile(s3SubPath, file, {
        mount,
        db,
        userIdOrInfo,
        userType,
      });
    }
  }

  /**
   * 上传流式数据
   * @param {string} path - 目标路径
   * @param {ReadableStream} stream - 数据流
   * @param {Object} options - 选项参数
   * @returns {Promise<Object>} 上传结果
   */
  async uploadStream(path, stream, options = {}) {
    this._ensureInitialized();

    const { mount, subPath, db, userIdOrInfo, userType, filename, contentType, contentLength, useMultipart = false } = options;

    // 规范化S3子路径
    const s3SubPath = normalizeS3SubPath(subPath, false);

    // 构建完整的S3 key
    const s3Key = this._normalizeFilePath(s3SubPath, path, filename);

    // 委托给上传操作模块
    return await this.uploadOps.uploadStream(s3Key, stream, {
      mount,
      db,
      userIdOrInfo,
      userType,
      filename,
      contentType,
      contentLength,
      useMultipart,
    });
  }

  /**
   * 创建目录
   * @param {string} path - 目录路径
   * @param {Object} options - 选项参数
   * @returns {Promise<Object>} 创建结果
   */
  async createDirectory(path, options = {}) {
    this._ensureInitialized();

    const { mount, subPath, db } = options;

    // 规范化S3子路径
    const s3SubPath = normalizeS3SubPath(subPath, true);

    // 更新挂载点的最后使用时间
    if (db && mount.id) {
      await updateMountLastUsed(db, mount.id);
    }

    // 委托给目录操作模块
    return await this.directoryOps.createDirectory(s3SubPath, {
      mount,
      subPath,
      path,
    });
  }

  /**
   * 重命名文件或目录
   * @param {string} oldPath - 原路径
   * @param {string} newPath - 新路径
   * @param {Object} options - 选项参数
   * @returns {Promise<Object>} 重命名结果
   */
  async renameItem(oldPath, newPath, options = {}) {
    this._ensureInitialized();

    // 委托给批量操作模块
    return await this.batchOps.renameItem(oldPath, newPath, {
      ...options,
      findMountPointByPath,
    });
  }

  /**
   * 批量删除文件
   * @param {Array<string>} paths - 路径数组
   * @param {Object} options - 选项参数
   * @returns {Promise<Object>} 批量删除结果
   */
  async batchRemoveItems(paths, options = {}) {
    this._ensureInitialized();

    // 委托给批量操作模块
    return await this.batchOps.batchRemoveItems(paths, {
      ...options,
      findMountPointByPath,
    });
  }

  /**
   * 复制文件或目录
   * @param {string} sourcePath - 源路径
   * @param {string} targetPath - 目标路径
   * @param {Object} options - 选项参数
   * @returns {Promise<Object>} 复制结果
   */
  async copyItem(sourcePath, targetPath, options = {}) {
    this._ensureInitialized();

    // 委托给批量操作模块
    return await this.batchOps.copyItem(sourcePath, targetPath, {
      ...options,
      findMountPointByPath,
    });
  }

  /**
   * 批量复制文件
   * @param {Array<Object>} items - 复制项数组
   * @param {Object} options - 选项参数
   * @returns {Promise<Object>} 批量复制结果
   */
  async batchCopyItems(items, options = {}) {
    this._ensureInitialized();

    // 委托给批量操作模块
    return await this.batchOps.batchCopyItems(items, {
      ...options,
      findMountPointByPath,
    });
  }

  /**
   * 生成预签名URL
   * @param {string} path - 文件路径
   * @param {Object} options - 选项参数
   * @param {string} options.operation - 操作类型：'download' 或 'upload'
   * @returns {Promise<Object>} 预签名URL信息
   */
  async generatePresignedUrl(path, options = {}) {
    this._ensureInitialized();

    const { mount, subPath, db, operation = "download" } = options;

    // 规范化S3子路径
    const s3SubPath = normalizeS3SubPath(subPath, false);

    // 更新挂载点的最后使用时间
    if (db && mount.id) {
      await updateMountLastUsed(db, mount.id);
    }

    // 根据操作类型委托给不同的模块
    if (operation === "upload") {
      const { fileName, fileSize, expiresIn } = options;
      return await this.uploadOps.generatePresignedUploadUrl(s3SubPath, {
        fileName,
        fileSize,
        expiresIn,
      });
    } else {
      const { expiresIn, forceDownload, userType, userId } = options;
      return await this.fileOps.generatePresignedUrl(s3SubPath, {
        expiresIn,
        forceDownload,
        userType,
        userId,
        mount,
      });
    }
  }

  /**
   * 更新文件内容
   * @param {string} path - 文件路径
   * @param {string} content - 新内容
   * @param {Object} options - 选项参数
   * @returns {Promise<Object>} 更新结果
   */
  async updateFile(path, content, options = {}) {
    this._ensureInitialized();

    const { mount, subPath, db } = options;

    // 规范化S3子路径
    const s3SubPath = normalizeS3SubPath(subPath, false);

    // 提取文件名
    const fileName = path.split("/").filter(Boolean).pop() || "file";

    // 委托给文件操作模块
    const result = await this.fileOps.updateFile(s3SubPath, content, {
      fileName,
    });

    // 处理缓存清理
    const { clearDirectoryCache } = await import("../../../cache/index.js");
    if (mount && mount.cache_ttl > 0) {
      await clearDirectoryCache({ mountId: mount.id });
    }

    // 更新挂载点的最后使用时间
    if (db && mount.id) {
      await updateMountLastUsed(db, mount.id);
    }

    return result;
  }

  /**
   * 处理跨存储复制
   * @param {string} sourcePath - 源路径
   * @param {string} targetPath - 目标路径
   * @param {Object} options - 选项参数
   * @returns {Promise<Object>} 跨存储复制结果
   */
  async handleCrossStorageCopy(sourcePath, targetPath, options = {}) {
    this._ensureInitialized();

    // 委托给批量操作模块
    return await this.batchOps.handleCrossStorageCopy(options.db, sourcePath, targetPath, options.userIdOrInfo, options.userType);
  }

  /**
   * 检查路径是否存在
   * @param {string} path - 路径
   * @param {Object} options - 选项参数
   * @returns {Promise<boolean>} 是否存在
   */
  async exists(path, options = {}) {
    this._ensureInitialized();

    const { subPath } = options;

    // 规范化S3子路径
    const s3SubPath = normalizeS3SubPath(subPath, false);

    // 委托给文件操作模块检查存在性
    return await this.fileOps.exists(s3SubPath);
  }

  /**
   * 搜索文件
   * @param {string} query - 搜索查询
   * @param {Object} options - 搜索选项
   * @param {Object} options.mount - 挂载点对象
   * @param {string} options.searchPath - 搜索路径范围
   * @param {number} options.maxResults - 最大结果数量
   * @param {D1Database} options.db - 数据库实例
   * @returns {Promise<Array>} 搜索结果数组
   */
  async search(query, options = {}) {
    this._ensureInitialized();

    // 委托给搜索操作模块
    return await this.searchOps.searchInMount(query, options);
  }

  /**
   * 获取存储统计信息
   * @returns {Promise<Object>} 统计信息
   */
  async getStats() {
    this._ensureInitialized();

    return {
      type: this.type,
      provider: this.config.provider_type,
      bucket: this.config.bucket_name,
      endpoint: this.config.endpoint_url,
      region: this.config.region || "auto",
      initialized: this.initialized,
    };
  }

  /**
   * 清理资源
   * @returns {Promise<void>}
   */
  async cleanup() {
    this.s3Client = null;
    this.fileOps = null;
    this.directoryOps = null;
    this.batchOps = null;
    this.uploadOps = null;
    this.initialized = false;
    console.log(`S3存储驱动已清理: ${this.config.name}`);
  }

  /**
   * 初始化前端分片上传（生成预签名URL列表）
   * @param {string} subPath - 子路径
   * @param {Object} options - 选项参数
   * @returns {Promise<Object>} 初始化结果
   */
  async initializeFrontendMultipartUpload(subPath, options = {}) {
    this._ensureInitialized();

    const { fileName, fileSize, partSize = 5 * 1024 * 1024, partCount, mount, db, userIdOrInfo, userType } = options;

    // 规范化S3子路径
    const s3SubPath = normalizeS3SubPath(subPath, false);

    // 委托给上传操作模块
    return await this.uploadOps.initializeFrontendMultipartUpload(s3SubPath, {
      fileName,
      fileSize,
      partSize,
      partCount,
      mount,
      db,
      userIdOrInfo,
      userType,
    });
  }

  /**
   * 完成前端分片上传
   * @param {string} subPath - 子路径
   * @param {Object} options - 选项参数
   * @returns {Promise<Object>} 完成结果
   */
  async completeFrontendMultipartUpload(subPath, options = {}) {
    this._ensureInitialized();

    const { uploadId, parts, fileName, fileSize, mount, db, userIdOrInfo, userType } = options;

    // 规范化S3子路径
    const s3SubPath = normalizeS3SubPath(subPath, false);

    // 委托给上传操作模块
    return await this.uploadOps.completeFrontendMultipartUpload(s3SubPath, {
      uploadId,
      parts,
      fileName,
      fileSize,
      mount,
      db,
      userIdOrInfo,
      userType,
    });
  }

  /**
   * 中止前端分片上传
   * @param {string} subPath - 子路径
   * @param {Object} options - 选项参数
   * @returns {Promise<Object>} 中止结果
   */
  async abortFrontendMultipartUpload(subPath, options = {}) {
    this._ensureInitialized();

    const { uploadId, fileName, mount, db, userIdOrInfo, userType } = options;

    // 规范化S3子路径
    const s3SubPath = normalizeS3SubPath(subPath, false);

    // 委托给上传操作模块
    return await this.uploadOps.abortFrontendMultipartUpload(s3SubPath, {
      uploadId,
      fileName,
      mount,
      db,
      userIdOrInfo,
      userType,
    });
  }

  /**
   * 列出进行中的分片上传
   * @param {string} subPath - 子路径（可选，用于过滤特定文件的上传）
   * @param {Object} options - 选项参数
   * @returns {Promise<Object>} 进行中的上传列表
   */
  async listMultipartUploads(subPath = "", options = {}) {
    this._ensureInitialized();

    const { mount, db } = options;

    // 规范化S3子路径
    const s3SubPath = normalizeS3SubPath(subPath, false);

    // 更新挂载点的最后使用时间
    if (db && mount && mount.id) {
      await updateMountLastUsed(db, mount.id);
    }

    // 委托给上传操作模块
    return await this.uploadOps.listMultipartUploads(s3SubPath, options);
  }

  /**
   * 列出已上传的分片
   * @param {string} subPath - 子路径
   * @param {string} uploadId - 上传ID
   * @param {Object} options - 选项参数
   * @returns {Promise<Object>} 已上传的分片列表
   */
  async listMultipartParts(subPath, uploadId, options = {}) {
    this._ensureInitialized();

    const { mount, db } = options;

    // 规范化S3子路径
    const s3SubPath = normalizeS3SubPath(subPath, false);

    // 更新挂载点的最后使用时间
    if (db && mount && mount.id) {
      await updateMountLastUsed(db, mount.id);
    }

    // 委托给上传操作模块
    return await this.uploadOps.listMultipartParts(s3SubPath, uploadId, options);
  }

  /**
   * 为现有上传刷新预签名URL
   * @param {string} subPath - 子路径
   * @param {string} uploadId - 现有的上传ID
   * @param {Array} partNumbers - 需要刷新URL的分片编号数组
   * @param {Object} options - 选项参数
   * @returns {Promise<Object>} 刷新的预签名URL列表
   */
  async refreshMultipartUrls(subPath, uploadId, partNumbers, options = {}) {
    this._ensureInitialized();

    const { mount, db } = options;

    // 规范化S3子路径
    const s3SubPath = normalizeS3SubPath(subPath, false);

    // 更新挂载点的最后使用时间
    if (db && mount && mount.id) {
      await updateMountLastUsed(db, mount.id);
    }

    // 委托给上传操作模块
    return await this.uploadOps.refreshMultipartUrls(s3SubPath, uploadId, partNumbers, options);
  }

  /**
   * 规范化文件路径（用于后端分片上传）
   * @param {string} subPath - 子路径
   * @param {string} path - 完整路径，用于提取文件名
   * @param {string} customFilename - 自定义文件名（可选）
   * @returns {string} 规范化后的S3路径
   * @private
   */
  _normalizeFilePath(subPath, path, customFilename) {
    // 规范化S3子路径 (不添加斜杠，因为是文件)
    let s3SubPath = normalizeS3SubPath(subPath, false);

    // 获取文件名，优先使用自定义文件名，其次从路径中提取
    const fileName = customFilename || path.split("/").filter(Boolean).pop() || "unnamed_file";

    // 智能检查s3SubPath是否已经包含完整的文件路径
    if (s3SubPath && isCompleteFilePath(s3SubPath, fileName)) {
      // 添加root_prefix（如果有）
      const rootPrefix = this.config.root_prefix ? (this.config.root_prefix.endsWith("/") ? this.config.root_prefix : this.config.root_prefix + "/") : "";
      return rootPrefix + s3SubPath;
    }

    // 否则，s3SubPath是目录路径，需要拼接文件名
    // 与目录列表逻辑保持一致，只使用root_prefix
    const rootPrefix = this.config.root_prefix ? (this.config.root_prefix.endsWith("/") ? this.config.root_prefix : this.config.root_prefix + "/") : "";

    let fullPrefix = rootPrefix;

    // 添加s3SubPath (如果不是空)
    if (s3SubPath && s3SubPath !== "/") {
      fullPrefix += s3SubPath;
    }

    // 确保前缀总是以斜杠结尾 (如果不为空)
    if (fullPrefix && !fullPrefix.endsWith("/")) {
      fullPrefix += "/";
    }

    // 构建最终路径
    return fullPrefix + fileName;
  }

  // /**
  //  * 初始化后端分片上传 - 已废弃，项目使用前端分片上传
  //  * @deprecated 使用前端分片上传 initializeFrontendMultipartUpload 替代
  //  * @param {string} path - 目标路径
  //  * @param {Object} options - 选项参数
  //  * @returns {Promise<Object>} 初始化结果
  //  */
  // async initializeBackendMultipartUpload(path, options = {}) {
  //   this._ensureInitialized();

  //   const { mount, subPath, db, contentType, fileSize, filename } = options;

  //   // 使用专门的文件路径规范化函数
  //   const s3SubPath = this._normalizeFilePath(subPath, path, filename);

  //   // 委托给后端分片操作模块
  //   const result = await this.backendMultipartOps.initializeBackendMultipartUpload(s3SubPath, {
  //     contentType,
  //     fileSize,
  //     filename,
  //   });

  //   // 更新挂载点的最后使用时间
  //   if (db && mount.id) {
  //     await updateMountLastUsed(db, mount.id);
  //   }

  //   // 添加挂载点信息到结果中
  //   return {
  //     ...result,
  //     mount_id: mount ? mount.id : null,
  //     path: path,
  //     storage_type: mount ? mount.storage_type : "S3",
  //   };
  // }

  // /**
  //  * 上传后端分片 - 已废弃，项目使用前端分片上传
  //  * @deprecated 使用前端分片上传替代
  //  * @param {string} path - 目标路径
  //  * @param {Object} options - 选项参数
  //  * @returns {Promise<Object>} 上传结果
  //  */
  // async uploadBackendPart(path, options = {}) {
  //   this._ensureInitialized();

  //   const { mount, subPath, db, uploadId, partNumber, partData, s3Key } = options;

  //   // 如果提供了s3Key，直接使用，否则重新计算
  //   const s3SubPath = s3Key || this._normalizeFilePath(subPath, path);

  //   // 委托给后端分片操作模块
  //   const result = await this.backendMultipartOps.uploadBackendPart(s3SubPath, {
  //     uploadId,
  //     partNumber,
  //     partData,
  //   });

  //   // 更新挂载点的最后使用时间
  //   if (db && mount.id) {
  //     await updateMountLastUsed(db, mount.id);
  //   }

  //   return result;
  // }

  // /**
  //  * 完成后端分片上传 - 已废弃，项目使用前端分片上传
  //  * @deprecated 使用前端分片上传 completeFrontendMultipartUpload 替代
  //  * @param {string} path - 目标路径
  //  * @param {Object} options - 选项参数
  //  * @returns {Promise<Object>} 完成结果
  //  */
  // async completeBackendMultipartUpload(path, options = {}) {
  //   this._ensureInitialized();

  //   const { mount, subPath, db, uploadId, parts, contentType, fileSize, userIdOrInfo, userType, s3Key } = options;

  //   // 如果提供了s3Key，直接使用，否则重新计算
  //   const s3SubPath = s3Key || this._normalizeFilePath(subPath, path);

  //   // 委托给后端分片操作模块
  //   const result = await this.backendMultipartOps.completeBackendMultipartUpload(s3SubPath, {
  //     uploadId,
  //     parts,
  //     contentType,
  //     fileSize,
  //   });

  //   // fs系统不再创建files表记录，只做纯粹的文件操作
  //   let fileName = s3SubPath.split("/").filter(Boolean).pop();
  //   if (!fileName) {
  //     fileName = path.split("/").filter(Boolean).pop();
  //   }
  //   if (!fileName) {
  //     fileName = "unnamed_file";
  //   }
  //   console.log(`后端分片上传完成: ${fileName}, 大小: ${fileSize || 0}字节`);

  //   // 更新挂载点的最后使用时间
  //   if (db && mount.id) {
  //     await updateMountLastUsed(db, mount.id);
  //   }

  //   // 清除缓存
  //   if (mount) {
  //     const { clearDirectoryCache } = await import("../../../cache/index.js");
  //     try {
  //       await clearDirectoryCache({ mountId: mount.id });
  //       console.log(`后端分片上传完成后缓存已清除 - 挂载点=${mount.id}`);
  //     } catch (cacheError) {
  //       console.warn(`清除缓存时出错: ${cacheError.message}`);
  //     }
  //   }

  //   return {
  //     ...result,
  //     path: path,
  //   };
  // }

  /**
   * 中止后端分片上传
   * @param {string} path - 目标路径
   * @param {Object} options - 选项参数
   * @returns {Promise<Object>} 中止结果
   */
  async abortBackendMultipartUpload(path, options = {}) {
    this._ensureInitialized();

    const { mount, subPath, db, uploadId, s3Key } = options;

    // 如果提供了s3Key，直接使用，否则重新计算
    const s3SubPath = s3Key || this._normalizeFilePath(subPath, path);

    // 委托给后端分片操作模块
    const result = await this.backendMultipartOps.abortBackendMultipartUpload(s3SubPath, {
      uploadId,
    });

    // 更新挂载点的最后使用时间
    if (db && mount.id) {
      try {
        await updateMountLastUsed(db, mount.id);
      } catch (updateError) {
        console.warn(`更新挂载点最后使用时间失败: ${updateError.message}`);
      }
    }

    return result;
  }

  /**
   * 生成代理URL（ProxyCapable接口实现）
   * @param {string} path - 文件路径
   * @param {Object} options - 选项参数
   * @param {Object} options.mount - 挂载点信息
   * @param {Request} options.request - 请求对象
   * @param {boolean} options.download - 是否为下载模式
   * @param {Object} options.db - 数据库连接对象
   * @returns {Promise<Object>} 代理URL对象
   */
  async generateProxyUrl(path, options = {}) {
    const { mount, request, download = false, db } = options;

    // 检查挂载点是否启用代理
    if (!this.supportsProxyMode(mount)) {
      throw new HTTPException(ApiStatus.FORBIDDEN, { message: "此挂载点未启用代理访问" });
    }

    // 检查是否需要签名
    const signatureService = new ProxySignatureService(db, this.encryptionSecret);
    const signatureNeed = await signatureService.needsSignature(mount);

    let proxyUrl;
    let signInfo = null;

    if (signatureNeed.required) {
      // 生成签名
      signInfo = await signatureService.generateStorageSignature(path, mount);

      // 生成带签名的代理URL
      proxyUrl = buildSignedProxyUrl(request, path, {
        download,
        signature: signInfo.signature,
        requestTimestamp: signInfo.requestTimestamp,
        needsSignature: true,
      });
    } else {
      // 生成普通代理URL
      proxyUrl = buildFullProxyUrl(request, path, download);
    }

    return {
      url: proxyUrl,
      type: "proxy",
      signed: signatureNeed.required,
      signatureLevel: signatureNeed.level,
      expiresAt: signInfo?.expiresAt,
      isTemporary: signInfo?.isTemporary,
      policy: mount?.webdav_policy || "302_redirect",
    };
  }

  /**
   * 检查是否支持代理模式（ProxyCapable接口实现）
   * @param {Object} mount - 挂载点信息
   * @returns {boolean} 是否支持代理模式
   */
  supportsProxyMode(mount) {
    return mount && !!mount.web_proxy;
  }

  /**
   * 获取代理配置（ProxyCapable接口实现）
   * @param {Object} mount - 挂载点信息
   * @returns {Object} 代理配置对象
   */
  getProxyConfig(mount) {
    return {
      enabled: this.supportsProxyMode(mount),
      webdavPolicy: mount?.webdav_policy || "302_redirect",
    };
  }

  /**
   * 确保驱动已初始化
   * @private
   */
  _ensureInitialized() {
    if (!this.initialized) {
      throw new HTTPException(ApiStatus.INTERNAL_ERROR, { message: "存储驱动未初始化" });
    }
  }
}
