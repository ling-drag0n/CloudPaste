/**
 * 文件分享服务层
 * 负责文件分享相关的所有业务逻辑
 * 包括：系统限制检查、存储空间检查、文件记录管理、权限验证等
 */

import { HTTPException } from "hono/http-exception";
import { ApiStatus } from "../constants/index.js";
import { FileShareSystem } from "../storage/fs/FileShareSystem.js";
import { RepositoryFactory } from "../repositories/index.js";
import { generateFileId, generateUniqueFileSlug, generateShortId, getFileNameAndExt, getSafeFileName, formatFileSize, shouldUseRandomSuffix } from "../utils/common.js";
import { getMimeTypeFromFilename } from "../utils/fileUtils.js";
import { hashPassword } from "../utils/crypto.js";
import { GetFileType, getFileTypeName } from "../utils/fileTypeDetector.js";

// 默认最大上传限制（MB）
const DEFAULT_MAX_UPLOAD_SIZE_MB = 100;

export class FileShareService {
  /**
   * 构造函数
   * @param {Object} db - 数据库实例
   * @param {string} encryptionSecret - 加密密钥
   */
  constructor(db, encryptionSecret) {
    this.db = db;
    this.encryptionSecret = encryptionSecret;
    this.shareSystem = new FileShareSystem(db, encryptionSecret);
    this.repositoryFactory = new RepositoryFactory(db);
  }

  /**
   * 创建预签名上传 - 包含完整的业务逻辑
   * @param {string} storageConfigId - 存储配置ID
   * @param {string} filename - 文件名
   * @param {string|Object} userIdOrInfo - 用户ID或API密钥信息
   * @param {string} userType - 用户类型
   * @param {Object} options - 选项参数
   * @returns {Promise<Object>} 预签名上传结果
   */
  async createPresignedUpload(storageConfigId, filename, userIdOrInfo, userType, options = {}) {
    const { fileSize, slug, override, customPath, remark, password, expires_in, max_views, use_proxy } = options;

    let fileRecord = null;

    try {
      // 1. 系统限制检查
      await this._checkSystemUploadLimit(fileSize);

      // 2. 获取存储配置并检查权限
      const config = await this._getStorageConfigWithPermissionCheck(storageConfigId, userIdOrInfo, userType);

      // 3. 存储空间检查
      await this._checkStorageSpace(config, storageConfigId, fileSize);

      // 4. 生成文件记录
      fileRecord = await this._createFileRecord(filename, {
        storageConfigId,
        userIdOrInfo,
        userType,
        slug,
        override,
        customPath,
        remark,
        password,
        expires_in,
        max_views,
        use_proxy,
        config,
        fileSize,
      });

      // 5. 委托给抽象层生成预签名URL（直接传递已验证的config）
      const presignedResult = await this.shareSystem.generatePresignedUploadUrlWithConfig(fileRecord.storagePath, filename, config, {
        fileSize,
      });

      return {
        file_id: fileRecord.fileId,
        upload_url: presignedResult.uploadUrl,
        storage_path: fileRecord.storagePath,
        s3_url: presignedResult.s3Url,
        slug: fileRecord.slug,
        provider_type: config.provider_type,
        contentType: presignedResult.contentType,
        hasPassword: fileRecord.hasPassword,
        expiresAt: fileRecord.expiresAt,
        maxViews: fileRecord.maxViews,
        url: `/file/${fileRecord.slug}`,
      };
    } catch (error) {
      // 如果生成预签名URL失败，清理已创建的文件记录
      if (fileRecord) {
        try {
          console.log(`预签名URL生成失败，清理文件记录: ${fileRecord.fileId}`);
          const fileRepository = this.repositoryFactory.getFileRepository();
          await fileRepository.deleteFile(fileRecord.fileId);

          // 如果有密码记录，也要删除
          if (password) {
            await fileRepository.deleteFilePasswordRecord(fileRecord.fileId);
          }
        } catch (cleanupError) {
          console.error("清理失败的文件记录时出错:", cleanupError);
        }
      }

      // 重新抛出原始错误
      throw error;
    }
  }

  /**
   * 提交上传完成 - 包含完整的业务逻辑
   * @param {string} fileId - 文件ID
   * @param {Object} uploadResult - 上传结果
   * @param {string|Object} userIdOrInfo - 用户ID或API密钥信息
   * @param {string} userType - 用户类型
   * @returns {Promise<Object>} 提交结果
   */
  async commitUpload(fileId, uploadResult, userIdOrInfo, userType) {
    // 1. 获取文件记录
    const fileRecord = await this._getFileRecord(fileId);

    // 2. 权限验证
    await this._validateUploadPermission(fileRecord, userIdOrInfo, userType);

    // 3. 获取存储配置
    const config = await this._getStorageConfigWithPermissionCheck(fileRecord.storage_config_id, userIdOrInfo, userType);

    // 4. 存储空间最终检查（基于实际文件大小）
    if (uploadResult.size) {
      await this._checkStorageSpaceForCommit(config, fileRecord.storage_config_id, fileRecord.id, uploadResult.size);
    }

    // 5. 更新文件记录
    const updatedFile = await this._updateFileRecord(fileId, uploadResult);

    // 6. 后续清理工作
    await this._postCommitCleanup(fileRecord, config);

    // 7. 添加文件类型信息
    const fileType = await GetFileType(updatedFile.filename, this.db);
    const fileTypeName = await getFileTypeName(updatedFile.filename, this.db);

    return {
      ...updatedFile,
      url: `/file/${updatedFile.slug}`,
      type: fileType, // 整数类型常量 (0-6)
      typeName: fileTypeName, // 类型名称（用于调试）
    };
  }

  /**
   * 检查系统最大上传限制
   * @private
   */
  async _checkSystemUploadLimit(fileSize) {
    if (!fileSize) return;

    const systemRepository = this.repositoryFactory.getSystemRepository();
    const maxUploadSizeResult = await systemRepository.getSettingMetadata("max_upload_size");

    const maxUploadSizeMB = maxUploadSizeResult ? parseInt(maxUploadSizeResult.value) : DEFAULT_MAX_UPLOAD_SIZE_MB;
    const maxUploadSizeBytes = maxUploadSizeMB * 1024 * 1024;

    if (fileSize > maxUploadSizeBytes) {
      throw new HTTPException(ApiStatus.BAD_REQUEST, {
        message: `文件大小超过系统限制，最大允许 ${formatFileSize(maxUploadSizeBytes)}，当前文件 ${formatFileSize(fileSize)}`,
      });
    }
  }

  /**
   * 获取存储配置并进行权限检查
   * @private
   */
  async _getStorageConfigWithPermissionCheck(storageConfigId, userIdOrInfo, userType) {
    const { StorageConfigUtils } = await import("../storage/utils/StorageConfigUtils.js");
    const config = await StorageConfigUtils.getStorageConfig(this.db, "S3", storageConfigId);

    if (!config) {
      throw new HTTPException(ApiStatus.NOT_FOUND, {
        message: "指定的存储配置不存在",
      });
    }

    // 如果是管理员授权，确认配置属于该管理员
    if (userType === "admin" && config.admin_id !== userIdOrInfo) {
      throw new HTTPException(ApiStatus.FORBIDDEN, {
        message: "您无权使用此存储配置",
      });
    }

    return config;
  }

  /**
   * 检查存储空间
   * @private
   */
  async _checkStorageSpace(config, storageConfigId, fileSize) {
    if (!fileSize || config.total_storage_bytes === null) return;

    const fileRepository = this.repositoryFactory.getFileRepository();
    const usageResult = await fileRepository.getTotalSize({
      storage_config_id: storageConfigId,
      storage_type: "S3",
    });

    const currentUsage = usageResult || 0;
    const totalAfterUpload = currentUsage + parseInt(fileSize);

    if (totalAfterUpload > config.total_storage_bytes) {
      const remainingSpace = Math.max(0, config.total_storage_bytes - currentUsage);
      const formattedRemaining = formatFileSize(remainingSpace);
      const formattedFileSize = formatFileSize(fileSize);
      const formattedTotal = formatFileSize(config.total_storage_bytes);

      throw new HTTPException(ApiStatus.BAD_REQUEST, {
        message: `存储空间不足。文件大小(${formattedFileSize})超过剩余空间(${formattedRemaining})。存储桶总容量限制为${formattedTotal}。`,
      });
    }
  }

  /**
   * 创建文件记录
   * @private
   */
  async _createFileRecord(filename, options) {
    const { storageConfigId, userIdOrInfo, userType, slug, override, customPath, remark, password, expires_in, max_views, use_proxy, config, fileSize, original_filename } =
      options;

    const fileId = generateFileId();

    const uniqueSlug = await generateUniqueFileSlug(this.db, slug, override, {
      userIdOrInfo,
      userType,
      encryptionSecret: this.encryptionSecret,
      repositoryFactory: this.repositoryFactory,
    });

    // 生成存储路径
    const storagePath = await this._generateStoragePath(filename, customPath, userIdOrInfo, userType, config, original_filename);
    const contentType = getMimeTypeFromFilename(filename);

    // 处理密码
    let passwordHash = null;
    if (password) {
      passwordHash = await hashPassword(password);
    }

    // 处理过期时间
    let expiresAt = null;
    if (expires_in && parseInt(expires_in) > 0) {
      const expiresDate = new Date();
      expiresDate.setHours(expiresDate.getHours() + parseInt(expires_in));
      expiresAt = expiresDate.toISOString();
    }

    // 处理最大查看次数
    const maxViews = typeof max_views === "number" && max_views > 0 ? max_views : null;

    // 获取全局默认代理设置
    let defaultUseProxy = false; // 硬编码默认值作为后备
    try {
      const systemRepository = this.repositoryFactory.getSystemRepository();
      const defaultProxySetting = await systemRepository.getSettingMetadata("default_use_proxy");
      defaultUseProxy = defaultProxySetting?.value === "true";
    } catch (error) {
      console.warn("获取全局默认代理设置失败，使用硬编码默认值:", error);
    }

    // 创建文件记录
    const fileRepository = this.repositoryFactory.getFileRepository();
    const fileData = {
      id: fileId,
      slug: uniqueSlug,
      filename: filename,
      storage_config_id: storageConfigId,
      storage_type: "S3",
      storage_path: storagePath,
      file_path: null,
      mimetype: contentType,
      size: fileSize || 0,
      etag: null,
      remark: remark || null,
      password: passwordHash,
      expires_at: expiresAt,
      max_views: maxViews > 0 ? maxViews : null,
      use_proxy: use_proxy !== undefined ? use_proxy : defaultUseProxy,
      created_by: userType === "admin" ? userIdOrInfo : `apikey:${userIdOrInfo}`,
      created_at: new Date().toISOString(),
      updated_at: new Date().toISOString(),
    };

    await fileRepository.createFile(fileData);

    // 如果设置了密码，保存明文密码记录
    if (password) {
      await fileRepository.createFilePasswordRecord(fileId, password);
    }

    return {
      fileId,
      slug: uniqueSlug,
      storagePath,
      contentType,
      hasPassword: !!passwordHash,
      expiresAt: expiresAt,
      maxViews: maxViews,
    };
  }

  /**
   * 直接上传接口
   * @param {string} filename - 文件名
   * @param {ArrayBuffer} fileContent - 文件内容
   * @param {number} fileSize - 文件大小
   * @param {string|Object} userIdOrInfo - 用户ID或API密钥信息
   * @param {string} userType - 用户类型
   * @param {Object} params - 所有参数
   * @returns {Promise<Object>} 核心上传结果
   */
  async uploadDirectComplete(filename, fileContent, fileSize, userIdOrInfo, userType, params) {
    let fileRecord = null;
    let config = null;

    try {
      // 1. 获取或选择S3配置
      config = await this._resolveS3Config(params.s3_config_id, userIdOrInfo, userType);

      // 2. 系统检查
      await this._checkSystemUploadLimit(fileSize);
      await this._checkStorageSpace(config, config.id, fileSize);

      // 3. 创建文件记录
      fileRecord = await this._createFileRecord(filename, {
        storageConfigId: config.id,
        userIdOrInfo,
        userType,
        slug: params.slug,
        override: params.override,
        customPath: params.path,
        remark: params.remark,
        password: params.password,
        expires_in: params.expires_in,
        max_views: params.max_views,
        use_proxy: params.use_proxy,
        config,
        fileSize,
        original_filename: params.original_filename,
      });

      // 4. 上传文件
      const mockFile = {
        name: filename,
        size: fileSize,
        arrayBuffer: async () => fileContent,
      };

      const uploadResult = await this.shareSystem.uploadFileWithConfig(fileRecord.storagePath, mockFile, config, {
        userIdOrInfo,
        userType,
      });

      // 5. 更新文件记录
      const fileRepository = this.repositoryFactory.getFileRepository();
      await fileRepository.updateFile(fileRecord.fileId, {
        etag: uploadResult.etag,
        updated_at: new Date().toISOString(),
      });

      // 6. 获取最终文件记录
      const finalFileRecord = await fileRepository.findById(fileRecord.fileId);

      // 7. 执行清理工作
      await this._postCommitCleanup(finalFileRecord, config);

      // 8. 返回核心数据，让路由层构建响应
      return {
        fileRecord: finalFileRecord,
        config,
        uploadResult,
      };
    } catch (error) {
      // 清理失败的文件记录
      if (fileRecord) {
        try {
          const fileRepository = this.repositoryFactory.getFileRepository();
          await fileRepository.deleteFile(fileRecord.fileId);
        } catch (cleanupError) {
          console.warn("清理失败的文件记录时出错:", cleanupError);
        }
      }
      throw error;
    }
  }

  /**
   * 解析S3配置
   * @private
   */
  async _resolveS3Config(s3ConfigId, userIdOrInfo, userType) {
    const repositoryFactory = new RepositoryFactory(this.db);
    const s3ConfigRepository = repositoryFactory.getS3ConfigRepository();
    const isAdmin = userType === "admin";

    // 如果指定了配置ID
    if (s3ConfigId) {
      if (userType === "apikey") {
        // API密钥用户只能使用公开配置
        const config = await s3ConfigRepository.findPublicById(s3ConfigId);
        if (!config) {
          throw new Error("API密钥用户只能使用公开的S3配置或默认配置");
        }
        return config;
      } else {
        // 管理员用户
        return await this._getStorageConfigWithPermissionCheck(s3ConfigId, userIdOrInfo, userType);
      }
    }

    // 没有指定配置ID，获取默认配置
    const { DbTables } = await import("../constants/index.js");

    if (isAdmin) {
      // 管理员用户 - 获取该管理员的默认配置
      const defaultConfig = await this.db
        .prepare(
          `
        SELECT * FROM ${DbTables.S3_CONFIGS}
        WHERE admin_id = ? AND is_default = 1
        LIMIT 1
      `
        )
        .bind(userIdOrInfo)
        .first();

      if (defaultConfig) {
        return defaultConfig;
      }

      // 没有默认配置，获取任意配置
      const anyConfig = await this.db
        .prepare(
          `
        SELECT * FROM ${DbTables.S3_CONFIGS}
        WHERE admin_id = ?
        LIMIT 1
      `
        )
        .bind(userIdOrInfo)
        .first();

      if (!anyConfig) {
        throw new Error("您的账户下没有可用的S3配置，请先创建配置或指定有效的S3配置ID");
      }
      return anyConfig;
    } else {
      // API密钥用户 - 获取公开的默认配置
      const defaultConfig = await this.db
        .prepare(
          `
        SELECT * FROM ${DbTables.S3_CONFIGS}
        WHERE is_public = 1 AND is_default = 1
        LIMIT 1
      `
        )
        .first();

      if (defaultConfig) {
        return defaultConfig;
      }

      // 没有默认配置，获取任意公开配置
      const anyPublicConfig = await this.db
        .prepare(
          `
        SELECT * FROM ${DbTables.S3_CONFIGS}
        WHERE is_public = 1
        LIMIT 1
      `
        )
        .first();

      if (!anyPublicConfig) {
        throw new Error("系统中没有公开可用的S3配置，请联系管理员设置公开配置或指定有效的S3配置ID");
      }
      return anyPublicConfig;
    }
  }

  /**
   * 生成存储路径
   * @private
   */
  async _generateStoragePath(filename, customPath, userIdOrInfo, userType, config, original_filename) {
    // 处理默认文件夹路径
    const folderPath = config.default_folder ? (config.default_folder.endsWith("/") ? config.default_folder : config.default_folder + "/") : "";

    // 构建目标目录
    let targetDirectory = folderPath;
    if (customPath) {
      const normalizedCustomPath = customPath.endsWith("/") ? customPath : customPath + "/";
      targetDirectory = folderPath + normalizedCustomPath;
    }

    // 根据系统设置或用户参数决定文件命名策略
    let useRandomSuffix;
    if (original_filename !== undefined) {
      // 用户明确指定了是否使用原始文件名（仅用于直接上传接口）
      useRandomSuffix = !original_filename;
    } else {
      // 使用系统设置
      useRandomSuffix = await shouldUseRandomSuffix(this.db);
    }

    const { name: baseName, ext: fileExt } = getFileNameAndExt(filename);
    const safeFileName = getSafeFileName(baseName);

    if (useRandomSuffix) {
      // 随机后缀模式：避免文件名冲突，格式为 filename-shortId.ext
      const shortId = generateShortId();
      return targetDirectory + safeFileName + "-" + shortId + fileExt;
    } else {
      // 覆盖模式：使用原始文件名（可能冲突）
      return targetDirectory + safeFileName + fileExt;
    }
  }

  /**
   * 获取文件记录
   * @private
   */
  async _getFileRecord(fileId) {
    const stmt = this.db.prepare("SELECT * FROM files WHERE id = ?");
    const file = await stmt.bind(fileId).first();

    if (!file) {
      throw new HTTPException(ApiStatus.NOT_FOUND, {
        message: "文件不存在或已被删除",
      });
    }

    return file;
  }

  /**
   * 验证上传权限
   * @private
   */
  async _validateUploadPermission(fileRecord, userIdOrInfo, userType) {
    if (userType === "admin" && fileRecord.created_by && fileRecord.created_by !== userIdOrInfo) {
      throw new HTTPException(ApiStatus.FORBIDDEN, {
        message: "您无权更新此文件",
      });
    }

    if (userType === "apikey" && fileRecord.created_by && fileRecord.created_by !== `apikey:${userIdOrInfo}`) {
      throw new HTTPException(ApiStatus.FORBIDDEN, {
        message: "此API密钥无权更新此文件",
      });
    }
  }

  /**
   * 检查存储空间（commit阶段）
   * @private
   */
  async _checkStorageSpaceForCommit(config, storageConfigId, fileId, fileSize) {
    if (!fileSize || config.total_storage_bytes === null) return;

    const fileRepository = this.repositoryFactory.getFileRepository();
    const usageResult = await fileRepository.getTotalSizeByStorageConfigExcludingFile(storageConfigId, fileId, "S3");
    const currentUsage = usageResult?.total_used || 0;
    const totalAfterCommit = currentUsage + parseInt(fileSize);

    if (totalAfterCommit > config.total_storage_bytes) {
      // 删除已上传的文件
      try {
        const { deleteFileFromS3 } = await import("../utils/s3Utils.js");
        const fileRecord = await fileRepository.findById(fileId);
        if (fileRecord) {
          await deleteFileFromS3(config, fileRecord.storage_path, this.encryptionSecret);
        }
      } catch (deleteError) {
        console.error("删除超出容量限制的临时文件失败:", deleteError);
      }

      // 删除文件记录
      await fileRepository.deleteFile(fileId);

      const remainingSpace = Math.max(0, config.total_storage_bytes - currentUsage);
      const formattedRemaining = formatFileSize(remainingSpace);
      const formattedFileSize = formatFileSize(fileSize);
      const formattedTotal = formatFileSize(config.total_storage_bytes);

      throw new HTTPException(ApiStatus.BAD_REQUEST, {
        message: `存储空间不足。文件大小(${formattedFileSize})超过剩余空间(${formattedRemaining})。存储桶总容量限制为${formattedTotal}。文件已被删除。`,
      });
    }
  }

  /**
   * 更新文件记录
   * @private
   */
  async _updateFileRecord(fileId, uploadResult) {
    const fileRepository = this.repositoryFactory.getFileRepository();

    const updateData = {
      size: uploadResult.size || 0,
      etag: uploadResult.etag || null,
      updated_at: new Date().toISOString(),
    };

    await fileRepository.updateFile(fileId, updateData);
    return await fileRepository.findById(fileId);
  }

  /**
   * 提交后清理工作
   * @private
   */
  async _postCommitCleanup(fileRecord, config) {
    try {
      // 更新父目录的修改时间
      const { updateParentDirectoriesModifiedTimeHelper } = await import("../storage/drivers/s3/utils/S3DirectoryUtils.js");
      await updateParentDirectoriesModifiedTimeHelper(config, fileRecord.storage_path, this.encryptionSecret);
    } catch (error) {
      console.warn("更新父目录修改时间失败:", error);
    }

    try {
      // 清除相关缓存
      const { clearDirectoryCache } = await import("../cache/index.js");
      await clearDirectoryCache({ db: this.db, s3ConfigId: fileRecord.storage_config_id });
    } catch (error) {
      console.warn("清除缓存失败:", error);
    }
  }

  // ==================== URL上传相关方法 ====================

  /**
   * 验证URL并获取元信息
   * @param {string} url - 要验证的URL
   * @returns {Promise<Object>} URL元信息
   */
  async validateUrlMetadata(url) {
    let metadata = null;

    try {
      // 验证URL格式
      const parsedUrl = new URL(url);

      // 确保协议为http或https
      if (parsedUrl.protocol !== "http:" && parsedUrl.protocol !== "https:") {
        throw new Error("仅支持HTTP/HTTPS协议的URL");
      }

      // 发送请求获取元信息
      let response;
      let method = "HEAD";
      let corsSupported = false;

      try {
        // 先尝试HEAD请求
        response = await fetch(url, {
          method: "HEAD",
        });

        if (response.ok) {
          corsSupported = true;
        } else {
          // HEAD请求返回错误状态码，尝试GET请求
          throw new Error(`HEAD请求返回状态码: ${response.status}`);
        }
      } catch (headError) {
        console.warn(`HEAD请求失败，尝试GET请求: ${headError.message}`);

        // HEAD请求失败，尝试GET请求
        try {
          response = await fetch(url, {
            method: "GET",
          });
          method = "GET";
          corsSupported = response.ok;
        } catch (getError) {
          throw new Error(`无法访问URL: ${getError.message}`);
        }
      }

      // 检查响应状态
      if (!response.ok) {
        throw new Error(`服务器返回错误状态码: ${response.status}`);
      }

      // 获取响应头信息
      const contentType = response.headers.get("content-type") || "application/octet-stream";
      const contentLength = response.headers.get("content-length");
      const lastModified = response.headers.get("last-modified");

      // 尝试从URL路径中提取文件名
      let filename = "";
      try {
        const pathname = parsedUrl.pathname;
        const segments = pathname.split("/").filter(Boolean);
        if (segments.length > 0) {
          const lastSegment = segments[segments.length - 1];
          // 检查最后一个段是否看起来像文件名（包含扩展名）
          if (lastSegment.includes(".") && !lastSegment.endsWith(".")) {
            filename = decodeURIComponent(lastSegment);
          }
        }
      } catch (decodeError) {
        console.warn(`解码文件名失败: ${decodeError.message}`);
      }

      // 如果从URL无法获取有效文件名，尝试从Content-Disposition头获取
      if (!filename || filename === "" || filename === "/") {
        const contentDisposition = response.headers.get("content-disposition");
        if (contentDisposition) {
          const filenameMatch = contentDisposition.match(/filename[^;=\n]*=((['"]).*?\2|[^;\n]*)/);
          if (filenameMatch && filenameMatch[1]) {
            filename = filenameMatch[1].replace(/['"]/g, "");
          }
        }
      }

      // 如果还是无法获取有效文件名，使用简单的默认名称
      if (!filename || filename === "" || filename === "/") {
        filename = "download";
      }

      // 添加文件类型检测
      const fileType = await GetFileType(filename, this.db);
      const fileTypeName = await getFileTypeName(filename, this.db);

      // 构建元数据对象
      metadata = {
        url: url,
        filename: filename,
        contentType: contentType,
        size: contentLength ? parseInt(contentLength) : null,
        lastModified: lastModified,
        method: method,
        corsSupported: corsSupported,
        type: fileType, // 整数类型常量 (0-6)
        typeName: fileTypeName, // 类型名称（用于调试）
      };

      return metadata;
    } catch (error) {
      if (error instanceof TypeError && error.message.includes("Invalid URL")) {
        throw new Error("无效的URL格式");
      }
      throw error;
    }
  }

  /**
   * 代理转发URL内容（用于不支持CORS的资源）
   * @param {string} url - 源URL
   * @returns {Promise<Response>} 可直接返回的Response流
   */
  async proxyUrlContent(url) {
    try {
      // 验证URL格式
      const parsedUrl = new URL(url);

      // 确保协议为http或https
      if (parsedUrl.protocol !== "http:" && parsedUrl.protocol !== "https:") {
        throw new Error("仅支持HTTP/HTTPS协议的URL");
      }

      // 请求源URL并流式返回内容
      const response = await fetch(url);

      if (!response.ok) {
        throw new Error(`源服务器返回错误状态码: ${response.status}`);
      }

      // 创建新的Response对象，保持流式传输
      return new Response(response.body, {
        status: response.status,
        statusText: response.statusText,
        headers: {
          "Content-Type": response.headers.get("content-type") || "application/octet-stream",
          "Content-Length": response.headers.get("content-length") || "",
          "Cache-Control": "public, max-age=3600",
          "Access-Control-Allow-Origin": "*",
          "Access-Control-Allow-Methods": "GET, HEAD, OPTIONS",
          "Access-Control-Allow-Headers": "Content-Type, Range",
        },
      });
    } catch (error) {
      if (error instanceof TypeError && error.message.includes("Invalid URL")) {
        throw new Error("无效的URL格式");
      }
      throw error;
    }
  }

  /**
   * 创建URL上传（单文件上传） - 复用现有的预签名上传逻辑
   * @param {string} url - 源URL
   * @param {Object} metadata - URL元信息
   * @param {string|Object} userIdOrInfo - 用户ID或API密钥信息
   * @param {string} userType - 用户类型
   * @param {Object} options - 选项参数
   * @returns {Promise<Object>} URL上传结果
   */
  async createUrlUpload(url, metadata, userIdOrInfo, userType, options = {}) {
    // 转换参数格式，复用现有的 createPresignedUpload 方法
    const adaptedOptions = {
      slug: options.slug,
      remark: options.remark,
      customPath: options.customPath,
      password: options.password,
      expires_in: options.expires_in,
      max_views: options.max_views,
      fileSize: metadata.size,
    };

    // 直接复用现有的预签名上传逻辑，只是文件名来源不同
    const result = await this.createPresignedUpload(options.storageConfigId, metadata.filename, userIdOrInfo, userType, adaptedOptions);

    // 添加URL上传特有的信息
    return {
      ...result,
      corsSupported: metadata.corsSupported,
      sourceUrl: url,
    };
  }

  /**
   * 初始化URL分片上传 - 分片上传是特殊逻辑，需要单独处理
   * @param {string} url - 源URL
   * @param {Object} metadata - URL元信息
   * @param {string|Object} userIdOrInfo - 用户ID或API密钥信息
   * @param {string} userType - 用户类型
   * @param {Object} options - 选项参数
   * @returns {Promise<Object>} 分片上传初始化结果
   */
  async initializeUrlMultipartUpload(url, metadata, userIdOrInfo, userType, options = {}) {
    const { storageConfigId, slug, remark, customPath, password, expires_in, max_views, partSize, partCount, override, use_proxy } = options;

    let fileRecord = null;

    try {
      // 1. 系统限制检查
      await this._checkSystemUploadLimit(metadata.size);

      // 2. 获取存储配置并检查权限
      const config = await this._getStorageConfigWithPermissionCheck(storageConfigId, userIdOrInfo, userType);

      // 3. 存储空间检查
      await this._checkStorageSpace(config, storageConfigId, metadata.size);

      // 4. 生成文件记录（复用现有逻辑）
      fileRecord = await this._createFileRecord(metadata.filename, {
        storageConfigId,
        userIdOrInfo,
        userType,
        slug,
        override,
        customPath,
        remark,
        password,
        expires_in,
        max_views,
        use_proxy,
        config,
        fileSize: metadata.size,
      });

      // 5. 委托给抽象层初始化分片上传（这是分片上传特有的逻辑）
      const multipartResult = await this.shareSystem.initializeMultipartUploadWithConfig(fileRecord.storagePath, metadata.filename, config, {
        fileSize: metadata.size,
        partSize,
        partCount,
      });

      return {
        file_id: fileRecord.fileId,
        upload_id: multipartResult.uploadId,
        slug: fileRecord.slug,
        storagePath: fileRecord.storagePath,
        s3Url: multipartResult.s3Url,
        presigned_urls: multipartResult.presignedUrls,
        partSize: multipartResult.partSize,
        totalSize: metadata.size,
        partCount: multipartResult.partCount,
        filename: metadata.filename,
        contentType: metadata.contentType,
        corsSupported: metadata.corsSupported,
        hasPassword: !!password,
        expiresAt: fileRecord.expiresAt,
        maxViews: fileRecord.maxViews,
        url: `/file/${fileRecord.slug}`,
        sourceUrl: url,
      };
    } catch (error) {
      // 如果创建了文件记录但后续步骤失败，需要清理
      if (fileRecord) {
        try {
          const fileRepository = this.repositoryFactory.getFileRepository();
          await fileRepository.deleteFile(fileRecord.fileId);
          console.log(`已清理失败的文件记录: ${fileRecord.fileId}`);
        } catch (cleanupError) {
          console.warn(`清理失败的文件记录时出错: ${cleanupError.message}`);
        }
      }
      throw error;
    }
  }

  /**
   * 完成URL分片上传 - 分片上传完成逻辑，复用现有方法
   * @param {string} fileId - 文件ID
   * @param {string} uploadId - 上传ID
   * @param {Array} parts - 分片信息数组
   * @param {string|Object} userIdOrInfo - 用户ID或API密钥信息
   * @param {string} userType - 用户类型
   * @returns {Promise<Object>} 完成结果
   */
  async completeUrlMultipartUpload(fileId, uploadId, parts, userIdOrInfo, userType) {
    let fileRecord = null;

    try {
      // 1. 获取文件记录
      fileRecord = await this._getFileRecord(fileId);

      // 2. 权限验证
      await this._validateUploadPermission(fileRecord, userIdOrInfo, userType);

      // 3. 获取存储配置
      const config = await this._getStorageConfigWithPermissionCheck(fileRecord.storage_config_id, userIdOrInfo, userType);

      // 4. 委托给抽象层完成分片上传（这是分片上传特有的逻辑）
      const completeResult = await this.shareSystem.completeMultipartUploadWithConfig(fileRecord.storage_path, uploadId, parts, config, {
        fileName: fileRecord.filename,
        fileSize: fileRecord.size,
      });

      // 5. 更新文件记录
      const fileRepository = this.repositoryFactory.getFileRepository();
      await fileRepository.updateFile(fileId, {
        size: completeResult.size || 0,
        etag: completeResult.etag,
        updated_at: new Date().toISOString(),
      });

      // 6. 提交后清理工作
      await this._postCommitCleanup(fileRecord, config);

      // 7. 添加文件类型信息
      const fileType = await GetFileType(fileRecord.filename, this.db);
      const fileTypeName = await getFileTypeName(fileRecord.filename, this.db);

      return {
        success: true,
        fileId: fileId,
        slug: fileRecord.slug,
        filename: fileRecord.filename,
        size: completeResult.size,
        etag: completeResult.etag,
        url: `/file/${fileRecord.slug}`,
        s3Url: completeResult.s3Url,
        type: fileType, // 整数类型常量 (0-6)
        typeName: fileTypeName, // 类型名称（用于调试）
      };
    } catch (error) {
      console.error("URL分片上传完成失败:", error);

      // 清理失败时的文件记录
      if (fileRecord && fileRecord.id) {
        try {
          const fileRepository = this.repositoryFactory.getFileRepository();
          await fileRepository.deleteFile(fileRecord.id);
          console.log(`已清理失败的文件记录: ${fileRecord.id}`);
        } catch (cleanupError) {
          console.warn(`清理失败的文件记录时出错: ${cleanupError.message}`);
        }
      }

      throw error;
    }
  }

  /**
   * 中止URL分片上传 - 分片上传中止逻辑，复用现有方法
   * @param {string} fileId - 文件ID
   * @param {string} uploadId - 上传ID
   * @param {string|Object} userIdOrInfo - 用户ID或API密钥信息
   * @param {string} userType - 用户类型
   * @returns {Promise<Object>} 中止结果
   */
  async abortUrlMultipartUpload(fileId, uploadId, userIdOrInfo, userType) {
    // 分片上传中止逻辑与普通分片上传完全相同，直接复用

    // 1. 获取文件记录
    const fileRecord = await this._getFileRecord(fileId);

    // 2. 权限验证
    await this._validateUploadPermission(fileRecord, userIdOrInfo, userType);

    // 3. 获取存储配置
    const config = await this._getStorageConfigWithPermissionCheck(fileRecord.storage_config_id, userIdOrInfo, userType);

    // 4. 委托给抽象层中止分片上传
    const abortResult = await this.shareSystem.abortMultipartUploadWithConfig(fileRecord.storage_path, uploadId, config, {
      fileName: fileRecord.filename,
    });

    // 5. 删除文件记录（因为上传已中止）
    try {
      const fileRepository = this.repositoryFactory.getFileRepository();
      await fileRepository.deleteFile(fileId);
    } catch (deleteError) {
      console.warn("删除中止的文件记录时出错:", deleteError);
    }

    return {
      success: true,
      message: abortResult.message || "分片上传已中止",
      fileId: fileId,
    };
  }

  /**
   * 从文件系统创建分享记录
   * @param {string} fsPath - 文件系统路径
   * @param {string|Object} userIdOrInfo - 用户ID或API密钥信息
   * @param {string} userType - 用户类型
   * @param {Object} options - 选项参数
   * @returns {Promise<Object>} 创建结果
   */
  async createShareFromFileSystem(fsPath, userIdOrInfo, userType, options = {}) {
    const { MountManager } = await import("../storage/managers/MountManager.js");
    const { FileSystem } = await import("../storage/fs/FileSystem.js");

    try {
      // 1. 通过FileSystem获取文件信息和权限验证
      const mountManager = new MountManager(this.db, this.encryptionSecret);
      const fileSystem = new FileSystem(mountManager);

      const fileInfo = await fileSystem.getFileInfo(fsPath, userIdOrInfo, userType);

      if (!fileInfo || fileInfo.isDirectory) {
        throw new HTTPException(ApiStatus.BAD_REQUEST, {
          message: "只能为文件创建分享，不支持目录分享",
        });
      }

      // 2. 获取文件所在的挂载点信息
      const { mount } = await mountManager.getDriverByPath(fsPath, userIdOrInfo, userType);

      // 3. 创建文件记录（复用现有逻辑的核心部分）
      const fileId = generateFileId();
      const uniqueSlug = await generateUniqueFileSlug(this.db, null, false);

      // 计算文件在存储中的实际路径
      const subPath = fsPath.replace(mount.mount_path, "").replace(/^\/+/, "");
      const storagePath = mount.storage_config_id ? subPath : fsPath;

      // 创建文件记录（与_createFileRecord保持一致的结构）
      const fileRepository = this.repositoryFactory.getFileRepository();
      const fileData = {
        id: fileId,
        slug: uniqueSlug,
        filename: fileInfo.name,
        storage_config_id: mount.storage_config_id,
        storage_type: mount.storage_type || "S3",
        storage_path: storagePath,
        file_path: fsPath, // 关键：存储原始文件系统路径，这是与普通分享的区别
        mimetype: fileInfo.mimeType || getMimeTypeFromFilename(fileInfo.name),
        size: fileInfo.size || 0,
        etag: fileInfo.etag || null,
        remark: options.remark || `来自文件系统: ${fsPath}`,
        password: null, // 文件系统分享暂不支持密码
        expires_at: null, // 文件系统分享暂不支持过期时间
        max_views: null, // 文件系统分享暂不支持查看次数限制
        use_proxy: true,
        created_by: userType === "admin" ? userIdOrInfo : `apikey:${userIdOrInfo.id}`,
        created_at: new Date().toISOString(),
        updated_at: new Date().toISOString(),
      };

      await fileRepository.createFile(fileData);

      return {
        success: true,
        fileId: fileId,
        slug: uniqueSlug,
        filename: fileInfo.name,
        size: fileInfo.size,
        url: `/file/${uniqueSlug}`,
        message: "分享创建成功",
      };
    } catch (error) {
      console.error("从文件系统创建分享失败:", error);

      // 统一错误处理
      if (error instanceof HTTPException) {
        throw error;
      }

      throw new HTTPException(ApiStatus.INTERNAL_ERROR, {
        message: error.message || "创建分享失败",
      });
    }
  }
}
