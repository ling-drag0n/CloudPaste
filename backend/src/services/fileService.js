/**
 * 文件服务类
 * 文件业务逻辑，通过Repository访问数据
 */

import { generatePresignedUrl } from "../utils/s3Utils.js";
import { FileRepository, S3ConfigRepository } from "../repositories/index.js";
import { StorageConfigUtils } from "../storage/utils/StorageConfigUtils.js";
import { StorageFactory } from "../storage/factory/StorageFactory.js";
import { GetFileType, getFileTypeName } from "../utils/fileTypeDetector.js";
import { validateSlugFormat } from "../utils/common.js";
import { hashPassword } from "../utils/crypto.js";
import { HTTPException } from "hono/http-exception";
import { ApiStatus, DbTables } from "../constants/index.js";

export class FileService {
  /**
   * 构造函数
   * @param {D1Database} db - 数据库实例
   * @param {string} encryptionSecret - 加密密钥
   */
  constructor(db, encryptionSecret) {
    this.db = db;
    this.encryptionSecret = encryptionSecret;
    this.fileRepository = new FileRepository(db);
    this.s3ConfigRepository = new S3ConfigRepository(db);
  }

  /**
   * 验证文件访问权限 - 纯业务逻辑
   * @param {Object} file - 文件对象
   * @returns {Object} 包含accessible和reason的对象
   */
  validateFileAccess(file) {
    if (!file) {
      return { accessible: false, reason: "not_found" };
    }

    // 检查文件是否已过期
    if (file.expires_at) {
      const expiryDate = new Date(file.expires_at);
      const now = new Date();
      if (now > expiryDate) {
        return { accessible: false, reason: "expired" };
      }
    }

    // 检查文件访问次数是否超过限制
    if (file.max_views !== null && file.max_views > 0) {
      if (file.views > file.max_views) {
        return { accessible: false, reason: "expired" };
      }
    }

    return { accessible: true };
  }

  /**
   * 根据文件信息获取存储驱动
   * @param {Object} file - 文件对象
   * @returns {Promise<Object>} 存储驱动实例
   */
  async getStorageDriver(file) {
    // 获取存储配置
    const config = await StorageConfigUtils.getStorageConfig(this.db, file.storage_type, file.storage_config_id);

    // 创建存储驱动
    const driver = await StorageFactory.createDriver(file.storage_type, config, this.encryptionSecret);

    return driver;
  }

  /**
   * 根据slug获取文件完整信息
   * @param {string} slug - 文件slug
   * @returns {Promise<Object>} 文件对象
   * @throws {Error} 如果文件不存在
   */
  async getFileBySlug(slug) {
    if (!slug) {
      throw new Error("缺少文件slug参数");
    }

    const file = await this.fileRepository.findBySlugWithStorageConfig(slug);

    if (!file) {
      throw new Error("文件不存在");
    }

    return file;
  }

  /**
   * 检查文件是否可访问
   * @param {Object} file - 文件对象
   * @returns {Object} 包含accessible和reason的对象
   */
  isFileAccessible(file) {
    return this.validateFileAccess(file);
  }

  /**
   * 增加文件查看次数并检查是否超过限制
   * @param {string} slug - 文件slug
   * @returns {Promise<Object>} 包含isExpired和file的对象
   */
  async incrementAndCheckFileViews(slug) {
    // 获取文件信息
    const file = await this.getFileBySlug(slug);

    // 增加views计数
    await this.fileRepository.incrementViews(file.id);

    // 重新获取文件信息，包括更新后的views计数
    const updatedFile = await this.fileRepository.findBySlugWithStorageConfig(slug);

    // 检查是否达到最大查看次数限制
    const accessResult = this.validateFileAccess(updatedFile);

    return {
      isExpired: !accessResult.accessible,
      file: updatedFile,
    };
  }

  /**
   * 生成文件下载URL
   * @param {Object} file - 文件对象
   * @param {string} encryptionSecret - 加密密钥
   * @param {Request} request - 原始请求对象，用于获取当前域名
   * @returns {Promise<Object>} 包含预览链接和下载链接的对象
   */
  async generateFileDownloadUrl(file, encryptionSecret, request = null) {
    let previewUrl = file.s3_url; // 默认使用原始URL作为回退
    let downloadUrl = file.s3_url; // 默认使用原始URL作为回退

    // 获取当前域名作为基础URL
    let baseUrl = "";
    if (request) {
      try {
        const url = new URL(request.url);
        baseUrl = url.origin; // 包含协议和域名，如 https://example.com
      } catch (error) {
        console.error("解析请求URL出错:", error);
        // 如果解析失败，baseUrl保持为空字符串
      }
    }

    // 构建代理URL，确保使用完整的绝对URL
    let proxyPreviewUrl = baseUrl ? `${baseUrl}/api/file-view/${file.slug}` : `/api/file-view/${file.slug}`;
    let proxyDownloadUrl = baseUrl ? `${baseUrl}/api/file-download/${file.slug}` : `/api/file-download/${file.slug}`;

    // 根据存储类型生成预览和下载URL
    if (file.storage_config_id && file.storage_path && file.storage_type) {
      if (file.storage_type === "S3") {
        const s3Config = await this.s3ConfigRepository.findById(file.storage_config_id);
        if (s3Config) {
          try {
            // 生成预览URL，使用S3配置的默认时效
            // 注意：文件分享页面没有用户上下文，禁用缓存避免权限泄露
            previewUrl = await generatePresignedUrl(s3Config, file.storage_path, encryptionSecret, null, false, null, { enableCache: false });

            // 生成下载URL，使用S3配置的默认时效，强制下载
            downloadUrl = await generatePresignedUrl(s3Config, file.storage_path, encryptionSecret, null, true, null, { enableCache: false });
          } catch (error) {
            console.error("生成预签名URL错误:", error);
            // 如果生成预签名URL失败，回退到使用原始S3 URL
          }
        }
      }
      // 未来可以在这里添加其他存储类型的处理逻辑
      // else if (file.storage_type === "WebDAV") {
      //   // WebDAV存储类型的URL生成逻辑
      // }
    }

    return {
      previewUrl,
      downloadUrl,
      proxyPreviewUrl,
      proxyDownloadUrl,
      use_proxy: file.use_proxy || 0,
    };
  }

  /**
   * 获取文件的公开信息
   * @param {Object} file - 文件对象
   * @param {boolean} requiresPassword - 是否需要密码
   * @param {Object} urlsObj - URL对象
   * @returns {Promise<Object>} 公开文件信息
   */
  async getPublicFileInfo(file, requiresPassword, urlsObj = null) {
    // 确定使用哪种URL
    const useProxy = urlsObj?.use_proxy !== undefined ? urlsObj.use_proxy : file.use_proxy || 0;

    // 根据是否使用代理选择URL
    const effectivePreviewUrl = useProxy === 1 ? urlsObj?.proxyPreviewUrl : urlsObj?.previewUrl || file.s3_url;
    const effectiveDownloadUrl = useProxy === 1 ? urlsObj?.proxyDownloadUrl : urlsObj?.downloadUrl || file.s3_url;

    // 获取文件类型
    const fileType = await GetFileType(file.filename, this.db);
    const fileTypeName = await getFileTypeName(file.filename, this.db);

    return {
      id: file.id,
      slug: file.slug,
      filename: file.filename,
      mimetype: file.mimetype,
      size: file.size,
      remark: file.remark,
      created_at: file.created_at,
      requires_password: requiresPassword,
      views: file.views,
      max_views: file.max_views,
      expires_at: file.expires_at,
      previewUrl: effectivePreviewUrl,
      downloadUrl: effectiveDownloadUrl,
      s3_direct_preview_url: urlsObj?.previewUrl || file.s3_url,
      s3_direct_download_url: urlsObj?.downloadUrl || file.s3_url,
      proxy_preview_url: urlsObj?.proxyPreviewUrl,
      proxy_download_url: urlsObj?.proxyDownloadUrl,
      use_proxy: useProxy,
      created_by: file.created_by || null,
      type: fileType, // 整数类型常量 (0-6)
      typeName: fileTypeName, // 类型名称（用于调试）
    };
  }

  /**
   * 根据存储路径删除文件记录
   * @param {string} storageConfigId - 存储配置ID
   * @param {string} storagePath - 存储路径
   * @param {string} storageType - 存储类型
   * @returns {Promise<Object>} 删除结果，包含deletedCount字段
   */
  async deleteFileRecordByStoragePath(storageConfigId, storagePath, storageType) {
    return await this.fileRepository.deleteByStorageConfigPath(storageConfigId, storagePath, storageType);
  }

  /**
   * 创建文件记录
   * @param {Object} fileData - 文件数据
   * @returns {Promise<Object>} 创建结果
   */
  async createFileRecord(fileData) {
    return await this.fileRepository.createFile(fileData);
  }

  /**
   * 更新文件记录
   * @param {string} fileId - 文件ID
   * @param {Object} updateData - 更新数据
   * @returns {Promise<Object>} 更新结果
   */
  async updateFileRecord(fileId, updateData) {
    return await this.fileRepository.updateFile(fileId, updateData);
  }

  /**
   * 更新文件元数据（业务逻辑层）
   * @param {string} fileId - 文件ID
   * @param {Object} updateData - 更新数据
   * @param {Object} userInfo - 用户信息
   * @param {string} userInfo.userType - 用户类型 ("admin" | "apikey")
   * @param {string} userInfo.userId - 用户ID
   * @returns {Promise<Object>} 更新结果
   */
  async updateFile(fileId, updateData, userInfo) {
    const { userType, userId } = userInfo;

    // 检查文件是否存在并验证权限
    let existingFile;
    if (userType === "admin") {
      // 管理员：可以更新任何文件
      existingFile = await this.fileRepository.findById(fileId);
      if (!existingFile) {
        throw new HTTPException(ApiStatus.NOT_FOUND, { message: "文件不存在" });
      }
    } else {
      // API密钥用户：只能更新自己的文件
      existingFile = await this.fileRepository.findOne(DbTables.FILES, {
        id: fileId,
        created_by: `apikey:${userId}`,
      });
      if (!existingFile) {
        throw new HTTPException(ApiStatus.NOT_FOUND, { message: "文件不存在或无权更新" });
      }
    }

    // 构建更新数据对象
    const finalUpdateData = {};

    // 处理可更新的字段
    if (updateData.remark !== undefined) {
      finalUpdateData.remark = updateData.remark;
    }

    // 处理 slug 更新（包含格式校验和冲突检查）
    if (updateData.slug !== undefined) {
      await this._validateAndProcessSlug(updateData.slug, fileId, userType);
      finalUpdateData.slug = updateData.slug;
    }

    // 处理过期时间
    if (updateData.expires_at !== undefined) {
      finalUpdateData.expires_at = updateData.expires_at;
    }

    // 处理Worker代理访问设置
    if (updateData.use_proxy !== undefined) {
      finalUpdateData.use_proxy = updateData.use_proxy ? 1 : 0;
    }

    // 处理最大查看次数
    if (updateData.max_views !== undefined) {
      finalUpdateData.max_views = updateData.max_views;
      finalUpdateData.views = 0; // 当修改max_views时，重置views计数为0
    }

    // 处理密码变更
    if (updateData.password !== undefined) {
      await this._processPasswordUpdate(fileId, updateData.password, finalUpdateData);
    }

    // 如果没有要更新的字段（API密钥用户需要检查）
    if (userType === "apikey" && Object.keys(finalUpdateData).length === 0) {
      throw new HTTPException(ApiStatus.BAD_REQUEST, { message: "没有提供有效的更新字段" });
    }

    // 添加更新时间
    finalUpdateData.updated_at = new Date().toISOString();

    // 使用 Repository 更新文件
    await this.fileRepository.updateFile(fileId, finalUpdateData);

    return {
      success: true,
      message: "文件元数据更新成功",
    };
  }

  /**
   * 验证并处理 slug 更新
   * @private
   * @param {string} slug - 新的 slug
   * @param {string} fileId - 文件ID
   * @param {string} userType - 用户类型
   */
  async _validateAndProcessSlug(slug, fileId, userType) {
    // 格式校验：只允许字母、数字、连字符、下划线、点号
    if (slug && !validateSlugFormat(slug)) {
      throw new HTTPException(ApiStatus.BAD_REQUEST, {
        message: "链接后缀格式无效，只能使用字母、数字、下划线、横杠和点号",
      });
    }

    // 检查slug是否可用 (不与其他文件冲突)
    let slugExistsCheck;
    if (userType === "admin") {
      slugExistsCheck = await this.fileRepository.findBySlug(slug);
      if (slugExistsCheck && slugExistsCheck.id !== fileId) {
        throw new HTTPException(ApiStatus.CONFLICT, {
          message: "此链接后缀已被其他文件使用",
        });
      }
    } else {
      slugExistsCheck = await this.fileRepository.findBySlugExcludingId(slug, fileId);
      if (slugExistsCheck) {
        throw new HTTPException(ApiStatus.CONFLICT, {
          message: "此链接后缀已被其他文件使用",
        });
      }
    }
  }

  /**
   * 处理密码更新
   * @private
   * @param {string} fileId - 文件ID
   * @param {string} password - 新密码（可能为空字符串表示清除）
   * @param {Object} updateData - 更新数据对象（引用传递）
   */
  async _processPasswordUpdate(fileId, password, updateData) {
    if (password) {
      // 设置新密码
      const passwordHash = await hashPassword(password);
      updateData.password = passwordHash;

      // 使用 FileRepository 更新或插入明文密码
      await this.fileRepository.upsertFilePasswordRecord(fileId, password);
    } else {
      // 明确提供了空密码，表示要清除密码
      updateData.password = null;

      // 使用 FileRepository 删除明文密码记录
      await this.fileRepository.deleteFilePasswordRecord(fileId);
    }
  }

  /**
   * 获取文件列表（管理员）
   * @param {Object} options - 查询选项
   * @param {number} options.limit - 每页条数
   * @param {number} options.offset - 偏移量
   * @param {string} options.createdBy - 创建者筛选
   * @param {string} options.search - 搜索关键词
   * @returns {Promise<Object>} 文件列表和分页信息
   */
  async getAdminFileList(options = {}) {
    const { limit = 30, offset = 0, createdBy, search } = options;

    // 如果有搜索关键词，使用搜索方法
    if (search && search.trim()) {
      const searchResult = await this.fileRepository.searchWithStorageConfig(search.trim(), {
        createdBy,
        limit,
        offset,
      });

      // 为搜索结果添加 type、typeName 和 key_name 字段
      if (searchResult.files) {
        searchResult.files = await this.processApiKeyNames(searchResult.files);
      }

      return searchResult;
    }

    // 构建查询条件
    const conditions = {};
    if (createdBy) conditions.created_by = createdBy;

    // 获取文件列表
    const files = await this.fileRepository.findManyWithStorageConfig(conditions, {
      orderBy: "created_at DESC",
      limit,
      offset,
    });

    // 获取总数
    const total = await this.fileRepository.count(conditions);

    // 处理API密钥名称
    const processedFiles = await this.processApiKeyNames(files);

    return {
      files: processedFiles,
      pagination: {
        total,
        limit,
        offset,
      },
    };
  }

  /**
   * 获取文件详情（管理员）
   * @param {string} fileId - 文件ID
   * @param {string} encryptionSecret - 加密密钥
   * @param {Object} request - 请求对象
   * @returns {Promise<Object>} 文件详情
   */
  async getAdminFileDetail(fileId, encryptionSecret, request = null) {
    // 获取文件详情
    const file = await this.fileRepository.findByIdWithStorageConfig(fileId);
    if (!file) {
      throw new Error("文件不存在");
    }

    // 生成文件下载URL
    const urlsObj = await this.generateFileDownloadUrl(file, encryptionSecret, request);

    // 构建响应
    const result = {
      ...file,
      has_password: file.password ? true : false,
      urls: urlsObj,
    };

    // 如果文件有密码保护，获取明文密码
    if (file.password) {
      const passwordInfo = await this.fileRepository.getFilePassword(file.id);
      if (passwordInfo && passwordInfo.plain_password) {
        result.plain_password = passwordInfo.plain_password;
      }
    }

    // 处理API密钥名称
    if (result.created_by && result.created_by.startsWith("apikey:")) {
      const keyId = result.created_by.substring(7);
      const keyInfo = await this.getApiKeyInfo(keyId);
      if (keyInfo) {
        result.key_name = keyInfo.name;
      }
    }

    return result;
  }

  /**
   * 处理文件列表中的API密钥名称和文件类型
   * @param {Array} files - 文件列表
   * @returns {Promise<Array>} 处理后的文件列表
   */
  async processApiKeyNames(files) {
    // 收集所有API密钥ID
    const apiKeyIds = files.filter((file) => file.created_by && file.created_by.startsWith("apikey:")).map((file) => file.created_by.substring(7));

    // 获取API密钥名称映射
    const keyNamesMap = new Map();
    if (apiKeyIds.length > 0) {
      const uniqueKeyIds = [...new Set(apiKeyIds)];

      for (const keyId of uniqueKeyIds) {
        const keyInfo = await this.getApiKeyInfo(keyId);
        if (keyInfo) {
          keyNamesMap.set(keyId, keyInfo.name);
        }
      }
    }

    // 为每个文件添加字段（包括文件类型检测）
    const processedFiles = await Promise.all(
      files.map(async (file) => {
        // 添加文件类型信息
        const fileType = await GetFileType(file.filename, this.db);
        const fileTypeName = await getFileTypeName(file.filename, this.db);

        const result = {
          ...file,
          has_password: file.password ? true : false,
          type: fileType, // 整数类型常量 (0-6)
          typeName: fileTypeName, // 类型名称（用于调试）
        };

        // 添加API密钥名称
        if (file.created_by && file.created_by.startsWith("apikey:")) {
          const keyId = file.created_by.substring(7);
          const keyName = keyNamesMap.get(keyId);
          if (keyName) {
            result.key_name = keyName;
          }
        }

        return result;
      })
    );

    return processedFiles;
  }

  /**
   * 获取API密钥信息
   * @param {string} keyId - API密钥ID
   * @returns {Promise<Object|null>} API密钥信息
   */
  async getApiKeyInfo(keyId) {
    // 这里需要通过Repository获取API密钥信息
    // 暂时使用直接查询，后续可以创建ApiKeyRepository
    const result = await this.fileRepository.queryFirst("SELECT id, name FROM api_keys WHERE id = ?", [keyId]);
    return result;
  }

  /**
   * 获取用户文件列表（API密钥用户）
   * @param {string} apiKeyId - API密钥ID
   * @param {Object} options - 查询选项
   * @param {number} options.limit - 每页条数
   * @param {number} options.offset - 偏移量
   * @param {string} options.search - 搜索关键词
   * @returns {Promise<Object>} 文件列表和分页信息
   */
  async getUserFileList(apiKeyId, options = {}) {
    const { limit = 30, offset = 0, search } = options;

    // 如果有搜索关键词，使用搜索方法
    if (search && search.trim()) {
      const searchResult = await this.fileRepository.searchWithStorageConfig(search.trim(), {
        createdBy: `apikey:${apiKeyId}`,
        limit,
        offset,
      });

      // 为搜索结果添加 type、typeName 和 key_name 字段
      if (searchResult.files) {
        searchResult.files = await this.processApiKeyNames(searchResult.files);
      }

      return searchResult;
    }

    // 构建查询条件
    const conditions = {
      created_by: `apikey:${apiKeyId}`,
    };

    // 获取文件列表
    const files = await this.fileRepository.findManyWithStorageConfig(conditions, {
      orderBy: "created_at DESC",
      limit,
      offset,
    });

    // 获取总数
    const total = await this.fileRepository.count(conditions);

    // 处理文件列表，添加has_password字段和API密钥名称
    const processedFiles = await this.processApiKeyNames(files);

    return {
      files: processedFiles,
      pagination: {
        total,
        limit,
        offset,
        hasMore: offset + limit < total,
      },
    };
  }

  /**
   * 获取用户文件详情（API密钥用户）
   * @param {string} fileId - 文件ID
   * @param {string} apiKeyId - API密钥ID
   * @param {string} encryptionSecret - 加密密钥
   * @param {Object} request - 请求对象
   * @returns {Promise<Object>} 文件详情
   */
  async getUserFileDetail(fileId, apiKeyId, encryptionSecret, request = null) {
    // 获取文件详情
    const file = await this.fileRepository.findByIdWithStorageConfig(fileId);
    if (!file) {
      throw new Error("文件不存在");
    }

    // 检查权限：确保文件属于该API密钥用户
    if (file.created_by !== `apikey:${apiKeyId}`) {
      throw new Error("没有权限查看此文件");
    }

    // 生成文件下载URL
    const urlsObj = await this.generateFileDownloadUrl(file, encryptionSecret, request);

    // 构建响应
    const result = {
      ...file,
      has_password: file.password ? true : false,
      urls: urlsObj,
    };

    // 如果文件有密码保护，获取明文密码
    if (file.password) {
      const passwordInfo = await this.fileRepository.getFilePassword(file.id);
      if (passwordInfo && passwordInfo.plain_password) {
        result.plain_password = passwordInfo.plain_password;
      }
    }

    return result;
  }
}

// 为了保持向后兼容，导出一些静态方法
export async function getFileBySlug(db, slug, encryptionSecret) {
  const fileService = new FileService(db, encryptionSecret);
  return await fileService.getFileBySlug(slug);
}

export async function isFileAccessible(db, file, encryptionSecret) {
  const fileService = new FileService(db, encryptionSecret);
  return fileService.isFileAccessible(file);
}

export async function incrementAndCheckFileViews(db, file, encryptionSecret) {
  const fileService = new FileService(db, encryptionSecret);
  return await fileService.incrementAndCheckFileViews(file.slug);
}

export async function generateFileDownloadUrl(db, file, encryptionSecret, request = null) {
  const fileService = new FileService(db, encryptionSecret);
  return await fileService.generateFileDownloadUrl(file, encryptionSecret, request);
}

export async function getPublicFileInfo(file, requiresPassword, urlsObj = null) {
  // 使用类方法，避免代码重复
  const fileService = new FileService(null);
  return await fileService.getPublicFileInfo(file, requiresPassword, urlsObj);
}

export async function deleteFileRecordByStoragePath(db, storageConfigId, storagePath, storageType) {
  const fileService = new FileService(db);
  return await fileService.deleteFileRecordByStoragePath(storageConfigId, storagePath, storageType);
}

// 管理员文件管理导出函数
export async function getAdminFileList(db, options = {}) {
  const fileService = new FileService(db);
  return await fileService.getAdminFileList(options);
}

export async function getAdminFileDetail(db, fileId, encryptionSecret, request = null) {
  const fileService = new FileService(db);
  return await fileService.getAdminFileDetail(fileId, encryptionSecret, request);
}

// 用户文件管理导出函数
export async function getUserFileList(db, apiKeyId, options = {}) {
  const fileService = new FileService(db);
  return await fileService.getUserFileList(apiKeyId, options);
}

export async function getUserFileDetail(db, fileId, apiKeyId, encryptionSecret, request = null) {
  const fileService = new FileService(db);
  return await fileService.getUserFileDetail(fileId, apiKeyId, encryptionSecret, request);
}

// 文件更新导出函数
export async function updateFile(db, fileId, updateData, userInfo) {
  const fileService = new FileService(db);
  return await fileService.updateFile(fileId, updateData, userInfo);
}
