/**
 * S3Adapter for Uppy.js
 * 重构版本：内部模块化，保持对外API不变
 */

import { useAuthStore } from "../../stores/authStore.js";
import * as fsApi from "../../api/services/fsService.js";

// ===== 内部工具类 =====

/**
 * 缓存管理器 - 处理localStorage和内存缓存
 */
class CacheManager {
  constructor(config) {
    this.config = config;
    this.memoryCache = new Map();
    this.cacheHits = 0;
    this.cacheMisses = 0;
  }

  getCachedParts(key) {
    // 先检查内存缓存
    if (this.memoryCache.has(key)) {
      this.cacheHits++;
      const cached = this.memoryCache.get(key);
      if (Date.now() - cached.timestamp < this.config.cacheExpiry) {
        return cached.parts;
      } else {
        this.memoryCache.delete(key);
      }
    }

    // localStorage 缓存
    this.cacheMisses++;
    try {
      const storageKey = this.config.storagePrefix + key;
      const stored = localStorage.getItem(storageKey);
      if (stored) {
        const data = JSON.parse(stored);
        const now = Date.now();
        if (now - data.timestamp < this.config.cacheExpiry) {
          // 更新内存缓存
          this.memoryCache.set(key, data);
          if (data.parts.length > 0) {
            const partNumbers = data.parts.map((p) => p.PartNumber).sort((a, b) => a - b);
            console.log(`[CacheManager] 缓存命中: [${partNumbers.join(", ")}] <- ${key}`);
          }
          return data.parts;
        } else {
          localStorage.removeItem(storageKey);
          console.log(`[CacheManager] 缓存已过期，已清理: ${key}`);
        }
      }
    } catch (error) {
      console.error(`[CacheManager] 读取缓存失败: ${key}`, error);
    }
    return [];
  }

  setCachedParts(key, parts) {
    const data = {
      parts: parts,
      timestamp: Date.now(),
    };

    try {
      // 更新 localStorage
      const storageKey = this.config.storagePrefix + key;
      localStorage.setItem(storageKey, JSON.stringify(data));

      // 更新内存缓存
      this.memoryCache.set(key, data);

      if (parts.length > 0) {
        const partNumbers = parts.map((p) => p.PartNumber).sort((a, b) => a - b);
        console.log(`[CacheManager] 缓存更新: [${partNumbers.join(", ")}] -> ${key}`);
      }
    } catch (error) {
      console.error(`[CacheManager] 保存缓存失败: ${key}`, error);
    }
  }

  addPartToCache(key, part) {
    const existingParts = this.getCachedParts(key);
    const updatedParts = [...existingParts];

    // 检查是否已存在该分片
    const existingIndex = updatedParts.findIndex((p) => p.PartNumber === part.PartNumber);
    if (existingIndex >= 0) {
      updatedParts[existingIndex] = part;
    } else {
      updatedParts.push(part);
    }

    this.setCachedParts(key, updatedParts);
  }

  getStats() {
    return {
      cacheHitRate: (this.cacheHits / (this.cacheHits + this.cacheMisses)) * 100,
      memoryCacheSize: this.memoryCache.size,
      cacheHits: this.cacheHits,
      cacheMisses: this.cacheMisses,
    };
  }

  clear() {
    this.memoryCache.clear();
    this.cacheHits = 0;
    this.cacheMisses = 0;
  }
}

/**
 * 会话管理器 - 处理上传会话的生命周期
 */
class SessionManager {
  constructor(config) {
    this.config = config;
    this.sessions = new Map();
    this.pausedFiles = new Set();

    // 定期清理过期会话
    this.cleanupInterval = setInterval(() => {
      this.cleanupExpiredSessions();
    }, 5 * 60 * 1000); // 5分钟清理一次
  }

  createSession(fileId, sessionData) {
    const session = {
      ...sessionData,
      createdAt: Date.now(),
      lastAccessAt: Date.now(),
    };
    this.sessions.set(fileId, session);
    return session;
  }

  getSession(fileId) {
    const session = this.sessions.get(fileId);
    if (session) {
      session.lastAccessAt = Date.now();
    }
    return session;
  }

  updateSession(fileId, updates) {
    const session = this.sessions.get(fileId);
    if (session) {
      Object.assign(session, updates, { lastAccessAt: Date.now() });
    }
  }

  deleteSession(fileId) {
    return this.sessions.delete(fileId);
  }

  setFilePaused(fileId, paused) {
    if (paused) {
      this.pausedFiles.add(fileId);
      console.log(`[SessionManager] 文件已暂停: ${fileId}`);
    } else {
      this.pausedFiles.delete(fileId);
      console.log(`[SessionManager] 文件已恢复: ${fileId}`);
    }
  }

  isFilePaused(fileId) {
    return this.pausedFiles.has(fileId);
  }

  cleanupExpiredSessions() {
    const now = Date.now();
    let cleanedCount = 0;

    for (const [fileId, session] of this.sessions) {
      if (now - session.lastAccessAt > this.config.sessionTimeout) {
        this.sessions.delete(fileId);
        this.pausedFiles.delete(fileId);
        cleanedCount++;
      }
    }

    if (cleanedCount > 0) {
      console.log(`[SessionManager] 清理了 ${cleanedCount} 个过期会话`);
    }
  }

  getStats() {
    return {
      activeSessions: this.sessions.size,
      pausedFiles: this.pausedFiles.size,
    };
  }

  destroy() {
    if (this.cleanupInterval) {
      clearInterval(this.cleanupInterval);
    }
    this.sessions.clear();
    this.pausedFiles.clear();
  }
}

/**
 * 认证提供器 - 处理认证相关逻辑
 */
class AuthProvider {
  constructor(authStore) {
    this.authStore = authStore;
  }

  getAuthHeaders() {
    const headers = {};

    // 检查管理员认证
    if (this.authStore.authType === "admin" && this.authStore.adminToken) {
      headers["Authorization"] = `Bearer ${this.authStore.adminToken}`;
    }
    // 检查API密钥认证
    else if (this.authStore.authType === "apikey" && this.authStore.apiKey) {
      headers["Authorization"] = `ApiKey ${this.authStore.apiKey}`;
    }

    return headers;
  }
}

/**
 * 路径解析器 - 处理路径转换逻辑
 */
class PathResolver {
  constructor(currentPath) {
    this.currentPath = currentPath;
  }

  updatePath(newPath) {
    this.currentPath = newPath;
  }

  buildFullPathFromKey(s3Key) {
    // 如果S3 Key已经包含完整路径，直接返回
    if (s3Key.startsWith("/")) {
      return s3Key;
    }

    // 规范化当前路径，去掉末尾斜杠
    const normalizedCurrentPath = this.currentPath.replace(/\/+$/, "");

    // 提取文件名
    const fileName = s3Key.split("/").pop();

    // 构建完整路径
    const result = `${normalizedCurrentPath}/${fileName}`;
    console.log(`[PathResolver] 最终路径: ${result}`);

    return result;
  }
}

/**
 * 错误处理器 - 统一错误处理逻辑
 */
class ErrorHandler {
  constructor(config) {
    this.config = config;
  }

  handleError(error, context, fallbackValue = null) {
    const errorMessage = error?.message || "未知错误";
    console.error(`[S3Adapter] ${context}失败:`, errorMessage, error);

    // 调用自定义错误处理器
    if (this.config.onError && typeof this.config.onError === "function") {
      this.config.onError(error, context);
    }

    return fallbackValue;
  }

  async retryOperation(operation, context = "操作") {
    const maxRetries = this.config.maxRetries || 3;
    const baseDelay = this.config.retryDelay || 1000;

    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        return await operation();
      } catch (error) {
        if (attempt === maxRetries) {
          throw this.handleError(error, `${context}(最终尝试)`);
        }

        const delay = baseDelay * Math.pow(2, attempt - 1); // 指数退避
        console.warn(`[ErrorHandler] ${context}失败，重试 ${attempt}/${maxRetries}，${delay}ms后重试:`, error.message);
        await new Promise((resolve) => setTimeout(resolve, delay));
      }
    }
  }
}

// ===== 主类 =====

export class S3Adapter {
  constructor(currentPath, uppyInstance = null, options = {}) {
    // 配置初始化
    this.config = {
      partSize: options.partSize || 5 * 1024 * 1024, // 5MB
      cacheExpiry: options.cacheExpiry || 24 * 60 * 60 * 1000, // 24小时
      storagePrefix: options.storagePrefix || "uppy_multipart_",
      maxRetries: options.maxRetries || 3,
      retryDelay: options.retryDelay || 1000,
      sessionTimeout: options.sessionTimeout || 60 * 60 * 1000, // 1小时
      onError: options.onError,
      ...options,
    };

    // 基本属性
    this.currentPath = currentPath;
    this.uppyInstance = uppyInstance;
    this.STORAGE_PREFIX = this.config.storagePrefix; // 保持向后兼容

    // 初始化内部模块
    this.cacheManager = new CacheManager(this.config);
    this.sessionManager = new SessionManager(this.config);
    this.authProvider = new AuthProvider(useAuthStore());
    this.pathResolver = new PathResolver(currentPath);
    this.errorHandler = new ErrorHandler(this.config);

    // 向后兼容的属性
    this.uploadSessions = this.sessionManager.sessions;
    this.customPausedFiles = this.sessionManager.pausedFiles;
    this.authStore = this.authProvider.authStore;
  }

  /**
   * 设置Uppy实例引用
   * @param {Object} uppyInstance Uppy实例
   */
  setUppyInstance(uppyInstance) {
    this.uppyInstance = uppyInstance;
  }

  /**
   * 设置文件暂停状态
   * @param {string} fileId 文件ID
   * @param {boolean} paused 是否暂停
   */
  setFilePaused(fileId, paused) {
    this.sessionManager.setFilePaused(fileId, paused);
  }

  /**
   * 获取性能统计信息
   * @returns {Object} 性能统计
   */
  getPerformanceStats() {
    return {
      ...this.cacheManager.getStats(),
      ...this.sessionManager.getStats(),
    };
  }

  /**
   * 更新当前路径
   * @param {string} newPath 新路径
   */
  updatePath(newPath) {
    this.currentPath = newPath;
    this.pathResolver.updatePath(newPath);
  }

  /**
   * 销毁适配器，清理资源
   */
  destroy() {
    this.sessionManager.destroy();
    this.cacheManager.clear();
  }

  /**
   * 批量处理预签名上传的commit阶段
   * @param {Array} successfulFiles 成功上传的文件列表
   * @returns {Promise<{failures: Array}>} commit结果
   */
  async batchCommitPresignedUploads(successfulFiles) {
    if (!successfulFiles || successfulFiles.length === 0) {
      return { failures: [] };
    }

    console.log(`[S3Adapter] 开始批量commit ${successfulFiles.length} 个文件`);
    const failures = [];

    // 并发处理commit，提高性能
    const commitPromises = successfulFiles.map(async (file) => {
      try {
        await this.commitPresignedUpload(file, file.response);
        return { file, success: true };
      } catch (error) {
        console.error(`[S3Adapter] ❌ commit失败: ${file.name}`, error);
        failures.push({
          fileName: file.name,
          fileId: file.id,
          error: error.message,
          timestamp: new Date().toISOString(),
        });
        return { file, success: false, error };
      }
    });

    // 等待所有commit操作完成
    const results = await Promise.allSettled(commitPromises);

    // 统计结果
    const successCount = results.filter((r) => r.status === "fulfilled" && r.value.success).length;
    const failureCount = failures.length;

    console.log(`[S3Adapter] 批量commit完成: ${successCount}成功, ${failureCount}失败`);

    if (failures.length > 0) {
      console.warn(`[S3Adapter] commit失败详情:`, failures);
    }

    return {
      failures,
      successCount,
      failureCount,
      totalCount: successfulFiles.length,
    };
  }

  /**
   * 检查文件是否被暂停
   * @param {string} fileId 文件ID
   * @returns {boolean} 是否暂停
   */
  isFilePaused(fileId) {
    return this.customPausedFiles.has(fileId);
  }

  /**
   * 从上传URL获取对应的文件ID
   * @param {string} url 上传URL
   * @returns {string|null} 文件ID
   */
  getFileIdFromUrl(url) {
    // 从uploadSessions中查找匹配的文件ID
    for (const [fileId, session] of this.uploadSessions.entries()) {
      if (session.presignedUrls && session.presignedUrls.some((urlInfo) => url.includes(urlInfo.partNumber))) {
        return fileId;
      }
    }
    return null;
  }

  /**
   * 从localStorage获取已上传分片信息
   * @param {string} key S3 Key
   * @returns {Array} 已上传分片列表
   */
  getUploadedPartsFromStorage(key) {
    return this.cacheManager.getCachedParts(key);
  }

  /**
   * 将已上传分片信息保存到localStorage
   * @param {string} key S3 Key
   * @param {Array} parts 已上传分片列表
   */
  saveUploadedPartsToStorage(key, parts) {
    this.cacheManager.setCachedParts(key, parts);
  }

  /**
   * 从localStorage删除已上传分片信息
   * @param {string} key S3 Key
   */
  removeUploadedPartsFromStorage(key) {
    try {
      const storageKey = this.STORAGE_PREFIX + key;
      localStorage.removeItem(storageKey);
      console.log(`[S3Adapter] 从localStorage删除分片缓存: ${key}`);
    } catch (error) {
      console.warn(`[S3Adapter] 从localStorage删除失败:`, error);
    }
  }

  /**
   * 添加单个分片到localStorage缓存
   * @param {string} key S3 Key
   * @param {Object} part 分片信息 {PartNumber, ETag, Size}
   */
  addPartToStorage(key, part) {
    this.cacheManager.addPartToCache(key, part);
  }

  /**
   * 从服务器获取权威的已上传分片信息
   * @param {string} key S3 Key
   * @param {string} uploadId 上传ID
   * @param {string} fileName 文件名
   * @returns {Promise<Array>} 服务器端的权威分片列表
   */
  async getServerUploadedParts(key, uploadId, fileName) {
    return this.errorHandler
      .retryOperation(async () => {
        // 将S3 Key转换为完整的挂载点路径
        const fullPath = this.buildFullPathFromKey(key);
        console.log(`[S3Adapter] 从服务器获取分片信息: ${fullPath}`);

        const response = await fsApi.listMultipartParts(fullPath, uploadId, fileName);

        if (!response.success) {
          throw new Error(`服务器分片查询失败: ${response.message}`);
        }

        const serverParts = (response.data.parts || []).map((part) => ({
          PartNumber: part.partNumber,
          ETag: part.etag,
          Size: part.size,
          LastModified: part.lastModified,
        }));

        console.log(`[S3Adapter] 服务器返回${serverParts.length}个分片信息`);

        // 更新localStorage缓存为服务器端数据
        this.saveUploadedPartsToStorage(key, serverParts);

        return serverParts;
      }, "获取服务器分片信息")
      .catch((error) => {
        return this.errorHandler.handleError(error, "获取服务器分片信息", []);
      });
  }

  /**
   * 初始化已上传分片缓存（一次性从服务器获取数据）
   * @param {string} key S3 Key
   * @param {string} uploadId 上传ID
   * @param {string} fileName 文件名
   */
  async initializeUploadedPartsCache(key, uploadId, fileName) {
    try {
      console.log(`[S3Adapter] 初始化分片缓存: ${key}`);

      // 从服务器获取权威的已上传分片信息
      const serverParts = await this.getServerUploadedParts(key, uploadId, fileName);

      console.log(`[S3Adapter] 缓存初始化完成，后续uploadPartBytes将直接使用缓存`);
      return serverParts;
    } catch (error) {
      console.error(`[S3Adapter] 初始化分片缓存失败:`, error);
      // 失败时初始化为空缓存
      this.saveUploadedPartsToStorage(key, []);
      return [];
    }
  }

  /**
   * 更新当前路径
   * @param {string} newPath 新路径
   */
  updatePath(newPath) {
    this.currentPath = newPath;
  }

  /**
   * 获取认证头部 - 用于XHR Upload插件
   * @returns {Object} 认证头部对象
   */
  getAuthHeaders() {
    return this.authProvider.getAuthHeaders();
  }

  /**
   * 单文件上传参数获取 预签名URL上传
   * @param {Object} file Uppy文件对象
   * @param {Object} options 选项
   * @returns {Promise<Object>} {method, url, fields, headers}
   */
  async getUploadParameters(file, options = {}) {
    try {
      console.log(`[S3Adapter] 获取预签名URL上传参数: ${file.name}`);

      const response = await fsApi.getPresignedUploadUrl(this.currentPath, file.name, file.type, file.size);

      if (!response.success) {
        throw new Error(response.message || "获取预签名URL失败");
      }

      // 缓存上传信息，供commit使用
      this.uploadSessions.set(file.id, {
        targetPath: response.data.targetPath,
        mountId: response.data.mountId,
        fileId: response.data.fileId,
        s3Path: response.data.s3Path,
        s3Url: response.data.s3Url,
        s3ConfigId: response.data.s3ConfigId,
        contentType: response.data.contentType,
      });

      return {
        method: "PUT",
        url: response.data.presignedUrl,
        fields: {},
        headers: {
          "Content-Type": file.type || "application/octet-stream",
        },
      };
    } catch (error) {
      console.error("[S3Adapter] 获取预签名URL上传参数失败:", error);
      throw error;
    }
  }

  /**
   * 创建分片上传
   * @param {Object} file Uppy文件对象
   * @returns {Promise<Object>} {uploadId, key}
   */
  async createMultipartUpload(file) {
    try {
      console.log(`[S3Adapter] 创建分片上传: ${file.name}`);

      // 检查是否为ServerResume标记的可恢复上传
      if (file.meta.resumable && file.meta.existingUpload && file.meta.serverResume) {
        const existingUpload = file.meta.existingUpload;
        console.log(`[S3Adapter] 尝试恢复现有上传: uploadId=${existingUpload.uploadId}, key=${existingUpload.key}`);

        try {
          // 1. 先验证uploadId有效性 - 使用完整的挂载点路径
          const fullPathForValidation = this.buildFullPathFromKey(existingUpload.key);
          console.log(`[S3Adapter] 验证uploadId有效性: ${fullPathForValidation}`);
          const listPartsResponse = await fsApi.listMultipartParts(fullPathForValidation, existingUpload.uploadId, file.name);

          if (!listPartsResponse.success) {
            throw new Error(`uploadId已失效: ${listPartsResponse.message}`);
          }

          const uploadedParts = listPartsResponse.data.parts || [];
          console.log(`[S3Adapter] 🔍 服务器返回: 找到${uploadedParts.length}个已上传分片`);

          // 2. 计算需要刷新的分片编号
          const partSize = 5 * 1024 * 1024;
          const totalParts = Math.ceil(file.size / partSize);
          const partNumbers = Array.from({ length: totalParts }, (_, i) => i + 1);

          // 3. 将S3 Key转换为完整的挂载点路径用于刷新URL
          const fullPath = this.buildFullPathFromKey(existingUpload.key);
          console.log(`[S3Adapter] 路径转换: S3Key=${existingUpload.key} -> FullPath=${fullPath}`);

          // 4. 获取现有上传的预签名URL（需要刷新）
          const refreshResponse = await fsApi.refreshMultipartUrls(fullPath, existingUpload.uploadId, partNumbers);

          if (!refreshResponse.success) {
            throw new Error(refreshResponse.message || "刷新预签名URL失败");
          }

          // 5. 转换已上传分片为Uppy标准格式
          const standardParts = uploadedParts.map((part) => ({
            PartNumber: part.partNumber,
            Size: part.size,
            ETag: part.etag,
          }));

          // 6. 计算已上传的字节数和显示分片信息
          const uploadedBytes = uploadedParts.reduce((sum, part) => sum + part.size, 0);
          const progressPercent = Math.round((uploadedBytes / file.size) * 100);

          if (standardParts.length > 0) {
            const partNumbers = standardParts.map((p) => p.PartNumber).sort((a, b) => a - b);
            console.log(`[S3Adapter] 服务器已上传分片: [${partNumbers.join(", ")}] (${progressPercent}%)`);
          }

          // 7. 缓存刷新后的预签名URL和已上传分片信息
          this.uploadSessions.set(file.id, {
            uploadId: existingUpload.uploadId,
            key: existingUpload.key,
            presignedUrls: refreshResponse.data.presignedUrls,
            path: this.currentPath,
            fileName: file.name,
            resumed: true, // 标记为恢复的上传
          });

          // 8. 将服务器的已上传分片信息保存到localStorage
          // 使用完整路径格式作为缓存key，与listParts保持一致
          const fullPathKey = this.buildFullPathFromKey(existingUpload.key);
          this.saveUploadedPartsToStorage(fullPathKey, standardParts);
          console.log(`[S3Adapter] 缓存到localStorage: ${standardParts.length}个分片 -> ${fullPathKey}`);

          // 重要：不要在这里手动设置进度，让Uppy通过listParts自然处理

          console.log(`[S3Adapter] 断点续传恢复成功`);
          return {
            uploadId: existingUpload.uploadId,
            key: existingUpload.key,
          };
        } catch (error) {
          console.warn(`[S3Adapter] 断点续传失败，创建新上传: ${error.message}`);

          // 清除失效的上传标记
          if (this.uppyInstance) {
            this.uppyInstance.setFileMeta(file.id, {
              resumable: false,
              existingUpload: null,
              serverResume: false,
            });
          }

          // 继续创建新的上传（不要递归调用，直接继续执行下面的代码）
        }
      }

      // 创建新的分片上传
      const partSize = 5 * 1024 * 1024; // 5MB
      const response = await fsApi.initMultipartUpload(this.currentPath, file.name, file.size, file.type, partSize);

      if (!response.success) {
        throw new Error(response.message || "初始化分片上传失败");
      }

      const uploadId = response.data.uploadId;
      const key = `${this.currentPath}/${file.name}`.replace(/\/+/g, "/");

      // 缓存预签名URL列表，供signPart使用
      this.uploadSessions.set(file.id, {
        uploadId,
        key,
        presignedUrls: response.data.presignedUrls,
        path: this.currentPath,
        fileName: file.name,
        resumed: false, // 标记为新的上传
      });

      // 对于新上传，也检查一次服务器是否有已上传分片（可能是其他会话的残留）
      // 使用完整路径格式作为缓存key
      const fullPathKey = this.buildFullPathFromKey(key);
      await this.initializeUploadedPartsCache(fullPathKey, uploadId, file.name);
      console.log(`[S3Adapter] 新上传初始化完成，已检查服务器状态，缓存key=${fullPathKey}`);

      return {
        uploadId,
        key,
      };
    } catch (error) {
      console.error("[S3Adapter] 创建分片上传失败:", error);
      throw error;
    }
  }

  /**
   * 签名分片
   * @param {Object} file Uppy文件对象
   * @param {Object} partData 分片数据 {uploadId, key, partNumber, body}
   * @returns {Promise<Object>} {url, headers}
   */
  async signPart(file, partData) {
    try {
      const session = this.uploadSessions.get(file.id);
      if (!session) {
        throw new Error("找不到上传会话信息");
      }

      console.log(`[S3Adapter] signPart被调用: 分片${partData.partNumber}`);

      // 不在signPart中处理已上传分片
      // 让Uppy通过listParts自然地处理断点续传

      // 从缓存的预签名URL列表中找到对应分片
      const urlInfo = session.presignedUrls.find((url) => url.partNumber === partData.partNumber);

      if (!urlInfo) {
        throw new Error(`找不到分片 ${partData.partNumber} 的预签名URL`);
      }

      return {
        url: urlInfo.url,
        headers: {
          "Content-Type": "application/octet-stream",
        },
      };
    } catch (error) {
      console.error("[S3Adapter] 签名分片失败:", error);
      throw error;
    }
  }

  /**
   * 完成分片上传
   * @param {Object} file Uppy文件对象
   * @param {Object} data {uploadId, key, parts}
   * @returns {Promise<Object>} {location}
   */
  async completeMultipartUpload(file, data) {
    try {
      console.log(`[S3Adapter] 完成分片上传: ${file.name}`);

      const session = this.uploadSessions.get(file.id);
      if (!session) {
        throw new Error("找不到上传会话信息");
      }

      // 检查Uppy传递的parts格式
      if (!data.parts || !Array.isArray(data.parts)) {
        throw new Error("无效的parts数据");
      }

      // Uppy内部使用AWS标准格式，直接传递即可
      const response = await fsApi.completeMultipartUpload(session.path, data.uploadId, data.parts, session.fileName, file.size);

      if (!response.success) {
        throw new Error(response.message || "完成分片上传失败");
      }

      // 清理上传会话和分片缓存
      this.uploadSessions.delete(file.id);
      if (session.key) {
        const fullPathKey = this.buildFullPathFromKey(session.key);
        this.removeUploadedPartsFromStorage(fullPathKey);
      }

      return {
        location: response.data.url || `${session.path}/${session.fileName}`,
      };
    } catch (error) {
      console.error("[S3Adapter] 完成分片上传失败:", error);
      throw error;
    }
  }

  /**
   * 中止分片上传
   * @param {Object} file Uppy文件对象
   * @param {Object} data {uploadId, key}
   */
  async abortMultipartUpload(file, data) {
    try {
      console.log(`[S3Adapter] 中止分片上传: ${file.name}`);

      const session = this.uploadSessions.get(file.id);
      if (session) {
        await fsApi.abortMultipartUpload(session.path, data.uploadId, session.fileName);
        // 清理上传会话和分片缓存
        this.uploadSessions.delete(file.id);
        if (session.key) {
          const fullPathKey = this.buildFullPathFromKey(session.key);
          this.removeUploadedPartsFromStorage(fullPathKey);
        }
      }
    } catch (error) {
      console.error("[S3Adapter] 中止分片上传失败:", error);
      // 中止操作失败不应该抛出错误，只记录日志
    }
  }

  /**
   * 列出已上传的分片
   * 使用前端缓存，避免重复调用后端API
   * @param {Object} file Uppy文件对象
   * @param {Object} options {uploadId, key}
   * @returns {Promise<Array>} 分片列表
   */
  async listParts(file, { uploadId, key }) {
    try {
      console.log(`[S3Adapter] listParts被调用: ${file.name}, uploadId: ${uploadId}, key: ${key}`);

      // 直接从localStorage返回已上传分片信息
      const cachedParts = this.getUploadedPartsFromStorage(key);
      console.log(`[S3Adapter] 从localStorage返回${cachedParts.length}个已上传分片`);
      console.log(`[S3Adapter] 缓存的分片信息:`, cachedParts);

      return cachedParts;
    } catch (error) {
      console.error("[S3Adapter] listParts失败:", error);
      return [];
    }
  }

  /**
   * 上传分片字节
   * 控制实际的分片上传过程，在这里处理已上传分片的跳过逻辑
   * @param {Object} options {signature, body, onComplete, size, onProgress, signal}
   * @returns {Promise<Object>} {ETag}
   */
  async uploadPartBytes({ signature, body, onComplete, size, onProgress, signal }) {
    try {
      const { url, headers } = signature;

      if (!url) {
        throw new Error("Cannot upload to an undefined URL");
      }

      console.log(`[S3Adapter] uploadPartBytes被调用: ${url}`);

      // 解析URL获取key和partNumber
      const urlObject = new URL(url);
      const pathParts = urlObject.pathname.split("/");
      // 获取S3的相对路径（与createMultipartUpload返回的key格式一致）
      const s3Key = pathParts.slice(1).join("/"); // 去掉第一个空字符串，获取完整路径
      const partNumber = parseInt(urlObject.searchParams.get("partNumber"), 10);

      console.log(`[S3Adapter] 🔄 处理分片${partNumber}上传...`);

      // 将S3 Key转换为与listParts一致的完整路径格式
      const key = this.buildFullPathFromKey(s3Key);

      // 直接使用localStorage缓存（已在createMultipartUpload时一次性从服务器获取）
      const cachedParts = this.getUploadedPartsFromStorage(key);
      const existingPart = cachedParts.find((part) => part.PartNumber === partNumber);

      if (existingPart) {
        console.log(`[S3Adapter] ✅ 分片${partNumber}已缓存，跳过上传 (ETag: ${existingPart.ETag})`);

        // 模拟一个瞬间完成的上传过程，而不是直接跳过
        return new Promise((resolve) => {
          // 使用setTimeout确保异步执行，让Uppy有时间设置内部状态
          setTimeout(() => {
            // 触发进度事件
            onProgress(size);
            // 触发完成回调
            onComplete(existingPart.ETag);
            // 返回结果
            resolve({ ETag: existingPart.ETag });
          }, 0);
        });
      }

      // 检查文件是否被自定义暂停
      const fileId = this.getFileIdFromUrl(url);
      if (fileId && this.isFilePaused(fileId)) {
        console.log(`[S3Adapter] ⏸️ 分片${partNumber}被暂停，等待恢复...`);

        // 返回一个等待恢复的Promise
        return new Promise((resolve, reject) => {
          const checkResumeInterval = setInterval(() => {
            if (!this.isFilePaused(fileId)) {
              clearInterval(checkResumeInterval);
              console.log(`[S3Adapter] ▶️ 分片${partNumber}恢复上传`);
              // 递归调用自己来执行实际上传
              this.uploadPartBytes({ signature, body, onComplete, size, onProgress, signal }).then(resolve).catch(reject);
            }
          }, 100); // 每100ms检查一次恢复状态

          // 监听取消信号
          if (signal) {
            signal.addEventListener("abort", () => {
              clearInterval(checkResumeInterval);
              reject(new DOMException("The operation was aborted", "AbortError"));
            });
          }
        });
      }

      // 执行实际的分片上传
      return new Promise((resolve, reject) => {
        const xhr = new XMLHttpRequest();
        xhr.open("PUT", url, true);

        if (headers) {
          Object.keys(headers).forEach((key) => {
            xhr.setRequestHeader(key, headers[key]);
          });
        }

        xhr.responseType = "text";

        // 处理取消信号
        function onabort() {
          xhr.abort();
        }
        function cleanup() {
          if (signal) {
            signal.removeEventListener("abort", onabort);
          }
        }
        if (signal) {
          signal.addEventListener("abort", onabort);
        }

        xhr.onabort = () => {
          cleanup();
          const err = new DOMException("The operation was aborted", "AbortError");
          reject(err);
        };

        xhr.upload.addEventListener("progress", onProgress);

        xhr.addEventListener("load", (ev) => {
          cleanup();
          const target = ev.target;

          if (target.status < 200 || target.status >= 300) {
            const error = new Error(`HTTP ${target.status}: ${target.statusText}`);
            error.source = target;
            reject(error);
            return;
          }

          onProgress(size);

          // 获取ETag
          const etag = target.getResponseHeader("ETag");
          if (etag === null) {
            reject(new Error("Could not read the ETag header. This likely means CORS is not configured correctly."));
            return;
          }

          // 将成功上传的分片添加到localStorage缓存
          this.addPartToStorage(key, {
            ETag: etag,
            PartNumber: partNumber,
            Size: size,
          });

          console.log(`[S3Adapter] 🚀 分片${partNumber}上传成功，添加到localStorage (ETag: ${etag})`);

          onComplete(etag);
          resolve({ ETag: etag });
        });

        xhr.addEventListener("error", (ev) => {
          cleanup();
          const error = new Error("Upload failed");
          error.source = ev.target;
          reject(error);
        });

        xhr.send(body);
      });
    } catch (error) {
      console.error("[S3Adapter] uploadPartBytes失败:", error);
      throw error;
    }
  }

  /**
   * 提交预签名上传完成 - CloudPaste特有功能
   * @param {Object} file Uppy文件对象
   * @param {Object} response 上传响应
   * @returns {Promise<Object>} 提交结果
   */
  async commitPresignedUpload(file, response) {
    try {
      console.log(`[S3Adapter] 提交预签名上传完成: ${file.name}`);

      // 获取缓存的上传信息
      const uploadInfo = this.uploadSessions.get(file.id);
      if (!uploadInfo) {
        throw new Error("找不到上传会话信息");
      }

      // 从响应中提取ETag（如果有的话）
      const etag = response?.etag || response?.ETag || null;

      // 调用commit接口，使用正确的参数格式
      const commitResponse = await fsApi.commitPresignedUpload(
        {
          targetPath: uploadInfo.targetPath,
          mountId: uploadInfo.mountId,
          fileId: uploadInfo.fileId,
          s3Path: uploadInfo.s3Path,
          s3Url: uploadInfo.s3Url,
          s3ConfigId: uploadInfo.s3ConfigId,
          contentType: uploadInfo.contentType,
        },
        etag,
        uploadInfo.contentType,
        file.size
      );

      if (!commitResponse.success) {
        throw new Error(commitResponse.message || "提交预签名上传失败");
      }

      // 清理上传会话
      this.uploadSessions.delete(file.id);

      console.log(`[S3Adapter] 预签名上传commit成功: ${file.name}`);
      return commitResponse;
    } catch (error) {
      console.error(`[S3Adapter] 预签名上传commit失败: ${file.name}`, error);
      throw error;
    }
  }

  /**
   * 清理所有上传会话和localStorage分片缓存
   */
  cleanup() {
    this.uploadSessions.clear();
    // 清理所有localStorage中的分片缓存
    this.clearAllUploadedPartsFromStorage();
    console.log(`[S3Adapter] 清理所有上传会话和localStorage分片缓存`);
  }

  /**
   * 清理所有localStorage中的分片缓存
   */
  clearAllUploadedPartsFromStorage() {
    try {
      const keysToRemove = [];
      for (let i = 0; i < localStorage.length; i++) {
        const key = localStorage.key(i);
        if (key && key.startsWith(this.STORAGE_PREFIX)) {
          keysToRemove.push(key);
        }
      }
      keysToRemove.forEach((key) => localStorage.removeItem(key));
      console.log(`[S3Adapter] 清理了${keysToRemove.length}个localStorage分片缓存`);
    } catch (error) {
      console.warn(`[S3Adapter] 清理localStorage失败:`, error);
    }
  }

  /**
   * 从S3 Key构建完整的挂载点路径
   * @param {string} s3Key S3的相对路径
   * @returns {string} 完整的挂载点路径
   */
  buildFullPathFromKey(s3Key) {
    return this.pathResolver.buildFullPathFromKey(s3Key);
  }
}
