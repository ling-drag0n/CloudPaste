/**
 * 压缩文件处理服务
 * 统一封装 zip.js 和 libarchive.js，提供一致的 API 接口
 */

import { getArchiveType } from "@/utils/fileTypes";
import { LRUCache } from "@/utils/lruCache.js";
import { zipService } from "./zipService.js";
import { libarchiveService } from "./libarchiveService.js";
import { ARCHIVE_CONSTANTS, clearSharedFileBlobCache } from "./archiveUtils.js";

/**
 * 压缩文件处理服务类
 */
class ArchiveService {
  constructor() {
    // 配置对象
    this.config = {
      cacheEnabled: true,
    };

    // 解压结果缓存
    this.extractCache = new LRUCache(ARCHIVE_CONSTANTS.CACHE.EXTRACT_SIZE);

    // 文件Blob缓存（用于密码验证时避免重复下载）
    this.fileBlobCache = new LRUCache(ARCHIVE_CONSTANTS.CACHE.FILE_BLOB_SIZE);
  }

  // ===================================================================
  // 统一入口
  // ===================================================================

  /**
   * 统一的压缩文件解压接口
   * @param {Blob|File|string} fileBlobOrUrl - 压缩文件 Blob 对象或URL
   * @param {string} filename - 文件名
   * @param {string} fileUrl - 文件URL（用于缓存键）
   * @param {Function} progressCallback - 进度回调函数 (progress: 0-100)
   * @param {string|null} password - 可选的解压密码
   * @returns {Promise<Array>} 统一格式的文件列表
   */
  async extractArchive(fileBlobOrUrl, filename, fileUrl = "", progressCallback = null, password = null) {
    console.log("开始解压文件:", filename);

    // 缓存检查
    if (this.config.cacheEnabled && fileUrl) {
      const cacheKey = `${fileUrl}_${filename}`;
      const cached = this.extractCache.get(cacheKey);
      if (cached) {
        console.log("使用缓存的解压结果:", filename);
        if (progressCallback) progressCallback(100);
        return cached;
      }
    }

    const archiveType = getArchiveType(filename);

    if (!archiveType.supported) {
      throw new Error(`不支持的压缩格式: ${archiveType.name}`);
    }

    // 根据格式和密码情况选择最优处理方式
    let result;
    if (archiveType.name === "ZIP") {
      // 使用zipService处理ZIP文件
      result = await zipService.extractArchive(fileBlobOrUrl, filename, fileUrl, progressCallback, password);
    } else {
      // 使用libarchiveService处理其他格式文件
      result = await libarchiveService.extractArchive(fileBlobOrUrl, filename, fileUrl, progressCallback, password, archiveType);
    }

    // 缓存结果
    if (this.config.cacheEnabled && fileUrl && result) {
      const cacheKey = `${fileUrl}_${filename}`;
      this.extractCache.set(cacheKey, result);
    }

    console.log(`解压完成，共 ${result.length} 个文件/目录`);
    return result;
  }

  // ===================================================================
  // 缓存管理
  // ===================================================================

  /**
   * 清除解压结果缓存
   * @param {string} fileUrl - 文件URL
   * @param {string} filename - 文件名
   */
  clearFileCache(fileUrl, filename) {
    if (!fileUrl || !filename) return;

    const cacheKey = `${fileUrl}_${filename}`;
    this.extractCache.delete(cacheKey);
    console.log("已清除解压缓存:", cacheKey);
  }

  /**
   * 清除文件Blob缓存
   * @param {string} fileUrl - 文件URL
   */
  clearFileBlobCache(fileUrl) {
    if (!fileUrl) return;

    this.fileBlobCache.delete(fileUrl);
    // 清除共享的文件Blob缓存
    clearSharedFileBlobCache(fileUrl);
    console.log("已清除文件Blob缓存:", fileUrl);
  }

  // ===================================================================
  // 工具方法
  // ===================================================================

  /**
   * 统一的文件内容获取接口
   * @param {Object} entryWrapper - 文件条目包装对象
   * @returns {Promise<ArrayBuffer>} 文件内容
   */
  async getFileContent(entryWrapper) {
    try {
      return await entryWrapper.getContent();
    } catch (error) {
      console.error("获取文件内容失败:", error);
      throw new Error(`获取文件内容失败: ${error.message}`);
    }
  }
}

// 导出单例实例
export const archiveService = new ArchiveService();
export default archiveService;
