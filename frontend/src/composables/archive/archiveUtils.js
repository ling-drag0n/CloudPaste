/**
 * 压缩文件处理工具类
 * 统一管理常量、工具方法和缓存，避免代码重复
 */

import { LRUCache } from "@/utils/lruCache.js";

// ===================================================================
// 统一常量定义
// ===================================================================

export const ARCHIVE_CONSTANTS = {
  // 缓存配置
  CACHE: {
    EXTRACT_SIZE: 10, // 解压结果缓存大小
    FILE_BLOB_SIZE: 2, // 文件Blob缓存大小
    TTL: 10 * 60 * 1000, // 缓存时间：10分钟
  },
  // 检测配置
  DETECTION: {
    MAX_CHECK_ENTRIES: 3, // 加密检测最大检查条目数
  },
  // 进度分配
  PROGRESS: {
    DOWNLOAD_RATIO: 70, // 下载进度占比：70%
    EXTRACT_RATIO: 30, // 解压进度占比：30%
  },
  // 估算配置
  ESTIMATION: {
    INITIAL_ENTRIES: 10, // 初始估算条目数
  },
};

// ===================================================================
// 共享缓存实例
// ===================================================================

/**
 * 共享的文件Blob缓存实例
 * 所有服务共用，避免重复创建
 */
export const sharedFileBlobCache = new LRUCache(ARCHIVE_CONSTANTS.CACHE.FILE_BLOB_SIZE);

// ===================================================================
// 共享工具方法
// ===================================================================

/**
 * 获取或下载文件Blob（带缓存）
 * @param {string} fileUrl - 文件URL
 * @param {Function} progressCallback - 进度回调
 * @param {number} startProgress - 起始进度
 * @param {number} endProgress - 结束进度
 * @param {string} stage - 阶段描述
 * @returns {Promise<Blob>} 文件Blob
 */
export async function getOrDownloadFileBlob(fileUrl, progressCallback = null, startProgress = 0, endProgress = 100, stage = "下载中") {
  const cacheKey = fileUrl;
  const cachedBlob = sharedFileBlobCache.get(cacheKey);

  if (cachedBlob) {
    console.log("使用缓存文件:", fileUrl);
    if (progressCallback) progressCallback(endProgress, "使用缓存文件");
    return cachedBlob;
  }

  // 下载并缓存
  const fileBlob = await downloadFileWithProgress(fileUrl, progressCallback, startProgress, endProgress, stage);
  sharedFileBlobCache.set(cacheKey, fileBlob, ARCHIVE_CONSTANTS.CACHE.TTL);
  console.log("文件已下载并缓存:", fileUrl);
  return fileBlob;
}

/**
 * 通用文件下载方法（带进度监控）
 * @param {string} url - 文件URL
 * @param {Function} progressCallback - 进度回调函数
 * @param {number} progressStart - 进度起始值 (0-100)
 * @param {number} progressEnd - 进度结束值 (0-100)
 * @param {string} stage - 进度阶段描述
 * @returns {Promise<Blob>} 下载的文件Blob
 */
export async function downloadFileWithProgress(url, progressCallback = null, progressStart = 0, progressEnd = 100, stage = "下载中") {
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`HTTP ${response.status}: ${response.statusText}`);
  }

  const contentLength = response.headers.get("content-length");
  const total = contentLength ? parseInt(contentLength, 10) : 0;
  let loaded = 0;

  const reader = response.body.getReader();
  const chunks = [];

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;

    chunks.push(value);
    loaded += value.length;

    if (total > 0 && progressCallback) {
      const progress = progressStart + (loaded / total) * (progressEnd - progressStart);
      progressCallback(Math.min(progress, progressEnd), stage);
    }
  }

  return new Blob(chunks);
}

/**
 * 检查是否支持 WebAssembly
 * @returns {boolean} 是否支持
 */
export function isWebAssemblySupported() {
  return typeof WebAssembly === "object" && typeof WebAssembly.instantiate === "function";
}

/**
 * 清除共享文件Blob缓存
 * @param {string} fileUrl - 文件URL
 */
export function clearSharedFileBlobCache(fileUrl) {
  if (!fileUrl) return;

  sharedFileBlobCache.delete(fileUrl);
  console.log("已清除共享文件Blob缓存:", fileUrl);
}

/**
 * 递归计算文件总数（libarchive.js辅助方法）
 * @param {Object} filesObject - getFilesObject()返回的嵌套对象
 * @returns {number} 总文件数量
 */
export function countTotalFiles(filesObject) {
  let count = 0;

  const countRecursive = (obj) => {
    for (const [, item] of Object.entries(obj)) {
      if (item && typeof item.extract === "function") {
        // CompressedFile
        count++;
      } else if (typeof item === "object" && item !== null) {
        // 目录
        count++;
        countRecursive(item);
      }
    }
  };

  countRecursive(filesObject);
  return count;
}

/**
 * 递归计算已解压文件总数（extractFiles()返回的数据）
 * @param {Object} extractedFiles - extractFiles()返回的嵌套对象
 * @returns {number} 总文件数量
 */
export function countExtractedFiles(extractedFiles) {
  let count = 0;

  const countRecursive = (obj) => {
    for (const [, item] of Object.entries(obj)) {
      if (item instanceof File) {
        // 已解压的File对象
        count++;
      } else if (typeof item === "object" && item !== null) {
        // 目录，递归计算
        count++;
        countRecursive(item);
      }
    }
  };

  countRecursive(extractedFiles);
  return count;
}
