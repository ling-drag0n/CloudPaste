/**
 * 压缩文件预览 Composable
 * 提供压缩文件解压、预览、下载等功能的响应式状态管理
 * 支持 ZIP、RAR、7Z、TAR、GZIP、BZIP2、XZ 等多种格式
 */

import { ref, computed, onUnmounted } from "vue";
import { archiveService } from "./archiveService.js";
import { cleanupZipJS } from "./zipService.js";
import { lookupMimeType, detectFileTypeFromFilename, FileType, getFileName } from "@/utils/fileTypes.js";

/**
 * 压缩文件预览 Composable
 * @returns {Object} 响应式状态和方法
 */
export function useArchivePreview() {
  // ===== 响应式状态 =====

  /** 是否正在解压 */
  const isExtracting = ref(false);

  /** 解压错误信息 */
  const extractError = ref(null);

  /** 压缩包文件列表 */
  const archiveEntries = ref([]);

  /** 是否已解压完成 */
  const isExtracted = ref(false);

  /** 当前预览的文件 */
  const currentPreviewFile = ref(null);

  /** 是否正在预览文件 */
  const isPreviewing = ref(false);

  /** 解压进度 (0-100) */
  const extractProgress = ref(0);

  /** 当前处理阶段 */
  const currentStage = ref("");

  // ===== 缓存状态 =====

  /** 缓存的文件名 */
  const cachedFileName = ref("");

  /** 缓存的文件URL */
  const cachedFileUrl = ref("");

  // ===== 计算属性 =====

  /** 总解压后大小 */
  const totalSize = computed(() => archiveEntries.value.reduce((total, entry) => total + (entry.size || 0), 0));

  // ===== 内部工具函数 =====

  /**
   * 统一的 Blob 数据创建（消除重复代码）
   * @param {ArrayBuffer|File} fileData - 文件数据
   * @param {string} mimeType - MIME类型
   * @returns {Blob} 标准化的 Blob 对象
   */
  const createBlobFromFileData = (fileData, mimeType) => {
    if (fileData instanceof File) {
      // libarchive.js 返回的 File 对象，MIME 类型可能不正确，需要重新创建
      return new Blob([fileData], { type: mimeType });
    } else {
      // Zip 返回的 ArrayBuffer，直接创建 Blob
      return new Blob([fileData], { type: mimeType });
    }
  };

  /**
   * 智能 URL 清理（根据文件大小调整延迟）
   * @param {string} url - 要清理的 URL
   * @param {number} fileSize - 文件大小
   */
  const smartUrlCleanup = (url, fileSize = 0) => {
    let delay = 10000; // 默认 10 秒

    // 大文件延长清理时间
    if (fileSize > 10 * 1024 * 1024) {
      // 10MB 以上
      delay = 20000; // 20 秒
    } else if (fileSize < 1024 * 1024) {
      // 1MB 以下
      delay = 5000; // 5 秒
    }

    setTimeout(() => URL.revokeObjectURL(url), delay);
  };

  // ===== 核心方法 =====

  /**
   * 解压压缩文件
   * @param {string} fileUrl - 文件下载URL
   * @param {string} fileName - 文件名
   * @param {string|null} password - 可选的解压密码
   * @returns {Promise<void>}
   */
  const extractArchive = async (fileUrl, fileName, password = null) => {
    if (isExtracting.value) {
      console.warn("正在解压中，请勿重复操作");
      return;
    }

    isExtracting.value = true;
    extractError.value = null;
    archiveEntries.value = [];
    isExtracted.value = false;
    extractProgress.value = 0;
    currentStage.value = "下载";

    try {
      console.log("开始处理压缩文件:", fileName);
      if (password) {
        console.log("使用提供的密码进行解压");
      }

      // 直接委托给archiveService，传递密码参数
      const processedEntries = await archiveService.extractArchive(
        fileUrl,
        fileName,
        fileUrl,
        (progress, stage) => {
          extractProgress.value = Math.round(progress * 10) / 10;
          if (stage) {
            currentStage.value = stage;
          }
        },
        password
      );

      // 只缓存必要信息
      cachedFileName.value = fileName;
      cachedFileUrl.value = fileUrl;

      archiveEntries.value = processedEntries;
      isExtracted.value = true;
      extractProgress.value = 100;
      currentStage.value = "完成";

      console.log("压缩文件处理完成，文件数量:", processedEntries.length);
    } catch (error) {
      console.error("解压失败:", error);

      // 检查是否是加密检测错误，如果是则重新抛出让上层处理
      if (error.message && (error.message.includes("ENCRYPTED_ARCHIVE_DETECTED") || error.message.includes("INVALID_ARCHIVE_PASSWORD"))) {
        console.log("检测到加密相关错误，重新抛出给上层处理");
        // 重置状态但不设置错误信息，让上层组件处理
        extractProgress.value = 0;
        currentStage.value = "";
        isExtracted.value = false;
        isExtracting.value = false;

        // 重新抛出错误给上层组件处理
        throw error;
      }

      // 其他错误正常处理
      extractError.value = error.message;
      extractProgress.value = 0; // 重置进度
      currentStage.value = "错误";
      isExtracted.value = false;
    } finally {
      isExtracting.value = false;
    }
  };

  /**
   * 预览文件 - 完整复制原有逻辑
   * @param {Object} entry - 文件条目
   * @returns {Promise<void>}
   */
  const previewFile = async (entry) => {
    if (entry.isDirectory) {
      console.warn("无法预览目录:", entry.name);
      return;
    }

    if (isPreviewing.value) {
      console.warn("正在预览其他文件，请稍候");
      return;
    }

    isPreviewing.value = true;
    currentPreviewFile.value = entry;

    try {
      console.log("开始预览文件:", entry.name);

      // 获取文件内容
      const arrayBuffer = await archiveService.getFileContent(entry.entry);

      // 根据文件类型进行不同的预览处理
      const fileName = entry.name;
      const type = detectFileTypeFromFilename(fileName);

      console.log(`预览文件: ${fileName}, 检测类型: ${type}`);

      if (type === FileType.TEXT) {
        // 文本文件：创建HTML页面在新标签页显示
        // 确保 arrayBuffer 是正确的类型
        let textBuffer;
        if (arrayBuffer instanceof ArrayBuffer) {
          textBuffer = arrayBuffer;
        } else if (arrayBuffer instanceof File) {
          // 如果是 File 对象，转换为 ArrayBuffer
          textBuffer = await arrayBuffer.arrayBuffer();
        } else if (arrayBuffer instanceof Uint8Array) {
          textBuffer = arrayBuffer.buffer;
        } else {
          throw new Error(`不支持的数据类型: ${typeof arrayBuffer}`);
        }

        const text = new TextDecoder("utf-8").decode(textBuffer);
        const htmlContent = createTextPreviewHtml(entry.name, text);
        const blob = new Blob([htmlContent], { type: "text/html" });
        const url = URL.createObjectURL(blob);
        window.open(url, "_blank");

        smartUrlCleanup(url, entry.size);
      } else if (type === FileType.IMAGE) {
        // 图片文件：使用正确的MIME类型
        const mimeType = lookupMimeType(fileName) || "image/jpeg";
        const blobData = createBlobFromFileData(arrayBuffer, mimeType);
        const url = URL.createObjectURL(blobData);
        window.open(url, "_blank");
        smartUrlCleanup(url, entry.size);
      } else if (type === FileType.DOCUMENT) {
        // PDF文件：直接在新标签页显示
        const blobData = createBlobFromFileData(arrayBuffer, "application/pdf");
        const url = URL.createObjectURL(blobData);
        window.open(url, "_blank");
        smartUrlCleanup(url, entry.size);
      } else if (type === FileType.VIDEO) {
        // 视频文件：直接打开（浏览器支持则预览，不支持则下载）
        const mimeType = lookupMimeType(fileName) || "video/mp4";
        const blobData = createBlobFromFileData(arrayBuffer, mimeType);
        const url = URL.createObjectURL(blobData);
        window.open(url, "_blank");
        smartUrlCleanup(url, entry.size);
      } else if (type === FileType.AUDIO) {
        // 音频文件：直接在新标签页显示
        const mimeType = lookupMimeType(fileName) || "audio/mpeg";
        const blobData = createBlobFromFileData(arrayBuffer, mimeType);
        const url = URL.createObjectURL(blobData);
        window.open(url, "_blank");
        smartUrlCleanup(url, entry.size);
      } else {
        // 不支持的类型（如Office文件）：下载文件
        const blobData = createBlobFromFileData(arrayBuffer, "application/octet-stream");
        const url = URL.createObjectURL(blobData);
        const a = document.createElement("a");
        a.href = url;
        a.download = getFileName(entry.name);
        a.click();
        URL.revokeObjectURL(url);
      }

      console.log("文件预览成功:", entry.name);
    } catch (error) {
      console.error("预览文件失败:", error);
      alert(`预览失败: ${error.message}`);
    } finally {
      isPreviewing.value = false;
      currentPreviewFile.value = null;
    }
  };

  /**
   * 下载单个文件
   * @param {Object} entry - 文件条目
   * @returns {Promise<void>}
   */
  const downloadFile = async (entry) => {
    if (entry.isDirectory) {
      console.warn("无法下载目录:", entry.name);
      return;
    }

    try {
      console.log("开始下载文件:", entry.name);

      // 获取文件内容
      const fileData = await archiveService.getFileContent(entry.entry);

      // 创建 Blob 对象（使用工具函数）
      const mimeType = lookupMimeType(entry.name) || "application/octet-stream";
      const blob = createBlobFromFileData(fileData, mimeType);

      // 创建下载链接
      const url = URL.createObjectURL(blob);
      const link = document.createElement("a");
      link.href = url;
      link.download = getFileName(entry.name);

      // 触发下载
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);

      // 释放 URL
      URL.revokeObjectURL(url);

      console.log("文件下载成功:", entry.name);
    } catch (error) {
      console.error("下载文件失败:", error);
      alert(`下载失败: ${error.message}`);
    }
  };

  /**
   * 重置状态
   */
  const resetState = () => {
    isExtracting.value = false;
    extractError.value = null;
    archiveEntries.value = [];
    isExtracted.value = false;
    currentPreviewFile.value = null;
    isPreviewing.value = false;
    extractProgress.value = 0;
    currentStage.value = "";

    // 清理缓存数据
    cachedFileName.value = "";
    cachedFileUrl.value = "";

    console.log("压缩文件预览状态已重置");
  };

  // ===== 工具函数 =====

  /**
   * 创建文本预览HTML页面
   * @param {string} fileName - 文件名
   * @param {string} content - 文本内容
   * @returns {string} HTML内容
   */
  const createTextPreviewHtml = (fileName, content) => {
    return `<!DOCTYPE html>
<html><head><meta charset="UTF-8"><title>${fileName}</title>
<style>body{font-family:monospace;padding:20px;white-space:pre-wrap;}</style>
</head><body>${content.replace(/</g, "&lt;").replace(/>/g, "&gt;")}</body></html>`;
  };

  // ===== 生命周期 =====

  onUnmounted(async () => {
    // 清理资源
    resetState();

    // 清理zip.js Worker资源
    try {
      await cleanupZipJS();
    } catch (error) {
      console.warn("清理zip.js资源时出错:", error);
    }
  });

  // ===== 返回接口 =====

  return {
    // 状态
    isExtracting,
    extractError,
    archiveEntries,
    isExtracted,
    currentPreviewFile,
    isPreviewing,
    extractProgress,
    currentStage,

    // 计算属性
    totalSize,

    // 方法
    extractArchive,
    previewFile,
    downloadFile,
    resetState,

    // 服务实例（用于高级用法）
    archiveService,
  };
}
