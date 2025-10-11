/**
 * 文本获取和编码处理 Composable
 * 提供文本文件的获取、编码检测和解码功能
 */

import { ref, computed, onUnmounted } from "vue";
import { useEncodingDetection } from "./useEncodingDetection.js";
import { SUPPORTED_ENCODINGS, decodeText, fetchAndDecodeText, isBinaryContent, cleanText, getTextStats } from "@/utils/textUtils.js";

export function useFetchText() {
  //异步操作取消机制
  let currentAbortController = null;

  // 状态
  const loading = ref(false);
  const error = ref(null);
  const textContent = ref("");
  const rawBuffer = ref(null);
  const fileInfo = ref(null);
  // 使用 computed 缓存文本统计，避免重复计算
  const textStats = computed(() => {
    return textContent.value ? getTextStats(textContent.value) : null;
  });

  // 使用编码检测
  const { selectedEncoding, detectionResult, hasDetectionResult, selectEncoding, resetDetection, detectEncoding, availableEncodings } = useEncodingDetection();

  // 计算属性
  const hasContent = computed(() => !!textContent.value);
  const isTextValid = computed(() => hasContent.value && !error.value);
  const isBinary = computed(() => {
    return textContent.value ? isBinaryContent(textContent.value) : false;
  });

  /**
   * 取消当前的异步操作
   */
  const cancelCurrentOperation = () => {
    if (currentAbortController) {
      currentAbortController.abort();
      currentAbortController = null;
    }
  };

  /**
   * 清理原始缓冲区以释放内存
   */
  const clearRawBuffer = () => {
    if (rawBuffer.value) {
      rawBuffer.value = null;
    }
  };

  /**
   * 获取文件并自动检测编码
   * @param {string} url - 文件URL
   * @param {Object} fileData - 文件信息
   * @param {Object} options - 选项
   * @returns {Promise<Object>} 获取结果
   */
  const fetchText = async (url, fileData = null, options = {}) => {
    if (!url) {
      throw new Error("URL不能为空");
    }

    // 内存管理：检查文件大小限制
    const MAX_FILE_SIZE = 50 * 1024 * 1024; // 50MB
    if (fileData?.size && fileData.size > MAX_FILE_SIZE) {
      throw new Error(`文件过大 (${(fileData.size / 1024 / 1024).toFixed(1)}MB)，最大支持 ${MAX_FILE_SIZE / 1024 / 1024}MB`);
    }

    //取消之前的操作（如果有的话）
    if (currentAbortController) {
      currentAbortController.abort();
    }

    //创建新的 AbortController
    currentAbortController = new AbortController();
    const signal = currentAbortController.signal;

    try {
      loading.value = true;
      error.value = null;

      // 保存文件信息
      fileInfo.value = fileData;

      // 首先检测编码（传递 signal 用于取消）
      const filename = fileData?.name || fileData?.filename || "";
      const detectionResult = await detectEncoding(url, filename, {
        ...options,
        signal, // 传递取消信号
      });

      // 检查是否已被取消
      if (signal.aborted) {
        throw new Error("操作已取消");
      }

      if (detectionResult.success) {
        console.log("编码自动检测成功:", {
          文件名: filename,
          检测编码: detectionResult.encoding,
          可信度: `${detectionResult.confidence}%`,
          检测方法: detectionResult.method || "智能检测",
          所有候选: detectionResult.allResults?.slice(0, 3).map((r) => `${r.encoding}(${r.confidence}%)`) || [],
          文件大小: detectionResult.fullFileSize ? `${(detectionResult.fullFileSize / 1024).toFixed(1)}KB` : "未知",
        });
      } else {
        console.warn(" 编码检测失败，使用默认编码 utf-8:", detectionResult.error);
      }

      // 如果编码检测已经返回了完整的原始数据，直接使用
      let fetchResult;
      if (detectionResult.success && detectionResult.rawBuffer) {
        // 使用编码检测时已下载的数据，避免重复下载
        const encoding = detectionResult.encoding || "utf-8";
        const decodeResult = await decodeText(detectionResult.rawBuffer, encoding);

        fetchResult = {
          ...decodeResult,
          fileSize: detectionResult.fullFileSize || detectionResult.rawBuffer.byteLength,
          rawBuffer: detectionResult.rawBuffer,
          url,
        };
      } else {
        // 降级：使用原有的下载方式
        const encoding = detectionResult.encoding || "utf-8";
        fetchResult = await fetchAndDecodeText(url, encoding, {
          ...options,
          signal, // 传递取消信号
        });
      }

      if (fetchResult.success) {
        textContent.value = cleanText(fetchResult.text, {
          removeNullBytes: true,
          normalizeLineEndings: true,
          maxLength: options.maxLength,
        });

        // 保存原始数据供编码切换使用
        rawBuffer.value = fetchResult.rawBuffer;

        // 内存管理：如果不需要重新编码功能，延迟清理 rawBuffer
        if (!options.keepRawBuffer) {
          setTimeout(clearRawBuffer, 2000); // 2秒后清理，确保其他操作完成
        }

        // textStats 通过 computed 自动计算，无需手动赋值

        return {
          success: true,
          text: textContent.value,
          encoding: fetchResult.encoding,
          stats: textStats.value,
          fileSize: fetchResult.fileSize,
        };
      } else {
        throw new Error(fetchResult.error || "文本获取失败");
      }
    } catch (err) {
      // 如果是取消操作，不记录为错误
      if (err.name === "AbortError" || err.message === "操作已取消") {
        console.log("文本获取操作已取消");
        return {
          success: false,
          text: "",
          encoding: "utf-8",
          stats: null,
          fileSize: 0,
          error: "操作已取消",
          cancelled: true,
        };
      }

      console.error("获取文本失败:", err);
      error.value = err.message;

      return {
        success: false,
        text: "",
        encoding: "utf-8",
        stats: null,
        fileSize: 0,
        error: err.message,
      };
    } finally {
      loading.value = false;
      //条件清理 AbortController
      currentAbortController = null;
    }
  };

  /**
   * 使用指定编码重新解码文本
   * @param {string} encoding - 编码格式
   * @returns {Promise<Object>} 解码结果
   */
  const reDecodeWithEncoding = async (encoding) => {
    if (!rawBuffer.value) {
      // 如果没有原始数据，重新获取
      if (fileInfo.value?.preview_url) {
        return await fetchText(fileInfo.value.preview_url, fileInfo.value);
      } else {
        throw new Error("没有可用的文件数据");
      }
    }

    try {
      loading.value = true;
      error.value = null;

      // 选择新编码
      selectEncoding(encoding);

      // 重新解码
      const decodeResult = await decodeText(rawBuffer.value, encoding);

      if (decodeResult.success) {
        textContent.value = cleanText(decodeResult.text, {
          removeNullBytes: true,
          normalizeLineEndings: true,
        });

        return {
          success: true,
          text: textContent.value,
          encoding: decodeResult.encoding,
          stats: textStats.value,
        };
      } else {
        throw new Error(decodeResult.error || "重新解码失败");
      }
    } catch (err) {
      console.error("重新解码失败:", err);
      error.value = err.message;

      return {
        success: false,
        text: textContent.value, // 保持原有内容
        encoding: selectedEncoding.value,
        stats: textStats.value,
        error: err.message,
      };
    } finally {
      loading.value = false;
    }
  };

  /**
   * 检测编码并解码文本
   * @param {string} filename - 文件名（用于编码提示）
   */
  const detectAndDecodeText = async (filename) => {
    if (!rawBuffer.value) {
      throw new Error("没有原始数据");
    }

    try {
      // 使用简单的编码检测（因为我们已经有了二进制数据）
      const detection = { encoding: "utf-8", confidence: 1.0 };
      detectionResult.value = detection;

      // 使用检测到的编码解码
      const encoding = detection.encoding || "utf-8";
      selectEncoding(encoding);
      await decodeWithEncoding(encoding);
    } catch (err) {
      console.error("编码检测失败:", err, "文件:", filename);
      // 降级到UTF-8
      selectEncoding("utf-8");
      await decodeWithEncoding("utf-8");
    }
  };

  /**
   * 使用指定编码解码
   * @param {string} encoding - 编码格式
   */
  const decodeWithEncoding = async (encoding) => {
    if (!rawBuffer.value) {
      throw new Error("没有原始数据");
    }

    const decodeResult = await decodeText(rawBuffer.value, encoding);

    if (decodeResult.success) {
      textContent.value = cleanText(decodeResult.text);
    } else {
      throw new Error(decodeResult.error || "解码失败");
    }
  };

  /**
   * 重置状态
   */
  const reset = () => {
    // 取消当前操作
    cancelCurrentOperation();

    // 清理所有状态
    loading.value = false;
    error.value = null;
    textContent.value = "";
    rawBuffer.value = null;
    fileInfo.value = null;
    resetDetection();
  };

  /**
   * 获取支持的编码列表
   */
  const getSupportedEncodings = () => {
    return SUPPORTED_ENCODINGS;
  };

  /**
   * 检查编码是否支持
   * @param {string} encoding - 编码格式
   */
  const isEncodingSupported = (encoding) => {
    return SUPPORTED_ENCODINGS.some((enc) => enc.value === encoding);
  };

  // 组件销毁时清理资源
  onUnmounted(() => {
    cancelCurrentOperation();
  });

  return {
    // 状态
    loading,
    error,
    textContent,
    rawBuffer,
    fileInfo,
    textStats,
    selectedEncoding,
    detectionResult,
    hasDetectionResult,

    // 计算属性
    hasContent,
    isTextValid,
    isBinary,
    availableEncodings,

    // 方法
    fetchText,
    reDecodeWithEncoding,
    reset,
    getSupportedEncodings,
    isEncodingSupported,
    cancelCurrentOperation,
    clearRawBuffer, // 内存管理

    // 内部方法（供高级用户使用）
    detectAndDecodeText,
    decodeWithEncoding,
  };
}
