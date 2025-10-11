/**
 * 文本预览公共逻辑 Composable
 */

import { ref } from "vue";
import { useFetchText } from "./useFetchText.js";
import { useCodeHighlight } from "./useCodeHighlight.js";

/**
 * 文本预览公共逻辑
 * @param {Object} options - 配置选项
 * @param {boolean} options.checkCancelled - 是否检查取消状态
 * @param {boolean} options.emitEncodingChange - 是否触发编码变化事件
 * @returns {Object} 文本预览相关的状态和方法
 */
export function useTextPreview(options = {}) {
  const {
    checkCancelled = false,
    emitEncodingChange = false,
  } = options;

  // 统一的状态管理
  const textContent = ref("");
  const detectedLanguage = ref("");
  const currentEncoding = ref("utf-8");
  const loading = ref(false);
  const error = ref(null);

  // 统一的 composable 使用
  const { fetchText, reDecodeWithEncoding } = useFetchText();
  const { detectLanguageFromFilename } = useCodeHighlight();

  /**
   * 统一的文本加载逻辑
   * @param {Object} fileData - 文件数据对象
   * @param {Function} emitFn - emit 函数
   * @returns {Promise<Object>} 加载结果
   */
  const loadTextContent = async (fileData, emitFn) => {
    if (!fileData?.preview_url) {
      console.warn("没有可用的预览URL");
      return { success: false, error: "没有可用的预览URL" };
    }

    try {
      loading.value = true;
      error.value = null;

      const result = await fetchText(fileData.preview_url, fileData);

      if (result.success) {
        textContent.value = result.text;
        currentEncoding.value = result.encoding || "utf-8";
        
        // 检测语言
        const filename = fileData.name || "";
        detectedLanguage.value = detectLanguageFromFilename(filename);

        console.log("文本加载成功:", {
          encoding: result.encoding,
          textLength: result.text.length,
          filename: filename,
        });

        emitFn?.("load", result);
        return { success: true, result };
      } else {
        // 支持可选的 cancelled 检查
        if (!result.cancelled || !checkCancelled) {
          emitFn?.("error", result.error);
        }
        return { success: false, error: result.error };
      }
    } catch (err) {
      console.error("加载文本内容失败:", err);
      error.value = err.message;
      emitFn?.("error", err.message);
      return { success: false, error: err.message };
    } finally {
      loading.value = false;
    }
  };

  /**
   * 统一的编码切换逻辑
   * @param {string} encoding - 新编码
   * @param {Function} emitFn - emit 函数
   * @returns {Promise<Object>} 切换结果
   */
  const handleEncodingChange = async (encoding, emitFn) => {
    try {
      loading.value = true;
      error.value = null;

      const result = await reDecodeWithEncoding(encoding);
      
      if (result.success) {
        textContent.value = result.text;
        currentEncoding.value = encoding;

        console.log("编码切换成功:", {
          encoding: encoding,
          textLength: result.text.length,
        });

        // 支持可选的编码变化事件
        if (emitEncodingChange) {
          emitFn?.("encoding-change", encoding);
        }
        
        return { success: true };
      } else {
        console.error("编码切换失败:", result.error);
        error.value = result.error;
        emitFn?.("error", result.error);
        return { success: false, error: result.error };
      }
    } catch (err) {
      console.error("编码切换失败:", err);
      error.value = err.message;
      emitFn?.("error", err.message);
      return { success: false, error: err.message };
    } finally {
      loading.value = false;
    }
  };

  /**
   * 重置状态
   */
  const reset = () => {
    textContent.value = "";
    detectedLanguage.value = "";
    currentEncoding.value = "utf-8";
    loading.value = false;
    error.value = null;
  };

  return {
    // 状态
    textContent,
    detectedLanguage,
    currentEncoding,
    loading,
    error,
    
    // 方法
    loadTextContent,
    handleEncodingChange,
    reset,
  };
}
