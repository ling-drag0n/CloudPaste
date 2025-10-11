/**
 * 文本编码处理工具
 * 提供文本解码、编码转换等功能
 */

import { SUPPORTED_ENCODINGS, normalizeEncoding } from "./encodingDetector.js";

/**
 * 使用指定编码解码二进制数据为文本
 * @param {ArrayBuffer|Uint8Array} buffer - 二进制数据
 * @param {string} encoding - 编码格式
 * @returns {Promise<string>} 解码后的文本
 */
export async function decodeText(buffer, encoding = "utf-8") {
  try {
    // 确保是Uint8Array
    const uint8Array = buffer instanceof ArrayBuffer ? new Uint8Array(buffer) : buffer;

    // 标准化编码名称
    const normalizedEncoding = normalizeEncoding(encoding);

    // 使用TextDecoder进行解码
    const decoder = new TextDecoder(normalizedEncoding, {
      fatal: false, // 不抛出错误，用替换字符
      ignoreBOM: false, // 保留BOM
    });

    const text = decoder.decode(uint8Array);

    // 检查解码结果是否包含过多替换字符
    const replacementCharCount = (text.match(/\uFFFD/g) || []).length;
    const replacementRatio = replacementCharCount / text.length;

    // 如果替换字符比例过高，可能编码不正确
    if (replacementRatio > 0.1) {
      console.warn(`编码 ${encoding} 解码质量较差，替换字符比例: ${(replacementRatio * 100).toFixed(2)}%`);
    }

    return {
      success: true,
      text,
      encoding: normalizedEncoding,
      replacementCharCount,
      replacementRatio,
      error: null,
    };
  } catch (error) {
    console.error(`使用编码 ${encoding} 解码失败:`, error);

    // 尝试使用UTF-8作为后备
    if (encoding !== "utf-8") {
      console.log("尝试使用UTF-8后备编码...");
      return await decodeText(buffer, "utf-8");
    }

    return {
      success: false,
      text: "",
      encoding,
      replacementCharCount: 0,
      replacementRatio: 0,
      error: error.message,
    };
  }
}

/**
 * 从URL获取文件并使用指定编码解码
 * @param {string} url - 文件URL
 * @param {string} encoding - 编码格式
 * @param {Object} options - 选项
 * @returns {Promise<Object>} 解码结果
 */
export async function fetchAndDecodeText(url, encoding = "utf-8", options = {}) {
  const { timeout = 30000, maxSize = 10 * 1024 * 1024 } = options; // 默认最大10MB

  try {
    // 创建超时控制器
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), timeout);

    // 获取文件
    const response = await fetch(url, {
      method: "GET",
      signal: controller.signal,
    });

    clearTimeout(timeoutId);

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}: ${response.statusText}`);
    }

    // 检查文件大小
    const contentLength = response.headers.get("content-length");
    if (contentLength && parseInt(contentLength) > maxSize) {
      throw new Error(`文件过大: ${Math.round(parseInt(contentLength) / 1024 / 1024)}MB，超过限制 ${Math.round(maxSize / 1024 / 1024)}MB`);
    }

    // 获取二进制数据
    const arrayBuffer = await response.arrayBuffer();

    // 检查实际大小
    if (arrayBuffer.byteLength > maxSize) {
      throw new Error(`文件过大: ${Math.round(arrayBuffer.byteLength / 1024 / 1024)}MB，超过限制 ${Math.round(maxSize / 1024 / 1024)}MB`);
    }

    // 解码文本
    const decodeResult = await decodeText(arrayBuffer, encoding);

    return {
      ...decodeResult,
      fileSize: arrayBuffer.byteLength,
      rawBuffer: arrayBuffer,
      url,
    };
  } catch (error) {
    console.error("获取和解码文件失败:", error);

    return {
      success: false,
      text: "",
      encoding,
      replacementCharCount: 0,
      replacementRatio: 0,
      fileSize: 0,
      url,
      error: error.message,
    };
  }
}

/**
 * 检测文本是否可能是二进制文件
 * @param {string} text - 文本内容
 * @param {number} threshold - 二进制字符阈值，默认0.3
 * @returns {boolean} 是否可能是二进制文件
 */
export function isBinaryContent(text, threshold = 0.3) {
  if (!text || text.length === 0) return false;

  let binaryCharCount = 0;
  const sampleSize = Math.min(text.length, 1000); // 只检查前1000个字符

  for (let i = 0; i < sampleSize; i++) {
    const charCode = text.charCodeAt(i);

    // 检查是否为控制字符（除了常见的换行、制表符等）
    if (charCode < 32 && charCode !== 9 && charCode !== 10 && charCode !== 13) {
      binaryCharCount++;
    }

    // 检查是否为替换字符
    if (charCode === 0xfffd) {
      binaryCharCount++;
    }
  }

  const binaryRatio = binaryCharCount / sampleSize;
  return binaryRatio > threshold;
}

/**
 * 清理文本内容，移除或替换问题字符
 * @param {string} text - 原始文本
 * @param {Object} options - 清理选项
 * @returns {string} 清理后的文本
 */
export function cleanText(text, options = {}) {
  const { removeNullBytes = true, normalizeLineEndings = true, removeReplacementChars = false, maxLength = null } = options;

  let cleanedText = text;

  // 移除空字节
  if (removeNullBytes) {
    cleanedText = cleanedText.replace(/\0/g, "");
  }

  // 标准化行结束符
  if (normalizeLineEndings) {
    cleanedText = cleanedText.replace(/\r\n/g, "\n").replace(/\r/g, "\n");
  }

  // 移除替换字符
  if (removeReplacementChars) {
    cleanedText = cleanedText.replace(/\uFFFD/g, "");
  }

  // 限制长度
  if (maxLength && cleanedText.length > maxLength) {
    cleanedText = cleanedText.substring(0, maxLength) + "\n\n... (内容已截断)";
  }

  return cleanedText;
}

/**
 * 获取文本统计信息
 * @param {string} text - 文本内容
 * @returns {Object} 统计信息
 */
export function getTextStats(text) {
  if (!text) {
    return {
      length: 0,
      lines: 0,
      words: 0,
      characters: 0,
      bytes: 0,
      encoding: null,
    };
  }

  const lines = text.split("\n").length;
  const words = text.trim() ? text.trim().split(/\s+/).length : 0;
  const characters = text.length;
  const bytes = new TextEncoder().encode(text).length;

  return {
    length: characters,
    lines,
    words,
    characters,
    bytes,
    encoding: "utf-8", // 内存中的字符串总是UTF-8
  };
}

/**
 * 检测文本的主要语言
 * @param {string} text - 文本内容
 * @returns {string} 语言代码
 */
export function detectTextLanguage(text) {
  if (!text || text.length < 10) return "unknown";

  const sample = text.substring(0, 1000); // 只检查前1000个字符

  // 中文检测
  if (/[\u4e00-\u9fff]/.test(sample)) {
    return "zh";
  }

  // 日文检测
  if (/[\u3040-\u309f\u30a0-\u30ff]/.test(sample)) {
    return "ja";
  }

  // 韩文检测
  if (/[\uac00-\ud7af]/.test(sample)) {
    return "ko";
  }

  // 俄文检测
  if (/[\u0400-\u04ff]/.test(sample)) {
    return "ru";
  }

  // 阿拉伯文检测
  if (/[\u0600-\u06ff]/.test(sample)) {
    return "ar";
  }

  // 默认为英文或其他拉丁字母语言
  return "en";
}

/**
 * 导出支持的编码列表
 */
export { SUPPORTED_ENCODINGS };

/**
 * 预览模式常量
 */
export const PREVIEW_MODES = {
  TEXT: "text", // 纯文本预览
  CODE: "code", // 代码语法高亮
  MARKDOWN: "markdown", // Markdown渲染
  HTML: "html", // HTML渲染
  EDIT: "edit", // 编辑模式
};

/**
 * 根据文件扩展名推测预览模式
 * @param {string} filename - 文件名
 * @returns {string} 预览模式
 */
export function getPreviewModeFromFilename(filename) {
  if (!filename) return PREVIEW_MODES.TEXT;

  const ext = filename.split(".").pop()?.toLowerCase();

  // Markdown文件
  if (["md", "markdown", "mdown", "mkd"].includes(ext)) {
    return PREVIEW_MODES.MARKDOWN;
  }

  // HTML文件
  if (["html", "htm", "xhtml"].includes(ext)) {
    return PREVIEW_MODES.HTML;
  }

  // 代码文件
  const codeExtensions = [
    "js",
    "ts",
    "jsx",
    "tsx",
    "vue",
    "py",
    "java",
    "cpp",
    "c",
    "h",
    "cs",
    "php",
    "rb",
    "go",
    "rs",
    "swift",
    "kt",
    "scala",
    "sh",
    "bash",
    "ps1",
    "sql",
    "json",
    "xml",
    "yaml",
    "yml",
    "toml",
    "ini",
    "cfg",
    "conf",
  ];

  if (codeExtensions.includes(ext)) {
    return PREVIEW_MODES.CODE;
  }

  // 默认为纯文本
  return PREVIEW_MODES.TEXT;
}
