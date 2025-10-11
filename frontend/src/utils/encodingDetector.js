/**
 * 编码检测工具
 * 基于 chardet 库实现文件编码自动检测
 */

import chardet from "chardet";

/**
 * 支持的编码格式列表
 */
export const SUPPORTED_ENCODINGS = [
  // Unicode编码（现代主流）
  { value: "utf-8", label: "UTF-8", description: "通用Unicode编码" },
  { value: "utf-16le", label: "UTF-16 LE", description: "Unicode 16位小端" },
  { value: "utf-16be", label: "UTF-16 BE", description: "Unicode 16位大端" },

  // 中文编码
  { value: "gb18030", label: "GB18030", description: "中文国家标准(最完整)" },
  { value: "gbk", label: "GBK", description: "中文简体扩展" },
  { value: "gb2312", label: "GB2312", description: "中文简体基础" },
  { value: "big5", label: "Big5", description: "中文繁体" },

  // 日韩编码
  { value: "shift_jis", label: "Shift_JIS", description: "日文" },
  { value: "euc-jp", label: "EUC-JP", description: "日文扩展" },
  { value: "euc-kr", label: "EUC-KR", description: "韩文" },

  // 西欧编码
  { value: "iso-8859-1", label: "ISO-8859-1", description: "西欧语言" },
  { value: "iso-8859-15", label: "ISO-8859-15", description: "西欧语言(含欧元符号)" },
  { value: "windows-1252", label: "Windows-1252", description: "西欧Windows" },

  // 中欧编码
  { value: "iso-8859-2", label: "ISO-8859-2", description: "中欧语言" },
  { value: "windows-1250", label: "Windows-1250", description: "中欧Windows(波兰/捷克/匈牙利)" },

  // 西里尔字母编码
  { value: "iso-8859-5", label: "ISO-8859-5", description: "西里尔字母(俄语/保加利亚语)" },
  { value: "windows-1251", label: "Windows-1251", description: "西里尔字母Windows" },
  { value: "koi8-r", label: "KOI8-R", description: "俄文Unix/Linux" },

  // 中东编码
  { value: "iso-8859-6", label: "ISO-8859-6", description: "阿拉伯文" },
  { value: "windows-1256", label: "Windows-1256", description: "阿拉伯文Windows" },

  // 其他编码
  { value: "iso-8859-7", label: "ISO-8859-7", description: "希腊文" },
  { value: "iso-8859-8", label: "ISO-8859-8", description: "希伯来文" },
  { value: "ascii", label: "ASCII", description: "基本ASCII" },
];

/**
 * 从URL获取文件的二进制数据并检测编码
 * @param {string} url - 文件URL
 * @param {Object} options - 选项
 * @param {number} options.sampleSize - 采样大小，默认4096字节
 * @param {number} options.timeout - 超时时间，默认10秒
 * @returns {Promise<Object>} 检测结果
 */
export async function detectEncodingFromUrl(url, options = {}) {
  const { sampleSize = 4096, timeout = 10000 } = options;

  try {
    // 创建超时控制器
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), timeout);

    // 下载完整文件进行编码检测
    const response = await fetch(url, {
      method: "GET",
      signal: controller.signal,
    });

    clearTimeout(timeoutId);

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}: ${response.statusText}`);
    }

    // 获取二进制数据
    const arrayBuffer = await response.arrayBuffer();
    const fullUint8Array = new Uint8Array(arrayBuffer);

    // 只使用前sampleSize字节进行编码检测
    const uint8Array = fullUint8Array.length > sampleSize ? fullUint8Array.slice(0, sampleSize) : fullUint8Array;

    // 使用chardet检测编码
    const detectionResults = chardet.analyse(uint8Array);

    // 获取最可能的编码
    const bestMatch = detectionResults[0];

    return {
      success: true,
      encoding: bestMatch?.name?.toLowerCase() || "utf-8",
      confidence: bestMatch?.confidence || 0,
      allResults: detectionResults.map((result) => ({
        encoding: result.name?.toLowerCase(),
        confidence: result.confidence,
        language: result.lang,
      })),
      sampleSize: uint8Array.length,
      fullFileSize: fullUint8Array.length, 
      rawBuffer: fullUint8Array,
      error: null,
    };
  } catch (error) {
    console.error("编码检测失败:", error);

    return {
      success: false,
      encoding: "utf-8", // 默认编码
      confidence: 0,
      allResults: [],
      sampleSize: 0,
      fullFileSize: 0,
      rawBuffer: null,
      error: error.message,
    };
  }
}

/**
 * 智能编码检测
 * 结合文件名、内容特征等进行更准确的编码检测
 * @param {string} url - 文件URL
 * @param {string} filename - 文件名
 * @param {Object} options - 选项
 * @returns {Promise<Object>} 检测结果
 */
export async function smartDetectEncoding(url, filename = "", options = {}) {
  // 基础编码检测
  const baseResult = await detectEncodingFromUrl(url, options);

  if (!baseResult.success) {
    return baseResult;
  }

  // 根据文件名推测可能的编码
  const filenameHints = getEncodingHintsFromFilename(filename);

  // 如果检测置信度较低，尝试使用文件名提示
  if (baseResult.confidence < 70 && filenameHints.length > 0) {
    // 查找文件名提示中是否有匹配的编码
    const hintMatch = baseResult.allResults.find((result) => filenameHints.includes(result.encoding));

    if (hintMatch) {
      return {
        ...baseResult,
        encoding: hintMatch.encoding,
        confidence: Math.max(hintMatch.confidence, 60), // 提升置信度
        detectionMethod: "filename_hint",
      };
    }
  }

  return {
    ...baseResult,
    detectionMethod: "content_analysis",
  };
}

/**
 * 根据文件名获取编码提示
 * @param {string} filename - 文件名
 * @returns {string[]} 可能的编码列表
 */
function getEncodingHintsFromFilename(filename) {
  const hints = [];
  const lowerName = filename.toLowerCase();

  // 中文文件名通常使用中文编码
  if (/[\u4e00-\u9fff]/.test(filename)) {
    hints.push("gbk", "gb2312", "big5", "utf-8");
  }

  // 日文文件名
  if (/[\u3040-\u309f\u30a0-\u30ff]/.test(filename)) {
    hints.push("shift_jis", "euc-jp", "utf-8");
  }

  // 韩文文件名
  if (/[\uac00-\ud7af]/.test(filename)) {
    hints.push("euc-kr", "utf-8");
  }

  // 特定文件扩展名的编码倾向
  if (lowerName.endsWith(".txt") || lowerName.endsWith(".log")) {
    hints.push("utf-8", "gbk", "windows-1252");
  }

  return hints;
}

/**
 * 验证编码是否受支持
 * @param {string} encoding - 编码名称
 * @returns {boolean} 是否支持
 */
export function isEncodingSupported(encoding) {
  if (!encoding) return false;

  const normalizedEncoding = encoding.toLowerCase().replace(/[-_]/g, "");

  return SUPPORTED_ENCODINGS.some((supported) => {
    const normalizedSupported = supported.value.toLowerCase().replace(/[-_]/g, "");
    return normalizedSupported === normalizedEncoding;
  });
}

/**
 * 获取编码的显示信息
 * @param {string} encoding - 编码名称
 * @returns {Object|null} 编码信息
 */
export function getEncodingInfo(encoding) {
  if (!encoding) return null;

  const normalizedEncoding = encoding.toLowerCase();

  return SUPPORTED_ENCODINGS.find((supported) => supported.value.toLowerCase() === normalizedEncoding) || null;
}

/**
 * 标准化编码名称
 * @param {string} encoding - 原始编码名称
 * @returns {string} 标准化后的编码名称
 */
export function normalizeEncoding(encoding) {
  if (!encoding) return "utf-8";

  const normalized = encoding.toLowerCase();

  // 常见编码名称映射
  const mappings = {
    // Unicode别名
    utf8: "utf-8",
    utf16: "utf-16le",
    unicode: "utf-16le",

    // 中文编码别名
    cp936: "gbk",
    ms936: "gbk",
    cp950: "big5",
    // 注意：gb18030不映射到gbk，它们是不同的编码

    // 日韩编码别名
    sjis: "shift_jis",
    cp932: "shift_jis",

    // 西欧编码别名
    latin1: "iso-8859-1",
    latin9: "iso-8859-15",

    // Windows编码别名
    cp1250: "windows-1250",
    cp1251: "windows-1251",
    cp1252: "windows-1252",
    cp1256: "windows-1256",

    // ISO编码别名
    cyrillic: "iso-8859-5",
    arabic: "iso-8859-6",
    greek: "iso-8859-7",
    hebrew: "iso-8859-8",
  };

  return mappings[normalized] || normalized;
}
