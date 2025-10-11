/**
 * 文件类型检测器
 * 基于预览设置的可配置文件类型判断系统
 */

import { FILE_TYPES, FILE_TYPE_NAMES } from "../constants/index.js";
import previewSettingsCache from "../cache/PreviewSettingsCache.js";

/**
 * 从文件名提取扩展名
 * @param {string} filename - 文件名
 * @returns {string} 扩展名（小写，不含点）
 */
export function getFileExtension(filename) {
  if (!filename || typeof filename !== "string") {
    return "";
  }

  const lastDotIndex = filename.lastIndexOf(".");
  if (lastDotIndex === -1 || lastDotIndex === filename.length - 1) {
    return "";
  }

  return filename.substring(lastDotIndex + 1).toLowerCase();
}

/**
 * 获取文件类型
 * 基于预览设置的文件类型判断，返回整数常量
 * @param {string} filename - 文件名
 * @param {D1Database} db - 数据库实例（可选）
 * @returns {Promise<number>} 文件类型常量 (0-6)
 */
export async function GetFileType(filename, db = null) {
  try {
    const extension = getFileExtension(filename);

    // 如果没有扩展名，返回UNKNOWN
    if (!extension) {
      return FILE_TYPES.UNKNOWN;
    }

    // 优先使用预览设置缓存
    if (previewSettingsCache.isLoaded()) {
      const previewType = await previewSettingsCache.getFileType(extension, db);

      switch (previewType) {
        case "text":
          return FILE_TYPES.TEXT;
        case "audio":
          return FILE_TYPES.AUDIO;
        case "video":
          return FILE_TYPES.VIDEO;
        case "image":
          return FILE_TYPES.IMAGE;
        case "office":
          return FILE_TYPES.OFFICE;
        case "document":
          return FILE_TYPES.DOCUMENT;
        default:
          // 如果预览设置中没有配置，回退到硬编码检测
          return getFileTypeByExtensionFallback(extension);
      }
    }

    // 如果缓存未加载，尝试加载缓存
    if (db) {
      try {
        await previewSettingsCache.refresh(db);
        const previewType = await previewSettingsCache.getFileType(extension, db);

        switch (previewType) {
          case "text":
            return FILE_TYPES.TEXT;
          case "audio":
            return FILE_TYPES.AUDIO;
          case "video":
            return FILE_TYPES.VIDEO;
          case "image":
            return FILE_TYPES.IMAGE;
          case "office":
            return FILE_TYPES.OFFICE;
          case "document":
            return FILE_TYPES.DOCUMENT;
          default:
            // 如果预览设置中没有配置，回退到硬编码检测
            console.log(`文件类型检测回退: ${filename} (扩展名: ${extension}) - 预览设置未配置，使用硬编码检测`);
            return getFileTypeByExtensionFallback(extension);
        }
      } catch (cacheError) {
        console.warn(`预览设置缓存加载失败，使用硬编码检测: ${cacheError.message}`);
      }
    }

    // 如果缓存未加载且没有数据库实例，使用硬编码回退
    console.log(`文件类型检测回退: ${filename} (扩展名: ${extension}) - 预览设置缓存未加载，使用硬编码检测`);
    return getFileTypeByExtensionFallback(extension);
  } catch (error) {
    console.error(`GetFileType 检测失败 "${filename}":`, error);
    return FILE_TYPES.UNKNOWN;
  }
}

/**
 * 硬编码的扩展名检测（回退机制）
 * @param {string} extension - 文件扩展名
 * @returns {number} 文件类型常量
 */
function getFileTypeByExtensionFallback(extension) {
  if (!extension) return FILE_TYPES.UNKNOWN;

  const ext = extension.toLowerCase();

  // 文本类型（与预览设置默认值保持一致）
  const textTypes = [
    "txt",
    "htm",
    "html",
    "xml",
    "java",
    "properties",
    "sql",
    "js",
    "md",
    "json",
    "conf",
    "ini",
    "vue",
    "php",
    "py",
    "bat",
    "yml",
    "yaml",
    "go",
    "sh",
    "c",
    "cpp",
    "h",
    "hpp",
    "tsx",
    "vtt",
    "srt",
    "ass",
    "rs",
    "lrc",
    "dockerfile",
    "makefile",
    "gitignore",
    "license",
    "readme",
  ];
  if (textTypes.includes(ext)) {
    return FILE_TYPES.TEXT;
  }

  // 音频类型（与预览设置默认值保持一致）
  const audioTypes = ["mp3", "flac", "ogg", "m4a", "wav", "opus", "wma"];
  if (audioTypes.includes(ext)) {
    return FILE_TYPES.AUDIO;
  }

  // 视频类型（与预览设置默认值保持一致，但排除htm/html）
  const videoTypes = ["mp4", "mkv", "avi", "mov", "rmvb", "webm", "flv", "m3u8"];
  if (videoTypes.includes(ext)) {
    return FILE_TYPES.VIDEO;
  }

  // 图片类型（与预览设置默认值保持一致）
  const imageTypes = ["jpg", "tiff", "jpeg", "png", "gif", "bmp", "svg", "ico", "swf", "webp", "avif"];
  if (imageTypes.includes(ext)) {
    return FILE_TYPES.IMAGE;
  }

  // Office文档类型（需要在线转换预览）
  const officeTypes = ["doc", "docx", "xls", "xlsx", "ppt", "pptx", "rtf"];
  if (officeTypes.includes(ext)) {
    return FILE_TYPES.OFFICE;
  }

  // 文档类型（可直接预览）
  const documentTypes = ["pdf"];
  if (documentTypes.includes(ext)) {
    return FILE_TYPES.DOCUMENT;
  }

  return FILE_TYPES.UNKNOWN;
}

/**
 * 获取文件类型名称
 * @param {string} filename - 文件名
 * @param {D1Database} db - 数据库实例（可选）
 * @returns {Promise<string>} 文件类型名称
 */
export async function getFileTypeName(filename, db = null) {
  const typeCode = await GetFileType(filename, db);
  return FILE_TYPE_NAMES[typeCode] || "unknown";
}
