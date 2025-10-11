/**
 * 文件处理工具
 * 基于mime-types标准库和可配置的预览设置
 */

import mime from "mime-types";

/**
 * 获取文件扩展名
 * @param {string} filename - 文件名
 * @returns {string} 扩展名
 */
export function getFileExtension(filename) {
  if (!filename) return "";
  return filename.split(".").pop().toLowerCase();
}

/**
 * 获取有效的MIME类型
 * @param {string} mimetype - 传入的MIME类型
 * @param {string} filename - 文件名
 * @returns {string} 有效的MIME类型
 */
export function getEffectiveMimeType(mimetype, filename) {
  if (mimetype && mimetype !== "application/octet-stream" && mimetype !== "unknown/unknown" && mimetype !== "" && mimetype !== "undefined") {
    console.log(`getEffectiveMimeType: 使用传入MIME类型 "${mimetype}" (文件: ${filename || "N/A"})`);
    return mimetype;
  }
  const detectedMime = mime.lookup(filename) || "application/octet-stream";
  console.log(`getEffectiveMimeType: 从文件名 "${filename}" 推断MIME类型 -> "${detectedMime}"`);
  return detectedMime;
}

/**
 * 获取响应的Content-Type
 * @param {string} filename - 文件名
 * @param {string} mimetype - MIME类型
 * @param {Object} options - 选项
 * @returns {Object} Content-Type和Content-Disposition
 */
export function getContentTypeAndDisposition(filename, mimetype, options = {}) {
  const { forceDownload = false } = options;
  const contentType = getEffectiveMimeType(mimetype, filename);

  if (forceDownload) {
    return {
      contentType,
      contentDisposition: `attachment; filename="${encodeURIComponent(filename)}"`,
    };
  }

  return {
    contentType,
    contentDisposition: `inline; filename="${encodeURIComponent(filename)}"`,
  };
}

/**
 * 从文件名推断MIME类型（兼容性函数）
 * @param {string} filename - 文件名
 * @returns {string} MIME类型
 */
export function getMimeTypeFromFilename(filename) {
  return getEffectiveMimeType(null, filename);
}

/**
 * 检查是否为Office文件（兼容性函数）
 * @param {string} mimeType - MIME类型
 * @param {string} filename - 文件名（忽略）
 * @returns {boolean}
 */
export function isOfficeFile(mimeType, filename = "") {
  const officeMimeTypes = [
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "application/vnd.openxmlformats-officedocument.presentationml.presentation",
    "application/vnd.ms-excel",
    "application/vnd.ms-powerpoint",
    "application/msword",
  ];
  const isOffice = officeMimeTypes.includes(mimeType);
  console.log(`isOfficeFile: "${mimeType}" (文件: ${filename || "N/A"}) -> ${isOffice ? "✅ Office文件" : "❌ 非Office文件"}`);
  return isOffice;
}

/**
 * 检查是否为文档文件（可直接预览）
 * @param {string} mimeType - MIME类型
 * @param {string} filename - 文件名（忽略）
 * @returns {boolean}
 */
export function isDocumentFile(mimeType, filename = "") {
  const documentMimeTypes = ["application/pdf"];
  const isDocument = documentMimeTypes.includes(mimeType);
  console.log(`isDocumentFile: "${mimeType}" (文件: ${filename || "N/A"}) -> ${isDocument ? "✅ 文档文件" : "❌ 非文档文件"}`);
  return isDocument;
}

// 导出标准库（便于直接使用）
export { mime };
