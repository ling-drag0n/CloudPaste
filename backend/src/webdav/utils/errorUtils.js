/**
 * WebDAV错误处理工具函数
 * 提供统一的错误日志记录和响应创建功能
 */

import { getWebDAVErrorHeaders } from "./headerUtils.js";

/**
 * 生成唯一错误ID
 * @returns {string} 错误ID
 */
export function generateErrorId() {
  return Date.now().toString(36) + Math.random().toString(36).substr(2, 5);
}

/**
 * 转义XML特殊字符，确保生成有效的XML
 * @param {string} text - 需要转义的文本
 * @returns {string} 转义后的文本
 */
function escapeXmlChars(text) {
  if (typeof text !== "string") return "";
  return text.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;").replace(/'/g, "&apos;");
}

/**
 * 获取HTTP状态码对应的文本描述
 * @param {number} statusCode - HTTP状态码
 * @returns {string} 状态描述文本
 */
function getStatusText(statusCode) {
  const statusTexts = {
    400: "Bad Request",
    401: "Unauthorized",
    403: "Forbidden",
    404: "Not Found",
    405: "Method Not Allowed",
    409: "Conflict",
    412: "Precondition Failed",
    415: "Unsupported Media Type",
    423: "Locked",
    500: "Internal Server Error",
    501: "Not Implemented",
    507: "Insufficient Storage",
  };

  return statusTexts[statusCode] || "Unknown Status";
}

/**
 * 创建标准WebDAV XML错误响应
 * @param {string} message - 错误消息
 * @param {number} status - HTTP状态码
 * @returns {Response} 符合WebDAV标准的XML错误响应
 */
export function createStandardWebDAVErrorResponse(message, status) {
  // 转义错误消息中的XML特殊字符
  const escapedMessage = escapeXmlChars(message);
  const statusText = getStatusText(status);

  // 构建符合WebDAV标准的XML错误响应
  const xml = `<?xml version="1.0" encoding="UTF-8"?>
<D:error xmlns:D="DAV:">
  <D:status>HTTP/1.1 ${status} ${statusText}</D:status>
  <D:message>${escapedMessage}</D:message>
</D:error>`;

  const headers = getWebDAVErrorHeaders("application/xml; charset=utf-8");

  return new Response(xml, {
    status: status,
    headers: headers,
  });
}

/**
 * 记录WebDAV操作错误并创建统一的错误响应
 * @param {string} operation - 操作名称，如 "GET", "PUT" 等
 * @param {Error} error - 错误对象
 * @param {boolean} [includeDetails=false] - 是否在响应中包含错误详细信息（仅用于开发环境）
 * @param {boolean} [useXmlResponse=true] - 是否使用XML格式的错误响应
 * @returns {Response} 格式化的错误响应
 */
export function handleWebDAVError(operation, error, includeDetails = false, useXmlResponse = true) {
  // 生成唯一错误ID
  const errorId = generateErrorId();

  // 记录错误信息
  console.error(`WebDAV ${operation} 操作错误 [${errorId}]:`, error);

  if (error.status && typeof error.status === "number") {
    const statusCode = error.status;
    const message = error.message || `${operation}操作失败`;

    return useXmlResponse
      ? createStandardWebDAVErrorResponse(message, statusCode)
      : new Response(message, {
          status: statusCode,
          headers: getWebDAVErrorHeaders("text/plain"),
        });
  }

  // 特殊处理S3的404错误
  if (error.$metadata && error.$metadata.httpStatusCode === 404) {
    return useXmlResponse
      ? createStandardWebDAVErrorResponse("文件或目录不存在", 404)
      : new Response("文件或目录不存在", {
          status: 404,
          headers: getWebDAVErrorHeaders("text/plain"),
        });
  }

  // 创建安全的错误响应
  const errorMessage = includeDetails ? `内部服务器错误: ${error.message} (错误ID: ${errorId})` : `内部服务器错误 (错误ID: ${errorId})`;

  return useXmlResponse
    ? createStandardWebDAVErrorResponse(errorMessage, 500)
    : new Response(errorMessage, {
        status: 500,
        headers: getWebDAVErrorHeaders("text/plain"),
      });
}

/**
 * 创建WebDAV错误响应
 * @param {string} message - 错误消息
 * @param {number} status - HTTP状态码
 * @param {boolean} [useXmlResponse=true] - 是否使用XML格式的错误响应
 * @returns {Response} 错误响应
 */
export function createWebDAVErrorResponse(message, status, useXmlResponse = true) {
  return useXmlResponse
    ? createStandardWebDAVErrorResponse(message, status)
    : new Response(message, {
        status,
        headers: getWebDAVErrorHeaders("text/plain"),
      });
}
