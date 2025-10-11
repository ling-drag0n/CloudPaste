/**
 * WebDAV响应头统一管理工具
 */

import { getWebDAVConfig } from "../auth/config/WebDAVConfig.js";

/**
 * 获取标准WebDAV响应头
 * 统一管理所有WebDAV相关的响应头设置
 * @param {Object} options - 头部选项
 * @param {boolean} options.includeCORS - 是否包含CORS头部（默认true）
 * @param {boolean} options.includeDAV - 是否包含DAV头部（默认true）
 * @param {boolean} options.includeAllow - 是否包含Allow头部（默认true）
 * @param {Object} options.customHeaders - 自定义头部
 * @returns {Object} 标准化的WebDAV响应头对象
 */
export function getStandardWebDAVHeaders(options = {}) {
  const { includeCORS = true, includeDAV = true, includeAllow = true, customHeaders = {} } = options;

  const config = getWebDAVConfig();
  const headers = {};

  // 基础WebDAV协议头部
  if (includeDAV) {
    headers.DAV = config.HEADERS.DAV;
    headers["MS-Author-Via"] = config.HEADERS["MS-Author-Via"];
  }

  // 允许的方法头部
  if (includeAllow) {
    headers.Allow = config.METHODS.join(", ");
    headers.Public = config.METHODS.join(", ");
  }

  // CORS头部
  if (includeCORS) {
    headers["Access-Control-Allow-Origin"] = config.HEADERS["Access-Control-Allow-Origin"];
    headers["Access-Control-Allow-Methods"] = config.HEADERS["Access-Control-Allow-Methods"];
    headers["Access-Control-Allow-Headers"] = config.HEADERS["Access-Control-Allow-Headers"];
    headers["Access-Control-Expose-Headers"] = config.HEADERS["Access-Control-Expose-Headers"];
    headers["Access-Control-Max-Age"] = config.HEADERS["Access-Control-Max-Age"];
  }

  // 合并自定义头部
  Object.assign(headers, customHeaders);

  return headers;
}

/**
 * 为Response对象添加标准WebDAV头部
 * @param {Response} response - 原始响应对象
 * @param {Object} options - 头部选项（同getStandardWebDAVHeaders）
 * @returns {Response} 添加了WebDAV头部的新响应对象
 */
export function addWebDAVHeaders(response, options = {}) {
  const webdavHeaders = getStandardWebDAVHeaders(options);
  const newResponse = new Response(response.body, response);

  // 只添加还没有的响应头，避免覆盖已有的头部
  for (const [key, value] of Object.entries(webdavHeaders)) {
    if (!newResponse.headers.has(key)) {
      newResponse.headers.set(key, value);
    }
  }

  return newResponse;
}

/**
 * 获取WebDAV错误响应头
 * 专门用于错误响应的头部设置
 * @param {string} contentType - 内容类型（默认text/plain）
 * @returns {Object} 错误响应头对象
 */
export function getWebDAVErrorHeaders(contentType = "text/plain") {
  return getStandardWebDAVHeaders({
    customHeaders: {
      "Content-Type": contentType,
      "Cache-Control": "no-cache, no-store, must-revalidate",
      Pragma: "no-cache",
      Expires: "0",
    },
  });
}

/**
 * 获取WebDAV Multi-Status响应头
 * 专门用于207 Multi-Status响应的头部设置
 * @returns {Object} Multi-Status响应头对象
 */
export function getWebDAVMultiStatusHeaders() {
  return getStandardWebDAVHeaders({
    customHeaders: {
      "Content-Type": "text/xml; charset=utf-8",
    },
  });
}

/**
 * 为Express响应设置WebDAV头部
 * 专门用于Express中间件的头部设置
 * @param {Object} res - Express响应对象
 * @param {Object} options - 头部选项
 */
export function setExpressWebDAVHeaders(res, options = {}) {
  const webdavHeaders = getStandardWebDAVHeaders(options);

  for (const [key, value] of Object.entries(webdavHeaders)) {
    res.setHeader(key, value);
  }
}
