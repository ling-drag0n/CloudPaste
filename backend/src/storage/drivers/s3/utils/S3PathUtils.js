/**
 * S3路径处理工具
 * 提供S3存储驱动专用的路径规范化功能
 */

import path from "path";

/**
 * 规范化S3子路径
 * @param {string} subPath - 子路径
 * @param {boolean} asDirectory - 是否作为目录处理
 * @returns {string} 规范化的S3子路径
 */
export function normalizeS3SubPath(subPath, asDirectory = false) {
  // 规范化S3子路径，移除开头的斜杠
  let s3SubPath = subPath.startsWith("/") ? subPath.substring(1) : subPath;

  // 如果路径为空，设置为根路径
  if (!s3SubPath) {
    s3SubPath = "";
  }

  // 规范化S3子路径，移除多余的斜杠
  s3SubPath = s3SubPath.replace(/\/+/g, "/");

  // 如果作为目录处理，确保路径以斜杠结尾
  if (asDirectory && s3SubPath !== "" && !s3SubPath.endsWith("/")) {
    s3SubPath += "/";
  }

  // 注意：root_prefix在调用时单独处理，避免重复添加
  // 在getS3DirectoryListing中会将s3SubPath与root_prefix组合

  return s3SubPath;
}

/**
 * 智能检查路径是否已经是完整的文件路径
 * @param {string} s3SubPath - S3子路径
 * @param {string} originalFileName - 原始文件名
 * @returns {boolean} 是否为完整文件路径
 */
export function isCompleteFilePath(s3SubPath, originalFileName) {
  if (!s3SubPath || !originalFileName) return false;

  // Node.js path 模块解析路径
  const pathInfo = path.parse(s3SubPath);
  const originalInfo = path.parse(originalFileName);

  // 检查是否有文件扩展名（区分文件和目录）
  if (!pathInfo.ext) {
    // 无扩展名情况：检查是否为原始文件名或带随机后缀的版本
    return pathInfo.base === originalFileName || pathInfo.base.startsWith(originalFileName + "-");
  }

  // 有扩展名情况：检查扩展名匹配 + 文件名模式
  if (pathInfo.ext === originalInfo.ext) {
    // 检查文件名是否匹配或者是带随机后缀的版本（如 black-abc123）
    return pathInfo.name === originalInfo.name || pathInfo.name.startsWith(originalInfo.name + "-");
  }

  return false;
}

/**
 * 检查S3子路径是否为挂载点根目录（空字符串）
 * @param {string} s3SubPath - S3子路径
 * @returns {boolean} 是否为挂载点根目录
 */
export function isMountRootPath(s3SubPath) {
  return !s3SubPath || s3SubPath.trim() === "";
}
