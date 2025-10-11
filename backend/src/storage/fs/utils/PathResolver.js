/**
 * 路径处理工具
 * 提供基础的路径规范化功能
 */

/**
 * 规范化路径格式
 * @param {string} path - 输入路径
 * @param {boolean} isDirectory - 是否为目录路径
 * @returns {string} 规范化的路径
 */
export function normalizePath(path, isDirectory = false) {
  // 确保路径以斜杠开始
  path = path.startsWith("/") ? path : "/" + path;
  // 如果是目录，确保路径以斜杠结束
  if (isDirectory) {
    path = path.endsWith("/") ? path : path + "/";
  }
  return path;
}
