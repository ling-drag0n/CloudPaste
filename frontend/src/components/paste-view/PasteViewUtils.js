// PasteView 组件的公共工具函数
// 这个文件提供了所有PasteView相关组件可复用的工具函数
// 将这些功能提取到单独的文件中有助于避免代码重复，并使主组件保持简洁

// 导入统一的时间处理工具
import { formatDateTime, formatRelativeTime as formatRelativeTimeUtil, formatExpiry as formatExpiryUtil, isExpired } from "@/utils/timeUtils.js";

/**
 * 格式化日期 - 使用统一的时间处理工具
 * @param {string} dateString - UTC 时间字符串
 * @returns {string} 格式化后的本地时间字符串
 */
export const formatDate = (dateString) => {
  return formatDateTime(dateString);
};

/**
 * 格式化相对时间（如：3天后过期）
 * @param {string} dateString - UTC 时间字符串
 * @returns {string} 相对时间描述
 */
export const formatRelativeTime = (dateString) => {
  if (!dateString) return "";

  const relativeTime = formatRelativeTimeUtil(dateString);

  // 为过期场景添加特殊处理
  if (relativeTime.includes("前")) {
    return "已过期";
  } else if (relativeTime === "即将") {
    return "即将过期";
  } else if (relativeTime.includes("后")) {
    return relativeTime.replace("后", "后过期");
  }

  return relativeTime;
};

/**
 * 格式化过期日期
 * @param {string} expiryDate - UTC 时间字符串
 * @returns {string} 格式化后的过期时间描述
 */
export const formatExpiry = (expiryDate) => {
  return formatExpiryUtil(expiryDate);
};

/**
 * 获取输入框样式
 * 根据当前主题模式返回合适的CSS类名
 * @param {boolean} darkMode - 是否为暗黑模式
 * @returns {string} CSS类名
 */
export const getInputClasses = (darkMode) => {
  return darkMode
    ? "bg-gray-800 border-gray-700 text-gray-100 focus:ring-primary-600 focus:border-primary-600"
    : "bg-white border-gray-300 text-gray-900 focus:ring-primary-500 focus:border-primary-500";
};

/**
 * 调试日志函数，只在调试模式开启时输出
 * 避免在生产环境中输出过多日志
 * @param {boolean} enableDebug - 是否启用调试
 * @param {boolean} isDev - 是否为开发环境
 * @param  {...any} args - 日志参数
 */
export const debugLog = (enableDebug, isDev, ...args) => {
  if (enableDebug || isDev) {
    console.log(...args);
  }
};
