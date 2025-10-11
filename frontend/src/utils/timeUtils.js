/**
 * 统一的时间处理工具函数
 * 用于处理从后端接收的 UTC 时间戳，并转换为用户本地时区的时间显示
 *
 * 后端现在统一使用 CURRENT_TIMESTAMP 存储 UTC 时间
 * 前端负责根据用户的时区设置进行本地化显示
 */

// 获取当前语言设置
const getCurrentLanguage = () => {
  try {
    return localStorage.getItem("language") || "zh-CN";
  } catch {
    return "zh-CN";
  }
};

// 简单的翻译映射 - 避免在工具函数中使用复杂的国际化
const translations = {
  "zh-CN": {
    unknown: "未知",
    dateInvalid: "日期无效",
    dateFormatError: "日期格式错误",
    soon: "即将",
    justNow: "刚刚",
    minutesAgo: "{count}分钟前",
    minutesLater: "{count}分钟后",
    hoursAgo: "{count}小时前",
    hoursLater: "{count}小时后",
    daysAgo: "{count}天前",
    daysLater: "{count}天后",
    weeksAgo: "{count}周前",
    weeksLater: "{count}周后",
    monthsAgo: "{count}个月前",
    monthsLater: "{count}个月后",
    yearsAgo: "{count}年前",
    yearsLater: "{count}年后",
    neverExpires: "永不过期",
    expired: "已过期",
  },
  "en-US": {
    unknown: "Unknown",
    dateInvalid: "Invalid Date",
    dateFormatError: "Date Format Error",
    soon: "Soon",
    justNow: "Just now",
    minutesAgo: "{count} minutes ago",
    minutesLater: "{count} minutes later",
    hoursAgo: "{count} hours ago",
    hoursLater: "{count} hours later",
    daysAgo: "{count} days ago",
    daysLater: "{count} days later",
    weeksAgo: "{count} weeks ago",
    weeksLater: "{count} weeks later",
    monthsAgo: "{count} months ago",
    monthsLater: "{count} months later",
    yearsAgo: "{count} years ago",
    yearsLater: "{count} years later",
    neverExpires: "Never expires",
    expired: "Expired",
  },
};

// 获取翻译文本
const t = (key, params = {}) => {
  const lang = getCurrentLanguage();
  const langTranslations = translations[lang] || translations["zh-CN"];
  let text = langTranslations[key] || key;

  // 简单的参数替换
  if (params.count !== undefined) {
    text = text.replace("{count}", params.count);
  }

  return text;
};

/**
 * 时间格式化选项配置
 */
const TIME_FORMAT_OPTIONS = {
  // 完整日期时间格式（年-月-日 时:分）
  FULL_DATETIME: {
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false, // 使用24小时制
  },

  // 完整日期时间格式（包含秒）
  FULL_DATETIME_WITH_SECONDS: {
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  },

  // 相对时间单位（毫秒）
  RELATIVE_TIME_UNITS: {
    MINUTE: 60,
    HOUR: 3600,
    DAY: 86400,
    WEEK: 604800,
    MONTH: 2592000, // 30天
    YEAR: 31536000, // 365天
  },
};

/**
 * 将 UTC 时间字符串转换为本地 Date 对象
 * @param {string|Date|number} utcDateString - UTC 时间字符串、Date对象或时间戳
 * @returns {Date|null} 本地 Date 对象，如果无效则返回 null
 */
export const parseUTCDate = (utcDateString) => {
  if (!utcDateString) {
    return null;
  }

  try {
    // 如果已经是 Date 对象，直接返回
    if (utcDateString instanceof Date) {
      return isNaN(utcDateString.getTime()) ? null : utcDateString;
    }

    // 如果不是字符串，说明数据类型不正确，返回 null
    if (typeof utcDateString !== "string") {
      return null;
    }

    let dateString = utcDateString.trim();

    // 处理不同的UTC时间格式
    // 1. 如果已经是ISO格式（带Z或时区偏移），直接解析
    if (dateString.includes("T") && (dateString.endsWith("Z") || /[+-]\d{2}:\d{2}$/.test(dateString))) {
      const date = new Date(dateString);
      return isNaN(date.getTime()) ? null : date;
    }

    // 2. 如果是SQLite CURRENT_TIMESTAMP格式 "YYYY-MM-DD HH:mm:ss"，需要明确指定为UTC
    if (/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/.test(dateString)) {
      // 将空格替换为T，并添加Z表示UTC时间
      dateString = dateString.replace(" ", "T") + "Z";
    }
    // 3. 如果是日期格式 "YYYY-MM-DD"，添加时间和UTC标识
    else if (/^\d{4}-\d{2}-\d{2}$/.test(dateString)) {
      dateString = dateString + "T00:00:00Z";
    }
    // 4. 如果是ISO格式但没有Z，添加Z
    else if (/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$/.test(dateString)) {
      dateString = dateString + "Z";
    }

    const date = new Date(dateString);
    return isNaN(date.getTime()) ? null : date;
  } catch (error) {
    console.error("解析 UTC 时间失败:", error, "输入:", utcDateString);
    return null;
  }
};

/**
 * 获取用户的首选语言设置
 * @returns {string} 用户的语言代码
 */
const getUserLocale = () => {
  // 优先使用浏览器语言设置
  if (navigator.language) {
    return navigator.language;
  }
  // 备选方案：使用浏览器语言列表的第一个
  if (navigator.languages && navigator.languages.length > 0) {
    return navigator.languages[0];
  }
  // 最后备选：默认中文
  return "zh-CN";
};

/**
 * 格式化日期时间为本地时间显示
 * @param {string} utcDateString - UTC 时间字符串
 * @param {Object} options - 格式化选项，默认为完整日期时间格式
 * @param {string} locale - 地区设置，默认自动检测用户语言
 * @returns {string} 格式化后的本地时间字符串
 */
export const formatDateTime = (utcDateString, options = TIME_FORMAT_OPTIONS.FULL_DATETIME, locale = getUserLocale()) => {
  if (!utcDateString) return t("unknown");

  const date = parseUTCDate(utcDateString);
  if (!date) {
    console.warn("时间解析失败:", utcDateString);
    return t("dateInvalid");
  }

  try {
    return new Intl.DateTimeFormat(locale, options).format(date);
  } catch (error) {
    console.error("日期格式化错误:", error, "输入:", utcDateString);
    return t("dateFormatError");
  }
};

/**
 * 格式化日期时间（包含秒）
 * @param {string} utcDateString - UTC 时间字符串
 * @param {string} locale - 地区设置，默认自动检测用户语言
 * @returns {string} 格式化后的时间字符串
 */
export const formatDateTimeWithSeconds = (utcDateString, locale = getUserLocale()) => {
  return formatDateTime(utcDateString, TIME_FORMAT_OPTIONS.FULL_DATETIME_WITH_SECONDS, locale);
};

/**
 * 计算相对时间（如：3天前、2小时后）
 * @param {string} utcDateString - UTC 时间字符串
 * @param {Date} baseDate - 基准时间，默认为当前时间
 * @returns {string} 相对时间描述
 */
export const formatRelativeTime = (utcDateString, baseDate = new Date()) => {
  if (!utcDateString) return "";

  const targetDate = parseUTCDate(utcDateString);
  if (!targetDate) {
    return "";
  }

  try {
    // 计算时间差（秒）
    const diffInSeconds = Math.floor((targetDate - baseDate) / 1000);
    const absDiff = Math.abs(diffInSeconds);
    const isInFuture = diffInSeconds > 0;

    const { MINUTE, HOUR, DAY, WEEK, MONTH, YEAR } = TIME_FORMAT_OPTIONS.RELATIVE_TIME_UNITS;

    // 根据时间差返回不同的描述
    if (absDiff < MINUTE) {
      return isInFuture ? t("soon") : t("justNow");
    } else if (absDiff < HOUR) {
      const minutes = Math.floor(absDiff / MINUTE);
      return isInFuture ? t("minutesLater", { count: minutes }) : t("minutesAgo", { count: minutes });
    } else if (absDiff < DAY) {
      const hours = Math.floor(absDiff / HOUR);
      return isInFuture ? t("hoursLater", { count: hours }) : t("hoursAgo", { count: hours });
    } else if (absDiff < WEEK) {
      const days = Math.floor(absDiff / DAY);
      return isInFuture ? t("daysLater", { count: days }) : t("daysAgo", { count: days });
    } else if (absDiff < MONTH) {
      const weeks = Math.floor(absDiff / WEEK);
      return isInFuture ? t("weeksLater", { count: weeks }) : t("weeksAgo", { count: weeks });
    } else if (absDiff < YEAR) {
      const months = Math.floor(absDiff / MONTH);
      return isInFuture ? t("monthsLater", { count: months }) : t("monthsAgo", { count: months });
    } else {
      const years = Math.floor(absDiff / YEAR);
      return isInFuture ? t("yearsLater", { count: years }) : t("yearsAgo", { count: years });
    }
  } catch (error) {
    console.error("相对时间计算错误:", error);
    return "";
  }
};

/**
 * 格式化过期时间显示
 * @param {string} expiryDateString - 过期时间的 UTC 字符串
 * @returns {string} 格式化后的过期时间描述
 */
export const formatExpiry = (expiryDateString) => {
  if (!expiryDateString) return t("neverExpires");

  const expiryDate = parseUTCDate(expiryDateString);
  if (!expiryDate) {
    return t("dateInvalid");
  }

  const now = new Date();

  try {
    // 判断是否已过期
    if (expiryDate < now) {
      return t("expired");
    }

    // 显示具体日期和相对时间
    const formattedDate = formatDateTime(expiryDateString);
    const relativeTime = formatRelativeTime(expiryDateString, now);

    return `${formattedDate} (${relativeTime})`;
  } catch (error) {
    console.error("过期时间格式化错误:", error);
    return t("dateFormatError");
  }
};

/**
 * 检查时间是否已过期
 * @param {string} expiryDateString - 过期时间的 UTC 字符串
 * @returns {boolean} 是否已过期
 */
export const isExpired = (expiryDateString) => {
  if (!expiryDateString) return false;

  const expiryDate = parseUTCDate(expiryDateString);
  if (!expiryDate) return false;

  return expiryDate < new Date();
};

/**
 * 格式化时间用于显示"最后刷新时间"等场景
 * @returns {string} 当前本地时间的简短格式
 */
export const formatCurrentTime = () => {
  const now = new Date();
  return now.toLocaleTimeString(getUserLocale(), {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
};

// 导出时间格式选项，供其他组件使用
export { TIME_FORMAT_OPTIONS };
