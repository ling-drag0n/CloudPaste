/**
 * 文件分享查看服务API
 * 统一管理文件分享相关的API调用，包括下载、预览、Office预览
 */

import { get } from "../client";

/**
 * 通过slug下载文件
 * @param {string} slug - 文件短链接
 * @param {string} [password] - 文件密码（如果需要）
 * @returns {Promise<Response>} 文件下载响应
 */
export async function downloadFileBySlug(slug, password = null) {
  const params = {};
  if (password) {
    params.password = password;
  }

  // 使用原生fetch以获取文件流响应
  const { getFullApiUrl, addAuthToken } = await import("../client");
  const url = getFullApiUrl(`file-download/${slug}`);

  // 构建查询参数
  const searchParams = new URLSearchParams(params);
  const fullUrl = searchParams.toString() ? `${url}?${searchParams}` : url;

  // 添加认证头（如果需要）
  const headers = await addAuthToken({});

  return fetch(fullUrl, {
    method: "GET",
    headers,
  });
}

/**
 * 通过slug预览文件
 * @param {string} slug - 文件短链接
 * @param {string} [password] - 文件密码（如果需要）
 * @returns {Promise<Response>} 文件预览响应
 */
export async function previewFileBySlug(slug, password = null) {
  const params = {};
  if (password) {
    params.password = password;
  }

  // 使用原生fetch以获取文件流响应
  const { getFullApiUrl, addAuthToken } = await import("../client");
  const url = getFullApiUrl(`file-view/${slug}`);

  // 构建查询参数
  const searchParams = new URLSearchParams(params);
  const fullUrl = searchParams.toString() ? `${url}?${searchParams}` : url;

  // 添加认证头（如果需要）
  const headers = await addAuthToken({});

  return fetch(fullUrl, {
    method: "GET",
    headers,
  });
}

// 预览服务提供商配置
const PREVIEW_PROVIDERS = {
  microsoft: {
    name: "Microsoft Office Online",
    urlTemplate: "https://view.officeapps.live.com/op/view.aspx?src={url}",
  },
  google: {
    name: "Google Docs Viewer",
    urlTemplate: "https://docs.google.com/viewer?url={url}&embedded=true",
  },
};

/**
 * 获取Office文件预览URL（通过slug）
 * @param {string} slug - 文件短链接
 * @param {string} [password] - 文件密码（如果需要）
 * @returns {Promise<string>} 预览URL字符串
 */
export async function getOfficePreviewUrlBySlug(slug, password = null) {
  const params = {};
  if (password) {
    params.password = password;
  }

  const response = await get(`office-preview/${slug}`, { params });

  // 后端返回格式：{ url, filename, mimetype, expires_in, is_temporary }
  // 提取 url 字段返回
  if (response.success && response.data && response.data.url) {
    return response.data.url;
  } else if (response.url) {
    // 兼容直接返回 url 字段的情况
    return response.url;
  } else {
    throw new Error("无法从响应中获取预览URL");
  }
}

/**
 * 统一的Office预览服务
 * @param {string|Object} input - 文件slug字符串 或 包含directUrl的对象
 * @param {Object} options - 选项
 * @param {string} [options.password] - 文件密码（当input为slug时）
 * @param {string} [options.provider='microsoft'] - 预览服务提供商 ('microsoft' | 'google')
 * @param {boolean} [options.returnAll=false] - 是否返回所有提供商的URL
 * @returns {Promise<string|Object>} 预览URL或包含所有URL的对象
 */
export async function getOfficePreviewUrl(input, options = {}) {
  const { password, provider = "microsoft", returnAll = false } = options;

  try {
    // 获取直接访问URL
    let directUrl;
    if (typeof input === "string") {
      // input是slug，需要调用API获取directUrl
      directUrl = await getOfficePreviewUrlBySlug(input, password);
      if (!directUrl) {
        throw new Error("无法获取Office预览URL");
      }
    } else if (input && typeof input === "object" && input.directUrl) {
      // input是包含directUrl的对象
      directUrl = input.directUrl;
    } else {
      throw new Error("无效的输入参数，需要slug字符串或包含directUrl的对象");
    }

    // 生成预览URL
    const encodedUrl = encodeURIComponent(directUrl);

    if (returnAll) {
      // 返回所有提供商的URL
      const result = { directUrl };
      Object.entries(PREVIEW_PROVIDERS).forEach(([key, config]) => {
        result[key] = config.urlTemplate.replace("{url}", encodedUrl);
      });
      return result;
    } else {
      // 返回指定提供商的URL
      const providerConfig = PREVIEW_PROVIDERS[provider];
      if (!providerConfig) {
        throw new Error(`不支持的预览服务提供商: ${provider}`);
      }
      return providerConfig.urlTemplate.replace("{url}", encodedUrl);
    }
  } catch (error) {
    console.error("获取Office预览URL失败:", error);
    throw new Error(`获取Office预览URL失败: ${error.message}`);
  }
}

/**
 * 构建文件下载URL（用于直接链接）
 * @param {string} slug - 文件短链接
 * @param {string} [password] - 文件密码（如果需要）
 * @returns {string} 完整的下载URL
 */
export function buildDownloadUrl(slug, password = null) {
  // 使用相对路径构建URL，避免异步导入
  const baseUrl = window.location.origin;
  let url = `${baseUrl}/api/file-download/${slug}`;

  if (password) {
    url += `?password=${encodeURIComponent(password)}`;
  }

  return url;
}

/**
 * 构建文件预览URL（用于直接链接）
 * @param {string} slug - 文件短链接
 * @param {string} [password] - 文件密码（如果需要）
 * @returns {string} 完整的预览URL
 */
export function buildPreviewUrl(slug, password = null) {
  // 使用相对路径构建URL，避免异步导入
  const baseUrl = window.location.origin;
  let url = `${baseUrl}/api/file-view/${slug}`;

  if (password) {
    url += `?password=${encodeURIComponent(password)}`;
  }

  return url;
}

/**
 * 检查URL是否为文件分享代理URL
 * @param {string} url - 要检查的URL
 * @returns {Object} 检查结果
 */
export function parseFileShareUrl(url) {
  if (!url) return { isFileShare: false };

  // 检查是否为下载URL
  const downloadMatch = url.match(/\/api\/file-download\/([^?]+)/);
  if (downloadMatch) {
    const slug = downloadMatch[1];
    const urlObj = new URL(url, window.location.origin);
    const password = urlObj.searchParams.get("password");

    return {
      isFileShare: true,
      type: "download",
      slug,
      password,
    };
  }

  // 检查是否为预览URL
  const previewMatch = url.match(/\/api\/file-view\/([^?]+)/);
  if (previewMatch) {
    const slug = previewMatch[1];
    const urlObj = new URL(url, window.location.origin);
    const password = urlObj.searchParams.get("password");

    return {
      isFileShare: true,
      type: "preview",
      slug,
      password,
    };
  }

  return { isFileShare: false };
}

/**
 * 为现有URL添加密码参数
 * @param {string} url - 原始URL
 * @param {string} password - 要添加的密码
 * @returns {string} 添加密码后的URL
 */
export function addPasswordToUrl(url, password) {
  if (!url || !password) return url;

  // 检查URL中是否已包含密码参数
  if (url.includes("password=")) return url;

  const separator = url.includes("?") ? "&" : "?";
  return `${url}${separator}password=${encodeURIComponent(password)}`;
}

/**
 * 将HTTP状态码映射到国际化错误键
 * @param {number} statusCode - HTTP状态码
 * @returns {string} 国际化错误键
 */
export function getErrorKeyByStatus(statusCode) {
  switch (statusCode) {
    case 401:
      return "fileView.errors.unauthorized";
    case 403:
      return "fileView.errors.forbidden";
    case 404:
      return "fileView.errors.notFound";
    case 410:
      return "fileView.errors.forbidden"; // 文件过期也使用 forbidden
    default:
      return "fileView.errors.serverError";
  }
}
