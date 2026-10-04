import * as fileService from "./fileService.js";
import { getFilePassword } from "@/utils/filePasswordUtils.js";
import { getFullApiUrl } from "@/api/config";

function normalizeListResponse(response, limit, offset) {
  const payload = response?.data ?? response?.files ?? response;
  const files = Array.isArray(payload?.files) ? payload.files : Array.isArray(payload) ? payload : [];
  const rawPagination = payload?.pagination;
  const pagination = rawPagination
    ? {
        total: rawPagination.total ?? files.length,
        limit: rawPagination.limit ?? limit,
        offset: rawPagination.offset ?? offset,
        hasMore: rawPagination.hasMore ?? rawPagination.has_more ?? false,
      }
    : { total: files.length, limit, offset, hasMore: false };
  return { files, pagination };
}

/**
 * 检查URL是否为文件分享代理URL
 * @param {string} url - 要检查的URL
 * @returns {{ isFileShare: boolean, type?: "preview"|"download", slug?: string, password?: string|null, mode?: "inline"|"attachment" }}
 */
function parseFileShareUrl(url) {
  if (!url) return { isFileShare: false };

  // Share 文本内容口：/api/share/content/:slug
  // 兼容：/api/share/content/:slug 以及 /api/share/content/:slug/:filename
  const contentMatch = url.match(/\/api\/share\/content\/([^/?#]+)(?:\/[^?]*)?/);
  if (contentMatch) {
    const slug = contentMatch[1];
    const urlObj = new URL(url, window.location.origin);
    const password = urlObj.searchParams.get("password");
    return {
      isFileShare: true,
      type: "preview",
      slug,
      password,
      mode: "inline",
    };
  }

  // Share 本地代理入口：/api/s/:slug
  // 兼容：/api/s/:slug 以及 /api/s/:slug/:filename
  const legacyMatch = url.match(/\/api\/s\/([^/?#]+)(?:\/[^?]*)?/);
  if (legacyMatch) {
    const slug = legacyMatch[1];
    const urlObj = new URL(url, window.location.origin);
    const password = urlObj.searchParams.get("password");
    const down = urlObj.searchParams.get("down");
    const mode = urlObj.searchParams.get("mode") || "inline";
    const isDownload =
      (down && down !== "0" && down !== "false") ||
      mode === "attachment" ||
      mode === "download";

    return {
      isFileShare: true,
      type: isDownload ? "download" : "preview",
      slug,
      password,
      mode: isDownload ? "attachment" : "inline",
    };
  }

  return { isFileShare: false };
}

function ensurePasswordInUrl(url, password, file) {
  if (!url || (!password && !file?.slug)) return url || "";

  // 仅对 share 同源路由（/api/share/content/:slug 或 /api/s/:slug）追加凭据
  // 以及标记为 use_proxy 的文件，避免污染直链/CDN URL
  const shareInfo = parseFileShareUrl(url);
  const isProxy = shareInfo.isFileShare || file?.use_proxy;

  if (!isProxy) return url;

  // 优先使用短时效访问令牌（?at=，由密码验证接口签发），避免明文密码进 URL
  const slug = shareInfo.slug || file?.slug;
  const token = slug ? getShareAccessToken(slug) : null;
  if (token) {
    if (url.includes("at=")) return url;
    const separator = url.includes("?") ? "&" : "?";
    return `${url}${separator}at=${encodeURIComponent(token)}`;
  }

  if (!password) return url;
  if (url.includes("password=")) return url;

  const separator = url.includes("?") ? "&" : "?";
  return `${url}${separator}password=${encodeURIComponent(password)}`;
}

/******************************************************************************
 * 分享访问令牌（?at=）注册表
 * - 密码验证成功后由 fileshareService 登记，URL 构建时优先使用
 * - 仅存内存，刷新页面后重新验证
 ******************************************************************************/
const shareAccessTokenRegistry = new Map();

export function registerShareAccessToken(slug, token) {
  if (!slug || !token) return;
  shareAccessTokenRegistry.set(String(slug), String(token));
}

export function getShareAccessToken(slug) {
  if (!slug) return null;
  return shareAccessTokenRegistry.get(String(slug)) || null;
}

export function clearShareAccessToken(slug) {
  if (slug) shareAccessTokenRegistry.delete(String(slug));
  else shareAccessTokenRegistry.clear();
}

function resolvePassword(file, explicitPassword) {
  if (explicitPassword !== undefined) return explicitPassword;
  return getFilePassword({ file });
}

export async function listSharedFiles(limit = 50, offset = 0, options = {}) {
  const response = await fileService.getFiles(limit, offset, options);
  if (!response?.success) {
    throw new Error(response?.message || "加载文件列表失败");
  }
  return normalizeListResponse(response, limit, offset);
}

export async function getSharedFileById(id, options = {}) {
  const response = await fileService.getFile(id, options);
  if (!response?.success) {
    throw new Error(response?.message || "获取文件详情失败");
  }
  return response.data;
}

export async function getSharedFileBySlug(slug) {
  const response = await fileService.getPublicFile(slug);
  if (!response?.success) {
    throw new Error(response?.message || "获取文件详情失败");
  }
  return response.data;
}

/******************************************************************************
 * 文件夹分享（公开只读）
 ******************************************************************************/

export async function getSharedFolderBySlug(slug) {
  const response = await fileService.getPublicFolder(slug);
  if (!response?.success) {
    throw new Error(response?.message || "获取文件夹分享失败");
  }
  return response.data;
}

export async function verifySharedFolderPassword(slug, password) {
  const response = await fileService.verifyFolderPassword(slug, password);
  if (!response?.success) {
    throw new Error(response?.message || "验证文件夹密码失败");
  }
  return response.data;
}

export async function getSharedFolderList(slug, options = {}) {
  const response = await fileService.getPublicFolderList(slug, options);
  if (!response?.success) {
    throw new Error(response?.message || "获取目录失败");
  }
  return response.data;
}

/**
 * 构建文件夹分享单文件下载 URL（后端代理入口）
 * - path 为分享根目录相对路径；设置了密码的分享需携带 password
 * @param {string} slug
 * @param {string} path
 * @param {string} [password]
 * @returns {string}
 */
export function buildFolderDownloadUrl(slug, path, password) {
  if (!slug || !path) return "";
  const params = new URLSearchParams();
  params.set("path", path);
  // 优先使用访问令牌，避免明文密码进 URL
  const token = getShareAccessToken(slug);
  if (token) params.set("at", token);
  else if (password) params.set("password", password);
  return `${getFullApiUrl(`/share/folder/${encodeURIComponent(slug)}/download`)}?${params.toString()}`;
}

/**
 * 构建文件夹分享单文件预览 URL（内联展示，不强制下载）
 * - 供图片/音视频/PDF 等在页面内预览使用；文本类预览亦复用同一入口
 * @param {string} slug
 * @param {string} path
 * @param {string} [password]
 * @returns {string}
 */
export function buildFolderPreviewUrl(slug, path, password) {
  if (!slug || !path) return "";
  const params = new URLSearchParams();
  params.set("path", path);
  params.set("inline", "1");
  // 优先使用访问令牌，避免明文密码进 URL
  const token = getShareAccessToken(slug);
  if (token) params.set("at", token);
  else if (password) params.set("password", password);
  return `${getFullApiUrl(`/share/folder/${encodeURIComponent(slug)}/download`)}?${params.toString()}`;
}

export function buildPreviewUrl(file, options = {}) {
  if (!file) return "";
  const password = resolvePassword(file, options.password);

  // 预览统一基于 Link JSON 的 previewUrl（已由后端决策为 inline 语义入口）
  let url = file.previewUrl || "";

  if (!url) return "";

  return ensurePasswordInUrl(url, password, file);
}

/**
 * 构建“内容访问” URL（用于文本类预览 / 编码检测）
 * - 语义：始终通过 share 内容路由 /api/share/content/:slug 访问（同源），避免直链 CORS
 * - 与 buildPreviewUrl 的区别：buildPreviewUrl 更偏向“外部预览入口”（新窗口/嵌入），这里专注于同源内容访问
 * @param {FileshareItem} file
 * @param {{ password?: string }} [options]
 * @returns {string}
 */
export function buildContentUrl(file, options = {}) {
  if (!file || !file.slug) return "";
  const password = resolvePassword(file, options.password);

  // 文本/编码检测专用同源内容口
  let url = `/api/share/content/${file.slug}`;

  // 仅在文件分享场景或 use_proxy=1 时追加密码参数
  url = ensurePasswordInUrl(url, password, file);

  return url;
}

export function buildDownloadUrl(file, options = {}) {
  if (!file) return "";
  const password = resolvePassword(file, options.password);

  // 下载统一基于 Link JSON 的 downloadUrl
  let url = file.downloadUrl || "";

  if (!url) return "";

  return ensurePasswordInUrl(url, password, file);
}

export async function getOfficePreviewUrl(file, options = {}) {
  const preview = file?.previewSelection;

  // 未提供 previewSelection 或不支持预览时直接返回 null
  const providers = preview?.providers || {};
  if (!preview || !Object.keys(providers).length) {
    return null;
  }

  const returnAll = options.returnAll || false;

  if (!returnAll) {
    // 默认使用 providers 中的 microsoft 作为推荐入口
    return providers.microsoft || null;
  }

  // 返回所有可用的预览 URL（如果服务端提供了 providers）
  return {
    directUrl: buildPreviewUrl(file, options) || null,
    microsoft: providers.microsoft || "",
    google: providers.google || "",
  };
}

export function parseShareUrl(url) {
  return parseFileShareUrl(url);
}

export function addPasswordToUrl(url, password) {
  if (!url || !password) return url;

  if (url.includes("password=")) return url;

  const separator = url.includes("?") ? "&" : "?";
  return `${url}${separator}password=${encodeURIComponent(password)}`;
}

export function getFileErrorKey(statusCode) {
  switch (statusCode) {
    case 401:
      return "fileView.errors.unauthorized";
    case 403:
      return "fileView.errors.forbidden";
    case 404:
      return "fileView.errors.notFound";
    case 410:
      return "fileView.errors.forbidden";
    default:
      return "fileView.errors.serverError";
  }
}
