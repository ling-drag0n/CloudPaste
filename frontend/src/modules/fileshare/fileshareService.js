import {
  listSharedFiles,
  getSharedFileById,
  getSharedFileBySlug,
  getSharedFolderBySlug,
  verifySharedFolderPassword,
  getSharedFolderList,
  buildDownloadUrl as gatewayBuildDownloadUrl,
  buildFolderDownloadUrl as gatewayBuildFolderDownloadUrl,
  buildFolderPreviewUrl as gatewayBuildFolderPreviewUrl,
  buildPreviewUrl as gatewayBuildPreviewUrl,
  buildContentUrl as gatewayBuildContentUrl,
  getOfficePreviewUrl as gatewayGetOfficePreviewUrl,
  registerShareAccessToken,
} from "@/api/services/fileGateway.js";
import {
  updateFile as apiUpdateFile,
  batchDeleteFiles as apiBatchDeleteFiles,
  verifyFilePassword as apiVerifyFilePassword,
} from "@/api/services/fileService.js";

/** @typedef {import("@/types/fileshare").FileshareItem} FileshareItem */
/** @typedef {import("@/types/api").PaginationInfo} PaginationInfo */

/**
 * Fileshare 服务
 *
 * - 基于 fileGateway / fileService 提供的 API 做统一封装
 * - 统一列表、详情、URL 构建、删除、密码验证等能力
 */
export function useFileshareService() {
  /**
   * 获取文件分享列表
   *
   * @param {{limit?:number,offset?:number,search?:string}} [options]
   * @returns {Promise<{files: FileshareItem[], pagination: PaginationInfo}>}
   */
  const fetchList = async ({ limit, offset, search } = {}) => {
    const listOptions = {};
    if (search && search.trim().length > 0) {
      listOptions.search = search.trim();
    }
    const result = await listSharedFiles(limit, offset, listOptions);
    return /** @type {{files: FileshareItem[], pagination: PaginationInfo}} */ (result);
  };

  /**
   * 根据 id 获取文件分享详情
   * @param {number|string} id
   * @returns {Promise<FileshareItem>}
   */
  const fetchById = async (id, options = {}) => {
    if (!id) {
      throw new Error("缺少文件 ID");
    }
    return /** @type {FileshareItem} */ (await getSharedFileById(id, options));
  };

  /**
   * 根据 slug 获取文件分享详情
   * @param {string} slug
   * @returns {Promise<FileshareItem>}
   */
  const fetchBySlug = async (slug) => {
    if (!slug) {
      throw new Error("缺少文件 slug");
    }
    return /** @type {FileshareItem} */ (await getSharedFileBySlug(slug));
  };

  /**
   * 获取文件夹分享元信息（无密码时含根目录 items）
   * @param {string} slug
   * @returns {Promise<Object>}
   */
  const fetchFolderBySlug = async (slug) => {
    if (!slug) {
      throw new Error("缺少文件夹分享 slug");
    }
    return await getSharedFolderBySlug(slug);
  };

  /**
   * 验证文件夹分享密码（成功后返回根目录 items）
   * @param {string} slug
   * @param {string} password
   * @returns {Promise<Object>}
   */
  const verifyFolderPassword = async (slug, password) => {
    if (!slug || !password) {
      throw new Error("缺少文件夹标识或密码");
    }
    const data = await verifySharedFolderPassword(slug, password);
    // 登记短时效访问令牌：后续 list/download URL 优先用 ?at=，避免明文密码进 URL
    if (data?.accessToken) {
      registerShareAccessToken(slug, data.accessToken);
    }
    return data;
  };

  /**
   * 获取文件夹分享子目录列表
   * @param {string} slug
   * @param {{path?:string, password?:string}} [options]
   * @returns {Promise<Object>}
   */
  const fetchFolderList = async (slug, options = {}) => {
    if (!slug) {
      throw new Error("缺少文件夹分享 slug");
    }
    return await getSharedFolderList(slug, options);
  };

  /**
   * 构建文件夹分享单文件下载 URL（需携带分享根相对路径，密码分享附加 password）
   * @param {string} slug
   * @param {string} path
   * @param {string} [password]
   * @returns {string}
   */
  const buildFolderDownloadUrl = (slug, path, password) => {
    return gatewayBuildFolderDownloadUrl(slug, path, password);
  };

  /**
   * 构建文件夹分享单文件预览 URL（内联展示，不强制下载）
   * @param {string} slug
   * @param {string} path
   * @param {string} [password]
   * @returns {string}
   */
  const buildFolderPreviewUrl = (slug, path, password) => {
    return gatewayBuildFolderPreviewUrl(slug, path, password);
  };

  /**
   * 构建文件夹分享页 URL（/folder/:slug）
   * @param {Object} folder - 至少包含 slug
   * @param {string} [origin]
   * @returns {string}
   */
  const buildFolderShareUrl = (folder, origin) => {
    if (!folder || !folder.slug) return "";
    const base =
      typeof origin === "string" && origin.length
        ? origin
        : typeof window !== "undefined"
        ? window.location.origin
        : "";
    if (!base) return `/folder/${folder.slug}`;
    const normalized = base.replace(/\/+$/, "");
    return `${normalized}/folder/${folder.slug}`;
  };

  /**
   * 获取永久下载 URL
   * @param {FileshareItem} file
   * @returns {string}
   */
  const getPermanentDownloadUrl = (file) => {
    if (!file || !file.slug) return "";
    return gatewayBuildDownloadUrl(file);
  };

  /**
   * 获取永久预览 URL
   * @param {FileshareItem} file
   * @returns {string}
   */
  const getPermanentPreviewUrl = (file) => {
    if (!file || !file.slug) return "";
    return gatewayBuildPreviewUrl(file);
  };

  /**
   * 获取用于“内容访问”的 URL（文本类预览使用）
   * - 始终通过 share 内容路由 /api/share/content/:slug 访问
   * @param {FileshareItem} file
   * @returns {string}
   */
  const getPermanentContentUrl = (file) => {
    if (!file || !file.slug) return "";
    return gatewayBuildContentUrl(file);
  };

  /**
   * 获取 Office 预览 URL
   * @param {FileshareItem} file
   * @param {{provider?: "microsoft"|"google", returnAll?: boolean}} [options]
   * @returns {Promise<any>}
   */
  const getOfficePreviewUrl = async (file, options = {}) => {
    return gatewayGetOfficePreviewUrl(file, {
      provider: options.provider || "microsoft",
      returnAll: options.returnAll || false,
    });
  };

  /**
   * 构建前端访问的分享页 URL（/file/:slug）
   *
   * @param {FileshareItem} file
   * @param {string} [origin]
   * @returns {string}
   */
  const buildShareUrl = (file, origin) => {
    if (!file || !file.slug) return "";
    const base =
      typeof origin === "string" && origin.length
        ? origin
        : typeof window !== "undefined"
        ? window.location.origin
        : "";
    if (!base) return `/file/${file.slug}`;
    const normalized = base.replace(/\/+$/, "");
    return `${normalized}/file/${file.slug}`;
  };

  /**
   * 更新文件元数据（备注、最大下载次数、过期时间等）
   * @param {number|string} fileId
   * @param {Partial<FileshareItem>} metadata
   * @returns {Promise<true|FileshareItem>}
   */
  const updateFileMetadata = async (fileId, metadata) => {
    if (!fileId) {
      throw new Error("缺少文件 ID");
    }
    const resp = await apiUpdateFile(fileId, metadata);
    if (resp && typeof resp === "object" && "success" in resp) {
      if (!resp.success) {
        throw new Error(resp.message || "更新文件信息失败");
      }
      return resp.data ?? true;
    }
    return true;
  };

  /**
   * 批量删除文件（admin/public 共用）
   * @param {Array<number|string>} ids
   * @param {string} deleteMode
   * @returns {Promise<any>}
   */
  const deleteFiles = async (ids, deleteMode) => {
    if (!Array.isArray(ids) || ids.length === 0) {
      throw new Error("缺少要删除的文件 ID 列表");
    }
    const resp = await apiBatchDeleteFiles(ids, deleteMode);
    if (!resp) {
      throw new Error("删除文件失败");
    }
    if (typeof resp === "object" && "success" in resp) {
      if (!resp.success) {
        throw new Error(resp.message || "删除文件失败");
      }
    }
    return resp;
  };

  /**
   * 验证受密码保护的文件分享
   * @param {string} slug
   * @param {string} password
   * @returns {Promise<boolean|any>}
   */
  const verifyFilePassword = async (slug, password) => {
    if (!slug || !password) {
      throw new Error("缺少文件标识或密码");
    }
    const resp = await apiVerifyFilePassword(slug, password);
    if (!resp) {
      throw new Error("验证文件密码失败");
    }
    if (typeof resp === "object" && "success" in resp) {
      if (!resp.success) {
        throw new Error(resp.message || "验证文件密码失败");
      }
      const data = resp.data ?? true;
      // 登记短时效访问令牌：下载/预览 URL 优先用 ?at=，避免明文密码进 URL
      if (data && typeof data === "object" && data.accessToken) {
        registerShareAccessToken(slug, data.accessToken);
      }
      return data;
    }
    return resp;
  };

  return {
    fetchList,
    fetchById,
    fetchBySlug,
    fetchFolderBySlug,
    verifyFolderPassword,
    fetchFolderList,
    buildFolderDownloadUrl,
    buildFolderPreviewUrl,
    buildFolderShareUrl,
    getPermanentDownloadUrl,
    getPermanentPreviewUrl,
    getPermanentContentUrl,
    getOfficePreviewUrl,
    buildShareUrl,
    updateFileMetadata,
    deleteFiles,
    verifyFilePassword,
  };
}
