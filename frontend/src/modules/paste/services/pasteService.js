import { api } from "@/api";
import { useAuthStore } from "@/stores/authStore.js";

/** @typedef {import("@/types/paste").Paste} Paste */
/** @typedef {import("@/types/paste").PasteListResponse} PasteListResponse */
/** @typedef {import("@/types/paste").PasteUpdatePayload} PasteUpdatePayload */
/** @typedef {import("@/types/api").PaginationInfo} PaginationInfo */

/**
 * Paste 领域 service
 *
 * - 统一封装文本分享相关 API
 * - 根据当前认证状态选择 admin / user / apikey 路径
 *
 * 约定：
 * - 成功时返回干净的领域数据（Paste / { items, pagination } / true / string）
 * - 失败时抛出 Error(message)，上层由 toast / error block 处理
 */
export function usePasteService() {
  const authStore = useAuthStore();

  const isAdmin = () => authStore.isAdmin;
  const isApiKeyManager = () => authStore.authType === "apikey" && authStore.hasTextManagePermission;
  const isApiKeyCreator = () => authStore.isKeyUser && authStore.hasTextSharePermission;

  const ensureCanManage = () => {
    if (!isAdmin() && !isApiKeyManager()) {
      throw new Error("当前账号无权管理文本数据");
    }
  };

  const ensureCanCreate = () => {
    if (!isAdmin() && !isApiKeyCreator()) {
      throw new Error("当前账号无权创建文本分享");
    }
  };

  /**
   * 列表查询（offset 模式）
   * @param {{limit?:number,offset?:number,search?:string,tagIds?:string[],untagged?:boolean}} [params]
   * @returns {Promise<PasteListResponse>}
   */
  const getPastes = async ({ limit = 20, offset = 0, search, tagIds = [], untagged = false } = {}) => {
    ensureCanManage();

    const options = {};
    if (search && search.trim()) {
      options.search = search.trim();
    }
    if (Array.isArray(tagIds) && tagIds.length > 0) {
      options.tag_ids = tagIds.join(",");
    }
    if (untagged) {
      options.untagged = 1;
    }

    // 当前 admin 与 API Key 文本用户统一走 /api/pastes endpoint
    const resp = await api.paste.getPastes(null, limit, offset, options);
    if (!resp || typeof resp !== "object" || resp.success !== true) {
      const message = resp && typeof resp === "object" && "message" in resp ? resp.message : null;
      throw new Error(message || "获取文本列表失败");
    }

    const payload = resp.data || {};
    const items = Array.isArray(payload.results) ? payload.results : [];
    const pagination = payload.pagination || {};

    /** @type {PaginationInfo} */
    const finalPagination = {
      total: typeof pagination.total === "number" ? pagination.total : items.length,
      limit: typeof pagination.limit === "number" ? pagination.limit : limit,
      offset: typeof pagination.offset === "number" ? pagination.offset : offset,
      hasMore: typeof pagination.hasMore === "boolean" ? pagination.hasMore : undefined,
    };

    return /** @type {PasteListResponse} */ ({
      items,
      pagination: finalPagination,
    });
  };

  /**
   * 根据 ID 获取单条文本（仅 admin / apikey）
   * @param {string|number} id
   * @returns {Promise<Paste>}
   */
  const getPasteById = async (id) => {
    ensureCanManage();
    const resp = isAdmin() ? await api.admin.getPasteById(id) : await api.user.paste.getPasteById(id);
    if (!resp) {
      throw new Error("获取文本详情失败");
    }

    if (typeof resp === "object" && "success" in resp) {
      if (!resp.success) {
        throw new Error(resp.message || "获取文本详情失败");
      }
      return /** @type {Paste} */ (resp.data ?? resp);
    }

    return /** @type {Paste} */ (resp);
  };

  /**
   * 更新文本（仅 admin / apikey）
   * @param {string} slug
   * @param {PasteUpdatePayload} data
   * @returns {Promise<Paste|true>}
   */
  const updatePaste = async (slug, data) => {
    ensureCanManage();
    const resp = isAdmin() ? await api.admin.updatePaste(slug, data) : await api.user.paste.updatePaste(slug, data);
    if (!resp) {
      throw new Error("更新文本失败");
    }

    if (typeof resp === "object" && "success" in resp) {
      if (!resp.success) {
        throw new Error(resp.message || "更新文本失败");
      }
      return resp.data ?? true;
    }

    return resp;
  };

  /**
   * 删除单条文本（内部通过批量删除接口完成）
   * @param {string|number} id
   * @returns {Promise<true>}
   */
  const deleteSinglePaste = async (id) => {
    ensureCanManage();
    const resp = isAdmin() ? await api.admin.batchDeletePastes([id]) : await api.user.paste.batchDeletePastes([id]);
    if (!resp) {
      throw new Error("删除文本失败");
    }

    if (typeof resp === "object" && "success" in resp) {
      if (!resp.success) {
        throw new Error(resp.message || "删除文本失败");
      }
    }

    return true;
  };

  /**
   * 批量删除文本
   * @param {Array<string|number>} ids
   * @returns {Promise<true>}
   */
  const deletePastes = async (ids) => {
    ensureCanManage();
    if (!Array.isArray(ids) || ids.length === 0) {
      throw new Error("缺少待删除文本 ID 列表");
    }
    const resp = isAdmin() ? await api.admin.batchDeletePastes(ids) : await api.user.paste.batchDeletePastes(ids);
    if (!resp) {
      throw new Error("批量删除文本失败");
    }
    if (typeof resp === "object" && "success" in resp && !resp.success) {
      throw new Error(resp.message || "批量删除文本失败");
    }
    return true;
  };

  /**
   * 清理已过期文本（仅 admin）
   * @returns {Promise<string>} 成功信息
   */
  const clearExpiredPastes = async () => {
    if (!isAdmin()) {
      throw new Error("仅管理员可以清理过期文本");
    }
    const resp = await api.system.clearExpiredPastes();
    if (!resp) {
      throw new Error("清理过期文本失败");
    }
    if (typeof resp === "object" && "success" in resp) {
      if (!resp.success) {
        throw new Error(resp.message || "清理过期文本失败");
      }
      return resp.message || "已清理过期文本";
    }
    return "已清理过期文本";
  };

  /**
   * 根据 slug 获取文本详情（公共访问，支持密码）
   * @param {string} slug
   * @param {string|null} [password]
   * @returns {Promise<Paste>}
   */
  const getPasteBySlug = async (slug, password = null) => {
    const resp = await api.paste.getPaste(slug, password);
    if (!resp) {
      throw new Error("获取文本详情失败");
    }

    if (typeof resp === "object" && "success" in resp) {
      if (!resp.success) {
        throw new Error(resp.message || "获取文本详情失败");
      }
      return /** @type {Paste} */ (resp.data ?? resp);
    }

    return /** @type {Paste} */ (resp);
  };

  /**
   * 创建新的文本（Markdown 编辑页使用）
   * @param {Partial<PasteUpdatePayload> & { title?: string | null; is_public?: boolean }} data
   * @returns {Promise<string>} 新创建的 slug
   */
  const createPaste = async (data) => {
    ensureCanCreate();
    const resp = await api.paste.createPaste(data);
    if (!resp) {
      throw new Error("创建文本失败");
    }

    let slug = null;

    if (typeof resp === "object") {
      if ("success" in resp || "code" in resp) {
        if (resp.success && resp.data && resp.data.slug) {
          slug = resp.data.slug;
        } else {
          throw new Error(resp.message || "创建文本失败");
        }
      } else if (resp.slug) {
        slug = resp.slug;
      } else {
        const candidates = ["id", "key", "identifier"];
        for (const field of candidates) {
          if (resp[field]) {
            slug = resp[field];
            break;
          }
        }
      }
    }

    if (!slug) {
      throw new Error("创建文本失败：未能获取到标识");
    }

    return slug;
  };

  /**
   * 获取原始文本直链
   * @param {string} slug
   * @param {string|null} [password]
   * @returns {string}
   */
  const getRawPasteUrl = (slug, password = null) => {
    return api.paste.getRawPasteUrl(slug, password);
  };

  /**
   * 获取 Markdown 渲染相关配置（系统设置组）
   * @returns {Promise<Array<{key:string,value:string}>>}
   */
  const getMarkdownSettings = async () => {
    const resp = await api.system.getSettingsByGroup(4, false);
    if (!resp) {
      throw new Error("获取 Markdown 设置失败");
    }
    if (typeof resp === "object" && "success" in resp) {
      if (!resp.success) {
        throw new Error(resp.message || "获取 Markdown 设置失败");
      }
      return Array.isArray(resp.data) ? resp.data : [];
    }
    return Array.isArray(resp) ? resp : [];
  };

  const unwrap = (resp, fallbackMessage) => {
    if (!resp) throw new Error(fallbackMessage);
    if (typeof resp === "object" && "success" in resp) {
      if (!resp.success) throw new Error(resp.message || fallbackMessage);
      return resp.data;
    }
    return resp;
  };

  const getTags = async () => {
    ensureCanManage();
    const data = unwrap(await api.paste.getPasteTags(), "获取标签失败") || {};
    return {
      tags: Array.isArray(data.tags) ? data.tags : [],
      totalCount: Number(data.total_count || 0),
      untaggedCount: Number(data.untagged_count || 0),
    };
  };

  const createTag = async (data) => {
    if (!isAdmin()) throw new Error("仅管理员可以新建标签");
    return unwrap(await api.paste.createPasteTag(data), "创建标签失败");
  };

  const updateTag = async (id, data) => {
    if (!isAdmin()) throw new Error("仅管理员可以修改标签");
    return unwrap(await api.paste.updatePasteTag(id, data), "修改标签失败");
  };

  const deleteTag = async (id) => {
    if (!isAdmin()) throw new Error("仅管理员可以删除标签");
    unwrap(await api.paste.deletePasteTag(id), "删除标签失败");
    return true;
  };

  const reorderTags = async (ids) => {
    if (!isAdmin()) throw new Error("仅管理员可以调整标签顺序");
    unwrap(await api.paste.reorderPasteTags(ids), "调整标签顺序失败");
    return true;
  };

  const setTags = async (id, tagIds) => {
    ensureCanManage();
    const data = unwrap(await api.paste.setPasteTags(id, tagIds), "更新文本标签失败") || {};
    return Array.isArray(data.tags) ? data.tags : [];
  };

  const batchUpdateTags = async (ids, tagIds, action) => {
    ensureCanManage();
    return unwrap(await api.paste.batchUpdatePasteTags(ids, tagIds, action), "批量更新标签失败");
  };

  return {
    getPastes,
    getPasteById,
    updatePaste,
    deleteSinglePaste,
    deletePastes,
    clearExpiredPastes,
    getPasteBySlug,
    createPaste,
    getRawPasteUrl,
    getMarkdownSettings,
    getTags,
    createTag,
    updateTag,
    deleteTag,
    reorderTags,
    setTags,
    batchUpdateTags,
  };
}
