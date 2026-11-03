import { ref, reactive, computed } from "vue";
import { useI18n } from "vue-i18n";
import { usePasteService } from "@/modules/paste";
import { copyToClipboard } from "@/utils/clipboard";
import { useAuthStore } from "@/stores/authStore.js";
import { useAdminBase } from "@/modules/admin";
import { generateQRCode as createQRCodeImage } from "@/utils/qrcodeUtils.js";
import { createLogger } from "@/utils/logger.js";

/** @typedef {import("@/types/paste").Paste} Paste */
/** @typedef {import("@/types/api").PaginationInfo} PaginationInfo */

/**
 * 文本管理 Admin composable
 * 基于 useAdminBase + usePasteService 统一管理 Admin 文本列表的加载、删除、搜索、预览、编辑等逻辑
 * @param {Object} options - 可选配置
 * @param {Function} options.confirmFn - 确认对话框函数（必需），接收 {title, message, confirmType}，返回 Promise<boolean>
 */
export function usePasteManagement(options = {}) {
  const { confirmFn } = options;
  if (!confirmFn) {
    throw new Error("usePasteManagement 必须传入 confirmFn（请在 View 里用 useConfirmDialog + createConfirmFn 创建）");
  }

  // 国际化
  const { t } = useI18n();
  const log = createLogger("PasteManagement");

  const base = useAdminBase('paste', {
    viewMode: {
      storageKey: 'paste-admin-view-mode',
      defaultMode: 'table',
    },
  });
  const pasteService = usePasteService();

  // ========== 搜索（服务端搜索） ==========
  const searchQuery = ref("");
  const isSearchMode = ref(false);
  const searchLoading = ref(false);

  // ========== 标签管理与筛选 ==========
  const tags = ref([]);
  const tagsLoading = ref(false);
  const tagTotalCount = ref(0);
  const untaggedCount = ref(0);
  const selectedTagIds = ref([]);
  const showUntagged = ref(false);
  const showTagManager = ref(false);
  const showBatchTagModal = ref(false);
  const batchTagMode = ref("add");

  /** @type {import("vue").Ref<Paste[]>} */
  const pastes = ref([]);
  /** @type {import("vue").Ref<Paste | null>} */
  const previewPaste = ref(null);
  /** @type {import("vue").Ref<Paste | null>} */
  const editingPaste = ref(null);
  const showPreview = ref(false);
  const showEdit = ref(false);
  const showQRCodeModal = ref(false);
  const qrCodeDataURL = ref("");
  const qrCodeSlug = ref("");

  const copiedTexts = reactive(/** @type {Record<string, boolean>} */ ({}));
  const copiedRawTexts = reactive(/** @type {Record<string, boolean>} */ ({}));

  const authStore = useAuthStore();

  const isAdmin = computed(() => authStore.isAdmin);
  const isApiKeyUser = computed(() => authStore.authType === "apikey" && authStore.hasTextManagePermission);
  const isAuthorized = computed(() => isAdmin.value || isApiKeyUser.value);

  const loadTags = async () => {
    tagsLoading.value = true;
    try {
      const result = await pasteService.getTags();
      tags.value = result.tags;
      tagTotalCount.value = result.totalCount;
      untaggedCount.value = result.untaggedCount;

      const validIds = new Set(tags.value.map((tag) => tag.id));
      selectedTagIds.value = selectedTagIds.value.filter((id) => validIds.has(id));
    } catch (err) {
      log.error("加载标签失败:", err);
      base.showError(err.message || "加载标签失败");
    } finally {
      tagsLoading.value = false;
    }
  };

  /**
   * 加载文本列表
   * @returns {Promise<void>}
   */
  const loadPastes = async () => {
    return base.withLoading(async () => {
      try {
        const { items, pagination } = await pasteService.getPastes({
          limit: base.pagination.limit,
          offset: base.pagination.offset,
          tagIds: selectedTagIds.value,
          untagged: showUntagged.value,
        });

        pastes.value = items;
        base.updatePagination(pagination, "offset");
        base.updateLastRefreshTime();
      } catch (err) {
        pastes.value = [];
        throw err;
      }
    });
  };

  /**
   * offset 变更（分页）
   * @param {number} newOffset
   */
  const handleOffsetChange = (newOffset) => {
    base.handlePaginationChange(newOffset, "offset");
    loadPastes();
  };

  /**
   * 删除单条文本
   * @param {string|number} pasteId
   */
  const deletePaste = async (pasteId) => {
    if (!pasteId) {
      base.showError("删除失败：缺少文本标识信息");
      return;
    }

    const confirmed = await confirmFn({
      title: t("common.dialogs.deleteTitle"),
      message: t("common.dialogs.deleteItem", { name: t("paste.item", "该文本") }),
      confirmType: "danger",
    });

    if (!confirmed) {
      return;
    }

    return base.withLoading(async () => {
      await pasteService.deleteSinglePaste(pasteId);
      base.showSuccess("删除成功");
      await Promise.all([loadPastes(), loadTags()]);
    });
  };

  /**
   * 批量删除文本
   */
  const batchDeletePastes = async () => {
    const selectedCount = base.selectedItems.value.length;
    if (selectedCount === 0) {
      base.showError("请先选择要删除的文本");
      return;
    }

    const confirmed = await confirmFn({
      title: t("common.dialogs.deleteTitle"),
      message: t("common.dialogs.deleteMultiple", { count: selectedCount }),
      confirmType: "danger",
    });

    if (!confirmed) {
      return;
    }

    return base.withLoading(async () => {
      await pasteService.deletePastes(base.selectedItems.value);
      base.showSuccess(`成功删除 ${selectedCount} 条文本`);
      base.clearSelection();
      await Promise.all([loadPastes(), loadTags()]);
    });
  };

  /**
   * 清理所有已过期文本（管理员）
   */
  const clearExpiredPastes = async () => {
    if (!isAdmin.value) {
      base.showError("只有管理员可以清理过期文本");
      return;
    }

    const confirmed = await confirmFn({
      title: t("common.dialogs.cleanupTitle"),
      message: t("common.dialogs.cleanupExpired"),
      confirmType: "warning",
    });

    if (!confirmed) {
      return;
    }

    return base.withLoading(async () => {
      const message = await pasteService.clearExpiredPastes();
      base.showSuccess(message || "已清理过期文本");
      await Promise.all([loadPastes(), loadTags()]);
    });
  };

  /**
   * 打开预览弹窗并加载完整内容
   * @param {Paste} paste
   */
  const openPreview = async (paste) => {
    try {
      // 加载完整的文本详情（包含 content 字段）
      const detail = await pasteService.getPasteById(paste.id);
      previewPaste.value = detail;
      showPreview.value = true;
    } catch (err) {
      log.error("获取文本详情失败:", err);
      base.showError("获取文本详情失败");
    }
  };

  const closePreview = () => {
    showPreview.value = false;
    previewPaste.value = null;
  };

  /**
   * 打开编辑弹窗并加载详情
   * @param {Paste} paste
   */
  const openEditModal = async (paste) => {
    try {
      const detail = await pasteService.getPasteById(paste.id);
      editingPaste.value = detail;
      showEdit.value = true;
    } catch (err) {
      log.error("获取文本详情失败:", err);
      base.showError("获取文本详情失败");
    }
  };

  const closeEditModal = () => {
    showEdit.value = false;
    editingPaste.value = null;
  };

  /**
   * 通用的文本更新方法
   * @param {string} pasteId - 文本ID
   * @param {Partial<Paste>} updates - 要更新的字段（支持部分更新）
   * @param {string} successMessage - 成功提示消息
   * @returns {Promise<void>}
   */
  const updatePasteFields = async (pasteId, updates, successMessage = "更新成功") => {
    return base.withLoading(async () => {
      try {
        // 获取完整数据（列表数据不包含content字段）
        const fullPaste = await pasteService.getPasteById(pasteId);

        // 合并更新数据（使用 ?? 空值合并，保持原值）
        const payload = {
          title: updates.title ?? fullPaste.title,
          content: updates.content ?? fullPaste.content,
          remark: updates.remark ?? fullPaste.remark,
          max_views: updates.max_views ?? fullPaste.max_views,
          expires_at: updates.expires_at ?? fullPaste.expires_at,
          is_public: updates.is_public ?? fullPaste.is_public,
        };
        if (Array.isArray(updates.tag_ids)) {
          payload.tag_ids = updates.tag_ids;
        }

        // 处理特殊字段
        if (Object.prototype.hasOwnProperty.call(updates, "newSlug")) {
          payload.newSlug = updates.newSlug;
        }
        if (updates.password) {
          payload.password = updates.password;
        } else if (updates.clearPassword) {
          payload.clearPassword = true;
        }

        await pasteService.updatePaste(fullPaste.slug, payload);
        base.showSuccess(successMessage);
        await loadPastes();
        await loadTags();
      } catch (err) {
        log.error("更新文本失败:", err);
        base.showError(err.message || "更新文本失败");
        throw err;
      }
    });
  };

  /**
   * 提交编辑修改
   * @param {Partial<Paste>} updated
   */
  const submitEdit = async (updated) => {
    if (updated && updated.error) {
      base.showError(updated.error);
      return;
    }

    if (!editingPaste.value || !editingPaste.value.slug) {
      base.showError("提交失败：缺少文本标识");
      return;
    }

    try {
      await updatePasteFields(editingPaste.value.id, updated, "更新成功");
      closeEditModal();
    } catch (err) {
      // 错误已在 updatePasteFields 中处理
    }
  };

  /**
   * 复制访问链接
   * @param {string} slug - 文本标识
   */
  const copyLink = async (slug) => {
    if (!slug) {
      base.showError("复制失败：缺少文本标识");
      return;
    }

    const baseUrl = window.location.origin;
    const link = `${baseUrl}/paste/${slug}`;

    try {
      const ok = await copyToClipboard(link);
      if (ok) {
        copiedTexts[slug] = true;
        base.showSuccess("访问链接已复制");
        setTimeout(() => {
          copiedTexts[slug] = false;
        }, 2000);
      } else {
        base.showError("复制访问链接失败");
      }
    } catch (err) {
      log.error("复制访问链接失败:", err);
      base.showError("复制访问链接失败");
    }
  };

  /**
   * 复制 Raw 链接
   * @param {string} slug - 文本标识
   */
  const copyRawLink = async (slug) => {
    if (!slug) {
      base.showError("复制失败：缺少文本标识");
      return;
    }

    // 从列表中查找完整对象以获取 plain_password
    const pasteObj = pastes.value.find((item) => item.slug === slug);

    try {
      const rawLink = pasteService.getRawPasteUrl(slug, pasteObj?.plain_password || null);
      const ok = await copyToClipboard(rawLink);
      if (ok) {
        copiedRawTexts[slug] = true;
        base.showSuccess("Raw 链接已复制");
        setTimeout(() => {
          copiedRawTexts[slug] = false;
        }, 2000);
      } else {
        base.showError("复制 Raw 链接失败");
      }
    } catch (err) {
      log.error("复制 Raw 链接失败:", err);
      base.showError("复制 Raw 链接失败");
    }
  };

  /**
   * 快速编辑内容（双击编辑功能）
   * @param {Object} payload - { id, slug, content }
   */
  const quickEditContent = async (payload) => {
    if (!payload || !payload.id || !payload.slug || !payload.content) {
      base.showError("编辑失败：缺少必要参数");
      return;
    }

    try {
      // 获取完整数据（列表数据不包含content字段）
      const fullPaste = await pasteService.getPasteById(payload.id);

      // 合并更新数据（只更新 content 字段）
      const updatePayload = {
        title: fullPaste.title,
        content: payload.content,
        remark: fullPaste.remark,
        max_views: fullPaste.max_views,
        expires_at: fullPaste.expires_at,
        is_public: fullPaste.is_public,
      };

      // 调用 API 更新
      await pasteService.updatePaste(fullPaste.slug, updatePayload);

      // 局部更新：直接修改对象属性
      const index = pastes.value.findIndex((p) => p.id === payload.id);
      if (index !== -1) {
        // 直接修改属性而不是替换对象（避免触发整个列表重新渲染）
        pastes.value[index].content = payload.content;
      }

      base.showSuccess("内容已更新");
    } catch (err) {
      log.error("更新文本内容失败:", err);
      base.showError(err.message || "更新文本内容失败");
      throw err;
    }
  };

  /**
   * 打开公共访问页
   * @param {string} slug
   */
  const goToViewPage = (slug) => {
    const baseUrl = window.location.origin;
    const viewUrl = `${baseUrl}/paste/${slug}`;
    window.open(viewUrl, "_blank");
  };

  /**
   * 生成并显示二维码
   * @param {string} slug
   * @param {boolean} darkMode
   */
  const showQRCode = async (slug, darkMode = false) => {
    try {
      if (!slug) {
        base.showError("生成二维码失败：缺少文本标识信息");
        return;
      }

      const baseUrl = window.location.origin;
      const pasteUrl = `${baseUrl}/paste/${slug}`;

      qrCodeDataURL.value = await createQRCodeImage(pasteUrl, { darkMode });
      qrCodeSlug.value = slug;
      showQRCodeModal.value = true;
    } catch (err) {
      log.error("生成二维码失败:", err);
      base.showError("生成二维码失败");
    }
  };

  /**
   * 切换全选状态（按 id 字段）
   */
  const toggleSelectAll = () => {
    base.toggleSelectAll(pastes.value, "id");
  };

  /**
   * 搜索文本列表
   * @param {string} searchTerm
   * @param {number|null} [offset]
   * @returns {Promise<{results: Paste[], pagination: PaginationInfo}>}
   */
  const searchPastes = async (searchTerm, offset = null) => {
    if (!searchTerm || searchTerm.trim().length < 2) {
      return { results: [], pagination: /** @type {PaginationInfo} */ ({ total: 0, limit: base.pagination.limit, offset: 0 }) };
    }

    const searchOffset = offset !== null ? offset : base.pagination.offset;

    return base.withLoading(async () => {
      try {
        const { items, pagination } = await pasteService.getPastes({
          limit: base.pagination.limit,
          offset: searchOffset,
          search: searchTerm.trim(),
          tagIds: selectedTagIds.value,
          untagged: showUntagged.value,
        });

        return {
          results: items,
          pagination: pagination || /** @type {PaginationInfo} */ ({
            total: items.length,
            limit: base.pagination.limit,
            offset: searchOffset,
          }),
        };
      } catch (err) {
        base.showError(err.message || "搜索失败");
        return {
          results: [],
          pagination: /** @type {PaginationInfo} */ ({
            total: 0,
            limit: base.pagination.limit,
            offset: searchOffset,
          }),
        };
      }
    });
  };

  /**
   * 处理全局搜索（服务端）
   * - 规则：少于 2 个字视为“未搜索”
   */
  const handleGlobalSearch = async (value) => {
    searchQuery.value = value;

    if (!value || value.trim().length < 2) {
      await clearSearch();
      return;
    }

    try {
      searchLoading.value = true;
      isSearchMode.value = true;

      base.resetPagination();
      const result = await searchPastes(value.trim(), 0);
      pastes.value = result.results || [];
      base.updatePagination(result.pagination || { total: pastes.value.length, limit: base.pagination.limit, offset: 0 }, "offset");
      base.updateLastRefreshTime();
    } finally {
      searchLoading.value = false;
    }
  };

  /**
   * 清除搜索，回到正常分页列表
   */
  const clearSearch = async () => {
    searchQuery.value = "";
    isSearchMode.value = false;
    base.resetPagination();
    await loadPastes();
  };

  /**
   * 分页变化（支持搜索模式）
   */
  const handleOffsetChangeWithSearch = async (newOffset) => {
    if (isSearchMode.value && searchQuery.value) {
      try {
        searchLoading.value = true;
        const result = await searchPastes(searchQuery.value, newOffset);
        pastes.value = result.results || [];
        base.updatePagination(result.pagination || { total: pastes.value.length, limit: base.pagination.limit, offset: newOffset }, "offset");
      } finally {
        searchLoading.value = false;
      }
      return;
    }

    handleOffsetChange(newOffset);
  };

  /**
   * 每页数量变化（支持搜索模式）
   */
  const handlePageSizeChange = async (newPageSize) => {
    base.changePageSize(newPageSize);
    if (isSearchMode.value && searchQuery.value) {
      await handleGlobalSearch(searchQuery.value);
      return;
    }
    await loadPastes();
  };

  /**
   * 刷新当前列表（保持搜索/分页状态）
   */
  const refreshPastes = async () => {
    if (isSearchMode.value && searchQuery.value) {
      try {
        searchLoading.value = true;
        const result = await searchPastes(searchQuery.value, base.pagination.offset);
        pastes.value = result.results || [];
        base.updatePagination(result.pagination || { total: pastes.value.length, limit: base.pagination.limit, offset: base.pagination.offset }, "offset");
        base.updateLastRefreshTime();
      } finally {
        searchLoading.value = false;
      }
      await loadTags();
      return;
    }
    await loadPastes();
    await loadTags();
  };

  const applyTagFilters = async () => {
    base.resetPagination();
    base.clearSelection();
    if (isSearchMode.value && searchQuery.value) {
      await handleGlobalSearch(searchQuery.value);
      return;
    }
    await loadPastes();
  };

  const toggleTagFilter = async (tagId) => {
    showUntagged.value = false;
    selectedTagIds.value = selectedTagIds.value.includes(tagId)
      ? selectedTagIds.value.filter((id) => id !== tagId)
      : [...selectedTagIds.value, tagId];
    await applyTagFilters();
  };

  const toggleUntaggedFilter = async () => {
    selectedTagIds.value = [];
    showUntagged.value = !showUntagged.value;
    await applyTagFilters();
  };

  const clearTagFilters = async () => {
    if (selectedTagIds.value.length === 0 && !showUntagged.value) return;
    selectedTagIds.value = [];
    showUntagged.value = false;
    await applyTagFilters();
  };

  const openBatchTags = (mode) => {
    if (base.selectedItems.value.length === 0) {
      base.showError("请先选择要整理的文本");
      return;
    }
    batchTagMode.value = mode === "remove" ? "remove" : "add";
    showBatchTagModal.value = true;
  };

  const applyBatchTags = async (tagIds) => {
    const selectedCount = base.selectedItems.value.length;
    if (!selectedCount || !Array.isArray(tagIds) || tagIds.length === 0) return;
    return base.withLoading(async () => {
      await pasteService.batchUpdateTags(base.selectedItems.value, tagIds, batchTagMode.value);
      base.showSuccess(`已为 ${selectedCount} 条文本${batchTagMode.value === "add" ? "添加" : "移除"}标签`);
      showBatchTagModal.value = false;
      base.clearSelection();
      await Promise.all([loadPastes(), loadTags()]);
    });
  };

  const createTag = async (payload) => {
    try {
      await pasteService.createTag(payload);
      base.showSuccess("标签已创建");
      await loadTags();
    } catch (err) {
      base.showError(err.message || "创建标签失败");
    }
  };

  const updateTag = async (id, payload) => {
    try {
      await pasteService.updateTag(id, payload);
      base.showSuccess("标签已更新");
      await Promise.all([loadTags(), loadPastes()]);
    } catch (err) {
      base.showError(err.message || "更新标签失败");
    }
  };

  const deleteTag = async (tag) => {
    const confirmed = await confirmFn({
      title: "删除标签",
      message: `确定删除标签“${tag.name}”吗？文本本身不会被删除。`,
      confirmType: "danger",
    });
    if (!confirmed) return;

    try {
      await pasteService.deleteTag(tag.id);
      base.showSuccess("标签已删除");
      await Promise.all([loadTags(), loadPastes()]);
    } catch (err) {
      base.showError(err.message || "删除标签失败");
    }
  };

  const reorderTags = async (ids) => {
    try {
      await pasteService.reorderTags(ids);
      await loadTags();
    } catch (err) {
      base.showError(err.message || "调整标签顺序失败");
    }
  };

  /**
   * 切换文本可见性
   * @param {Paste} paste
   */
  const toggleVisibility = async (paste) => {
    if (!paste || !paste.id) {
      base.showError("操作失败：缺少文本标识");
      return;
    }

    const newVisibility = !paste.is_public;
    const visibilityText = newVisibility ? "公开" : "私密";

    try {
      // 复用通用更新方法，只更新 is_public 字段
      await updatePasteFields(paste.id, { is_public: newVisibility }, `已切换为${visibilityText}`);
    } catch (err) {
      // 错误已在 updatePasteFields 中处理
    }
  };

  /**
   * 关闭所有弹窗
   */
  const closeAllModals = () => {
    showPreview.value = false;
    showEdit.value = false;
    showQRCodeModal.value = false;
    showTagManager.value = false;
    showBatchTagModal.value = false;
    previewPaste.value = null;
    editingPaste.value = null;
    qrCodeDataURL.value = "";
    qrCodeSlug.value = "";
  };

  return {
    // 继承 admin base
    ...base,

    // 搜索状态
    searchQuery,
    isSearchMode,
    searchLoading,

    // 标签状态
    tags,
    tagsLoading,
    tagTotalCount,
    untaggedCount,
    selectedTagIds,
    showUntagged,
    showTagManager,
    showBatchTagModal,
    batchTagMode,

    // 文本管理状态
    pastes,
    previewPaste,
    editingPaste,
    showPreview,
    showEdit,
    showQRCodeModal,
    qrCodeDataURL,
    qrCodeSlug,
    copiedTexts,
    copiedRawTexts,

    // 权限状态
    isAdmin,
    isApiKeyUser,
    isAuthorized,

    // 操作
    loadPastes,
    loadTags,
    refreshPastes,
    searchPastes,
    handleOffsetChange,
    handleGlobalSearch,
    clearSearch,
    handleOffsetChangeWithSearch,
    handlePageSizeChange,
    deletePaste,
    batchDeletePastes,
    clearExpiredPastes,
    openPreview,
    closePreview,
    openEditModal,
    closeEditModal,
    submitEdit,
    copyLink,
    copyRawLink,
    quickEditContent,
    goToViewPage,
    showQRCode,
    toggleSelectAll,
    toggleVisibility,
    toggleTagFilter,
    toggleUntaggedFilter,
    clearTagFilters,
    openBatchTags,
    applyBatchTags,
    createTag,
    updateTag,
    deleteTag,
    reorderTags,
    closeAllModals,
  };
}
