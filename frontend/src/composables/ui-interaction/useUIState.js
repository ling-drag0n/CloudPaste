/**
 * UI状态管理 Composable
 * 统一管理组件的UI状态，包括弹窗、视图模式等
 */

import { ref, computed } from "vue";
import { useGlobalMessage } from "../core/useGlobalMessage.js";

export function useUIState() {
  // ===== 消息管理 - 使用全局消息系统 =====
  const globalMessage = useGlobalMessage();

  // 兼容性包装器
  const message = globalMessage.message;
  const showMessage = globalMessage.showMessage;
  const showSuccess = globalMessage.showSuccess;
  const showError = globalMessage.showError;
  const showWarning = globalMessage.showWarning;
  const showInfo = globalMessage.showInfo;
  const clearMessage = globalMessage.clearMessage;
  const hasMessage = globalMessage.hasMessage;
  const messageType = globalMessage.messageType;
  const messageContent = globalMessage.messageContent;

  // ===== 视图模式管理 =====
  const viewMode = ref("list"); // 'list' | 'grid' | 'gallery'

  /**
   * 是否为列表模式
   */
  const isListMode = computed(() => viewMode.value === "list");

  /**
   * 是否为网格模式
   */
  const isGridMode = computed(() => viewMode.value === "grid");

  /**
   * 是否为图廊模式
   */
  const isGalleryMode = computed(() => viewMode.value === "gallery");

  /**
   * 设置视图模式
   * @param {string} mode - 目标模式 ('list' | 'grid' | 'gallery')
   */
  const setViewMode = (mode) => {
    if (mode === "list" || mode === "grid" || mode === "gallery") {
      viewMode.value = mode;
    }
  };

  /**
   * 在列表和网格模式之间切换
   */
  const toggleViewMode = () => {
    viewMode.value = viewMode.value === "list" ? "grid" : "list";
  };

  /**
   * 切换到列表模式
   */
  const switchToListMode = () => {
    setViewMode("list");
  };

  /**
   * 切换到网格模式
   */
  const switchToGridMode = () => {
    setViewMode("grid");
  };

  /**
   * 切换到图廊模式
   */
  const switchToGalleryMode = () => {
    setViewMode("gallery");
  };

  // ===== 弹窗状态管理 =====
  const isUploadModalOpen = ref(false);
  const isCopyModalOpen = ref(false);
  const isTasksModalOpen = ref(false);
  const isSearchModalOpen = ref(false);
  const isDeleteConfirmModalOpen = ref(false);
  const isRenameModalOpen = ref(false);
  const isCreateFolderModalOpen = ref(false);

  /**
   * 打开上传弹窗
   */
  const openUploadModal = () => {
    isUploadModalOpen.value = true;
  };

  /**
   * 关闭上传弹窗
   */
  const closeUploadModal = () => {
    isUploadModalOpen.value = false;
  };

  /**
   * 切换上传弹窗状态
   */
  const toggleUploadModal = () => {
    isUploadModalOpen.value = !isUploadModalOpen.value;
  };

  /**
   * 打开复制弹窗
   */
  const openCopyModal = () => {
    isCopyModalOpen.value = true;
  };

  /**
   * 关闭复制弹窗
   */
  const closeCopyModal = () => {
    isCopyModalOpen.value = false;
  };

  /**
   * 切换复制弹窗状态
   */
  const toggleCopyModal = () => {
    isCopyModalOpen.value = !isCopyModalOpen.value;
  };

  /**
   * 打开任务弹窗
   */
  const openTasksModal = () => {
    isTasksModalOpen.value = true;
  };

  /**
   * 关闭任务弹窗
   */
  const closeTasksModal = () => {
    isTasksModalOpen.value = false;
  };

  /**
   * 切换任务弹窗状态
   */
  const toggleTasksModal = () => {
    isTasksModalOpen.value = !isTasksModalOpen.value;
  };

  /**
   * 打开搜索弹窗
   */
  const openSearchModal = () => {
    isSearchModalOpen.value = true;
  };

  /**
   * 关闭搜索弹窗
   */
  const closeSearchModal = () => {
    isSearchModalOpen.value = false;
  };

  /**
   * 切换搜索弹窗状态
   */
  const toggleSearchModal = () => {
    isSearchModalOpen.value = !isSearchModalOpen.value;
  };

  /**
   * 打开删除确认弹窗
   */
  const openDeleteConfirmModal = () => {
    isDeleteConfirmModalOpen.value = true;
  };

  /**
   * 关闭删除确认弹窗
   */
  const closeDeleteConfirmModal = () => {
    isDeleteConfirmModalOpen.value = false;
  };

  /**
   * 切换删除确认弹窗状态
   */
  const toggleDeleteConfirmModal = () => {
    isDeleteConfirmModalOpen.value = !isDeleteConfirmModalOpen.value;
  };

  /**
   * 打开重命名弹窗
   */
  const openRenameModal = () => {
    isRenameModalOpen.value = true;
  };

  /**
   * 关闭重命名弹窗
   */
  const closeRenameModal = () => {
    isRenameModalOpen.value = false;
  };

  /**
   * 切换重命名弹窗状态
   */
  const toggleRenameModal = () => {
    isRenameModalOpen.value = !isRenameModalOpen.value;
  };

  /**
   * 打开创建文件夹弹窗
   */
  const openCreateFolderModal = () => {
    isCreateFolderModalOpen.value = true;
  };

  /**
   * 关闭创建文件夹弹窗
   */
  const closeCreateFolderModal = () => {
    isCreateFolderModalOpen.value = false;
  };

  /**
   * 切换创建文件夹弹窗状态
   */
  const toggleCreateFolderModal = () => {
    isCreateFolderModalOpen.value = !isCreateFolderModalOpen.value;
  };

  /**
   * 关闭所有弹窗
   */
  const closeAllModals = () => {
    isUploadModalOpen.value = false;
    isCopyModalOpen.value = false;
    isTasksModalOpen.value = false;
    isSearchModalOpen.value = false;
    isDeleteConfirmModalOpen.value = false;
    isRenameModalOpen.value = false;
    isCreateFolderModalOpen.value = false;
  };

  /**
   * 检查是否有弹窗打开
   * @returns {boolean} 是否有弹窗打开
   */
  const hasOpenModal = computed(() => {
    return (
      isUploadModalOpen.value ||
      isCopyModalOpen.value ||
      isTasksModalOpen.value ||
      isSearchModalOpen.value ||
      isDeleteConfirmModalOpen.value ||
      isRenameModalOpen.value ||
      isCreateFolderModalOpen.value
    );
  });

  /**
   * 获取当前打开的弹窗列表
   * @returns {Array} 打开的弹窗名称列表
   */
  const getOpenModals = () => {
    const openModals = [];
    if (isUploadModalOpen.value) openModals.push("upload");
    if (isCopyModalOpen.value) openModals.push("copy");
    if (isTasksModalOpen.value) openModals.push("tasks");
    if (isSearchModalOpen.value) openModals.push("search");
    if (isDeleteConfirmModalOpen.value) openModals.push("deleteConfirm");
    if (isRenameModalOpen.value) openModals.push("rename");
    if (isCreateFolderModalOpen.value) openModals.push("createFolder");
    return openModals;
  };

  // ===== 加载状态管理 =====
  const isLoading = ref(false);
  const loadingMessage = ref("");

  /**
   * 设置加载状态
   * @param {boolean} loading - 是否加载中
   * @param {string} message - 加载消息
   */
  const setLoading = (loading, message = "") => {
    isLoading.value = loading;
    loadingMessage.value = message;
  };

  /**
   * 开始加载
   * @param {string} message - 加载消息
   */
  const startLoading = (message = "加载中...") => {
    setLoading(true, message);
  };

  /**
   * 停止加载
   */
  const stopLoading = () => {
    setLoading(false, "");
  };

  // ===== 工具方法 =====

  /**
   * 重置所有UI状态
   */
  const resetUIState = () => {
    globalMessage.clearMessage();
    closeAllModals();
    stopLoading();
    setViewMode("list");
  };

  /**
   * 获取UI状态摘要
   * @returns {Object} UI状态摘要
   */
  const getUIStateSummary = () => {
    return {
      hasMessage: hasMessage.value,
      messageType: messageType.value,
      viewMode: viewMode.value,
      hasOpenModal: hasOpenModal.value,
      openModals: getOpenModals(),
      isLoading: isLoading.value,
      loadingMessage: loadingMessage.value,
    };
  };

  return {
    // 消息管理
    message,
    showMessage,
    showSuccess,
    showError,
    showWarning,
    showInfo,
    clearMessage,
    hasMessage,
    messageType,
    messageContent,

    // 视图模式管理
    viewMode,
    isListMode,
    isGridMode,
    isGalleryMode,
    setViewMode,
    toggleViewMode,
    switchToListMode,
    switchToGridMode,
    switchToGalleryMode,

    // 弹窗状态管理
    isUploadModalOpen,
    isCopyModalOpen,
    isTasksModalOpen,
    isSearchModalOpen,
    isDeleteConfirmModalOpen,
    isRenameModalOpen,
    isCreateFolderModalOpen,
    openUploadModal,
    closeUploadModal,
    toggleUploadModal,
    openCopyModal,
    closeCopyModal,
    toggleCopyModal,
    openTasksModal,
    closeTasksModal,
    toggleTasksModal,
    openSearchModal,
    closeSearchModal,
    toggleSearchModal,
    openDeleteConfirmModal,
    closeDeleteConfirmModal,
    toggleDeleteConfirmModal,
    openRenameModal,
    closeRenameModal,
    toggleRenameModal,
    openCreateFolderModal,
    closeCreateFolderModal,
    toggleCreateFolderModal,
    closeAllModals,
    hasOpenModal,
    getOpenModals,

    // 加载状态管理
    isLoading,
    loadingMessage,
    setLoading,
    startLoading,
    stopLoading,

    // 工具方法
    resetUIState,
    getUIStateSummary,
  };
}
