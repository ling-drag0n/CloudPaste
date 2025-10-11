import { ref, reactive } from "vue";

/**
 * 管理功能基础composable
 * 提供通用的状态管理、选择逻辑、消息处理等
 * @param {string} pageKey - 页面标识符，用于区分不同页面的分页设置
 */
export function useAdminBase(pageKey = "default") {
  // 基础状态管理
  const loading = ref(false);
  const error = ref("");
  const successMessage = ref("");
  const selectedItems = ref([]);
  const lastRefreshTime = ref("");

  // 可选的每页数量选项
  const pageSizeOptions = [10, 20, 30, 50, 100];

  // 从localStorage获取用户偏好的每页数量，按页面区分，默认为20
  const getDefaultPageSize = () => {
    try {
      const saved = localStorage.getItem("admin-page-size");
      if (saved) {
        const pageSizes = JSON.parse(saved);
        return pageSizes[pageKey] || 20;
      }
    } catch (error) {
      // 可能是旧格式（直接存储数字），尝试迁移
      const oldValue = parseInt(saved) || 20;
      console.log(`迁移旧的分页设置: ${saved} -> {default: ${oldValue}}`);

      // 迁移到新格式
      const newFormat = { default: oldValue };
      localStorage.setItem("admin-page-size", JSON.stringify(newFormat));

      return pageKey === "default" ? oldValue : 20;
    }
    return 20;
  };

  // 统一的分页状态（支持两种模式的转换）
  const pagination = reactive({
    // 通用字段
    limit: getDefaultPageSize(),
    total: 0,

    // page模式字段
    page: 1,
    totalPages: 0,

    // offset模式字段
    offset: 0,
    hasMore: false,
  });

  /**
   * 更新分页信息
   * @param {Object} data - 后端返回的数据
   * @param {string} mode - 分页模式 'page' 或 'offset'
   */
  const updatePagination = (data, mode = "page") => {
    if (mode === "page") {
      // page模式更新
      pagination.total = data.total || 0;
      pagination.totalPages = Math.ceil(pagination.total / pagination.limit);
    } else {
      // offset模式更新
      pagination.total = data.total || 0;
      pagination.hasMore = data.hasMore !== undefined ? data.hasMore : pagination.offset + pagination.limit < pagination.total;

      // 同时更新limit和offset，确保与后端返回的数据一致
      if (data.limit !== undefined) {
        pagination.limit = data.limit;
      }
      if (data.offset !== undefined) {
        pagination.offset = data.offset;
      }
    }
  };

  /**
   * 处理分页变化
   * @param {number} value - 页码或偏移量
   * @param {string} mode - 分页模式
   */
  const handlePaginationChange = (value, mode = "page") => {
    if (mode === "page") {
      pagination.page = value;
      pagination.offset = (value - 1) * pagination.limit;
    } else {
      pagination.offset = value;
      pagination.page = Math.floor(value / pagination.limit) + 1;
    }
  };

  /**
   * 重置分页到第一页
   */
  const resetPagination = () => {
    pagination.page = 1;
    pagination.offset = 0;
  };

  /**
   * 更改每页数量
   * @param {number} newLimit - 新的每页数量
   */
  const changePageSize = (newLimit) => {
    try {
      // 获取现有的分页设置
      const saved = localStorage.getItem("admin-page-size");
      const pageSizes = saved ? JSON.parse(saved) : {};

      // 更新当前页面的分页设置
      pageSizes[pageKey] = newLimit;

      // 保存到localStorage
      localStorage.setItem("admin-page-size", JSON.stringify(pageSizes));
    } catch (error) {
      console.warn("保存分页设置失败:", error);
    }

    // 更新分页状态
    pagination.limit = newLimit;

    // 重置到第一页
    resetPagination();
  };

  /**
   * 切换单个项目的选择状态
   * @param {string|number} id - 项目ID
   */
  const toggleSelectItem = (id) => {
    const index = selectedItems.value.indexOf(id);
    if (index > -1) {
      selectedItems.value.splice(index, 1);
    } else {
      selectedItems.value.push(id);
    }
  };

  /**
   * 切换全选状态
   * @param {Array} allItems - 所有项目列表
   * @param {string} idField - ID字段名，默认为'id'
   */
  const toggleSelectAll = (allItems, idField = "id") => {
    if (selectedItems.value.length === allItems.length) {
      // 当前全选，取消全选
      selectedItems.value = [];
    } else {
      // 当前非全选，执行全选
      selectedItems.value = allItems.map((item) => item[idField]);
    }
  };

  /**
   * 清空选择
   */
  const clearSelection = () => {
    selectedItems.value = [];
  };

  /**
   * 显示成功消息
   * @param {string} message - 消息内容
   * @param {number} duration - 显示时长（毫秒）
   */
  const showSuccess = (message, duration = 4000) => {
    successMessage.value = message;
    setTimeout(() => {
      successMessage.value = "";
    }, duration);
  };

  /**
   * 显示错误消息
   * @param {string} message - 错误消息
   */
  const showError = (message) => {
    error.value = message;
  };

  /**
   * 清空所有消息
   */
  const clearMessages = () => {
    error.value = "";
    successMessage.value = "";
  };

  /**
   * 更新最后刷新时间
   */
  const updateLastRefreshTime = () => {
    const now = new Date();
    const timeString = now.toLocaleTimeString("zh-CN", {
      hour12: false,
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
    });
    lastRefreshTime.value = `最后刷新: ${timeString}`;
  };

  /**
   * 通用的加载包装器
   * @param {Function} asyncFn - 异步函数
   * @param {Object} options - 选项
   */
  const withLoading = async (asyncFn, options = {}) => {
    const { clearMessagesFirst = true, showErrorOnCatch = true } = options;

    loading.value = true;
    if (clearMessagesFirst) {
      clearMessages();
    }

    try {
      const result = await asyncFn();
      return result;
    } catch (err) {
      console.error("操作失败:", err);
      if (showErrorOnCatch) {
        showError(err.message || "操作失败，请重试");
      }
      throw err;
    } finally {
      loading.value = false;
    }
  };

  return {
    // 状态
    loading,
    error,
    successMessage,
    selectedItems,
    lastRefreshTime,
    pagination,
    pageSizeOptions,

    // 分页方法
    updatePagination,
    handlePaginationChange,
    resetPagination,
    changePageSize,

    // 选择方法
    toggleSelectItem,
    toggleSelectAll,
    clearSelection,

    // 消息方法
    showSuccess,
    showError,
    clearMessages,

    // 工具方法
    updateLastRefreshTime,
    withLoading,
  };
}
