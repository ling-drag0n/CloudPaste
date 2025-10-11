/**
 * 文件搜索组合式函数
 */
import { ref, computed, watch } from "vue";
import { useAuthStore } from "@/stores/authStore.js";
import { api } from "@/api";
import { useI18n } from "vue-i18n";

export function useFileSearch() {
  const authStore = useAuthStore();
  const { t } = useI18n();

  // API调用函数 - 使用统一API，自动根据认证信息处理用户类型
  const searchApi = computed(() => {
    return api.fs.searchFiles;
  });

  // 状态管理
  const searchQuery = ref("");
  const searchResults = ref([]);
  const isSearching = ref(false);
  const searchError = ref(null);
  const hasPerformedSearch = ref(false); // 新增：是否已执行过搜索
  const searchParams = ref({
    scope: "global", // 'global', 'mount', 'directory'
    mountId: "",
    path: "",
    limit: 50,
    offset: 0,
  });

  // 搜索历史
  const searchHistory = ref([]);
  const maxHistoryItems = 10;

  // 计算属性 - 复用useFilePreview的计算属性模式
  const hasSearchQuery = computed(() => searchQuery.value.trim().length >= 2);
  const hasSearchResults = computed(() => searchResults.value.length > 0);
  const searchResultsCount = computed(() => searchResults.value.length);
  const canSearch = computed(() => hasSearchQuery.value && !isSearching.value);

  // 搜索结果统计
  const searchStats = ref({
    total: 0,
    hasMore: false,
    mountsSearched: 0,
  });

  /**
   * 执行搜索
   * @param {string} query - 搜索查询字符串
   * @param {Object} options - 搜索选项
   */
  const performSearch = async (query = null, options = {}) => {
    try {
      const searchTerm = query || searchQuery.value;

      if (!searchTerm || searchTerm.trim().length < 2) {
        searchError.value = t("search.errors.queryTooShort");
        return;
      }

      isSearching.value = true;
      searchError.value = null;

      // 合并搜索参数
      const finalSearchParams = {
        ...searchParams.value,
        ...options,
      };

      console.log("开始搜索文件:", searchTerm, finalSearchParams);

      // 标记已执行搜索
      hasPerformedSearch.value = true;

      // 调用搜索API
      const response = await searchApi.value(searchTerm, finalSearchParams);

      if (response.success) {
        const searchData = response.data;
        searchResults.value = searchData.results || [];
        searchStats.value = {
          total: searchData.total || 0,
          hasMore: searchData.hasMore || false,
          mountsSearched: searchData.mountsSearched || 0,
        };

        // 添加到搜索历史
        addToSearchHistory(searchTerm);

        console.log("搜索完成:", {
          query: searchTerm,
          results: searchResults.value.length,
          total: searchStats.value.total,
        });
      } else {
        throw new Error(response.message || t("search.errors.searchFailed"));
      }
    } catch (err) {
      console.error("搜索失败:", err);
      searchError.value = err.message || t("search.errors.searchFailed");
      searchResults.value = [];
      searchStats.value = { total: 0, hasMore: false, mountsSearched: 0 };
    } finally {
      isSearching.value = false;
    }
  };

  /**
   * 加载更多搜索结果（分页）
   */
  const loadMoreResults = async () => {
    if (!searchStats.value.hasMore || isSearching.value) {
      return;
    }

    try {
      isSearching.value = true;
      searchError.value = null;

      // 更新偏移量
      const newOffset = searchResults.value.length;
      const paginationParams = {
        ...searchParams.value,
        offset: newOffset,
      };

      console.log("加载更多搜索结果:", { offset: newOffset });

      const response = await searchApi.value(searchQuery.value, paginationParams);

      if (response.success) {
        const searchData = response.data;
        // 追加新结果到现有结果
        searchResults.value.push(...(searchData.results || []));
        searchStats.value = {
          total: searchData.total || 0,
          hasMore: searchData.hasMore || false,
          mountsSearched: searchData.mountsSearched || 0,
        };

        console.log("加载更多结果完成:", {
          newResults: searchData.results?.length || 0,
          totalResults: searchResults.value.length,
        });
      } else {
        throw new Error(response.message || t("search.errors.loadMoreFailed"));
      }
    } catch (err) {
      console.error("加载更多结果失败:", err);
      searchError.value = err.message || t("search.errors.loadMoreFailed");
    } finally {
      isSearching.value = false;
    }
  };

  /**
   * 清除搜索结果
   */
  const clearSearch = () => {
    searchQuery.value = "";
    searchResults.value = [];
    searchError.value = null;
    hasPerformedSearch.value = false; // 重置搜索执行状态
    searchStats.value = { total: 0, hasMore: false, mountsSearched: 0 };
    // 重置搜索参数的偏移量
    searchParams.value.offset = 0;
    console.log("搜索结果已清除");
  };

  /**
   * 更新搜索参数
   * @param {Object} params - 新的搜索参数
   */
  const updateSearchParams = (params) => {
    searchParams.value = {
      ...searchParams.value,
      ...params,
      offset: 0, // 重置偏移量
    };
    console.log("搜索参数已更新:", searchParams.value);
  };

  /**
   * 设置搜索范围
   * @param {string} scope - 搜索范围 ('global', 'mount', 'directory')
   * @param {Object} options - 额外选项
   */
  const setSearchScope = (scope, options = {}) => {
    const newParams = {
      scope,
      mountId: options.mountId || "",
      path: options.path || "",
      offset: 0,
    };
    updateSearchParams(newParams);
  };

  /**
   * 添加到搜索历史
   * @param {string} query - 搜索查询
   */
  const addToSearchHistory = (query) => {
    const trimmedQuery = query.trim();
    if (!trimmedQuery) return;

    // 移除重复项
    const filteredHistory = searchHistory.value.filter((item) => item !== trimmedQuery);

    // 添加到开头
    searchHistory.value = [trimmedQuery, ...filteredHistory].slice(0, maxHistoryItems);

    // 保存到本地存储
    try {
      localStorage.setItem("fileSearchHistory", JSON.stringify(searchHistory.value));
    } catch (error) {
      console.warn("保存搜索历史失败:", error);
    }
  };

  /**
   * 加载搜索历史
   */
  const loadSearchHistory = () => {
    try {
      const saved = localStorage.getItem("fileSearchHistory");
      if (saved) {
        searchHistory.value = JSON.parse(saved);
      }
    } catch (error) {
      console.warn("加载搜索历史失败:", error);
      searchHistory.value = [];
    }
  };

  /**
   * 清除搜索历史
   */
  const clearSearchHistory = () => {
    searchHistory.value = [];
    try {
      localStorage.removeItem("fileSearchHistory");
    } catch (error) {
      console.warn("清除搜索历史失败:", error);
    }
  };

  /**
   * 清除搜索错误
   */
  const clearSearchError = () => {
    searchError.value = null;
  };

  /**
   * 获取搜索错误信息
   * @returns {string|null} 错误信息
   */
  const getSearchError = () => {
    return searchError.value;
  };

  // 初始化时加载搜索历史
  loadSearchHistory();

  // 监听认证状态变化，清除搜索结果
  watch(
    () => authStore.isAuthenticated,
    (newValue) => {
      if (!newValue) {
        clearSearch();
      }
    }
  );

  return {
    // 状态
    searchQuery,
    searchResults,
    isSearching,
    searchError,
    searchParams,
    searchHistory,
    searchStats,
    hasPerformedSearch,

    // 计算属性
    hasSearchQuery,
    hasSearchResults,
    searchResultsCount,
    canSearch,

    // 方法
    performSearch,
    loadMoreResults,
    clearSearch,
    updateSearchParams,
    setSearchScope,
    addToSearchHistory,
    loadSearchHistory,
    clearSearchHistory,
    clearSearchError,
    getSearchError,
  };
}
