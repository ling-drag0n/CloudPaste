export default {
  search: {
    title: "文件搜索",
    placeholder: "搜索文件名...",

    scope: {
      global: "全局搜索",
      mount: "单盘搜索",
      directory: "当前及子目录搜索",
    },

    results: {
      foundInMounts: "在 {mounts} 个挂载点中找到 {count} 个文件",
      loadMore: "加载更多",
      loadingMore: "加载更多结果...",
      noResults: "未找到匹配的文件",
      noResultsHint: "请尝试调整搜索条件或使用不同的关键词",

      item: {
        copyPath: "复制路径",
      },
    },

    status: {
      idle: "请输入搜索关键词",
      searching: "正在搜索...",
      failed: "搜索失败",
    },

    errors: {
      queryTooShort: "搜索关键词至少需要2个字符",
      searchFailed: "搜索失败，请稍后重试",
      loadMoreFailed: "加载更多结果失败",
    },

    history: {
      recent: "最近搜索",
      clear: "清除历史",
    },

    tips: {
      keywordTips: "支持文件名模糊搜索",
    },
  },
};
