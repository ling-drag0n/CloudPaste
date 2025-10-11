export default {
  search: {
    title: "File Search",
    placeholder: "Search files...",

    scope: {
      global: "Global Search",
      mount: "Single Mount Search",
      directory: "Current & Subdirectories Search",
    },

    results: {
      foundInMounts: "Found {count} files in {mounts} mount points",
      loadMore: "Load More",
      loadingMore: "Loading more results...",
      noResults: "No matching files found",
      noResultsHint: "Try adjusting your search criteria or using different keywords",

      item: {
        copyPath: "Copy Path",
      },
    },

    status: {
      idle: "Enter search keywords",
      searching: "Searching...",
      failed: "Search Failed",
    },

    errors: {
      queryTooShort: "Search keywords must be at least 2 characters",
      searchFailed: "Search failed, please try again later",
      loadMoreFailed: "Failed to load more results",
    },

    history: {
      recent: "Recent Searches",
      clear: "Clear History",
    },

    tips: {
      keywordTips: "Supports fuzzy file name search",
    },
  },
};
