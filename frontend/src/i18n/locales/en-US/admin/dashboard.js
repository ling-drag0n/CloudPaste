export default {
  dashboard: {
    systemOverview: "System Overview",
    refresh: "Refresh",
    refreshing: "Refreshing...",
    fetchError: "Failed to fetch data",
    loading: "Loading...",
    error: "Loading failed",

    // Statistics cards
    totalPastes: "Text Shares",
    totalFiles: "File Uploads",
    totalApiKeys: "API Keys",
    totalS3Configs: "S3 Configs",
    totalStorageUsed: "Storage Used",

    // Cache monitoring
    cacheMonitoring: "Cache Monitoring",
    directoryCache: "Directory Cache",
    s3UrlCache: "S3 URL Cache",
    searchCache: "Search Cache",
    hitRate: "Hit Rate",
    cacheItems: "Cache Items",
    cacheUnavailable: "Cache data unavailable",
    clearAllCache: "Clear All Cache",

    // Storage related
    storageUsage: "Storage Usage",
    allBuckets: "All Buckets",
    selectBucket: "Select Bucket",
    usagePercent: "Usage",
    availableStorage: "Available Storage",
    usedStorage: "Used Storage",
    storageUnits: {
      bytes: "Bytes",
      kb: "KB",
      mb: "MB",
      gb: "GB",
      tb: "TB",
    },

    // Chart related
    chartTitle: "Activity Statistics (Last 7 Days)",
    chartType: {
      bar: "Bar Chart",
      line: "Line Chart",
      toggle: "Toggle Chart Type",
    },
    weeklyStats: "Weekly Stats",
    weeklyActivity: "Weekly Activity",
    weeklyPastes: "Weekly Texts",
    weeklyFiles: "Weekly Files",
    mostActiveDate: "Most Active Date",
    highestDailyActivity: "Highest Daily Activity",
    activityOverview: "Activity Overview",
    items: "items",
    switchToLineChart: "Switch to Line Chart",
    switchToBarChart: "Switch to Bar Chart",
    dailyActivity: "Daily Activity",
    noData: "No data available",

    // Time related
    lastUpdated: "Last Updated",
    timeAgo: "{time} ago",
    justNow: "Just now",

    // Storage providers
    providers: {
      cloudflareR2: "Cloudflare R2",
      backblazeB2: "Backblaze B2",
      awsS3: "AWS S3",
      other: "Other Providers",
    },

    // Status information
    status: {
      healthy: "Healthy",
      warning: "Warning",
      error: "Error",
      offline: "Offline",
    },

    // Action buttons
    actions: {
      viewDetails: "View Details",
      manage: "Manage",
      configure: "Configure",
      export: "Export Data",
    },

    // System information
    systemVersion: "System Version",
    serverEnvironment: "Server Environment",
    dataStorage: "Data Storage",

    // Storage bucket distribution
    storageBucketDistribution: "Storage Bucket Distribution",
    otherStorage: "Other Storage",

    // Tips and messages
    tips: {
      noApiKeys: "No API keys created yet",
      noS3Configs: "No S3 storage configured yet",
      noActivity: "No recent activity",
      lowStorage: "Low storage space",
    },
  },
};
