export default {
  dashboard: {
    systemOverview: "系统概览",
    refresh: "刷新",
    refreshing: "刷新中...",
    fetchError: "获取数据失败",
    loading: "加载中...",
    error: "加载失败",

    // 统计卡片
    totalPastes: "文本分享",
    totalFiles: "文件上传",
    totalApiKeys: "API密钥",
    totalS3Configs: "S3配置",
    totalStorageUsed: "存储使用",

    // 缓存监控
    cacheMonitoring: "缓存监控",
    directoryCache: "目录缓存",
    s3UrlCache: "S3URL缓存",
    searchCache: "搜索缓存",
    hitRate: "命中率",
    cacheItems: "缓存项",
    cacheUnavailable: "缓存数据不可用",
    clearAllCache: "清理所有缓存",

    // 存储相关
    storageUsage: "存储使用情况",
    allBuckets: "所有存储桶",
    selectBucket: "选择存储桶",
    usagePercent: "使用率",
    availableStorage: "可用存储",
    usedStorage: "已用存储",
    storageUnits: {
      bytes: "字节",
      kb: "KB",
      mb: "MB",
      gb: "GB",
      tb: "TB",
    },

    // 图表相关
    chartTitle: "过去7天活动统计",
    chartType: {
      bar: "柱状图",
      line: "折线图",
      toggle: "切换图表类型",
    },
    weeklyStats: "本周统计",
    weeklyActivity: "本周活动",
    weeklyPastes: "本周文本",
    weeklyFiles: "本周文件",
    mostActiveDate: "最活跃日期",
    highestDailyActivity: "最高日活跃",
    activityOverview: "活动概览",
    items: "项",
    switchToLineChart: "切换到折线图",
    switchToBarChart: "切换到柱状图",
    dailyActivity: "每日活动",
    noData: "暂无数据",

    // 时间相关
    lastUpdated: "最后更新",
    timeAgo: "{time}前",
    justNow: "刚刚",

    // 存储服务商
    providers: {
      cloudflareR2: "Cloudflare R2",
      backblazeB2: "Backblaze B2",
      awsS3: "AWS S3",
      other: "其他服务商",
    },

    // 状态信息
    status: {
      healthy: "正常",
      warning: "警告",
      error: "错误",
      offline: "离线",
    },

    // 操作按钮
    actions: {
      viewDetails: "查看详情",
      manage: "管理",
      configure: "配置",
      export: "导出数据",
    },

    // 系统信息
    systemVersion: "系统版本",
    serverEnvironment: "服务器环境",
    dataStorage: "数据存储",

    // 存储桶分布
    storageBucketDistribution: "存储桶分布",
    otherStorage: "其他存储",

    // 提示信息
    tips: {
      noApiKeys: "还没有创建API密钥",
      noS3Configs: "还没有配置S3存储",
      noActivity: "最近没有活动",
      lowStorage: "存储空间不足",
    },
  },
};
