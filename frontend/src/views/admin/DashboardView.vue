<script setup>
import { ref, onMounted, computed, watch, onBeforeUnmount } from "vue";
import { api } from "@/api";
import { useI18n } from "vue-i18n";
// 引入Chart.js相关组件
import { Bar, Line } from "vue-chartjs";
import { Chart as ChartJS, CategoryScale, LinearScale, PointElement, LineElement, BarElement, Title, Tooltip, Legend } from "chart.js";

// 注册Chart.js组件
ChartJS.register(CategoryScale, LinearScale, PointElement, LineElement, BarElement, Title, Tooltip, Legend);

// 定义props
const props = defineProps({
  darkMode: {
    type: Boolean,
    required: true,
  },
  permissions: {
    type: Object,
    required: true,
  },
});

const { t } = useI18n();

// 系统统计数据
const statsData = ref({
  totalPastes: 0,
  totalFiles: 0,
  totalApiKeys: 0,
  totalS3Configs: 0,
  totalStorageUsed: 0,
  s3Buckets: [],
  lastWeekPastes: [],
  lastWeekFiles: [],
});

// 缓存统计数据
const cacheStats = ref({
  directory: {
    cacheSize: 0,
    hitRate: 0,
  },
  s3Url: {
    cacheSize: 0,
    hitRate: 0,
  },
  search: {
    cacheSize: 0,
    hitRate: 0,
  },
  error: null,
});

// 缓存卡片折叠状态
const isCacheExpanded = ref(false);

// 清理缓存状态
const isClearingCache = ref(false);

// 版本信息数据
const versionInfo = ref({
  version: "加载中...",
  environment: "加载中...",
  storage: "加载中...",
  uptime: 0,
  error: null,
});

// 当前选中的存储桶
const selectedBucketId = ref(null);

// 加载状态
const isLoading = ref(true);
const error = ref(null);

// 图表显示类型切换
const chartType = ref("bar"); // 'bar' 或 'line'

// 导入统一的时间处理工具
import { formatCurrentTime } from "@/utils/timeUtils.js";

// 图表日期标签
const dateLabels = computed(() => {
  // 获取过去7天的日期
  const dates = [];
  for (let i = 6; i >= 0; i--) {
    const date = new Date();
    date.setDate(date.getDate() - i);
    // 使用 Intl.DateTimeFormat 确保正确的本地化
    dates.push(new Intl.DateTimeFormat("zh-CN", { month: "short", day: "numeric" }).format(date));
  }
  return dates;
});

// Chart.js图表配置
const chartData = computed(() => {
  return {
    labels: dateLabels.value,
    datasets: [
      {
        label: t("admin.dashboard.totalPastes"),
        backgroundColor: props.darkMode ? "rgba(59, 130, 246, 0.7)" : "rgba(37, 99, 235, 0.7)",
        borderColor: props.darkMode ? "rgba(59, 130, 246, 1)" : "rgba(37, 99, 235, 1)",
        borderWidth: 1,
        data: statsData.value.lastWeekPastes,
        borderRadius: 4,
      },
      {
        label: t("admin.dashboard.totalFiles"),
        backgroundColor: props.darkMode ? "rgba(16, 185, 129, 0.7)" : "rgba(5, 150, 105, 0.7)",
        borderColor: props.darkMode ? "rgba(16, 185, 129, 1)" : "rgba(5, 150, 105, 1)",
        borderWidth: 1,
        data: statsData.value.lastWeekFiles,
        borderRadius: 4,
      },
    ],
  };
});

// Chart.js选项配置
const chartOptions = computed(() => {
  return {
    responsive: true,
    maintainAspectRatio: false,
    interaction: {
      mode: "index",
      intersect: false,
    },
    scales: {
      x: {
        grid: {
          color: props.darkMode ? "rgba(255, 255, 255, 0.1)" : "rgba(0, 0, 0, 0.1)",
        },
        ticks: {
          color: props.darkMode ? "rgba(255, 255, 255, 0.7)" : "rgba(0, 0, 0, 0.7)",
        },
      },
      y: {
        beginAtZero: true,
        grid: {
          color: props.darkMode ? "rgba(255, 255, 255, 0.1)" : "rgba(0, 0, 0, 0.1)",
        },
        ticks: {
          precision: 0,
          color: props.darkMode ? "rgba(255, 255, 255, 0.7)" : "rgba(0, 0, 0, 0.7)",
        },
      },
    },
    plugins: {
      tooltip: {
        backgroundColor: props.darkMode ? "rgba(17, 24, 39, 0.9)" : "rgba(255, 255, 255, 0.9)",
        titleColor: props.darkMode ? "rgba(255, 255, 255, 0.9)" : "rgba(0, 0, 0, 0.9)",
        bodyColor: props.darkMode ? "rgba(255, 255, 255, 0.9)" : "rgba(0, 0, 0, 0.9)",
        borderColor: props.darkMode ? "rgba(55, 65, 81, 1)" : "rgba(229, 231, 235, 1)",
        borderWidth: 1,
        padding: 10,
        boxPadding: 5,
        callbacks: {
          title: function (tooltipItems) {
            const date = tooltipItems[0].label;
            return date;
          },
          label: function (context) {
            const label = context.dataset.label || "";
            const value = context.parsed.y;
            return `${label}: ${value} ${t("admin.dashboard.items")}`;
          },
          footer: function (tooltipItems) {
            // 获取当前日期的总活动数
            const dataIndex = tooltipItems[0].dataIndex;
            const totalThisDay = statsData.value.lastWeekPastes[dataIndex] + statsData.value.lastWeekFiles[dataIndex];
            return `${t("admin.dashboard.activityOverview")}: ${totalThisDay} ${t("admin.dashboard.items")}`;
          },
        },
      },
      legend: {
        labels: {
          color: props.darkMode ? "rgba(255, 255, 255, 0.7)" : "rgba(0, 0, 0, 0.7)",
          boxWidth: 12,
          padding: 15,
        },
        position: "top",
      },
    },
  };
});

// 获取当前选择的存储桶数据
const currentBucketData = computed(() => {
  if (!selectedBucketId.value) {
    // 返回总体存储使用情况
    return {
      name: t("admin.dashboard.allBuckets"),
      usedStorage: statsData.value.totalStorageUsed,
      totalStorage: statsData.value.s3Buckets.reduce((total, bucket) => total + bucket.totalStorage, 0),
      usagePercent: calculateTotalUsagePercent(),
    };
  }

  // 返回选中的存储桶数据
  const bucket = statsData.value.s3Buckets.find((b) => b.id === selectedBucketId.value);
  return (
    bucket || {
      name: t("admin.dashboard.allBuckets"),
      usedStorage: statsData.value.totalStorageUsed,
      totalStorage: statsData.value.s3Buckets.reduce((total, bucket) => total + bucket.totalStorage, 0),
      usagePercent: calculateTotalUsagePercent(),
    }
  );
});

// 计算总体存储使用百分比
const calculateTotalUsagePercent = () => {
  const totalUsed = statsData.value.totalStorageUsed;
  const totalAvailable = statsData.value.s3Buckets.reduce((total, bucket) => total + bucket.totalStorage, 0);

  if (!totalAvailable) return 0;
  return Math.min(100, Math.round((totalUsed / totalAvailable) * 100));
};

// 获取指定服务商存储桶的使用占比
const getProviderPercent = (providerType) => {
  if (!statsData.value.s3Buckets || statsData.value.s3Buckets.length === 0) {
    // 如果没有数据，则按服务商平均分配
    const providers = ["Cloudflare R2", "Backblaze B2", "AWS S3", "Other"];
    return Math.floor(100 / providers.length);
  }

  // 计算指定服务商的配置数量（不是存储使用量）
  const providerBuckets = statsData.value.s3Buckets.filter((bucket) => bucket.providerType === providerType);

  // 配置数量占比 = 该服务商配置数量 / 总配置数量
  const configCount = providerBuckets.length;
  const totalConfigs = statsData.value.s3Buckets.length;

  // 计算占比
  return Math.round((configCount / totalConfigs) * 100) || 0;
};

// 获取其他服务商的使用占比
const getOtherProvidersPercent = () => {
  const mainProviders = ["Cloudflare R2", "Backblaze B2", "AWS S3"];

  if (!statsData.value.s3Buckets || statsData.value.s3Buckets.length === 0) {
    return Math.floor(100 / 4); // 平均分配
  }

  // 计算其他服务商的配置数量
  const otherBuckets = statsData.value.s3Buckets.filter((bucket) => !mainProviders.includes(bucket.providerType));

  // 计算占比
  const otherCount = otherBuckets.length;
  const totalConfigs = statsData.value.s3Buckets.length;

  return Math.round((otherCount / totalConfigs) * 100) || 0;
};

// 格式化存储大小
const formatBytes = (bytes, decimals = 2) => {
  if (bytes === 0) return `0 ${t("admin.dashboard.storageUnits.bytes")}`;

  const k = 1024;
  const sizeKeys = ["bytes", "kb", "mb", "gb", "tb"];
  const i = Math.floor(Math.log(bytes) / Math.log(k));

  const sizeKey = sizeKeys[i] || "bytes";
  const sizeUnit = t(`admin.dashboard.storageUnits.${sizeKey}`);

  return parseFloat((bytes / Math.pow(k, i)).toFixed(decimals)) + " " + sizeUnit;
};

// 切换选中的存储桶
const selectBucket = (bucketId) => {
  selectedBucketId.value = bucketId;
};

// 切换图表类型
const toggleChartType = () => {
  chartType.value = chartType.value === "bar" ? "line" : "bar";
};

// 计算总共文本分享数量
const totalWeekPastes = computed(() => {
  return statsData.value.lastWeekPastes.reduce((sum, current) => sum + current, 0);
});

// 计算总共文件上传数量
const totalWeekFiles = computed(() => {
  return statsData.value.lastWeekFiles.reduce((sum, current) => sum + current, 0);
});

// 获取每日最高值
const weeklyMaxValues = computed(() => {
  const combinedData = [];
  for (let i = 0; i < 7; i++) {
    combinedData.push(statsData.value.lastWeekPastes[i] + statsData.value.lastWeekFiles[i]);
  }
  return {
    maxDay: combinedData.indexOf(Math.max(...combinedData)),
    maxValue: Math.max(...combinedData),
  };
});

// 获取缓存统计数据
const fetchCacheStats = async () => {
  try {
    const response = await api.system.getCacheStats();
    if (response.success && response.data) {
      cacheStats.value = {
        directory: {
          cacheSize: response.data.cache?.directory?.cacheSize || 0,
          hitRate: response.data.cache?.directory?.hitRate || 0,
        },
        s3Url: {
          cacheSize: response.data.cache?.s3Url?.cacheSize || 0,
          hitRate: response.data.cache?.s3Url?.hitRate || 0,
        },
        search: {
          cacheSize: response.data.cache?.search?.cacheSize || 0,
          hitRate: response.data.cache?.search?.hitRate || 0,
        },
        error: null,
      };
    }
  } catch (err) {
    console.warn("获取缓存统计失败:", err);
    cacheStats.value.error = "获取缓存数据失败";
  }
};

// 获取版本信息
const fetchVersionInfo = async () => {
  try {
    const response = await api.system.getVersionInfo();
    if (response.success && response.data) {
      versionInfo.value = {
        version: response.data.version || "1.0.0",
        environment: response.data.environment || "Docker",
        storage: response.data.storage || "SQLite",
        uptime: response.data.uptime || 0,
        error: null,
      };
    }
  } catch (err) {
    console.warn("获取版本信息失败:", err);
    versionInfo.value.error = "获取版本信息失败";
  }
};

// 清理所有缓存
const clearAllCache = async () => {
  if (isClearingCache.value) return;

  isClearingCache.value = true;
  try {
    const response = await api.admin.clearCache(); // 无参数 = 清理所有缓存
    if (response.success) {
      // 显示成功消息
      console.log(`缓存清理成功：${response.data.clearedCount} 项`);

      // 重新获取缓存统计
      await fetchCacheStats();

      // 可以添加toast通知
      // toast.success(`缓存清理成功，共清理 ${response.data.clearedCount} 项`);
    } else {
      throw new Error(response.message || "清理缓存失败");
    }
  } catch (err) {
    console.error("清理缓存失败:", err);
    // toast.error("清理缓存失败：" + err.message);
  } finally {
    isClearingCache.value = false;
  }
};

// 从后端获取统计数据
const fetchDashboardStats = async () => {
  isLoading.value = true;
  error.value = null;

  try {
    // 并行获取仪表盘数据、缓存统计和版本信息
    const [dashboardResponse] = await Promise.all([
      api.admin.getDashboardStats(),
      fetchCacheStats(), // 缓存统计失败不影响主要数据
      fetchVersionInfo(), // 版本信息失败不影响主要数据
    ]);

    if (dashboardResponse.success && dashboardResponse.data) {
      statsData.value = dashboardResponse.data;
      // 重置选中的存储桶
      selectedBucketId.value = null;
    } else {
      throw new Error(dashboardResponse.error || t("admin.dashboard.fetchError"));
    }
  } catch (err) {
    console.error("获取控制面板数据失败:", err);
    error.value = t("admin.dashboard.fetchError");
  } finally {
    isLoading.value = false;
  }
};

// 监听暗色模式变化
watch(
  () => props.darkMode,
  () => {
    // 当暗色模式变化时，通过重新计算chartData和chartOptions来更新图表
    chartData.value; // 触发重新计算
    chartOptions.value; // 触发重新计算
  }
);

// 监听语言变化事件
const handleLanguageChange = () => {
  // 语言变化时的处理逻辑
};

// 组件挂载时加载数据和添加事件监听
onMounted(() => {
  fetchDashboardStats();
  window.addEventListener("languageChanged", handleLanguageChange);
});

// 组件卸载时移除事件监听
onBeforeUnmount(() => {
  window.removeEventListener("languageChanged", handleLanguageChange);
});
</script>

<template>
  <!-- 当API密钥用户尝试访问Dashboard时显示权限不足提示 -->
  <div v-if="!permissions.isAdmin" class="p-6 flex-1 flex flex-col items-center justify-center text-center">
    <svg xmlns="http://www.w3.org/2000/svg" class="h-16 w-16 mb-4" :class="darkMode ? 'text-gray-600' : 'text-gray-400'" fill="none" viewBox="0 0 24 24" stroke="currentColor">
      <path
        stroke-linecap="round"
        stroke-linejoin="round"
        stroke-width="2"
        d="M12 15v2m-6 4h12a2 2 0 002-2v-6a2 2 0 00-2-2H6a2 2 0 00-2 2v6a2 2 0 002 2zm10-10V7a4 4 0 00-8 0v4h8z"
      />
    </svg>
    <h3 class="text-xl font-semibold mb-2" :class="darkMode ? 'text-white' : 'text-gray-800'">权限不足</h3>
    <p class="text-base mb-4" :class="darkMode ? 'text-gray-300' : 'text-gray-600'">您没有访问此页面的权限</p>
  </div>

  <!-- 管理员仪表板内容 -->
  <div v-else class="flex flex-col h-full w-full">
    <!-- 标题和刷新按钮 -->
    <div class="flex justify-between items-center mb-4 md:mb-6">
      <h2 class="text-xl font-bold" :class="darkMode ? 'text-white' : 'text-gray-800'">
        {{ t("admin.dashboard.systemOverview") }}
      </h2>
      <button
        @click="fetchDashboardStats"
        class="flex items-center px-3 py-1.5 rounded-md text-sm font-medium transition-colors"
        :class="[darkMode ? 'bg-gray-700 text-gray-100 hover:bg-gray-600' : 'bg-gray-200 text-gray-700 hover:bg-gray-300']"
      >
        <svg class="w-4 h-4 mr-1.5" :class="isLoading ? 'animate-spin' : ''" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor">
          <path
            stroke-linecap="round"
            stroke-linejoin="round"
            stroke-width="2"
            d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"
          />
        </svg>
        {{ isLoading ? t("admin.dashboard.refreshing") : t("admin.dashboard.refresh") }}
      </button>
    </div>

    <!-- 错误提示 -->
    <div v-if="error" class="mb-4 p-3 bg-red-100 text-red-700 rounded-md border border-red-200">
      {{ error }}
    </div>

    <!-- 统计卡片布局 -->
    <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4 mb-6">
      <!-- 文本分享统计 -->
      <div class="p-4 rounded-lg shadow transition-shadow hover:shadow-md" :class="darkMode ? 'bg-gray-700' : 'bg-white'">
        <div class="flex justify-between">
          <div>
            <p class="text-sm font-medium" :class="darkMode ? 'text-gray-300' : 'text-gray-500'">
              {{ t("admin.dashboard.totalPastes") }}
            </p>
            <p class="mt-1 text-2xl font-semibold" :class="darkMode ? 'text-white' : 'text-gray-800'">
              {{ statsData.totalPastes }}
            </p>
          </div>
          <div class="h-12 w-12 rounded-lg flex items-center justify-center" :class="darkMode ? 'bg-blue-500/20 text-blue-400' : 'bg-blue-100 text-blue-600'">
            <svg xmlns="http://www.w3.org/2000/svg" class="h-6 w-6" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path
                stroke-linecap="round"
                stroke-linejoin="round"
                stroke-width="2"
                d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z"
              />
            </svg>
          </div>
        </div>
      </div>

      <!-- 文件上传统计 -->
      <div class="p-4 rounded-lg shadow transition-shadow hover:shadow-md" :class="darkMode ? 'bg-gray-700' : 'bg-white'">
        <div class="flex justify-between">
          <div>
            <p class="text-sm font-medium" :class="darkMode ? 'text-gray-300' : 'text-gray-500'">
              {{ t("admin.dashboard.totalFiles") }}
            </p>
            <p class="mt-1 text-2xl font-semibold" :class="darkMode ? 'text-white' : 'text-gray-800'">
              {{ statsData.totalFiles }}
            </p>
          </div>
          <div class="h-12 w-12 rounded-lg flex items-center justify-center" :class="darkMode ? 'bg-green-500/20 text-green-400' : 'bg-green-100 text-green-600'">
            <svg xmlns="http://www.w3.org/2000/svg" class="h-6 w-6" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3 7v10a2 2 0 002 2h14a2 2 0 002-2V9a2 2 0 00-2-2h-6l-2-2H5a2 2 0 00-2 2z" />
            </svg>
          </div>
        </div>
      </div>

      <!-- API密钥统计 -->
      <div class="p-4 rounded-lg shadow transition-shadow hover:shadow-md" :class="darkMode ? 'bg-gray-700' : 'bg-white'">
        <div class="flex justify-between">
          <div>
            <p class="text-sm font-medium" :class="darkMode ? 'text-gray-300' : 'text-gray-500'">
              {{ t("admin.dashboard.totalApiKeys") }}
            </p>
            <p class="mt-1 text-2xl font-semibold" :class="darkMode ? 'text-white' : 'text-gray-800'">
              {{ statsData.totalApiKeys }}
            </p>
          </div>
          <div class="h-12 w-12 rounded-lg flex items-center justify-center" :class="darkMode ? 'bg-purple-500/20 text-purple-400' : 'bg-purple-100 text-purple-600'">
            <svg xmlns="http://www.w3.org/2000/svg" class="h-6 w-6" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path
                stroke-linecap="round"
                stroke-linejoin="round"
                stroke-width="2"
                d="M15 7a2 2 0 012 2m4 0a6 6 0 01-7.743 5.743L11 17H9v2H7v2H4a1 1 0 01-1-1v-2.586a1 1 0 01.293-.707l5.964-5.964A6 6 0 1121 9z"
              />
            </svg>
          </div>
        </div>
      </div>

      <!-- S3存储配置统计 -->
      <div class="p-4 rounded-lg shadow transition-shadow hover:shadow-md" :class="darkMode ? 'bg-gray-700' : 'bg-white'">
        <div class="flex justify-between">
          <div>
            <p class="text-sm font-medium" :class="darkMode ? 'text-gray-300' : 'text-gray-500'">
              {{ t("admin.dashboard.totalS3Configs") }}
            </p>
            <p class="mt-1 text-2xl font-semibold" :class="darkMode ? 'text-white' : 'text-gray-800'">
              {{ statsData.totalS3Configs }}
            </p>
          </div>
          <div class="h-12 w-12 rounded-lg flex items-center justify-center" :class="darkMode ? 'bg-orange-500/20 text-orange-400' : 'bg-orange-100 text-orange-600'">
            <svg xmlns="http://www.w3.org/2000/svg" class="h-6 w-6" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3 15a4 4 0 004 4h9a5 5 0 10-.1-9.999 5.002 5.002 0 10-9.78 2.096A4.001 4.001 0 003 15z" />
            </svg>
          </div>
        </div>
      </div>
    </div>

    <!-- 存储使用情况 -->
    <div class="mb-6 grid grid-cols-1 lg:grid-cols-2 gap-4">
      <!-- 存储空间使用量 -->
      <div class="p-4 rounded-lg shadow transition-shadow hover:shadow-md" :class="darkMode ? 'bg-gray-700' : 'bg-white'">
        <div class="flex justify-between items-center mb-2">
          <h3 class="text-lg font-semibold" :class="darkMode ? 'text-white' : 'text-gray-800'">
            {{ t("admin.dashboard.storageUsage") }}
          </h3>

          <!-- 存储桶选择器 -->
          <div class="relative">
            <button
              @click="$refs.bucketDropdown.classList.toggle('hidden')"
              class="px-2 py-1 text-xs rounded flex items-center"
              :class="darkMode ? 'bg-gray-600 text-gray-200 hover:bg-gray-500' : 'bg-gray-200 text-gray-700 hover:bg-gray-300'"
            >
              <span>{{ currentBucketData.name }}</span>
              <svg xmlns="http://www.w3.org/2000/svg" class="h-3 w-3 ml-1" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7" />
              </svg>
            </button>

            <!-- 存储桶下拉菜单 -->
            <div
              ref="bucketDropdown"
              class="hidden absolute right-0 mt-1 w-40 rounded-md shadow-lg z-10"
              :class="darkMode ? 'bg-gray-800 border border-gray-700' : 'bg-white border border-gray-200'"
            >
              <div class="py-1">
                <a
                  href="#"
                  @click.prevent="selectBucket(null)"
                  class="block px-4 py-2 text-xs"
                  :class="[
                    !selectedBucketId ? (darkMode ? 'bg-gray-700 text-white' : 'bg-gray-100 text-gray-900') : '',
                    darkMode ? 'text-gray-300 hover:bg-gray-700 hover:text-white' : 'text-gray-700 hover:bg-gray-100 hover:text-gray-900',
                  ]"
                >
                  {{ t("admin.dashboard.allBuckets") }}
                </a>

                <!-- 各个存储桶选项 -->
                <a
                  v-for="bucket in statsData.s3Buckets"
                  :key="bucket.id"
                  href="#"
                  @click.prevent="selectBucket(bucket.id)"
                  class="block px-4 py-2 text-xs"
                  :class="[
                    selectedBucketId === bucket.id ? (darkMode ? 'bg-gray-700 text-white' : 'bg-gray-100 text-gray-900') : '',
                    darkMode ? 'text-gray-300 hover:bg-gray-700 hover:text-white' : 'text-gray-700 hover:bg-gray-100 hover:text-gray-900',
                  ]"
                >
                  {{ bucket.name }}
                </a>
              </div>
            </div>
          </div>
        </div>

        <div class="flex justify-between items-center mb-1.5">
          <span class="text-sm" :class="darkMode ? 'text-gray-300' : 'text-gray-500'">
            {{ formatBytes(currentBucketData.usedStorage) }} / {{ formatBytes(currentBucketData.totalStorage) }}
          </span>
          <span class="text-sm font-medium" :class="darkMode ? 'text-blue-300' : 'text-blue-600'"> {{ currentBucketData.usagePercent }}% </span>
        </div>

        <div class="w-full bg-gray-200 rounded-full h-2.5" :class="darkMode ? 'bg-gray-600' : 'bg-gray-200'">
          <div
            class="h-2.5 rounded-full transition-all duration-500"
            :class="[currentBucketData.usagePercent > 80 ? 'bg-red-500' : currentBucketData.usagePercent > 60 ? 'bg-orange-500' : 'bg-primary-500']"
            :style="{ width: `${currentBucketData.usagePercent}%` }"
          ></div>
        </div>
      </div>

      <!-- 存储桶分布占比 -->
      <div class="p-4 rounded-lg shadow transition-shadow hover:shadow-md" :class="darkMode ? 'bg-gray-700' : 'bg-white'">
        <h3 class="text-lg font-semibold mb-2" :class="darkMode ? 'text-white' : 'text-gray-800'">
          {{ t("admin.dashboard.storageBucketDistribution") }}
        </h3>

        <!-- 简易图表，按服务商类型固定展示 -->
        <div class="grid grid-cols-2 gap-2">
          <div class="flex items-center gap-2">
            <div class="w-3 h-3 rounded-full bg-blue-500"></div>
            <span class="text-sm" :class="darkMode ? 'text-gray-300' : 'text-gray-600'">Cloudflare R2</span>
          </div>
          <div class="text-right text-sm font-medium" :class="darkMode ? 'text-white' : 'text-gray-800'">{{ getProviderPercent("Cloudflare R2") }}%</div>

          <div class="flex items-center gap-2">
            <div class="w-3 h-3 rounded-full bg-green-500"></div>
            <span class="text-sm" :class="darkMode ? 'text-gray-300' : 'text-gray-600'">Backblaze B2</span>
          </div>
          <div class="text-right text-sm font-medium" :class="darkMode ? 'text-white' : 'text-gray-800'">{{ getProviderPercent("Backblaze B2") }}%</div>

          <div class="flex items-center gap-2">
            <div class="w-3 h-3 rounded-full bg-yellow-500"></div>
            <span class="text-sm" :class="darkMode ? 'text-gray-300' : 'text-gray-600'">AWS S3</span>
          </div>
          <div class="text-right text-sm font-medium" :class="darkMode ? 'text-white' : 'text-gray-800'">{{ getProviderPercent("AWS S3") }}%</div>

          <div class="flex items-center gap-2">
            <div class="w-3 h-3 rounded-full bg-purple-500"></div>
            <span class="text-sm" :class="darkMode ? 'text-gray-300' : 'text-gray-600'">{{ t("admin.dashboard.otherStorage") }}</span>
          </div>
          <div class="text-right text-sm font-medium" :class="darkMode ? 'text-white' : 'text-gray-800'">{{ getOtherProvidersPercent() }}%</div>
        </div>
      </div>
    </div>

    <!-- 缓存监控卡片 -->
    <div class="mb-6">
      <div class="p-4 rounded-lg shadow transition-shadow hover:shadow-md" :class="darkMode ? 'bg-gray-700' : 'bg-white'">
        <div class="flex justify-between items-center">
          <div class="flex-1 cursor-pointer" @click="isCacheExpanded = !isCacheExpanded">
            <div class="flex items-center mb-2">
              <div class="h-10 w-10 rounded-lg flex items-center justify-center mr-3" :class="darkMode ? 'bg-indigo-500/20 text-indigo-400' : 'bg-indigo-100 text-indigo-600'">
                <svg xmlns="http://www.w3.org/2000/svg" class="h-5 w-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path
                    stroke-linecap="round"
                    stroke-linejoin="round"
                    stroke-width="2"
                    d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z"
                  />
                </svg>
              </div>
              <div>
                <p class="text-base font-medium" :class="darkMode ? 'text-white' : 'text-gray-800'">
                  {{ t("admin.dashboard.cacheMonitoring") }}
                </p>
                <p v-if="!isCacheExpanded && !cacheStats.error" class="text-sm" :class="darkMode ? 'text-gray-400' : 'text-gray-600'">
                  {{ t("admin.dashboard.directoryCache") }}: {{ Math.round(cacheStats.directory.hitRate * 100) }}% | {{ t("admin.dashboard.s3UrlCache") }}:
                  {{ Math.round(cacheStats.s3Url.hitRate * 100) }}% | {{ t("admin.dashboard.searchCache") }}: {{ Math.round(cacheStats.search.hitRate * 100) }}%
                </p>
                <p v-else-if="!isCacheExpanded && cacheStats.error" class="text-sm text-red-500">
                  {{ t("admin.dashboard.cacheUnavailable") }}
                </p>
              </div>
            </div>
          </div>
          <div class="flex items-center gap-2">
            <!-- 清理缓存按钮 -->
            <button
              @click.stop="clearAllCache"
              :disabled="isClearingCache"
              class="p-2 rounded-lg transition-colors"
              :class="[
                darkMode
                  ? 'bg-red-500/20 text-red-400 hover:bg-red-500/30 disabled:bg-gray-600 disabled:text-gray-400'
                  : 'bg-red-100 text-red-600 hover:bg-red-200 disabled:bg-gray-100 disabled:text-gray-400',
              ]"
              :title="t('admin.dashboard.clearAllCache')"
            >
              <svg v-if="!isClearingCache" xmlns="http://www.w3.org/2000/svg" class="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path
                  stroke-linecap="round"
                  stroke-linejoin="round"
                  stroke-width="2"
                  d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16"
                />
              </svg>
              <svg v-else class="h-4 w-4 animate-spin" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
                <path
                  class="opacity-75"
                  fill="currentColor"
                  d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"
                ></path>
              </svg>
            </button>
            <!-- 展开/收起按钮 -->
            <button @click.stop="isCacheExpanded = !isCacheExpanded" class="p-1">
              <svg
                class="w-5 h-5 transition-transform duration-200"
                :class="[isCacheExpanded ? 'rotate-180' : '', darkMode ? 'text-gray-400' : 'text-gray-500']"
                xmlns="http://www.w3.org/2000/svg"
                fill="none"
                viewBox="0 0 24 24"
                stroke="currentColor"
              >
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7" />
              </svg>
            </button>
          </div>
        </div>

        <!-- 展开的详细信息 -->
        <transition name="slide-down">
          <div v-if="isCacheExpanded" class="mt-4 pt-4 border-t" :class="darkMode ? 'border-gray-600' : 'border-gray-200'">
            <div v-if="!cacheStats.error" class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
              <!-- 目录缓存 -->
              <div class="p-3 rounded-lg" :class="darkMode ? 'bg-gray-600' : 'bg-gray-50'">
                <p class="text-sm font-medium mb-1" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">
                  {{ t("admin.dashboard.directoryCache") }}
                </p>
                <p class="text-lg font-semibold" :class="darkMode ? 'text-white' : 'text-gray-800'">
                  {{ t("admin.dashboard.hitRate") }}: {{ Math.round(cacheStats.directory.hitRate * 100) }}%
                </p>
                <p class="text-sm" :class="darkMode ? 'text-gray-400' : 'text-gray-600'">{{ t("admin.dashboard.cacheItems") }}: {{ cacheStats.directory.cacheSize }}</p>
              </div>

              <!-- S3URL缓存 -->
              <div class="p-3 rounded-lg" :class="darkMode ? 'bg-gray-600' : 'bg-gray-50'">
                <p class="text-sm font-medium mb-1" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">
                  {{ t("admin.dashboard.s3UrlCache") }}
                </p>
                <p class="text-lg font-semibold" :class="darkMode ? 'text-white' : 'text-gray-800'">
                  {{ t("admin.dashboard.hitRate") }}: {{ Math.round(cacheStats.s3Url.hitRate * 100) }}%
                </p>
                <p class="text-sm" :class="darkMode ? 'text-gray-400' : 'text-gray-600'">{{ t("admin.dashboard.cacheItems") }}: {{ cacheStats.s3Url.cacheSize }}</p>
              </div>

              <!-- 搜索缓存 -->
              <div class="p-3 rounded-lg" :class="darkMode ? 'bg-gray-600' : 'bg-gray-50'">
                <p class="text-sm font-medium mb-1" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">
                  {{ t("admin.dashboard.searchCache") }}
                </p>
                <p class="text-lg font-semibold" :class="darkMode ? 'text-white' : 'text-gray-800'">
                  {{ t("admin.dashboard.hitRate") }}: {{ Math.round(cacheStats.search.hitRate * 100) }}%
                </p>
                <p class="text-sm" :class="darkMode ? 'text-gray-400' : 'text-gray-600'">{{ t("admin.dashboard.cacheItems") }}: {{ cacheStats.search.cacheSize }}</p>
              </div>
            </div>
            <div v-else class="text-center py-4">
              <p class="text-red-500">{{ cacheStats.error }}</p>
            </div>
          </div>
        </transition>
      </div>
    </div>

    <!-- 活动趋势图表 -->
    <div class="mb-5">
      <div class="p-3 rounded-lg shadow transition-shadow hover:shadow-md" :class="darkMode ? 'bg-gray-700' : 'bg-white'">
        <div class="flex items-center justify-between mb-3">
          <h3 class="text-base font-semibold" :class="darkMode ? 'text-white' : 'text-gray-800'">
            {{ t("admin.dashboard.weeklyActivity") }}
          </h3>

          <div class="flex items-center space-x-2">
            <!-- 图表类型切换按钮 -->
            <button
              @click="toggleChartType"
              class="px-2 py-1 rounded-md text-xs transition-colors flex items-center"
              :class="darkMode ? 'bg-gray-600 hover:bg-gray-500 text-white' : 'bg-gray-200 hover:bg-gray-300 text-gray-700'"
            >
              <svg v-if="chartType === 'bar'" xmlns="http://www.w3.org/2000/svg" class="h-3.5 w-3.5 mr-1" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M7 12l3-3 3 3 4-4M8 21l4-4 4 4M3 4h18M4 4h16v12a1 1 0 01-1 1H5a1 1 0 01-1-1V4z" />
              </svg>
              <svg v-else xmlns="http://www.w3.org/2000/svg" class="h-3.5 w-3.5 mr-1" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path
                  stroke-linecap="round"
                  stroke-linejoin="round"
                  stroke-width="2"
                  d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z"
                />
              </svg>
              {{ chartType === "bar" ? t("admin.dashboard.switchToLineChart") : t("admin.dashboard.switchToBarChart") }}
            </button>
          </div>
        </div>

        <!-- 活动统计概览卡片 -->
        <div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-2 mb-4">
          <div class="p-2 rounded-lg bg-opacity-10" :class="darkMode ? 'bg-blue-500' : 'bg-blue-100'">
            <div class="flex items-center">
              <div class="w-7 h-7 rounded-full flex items-center justify-center mr-2" :class="darkMode ? 'bg-blue-500/20 text-blue-400' : 'bg-blue-100 text-blue-600'">
                <svg xmlns="http://www.w3.org/2000/svg" class="h-3.5 w-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path
                    stroke-linecap="round"
                    stroke-linejoin="round"
                    stroke-width="2"
                    d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z"
                  />
                </svg>
              </div>
              <div>
                <p class="text-xs font-medium" :class="darkMode ? 'text-blue-200' : 'text-blue-700'">
                  {{ t("admin.dashboard.weeklyPastes") }}
                </p>
                <p class="text-lg font-semibold" :class="darkMode ? 'text-white' : 'text-gray-800'">
                  {{ totalWeekPastes }}
                </p>
              </div>
            </div>
          </div>

          <div class="p-2 rounded-lg bg-opacity-10" :class="darkMode ? 'bg-green-500' : 'bg-green-100'">
            <div class="flex items-center">
              <div class="w-7 h-7 rounded-full flex items-center justify-center mr-2" :class="darkMode ? 'bg-green-500/20 text-green-400' : 'bg-green-100 text-green-600'">
                <svg xmlns="http://www.w3.org/2000/svg" class="h-3.5 w-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path
                    stroke-linecap="round"
                    stroke-linejoin="round"
                    stroke-width="2"
                    d="M7 21h10a2 2 0 002-2V9.414a1 1 0 00-.293-.707l-5.414-5.414A1 1 0 0012.586 3H7a2 2 0 00-2 2v14a2 2 0 002 2z"
                  />
                </svg>
              </div>
              <div>
                <p class="text-xs font-medium" :class="darkMode ? 'text-green-200' : 'text-green-700'">
                  {{ t("admin.dashboard.weeklyFiles") }}
                </p>
                <p class="text-lg font-semibold" :class="darkMode ? 'text-white' : 'text-gray-800'">
                  {{ totalWeekFiles }}
                </p>
              </div>
            </div>
          </div>

          <div class="p-2 rounded-lg bg-opacity-10" :class="darkMode ? 'bg-purple-500' : 'bg-purple-100'">
            <div class="flex items-center">
              <div class="w-7 h-7 rounded-full flex items-center justify-center mr-2" :class="darkMode ? 'bg-purple-500/20 text-purple-400' : 'bg-purple-100 text-purple-600'">
                <svg xmlns="http://www.w3.org/2000/svg" class="h-3.5 w-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path
                    stroke-linecap="round"
                    stroke-linejoin="round"
                    stroke-width="2"
                    d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z"
                  />
                </svg>
              </div>
              <div>
                <p class="text-xs font-medium" :class="darkMode ? 'text-purple-200' : 'text-purple-700'">
                  {{ t("admin.dashboard.mostActiveDate") }}
                </p>
                <p class="text-lg font-semibold" :class="darkMode ? 'text-white' : 'text-gray-800'">
                  {{ dateLabels[weeklyMaxValues.maxDay] }}
                </p>
              </div>
            </div>
          </div>

          <div class="p-2 rounded-lg bg-opacity-10" :class="darkMode ? 'bg-yellow-500' : 'bg-yellow-100'">
            <div class="flex items-center">
              <div class="w-7 h-7 rounded-full flex items-center justify-center mr-2" :class="darkMode ? 'bg-yellow-500/20 text-yellow-400' : 'bg-yellow-100 text-yellow-600'">
                <svg xmlns="http://www.w3.org/2000/svg" class="h-3.5 w-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M11 3.055A9.001 9.001 0 1020.945 13H11V3.055z" />
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M20.488 9H15V3.512A9.025 9.025 0 0120.488 9z" />
                </svg>
              </div>
              <div>
                <p class="text-xs font-medium" :class="darkMode ? 'text-yellow-200' : 'text-yellow-700'">
                  {{ t("admin.dashboard.highestDailyActivity") }}
                </p>
                <p class="text-lg font-semibold" :class="darkMode ? 'text-white' : 'text-gray-800'">{{ weeklyMaxValues.maxValue }} {{ t("admin.dashboard.items") }}</p>
              </div>
            </div>
          </div>
        </div>

        <!-- Chart.js图表 -->
        <div class="h-60 sm:h-68 md:h-72">
          <Bar v-if="chartType === 'bar'" :data="chartData" :options="chartOptions" />
          <Line v-else :data="chartData" :options="chartOptions" />
        </div>
      </div>
    </div>

    <!-- 系统信息卡片 -->
    <div class="flex flex-col sm:flex-row gap-4">
      <!-- 版本信息 -->
      <div class="flex-1 p-3 rounded-lg shadow transition-shadow hover:shadow-md" :class="darkMode ? 'bg-gray-700' : 'bg-white'">
        <div class="flex items-center mb-2">
          <div class="w-6 h-6 rounded-full flex items-center justify-center mr-2" :class="darkMode ? 'bg-purple-500/20 text-purple-400' : 'bg-purple-100 text-purple-600'">
            <svg xmlns="http://www.w3.org/2000/svg" class="h-3.5 w-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path
                stroke-linecap="round"
                stroke-linejoin="round"
                stroke-width="2"
                d="M7 4V2a1 1 0 011-1h8a1 1 0 011 1v2m0 0V3a1 1 0 011 1v1M7 4V3a1 1 0 011-1v1m8 0V3a1 1 0 00-1-1v1m-8 0h8m-8 0v16a1 1 0 001 1h6a1 1 0 001-1V4"
              />
            </svg>
          </div>
          <h3 class="text-sm font-medium" :class="darkMode ? 'text-gray-300' : 'text-gray-500'">
            {{ t("admin.dashboard.systemVersion") }}
          </h3>
        </div>
        <p v-if="!versionInfo.error" class="text-sm ml-8" :class="darkMode ? 'text-gray-400' : 'text-gray-600'">v{{ versionInfo.version }}</p>
        <p v-else class="text-sm ml-8 text-red-500">{{ versionInfo.error }}</p>
      </div>

      <!-- 服务器信息 -->
      <div class="flex-1 p-3 rounded-lg shadow transition-shadow hover:shadow-md" :class="darkMode ? 'bg-gray-700' : 'bg-white'">
        <div class="flex items-center mb-2">
          <div class="w-6 h-6 rounded-full flex items-center justify-center mr-2" :class="darkMode ? 'bg-blue-500/20 text-blue-400' : 'bg-blue-100 text-blue-600'">
            <svg xmlns="http://www.w3.org/2000/svg" class="h-3.5 w-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path
                stroke-linecap="round"
                stroke-linejoin="round"
                stroke-width="2"
                d="M5 12h14M5 12a2 2 0 01-2-2V6a2 2 0 012-2h14a2 2 0 012 2v4a2 2 0 01-2 2M5 12a2 2 0 00-2 2v4a2 2 0 002 2h14a2 2 0 002-2v-4a2 2 0 00-2-2m-2-4h.01M17 16h.01"
              />
            </svg>
          </div>
          <h3 class="text-sm font-medium" :class="darkMode ? 'text-gray-300' : 'text-gray-500'">
            {{ t("admin.dashboard.serverEnvironment") }}
          </h3>
        </div>
        <p v-if="!versionInfo.error" class="text-sm ml-8" :class="darkMode ? 'text-gray-400' : 'text-gray-600'">
          {{ versionInfo.environment }}
        </p>
        <p v-else class="text-sm ml-8 text-red-500">{{ versionInfo.error }}</p>
      </div>

      <!-- 数据库信息 -->
      <div class="flex-1 p-3 rounded-lg shadow transition-shadow hover:shadow-md" :class="darkMode ? 'bg-gray-700' : 'bg-white'">
        <div class="flex items-center mb-2">
          <div class="w-6 h-6 rounded-full flex items-center justify-center mr-2" :class="darkMode ? 'bg-green-500/20 text-green-400' : 'bg-green-100 text-green-600'">
            <svg xmlns="http://www.w3.org/2000/svg" class="h-3.5 w-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path
                stroke-linecap="round"
                stroke-linejoin="round"
                stroke-width="2"
                d="M4 7v10c0 2.21 3.582 4 8 4s8-1.79 8-4V7M4 7c0 2.21 3.582 4 8 4s8-1.79 8-4M4 7c0-2.21 3.582-4 8-4s8 1.79 8 4m0 5c0 2.21-3.582 4-8 4s-8-1.79-8-4"
              />
            </svg>
          </div>
          <h3 class="text-sm font-medium" :class="darkMode ? 'text-gray-300' : 'text-gray-500'">
            {{ t("admin.dashboard.dataStorage") }}
          </h3>
        </div>
        <p v-if="!versionInfo.error" class="text-sm ml-8" :class="darkMode ? 'text-gray-400' : 'text-gray-600'">
          {{ versionInfo.storage }}
        </p>
        <p v-else class="text-sm ml-8 text-red-500">{{ versionInfo.error }}</p>
      </div>

      <!-- 上次更新时间 -->
      <div class="flex-1 p-3 rounded-lg shadow transition-shadow hover:shadow-md" :class="darkMode ? 'bg-gray-700' : 'bg-white'">
        <div class="flex items-center mb-2">
          <div class="w-6 h-6 rounded-full flex items-center justify-center mr-2" :class="darkMode ? 'bg-purple-500/20 text-purple-400' : 'bg-purple-100 text-purple-600'">
            <svg xmlns="http://www.w3.org/2000/svg" class="h-3.5 w-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z" />
            </svg>
          </div>
          <h3 class="text-sm font-medium" :class="darkMode ? 'text-gray-300' : 'text-gray-500'">
            {{ t("admin.dashboard.lastUpdated") }}
          </h3>
        </div>
        <p class="text-sm ml-8" :class="darkMode ? 'text-gray-400' : 'text-gray-600'">
          {{ formatCurrentTime() }}
        </p>
      </div>
    </div>
  </div>
</template>

<style scoped>
/* 添加过渡动画效果 */
.transition-all {
  transition-property: all;
  transition-timing-function: cubic-bezier(0.4, 0, 0.2, 1);
  transition-duration: 300ms;
}

/* 缓存卡片折叠动画 */
.slide-down-enter-active,
.slide-down-leave-active {
  transition: all 0.3s ease;
  overflow: hidden;
}

.slide-down-enter-from,
.slide-down-leave-to {
  max-height: 0;
  opacity: 0;
  transform: translateY(-10px);
}

.slide-down-enter-to,
.slide-down-leave-from {
  max-height: 200px;
  opacity: 1;
  transform: translateY(0);
}

/* 悬停效果 */
.hover\:shadow-md:hover {
  box-shadow: 0 4px 6px -1px rgb(0 0 0 / 0.1), 0 2px 4px -2px rgb(0 0 0 / 0.1);
}
</style>
