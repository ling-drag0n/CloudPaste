<script setup>
import { computed } from "vue";
// 预览弹窗

// 组件接收的属性定义
const props = defineProps({
  darkMode: {
    type: Boolean,
    required: true,
  },
  showPreview: {
    type: Boolean,
    required: true,
  },
  paste: {
    type: Object,
    default: null,
  },
  copiedTexts: {
    type: Object,
    default: () => ({}),
  },
});

// 定义组件要触发的事件
const emit = defineEmits(["close", "view-paste", "copy-link"]);

// 导入统一的时间处理工具
import { formatDateTime, formatRelativeTime as formatRelativeTimeUtil, formatExpiry as formatExpiryUtil } from "@/utils/timeUtils.js";

/**
 * 格式化日期为本地日期时间字符串
 * @param {string} dateString - UTC 时间字符串
 * @returns {string} 格式化后的本地时间字符串
 */
const formatDate = (dateString) => {
  return formatDateTime(dateString);
};

/**
 * 格式化相对时间（如：3天后过期）
 * @param {string} dateString - UTC 时间字符串
 * @returns {string} 相对时间描述
 */
const formatRelativeTime = (dateString) => {
  if (!dateString) return "";

  const relativeTime = formatRelativeTimeUtil(dateString);

  // 为过期场景添加特殊处理
  if (relativeTime.includes("前")) {
    return "已过期";
  } else if (relativeTime === "即将") {
    return "即将过期";
  } else if (relativeTime.includes("后")) {
    return relativeTime.replace("后", "后过期");
  }

  return relativeTime;
};

/**
 * 格式化过期日期，同时显示具体日期和相对时间
 * @param {string} expiryDate - UTC 时间字符串
 * @returns {string} 格式化后的过期信息
 */
const formatExpiry = (expiryDate) => {
  return formatExpiryUtil(expiryDate);
};

/**
 * 检查文本分享是否设置了密码
 * @param {Object} paste - 文本分享对象
 * @returns {boolean} 是否有密码
 */
const hasPassword = (paste) => {
  return paste?.has_password;
};

// 导入统一的工具函数
import { getRemainingViews as getRemainingViewsUtil } from "@/utils/fileUtils.js";

/**
 * 计算剩余可访问次数
 * @param {Object} paste - 文本分享对象
 * @returns {string|number} 剩余访问次数或状态描述
 */
const getRemainingViews = (paste) => {
  return getRemainingViewsUtil(paste); // 不传t函数，使用中文
};

/**
 * 检查文本分享是否已过期
 * @param {Object} paste - 文本分享对象
 * @returns {boolean} 是否已过期
 */
const isExpired = (paste) => {
  if (!paste?.expires_at) return false;
  const expiryDate = new Date(paste.expires_at);
  const now = new Date();
  return expiryDate < now;
};

/**
 * 关闭预览弹窗
 * 触发close事件通知父组件
 */
const closePreview = () => {
  emit("close");
};

/**
 * 查看文本分享链接
 * 触发view-paste事件通知父组件
 * @param {string} slug - 文本分享的唯一标识
 */
const viewPaste = (slug) => {
  emit("view-paste", slug);
};
</script>

<template>
  <!-- 预览弹窗 - 仅在showPreview为true时显示 -->
  <div v-if="showPreview" class="fixed inset-0 z-[60] overflow-y-auto" aria-labelledby="modal-title" role="dialog" aria-modal="true">
    <div class="flex items-center sm:items-center justify-center min-h-screen pt-20 sm:pt-2 px-2 sm:px-4 pb-4 sm:pb-20 text-center sm:p-0">
      <!-- 背景蒙层 - 点击时关闭弹窗 -->
      <div class="fixed inset-0 bg-gray-500 bg-opacity-75 dark:bg-gray-900 dark:bg-opacity-75 transition-opacity" aria-hidden="true" @click="closePreview"></div>

      <!-- 使对话框居中的辅助元素 -->
      <span class="hidden sm:inline-block sm:align-middle sm:h-screen" aria-hidden="true">&#8203;</span>

      <!-- 弹窗主体内容 -->
      <div
        class="inline-block align-middle sm:align-middle bg-white dark:bg-gray-800 rounded-lg text-left overflow-hidden shadow-xl transform transition-all w-full max-w-sm sm:max-w-2xl lg:max-w-3xl max-h-[95vh] sm:max-h-[85vh] my-1 sm:my-8"
        :class="darkMode ? 'dark' : ''"
      >
        <!-- 弹窗头部带关闭按钮 -->
        <div class="bg-white dark:bg-gray-800 px-4 py-3 sm:py-4 border-b dark:border-gray-700 flex justify-between items-center">
          <h3 class="text-lg leading-6 font-medium text-gray-900 dark:text-gray-100" id="modal-title">内容预览</h3>
          <button
            type="button"
            @click="closePreview"
            class="rounded-md p-1 inline-flex items-center justify-center text-gray-400 hover:text-gray-500 dark:hover:text-gray-300 focus:outline-none"
          >
            <span class="sr-only">关闭</span>
            <svg class="h-5 w-5" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
            </svg>
          </button>
        </div>

        <!-- 弹窗内容区 - 带滚动条 -->
        <div class="bg-white dark:bg-gray-800 px-3 sm:px-4 py-3 sm:py-4 overflow-y-auto" style="max-height: calc(95vh - 140px); min-height: 200px">
          <div class="flex flex-col space-y-4 w-full">
            <!-- 元数据展示区域 - 网格布局 -->
            <div class="grid grid-cols-1 sm:grid-cols-2 gap-3 sm:gap-4 bg-gray-50 dark:bg-gray-700 p-3 sm:p-4 rounded-lg text-sm">
              <!-- ID信息 -->
              <div>
                <p class="text-xs sm:text-sm text-gray-500 dark:text-gray-400">ID:</p>
                <p class="font-medium text-gray-900 dark:text-gray-100 truncate">{{ paste?.id }}</p>
              </div>

              <!-- 链接信息及复制按钮 -->
              <div>
                <p class="text-xs sm:text-sm text-gray-500 dark:text-gray-400">链接后缀:</p>
                <div class="flex items-center space-x-2">
                  <p class="font-medium text-primary-600 dark:text-primary-400 truncate">{{ paste?.slug }}</p>
                  <button @click="emit('copy-link', paste?.slug)" class="text-gray-400 hover:text-gray-600 dark:hover:text-gray-200 flex-shrink-0 relative">
                    <svg xmlns="http://www.w3.org/2000/svg" class="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                      <path
                        stroke-linecap="round"
                        stroke-linejoin="round"
                        stroke-width="2"
                        d="M8 16H6a2 2 0 01-2-2V6a2 2 0 012-2h8a2 2 0 012 2v2m-6 12h8a2 2 0 002-2v-8a2 2 0 00-2-2h-8a2 2 0 00-2 2v8a2 2 0 002 2z"
                      />
                    </svg>
                    <!-- 复制成功提示 -->
                    <span
                      v-if="props.copiedTexts[paste?.id]"
                      class="absolute -top-8 left-1/2 transform -translate-x-1/2 px-2 py-1 text-xs text-white bg-green-500 rounded whitespace-nowrap"
                    >
                      已复制
                    </span>
                  </button>
                </div>
              </div>

              <!-- 创建时间 -->
              <div>
                <p class="text-xs sm:text-sm text-gray-500 dark:text-gray-400">创建时间:</p>
                <p class="font-medium text-gray-900 dark:text-gray-100">{{ formatDate(paste?.created_at) }}</p>
              </div>

              <!-- 过期时间 - 过期则使用红色文本 -->
              <div>
                <p class="text-xs sm:text-sm text-gray-500 dark:text-gray-400">过期时间:</p>
                <p class="font-medium" :class="[isExpired(paste) ? 'text-red-600 dark:text-red-400' : 'text-gray-900 dark:text-gray-100']">
                  {{ formatExpiry(paste?.expires_at) }}
                </p>
              </div>

              <!-- 密码保护状态 - 使用不同颜色的标签 -->
              <div>
                <p class="text-xs sm:text-sm text-gray-500 dark:text-gray-400">密码保护:</p>
                <p class="font-medium">
                  <span
                    v-if="paste && hasPassword(paste)"
                    class="inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium"
                    :class="[
                      isExpired(paste) ? 'bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-300' : 'bg-yellow-100 text-yellow-800 dark:bg-yellow-800 dark:text-yellow-100',
                    ]"
                  >
                    <svg xmlns="http://www.w3.org/2000/svg" class="h-3 w-3 mr-1" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                      <path
                        stroke-linecap="round"
                        stroke-linejoin="round"
                        stroke-width="2"
                        d="M12 15v2m-6 4h12a2 2 0 002-2v-6a2 2 0 00-2-2H6a2 2 0 00-2 2v6a2 2 0 002 2zm10-10V7a4 4 0 00-8 0v4h8z"
                      />
                    </svg>
                    已加密
                  </span>
                  <span
                    v-else
                    class="inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium"
                    :class="[isExpired(paste) ? 'bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-300' : 'bg-green-100 text-green-800 dark:bg-green-800 dark:text-green-100']"
                  >
                    <svg xmlns="http://www.w3.org/2000/svg" class="h-3 w-3 mr-1" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                      <path
                        stroke-linecap="round"
                        stroke-linejoin="round"
                        stroke-width="2"
                        d="M8 11V7a4 4 0 118 0m-4 8v2m-6 4h12a2 2 0 002-2v-6a2 2 0 00-2-2H6a2 2 0 00-2 2v6a2 2 0 002 2z"
                      />
                    </svg>
                    公开
                  </span>
                </p>
              </div>

              <!-- 剩余访问次数 - 根据状态使用不同颜色 -->
              <div>
                <p class="text-xs sm:text-sm text-gray-500 dark:text-gray-400">剩余访问次数:</p>
                <p
                  class="font-medium"
                  :class="[
                    isExpired(paste)
                      ? 'text-red-600 dark:text-red-400'
                      : getRemainingViews(paste) === '已用完'
                      ? 'text-red-500'
                      : paste?.max_views && getRemainingViews(paste) < 3 && getRemainingViews(paste) !== '无限制'
                      ? 'text-yellow-500'
                      : 'text-gray-900 dark:text-gray-100',
                  ]"
                >
                  {{ paste ? getRemainingViews(paste) : "-" }}
                </p>
              </div>

              <!-- 备注信息 - 如果存在则跨两列显示 -->
              <div v-if="paste?.remark" class="col-span-1 sm:col-span-2">
                <p class="text-xs sm:text-sm text-gray-500 dark:text-gray-400">备注:</p>
                <p class="font-medium whitespace-pre-wrap break-words text-sm" :class="[isExpired(paste) ? 'text-red-600 dark:text-red-400' : 'text-gray-900 dark:text-gray-100']">
                  {{ paste.remark }}
                </p>
              </div>
            </div>

            <!-- 内容展示区域 - 带滚动条 -->
            <div class="mt-1 sm:mt-2">
              <p class="text-xs sm:text-sm text-gray-500 dark:text-gray-400 mb-1 sm:mb-2">内容:</p>
              <div class="bg-gray-50 dark:bg-gray-700 p-3 sm:p-4 rounded-lg max-h-48 sm:max-h-64 overflow-y-auto">
                <pre class="text-gray-900 dark:text-gray-100 whitespace-pre-wrap break-words text-xs sm:text-sm">{{ paste?.content }}</pre>
              </div>
            </div>
          </div>
        </div>

        <!-- 弹窗底部按钮区 -->
        <div class="bg-gray-50 dark:bg-gray-700 px-4 py-3 sm:px-6 flex flex-col sm:flex-row-reverse sm:space-x-reverse space-y-2 sm:space-y-0 sm:space-x-3">
          <button
            type="button"
            class="w-full inline-flex justify-center rounded-md border border-transparent shadow-sm px-4 py-2 bg-primary-600 text-base font-medium text-white hover:bg-primary-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500 sm:w-auto sm:text-sm"
            @click="viewPaste(paste?.slug)"
          >
            查看链接
          </button>
          <button
            type="button"
            class="w-full inline-flex justify-center rounded-md border border-gray-300 dark:border-gray-600 shadow-sm px-4 py-2 bg-white dark:bg-gray-800 text-base font-medium text-gray-700 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500 sm:w-auto sm:text-sm"
            @click="closePreview"
          >
            关闭
          </button>
        </div>
      </div>
    </div>
  </div>
</template>
