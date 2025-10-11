<template>
  <div class="fixed inset-0 z-[60] overflow-auto bg-black bg-opacity-50 flex items-center justify-center p-2 sm:p-4 pt-20 sm:pt-4">
    <div class="relative bg-white dark:bg-gray-800 rounded-lg max-w-sm sm:max-w-lg w-full mx-auto shadow-xl overflow-hidden max-h-[95vh] sm:max-h-[85vh]">
      <!-- 标题栏 -->
      <div class="px-4 sm:px-6 py-3 sm:py-4 border-b flex justify-between items-center" :class="darkMode ? 'border-gray-700' : 'border-gray-200'">
        <h3 class="text-base sm:text-lg font-medium" :class="darkMode ? 'text-white' : 'text-gray-900'">文件详情</h3>
        <button @click="$emit('close')" class="text-gray-400 hover:text-gray-500">
          <svg class="h-6 w-6" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
          </svg>
        </button>
      </div>

      <!-- 文件内容 -->
      <div class="px-4 sm:px-6 py-3 sm:py-4 overflow-y-auto" style="max-height: calc(95vh - 160px)">
        <div class="space-y-4">
          <!-- 基本信息 -->
          <div>
            <h4 class="text-sm font-medium mb-2" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">基本信息</h4>
            <div class="bg-gray-50 dark:bg-gray-900 rounded p-3">
              <dl class="grid grid-cols-3 gap-x-4 gap-y-2 text-sm">
                <dt :class="darkMode ? 'text-gray-400' : 'text-gray-500'">文件名</dt>
                <dd class="col-span-2" :class="darkMode ? 'text-white' : 'text-gray-900'">{{ file.filename }}</dd>

                <dt :class="darkMode ? 'text-gray-400' : 'text-gray-500'">短链接</dt>
                <dd class="col-span-2 break-words" :class="darkMode ? 'text-white' : 'text-gray-900'">
                  <span v-if="file.slug">{{ baseUrl }}/file/{{ file.slug }}</span>
                  <span v-else class="text-gray-400">未设置</span>
                </dd>

                <dt :class="darkMode ? 'text-gray-400' : 'text-gray-500'">MIME类型</dt>
                <dd class="col-span-2 break-words" :class="darkMode ? 'text-white' : 'text-gray-900'">{{ file.mimetype || "未知" }}</dd>

                <dt :class="darkMode ? 'text-gray-400' : 'text-gray-500'">文件大小</dt>
                <dd class="col-span-2" :class="darkMode ? 'text-white' : 'text-gray-900'">{{ formatFileSize(file.size) }}</dd>

                <dt :class="darkMode ? 'text-gray-400' : 'text-gray-500'">备注</dt>
                <dd class="col-span-2 max-h-24 overflow-y-auto">
                  <span v-if="file.remark" :class="darkMode ? 'text-blue-400' : 'text-blue-600'" class="block break-words whitespace-pre-wrap">{{ file.remark }}</span>
                  <span v-else class="text-gray-400">无备注</span>
                </dd>

                <dt :class="darkMode ? 'text-gray-400' : 'text-gray-500'">密码保护</dt>
                <dd class="col-span-2" :class="darkMode ? 'text-white' : 'text-gray-900'">
                  <span v-if="file.has_password" class="text-yellow-500">已启用</span>
                  <span v-else>未启用</span>
                </dd>
              </dl>
            </div>
          </div>

          <!-- 访问统计 -->
          <div>
            <h4 class="text-sm font-medium mb-2" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">访问统计</h4>
            <div class="bg-gray-50 dark:bg-gray-900 rounded p-3">
              <dl class="grid grid-cols-3 gap-x-4 gap-y-2 text-sm">
                <dt :class="darkMode ? 'text-gray-400' : 'text-gray-500'">访问次数</dt>
                <dd class="col-span-2" :class="darkMode ? 'text-white' : 'text-gray-900'">{{ file.views || 0 }} 次</dd>

                <dt :class="darkMode ? 'text-gray-400' : 'text-gray-500'">剩余次数</dt>
                <dd class="col-span-2" :class="remainingViewsClass">
                  {{ getRemainingViews }}
                </dd>

                <dt :class="darkMode ? 'text-gray-400' : 'text-gray-500'">过期时间</dt>
                <dd class="col-span-2" :class="expiresClass">
                  <span v-if="file.expires_at">{{ formatDateTime(file.expires_at) }}</span>
                  <span v-else>永不过期</span>
                </dd>
              </dl>
            </div>
          </div>

          <!-- 存储信息 -->
          <div>
            <h4 class="text-sm font-medium mb-2" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">存储信息</h4>
            <div class="bg-gray-50 dark:bg-gray-900 rounded p-3">
              <dl class="grid grid-cols-3 gap-x-4 gap-y-2 text-sm">
                <dt :class="darkMode ? 'text-gray-400' : 'text-gray-500'">存储配置</dt>
                <dd class="col-span-2" :class="darkMode ? 'text-white' : 'text-gray-900'">
                  {{ file.storage_config_name || "默认存储" }}
                </dd>

                <dt :class="darkMode ? 'text-gray-400' : 'text-gray-500'">提供商</dt>
                <dd class="col-span-2" :class="darkMode ? 'text-white' : 'text-gray-900'">
                  {{ file.storage_provider_type || "未知" }}
                </dd>

                <dt :class="darkMode ? 'text-gray-400' : 'text-gray-500'">存储路径</dt>
                <dd class="col-span-2 break-all text-xs" :class="darkMode ? 'text-white' : 'text-gray-900'">
                  {{ file.storage_path || "未知" }}
                </dd>

                <dt :class="darkMode ? 'text-gray-400' : 'text-gray-500'">ETag</dt>
                <dd class="col-span-2 break-all text-xs" :class="darkMode ? 'text-white' : 'text-gray-900'">
                  {{ file.etag || "未知" }}
                </dd>

                <dt :class="darkMode ? 'text-gray-400' : 'text-gray-500'">创建时间</dt>
                <dd class="col-span-2" :class="darkMode ? 'text-white' : 'text-gray-900'">
                  {{ formatDateTime(file.created_at) }}
                </dd>
              </dl>
            </div>
          </div>

          <!-- 操作按钮 -->
          <div class="flex justify-between mt-4">
            <!-- 左侧按钮组 -->
            <div class="flex space-x-2">
              <!-- 下载文件按钮 - 使用永久链接 -->
              <button @click="emit('download-file', file)" class="px-4 py-2 bg-blue-600 hover:bg-blue-700 text-white rounded-md text-sm font-medium">下载文件</button>

              <!-- 预览按钮 - 使用永久链接 -->
              <button @click="emit('preview-file', file)" class="px-4 py-2 bg-indigo-600 hover:bg-indigo-700 text-white rounded-md text-sm font-medium">预览</button>
            </div>

            <button
              @click="$emit('close')"
              class="px-4 py-2 rounded-md text-sm font-medium transition-colors"
              :class="darkMode ? 'bg-gray-700 hover:bg-gray-600 text-white' : 'bg-gray-200 hover:bg-gray-300 text-gray-700'"
            >
              关闭
            </button>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { defineProps, defineEmits, computed } from "vue";

const props = defineProps({
  file: {
    type: Object,
    required: true,
  },
  darkMode: {
    type: Boolean,
    default: false,
  },
});

const emit = defineEmits(["close", "preview-file", "download-file"]);

// 获取当前网站基础URL
const baseUrl = computed(() => {
  return window.location.origin;
});

// 导入统一的工具函数
import { formatFileSize, getRemainingViews as getRemainingViewsUtil } from "@/utils/fileUtils.js";

// 导入统一的时间处理工具
import { formatDateTimeWithSeconds } from "@/utils/timeUtils.js";

/**
 * 格式化日期时间
 * @param {string} dateString - UTC 时间字符串
 * @returns {string} 格式化后的本地时间字符串
 */
const formatDateTime = (dateString) => {
  return formatDateTimeWithSeconds(dateString);
};

/**
 * 计算过期时间的显示样式
 */
const expiresClass = computed(() => {
  if (!props.file.expires_at) {
    return props.darkMode ? "text-white" : "text-gray-900";
  }

  const expiryDate = new Date(props.file.expires_at);
  const now = new Date();

  if (expiryDate < now) {
    return "text-red-500";
  }

  const diffMs = expiryDate - now;
  const diffDays = Math.floor(diffMs / (1000 * 60 * 60 * 24));

  if (diffDays < 1) {
    return "text-orange-500";
  }

  return props.darkMode ? "text-white" : "text-gray-900";
});

/**
 * 计算剩余访问次数
 */
const getRemainingViews = computed(() => {
  const result = getRemainingViewsUtil(props.file); // 不传t函数，使用中文
  return typeof result === "number" ? `${result} 次` : result;
});

/**
 * 计算剩余次数的显示样式
 */
const remainingViewsClass = computed(() => {
  if (!props.file.max_views) {
    return props.darkMode ? "text-white" : "text-gray-900";
  }

  const viewCount = props.file.views || 0;
  const remaining = props.file.max_views - viewCount;

  if (remaining <= 0) {
    return "text-red-500";
  }

  if (remaining < 3) {
    return "text-orange-500";
  }

  return props.darkMode ? "text-white" : "text-gray-900";
});
</script>
