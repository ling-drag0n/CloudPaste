<script setup>
import { computed } from "vue";
import { useCreatorBadge } from "@/composables/admin-management/useCreatorBadge.js";
import { IconCopy, IconDelete, IconEye, IconEyeOff, IconGlobeAlt, IconLink, IconLockClosed, IconQrCode, IconRefresh, IconRename } from "@/components/icons";
import PasteTagPills from "./PasteTagPills.vue";

// 移动端的卡片列表视图
// 创建者徽章统一逻辑
const { formatCreator } = useCreatorBadge();

/**
 * 组件接收的属性定义
 * darkMode: 主题模式
 * pastes: 要展示的文本分享列表
 * loading: 加载状态
 * selectedPastes: 已选中的文本分享ID列表
 * copiedTexts: 已复制的文本ID列表
 * copiedRawTexts: 已复制的原始文本ID列表
 */
const props = defineProps({
  darkMode: {
    type: Boolean,
    required: true,
  },
  pastes: {
    type: Array,
    required: true,
  },
  loading: {
    type: Boolean,
    default: false,
  },
  selectedPastes: {
    type: Array,
    required: true,
  },
  copiedTexts: {
    type: Object,
    default: () => ({}),
  },
  copiedRawTexts: {
    type: Object,
    default: () => ({}),
  },
});

/**
 * 定义组件要触发的事件
 * toggle-select-item: 切换单个项目的选择状态
 * view: 查看分享链接
 * copy-link: 复制分享链接
 * copy-raw-link: 复制原始文本链接
 * preview: 预览分享内容
 * edit: 编辑分享属性
 * delete: 删除分享
 * show-qrcode: 显示分享链接的二维码
 */
const emit = defineEmits(["toggle-select-item", "view", "copy-link", "copy-raw-link", "preview", "edit", "delete", "show-qrcode"]);

/**
 * 截取文本并添加省略号
 * @param {string} text - 需要截取的文本
 * @param {number} length - 截取长度
 * @returns {string} 截取后的文本
 */
const truncateText = (text, length = 10) => {
  if (!text) return "无";
  return text.length <= length ? text : `${text.substring(0, length)}...`;
};

// 导入统一的时间处理工具
import { formatDateTime, formatExpiry as formatExpiryUtil, isExpired as isExpiredUtil } from "@/utils/timeUtils.js";

/**
 * 格式化日期为本地日期时间字符串
 * @param {string} dateString - UTC 时间字符串
 * @returns {string} 格式化后的本地时间字符串
 */
const formatDate = (dateString) => {
  return formatDateTime(dateString);
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
  return paste.has_password;
};

// 导入统一的工具函数
import { getRemainingViews as getRemainingViewsUtil } from "@/utils/fileUtils.js";

/**
 * 计算剩余可访问次数（数值）
 * @param {Object} paste - 文本分享对象
 * @returns {number} Infinity 表示无限制，0 表示已用完，正数表示剩余次数
 */
const getRemainingViews = (paste) => {
  return getRemainingViewsUtil(paste);
};

/**
 * 格式化剩余访问次数为展示文案
 * @param {Object} paste - 文本分享对象
 * @returns {string}
 */
const getRemainingViewsLabel = (paste) => {
  const remaining = getRemainingViews(paste);
  if (remaining === Infinity) {
    return "无限制";
  }
  if (remaining === 0) {
    return "已用完";
  }
  return `${remaining}`;
};

/**
 * 检查文本分享是否已过期
 * @param {Object} paste - 文本分享对象
 * @returns {boolean} 是否已过期
 */
const isExpired = (paste) => {
  if (!paste.expires_at) return false;
  return isExpiredUtil(paste.expires_at);
};

</script>

<template>
  <!-- 移动端卡片视图 - 仅在移动设备上显示 -->
  <div class="md:hidden divide-y divide-gray-200 dark:divide-gray-700">
    <!-- 加载状态 -->
    <div v-if="loading" class="p-4 text-center" :class="darkMode ? 'text-gray-300' : 'text-gray-500'">
      <div class="flex justify-center">
        <IconRefresh class="animate-spin h-5 w-5 text-primary-500" />
        <span class="ml-2">加载中...</span>
      </div>
    </div>

    <!-- 空数据状态 -->
    <div v-else-if="pastes.length === 0" class="p-4 text-center" :class="darkMode ? 'text-gray-300' : 'text-gray-500'">未找到分享记录</div>

    <!-- 移动端卡片列表 - 遍历pastes数组生成卡片 -->
    <div v-for="paste in pastes" :key="paste.id" class="p-4 bg-white dark:bg-gray-800 border-b dark:border-gray-700">
      <!-- 卡片头部 - 链接和操作按钮 -->
      <div class="flex justify-between items-start mb-2">
        <!-- 左侧：复选框、链接和复制按钮 -->
        <div class="flex items-center">
          <input
            type="checkbox"
            :checked="selectedPastes.includes(paste.id)"
            @change="$emit('toggle-select-item', paste.id)"
            class="h-4 w-4 text-primary-600 focus:ring-primary-500 border-gray-300 rounded cursor-pointer mr-2"
          />
          <span
            class="cursor-pointer font-medium text-base hover:underline"
            @click="$emit('view', paste.slug)"
            :class="isExpired(paste) ? 'text-red-600 dark:text-red-400' : 'text-primary-600 dark:text-primary-400'"
          >
            {{ paste.title || paste.remark || paste.slug }}
          </span>
          <button @click="$emit('copy-link', paste.slug)" class="ml-2 text-gray-400 hover:text-gray-600 dark:hover:text-gray-200 relative" title="复制链接">
            <IconCopy class="h-4 w-4" />
            <!-- 添加复制成功提示 -->
            <span v-if="copiedTexts[paste.id]" class="absolute -top-8 left-1/2 transform -translate-x-1/2 px-2 py-1 text-xs text-white bg-green-500 rounded whitespace-nowrap">
              已复制
            </span>
          </button>
          <!-- 添加二维码按钮 -->
          <button @click="$emit('show-qrcode', paste.slug)" class="ml-2 text-gray-400 hover:text-gray-600 dark:hover:text-gray-200" title="显示二维码">
            <IconQrCode class="h-4 w-4" />
          </button>
        </div>

        <!-- 右侧：操作按钮组（预览、编辑、删除） -->
        <div class="flex space-x-2">
          <!-- 预览按钮 -->
          <button class="text-primary-600 hover:text-primary-900 dark:text-primary-400 dark:hover:text-primary-300 p-1" @click="$emit('preview', paste)">
            <IconEye class="h-5 w-5" />
          </button>

          <!-- 编辑按钮 -->
          <button class="text-indigo-600 hover:text-indigo-900 dark:text-indigo-400 dark:hover:text-indigo-300 p-1" @click="$emit('edit', paste)">
            <IconRename class="h-5 w-5" />
          </button>

          <!-- 复制原始文本直链按钮 -->
          <button class="text-purple-600 hover:text-purple-900 dark:text-purple-400 dark:hover:text-purple-300 p-1 relative" @click="$emit('copy-raw-link', paste.slug)">
            <IconLink class="h-5 w-5" />
            <!-- 原始文本直链复制成功提示 -->
            <span v-if="copiedRawTexts[paste.id]" class="absolute -top-8 right-0 px-2 py-1 text-xs text-white bg-green-500 rounded whitespace-nowrap">已复制直链</span>
          </button>

          <!-- 删除按钮 -->
          <button class="text-red-600 hover:text-red-900 dark:text-red-400 dark:hover:text-red-300 p-1" @click="$emit('delete', paste.id)">
            <IconDelete class="h-5 w-5" />
          </button>
        </div>
      </div>

      <!-- 卡片内容 - 网格布局展示详细信息 -->
      <div class="grid grid-cols-2 gap-2 text-sm mt-3">
        <div v-if="paste.tags?.length" class="col-span-2">
          <div class="text-gray-500 dark:text-gray-400 text-xs mb-1">标签</div>
          <PasteTagPills :tags="paste.tags" :max="4" />
        </div>
        <!-- 备注信息 -->
        <div>
          <div class="text-gray-500 dark:text-gray-400 text-xs mb-1">备注</div>
          <span
            class="truncate max-w-xs inline-block px-2 py-1 rounded"
            :class="[
              paste.remark ? 'bg-blue-50 dark:bg-blue-900 text-blue-700 dark:text-blue-300' : darkMode ? 'text-gray-300' : 'text-gray-500',
              isExpired(paste) ? 'text-red-600 dark:text-red-400' : '',
            ]"
            :title="paste.remark || '无'"
          >
            {{ truncateText(paste.remark) }}
          </span>
        </div>

        <!-- 创建时间 -->
        <div>
          <div class="text-gray-500 dark:text-gray-400 text-xs mb-1">创建时间</div>
          <div :class="[darkMode ? 'text-gray-300' : 'text-gray-500', isExpired(paste) ? 'text-red-600 dark:text-red-400' : '']">
            {{ formatDate(paste.created_at) }}
          </div>
        </div>

        <!-- 过期时间 -->
        <div>
          <div class="text-gray-500 dark:text-gray-400 text-xs mb-1">过期时间</div>
          <div :class="[darkMode ? 'text-gray-300' : 'text-gray-500', isExpired(paste) ? 'text-red-600 dark:text-red-400' : '']">
            {{ formatExpiry(paste.expires_at) }}
          </div>
        </div>

        <!-- 创建者 -->
        <div>
          <div class="text-gray-500 dark:text-gray-400 text-xs mb-1">创建者</div>
          <div class="flex">
            <span
              v-if="paste.created_by && paste.created_by.startsWith('apikey:')"
              class="px-3 py-1 text-xs rounded bg-blue-100 text-blue-800 dark:bg-blue-800 dark:text-blue-100 inline-block mt-1"
            >
              {{ paste.key_name ? `密钥：${paste.key_name}` : `密钥：${paste.created_by.substring(7, 12)}...` }}
            </span>
            <span
              v-else-if="paste.created_by === 'admin' || (paste.created_by && /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(paste.created_by))"
              class="px-3 py-1 text-xs rounded bg-green-100 text-green-800 dark:bg-green-800 dark:text-green-100 inline-block mt-1"
            >
              管理员
            </span>
            <span v-else class="px-3 py-1 text-xs rounded bg-gray-100 text-gray-800 dark:bg-gray-700 dark:text-gray-300 inline-block mt-1">
              {{ paste.created_by || "未知来源" }}
            </span>
          </div>
        </div>

        <!-- 状态信息 - 包含密码保护、可见性和剩余次数 -->
        <div class="col-span-2">
          <div class="text-gray-500 dark:text-gray-400 text-xs mb-1">状态信息</div>
          <div class="flex items-center flex-wrap gap-2">
            <!-- 可见性标签 -->
            <span
              v-if="paste.is_public === false"
              class="inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium bg-gray-100 text-gray-700 dark:bg-gray-700 dark:text-gray-200"
            >
              <IconEyeOff class="h-3 w-3 mr-1" />
              仅内部
            </span>

            <!-- 密码状态标签 - 已加密 -->
            <span
              v-if="hasPassword(paste)"
              class="inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium"
              :class="[isExpired(paste) ? 'bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-300' : 'bg-yellow-100 text-yellow-800 dark:bg-yellow-800 dark:text-yellow-100']"
            >
              <IconLockClosed class="h-3 w-3 mr-1" />
              已加密
            </span>

            <!-- 密码状态标签 - 公开 -->
            <span
              v-else
              class="inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium"
              :class="[isExpired(paste) ? 'bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-300' : 'bg-green-100 text-green-800 dark:bg-green-800 dark:text-green-100']"
            >
              <IconGlobeAlt class="h-3 w-3 mr-1" />
              公开
            </span>

            <!-- 剩余访问次数标签 -->
            <span
              class="inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium"
              :class="[
                isExpired(paste)
                  ? 'bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-300'
                  : getRemainingViews(paste) === 0
                  ? 'bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-300'
                  : 'bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-300',
              ]"
            >
              <IconEye class="h-3 w-3 mr-1" />
              剩余 {{ getRemainingViewsLabel(paste) }}
            </span>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>
