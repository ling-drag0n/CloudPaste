<script setup>
import { ref, computed, onMounted, nextTick } from "vue";
import { onClickOutside } from "@vueuse/core";
import { useI18n } from "vue-i18n";
// 导入统一的时间处理工具
import { formatDateTime, formatRelativeTime, isExpired as isExpiredUtil } from "@/utils/timeUtils.js";
// 导入统一的文件工具
import { getRemainingViews as getRemainingViewsUtil } from "@/utils/fileUtils.js";
// 导入创建者徽章统一逻辑
import { useCreatorBadge } from "@/composables/admin-management/useCreatorBadge.js";
import { useGlobalMessage } from "@/composables/core/useGlobalMessage.js";
import { IconArrowUp, IconCalendar, IconCopy, IconDelete, IconError, IconEye, IconEyeOff, IconGlobeAlt, IconLink, IconLockClosed, IconMenu, IconQrCode, IconRename, IconUser } from "@/components/icons";
import PasteTagPills from "./PasteTagPills.vue";

const { getCreatorText } = useCreatorBadge();
const { t } = useI18n();
const { showError } = useGlobalMessage();

/**
 * Paste 瀑布流卡片组件
 * 用于在瀑布流布局中展示文本分享的预览卡片
 */
const props = defineProps({
  darkMode: {
    type: Boolean,
    required: true,
  },
  paste: {
    type: Object,
    required: true,
  },
});

const emit = defineEmits(["click", "preview", "edit", "delete", "copy-link", "copy-raw-link", "show-qrcode", "quick-edit-content"]);

/**
 * 截断文本内容用于预览
 * @param {string} text - 原始文本
 * @param {number} maxLength - 最大长度
 * @returns {string} 截断后的文本
 */
const truncateContent = (text, maxLength = 800) => {
  if (!text) return "";
  if (text.length <= maxLength) return text;
  return text.substring(0, maxLength) + "...";
};

/**
 * 获取显示内容
 * 管理端列表统一返回完整 content，这里直接使用全文，预览截断交给前端 truncateContent 处理
 */
const displayContent = computed(() => {
  return props.paste.content || "";
});

/**
 * 检查是否已过期
 */
const isExpired = computed(() => {
  return props.paste.expires_at ? isExpiredUtil(props.paste.expires_at) : false;
});

/**
 * 获取剩余访问次数
 */
const remainingViews = computed(() => {
  return getRemainingViewsUtil(props.paste);
});

/**
 * 格式化创建者信息
 */
const creatorLabel = computed(() => {
  return getCreatorText(props.paste.created_by, props.paste.key_name);
});

/**
 * 是否显示剩余次数警告角标（<20时显示）
 */
const showViewsWarning = computed(() => {
  const num = remainingViews.value;
  return num !== Infinity && num > 0 && num < 10;
});

/**
 * 编辑状态管理
 */
const isEditing = ref(false);
const editingContent = ref("");
const isTouchDevice = ref(false);

// 移动端双击检测
const lastTapTime = ref(0);
const doubleTapDelay = 300;

/**
 * 开始编辑内容
 */
const startEditing = () => {
  editingContent.value = displayContent.value;
  isEditing.value = true;
  // 下一帧聚焦到 textarea
  nextTick(() => {
    const textarea = document.querySelector(`#edit-textarea-${props.paste.id}`);
    if (textarea) {
      // 防止浏览器自动滚动
      textarea.focus({ preventScroll: true });
      // 将 textarea 内部滚动位置重置到顶部
      textarea.scrollTop = 0;
      // 将光标定位到开头
      textarea.setSelectionRange(0, 0);
    }
  });
};

/**
 * 保存编辑内容
 */
const saveContent = () => {
  if (editingContent.value.trim() === "") {
    showError(t("markdown.messages.contentEmpty"));
    return;
  }

  emit("quick-edit-content", {
    id: props.paste.id,
    slug: props.paste.slug,
    content: editingContent.value,
  });

  isEditing.value = false;
};

/**
 * 取消编辑
 */
const cancelEdit = () => {
  isEditing.value = false;
  editingContent.value = "";
};

/**
 * 处理桌面端双击事件
 */
const handleDoubleClick = (event) => {
  // 如果点击的是按钮或链接，不触发编辑
  if (event.target.closest("button") || event.target.closest("a")) {
    return;
  }

  startEditing();
};

/**
 * 处理移动端双击事件
 * 通过检测两次触摸的时间间隔来判断是否为双击
 */
const handleTouchStart = (event) => {
  // 如果点击的是按钮或链接，不触发编辑
  if (event.target.closest("button") || event.target.closest("a")) {
    return;
  }

  const currentTime = new Date().getTime();
  const tapInterval = currentTime - lastTapTime.value;

  // 如果两次点击间隔小于阈值，认为是双击
  if (tapInterval < doubleTapDelay && tapInterval > 0) {
    // 阻止默认行为(如缩放)
    event.preventDefault();
    startEditing();
    // 重置时间，避免连续触发
    lastTapTime.value = 0;
  } else {
    // 记录本次点击时间
    lastTapTime.value = currentTime;
  }
};

/**
 * 下拉菜单显示状态
 */
const dropdownRef = ref(null);
const showDropdown = ref(false);

/**
 * 切换下拉菜单显示状态
 */
const toggleDropdown = (event) => {
  event.stopPropagation();
  showDropdown.value = !showDropdown.value;
};

/**
 * 关闭下拉菜单
 */
const closeDropdown = () => {
  showDropdown.value = false;
};

/**
 * 处理卡片点击 - 取消点击行为
 */
const handleCardClick = () => {
  // 不执行任何操作，预览功能移到三点菜单
};

// 点击外部关闭下拉菜单（VueUse 自动管理监听器与清理）
onClickOutside(dropdownRef, () => {
  if (!showDropdown.value) return;
  closeDropdown();
});

// 生命周期：添加全局点击事件监听
onMounted(() => {
  // 检测是否为触摸设备（移动端）
  isTouchDevice.value =
    'ontouchstart' in window ||
    navigator.maxTouchPoints > 0 ||
    /Android|iPhone|iPad|iPod/i.test(navigator.userAgent);
});
</script>

<template>
  <div
    class="paste-masonry-card relative group flex flex-col rounded-lg border border-gray-200 dark:border-gray-700 transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-primary-500"
    :class="isExpired && !isEditing
      ? 'bg-gray-50 dark:bg-gray-900'
      : 'bg-white dark:bg-gray-800'"
    tabindex="0"
    @dblclick.prevent="handleDoubleClick"
    @touchstart="handleTouchStart"
  >
    <!-- 编辑模式：整个卡片变成编辑区 -->
    <div v-if="isEditing" class="flex flex-col min-h-[180px] sm:min-h-[220px]">
      <textarea
        :id="`edit-textarea-${paste.id}`"
        v-model="editingContent"
        class="w-full flex-1 p-3 sm:p-4 text-sm leading-relaxed resize-none border-none outline-none paste-edit-textarea min-h-[120px] sm:min-h-[160px] max-h-[400px] sm:max-h-[500px]"
        :class="darkMode ? 'bg-gray-800 text-gray-100' : 'bg-white text-gray-900'"
        placeholder="编辑内容..."
        @keydown.esc="cancelEdit"
        @keydown.meta.enter="saveContent"
        @keydown.ctrl.enter="saveContent"
      ></textarea>

      <!-- 编辑按钮组 -->
      <div class="flex items-center justify-end space-x-2 p-2 sm:p-3 border-t" :class="darkMode ? 'border-gray-700' : 'border-gray-200'">
        <button
          @click="cancelEdit"
          class="px-4 py-2 text-sm font-medium rounded transition-colors"
          :class="darkMode
            ? 'bg-gray-700 text-gray-200 hover:bg-gray-600'
            : 'bg-gray-100 text-gray-700 hover:bg-gray-200'"
        >
          取消
        </button>
        <button
          @click="saveContent"
          class="px-4 py-2 text-sm font-medium text-white rounded transition-colors"
          :class="darkMode
            ? 'bg-primary-600 hover:bg-primary-700'
            : 'bg-primary-600 hover:bg-primary-700'"
        >
          保存
        </button>
      </div>
    </div>

    <!-- 阅读模式：显示完整卡片内容 -->
    <template v-else>
      <!-- 卡片头部：创建时间 + 状态标签 + 操作按钮 -->
      <div class="px-3 pt-3 pb-2 sm:px-4 sm:pt-4">
      <!-- 第一行：创建时间（左）+ 状态标签和按钮（右） -->
      <div class="flex items-center justify-between mb-2">
        <!-- 左：创建时间（小字，显示具体时分秒） -->
        <time class="text-xs text-gray-500 dark:text-gray-400" :datetime="paste.created_at" :title="formatDateTime(paste.created_at)">
          {{ formatDateTime(paste.created_at) }}
        </time>

        <!-- 右侧：状态标签 + 操作按钮 -->
        <div class="flex items-center space-x-2 flex-shrink-0">
          <!-- 状态图标组（仅图标） -->
          <div class="flex items-center space-x-1.5">
            <!-- 密码图标 -->
            <span
              v-if="paste.has_password"
              class="inline-flex items-center justify-center w-6 h-6 rounded-full"
              :class="darkMode ? 'bg-amber-500/15' : 'bg-amber-50'"
              title="密码保护"
            >
              <IconLockClosed class="h-4 w-4" :class="darkMode ? 'text-amber-200' : 'text-amber-600'" />
            </span>

            <!-- 可见性图标 -->
            <span
              class="inline-flex items-center justify-center w-6 h-6 rounded-full"
              :class="paste.is_public
                ? (darkMode ? 'bg-green-500/15' : 'bg-green-50')
                : (darkMode ? 'bg-gray-500/15' : 'bg-gray-50')"
              :title="paste.is_public ? '公开访问' : '仅管理员和创建者可见'"
            >
              <!-- 公开图标 -->
              <IconGlobeAlt v-if="paste.is_public" class="h-4 w-4" :class="darkMode ? 'text-green-200' : 'text-green-600'" />
              <!-- 私密图标 -->
              <IconEyeOff v-else class="h-4 w-4" :class="darkMode ? 'text-gray-200' : 'text-gray-600'" />
            </span>

            <!-- 过期图标 -->
            <span v-if="isExpired" class="inline-flex items-center justify-center w-6 h-6 rounded-full bg-red-100 dark:bg-red-900/30" title="已过期">
              <IconError class="h-4 w-4 text-red-600 dark:text-red-400" />
            </span>

            <!-- 剩余次数警告（<10时显示）- 图标+数字组合 -->
            <span
              v-if="showViewsWarning"
              class="inline-flex items-center space-x-0.5"
              :title="`剩余次数: ${remainingViews}`"
            >
              <!-- 眼睛图标（与其他图标风格统一） -->
              <span
                class="inline-flex items-center justify-center w-6 h-6 rounded-full"
                :class="darkMode ? 'bg-orange-500/15' : 'bg-orange-50'"
              >
                <IconEye class="h-4 w-4" :class="darkMode ? 'text-orange-300' : 'text-orange-600'" />
              </span>
              <!-- 数字标签 -->
              <span
                class="text-xs font-medium"
                :class="darkMode ? 'text-orange-300' : 'text-orange-600'"
              >
                {{ remainingViews }}
              </span>
            </span>
          </div>

          <!-- 三点操作按钮 -->
          <div class="relative" ref="dropdownRef">
            <button
              @click.stop="toggleDropdown"
              class="p-2 sm:p-1 -m-1 sm:m-0 rounded-md hover:bg-gray-100 dark:hover:bg-gray-700 active:bg-gray-200 dark:active:bg-gray-600 text-gray-500 dark:text-gray-400 transition-colors"
              title="更多操作"
            >
              <IconMenu class="h-4 w-4" />
            </button>

            <!-- 下拉菜单 -->
            <div
              v-if="showDropdown"
              class="absolute right-0 top-8 mt-1 w-40 rounded-md shadow-lg bg-white dark:bg-gray-700 ring-1 ring-black ring-opacity-5 z-50"
              @click.stop
            >
              <div class="py-1">
                <!-- 预览 -->
                <button
                  @click.stop="
                    emit('preview', paste);
                    closeDropdown();
                  "
                  class="w-full text-left px-4 py-2 text-sm text-gray-700 dark:text-gray-200 hover:bg-gray-100 dark:hover:bg-gray-600 flex items-center transition-colors"
                >
                  <IconEye class="h-4 w-4 mr-2 text-blue-600 dark:text-blue-400" />
                  预览
                </button>

                <!-- 快速编辑 -->
                <button
                  @click.stop="
                    startEditing();
                    closeDropdown();
                  "
                  class="w-full text-left px-4 py-2 text-sm text-gray-700 dark:text-gray-200 hover:bg-gray-100 dark:hover:bg-gray-600 flex items-center transition-colors"
                >
                  <IconArrowUp class="h-4 w-4 mr-2 text-green-600 dark:text-green-400" />
                  快速编辑
                </button>

                <!-- 编辑 -->
                <button
                  @click.stop="
                    emit('edit', paste);
                    closeDropdown();
                  "
                  class="w-full text-left px-4 py-2 text-sm text-gray-700 dark:text-gray-200 hover:bg-gray-100 dark:hover:bg-gray-600 flex items-center transition-colors"
                >
                  <IconRename class="h-4 w-4 mr-2 text-indigo-600 dark:text-indigo-400" />
                  完整编辑
                </button>

                <!-- 复制链接 -->
                <button
                  @click.stop="
                    emit('copy-link', paste.slug);
                    closeDropdown();
                  "
                  class="w-full text-left px-4 py-2 text-sm text-gray-700 dark:text-gray-200 hover:bg-gray-100 dark:hover:bg-gray-600 flex items-center transition-colors"
                >
                  <IconCopy class="h-4 w-4 mr-2 text-purple-600 dark:text-purple-400" />
                  复制链接
                </button>

                <!-- 复制直链 -->
                <button
                  @click.stop="
                    emit('copy-raw-link', paste.slug);
                    closeDropdown();
                  "
                  class="w-full text-left px-4 py-2 text-sm text-gray-700 dark:text-gray-200 hover:bg-gray-100 dark:hover:bg-gray-600 flex items-center transition-colors"
                >
                  <IconLink class="h-4 w-4 mr-2 text-cyan-600 dark:text-cyan-400" />
                  复制直链
                </button>

                <!-- 显示二维码 -->
                <button
                  @click.stop="
                    emit('show-qrcode', paste.slug);
                    closeDropdown();
                  "
                  class="w-full text-left px-4 py-2 text-sm text-gray-700 dark:text-gray-200 hover:bg-gray-100 dark:hover:bg-gray-600 flex items-center transition-colors"
                >
                  <IconQrCode class="h-4 w-4 mr-2 text-gray-600 dark:text-gray-400" />
                  显示二维码
                </button>

                <!-- 分隔线 -->
                <div class="border-t border-gray-200 dark:border-gray-600 my-1"></div>

                <!-- 删除 -->
                <button
                  @click.stop="
                    emit('delete', paste.id);
                    closeDropdown();
                  "
                  class="w-full text-left px-4 py-2 text-sm text-red-600 dark:text-red-400 hover:bg-red-50 dark:hover:bg-red-900/30 flex items-center transition-colors"
                >
                  <IconDelete class="h-4 w-4 mr-2" />
                  删除
                </button>
              </div>
            </div>
          </div>
        </div>
      </div>

      <!-- 第二行：标题 -->
      <h3
        class="text-sm sm:text-base font-medium truncate mb-2"
        :class="isExpired ? 'text-red-600 dark:text-red-400' : 'text-primary-600 dark:text-primary-400'"
        :title="paste.title || paste.remark || paste.slug"
      >
        {{ paste.title || paste.slug }}
      </h3>

      <PasteTagPills v-if="paste.tags?.length" :tags="paste.tags" :max="4" class="mb-3" />

      <!-- 内容区域 -->
      <div class="text-sm text-gray-700 dark:text-gray-300 mb-3 line-clamp-8 sm:line-clamp-10 whitespace-pre-wrap break-words leading-relaxed">
        {{ truncateContent(displayContent, 400) }}
      </div>
    </div>

    <!-- 卡片底部：元数据（小字灰色） -->
    <div class="px-3 pb-2.5 sm:px-4 sm:pb-3">
      <div class="flex flex-wrap gap-x-3 gap-y-1 text-xs text-gray-500 dark:text-gray-400">
        <!-- 创建者 -->
        <span class="inline-flex items-center">
          <IconUser class="h-3 w-3 mr-1" />
          {{ creatorLabel }}
        </span>

        <!-- 过期时间（如果有） -->
        <span v-if="paste.expires_at" class="inline-flex items-center" :class="isExpired ? 'text-red-600 dark:text-red-400' : ''">
          <IconCalendar class="h-3 w-3 mr-1" />
          {{ formatRelativeTime(paste.expires_at) }}
        </span>
      </div>
    </div>
    </template>
  </div>
</template>

<style scoped>
/* 编辑态 textarea 自适应内容高度，叠加最大高度限制 */
.paste-edit-textarea {
  field-sizing: content;
}

/* line-clamp 工具类（Tailwind 3.x 已内置，这里作为备用） */
.line-clamp-2 {
  display: -webkit-box;
  -webkit-line-clamp: 2;
  -webkit-box-orient: vertical;
  overflow: hidden;
}

.line-clamp-8 {
  display: -webkit-box;
  -webkit-line-clamp: 8;
  -webkit-box-orient: vertical;
  overflow: hidden;
}

.line-clamp-10 {
  display: -webkit-box;
  -webkit-line-clamp: 10;
  -webkit-box-orient: vertical;
  overflow: hidden;
}
</style>
