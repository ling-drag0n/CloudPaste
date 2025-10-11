<template>
  <!-- 公告弹窗 - 主流设计模式 -->
  <Teleport to="body">
    <Transition name="modal" appear>
      <div
        v-if="showModal"
        class="fixed inset-0 z-[70] overflow-auto bg-black bg-opacity-50 flex items-center justify-center p-3 sm:p-4 pt-16 sm:pt-4"
        @click="handleBackdropClick"
      >
        <div
          class="relative w-full max-w-sm sm:max-w-md lg:max-w-lg bg-white dark:bg-gray-800 rounded-lg shadow-xl transform transition-all max-h-[60vh] sm:max-h-[55vh] flex flex-col"
          @click.stop
          role="dialog"
          aria-modal="true"
          :aria-labelledby="titleId"
        >
          <!-- 标题栏 -->
          <div class="px-4 sm:px-6 py-3 sm:py-4 border-b border-gray-200 dark:border-gray-700">
            <div class="flex items-center justify-between">
              <div class="flex items-center">
                <!-- 公告图标 -->
                <div class="flex-shrink-0 w-8 h-8 bg-blue-100 dark:bg-blue-900/30 rounded-full flex items-center justify-center mr-3">
                  <svg class="w-5 h-5 text-blue-600 dark:text-blue-400" fill="currentColor" viewBox="0 0 20 20">
                    <path
                      fill-rule="evenodd"
                      d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-7-4a1 1 0 11-2 0 1 1 0 012 0zM9 9a1 1 0 000 2v3a1 1 0 001 1h1a1 1 0 100-2v-3a1 1 0 00-1-1H9z"
                      clip-rule="evenodd"
                    />
                  </svg>
                </div>
                <h3 :id="titleId" class="text-base sm:text-lg font-semibold text-gray-900 dark:text-white">
                  {{ t("announcement.title") }}
                </h3>
              </div>

              <!-- 关闭按钮 -->
              <button @click="closeModal" class="text-gray-400 hover:text-gray-500 dark:text-gray-500 dark:hover:text-gray-400 transition-colors" :aria-label="t('common.close')">
                <svg class="w-6 h-6" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
                </svg>
              </button>
            </div>
          </div>

          <!-- 内容区域 - 可滚动 -->
          <div class="flex-1 overflow-y-auto px-4 sm:px-6 py-3 sm:py-4 min-h-0 custom-scrollbar">
            <div ref="contentRef" class="vditor-reset markdown-body"></div>
          </div>

          <!-- 底部操作栏 - 固定在底部 -->
          <div class="flex-shrink-0 px-4 sm:px-6 py-3 sm:py-4 bg-gray-50 dark:bg-gray-700/50 rounded-b-lg border-t border-gray-200 dark:border-gray-700">
            <div class="flex items-center justify-between">
              <!-- 不再显示选项 -->
              <label class="flex items-center cursor-pointer">
                <input type="checkbox" v-model="dontShowAgain" class="sr-only" />
                <div class="relative flex items-center justify-center">
                  <div
                    class="w-4 h-4 border-2 rounded transition-colors flex items-center justify-center"
                    :class="dontShowAgain ? 'bg-blue-600 border-blue-600 dark:bg-blue-500 dark:border-blue-500' : 'border-gray-300 dark:border-gray-600'"
                  >
                    <svg v-if="dontShowAgain" class="w-2.5 h-2.5 text-white" fill="currentColor" viewBox="0 0 20 20">
                      <path
                        fill-rule="evenodd"
                        d="M16.707 5.293a1 1 0 010 1.414l-8 8a1 1 0 01-1.414 0l-4-4a1 1 0 011.414-1.414L8 12.586l7.293-7.293a1 1 0 011.414 0z"
                        clip-rule="evenodd"
                      />
                    </svg>
                  </div>
                </div>
                <span class="ml-2 text-xs sm:text-sm text-gray-600 dark:text-gray-400">
                  {{ t("announcement.dontShowAgain") }}
                </span>
              </label>

              <!-- 确定按钮 -->
              <button
                @click="closeModal"
                class="px-3 sm:px-4 py-1.5 sm:py-2 bg-blue-600 hover:bg-blue-700 text-white text-xs sm:text-sm font-medium rounded-md transition-colors focus:outline-none focus:ring-2 focus:ring-blue-500 focus:ring-offset-2 dark:focus:ring-offset-gray-800"
              >
                {{ t("announcement.gotIt") }}
              </button>
            </div>
          </div>
        </div>
      </div>
    </Transition>
  </Teleport>
</template>

<script setup>
import { ref, computed, onMounted, onUnmounted, watch, nextTick } from "vue";
import { useI18n } from "vue-i18n";

const props = defineProps({
  content: {
    type: String,
    default: "",
  },
  enabled: {
    type: Boolean,
    default: false,
  },
  darkMode: {
    type: Boolean,
    default: false,
  },
});

const { t } = useI18n();
const showModal = ref(false);
const dontShowAgain = ref(false);
const contentRef = ref(null);

// 生成唯一的标题ID
const titleId = `announcement-title-${Math.random().toString(36).substr(2, 9)}`;

// 用户关闭状态管理
const STORAGE_KEY = "cloudpaste_announcement_dismissed";
const MAX_DISMISSED_COUNT = 7; // 最多记住7个公告

// 生成内容唯一标识
const getContentKey = (content) => {
  if (!content) return "";
  // 使用完整内容生成唯一标识，支持中文字符
  // 任何内容变化都会生成新的哈希，确保更新后重新显示
  let hash = 0;
  for (let i = 0; i < content.length; i++) {
    const char = content.charCodeAt(i);
    hash = (hash << 5) - hash + char;
    hash = hash & hash; // 转换为32位整数
  }
  return Math.abs(hash).toString(36); // 转换为36进制字符串
};

// 检查是否已被用户关闭
const isDismissed = (contentKey) => {
  if (!contentKey) return false;
  const dismissed = localStorage.getItem(STORAGE_KEY);
  return dismissed && dismissed.includes(contentKey);
};

// 标记为已关闭
const markDismissed = (contentKey) => {
  if (!contentKey) return;

  const dismissed = localStorage.getItem(STORAGE_KEY) || "";
  let dismissedArray = dismissed ? dismissed.split(",").filter(Boolean) : [];

  // 添加新的哈希（如果不存在）
  if (!dismissedArray.includes(contentKey)) {
    dismissedArray.push(contentKey);
  }

  // 限制数量，保留最新的10个
  if (dismissedArray.length > MAX_DISMISSED_COUNT) {
    dismissedArray = dismissedArray.slice(-MAX_DISMISSED_COUNT);
  }

  localStorage.setItem(STORAGE_KEY, dismissedArray.join(","));
};

// 加载 Vditor CSS
const loadVditorCSS = async () => {
  const existingLink = document.querySelector('link[href="/assets/vditor/dist/index.css"]');
  if (!existingLink) {
    const link = document.createElement("link");
    link.rel = "stylesheet";
    link.href = "/assets/vditor/dist/index.css";
    document.head.appendChild(link);
  }
};

// 使用 Vditor 渲染 Markdown 内容
const renderContent = async () => {
  if (!contentRef.value || !props.content) return;

  try {
    // 加载 CSS
    await loadVditorCSS();

    // 清空之前的内容
    contentRef.value.innerHTML = "";

    // 动态导入 Vditor
    const { default: Vditor } = await import("vditor");

    // 使用 Vditor 的预览功能渲染 Markdown
    Vditor.preview(contentRef.value, props.content, {
      cdn: "/assets/vditor",
      theme: {
        current: props.darkMode ? "dark" : "light",
        path: "/assets/vditor/dist/css/content-theme",
      },
      hljs: {
        lineNumber: false,
        style: props.darkMode ? "vs2015" : "github",
      },
      markdown: {
        toc: false,
        mark: true,
        footnotes: false,
        autoSpace: true,
        listStyle: true,
        task: true,
        paragraphBeginningSpace: true,
        fixTermTypo: true,
        media: false,
      },
      math: {
        engine: "KaTeX",
        inlineDigit: true,
      },
    });
  } catch (error) {
    console.error("Vditor 渲染失败:", error);
    // 降级到简单渲染
    if (contentRef.value) {
      contentRef.value.innerHTML = `<div class="text-gray-700 dark:text-gray-300 leading-relaxed whitespace-pre-wrap">${props.content}</div>`;
    }
  }
};

// 关闭弹窗
const closeModal = () => {
  if (props.content) {
    const contentKey = getContentKey(props.content);
    if (dontShowAgain.value) {
      markDismissed(contentKey);
    }
  }
  showModal.value = false;
};

// 背景点击关闭
const handleBackdropClick = () => {
  closeModal();
};

// 键盘事件处理
const handleKeydown = (event) => {
  if (event.key === "Escape" && showModal.value) {
    closeModal();
  }
};

// 检查是否应该显示公告
const checkShouldShow = async () => {
  if (props.enabled && props.content) {
    const contentKey = getContentKey(props.content);
    if (!isDismissed(contentKey)) {
      // 延迟显示，让页面先加载完成
      await nextTick();
      setTimeout(async () => {
        showModal.value = true;
        // 等待弹窗显示后再渲染内容
        await nextTick();
        renderContent();
      }, 500); // 页面加载后0.5秒显示
    }
  }
};

onMounted(() => {
  checkShouldShow();
  document.addEventListener("keydown", handleKeydown);
});

onUnmounted(() => {
  document.removeEventListener("keydown", handleKeydown);
});

// 监听 props 变化
watch(
  () => [props.enabled, props.content],
  () => {
    checkShouldShow();
  }
);

// 监听暗色模式变化，重新渲染内容
watch(
  () => props.darkMode,
  () => {
    if (showModal.value && contentRef.value) {
      renderContent();
    }
  }
);
</script>

<style scoped>
/* 模态框动画 */
.modal-enter-active {
  transition: all 0.3s ease-out;
}

.modal-leave-active {
  transition: all 0.2s ease-in;
}

.modal-enter-from {
  opacity: 0;
  transform: scale(0.9) translateY(-20px);
}

.modal-leave-to {
  opacity: 0;
  transform: scale(0.95) translateY(-10px);
}

/* 自定义滚动条样式 */
.custom-scrollbar {
  scrollbar-width: thin;
  scrollbar-color: rgba(156, 163, 175, 0.5) transparent;
}

.custom-scrollbar::-webkit-scrollbar {
  width: 6px;
}

.custom-scrollbar::-webkit-scrollbar-track {
  background: transparent;
}

.custom-scrollbar::-webkit-scrollbar-thumb {
  background-color: rgba(156, 163, 175, 0.5);
  border-radius: 3px;
  transition: background-color 0.2s ease;
}

.custom-scrollbar::-webkit-scrollbar-thumb:hover {
  background-color: rgba(156, 163, 175, 0.7);
}

/* 暗色模式滚动条 */
.dark .custom-scrollbar {
  scrollbar-color: rgba(75, 85, 99, 0.5) transparent;
}

.dark .custom-scrollbar::-webkit-scrollbar-thumb {
  background-color: rgba(75, 85, 99, 0.5);
}

.dark .custom-scrollbar::-webkit-scrollbar-thumb:hover {
  background-color: rgba(75, 85, 99, 0.7);
}

/* 确保暗色模式正确显示 */
:deep(.vditor-reset) {
  color: v-bind('props.darkMode ? "#d4d4d4" : "#374151"') !important;
  background-color: transparent !important;
  font-size: 1rem !important;
  line-height: 1.7 !important;
  transition: all 0.3s ease;
}

/* 确保暗色模式下的特定样式 */
:deep(.vditor-reset--dark) {
  color: #d4d4d4 !important;
  background-color: transparent !important;
}

/* 确保亮色模式下的特定样式 */
:deep(.vditor-reset--light) {
  color: #374151 !important;
  background-color: transparent !important;
}

/* 表格暗色模式背景 */
:deep(.vditor-reset table),
:deep(.vditor-reset thead),
:deep(.vditor-reset tbody),
:deep(.vditor-reset tr),
:deep(.vditor-reset th),
:deep(.vditor-reset td) {
  background-color: v-bind('props.darkMode ? "transparent" : ""') !important;
  background: v-bind('props.darkMode ? "transparent" : ""') !important;
}
</style>
