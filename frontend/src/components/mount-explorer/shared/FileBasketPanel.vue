<template>
  <div v-if="isOpen" class="fixed inset-0 z-[60] bg-black bg-opacity-50 flex items-center justify-center p-2 sm:p-4 pt-20 sm:pt-4">
    <div class="relative w-full max-w-sm sm:max-w-md bg-white dark:bg-gray-800 rounded-lg shadow-xl max-h-[85vh] sm:max-h-[80vh] flex flex-col overflow-hidden">
      <!-- 标题栏 -->
      <div class="flex-shrink-0 px-4 py-3 border-b flex justify-between items-center" :class="darkMode ? 'border-gray-700' : 'border-gray-200'">
        <h3 class="text-lg font-medium" :class="darkMode ? 'text-gray-100' : 'text-gray-900'">
          {{ t("fileBasket.panel.title") }}
        </h3>
        <button @click="close" class="text-gray-400 hover:text-gray-500 dark:text-gray-500 dark:hover:text-gray-400">
          <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
          </svg>
        </button>
      </div>

      <!-- 统计信息 -->
      <div v-if="hasCollection" class="px-4 py-3 border-b" :class="darkMode ? 'bg-blue-900/20 border-gray-700' : 'bg-blue-50 border-gray-200'">
        <div class="flex items-center justify-between">
          <div>
            <div class="text-sm" :class="darkMode ? 'text-blue-200' : 'text-blue-800'">{{ collectionCount }} 个文件，来自 {{ directoryCount }} 个目录</div>
            <div class="text-xs mt-1" :class="darkMode ? 'text-blue-300' : 'text-blue-600'">总大小：{{ collectionTotalSizeMB }} MB</div>
          </div>
          <!-- 全部展开/收起按钮 -->
          <button
            @click="toggleAllDirectories"
            class="text-xs px-2 py-1 rounded transition-colors"
            :class="darkMode ? 'text-blue-300 hover:text-blue-200 hover:bg-blue-800/30' : 'text-blue-600 hover:text-blue-700 hover:bg-blue-100'"
          >
            {{ allDirectoriesExpanded ? "全部收起" : "全部展开" }}
          </button>
        </div>
      </div>

      <!-- 内容区 -->
      <div class="flex-1 overflow-y-auto p-3 sm:p-4" style="max-height: calc(85vh - 200px)">
        <!-- 空状态 -->
        <div v-if="!hasCollection" class="text-center py-8" :class="darkMode ? 'text-gray-400' : 'text-gray-600'">
          <!-- 文件列表图标 (与按钮保持一致) -->
          <svg class="h-12 w-12 mx-auto mb-3 opacity-30" fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2">
            <path
              stroke-linecap="round"
              stroke-linejoin="round"
              d="M15.5 2H8.6c-.4 0-.8.2-1.1.5-.3.3-.5.7-.5 1.1v12.8c0 .4.2.8.5 1.1.3.3.7.5 1.1.5h9.8c.4 0 .8-.2 1.1-.5.3-.3.5-.7.5-1.1V6.5L15.5 2z"
            />
            <path stroke-linecap="round" stroke-linejoin="round" d="M3 7.6v12.8c0 .4.2.8.5 1.1.3.3.7.5 1.1.5h9.8" />
            <path stroke-linecap="round" stroke-linejoin="round" d="M15 2v5h5" />
          </svg>
          <p class="font-medium">{{ t("fileBasket.panel.empty") }}</p>
          <p class="text-xs mt-1 opacity-75">{{ t("fileBasket.panel.emptyDescription") }}</p>
        </div>

        <!-- 文件列表 -->
        <div v-else class="space-y-3">
          <!-- 按目录分组显示 -->
          <div v-for="(files, directory) in filesByDirectory" :key="directory" class="border rounded-lg overflow-hidden" :class="darkMode ? 'border-gray-700' : 'border-gray-200'">
            <!-- 目录标题 - 可点击展开/收起 -->
            <div
              @click="toggleDirectory(directory)"
              class="px-3 py-2 text-sm font-medium flex items-center justify-between cursor-pointer hover:bg-opacity-80 transition-colors"
              :class="darkMode ? 'bg-gray-750 text-gray-300 hover:bg-gray-700' : 'bg-gray-50 text-gray-700 hover:bg-gray-100'"
            >
              <div class="flex items-center space-x-2">
                <!-- 展开/收起箭头 -->
                <svg
                  class="w-4 h-4 transition-transform duration-200"
                  :class="isDirectoryExpanded(directory) ? 'rotate-90' : ''"
                  fill="none"
                  stroke="currentColor"
                  viewBox="0 0 24 24"
                >
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5l7 7-7 7" />
                </svg>
                <!-- 文件夹图标 -->
                <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3 7v10a2 2 0 002 2h14a2 2 0 002-2V9a2 2 0 00-2-2h-5l-2-2H5a2 2 0 00-2 2z" />
                </svg>
                <span class="truncate">{{ directory }}</span>
              </div>
              <span class="text-xs opacity-75 flex-shrink-0">{{ (files || []).length }} 个文件</span>
            </div>

            <!-- 文件列表 - 根据展开状态显示 -->
            <div v-if="isDirectoryExpanded(directory)" class="divide-y" :class="darkMode ? 'divide-gray-700' : 'divide-gray-200'">
              <div
                v-for="file in getValidFiles(files)"
                :key="file.uniqueId"
                class="px-3 py-2 flex items-center justify-between hover:bg-opacity-50 group"
                :class="darkMode ? 'hover:bg-gray-700' : 'hover:bg-gray-100'"
              >
                <div class="flex items-center space-x-2 flex-1 min-w-0">
                  <!-- 文件图标 -->
                  <div class="w-5 h-5 flex-shrink-0" v-html="getFileIcon(file, darkMode)"></div>

                  <!-- 文件信息 -->
                  <div class="flex-1 min-w-0">
                    <p class="text-sm font-medium truncate" :class="darkMode ? 'text-gray-200' : 'text-gray-900'">
                      {{ file.name }}
                    </p>
                    <p class="text-xs" :class="darkMode ? 'text-gray-400' : 'text-gray-500'">
                      {{ formatFileSize(file.size) }}
                    </p>
                  </div>
                </div>

                <!-- 移除按钮 -->
                <button
                  @click="removeFile(file.path)"
                  class="opacity-0 group-hover:opacity-100 p-1 rounded transition-all duration-200 flex-shrink-0"
                  :class="darkMode ? 'text-red-400 hover:text-red-300 hover:bg-red-900/30' : 'text-red-500 hover:text-red-600 hover:bg-red-50'"
                  :title="t('fileBasket.actions.remove')"
                >
                  <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
                  </svg>
                </button>
              </div>
            </div>
          </div>
        </div>
      </div>

      <!-- 底部操作栏 -->
      <div v-if="hasCollection" class="px-4 py-3 border-t space-y-2" :class="darkMode ? 'border-gray-700' : 'border-gray-200'">
        <!-- 主要操作按钮 -->
        <button
          @click="handlePackDownload"
          :disabled="isProcessing"
          class="w-full flex items-center justify-center space-x-2 px-4 py-2 rounded-md font-medium transition-colors"
          :class="[isProcessing ? 'bg-gray-400 text-gray-200 cursor-not-allowed' : 'bg-blue-600 hover:bg-blue-700 text-white']"
        >
          <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path
              stroke-linecap="round"
              stroke-linejoin="round"
              stroke-width="2"
              d="M12 10v6m0 0l-3-3m3 3l3-3m2 8H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z"
            />
          </svg>
          <span>{{ isProcessing ? t("fileBasket.status.processing") : t("fileBasket.actions.packDownload") }}</span>
        </button>

        <!-- 次要操作按钮 -->
        <button
          @click="handleClearBasket"
          :disabled="isProcessing"
          class="w-full px-4 py-2 rounded-md font-medium transition-colors"
          :class="[darkMode ? 'bg-gray-700 hover:bg-gray-600 text-gray-300' : 'bg-gray-200 hover:bg-gray-300 text-gray-700', isProcessing ? 'opacity-50 cursor-not-allowed' : '']"
        >
          {{ t("fileBasket.actions.clear") }}
        </button>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, watch, onUnmounted } from "vue";
import { useI18n } from "vue-i18n";
import { storeToRefs } from "pinia";
import { useFileBasket } from "../../../composables/file-system/useFileBasket.js";
import { formatFileSize } from "@/utils/fileUtils.js";
import { getFileIcon } from "../../../utils/fileTypeIcons.js";

const { t } = useI18n();

const props = defineProps({
  isOpen: {
    type: Boolean,
    default: false,
  },
  darkMode: {
    type: Boolean,
    default: false,
  },
});

const emit = defineEmits(["close", "task-created", "show-message"]);

// 使用文件篮composable
const fileBasket = useFileBasket();

// 直接从store获取计算属性，避免storeToRefs的问题
const filesByDirectory = computed(() => fileBasket.filesByDirectory.value);
const collectionCount = computed(() => fileBasket.collectionCount.value);
const directoryCount = computed(() => fileBasket.directoryCount.value);
const collectionTotalSizeMB = computed(() => fileBasket.collectionTotalSizeMB.value);
const hasCollection = computed(() => fileBasket.hasCollection.value);

// 本地状态
const isProcessing = ref(false);

// 关闭面板
const close = () => {
  emit("close");
};

// 过滤有效的文件对象
const getValidFiles = (files) => {
  if (!Array.isArray(files)) return [];

  return files.filter((file) => {
    // 严格验证文件对象
    if (!file || typeof file !== "object") return false;
    if (!file.name || typeof file.name !== "string") return false;
    if (!file.uniqueId || typeof file.uniqueId !== "string") return false;
    if (!file.path || typeof file.path !== "string") return false;
    if (typeof file.size !== "number" || file.size < 0) return false;

    return true;
  });
};

// 文件夹展开状态管理
const expandedDirectories = ref(new Set());

// 切换文件夹展开状态
const toggleDirectory = (directory) => {
  if (expandedDirectories.value.has(directory)) {
    expandedDirectories.value.delete(directory);
  } else {
    expandedDirectories.value.add(directory);
  }
};

// 检查文件夹是否展开
const isDirectoryExpanded = (directory) => {
  return expandedDirectories.value.has(directory);
};

// 检查是否所有文件夹都已展开
const allDirectoriesExpanded = computed(() => {
  if (!filesByDirectory.value || typeof filesByDirectory.value !== "object") return false;
  const allDirectories = Object.keys(filesByDirectory.value);
  return allDirectories.length > 0 && allDirectories.every((dir) => expandedDirectories.value.has(dir));
});

// 全部展开/收起
const toggleAllDirectories = () => {
  if (!filesByDirectory.value || typeof filesByDirectory.value !== "object") return;

  const allDirectories = Object.keys(filesByDirectory.value);

  if (allDirectoriesExpanded.value) {
    // 全部收起
    expandedDirectories.value.clear();
  } else {
    // 全部展开
    allDirectories.forEach((directory) => {
      expandedDirectories.value.add(directory);
    });
  }
};

// 初始化展开状态 - 默认展开所有文件夹
watch(
  filesByDirectory,
  (newValue) => {
    if (newValue && typeof newValue === "object") {
      Object.keys(newValue).forEach((directory) => {
        if (!expandedDirectories.value.has(directory)) {
          expandedDirectories.value.add(directory);
        }
      });
    }
  },
  { immediate: true }
);

// 移除文件
const removeFile = (filePath) => {
  try {
    const result = fileBasket.removeFromBasket(filePath);
    if (result.success) {
      emit("show-message", { type: "success", message: result.message });
    } else {
      emit("show-message", { type: "error", message: result.message });
    }
  } catch (error) {
    console.error("移除文件失败:", error);
    emit("show-message", { type: "error", message: t("fileBasket.messages.removeFailed") });
  }
};

// 处理打包下载
const handlePackDownload = async () => {
  if (isProcessing.value) return;

  try {
    isProcessing.value = true;

    const result = await fileBasket.createPackTask();

    if (result.success) {
      emit("show-message", { type: "success", message: result.message });
      emit("task-created", result);
      close(); // 关闭面板
    } else {
      emit("show-message", { type: "error", message: result.message });
    }
  } catch (error) {
    console.error("创建打包任务失败:", error);
    emit("show-message", { type: "error", message: t("fileBasket.messages.taskCreateFailed") });
  } finally {
    isProcessing.value = false;
  }
};

// 处理清空篮子
const handleClearBasket = () => {
  if (isProcessing.value) return;

  const confirmed = confirm(t("fileBasket.confirmations.clearBasket"));
  if (!confirmed) return;

  try {
    const result = fileBasket.clearBasket();
    if (result.success) {
      emit("show-message", { type: "success", message: result.message });
      close(); // 关闭面板
    } else {
      emit("show-message", { type: "error", message: result.message });
    }
  } catch (error) {
    console.error("清空文件篮失败:", error);
    emit("show-message", { type: "error", message: t("fileBasket.messages.clearFailed") });
  }
};

// 组件卸载时清理
onUnmounted(() => {
  // 如果正在处理任务，尝试清理
  if (isProcessing.value) {
    isProcessing.value = false;
  }

  // 清理展开状态
  expandedDirectories.value.clear();

  console.log("FileBasketPanel组件已卸载并清理");
});
</script>
