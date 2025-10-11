<template>
  <Teleport to="body">
    <div v-if="isOpen" class="fixed inset-0 z-[70] overflow-auto bg-black bg-opacity-50 flex items-center justify-center p-4" @click="handleBackdropClick">
      <div class="relative w-full max-w-2xl rounded-lg shadow-xl" :class="darkMode ? 'bg-gray-800' : 'bg-white'" @click.stop>
        <!-- 标题栏 -->
        <div class="px-6 py-4 border-b" :class="darkMode ? 'border-gray-700' : 'border-gray-200'">
          <h3 class="text-lg font-semibold" :class="darkMode ? 'text-gray-100' : 'text-gray-900'">{{ t("common.dialogs.selectUpload.title") }}</h3>
          <p class="text-sm mt-1" :class="darkMode ? 'text-gray-400' : 'text-gray-500'">{{ t("common.dialogs.selectUpload.description", { count: uploads.length }) }}</p>
        </div>

        <!-- 上传选项列表 -->
        <div class="px-6 py-4">
          <div class="max-h-96 overflow-y-auto space-y-3 custom-scrollbar">
            <div
              v-for="(upload, index) in uploads"
              :key="upload.uploadId || index"
              class="p-4 border rounded-lg cursor-pointer transition-all duration-200"
              :class="[
                selectedIndex === index
                  ? darkMode
                    ? 'border-blue-500 bg-blue-900/20 ring-1 ring-blue-500/50'
                    : 'border-blue-500 bg-blue-50 ring-1 ring-blue-500/50'
                  : darkMode
                  ? 'border-gray-600 hover:border-gray-500 hover:bg-gray-700/50'
                  : 'border-gray-200 hover:border-gray-300 hover:bg-gray-50',
                index === 0 ? 'ring-2 ring-green-200 dark:ring-green-800' : '',
              ]"
              @click="selectedIndex = index"
            >
              <div class="flex items-center justify-between">
                <div class="flex-1 min-w-0">
                  <!-- 文件路径和推荐标签 -->
                  <div class="flex items-center gap-2 mb-2">
                    <span class="font-medium truncate" :class="darkMode ? 'text-gray-100' : 'text-gray-900'">
                      {{ upload.key }}
                    </span>
                    <span v-if="index === 0" class="px-2 py-1 text-xs rounded-full bg-green-100 text-green-800 dark:bg-green-900/30 dark:text-green-300 flex-shrink-0">
                      {{ t("common.dialogs.selectUpload.recommended") }}
                    </span>
                  </div>

                  <!-- 详细信息 -->
                  <div class="space-y-2">
                    <!-- 第一行：时间信息 -->
                    <div class="text-sm" :class="darkMode ? 'text-gray-400' : 'text-gray-500'">
                      <span class="inline-flex items-center gap-1">
                        <svg class="w-4 h-4 flex-shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z" />
                        </svg>
                        {{ formatDate(upload.initiated) }}
                      </span>
                    </div>

                    <!-- 第二行：匹配度、分片信息和ID -->
                    <div class="flex items-center gap-3 text-sm flex-wrap" :class="darkMode ? 'text-gray-400' : 'text-gray-500'">
                      <!-- 匹配度 -->
                      <span v-if="showMatchScore" class="inline-flex items-center gap-1">
                        <svg class="w-4 h-4 flex-shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z" />
                        </svg>
                        {{ t("common.dialogs.selectUpload.matchScore", { score: getMatchScoreDisplay(upload, index) }) }}
                      </span>

                      <!-- 分隔符 -->
                      <span v-if="showMatchScore" class="text-gray-300 dark:text-gray-600">•</span>

                      <!-- 已上传分片数量 -->
                      <span class="inline-flex items-center gap-1">
                        <svg class="w-4 h-4 flex-shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                          <path
                            stroke-linecap="round"
                            stroke-linejoin="round"
                            stroke-width="2"
                            d="M7 21h10a2 2 0 002-2V9.414a1 1 0 00-.293-.707l-5.414-5.414A1 1 0 0012.586 3H7a2 2 0 00-2 2v14a2 2 0 002 2z"
                          />
                        </svg>
                        {{ getUploadedPartsDisplay(upload) }}
                      </span>

                      <!-- 分隔符和上传ID -->
                      <template v-if="upload.uploadId">
                        <span class="text-gray-300 dark:text-gray-600">•</span>
                        <button
                          @click.stop="copyUploadId(upload.uploadId)"
                          class="text-xs hover:text-blue-600 dark:hover:text-blue-400 transition-colors cursor-pointer inline-flex items-center gap-1"
                          :title="t('common.dialogs.selectUpload.copyIdTooltip', { id: upload.uploadId })"
                        >
                          <span>ID {{ formatUploadId(upload.uploadId) }}</span>
                          <svg class="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                            <path
                              stroke-linecap="round"
                              stroke-linejoin="round"
                              stroke-width="2"
                              d="M8 16H6a2 2 0 01-2-2V6a2 2 0 012-2h8a2 2 0 012 2v2m-6 12h8a2 2 0 002-2v-8a2 2 0 00-2-2h-8a2 2 0 00-2 2v8a2 2 0 002 2z"
                            />
                          </svg>
                        </button>
                      </template>
                    </div>
                  </div>
                </div>

                <!-- 选择指示器 -->
                <div class="ml-4 flex-shrink-0">
                  <div
                    class="w-5 h-5 rounded-full border-2 flex items-center justify-center"
                    :class="selectedIndex === index ? 'border-blue-500 bg-blue-500' : darkMode ? 'border-gray-500' : 'border-gray-300'"
                  >
                    <div v-if="selectedIndex === index" class="w-2 h-2 bg-white rounded-full"></div>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>

        <!-- 操作按钮 -->
        <div class="px-6 py-4 border-t flex justify-end space-x-3" :class="darkMode ? 'border-gray-700' : 'border-gray-200'">
          <button
            @click="handleCancel"
            class="px-4 py-2 text-sm font-medium rounded-md transition-colors"
            :class="darkMode ? 'text-gray-300 hover:text-white hover:bg-gray-700' : 'text-gray-600 hover:text-gray-800 hover:bg-gray-100'"
          >
            {{ t("common.dialogs.selectUpload.reupload") }}
          </button>
          <button
            @click="handleConfirm"
            class="px-4 py-2 text-sm font-medium rounded-md transition-colors"
            :class="darkMode ? 'bg-blue-600 hover:bg-blue-700 text-white' : 'bg-blue-600 hover:bg-blue-700 text-white'"
          >
            {{ t("common.dialogs.selectUpload.resumeSelected") }}
          </button>
        </div>
      </div>
    </div>
  </Teleport>
</template>

<script setup>
import { ref, watch, onMounted, onUnmounted } from "vue";
import { useI18n } from "vue-i18n";

// 国际化
const { t } = useI18n();

const props = defineProps({
  isOpen: {
    type: Boolean,
    default: false,
  },
  file: {
    type: Object,
    default: null,
  },
  uploads: {
    type: Array,
    default: () => [],
  },
  showMatchScore: {
    type: Boolean,
    default: true,
  },
  darkMode: {
    type: Boolean,
    default: false,
  },
  allowBackdropClose: {
    type: Boolean,
    default: true,
  },
});

const emit = defineEmits(["select", "cancel", "close"]);

// 选中的索引
const selectedIndex = ref(-1);

// 监听打开状态，重置选择
watch(
  () => props.isOpen,
  (newValue) => {
    if (newValue) {
      // 默认选中第一个（最佳匹配）
      selectedIndex.value = props.uploads.length > 0 ? 0 : -1;
    } else {
      selectedIndex.value = -1;
    }
  }
);

// 格式化日期
const formatDate = (dateString) => {
  try {
    return new Date(dateString).toLocaleString("zh-CN", {
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
    });
  } catch (error) {
    return dateString;
  }
};

// 获取匹配分数显示
const getMatchScoreDisplay = (upload, index) => {
  //使用 ServerResumePlugin 计算的真实匹配分数
  if (upload.matchScore !== undefined) {
    return (upload.matchScore * 100).toFixed(1);
  }

  // 备用计算：如果没有预计算的分数，使用简化算法
  const baseScore = 95 - index * 5;
  return Math.max(baseScore, 60).toFixed(1);
};

// 获取已上传分片数量的精确显示
const getUploadedPartsDisplay = (upload) => {
  // 1. 优先使用服务器返回的分片信息（最准确）
  if (upload.uploadedParts && Array.isArray(upload.uploadedParts)) {
    const partCount = upload.uploadedParts.length;
    if (partCount > 0) {
      // 计算总分片数（假设5MB分片大小）
      const partSize = 5 * 1024 * 1024; // 5MB
      const totalParts = upload.fileSize ? Math.ceil(upload.fileSize / partSize) : "?";
      return t("common.dialogs.selectUpload.partsInfo", { count: partCount, total: totalParts });
    }
  }

  // 2. 使用 parts 字段（S3 ListParts 返回的格式）
  if (upload.parts && Array.isArray(upload.parts)) {
    const partCount = upload.parts.length;
    if (partCount > 0) {
      const partSize = 5 * 1024 * 1024;
      const totalParts = upload.fileSize ? Math.ceil(upload.fileSize / partSize) : "?";
      return t("common.dialogs.selectUpload.partsInfo", { count: partCount, total: totalParts });
    }
  }

  // 3. 使用进度信息计算
  if (upload.progress && upload.progress.uploadedBytes && upload.progress.totalBytes) {
    const percentage = ((upload.progress.uploadedBytes / upload.progress.totalBytes) * 100).toFixed(1);
    const uploadedMB = (upload.progress.uploadedBytes / (1024 * 1024)).toFixed(1);
    const totalMB = (upload.progress.totalBytes / (1024 * 1024)).toFixed(1);
    return t("common.dialogs.selectUpload.progressInfo", { percentage, uploaded: uploadedMB, total: totalMB });
  }

  // 4. 使用单个分片编号（不太准确，但总比没有好）
  if (upload.partNumber && typeof upload.partNumber === "number") {
    return t("common.dialogs.selectUpload.atLeastParts", { count: upload.partNumber });
  }

  // 5. 默认显示（最不准确）
  return t("common.dialogs.selectUpload.partialComplete");
};

// 格式化上传ID显示
const formatUploadId = (uploadId) => {
  if (!uploadId) return "";

  // 如果ID很长，显示前6位...后4位的格式
  if (uploadId.length > 8) {
    return `${uploadId.substring(0, 6)}...${uploadId.substring(uploadId.length - 4)}`;
  }

  // 短ID直接显示
  return uploadId;
};

// 制上传ID到剪贴板
const copyUploadId = async (uploadId) => {
  try {
    await navigator.clipboard.writeText(uploadId);
  } catch (error) {
    console.error("复制失败:", error);
    // 降级方案：使用传统的复制方法
    try {
      const textArea = document.createElement("textarea");
      textArea.value = uploadId;
      document.body.appendChild(textArea);
      textArea.select();
      document.execCommand("copy");
      document.body.removeChild(textArea);
      console.log("上传ID已复制到剪贴板 (降级方案):", uploadId);
    } catch (fallbackError) {
      console.error("降级复制也失败:", fallbackError);
    }
  }
};

// 事件处理
const handleConfirm = () => {
  if (selectedIndex.value >= 0) {
    const selectedUpload = props.uploads[selectedIndex.value];
    emit("select", selectedUpload);
    emit("close");
  }
};

const handleCancel = () => {
  emit("cancel");
  emit("close");
};

const handleBackdropClick = () => {
  if (props.allowBackdropClose) {
    handleCancel();
  }
};

// 键盘导航
const handleKeydown = (event) => {
  if (!props.isOpen) return;

  switch (event.key) {
    case "Escape":
      handleCancel();
      break;
    case "Enter":
      if (selectedIndex.value >= 0) {
        handleConfirm();
      }
      break;
    case "ArrowUp":
      event.preventDefault();
      if (selectedIndex.value > 0) {
        selectedIndex.value--;
      }
      break;
    case "ArrowDown":
      event.preventDefault();
      if (selectedIndex.value < props.uploads.length - 1) {
        selectedIndex.value++;
      }
      break;
  }
};

// 生命周期
onMounted(() => {
  document.addEventListener("keydown", handleKeydown);
});

onUnmounted(() => {
  document.removeEventListener("keydown", handleKeydown);
});
</script>

<style scoped>
/* 自定义滚动条 */
.custom-scrollbar {
  scrollbar-width: thin;
  scrollbar-color: rgba(156, 163, 175, 0.5) transparent;
}

.custom-scrollbar::-webkit-scrollbar {
  width: 8px;
}

.custom-scrollbar::-webkit-scrollbar-track {
  background: transparent;
}

.custom-scrollbar::-webkit-scrollbar-thumb {
  background-color: rgba(156, 163, 175, 0.5);
  border-radius: 4px;
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
</style>
