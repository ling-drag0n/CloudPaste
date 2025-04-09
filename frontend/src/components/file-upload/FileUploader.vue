<template>
  <div class="file-uploader">
    <!-- 文件拖放区域 -->
    <div
        class="drop-zone mb-4 border-2 border-dashed rounded-lg flex flex-col items-center justify-center py-6 px-4 cursor-pointer transition-all duration-300"
        :class="[
        darkMode ? 'border-gray-600 hover:border-gray-500 bg-gray-800/30' : 'border-gray-300 hover:border-gray-400 bg-gray-50',
        isDragging ? (darkMode ? 'border-blue-500 bg-blue-500/10 pulsing-border' : 'border-blue-500 bg-blue-50 pulsing-border') : '',
      ]"
        @dragenter.prevent="onDragOver"
        @dragover.prevent="onDragOver"
        @dragleave.prevent="onDragLeave"
        @drop.prevent="onDrop"
        @click="triggerFileInput"
    >
      <div class="icon-container mb-3">
        <svg
            xmlns="http://www.w3.org/2000/svg"
            class="h-12 w-12 transition-colors duration-300"
            :class="[darkMode ? 'text-gray-400' : 'text-gray-500', isDragging ? (darkMode ? 'text-blue-400' : 'text-blue-500') : '']"
            fill="none"
            viewBox="0 0 24 24"
            stroke="currentColor"
        >
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.5" d="M7 16a4 4 0 01-.88-7.903A5 5 0 1115.9 6L16 6a5 5 0 011 9.9M15 13l-3-3m0 0l-3 3m3-3v12" />
        </svg>
      </div>
      <div class="text-center">
        <p
            class="text-base font-medium transition-colors duration-300"
            :class="[darkMode ? 'text-gray-300' : 'text-gray-700', isDragging ? (darkMode ? 'text-blue-300' : 'text-blue-700') : '']"
        >
          {{ isDragging ? t("file.drag") : t("file.select") }}
        </p>
        <p
            class="text-sm mt-1 transition-colors duration-300"
            :class="[darkMode ? 'text-gray-400' : 'text-gray-500', isDragging ? (darkMode ? 'text-blue-300/80' : 'text-blue-600/80') : '']"
        >
          {{ t("file.maxSizeExceeded", { size: formatMaxFileSize() }) }}
        </p>
        <p class="text-sm mt-1 font-medium" :class="darkMode ? 'text-blue-400' : 'text-blue-600'">
          {{ t("file.multipleFilesSupported") }}
        </p>
      </div>
      <input ref="fileInput" type="file" class="hidden" @change="onFileSelected" multiple />
    </div>

    <!-- 已选文件预览 -->
    <div v-if="selectedFiles.length > 0" class="selected-files mb-6">
      <div class="flex justify-between items-center mb-2">
        <h3 class="text-base font-medium" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">
          {{ t("file.selectedFiles", { count: selectedFiles.length }) }}
        </h3>
        <button
            @click="clearAllSelectedFiles"
            class="text-sm px-2 py-1 rounded transition-colors flex items-center"
            :class="darkMode ? 'text-gray-400 hover:text-white hover:bg-gray-700' : 'text-gray-600 hover:text-gray-900 hover:bg-gray-100'"
        >
          <svg xmlns="http://www.w3.org/2000/svg" class="h-4 w-4 mr-1" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
          </svg>
          {{ t("file.clearAll") }}
        </button>
      </div>

      <div class="space-y-2 max-h-60 overflow-y-auto pr-1 custom-scrollbar">
        <div 
          v-for="(file, index) in selectedFiles" 
          :key="index" 
          class="selected-file flex items-center p-3 rounded-md" 
          :class="darkMode ? 'bg-gray-700/50' : 'bg-gray-100'"
        >
          <div class="file-icon mr-3">
            <svg xmlns="http://www.w3.org/2000/svg" class="h-8 w-8" :class="darkMode ? 'text-gray-300' : 'text-gray-600'" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path
                  stroke-linecap="round"
                  stroke-linejoin="round"
                  stroke-width="2"
                  d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z"
              />
            </svg>
          </div>
          <div class="file-info flex-grow mr-3">
            <div class="font-medium truncate" :class="darkMode ? 'text-white' : 'text-gray-900'">
              {{ file.name }}
            </div>
            <div class="text-sm" :class="darkMode ? 'text-gray-400' : 'text-gray-500'">
              {{ formatFileSize(file.size) }}
            </div>
          </div>
          <button
              type="button"
              @click="removeSelectedFile(index)"
              class="p-1 rounded-full hover:bg-opacity-20 transition-colors"
              :class="darkMode ? 'hover:bg-gray-600 text-gray-400' : 'hover:bg-gray-200 text-gray-500'"
              :title="t('file.clearSelected')"
          >
            <svg xmlns="http://www.w3.org/2000/svg" class="h-5 w-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
            </svg>
          </button>
        </div>
      </div>
    </div>

    <!-- 上传选项表单 -->
    <div class="upload-form">
      <form @submit.prevent="submitUpload">
        <!-- S3配置选择 -->
        <div class="mb-6">
          <h3 class="text-lg font-medium mb-4" :class="darkMode ? 'text-gray-200' : 'text-gray-700'">{{ t("file.storage") }}</h3>
          <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div class="form-group flex flex-col">
              <label class="form-label text-sm font-medium mb-1.5" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">{{ t("file.storage") }}</label>
              <div class="relative">
                <select
                    v-model="formData.s3_config_id"
                    class="form-input w-full rounded-md shadow-sm focus:ring-2 focus:ring-offset-1 focus:border-transparent appearance-none"
                    :class="[
                    darkMode
                      ? 'bg-gray-700 border-gray-600 text-white focus:ring-blue-600 focus:ring-offset-gray-800'
                      : 'bg-white border-gray-300 text-gray-900 focus:ring-blue-500 focus:ring-offset-white',
                  ]"
                    :disabled="!s3Configs.length || loading || isUploading"
                    required
                >
                  <option value="" disabled selected>{{ s3Configs.length ? t("file.selectStorage") : t("file.noStorage") }}</option>
                  <option v-for="config in s3Configs" :key="config.id" :value="config.id">{{ config.name }} ({{ config.provider_type }})</option>
                </select>
                <div class="absolute inset-y-0 right-0 flex items-center pr-3 pointer-events-none">
                  <svg
                      xmlns="http://www.w3.org/2000/svg"
                      class="h-5 w-5"
                      :class="darkMode ? 'text-gray-400' : 'text-gray-500'"
                      fill="none"
                      viewBox="0 0 24 24"
                      stroke="currentColor"
                  >
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7" />
                  </svg>
                </div>
              </div>
            </div>

            <!-- 存储路径 -->
            <div class="form-group flex flex-col">
              <label class="form-label text-sm font-medium mb-1.5" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">{{ t("file.path") }}</label>
              <input
                  type="text"
                  v-model="formData.path"
                  class="form-input w-full rounded-md shadow-sm focus:ring-2 focus:ring-offset-1 focus:border-transparent"
                  :class="[
                  darkMode
                    ? 'bg-gray-700 border-gray-600 text-white focus:ring-blue-600 focus:ring-offset-gray-800'
                    : 'bg-white border-gray-300 text-gray-900 focus:ring-blue-500 focus:ring-offset-white',
                ]"
                  :placeholder="t('file.pathPlaceholder')"
                  :disabled="isUploading"
              />
            </div>
          </div>
        </div>

        <!-- 分享设置表单 -->
        <div class="mt-6 border-t pt-4 w-full overflow-hidden" :class="darkMode ? 'border-gray-700' : 'border-gray-200'">
          <h3 class="text-lg font-medium mb-4" :class="darkMode ? 'text-gray-200' : 'text-gray-700'">{{ t("file.shareSettings") }}</h3>

          <!-- 更改这里的布局，使用更灵活的网格系统，每个栏位使用相同的模式 -->
          <div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-5 gap-4">
            <!-- 每个表单组都使用一致的结构，增强布局的一致性 -->

            <!-- 备注 -->
            <div class="form-group flex flex-col">
              <label class="form-label text-sm font-medium mb-1.5" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">{{ t("file.remark") }}</label>
              <input
                  type="text"
                  v-model="formData.remark"
                  class="form-input w-full rounded-md shadow-sm focus:ring-2 focus:ring-offset-1 focus:border-transparent"
                  :class="[
                  darkMode
                    ? 'bg-gray-700 border-gray-600 text-white focus:ring-blue-600 focus:ring-offset-gray-800'
                    : 'bg-white border-gray-300 text-gray-900 focus:ring-blue-500 focus:ring-offset-white',
                ]"
                  :placeholder="t('file.remarkPlaceholder')"
                  :disabled="isUploading"
              />
            </div>

            <!-- 自定义链接 -->
            <div class="form-group flex flex-col">
              <label class="form-label text-sm font-medium mb-1.5" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">{{ t("file.customLink") }}</label>
              <input
                  type="text"
                  v-model="formData.slug"
                  class="form-input w-full rounded-md shadow-sm focus:ring-2 focus:ring-offset-1 focus:border-transparent"
                  :class="[
                  darkMode
                    ? 'bg-gray-700 border-gray-600 text-white focus:ring-blue-600 focus:ring-offset-gray-800'
                    : 'bg-white border-gray-300 text-gray-900 focus:ring-blue-500 focus:ring-offset-white',
                  slugError ? (darkMode ? 'border-red-500' : 'border-red-600') : '',
                ]"
                  :placeholder="t('file.customLinkPlaceholder')"
                  :disabled="isUploading"
                  @input="validateCustomLink"
              />
              <p v-if="slugError" class="mt-1 text-sm text-red-600 dark:text-red-400">{{ slugError }}</p>
              <p v-else class="mt-1 text-xs text-gray-500 dark:text-gray-400">{{ t("file.onlyAllowedChars") }}</p>
            </div>

            <!-- 密码保护 -->
            <div class="form-group flex flex-col">
              <label class="form-label text-sm font-medium mb-1.5" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">{{ t("file.passwordProtection") }}</label>
              <input
                  type="text"
                  v-model="formData.password"
                  class="form-input w-full rounded-md shadow-sm focus:ring-2 focus:ring-offset-1 focus:border-transparent"
                  :class="[
                  darkMode
                    ? 'bg-gray-700 border-gray-600 text-white focus:ring-blue-600 focus:ring-offset-gray-800'
                    : 'bg-white border-gray-300 text-gray-900 focus:ring-blue-500 focus:ring-offset-white',
                ]"
                  :placeholder="t('file.passwordPlaceholder')"
                  :disabled="isUploading"
              />
            </div>

            <!-- 过期时间 -->
            <div class="form-group flex flex-col">
              <label class="form-label text-sm font-medium mb-1.5" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">{{ t("file.expireTime") }}</label>
              <select
                  v-model="formData.expires_in"
                  class="form-input w-full rounded-md shadow-sm focus:ring-2 focus:ring-offset-1 focus:border-transparent"
                  :class="[
                  darkMode
                    ? 'bg-gray-700 border-gray-600 text-white focus:ring-blue-600 focus:ring-offset-gray-800'
                    : 'bg-white border-gray-300 text-gray-900 focus:ring-blue-500 focus:ring-offset-white',
                ]"
                  :disabled="isUploading"
              >
                <option value="1">{{ t("file.expireOptions.hour1") }}</option>
                <option value="24">{{ t("file.expireOptions.day1") }}</option>
                <option value="168">{{ t("file.expireOptions.day7") }}</option>
                <option value="720">{{ t("file.expireOptions.day30") }}</option>
                <option value="0">{{ t("file.expireOptions.never") }}</option>
              </select>
            </div>

            <!-- 最大查看次数 -->
            <div class="form-group flex flex-col">
              <label class="form-label text-sm font-medium mb-1.5" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">{{ t("file.maxViews") }}</label>
              <input
                  type="number"
                  v-model.number="formData.max_views"
                  min="0"
                  step="1"
                  pattern="\d*"
                  class="form-input w-full rounded-md shadow-sm focus:ring-2 focus:ring-offset-1 focus:border-transparent"
                  :class="[
                  darkMode
                    ? 'bg-gray-700 border-gray-600 text-white focus:ring-blue-600 focus:ring-offset-gray-800'
                    : 'bg-white border-gray-300 text-gray-900 focus:ring-blue-500 focus:ring-offset-white',
                ]"
                  :placeholder="t('file.maxViewsPlaceholder')"
                  :disabled="isUploading"
                  @input="validateMaxViews"
              />
            </div>
          </div>

          <!-- 上传进度 -->
          <div v-if="uploadProgress > 0 && isUploading" class="mt-4">
            <div class="flex justify-between items-center mb-1">
              <div class="flex items-center">
                <span class="text-sm mr-2" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">{{ t("file.uploadProgress") }}</span>
                <span v-if="uploadSpeed" class="text-xs px-2 py-0.5 rounded" :class="darkMode ? 'bg-blue-900/30 text-blue-200' : 'bg-blue-100 text-blue-700'">
                  {{ uploadSpeed }}
                </span>
              </div>
              <span class="text-sm font-medium" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">{{ uploadProgress }}%</span>
            </div>
            <div class="w-full bg-gray-200 rounded-full h-2.5 dark:bg-gray-700 overflow-hidden">
              <div
                  class="h-2.5 rounded-full transition-all duration-200 ease-out relative overflow-hidden"
                  :class="uploadProgress >= 95 ? 'bg-green-600' : 'bg-blue-600'"
                  :style="{ width: `${uploadProgress}%` }"
              >
                <div class="progress-stripes absolute inset-0 w-full h-full" :class="uploadProgress < 100 ? 'animate-progress-stripes' : ''"></div>
              </div>
            </div>
          </div>

          <!-- 表单按钮 -->
          <div class="submit-section mt-6 flex flex-row items-center gap-3">
            <button
                type="submit"
                :disabled="!selectedFiles.length || !formData.s3_config_id || isUploading || loading"
                class="btn-primary px-4 py-2 rounded-md font-medium focus:outline-none focus:ring-2 focus:ring-offset-2 transition-colors flex items-center justify-center min-w-[120px]"
                :class="[
                !selectedFiles.length || !formData.s3_config_id || isUploading || loading
                  ? darkMode
                    ? 'bg-gray-700 text-gray-400 cursor-not-allowed'
                    : 'bg-gray-200 text-gray-500 cursor-not-allowed'
                  : darkMode
                  ? 'bg-blue-600 hover:bg-blue-700 text-white focus:ring-blue-600 focus:ring-offset-gray-800'
                  : 'bg-blue-600 hover:bg-blue-700 text-white focus:ring-blue-500 focus:ring-offset-white',
              ]"
            >
              <svg v-if="isUploading" class="animate-spin -ml-1 mr-2 h-4 w-4" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
                <path
                    class="opacity-75"
                    fill="currentColor"
                    d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"
                ></path>
              </svg>
              {{ isUploading ? t("file.loading") : (selectedFiles.length > 1 ? t("file.uploadMultiple", { count: selectedFiles.length }) : t("file.upload")) }}
            </button>

            <!-- 将取消按钮放在上传按钮右侧 -->
            <button
                v-if="isUploading"
                type="button"
                @click="cancelUpload"
                class="px-4 py-2 rounded-md font-medium focus:outline-none focus:ring-2 focus:ring-offset-2 transition-colors flex items-center justify-center border"
                :class="
                darkMode
                  ? 'bg-gray-700 border-gray-600 text-gray-300 hover:bg-red-900/60 hover:text-red-200 hover:border-red-800 focus:ring-gray-500 focus:ring-offset-gray-800'
                  : 'bg-gray-100 border-gray-300 text-gray-700 hover:bg-red-50 hover:text-red-600 hover:border-red-300 focus:ring-gray-300 focus:ring-offset-white'
              "
            >
              <svg xmlns="http://www.w3.org/2000/svg" class="h-4 w-4 mr-1" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
              </svg>
              {{ t("file.cancel") }}
            </button>
          </div>
        </div>
      </form>
    </div>
  </div>
</template>

<script setup>
import { ref, reactive, defineProps, defineEmits, getCurrentInstance, onMounted, watch } from "vue";
import { api } from "../../api";
import { API_BASE_URL } from "../../api/config"; // 导入API_BASE_URL
import { directUploadFile, uploadToWebDAV } from "../../api/fileService"; // 导入直接上传函数，包括WebDAV上传
import { getMaxUploadSize, deleteFile, deleteUserFile } from "../../api/fileService"; // 导入获取最大上传大小函数和删除文件函数
import { useI18n } from "vue-i18n"; // 导入i18n

const { t } = useI18n(); // 初始化i18n

const props = defineProps({
  darkMode: {
    type: Boolean,
    default: false,
  },
  s3Configs: {
    type: Array,
    default: () => [],
  },
  loading: {
    type: Boolean,
    default: false,
  },
  isAdmin: {
    type: Boolean,
    default: false,
  },
});

const emit = defineEmits(["upload-success", "upload-error"]);

// 最大文件大小限制(MB)
const maxFileSizeMB = ref(100); // 默认值

// 拖拽状态
const isDragging = ref(false);
const fileInput = ref(null);
const selectedFiles = ref([]);

// 上传状态
const isUploading = ref(false);
const uploadProgress = ref(0);
const uploadSpeed = ref("");
const message = ref(null);
const activeXhr = ref(null);
const lastLoaded = ref(0);
const lastTime = ref(0);
const slugError = ref(""); // 添加slug错误状态
const currentFileId = ref([]);

// 表单数据
const formData = reactive({
  s3_config_id: "",
  slug: "",
  path: "",
  remark: "",
  password: "",
  expires_in: "0", // 默认永不过期
  max_views: 0, // 默认无限制
});

// 组件挂载时获取最大上传大小
onMounted(async () => {
  try {
    const size = await getMaxUploadSize();
    maxFileSizeMB.value = size;
  } catch (error) {
    console.error("获取最大上传大小失败:", error);
    // 失败时保持默认值
  }
});

// 监听s3Configs变化，自动选择默认配置
watch(
    () => props.s3Configs,
    (configs) => {
      if (configs && configs.length > 0) {
        // 查找默认配置
        const defaultConfig = configs.find((config) => config.is_default);
        if (defaultConfig) {
          // 使用默认配置的ID
          formData.s3_config_id = defaultConfig.id;
        } else if (!formData.s3_config_id && configs.length > 0) {
          // 如果没有默认配置且当前未选择配置，则选择第一个
          formData.s3_config_id = configs[0].id;
        }
      }
    },
    { immediate: true } // 页面加载时立即执行
);

/**
 * 验证并处理可打开次数的输入
 * 确保输入的是有效的非负整数
 * @param {Event} event - 输入事件对象
 */
const validateMaxViews = (event) => {
  // 获取输入的值
  const value = event.target.value;

  // 如果是负数，则设置为0
  if (value < 0) {
    formData.max_views = 0;
    return;
  }

  // 如果包含小数点，截取整数部分
  if (value.toString().includes(".")) {
    formData.max_views = parseInt(value);
  }

  // 确保值为有效数字
  if (isNaN(value) || value === "") {
    formData.max_views = 0;
  } else {
    // 确保是整数
    formData.max_views = parseInt(value);
  }
};

// 拖拽处理函数
const onDragOver = () => {
  isDragging.value = true;
};

const onDragLeave = (event) => {
  // 只有当鼠标离开拖拽区域而不是其内部元素时才重置
  const rect = event.currentTarget.getBoundingClientRect();
  const x = event.clientX;
  const y = event.clientY;

  // 判断鼠标是否真的离开了整个区域
  if (x <= rect.left || x >= rect.right || y <= rect.top || y >= rect.bottom) {
    isDragging.value = false;
  }
};

const onDrop = (event) => {
  isDragging.value = false;
  if (event.dataTransfer.files.length > 0) {
    const files = event.dataTransfer.files;
    // 验证文件大小是否超过限制
    for (const file of files) {
      if (file.size > maxFileSizeMB.value * 1024 * 1024) {
        message.value = {
          type: "error",
          content: t("file.maxSizeExceeded", { size: maxFileSizeMB.value }),
        };
        // 触发错误事件，让父组件处理消息显示
        emit("upload-error", new Error(t("file.maxSizeExceeded", { size: maxFileSizeMB.value })));
        return;
      }
    }
    selectedFiles.value = files;
  }
};

// 点击选择文件
const triggerFileInput = () => {
  fileInput.value.click();
};

const onFileSelected = (event) => {
  if (event.target.files.length > 0) {
    const files = event.target.files;
    // 验证文件大小是否超过限制
    for (const file of files) {
      if (file.size > maxFileSizeMB.value * 1024 * 1024) {
        message.value = {
          type: "error",
          content: t("file.maxSizeExceeded", { size: maxFileSizeMB.value }),
        };
        // 触发错误事件，让父组件处理消息显示
        emit("upload-error", new Error(t("file.maxSizeExceeded", { size: maxFileSizeMB.value })));
        return;
      }
    }
    selectedFiles.value = files;
  }
};

// 清除已选文件
const clearAllSelectedFiles = () => {
  selectedFiles.value = [];
  // 重置文件输入框，以便重新选择同一文件
  if (fileInput.value) {
    fileInput.value.value = "";
  }
};

// 删除单个文件
const removeSelectedFile = (index) => {
  selectedFiles.value = selectedFiles.value.filter((_, i) => i !== index);
  
  // 如果删除后没有文件了，重置文件输入框
  if (selectedFiles.value.length === 0 && fileInput.value) {
    fileInput.value.value = "";
  }
};

// 格式化最大文件大小
const formatMaxFileSize = () => {
  // 将MB转换为字节
  const sizeInBytes = maxFileSizeMB.value * 1024 * 1024;
  return formatFileSize(sizeInBytes);
};

// 格式化文件大小
const formatFileSize = (bytes) => {
  if (bytes === 0) return "0 B";

  const units = ["B", "KB", "MB", "GB", "TB"];
  const i = Math.floor(Math.log(bytes) / Math.log(1024));

  return parseFloat((bytes / Math.pow(1024, i)).toFixed(2)) + " " + units[i];
};

// 取消上传
const cancelUpload = () => {
  if (activeXhr.value && isUploading.value) {
    activeXhr.value.abort();
    isUploading.value = false;
    uploadProgress.value = 0;
    uploadSpeed.value = "";

    // 如果已获取了文件ID，则删除相应的文件记录
    if (currentFileId.value && currentFileId.value.length > 0) {
      // 根据用户身份选择合适的删除API
      const deleteApi = props.isAdmin ? deleteFile : deleteUserFile;

      // 为每个已创建的文件ID执行删除操作
      const deletePromises = currentFileId.value.map(fileId => {
        return deleteApi(fileId)
          .then(() => {
            console.log(`已成功删除被取消的文件记录: ${fileId}`);
            return true;
          })
          .catch((error) => {
            console.error(`删除被取消的文件记录失败 ${fileId}:`, error);
            return false;
          });
      });

      // 等待所有删除操作完成
      Promise.all(deletePromises)
        .then((results) => {
          const successCount = results.filter(result => result).length;
          console.log(`成功删除了 ${successCount}/${currentFileId.value.length} 个文件记录`);
        })
        .finally(() => {
          // 重置文件ID
          currentFileId.value = [];

          // 设置message但不显示，由父组件处理
          message.value = {
            type: "error",
            content: t("file.cancelMessage"),
          };

          // 触发错误事件，让父组件处理消息显示
          emit("upload-error", new Error(t("file.cancelMessage")));
        });
    } else {
      // 如果没有文件ID，直接显示取消消息
      message.value = {
        type: "error",
        content: t("file.cancelMessage"),
      };

      // 触发错误事件，让父组件处理消息显示
      emit("upload-error", new Error(t("file.cancelMessage")));
    }
  }
};

// 验证自定义链接后缀格式
const validateCustomLink = () => {
  // 清除现有错误
  slugError.value = "";

  // 如果为空则不验证（使用随机生成的slug）
  if (!formData.slug) {
    return true;
  }

  // 验证格式：只允许字母、数字、连字符、下划线
  const slugRegex = /^[a-zA-Z0-9_-]+$/;
  if (!slugRegex.test(formData.slug)) {
    slugError.value = t("file.invalidFormat");
    return false;
  }

  return true;
};

// 上传文件
const submitUpload = async () => {
  if (!selectedFiles.value.length || !formData.s3_config_id || isUploading.value) return;

  // 再次验证所有文件大小是否超过限制
  for (const file of selectedFiles.value) {
    if (file.size > maxFileSizeMB.value * 1024 * 1024) {
      message.value = {
        type: "error",
        content: t("file.maxSizeExceeded", { size: maxFileSizeMB.value }),
      };
      // 触发错误事件，让父组件处理消息显示
      emit("upload-error", new Error(t("file.maxSizeExceeded", { size: maxFileSizeMB.value })));
      return;
    }
  }

  // 验证可打开次数，确保是非负整数
  if (formData.max_views < 0) {
    // 仍然设置message，但不在本组件中显示，而是通过事件传递给父组件
    message.value = {
      type: "error",
      content: t("file.negativeMaxViews"),
    };

    // 触发错误事件，让父组件处理消息显示
    emit("upload-error", new Error(t("file.negativeMaxViews")));

    return;
  }

  // 验证自定义链接格式
  if (!validateCustomLink()) {
    message.value = { type: "error", text: slugError.value };
    return;
  }

  isUploading.value = true;
  uploadProgress.value = 0;
  uploadSpeed.value = "";
  message.value = null;
  currentFileId.value = [];

  // 重置上传速度计算相关变量
  lastLoaded.value = 0;
  lastTime.value = Date.now();
  
  const totalFiles = selectedFiles.value.length;
  let successCount = 0;
  const uploadErrors = [];
  const uploadResults = [];

  try {
    // 处理多个文件的上传
    for (let i = 0; i < selectedFiles.value.length; i++) {
      const file = selectedFiles.value[i];
      
      // 更新上传进度消息，显示当前正在上传的文件
      uploadProgress.value = 0;
      uploadSpeed.value = '';
      
      try {
        // 获取选中的S3配置信息
        const selectedConfig = props.s3Configs.find(config => config.id === formData.s3_config_id);
        const isWebDAV = selectedConfig && selectedConfig.provider_type === 'WebDAV';
        
        let response;
        
        // 根据提供商类型选择不同的上传方法
        if (isWebDAV) {
          // 使用WebDAV上传方法
          response = await uploadToWebDAV(
            file,
            {
              s3_config_id: formData.s3_config_id,
              slug: formData.slug ? `${formData.slug}-${i+1}` : "", // 为每个文件生成不同的slug
              path: formData.path || "",
              remark: formData.remark || "",
              password: formData.password || "",
              expires_in: formData.expires_in || "0",
              max_views: formData.max_views !== undefined ? Number(formData.max_views) : 0,
            },
            // 进度回调函数
            (progress, loaded, total) => {
              uploadProgress.value = progress;

              // 计算上传速度
              const now = Date.now();
              const timeElapsed = (now - lastTime.value) / 1000; // 转换为秒

              if (timeElapsed > 0.5) {
                // 每0.5秒更新一次速度
                const loadedChange = loaded - lastLoaded.value; // 这段时间内上传的字节数
                const speed = loadedChange / timeElapsed; // 字节/秒

                uploadSpeed.value = formatSpeed(speed);

                // 更新上次加载值和时间
                lastLoaded.value = loaded;
                lastTime.value = now;
              }
            }
          );
          
          // 如果响应包含文件ID，添加到currentFileId用于可能的取消操作
          if (response && response.data && response.data.id) {
            currentFileId.value.push(response.data.id);
          }
        } else {
          // 使用原有的S3直接上传方法
          response = await directUploadFile(
            file,
            {
              s3_config_id: formData.s3_config_id,
              slug: formData.slug ? `${formData.slug}-${i+1}` : "", // 为每个文件生成不同的slug
              path: formData.path || "",
              remark: formData.remark || "",
              password: formData.password || "",
              expires_in: formData.expires_in || "0",
              max_views: formData.max_views !== undefined ? Number(formData.max_views) : 0,
            },
            // 进度回调函数
            (progress, loaded, total) => {
              uploadProgress.value = progress;

              // 计算上传速度
              const now = Date.now();
              const timeElapsed = (now - lastTime.value) / 1000; // 转换为秒

              if (timeElapsed > 0.5) {
                // 每0.5秒更新一次速度
                const loadedChange = loaded - lastLoaded.value; // 这段时间内上传的字节数
                const speed = loadedChange / timeElapsed; // 字节/秒

                uploadSpeed.value = formatSpeed(speed);

                // 更新上次加载值和时间
                lastLoaded.value = loaded;
                lastTime.value = now;
              }
            },
            // 获取XHR实例的回调
            (xhr) => {
              activeXhr.value = xhr;
            },
            // 获取文件ID的回调
            (fileId) => {
              currentFileId.value.push(fileId);
            }
          );
        }

        // 记录成功的上传
        successCount++;
        uploadResults.push(response.data);
      } catch (error) {
        console.error(`上传文件 ${file.name} 失败:`, error);
        uploadErrors.push({
          fileName: file.name,
          error
        });
      }
    }

    // 处理上传结果
    if (successCount === 0) {
      // 所有文件均上传失败
      throw new Error(t("file.allUploadsFailed"));
    } else if (uploadErrors.length > 0) {
      // 部分文件上传失败
      message.value = {
        type: "warning",
        content: t("file.partialUploadSuccess", { success: successCount, total: totalFiles }),
      };
    } else {
      // 所有文件上传成功
      message.value = {
        type: "success",
        content: t("file.allUploadsSuccessful", { count: totalFiles }),
      };
    }

    // 清空表单
    selectedFiles.value = [];
    if (fileInput.value) {
      fileInput.value.value = "";
    }

    // 重置文件ID
    currentFileId.value = [];

    // 重置其他表单数据
    formData.slug = "";
    formData.path = "";
    formData.remark = "";
    formData.password = "";

    // 发出成功事件，传递所有成功的上传结果
    emit("upload-success", uploadResults);
  } catch (error) {
    console.error("上传失败:", error);
    // 设置message但不显示，由父组件处理
    message.value = {
      type: "error",
      content: `${t("file.error")}: ${error.message || t("common.unknownError")}`,
    };

    // 重置文件ID
    currentFileId.value = [];

    // 发出错误事件
    emit("upload-error", error);
  } finally {
    isUploading.value = false;
  }
};

// 格式化上传速度
const formatSpeed = (bytesPerSecond) => {
  if (bytesPerSecond < 1024) {
    return `${bytesPerSecond.toFixed(0)} B/s`;
  } else if (bytesPerSecond < 1024 * 1024) {
    return `${(bytesPerSecond / 1024).toFixed(1)} KB/s`;
  } else {
    return `${(bytesPerSecond / (1024 * 1024)).toFixed(1)} MB/s`;
  }
};
</script>

<style scoped>
/* 脉动边框动画 */
@keyframes pulseBorder {
  0% {
    border-color: rgba(59, 130, 246, 0.5); /* 淡蓝色 */
  }
  50% {
    border-color: rgba(59, 130, 246, 1); /* 全蓝色 */
  }
  100% {
    border-color: rgba(59, 130, 246, 0.5); /* 淡蓝色 */
  }
}

/* 进度条条纹动画 */
@keyframes progressStripes {
  0% {
    background-position: 0 0;
  }
  100% {
    background-position: 30px 0;
  }
}

.pulsing-border {
  animation: pulseBorder 1.5s ease-in-out infinite;
}

/* 提升动画性能 */
.drop-zone {
  will-change: border-color, background-color, transform;
  transform: translateZ(0);
}

/* 拖动元素进入时的缩放效果 */
.drop-zone.pulsing-border {
  transform: scale(1.01);
  transition: transform 0.3s ease;
}

/* 进度条条纹样式 */
.progress-stripes {
  background-image: linear-gradient(
      45deg,
      rgba(255, 255, 255, 0.15) 25%,
      transparent 25%,
      transparent 50%,
      rgba(255, 255, 255, 0.15) 50%,
      rgba(255, 255, 255, 0.15) 75%,
      transparent 75%,
      transparent
  );
  background-size: 30px 30px;
}

/* 进度条条纹动画 */
.animate-progress-stripes {
  animation: progressStripes 1s linear infinite;
}
</style>
