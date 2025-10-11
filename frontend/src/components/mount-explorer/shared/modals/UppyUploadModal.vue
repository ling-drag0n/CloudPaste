<template>
  <div v-if="isOpen" class="fixed inset-0 z-[60] overflow-auto bg-black bg-opacity-50 flex items-center justify-center p-2 sm:p-4 pt-20 sm:pt-4">
    <div
      class="relative w-full max-w-sm sm:max-w-3xl lg:max-w-5xl h-auto min-h-[400px] sm:min-h-[500px] max-h-[85vh] sm:max-h-[80vh] rounded-lg shadow-xl flex flex-col"
      :class="darkMode ? 'bg-gray-800' : 'bg-white'"
    >
      <!-- 弹窗标题栏 -->
      <div class="p-4 flex justify-between items-center border-b" :class="darkMode ? 'border-gray-700' : 'border-gray-200'">
        <h3 class="text-lg font-semibold" :class="darkMode ? 'text-gray-100' : 'text-gray-900'">{{ t("mount.uppy.title") }}</h3>
        <button
          @click="closeModal"
          class="p-1 rounded-full transition-colors"
          :class="darkMode ? 'hover:bg-gray-700 text-gray-400 hover:text-gray-200' : 'hover:bg-gray-200 text-gray-500 hover:text-gray-700'"
        >
          <svg xmlns="http://www.w3.org/2000/svg" class="h-6 w-6" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
          </svg>
        </button>
      </div>

      <!-- 弹窗内容区 -->
      <div class="flex-1 p-4 overflow-y-auto">
        <!-- 上传方式选择 -->
        <div class="mb-4 p-3 rounded-lg" :class="darkMode ? 'bg-gray-700/50' : 'bg-gray-100'">
          <!-- 标题和模式指示器 -->
          <div class="flex items-center justify-between mb-3">
            <span class="text-sm font-medium" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">{{ t("mount.uppy.uploadMethod") }}</span>
            <span
              class="text-xs px-2 py-1 rounded-full cursor-help"
              :class="
                uploadMethod === 'presigned'
                  ? darkMode
                    ? 'bg-green-900/30 text-green-300'
                    : 'bg-green-100 text-green-700'
                  : uploadMethod === 'direct'
                  ? darkMode
                    ? 'bg-blue-900/30 text-blue-300'
                    : 'bg-blue-100 text-blue-700'
                  : darkMode
                  ? 'bg-amber-900/30 text-amber-300'
                  : 'bg-amber-100 text-amber-700'
              "
              :title="
                uploadMethod === 'presigned'
                  ? t('mount.uppy.presignedModeTooltip')
                  : uploadMethod === 'direct'
                  ? t('mount.uppy.directModeTooltip')
                  : t('mount.uppy.multipartModeTooltip')
              "
            >
              {{ uploadMethod === "presigned" ? t("mount.uppy.presignedMode") : uploadMethod === "direct" ? t("mount.uppy.directMode") : t("mount.uppy.multipartMode") }}
            </span>
          </div>

          <!-- 选项区域 - 响应式布局 -->
          <div class="flex flex-col sm:flex-row sm:space-x-4 space-y-2 sm:space-y-0">
            <label class="flex items-center cursor-pointer">
              <input
                type="radio"
                name="uploadMethod"
                value="presigned"
                v-model="uploadMethod"
                class="w-4 h-4 text-blue-600 bg-gray-100 border-gray-300 dark:bg-gray-700 dark:border-gray-600 flex-shrink-0"
                @change="updateUploadMethod"
              />
              <span class="ml-2 text-sm" :class="darkMode ? 'text-gray-300' : 'text-gray-600'">{{ t("mount.uppy.presignedUpload") }}</span>
            </label>
            <label class="flex items-center cursor-pointer">
              <input
                type="radio"
                name="uploadMethod"
                value="direct"
                v-model="uploadMethod"
                class="w-4 h-4 text-blue-600 bg-gray-100 border-gray-300 dark:bg-gray-700 dark:border-gray-600 flex-shrink-0"
                @change="updateUploadMethod"
              />
              <span class="ml-2 text-sm" :class="darkMode ? 'text-gray-300' : 'text-gray-600'">{{ t("mount.uppy.directUpload") }}</span>
            </label>
            <label class="flex items-center cursor-pointer">
              <input
                type="radio"
                name="uploadMethod"
                value="multipart"
                v-model="uploadMethod"
                class="w-4 h-4 text-blue-600 bg-gray-100 border-gray-300 dark:bg-gray-700 dark:border-gray-600 flex-shrink-0"
                @change="updateUploadMethod"
              />
              <div class="ml-2 flex items-center flex-wrap gap-1">
                <span class="text-sm" :class="darkMode ? 'text-gray-300' : 'text-gray-600'">{{ t("mount.uppy.multipartUpload") }}</span>
                <span class="text-xs px-1 py-0.5 rounded flex-shrink-0" :class="darkMode ? 'bg-green-900/50 text-green-300' : 'bg-green-100 text-green-700'">
                  {{ t("mount.uppy.resumeSupport") }}
                </span>
              </div>
            </label>
          </div>
        </div>

        <!-- 高级功能（可折叠） -->
        <div class="mb-4">
          <button
            @click="showAdvancedOptions = !showAdvancedOptions"
            class="flex items-center justify-between w-full p-3 text-left rounded-lg transition-colors"
            :class="darkMode ? 'bg-gray-700/50 hover:bg-gray-700/70' : 'bg-gray-100 hover:bg-gray-200'"
          >
            <span class="text-sm font-medium" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">
              {{ t("mount.uppy.advancedFeatures") }}
              <span v-if="enabledPluginsCount > 0" class="ml-2 px-2 py-0.5 text-xs rounded-full" :class="darkMode ? 'bg-blue-900/50 text-blue-300' : 'bg-blue-100 text-blue-700'">
                {{ t("mount.uppy.enabledCount", { count: enabledPluginsCount }) }}
              </span>
            </span>
            <svg
              class="w-4 h-4 transition-transform duration-200"
              :class="[showAdvancedOptions ? 'rotate-180' : '', darkMode ? 'text-gray-400' : 'text-gray-500']"
              fill="none"
              stroke="currentColor"
              viewBox="0 0 24 24"
            >
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7" />
            </svg>
          </button>

          <!-- 可折叠的插件选项 -->
          <div v-show="showAdvancedOptions" class="mt-2 p-3 rounded border-l-2" :class="darkMode ? 'bg-gray-800/30 border-gray-600' : 'bg-gray-50 border-gray-300'">
            <div class="grid grid-cols-2 sm:grid-cols-3 gap-2">
              <label v-for="plugin in mediaPlugins" :key="plugin.key" class="flex items-center cursor-pointer">
                <input
                  type="checkbox"
                  :checked="plugin.enabled"
                  @change="togglePlugin(plugin.key)"
                  class="w-4 h-4 text-blue-600 bg-gray-100 border-gray-300 rounded dark:bg-gray-700 dark:border-gray-600"
                />
                <span class="ml-2 text-sm" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">
                  {{ plugin.label }}
                </span>
              </label>
            </div>
          </div>
        </div>

        <!-- Uppy Dashboard 容器 -->
        <div class="uppy-container mb-4">
          <div ref="uppyContainer" id="uppy-dashboard" class="min-h-[300px]"></div>
          <!-- 粘贴提示 -->
          <div class="mt-2 text-center">
            <span class="text-xs" :class="darkMode ? 'text-gray-500' : 'text-gray-400'">
              {{ t("mount.uppy.pasteSupport") }}
              <kbd class="px-1 py-0.5 text-xs font-mono rounded" :class="darkMode ? 'bg-gray-700 text-gray-300' : 'bg-gray-200 text-gray-600'">{{ t("mount.uppy.pasteKey") }}</kbd>
              {{ t("mount.uppy.pasteHint") }}
            </span>
          </div>
        </div>

        <!-- 错误显示 -->
        <div v-if="errorMessage" class="mb-4 p-3 rounded-md" :class="darkMode ? 'bg-red-900/20 border border-red-700' : 'bg-red-50 border border-red-200'">
          <div class="flex items-center">
            <svg class="w-4 h-4 mr-2 text-red-500" fill="currentColor" viewBox="0 0 20 20">
              <path
                fill-rule="evenodd"
                d="M10 18a8 8 0 100-16 8 8 0 000 16zM8.707 7.293a1 1 0 00-1.414 1.414L8.586 10l-1.293 1.293a1 1 0 101.414 1.414L10 11.414l1.293 1.293a1 1 0 001.414-1.414L11.414 10l1.293-1.293a1 1 0 00-1.414-1.414L10 8.586 8.707 7.293z"
                clip-rule="evenodd"
              />
            </svg>
            <span class="text-sm" :class="darkMode ? 'text-red-300' : 'text-red-700'">{{ errorMessage }}</span>
          </div>
        </div>
      </div>

      <!-- 底部操作栏 -->
      <div class="p-4 border-t" :class="darkMode ? 'border-gray-700 bg-gray-800/50' : 'border-gray-200 bg-gray-50/50'">
        <div class="flex justify-between items-center">
          <div class="flex items-center space-x-4">
            <div class="flex items-center space-x-2">
              <svg class="w-4 h-4" :class="darkMode ? 'text-gray-400' : 'text-gray-500'" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3 7v10a2 2 0 002 2h14a2 2 0 002-2V9a2 2 0 00-2-2H5a2 2 0 00-2-2z" />
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8 7V3a2 2 0 012-2h4a2 2 0 012 2v4" />
              </svg>
              <span class="text-sm" :class="darkMode ? 'text-gray-400' : 'text-gray-500'"> {{ t("mount.uppy.targetPath") }} {{ currentPath }} </span>
            </div>
          </div>

          <div class="flex items-center space-x-2">
            <!-- 开始上传按钮 -->
            <button
              v-if="canStartUpload"
              @click="startUpload"
              :disabled="isUploading"
              class="px-4 py-2 text-sm font-medium rounded-md transition-colors flex items-center space-x-2 disabled:opacity-50"
              :class="darkMode ? 'bg-blue-600 hover:bg-blue-700 text-white' : 'bg-blue-600 hover:bg-blue-700 text-white'"
            >
              <svg v-if="isUploading" class="animate-spin w-4 h-4" fill="none" viewBox="0 0 24 24">
                <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
                <path
                  class="opacity-75"
                  fill="currentColor"
                  d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"
                ></path>
              </svg>
              <svg v-else class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M7 16a4 4 0 01-.88-7.903A5 5 0 1115.9 6L16 6a5 5 0 011 9.9M15 13l-3-3m0 0l-3 3m3-3v12" />
              </svg>
              <span>{{ isUploading ? t("mount.uppy.uploading") : t("mount.uppy.startUpload") }}</span>
            </button>
          </div>
        </div>
      </div>
    </div>

    <!-- 多个上传选择对话框 -->
    <Teleport to="body">
      <SelectUploadDialog
        v-if="showSelectUploadDialog"
        :is-open="showSelectUploadDialog"
        :file="selectUploadData.file"
        :uploads="selectUploadData.uploads"
        :show-match-score="selectUploadData.showMatchScore"
        :dark-mode="darkMode"
        @select="handleUploadSelect"
        @cancel="handleUploadSelectCancel"
        @close="showSelectUploadDialog = false"
      />
    </Teleport>
  </div>
</template>

<script setup>
import { ref, computed, watch, onMounted, onBeforeUnmount, nextTick } from "vue";
import { useI18n } from "vue-i18n";

import Uppy from "@uppy/core";
import Dashboard from "@uppy/dashboard";
import AwsS3 from "@uppy/aws-s3";
import XHRUpload from "@uppy/xhr-upload";

// 导入Uppy官方locale
import ChineseLocale from "@uppy/locales/lib/zh_CN";
import EnglishLocale from "@uppy/locales/lib/en_US";

// 导入样式
import "@uppy/core/dist/style.min.css";
import "@uppy/dashboard/dist/style.min.css";
import "@uppy/webcam/dist/style.min.css";
import "@uppy/screen-capture/dist/style.min.css";
import "@uppy/audio/dist/style.min.css";
import "@uppy/image-editor/dist/style.min.css";
import "@uppy/url/dist/style.min.css";

// 导入S3适配器和ServerResumePlugin插件
import { S3Adapter } from "../../../../composables/uppy/S3Adapter.js";
import ServerResumePlugin from "../../../../composables/uppy/ServerResumePlugin.js";

// 导入API配置
import { getFullApiUrl } from "../../../../api/config.js";

// 导入插件管理器
import { createUppyPluginManager } from "../../../../composables/uppy/UppyPluginManager.js";

// 导入对话框组件
import SelectUploadDialog from "../../../common/dialogs/SelectUploadDialog.vue";

// 组件属性
const props = defineProps({
  isOpen: {
    type: Boolean,
    default: false,
  },
  darkMode: {
    type: Boolean,
    default: false,
  },
  currentPath: {
    type: String,
    required: true,
  },
  isAdmin: {
    type: Boolean,
    default: false,
  },
});

// 事件
const emit = defineEmits(["close", "upload-success", "upload-error"]);

// 国际化
const { locale, t } = useI18n();

// 根据当前语言选择Uppy locale
const getUppyLocale = () => {
  return locale.value === "zh-CN" ? ChineseLocale : EnglishLocale;
};

// 插件文本管理已移至 UppyPluginManager.js

// 响应式数据
const uppyContainer = ref(null);
const uploadMethod = ref("presigned");
const errorMessage = ref("");
const isUploading = ref(false);
const showAdvancedOptions = ref(false);

const mediaPlugins = ref([]);

// 多个上传选择对话框状态
const showSelectUploadDialog = ref(false);
const selectUploadData = ref({
  file: null,
  uploads: [],
  showMatchScore: true,
  onSelect: null,
  onCancel: null,
});

// 插件管理器实例
let pluginManager = null;

// Uppy实例和适配器
let uppyInstance = null;
let s3Adapter = null;

/**
 * 获取Dashboard插件配置
 */
const getDashboardConfig = () => ({
  inline: true,
  target: uppyContainer.value,
  theme: props.darkMode ? "dark" : "light",
  width: "100%",
  height: 400,
  showProgressDetails: true,
  showRemoveButtonAfterComplete: true,
  hideUploadButton: true, // 使用自定义按钮
  hidePauseResumeButton: false, // 保留暂停/恢复按钮，但使用自定义逻辑
  proudlyDisplayPoweredByUppy: false,
  disableLocalFiles: false, // 明确启用本地文件选择
  // 启用文件名编辑功能，使用项目国际化系统
  metaFields: [
    {
      id: "name",
      name: t("mount.uppy.fileName"), // 使用项目国际化
      placeholder: t("mount.uppy.customFilename"), // 使用项目国际化
    },
  ],
  // 覆盖有问题的进度显示字符串
  locale: {
    strings: {
      dataUploadedOfTotal: "%{complete} / %{total}",
    },
  },
});

/**
 * 获取ServerResumePlugin插件配置
 */
const getServerResumeConfig = () => ({
  autoCheck: true, // 自动检测可恢复上传
  matchThreshold: 0.8, // 匹配阈值
  timeWindow: 24, // 24小时内的上传
  showConfirmDialog: true, // 显示确认对话框
  currentPath: props.currentPath, // 传递当前路径

  // 用户选择相关配置
  maxSelectionOptions: 5, // 最多显示5个选项
  showMatchScore: true, // 显示匹配分数
});

// 响应式状态：文件数量
const fileCount = ref(0);

// 计算属性：是否可以开始上传
const canStartUpload = computed(() => {
  return fileCount.value > 0 && !isUploading.value;
});

// 计算属性：启用的插件数量
const enabledPluginsCount = computed(() => {
  return pluginManager ? pluginManager.getEnabledPluginsCount() : 0;
});

/**
 * 切换插件状态（使用插件管理器）
 */
const togglePlugin = (pluginKey) => {
  if (pluginManager) {
    pluginManager.togglePlugin(pluginKey);
    // 更新本地显示
    mediaPlugins.value = pluginManager.getPluginList();
    // 重新初始化Uppy以应用插件变化
    if (props.isOpen && uppyInstance) {
      initializeUppy();
    }
  }
};

/**
 * 设置Uppy核心组件
 */
const setupUppyCore = () => {
  console.log("[Uppy] 开始初始化");

  // 创建S3适配器
  s3Adapter = new S3Adapter(props.currentPath);

  // 创建Uppy实例
  uppyInstance = new Uppy({
    id: "new-uppy-dashboard",
    autoProceed: false,
    allowMultipleUploadBatches: true,
    debug: import.meta.env.DEV,
    locale: getUppyLocale(), // 根据当前语言动态选择locale
  });

  // 设置S3Adapter的Uppy实例引用
  s3Adapter.setUppyInstance(uppyInstance);

  // 创建插件管理器
  pluginManager = createUppyPluginManager(uppyInstance, locale.value);

  // 更新插件列表显示
  mediaPlugins.value = pluginManager.getPluginList();
};

/**
 * 配置Dashboard插件
 */
const configureDashboard = () => {
  uppyInstance.use(Dashboard, getDashboardConfig());
};

/**
 * 配置ServerResumePlugin插件
 */
const configureServerResumePlugin = () => {
  uppyInstance.use(ServerResumePlugin, getServerResumeConfig());
};

/**
 * 配置上传方式
 */
const configureUploadMethod = () => {
  // 根据上传方式选择不同的插件
  if (uploadMethod.value === "direct") {
    // 直接上传模式：使用XHR Upload插件
    // 设置上传metadata
    uppyInstance.setMeta({
      path: props.currentPath,
      use_multipart: "false",
    });

    uppyInstance.use(XHRUpload, {
      id: "XHRUpload",
      endpoint: getFullApiUrl("/fs/upload"), //TODO: 移到配置层
      method: "POST",
      formData: true,
      fieldName: "file",
      allowedMetaFields: ["path", "use_multipart"], // 允许这些metadata字段被发送
      headers: () => s3Adapter?.getAuthHeaders() || {}, // 委托给业务逻辑层
      limit: 3,
      getResponseData: (xhr) => {
        // 遵循项目中原生XMLHttpRequest的一致模式
        if (xhr.status >= 200 && xhr.status < 300) {
          const data = JSON.parse(xhr.responseText);
          return {
            uploadURL: data.data?.url || data.data?.s3Url || "",
          };
        } else {
          throw new Error(`上传失败: ${xhr.status} ${xhr.statusText}`);
        }
      },
    });
  } else {
    // 预签名URL和分片上传模式：使用AWS S3插件
    uppyInstance.use(AwsS3, {
      id: "AwsS3",
      shouldUseMultipart: (_file) => {
        // 只根据用户选择来判断是否使用分片上传
        return uploadMethod.value === "multipart";
      },
      limit: 3,
      getUploadParameters: s3Adapter.getUploadParameters.bind(s3Adapter),
      createMultipartUpload: s3Adapter.createMultipartUpload.bind(s3Adapter),
      signPart: s3Adapter.signPart.bind(s3Adapter),
      completeMultipartUpload: s3Adapter.completeMultipartUpload.bind(s3Adapter),
      abortMultipartUpload: s3Adapter.abortMultipartUpload.bind(s3Adapter),
      listParts: s3Adapter.listParts.bind(s3Adapter),
      uploadPartBytes: s3Adapter.uploadPartBytes.bind(s3Adapter),
    });
  }
};

/**
 * 完成最终设置
 */
const finalizeSetup = () => {
  // 添加插件到Uppy实例
  pluginManager.addPluginsToUppy();

  // 设置事件监听器
  setupEventListeners();

  // 设置粘贴事件监听器
  const cleanupPaste = setupPasteListener();

  // 保存清理函数
  uppyInstance._cleanupPaste = cleanupPaste;

  // 使用正确的API获取插件信息
  const installedPlugins = [];

  // 检查常见的插件
  const commonPlugins = ["Dashboard", "AwsS3", "ServerResumePlugin", "Webcam", "ScreenCapture", "Audio", "ImageEditor"];
  commonPlugins.forEach((pluginName) => {
    const plugin = uppyInstance.getPlugin(pluginName);
    if (plugin) {
      installedPlugins.push(pluginName);
    }
  });

  console.log(`[Uppy] 已安装的插件:`, installedPlugins);
};

/**
 * 初始化Uppy实例
 */
const initializeUppy = async () => {
  try {
    // 清理旧实例
    if (uppyInstance) {
      // 清理粘贴监听器
      if (uppyInstance._cleanupPaste) {
        uppyInstance._cleanupPaste();
      }
      uppyInstance.destroy();
    }

    // 设置核心组件
    setupUppyCore();

    // 配置Dashboard插件
    configureDashboard();

    // 配置ServerResumePlugin插件
    configureServerResumePlugin();

    // 立即设置断点续传对话框事件监听器
    setupResumeDialogEvents();

    // 配置上传方式
    configureUploadMethod();

    // 完成最终设置
    finalizeSetup();
  } catch (error) {
    console.error("[Uppy] 初始化失败:", error);
    errorMessage.value = t("mount.uppy.initializationFailed", { message: error.message });
  }
};

/**
 * 设置粘贴事件监听器
 */
const setupPasteListener = () => {
  if (!uppyInstance) return;

  const handlePaste = (event) => {
    if (!props.isOpen) return;

    const items = event.clipboardData?.items;
    if (!items) return;

    for (const item of items) {
      if (item.kind === "file") {
        const file = item.getAsFile();
        if (file) {
          const ext = file.type.split("/")[1] || "bin";
          const fileName = file.name || `pasted-${Date.now()}.${ext}`;

          uppyInstance.addFile({
            name: fileName,
            type: file.type,
            data: file,
            source: "clipboard",
          });
          event.preventDefault();
        }
      }
    }
  };

  document.addEventListener("paste", handlePaste);

  // 返回清理函数
  return () => document.removeEventListener("paste", handlePaste);
};

/**
 * 设置文件相关事件监听器
 */
const setupFileEvents = () => {
  uppyInstance
    .on("file-added", (file) => {
      console.log("[Uppy] 文件已添加:", file.name);
      fileCount.value = uppyInstance.getFiles().length;
      console.log(`[Uppy] 当前文件数量: ${fileCount.value}, canStartUpload: ${canStartUpload.value}, isUploading: ${isUploading.value}`);
      errorMessage.value = "";
    })
    .on("file-removed", (file) => {
      console.log("[Uppy] 文件已移除:", file.name);
      fileCount.value = uppyInstance.getFiles().length;
    });
};

/**
 * 设置上传相关事件监听器
 */
const setupUploadEvents = (customPausedFiles) => {
  uppyInstance
    .on("upload-pause", (fileID) => {
      if (fileID) {
        // 单个文件暂停
        customPausedFiles.add(fileID);
        // 同步到S3Adapter
        if (s3Adapter) {
          s3Adapter.setFilePaused(fileID, true);
        }
        console.log(`[Uppy] ⏸️ 暂停文件: ${fileID}`);
      } else {
        // 全部文件暂停
        const files = uppyInstance.getFiles();
        files.forEach((file) => {
          if (file.progress && file.progress.uploadStarted && !file.progress.uploadComplete) {
            customPausedFiles.add(file.id);
            // 同步到S3Adapter
            if (s3Adapter) {
              s3Adapter.setFilePaused(file.id, true);
            }
          }
        });
        console.log(`[Uppy] ⏸️ 暂停所有文件: ${customPausedFiles.size}个`);
      }
    })
    .on("upload-resume", (fileID) => {
      if (fileID) {
        // 单个文件恢复
        customPausedFiles.delete(fileID);
        // 同步到S3Adapter
        if (s3Adapter) {
          s3Adapter.setFilePaused(fileID, false);
        }
        console.log(`[Uppy] ▶️ 恢复文件: ${fileID}`);
      } else {
        // 全部文件恢复
        customPausedFiles.clear();
        // 同步到S3Adapter
        if (s3Adapter) {
          const files = uppyInstance.getFiles();
          files.forEach((file) => {
            s3Adapter.setFilePaused(file.id, false);
          });
        }
        console.log(`[Uppy] ▶️ 恢复所有文件`);
      }
    })
    .on("upload", () => {
      console.log("[Uppy] 开始上传");
      isUploading.value = true;
      errorMessage.value = "";
    })
    .on("complete", handleUploadComplete);
};

/**
 * 设置错误处理事件监听器
 */
const setupErrorEvents = () => {
  uppyInstance
    .on("upload-error", (file, error) => {
      console.error("[Uppy] 上传失败:", file.name, error);
      errorMessage.value = error.message || "上传失败";
    })
    .on("error", (error) => {
      console.error("[Uppy] 系统错误:", error);
      errorMessage.value = error.message || "系统错误";
    });
};

/**
 * 处理上传完成事件
 */
const handleUploadComplete = async (result) => {
  console.log("[Uppy] 上传完成:", result);
  isUploading.value = false;

  if (result.successful.length > 0) {
    // 处理预签名上传的commit阶段 - 委托给业务逻辑层
    let commitResults = { failures: [] };
    if (uploadMethod.value === "presigned" && s3Adapter) {
      commitResults = await s3Adapter.batchCommitPresignedUploads(result.successful);
    }

    // 发送上传成功事件，触发父组件刷新目录
    emit("upload-success", {
      count: result.successful.length,
      commitFailures: commitResults.failures,
      commitStats: {
        successCount: commitResults.successCount || 0,
        failureCount: commitResults.failureCount || 0,
        totalCount: commitResults.totalCount || 0,
      },
      results: result.successful.map((file) => ({
        name: file.name,
        size: file.size,
        type: file.type,
        url: file.uploadURL,
      })),
    });

    // 清理Uppy实例中的文件，为下次上传做准备
    setTimeout(() => {
      if (uppyInstance) {
        uppyInstance.clear();
        fileCount.value = 0;
      }
    }, 4000); // 延迟4秒清理，让用户看到完成状态
  }

  if (result.failed.length > 0) {
    const firstError = result.failed[0].error;
    emit("upload-error", new Error(firstError?.message || "上传失败"));
  }
};

/**
 * 设置事件监听器
 */
const setupEventListeners = () => {
  if (!uppyInstance) return;

  // 自定义暂停/恢复逻辑，替换Uppy内置机制
  let customPausedFiles = new Set(); // 跟踪自定义暂停的文件

  // 设置各类事件监听器
  setupFileEvents();
  setupUploadEvents(customPausedFiles);
  setupErrorEvents();
};

/**
 * 更新上传方式
 */
const updateUploadMethod = () => {
  if (props.isOpen && uppyInstance) {
    initializeUppy();
  }
};

/**
 * 开始上传
 */
const startUpload = () => {
  if (uppyInstance && canStartUpload.value) {
    // 所有模式都使用Uppy的标准上传流程
    // XHR Upload插件会自动处理直接上传
    // AWS S3插件会自动处理预签名URL和分片上传
    uppyInstance.upload();
  }
};

/**
 * 关闭弹窗
 */
const closeModal = () => {
  if (uppyInstance) {
    uppyInstance.clear();
  }
  fileCount.value = 0;
  errorMessage.value = "";
  isUploading.value = false;
  emit("close");
};

// 监听弹窗打开状态
watch(
  () => props.isOpen,
  async (newValue) => {
    if (newValue) {
      await nextTick();
      await initializeUppy();
    } else {
      if (uppyInstance) {
        // 清理粘贴监听器
        if (uppyInstance._cleanupPaste) {
          uppyInstance._cleanupPaste();
        }
        uppyInstance.clear();
      }
      fileCount.value = 0;
      errorMessage.value = "";
      isUploading.value = false;
    }
  }
);

// 监听路径变化
watch(
  () => props.currentPath,
  (newPath) => {
    if (s3Adapter) {
      s3Adapter.updatePath(newPath);
    }
  }
);

// 监听暗色模式变化
watch(
  () => props.darkMode,
  async () => {
    if (props.isOpen && uppyInstance) {
      await initializeUppy();
    }
  }
);

// 监听语言变化
watch(
  () => locale.value,
  () => {
    // 更新插件管理器语言
    if (pluginManager) {
      pluginManager.updateLocale(locale.value);
      mediaPlugins.value = pluginManager.getPluginList();
    }
    // 如果弹窗打开，重新初始化Uppy以应用新语言
    if (props.isOpen && uppyInstance) {
      initializeUppy();
    }
  }
);

// 生命周期
onMounted(() => {
  if (props.isOpen) {
    nextTick(() => {
      initializeUppy();
    });
  }
});

onBeforeUnmount(() => {
  if (uppyInstance) {
    // 清理粘贴监听器
    if (uppyInstance._cleanupPaste) {
      uppyInstance._cleanupPaste();
    }
    uppyInstance.destroy();
  }
  if (s3Adapter) {
    s3Adapter.cleanup();
  }
});

/**
 * 设置断点续传对话框事件监听器
 */
const setupResumeDialogEvents = () => {
  // 选择对话框事件
  uppyInstance.on("server-resume-select-dialog", (data) => {
    selectUploadData.value = {
      file: data.file,
      uploads: data.uploads,
      showMatchScore: data.showMatchScore,
      onSelect: data.onSelect,
      onCancel: data.onCancel,
    };
    showSelectUploadDialog.value = true;
  });
};

/**
 * 处理上传选择对话框确认
 */
const handleUploadSelect = (selectedUpload) => {
  showSelectUploadDialog.value = false;
  if (selectUploadData.value.onSelect) {
    selectUploadData.value.onSelect(selectedUpload);
  }
};

/**
 * 处理上传选择对话框取消
 */
const handleUploadSelectCancel = () => {
  showSelectUploadDialog.value = false;
  if (selectUploadData.value.onCancel) {
    selectUploadData.value.onCancel();
  }
};
</script>

<style scoped>
.uppy-container {
  border-radius: 0.5rem;
  overflow: hidden;
}
</style>

<style>
.uppy-Dashboard {
  border: none !important;
  background: transparent !important;
  font-family: inherit !important;
  border-radius: 0.5rem !important;
}

.uppy-Dashboard-inner {
  border-radius: 0.5rem !important;
  border: 2px dashed #d1d5db !important;
  background: #f9fafb !important;
  transition: all 0.3s ease !important;
}

.uppy-Dashboard-inner:hover {
  border-color: #3b82f6 !important;
  background: #eff6ff !important;
}

/* 暗色模式适配 */
.dark .uppy-Dashboard-inner {
  border-color: #4b5563 !important;
  background: #374151 !important;
  color: #f3f4f6 !important;
}

.dark .uppy-Dashboard-inner:hover {
  border-color: #3b82f6 !important;
  background: #1f2937 !important;
}

.dark .uppy-Dashboard-AddFiles {
  background: transparent !important;
  color: #f3f4f6 !important;
}

.dark .uppy-Dashboard-AddFiles-title {
  color: #f3f4f6 !important;
}

/* 文件项样式 */
.uppy-Dashboard-file {
  border-radius: 0.5rem !important;
  border: 1px solid #e5e7eb !important;
  background: white !important;
  transition: all 0.2s ease !important;
}

.uppy-Dashboard-file:hover {
  border-color: #3b82f6 !important;
  box-shadow: 0 1px 3px 0 rgba(0, 0, 0, 0.1) !important;
}

.dark .uppy-Dashboard-file {
  background: #4b5563 !important;
  border-color: #6b7280 !important;
  color: #f3f4f6 !important;
}

.dark .uppy-Dashboard-file:hover {
  border-color: #3b82f6 !important;
  background: #6b7280 !important;
}

/* 进度条样式 */
.uppy-ProgressBar {
  background: #3b82f6 !important;
  border-radius: 0.25rem !important;
  height: 0.25rem !important;
}

/* 状态栏样式 */
.uppy-StatusBar {
  border-radius: 0.5rem !important;
  margin-top: 0.5rem !important;
  border: 1px solid #e5e7eb !important;
  background: #f9fafb !important;
}

.dark .uppy-StatusBar {
  background: #4b5563 !important;
  border-color: #6b7280 !important;
  color: #f3f4f6 !important;
}

/* 隐藏品牌信息 */
.uppy-Dashboard-poweredBy {
  display: none !important;
}

/* 响应式调整 */
@media (max-width: 768px) {
  .uppy-Dashboard-AddFiles {
    padding: 1.5rem 1rem !important;
  }

  .uppy-Dashboard-browse {
    padding: 0.5rem 1rem !important;
    font-size: 0.875rem !important;
  }
}
</style>
