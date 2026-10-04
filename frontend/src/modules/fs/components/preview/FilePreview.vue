<template>
  <div class="file-preview-container">
    <!-- 文件预览区域 -->
    <!-- File Header & Actions Area -->
    <div class="mb-4 px-1 transition-all duration-300">
      <div class="flex flex-col sm:flex-row sm:items-center justify-between gap-4">
        <!-- Left: Title & Metadata -->
        <div class="min-w-0 flex-1">
          <div class="flex items-center gap-2 mb-1.5">
             <h3 class="text-xl font-bold bg-clip-text text-transparent bg-gradient-to-r from-gray-900 to-gray-600 dark:from-white dark:to-gray-400 truncate leading-tight" :title="file.name">
              {{ file.name }}
            </h3>
            <span class="hidden sm:inline-flex items-center px-2 py-0.5 rounded text-[10px] font-bold tracking-wider uppercase bg-gray-100 dark:bg-gray-800 text-gray-500 dark:text-gray-400 border border-gray-200 dark:border-gray-700">
              {{ file.mimetype || 'FILE' }}
            </span>
          </div>
          
          <div class="flex items-center flex-wrap gap-x-4 gap-y-1 text-xs text-gray-500 dark:text-gray-400 font-medium tracking-wide">
             <span class="flex items-center gap-1.5">
                <IconDatabase size="sm" class="w-3.5 h-3.5 opacity-70" />
                <span>{{ formatFileSize(file.size) }}</span>
             </span>
             <span class="w-px h-3 bg-gray-300 dark:bg-gray-700"></span>
             <span class="flex items-center gap-1.5">
                <IconClock size="sm" class="w-3.5 h-3.5 opacity-70" />
                <span>{{ formatDate(file.modified) }}</span>
             </span>
          </div>
        </div>

        <!-- Right: Actions Toolbar -->
        <div class="flex items-center gap-2 self-start sm:self-center">
          <!-- Download -->
          <button
            @click="handleDownload"
            class="group flex items-center justify-center w-8 h-8 rounded-full transition-all bg-transparent text-blue-600 hover:text-blue-700 hover:bg-blue-50 dark:text-blue-400 dark:hover:text-blue-300 dark:hover:bg-blue-900/20 transform hover:scale-105"
            :title="t('mount.filePreview.downloadFile')"
          >
            <IconDownload size="sm" class="w-5 h-5" />
          </button>

          <!-- Direct Preview -->
          <button
            @click="handleS3DirectPreview"
            class="group flex items-center justify-center w-8 h-8 rounded-full transition-all bg-transparent text-purple-600 hover:text-purple-700 hover:bg-purple-50 dark:text-purple-400 dark:hover:text-purple-300 dark:hover:bg-purple-900/20 transform hover:scale-105"
            :title="t('mount.filePreview.directPreview')"
            :disabled="isGeneratingPreview"
          >
            <IconRefresh v-if="isGeneratingPreview" class="w-5 h-5 animate-spin" />
            <IconEye v-else size="sm" class="w-5 h-5" />
          </button>

          <!-- Share -->
          <button
            @click="handleCreateShare"
            class="group flex items-center justify-center w-8 h-8 rounded-full transition-all bg-transparent text-indigo-600 hover:text-indigo-700 hover:bg-indigo-50 dark:text-indigo-400 dark:hover:text-indigo-300 dark:hover:bg-indigo-900/20 transform hover:scale-105"
            :title="t('mount.filePreview.createShare')"
            :disabled="isCreatingShare"
          >
            <IconRefresh v-if="isCreatingShare" class="w-5 h-5 animate-spin" />
            <IconLink v-else size="sm" class="w-5 h-5" />
          </button>
        </div>
      </div>
    </div>

    <!-- File Content Area (Borderless) -->
    <div class="file-content overflow-hidden transition-all duration-300">
      <!-- 非全屏：工具栏和内容分离（保持你原来的布局习惯） -->
      <PreviewChannelToolbar
        v-if="!isContentFullscreen"
        :title="toolbarTitle"
        :dark-mode="darkMode"
        :provider-options="toolbarProviderOptions"
        v-model="toolbarProviderKey"
        :is-fullscreen="isContentFullscreen"
        :fullscreen-enter-title="$t('mount.filePreview.fullscreen')"
        :fullscreen-exit-title="$t('mount.filePreview.exitFullscreen')"
        @toggle-fullscreen="toggleFullscreen"
      >
        <template #left>
          <template v-if="isText">
            <select
              v-model="textPreviewMode"
              class="mode-select px-3 py-1 text-sm border rounded"
              :class="darkMode ? 'bg-gray-600 border-gray-500 text-gray-200' : 'bg-white border-gray-300 text-gray-700'"
            >
              <option v-for="mode in availablePreviewModes" :key="mode.value" :value="mode.value">
                {{ mode.label }}
              </option>
            </select>

            <select
              v-model="textEncoding"
              class="encoding-select px-3 py-1 text-sm border rounded"
              :class="darkMode ? 'bg-gray-600 border-gray-500 text-gray-200' : 'bg-white border-gray-300 text-gray-700'"
            >
              <option v-for="encoding in availableEncodings" :key="encoding.value" :value="encoding.value" :title="encoding.description">
                {{ encoding.label }}
              </option>
            </select>
          </template>
        </template>

        <template #right>
          <template v-if="isText">
            <div
              v-if="textPreviewMode === 'edit'"
              class="context-menu-hint flex items-center justify-center w-8 h-8 rounded-full transition-all duration-200 cursor-help hover:scale-110"
              :class="darkMode ? 'hover:bg-gray-700/50' : 'hover:bg-gray-100/50'"
              :title="$t('mount.filePreview.rightClickHint')"
            >
              <IconError class="text-yellow-500" aria-hidden="true" />
            </div>

            <button
              v-if="textPreviewMode === 'edit'"
              @click="handleSaveFile"
              :disabled="isSaving"
              class="save-btn flex items-center px-3 py-1 text-sm border rounded transition-colors"
              :class="[
                darkMode ? 'bg-blue-600 hover:bg-blue-700 border-blue-500 text-white' : 'bg-blue-500 hover:bg-blue-600 border-blue-400 text-white',
                isSaving ? 'opacity-50 cursor-not-allowed' : '',
              ]"
              :title="$t('mount.filePreview.saveFileShortcut')"
            >
              <IconRefresh v-if="isSaving" class="w-4 h-4 mr-1 animate-spin" aria-hidden="true" />
              <IconSave v-else size="sm" class="mr-1" aria-hidden="true" />
              {{ isSaving ? $t("mount.filePreview.saving") : $t("mount.filePreview.save") }}
            </button>
          </template>
        </template>
      </PreviewChannelToolbar>

      <!-- 预览内容 -->
      <div
        ref="previewContentRef"
        class="preview-content border rounded-xl overflow-hidden transition-all duration-300 flex flex-col"
        :class="[darkMode ? 'border-gray-700' : 'border-gray-200']"
        :style="previewContainerStyle"
      >
        <!-- 全屏：在全屏容器里显示一条"内置工具栏" -->
        <div
          v-if="isContentFullscreen && !isVideo"
          class="fullscreen-toolbar sticky top-0 z-20 p-3 border-b"
          :class="darkMode ? 'bg-gray-800 border-gray-700' : 'bg-white border-gray-200'"
        >
          <div class="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-3">
            <!-- 左侧：文件信息 -->
            <div class="toolbar-left flex flex-wrap items-center gap-3 min-w-0">
              <h3 class="text-lg font-medium truncate" :class="darkMode ? 'text-gray-200' : 'text-gray-800'" :title="file.name">{{ file.name }}</h3>
              <span class="text-sm" :class="darkMode ? 'text-gray-400' : 'text-gray-600'">{{ toolbarTitle }}</span>
            </div>

            <!-- 右侧：控制按钮 -->
            <div class="toolbar-right flex flex-wrap items-center gap-2">
              <!-- 文本类：模式切换 + 编码 -->
              <template v-if="isText">
                <select
                  v-model="textPreviewMode"
                  class="mode-select px-2 py-1 text-sm border rounded"
                  :class="darkMode ? 'bg-gray-700 border-gray-600 text-gray-200' : 'bg-white border-gray-300 text-gray-700'"
                >
                  <option v-for="mode in availablePreviewModes" :key="mode.value" :value="mode.value">
                    {{ mode.label }}
                  </option>
                </select>

                <select
                  v-model="textEncoding"
                  class="encoding-select px-2 py-1 text-sm border rounded"
                  :class="darkMode ? 'bg-gray-700 border-gray-600 text-gray-200' : 'bg-white border-gray-300 text-gray-700'"
                >
                  <option v-for="encoding in availableEncodings" :key="encoding.value" :value="encoding.value" :title="encoding.description">
                    {{ encoding.label }}
                  </option>
                </select>
              </template>

              <!-- 渠道选择：PDF/Office/EPUB/Iframe（多于 1 个时才显示） -->
              <select
                v-if="toolbarProviderOptions.length > 1"
                v-model="toolbarProviderKey"
                class="px-2 py-1 text-sm border rounded"
                :class="darkMode ? 'bg-gray-700 border-gray-600 text-gray-200' : 'bg-white border-gray-300 text-gray-700'"
              >
                <option v-for="opt in toolbarProviderOptions" :key="opt.key" :value="opt.key">
                  {{ opt.label }}
                </option>
              </select>

              <!-- 文本类：保存按钮（仅编辑模式） -->
              <button
                v-if="isText && textPreviewMode === 'edit'"
                @click="handleSaveFile"
                :disabled="isSaving"
                class="save-btn flex items-center px-2 py-1 text-sm border rounded transition-colors"
                :class="[
                  darkMode ? 'bg-blue-600 hover:bg-blue-700 border-blue-500 text-white' : 'bg-blue-500 hover:bg-blue-600 border-blue-400 text-white',
                  isSaving ? 'opacity-50 cursor-not-allowed' : '',
                ]"
                :title="$t('mount.filePreview.saveFileShortcut')"
              >
                <IconRefresh v-if="isSaving" class="w-4 h-4 mr-1 animate-spin" aria-hidden="true" />
                <IconSave v-else size="sm" class="mr-1" aria-hidden="true" />
                {{ isSaving ? $t("mount.filePreview.saving") : $t("mount.filePreview.save") }}
              </button>

              <!-- 退出全屏按钮 -->
              <button
                @click="toggleFullscreen"
                class="exit-fullscreen-btn flex items-center px-2 py-1 text-sm border rounded transition-colors"
                :class="darkMode ? 'bg-gray-600 hover:bg-gray-700 border-gray-500 text-gray-200' : 'bg-white hover:bg-gray-50 border-gray-300 text-gray-700'"
                :title="$t('mount.filePreview.exitFullscreen')"
              >
                <IconCollapse size="sm" aria-hidden="true" />
              </button>
            </div>
          </div>
        </div>
        <!-- 加载指示器 -->
        <div v-if="isLoading" class="flex-1 flex items-center justify-center">
          <div class="p-12">
            <LoadingIndicator
              :dark-mode="darkMode"
              size="3xl"
              :icon-class="darkMode ? 'text-primary-500' : 'text-primary-600'"
            />
          </div>
        </div>

        <!-- 图片预览 (含 Live Photo 支持) -->
        <div v-else-if="isImage" class="flex-1 flex justify-center items-center p-4">
          <!-- Live Photo 预览 -->
          <LivePhotoViewer
            v-if="isLivePhoto && authenticatedPreviewUrl && livePhotoVideoUrl"
            :photo-src="authenticatedPreviewUrl"
            :video-src="livePhotoVideoUrl"
            :dark-mode="darkMode"
            :max-width="'100%'"
            :show-badge="true"
            :show-badge-text="true"
            :show-progress="true"
            :lazy-load="true"
            :enable-vibration="true"
            class="max-w-full max-h-[600px]"
            @load="handleContentLoaded"
            @error="handleContentError"
          />
          <!-- 普通图片预览 -->
          <img
            v-else-if="authenticatedPreviewUrl"
            :src="authenticatedPreviewUrl"
            :alt="file.name"
            class="max-w-full max-h-[600px] object-contain"
            @load="handleContentLoaded"
            @error="handleContentError"
          />
          <div v-else class="loading-indicator text-center py-8">
            <LoadingIndicator
              :dark-mode="darkMode"
              size="2xl"
              :icon-class="darkMode ? 'text-primary-500' : 'text-primary-600'"
            />
          </div>
        </div>

        <!-- 视频预览 -->
        <div v-else-if="isVideo" class="flex-1 min-h-0">
          <VideoPreview
            :file="file"
            :video-url="authenticatedPreviewUrl"
            :dark-mode="darkMode"
            :is-admin="isAdmin"
            :is-fullscreen="isContentFullscreen"
            :current-path="getCurrentDirectoryPath()"
            :directory-items="directoryItems"
            @loaded="handleContentLoaded"
            @toggle-fullscreen="toggleFullscreen"
          />
        </div>

        <!-- 音频预览 -->
        <div v-else-if="isAudio" class="flex-1 flex items-center justify-center">
          <AudioPreview
            :file="file"
            :audio-url="authenticatedPreviewUrl"
            :dark-mode="darkMode"
            :is-admin="isAdmin"
            :current-path="getCurrentDirectoryPath()"
            :directory-items="directoryItems"
            @play="handleAudioPlay"
            @pause="handleAudioPause"
            @error="handleAudioError"
            @loaded="handleContentLoaded"
          />
        </div>

        <!-- PDF预览 -->
        <div v-else-if="isPdf" class="pdf-preview h-[600px]">
          <PdfFsPreview
            :preview-url="currentPdfPreviewUrl"
            :error-message="officePreviewError"
            @load="handleContentLoaded"
            @error="handleContentError"
          />
        </div>

        <!-- EPUB预览 -->
        <div v-else-if="isEpub" class="epub-preview h-[600px]">
          <EpubFsPreview
            :provider-key="selectedEpubProvider"
            :providers="resolvedPreview.providers || {}"
            :native-url="authenticatedPreviewUrl"
            :dark-mode="darkMode"
            @load="handleContentLoaded"
            @error="handleContentError"
          />
        </div>

        <!-- Office文件预览 -->
        <div v-else-if="isOffice" ref="officePreviewRef" class="office-preview">
          <OfficeFsPreview
            :preview-url="currentOfficePreviewUrl"
            :content-url="officeContentUrl"
            :filename="file.name"
            :dark-mode="darkMode"
            :provider-key="selectedOfficeProvider"
            :error-message="officePreviewError"
            :is-fullscreen="isContentFullscreen"
            @load="handleOfficePreviewLoaded"
            @error="handleOfficePreviewError"
          />
        </div>

        <!-- iframe 预览 -->
        <div v-else-if="isIframe" class="iframe-preview h-[600px]">
          <IframePreview
            :providers="iframeProviders"
            :dark-mode="darkMode"
            :loading-text="t('mount.filePreview.loadingPreview')"
            :error-text="t('mount.filePreview.previewError')"
            :selected-provider="selectedIframeProvider"
            @load="handleContentLoaded"
            @error="handleContentError"
            @provider-options="handleIframeProviderOptions"
          />
        </div>

        <!-- Markdown预览 - 使用TextRenderer统一处理 -->
        <div v-else-if="isMarkdown" :class="isContentFullscreen ? 'fullscreen-text-container' : ''">
          <TextPreview
            ref="textPreviewRef"
            :file="file"
            :text-url="authenticatedPreviewUrl"
            :dark-mode="darkMode"
            :is-admin="isAdmin"
            :current-path="getCurrentDirectoryPath()"
            :directory-items="directoryItems"
            :initial-mode="textPreviewMode"
            :initial-encoding="textEncoding"
            :max-height="dynamicMaxHeight"
            @load="handleContentLoaded"
            @error="handleContentError"
            @mode-change="handleModeChange"
            @encoding-change="handleEncodingChange"
          />
        </div>

        <!-- 文本预览 -->
        <div v-else-if="isText" :class="isContentFullscreen ? 'fullscreen-text-container' : ''">
          <TextPreview
            ref="textPreviewRef"
            :file="file"
            :text-url="authenticatedPreviewUrl"
            :dark-mode="darkMode"
            :is-admin="isAdmin"
            :current-path="getCurrentDirectoryPath()"
            :directory-items="directoryItems"
            :initial-mode="textPreviewMode"
            :initial-encoding="textEncoding"
            :max-height="dynamicMaxHeight"
            @load="handleContentLoaded"
            @error="handleContentError"
            @mode-change="handleModeChange"
            @encoding-change="handleEncodingChange"
          />
        </div>

        <!-- 其他文件类型或错误状态 -->
        <div v-else-if="loadError" class="flex-1 flex items-center justify-center">
          <div class="generic-preview text-center py-12">
            <IconExclamationSolid size="5xl" class="mx-auto mb-4" :class="darkMode ? 'text-red-400' : 'text-red-500'" aria-hidden="true" />
            <p class="text-lg font-medium mb-2" :class="darkMode ? 'text-red-300' : 'text-red-700'">{{ t("mount.filePreview.previewError") }}</p>
            <p class="text-sm" :class="darkMode ? 'text-gray-400' : 'text-gray-500'">{{ t("mount.filePreview.retryLoad") }}</p>
          </div>
        </div>

        <!-- 压缩文件预览 -->
        <div v-else-if="isArchive" class="flex-1">
          <ArchivePreview
            :file="file"
            :dark-mode="darkMode"
            :authenticated-preview-url="authenticatedPreviewUrl"
            @download="handleDownload"
            @loaded="handleContentLoaded"
            @error="handleContentError"
          />
        </div>

        <!-- 不支持预览的文件类型 -->
        <div v-else class="flex-1 flex items-center justify-center">
          <div class="generic-preview text-center py-12">
            <IconDocument size="5xl" class="mx-auto mb-4" :class="darkMode ? 'text-gray-500' : 'text-gray-400'" aria-hidden="true" />
            <p class="text-lg font-medium mb-2" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">{{ t("mount.filePreview.cannotPreview") }}</p>
            <p class="text-sm" :class="darkMode ? 'text-gray-400' : 'text-gray-500'">{{ t("mount.filePreview.downloadToView") }}</p>
          </div>
        </div>
      </div>
    </div>

    <!-- 外部播放器 Dock (仅视频类型显示，全屏时隐藏) -->
    <div v-if="isVideo && authenticatedPreviewUrl && !isContentFullscreen" class="external-player-section mt-3 overflow-visible">
      <ExternalPlayerDock
        :video-url="authenticatedPreviewUrl"
        :file-name="file?.name"
      />
    </div>
  </div>
</template>

<script setup>
import { computed, ref, watch, onBeforeUnmount } from "vue";
import { useI18n } from "vue-i18n";
import { IconCollapse, IconDocument, IconDownload, IconError, IconExclamationSolid, IconEye, IconLink, IconRefresh, IconSave, IconDatabase, IconClock } from "@/components/icons";
import LoadingIndicator from "@/components/common/LoadingIndicator.vue";
import { usePreviewRenderers, useFilePreviewExtensions, useFileSave, resolvePreviewSelection, PREVIEW_KEYS } from "@/composables/index.js";
import { useProviderSelector } from "@/composables/file-preview/useProviderSelector.js";
import { useElementFullscreen } from "@/composables/useElementFullscreen.js";
import { usePathPassword } from "@/composables/usePathPassword.js";
import { useAuthStore } from "@/stores/authStore.js";
import { useFsService } from "@/modules/fs/fsService.js";
import { getPreviewModeFromFilename, PREVIEW_MODES, SUPPORTED_ENCODINGS } from "@/utils/textUtils.js";
import AudioPreview from "./AudioPreview.vue";
import VideoPreview from "./VideoPreview.vue";
import TextPreview from "./TextPreview.vue";
import ArchivePreview from "./ArchivePreview.vue";
import PdfFsPreview from "./PdfFsPreview.vue";
import EpubFsPreview from "./EpubFsPreview.vue";
import OfficeFsPreview from "./OfficeFsPreview.vue";
import IframePreview from "@/components/common/IframePreview.vue";
import PreviewChannelToolbar from "@/components/common/preview/PreviewChannelToolbar.vue";
import { LivePhotoViewer } from "@/components/common/LivePhoto";
import { detectLivePhoto, isLivePhotoImage } from "@/utils/livePhotoUtils.js";
import ExternalPlayerDock from "./ExternalPlayerDock.vue";
import { createLogger } from "@/utils/logger.js";

const { t } = useI18n();
const log = createLogger("FilePreview");
const pathPassword = usePathPassword();
const fsService = useFsService();

// Props 定义
const props = defineProps({
  file: {
    type: Object,
    required: true,
  },
  darkMode: {
    type: Boolean,
    default: false,
  },
  isLoading: {
    type: Boolean,
    default: false,
  },
  // 认证相关props
  isAdmin: {
    type: Boolean,
    default: false,
  },
  apiKeyInfo: {
    type: Object,
    default: null,
  },
  hasFilePermission: {
    type: Boolean,
    default: false,
  },
  // 目录项目列表（用于音频播放列表等功能）
  directoryItems: {
    type: Array,
    default: () => [],
  },
});

// Emit 事件定义
const emit = defineEmits(["download", "loaded", "error", "updated", "switch-audio", "show-message", "create-share"]);

// 使用认证Store
const authStore = useAuthStore();

// 创建认证信息对象
const authInfo = computed(() => ({
  isAdmin: props.isAdmin ?? authStore.isAdmin,
  apiKeyInfo: props.apiKeyInfo ?? authStore.apiKeyInfo,
  hasFilePermission: props.hasFilePermission ?? authStore.hasPathPermission(props.file?.path || ""),
  get isAuthenticated() {
    return this.isAdmin || !!this.apiKeyInfo;
  },
  get authType() {
    return this.isAdmin ? "admin" : this.apiKeyInfo ? "apikey" : "none";
  },
}));

// 使用文件预览渲染器
const renderers = usePreviewRenderers(
  computed(() => props.file),
  emit,
  computed(() => props.darkMode)
);

// 使用文件预览扩展功能
const extensions = useFilePreviewExtensions(
  computed(() => props.file),
  authInfo,
  renderers.officePreviewLoading,
  renderers.officePreviewError,
  renderers.officePreviewTimedOut,
  renderers.previewUrl,
  renderers.handleFullscreenChange,
  renderers.handleKeyDown,
  emit,
  renderers.authenticatedPreviewUrl,
  renderers.previewTimeoutId
);

// 解构保留的预览功能
const {
  // 模板中使用的状态
  loadError,
  authenticatedPreviewUrl,
  officePreviewLoading,
  officePreviewError,
  isOfficeFullscreen,

  // 模板中使用的DOM引用
  officePreviewRef,

  // 模板中使用的方法
  formatFileSize,
  formatDate,
  toggleOfficeFullscreen,
  updateOfficePreviewUrls,
  handleContentLoaded,
  handleContentError,
} = renderers;

const resolvedPreview = computed(() => resolvePreviewSelection({ file: props.file }));

const previewKey = computed(() => resolvedPreview.value.key);
const iframeProviders = computed(() => resolvedPreview.value.providers || {});

const isImage = computed(() => previewKey.value === PREVIEW_KEYS.IMAGE);
const isVideo = computed(() => previewKey.value === PREVIEW_KEYS.VIDEO);
const isAudio = computed(() => previewKey.value === PREVIEW_KEYS.AUDIO);
const isPdf = computed(() => previewKey.value === PREVIEW_KEYS.PDF);
const isEpub = computed(() => previewKey.value === PREVIEW_KEYS.EPUB);
const isOffice = computed(() => previewKey.value === PREVIEW_KEYS.OFFICE);
const isIframe = computed(() => previewKey.value === PREVIEW_KEYS.IFRAME);
const isArchive = computed(() => previewKey.value === PREVIEW_KEYS.ARCHIVE);
const isMarkdown = computed(() => previewKey.value === PREVIEW_KEYS.MARKDOWN);
const isText = computed(() =>
  [PREVIEW_KEYS.TEXT, PREVIEW_KEYS.CODE, PREVIEW_KEYS.MARKDOWN, PREVIEW_KEYS.HTML].includes(previewKey.value)
);

// 从扩展功能中解构保留的功能
const {
  isGeneratingPreview,
  handleDownload,
  handleS3DirectPreview,
  getCurrentDirectoryPath,
  isCreatingShare,
  handleCreateShare,
  handleOfficePreviewLoaded,
  handleOfficePreviewError,
  handleAudioPlay,
  handleAudioPause,
  handleAudioError,
} = extensions;

// 智能初始模式计算属性
const smartInitialMode = computed(() => {
  if (!props.file?.name) return "text";
  return getPreviewModeFromFilename(props.file.name);
});

// 文本预览状态管理
const textPreviewMode = ref("text");
const textEncoding = ref("utf-8");
const textPreviewRef = ref(null);
const userHasManuallyChanged = ref(false);

// 文本预览标题（根据当前模式动态显示）
const textPreviewTitle = computed(() => {
  const modeLabels = {
    text: t("mount.filePreview.textPreview"),
    code: t("mount.filePreview.codePreview"),
    markdown: "Markdown",
    html: "HTML",
    edit: t("mount.filePreview.editMode"),
  };
  return modeLabels[textPreviewMode.value] || t("mount.filePreview.textPreview");
});

// iframe 预览状态管理
const selectedIframeProvider = ref("");
const iframeProviderOptionsForToolbar = ref([]);

// 处理 iframe 组件传递的 provider options
const handleIframeProviderOptions = (options) => {
  const list = Array.isArray(options) ? options : [];
  iframeProviderOptionsForToolbar.value = list;

  // 没有可用 provider 时，清空选中值，避免下拉框出现“空值但还显示”的怪状态
  if (!list.length) {
    selectedIframeProvider.value = "";
    return;
  }

  // 如果当前选中值不存在于新的 options 里，默认选中第一个（比如切换文件/规则后）
  const exists = list.some((opt) => opt.key === selectedIframeProvider.value);
  if (!exists) {
    selectedIframeProvider.value = list[0].key;
  }
};

// ===== 统一工具栏（放在 previewContentRef 内，保证全屏时也能显示）=====

const toolbarTitle = computed(() => {
  if (isText.value) return textPreviewTitle.value;
  if (isPdf.value) return "PDF";
  if (isOffice.value) return officeTypeDisplayName.value;
  if (isEpub.value) return "EPUB";
  if (isImage.value) return t("mount.filePreview.imagePreview");
  if (isVideo.value) return t("mount.filePreview.videoPreview");
  if (isAudio.value) return t("mount.filePreview.audioPreview");
  if (isIframe.value) return t("mount.filePreview.iframePreview");
  if (isArchive.value) return t("mount.filePreview.archivePreview");
  return t("mount.filePreview.previewTypeOther");
});

const toolbarProviderOptions = computed(() => {
  if (isPdf.value) return pdfProviderOptions.value;
  if (isOffice.value) return officeProviderOptions.value;
  if (isEpub.value) return epubProviderOptions.value;
  if (isIframe.value) return iframeProviderOptionsForToolbar.value;
  return [];
});

const toolbarProviderKey = computed({
  get() {
    if (isPdf.value) return selectedPdfProvider.value;
    if (isOffice.value) return selectedOfficeProvider.value;
    if (isEpub.value) return selectedEpubProvider.value;
    if (isIframe.value) return selectedIframeProvider.value;
    return "";
  },
  set(value) {
    const key = String(value || "");
    if (isPdf.value) selectedPdfProvider.value = key;
    if (isOffice.value) selectedOfficeProvider.value = key;
    if (isEpub.value) selectedEpubProvider.value = key;
    if (isIframe.value) selectedIframeProvider.value = key;
  },
});

// PDF 预览：统一用通用 providers 选择器
const {
  providerOptions: pdfProviderOptions,
  selectedKey: selectedPdfProvider,
  currentUrl: currentPdfPreviewUrl,
} = useProviderSelector({
  providers: computed(() => resolvedPreview.value.providers || {}),
  nativeUrl: authenticatedPreviewUrl,
  nativeLabel: computed(() => t("mount.filePreview.browserNative")),
  labelMap: computed(() => ({
    pdfjs: t("mount.filePreview.pdfjsLabel"),
  })),
});

// Office 预览状态管理
const selectedOfficeProvider = ref("");

// Office native 渲染内容 URL：强制使用同源 /api/fs/content（避免跨域直链 fetch）
const officeContentUrl = computed(() => {
  const fsPath = props.file?.path || "";
  if (!fsPath) return "";
  let url = `/api/fs/content?path=${encodeURIComponent(fsPath)}`;

  // 非管理员访问时，附加路径密码 token（如果存在）
  if (!props.isAdmin) {
    const token = pathPassword.getPathToken(fsPath);
    if (token) {
      url += `&path_token=${encodeURIComponent(token)}`;
    }
  }

  return url;
});

// Office provider 选项
const officeProviderOptions = computed(() => {
  const options = [];
  const providers = resolvedPreview.value.providers || {};

  for (const [key, url] of Object.entries(providers)) {
    const labelKey = `mount.filePreview.officeProvider.${key}`;
    const translated = t(labelKey);
    options.push({
      key,
      label: translated === labelKey ? key : translated,
      url,
    });
  }

  return options;
});

watch(
  officeProviderOptions,
  (options) => {
    if (!options.length) {
      selectedOfficeProvider.value = "";
      return;
    }
    // 当当前选中值为空或不在 options 中时，默认选中第一个
    const exists = options.some((opt) => opt.key === selectedOfficeProvider.value);
    if (!exists) {
      selectedOfficeProvider.value = options[0].key;
    }
  },
  { immediate: true },
);

// Office 类型显示名称
const officeTypeDisplayName = computed(() => {
  const filename = props.file?.name || "";
  const ext = filename.split(".").pop()?.toLowerCase();

  // Word 文档
  if (["doc", "docx", "odt", "rtf"].includes(ext)) {
    return t("mount.filePreview.wordPreview");
  }
  // Excel 表格
  if (["xls", "xlsx", "ods", "csv"].includes(ext)) {
    return t("mount.filePreview.excelPreview");
  }
  // PowerPoint 演示文稿
  if (["ppt", "pptx", "odp"].includes(ext)) {
    return t("mount.filePreview.powerpointPreview");
  }

  return t("mount.filePreview.officePreview");
});

// 当前 Office 预览 URL
const currentOfficePreviewUrl = computed(() => {
  const options = officeProviderOptions.value;
  if (!options.length) return "";
  const current = options.find((opt) => opt.key === selectedOfficeProvider.value) || options[0];
  return current.url || "";
});

// EPUB/电子书预览
const { providerOptions: epubProviderOptions, selectedKey: selectedEpubProvider } = useProviderSelector({
  providers: computed(() => resolvedPreview.value.providers || {}),
  nativeUrl: authenticatedPreviewUrl,
  nativeLabel: computed(() => t("mount.filePreview.browserNative")),
});

// 使用文件保存composable
const { isSaving, saveFile } = useFileSave();

// 编辑权限控制
const canEdit = computed(() => {
  // 管理员总是可以编辑
  if (authStore.isAdmin) {
    return true;
  }

  // API密钥用户需要有上传权限和路径权限
  return authStore.hasMountUploadPermission && authStore.hasPathPermission(props.file?.path || "");
});

// 可用的预览模式选项
const availablePreviewModes = computed(() => {
  const modes = [
    { value: PREVIEW_MODES.TEXT, label: "Text" },
    { value: PREVIEW_MODES.CODE, label: "Code" },
    { value: PREVIEW_MODES.MARKDOWN, label: "Markdown" },
    { value: PREVIEW_MODES.HTML, label: "HTML" },
  ];

  // 根据权限添加编辑模式
  if (canEdit.value) {
    modes.push({ value: PREVIEW_MODES.EDIT, label: "Edit" });
  }

  return modes;
});

// 可用的编码选项
const availableEncodings = computed(() => {
  // 返回所有支持的编码
  return SUPPORTED_ENCODINGS;
});

// 内容区域全屏状态管理
const previewContentRef = ref(null);
const { isFullscreen: isContentFullscreen, toggleFullscreen, exitFullscreen } = useElementFullscreen(previewContentRef, { includeChildren: false });

const previewContainerStyle = computed(() => {
  if (isContentFullscreen.value) {
    return { height: "100vh" };
  }
  // 视频预览：视频自适应
  if (isVideo.value) {
    return {
      maxHeight: "80vh",
    };
  }
  return {
    minHeight: "400px",
    maxHeight: "80vh",
  };
});

// 动态计算文本预览的最大高度（仅 TextPreview 使用）
const dynamicMaxHeight = computed(() => {
  if (isContentFullscreen.value) {
    // 全屏模式下：100vh减去工具栏高度(60px)
    return "calc(100vh - 60px)";
  } else {
    // 普通模式下：保持原有的600px限制
    return 600;
  }
});

// 保存文件功能
const handleSaveFile = async () => {
  if (!textPreviewRef.value || !textPreviewRef.value.getValue) {
    log.error("无法获取编辑器内容");
    emit("show-message", {
      type: "error",
      message: t("mount.filePreview.cannotGetEditorContent"),
    });
    return;
  }

  // 获取编辑器最新内容
  const content = textPreviewRef.value.getValue();

  // 使用composable保存文件
  const result = await saveFile(props.file.path, props.file.name, content, getCurrentDirectoryPath());

  if (result.success) {
    // 显示成功消息
    emit("show-message", {
      type: "success",
      message: result.message,
    });

    // 触发文件更新事件，通知父组件刷新
    emit("updated", {
      file: props.file,
      action: "save",
      result: result.data,
    });
  } else {
    // 显示错误消息
    emit("show-message", {
      type: "error",
      message: result.message,
    });
  }
};

// Live Photo 检测
const livePhotoData = computed(() => {
  if (!props.file?.name || !isLivePhotoImage(props.file.name)) {
    return { isLivePhoto: false, videoFile: null };
  }
  return detectLivePhoto(props.file, props.directoryItems);
});

const isLivePhoto = computed(() => livePhotoData.value.isLivePhoto);

// Live Photo 视频 URL（注意：<video> 标签无法携带自定义 header；API Key/路径密码场景必须走“预签名直链”）
const livePhotoVideoUrl = ref("");
let livePhotoVideoUrlRequestId = 0;

watch(
  () => livePhotoData.value.videoFile,
  async (videoFile) => {
    const currentRequestId = ++livePhotoVideoUrlRequestId;

    if (!videoFile) {
      livePhotoVideoUrl.value = "";
      return;
    }

    // 走 getFileLink 获取预签名 URL（适配 apikey/路径密码）
    // 说明：当前目录列表的 FsDirectoryItem 类型不包含 previewUrl，因此这里不做 previewUrl 复用分支（避免死代码与误解）。
    const videoPath = typeof videoFile.path === "string" ? videoFile.path : "";
    if (!videoPath) {
      livePhotoVideoUrl.value = "";
      return;
    }

    try {
      const url = await fsService.getFileLink(videoPath, null, false);
      if (currentRequestId !== livePhotoVideoUrlRequestId) return;
      livePhotoVideoUrl.value = url || "";
    } catch (error) {
      if (currentRequestId !== livePhotoVideoUrlRequestId) return;
      log.error("[LivePhoto] 获取视频直链失败:", error);
      livePhotoVideoUrl.value = "";
    }
  },
  { immediate: true }
);

// 监听模式变化
watch(textPreviewMode, (newMode) => {
  // 通过ref调用TextPreview组件的方法
  if (textPreviewRef.value) {
    textPreviewRef.value.switchMode(newMode);
  }
});

// 监听编码变化
watch(textEncoding, (newEncoding) => {
  // 通过ref调用TextPreview组件的方法
  if (textPreviewRef.value) {
    textPreviewRef.value.switchEncoding(newEncoding);
  }
});

// 文本预览事件处理（保留用于其他地方调用）
const handleModeChange = (newMode) => {
  textPreviewMode.value = newMode;
  userHasManuallyChanged.value = true; // 标记用户已手动切换
};

const handleEncodingChange = (newEncoding) => {
  textEncoding.value = newEncoding;
};

// 监听文件变化，智能设置初始预览模式
watch(
  () => props.file,
  (newFile, oldFile) => {
    if (newFile) {
      // 如果是新文件或文件名发生变化，重置用户手动切换标记并设置智能模式
      if (!oldFile || (oldFile && newFile.name !== oldFile.name)) {
        userHasManuallyChanged.value = false;
        textPreviewMode.value = smartInitialMode.value;
      }
    }
  },
  { immediate: true }
);

onBeforeUnmount(() => {
  void exitFullscreen();
});
</script>

<style scoped>
/* 代码高亮和Markdown预览样式 */
.preview-content pre {
  margin: 0;
  padding: 1rem;
  border-radius: 0.5rem;
  overflow: auto;
}

.preview-content code {
  font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
  font-size: 0.875rem;
  line-height: 1.5;
}

/* 全屏模式样式 */
:deep(:fullscreen) {
  background-color: white;
  padding: 0;
  overflow: auto;
  border: none !important;
  border-radius: 0 !important;
}

:deep(.dark :fullscreen) {
  background-color: #1f2937;
}

:deep(:fullscreen iframe) {
  height: calc(100vh - 60px);
  width: 100%;
  border: none;
}

/* 全屏模式下 Office 预览填满容器 */
:deep(:fullscreen .office-preview) {
  height: 100%;
}

:deep(:fullscreen .office-fs-preview-wrapper) {
  height: 100%;
  max-height: none;
}

/* 全屏模式下 EPUB 预览填满容器 */
:deep(:fullscreen .epub-preview) {
  height: 100% !important;
  max-height: none !important;
}

:deep(:fullscreen .epub-fs-preview) {
  height: 100% !important;
  width: 100% !important;
}

:deep(:fullscreen .foliate-epub-view) {
  height: 100% !important;
  width: 100% !important;
}

:deep(:fullscreen foliate-view) {
  height: 100% !important;
  width: 100% !important;
}

/* 全屏模式下 iframe 预览填满容器 */
:deep(:fullscreen .iframe-preview) {
  height: 100% !important;
  max-height: none !important;
}

:deep(:fullscreen .iframe-container) {
  height: 100% !important;
  min-height: unset !important;
}

:deep(:fullscreen .iframe-preview iframe) {
  height: 100% !important;
  width: 100% !important;
}

/* 全屏模式下 PDF 预览填满容器） */
:deep(:fullscreen .pdf-preview) {
  height: 100% !important;
  max-height: none !important;
}

/* 全屏模式下视频容器占满 */
:deep(:fullscreen .video-preview-container) {
  height: 100% !important;
  border-radius: 0 !important;
}

/* 确保全屏模式下的控制栏固定在顶部 */
:deep(:fullscreen .sticky) {
  position: sticky;
  top: 0;
  z-index: 20;
  width: 100%;
}

/* 全屏模式下的文本容器 */
:deep(:fullscreen .fullscreen-text-container) {
  height: calc(100vh - 60px);
  display: flex;
  flex-direction: column;
}

/* 全屏按钮悬停效果增强 */
button:hover svg {
  transform: scale(1.05);
  transition: transform 0.2s ease;
}

.fullscreen-toolbar {
  flex-shrink: 0;
}

/* Markdown预览样式 */
.markdown-preview {
  line-height: 1.6;
}

/* Office预览区样式 - 使用 flex 布局确保子组件正确填充 */
.office-preview {
  flex: 1;
  min-height: 0;
  display: flex;
  flex-direction: column;
}

/* Vditor相关样式 */
:deep(.vditor-reset) {
  font-size: 1rem;
  line-height: 1.7;
  padding: 0.5rem;
  transition: all 0.3s ease;
  color: v-bind('props.darkMode ? "#d4d4d4" : "#374151"');
  background-color: transparent !important;
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

/* 标题样式 */
:deep(.vditor-reset h1, .vditor-reset h2) {
  border-bottom: 1px solid v-bind('props.darkMode ? "#30363d" : "#e5e7eb"');
  padding-bottom: 0.3em;
  margin-top: 1.8em;
  margin-bottom: 1em;
}

:deep(.vditor-reset h1) {
  font-size: 2em;
}

:deep(.vditor-reset h2) {
  font-size: 1.6em;
}

:deep(.vditor-reset h3) {
  font-size: 1.4em;
  margin-top: 1.5em;
  margin-bottom: 0.75em;
}

/* 段落样式 */
:deep(.vditor-reset p) {
  margin-top: 0.75em;
  margin-bottom: 0.75em;
}

/* 行内代码样式 */
:deep(.vditor-reset code:not(.hljs)) {
  background-color: v-bind('props.darkMode ? "#252526" : "#f3f4f6"');
  color: v-bind('props.darkMode ? "#ce9178" : "#ef4444"');
  padding: 0.2em 0.4em;
  border-radius: 3px;
  font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
}

/* 引用块样式 */
:deep(.vditor-reset blockquote) {
  border-left: 4px solid v-bind('props.darkMode ? "#4b5563" : "#e5e7eb"');
  padding: 0.5em 1em;
  margin-left: 0;
  margin-right: 0;
  margin-top: 1em;
  margin-bottom: 1em;
  color: v-bind('props.darkMode ? "#9ca3af" : "#6b7280"');
  background-color: v-bind('props.darkMode ? "#1a1a1a" : "#f9fafb"');
  border-radius: 0.25rem;
}

/* 链接样式 */
:deep(.vditor-reset a) {
  color: v-bind('props.darkMode ? "#3b82f6" : "#2563eb"');
  text-decoration: none;
}

:deep(.vditor-reset a:hover) {
  text-decoration: underline;
}

/* 表格样式 */
:deep(.vditor-reset table) {
  border-collapse: collapse;
  margin: 1.25em 0;
  width: 100%;
}

:deep(.vditor-reset table th, .vditor-reset table td) {
  border: 1px solid v-bind('props.darkMode ? "#30363d" : "#e5e7eb"');
  padding: 0.6em 1em;
}

:deep(.vditor-reset table th) {
  background-color: v-bind('props.darkMode ? "#252526" : "#f3f4f6"');
  font-weight: 600;
  color: v-bind('props.darkMode ? "#e2e8f0" : "#374151"');
}

:deep(.vditor-reset table td) {
  background-color: v-bind('props.darkMode ? "#1e1e1e" : "#ffffff"');
}

:deep(.vditor-reset table tr:nth-child(even) td) {
  background-color: v-bind('props.darkMode ? "#252526" : "#f9fafb"');
}

/* 列表样式 */
:deep(.vditor-reset ul, .vditor-reset ol) {
  padding-left: 2em;
  margin: 1em 0;
}

/* 图片样式 */
:deep(.vditor-reset img) {
  max-width: 100%;
  margin: 1.25em 0;
  border-radius: 0.25rem;
}

/* 针对暗色模式的自定义样式 */
:deep(.hljs) {
  background: transparent !important;
}

/* 代码块在暗色模式下的样式 */
:deep(.vditor-reset--dark pre) {
  background-color: #1e1e1e !important;
  border: 1px solid #333 !important;
}

:deep(.vditor-reset--dark code.hljs) {
  background-color: #1e1e1e !important;
  color: #d4d4d4 !important;
}

/* 代码块在亮色模式下的样式 */
:deep(.vditor-reset--light pre) {
  background-color: #f6f8fa !important;
  border: 1px solid #e5e7eb !important;
}

:deep(.vditor-reset--light code.hljs) {
  background-color: #f6f8fa !important;
  color: #24292e !important;
}

/* 响应式调整 */
@media (max-width: 640px) {
  :deep(.vditor-reset) {
    font-size: 15px;
    padding: 0.25rem;
  }
}

/* 移动端文件预览容器优化 */
@media (max-width: 768px) {
  .file-preview-container {
    margin: 0 -1rem;
    padding: 0 0.5rem;
  }

  .file-preview {
    margin-bottom: 1rem !important;
    padding: 0.75rem !important;
    border-radius: 0.5rem !important;
  }

  /* 文件信息网格在移动端单列显示 */
  .file-info {
    grid-template-columns: 1fr !important;
    gap: 0.5rem !important;
    padding: 0.75rem !important;
  }

  /* 按钮组在移动端更紧凑 */
  .file-preview .flex.flex-wrap {
    gap: 0.5rem !important;
  }

  /* 预览内容区域优化 */
  .preview-content {
    border-radius: 0.5rem !important;
  }

  /* PDF预览高度调整 */
  .pdf-preview {
    height: 60vh !important;
  }

  /* Office预览高度调整 */
  .office-preview {
    height: 65vh !important;
  }

  /* Markdown预览高度调整 */
  .markdown-preview {
    max-height: 60vh !important;
    padding: 0.75rem !important;
  }
}

/* 全局确保代码高亮在暗色模式下可见  */
:deep(.hljs-comment) {
  color: #6a9955 !important;
}
:deep(.hljs-keyword) {
  color: #569cd6 !important;
}
:deep(.hljs-attribute) {
  color: #9cdcfe !important;
}
:deep(.hljs-literal) {
  color: #569cd6 !important;
}
:deep(.hljs-symbol) {
  color: #b5cea8 !important;
}
:deep(.hljs-name) {
  color: #569cd6 !important;
}
:deep(.hljs-tag) {
  color: #569cd6 !important;
}
:deep(.hljs-string) {
  color: #ce9178 !important;
}
:deep(.hljs-number) {
  color: #b5cea8 !important;
}
:deep(.hljs-title) {
  color: #dcdcaa !important;
}
:deep(.hljs-built_in) {
  color: #4ec9b0 !important;
}
:deep(.hljs-class) {
  color: #4ec9b0 !important;
}
:deep(.hljs-variable) {
  color: #9cdcfe !important;
}
:deep(.hljs-params) {
  color: #9cdcfe !important;
}
:deep(.hljs-meta) {
  color: #db8942 !important;
}

/* 上面是旧的：历史遗留样式（已不再使用） */

.fullscreen-text-container {
  height: calc(100vh - 60px); /* 减去工具栏高度 */
  display: flex;
  flex-direction: column;
}

.fullscreen-text-container :deep(.text-preview-wrapper) {
  height: 100%;
  flex: 1;
}

.fullscreen-text-container :deep(.editor-container) {
  height: 100%;
  min-height: unset;
}

/* 确保Monaco编辑器在全屏模式下正确显示 */
.fullscreen-text-container :deep(.monaco-editor) {
  height: 100% !important;
}
</style>
