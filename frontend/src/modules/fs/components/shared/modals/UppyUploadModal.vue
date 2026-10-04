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
          <IconClose size="lg" aria-hidden="true" />
        </button>
      </div>

      <!-- 弹窗内容区 -->
      <div class="flex-1 p-4 overflow-y-auto">
        <!-- 上传模式选择 -->
        <UploadModeSelector
          v-model="uploadMethod"
          :modes="uploadModes"
          :title="t('mount.uppy.uploadMethod')"
          :dark-mode="darkMode"
          :disabled="isUploading"
        />

        <!-- 高级功能 -->
        <AdvancedPluginsPanel
          :plugins="mediaPlugins"
          :enabled-count="enabledPluginsCount"
          :title="t('mount.uppy.advancedFeatures')"
          :enabled-count-template="t('mount.uppy.enabledCount', { count: '{count}' })"
          :dark-mode="darkMode"
          @toggle-plugin="togglePlugin"
        />

        <!-- 上传后生成分享链接 -->
        <div class="mb-4 rounded-md border" :class="darkMode ? 'border-gray-700 bg-gray-800/40' : 'border-gray-200 bg-gray-50/60'">
          <label class="flex items-center space-x-2 px-3 py-2 cursor-pointer select-none">
            <input
              type="checkbox"
              v-model="shareEnabled"
              :disabled="isUploading"
              class="h-4 w-4 rounded border-gray-300 text-blue-600 focus:ring-blue-500"
            />
            <span class="text-sm font-medium" :class="darkMode ? 'text-gray-200' : 'text-gray-700'">
              {{ t("mount.uppy.shareAfterUpload") }}
            </span>
          </label>

          <div
            v-if="shareEnabled"
            class="px-3 pb-3 pt-3 space-y-3 border-t"
            :class="darkMode ? 'border-gray-700' : 'border-gray-200'"
          >
            <!-- 备注 -->
            <div>
              <label class="block text-sm font-medium mb-1" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">{{ t("mount.shareCreate.remark") }}</label>
              <textarea
                v-model="shareForm.remark"
                rows="2"
                class="w-full px-3 py-2 border rounded-md text-sm focus:ring-blue-500 focus:border-blue-500"
                :class="darkMode ? 'bg-gray-700 border-gray-600 text-gray-100' : 'bg-white border-gray-300 text-gray-900'"
                :placeholder="t('mount.shareCreate.remarkPlaceholder')"
              ></textarea>
            </div>

            <!-- 访问密码 -->
            <div>
              <label class="block text-sm font-medium mb-1" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">{{ t("mount.shareCreate.password") }}</label>
              <input
                type="text"
                v-model="shareForm.password"
                class="w-full px-3 py-2 border rounded-md text-sm focus:ring-blue-500 focus:border-blue-500"
                :class="darkMode ? 'bg-gray-700 border-gray-600 text-gray-100' : 'bg-white border-gray-300 text-gray-900'"
                :placeholder="t('mount.shareCreate.passwordPlaceholder')"
              />
            </div>

            <!-- 有效期 -->
            <div>
              <label class="block text-sm font-medium mb-1" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">{{ t("mount.shareCreate.expiry") }}</label>
              <select
                v-model="shareForm.expires_in"
                class="w-full px-3 py-2 border rounded-md text-sm focus:ring-blue-500 focus:border-blue-500"
                :class="darkMode ? 'bg-gray-700 border-gray-600 text-gray-100' : 'bg-white border-gray-300 text-gray-900'"
              >
                <option v-for="opt in shareExpiryOptions" :key="opt.value" :value="opt.value">{{ opt.label }}</option>
              </select>
            </div>

            <!-- 下载次数上限 -->
            <div>
              <label class="block text-sm font-medium mb-1" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">{{ t("mount.shareCreate.maxViews") }}</label>
              <input
                type="number"
                min="0"
                :value="shareForm.max_views"
                @input="handleMaxViewsInput($event.target.value)"
                class="w-full px-3 py-2 border rounded-md text-sm focus:ring-blue-500 focus:border-blue-500"
                :class="darkMode ? 'bg-gray-700 border-gray-600 text-gray-100' : 'bg-white border-gray-300 text-gray-900'"
                :placeholder="t('mount.shareCreate.maxViewsPlaceholder')"
              />
            </div>

            <!-- 端到端加密 -->
            <div>
              <label class="flex items-center space-x-2 cursor-pointer select-none">
                <input
                  type="checkbox"
                  v-model="e2eEnabled"
                  :disabled="isUploading"
                  class="h-4 w-4 rounded border-gray-300 text-emerald-600 focus:ring-emerald-500"
                />
                <span class="text-sm font-medium" :class="darkMode ? 'text-gray-200' : 'text-gray-700'">
                  {{ t("mount.shareCreate.e2eEncryption") }}
                </span>
              </label>
              <p class="mt-1 text-xs" :class="darkMode ? 'text-gray-500' : 'text-gray-400'">
                {{ e2ePreparing ? t("mount.shareCreate.e2eEncrypting") : t("mount.shareCreate.e2eEncryptionHint") }}
              </p>
            </div>
          </div>
        </div>

        <!-- Uppy Dashboard 容器 -->
        <UppyDashboardContainer
          ref="uppyContainerRef"
          container-id="uppy-dashboard"
          :dark-mode="darkMode"
          :show-paste-hint="true"
          :paste-hint-prefix="t('mount.uppy.pasteSupport')"
          :paste-key="t('mount.uppy.pasteKey')"
          :paste-hint-suffix="t('mount.uppy.pasteHint')"
        />

        <!-- 分享链接结果 -->
        <div v-if="isCreatingShares || shareResults.length" class="mt-4 space-y-2">
          <div class="text-sm font-medium" :class="darkMode ? 'text-gray-200' : 'text-gray-700'">
            {{ isCreatingShares ? t("mount.uppy.shareCreating") : t("mount.uppy.shareResultsTitle") }}
          </div>
          <div v-for="(item, index) in shareResults" :key="index">
            <ShareLinkBox
              v-if="item.success"
              :dark-mode="darkMode"
              :label="item.name"
              :share-link="item.link"
              :copy-tooltip="t('mount.shareCreate.copyLink')"
              :copy-success-text="t('mount.shareCreate.linkCopied')"
              :copy-failure-text="t('mount.shareCreate.copyFailed')"
              :show-qr-button="true"
              :qr-tooltip="t('mount.shareCreate.showQRCode')"
              @show-qr-code="handleShowQRCode"
              @status-message="handleStatusMessage"
            />
            <div
              v-else
              class="p-2 rounded text-xs"
              :class="darkMode ? 'bg-red-900/20 text-red-300' : 'bg-red-50 text-red-600'"
            >
              {{ t("mount.uppy.shareFailedFor", { name: item.name, message: item.error }) }}
            </div>
          </div>
        </div>
      </div>

      <!-- 底部操作栏 -->
      <div class="p-4 border-t" :class="darkMode ? 'border-gray-700 bg-gray-800/50' : 'border-gray-200 bg-gray-50/50'">
        <div class="flex justify-between items-center">
          <div class="flex items-center space-x-4">
            <div class="flex items-center space-x-2">
              <IconFolder size="sm" :class="darkMode ? 'text-gray-400' : 'text-gray-500'" aria-hidden="true" />
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
              <IconRefresh v-if="isUploading" size="sm" class="animate-spin" aria-hidden="true" />
              <IconUpload v-else size="sm" aria-hidden="true" />
              <span>{{ isUploading ? t("mount.uppy.uploading") : t("mount.uppy.startUpload") }}</span>
            </button>
          </div>
        </div>
      </div>
    </div>

    <!-- 二维码弹窗 -->
    <QRCodeModal :visible="showQRCodeModal" :share-link="qrLink" @close="showQRCodeModal = false" @status-message="handleStatusMessage" />

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
import { IconClose, IconFolder, IconRefresh, IconUpload } from "@/components/icons";
import { createLogger } from "@/utils/logger.js";

import Dashboard from "@uppy/dashboard";

// 导入Uppy样式
import "@uppy/core/dist/style.min.css";
import "@uppy/dashboard/dist/style.min.css";
import "@uppy/webcam/dist/style.min.css";
import "@uppy/screen-capture/dist/style.min.css";
import "@uppy/audio/dist/style.min.css";
import "@uppy/image-editor/dist/style.min.css";
import "@uppy/url/dist/style.min.css";

// 导入共享CSS
import "@/styles/uppy-dashboard.css";

// 导入共享Composables
import { useUppyCore, useUppyEvents, useUppyPaste, useUppyBackendProgress } from "@/composables/uppy";

// 导入共享UI组件
import UploadModeSelector from "@/components/uppy/UploadModeSelector.vue";
import AdvancedPluginsPanel from "@/components/uppy/AdvancedPluginsPanel.vue";
import UppyDashboardContainer from "@/components/uppy/UppyDashboardContainer.vue";
import ServerResumePlugin from "@/modules/storage-core/uppy/plugins/ServerResumePlugin.js";
import Sha256PreprocessPlugin from "@/modules/storage-core/uppy/plugins/Sha256PreprocessPlugin.js";
import { api } from "@/api";
import { getUploadProgress } from "@/api/services/systemService.js";
import { useStorageConfigsStore } from "@/stores/storageConfigsStore.js";
import { STORAGE_STRATEGIES } from "@/modules/storage-core/drivers/types.js";
import { resolveDriverByConfigId } from "@/modules/storage-core/drivers/registry.js";
import { useShareUploadController } from "@/modules/upload";
import { normalizeFsPath } from "@/utils/fsPathUtils.js";
import { createShareFromFileSystem } from "@/api/services/fsService.js";
import { useShareSettingsForm } from "@/composables/upload/useShareSettingsForm.js";
import { useGlobalMessage } from "@/composables/core/useGlobalMessage.js";

// 导入插件管理器
import { createUppyPluginManager } from "@/modules/storage-core/uppy/UppyPluginManager.js";

// 导入 URL 上传服务
import { validateUrlInfo, fetchUrlContent } from "@/api/services/urlUploadService.js";

// 端到端加密
import { encryptBlobForE2E, appendE2EKeyToUrl } from "@/modules/storage-core/e2e/e2eCrypto.js";

// 导入对话框组件
import SelectUploadDialog from "@/components/common/dialogs/SelectUploadDialog.vue";
import ShareLinkBox from "@/components/common/ShareLinkBox.vue";
import QRCodeModal from "@/modules/paste/editor/components/QRCodeModal.vue";

// 组件属性
const props = defineProps({
  isOpen: { type: Boolean, default: false },
  darkMode: { type: Boolean, default: false },
  currentPath: { type: String, required: true },
  isAdmin: { type: Boolean, default: false },
});

// 事件
const emit = defineEmits(["close", "upload-success", "upload-error"]);

// 国际化
const { locale, t } = useI18n();
const log = createLogger("UppyUploadModal");

// 使用Composables
const { uppyInstance, initializeUppy, destroyUppy } = useUppyCore();
const { fileCount } = useUppyEvents({
  uppy: uppyInstance,
  onFileAdded: (file) => {
    ensureUploadIdForFile(file);
    errorMessage.value = "";
  },
  onFileRemoved: (file) => {
  },
  onError: (error) => {
    // 捕获Uppy系统错误，统一展示到错误区域
    const message = error?.message || t("file.messages.uploadFailed");
    errorMessage.value = message;
  },
});

useUppyPaste({
  uppy: uppyInstance,
  enabled: computed(() => props.isOpen),
  onPaste: (file) => {
  },
});

// 响应式数据
const uppyContainerRef = ref(null);
const uploadMethod = ref("presigned");
const canUsePresigned = ref(true);
const canUseMultipart = ref(true);
const canUseStream = ref(true);
const canUseForm = ref(true);
const currentDriverType = ref(null);
const errorMessage = ref("");
const isUploading = ref(false);
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

// 上传后生成分享链接
const { formData: shareForm, handleMaxViewsInput, resetShareSettings } = useShareSettingsForm();
const { showSuccess, showError } = useGlobalMessage();
const shareEnabled = ref(false);
const shareResults = ref([]);
const isCreatingShares = ref(false);
const showQRCodeModal = ref(false);
const qrLink = ref("");

// 端到端加密：密钥只存内存（fileId -> keyB64），绝不放入 Uppy meta（meta 会随表单上传发往服务器）
const e2eEnabled = ref(false);
const e2ePreparing = ref(false);
const e2eKeys = new Map();

// 开启端到端加密时必须生成分享链接，否则密文落库后将无法解密
watch(e2eEnabled, (enabled) => {
  if (enabled && !shareEnabled.value) {
    shareEnabled.value = true;
  }
});

const shareExpiryOptions = computed(() => [
  { value: "1", label: t("mount.shareCreate.expiryOptions.hour1") },
  { value: "24", label: t("mount.shareCreate.expiryOptions.day1") },
  { value: "168", label: t("mount.shareCreate.expiryOptions.day7") },
  { value: "720", label: t("mount.shareCreate.expiryOptions.day30") },
  { value: "0", label: t("mount.shareCreate.expiryOptions.never") },
]);

// 插件管理器实例
let pluginManager = null;

// 驱动适配器句柄
let fsAdapterHandle = null;
let fsUploadSession = null;

const disposeFsAdapterHandle = () => {
  if (fsAdapterHandle?.adapter?.destroy) {
    try {
      fsAdapterHandle.adapter.destroy();
    } catch (error) {
      log.warn("[Uppy] 清理StorageAdapter失败", error);
    }
  }
  fsAdapterHandle = null;
};

const disposeFsSession = (shouldCancel = false) => {
  if (shouldCancel) {
    try {
      fsUploadSession?.cancel?.();
    } catch {}
  }
  try {
    fsUploadSession?.destroy?.();
  } catch {}
  fsUploadSession = null;
};

const storageConfigsStore = useStorageConfigsStore();
const { createFsUploadSession } = useShareUploadController();
const driverStrategy = ref(STORAGE_STRATEGIES.PRESIGNED_SINGLE);
const mountsCache = ref([]);
const mountsLoading = ref(false);

const enforceUploadMethodByDriver = (driver) => {
  const fsCaps = driver?.capabilities?.fs || {};
  const allowPresigned = fsCaps.presignedSingle === true;
  const allowMultipart = fsCaps.multipart === true;
  const allowStream = fsCaps.backendStream !== false;
  const allowForm = fsCaps.backendForm !== false;
  canUsePresigned.value = allowPresigned;
  canUseMultipart.value = allowMultipart;
  canUseStream.value = allowStream;
  canUseForm.value = allowForm;
  currentDriverType.value = driver?.config?.storage_type || driver?.type || null;

  const pickFallback = () => {
    if (allowMultipart) return "multipart";
    if (allowPresigned) return "presigned";
    if (allowStream) return "stream";
    if (allowForm) return "form";
    return "stream";
  };

  if (uploadMethod.value === "presigned" && !allowPresigned) uploadMethod.value = pickFallback();
  if (uploadMethod.value === "multipart" && !allowMultipart) uploadMethod.value = pickFallback();
  if (uploadMethod.value === "stream" && !allowStream) uploadMethod.value = pickFallback();
  if (uploadMethod.value === "form" && !allowForm) uploadMethod.value = pickFallback();
};

const getMountRootFromPath = (path) => {
  const normalized = normalizeFsPath(path);
  const segments = normalized.split("/").filter(Boolean);
  if (!segments.length) return null;
  return `/${segments[0]}`;
};

const ensureMountsLoaded = async () => {
  if (mountsCache.value.length || mountsLoading.value) return;
  mountsLoading.value = true;
  try {
    const { useAdminMountService } = await import("@/modules/admin/services/mountService.js");
    const { getMountsList } = useAdminMountService();
    const mounts = await getMountsList();
    mountsCache.value = Array.isArray(mounts) ? mounts : [];
  } catch (error) {
    log.error("[Uppy] 加载挂载列表失败", error);
  } finally {
    mountsLoading.value = false;
  }
};

const getStorageConfigIdForCurrentPath = async () => {
  await ensureMountsLoaded();
  const mountRoot = getMountRootFromPath(props.currentPath);
  if (!mountRoot) return null;
  const mount = mountsCache.value.find((item) => item.mount_path === mountRoot);
  return mount?.storage_config_id || null;
};

const ensureStorageConfigForCurrentPath = async () => {
  const id = await getStorageConfigIdForCurrentPath();
  if (!id) throw new Error(t("file.messages.noStorageConfig"));

  // 1) 尝试通过通用 storageConfigsStore 加载配置（可能对非 admin 返回 403）
  let loadError = null;
  try {
    if (!storageConfigsStore.hasFreshCache.value) {
      await storageConfigsStore.loadConfigs();
    }
  } catch (e) {
    // 记录加载错误，但先不急着决定提示内容
    loadError = e;
  }

  // 如果缓存中已有目标配置，直接返回
  if (storageConfigsStore.getConfigById(id)) return id;

  // 如果加载过程中发生错误（例如非 admin 无权访问存储配置列表），
  // 且仍然找不到目标配置，则将原始错误抛出，交由上层统一规范化（包括权限错误提示）。
  if (loadError) {
    throw loadError;
  }

  // 2) admin 场景下的兜底：直接调用 admin storageConfigService 补全配置列表
  try {
    const { useAdminStorageConfigService } = await import("@/modules/admin/services/storageConfigService.js");
    const { getStorageConfigs } = useAdminStorageConfigService();
    const resp = await getStorageConfigs();
    if (resp?.data) {
      storageConfigsStore.replaceConfigs(resp.data);
      if (storageConfigsStore.getConfigById(id)) return id;
    }
  } catch (e) {
    // admin 兜底失败时不覆盖之前的错误语义，仍然落到“缺少配置”提示
  }

  // 3) 确实不存在该存储配置（或配置被删除）
  throw new Error(`${t("file.messages.noStorageConfig")}: ${id}`);
};

// 上传模式配置
const uploadModes = computed(() => {
  return [
    {
      value: "presigned",
      label: t("mount.uppy.presignedUpload"),
      modeLabel: t("mount.uppy.presignedMode"),
      tooltip: t("mount.uppy.presignedModeTooltip"),
      disabled: !canUsePresigned.value,
    },
    {
      value: "stream",
      label: t("mount.uppy.streamUpload"),
      modeLabel: t("mount.uppy.streamMode"),
      tooltip: t("mount.uppy.streamModeTooltip"),
      disabled: !canUseStream.value,
    },
    {
      value: "form",
      label: t("mount.uppy.formUpload"),
      modeLabel: t("mount.uppy.formMode"),
      tooltip: t("mount.uppy.formModeTooltip"),
      disabled: !canUseForm.value,
    },
    {
      value: "multipart",
      label: t("mount.uppy.multipartUpload"),
      modeLabel: t("mount.uppy.multipartMode"),
      tooltip: t("mount.uppy.multipartModeTooltip"),
      badge: t("mount.uppy.resumeSupport"),
      disabled: !canUseMultipart.value,
    },
  ];
});

const {
  ensureUploadIdForFile,
  resetBackendProgressTracking,
  updateBrowserProgressState,
  startBackendProgressPolling,
} = useUppyBackendProgress({
  uppy: uppyInstance,
  isDirectMode: () => uploadMethod.value === "stream",
});

// 计算属性
const canStartUpload = computed(() => {
  const hasFiles = fileCount.value > 0 && !isUploading.value;
  if (!hasFiles) return false;

  // 如果当前模式已被禁用，就不允许“开始上传”
  if (uploadMethod.value === "presigned") return canUsePresigned.value === true;
  if (uploadMethod.value === "multipart") return canUseMultipart.value === true;
  if (uploadMethod.value === "stream") return canUseStream.value === true;
  if (uploadMethod.value === "form") return canUseForm.value === true;
  return false;
});

const enabledPluginsCount = computed(() => {
  return pluginManager ? pluginManager.getEnabledPluginsCount() : 0;
});

/**
 * 切换插件状态
 */
const togglePlugin = (pluginKey) => {
  if (pluginManager) {
    pluginManager.togglePlugin(pluginKey);
    mediaPlugins.value = pluginManager.getPluginList();
    if (props.isOpen && uppyInstance.value) {
      setupUppy();
    }
  }
};

/**
 * 获取Dashboard插件配置
 */
const getDashboardConfig = () => ({
  inline: true,
  target: uppyContainerRef.value.container,
  theme: props.darkMode ? "dark" : "light",
  width: "100%",
  height: 400,
  showProgressDetails: true,
  showRemoveButtonAfterComplete: true,
  hideUploadButton: true,
  hidePauseResumeButton: false,
  proudlyDisplayPoweredByUppy: false,
  disableLocalFiles: false,
  metaFields: [
    {
      id: "name",
      name: t("mount.uppy.fileName"),
      placeholder: t("mount.uppy.customFilename"),
    },
  ],
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
  autoCheck: true,
  matchThreshold: 0.8,
  timeWindow: 24,
  showConfirmDialog: true,
  currentPath: props.currentPath,
  maxSelectionOptions: 5,
  showMatchScore: true,
  shouldUseMultipart: () => uploadMethod.value === 'multipart',
  resolveStorageConfigId: async () => await ensureStorageConfigForCurrentPath(),
  // 让 ServerResumePlugin 能读取 StorageAdapter 的本地分片账本（client_keeps）
  storagePrefix: fsAdapterHandle?.adapter?.config?.storagePrefix || 'uppy_multipart_',
  cacheExpiry: fsAdapterHandle?.adapter?.config?.cacheExpiry || 24 * 60 * 60 * 1000,
});

const strategyMap = {
  stream: STORAGE_STRATEGIES.BACKEND_STREAM, // 流式直传：PUT /fs/upload
  form: STORAGE_STRATEGIES.BACKEND_FORM,     // 表单上传：POST /fs/upload
  presigned: STORAGE_STRATEGIES.PRESIGNED_SINGLE,
  multipart: STORAGE_STRATEGIES.PRESIGNED_MULTIPART,
};

/**
 * 配置上传方式
 */
const configureUploadMethod = async () => {
  try {
    const storageConfigId = await ensureStorageConfigForCurrentPath();
    const driver = resolveDriverByConfigId(storageConfigId);
    enforceUploadMethodByDriver(driver);
    driverStrategy.value = strategyMap[uploadMethod.value] || STORAGE_STRATEGIES.BACKEND_STREAM;
    if (
      driver?.fs?.applyFsUploader &&
      (driverStrategy.value === STORAGE_STRATEGIES.PRESIGNED_SINGLE ||
        driverStrategy.value === STORAGE_STRATEGIES.PRESIGNED_MULTIPART ||
        driverStrategy.value === STORAGE_STRATEGIES.BACKEND_STREAM ||
        driverStrategy.value === STORAGE_STRATEGIES.BACKEND_FORM)
    ) {
      disposeFsAdapterHandle();
      const handle = driver.fs.applyFsUploader(uppyInstance.value, { strategy: driverStrategy.value, path: props.currentPath });
      fsAdapterHandle = handle ? { ...handle, mode: handle.mode || driverStrategy.value } : null;
    }
  } catch (e) {
    log.warn('[Uppy] configureUploadMethod 解析驱动失败', e);
    disposeFsAdapterHandle();
  }
};

/**
 * 配置 ServerResumePlugin 插件
 * - 避免重复安装相同 id 的插件
 * - 根据当前上传模式动态开启/关闭并更新配置
 */
const configureServerResumePlugin = () => {
  const uppy = uppyInstance.value;
  if (!uppy) return;

  const existing = uppy.getPlugin('ServerResumePlugin');

  if (uploadMethod.value === 'multipart') {
    const opts = getServerResumeConfig();
    if (existing) {
      try {
        existing.setOptions(opts);
      } catch (e) {
        log.warn('[Uppy] 更新 ServerResumePlugin 配置失败', e);
      }
    } else {
      uppy.use(ServerResumePlugin, opts);
    }
  } else if (existing) {
    // 非分片模式时移除插件，避免多次初始化
    try {
      uppy.removePlugin(existing);
    } catch (e) {
      log.warn("[Uppy] 移除 ServerResumePlugin 失败（可忽略）", e);
    }
  }
};

/**
 * 配置 Sha256PreprocessPlugin 插件（预签名模式使用）
 * “计算 sha256” 发生在 preprocess 阶段，Uppy 面板就能显示进度
 */
const configureSha256PreprocessPlugin = () => {
  const uppy = uppyInstance.value;
  if (!uppy) return;

  const existing = uppy.getPlugin("Sha256PreprocessPlugin");

  // 只有当“该驱动的预签名模式确实需要 sha256”时才开启：
  // HuggingFace LFS 需要 sha256(oid) 才能换到 uploadUrl
  const requireSha256ForPresign = fsAdapterHandle?.adapter?.config?.requireSha256ForPresign === true;

  if ((uploadMethod.value === "presigned" || uploadMethod.value === "multipart") && requireSha256ForPresign) {
    const opts = {
      enabled: true,
      maxWebCryptoSize: 10_000_000,
      metaKey: "cloudpasteSha256",
    };

    if (existing) {
      try {
        existing.setOptions(opts);
      } catch (e) {
        log.warn("[Uppy] 更新 Sha256PreprocessPlugin 配置失败", e);
      }
    } else {
      uppy.use(Sha256PreprocessPlugin, opts);
    }
  } else if (existing) {
    try {
      uppy.removePlugin(existing);
    } catch (e) {
      log.warn("[Uppy] 移除 Sha256PreprocessPlugin 失败（可忽略）", e);
    }
  }
};

/**
 * 设置断点续传对话框事件监听器
 */
const setupResumeDialogEvents = () => {
  uppyInstance.value.on("server-resume-select-dialog", (data) => {
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
 * 初始化Uppy实例
 */
const setupUppy = async () => {
  try {
    if (uppyInstance.value) {
      disposeFsSession(true);
      destroyUppy();
    }
    disposeFsAdapterHandle();

    await initializeUppy({
      id: "new-uppy-dashboard",
      locale: locale.value,
    });

    pluginManager = createUppyPluginManager(uppyInstance.value, locale.value);
    mediaPlugins.value = pluginManager.getPluginList();

    // 设置 URL 导入插件回调
    pluginManager.setUrlImportCallbacks({
      validateUrlInfo,
      fetchUrlContent,
    });

    uppyInstance.value.use(Dashboard, getDashboardConfig());

    // 安装“断点续传检查”插件：
    configureServerResumePlugin();
    if (uploadMethod.value === "multipart") {
      setupResumeDialogEvents();
    }

    await configureUploadMethod();
    // configureUploadMethod 可能会创建 StorageAdapter（其中含有 storagePrefix/cacheExpiry），
    // 这里再更新一次 ServerResumePlugin 的配置，确保 client_keeps 能读到本地账本。
    configureServerResumePlugin();

    configureSha256PreprocessPlugin();

    await pluginManager.addPluginsToUppy();
  } catch (error) {
    log.error("[Uppy] 初始化失败:", error);
    errorMessage.value = t("mount.uppy.initializationFailed", { message: error.message });
  }
};

/**
 * 根据上传完成的文件推导其 FS 挂载视图路径
 * 上传结果的最终文件名保存在 file.name（各驱动会将自定义文件名同步到此字段）
 * @param {Object} file Uppy 文件对象
 * @returns {string} FS 视图路径
 */
const buildUploadedFsPath = (file) => {
  const name = file?.name || file?.meta?.name || "";
  if (!name) return "";
  const base = normalizeFsPath(props.currentPath);
  return base === "/" ? `/${name}` : `${base}/${name}`;
};

/**
 * 上传成功后按文件逐个创建分享链接
 * - 单个文件失败不影响其他文件
 * - 结果写入 shareResults，供结果面板展示
 */
const createSharesForUploaded = async (successfulFiles) => {
  if (!shareEnabled.value || !Array.isArray(successfulFiles) || successfulFiles.length === 0) {
    return;
  }

  isCreatingShares.value = true;
  shareResults.value = [];

  try {
    for (const file of successfulFiles) {
      const name = file?.name || file?.meta?.name || "";
      const fsPath = buildUploadedFsPath(file);
      try {
        if (!fsPath) {
          throw new Error(t("mount.shareCreate.failed"));
        }
        // 端到端加密：登记加密元数据（不含密钥），并把密钥放进分享链接 fragment
        const e2eMeta = file?.meta?.cloudpasteE2E || null;
        const e2eKey = e2eKeys.get(file.id) || null;
        const resp = await createShareFromFileSystem(fsPath, {
          password: shareForm.password || "",
          expires_in: String(shareForm.expires_in ?? "0"),
          max_views: Math.max(0, Number(shareForm.max_views) || 0),
          remark: shareForm.remark || "",
          ...(e2eMeta ? { encryption_meta: e2eMeta } : {}),
        });
        if (!resp || resp.success === false) {
          throw new Error(resp?.message || t("mount.shareCreate.failed"));
        }
        const data = resp.data || {};
        let link = data.url ? `${window.location.origin}${data.url}` : "";
        if (link && e2eMeta && e2eKey) {
          link = appendE2EKeyToUrl(link, e2eKey);
        }
        shareResults.value.push({
          name,
          success: true,
          link,
          result: data,
        });
      } catch (error) {
        log.warn("[Uppy] 上传后创建分享链接失败", name, error);
        shareResults.value.push({
          name,
          success: false,
          error: error?.message || t("mount.shareCreate.failed"),
        });
      }
    }
  } finally {
    isCreatingShares.value = false;
  }
};

const handleShowQRCode = (link) => {
  qrLink.value = typeof link === "string" ? link : "";
  showQRCodeModal.value = true;
};

const handleStatusMessage = (payload) => {
  if (typeof payload === "string") {
    if (payload) showSuccess(payload);
    return;
  }
  if (!payload?.message) return;
  if (payload.type === "error") {
    showError(payload.message);
  } else {
    showSuccess(payload.message);
  }
};

const resetShareState = () => {
  shareEnabled.value = false;
  shareResults.value = [];
  isCreatingShares.value = false;
  showQRCodeModal.value = false;
  qrLink.value = "";
  e2eEnabled.value = false;
  e2ePreparing.value = false;
  e2eKeys.clear();
  resetShareSettings();
};

/**
 * 处理上传完成事件
 */
const handleUploadComplete = async (result) => {
  isUploading.value = false;

  if (result.successful.length > 0) {
    // skipUpload（秒传/去重）统计来源说明：
    // - 对 presigned-single：commit 阶段会清理 uploadSessions，所以必须在 runFsCommitIfNeeded 里先做快照并写回 result.cloudpaste
    // - 对 multipart（HuggingFace 的 skipUpload=true）：complete 阶段会清理 uploadSessions，因此必须依赖 file.meta.cloudpasteSkipUpload
    let skippedUploadCount = 0;
    try {
      const fromMeta = result.successful.filter((file) => file?.meta?.cloudpasteSkipUpload === true).length;
      const fromCloudpaste = Number(result?.cloudpaste?.skippedUploadCount || 0);
      skippedUploadCount = Math.max(fromMeta, fromCloudpaste);
    } catch {
      skippedUploadCount = Number(result?.cloudpaste?.skippedUploadCount || 0);
    }

    const successCount = result.successful.length;
    const message =
      skippedUploadCount > 0
        ? `上传完成：成功 ${successCount} 个（其中 ${skippedUploadCount} 个已跳过上传/秒传）`
        : `上传完成：成功 ${successCount} 个`;

    emit("upload-success", {
      count: result.successful.length,
      skippedUploadCount,
      message,
      commitFailures: [],
      commitStats: {
        successCount: result.successful.length,
        failureCount: 0,
        totalCount: result.successful.length,
      },
      results: result.successful.map((file) => ({
        name: file.name,
        size: file.size,
        type: file.type,
        url: file.uploadURL,
      })),
    });

    setTimeout(() => {
      if (uppyInstance.value) {
        uppyInstance.value.clear();
      }
    }, 4000);

    // 上传后生成分享链接（开关关闭时保持原有行为）
    if (shareEnabled.value) {
      await createSharesForUploaded(result.successful);
    }
  }

  if (result.failed.length > 0) {
    const firstError = result.failed[0].error;
    emit("upload-error", new Error(firstError?.message || "上传失败"));
  }
};

const runFsCommitIfNeeded = async (result) => {
  if (!result) return;
  result.failed = result.failed || [];
  result.successful = result.successful || [];
  if (!fsAdapterHandle?.adapter || fsAdapterHandle.mode !== STORAGE_STRATEGIES.PRESIGNED_SINGLE) {
    return;
  }
  if (!result.successful.length) {
    return;
  }

  // 先快照本次上传的 skipUpload 信息（对象已存在/秒传/去重）：
  // commitPresignedUpload 成功后会 delete uploadSessions，所以必须在 commit 前保存。
  const adapter = fsAdapterHandle.adapter;
  const skipSnapshot = {};
  try {
    if (adapter?.isUploadSkipped && typeof adapter.isUploadSkipped === "function") {
      result.successful.forEach((file) => {
        skipSnapshot[file.id] = adapter.isUploadSkipped(file.id) === true;
      });
    }
  } catch (e) {
    log.warn("[Uppy] 生成 skipUpload 快照失败，将忽略该提示", e);
  }

  try {
    const summary = await fsAdapterHandle.adapter.batchCommitPresignedUploads(result.successful);
    const failures = summary?.failures || [];
    if (!failures.length) {
      result.cloudpaste = {
        ...(result.cloudpaste || {}),
        skippedUploadCount: result.successful.filter((file) => skipSnapshot[file.id] === true).length,
      };
      return;
    }

    const failureMap = new Map();
    failures.forEach((failure) => {
      const error = new Error(failure.error || "提交预签名上传失败");
      failureMap.set(failure.fileId, error);
    });

    const remaining = [];
    result.successful.forEach((file) => {
      const failureError = failureMap.get(file.id);
      if (failureError) {
        try {
          uppyInstance.value?.emit?.("upload-error", file, failureError);
        } catch {}
        result.failed.push({ file, error: failureError });
      } else {
        remaining.push(file);
      }
    });

    result.successful = remaining;
    result.cloudpaste = {
      ...(result.cloudpaste || {}),
      skippedUploadCount: result.successful.filter((file) => skipSnapshot[file.id] === true).length,
    };
  } catch (error) {
    const failureError = error instanceof Error ? error : new Error(String(error));
    const failedFiles = result.successful.slice();
    failedFiles.forEach((file) => {
      try {
        uppyInstance.value?.emit?.("upload-error", file, failureError);
      } catch {}
      result.failed.push({ file, error: failureError });
    });
    result.successful = [];
    result.cloudpaste = {
      ...(result.cloudpaste || {}),
      skippedUploadCount: 0,
    };
  }
};

/**
 * 规范化 FS 上传错误信息，统一处理权限错误等场景
 */
const normalizeFsUploadError = (error) => {
  if (!error) {
    return t("file.messages.uploadFailed");
  }

  const status = error?.request?.status || error?.response?.status;
  const body = error?.response?.body;
  const code = body?.code || error.code;

  // 权限错误：后端返回 403 或特定错误码时，统一提示为“没有权限使用此存储配置”
  if (status === 403 || code === "FS_PERMISSION_DENIED") {
    return t("file.messages.permissionError");
  }

  if (typeof body?.message === "string" && body.message) {
    return body.message;
  }

  if (typeof error.message === "string" && error.message) {
    return error.message;
  }

  return t("file.messages.uploadFailed");
};

/**
 * 端到端加密准备：把 Uppy 队列内所有本地文件的 data 替换为密文 Blob
 * - 密钥保存在组件内存 e2eKeys（fileId -> keyB64），不进 meta
 * - meta 只带 cloudpasteE2E（加密元数据，不含密钥），供创建分享时登记
 */
const prepareE2EForUppyFiles = async () => {
  const uppy = uppyInstance.value;
  if (!uppy || !e2eEnabled.value) return;
  e2ePreparing.value = true;
  try {
    for (const file of uppy.getFiles()) {
      if (!file?.id || file?.meta?.cloudpasteE2E) continue;
      const blob = file?.data instanceof Blob ? file.data : null;
      if (!blob) continue; // remote/URL 导入文件不支持本地加密
      const { cipherBlob, meta, keyB64 } = await encryptBlobForE2E(blob);
      try {
        uppy.setFileState(file.id, { data: cipherBlob, size: cipherBlob.size });
      } catch {
        file.data = cipherBlob;
        file.size = cipherBlob.size;
      }
      try {
        uppy.setFileMeta(file.id, { cloudpasteE2E: meta });
      } catch {}
      e2eKeys.set(file.id, keyB64);
    }
  } finally {
    e2ePreparing.value = false;
  }
};

/**
 * 开始上传
 */
const startUpload = async () => {
  if (!uppyInstance.value || !canStartUpload.value || isUploading.value) {
    return;
  }

  try {
    errorMessage.value = "";
    isUploading.value = true;
    const storageConfigId = await ensureStorageConfigForCurrentPath();
    try {
      const driver = resolveDriverByConfigId(storageConfigId);
      enforceUploadMethodByDriver(driver);
    } catch (e) {
      log.warn("[Uppy] startUpload 驱动解析失败", e);
    }

    driverStrategy.value = strategyMap[uploadMethod.value] || STORAGE_STRATEGIES.BACKEND_STREAM;

    // 端到端加密：上传前把文件内容替换为密文（字节层面加密，对所有上传通道透明）
    await prepareE2EForUppyFiles();

    disposeFsSession();
    resetBackendProgressTracking();
    fsUploadSession = createFsUploadSession({
      storageConfigId,
      fsOptions: { strategy: driverStrategy.value, path: props.currentPath },
      uppy: uppyInstance.value,
      events: {
        onProgress: (progress) => {
          if (progress) {
            updateBrowserProgressState(progress);
          }
        },
        onError: ({ error }) => {
          errorMessage.value = normalizeFsUploadError(error);
        },
        onComplete: async (result) => {
          try {
            await runFsCommitIfNeeded(result);
            await handleUploadComplete(result);
          } finally {
            resetBackendProgressTracking();
            disposeFsSession();
          }
        },
      },
    });

    if (uploadMethod.value === "stream") {
      try {
        uppyInstance.value.getFiles().forEach((file) => ensureUploadIdForFile(file));
      } catch {}
      startBackendProgressPolling();
    }

    await fsUploadSession.start();
  } catch (error) {
    log.error("[Uppy] 上传失败", error);
    errorMessage.value = normalizeFsUploadError(error);
    emit("upload-error", error);
    disposeFsSession();
  } finally {
    resetBackendProgressTracking();
    isUploading.value = false;
  }
};

const closeModal = () => {
  if (uppyInstance.value) {
    uppyInstance.value.cancelAll?.();
    uppyInstance.value.clear();
  }
  errorMessage.value = "";
  isUploading.value = false;
  resetShareState();
  resetBackendProgressTracking();
  disposeFsSession(true);
  disposeFsAdapterHandle();
  emit("close");
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

// 监听上传方式变化，重新初始化 Uppy（确保清理旧插件）
watch(
  () => uploadMethod.value,
  async () => {
    if (props.isOpen && uppyInstance.value && !isUploading.value) {
      resetBackendProgressTracking();
      disposeFsSession(true);
      await setupUppy();
    }
  }
);

// 监听弹窗打开状态
watch(
  () => props.isOpen,
  async (newValue) => {
    if (newValue) {
      resetShareState();
      await ensureMountsLoaded();
      try {
        await storageConfigsStore.loadConfigs();
      } catch (e) {}
      await nextTick();
      await setupUppy();
    } else if (uppyInstance.value) {
      uppyInstance.value.cancelAll?.();
      uppyInstance.value.clear();
      errorMessage.value = "";
      isUploading.value = false;
      resetShareState();
      resetBackendProgressTracking();
      disposeFsSession(true);
      disposeFsAdapterHandle();
    }
  }
);

// 监听路径变化
watch(
  () => props.currentPath,
  async () => {
    if (!props.isOpen || !uppyInstance.value || isUploading.value) {
      return;
    }
    resetBackendProgressTracking();
    disposeFsSession(true);
    await setupUppy();
  }
);

// 监听暗色模式变化
watch(
  () => props.darkMode,
  async () => {
    if (props.isOpen && uppyInstance.value) {
      resetBackendProgressTracking();
      disposeFsSession(true);
      await setupUppy();
    }
  }
);

// 监听语言变化
watch(
  () => locale.value,
  () => {
    if (pluginManager) {
      pluginManager.updateLocale(locale.value);
      mediaPlugins.value = pluginManager.getPluginList();
    }
    if (props.isOpen && uppyInstance.value) {
      resetBackendProgressTracking();
      disposeFsSession(true);
      setupUppy();
    }
  }
);

// 生命周期
onMounted(async () => {
  if (props.isOpen) {
    await ensureMountsLoaded();
    await nextTick();
    await setupUppy();
  }
});

onBeforeUnmount(() => {
  destroyUppy();
  resetBackendProgressTracking();
  disposeFsSession(true);
  disposeFsAdapterHandle();
});
</script>

<style scoped>
/* 组件特有样式 */
</style>
