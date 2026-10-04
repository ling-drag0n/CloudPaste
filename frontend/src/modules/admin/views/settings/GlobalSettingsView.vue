<script setup>
import { ref, onMounted, computed } from "vue";
import { useI18n } from "vue-i18n";
import { useAdminSystemService } from "@/modules/admin/services/systemService.js";
import { useThemeMode } from "@/composables/core/useThemeMode.js";
import { useGlobalMessage } from "@/composables/core/useGlobalMessage.js";
import { IconRefresh, IconUpload, IconShieldCheck } from "@/components/icons";
import { createLogger } from "@/utils/logger.js";

const { t } = useI18n();
const log = createLogger("GlobalSettingsView");
const { getGlobalSettings, updateGlobalSettings } = useAdminSystemService();
const { isDarkMode: darkMode } = useThemeMode();
const { showSuccess, showError } = useGlobalMessage();

// ============ 状态管理 ============

// 文件上传限制设置（包含默认使用代理）
const uploadSettings = ref({
  max_upload_size: 100,
  max_upload_size_unit: "MB",
  enableOverwrite: true,
  defaultUseProxy: false,
});

// 代理签名设置
const signSettings = ref({
  signAll: false,
  expires: 0,
});

// 安全设置（传输加密 / 强制 HTTPS）
const securitySettings = ref({
  fileTransferEncryption: false,
  forceHttps: false,
});

// 可选的大小单位
const sizeUnits = ["KB", "MB", "GB"];

// 加载状态
const isLoading = ref(false);
const isSavingUpload = ref(false);
const isSavingSign = ref(false);
const isSavingSecurity = ref(false);

// ============ 计算属性 ============

// 上传设置表单是否有效
const isUploadFormValid = computed(() => {
  return uploadSettings.value.max_upload_size > 0;
});

// ============ 数据加载 ============

onMounted(async () => {
  isLoading.value = true;
  try {
    const settings = await getGlobalSettings();
    settings.forEach((setting) => {
      switch (setting.key) {
        case "max_upload_size":
          uploadSettings.value.max_upload_size = parseInt(setting.value) || 100;
          uploadSettings.value.max_upload_size_unit = "MB";
          break;
        case "file_naming_strategy":
          uploadSettings.value.enableOverwrite = setting.value === "overwrite";
          break;
        case "default_use_proxy":
          uploadSettings.value.defaultUseProxy = setting.value === "true";
          break;
        case "proxy_sign_all":
          signSettings.value.signAll = setting.value === "true";
          break;
        case "proxy_sign_expires":
          signSettings.value.expires = parseInt(setting.value) || 0;
          break;
        case "file_transfer_encryption":
          securitySettings.value.fileTransferEncryption = setting.value === "true";
          break;
        case "force_https":
          securitySettings.value.forceHttps = setting.value === "true";
          break;
      }
    });
  } catch (error) {
    log.error("获取全局设置失败:", error);
    showError(t("admin.global.messages.updateFailed"));
  } finally {
    isLoading.value = false;
  }
});

// ============ 工具函数 ============

const convertToMB = (value, unit) => {
  switch (unit) {
    case "KB": return value / 1024;
    case "GB": return value * 1024;
    default: return value;
  }
};

// ============ 表单提交 ============

// 保存上传设置
const handleSaveUpload = async () => {
  if (!isUploadFormValid.value) {
    showError(t("admin.global.uploadSettings.validationError"));
    return;
  }

  isSavingUpload.value = true;
  try {
    const convertedSize = convertToMB(
      uploadSettings.value.max_upload_size,
      uploadSettings.value.max_upload_size_unit
    );

    await updateGlobalSettings({
      max_upload_size: Math.round(convertedSize),
      file_naming_strategy: uploadSettings.value.enableOverwrite ? "overwrite" : "random_suffix",
      default_use_proxy: uploadSettings.value.defaultUseProxy.toString(),
    });

    showSuccess(t("admin.global.messages.updateSuccess"));
  } catch (error) {
    log.error("更新上传设置失败:", error);
    showError(error.message || t("admin.global.messages.updateFailed"));
  } finally {
    isSavingUpload.value = false;
  }
};

// 保存签名设置
const handleSaveSign = async () => {
  isSavingSign.value = true;
  try {
    await updateGlobalSettings({
      proxy_sign_all: signSettings.value.signAll.toString(),
      proxy_sign_expires: signSettings.value.expires.toString(),
    });

    showSuccess(t("admin.global.messages.updateSuccess"));
  } catch (error) {
    log.error("更新签名设置失败:", error);
    showError(error.message || t("admin.global.messages.updateFailed"));
  } finally {
    isSavingSign.value = false;
  }
};

// 保存安全设置
const handleSaveSecurity = async () => {
  isSavingSecurity.value = true;
  try {
    await updateGlobalSettings({
      file_transfer_encryption: securitySettings.value.fileTransferEncryption.toString(),
      force_https: securitySettings.value.forceHttps.toString(),
    });

    showSuccess(t("admin.global.messages.updateSuccess"));
  } catch (error) {
    log.error("更新安全设置失败:", error);
    showError(error.message || t("admin.global.messages.updateFailed"));
  } finally {
    isSavingSecurity.value = false;
  }
};
</script>

<template>
  <div class="flex-1 flex flex-col overflow-y-auto">
    <!-- 页面标题 -->
    <div class="mb-8">
      <h1 class="text-2xl font-bold mb-2" :class="darkMode ? 'text-white' : 'text-gray-900'">
        {{ t("admin.global.title") }}
      </h1>
      <p class="text-base" :class="darkMode ? 'text-gray-400' : 'text-gray-600'">
        {{ t("admin.global.description") }}
      </p>
    </div>

    <!-- 加载状态 -->
    <div v-if="isLoading" class="flex items-center justify-center py-12">
      <IconRefresh size="lg" class="animate-spin" :class="darkMode ? 'text-gray-400' : 'text-gray-500'" />
    </div>

    <!-- 设置内容 - 垂直布局 -->
    <div v-else class="space-y-6 max-w-2xl">

      <!-- ==================== 卡片1：文件上传限制 ==================== -->
      <section
        class="rounded-xl border transition-colors"
        :class="darkMode ? 'bg-gray-800/50 border-gray-700' : 'bg-white border-gray-200'"
      >
        <!-- 卡片头部 -->
        <div class="px-5 py-4 border-b" :class="darkMode ? 'border-gray-700' : 'border-gray-200'">
          <div class="flex items-center gap-3">
            <div
              class="flex items-center justify-center w-9 h-9 rounded-lg"
              :class="darkMode ? 'bg-blue-500/20' : 'bg-blue-50'"
            >
              <IconUpload size="sm" :class="darkMode ? 'text-blue-400' : 'text-blue-600'" />
            </div>
            <div>
              <h2 class="text-base font-semibold" :class="darkMode ? 'text-white' : 'text-gray-900'">
                {{ t("admin.global.uploadSettings.title") }}
              </h2>
              <p class="text-xs" :class="darkMode ? 'text-gray-400' : 'text-gray-500'">
                {{ t("admin.global.uploadSettings.description") }}
              </p>
            </div>
          </div>
        </div>

        <!-- 卡片内容 -->
        <div class="p-5 space-y-4">
          <!-- 最大上传大小 -->
          <div class="flex items-center justify-between gap-4">
            <div class="flex-1 min-w-0">
              <label class="block text-sm font-medium" :class="darkMode ? 'text-gray-200' : 'text-gray-700'">
                {{ t("admin.global.uploadSettings.maxUploadSizeLabel") }}
                <span class="text-red-500 ml-0.5">*</span>
              </label>
            </div>
            <div class="flex items-center gap-2 flex-shrink-0">
              <input
                type="number"
                min="1"
                step="1"
                v-model.number="uploadSettings.max_upload_size"
                required
                class="w-24 px-3 py-2 border rounded-lg text-sm transition-colors focus:outline-none focus:ring-2 focus:ring-blue-500/40"
                :class="darkMode
                  ? 'bg-gray-700 border-gray-600 text-white placeholder-gray-400'
                  : 'bg-white border-gray-300 text-gray-900 placeholder-gray-500'"
                :placeholder="t('admin.global.uploadSettings.maxUploadSizePlaceholder')"
              />
              <select
                v-model="uploadSettings.max_upload_size_unit"
                class="px-3 py-2 border rounded-lg text-sm transition-colors focus:outline-none focus:ring-2 focus:ring-blue-500/40"
                :class="darkMode
                  ? 'bg-gray-700 border-gray-600 text-white'
                  : 'bg-white border-gray-300 text-gray-900'"
              >
                <option v-for="unit in sizeUnits" :key="unit" :value="unit">{{ unit }}</option>
              </select>
            </div>
          </div>

          <!-- 分隔线 -->
          <div class="border-t" :class="darkMode ? 'border-gray-700' : 'border-gray-200'"></div>

          <!-- 文件覆盖模式 -->
          <div class="flex items-center justify-between gap-4">
            <div class="flex-1 min-w-0">
              <label class="block text-sm font-medium" :class="darkMode ? 'text-gray-200' : 'text-gray-700'">
                {{ t("admin.global.uploadSettings.fileOverwriteModeLabel") }}
              </label>
              <p class="text-xs mt-0.5" :class="darkMode ? 'text-gray-400' : 'text-gray-500'">
                {{ t("admin.global.uploadSettings.fileOverwriteModeHint") }}
              </p>
            </div>
            <label class="relative inline-flex items-center cursor-pointer flex-shrink-0">
              <input type="checkbox" v-model="uploadSettings.enableOverwrite" class="sr-only peer" />
              <div class="w-11 h-6 bg-gray-200 peer-focus:outline-none peer-focus:ring-4 peer-focus:ring-blue-500/20 rounded-full peer dark:bg-gray-600 peer-checked:after:translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:left-[2px] after:bg-white after:border-gray-300 after:border after:rounded-full after:h-5 after:w-5 after:transition-all peer-checked:bg-blue-600"></div>
            </label>
          </div>

          <!-- 分隔线 -->
          <div class="border-t" :class="darkMode ? 'border-gray-700' : 'border-gray-200'"></div>

          <!-- 默认使用代理 -->
          <div class="flex items-center justify-between gap-4">
            <div class="flex-1 min-w-0">
              <label class="block text-sm font-medium" :class="darkMode ? 'text-gray-200' : 'text-gray-700'">
                {{ t("admin.global.uploadSettings.defaultUseProxyLabel") }}
              </label>
              <p class="text-xs mt-0.5" :class="darkMode ? 'text-gray-400' : 'text-gray-500'">
                {{ t("admin.global.uploadSettings.defaultUseProxyHint") }}
              </p>
            </div>
            <label class="relative inline-flex items-center cursor-pointer flex-shrink-0">
              <input type="checkbox" v-model="uploadSettings.defaultUseProxy" class="sr-only peer" />
              <div class="w-11 h-6 bg-gray-200 peer-focus:outline-none peer-focus:ring-4 peer-focus:ring-blue-500/20 rounded-full peer dark:bg-gray-600 peer-checked:after:translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:left-[2px] after:bg-white after:border-gray-300 after:border after:rounded-full after:h-5 after:w-5 after:transition-all peer-checked:bg-blue-600"></div>
            </label>
          </div>
        </div>

        <!-- 卡片底部：保存按钮 -->
        <div class="px-5 py-4 border-t flex justify-end" :class="darkMode ? 'border-gray-700 bg-gray-800/30' : 'border-gray-100 bg-gray-50/50'">
          <button
            type="button"
            @click="handleSaveUpload"
            :disabled="isSavingUpload || !isUploadFormValid"
            class="inline-flex items-center px-4 py-2 text-sm font-medium text-white rounded-lg transition-all focus:outline-none focus:ring-4 focus:ring-blue-500/30 disabled:opacity-50 disabled:cursor-not-allowed"
            :class="isSavingUpload ? 'bg-gray-400' : 'bg-blue-600 hover:bg-blue-700 active:bg-blue-800'"
          >
            <IconRefresh v-if="isSavingUpload" size="sm" class="animate-spin -ml-0.5 mr-2" />
            {{ isSavingUpload ? t("admin.global.buttons.updating") : t("admin.global.buttons.updateSettings") }}
          </button>
        </div>
      </section>

      <!-- ==================== 卡片2：代理签名设置 ==================== -->
      <section
        class="rounded-xl border transition-colors"
        :class="darkMode ? 'bg-gray-800/50 border-gray-700' : 'bg-white border-gray-200'"
      >
        <!-- 卡片头部 -->
        <div class="px-5 py-4 border-b" :class="darkMode ? 'border-gray-700' : 'border-gray-200'">
          <div class="flex items-center gap-3">
            <div
              class="flex items-center justify-center w-9 h-9 rounded-lg"
              :class="darkMode ? 'bg-emerald-500/20' : 'bg-emerald-50'"
            >
              <IconShieldCheck size="sm" :class="darkMode ? 'text-emerald-400' : 'text-emerald-600'" />
            </div>
            <div>
              <h2 class="text-base font-semibold" :class="darkMode ? 'text-white' : 'text-gray-900'">
                {{ t("admin.global.proxySignSettings.title") }}
              </h2>
              <p class="text-xs" :class="darkMode ? 'text-gray-400' : 'text-gray-500'">
                {{ t("admin.global.proxySignSettings.description") }}
              </p>
            </div>
          </div>
        </div>

        <!-- 卡片内容 -->
        <div class="p-5 space-y-4">
          <!-- 签名所有请求 -->
          <div class="flex items-center justify-between gap-4">
            <div class="flex-1 min-w-0">
              <label class="block text-sm font-medium" :class="darkMode ? 'text-gray-200' : 'text-gray-700'">
                {{ t("admin.global.proxySignSettings.signAllLabel") }}
              </label>
              <p class="text-xs mt-0.5" :class="darkMode ? 'text-gray-400' : 'text-gray-500'">
                {{ t("admin.global.proxySignSettings.signAllHint") }}
              </p>
            </div>
            <label class="relative inline-flex items-center cursor-pointer flex-shrink-0">
              <input type="checkbox" v-model="signSettings.signAll" class="sr-only peer" />
              <div class="w-11 h-6 bg-gray-200 peer-focus:outline-none peer-focus:ring-4 peer-focus:ring-blue-500/20 rounded-full peer dark:bg-gray-600 peer-checked:after:translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:left-[2px] after:bg-white after:border-gray-300 after:border after:rounded-full after:h-5 after:w-5 after:transition-all peer-checked:bg-blue-600"></div>
            </label>
          </div>

          <!-- 分隔线 -->
          <div class="border-t" :class="darkMode ? 'border-gray-700' : 'border-gray-200'"></div>

          <!-- 签名过期时间 -->
          <div class="flex items-center justify-between gap-4">
            <div class="flex-1 min-w-0">
              <label class="block text-sm font-medium" :class="darkMode ? 'text-gray-200' : 'text-gray-700'">
                {{ t("admin.global.proxySignSettings.expiresLabel") }}
              </label>
              <p class="text-xs mt-0.5" :class="darkMode ? 'text-gray-400' : 'text-gray-500'">
                {{ t("admin.global.proxySignSettings.expiresHint") }}
              </p>
            </div>
            <div class="flex items-center gap-2 flex-shrink-0">
              <input
                type="number"
                min="0"
                step="1"
                v-model.number="signSettings.expires"
                class="w-24 px-3 py-2 border rounded-lg text-sm transition-colors focus:outline-none focus:ring-2 focus:ring-blue-500/40"
                :class="darkMode
                  ? 'bg-gray-700 border-gray-600 text-white placeholder-gray-400'
                  : 'bg-white border-gray-300 text-gray-900 placeholder-gray-500'"
                placeholder="0"
              />
              <span class="text-sm" :class="darkMode ? 'text-gray-400' : 'text-gray-500'">
                {{ t("admin.global.proxySignSettings.expiresUnit") }}
              </span>
            </div>
          </div>
        </div>

        <!-- 卡片底部：保存按钮 -->
        <div class="px-5 py-4 border-t flex justify-end" :class="darkMode ? 'border-gray-700 bg-gray-800/30' : 'border-gray-100 bg-gray-50/50'">
          <button
            type="button"
            @click="handleSaveSign"
            :disabled="isSavingSign"
            class="inline-flex items-center px-4 py-2 text-sm font-medium text-white rounded-lg transition-all focus:outline-none focus:ring-4 focus:ring-blue-500/30 disabled:opacity-50 disabled:cursor-not-allowed"
            :class="isSavingSign ? 'bg-gray-400' : 'bg-blue-600 hover:bg-blue-700 active:bg-blue-800'"
          >
            <IconRefresh v-if="isSavingSign" size="sm" class="animate-spin -ml-0.5 mr-2" />
            {{ isSavingSign ? t("admin.global.buttons.updating") : t("admin.global.buttons.updateSettings") }}
          </button>
        </div>
      </section>

      <!-- 安全设置：传输加密 + 强制 HTTPS -->
      <section
        class="rounded-xl border transition-colors"
        :class="darkMode ? 'bg-gray-800/50 border-gray-700' : 'bg-white border-gray-200'"
      >
        <!-- 卡片头部 -->
        <div class="px-5 py-4 border-b" :class="darkMode ? 'border-gray-700' : 'border-gray-200'">
          <div class="flex items-center gap-3">
            <div
              class="flex items-center justify-center w-9 h-9 rounded-lg"
              :class="darkMode ? 'bg-emerald-500/20' : 'bg-emerald-50'"
            >
              <IconShieldCheck size="sm" :class="darkMode ? 'text-emerald-400' : 'text-emerald-600'" />
            </div>
            <div>
              <h2 class="text-base font-semibold" :class="darkMode ? 'text-white' : 'text-gray-900'">
                {{ t("admin.global.securitySettings.title") }}
              </h2>
              <p class="text-xs" :class="darkMode ? 'text-gray-400' : 'text-gray-500'">
                {{ t("admin.global.securitySettings.description") }}
              </p>
            </div>
          </div>
        </div>

        <!-- 卡片内容 -->
        <div class="p-5 space-y-4">
          <!-- 服务端文件传输加密 -->
          <div class="flex items-center justify-between gap-4">
            <div class="flex-1 min-w-0">
              <label class="block text-sm font-medium" :class="darkMode ? 'text-gray-200' : 'text-gray-700'">
                {{ t("admin.global.securitySettings.fileEncryptionLabel") }}
              </label>
              <p class="text-xs mt-0.5" :class="darkMode ? 'text-gray-400' : 'text-gray-500'">
                {{ t("admin.global.securitySettings.fileEncryptionHint") }}
              </p>
            </div>
            <label class="relative inline-flex items-center cursor-pointer flex-shrink-0">
              <input type="checkbox" v-model="securitySettings.fileTransferEncryption" class="sr-only peer" />
              <div class="w-11 h-6 bg-gray-200 peer-focus:outline-none peer-focus:ring-4 peer-focus:ring-blue-500/20 rounded-full peer dark:bg-gray-600 peer-checked:after:translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:left-[2px] after:bg-white after:border-gray-300 after:border after:rounded-full after:h-5 after:w-5 after:transition-all peer-checked:bg-blue-600"></div>
            </label>
          </div>

          <!-- 分隔线 -->
          <div class="border-t" :class="darkMode ? 'border-gray-700' : 'border-gray-200'"></div>

          <!-- 强制 HTTPS -->
          <div class="flex items-center justify-between gap-4">
            <div class="flex-1 min-w-0">
              <label class="block text-sm font-medium" :class="darkMode ? 'text-gray-200' : 'text-gray-700'">
                {{ t("admin.global.securitySettings.forceHttpsLabel") }}
              </label>
              <p class="text-xs mt-0.5" :class="darkMode ? 'text-gray-400' : 'text-gray-500'">
                {{ t("admin.global.securitySettings.forceHttpsHint") }}
              </p>
            </div>
            <label class="relative inline-flex items-center cursor-pointer flex-shrink-0">
              <input type="checkbox" v-model="securitySettings.forceHttps" class="sr-only peer" />
              <div class="w-11 h-6 bg-gray-200 peer-focus:outline-none peer-focus:ring-4 peer-focus:ring-blue-500/20 rounded-full peer dark:bg-gray-600 peer-checked:after:translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:left-[2px] after:bg-white after:border-gray-300 after:border after:rounded-full after:h-5 after:w-5 after:transition-all peer-checked:bg-blue-600"></div>
            </label>
          </div>
        </div>

        <!-- 卡片底部：保存按钮 -->
        <div class="px-5 py-4 border-t flex justify-end" :class="darkMode ? 'border-gray-700 bg-gray-800/30' : 'border-gray-100 bg-gray-50/50'">
          <button
            type="button"
            @click="handleSaveSecurity"
            :disabled="isSavingSecurity"
            class="inline-flex items-center px-4 py-2 text-sm font-medium text-white rounded-lg transition-all focus:outline-none focus:ring-4 focus:ring-blue-500/30 disabled:opacity-50 disabled:cursor-not-allowed"
            :class="isSavingSecurity ? 'bg-gray-400' : 'bg-blue-600 hover:bg-blue-700 active:bg-blue-800'"
          >
            <IconRefresh v-if="isSavingSecurity" size="sm" class="animate-spin -ml-0.5 mr-2" />
            {{ isSavingSecurity ? t("admin.global.buttons.updating") : t("admin.global.buttons.updateSettings") }}
          </button>
        </div>
      </section>

    </div>
  </div>
</template>
