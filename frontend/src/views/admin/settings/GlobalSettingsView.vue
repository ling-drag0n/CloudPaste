<script setup>
import { ref, onMounted } from "vue";
import { api } from "@/api";
import { useI18n } from "vue-i18n";

// 使用i18n
const { t } = useI18n();

// 定义props
const props = defineProps({
  darkMode: {
    type: Boolean,
    required: true,
  },
});

// 上传限制设置
const uploadSettings = ref({
  max_upload_size: 100,
  max_upload_size_unit: "MB",
});

// 可选的大小单位
const sizeUnits = ref(["KB", "MB", "GB"]);

// 上传设置更新状态
const uploadStatus = ref({
  loading: false,
  success: false,
  error: "",
});

// 代理签名设置
const proxySignSettings = ref({
  signAll: false,
  expires: 0,
});

// 代理签名设置更新状态
const proxySignStatus = ref({
  loading: false,
  success: false,
  error: "",
});

// 默认代理设置
const defaultProxySettings = ref({
  defaultUseProxy: false,
});

// 文件命名策略设置
const fileNamingSettings = ref({
  enableOverwrite: true, // true = 覆盖模式（默认）, false = 随机后缀模式
});

// 移除未使用的提示框状态

// 获取设置数据（使用新的分组API）
onMounted(async () => {
  try {
    // 使用新的分组API获取全局设置（分组ID = 1）
    const response = await api.system.getSettingsByGroup(1, true);
    if (response && response.success && response.data) {
      response.data.forEach((setting) => {
        if (setting.key === "max_upload_size") {
          const value = parseInt(setting.value);
          uploadSettings.value.max_upload_size = value;
          uploadSettings.value.max_upload_size_unit = "MB";
        } else if (setting.key === "proxy_sign_all") {
          proxySignSettings.value.signAll = setting.value === "true";
        } else if (setting.key === "proxy_sign_expires") {
          proxySignSettings.value.expires = parseInt(setting.value) || 0;
        } else if (setting.key === "default_use_proxy") {
          defaultProxySettings.value.defaultUseProxy = setting.value === "true";
        } else if (setting.key === "file_naming_strategy") {
          fileNamingSettings.value.enableOverwrite = setting.value === "overwrite";
        }
      });
    } else {
      throw new Error(response?.message || "获取设置失败");
    }
  } catch (error) {
    console.error("获取全局设置失败:", error);
  }
});

// 将值根据单位转换为MB
const convertToMB = (value, unit) => {
  switch (unit) {
    case "KB":
      return value / 1024;
    case "MB":
      return value;
    case "GB":
      return value * 1024;
    default:
      return value;
  }
};

// 更新上传限制设置
const handleUpdateUploadSettings = async (event) => {
  event.preventDefault();

  if (!uploadSettings.value.max_upload_size || uploadSettings.value.max_upload_size <= 0) {
    uploadStatus.value.error = t("admin.global.uploadSettings.validationError");
    return;
  }

  uploadStatus.value = {
    loading: true,
    success: false,
    error: "",
  };

  try {
    const convertedSize = convertToMB(uploadSettings.value.max_upload_size, uploadSettings.value.max_upload_size_unit);

    // 使用新的分组更新API（全局设置组，分组ID = 1）
    const response = await api.system.updateGroupSettings(
      1,
      {
        max_upload_size: Math.round(convertedSize),
        default_use_proxy: defaultProxySettings.value.defaultUseProxy.toString(),
        file_naming_strategy: fileNamingSettings.value.enableOverwrite ? "overwrite" : "random_suffix",
      },
      true
    );

    if (response && response.success) {
      uploadStatus.value.success = true;
      setTimeout(() => {
        uploadStatus.value.success = false;
      }, 3000);
    } else {
      throw new Error(response?.message || "更新失败");
    }
  } catch (error) {
    uploadStatus.value.error = error.message || t("admin.global.messages.updateFailed");
  } finally {
    uploadStatus.value.loading = false;
  }
};

// 更新代理签名设置
const handleUpdateProxySignSettings = async (event) => {
  event.preventDefault();

  proxySignStatus.value = {
    loading: true,
    success: false,
    error: "",
  };

  try {
    // 使用新的分组更新API（代理签名设置也属于全局设置组，分组ID = 1）
    const response = await api.system.updateGroupSettings(
      1,
      {
        proxy_sign_all: proxySignSettings.value.signAll.toString(),
        proxy_sign_expires: proxySignSettings.value.expires.toString(),
      },
      true
    );

    if (response && response.success) {
      proxySignStatus.value.success = true;
      setTimeout(() => {
        proxySignStatus.value.success = false;
      }, 3000);
    } else {
      throw new Error(response?.message || "更新失败");
    }
  } catch (error) {
    proxySignStatus.value.error = error.message || t("admin.global.messages.updateFailed");
  } finally {
    proxySignStatus.value.loading = false;
  }
};
</script>

<template>
  <div class="flex-1 flex flex-col overflow-y-auto">
    <!-- 页面标题 -->
    <div class="mb-6">
      <h1 class="text-2xl font-bold mb-2" :class="darkMode ? 'text-white' : 'text-gray-800'">{{ t("admin.global.title") }}</h1>
      <p class="text-base" :class="darkMode ? 'text-gray-300' : 'text-gray-600'">{{ t("admin.global.description") }}</p>
    </div>

    <!-- 设置分组 -->
    <div class="space-y-6">
      <!-- 上传限制设置组 -->
      <div class="setting-group bg-white dark:bg-gray-800 rounded-lg p-6 border border-gray-200 dark:border-gray-700 max-w-2xl">
        <h2 class="text-lg font-medium mb-4 pb-2 border-b border-gray-200 dark:border-gray-700">{{ t("admin.global.uploadSettings.title") }}</h2>
        <div class="space-y-4">
          <!-- 状态消息 -->
          <div
            v-if="uploadStatus.success"
            class="mb-4 rounded-lg p-4 border transition-colors duration-200"
            :class="darkMode ? 'bg-green-900/20 border-green-800/40 text-green-200' : 'bg-green-50 border-green-200 text-green-800'"
          >
            <div class="flex items-center">
              <svg class="h-5 w-5 mr-2 flex-shrink-0" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20" fill="currentColor">
                <path
                  fill-rule="evenodd"
                  d="M10 18a8 8 0 100-16 8 8 0 000 16zm3.707-9.293a1 1 0 00-1.414-1.414L9 10.586 7.707 9.293a1 1 0 00-1.414 1.414l2 2a1 1 0 001.414 0l4-4z"
                  clip-rule="evenodd"
                />
              </svg>
              <p class="text-sm font-medium">{{ t("admin.global.messages.updateSuccess") }}</p>
            </div>
          </div>

          <div
            v-if="uploadStatus.error"
            class="mb-4 rounded-lg p-4 border transition-colors duration-200"
            :class="darkMode ? 'bg-red-900/20 border-red-800/40 text-red-200' : 'bg-red-50 border-red-200 text-red-800'"
          >
            <div class="flex items-center">
              <svg class="h-5 w-5 mr-2 flex-shrink-0" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20" fill="currentColor">
                <path
                  fill-rule="evenodd"
                  d="M10 18a8 8 0 100-16 8 8 0 000 16zM8.707 7.293a1 1 0 00-1.414 1.414L8.586 10l-1.293 1.293a1 1 0 101.414 1.414L10 11.414l1.293 1.293a1 1 0 001.414-1.414L11.414 10l1.293-1.293a1 1 0 00-1.414-1.414L10 8.586 8.707 7.293z"
                  clip-rule="evenodd"
                />
              </svg>
              <p class="text-sm font-medium">{{ uploadStatus.error }}</p>
            </div>
          </div>

          <!-- 上传限制设置表单 -->
          <form @submit="handleUpdateUploadSettings" class="space-y-6">
            <div class="setting-item">
              <label class="block text-sm font-medium text-gray-900 dark:text-gray-100 mb-2">
                {{ t("admin.global.uploadSettings.maxUploadSizeLabel") }}
                <span class="text-red-500 ml-0.5">*</span>
              </label>
              <p class="text-xs text-gray-500 dark:text-gray-400 mb-3">{{ t("admin.global.uploadSettings.description") }}</p>
              <div class="space-y-4">
                <div class="flex items-center space-x-3 max-w-md">
                  <input
                    type="number"
                    min="1"
                    step="1"
                    name="maxUploadSize"
                    id="maxUploadSize"
                    v-model.number="uploadSettings.max_upload_size"
                    required
                    class="flex-1 px-3 py-2 border rounded-md text-sm transition-colors focus:outline-none focus:ring-2 focus:ring-primary-500/20"
                    :class="darkMode ? 'bg-gray-700 border-gray-600 text-white focus:border-primary-500' : 'bg-white border-gray-300 text-gray-900 focus:border-primary-500'"
                    placeholder="100"
                  />
                  <select
                    v-model="uploadSettings.max_upload_size_unit"
                    class="px-3 py-2 border rounded-md text-sm transition-colors focus:outline-none focus:ring-2 focus:ring-primary-500/20"
                    :class="darkMode ? 'bg-gray-700 border-gray-600 text-white focus:border-primary-500' : 'bg-white border-gray-300 text-gray-900 focus:border-primary-500'"
                  >
                    <option v-for="unit in sizeUnits" :key="unit" :value="unit">
                      {{ unit }}
                    </option>
                  </select>
                </div>

                <!-- 默认代理设置 -->
                <div class="setting-item mt-6">
                  <div class="flex items-start justify-between">
                    <div class="flex-1">
                      <label class="block text-sm font-medium text-gray-900 dark:text-gray-100 mb-2"> {{ t("admin.global.uploadSettings.defaultUseProxyLabel") }} </label>
                      <p class="text-xs text-gray-500 dark:text-gray-400">{{ t("admin.global.uploadSettings.defaultUseProxyHint") }}</p>
                    </div>
                    <div class="flex-shrink-0 ml-4">
                      <label class="relative inline-flex items-center cursor-pointer">
                        <input type="checkbox" id="defaultUseProxy" v-model="defaultProxySettings.defaultUseProxy" class="sr-only peer" />
                        <div
                          class="w-11 h-6 bg-gray-200 peer-focus:outline-none peer-focus:ring-4 peer-focus:ring-blue-300 dark:peer-focus:ring-blue-800 rounded-full peer dark:bg-gray-700 peer-checked:after:translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:left-[2px] after:bg-white after:border-gray-300 after:border after:rounded-full after:h-5 after:w-5 after:transition-all dark:border-gray-600 peer-checked:bg-blue-600"
                        ></div>
                      </label>
                    </div>
                  </div>
                </div>

                <!-- 文件命名策略设置 -->
                <div class="setting-item">
                  <div class="flex items-center justify-between">
                    <div class="flex-1">
                      <label class="block text-sm font-medium text-gray-900 dark:text-gray-100 mb-1">{{ t("admin.global.uploadSettings.fileOverwriteModeLabel") }}</label>
                      <p class="text-xs text-gray-500 dark:text-gray-400">{{ t("admin.global.uploadSettings.fileOverwriteModeHint") }}</p>
                    </div>
                    <div class="flex-shrink-0 ml-4">
                      <label class="relative inline-flex items-center cursor-pointer">
                        <input type="checkbox" id="enableOverwrite" v-model="fileNamingSettings.enableOverwrite" class="sr-only peer" />
                        <div
                          class="w-11 h-6 bg-gray-200 peer-focus:outline-none peer-focus:ring-4 peer-focus:ring-blue-300 dark:peer-focus:ring-blue-800 rounded-full peer dark:bg-gray-700 peer-checked:after:translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:left-[2px] after:bg-white after:border-gray-300 after:border after:rounded-full after:h-5 after:w-5 after:transition-all dark:border-gray-600 peer-checked:bg-blue-600"
                        ></div>
                      </label>
                    </div>
                  </div>
                </div>

                <div class="flex justify-start">
                  <button
                    type="submit"
                    :disabled="uploadStatus.loading"
                    class="inline-flex items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md text-white transition-colors"
                    :class="uploadStatus.loading ? 'bg-gray-400 cursor-not-allowed' : 'bg-primary-600 hover:bg-primary-700'"
                  >
                    <svg v-if="uploadStatus.loading" class="animate-spin -ml-1 mr-2 h-4 w-4" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                      <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
                      <path
                        class="opacity-75"
                        fill="currentColor"
                        d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"
                      ></path>
                    </svg>
                    {{ uploadStatus.loading ? t("admin.global.buttons.updating") : t("admin.global.buttons.updateSettings") }}
                  </button>
                </div>
              </div>
            </div>
          </form>
        </div>
      </div>

      <!-- 代理签名设置组 -->
      <div class="setting-group bg-white dark:bg-gray-800 rounded-lg p-6 border border-gray-200 dark:border-gray-700 max-w-2xl">
        <h2 class="text-lg font-medium mb-4 pb-2 border-b border-gray-200 dark:border-gray-700">{{ t("admin.global.proxySignSettings.title") }}</h2>
        <div class="space-y-4">
          <!-- 状态消息 -->
          <div
            v-if="proxySignStatus.success"
            class="mb-4 rounded-lg p-4 border transition-colors duration-200"
            :class="darkMode ? 'bg-green-900/20 border-green-800/40 text-green-200' : 'bg-green-50 border-green-200 text-green-800'"
          >
            <div class="flex items-center">
              <svg class="h-5 w-5 mr-2 flex-shrink-0" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20" fill="currentColor">
                <path
                  fill-rule="evenodd"
                  d="M10 18a8 8 0 100-16 8 8 0 000 16zm3.707-9.293a1 1 0 00-1.414-1.414L9 10.586 7.707 9.293a1 1 0 00-1.414 1.414l2 2a1 1 0 001.414 0l4-4z"
                  clip-rule="evenodd"
                />
              </svg>
              <p class="text-sm font-medium">{{ t("admin.global.messages.updateSuccess") }}</p>
            </div>
          </div>

          <div
            v-if="proxySignStatus.error"
            class="mb-4 rounded-lg p-4 border transition-colors duration-200"
            :class="darkMode ? 'bg-red-900/20 border-red-800/40 text-red-200' : 'bg-red-50 border-red-200 text-red-800'"
          >
            <div class="flex items-center">
              <svg class="h-5 w-5 mr-2 flex-shrink-0" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20" fill="currentColor">
                <path
                  fill-rule="evenodd"
                  d="M10 18a8 8 0 100-16 8 8 0 000 16zM8.707 7.293a1 1 0 00-1.414 1.414L8.586 10l-1.293 1.293a1 1 0 101.414 1.414L10 11.414l1.293 1.293a1 1 0 001.414-1.414L11.414 10l1.293-1.293a1 1 0 00-1.414-1.414L10 8.586 8.707 7.293z"
                  clip-rule="evenodd"
                />
              </svg>
              <p class="text-sm font-medium">{{ proxySignStatus.error }}</p>
            </div>
          </div>

          <!-- 代理签名设置表单 -->
          <form @submit="handleUpdateProxySignSettings" class="space-y-6">
            <!-- 签名所有请求开关 -->
            <div class="setting-item">
              <div class="flex items-start justify-between">
                <div class="flex-1">
                  <label class="block text-sm font-medium text-gray-900 dark:text-gray-100 mb-2"> {{ t("admin.global.proxySignSettings.signAllLabel") }} </label>
                  <p class="text-xs text-gray-500 dark:text-gray-400">{{ t("admin.global.proxySignSettings.signAllHint") }}</p>
                </div>
                <div class="flex-shrink-0 ml-4">
                  <label class="relative inline-flex items-center cursor-pointer">
                    <input type="checkbox" id="signAll" v-model="proxySignSettings.signAll" class="sr-only peer" />
                    <div
                      class="w-11 h-6 bg-gray-200 peer-focus:outline-none peer-focus:ring-4 peer-focus:ring-primary-300 dark:peer-focus:ring-primary-800 rounded-full peer dark:bg-gray-700 peer-checked:after:translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:left-[2px] after:bg-white after:border-gray-300 after:border after:rounded-full after:h-5 after:w-5 after:transition-all dark:border-gray-600 peer-checked:bg-primary-600"
                    ></div>
                  </label>
                </div>
              </div>
            </div>

            <!-- 过期时间设置 -->
            <div class="setting-item">
              <label class="block text-sm font-medium text-gray-900 dark:text-gray-100 mb-2"> {{ t("admin.global.proxySignSettings.expiresLabel") }} </label>
              <p class="text-xs text-gray-500 dark:text-gray-400 mb-3">{{ t("admin.global.proxySignSettings.expiresHint") }}</p>
              <div class="flex items-center space-x-3 max-w-md">
                <input
                  type="number"
                  min="0"
                  step="1"
                  name="expires"
                  id="expires"
                  v-model.number="proxySignSettings.expires"
                  class="flex-1 px-3 py-2 border rounded-md text-sm transition-colors focus:outline-none focus:ring-2 focus:ring-primary-500/20"
                  :class="darkMode ? 'bg-gray-700 border-gray-600 text-white focus:border-primary-500' : 'bg-white border-gray-300 text-gray-900 focus:border-primary-500'"
                  placeholder="0"
                />
                <span class="text-sm text-gray-500 dark:text-gray-400">{{ t("admin.global.proxySignSettings.expiresUnit") }}</span>
              </div>
            </div>

            <!-- 保存按钮 -->
            <div class="mt-6 pt-4 border-t border-gray-200 dark:border-gray-700">
              <button
                type="submit"
                :disabled="proxySignStatus.loading"
                class="inline-flex items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md text-white transition-colors"
                :class="proxySignStatus.loading ? 'bg-gray-400 cursor-not-allowed' : 'bg-primary-600 hover:bg-primary-700'"
              >
                <svg v-if="proxySignStatus.loading" class="animate-spin -ml-1 mr-2 h-4 w-4" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                  <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
                  <path
                    class="opacity-75"
                    fill="currentColor"
                    d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"
                  ></path>
                </svg>
                {{ proxySignStatus.loading ? t("admin.global.buttons.updating") : t("admin.global.buttons.updateSettings") }}
              </button>
            </div>
          </form>
        </div>
      </div>
    </div>
  </div>
</template>

<style scoped>
/* 移动端适配 */
@media (max-width: 768px) {
  .settings-grid {
    grid-template-columns: 1fr;
  }
}
</style>
