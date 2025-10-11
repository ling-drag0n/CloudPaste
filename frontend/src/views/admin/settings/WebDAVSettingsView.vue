<script setup>
import { ref, onMounted } from "vue";
import { api } from "@/api";
import { API_BASE_URL } from "@/api/config";
import { useI18n } from "vue-i18n";

// 使用i18n
const { t } = useI18n();

// 定义props，接收父组件传递的darkMode
const props = defineProps({
  darkMode: {
    type: Boolean,
    required: true,
  },
});

// WebDAV设置
const webdavSettings = ref({
  webdav_upload_mode: "direct", // 默认直接上传模式 - 可选值: multipart, direct
});

// WebDAV上传模式选项
const webdavUploadModes = ref([
  { value: "direct", label: t("admin.webdav.uploadSettings.modes.direct") },
  { value: "multipart", label: t("admin.webdav.uploadSettings.modes.multipart") },
]);

// WebDAV设置更新状态
const webdavSettingsStatus = ref({
  loading: false,
  success: false,
  error: "",
});

// WebDAV服务地址
const webdavUrl = ref("");

// 获取后端域名并构建WebDAV地址
const getWebdavUrl = () => {
  // 使用项目配置的API基础URL
  return `${API_BASE_URL}/dav`;
};

// 获取WebDAV设置
onMounted(async () => {
  // 设置WebDAV地址
  webdavUrl.value = getWebdavUrl();

  try {
    // 使用新的分组API获取WebDAV设置（分组ID = 3）
    const response = await api.system.getSettingsByGroup(3, true);
    if (response && response.success && response.data) {
      // 处理响应数据
      response.data.forEach((setting) => {
        if (setting.key === "webdav_upload_mode") {
          webdavSettings.value.webdav_upload_mode = setting.value;
        }
      });
    } else {
      throw new Error(response?.message || "获取设置失败");
    }
  } catch (error) {
    console.error("获取WebDAV设置失败:", error);
  }
});

// 更新WebDAV设置
const handleUpdateWebdavSettings = async (event) => {
  event.preventDefault();

  webdavSettingsStatus.value = {
    loading: true,
    success: false,
    error: "",
  };

  try {
    // 使用新的分组更新API（WebDAV设置组，分组ID = 3）
    const response = await api.system.updateGroupSettings(
      3,
      {
        webdav_upload_mode: webdavSettings.value.webdav_upload_mode,
      },
      true
    );

    if (response && response.success) {
      // 更新成功
      webdavSettingsStatus.value.success = true;

      // 3秒后清除成功消息
      setTimeout(() => {
        webdavSettingsStatus.value.success = false;
      }, 3000);
    } else {
      throw new Error(response?.message || "更新失败");
    }
  } catch (error) {
    webdavSettingsStatus.value.error = error.message || t("admin.webdav.messages.updateFailed");
  } finally {
    webdavSettingsStatus.value.loading = false;
  }
};
</script>

<template>
  <div class="flex-1 flex flex-col overflow-y-auto">
    <!-- 页面标题和说明 -->
    <div class="mb-6">
      <h1 class="text-2xl font-bold mb-2" :class="darkMode ? 'text-white' : 'text-gray-800'">{{ t("admin.webdav.title") }}</h1>
      <p class="text-base" :class="darkMode ? 'text-gray-300' : 'text-gray-600'">{{ t("admin.webdav.description") }}</p>
    </div>

    <!-- 状态消息 -->
    <div v-if="webdavSettingsStatus.success || webdavSettingsStatus.error" class="mb-6">
      <div
        v-if="webdavSettingsStatus.success"
        class="rounded-lg p-4 border"
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
          <p class="text-sm font-medium">WebDAV设置更新成功</p>
        </div>
      </div>

      <div
        v-if="webdavSettingsStatus.error"
        class="rounded-lg p-4 border"
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
          <p class="text-sm font-medium">{{ webdavSettingsStatus.error }}</p>
        </div>
      </div>
    </div>

    <!-- 设置表单 -->
    <div class="space-y-6">
      <!-- WebDAV上传设置组 -->
      <div class="setting-group bg-white dark:bg-gray-800 rounded-lg p-6 border border-gray-200 dark:border-gray-700 max-w-2xl">
        <h2 class="text-lg font-medium mb-4 pb-2 border-b border-gray-200 dark:border-gray-700">{{ t("admin.webdav.uploadSettings.title") }}</h2>
        <div class="space-y-4">
          <p class="text-sm" :class="darkMode ? 'text-gray-400' : 'text-gray-500'">{{ t("admin.webdav.uploadSettings.description") }}</p>

          <form @submit="handleUpdateWebdavSettings" class="space-y-6">
            <div class="setting-item grid grid-cols-1 md:grid-cols-3 gap-4 items-start">
              <div class="md:col-span-1">
                <label for="webdavUploadMode" class="block text-sm font-medium" :class="darkMode ? 'text-gray-200' : 'text-gray-700'">{{
                  t("admin.webdav.uploadSettings.uploadModeLabel")
                }}</label>
              </div>
              <div class="md:col-span-2">
                <select
                  id="webdavUploadMode"
                  v-model="webdavSettings.webdav_upload_mode"
                  class="block w-full rounded border shadow-sm px-3 py-2 text-sm"
                  :class="
                    darkMode
                      ? 'bg-gray-700 border-gray-600 text-white focus:ring-blue-500 focus:border-blue-500'
                      : 'bg-white border-gray-300 text-gray-900 focus:ring-blue-500 focus:border-blue-500'
                  "
                >
                  <option v-for="mode in webdavUploadModes" :key="mode.value" :value="mode.value">
                    {{ mode.label }}
                  </option>
                </select>
                <p class="mt-2 text-xs" :class="darkMode ? 'text-gray-400' : 'text-gray-500'">{{ t("admin.webdav.uploadSettings.uploadModeHint") }}</p>
              </div>
            </div>

            <div class="flex justify-end pt-4">
              <button
                type="submit"
                :disabled="webdavSettingsStatus.loading"
                class="inline-flex items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md shadow-sm text-white bg-blue-600 hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500 disabled:opacity-50 disabled:cursor-not-allowed"
              >
                <span v-if="webdavSettingsStatus.loading" class="flex items-center">
                  <svg class="animate-spin -ml-0.5 mr-2 h-4 w-4" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                    <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
                    <path
                      class="opacity-75"
                      fill="currentColor"
                      d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"
                    ></path>
                  </svg>
                  {{ t("admin.webdav.buttons.updating") }}
                </span>
                <span v-else>{{ t("admin.webdav.buttons.updateSettings") }}</span>
              </button>
            </div>
          </form>
        </div>
      </div>

      <!-- WebDAV协议信息组 -->
      <div class="setting-group bg-white dark:bg-gray-800 rounded-lg p-6 border border-gray-200 dark:border-gray-700 max-w-2xl">
        <h2 class="text-lg font-medium mb-4 pb-2 border-b border-gray-200 dark:border-gray-700">{{ t("admin.webdav.protocolInfo.title") }}</h2>
        <div class="space-y-4">
          <p class="text-sm" :class="darkMode ? 'text-gray-400' : 'text-gray-500'">{{ t("admin.webdav.protocolInfo.description") }}</p>
          <div class="info-item grid grid-cols-1 md:grid-cols-3 gap-4 items-start">
            <div class="md:col-span-1">
              <label class="block text-sm font-medium" :class="darkMode ? 'text-gray-200' : 'text-gray-700'">{{ t("admin.webdav.protocolInfo.webdavUrlLabel") }}</label>
            </div>
            <div class="md:col-span-2">
              <div class="p-3 rounded-md border" :class="darkMode ? 'bg-gray-700/50 border-gray-600' : 'bg-gray-50 border-gray-200'">
                <code class="text-sm" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">{{ webdavUrl }}</code>
              </div>
              <p class="mt-2 text-xs" :class="darkMode ? 'text-gray-400' : 'text-gray-500'">{{ t("admin.webdav.protocolInfo.webdavUrlHint") }}</p>
            </div>
          </div>

          <div class="info-item grid grid-cols-1 md:grid-cols-3 gap-4 items-start">
            <div class="md:col-span-1">
              <label class="block text-sm font-medium" :class="darkMode ? 'text-gray-200' : 'text-gray-700'">{{ t("admin.webdav.protocolInfo.authMethodLabel") }}</label>
            </div>
            <div class="md:col-span-2">
              <div class="p-3 rounded-md border space-y-3" :class="darkMode ? 'bg-gray-700/50 border-gray-600' : 'bg-gray-50 border-gray-200'">
                <div>
                  <h4 class="text-sm font-medium mb-1" :class="darkMode ? 'text-gray-200' : 'text-gray-800'">{{ t("admin.webdav.protocolInfo.adminAuth") }}</h4>
                </div>
                <div>
                  <h4 class="text-sm font-medium mb-1" :class="darkMode ? 'text-gray-200' : 'text-gray-800'">{{ t("admin.webdav.protocolInfo.apiKeyAuth") }}</h4>
                </div>
              </div>
              <p class="mt-2 text-xs" :class="darkMode ? 'text-gray-400' : 'text-gray-500'">{{ t("admin.webdav.protocolInfo.authHint") }}</p>
            </div>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<style scoped>
.setting-group {
  background: transparent;
}

.setting-item,
.info-item {
  padding: 1rem 0;
}

.setting-item:not(:last-child),
.info-item:not(:last-child) {
  border-bottom: 1px solid;
  border-color: inherit;
}

/* 移动端适配 */
@media (max-width: 768px) {
  .setting-item,
  .info-item {
    grid-template-columns: 1fr;
  }
}
</style>
