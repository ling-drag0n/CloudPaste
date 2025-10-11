<template>
  <div class="flex-1 flex flex-col overflow-y-auto">
    <!-- 页面标题和说明 -->
    <div class="mb-6">
      <h1 class="text-2xl font-bold mb-2" :class="darkMode ? 'text-white' : 'text-gray-800'">{{ t("admin.preview.title") }}</h1>
      <p class="text-base" :class="darkMode ? 'text-gray-300' : 'text-gray-600'">{{ t("admin.preview.description") }}</p>
    </div>

    <!-- 设置分组 -->
    <div class="space-y-6">
      <!-- 预览设置组 -->
      <div class="setting-group bg-white dark:bg-gray-800 rounded-lg p-6 border border-gray-200 dark:border-gray-700 max-w-2xl">
        <h2 class="text-lg font-medium mb-4 pb-2 border-b border-gray-200 dark:border-gray-700">{{ t("admin.preview.title") }}</h2>
        <div class="space-y-4">
          <!-- 状态消息 -->
          <div
            v-if="status.success"
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
              {{ t("admin.preview.saveSuccess") }}
            </div>
          </div>

          <div
            v-if="status.error"
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
              {{ status.error }}
            </div>
          </div>

          <form @submit="handleSaveSettings" class="space-y-6">
            <!-- 文本文件类型设置 -->
            <div class="setting-item">
              <label class="block text-sm font-medium mb-2" :class="darkMode ? 'text-gray-200' : 'text-gray-700'">
                {{ t("admin.preview.textTypesLabel") }}
              </label>
              <textarea
                v-model="settings.preview_text_types"
                :placeholder="t('admin.preview.textTypesPlaceholder')"
                rows="3"
                class="block w-full rounded border shadow-sm px-3 py-2 text-sm"
                :class="
                  darkMode
                    ? 'bg-gray-700 border-gray-600 text-white focus:ring-blue-500 focus:border-blue-500 placeholder-gray-400'
                    : 'bg-white border-gray-300 text-gray-900 focus:ring-blue-500 focus:border-blue-500 placeholder-gray-500'
                "
              ></textarea>
              <p class="mt-2 text-xs" :class="darkMode ? 'text-gray-400' : 'text-gray-500'">
                {{ t("admin.preview.textTypesHelp") }}
              </p>
            </div>

            <!-- 图片文件类型设置 -->
            <div class="setting-item">
              <label class="block text-sm font-medium mb-2" :class="darkMode ? 'text-gray-200' : 'text-gray-700'">
                {{ t("admin.preview.imageTypesLabel") }}
              </label>
              <textarea
                v-model="settings.preview_image_types"
                :placeholder="t('admin.preview.imageTypesPlaceholder')"
                rows="3"
                class="block w-full rounded border shadow-sm px-3 py-2 text-sm"
                :class="
                  darkMode
                    ? 'bg-gray-700 border-gray-600 text-white focus:ring-blue-500 focus:border-blue-500 placeholder-gray-400'
                    : 'bg-white border-gray-300 text-gray-900 focus:ring-blue-500 focus:border-blue-500 placeholder-gray-500'
                "
              ></textarea>
              <p class="mt-2 text-xs" :class="darkMode ? 'text-gray-400' : 'text-gray-500'">
                {{ t("admin.preview.imageTypesHelp") }}
              </p>
            </div>

            <!-- 视频文件类型设置 -->
            <div class="setting-item">
              <label class="block text-sm font-medium mb-2" :class="darkMode ? 'text-gray-200' : 'text-gray-700'">
                {{ t("admin.preview.videoTypesLabel") }}
              </label>
              <textarea
                v-model="settings.preview_video_types"
                :placeholder="t('admin.preview.videoTypesPlaceholder')"
                rows="3"
                class="block w-full rounded border shadow-sm px-3 py-2 text-sm"
                :class="
                  darkMode
                    ? 'bg-gray-700 border-gray-600 text-white focus:ring-blue-500 focus:border-blue-500 placeholder-gray-400'
                    : 'bg-white border-gray-300 text-gray-900 focus:ring-blue-500 focus:border-blue-500 placeholder-gray-500'
                "
              ></textarea>
              <p class="mt-2 text-xs" :class="darkMode ? 'text-gray-400' : 'text-gray-500'">
                {{ t("admin.preview.videoTypesHelp") }}
              </p>
            </div>

            <!-- 音频文件类型设置 -->
            <div class="setting-item">
              <label class="block text-sm font-medium mb-2" :class="darkMode ? 'text-gray-200' : 'text-gray-700'">
                {{ t("admin.preview.audioTypesLabel") }}
              </label>
              <textarea
                v-model="settings.preview_audio_types"
                :placeholder="t('admin.preview.audioTypesPlaceholder')"
                rows="3"
                class="block w-full rounded border shadow-sm px-3 py-2 text-sm"
                :class="
                  darkMode
                    ? 'bg-gray-700 border-gray-600 text-white focus:ring-blue-500 focus:border-blue-500 placeholder-gray-400'
                    : 'bg-white border-gray-300 text-gray-900 focus:ring-blue-500 focus:border-blue-500 placeholder-gray-500'
                "
              ></textarea>
              <p class="mt-2 text-xs" :class="darkMode ? 'text-gray-400' : 'text-gray-500'">
                {{ t("admin.preview.audioTypesHelp") }}
              </p>
            </div>

            <!-- Office文件类型设置 -->
            <div class="setting-item">
              <label class="block text-sm font-medium mb-2" :class="darkMode ? 'text-gray-200' : 'text-gray-700'">
                {{ t("admin.preview.officeTypesLabel") }}
              </label>
              <textarea
                v-model="settings.preview_office_types"
                :placeholder="t('admin.preview.officeTypesPlaceholder')"
                rows="3"
                class="block w-full rounded border shadow-sm px-3 py-2 text-sm"
                :class="
                  darkMode
                    ? 'bg-gray-700 border-gray-600 text-white focus:ring-blue-500 focus:border-blue-500 placeholder-gray-400'
                    : 'bg-white border-gray-300 text-gray-900 focus:ring-blue-500 focus:border-blue-500 placeholder-gray-500'
                "
              ></textarea>
              <p class="mt-2 text-xs" :class="darkMode ? 'text-gray-400' : 'text-gray-500'">
                {{ t("admin.preview.officeTypesHelp") }}
              </p>
            </div>

            <!-- 文档文件类型设置 -->
            <div class="setting-item">
              <label class="block text-sm font-medium mb-2" :class="darkMode ? 'text-gray-200' : 'text-gray-700'">
                {{ t("admin.preview.documentTypesLabel") }}
              </label>
              <textarea
                v-model="settings.preview_document_types"
                :placeholder="t('admin.preview.documentTypesPlaceholder')"
                rows="3"
                class="block w-full rounded border shadow-sm px-3 py-2 text-sm"
                :class="
                  darkMode
                    ? 'bg-gray-700 border-gray-600 text-white focus:ring-blue-500 focus:border-blue-500 placeholder-gray-400'
                    : 'bg-white border-gray-300 text-gray-900 focus:ring-blue-500 focus:border-blue-500 placeholder-gray-500'
                "
              ></textarea>
              <p class="mt-2 text-xs" :class="darkMode ? 'text-gray-400' : 'text-gray-500'">
                {{ t("admin.preview.documentTypesHelp") }}
              </p>
            </div>

            <!-- 操作按钮 -->
            <div class="flex justify-between items-center pt-6">
              <button
                type="button"
                @click="handleResetToDefaults"
                :disabled="status.loading"
                class="inline-flex items-center px-4 py-2 border rounded-md focus:outline-none focus:ring-2 focus:ring-primary-500/20 transition-colors"
                :class="[
                  status.loading ? 'opacity-50 cursor-not-allowed' : '',
                  darkMode
                    ? 'border-gray-600 text-gray-300 bg-gray-800 hover:bg-gray-700 hover:border-gray-500'
                    : 'border-gray-300 text-gray-700 bg-white hover:bg-gray-50 hover:border-gray-400',
                ]"
              >
                <svg class="w-4 h-4 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg">
                  <path
                    stroke-linecap="round"
                    stroke-linejoin="round"
                    stroke-width="2"
                    d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"
                  />
                </svg>
                {{ t("admin.preview.resetDefaults") }}
              </button>

              <button
                type="submit"
                :disabled="status.loading"
                class="px-4 py-2 bg-primary-600 text-white rounded-md hover:bg-primary-700 focus:outline-none focus:ring-2 focus:ring-primary-500/20 transition-colors"
                :class="status.loading ? 'opacity-50 cursor-not-allowed' : ''"
              >
                <svg v-if="status.loading" class="animate-spin -ml-1 mr-2 h-4 w-4 text-white inline" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                  <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
                  <path
                    class="opacity-75"
                    fill="currentColor"
                    d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"
                  ></path>
                </svg>
                {{ status.loading ? t("admin.global.buttons.updating") : t("admin.global.buttons.updateSettings") }}
              </button>
            </div>
          </form>
        </div>
      </div>
    </div>
  </div>
</template>

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

// 状态管理
const status = ref({
  loading: false,
  success: false,
  error: "",
});

// 预览设置数据
const settings = ref({
  preview_text_types: "",
  preview_image_types: "",
  preview_video_types: "",
  preview_audio_types: "",
  preview_office_types: "",
  preview_document_types: "",
});

// 默认设置
const defaultSettings = {
  preview_text_types:
    "txt,htm,html,xml,java,properties,sql,js,md,json,conf,ini,vue,php,py,bat,yml,yaml,go,sh,c,cpp,h,hpp,tsx,vtt,srt,ass,rs,lrc,dockerfile,makefile,gitignore,license,readme",
  preview_image_types: "jpg,tiff,jpeg,png,gif,bmp,svg,ico,swf,webp,avif",
  preview_video_types: "mp4,mkv,avi,mov,rmvb,webm,flv,m3u8,ts,m2ts",
  preview_audio_types: "mp3,flac,ogg,m4a,wav,opus,wma",
  preview_office_types: "doc,docx,xls,xlsx,ppt,pptx,rtf",
  preview_document_types: "pdf",
};

// 加载设置
const loadSettings = async () => {
  try {
    status.value.loading = true;
    status.value.error = "";

    // 使用分组API获取预览设置（分组ID = 2）
    const response = await api.system.getSettingsByGroup(2, true);

    if (response && response.success && response.data) {
      // 将设置数据映射到本地状态
      response.data.forEach((setting) => {
        if (settings.value.hasOwnProperty(setting.key)) {
          settings.value[setting.key] = setting.value || "";
        }
      });
    } else {
      throw new Error(response?.message || "获取设置失败");
    }
  } catch (err) {
    console.error("加载预览设置失败:", err);
    status.value.error = err.message || "加载设置失败";
  } finally {
    status.value.loading = false;
  }
};

// 保存设置
const handleSaveSettings = async (event) => {
  event.preventDefault();

  status.value = {
    loading: true,
    success: false,
    error: "",
  };

  try {
    // 预览设置组，分组ID = 2
    const response = await api.system.updateGroupSettings(
      2,
      {
        preview_text_types: settings.value.preview_text_types,
        preview_image_types: settings.value.preview_image_types,
        preview_video_types: settings.value.preview_video_types,
        preview_audio_types: settings.value.preview_audio_types,
        preview_office_types: settings.value.preview_office_types,
      },
      true
    );

    if (response && response.success) {
      status.value.success = true;

      // 3秒后清除成功消息
      setTimeout(() => {
        status.value.success = false;
      }, 3000);
    } else {
      throw new Error(response?.message || "保存设置失败");
    }
  } catch (err) {
    console.error("保存预览设置失败:", err);
    status.value.error = err.message || "保存设置失败";
  } finally {
    status.value.loading = false;
  }
};

// 重置为默认设置
const handleResetToDefaults = () => {
  if (confirm(t("admin.preview.resetConfirm"))) {
    Object.assign(settings.value, defaultSettings);
  }
};

// 组件挂载时加载设置
onMounted(() => {
  loadSettings();
});
</script>
