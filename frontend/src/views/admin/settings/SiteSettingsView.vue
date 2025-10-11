<script setup>
import { ref, onMounted } from "vue";
import { api } from "@/api";
import { useI18n } from "vue-i18n";
import VditorUnified from "@/components/common/VditorUnified.vue";

// 使用i18n
const { t } = useI18n();

// 定义props
const props = defineProps({
  darkMode: {
    type: Boolean,
    required: true,
  },
});

// 站点设置
const siteSettings = ref({
  site_title: "CloudPaste",
  site_favicon_url: "",
  site_footer_markdown: "© 2025 CloudPaste. 保留所有权利。",
  site_announcement_enabled: false,
  site_announcement_content: "",
  site_custom_head: "",
  site_custom_body: "",
});

// 设置更新状态
const settingsStatus = ref({
  loading: false,
  success: false,
  error: "",
});

// 获取设置数据（使用新的分组API）
onMounted(async () => {
  try {
    // 使用新的分组API获取站点设置（分组ID = 4）
    const response = await api.system.getSettingsByGroup(4, true);
    if (response && response.success && response.data) {
      response.data.forEach((setting) => {
        if (setting.key === "site_title") {
          siteSettings.value.site_title = setting.value || "CloudPaste";
        } else if (setting.key === "site_favicon_url") {
          siteSettings.value.site_favicon_url = setting.value || "";
        } else if (setting.key === "site_footer_markdown") {
          // 直接使用API返回的值，包括空字符串（用户主动清空的状态）
          siteSettings.value.site_footer_markdown = setting.value;
        } else if (setting.key === "site_announcement_enabled") {
          siteSettings.value.site_announcement_enabled = setting.value === "true";
        } else if (setting.key === "site_announcement_content") {
          siteSettings.value.site_announcement_content = setting.value || "";
        } else if (setting.key === "site_custom_head") {
          siteSettings.value.site_custom_head = setting.value || "";
        } else if (setting.key === "site_custom_body") {
          siteSettings.value.site_custom_body = setting.value || "";
        }
      });
    } else {
      throw new Error(response?.message || "获取设置失败");
    }
  } catch (error) {
    console.error("获取站点设置失败:", error);
    settingsStatus.value.error = "获取站点设置失败: " + error.message;
  }
});

// 更新站点设置
const handleUpdateSiteSettings = async (event) => {
  event.preventDefault();

  settingsStatus.value.loading = true;
  settingsStatus.value.success = false;
  settingsStatus.value.error = "";

  try {
    // 准备更新数据
    const updateData = {
      site_title: siteSettings.value.site_title || "CloudPaste",
      site_favicon_url: siteSettings.value.site_favicon_url || "",
      site_footer_markdown: siteSettings.value.site_footer_markdown || "",
      site_announcement_enabled: siteSettings.value.site_announcement_enabled.toString(),
      site_announcement_content: siteSettings.value.site_announcement_content,
      site_custom_head: siteSettings.value.site_custom_head || "",
      site_custom_body: siteSettings.value.site_custom_body || "",
    };

    // 使用分组批量更新API
    const response = await api.system.updateGroupSettings(4, updateData);

    if (response && response.success) {
      settingsStatus.value.success = true;

      // 更新站点配置Store缓存
      try {
        const { useSiteConfigStore } = await import("@/stores/siteConfigStore.js");
        const siteConfigStore = useSiteConfigStore();
        siteConfigStore.updateSiteTitle(siteSettings.value.site_title);
        siteConfigStore.updateSiteFavicon(siteSettings.value.site_favicon_url);
        siteConfigStore.updateSiteFooter(siteSettings.value.site_footer_markdown);
        siteConfigStore.updateCustomHead(siteSettings.value.site_custom_head);
        siteConfigStore.updateCustomBody(siteSettings.value.site_custom_body);
      } catch (storeError) {
        console.warn("更新站点配置Store失败:", storeError);
      }

      setTimeout(() => {
        settingsStatus.value.success = false;
      }, 3000);
    } else {
      throw new Error(response?.message || "更新设置失败");
    }
  } catch (error) {
    console.error("更新站点设置失败:", error);
    settingsStatus.value.error = error.message || "更新站点设置失败";
  } finally {
    settingsStatus.value.loading = false;
  }
};

// 重置设置
const resetSettings = () => {
  if (confirm(t("admin.site.messages.confirmReset"))) {
    // 重置所有站点设置到默认值
    siteSettings.value.site_title = "CloudPaste";
    siteSettings.value.site_favicon_url = "";
    siteSettings.value.site_footer_markdown = "© 2025 CloudPaste. 保留所有权利。";
    siteSettings.value.site_announcement_enabled = false;
    siteSettings.value.site_announcement_content = "";

    console.log("站点设置已重置为默认值");
  }
};

// 清空公告内容
const handleClearAnnouncementContent = () => {
  siteSettings.value.site_announcement_content = "";
  console.log("公告内容已清空");
};
</script>

<template>
  <div class="flex-1 flex flex-col overflow-y-auto">
    <!-- 页面标题 -->
    <div class="mb-6">
      <h1 class="text-2xl font-bold mb-2" :class="darkMode ? 'text-white' : 'text-gray-800'">{{ t("admin.site.title") }}</h1>
      <p class="text-base" :class="darkMode ? 'text-gray-300' : 'text-gray-600'">{{ t("admin.site.description") }}</p>
    </div>

    <!-- 状态消息 -->
    <div v-if="settingsStatus.success || settingsStatus.error" class="mb-6">
      <div
        v-if="settingsStatus.success"
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
          <p class="text-sm font-medium">{{ t("admin.site.messages.updateSuccess") }}</p>
        </div>
      </div>

      <div v-if="settingsStatus.error" class="rounded-lg p-4 border" :class="darkMode ? 'bg-red-900/20 border-red-800/40 text-red-200' : 'bg-red-50 border-red-200 text-red-800'">
        <div class="flex items-center">
          <svg class="h-5 w-5 mr-2 flex-shrink-0" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20" fill="currentColor">
            <path
              fill-rule="evenodd"
              d="M10 18a8 8 0 100-16 8 8 0 000 16zM8.707 7.293a1 1 0 00-1.414 1.414L8.586 10l-1.293 1.293a1 1 0 101.414 1.414L10 11.414l1.293 1.293a1 1 0 001.414-1.414L11.414 10l1.293-1.293a1 1 0 00-1.414-1.414L10 8.586 8.707 7.293z"
              clip-rule="evenodd"
            />
          </svg>
          <p class="text-sm font-medium">{{ settingsStatus.error }}</p>
        </div>
      </div>
    </div>

    <!-- 设置分组 -->
    <div class="space-y-6">
      <!-- 站点设置组 -->
      <div class="setting-group bg-white dark:bg-gray-800 rounded-lg p-6 border border-gray-200 dark:border-gray-700 max-w-2xl">
        <h2 class="text-lg font-medium mb-4 pb-2 border-b border-gray-200 dark:border-gray-700" :class="darkMode ? 'text-white' : 'text-gray-800'">
          {{ t("admin.site.title") }}
        </h2>

        <!-- 站点设置表单 -->
        <form @submit="handleUpdateSiteSettings" class="space-y-6">
          <!-- 站点标题设置 -->
          <div class="setting-item">
            <label for="siteTitle" class="block text-sm font-medium mb-2" :class="darkMode ? 'text-gray-200' : 'text-gray-700'">
              {{ t("admin.site.siteTitle.label") }}
            </label>
            <input
              type="text"
              id="siteTitle"
              v-model="siteSettings.site_title"
              :placeholder="t('admin.site.siteTitle.placeholder')"
              class="w-full px-3 py-2 border rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 transition-colors"
              :class="darkMode ? 'bg-gray-700 border-gray-600 text-gray-100 placeholder-gray-400' : 'bg-white border-gray-300 text-gray-900 placeholder-gray-500'"
              maxlength="100"
              required
            />
            <p class="text-xs mt-1" :class="darkMode ? 'text-gray-400' : 'text-gray-500'">
              {{ t("admin.site.siteTitle.hint") }}
            </p>
          </div>

          <!-- 站点图标设置 -->
          <div class="setting-item">
            <label for="siteFavicon" class="block text-sm font-medium mb-2" :class="darkMode ? 'text-gray-200' : 'text-gray-700'">
              {{ t("admin.site.favicon.label") }}
            </label>
            <div class="flex items-center space-x-3">
              <!-- 当前图标预览 -->
              <div class="flex-shrink-0">
                <div class="w-8 h-8 border rounded flex items-center justify-center" :class="darkMode ? 'border-gray-600 bg-gray-700' : 'border-gray-300 bg-gray-50'">
                  <img
                    v-if="siteSettings.site_favicon_url"
                    :src="siteSettings.site_favicon_url"
                    alt="站点图标预览"
                    class="w-6 h-6 object-contain"
                    @error="$event.target.style.display = 'none'"
                  />
                  <svg v-else class="w-4 h-4" :class="darkMode ? 'text-gray-400' : 'text-gray-500'" fill="currentColor" viewBox="0 0 20 20">
                    <path fill-rule="evenodd" d="M4 3a2 2 0 00-2 2v10a2 2 0 002 2h12a2 2 0 002-2V5a2 2 0 00-2-2H4zm12 12H4l4-8 3 6 2-4 3 6z" clip-rule="evenodd" />
                  </svg>
                </div>
              </div>
              <!-- URL输入框 -->
              <div class="flex-1">
                <input
                  type="url"
                  id="siteFavicon"
                  v-model="siteSettings.site_favicon_url"
                  :placeholder="t('admin.site.favicon.placeholder')"
                  class="w-full px-3 py-2 border rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 transition-colors"
                  :class="darkMode ? 'bg-gray-700 border-gray-600 text-gray-100 placeholder-gray-400' : 'bg-white border-gray-300 text-gray-900 placeholder-gray-500'"
                />
              </div>
            </div>
            <p class="text-xs mt-1" :class="darkMode ? 'text-gray-400' : 'text-gray-500'">
              {{ t("admin.site.favicon.hint") }}
            </p>
          </div>

          <!-- 页脚内容设置 -->
          <div class="setting-item">
            <label for="siteFooter" class="block text-sm font-medium mb-2" :class="darkMode ? 'text-gray-200' : 'text-gray-700'">
              {{ t("admin.site.footer.label") }}
            </label>
            <textarea
              id="siteFooter"
              v-model="siteSettings.site_footer_markdown"
              :placeholder="t('admin.site.footer.placeholder')"
              rows="3"
              class="w-full px-3 py-2 border rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 transition-colors resize-vertical"
              :class="darkMode ? 'bg-gray-700 border-gray-600 text-gray-100 placeholder-gray-400' : 'bg-white border-gray-300 text-gray-900 placeholder-gray-500'"
            />
            <p class="text-xs mt-1" :class="darkMode ? 'text-gray-400' : 'text-gray-500'">
              {{ t("admin.site.footer.hint") }}
            </p>
          </div>

          <!-- 公告栏开关 -->
          <div class="setting-item flex items-start justify-between">
            <div class="flex-1">
              <label class="block text-sm font-medium mb-1" :class="darkMode ? 'text-gray-200' : 'text-gray-700'">
                {{ t("admin.site.announcement.enableLabel") }}
              </label>
              <p class="text-xs" :class="darkMode ? 'text-gray-400' : 'text-gray-500'">
                {{ t("admin.site.announcement.enableHint") }}
              </p>
            </div>
            <div class="flex-shrink-0 ml-4">
              <label class="relative inline-flex items-center cursor-pointer">
                <input type="checkbox" id="announcementEnabled" v-model="siteSettings.site_announcement_enabled" class="sr-only peer" />
                <div
                  class="w-11 h-6 bg-gray-200 peer-focus:outline-none peer-focus:ring-4 peer-focus:ring-blue-300 dark:peer-focus:ring-blue-800 rounded-full peer dark:bg-gray-700 peer-checked:after:translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:left-[2px] after:bg-white after:border-gray-300 after:border after:rounded-full after:h-5 after:w-5 after:transition-all dark:border-gray-600 peer-checked:bg-blue-600"
                ></div>
              </label>
            </div>
          </div>

          <!-- 公告内容编辑器 -->
          <div class="setting-item">
            <label class="block text-sm font-medium mb-2" :class="darkMode ? 'text-gray-200' : 'text-gray-700'">
              {{ t("admin.site.announcement.contentLabel") }}
            </label>
            <p class="text-xs mb-3" :class="darkMode ? 'text-gray-400' : 'text-gray-500'">
              {{ t("admin.site.announcement.contentHint") }}
            </p>

            <VditorUnified
              v-model="siteSettings.site_announcement_content"
              :dark-mode="darkMode"
              :mini-mode="true"
              :placeholder="t('admin.site.announcement.contentPlaceholder')"
              @clear-content="handleClearAnnouncementContent"
            />
          </div>

          <!-- 自定义头部 -->
          <div>
            <label for="site_custom_head" class="block text-sm font-medium mb-2">
              {{ t("admin.site.customHead") }}
            </label>
            <textarea
              id="site_custom_head"
              v-model="siteSettings.site_custom_head"
              rows="8"
              :placeholder="t('admin.site.customHeadPlaceholder')"
              :class="[
                'w-full px-3 py-2 border rounded-md font-mono text-sm',
                darkMode ? 'bg-gray-700 border-gray-600 text-gray-100 placeholder-gray-400' : 'bg-white border-gray-300 text-gray-900 placeholder-gray-500',
              ]"
            ></textarea>
            <p class="text-xs text-gray-500 mt-1">
              {{ t("admin.site.customHeadHelp") }}
            </p>
          </div>

          <!-- 自定义body -->
          <div>
            <label for="site_custom_body" class="block text-sm font-medium mb-2">
              {{ t("admin.site.customBody") }}
            </label>
            <textarea
              id="site_custom_body"
              v-model="siteSettings.site_custom_body"
              rows="8"
              :placeholder="t('admin.site.customBodyPlaceholder')"
              :class="[
                'w-full px-3 py-2 border rounded-md font-mono text-sm',
                darkMode ? 'bg-gray-700 border-gray-600 text-gray-100 placeholder-gray-400' : 'bg-white border-gray-300 text-gray-900 placeholder-gray-500',
              ]"
            ></textarea>
            <p class="text-xs text-gray-500 mt-1">
              {{ t("admin.site.customBodyHelp") }}
            </p>
          </div>

          <!-- 操作按钮 -->
          <div class="flex justify-between items-center pt-4">
            <button
              type="button"
              @click="resetSettings"
              class="px-4 py-2 text-sm font-medium border rounded-md transition-colors focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500"
              :class="darkMode ? 'text-gray-300 bg-gray-700 border-gray-600 hover:bg-gray-600' : 'text-gray-700 bg-white border-gray-300 hover:bg-gray-50'"
            >
              {{ t("admin.site.buttons.reset") }}
            </button>

            <button
              type="submit"
              :disabled="settingsStatus.loading"
              class="px-4 py-2 text-sm font-medium text-white bg-blue-600 border border-transparent rounded-md hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500 disabled:opacity-50 disabled:cursor-not-allowed transition-colors flex items-center"
            >
              <svg v-if="settingsStatus.loading" class="animate-spin -ml-1 mr-2 h-4 w-4 text-white" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
                <path
                  class="opacity-75"
                  fill="currentColor"
                  d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"
                ></path>
              </svg>
              {{ settingsStatus.loading ? t("admin.site.buttons.updating") : t("admin.site.buttons.updateSettings") }}
            </button>
          </div>
        </form>
      </div>
    </div>
  </div>
</template>

<style scoped>
.setting-group {
  transition: all 0.2s ease;
}

.setting-group:hover {
  box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06);
}

.setting-item {
  padding: 1rem 0;
}

.setting-item:not(:last-child) {
  border-bottom: 1px solid theme("colors.gray.200");
}

.dark .setting-item:not(:last-child) {
  border-bottom-color: theme("colors.gray.700");
}

/* 移动端优化 */
@media (max-width: 768px) {
  .setting-item.flex {
    flex-direction: column;
    align-items: flex-start;
  }

  .setting-item .flex-shrink-0 {
    margin-left: 0;
    margin-top: 0.75rem;
    align-self: flex-end;
  }
}
</style>
