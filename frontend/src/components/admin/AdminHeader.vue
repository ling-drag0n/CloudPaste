<template>
  <!-- 移动端顶部栏 - 混合导航模式下位于全局header下方 -->
  <div class="md:hidden fixed top-16 left-0 right-0 z-40 border-b px-4 py-3 flex items-center" :class="darkMode ? 'bg-gray-800 border-gray-700' : 'bg-white border-gray-200'">
    <button
      type="button"
      @click="$emit('toggle-mobile-sidebar')"
      class="h-10 w-10 inline-flex items-center justify-center rounded-md focus:outline-none"
      :class="darkMode ? 'text-gray-300 hover:text-white' : 'text-gray-500 hover:text-gray-900'"
    >
      <span class="sr-only">打开菜单</span>
      <svg class="h-6 w-6" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor">
        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 6h16M4 12h16M4 18h16" />
      </svg>
    </button>
    <h1 class="ml-3 text-lg font-medium" :class="darkMode ? 'text-white' : 'text-gray-900'">
      {{ currentPageTitle }}
    </h1>
  </div>
</template>

<script setup>
import { computed } from "vue";
import { useRoute } from "vue-router";
import { useI18n } from "vue-i18n";

const route = useRoute();
const { t } = useI18n();

const props = defineProps({
  darkMode: {
    type: Boolean,
    required: true,
  },
});

const emit = defineEmits(["toggle-mobile-sidebar"]);

// 计算当前页面标题
const currentPageTitle = computed(() => {
  // 根据路由名称返回对应的页面标题
  switch (route.name) {
    case "AdminDashboard":
      return t("pageTitle.adminModules.dashboard");
    case "AdminTextManagement":
      return t("pageTitle.adminModules.textManagement");
    case "AdminFileManagement":
      return t("pageTitle.adminModules.fileManagement");
    case "AdminStorageConfig":
      return t("pageTitle.adminModules.storageConfig");
    case "AdminMountManagement":
      return t("pageTitle.adminModules.mountManagement");
    case "AdminKeyManagement":
      return t("pageTitle.adminModules.keyManagement");
    case "AdminGlobalSettings":
      return t("pageTitle.adminModules.globalSettings");
    case "AdminAccountSettings":
      return t("pageTitle.adminModules.accountSettings");
    case "AdminAccountManagement":
      return t("pageTitle.adminModules.accountSettings");
    case "AdminWebDAVSettings":
      return t("pageTitle.adminModules.webdavSettings");
    default:
      return t("nav.admin");
  }
});
</script>

<style scoped>
/* 组件特定样式 */
</style>
