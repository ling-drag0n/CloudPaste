<template>
  <div class="p-3 sm:p-4 md:p-5 lg:p-6 flex-1 flex flex-col overflow-y-auto">
    <!-- 顶部操作栏 -->
    <div class="flex flex-col space-y-3 mb-4">
      <!-- 标题和刷新按钮 -->
      <div class="flex justify-between items-center">
        <h2 class="text-lg font-medium text-gray-900 dark:text-white">{{ t("admin.sidebar.shareManagement") }}</h2>
        <button
          type="button"
          class="inline-flex items-center px-2 py-1 sm:px-3 sm:py-1.5 md:px-4 md:py-2 border border-transparent text-sm font-medium rounded-md shadow-sm text-white bg-primary-600 hover:bg-primary-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500"
          @click.prevent="refreshFiles"
          :disabled="loading || searchLoading"
        >
          <IconRefresh class="h-3 w-3 sm:h-4 sm:w-4 mr-1" :class="loading || searchLoading ? 'animate-spin' : ''" />
          <span>{{ t("admin.shareManagement.refresh") }}</span>
        </button>
      </div>

      <!-- 搜索框 -->
      <div class="w-full">
        <GlobalSearchBox
          v-model="searchQuery"
          :placeholder="t('admin.shareManagement.searchPlaceholder')"
          :show-hint="true"
          :search-hint="t('admin.shareManagement.searchHint')"
          size="md"
          :debounce-ms="300"
          @search="handleGlobalSearch"
          @clear="clearSearch"
        />
      </div>

      <!-- 统计信息和操作按钮 -->
      <div class="flex flex-col sm:flex-row sm:justify-between sm:items-center space-y-2 sm:space-y-0">
        <div class="text-sm text-gray-600 dark:text-gray-400">
          {{ t("admin.shareManagement.total", { count: pagination.total }) }}
          <span v-if="selectedFiles.length > 0" class="ml-2"> ({{ t("admin.shareManagement.selected", { count: selectedFiles.length }) }}) </span>
        </div>
        <div class="flex flex-wrap gap-1 sm:gap-2">
          <!-- 删除模式开关 -->
          <div
            class="inline-flex items-center px-2 py-1 sm:px-3 sm:py-1.5 md:px-4 md:py-2 border border-transparent text-xs sm:text-sm font-medium rounded-md shadow-sm bg-gray-100 dark:bg-gray-700 text-gray-700 dark:text-gray-300 flex-grow sm:flex-grow-0"
          >
            <span class="mr-2">{{ t("admin.shareManagement.deleteRecordOnly") }}</span>
            <button
              type="button"
              @click.prevent="deleteSettingsStore.toggleDeleteMode"
              class="relative inline-flex h-5 w-9 items-center rounded-full transition-colors focus:outline-none focus:ring-2 focus:ring-blue-500 focus:ring-offset-2"
              :class="deleteSettingsStore.deleteRecordOnly ? 'bg-blue-600' : 'bg-gray-300 dark:bg-gray-600'"
            >
              <span
                class="inline-block h-3 w-3 transform rounded-full bg-white transition-transform"
                :class="deleteSettingsStore.deleteRecordOnly ? 'translate-x-5' : 'translate-x-1'"
              ></span>
            </button>
          </div>

          <!-- 批量删除按钮 -->
          <button
            type="button"
            @click.prevent="handleBatchDelete"
            :disabled="selectedFiles.length === 0"
            :class="[
              'inline-flex items-center px-2 py-1 sm:px-3 sm:py-1.5 md:px-4 md:py-2 border border-transparent text-xs sm:text-sm font-medium rounded-md shadow-sm focus:outline-none focus:ring-2 focus:ring-offset-2 flex-grow sm:flex-grow-0',
              selectedFiles.length === 0 ? 'bg-gray-300 text-gray-500 cursor-not-allowed' : 'text-white bg-red-600 hover:bg-red-700 focus:ring-red-500',
            ]"
          >
            <IconDelete class="h-3 w-3 sm:h-4 sm:w-4 mr-1" />
            <span>{{ t("admin.shareManagement.batchDelete") }}{{ selectedFiles.length ? ` (${selectedFiles.length})` : "" }}</span>
          </button>
        </div>
      </div>
    </div>

    <!-- 上次刷新时间显示 -->
    <div class="flex justify-between items-center mb-2 sm:mb-3" v-if="lastRefreshTime">
      <div class="text-xs sm:text-sm text-gray-500 dark:text-gray-400">
        <span class="inline-flex items-center">
          <IconClock class="h-3 w-3 sm:h-4 sm:w-4 mr-1" />
          {{ t("admin.shareManagement.lastRefresh", { time: lastRefreshTime }) }}
        </span>
      </div>
    </div>

    <!-- 加载中指示器 -->
    <div v-if="loading" class="flex justify-center my-8">
      <IconRefresh class="animate-spin h-8 w-8" :class="darkMode ? 'text-blue-400' : 'text-blue-500'" />
    </div>

    <!-- 数据展示区域 -->
    <div v-if="!loading" class="overflow-hidden bg-white dark:bg-gray-800 shadow-md rounded-lg flex-1">
      <div class="flex flex-col h-full">
        <FileTable
          :files="files"
          :dark-mode="darkMode"
          :selected-files="selectedFiles"
          :user-type="props.userType"
          :loading="loading || searchLoading"
          mode="share"
          @toggle-select="toggleSelectItem"
          @toggle-select-all="toggleSelectAll"
          @delete="handleFileDelete"
          @generate-qr="(file) => generateQRCode(file, darkMode)"
          @copy-link="copyFileLink"
          @error="showError"
        />
      </div>
    </div>

    <!-- 分页组件 -->
    <div class="mt-2 mb-4 sm:mt-4 sm:mb-0">
      <CommonPagination
        :dark-mode="darkMode"
        :pagination="pagination"
        :page-size-options="pageSizeOptions"
        :search-mode="isSearchMode"
        :search-term="searchQuery"
        mode="offset"
        @offset-changed="handleOffsetChangeWithSearch"
        @limit-changed="handlePageSizeChange"
      />
    </div>

    <!-- 二维码弹窗 -->
    <QRCodeModal v-if="showQRCodeModal" :qr-code-url="qrCodeDataURL" :file-slug="qrCodeSlug" :dark-mode="darkMode" @close="showQRCodeModal = false" />

    <!-- 确认对话框 -->
    <ConfirmDialog v-bind="dialogState" @confirm="handleConfirm" @cancel="handleCancel" />
  </div>
</template>

<script setup>
import { onMounted } from "vue";
import { useI18n } from "vue-i18n";
import { useFileManagement } from "@/modules/fileshare/admin/useFileManagement.js";
import { useThemeMode } from "@/composables/core/useThemeMode.js";
import { useConfirmDialog, createConfirmFn } from "@/composables/core/useConfirmDialog.js";
import { IconClock, IconDelete, IconRefresh } from "@/components/icons";
import { useDeleteSettingsStore } from "@/stores/deleteSettingsStore.js";

// 导入子组件
import FileTable from "@/modules/fileshare/admin/components/FileTable.vue";
import CommonPagination from "@/components/common/CommonPagination.vue";
import QRCodeModal from "@/modules/fileshare/admin/components/QRCodeModal.vue";
import GlobalSearchBox from "@/components/common/GlobalSearchBox.vue";
import ConfirmDialog from "@/components/common/dialogs/ConfirmDialog.vue";

/**
 * 组件接收的属性定义
 * userType: 用户类型，'admin'或'apikey'
 */
const props = defineProps({
  userType: {
    type: String,
    default: "admin",
    validator: (value) => ["admin", "apikey"].includes(value),
  },
});

/**
 * 使用主题模式 composable
 */
const { isDarkMode: darkMode } = useThemeMode();

// 国际化
const { t } = useI18n();

// 确认对话框
const { dialogState, confirm, handleConfirm, handleCancel } = useConfirmDialog();

// 创建适配确认函数，用于传递给 composable
const confirmFn = createConfirmFn(confirm, {
  t,
  darkMode,
  getConfirmText: () => t("common.dialogs.deleteButton"),
});

// 使用文件管理composable（分享管理与文件管理共用同一数据源）
const {
  loading,
  selectedItems: selectedFiles,
  lastRefreshTime,
  pagination,
  pageSizeOptions,
  files,
  showQRCodeModal,
  qrCodeDataURL,
  qrCodeSlug,
  searchQuery,
  isSearchMode,
  searchLoading,

  loadFiles,
  refreshFiles,
  handleGlobalSearch,
  clearSearch,
  handleOffsetChangeWithSearch,
  handlePageSizeChange,
  handleFileDelete,
  handleBatchDelete,
  generateQRCode,
  copyFileLink,
  showError,
  toggleSelectItem,
  toggleSelectAll,
} = useFileManagement(props.userType, { confirmFn });

// 删除设置store
const deleteSettingsStore = useDeleteSettingsStore();

// 组件挂载时加载分享列表
onMounted(loadFiles);
</script>
