<template>
  <div class="p-3 sm:p-4 md:p-5 lg:p-6 flex-1 flex flex-col overflow-y-auto">
    <!-- 顶部操作栏 -->
    <div class="flex flex-col space-y-3 mb-4">
      <!-- 标题和刷新按钮 -->
      <div class="flex justify-between items-center">
        <h2 class="text-lg font-medium text-gray-900 dark:text-white">文件管理</h2>
        <!-- 刷新按钮 - 在所有屏幕尺寸显示 -->
        <button
          type="button"
          class="inline-flex items-center px-2 py-1 sm:px-3 sm:py-1.5 md:px-4 md:py-2 border border-transparent text-sm font-medium rounded-md shadow-sm text-white bg-primary-600 hover:bg-primary-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500"
          @click.prevent="loadFiles"
          :disabled="loading"
        >
          <svg xmlns="http://www.w3.org/2000/svg" :class="['h-3 w-3 sm:h-4 sm:w-4 mr-1', loading ? 'animate-spin' : '']" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path
              v-if="!loading"
              stroke-linecap="round"
              stroke-linejoin="round"
              stroke-width="2"
              d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"
            />
            <template v-else>
              <circle cx="12" cy="12" r="10"></circle>
              <path d="m16 12-4-4-4 4"></path>
              <path d="M12 16V8"></path>
            </template>
          </svg>
          <span class="hidden xs:inline">刷新</span>
          <span class="xs:hidden">刷新</span>
        </button>
      </div>

      <!-- 搜索框 -->
      <div class="w-full">
        <GlobalSearchBox
          v-model="globalSearchValue"
          placeholder="搜索文件（支持文件名、链接、备注）"
          :show-hint="true"
          search-hint="服务端搜索，支持模糊匹配"
          size="md"
          :debounce-ms="300"
          @search="handleGlobalSearch"
          @clear="clearGlobalSearch"
        />
      </div>

      <!-- 统计信息和操作按钮 -->
      <div class="flex flex-col sm:flex-row sm:justify-between sm:items-center space-y-2 sm:space-y-0">
        <div class="text-sm text-gray-600 dark:text-gray-400">
          共 {{ pagination.total }} 个文件
          <span v-if="selectedFiles.length > 0" class="ml-2"> (已选择 {{ selectedFiles.length }} 个) </span>
        </div>
        <div class="flex flex-wrap gap-1 sm:gap-2">
          <!-- 删除模式开关 -->
          <div
            class="inline-flex items-center px-2 py-1 sm:px-3 sm:py-1.5 md:px-4 md:py-2 border border-transparent text-xs sm:text-sm font-medium rounded-md shadow-sm bg-gray-100 dark:bg-gray-700 text-gray-700 dark:text-gray-300 flex-grow sm:flex-grow-0"
          >
            <span class="mr-2">仅删除记录</span>
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
            <svg xmlns="http://www.w3.org/2000/svg" class="h-3 w-3 sm:h-4 sm:w-4 mr-1" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path
                stroke-linecap="round"
                stroke-linejoin="round"
                stroke-width="2"
                d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16"
              />
            </svg>
            <span class="hidden xs:inline">批量删除{{ selectedFiles.length ? ` (${selectedFiles.length})` : "" }}</span>
            <span class="xs:hidden">删除{{ selectedFiles.length ? ` (${selectedFiles.length})` : "" }}</span>
          </button>
        </div>
      </div>
    </div>

    <!-- 错误和成功消息提示 -->
    <div v-if="error" class="mb-4 p-3 bg-red-100 text-red-600 rounded">
      <p>{{ error }}</p>
    </div>
    <div v-if="successMessage" class="mb-4 p-3 bg-green-100 text-green-600 rounded">
      <p>{{ successMessage }}</p>
    </div>

    <!-- 上次刷新时间显示 -->
    <div class="flex justify-between items-center mb-2 sm:mb-3" v-if="lastRefreshTime">
      <div class="text-xs sm:text-sm text-gray-500 dark:text-gray-400">
        <span class="inline-flex items-center">
          <svg xmlns="http://www.w3.org/2000/svg" class="h-3 w-3 sm:h-4 sm:w-4 mr-1" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z" />
          </svg>
          上次刷新: {{ lastRefreshTime }}
        </span>
      </div>
    </div>

    <!-- 加载中指示器 -->
    <div v-if="loading" class="flex justify-center my-8">
      <svg class="animate-spin h-8 w-8" :class="darkMode ? 'text-blue-400' : 'text-blue-500'" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
        <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
        <path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
      </svg>
    </div>

    <!-- 数据展示区域 -->
    <div v-if="!loading" class="overflow-hidden bg-white dark:bg-gray-800 shadow-md rounded-lg flex-1">
      <div class="flex flex-col h-full">
        <FileTable
          :files="files"
          :dark-mode="darkMode"
          :selected-files="selectedFiles"
          :user-type="props.userType"
          :copied-files="copiedFiles"
          :copied-permanent-files="copiedPermanentFiles"
          :loading="loading || searchLoading"
          @toggle-select="toggleSelectItem"
          @toggle-select-all="toggleSelectAll"
          @edit="openEditModal"
          @preview="openPreviewModal"
          @delete="handleFileDelete"
          @generate-qr="(file) => generateQRCode(file, darkMode)"
          @copy-link="copyFileLink"
          @copy-permanent-link="copyPermanentLink"
        />
      </div>
    </div>

    <!-- 分页组件（搜索和正常模式统一显示） -->
    <div class="mt-2 mb-4 sm:mt-4 sm:mb-0">
      <CommonPagination
        :dark-mode="darkMode"
        :pagination="pagination"
        :page-size-options="pageSizeOptions"
        :search-mode="isSearchMode"
        :search-term="globalSearchValue"
        mode="offset"
        @offset-changed="handleOffsetChangeWithSearch"
        @limit-changed="handlePageSizeChange"
      />
    </div>

    <!-- 编辑文件元数据弹窗 -->
    <FileEditModal v-if="showEdit" :file="editingFile" :dark-mode="darkMode" @close="showEdit = false" @save="updateFileMetadata" />

    <!-- 文件预览弹窗 -->
    <FilePreviewModal
      v-if="showPreview"
      :file="previewFile"
      :dark-mode="darkMode"
      @close="showPreview = false"
      @preview-file="previewFileInNewWindow"
      @download-file="downloadFileDirectly"
    />

    <!-- 二维码弹窗 -->
    <QRCodeModal v-if="showQRCodeModal" :qr-code-url="qrCodeDataURL" :file-slug="qrCodeSlug" :dark-mode="darkMode" @close="showQRCodeModal = false" />
  </div>
</template>

<script setup>
import { onMounted, ref } from "vue";
import { useFileManagement } from "@/composables/admin-management";

// 导入子组件
import FileTable from "@/components/admin/FileTable.vue";
import CommonPagination from "@/components/common/CommonPagination.vue";
import FileEditModal from "@/components/file/FileEditModal.vue";
import FilePreviewModal from "@/components/admin/FilePreviewModal.vue";
import QRCodeModal from "@/components/admin/QRCodeModal.vue";
import GlobalSearchBox from "@/components/common/GlobalSearchBox.vue";

/**
 * 组件接收的属性定义
 * darkMode: 主题模式
 * userType: 用户类型，'admin'或'apikey'
 */
const props = defineProps({
  darkMode: {
    type: Boolean,
    required: true,
  },
  userType: {
    type: String,
    default: "admin", // 默认为管理员
    validator: (value) => ["admin", "apikey"].includes(value),
  },
});

// 使用文件管理composable
const {
  // 状态
  loading,
  error,
  successMessage,
  selectedItems: selectedFiles,
  lastRefreshTime,
  pagination,
  pageSizeOptions,
  files,
  editingFile,
  previewFile,
  showEdit,
  showPreview,
  showQRCodeModal,
  qrCodeDataURL,
  qrCodeSlug,
  copiedFiles,
  copiedPermanentFiles,

  // 方法
  loadFiles,
  searchFiles,
  handleOffsetChange,
  changePageSize,
  handleFileDelete,
  handleBatchDelete,
  openEditModal,
  updateFileMetadata,
  openPreviewModal,
  generateQRCode,
  copyFileLink,
  copyPermanentLink,
  getFilePassword,
  getOfficePreviewUrl,
  previewFileInNewWindow,
  downloadFileDirectly,
  getPermanentDownloadUrl,
  getPermanentViewUrl,
  toggleSelectItem,
  toggleSelectAll,
  clearSelection,
} = useFileManagement(props.userType);

// 需要在模板中使用的删除设置store
import { useDeleteSettingsStore } from "@/stores/deleteSettingsStore.js";
const deleteSettingsStore = useDeleteSettingsStore();

// 全局搜索状态
const globalSearchValue = ref("");

// 搜索状态
const isSearchMode = ref(false);
const searchResults = ref([]);
const searchLoading = ref(false);

// 智能预加载缓存
const preloadedData = ref([]);
const preloadedPages = ref(new Set());
const maxPreloadPages = 5; // 最多预加载5页数据

// 搜索处理函数 - 使用服务端搜索
const handleGlobalSearch = async (searchValue) => {
  globalSearchValue.value = searchValue;

  if (!searchValue || searchValue.trim().length < 2) {
    // 清除搜索，立即回到正常分页模式
    isSearchMode.value = false;
    searchResults.value = [];
    // 立即重新加载原始数据
    await loadFiles();
    console.log("搜索已清除，恢复到原始列表");
    return;
  }

  try {
    searchLoading.value = true;
    isSearchMode.value = true;

    // 重置分页到第一页进行搜索
    const result = await searchFiles(searchValue.trim(), 0);

    if (result && result.files) {
      // 搜索模式下，直接更新主要的files状态和分页信息
      files.value = result.files;
      // 更新分页信息
      if (result.pagination) {
        pagination.total = result.pagination.total;
        pagination.offset = result.pagination.offset || 0;
        pagination.hasMore = result.pagination.hasMore !== undefined ? result.pagination.hasMore : pagination.offset + pagination.limit < pagination.total;
      }
      console.log(`文件搜索完成: "${searchValue}", 找到 ${result.pagination?.total || result.files.length} 个结果`);
    } else {
      files.value = [];
      pagination.total = 0;
      pagination.offset = 0;
      pagination.hasMore = false;
    }
  } catch (error) {
    console.error("文件搜索失败:", error);
    files.value = [];
    pagination.total = 0;
    pagination.offset = 0;
    pagination.hasMore = false;
  } finally {
    searchLoading.value = false;
  }
};

const clearGlobalSearch = async () => {
  globalSearchValue.value = "";
  isSearchMode.value = false;
  searchResults.value = [];

  // 立即重新加载正常的文件列表
  try {
    await loadFiles();
    console.log("清除文件搜索，已恢复到原始列表");
  } catch (error) {
    console.error("清除搜索后重新加载失败:", error);
  }
};

// 处理分页变化（支持搜索模式）
const handleOffsetChangeWithSearch = async (newOffset) => {
  if (isSearchMode.value && globalSearchValue.value) {
    // 搜索模式下的分页
    try {
      searchLoading.value = true;
      const result = await searchFiles(globalSearchValue.value, newOffset);

      if (result && result.files) {
        files.value = result.files;
        // 更新分页信息
        if (result.pagination) {
          pagination.total = result.pagination.total;
          pagination.offset = result.pagination.offset || newOffset;
          pagination.hasMore = result.pagination.hasMore !== undefined ? result.pagination.hasMore : pagination.offset + pagination.limit < pagination.total;
        }
      }
    } catch (error) {
      console.error("搜索分页失败:", error);
    } finally {
      searchLoading.value = false;
    }
  } else {
    // 正常模式下的分页
    handleOffsetChange(newOffset);
  }
};

// 处理每页数量变化
const handlePageSizeChange = (newPageSize) => {
  changePageSize(newPageSize);
  // 如果在搜索模式，重新搜索
  if (isSearchMode.value && globalSearchValue.value) {
    handleGlobalSearch(globalSearchValue.value);
  } else {
    // 否则重新加载文件列表
    loadFiles();
  }
};

// 组件挂载时加载文件列表
onMounted(loadFiles);
</script>
