<script setup>
import { onMounted } from "vue";
import { useI18n } from "vue-i18n";
import { usePasteManagement } from "@/modules/paste";
import { useThemeMode } from "@/composables/core/useThemeMode.js";
import { useConfirmDialog, createConfirmFn } from "@/composables/core/useConfirmDialog.js";
import { IconClock, IconDelete, IconRefresh } from "@/components/icons";

// 导入子组件
import PasteTable from "@/modules/paste/admin/components/PasteTable.vue";
import PasteCardList from "@/modules/paste/admin/components/PasteCardList.vue";
import PasteMasonryView from "@/modules/paste/admin/components/PasteMasonryView.vue";
import PastePreviewModal from "@/modules/paste/admin/components/PastePreviewModal.vue";
import PasteEditModal from "@/modules/paste/admin/components/PasteEditModal.vue";
import CommonPagination from "@/components/common/CommonPagination.vue";
import QRCodeModal from "@/modules/fileshare/admin/components/QRCodeModal.vue";
import GlobalSearchBox from "@/components/common/GlobalSearchBox.vue";
import ConfirmDialog from "@/components/common/dialogs/ConfirmDialog.vue";
import ViewModeToggle from "@/components/common/ViewModeToggle.vue";
import PasteTagFilterBar from "@/modules/paste/admin/components/PasteTagFilterBar.vue";
import PasteTagManagerModal from "@/modules/paste/admin/components/PasteTagManagerModal.vue";
import PasteBatchTagModal from "@/modules/paste/admin/components/PasteBatchTagModal.vue";

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
  getConfirmText: ({ confirmType }) => (confirmType === "warning" ? t("common.dialogs.cleanupButton") : t("common.dialogs.deleteButton")),
});

// 使用文本管理composable
const {
  // 状态
  loading,
  selectedItems: selectedPastes,
  lastRefreshTime,
  pagination,
  pageSizeOptions,
  pastes,
  previewPaste,
  editingPaste,
  showPreview,
  showEdit,
  showQRCodeModal,
  qrCodeDataURL,
  qrCodeSlug,
  copiedTexts,
  copiedRawTexts,
  tags,
  tagsLoading,
  selectedTagIds,
  showUntagged,
  showTagManager,
  showBatchTagModal,
  batchTagMode,

  // 视图模式
  viewMode,

  // 权限状态
  isAdmin,

  // 方法
  loadPastes,
  loadTags,
  refreshPastes,
  searchQuery,
  isSearchMode,
  searchLoading,
  handleGlobalSearch,
  clearSearch,
  handleOffsetChangeWithSearch,
  handlePageSizeChange,
  deletePaste,
  batchDeletePastes,
  clearExpiredPastes,
  openPreview,
  closePreview,
  openEditModal,
  closeEditModal,
  submitEdit,
  copyLink,
  copyRawLink,
  quickEditContent,
  goToViewPage,
  showQRCode,
  toggleSelectItem,
  toggleSelectAll,
  toggleVisibility,
  toggleTagFilter,
  toggleUntaggedFilter,
  clearTagFilters,
  openBatchTags,
  applyBatchTags,
  createTag,
  updateTag,
  deleteTag,
  reorderTags,
} = usePasteManagement({ confirmFn });

const deleteSelectedPastes = batchDeletePastes;

// 视图模式选项配置
const viewModeOptions = [
  { value: "table", icon: "table", title: "表格视图" },
  { value: "masonry", icon: "masonry", title: "瀑布流视图" },
];

// 组件挂载时加载数据
onMounted(() => {
  Promise.all([loadPastes(), loadTags()]);
});
</script>

<template>
  <div class="p-3 sm:p-4 md:p-5 lg:p-6 flex-1 flex flex-col overflow-y-auto">
    <!-- 顶部操作栏 -->
    <div class="flex flex-col space-y-3 mb-4">
      <!-- 标题和操作按钮组 -->
      <div class="flex justify-between items-center">
        <h2 class="text-lg sm:text-xl font-medium" :class="darkMode ? 'text-white' : 'text-gray-900'">文本管理</h2>

        <div class="flex items-center space-x-2">
          <!-- 视图模式切换按钮组 - 移动端 -->
          <ViewModeToggle
            v-model="viewMode"
            :options="viewModeOptions"
            :dark-mode="darkMode"
            size="sm"
            class="md:hidden"
          />
          <!-- 视图模式切换按钮组 - 桌面端 -->
          <ViewModeToggle
            v-model="viewMode"
            :options="viewModeOptions"
            :dark-mode="darkMode"
            size="md"
            class="hidden md:inline-flex"
          />

          <!-- 刷新按钮 - 在所有屏幕尺寸显示 -->
          <button
            class="inline-flex items-center px-2 py-1 sm:px-3 sm:py-1.5 md:px-4 md:py-2 border border-transparent text-sm font-medium rounded-md shadow-sm text-white bg-primary-600 hover:bg-primary-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500"
            @click="refreshPastes"
            :disabled="loading || searchLoading"
          >
            <IconRefresh class="h-3 w-3 sm:h-4 sm:w-4 mr-1" :class="loading || searchLoading ? 'animate-spin' : ''" />
            <span class="hidden xs:inline">{{ loading || searchLoading ? "刷新中..." : "刷新" }}</span>
            <span class="xs:hidden">{{ loading || searchLoading ? "..." : "刷" }}</span>
          </button>
        </div>
      </div>

      <!-- 搜索框 -->
      <div class="w-full">
        <GlobalSearchBox
          v-model="searchQuery"
          placeholder="搜索文本分享（支持链接、备注、内容）"
          :show-hint="true"
          search-hint="服务端搜索，支持模糊匹配"
          size="md"
          :debounce-ms="300"
          @search="handleGlobalSearch"
          @clear="clearSearch"
        />
      </div>

      <PasteTagFilterBar
        :tags="tags"
        :selected-tag-ids="selectedTagIds"
        :untagged="showUntagged"
        :selected-count="selectedPastes.length"
        :can-manage="isAdmin"
        :loading="tagsLoading"
        @clear="clearTagFilters"
        @toggle-tag="toggleTagFilter"
        @toggle-untagged="toggleUntaggedFilter"
        @batch-add="openBatchTags('add')"
        @batch-remove="openBatchTags('remove')"
        @manage="showTagManager = true"
      />

      <!-- 其他操作按钮行 -->
      <div class="flex flex-wrap items-center gap-1 sm:gap-2">
        <!-- 清理过期按钮 -->
        <button
          class="inline-flex items-center px-2 py-1 sm:px-3 sm:py-1.5 md:px-4 md:py-2 border border-transparent text-xs sm:text-sm font-medium rounded-md shadow-sm text-white bg-amber-600 hover:bg-amber-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-amber-500 flex-grow sm:flex-grow-0"
          @click="clearExpiredPastes"
          title="系统会自动删除过期内容，但您也可以通过此功能手动立即清理"
        >
          <IconDelete class="h-3 w-3 sm:h-4 sm:w-4 mr-1" />
          <span class="hidden xs:inline">清理过期</span>
          <span class="xs:hidden">清理</span>
        </button>

        <!-- 批量删除按钮 -->
        <button
          :disabled="selectedPastes.length === 0"
          :class="[
            'inline-flex items-center px-2 py-1 sm:px-3 sm:py-1.5 md:px-4 md:py-2 border border-transparent text-xs sm:text-sm font-medium rounded-md shadow-sm focus:outline-none focus:ring-2 focus:ring-offset-2 flex-grow sm:flex-grow-0',
            selectedPastes.length === 0 ? 'bg-gray-300 text-gray-500 cursor-not-allowed' : 'text-white bg-red-600 hover:bg-red-700 focus:ring-red-500',
          ]"
          @click="deleteSelectedPastes"
        >
          <IconDelete class="h-3 w-3 sm:h-4 sm:w-4 mr-1" />
          <span class="hidden xs:inline">批量删除{{ selectedPastes.length ? `(${selectedPastes.length})` : "" }}</span>
          <span class="xs:hidden">删除{{ selectedPastes.length ? `(${selectedPastes.length})` : "" }}</span>
        </button>
      </div>
    </div>

    <!-- 上次刷新时间显示 -->
    <div class="flex justify-between items-center mb-2 sm:mb-3" v-if="lastRefreshTime">
      <div class="text-xs sm:text-sm text-gray-500 dark:text-gray-400">
        <span class="inline-flex items-center">
          <IconClock class="h-3 w-3 sm:h-4 sm:w-4 mr-1" />
          上次刷新: {{ lastRefreshTime }}
        </span>
      </div>
    </div>

    <!-- 数据展示区域 -->
    <div class="overflow-visible bg-white dark:bg-gray-800 shadow-md rounded-lg flex-1">
      <div class="flex flex-col h-full">
        <!-- 桌面端 - 表格视图 (中等及以上设备显示) -->
        <div v-if="viewMode === 'table'" class="hidden md:block flex-1 overflow-auto">
          <PasteTable
            :dark-mode="darkMode"
            :pastes="pastes"
            :selectedPastes="selectedPastes"
            :loading="loading || searchLoading"
            :copiedTexts="copiedTexts"
            :copiedRawTexts="copiedRawTexts"
            @toggle-select-all="toggleSelectAll"
            @toggle-select-item="toggleSelectItem"
            @view="goToViewPage"
            @copy-link="copyLink"
            @copy-raw-link="copyRawLink"
            @preview="openPreview"
            @edit="openEditModal"
            @delete="deletePaste"
            @show-qrcode="showQRCode"
            @toggle-visibility="toggleVisibility"
          />
        </div>

        <!-- 桌面端 - 瀑布流视图 (中等及以上设备显示) -->
        <div v-if="viewMode === 'masonry'" class="hidden md:block flex-1 overflow-visible">
          <PasteMasonryView
            :dark-mode="darkMode"
            :pastes="pastes"
            :selectedPastes="selectedPastes"
            :loading="loading || searchLoading"
            :copiedTexts="copiedTexts"
            :copiedRawTexts="copiedRawTexts"
            @toggle-select-all="toggleSelectAll"
            @toggle-select-item="toggleSelectItem"
            @view="goToViewPage"
            @copy-link="copyLink"
            @copy-raw-link="copyRawLink"
            @preview="openPreview"
            @edit="openEditModal"
            @delete="deletePaste"
            @show-qrcode="showQRCode"
            @quick-edit-content="quickEditContent"
          />
        </div>

        <!-- 移动端 - 列表视图 (小于中等设备显示) -->
        <div v-if="viewMode === 'table'" class="md:hidden flex-1 overflow-auto">
          <PasteCardList
            :dark-mode="darkMode"
            :pastes="pastes"
            :selectedPastes="selectedPastes"
            :loading="loading"
            :copiedTexts="copiedTexts"
            :copiedRawTexts="copiedRawTexts"
            @toggle-select-item="toggleSelectItem"
            @view="goToViewPage"
            @copy-link="copyLink"
            @copy-raw-link="copyRawLink"
            @preview="openPreview"
            @edit="openEditModal"
            @delete="deletePaste"
            @show-qrcode="showQRCode"
          />
        </div>

        <!-- 移动端 - 瀑布流视图 (小于中等设备显示) -->
        <div v-if="viewMode === 'masonry'" class="md:hidden flex-1 overflow-visible">
          <PasteMasonryView
            :dark-mode="darkMode"
            :pastes="pastes"
            :selectedPastes="selectedPastes"
            :loading="loading || searchLoading"
            :copiedTexts="copiedTexts"
            :copiedRawTexts="copiedRawTexts"
            @toggle-select-all="toggleSelectAll"
            @toggle-select-item="toggleSelectItem"
            @view="goToViewPage"
            @copy-link="copyLink"
            @copy-raw-link="copyRawLink"
            @preview="openPreview"
            @edit="openEditModal"
            @delete="deletePaste"
            @show-qrcode="showQRCode"
            @quick-edit-content="quickEditContent"
          />
        </div>
      </div>
    </div>

    <!-- 分页组件（搜索和正常模式统一显示） -->
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

    <!-- 预览弹窗组件 -->
    <PastePreviewModal
      :dark-mode="darkMode"
      :show-preview="showPreview"
      :paste="previewPaste"
      :copied-texts="copiedTexts"
      @close="closePreview"
      @view-paste="goToViewPage"
      @copy-link="copyLink"
    />

    <!-- 修改弹窗组件 -->
    <PasteEditModal :dark-mode="darkMode" :show-edit="showEdit" :paste="editingPaste" :available-tags="tags" @close="closeEditModal" @save="submitEdit" />

    <PasteTagManagerModal
      :show="showTagManager"
      :tags="tags"
      :loading="tagsLoading || loading"
      @close="showTagManager = false"
      @create="createTag"
      @update="updateTag"
      @delete="deleteTag"
      @reorder="reorderTags"
    />

    <PasteBatchTagModal
      :show="showBatchTagModal"
      :mode="batchTagMode"
      :tags="tags"
      :selected-count="selectedPastes.length"
      :loading="loading"
      @close="showBatchTagModal = false"
      @apply="applyBatchTags"
    />

    <!-- 二维码弹窗组件 -->
    <QRCodeModal v-if="showQRCodeModal" :qr-code-url="qrCodeDataURL" :file-slug="qrCodeSlug" :dark-mode="darkMode" @close="showQRCodeModal = false" />

    <!-- 确认对话框 -->
    <ConfirmDialog
      v-bind="dialogState"
      @confirm="handleConfirm"
      @cancel="handleCancel"
    />
  </div>
</template>
