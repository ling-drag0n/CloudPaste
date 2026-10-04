<template>
  <div>
    <!-- 列表视图的表头 -->
    <div
      v-if="viewMode === 'list'"
      class="grid items-center gap-3 py-2 px-3 mx-1 border-b sticky top-16 z-10 backdrop-blur-md rounded-t-xl transition-colors duration-200"
      :class="[
        darkMode ? 'bg-gray-800/80 border-gray-700' : 'bg-white/80 border-gray-200',
        headerGridClass,
      ]"
    >
      <!-- 全选勾选框 (根据全局设置显示) -->
      <div v-if="showCheckboxes" class="flex items-center justify-center p-2 -m-2">
        <input
          type="checkbox"
          :checked="isAllSelected"
          @change="toggleSelectAll(!isAllSelected)"
          class="h-4 w-4 rounded border-gray-300 text-primary-600 focus:ring-primary-500"
          :class="darkMode ? 'bg-gray-700 border-gray-500' : ''"
        />
      </div>
      <div
        class="flex items-center justify-center"
        :class="darkMode ? 'text-gray-300' : 'text-gray-700'"
        :title="t('mount.fileList.type')"
      >
        <IconDocument class="w-4 h-4 opacity-70" aria-hidden="true" />
        <span class="sr-only">{{ t("mount.fileList.type") }}</span>
      </div>

      <!-- 可排序的名称列 -->
      <div
        class="font-medium cursor-pointer select-none flex items-center hover:opacity-75 transition-opacity"
        :class="darkMode ? 'text-gray-300' : 'text-gray-700'"
        @click="handleSort('name')"
        :title="t('mount.fileList.clickToSort')"
      >
        <span class="truncate">{{ t("mount.fileList.name") }}</span>
        <!-- 排序图标 -->
        <template v-if="getSortIcon('name')">
          <IconChevronDown v-if="sortField === 'name' && sortOrder === 'asc'" size="xs" class="ml-1 flex-shrink-0" aria-hidden="true" />
          <IconChevronUp v-else size="xs" class="ml-1 flex-shrink-0" aria-hidden="true" />
        </template>
        <!-- 默认排序提示图标 -->
        <IconHamburger v-else size="xs" class="ml-1 flex-shrink-0 opacity-40" aria-hidden="true" />
      </div>

      <!-- 可排序的大小列 -->
      <div
        class="min-w-24 text-center font-medium hidden sm:flex cursor-pointer select-none items-center justify-center hover:opacity-75 transition-opacity"
        :class="darkMode ? 'text-gray-300' : 'text-gray-700'"
        @click="handleSort('size')"
        :title="t('mount.fileList.clickToSort')"
      >
        <span>{{ t("mount.fileList.size") }}</span>
        <!-- 排序图标 -->
        <template v-if="getSortIcon('size')">
          <IconChevronDown v-if="sortField === 'size' && sortOrder === 'asc'" size="xs" class="ml-1 flex-shrink-0" aria-hidden="true" />
          <IconChevronUp v-else size="xs" class="ml-1 flex-shrink-0" aria-hidden="true" />
        </template>
        <!-- 默认排序提示图标 -->
        <IconHamburger v-else size="xs" class="ml-1 flex-shrink-0 opacity-40" aria-hidden="true" />
      </div>

      <!-- 可排序的修改时间列 -->
      <div
        class="min-w-36 text-center font-medium hidden sm:flex cursor-pointer select-none items-center justify-center hover:opacity-75 transition-opacity"
        :class="darkMode ? 'text-gray-300' : 'text-gray-700'"
        @click="handleSort('modified')"
        :title="t('mount.fileList.clickToSort')"
      >
        <span>{{ t("mount.fileList.modifiedTime") }}</span>
        <!-- 排序图标 -->
        <template v-if="getSortIcon('modified')">
          <IconChevronDown v-if="sortField === 'modified' && sortOrder === 'asc'" size="xs" class="ml-1 flex-shrink-0" aria-hidden="true" />
          <IconChevronUp v-else size="xs" class="ml-1 flex-shrink-0" aria-hidden="true" />
        </template>
        <!-- 默认排序提示图标 -->
        <IconHamburger v-else size="xs" class="ml-1 flex-shrink-0 opacity-40" aria-hidden="true" />
      </div>

      <div v-if="showActionButtons" class="min-w-[80px] sm:min-w-32 text-center font-medium" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">{{ t("mount.fileList.actions") }}</div>
    </div>

    <div v-if="loading">
      <!-- 骨架屏加载 -->
      <SkeletonLoader :dark-mode="darkMode" :count="8" />
    </div>

    <div v-else-if="!items.length" class="py-8 flex flex-col items-center justify-center" :class="darkMode ? 'text-gray-400' : 'text-gray-500'">
      <IconFolderOpen size="3xl" class="mb-2" aria-hidden="true" />
      <div>{{ isVirtual ? t("mount.fileList.noMountPoints") : t("mount.fileList.empty") }}</div>
    </div>

    <div v-else>
      <!-- 列表视图 -->
      <div v-if="viewMode === 'list'" ref="listContainerRef">
        <!-- 虚拟滚动模式（文件数量超过阈值时启用） -->
        <div 
          v-if="shouldVirtualize"
          class="virtual-scroll-wrapper"
          :style="{ height: `${totalVirtualSize}px`, width: '100%', position: 'relative' }"
        >
          <FileItem
            v-for="virtualRow in virtualItems"
            :key="sortedItems[virtualRow.index].path"
            :item="sortedItems[virtualRow.index]"
            :index="virtualRow.index"
            :dark-mode="darkMode"
            :show-checkboxes="showCheckboxes"
            :is-selected="isItemSelected(sortedItems[virtualRow.index])"
            :is-context-highlight="isContextHighlight(sortedItems[virtualRow.index])"
            :current-path="currentPath"
            :animations-enabled="animationsEnabled"
            :file-name-overflow="fileNameOverflow"
            :show-action-buttons="showActionButtons"
            :style="{
              position: 'absolute',
              top: 0,
              left: 0,
              width: '100%',
              height: `${virtualRow.size}px`,
              transform: `translateY(${virtualRow.start - scrollMargin}px)`,
            }"
            @click="handleItemClick(sortedItems[virtualRow.index])"
            @download="handleDownload"
            @rename="handleRename"
            @delete="handleDelete"
            @select="handleItemSelect"
            @getLink="handleGetLink"
            @share="handleShare"
            @show-message="handleShowMessage"
            @contextmenu="handleContextMenu"
          />
        </div>
        
        <!-- 普通渲染模式（文件数量较少时使用） -->
        <template v-else>
          <FileItem
            v-for="(item, index) in sortedItems"
            :key="item.path"
            :item="item"
            :index="index"
            :dark-mode="darkMode"
            :show-checkboxes="showCheckboxes"
            :is-selected="isItemSelected(item)"
            :is-context-highlight="isContextHighlight(item)"
            :current-path="currentPath"
            :animations-enabled="animationsEnabled"
            :file-name-overflow="fileNameOverflow"
            :show-action-buttons="showActionButtons"
            @click="handleItemClick(item)"
            @download="handleDownload"
            @rename="handleRename"
            @delete="handleDelete"
            @select="handleItemSelect"
            @getLink="handleGetLink"
            @share="handleShare"
            @show-message="handleShowMessage"
            @contextmenu="handleContextMenu"
          />
        </template>
      </div>

      <!-- 网格视图 - 使用 auto-fill + minmax 实现平滑响应式 -->
      <div v-else-if="viewMode === 'grid'" class="directory-grid p-4">
        <div
          v-for="item in sortedItems"
          :key="item.path"
          class="relative flex flex-col items-center p-2 sm:p-3 rounded-lg transition-colors hover:cursor-pointer group"
          :class="darkMode ? 'hover:bg-gray-700/50' : 'hover:bg-gray-100'"
          @click="handleItemClick(item)"
        >
          <!-- 勾选框 (根据全局设置显示) -->
          <div v-if="showCheckboxes" class="absolute top-1 left-1 z-10" @click.stop="toggleItemSelect(item)">
            <input
              type="checkbox"
              :checked="isItemSelected(item)"
              class="h-4 w-4 rounded border-gray-300 text-primary-600 focus:ring-primary-500"
              :class="darkMode ? 'bg-gray-700 border-gray-500' : ''"
            />
          </div>

          <!-- 文件/文件夹图标 -->
          <div class="mb-2 w-8 h-8 sm:w-10 sm:h-10 md:w-12 md:h-12">
            <span v-html="getFileIcon(item, darkMode)"></span>
          </div>

          <!-- 文件/文件夹名称 -->
          <div class="text-center truncate w-full text-xs sm:text-sm md:text-base" :class="darkMode ? 'text-gray-200' : 'text-gray-700'" :title="item.name">
            {{ item.name }}
          </div>

          <!-- 操作按钮 -->
          <div class="mt-2 flex space-x-0.5 sm:space-x-0.5 opacity-100 sm:opacity-0 sm:group-hover:opacity-100 transition-opacity">
            <!-- 下载按钮 (仅文件) -->
            <button
              v-if="!item.isDirectory"
              @click.stop="handleDownload(item)"
              class="p-1 sm:p-1 rounded-full"
              :class="darkMode ? 'hover:bg-gray-600 text-gray-300' : 'hover:bg-gray-200 text-gray-600'"
            >
              <IconDownload class="w-3.5 h-3.5 sm:w-4 sm:h-4" aria-hidden="true" />
            </button>

            <!-- 直链按钮 (仅文件) -->
            <button
              v-if="!item.isDirectory"
              @click.stop="handleGetLink(item)"
              class="p-1 sm:p-1 rounded-full"
              :class="darkMode ? 'hover:bg-gray-600 text-gray-300' : 'hover:bg-gray-200 text-gray-600'"
            >
              <IconLink class="w-3.5 h-3.5 sm:w-4 sm:h-4" aria-hidden="true" />
            </button>

            <!-- 重命名按钮 - 只对文件显示，文件夹暂时不显示重命名按钮 -->
            <button
              v-if="!item.isDirectory"
              @click.stop="handleRename(item)"
              class="p-1 sm:p-1 rounded-full"
              :class="darkMode ? 'hover:bg-gray-600 text-gray-300' : 'hover:bg-gray-200 text-gray-600'"
            >
              <IconRename class="w-3.5 h-3.5 sm:w-4 sm:h-4" aria-hidden="true" />
            </button>

            <!-- 删除按钮 -->
            <button @click.stop="handleDelete(item)" class="p-1 sm:p-1 rounded-full" :class="darkMode ? 'hover:bg-gray-600 text-gray-300' : 'hover:bg-gray-200 text-gray-600'">
              <IconDelete class="w-3.5 h-3.5 sm:w-4 sm:h-4" aria-hidden="true" />
            </button>
          </div>

          <!-- 文件大小与修改时间提示 -->
          <div class="mt-1 text-xs text-center" :class="darkMode ? 'text-gray-400' : 'text-gray-500'">
            <span v-if="item.isDirectory && item.isVirtual">-</span>
            <span v-else>{{ formatSize(item.size) }}</span>
          </div>
        </div>
      </div>

      <!-- 图廊视图 -->
      <GalleryView
        v-else-if="viewMode === 'gallery'"
        :items="sortedItems"
        :dark-mode="darkMode"
        :is-checkbox-mode="showCheckboxes"
        :selected-items="selectedItems"
        @item-click="handleItemClick"
        @item-select="handleItemSelect"
        @download="handleDownload"
        @getLink="handleGetLink"
        @rename="handleRename"
        @delete="handleDelete"
        @contextmenu="handleContextMenu"
      />

      <!-- 目录分页：加载更多（通用能力：上游分页/超大目录时按页拉取） -->
      <div v-if="hasMore" class="px-4 pb-6 flex flex-col items-center gap-2">
        <!-- 哨兵：用于“滚动到底自动触发加载更多”（无感） -->
        <!-- 注意：必须有非零尺寸，否则部分浏览器不会稳定触发 IntersectionObserver -->
        <div ref="loadMoreSentinelRef" class="h-2 w-full" aria-hidden="true"></div>
        <button
          class="px-4 py-2 rounded-lg text-sm font-medium transition-colors"
          :class="[
            darkMode ? 'bg-gray-700 hover:bg-gray-600 text-gray-100' : 'bg-gray-200 hover:bg-gray-300 text-gray-800',
            loadingMore ? 'opacity-70 cursor-not-allowed' : '',
          ]"
          :disabled="loadingMore"
          @click="emit('load-more')"
        >
          {{ loadingMore ? t("common.loading") : t("mount.fileList.loadMore") }}
        </button>
        <div class="text-xs text-center" :class="darkMode ? 'text-gray-400' : 'text-gray-500'">
          {{ t("mount.fileList.pagedHint") }}
        </div>
      </div>
    </div>

    <!-- 通用 InputDialog 组件替换内联重命名对话框 -->
    <InputDialog
      :is-open="showRenameDialog"
      :title="t('mount.rename.title')"
      :description="t('mount.rename.enterNewName')"
      :label="t('mount.rename.newName')"
      :initial-value="newName"
      :validator="validateFsItemNameDialog"
      :confirm-text="t('mount.rename.confirm')"
      :cancel-text="t('mount.rename.cancel')"
      :loading="renameLoading"
      :loading-text="t('mount.rename.renaming')"
      :dark-mode="darkMode"
      @confirm="handleRenameConfirm"
      @cancel="handleRenameCancel"
      @close="handleRenameDialogClose"
    />

  </div>
</template>

<script setup>
import { ref, computed, nextTick, onMounted, onUnmounted, watch, defineAsyncComponent } from "vue";
import { useEventListener, useWindowScroll } from "@vueuse/core";
import { useI18n } from "vue-i18n";
import { useWindowVirtualizer } from "@tanstack/vue-virtual";
import FileItem from "./FileItem.vue";
// 仅当切到 viewMode=gallery 时才需要加载。
const GalleryView = defineAsyncComponent(() => import("./GalleryView.vue"));
import SkeletonLoader from "../shared/SkeletonLoader.vue";
import { IconChevronDown, IconChevronUp, IconDelete, IconDocument, IconDownload, IconFolderOpen, IconHamburger, IconLink, IconRename } from "@/components/icons";
import { getFileIcon } from "@/utils/fileTypeIcons.js";
import { useDirectorySort } from "@/composables/file-system/useDirectorySort.js";
import { useFileOperations } from "@/composables/file-system/useFileOperations.js";
import InputDialog from "@/components/common/dialogs/InputDialog.vue";
import { createFsItemNameDialogValidator, validateFsItemName } from "@/utils/fsPathUtils.js";

const { t } = useI18n();

const validateFsItemNameDialog = createFsItemNameDialogValidator(t);

// ===== 虚拟滚动配置 =====
// 虚拟滚动阈值：超过此数量启用虚拟滚动
const VIRTUAL_SCROLL_THRESHOLD = 500;
// FileItem 高度（像素），包含 margin
const FILE_ITEM_HEIGHT = 52;
// 缓冲区项目数
const OVERSCAN = 10;

// 使用新的组合式函数
const { sortField, sortOrder, handleSort, getSortIcon, createSortedItems, initializeSortState } = useDirectorySort();

const { getFileLink } = useFileOperations();

const props = defineProps({
  items: {
    type: Array,
    default: () => [],
  },
  loading: {
    type: Boolean,
    default: false,
  },
  // 目录分页：是否还有下一页（由父组件/后端决定）
  hasMore: {
    type: Boolean,
    default: false,
  },
  // 目录分页：加载更多按钮的 loading 状态
  loadingMore: {
    type: Boolean,
    default: false,
  },
  isVirtual: {
    type: Boolean,
    default: false,
  },
  darkMode: {
    type: Boolean,
    default: false,
  },
  viewMode: {
    type: String,
    default: "list", // 'list' | 'grid' | 'gallery'
  },
  showCheckboxes: {
    type: Boolean,
    default: false,
  },
  selectedItems: {
    type: Array,
    default: () => [],
  },
  // 右键菜单高亮的项目路径（用于临时高亮显示）
  contextHighlightPath: {
    type: String,
    default: null,
  },
  currentPath: {
    type: String,
    required: true,
  },
  animationsEnabled: {
    type: Boolean,
    default: true,
  },
  // 文件名过长处理模式: ellipsis(省略号) | scroll(滚动) | wrap(换行)
  fileNameOverflow: {
    type: String,
    default: 'ellipsis',
  },
  // 是否显示操作按钮列
  showActionButtons: {
    type: Boolean,
    default: true,
  },
  // 重命名操作的 loading 状态（由父组件控制）
  renameLoading: {
    type: Boolean,
    default: false,
  },
});

const emit = defineEmits(["navigate", "download", "getLink", "share", "rename", "delete", "preview", "item-select", "toggle-select-all", "show-message", "contextmenu", "load-more"]);

const sortedItems = createSortedItems(computed(() => props.items));

// ===== 虚拟滚动实现（Window Virtualizer 模式）=====
// 虚拟列表容器引用（用于计算 scrollMargin）
const listContainerRef = ref(null);
// 容器距离文档顶部的偏移量（用于 Window Virtualizer 的 scrollMargin）
const scrollMargin = ref(0);
const { y: windowScrollY } = useWindowScroll();

// ===== 目录分页：无感加载（滚动到底自动加载下一页）=====
const loadMoreSentinelRef = ref(null);
const hasUserScrolled = ref(false);
const lastAutoLoadAt = ref(0);
let loadMoreObserver = null;

const AUTO_LOAD_THROTTLE_MS = 800;

const cleanupLoadMoreObserver = () => {
  try {
    if (loadMoreObserver) {
      loadMoreObserver.disconnect();
      loadMoreObserver = null;
    }
  } catch {
    loadMoreObserver = null;
  }
};

const tryAutoLoadMore = () => {
  // 只在“确实往下滑了”之后，才自动加载。
  // 这样可以避免：刚进入目录时因为列表太短、哨兵在视口里，导致自动连刷很多页。
  if (!hasUserScrolled.value) return;
  if (!props.hasMore || props.loadingMore || props.loading) return;

  const now = Date.now();
  if (now - lastAutoLoadAt.value < AUTO_LOAD_THROTTLE_MS) return;
  lastAutoLoadAt.value = now;
  emit("load-more");
};

const setupLoadMoreObserver = async () => {
  cleanupLoadMoreObserver();

  // 没有更多页就不用观察
  if (!props.hasMore) return;

  // SSR / 不支持 IntersectionObserver：直接退化为按钮点击
  if (typeof window === "undefined" || typeof window.IntersectionObserver !== "function") return;

  await nextTick();
  const el = loadMoreSentinelRef.value;
  if (!el) return;

  loadMoreObserver = new IntersectionObserver(
    (entries) => {
      const hit = entries?.some?.((e) => e && e.isIntersecting);
      if (hit) tryAutoLoadMore();
    },
    {
      root: null, // window 级别滚动
      // 提前一点点触发（离底部还有 200px 就开始拉下一页），体验更顺滑
      rootMargin: "200px 0px 200px 0px",
      threshold: 0,
    },
  );
  loadMoreObserver.observe(el);
};

// 是否启用虚拟滚动（文件数量超过阈值时启用）
const shouldVirtualize = computed(() => {
  return sortedItems.value && sortedItems.value.length > VIRTUAL_SCROLL_THRESHOLD;
});

// 更新 scrollMargin（容器距离文档顶部的绝对偏移量）
// 使用 getBoundingClientRect + scrollY 获取准确的文档偏移
const updateScrollMargin = () => {
  if (listContainerRef.value) {
    const rect = listContainerRef.value.getBoundingClientRect();
    // 计算元素相对于文档顶部的绝对位置
    scrollMargin.value = rect.top + windowScrollY.value;
  }
};

// 虚拟滚动器 - 使用 useWindowVirtualizer（窗口级别滚动）
// 这样虚拟滚动会使用整个窗口作为滚动容器，不会创建独立的滚动区域
const rowVirtualizer = useWindowVirtualizer(
  computed(() => ({
    count: sortedItems.value?.length || 0,
    estimateSize: () => FILE_ITEM_HEIGHT,
    overscan: OVERSCAN,
    scrollMargin: scrollMargin.value,
  }))
);

// 虚拟项目列表
const virtualItems = computed(() => {
  if (!shouldVirtualize.value) return [];
  return rowVirtualizer.value.getVirtualItems();
});

// 虚拟滚动总高度
const totalVirtualSize = computed(() => {
  if (!shouldVirtualize.value) return 0;
  return rowVirtualizer.value.getTotalSize();
});

// 监听容器位置变化，更新 scrollMargin
onMounted(() => {
  // 初始化时更新 scrollMargin
  nextTick(() => {
    updateScrollMargin();
  });
  // 监听窗口 resize/scroll（VueUse 自动管理监听器与清理）
  useEventListener(window, 'resize', updateScrollMargin);
  useEventListener(window, 'scroll', updateScrollMargin, { once: true });

  // 记录“用户确实滚动过”，用于避免进入目录时自动疯狂拉页
  useEventListener(
    window,
    "scroll",
    () => {
      hasUserScrolled.value = true;
    },
    { passive: true, once: true },
  );

  // 初始化目录分页的观察器
  setupLoadMoreObserver();
});

onUnmounted(() => {
  cleanupLoadMoreObserver();
});

// 目录分页状态变化时，重新挂载/卸载观察器
watch(
  () => props.hasMore,
  () => {
    setupLoadMoreObserver();
  },
);

watch(
  () => props.items?.length,
  () => {
    // items 变多后，哨兵的位置变化，保险起见重建一次 observer
    setupLoadMoreObserver();
  },
);

// 切目录时重置自动加载状态，避免“从上一个目录的滚动状态”串到新目录导致误触发连拉
watch(
  () => props.currentPath,
  () => {
    hasUserScrolled.value = false;
    lastAutoLoadAt.value = 0;
    setupLoadMoreObserver();
  },
);


// 计算表头网格布局类 - 根据勾选框显示和操作按钮显示状态动态调整
const headerGridClass = computed(() => {
  const hasCheckbox = props.showCheckboxes;
  const hasActions = props.showActionButtons;
  
  if (hasCheckbox && hasActions) {
    // 显示勾选框 + 操作列
    return 'grid-cols-[auto_var(--explorer-icon-size,20px)_1fr_auto] sm:grid-cols-[auto_var(--explorer-icon-size,20px)_1fr_6rem_9rem_auto]';
  } else if (hasCheckbox && !hasActions) {
    // 显示勾选框 + 无操作列
    return 'grid-cols-[auto_var(--explorer-icon-size,20px)_1fr] sm:grid-cols-[auto_var(--explorer-icon-size,20px)_1fr_6rem_9rem]';
  } else if (!hasCheckbox && hasActions) {
    // 不显示勾选框 + 操作列
    return 'grid-cols-[var(--explorer-icon-size,20px)_1fr_auto] sm:grid-cols-[var(--explorer-icon-size,20px)_1fr_6rem_9rem_auto]';
  } else {
    // 不显示勾选框 + 无操作列
    return 'grid-cols-[var(--explorer-icon-size,20px)_1fr] sm:grid-cols-[var(--explorer-icon-size,20px)_1fr_6rem_9rem]';
  }
});

// 导入统一的工具函数
import { formatFileSize } from "@/utils/fileUtils.js";

// 格式化大小：未知则显示 "-"
const formatSize = (value) => {
  if (typeof value !== "number" || !Number.isFinite(value) || value < 0) return "-";
  return formatFileSize(value);
};

// 判断一个项目是否被选中
const isItemSelected = (item) => {
  return props.selectedItems.some((selectedItem) => selectedItem.path === item.path);
};

// 判断一个项目是否是右键菜单高亮状态
const isContextHighlight = (item) => {
  return props.contextHighlightPath === item.path;
};

// 判断是否全部选中
const isAllSelected = computed(() => {
  return props.items.length > 0 && props.items.every((item) => isItemSelected(item));
});

// 切换单个项目的选中状态
const toggleItemSelect = (item) => {
  emit("item-select", item, !isItemSelected(item));
};

// 全选/取消全选
const toggleSelectAll = (select) => {
  emit("toggle-select-all", select);
};

// 处理文件/文件夹点击
// 无论是否显示勾选框，点击文件/文件夹都应该正常导航或预览
// 只有点击勾选框本身才切换选中状态
const handleItemClick = (item) => {
  if (item.isDirectory) {
    // 对于目录，导航进入
    emit("navigate", item.path);
  } else {
    // 对于文件，触发预览
    emit("preview", item);
  }
};

// 处理下载
const handleDownload = (item) => {
  emit("download", item);
};

// 重命名相关
const showRenameDialog = ref(false);
const itemToRename = ref(null);
const newName = ref("");
const renameInput = ref(null);

// 处理重命名
const handleRename = (item) => {
  itemToRename.value = item;
  newName.value = item.name;
  showRenameDialog.value = true;

  // 在下一个DOM更新周期聚焦输入框
  nextTick(() => {
    renameInput.value?.focus();
  });
};

// 处理重命名确认 - 不立即关闭对话框，等待父组件完成操作后关闭
const handleRenameConfirm = (fileName) => {
  if (itemToRename.value) {
    emit("rename", {
      item: itemToRename.value,
      newName: fileName,
    });
    // 注意：不在这里关闭对话框，由父组件通过 closeRenameDialog 事件控制
  }
};

// 处理重命名取消
const handleRenameCancel = () => {
  showRenameDialog.value = false;
  newName.value = "";
  itemToRename.value = null;
};

// 处理对话框关闭（仅在非 loading 状态下允许关闭）
const handleRenameDialogClose = () => {
  if (!props.renameLoading) {
    showRenameDialog.value = false;
    newName.value = "";
    itemToRename.value = null;
  }
};

// 关闭重命名对话框（供父组件调用）
const closeRenameDialog = () => {
  showRenameDialog.value = false;
  newName.value = "";
  itemToRename.value = null;
};

// 暴露方法给父组件
defineExpose({
  closeRenameDialog,
});

// 处理删除
const handleDelete = (item) => {
  emit("delete", item);
};

// 处理项目选择
const handleItemSelect = (item, selected) => {
  emit("item-select", item, selected);
};

// 处理文件直链获取（使用新的 composable）
const handleGetLink = async (item) => {
  const result = await getFileLink(item);

  emit("show-message", {
    type: result.success ? "success" : "error",
    message: result.message,
  });
};

// 处理消息显示
const handleShowMessage = (messageInfo) => {
  emit("show-message", messageInfo);
};

// 处理生成分享链接（文件与文件夹均支持）
const handleShare = (item) => {
  emit("share", item);
};

// 处理右键菜单事件
const handleContextMenu = (payload) => {
  emit("contextmenu", payload);
};

// 组件挂载时初始化排序状态
onMounted(() => {
  initializeSortState();
});
</script>


<style scoped>
/* ===== 虚拟滚动样式（Window Virtualizer 模式）===== */
.virtual-scroll-wrapper {
  /* 虚拟滚动内容容器，使用相对定位 */
  position: relative;
  width: 100%;
  /* 启用 GPU 加速 */
  will-change: contents;
  /* 优化渲染性能 */
  contain: layout style;
}

/* 网格视图 - 使用 auto-fill + minmax 实现平滑响应式 */
.directory-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(var(--explorer-grid-min-width, 120px), 1fr));
  gap: var(--explorer-gap, 12px);
  transition: gap var(--duration-fast, 150ms);
}

/* 移动端优化 */
@media (max-width: 640px) {
  .directory-grid {
    grid-template-columns: repeat(auto-fill, minmax(100px, 1fr));
    gap: 8px;
  }
}
</style>
