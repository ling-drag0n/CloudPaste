<!--
  图廊视图组件
  专门用于展示图片文件的瀑布流布局
  基于@yeger/vue-masonry-wall实现专业的瀑布流效果
-->
<template>
  <div class="gallery-view">
    <!-- 现代化图廊工具栏 -->
    <div class="gallery-toolbar mb-4" :class="darkMode ? 'bg-gray-800/80' : 'bg-white/90'">
      <!-- 主工具栏 -->
      <div class="px-3 py-2 border-b" :class="darkMode ? 'border-gray-700' : 'border-gray-200'">
        <div class="flex items-center justify-between">
          <!-- 左侧：统计信息 -->
          <div class="flex items-center gap-3">
            <div class="flex items-center gap-2">
              <!-- 图廊图标 -->
              <svg class="w-5 h-5" :class="darkMode ? 'text-blue-400' : 'text-blue-600'" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path
                  stroke-linecap="round"
                  stroke-linejoin="round"
                  stroke-width="2"
                  d="M4 16l4.586-4.586a2 2 0 012.828 0L16 16m-2-2l1.586-1.586a2 2 0 012.828 0L20 14m-6-6h.01M6 20h12a2 2 0 002-2V6a2 2 0 00-2-2H6a2 2 0 00-2 2v12a2 2 0 002 2z"
                />
              </svg>
              <span class="font-medium text-sm" :class="darkMode ? 'text-gray-200' : 'text-gray-900'">
                {{ t("gallery.viewModeName") }}
              </span>
            </div>

            <!-- 分隔符 -->
            <div class="w-px h-4" :class="darkMode ? 'bg-gray-600' : 'bg-gray-300'"></div>

            <!-- 统计信息 -->
            <span class="text-sm" :class="darkMode ? 'text-gray-400' : 'text-gray-600'">
              {{ getContentSummary() }}
            </span>
          </div>

          <!-- 右侧：快速操作 -->
          <div class="flex items-center gap-2">
            <!-- 排序按钮 -->
            <div class="relative">
              <button
                @click="toggleSortMenu"
                class="flex items-center gap-1 px-3 py-1.5 text-sm rounded-md transition-colors"
                :class="darkMode ? 'hover:bg-gray-700 text-gray-300' : 'hover:bg-gray-100 text-gray-700'"
              >
                <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3 4h13M3 8h9m-9 4h6m4 0l4-4m0 0l4 4m-4-4v12" />
                </svg>
                <span class="hidden sm:inline">{{ t("gallery.sort") }}</span>
                <svg class="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7" />
                </svg>
              </button>

              <!-- 排序菜单 -->
              <div
                v-if="showSortMenu"
                class="absolute right-0 top-full mt-1 w-48 rounded-md shadow-lg z-[9999]"
                :class="darkMode ? 'bg-gray-800 border border-gray-700' : 'bg-white border border-gray-200'"
              >
                <div class="py-1">
                  <button
                    v-for="option in sortOptions"
                    :key="option.value"
                    @click="handleSortChange(option.value)"
                    class="w-full text-left px-3 py-2 text-sm transition-colors"
                    :class="[
                      sortBy === option.value
                        ? darkMode
                          ? 'bg-gray-700 text-white'
                          : 'bg-gray-100 text-gray-900'
                        : darkMode
                        ? 'text-gray-300 hover:bg-gray-700'
                        : 'text-gray-700 hover:bg-gray-50',
                    ]"
                  >
                    {{ option.label }}
                  </button>
                </div>
              </div>
            </div>

            <!-- 视图设置按钮 -->
            <button
              @click="toggleViewSettings"
              class="flex items-center gap-1 px-3 py-1.5 text-sm rounded-md transition-colors"
              :class="darkMode ? 'hover:bg-gray-700 text-gray-300' : 'hover:bg-gray-100 text-gray-700'"
            >
              <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path
                  stroke-linecap="round"
                  stroke-linejoin="round"
                  stroke-width="2"
                  d="M12 6V4m0 2a2 2 0 100 4m0-4a2 2 0 110 4m-6 8a2 2 0 100-4m0 4a2 2 0 100 4m0-4v2m0-6V4m6 6v10m6-2a2 2 0 100-4m0 4a2 2 0 100 4m0-4v2m0-6V4"
                />
              </svg>
              <span class="hidden sm:inline">{{ t("gallery.settings") }}</span>
            </button>
          </div>
        </div>
      </div>

      <!-- 展开的视图设置面板 -->
      <div v-if="showViewSettings" class="px-3 py-2 border-b" :class="darkMode ? 'border-gray-700 bg-gray-800/30' : 'border-gray-200 bg-gray-50/50'">
        <div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
          <!-- 列数控制 -->
          <div class="space-y-2">
            <label class="block text-sm font-medium" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">
              {{ t("gallery.columns") }}
            </label>
            <div class="flex items-center gap-2">
              <!-- 自动按钮 -->
              <button
                @click="columnCount = 'auto'"
                class="px-3 py-1.5 text-xs rounded-md transition-colors"
                :class="[
                  columnCount === 'auto'
                    ? darkMode
                      ? 'bg-blue-600 text-white'
                      : 'bg-blue-500 text-white'
                    : darkMode
                    ? 'bg-gray-700 text-gray-300 hover:bg-gray-600'
                    : 'bg-gray-200 text-gray-700 hover:bg-gray-300',
                ]"
              >
                {{ t("gallery.auto") }}
              </button>

              <!-- 列数按钮组 -->
              <div class="flex rounded-md overflow-hidden border" :class="darkMode ? 'border-gray-600' : 'border-gray-300'">
                <button
                  v-for="cols in [2, 3, 4, 5, 6]"
                  :key="cols"
                  @click="columnCount = cols.toString()"
                  class="px-2 py-1.5 text-xs transition-colors"
                  :class="[
                    columnCount === cols.toString()
                      ? darkMode
                        ? 'bg-gray-700 text-white'
                        : 'bg-gray-200 text-gray-900'
                      : darkMode
                      ? 'bg-gray-800 text-gray-400 hover:bg-gray-700'
                      : 'bg-white text-gray-600 hover:bg-gray-50',
                  ]"
                >
                  {{ cols }}
                </button>
              </div>
            </div>
          </div>

          <!-- 间距控制 -->
          <div class="space-y-3">
            <div class="flex items-center justify-between">
              <label class="block text-sm font-medium" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">
                {{ t("gallery.spacing") }}
              </label>
              <button
                @click="resetGallerySettings"
                :disabled="isDefaultSettings"
                class="text-xs px-2 py-1 rounded transition-colors"
                :class="[
                  isDefaultSettings
                    ? darkMode
                      ? 'text-gray-600 cursor-not-allowed'
                      : 'text-gray-400 cursor-not-allowed'
                    : darkMode
                    ? 'text-gray-400 hover:text-gray-300 hover:bg-gray-700'
                    : 'text-gray-600 hover:text-gray-700 hover:bg-gray-100',
                ]"
                :title="isDefaultSettings ? t('gallery.alreadyDefault') : t('gallery.resetSettings')"
              >
                {{ t("gallery.reset") }}
              </button>
            </div>

            <!-- 水平排列的间距控制 -->
            <div class="grid grid-cols-1 lg:grid-cols-2 gap-6">
              <!-- 水平间距控制 -->
              <div class="space-y-2">
                <div class="flex items-center justify-between">
                  <label class="text-xs font-medium" :class="darkMode ? 'text-gray-400' : 'text-gray-600'">
                    {{ t("gallery.horizontalSpacing") }}
                  </label>
                  <span class="text-xs" :class="darkMode ? 'text-gray-400' : 'text-gray-600'"> {{ horizontalGap }}px </span>
                </div>
                <div class="relative">
                  <input
                    v-model.number="horizontalGap"
                    type="range"
                    min="0"
                    max="48"
                    step="2"
                    class="w-full h-2 rounded-lg appearance-none cursor-pointer spacing-slider horizontal-slider"
                    :class="darkMode ? 'bg-gray-700' : 'bg-gray-200'"
                  />
                  <div class="flex justify-between text-xs mt-1" :class="darkMode ? 'text-gray-500' : 'text-gray-400'">
                    <span>{{ t("gallery.tight") }}</span>
                    <span>{{ t("gallery.loose") }}</span>
                  </div>
                </div>
              </div>

              <!-- 垂直间距控制 -->
              <div class="space-y-2">
                <div class="flex items-center justify-between">
                  <label class="text-xs font-medium" :class="darkMode ? 'text-gray-400' : 'text-gray-600'">
                    {{ t("gallery.verticalSpacing") }}
                  </label>
                  <span class="text-xs" :class="darkMode ? 'text-gray-400' : 'text-gray-600'"> {{ verticalGap }}px </span>
                </div>
                <div class="relative">
                  <input
                    v-model.number="verticalGap"
                    type="range"
                    min="0"
                    max="48"
                    step="2"
                    class="w-full h-2 rounded-lg appearance-none cursor-pointer spacing-slider vertical-slider"
                    :class="darkMode ? 'bg-gray-700' : 'bg-gray-200'"
                  />
                  <div class="flex justify-between text-xs mt-1" :class="darkMode ? 'text-gray-500' : 'text-gray-400'">
                    <span>{{ t("gallery.tight") }}</span>
                    <span>{{ t("gallery.loose") }}</span>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>

    <!-- 瀑布流容器 -->
    <MasonryWall
      v-if="allImages.length > 0"
      :items="masonryItems"
      :column-width="columnWidth"
      :gap="baseGap"
      :min-columns="minColumns"
      :max-columns="maxColumns"
      :ssr-columns="1"
      :key-mapper="(item, column, row, index) => item.id || index"
      class="masonry-wall-gallery"
    >
      <template #default="{ item, index }">
        <div class="masonry-item" @click="handleItemClick(item.image)" v-context-menu="(event) => handleContextMenu(event, item.image)">
          <div class="masonry-image-container">
            <!-- 选择框 -->
            <div v-if="isCheckboxMode" class="absolute top-2 left-2 z-10" @click.stop="toggleItemSelect(item.image)">
              <input
                type="checkbox"
                :checked="isItemSelected(item.image)"
                class="h-4 w-4 rounded border-gray-300 text-primary-600 focus:ring-primary-500"
                :class="darkMode ? 'bg-gray-700 border-gray-500' : ''"
              />
            </div>

            <!-- 图片容器：始终渲染，实现真正的懒加载 -->
            <div class="masonry-image-wrapper">
              <!-- 真实图片：只有URL时才显示 -->
              <img
                v-if="getImageSrc(item.image)"
                :src="getImageSrc(item.image)"
                :alt="item.image.name"
                class="masonry-image"
                decoding="async"
                @load="(event) => handleImageLoad(item.image, event)"
                @error="handleImageError(item.image)"
              />

              <!-- 错误占位图：图片加载失败时显示 -->
              <div v-else-if="getImageState(item.image)?.status === 'error'" class="masonry-placeholder bg-red-100 dark:bg-red-900/20" :style="getPlaceholderStyle()">
                <div class="placeholder-content">
                  <div class="w-8 h-8 mx-auto mb-2 opacity-50">
                    <svg class="w-full h-full text-red-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                      <path
                        stroke-linecap="round"
                        stroke-linejoin="round"
                        stroke-width="2"
                        d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-2.5L13.732 4c-.77-.833-1.964-.833-2.732 0L4.082 16.5c-.77.833.192 2.5 1.732 2.5z"
                      />
                    </svg>
                  </div>
                  <span class="text-xs opacity-75 text-red-600 dark:text-red-400"> {{ t("gallery.loadError") }} </span>
                </div>
              </div>

              <!-- 懒加载占位图：用于IntersectionObserver观察 -->
              <div v-else class="masonry-placeholder lazy-image bg-gray-200 dark:bg-gray-700 animate-pulse" :data-image-path="item.image.path" :style="getPlaceholderStyle()">
                <div class="placeholder-content">
                  <div class="w-8 h-8 mx-auto mb-2 opacity-50">
                    <div v-html="getFileIcon(item.image, darkMode)" class="w-full h-full"></div>
                  </div>
                  <span class="text-xs opacity-75" :class="darkMode ? 'text-gray-400' : 'text-gray-600'"> {{ t("gallery.loading") }} </span>
                </div>
              </div>
            </div>

            <!-- 悬浮操作层 - 现在使用上下文菜单 -->
            <div class="masonry-overlay">
              <!-- 图片信息（保留原有的悬浮信息） -->
              <div class="masonry-info">
                <div class="text-sm font-medium truncate">{{ item.image.name }}</div>
                <div class="text-xs opacity-75 mt-1">{{ formatFileSize(item.image.size) }}</div>
              </div>
            </div>
          </div>
        </div>
      </template>
    </MasonryWall>

    <!-- 空状态提示 -->
    <div v-else class="text-center py-16">
      <div class="max-w-md mx-auto">
        <!-- 图片图标 -->
        <div class="w-24 h-24 mx-auto mb-6 opacity-30">
          <svg class="w-full h-full" :class="darkMode ? 'text-gray-500' : 'text-gray-400'" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path
              stroke-linecap="round"
              stroke-linejoin="round"
              stroke-width="1.5"
              d="M4 16l4.586-4.586a2 2 0 012.828 0L16 16m-2-2l1.586-1.586a2 2 0 012.828 0L20 14m-6-6h.01M6 20h12a2 2 0 002-2V6a2 2 0 00-2-2H6a2 2 0 00-2 2v12a2 2 0 002 2z"
            />
          </svg>
        </div>

        <!-- 主要消息 -->
        <h3 class="text-lg font-medium mb-2" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">
          {{ t("gallery.noImagesTitle") }}
        </h3>

        <!-- 详细说明 -->
        <p class="text-sm mb-4" :class="darkMode ? 'text-gray-400' : 'text-gray-500'">
          {{ t("gallery.noImagesDescription") }}
        </p>

        <!-- 统计信息 -->
        <div class="text-xs" :class="darkMode ? 'text-gray-500' : 'text-gray-400'">
          <span v-if="allFolders.length > 0">{{ allFolders.length }} {{ t("gallery.foldersCount") }}</span>
          <span v-if="allFolders.length > 0 && allOtherFiles.length > 0"> • </span>
          <span v-if="allOtherFiles.length > 0">{{ allOtherFiles.length }} {{ t("gallery.otherFilesCount") }}</span>
          <span v-if="allFolders.length === 0 && allOtherFiles.length === 0">{{ t("gallery.emptyFolder") }}</span>
        </div>
      </div>
    </div>

    <!-- 懒加载模式下不需要加载更多按钮 -->
  </div>
</template>

<script setup>
import { ref, onMounted, onBeforeUnmount, nextTick, watch } from "vue";
import { useI18n } from "vue-i18n";
import { useGalleryView } from "@/composables/ui-interaction/useGalleryView";
import { usePhotoSwipe } from "@/composables/ui-interaction/usePhotoSwipe";
import { getFileIcon } from "@/utils/fileTypeIcons";
import { formatFileSize } from "@/utils/fileUtils";
import MasonryWall from "@yeger/vue-masonry-wall";
import ContextMenu from "@imengyu/vue3-context-menu";

const { t } = useI18n();

const props = defineProps({
  items: {
    type: Array,
    default: () => [],
  },
  darkMode: {
    type: Boolean,
    default: false,
  },
  isCheckboxMode: {
    type: Boolean,
    default: false,
  },
  selectedItems: {
    type: Array,
    default: () => [],
  },
});

const emit = defineEmits(["item-click", "item-select", "download", "getLink", "rename", "delete", "show-message"]);

// 使用图廊视图组合式函数
const {
  // 设置状态
  columnCount,
  horizontalGap,
  verticalGap,
  sortBy,
  showSortMenu,
  showViewSettings,

  // MasonryWall配置
  baseGap,
  columnWidth,
  minColumns,
  maxColumns,

  // 工具栏配置
  sortOptions,

  // 图片数据处理
  imageStates,
  createImageGroups,
  createVisibleImages,
  createMasonryItems,

  // 图片URL管理
  loadImageUrl,
  initializeImageStates,

  // 设置管理
  isDefaultSettings,
  resetGallerySettings,

  // 工具栏交互
  toggleSortMenu,
  toggleViewSettings,
  handleSortChange,

  // 初始化方法
  setupWatchers,
} = useGalleryView();

// 使用PhotoSwipe图片预览组合式函数
const { initPhotoSwipe, openPhotoSwipe, destroyPhotoSwipe } = usePhotoSwipe();

// 使用composable中的数据处理方法
const { allFolders, allImages, allOtherFiles } = createImageGroups(props.items);

// 计算可见图片和相关状态
const visibleImages = createVisibleImages(allImages);
const masonryItems = createMasonryItems(visibleImages);

// 内容摘要 - 只显示图片统计
const getContentSummary = () => {
  const imageCount = allImages.length;

  if (imageCount === 0) {
    return t("gallery.noImages");
  }

  return `${imageCount} ${t("gallery.imagesCount")}`;
};

// 懒加载：只返回状态，不触发加载
const getImageSrc = (image) => {
  const imageState = imageStates.value.get(image.path);

  // 如果状态为loaded，返回URL
  if (imageState?.status === "loaded" && imageState.url) {
    return imageState.url;
  }

  // 不在这里触发加载，由IntersectionObserver负责
  return "";
};

// 获取图片状态
const getImageState = (image) => {
  return imageStates.value.get(image.path);
};

const handleImageLoad = (image, event) => {
  const img = event.target;
  const aspectRatio = img.naturalWidth / img.naturalHeight;

  // 🔍 检测图片是否来自缓存
  const loadSource = img.complete && img.naturalWidth > 0 ? "可能来自缓存" : "网络加载";
  console.log(`🖼️ 图片加载完成: ${image.name}, 尺寸: ${img.naturalWidth}x${img.naturalHeight}, 宽高比: ${aspectRatio.toFixed(2)}, 来源: ${loadSource}`);

  // 更新图片状态，添加尺寸信息
  const currentState = imageStates.value.get(image.path);
  if (currentState) {
    imageStates.value.set(image.path, {
      ...currentState,
      aspectRatio,
      naturalWidth: img.naturalWidth,
      naturalHeight: img.naturalHeight,
    });
  }
};

const handleImageError = (image) => {
  console.error(`图片加载失败: ${image.name}`);

  // 设置错误状态
  imageStates.value.set(image.path, { status: "error", url: null });
};

const getPlaceholderStyle = () => {
  // 瀑布流占位符样式
  return {
    width: "100%",
    height: "200px", // 固定高度，避免布局跳动
    display: "flex",
    alignItems: "center",
    justifyContent: "center",
  };
};

// 设备检测函数已在contextMenu指令中实现，这里不需要重复定义

// 上下文菜单处理
const handleContextMenu = ({ x, y, isMobile }, image) => {
  console.log(`${isMobile ? "📱 长按" : "🖱️ 右键"}触发上下文菜单:`, image.name);

  // 构建菜单项
  const menuItems = [
    {
      label: t("mount.fileItem.download"),
      svgIcon: "#icon-download",
      svgProps: {
        width: "16",
        height: "16",
      },
      onClick: () => handleDownload(image),
    },
    {
      label: t("mount.fileItem.getLink"),
      svgIcon: "#icon-link",
      svgProps: {
        width: "16",
        height: "16",
      },
      onClick: () => handleGetLink(image),
    },
    {
      label: t("mount.fileItem.rename"),
      svgIcon: "#icon-edit",
      svgProps: {
        width: "16",
        height: "16",
      },
      onClick: () => handleRename(image),
    },
    {
      label: t("mount.fileItem.delete"),
      svgIcon: "#icon-delete",
      svgProps: {
        width: "16",
        height: "16",
      },
      onClick: () => handleDelete(image),
    },
  ];

  // 显示上下文菜单
  ContextMenu.showContextMenu({
    x,
    y,
    items: menuItems,
    theme: props.darkMode ? "default dark" : "default",
    zIndex: 9999,
    minWidth: 160,
  });
};

// 懒加载：IntersectionObserver实现
const imageObserver = ref(null);

// 定时器管理
const timers = new Set();

// 安全的定时器函数
const safeSetTimeout = (callback, delay) => {
  const id = setTimeout(() => {
    timers.delete(id);
    callback();
  }, delay);
  timers.add(id);
  return id;
};

// 初始化图片懒加载Observer
const initImageLazyLoading = () => {
  if (imageObserver.value) {
    imageObserver.value.disconnect();
  }

  imageObserver.value = new IntersectionObserver(
    (entries) => {
      entries.forEach((entry) => {
        if (entry.isIntersecting) {
          const placeholder = entry.target;
          const imagePath = placeholder.dataset.imagePath;

          if (imagePath) {
            // 查找对应的图片对象
            const image = allImages.find((img) => img.path === imagePath);
            if (image) {
              // 根据可见比例确定优先级
              const priority = entry.intersectionRatio > 0.5 ? "high" : "normal";
              console.log(`🔍 懒加载触发: ${image.name} (intersectionRatio: ${entry.intersectionRatio.toFixed(2)}, priority: ${priority})`);

              // 触发图片URL加载，传递优先级
              loadImageUrl(image, priority);
              // 停止观察这个占位符
              imageObserver.value.unobserve(placeholder);
            }
          }
        }
      });
    },
    {
      rootMargin: "100px", // 增加提前加载范围，配合预加载策略
      threshold: [0.1, 0.5], // 多个阈值：10%触发加载，50%触发高优先级
    }
  );
};

// 观察所有懒加载占位符（带重试机制）
const observeLazyImages = (retryCount = 0) => {
  if (!imageObserver.value) return;

  // 查找所有懒加载占位符
  const lazyPlaceholders = document.querySelectorAll(".lazy-image");
  console.log(`🔍 找到 ${lazyPlaceholders.length} 个懒加载占位符 (尝试 ${retryCount + 1})`);

  if (lazyPlaceholders.length === 0 && retryCount < 5) {
    // 如果没有找到元素且重试次数未达上限，延迟重试
    console.log(`⏳ MasonryWall可能还在渲染，${200 * (retryCount + 1)}ms后重试...`);
    safeSetTimeout(() => {
      observeLazyImages(retryCount + 1);
    }, 200 * (retryCount + 1)); // 递增延迟：200ms, 400ms, 600ms...
    return;
  }

  if (lazyPlaceholders.length === 0) {
    console.warn("❌ 重试5次后仍未找到懒加载占位符，可能存在渲染问题");
    return;
  }

  let observedCount = 0;
  lazyPlaceholders.forEach((placeholder) => {
    // 只观察还没有URL的图片
    const imagePath = placeholder.dataset.imagePath;
    const imageState = imageStates.value.get(imagePath);
    if (imageState?.status === "idle") {
      imageObserver.value.observe(placeholder);
      observedCount++;

      // 检查占位符是否已经在视口内
      const rect = placeholder.getBoundingClientRect();
      const isInViewport = rect.top < window.innerHeight && rect.bottom > 0;
      console.log(`📍 占位符位置 ${imagePath}: top=${Math.round(rect.top)}, bottom=${Math.round(rect.bottom)}, 视口高度=${window.innerHeight}, 在视口内=${isInViewport}`);
    }
  });

  console.log(`👀 开始观察 ${observedCount}/${lazyPlaceholders.length} 个懒加载占位符`);
};

// 点击外部关闭菜单
const handleClickOutside = (event) => {
  // 关闭工具栏菜单
  if (!event.target.closest(".gallery-toolbar")) {
    showSortMenu.value = false;
    showViewSettings.value = false;
  }
};

// 事件处理 - 集成PhotoSwipe预览
const handleItemClick = async (item) => {
  // 如果是勾选模式，不触发预览
  if (props.isCheckboxMode) {
    toggleItemSelect(item);
    return;
  }

  try {
    console.log(`🔍 点击图片预览: ${item.name}`);

    // 找到当前图片在所有图片中的索引
    const currentIndex = allImages.findIndex((img) => img.path === item.path);

    if (currentIndex === -1) {
      console.warn(`⚠️ 无法找到图片索引: ${item.name}`);
      // 降级到原有的预览方式
      emit("item-click", item);
      return;
    }

    // 使用PhotoSwipe打开图片预览
    await openPhotoSwipe(
      allImages, // 所有图片数组
      currentIndex, // 当前图片索引
      imageStates.value, // 图片状态管理
      loadImageUrl // 图片URL加载函数
    );
  } catch (error) {
    console.error("❌ PhotoSwipe预览失败:", error);
    // 降级到原有的预览方式
    emit("item-click", item);
  }
};

const toggleItemSelect = (item) => {
  emit("item-select", item, !isItemSelected(item));
};

const isItemSelected = (item) => {
  return props.selectedItems.some((selected) => selected.path === item.path);
};

// ===== 操作菜单相关方法 =====

// 处理下载操作
const handleDownload = (image) => {
  emit("download", image);
};

// 处理获取链接操作
const handleGetLink = (image) => {
  emit("getLink", image);
};

// 处理重命名操作
const handleRename = (image) => {
  emit("rename", image);
};

// 处理删除操作
const handleDelete = (image) => {
  emit("delete", image);
};

// 更新CSS变量以控制垂直间距（水平间距由MasonryWall的gap属性控制）
const updateSpacingCSSVariables = () => {
  const galleryElement = document.querySelector(".masonry-wall-gallery");
  if (galleryElement) {
    galleryElement.style.setProperty("--vertical-gap", `${verticalGap.value}px`);
  }
};

// 监听垂直间距变化
watch(
  verticalGap,
  () => {
    updateSpacingCSSVariables();
  },
  { immediate: true }
);

// 监听masonryItems变化，重新观察新的懒加载图片
watch(
  masonryItems,
  () => {
    // 延迟观察，等待MasonryWall重新渲染完成
    safeSetTimeout(() => {
      observeLazyImages();
    }, 100);
  },
  { flush: "post" }
);

// 🔍 检查Service Worker状态
const checkServiceWorkerStatus = async () => {
  try {
    if ("caches" in window) {
      const galleryCache = await caches.open("gallery-images");
      const cachedRequests = await galleryCache.keys();
      console.log(`🖼️ 图廊缓存: ${cachedRequests.length} 张图片`);
    }
  } catch (error) {
    console.log("📡 图廊缓存: 0 张图片");
  }
};

// 生命周期
onMounted(() => {
  // 设置监听器
  setupWatchers();

  // 初始化真正的懒加载
  nextTick(async () => {
    console.log(`📊 初始化图片状态，所有图片数量: ${visibleImages.value.length}`);
    console.log(`📊 allImages数量: ${allImages.length}`);
    console.log(`📊 懒加载模式：所有图片都会渲染占位符，由IntersectionObserver控制加载`);

    // 🔍 检查Service Worker状态
    await checkServiceWorkerStatus();

    initializeImageStates(visibleImages.value);
    // 初始化CSS变量
    updateSpacingCSSVariables();
    // 初始化图片懒加载Observer
    initImageLazyLoading();
    // 初始化PhotoSwipe
    initPhotoSwipe();
  });

  // 延迟观察懒加载图片，等待MasonryWall完全渲染
  safeSetTimeout(() => {
    console.log(`🔍 开始查找懒加载占位符...`);
    observeLazyImages();
  }, 100);

  // 添加点击外部关闭菜单的监听器
  document.addEventListener("click", handleClickOutside);
});

// 清理事件监听器
onBeforeUnmount(() => {
  document.removeEventListener("click", handleClickOutside);

  // 清理所有定时器
  timers.forEach((id) => clearTimeout(id));
  timers.clear();

  // 清理IntersectionObserver
  if (imageObserver.value) {
    imageObserver.value.disconnect();
    imageObserver.value = null;
  }

  // 清理PhotoSwipe
  destroyPhotoSwipe();
});
</script>

<style scoped>
@import "@/styles/pages/mount-explorer/gallery.css";
</style>
