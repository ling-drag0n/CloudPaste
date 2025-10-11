/**
 * 图廊视图组合式函数
 * 提供图廊视图的完整功能逻辑，包括设置管理、数据处理、MasonryWall配置等
 */

import { ref, computed, watch } from "vue";
import { useI18n } from "vue-i18n";
import api from "@/api";

export function useGalleryView() {
  const { t } = useI18n();

  // ===== localStorage设置管理 =====

  // localStorage键名
  const STORAGE_KEYS = {
    COLUMN_COUNT: "gallery_column_count",
    HORIZONTAL_GAP: "gallery_horizontal_gap",
    VERTICAL_GAP: "gallery_vertical_gap",
    SORT_BY: "gallery_sort_by",
  };

  // 从localStorage恢复设置
  const getStoredValue = (key, defaultValue) => {
    try {
      const stored = localStorage.getItem(key);
      return stored !== null ? JSON.parse(stored) : defaultValue;
    } catch (error) {
      console.warn(`恢复图廊设置失败 (${key}):`, error);
      return defaultValue;
    }
  };

  // 保存设置到localStorage
  const saveToStorage = (key, value) => {
    try {
      localStorage.setItem(key, JSON.stringify(value));
    } catch (error) {
      console.warn(`保存图廊设置失败 (${key}):`, error);
    }
  };

  // ===== 图廊设置状态 =====

  // 瀑布流布局控制 - 从localStorage恢复或使用默认值
  const columnCount = ref(getStoredValue(STORAGE_KEYS.COLUMN_COUNT, "auto"));

  // 分别控制水平和垂直间距 - 从localStorage恢复或使用默认值
  const horizontalGap = ref(getStoredValue(STORAGE_KEYS.HORIZONTAL_GAP, 16));
  const verticalGap = ref(getStoredValue(STORAGE_KEYS.VERTICAL_GAP, 20));

  // 排序方式 - 从localStorage恢复或使用默认值
  const sortBy = ref(getStoredValue(STORAGE_KEYS.SORT_BY, "name"));

  // 工具栏状态管理
  const showSortMenu = ref(false);
  const showViewSettings = ref(false);

  // ===== MasonryWall配置 =====

  // MasonryWall的gap直接使用水平间距（控制列间距）
  const baseGap = computed(() => horizontalGap.value);

  // MasonryWall配置
  const columnWidth = computed(() => {
    // 固定列宽，让MasonryWall根据min-columns和max-columns控制列数
    return 280; // 固定列宽280px
  });

  // 计算最小和最大列数
  const minColumns = computed(() => {
    if (columnCount.value === "auto") {
      return 1; // 自动模式：最少1列
    }
    const cols = parseInt(columnCount.value);
    return cols; // 固定列数模式：最小列数等于设定值
  });

  const maxColumns = computed(() => {
    if (columnCount.value === "auto") {
      return undefined; // 自动模式：无最大列数限制
    }
    const cols = parseInt(columnCount.value);
    return cols; // 固定列数模式：最大列数等于设定值
  });

  // ===== 工具栏选项配置 =====

  const sortOptions = computed(() => [
    { value: "name", label: t("gallery.sortByName") },
    { value: "size", label: t("gallery.sortBySize") },
    { value: "date", label: t("gallery.sortByDate") },
    { value: "type", label: t("gallery.sortByType") },
  ]);

  // ===== 图片数据处理 =====

  // 状态驱动的图片管理 - 移除分页逻辑，实现真正的懒加载
  const imageStates = ref(new Map()); // 每张图片的完整状态
  // 状态结构：{ status: 'idle' | 'loading' | 'loaded' | 'error', url: string | null }

  // 智能分组函数（直接使用后端type字段）
  const createImageGroups = (items) => {
    const allFolders = items.filter((item) => item.isDirectory);
    const allImages = items.filter((item) => !item.isDirectory && item.type === 5); // IMAGE = 5
    const allOtherFiles = items.filter((item) => !item.isDirectory && item.type !== 5 && item.type !== 2); // 非图片非视频

    return { allFolders, allImages, allOtherFiles };
  };

  // 排序函数
  const sortImages = (images) => {
    const sorted = [...images];

    switch (sortBy.value) {
      case "name":
        return sorted.sort((a, b) => a.name.localeCompare(b.name));
      case "size":
        return sorted.sort((a, b) => (b.size || 0) - (a.size || 0));
      case "date":
        return sorted.sort((a, b) => new Date(b.modified || 0) - new Date(a.modified || 0));
      case "type":
        return sorted.sort((a, b) => {
          const extA = a.name.split(".").pop().toLowerCase();
          const extB = b.name.split(".").pop().toLowerCase();
          return extA.localeCompare(extB);
        });
      default:
        return sorted;
    }
  };

  // 可见图片计算 - 移除分页限制，显示所有图片
  const createVisibleImages = (allImages) => {
    return computed(() => {
      return sortImages(allImages);
    });
  };

  // 是否有更多图片 - 懒加载模式下不需要此概念
  const createHasMoreImages = (allImages) => {
    return computed(() => false); // 始终返回false，因为所有图片都会渲染占位符
  };

  // 将图片数据转换为MasonryWall需要的格式
  const createMasonryItems = (visibleImages) => {
    return computed(() => {
      return visibleImages.value.map((image, index) => ({
        id: image.path,
        image: image,
        index: index,
      }));
    });
  };

  // ===== 图片URL管理 =====

  // 🔍 检测图片缓存状态的函数
  const checkImageCacheStatus = async (imageUrl, imageName) => {
    try {
      if ("caches" in window) {
        const galleryCache = await caches.open("gallery-images");
        const cachedResponse = await galleryCache.match(imageUrl);

        if (cachedResponse) {
          console.log(`🎯 ${imageName}: gallery-images 缓存命中`);
        } else {
          console.log(`📡 ${imageName}: 网络请求`);
        }
      }
    } catch (error) {
      console.log(`📡 ${imageName}: 网络请求`);
    }
  };

  // 图片URL获取
  const loadImageUrl = async (image) => {
    const imagePath = image.path;

    // 检查当前状态
    const currentState = imageStates.value.get(imagePath);

    // 如果已经在加载中或已加载完成，直接返回
    if (currentState?.status === "loading" || currentState?.status === "loaded") {
      return;
    }

    // 设置加载状态
    imageStates.value.set(imagePath, { status: "loading", url: null });

    try {
      // 使用统一的API函数
      const getFileInfo = api.fs.getFileInfo;

      // 获取文件信息，包含preview_url字段
      const response = await getFileInfo(imagePath);

      if (response?.success && response.data?.preview_url) {
        // 设置加载完成状态
        imageStates.value.set(imagePath, {
          status: "loaded",
          url: response.data.preview_url,
        });
        console.log(`✅ 懒加载完成: ${image.name}`);

        // 🔍 检测图片是否会走Service Worker缓存
        checkImageCacheStatus(response.data.preview_url, image.name);
      } else {
        // 设置错误状态
        imageStates.value.set(imagePath, { status: "error", url: null });
        console.error(`❌ API响应无效: ${image.name}`, response);
      }
    } catch (error) {
      console.error(`获取图片预览URL失败: ${image.name}`, error);
      // 设置错误状态
      imageStates.value.set(imagePath, { status: "error", url: null });
    }
  };

  // 批量初始化图片状态 - 真正的懒加载：所有图片都初始化为idle状态
  const initializeImageStates = (visibleImages) => {
    visibleImages.forEach((image) => {
      // 所有图片都初始化为idle状态，等待IntersectionObserver触发懒加载
      if (!imageStates.value.has(image.path)) {
        imageStates.value.set(image.path, { status: "idle", url: null });
      }
    });
  };

  // ===== 懒加载管理 =====

  // 检查是否应该显示图片 - 现在所有图片都显示占位符
  const shouldShowImage = (index) => {
    return true; // 所有图片都显示占位符，由IntersectionObserver控制实际加载
  };

  // ===== 设置管理方法 =====

  // 检查是否为默认设置
  const isDefaultSettings = computed(() => {
    return columnCount.value === "auto" && horizontalGap.value === 16 && verticalGap.value === 20 && sortBy.value === "name";
  });

  // 重置图廊设置到默认值
  const resetGallerySettings = () => {
    // 重置到默认值
    columnCount.value = "auto";
    horizontalGap.value = 16;
    verticalGap.value = 20;
    sortBy.value = "name";

    // 清除localStorage中的设置
    Object.values(STORAGE_KEYS).forEach((key) => {
      try {
        localStorage.removeItem(key);
      } catch (error) {
        console.warn(`清除图廊设置失败 (${key}):`, error);
      }
    });

    console.log("图廊设置已重置为默认值");
  };

  // ===== 工具栏交互方法 =====

  const toggleSortMenu = () => {
    showSortMenu.value = !showSortMenu.value;
    if (showSortMenu.value) {
      showViewSettings.value = false;
    }
  };

  const toggleViewSettings = () => {
    showViewSettings.value = !showViewSettings.value;
    if (showViewSettings.value) {
      showSortMenu.value = false;
    }
  };

  const handleSortChange = (sortValue) => {
    sortBy.value = sortValue;
    showSortMenu.value = false;
    console.log(`图廊排序方式变更为: ${sortValue}`);
  };

  // ===== 监听器设置 =====

  // 监听设置变化并自动保存到localStorage
  const setupWatchers = () => {
    watch(columnCount, (newValue) => {
      saveToStorage(STORAGE_KEYS.COLUMN_COUNT, newValue);
      console.log(`图廊列数设置已保存: ${newValue}`);
    });

    watch(horizontalGap, (newValue) => {
      saveToStorage(STORAGE_KEYS.HORIZONTAL_GAP, newValue);
      console.log(`图廊水平间距设置已保存: ${newValue}px`);
    });

    watch(verticalGap, (newValue) => {
      saveToStorage(STORAGE_KEYS.VERTICAL_GAP, newValue);
      console.log(`图廊垂直间距设置已保存: ${newValue}px`);
    });

    watch(sortBy, (newValue) => {
      saveToStorage(STORAGE_KEYS.SORT_BY, newValue);
      console.log(`图廊排序方式设置已保存: ${newValue}`);
    });
  };

  // 返回所有需要的状态和方法
  return {
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
    sortImages,
    createVisibleImages,
    createHasMoreImages,
    createMasonryItems,

    // 图片URL管理
    loadImageUrl,
    initializeImageStates,

    // 懒加载管理
    shouldShowImage,

    // 设置管理
    isDefaultSettings,
    resetGallerySettings,

    // 工具栏交互
    toggleSortMenu,
    toggleViewSettings,
    handleSortChange,

    // 初始化方法
    setupWatchers,
  };
}
