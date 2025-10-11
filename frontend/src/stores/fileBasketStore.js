/**
 * 文件篮 Pinia Store
 * 管理跨目录文件收集和批量操作的全局状态
 */

import { defineStore } from "pinia";
import { ref, computed, nextTick } from "vue";

export const useFileBasketStore = defineStore("fileBasket", () => {
  // ===== 状态管理 =====

  // 收集的文件列表 - 确保初始化为空数组
  const collectedFiles = ref([]);

  // 文件篮面板显示状态
  const isBasketOpen = ref(false);

  // 最后收集时间
  const lastCollectionTime = ref(null);

  // Store初始化状态 - 默认为true，避免初始化时序问题
  const isInitialized = ref(true);

  // 自动清理配置
  const AUTO_CLEANUP_HOURS = 24; // 24小时后自动清理

  // localStorage键名
  const STORAGE_KEY = "cloudpaste_file_basket";

  // ===== 计算属性 =====

  /**
   * 收集的文件数量
   */
  const collectionCount = computed(() => {
    // 确保Store已初始化且collectedFiles是有效数组
    if (!isInitialized.value || !Array.isArray(collectedFiles.value)) {
      return 0;
    }
    return collectedFiles.value.length;
  });

  /**
   * 是否有收集的文件
   */
  const hasCollection = computed(() => {
    return isInitialized.value && collectionCount.value > 0;
  });

  /**
   * 收集文件的总大小（字节）
   */
  const collectionTotalSize = computed(() => {
    if (!isInitialized.value || !Array.isArray(collectedFiles.value)) {
      return 0;
    }
    return collectedFiles.value.reduce((sum, file) => sum + (file.size || 0), 0);
  });

  /**
   * 收集文件的总大小（MB）
   */
  const collectionTotalSizeMB = computed(() => {
    return Math.round((collectionTotalSize.value / (1024 * 1024)) * 100) / 100;
  });

  /**
   * 按目录分组的文件
   */
  const filesByDirectory = computed(() => {
    const groups = {};
    if (!isInitialized.value || !Array.isArray(collectedFiles.value)) {
      return groups;
    }

    collectedFiles.value.forEach((file) => {
      // 确保file对象存在且有必要的属性
      if (!file || typeof file !== "object" || !file.path) {
        console.warn("发现无效的文件对象，跳过:", file);
        return;
      }

      const dir = file.sourceDirectory || "/";
      if (!groups[dir]) {
        groups[dir] = [];
      }
      groups[dir].push(file);
    });
    return groups;
  });

  /**
   * 收集的目录数量
   */
  const directoryCount = computed(() => {
    if (!isInitialized.value) {
      return 0;
    }
    return Object.keys(filesByDirectory.value).length;
  });

  // ===== 文件收集方法 =====

  /**
   * 添加文件到收集篮
   * @param {Object|Array} files - 文件或文件数组
   * @param {string} currentPath - 当前目录路径
   */
  const addToBasket = (files, currentPath) => {
    // 确保collectedFiles是数组
    if (!Array.isArray(collectedFiles.value)) {
      collectedFiles.value = [];
    }

    const fileArray = Array.isArray(files) ? files : [files];

    fileArray.forEach((file) => {
      // 验证文件对象的有效性
      if (!file || typeof file !== "object" || !file.path || !file.name) {
        console.warn("尝试添加无效的文件对象，跳过:", file);
        return;
      }

      // 检查文件是否已经在收集篮中（基于完整路径）
      const existingIndex = collectedFiles.value.findIndex((collected) => collected.path === file.path);

      if (existingIndex === -1) {
        // 添加新文件，包含来源目录信息
        const fileToAdd = {
          ...file,
          sourceDirectory: currentPath,
          collectedAt: new Date().toISOString(),
          uniqueId: `${currentPath}::${file.path}`, // 唯一标识
        };

        // 确保uniqueId存在
        if (!fileToAdd.uniqueId) {
          fileToAdd.uniqueId = `${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
        }

        collectedFiles.value.push(fileToAdd);
      }
    });

    lastCollectionTime.value = new Date().toISOString();
    saveToStorage();
  };

  /**
   * 从收集篮移除文件
   * @param {string|Array} filePaths - 文件路径或路径数组
   */
  const removeFromBasket = (filePaths) => {
    if (!Array.isArray(collectedFiles.value)) return;

    const pathArray = Array.isArray(filePaths) ? filePaths : [filePaths];

    pathArray.forEach((path) => {
      const index = collectedFiles.value.findIndex((file) => file.path === path);
      if (index !== -1) {
        collectedFiles.value.splice(index, 1);
      }
    });

    saveToStorage();
  };

  /**
   * 检查文件是否已在收集篮中
   * @param {string} filePath - 文件路径
   * @returns {boolean}
   */
  const isFileInBasket = (filePath) => {
    if (!isInitialized.value || !Array.isArray(collectedFiles.value)) {
      return false;
    }
    return collectedFiles.value.some((file) => file.path === filePath);
  };

  /**
   * 切换文件的收集状态
   * @param {Object} file - 文件对象
   * @param {string} currentPath - 当前目录路径
   */
  const toggleFileInBasket = (file, currentPath) => {
    if (isFileInBasket(file.path)) {
      removeFromBasket(file.path);
    } else {
      addToBasket(file, currentPath);
    }
  };

  /**
   * 批量添加当前目录的选中文件
   * @param {Array} selectedFiles - 选中的文件列表
   * @param {string} currentPath - 当前目录路径
   * @returns {number} 添加的文件数量
   */
  const addSelectedToBasket = (selectedFiles, currentPath) => {
    // 只添加文件，排除文件夹
    const fileItems = selectedFiles.filter((item) => !item.isDirectory);
    if (fileItems.length > 0) {
      addToBasket(fileItems, currentPath);
    }
    return fileItems.length;
  };

  // ===== 面板管理 =====

  /**
   * 打开文件篮面板
   */
  const openBasket = () => {
    isBasketOpen.value = true;
  };

  /**
   * 关闭文件篮面板
   */
  const closeBasket = () => {
    isBasketOpen.value = false;
  };

  /**
   * 切换文件篮面板显示状态
   */
  const toggleBasket = () => {
    isBasketOpen.value = !isBasketOpen.value;
  };

  // ===== 清理方法 =====

  /**
   * 清空收集篮
   */
  const clearBasket = () => {
    collectedFiles.value = [];
    lastCollectionTime.value = null;
    saveToStorage();
  };

  /**
   * 清空收集篮并关闭面板
   */
  const resetBasket = () => {
    clearBasket();
    closeBasket();
  };

  // ===== 持久化方法 =====

  /**
   * 保存到localStorage
   */
  const saveToStorage = () => {
    try {
      const data = {
        collectedFiles: collectedFiles.value,
        lastCollectionTime: lastCollectionTime.value,
        savedAt: new Date().toISOString(),
      };
      localStorage.setItem(STORAGE_KEY, JSON.stringify(data));
    } catch (error) {
      console.error("保存文件篮状态失败:", error);
    }
  };

  /**
   * 从localStorage加载
   */
  const loadFromStorage = () => {
    try {
      const stored = localStorage.getItem(STORAGE_KEY);
      if (stored) {
        const data = JSON.parse(stored);

        // 检查数据是否过期（超过24小时）
        if (data.savedAt) {
          const savedTime = new Date(data.savedAt);
          const now = new Date();
          const hoursDiff = (now - savedTime) / (1000 * 60 * 60);

          if (hoursDiff > AUTO_CLEANUP_HOURS) {
            console.log("文件篮数据已过期，自动清理");
            clearBasket();
            return;
          }
        }

        // 严格验证和清理数据
        if (Array.isArray(data.collectedFiles)) {
          const validFiles = [];

          data.collectedFiles.forEach((file) => {
            // 严格验证文件对象的完整性
            if (!file || typeof file !== "object" || !file.path || !file.name || typeof file.path !== "string" || typeof file.name !== "string") {
              console.warn("从localStorage加载时发现无效文件对象，已过滤:", file);
              return;
            }

            // 验证必需的数值字段
            if (typeof file.size !== "number" || file.size < 0) {
              file.size = 0;
            }

            // 确保uniqueId存在且有效
            if (!file.uniqueId || typeof file.uniqueId !== "string") {
              file.uniqueId = `${file.sourceDirectory || "unknown"}::${file.path}`;
            }

            // 确保sourceDirectory存在
            if (!file.sourceDirectory || typeof file.sourceDirectory !== "string") {
              file.sourceDirectory = "/";
            }

            // 确保collectedAt存在
            if (!file.collectedAt) {
              file.collectedAt = new Date().toISOString();
            }

            validFiles.push(file);
          });

          collectedFiles.value = validFiles;
        } else {
          collectedFiles.value = [];
        }
        lastCollectionTime.value = data.lastCollectionTime || null;
      } else {
        // 没有存储数据时，确保初始化为空数组
        collectedFiles.value = [];
        lastCollectionTime.value = null;
      }
    } catch (error) {
      console.error("加载文件篮状态失败，清理localStorage:", error);
      // 清理损坏的localStorage数据
      try {
        localStorage.removeItem(STORAGE_KEY);
      } catch (e) {
        console.error("清理localStorage失败:", e);
      }
      collectedFiles.value = [];
      lastCollectionTime.value = null;
    }
  };

  // ===== 获取信息方法 =====

  /**
   * 获取收集摘要
   */
  const getCollectionSummary = () => {
    return {
      totalFiles: collectionCount.value,
      totalDirectories: directoryCount.value,
      totalSize: collectionTotalSize.value,
      totalSizeMB: collectionTotalSizeMB.value,
      filesByDirectory: filesByDirectory.value,
      lastCollectionTime: lastCollectionTime.value,
    };
  };

  /**
   * 获取所有收集的文件
   */
  const getCollectedFiles = () => {
    if (!isInitialized.value || !Array.isArray(collectedFiles.value)) {
      return [];
    }
    return [...collectedFiles.value];
  };

  /**
   * 强制清理localStorage中的文件篮数据
   */
  const forceCleanStorage = () => {
    try {
      localStorage.removeItem(STORAGE_KEY);
      console.log("已强制清理文件篮localStorage数据");
    } catch (error) {
      console.error("强制清理localStorage失败:", error);
    }
  };

  // 检查是否需要强制清理（开发时临时使用）
  const shouldForceClean = localStorage.getItem(STORAGE_KEY + "_force_clean");
  if (shouldForceClean) {
    forceCleanStorage();
    localStorage.removeItem(STORAGE_KEY + "_force_clean");
  }

  // 立即初始化，但使用同步方式确保数据完整性
  loadFromStorage();

  return {
    // 状态
    collectedFiles,
    isBasketOpen,
    lastCollectionTime,
    isInitialized,

    // 计算属性
    collectionCount,
    hasCollection,
    collectionTotalSize,
    collectionTotalSizeMB,
    filesByDirectory,
    directoryCount,

    // 文件收集方法
    addToBasket,
    removeFromBasket,
    isFileInBasket,
    toggleFileInBasket,
    addSelectedToBasket,

    // 面板管理
    openBasket,
    closeBasket,
    toggleBasket,

    // 清理方法
    clearBasket,
    resetBasket,

    // 获取信息方法
    getCollectionSummary,
    getCollectedFiles,

    // 持久化方法
    saveToStorage,
    loadFromStorage,
  };
});
