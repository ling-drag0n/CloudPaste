/**
 * 选择状态管理组合式函数
 * 管理文件列表的选择状态和批量操作
 */

import { ref, computed } from "vue";
import { formatFileSize } from "@/utils/fileUtils.js";

export function useSelection() {
  // ===== 状态管理 =====
  const isCheckboxMode = ref(false);
  const selectedItems = ref([]);
  const availableItems = ref([]);

  // ===== 计算属性 =====
  const selectedCount = computed(() => selectedItems.value.length);

  const hasSelection = computed(() => selectedCount.value > 0);

  const isAllSelected = computed(() => {
    return selectedItems.value.length > 0 && selectedItems.value.length === availableItems.value.length;
  });

  const isPartiallySelected = computed(() => {
    return selectedItems.value.length > 0 && selectedItems.value.length < availableItems.value.length;
  });

  // ===== 项目管理方法 =====

  /**
   * 设置可选择的项目列表
   * @param {Array} items - 项目列表
   */
  const setAvailableItems = (items) => {
    availableItems.value = items || [];
    // 清理已选择但不在当前列表中的项目
    selectedItems.value = selectedItems.value.filter((selectedItem) =>
      items.some((item) => item.path === selectedItem.path)
    );
  };

  /**
   * 获取可选择的项目列表
   * @returns {Array} 可选择的项目列表
   */
  const getAvailableItems = () => {
    return [...availableItems.value];
  };

  // ===== 选择模式管理 =====

  /**
   * 切换选择模式
   * @param {boolean} enabled - 是否启用选择模式
   */
  const toggleCheckboxMode = (enabled = null) => {
    if (enabled === null) {
      isCheckboxMode.value = !isCheckboxMode.value;
    } else {
      isCheckboxMode.value = enabled;
    }

    // 退出选择模式时清空选择
    if (!isCheckboxMode.value) {
      clearSelection();
    }
  };

  /**
   * 启用选择模式
   */
  const enableCheckboxMode = () => {
    toggleCheckboxMode(true);
  };

  /**
   * 禁用选择模式
   */
  const disableCheckboxMode = () => {
    toggleCheckboxMode(false);
  };

  // ===== 单项选择方法 =====

  /**
   * 选择或取消选择项目
   * @param {Object} item - 项目对象
   * @param {boolean} selected - 是否选择
   */
  const selectItem = (item, selected) => {
    if (!item || !item.path) return;

    const index = selectedItems.value.findIndex((selectedItem) => selectedItem.path === item.path);

    if (selected && index === -1) {
      // 添加到选择列表
      selectedItems.value.push(item);
    } else if (!selected && index !== -1) {
      // 从选择列表移除
      selectedItems.value.splice(index, 1);
    }
  };

  /**
   * 切换项目选择状态
   * @param {Object} item - 项目对象
   */
  const toggleItemSelection = (item) => {
    const isSelected = isItemSelected(item);
    selectItem(item, !isSelected);
  };

  /**
   * 检查项目是否被选择
   * @param {Object} item - 项目对象
   * @returns {boolean} 是否被选择
   */
  const isItemSelected = (item) => {
    if (!item || !item.path) return false;

    return selectedItems.value.some((selectedItem) => selectedItem.path === item.path);
  };

  /**
   * 添加项目到选择列表（如果未选择）
   * @param {Object} item - 项目对象
   */
  const addToSelection = (item) => {
    if (!isItemSelected(item)) {
      selectItem(item, true);
    }
  };

  /**
   * 从选择列表移除项目
   * @param {Object} item - 项目对象
   */
  const removeFromSelection = (item) => {
    if (isItemSelected(item)) {
      selectItem(item, false);
    }
  };

  // ===== 批量选择方法 =====

  /**
   * 全选或取消全选
   * @param {boolean} selectAll - 是否全选
   */
  const toggleSelectAll = (selectAll = null) => {
    if (selectAll === null) {
      selectAll = !isAllSelected.value;
    }

    if (selectAll) {
      // 全选：添加所有未选择的项目
      availableItems.value.forEach((item) => {
        if (!isItemSelected(item)) {
          selectedItems.value.push(item);
        }
      });
    } else {
      // 取消全选：清空选择
      clearSelection();
    }
  };

  /**
   * 全选所有项目
   */
  const selectAll = () => {
    toggleSelectAll(true);
  };

  /**
   * 取消全选
   */
  const deselectAll = () => {
    toggleSelectAll(false);
  };

  /**
   * 反选所有项目
   */
  const invertSelection = () => {
    const newSelection = availableItems.value.filter((item) => !isItemSelected(item));
    selectedItems.value = newSelection;
  };

  /**
   * 选择指定范围的项目
   * @param {number} startIndex - 开始索引
   * @param {number} endIndex - 结束索引
   */
  const selectRange = (startIndex, endIndex) => {
    const start = Math.min(startIndex, endIndex);
    const end = Math.max(startIndex, endIndex);

    for (let i = start; i <= end && i < availableItems.value.length; i++) {
      const item = availableItems.value[i];
      if (!isItemSelected(item)) {
        selectedItems.value.push(item);
      }
    }
  };

  // ===== 清理方法 =====

  /**
   * 清空所有选择
   */
  const clearSelection = () => {
    selectedItems.value = [];
  };

  /**
   * 重置选择状态
   */
  const resetSelection = () => {
    isCheckboxMode.value = false;
    selectedItems.value = [];
    availableItems.value = [];
  };

  // ===== 获取选择信息方法 =====

  /**
   * 获取选择的项目列表
   * @returns {Array} 选择的项目列表
   */
  const getSelectedItems = () => {
    return [...selectedItems.value];
  };

  /**
   * 获取选择的文件列表（排除目录）
   * @returns {Array} 选择的文件列表
   */
  const getSelectedFiles = () => {
    return selectedItems.value.filter((item) => !item.isDirectory);
  };

  /**
   * 获取选择的目录列表
   * @returns {Array} 选择的目录列表
   */
  const getSelectedDirectories = () => {
    return selectedItems.value.filter((item) => item.isDirectory);
  };

  /**
   * 检查选择中是否包含目录
   * @returns {boolean} 是否包含目录
   */
  const hasSelectedDirectories = () => {
    return selectedItems.value.some((item) => item.isDirectory);
  };

  /**
   * 检查选择中是否包含文件
   * @returns {boolean} 是否包含文件
   */
  const hasSelectedFiles = () => {
    return selectedItems.value.some((item) => !item.isDirectory);
  };

  /**
   * 获取选择项目的总大小
   * @returns {number} 总大小（字节）
   */
  const getSelectedTotalSize = () => {
    return selectedItems.value.reduce((total, item) => {
      return total + (item.size || 0);
    }, 0);
  };

  /**
   * 格式化选择项目的总大小
   * @returns {string} 格式化后的大小
   */
  const getSelectedTotalSizeFormatted = () => {
    const totalSize = getSelectedTotalSize();
    return formatFileSize(totalSize);
  };

  /**
   * 获取选择摘要信息
   * @returns {Object} 选择摘要
   */
  const getSelectionSummary = () => {
    const files = getSelectedFiles();
    const directories = getSelectedDirectories();

    return {
      total: selectedCount.value,
      files: files.length,
      directories: directories.length,
      totalSize: getSelectedTotalSize(),
      totalSizeFormatted: getSelectedTotalSizeFormatted(),
      hasFiles: files.length > 0,
      hasDirectories: directories.length > 0,
      isAllFiles: files.length === selectedCount.value,
      isAllDirectories: directories.length === selectedCount.value,
    };
  };

  /**
   * 获取选择的路径列表
   * @returns {Array} 路径列表
   */
  const getSelectedPaths = () => {
    return selectedItems.value.map((item) => item.path);
  };

  /**
   * 获取选择的名称列表
   * @returns {Array} 名称列表
   */
  const getSelectedNames = () => {
    return selectedItems.value.map((item) => item.name);
  };

  // ===== 查询方法 =====

  /**
   * 根据路径查找选择的项目
   * @param {string} path - 文件路径
   * @returns {Object|null} 找到的项目或null
   */
  const findSelectedItemByPath = (path) => {
    return selectedItems.value.find((item) => item.path === path) || null;
  };

  /**
   * 检查是否选择了指定路径的项目
   * @param {string} path - 文件路径
   * @returns {boolean} 是否选择
   */
  const isPathSelected = (path) => {
    return selectedItems.value.some((item) => item.path === path);
  };

  /**
   * 获取选择项目的索引列表
   * @returns {Array} 索引列表
   */
  const getSelectedIndices = () => {
    return selectedItems.value
      .map((selectedItem) => {
        return availableItems.value.findIndex((item) => item.path === selectedItem.path);
      })
      .filter((index) => index !== -1);
  };

  return {
    // 状态 - 直接返回ref，让Vue在模板中自动解包
    isCheckboxMode,
    selectedItems,

    // 计算属性
    selectedCount,
    hasSelection,
    isAllSelected,
    isPartiallySelected,

    // 项目管理方法
    setAvailableItems,
    getAvailableItems,

    // 选择模式管理
    toggleCheckboxMode,
    enableCheckboxMode,
    disableCheckboxMode,

    // 单项选择方法
    selectItem,
    toggleItemSelection,
    isItemSelected,
    addToSelection,
    removeFromSelection,

    // 批量选择方法
    toggleSelectAll,
    selectAll,
    deselectAll,
    invertSelection,
    selectRange,

    // 清理方法
    clearSelection,
    resetSelection,

    // 获取选择信息方法
    getSelectedItems,
    getSelectedFiles,
    getSelectedDirectories,
    hasSelectedDirectories,
    hasSelectedFiles,
    getSelectedTotalSize,
    getSelectedTotalSizeFormatted,
    getSelectionSummary,
    getSelectedPaths,
    getSelectedNames,

    // 查询方法
    findSelectedItemByPath,
    isPathSelected,
    getSelectedIndices,
  };
}
