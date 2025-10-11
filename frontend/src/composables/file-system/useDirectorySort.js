/**
 * 目录排序管理 Composable
 * 处理文件列表的排序逻辑和状态管理，支持持久化
 */

import { ref, computed } from "vue";

export function useDirectorySort() {
  // ===== 排序状态 =====
  const sortField = ref("name"); // 当前排序字段：'name', 'size', 'modified'
  const sortOrder = ref("default"); // 当前排序顺序：'asc', 'desc', 'default'

  // 排序字段的状态循环：default -> asc -> desc -> default
  const sortStates = ["default", "asc", "desc"];

  // ===== 计算属性 =====

  /**
   * 当前排序状态的描述
   */
  const sortDescription = computed(() => {
    if (sortOrder.value === "default") {
      return "默认排序（目录优先，按名称）";
    }
    
    const fieldNames = {
      name: "名称",
      size: "大小", 
      modified: "修改时间"
    };
    
    const orderNames = {
      asc: "升序",
      desc: "降序"
    };
    
    return `按${fieldNames[sortField.value]}${orderNames[sortOrder.value]}`;
  });

  /**
   * 是否为默认排序
   */
  const isDefaultSort = computed(() => sortOrder.value === "default");

  /**
   * 是否为自定义排序
   */
  const isCustomSort = computed(() => !isDefaultSort.value);

  // ===== 持久化方法 =====

  /**
   * 初始化排序状态（从localStorage恢复）
   */
  const initializeSortState = () => {
    try {
      const savedSortField = localStorage.getItem("file_explorer_sort_field");
      const savedSortOrder = localStorage.getItem("file_explorer_sort_order");

      if (savedSortField && ["name", "size", "modified"].includes(savedSortField)) {
        sortField.value = savedSortField;
      }

      if (savedSortOrder && sortStates.includes(savedSortOrder)) {
        sortOrder.value = savedSortOrder;
      }

      console.log("排序状态已恢复:", {
        field: sortField.value,
        order: sortOrder.value,
      });
    } catch (error) {
      console.warn("恢复排序状态失败:", error);
    }
  };

  /**
   * 保存排序状态到localStorage
   */
  const saveSortState = () => {
    try {
      localStorage.setItem("file_explorer_sort_field", sortField.value);
      localStorage.setItem("file_explorer_sort_order", sortOrder.value);
      
      console.log("排序状态已保存:", {
        field: sortField.value,
        order: sortOrder.value,
      });
    } catch (error) {
      console.warn("保存排序状态失败:", error);
    }
  };

  /**
   * 重置排序状态为默认值
   */
  const resetSortState = () => {
    sortField.value = "name";
    sortOrder.value = "default";
    saveSortState();
  };

  // ===== 排序控制方法 =====

  /**
   * 点击表头排序
   * @param {string} field - 排序字段
   */
  const handleSort = (field) => {
    if (!["name", "size", "modified"].includes(field)) {
      console.warn("无效的排序字段:", field);
      return;
    }

    if (sortField.value === field) {
      // 同一字段，切换排序状态
      const currentIndex = sortStates.indexOf(sortOrder.value);
      const nextIndex = (currentIndex + 1) % sortStates.length;
      sortOrder.value = sortStates[nextIndex];
    } else {
      // 不同字段，重置为升序
      sortField.value = field;
      sortOrder.value = "asc";
    }

    // 保存排序状态
    saveSortState();
  };

  /**
   * 设置特定的排序状态
   * @param {string} field - 排序字段
   * @param {string} order - 排序顺序
   */
  const setSortState = (field, order) => {
    if (!["name", "size", "modified"].includes(field)) {
      console.warn("无效的排序字段:", field);
      return;
    }

    if (!sortStates.includes(order)) {
      console.warn("无效的排序顺序:", order);
      return;
    }

    sortField.value = field;
    sortOrder.value = order;
    saveSortState();
  };

  // ===== UI辅助方法 =====

  /**
   * 获取排序图标
   * @param {string} field - 字段名
   * @returns {string} 排序图标
   */
  const getSortIcon = (field) => {
    if (sortField.value !== field || sortOrder.value === "default") {
      return ""; // 默认状态不显示图标
    }
    return sortOrder.value === "asc" ? "↑" : "↓";
  };

  /**
   * 获取排序图标类名（用于CSS图标）
   * @param {string} field - 字段名
   * @returns {string} CSS类名
   */
  const getSortIconClass = (field) => {
    if (sortField.value !== field || sortOrder.value === "default") {
      return "";
    }
    return sortOrder.value === "asc" ? "sort-asc" : "sort-desc";
  };

  /**
   * 检查字段是否为当前排序字段
   * @param {string} field - 字段名
   * @returns {boolean} 是否为当前排序字段
   */
  const isCurrentSortField = (field) => {
    return sortField.value === field && sortOrder.value !== "default";
  };

  /**
   * 获取字段的排序状态
   * @param {string} field - 字段名
   * @returns {Object} 排序状态信息
   */
  const getFieldSortState = (field) => {
    return {
      isActive: sortField.value === field && sortOrder.value !== "default",
      order: sortField.value === field ? sortOrder.value : "default",
      icon: getSortIcon(field),
      iconClass: getSortIconClass(field),
    };
  };

  // ===== 排序算法 =====

  /**
   * 对项目列表进行排序
   * @param {Array} items - 原始项目列表
   * @returns {Array} 排序后的项目列表
   */
  const sortItems = (items) => {
    if (!items || !Array.isArray(items)) {
      return [];
    }

    let sortedItems = [...items];

    // 如果是默认排序，按名称排序
    if (sortOrder.value === "default") {
      return sortedItems.sort((a, b) => {
        // 先按类型排序（目录在前，文件在后）
        if (a.isDirectory && !b.isDirectory) return -1;
        if (!a.isDirectory && b.isDirectory) return 1;
        // 再按名称字母排序
        return a.name.localeCompare(b.name, undefined, { numeric: true, sensitivity: "base" });
      });
    }

    // 自定义排序
    return sortedItems.sort((a, b) => {
      // 文件夹始终优先（除非两个都是文件夹或都是文件）
      if (a.isDirectory && !b.isDirectory) return -1;
      if (!a.isDirectory && b.isDirectory) return 1;

      let comparison = 0;

      switch (sortField.value) {
        case "name":
          comparison = a.name.localeCompare(b.name, undefined, { 
            numeric: true, 
            sensitivity: "base" 
          });
          break;
        case "size":
          // 虚拟目录的大小视为0
          const aSize = a.isDirectory && a.isVirtual ? 0 : a.size || 0;
          const bSize = b.isDirectory && b.isVirtual ? 0 : b.size || 0;
          comparison = aSize - bSize;
          break;
        case "modified":
          // 虚拟目录没有修改时间，视为最早
          const aTime = a.isDirectory && a.isVirtual ? 0 : new Date(a.modified || 0).getTime();
          const bTime = b.isDirectory && b.isVirtual ? 0 : new Date(b.modified || 0).getTime();
          comparison = aTime - bTime;
          break;
        default:
          comparison = a.name.localeCompare(b.name, undefined, { 
            numeric: true, 
            sensitivity: "base" 
          });
      }

      return sortOrder.value === "asc" ? comparison : -comparison;
    });
  };

  /**
   * 创建排序后的计算属性
   * @param {Ref} itemsRef - 项目列表的响应式引用
   * @returns {ComputedRef} 排序后的项目列表
   */
  const createSortedItems = (itemsRef) => {
    return computed(() => sortItems(itemsRef.value));
  };

  // ===== 批量操作方法 =====

  /**
   * 对多个项目列表应用相同的排序
   * @param {Array} itemsArrays - 多个项目列表的数组
   * @returns {Array} 排序后的项目列表数组
   */
  const sortMultipleItemArrays = (itemsArrays) => {
    return itemsArrays.map(items => sortItems(items));
  };

  /**
   * 获取排序配置对象（用于API调用或其他用途）
   * @returns {Object} 排序配置
   */
  const getSortConfig = () => {
    return {
      field: sortField.value,
      order: sortOrder.value,
      isDefault: isDefaultSort.value,
      description: sortDescription.value,
    };
  };

  return {
    // 状态
    sortField,
    sortOrder,

    // 计算属性
    sortDescription,
    isDefaultSort,
    isCustomSort,

    // 持久化方法
    initializeSortState,
    saveSortState,
    resetSortState,

    // 排序控制方法
    handleSort,
    setSortState,

    // UI辅助方法
    getSortIcon,
    getSortIconClass,
    isCurrentSortField,
    getFieldSortState,

    // 排序算法
    sortItems,
    createSortedItems,
    sortMultipleItemArrays,

    // 工具方法
    getSortConfig,
  };
}
