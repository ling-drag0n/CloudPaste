<template>
  <div class="w-full">
    <!-- 桌面端表格 -->
    <div class="hidden md:block flex-1 overflow-auto">
      <table class="min-w-full divide-y divide-gray-200 dark:divide-gray-700 table-fixed">
        <thead class="bg-gray-50 dark:bg-gray-700">
          <tr v-for="headerGroup in table.getHeaderGroups()" :key="headerGroup.id">
            <th
              v-for="header in headerGroup.headers"
              :key="header.id"
              scope="col"
              class="px-3 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-300 uppercase tracking-wider"
              :class="props.columnClasses[header.column.id]"
            >
              <div v-if="!header.isPlaceholder" :class="getSortHeaderClass(header.column)" @click="handleSort(header.column)">
                <FlexRender :render="header.column.columnDef.header" :props="header.getContext()" />
                <!-- 排序指示器 -->
                <span v-if="header.column.getCanSort()" class="ml-1 text-xs">
                  <span v-if="header.column.getIsSorted() === 'asc'" class="text-blue-600 dark:text-blue-400">↑</span>
                  <span v-else-if="header.column.getIsSorted() === 'desc'" class="text-blue-600 dark:text-blue-400">↓</span>
                  <span v-else class="text-gray-400 opacity-50">↕</span>
                </span>
              </div>
            </th>
          </tr>
        </thead>
        <tbody class="bg-white dark:bg-gray-800 divide-y divide-gray-200 dark:divide-gray-700">
          <tr v-if="props.data.length === 0">
            <td class="px-3 py-4 text-sm text-gray-500 dark:text-gray-400 text-center" :colspan="table.getAllColumns().length">
              {{ props.emptyText }}
            </td>
          </tr>
          <tr v-for="row in table.getRowModel().rows" :key="row.id" class="hover:bg-gray-50 dark:hover:bg-gray-700">
            <td
              v-for="cell in row.getVisibleCells()"
              :key="cell.id"
              class="px-3 py-4 whitespace-nowrap text-sm text-gray-900 dark:text-gray-100"
              :class="props.columnClasses[cell.column.id]"
            >
              <FlexRender :render="cell.column.columnDef.cell" :props="cell.getContext()" />
            </td>
          </tr>
        </tbody>
      </table>
    </div>

    <!-- 移动端插槽 -->
    <div class="md:hidden flex-1 overflow-auto">
      <div v-if="props.data.length === 0" class="text-center py-8 text-gray-500 dark:text-gray-400">
        {{ props.emptyText }}
      </div>
      <slot name="mobile" :data="props.data" />
    </div>
  </div>
</template>

<script setup>
import { computed, h } from "vue";
import { useVueTable, getCoreRowModel, getSortedRowModel, FlexRender, createColumnHelper } from "@tanstack/vue-table";

const props = defineProps({
  data: {
    type: Array,
    required: true,
  },
  columns: {
    type: Array,
    required: true,
  },
  columnClasses: {
    type: Object,
    default: () => ({}),
  },
  emptyText: {
    type: String,
    default: "暂无数据",
  },
  manualSorting: {
    type: Boolean,
    default: false,
  },
  sorting: {
    type: Array,
    default: () => [],
  },
  // 新增：选择相关配置
  selectable: {
    type: Boolean,
    default: false,
  },
  selectedItems: {
    type: Array,
    default: () => [],
  },
  // 新增：行标识字段
  rowIdField: {
    type: String,
    default: "id",
  },
});

const emit = defineEmits(["sort-change", "selection-change"]);

// 创建列定义助手
const columnHelper = createColumnHelper();

// 将简化配置转换为TanStack列定义
const tanstackColumns = computed(() => {
  const columns = [];

  // 如果启用选择，添加选择列
  if (props.selectable) {
    columns.push(
      columnHelper.display({
        id: "select",
        header: () =>
          h("input", {
            type: "checkbox",
            checked: props.data.length > 0 && (props.selectedItems || []).length === props.data.length,
            onClick: () => emit("selection-change", { type: "toggle-all" }),
            class: "h-4 w-4 text-primary-600 focus:ring-primary-500 border-gray-300 rounded cursor-pointer",
          }),
        cell: ({ row }) =>
          h("input", {
            type: "checkbox",
            checked: (props.selectedItems || []).includes(row.original[props.rowIdField]),
            onClick: () =>
              emit("selection-change", {
                type: "toggle-item",
                id: row.original[props.rowIdField],
                item: row.original,
              }),
            class: "h-4 w-4 text-primary-600 focus:ring-primary-500 border-gray-300 rounded cursor-pointer",
          }),
        enableSorting: false,
      })
    );
  }

  // 处理业务列
  props.columns.forEach((col) => {
    if (col.type === "accessor") {
      // 简单字段列
      const columnDef = {
        id: col.key,
        header: col.header,
        cell: ({ getValue, row }) => {
          if (col.render) {
            return col.render(getValue(), row.original);
          }
          return getValue();
        },
        enableSorting: col.sortable !== false,
      };

      // 添加自定义排序函数支持
      if (col.sortingFn) {
        columnDef.sortingFn = col.sortingFn;
      }

      columns.push(columnHelper.accessor(col.key, columnDef));
    } else if (col.type === "display") {
      // 自定义显示列
      const columnDef = {
        id: col.key,
        header: col.header,
        cell: ({ row }) => {
          if (col.render) {
            return col.render(row.original);
          }
          return "";
        },
        enableSorting: col.sortable === true,
      };

      // 添加自定义排序函数支持
      if (col.sortingFn) {
        columnDef.sortingFn = col.sortingFn;
      }

      columns.push(columnHelper.display(columnDef));
    }
  });

  return columns;
});

// 创建TanStack表格实例
const table = useVueTable({
  get data() {
    return props.data;
  },
  get columns() {
    return tanstackColumns.value;
  },
  getCoreRowModel: getCoreRowModel(),
  getSortedRowModel: getSortedRowModel(),
  // 移除客户端过滤，因为我们使用服务端搜索
  manualSorting: props.manualSorting,
  manualFiltering: true, // 明确声明使用手动过滤（服务端搜索）
  ...(props.manualSorting && {
    state: {
      get sorting() {
        return props.sorting || [];
      },
    },
    onSortingChange: (updater) => {
      const newSorting = typeof updater === "function" ? updater(props.sorting || []) : updater;
      emit("sort-change", newSorting);
    },
  }),
  enableSorting: true,
  enableMultiSort: true,
});

// 工具函数
const getSortHeaderClass = (column) => {
  return column.getCanSort() ? "cursor-pointer select-none flex items-center hover:text-gray-700 dark:hover:text-gray-200 transition-colors" : "flex items-center";
};

const handleSort = (column) => {
  if (column.getCanSort()) {
    column.toggleSorting();
  }
};
</script>
