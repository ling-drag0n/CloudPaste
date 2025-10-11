<script setup>
import { computed } from "vue";
import { useI18n } from "vue-i18n";

const { t } = useI18n();

const props = defineProps({
  darkMode: {
    type: Boolean,
    required: true,
  },
  pagination: {
    type: Object,
    required: true,
    // 支持两种分页模式：
    // 1. page模式：{ page, limit, total, totalPages }
    // 2. offset模式：{ offset, limit, total, hasMore }
  },
  mode: {
    type: String,
    default: "page", // 'page' 或 'offset'
    validator: (value) => ["page", "offset"].includes(value),
  },
  showPageSizeSelector: {
    type: Boolean,
    default: true,
  },
  pageSizeOptions: {
    type: Array,
    default: () => [10, 20, 30, 50, 100],
  },
  searchMode: {
    type: Boolean,
    default: false,
  },
  searchTerm: {
    type: String,
    default: "",
  },
});

const emit = defineEmits(["page-changed", "offset-changed", "limit-changed"]);

// 计算当前显示范围
const displayRange = computed(() => {
  if (props.mode === "page") {
    const { page, limit, total } = props.pagination;
    const start = (page - 1) * limit + 1;
    const end = Math.min(page * limit, total);
    return { start, end };
  } else {
    const { offset, limit, total } = props.pagination;
    const start = offset + 1;
    const end = Math.min(offset + limit, total);
    return { start, end };
  }
});

// 计算当前页码（适用于offset模式）
const currentPage = computed(() => {
  if (props.mode === "offset") {
    return Math.floor(props.pagination.offset / props.pagination.limit) + 1;
  }
  return props.pagination.page;
});

// 计算总页数（适用于offset模式）
const totalPages = computed(() => {
  if (props.mode === "page") {
    return props.pagination.totalPages;
  }
  return Math.ceil(props.pagination.total / props.pagination.limit);
});

// 判断是否有下一页
const hasNextPage = computed(() => {
  if (props.mode === "page") {
    return currentPage.value < totalPages.value;
  } else {
    // 如果明确指定了hasMore，优先使用
    if (props.pagination.hasMore !== undefined) {
      return props.pagination.hasMore;
    }
    // 否则根据当前offset、limit和total计算是否有下一页
    const { offset, limit, total } = props.pagination;
    return offset + limit < total;
  }
});

// 处理页码变化
const handlePageChange = (targetPage) => {
  if (props.mode === "page") {
    emit("page-changed", targetPage);
  } else {
    const newOffset = (targetPage - 1) * props.pagination.limit;
    emit("offset-changed", newOffset);
  }
};

// 处理每页数量变化
const handlePageSizeChange = (event) => {
  const newSize = parseInt(event.target.value);
  emit("limit-changed", newSize);
};
</script>

<template>
  <div class="mt-2 sm:mt-4 flex flex-col sm:flex-row items-center justify-between" v-if="pagination.total > 0">
    <!-- 移动端简化分页控件 -->
    <div class="w-full flex justify-between items-center mb-2 sm:mb-3 sm:hidden">
      <!-- 上一页按钮 -->
      <button
        @click="handlePageChange(currentPage - 1)"
        :disabled="currentPage <= 1"
        :class="[
          currentPage <= 1 ? 'opacity-50 cursor-not-allowed' : 'hover:bg-gray-50 dark:hover:bg-gray-700',
          'flex-grow flex justify-center items-center px-2 py-1.5 xs:px-3 xs:py-2 border border-gray-300 dark:border-gray-600 text-xs xs:text-sm font-medium rounded-md text-gray-700 dark:text-gray-300 bg-white dark:bg-gray-800 mr-1 xs:mr-2',
        ]"
      >
        <svg xmlns="http://www.w3.org/2000/svg" class="h-4 w-4 xs:h-5 xs:w-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 19l-7-7 7-7" />
        </svg>
        <span class="ml-1">{{ t("common.pagination.previousPage") }}</span>
      </button>

      <!-- 当前页码/总页数显示 -->
      <span class="text-xs xs:text-sm text-gray-700 dark:text-gray-300 mx-1 xs:mx-2 whitespace-nowrap">
        {{ t("common.pagination.pageInfo", { current: currentPage, total: totalPages }) }}
      </span>

      <!-- 下一页按钮 -->
      <button
        @click="handlePageChange(currentPage + 1)"
        :disabled="!hasNextPage"
        :class="[
          !hasNextPage ? 'opacity-50 cursor-not-allowed' : 'hover:bg-gray-50 dark:hover:bg-gray-700',
          'flex-grow flex justify-center items-center px-2 py-1.5 xs:px-3 xs:py-2 border border-gray-300 dark:border-gray-600 text-xs xs:text-sm font-medium rounded-md text-gray-700 dark:text-gray-300 bg-white dark:bg-gray-800 ml-1 xs:ml-2',
        ]"
      >
        <span class="mr-1">{{ t("common.pagination.nextPage") }}</span>
        <svg xmlns="http://www.w3.org/2000/svg" class="h-4 w-4 xs:h-5 xs:w-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5l7 7-7 7" />
        </svg>
      </button>
    </div>

    <!-- 桌面端完整分页控件 -->
    <div class="hidden sm:flex-1 sm:flex sm:items-center sm:justify-between">
      <!-- 左侧：显示范围信息和每页数量选择器 -->
      <div class="flex items-center space-x-4">
        <!-- 显示范围信息 -->
        <div>
          <p class="text-xs md:text-sm text-gray-700 dark:text-gray-300">
            <span v-if="searchMode && searchTerm">
              {{ t("common.pagination.searchResults", { term: searchTerm, start: displayRange.start, end: displayRange.end, total: pagination.total }) }}
            </span>
            <span v-else>
              {{
                t("common.pagination.showingRange", {
                  start: displayRange.start,
                  end: displayRange.end,
                  total: pagination.total,
                })
              }}
            </span>
          </p>
        </div>

        <!-- 每页数量选择器 -->
        <div v-if="showPageSizeSelector" class="flex items-center space-x-2">
          <span class="text-xs md:text-sm text-gray-700 dark:text-gray-300">{{ t("common.pagination.pageSize") }}</span>
          <select
            :value="pagination.limit"
            @change="handlePageSizeChange"
            class="px-2 py-1 text-xs md:text-sm border border-gray-300 dark:border-gray-600 rounded-md bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 focus:outline-none focus:ring-2 focus:ring-primary-500 focus:border-primary-500"
          >
            <option v-for="size in pageSizeOptions" :key="size" :value="size">
              {{ size }}
            </option>
          </select>
        </div>
      </div>

      <!-- 分页按钮组 -->
      <div>
        <nav class="relative z-0 inline-flex rounded-md shadow-sm -space-x-px" :aria-label="t('common.pagination.ariaLabel')">
          <!-- 第一页按钮 -->
          <button
            @click="handlePageChange(1)"
            :disabled="currentPage <= 1"
            :class="[
              currentPage <= 1 ? 'opacity-50 cursor-not-allowed' : 'hover:bg-gray-50 dark:hover:bg-gray-700',
              'relative inline-flex items-center px-1 py-1 md:px-2 md:py-2 rounded-l-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 text-xs md:text-sm font-medium text-gray-500 dark:text-gray-300',
            ]"
          >
            <span class="sr-only">{{ t("common.pagination.srFirstPage") }}</span>
            <svg class="h-4 w-4 md:h-5 md:w-5" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M11 19l-7-7 7-7m8 14l-7-7 7-7" />
            </svg>
          </button>

          <!-- 上一页按钮 -->
          <button
            @click="handlePageChange(currentPage - 1)"
            :disabled="currentPage <= 1"
            :class="[
              currentPage <= 1 ? 'opacity-50 cursor-not-allowed' : 'hover:bg-gray-50 dark:hover:bg-gray-700',
              'relative inline-flex items-center px-1 py-1 md:px-2 md:py-2 border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 text-xs md:text-sm font-medium text-gray-500 dark:text-gray-300',
            ]"
          >
            <span class="sr-only">{{ t("common.pagination.srPreviousPage") }}</span>
            <svg class="h-4 w-4 md:h-5 md:w-5" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 19l-7-7 7-7" />
            </svg>
          </button>

          <!-- 页码按钮 - 动态生成 -->
          <template v-for="pageNum in totalPages">
            <!-- 页码按钮 - 显示首尾页和当前页附近的页码 -->
            <button
              v-if="pageNum === 1 || pageNum === totalPages || (pageNum >= currentPage - 1 && pageNum <= currentPage + 1)"
              :key="pageNum"
              @click="handlePageChange(pageNum)"
              :class="[
                'relative inline-flex items-center px-2 py-1 md:px-4 md:py-2 border text-xs md:text-sm font-medium',
                pageNum === currentPage
                  ? 'z-10 bg-primary-50 dark:bg-primary-900 border-primary-500 dark:border-primary-500 text-primary-600 dark:text-primary-200'
                  : 'bg-white dark:bg-gray-800 border-gray-300 dark:border-gray-600 text-gray-500 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-700',
              ]"
            >
              {{ pageNum }}
            </button>

            <!-- 省略号 - 当页码较多时显示 -->
            <span
              v-else-if="(pageNum === 2 && currentPage > 3) || (pageNum === totalPages - 1 && currentPage < totalPages - 2)"
              :key="`ellipsis-${pageNum}`"
              class="relative inline-flex items-center px-2 py-1 md:px-4 md:py-2 border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 text-xs md:text-sm font-medium text-gray-700 dark:text-gray-300"
            >
              ...
            </span>
          </template>

          <!-- 下一页按钮 -->
          <button
            @click="handlePageChange(currentPage + 1)"
            :disabled="!hasNextPage"
            :class="[
              !hasNextPage ? 'opacity-50 cursor-not-allowed' : 'hover:bg-gray-50 dark:hover:bg-gray-700',
              'relative inline-flex items-center px-1 py-1 md:px-2 md:py-2 border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 text-xs md:text-sm font-medium text-gray-500 dark:text-gray-300',
            ]"
          >
            <span class="sr-only">{{ t("common.pagination.srNextPage") }}</span>
            <svg class="h-4 w-4 md:h-5 md:w-5" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5l7 7-7 7" />
            </svg>
          </button>

          <!-- 最后一页按钮 -->
          <button
            @click="handlePageChange(totalPages)"
            :disabled="!hasNextPage"
            :class="[
              !hasNextPage ? 'opacity-50 cursor-not-allowed' : 'hover:bg-gray-50 dark:hover:bg-gray-700',
              'relative inline-flex items-center px-1 py-1 md:px-2 md:py-2 rounded-r-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 text-xs md:text-sm font-medium text-gray-500 dark:text-gray-300',
            ]"
          >
            <span class="sr-only">{{ t("common.pagination.srLastPage") }}</span>
            <svg class="h-4 w-4 md:h-5 md:w-5" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 5l7 7-7 7M5 5l7 7-7 7" />
            </svg>
          </button>
        </nav>
      </div>
    </div>
  </div>
</template>
