<template>
  <nav class="flex flex-col sm:flex-row sm:justify-between sm:items-center mb-3 gap-2 sm:gap-0" :aria-label="$t('breadcrumb.navigation')">
    <!-- 左侧面包屑 -->
    <ol class="flex flex-wrap items-center space-x-1">
      <li class="flex items-center">
        <a
          href="#"
          @click.prevent="navigateToRoot"
          :class="['flex items-center font-medium transition-colors duration-200', darkMode ? 'text-gray-300 hover:text-white' : 'text-gray-600 hover:text-gray-900']"
        >
          <!-- 根目录图标 -->
          <svg class="w-5 h-5 mr-1" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path
              stroke-linecap="round"
              stroke-linejoin="round"
              stroke-width="2"
              d="M3 12l2-2m0 0l7-7 7 7M5 10v10a1 1 0 001 1h3m10-11l2 2m-2-2v10a1 1 0 01-1 1h-3m-6 0a1 1 0 001-1v-4a1 1 0 011-1h2a1 1 0 011 1v4a1 1 0 001 1m-6 0h6"
            />
          </svg>
          <span>{{ $t("breadcrumb.root") }}</span>
        </a>
      </li>

      <!-- 路径分隔符和目录段 -->
      <li v-for="(segment, index) in pathSegments" :key="index" class="flex items-center">
        <span :class="['mx-1', darkMode ? 'text-gray-500' : 'text-gray-400']"> / </span>
        <a
          href="#"
          @click.prevent="navigateToSegment(index)"
          :class="[
            'font-medium transition-colors duration-200',
            index === pathSegments.length - 1 && !previewFile
              ? darkMode
                ? 'text-primary-400'
                : 'text-primary-600'
              : darkMode
              ? 'text-gray-300 hover:text-white'
              : 'text-gray-600 hover:text-gray-900',
          ]"
        >
          {{ segment }}
        </a>
      </li>

      <!-- 文件预览项 -->
      <li v-if="previewFile" class="flex items-center">
        <span :class="['mx-1', darkMode ? 'text-gray-500' : 'text-gray-400']"> / </span>
        <span :class="['font-medium', darkMode ? 'text-primary-400' : 'text-primary-600']">
          {{ previewFile.name }}
        </span>
      </li>
    </ol>

    <!-- 右侧操作按钮 -->
    <div v-if="!previewFile" class="flex items-center">
      <!-- 启用/禁用勾选框按钮 -->
      <button
        @click="$emit('toggle-checkbox-mode')"
        class="inline-flex items-center px-2 sm:px-3 py-1.5 rounded-md transition-colors text-xs sm:text-sm font-medium mr-2 sm:ml-2"
        :class="[
          isCheckboxMode
            ? darkMode
              ? 'bg-primary-600 text-white hover:bg-primary-700'
              : 'bg-primary-500 text-white hover:bg-primary-600'
            : darkMode
            ? 'bg-gray-700 hover:bg-gray-600 text-white'
            : 'bg-gray-100 hover:bg-gray-200 text-gray-700',
        ]"
        :title="$t('breadcrumb.batchOperations')"
      >
        <svg class="w-4 h-4 mr-1 sm:mr-1.5" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor">
          <path v-if="isCheckboxMode" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7" />
          <rect v-else x="4" y="4" width="16" height="16" rx="2" stroke-width="2" />
        </svg>
        <span class="hidden sm:inline">{{ isCheckboxMode ? $t("breadcrumb.exitSelection") : $t("breadcrumb.enableSelection") }}</span>
        <span class="sm:hidden">{{ isCheckboxMode ? $t("breadcrumb.mobile.exitSelection") : $t("breadcrumb.mobile.enableSelection") }}</span>
      </button>

      <!-- 复制按钮 (在勾选模式且有选中项时显示) -->
      <button
        v-if="isCheckboxMode && selectedCount > 0"
        @click="$emit('batch-copy')"
        class="inline-flex items-center px-2 sm:px-3 py-1.5 rounded-md transition-colors text-xs sm:text-sm font-medium mr-2 bg-blue-600 hover:bg-blue-700 text-white"
      >
        <svg class="w-4 h-4 mr-1 sm:mr-1.5" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor">
          <path
            stroke-linecap="round"
            stroke-linejoin="round"
            stroke-width="2"
            d="M8 16H6a2 2 0 01-2-2V6a2 2 0 012-2h8a2 2 0 012 2v2m-6 12h8a2 2 0 002-2v-8a2 2 0 00-2-2h-8a2 2 0 00-2 2v8a2 2 0 002 2z"
          />
        </svg>
        <span class="hidden sm:inline whitespace-nowrap">{{ $t("breadcrumb.copySelected") }} {{ $t("breadcrumb.selectedCount", { count: selectedCount }) }}</span>
        <span class="sm:hidden whitespace-nowrap">{{ $t("breadcrumb.mobile.copySelected") }} ({{ selectedCount }})</span>
      </button>

      <!-- 批量添加到文件篮按钮 (在勾选模式且有选中项时显示) -->
      <button
        v-if="isCheckboxMode && selectedCount > 0"
        @click="$emit('batch-add-to-basket')"
        class="inline-flex items-center px-2 sm:px-3 py-1.5 rounded-md transition-colors text-xs sm:text-sm font-medium mr-2 bg-green-600 hover:bg-green-700 text-white"
      >
        <svg class="w-4 h-4 mr-1 sm:mr-1.5" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor">
          <path
            stroke-linecap="round"
            stroke-linejoin="round"
            stroke-width="2"
            d="M12 10.5v6m3-3H9m4.06-7.19-2.12-2.12a1.5 1.5 0 0 0-1.061-.44H4.5A2.25 2.25 0 0 0 2.25 6v12a2.25 2.25 0 0 0 2.25 2.25h15A2.25 2.25 0 0 0 21.75 18V9a2.25 2.25 0 0 0-2.25-2.25h-5.379a1.5 1.5 0 0 1-1.06-.44Z"
          />
        </svg>
        <span class="hidden sm:inline whitespace-nowrap">{{ $t("fileBasket.actions.batchAdd") }}</span>
        <span class="sm:hidden whitespace-nowrap">{{ $t("fileBasket.actions.mobile.batchAdd") }}</span>
      </button>

      <!-- 批量删除按钮 (在勾选模式且有选中项时显示) -->
      <button
        v-if="isCheckboxMode && selectedCount > 0"
        @click="$emit('batch-delete')"
        class="inline-flex items-center px-2 sm:px-3 py-1.5 rounded-md transition-colors text-xs sm:text-sm font-medium bg-red-600 hover:bg-red-700 text-white"
      >
        <svg class="w-4 h-4 mr-1 sm:mr-1.5" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor">
          <path
            stroke-linecap="round"
            stroke-linejoin="round"
            stroke-width="2"
            d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16"
          />
        </svg>
        <span class="hidden sm:inline whitespace-nowrap">{{ $t("breadcrumb.deleteSelected") }} {{ $t("breadcrumb.selectedCount", { count: selectedCount }) }}</span>
        <span class="sm:hidden whitespace-nowrap">{{ $t("breadcrumb.mobile.deleteSelected") }} ({{ selectedCount }})</span>
      </button>
    </div>
  </nav>
</template>

<script setup>
import { computed } from "vue";
import { useI18n } from "vue-i18n";

const { t } = useI18n();

const props = defineProps({
  currentPath: {
    type: String,
    required: true,
    default: "/",
  },
  darkMode: {
    type: Boolean,
    default: false,
  },
  previewFile: {
    type: Object,
    default: null,
  },
  isCheckboxMode: {
    type: Boolean,
    default: false,
  },
  selectedCount: {
    type: Number,
    default: 0,
  },
  basicPath: {
    type: String,
    default: "/",
  },
  userType: {
    type: String,
    default: "admin", // 'admin' 或 'user'
  },
});

const emit = defineEmits(["navigate", "toggle-checkbox-mode", "batch-delete", "batch-copy", "batch-add-to-basket"]);

// 计算路径段
const pathSegments = computed(() => {
  // 移除开头的斜杠，然后按斜杠分割
  const segments = props.currentPath
    .replace(/^\/+/, "") // 移除开头的斜杠
    .split("/")
    .filter((segment) => segment); // 过滤空字符串

  // 对于API密钥用户，需要根据基本路径过滤可显示的路径段
  if (props.userType === "user" && props.basicPath !== "/") {
    const normalizedBasicPath = props.basicPath.replace(/\/+$/, "");
    const basicSegments = normalizedBasicPath
      .replace(/^\/+/, "")
      .split("/")
      .filter((segment) => segment);

    // 如果当前路径在基本路径范围内，只显示基本路径及其子路径的段
    const currentPathNormalized = "/" + segments.join("/");
    const basicPathNormalized = "/" + basicSegments.join("/");

    if (currentPathNormalized === basicPathNormalized) {
      // 当前路径就是基本路径，显示为根路径（空数组）
      return [];
    } else if (currentPathNormalized.startsWith(basicPathNormalized + "/")) {
      // 当前路径在基本路径范围内，显示从基本路径开始的段
      return segments.slice(basicSegments.length);
    } else if (basicPathNormalized.startsWith(currentPathNormalized + "/")) {
      // 当前路径是基本路径的父级路径，显示到基本路径为止的段
      return segments;
    } else {
      // 其他情况，显示所有段（这种情况理论上不应该发生，因为后端会阻止访问）
      return segments;
    }
  }

  return segments;
});

// 导航到指定段
const navigateToSegment = (segmentIndex) => {
  // 对于API密钥用户，需要检查导航权限
  if (props.userType === "user" && props.basicPath !== "/") {
    const normalizedBasicPath = props.basicPath.replace(/\/+$/, "");
    const basicSegments = normalizedBasicPath
      .replace(/^\/+/, "")
      .split("/")
      .filter((segment) => segment);

    // 构建目标路径
    let targetSegments;
    if (props.currentPath.startsWith(normalizedBasicPath)) {
      // 当前在基本路径范围内，目标路径需要加上基本路径前缀
      targetSegments = [...basicSegments, ...pathSegments.value.slice(0, segmentIndex + 1)];
    } else {
      // 当前在基本路径的父级路径，直接使用段
      targetSegments = pathSegments.value.slice(0, segmentIndex + 1);
    }

    let targetPath = "/";
    if (targetSegments.length > 0) {
      targetPath += targetSegments.join("/") + "/";
    }

    // 检查目标路径是否在权限范围内
    const basicPathNormalized = "/" + basicSegments.join("/");
    const targetPathNormalized = "/" + targetSegments.join("/");

    if (targetPathNormalized === basicPathNormalized || targetPathNormalized.startsWith(basicPathNormalized + "/") || basicPathNormalized.startsWith(targetPathNormalized + "/")) {
      emit("navigate", targetPath);
    }
    // 如果无权限，不执行导航
    return;
  }

  // 管理员用户或根路径用户，正常导航
  let targetPath = "/";
  if (segmentIndex >= 0) {
    targetPath += pathSegments.value.slice(0, segmentIndex + 1).join("/") + "/";
  }
  emit("navigate", targetPath);
};

// 导航到根目录
const navigateToRoot = () => {
  // 对于API密钥用户，如果有基本路径限制，导航到基本路径而不是真正的根路径
  if (props.userType === "user" && props.basicPath !== "/") {
    emit("navigate", props.basicPath);
  } else {
    emit("navigate", "/");
  }
};
</script>
