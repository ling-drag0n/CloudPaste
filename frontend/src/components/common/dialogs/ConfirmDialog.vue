<template>
  <div v-if="isOpen" class="fixed inset-0 z-[70] overflow-auto bg-black bg-opacity-50 flex items-center justify-center" @click="handleBackdropClick">
    <div class="relative w-full max-w-md p-6 rounded-lg shadow-xl" :class="darkMode ? 'bg-gray-800' : 'bg-white'" @click.stop>
      <!-- 标题和内容 -->
      <div class="mb-4">
        <h3 class="text-lg font-semibold" :class="darkMode ? 'text-gray-100' : 'text-gray-900'">
          {{ title }}
        </h3>
        <p v-if="message" class="text-sm mt-1" :class="darkMode ? 'text-gray-400' : 'text-gray-500'">
          {{ message }}
        </p>
        <!-- 支持自定义内容插槽 -->
        <div v-if="$slots.content" class="text-sm mt-1" :class="darkMode ? 'text-gray-400' : 'text-gray-500'">
          <slot name="content"></slot>
        </div>
      </div>

      <!-- 按钮组 -->
      <div class="flex justify-end space-x-2">
        <button
          @click="handleCancel"
          :disabled="loading"
          class="px-4 py-2 rounded-md transition-colors"
          :class="[darkMode ? 'text-gray-300 hover:bg-gray-700' : 'text-gray-700 hover:bg-gray-100', loading ? 'opacity-50 cursor-not-allowed' : '']"
        >
          {{ displayCancelText }}
        </button>
        <button @click="handleConfirm" :disabled="loading" class="px-4 py-2 rounded-md text-white transition-colors flex items-center space-x-2" :class="confirmButtonClass">
          <!-- 加载状态图标 -->
          <svg v-if="loading" class="animate-spin h-4 w-4" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
            <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
            <path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
          </svg>
          <span>{{ loading ? displayLoadingText : displayConfirmText }}</span>
        </button>
      </div>
    </div>
  </div>
</template>

<script setup>
import { computed, onMounted, onUnmounted } from "vue";
import { useI18n } from "vue-i18n";

// 国际化
const { t } = useI18n();

const props = defineProps({
  isOpen: {
    type: Boolean,
    default: false,
  },
  title: {
    type: String,
    required: true,
  },
  message: {
    type: String,
    default: "",
  },
  confirmText: {
    type: String,
    default: "",
  },
  cancelText: {
    type: String,
    default: "",
  },
  confirmType: {
    type: String,
    default: "primary", // 'primary', 'danger', 'warning'
    validator: (value) => ["primary", "danger", "warning"].includes(value),
  },
  darkMode: {
    type: Boolean,
    default: false,
  },
  loading: {
    type: Boolean,
    default: false,
  },
  loadingText: {
    type: String,
    default: "",
  },
  allowBackdropClose: {
    type: Boolean,
    default: true,
  },
});

const emit = defineEmits(["confirm", "cancel", "close"]);

// 国际化文本计算属性
const displayConfirmText = computed(() => {
  return props.confirmText || t("common.dialogs.confirm");
});

const displayCancelText = computed(() => {
  return props.cancelText || t("common.dialogs.cancel");
});

const displayLoadingText = computed(() => {
  return props.loadingText || t("common.dialogs.processing");
});

// 确认按钮样式
const confirmButtonClass = computed(() => {
  const baseClass = "transition-colors";
  const loadingClass = props.loading ? "cursor-not-allowed" : "";

  switch (props.confirmType) {
    case "danger":
      return `${baseClass} ${props.loading ? "bg-red-500" : "bg-red-600 hover:bg-red-700"} ${loadingClass}`;
    case "warning":
      return `${baseClass} ${props.loading ? "bg-yellow-500" : "bg-yellow-600 hover:bg-yellow-700"} ${loadingClass}`;
    case "primary":
    default:
      return `${baseClass} ${props.loading ? "bg-primary-500" : props.darkMode ? "bg-primary-600 hover:bg-primary-700" : "bg-primary-500 hover:bg-primary-600"} ${loadingClass}`;
  }
});

// 事件处理
const handleConfirm = () => {
  if (props.loading) return;
  emit("confirm");
};

const handleCancel = () => {
  if (props.loading) return;
  emit("cancel");
  emit("close");
};

const handleBackdropClick = () => {
  if (props.allowBackdropClose && !props.loading) {
    handleCancel();
  }
};

// 键盘事件处理
const handleKeydown = (event) => {
  if (!props.isOpen || props.loading) return;

  if (event.key === "Escape") {
    handleCancel();
  } else if (event.key === "Enter") {
    handleConfirm();
  }
};

// 生命周期
onMounted(() => {
  document.addEventListener("keydown", handleKeydown);
});

onUnmounted(() => {
  document.removeEventListener("keydown", handleKeydown);
});
</script>
