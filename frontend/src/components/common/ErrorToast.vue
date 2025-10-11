<template>
  <div
    v-if="visible"
    class="fixed top-4 right-4 max-w-sm w-full bg-white dark:bg-gray-800 border border-red-200 dark:border-red-700 rounded-lg shadow-lg z-50 transition-all duration-300"
    :class="{ 'opacity-0 translate-x-full': !visible }"
  >
    <div class="p-4">
      <div class="flex items-start">
        <div class="flex-shrink-0">
          <svg class="h-5 w-5 text-red-400" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20" fill="currentColor">
            <path
              fill-rule="evenodd"
              d="M10 18a8 8 0 100-16 8 8 0 000 16zM8.707 7.293a1 1 0 00-1.414 1.414L8.586 10l-1.293 1.293a1 1 0 101.414 1.414L10 11.414l1.293 1.293a1 1 0 001.414-1.414L11.414 10l1.293-1.293a1 1 0 00-1.414-1.414L10 8.586 8.707 7.293z"
              clip-rule="evenodd"
            />
          </svg>
        </div>
        <div class="ml-3 w-0 flex-1">
          <p class="text-sm font-medium text-gray-900 dark:text-white">{{ title || t("common.errorToast.defaultTitle") }}</p>
          <p class="mt-1 text-sm text-gray-500 dark:text-gray-300">{{ message }}</p>
          <div v-if="actionText && actionHandler" class="mt-3">
            <button @click="actionHandler" class="text-sm bg-red-600 hover:bg-red-700 text-white px-3 py-1 rounded-md transition-colors">
              {{ actionText }}
            </button>
          </div>
        </div>
        <div class="ml-4 flex-shrink-0 flex">
          <button
            @click="close"
            class="bg-white dark:bg-gray-800 rounded-md inline-flex text-gray-400 hover:text-gray-500 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-red-500"
          >
            <span class="sr-only">{{ t("common.errorToast.srClose") }}</span>
            <svg class="h-5 w-5" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20" fill="currentColor">
              <path
                fill-rule="evenodd"
                d="M4.293 4.293a1 1 0 011.414 0L10 8.586l4.293-4.293a1 1 0 111.414 1.414L11.414 10l4.293 4.293a1 1 0 01-1.414 1.414L10 11.414l-4.293 4.293a1 1 0 01-1.414-1.414L8.586 10 4.293 5.707a1 1 0 010-1.414z"
                clip-rule="evenodd"
              />
            </svg>
          </button>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, watch, onMounted, onUnmounted } from "vue";
import { useI18n } from "vue-i18n";

const { t } = useI18n();

const props = defineProps({
  visible: {
    type: Boolean,
    default: false,
  },
  title: {
    type: String,
    default: "",
  },
  message: {
    type: String,
    required: true,
  },
  actionText: {
    type: String,
    default: "",
  },
  actionHandler: {
    type: Function,
    default: null,
  },
  autoClose: {
    type: Boolean,
    default: true,
  },
  duration: {
    type: Number,
    default: 5000,
  },
});

const emit = defineEmits(["close"]);

let autoCloseTimer = null;

const close = () => {
  emit("close");
};

const startAutoClose = () => {
  if (props.autoClose && props.duration > 0) {
    autoCloseTimer = setTimeout(() => {
      close();
    }, props.duration);
  }
};

const clearAutoClose = () => {
  if (autoCloseTimer) {
    clearTimeout(autoCloseTimer);
    autoCloseTimer = null;
  }
};

watch(
  () => props.visible,
  (newVisible) => {
    if (newVisible) {
      startAutoClose();
    } else {
      clearAutoClose();
    }
  },
  { immediate: true }
);

onUnmounted(() => {
  clearAutoClose();
});
</script>
