<template>
  <div>
    <!-- 文件篮按钮 -->
    <button
      @click="toggleBasket"
      class="relative inline-flex items-center px-2 sm:px-3 py-1.5 rounded-md transition-colors text-xs sm:text-sm font-medium"
      :class="[
        hasCollection
          ? 'bg-primary-600 hover:bg-primary-700 text-white shadow-md'
          : darkMode
          ? 'bg-gray-700 hover:bg-gray-600 text-gray-300'
          : 'bg-gray-200 hover:bg-gray-300 text-gray-700',
      ]"
      :title="hasCollection ? t('fileBasket.panel.summary', { fileCount: collectionCount, directoryCount: directoryCount }) : t('fileBasket.panel.empty')"
    >
      <!-- 文件列表图标 (Lucide Files) -->
      <svg class="w-4 h-4 mr-1 sm:mr-1.5" fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2">
        <path
          stroke-linecap="round"
          stroke-linejoin="round"
          d="M15.5 2H8.6c-.4 0-.8.2-1.1.5-.3.3-.5.7-.5 1.1v12.8c0 .4.2.8.5 1.1.3.3.7.5 1.1.5h9.8c.4 0 .8-.2 1.1-.5.3-.3.5-.7.5-1.1V6.5L15.5 2z"
        />
        <path stroke-linecap="round" stroke-linejoin="round" d="M3 7.6v12.8c0 .4.2.8.5 1.1.3.3.7.5 1.1.5h9.8" />
        <path stroke-linecap="round" stroke-linejoin="round" d="M15 2v5h5" />
      </svg>

      <!-- 按钮文本 -->
      <span class="whitespace-nowrap">{{ basketButtonText }}</span>

      <!-- 文件数量徽章 -->
      <span v-if="collectionCount > 0" class="absolute -top-1 -right-1 bg-red-500 text-white text-xs rounded-full h-5 w-5 flex items-center justify-center font-bold shadow-sm">
        {{ collectionCount > 99 ? "99+" : collectionCount }}
      </span>
    </button>
  </div>
</template>

<script setup>
import { storeToRefs } from "pinia";
import { useI18n } from "vue-i18n";
import { useFileBasket } from "../../../composables/file-system/useFileBasket.js";

const { t } = useI18n();

const props = defineProps({
  darkMode: {
    type: Boolean,
    default: false,
  },
});

// 使用文件篮composable
const fileBasket = useFileBasket();
const { collectionCount, hasCollection, directoryCount, basketButtonText } = storeToRefs(fileBasket);

// 切换文件篮面板
const toggleBasket = () => {
  try {
    fileBasket.toggleBasket();
  } catch (error) {
    console.error("切换文件篮面板失败:", error);
  }
};
</script>
