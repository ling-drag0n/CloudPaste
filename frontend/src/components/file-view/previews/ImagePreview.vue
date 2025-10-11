<template>
  <div class="image-preview rounded-lg overflow-hidden mb-2 flex justify-center relative">
    <img v-if="showImage" :src="previewUrl" :alt="filename" class="max-w-full max-h-[calc(100vh-350px)] h-auto object-contain" @load="handleLoad" @error="handleError" />
    <!-- 图片加载状态 -->
    <div v-if="loading" class="absolute inset-0 flex items-center justify-center bg-gray-100 dark:bg-gray-700 rounded-lg">
      <div class="text-center">
        <svg class="animate-spin h-8 w-8 text-blue-500 mx-auto mb-2" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
          <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
          <path
            class="opacity-75"
            fill="currentColor"
            d="M4 12a8 8 0 0 1 8-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 0 0 4 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"
          ></path>
        </svg>
        <p class="text-blue-600 dark:text-blue-400">{{ t("fileView.preview.image.loading") }}</p>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, onMounted } from "vue";
import { useI18n } from "vue-i18n";

const { t } = useI18n();

const props = defineProps({
  previewUrl: {
    type: String,
    required: true,
  },
  filename: {
    type: String,
    required: true,
  },
});

const emit = defineEmits(["load", "error"]);

const loading = ref(true);
const showImage = ref(false);

const handleLoad = () => {
  loading.value = false;
  emit("load");
};

const handleError = () => {
  loading.value = false;
  emit("error");
};

onMounted(() => {
  // 立即显示图片，让浏览器自然处理加载状态
  showImage.value = true;
});
</script>
