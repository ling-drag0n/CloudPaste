<template>
  <div class="text-preview rounded-lg overflow-hidden mb-2 bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 flex-grow flex flex-col w-full">
    <div class="flex items-center justify-between p-2 bg-gray-100 dark:bg-gray-700 border-b border-gray-200 dark:border-gray-600">
      <span class="text-sm font-medium text-gray-700 dark:text-gray-300">{{ title || t("fileView.preview.text.title") }}</span>
      <div class="flex items-center gap-3">
        <!-- 统计信息 -->
        <div v-if="textContent" class="text-xs text-gray-500 dark:text-gray-400 flex gap-2">
          <span>{{ lineCount }} L</span>
          <span>{{ characterCount }} Chars</span>
        </div>
        <!-- 编码选择器 -->
        <div v-if="textContent" class="flex items-center gap-1">
          <span class="text-xs text-gray-500 dark:text-gray-400">Enc:</span>
          <select
            v-model="currentEncoding"
            @change="handleEncodingChange"
            class="text-xs px-1 py-0.5 rounded border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 text-gray-700 dark:text-gray-300"
          >
            <option v-for="encoding in availableEncodings" :key="encoding.value" :value="encoding.value">
              {{ encoding.label }}
            </option>
          </select>
        </div>
      </div>
    </div>
    <div class="p-4 overflow-auto flex-grow relative" style="max-height: calc(100vh - 350px); min-height: 200px">
      <!-- 使用统一的 TextRenderer 组件 -->
      <TextRenderer
        v-if="textContent"
        :content="textContent"
        :mode="'text'"
        :language="detectedLanguage"
        :filename="adaptedFileData?.name || ''"
        :dark-mode="darkMode"
        :show-line-numbers="true"
        :read-only="true"
        :show-stats="false"
        :max-height="'100%'"
        @load="handleLoad"
        @error="handleError"
      />
      <!-- 加载状态 -->
      <div v-else class="absolute inset-0 flex items-center justify-center bg-gray-100 dark:bg-gray-700">
        <div class="text-center">
          <svg class="animate-spin h-8 w-8 text-blue-500 mx-auto mb-2" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
            <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
            <path
              class="opacity-75"
              fill="currentColor"
              d="M4 12a8 8 0 0 1 8-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 0 0 4 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"
            ></path>
          </svg>
          <p class="text-blue-600 dark:text-blue-400">{{ loadingText || t("fileView.preview.text.loading") }}</p>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { computed, ref, onMounted, watch } from "vue";
import { useI18n } from "vue-i18n";
import TextRenderer from "@/components/common/text-preview/TextRenderer.vue";
import { useFetchText } from "@/composables/text-preview/useFetchText.js";
import { useTextPreview } from "@/composables/text-preview/useTextPreview.js";

const { t } = useI18n();

const props = defineProps({
  previewUrl: {
    type: String,
    required: true,
  },
  filename: {
    type: String,
    default: "",
  },
  title: {
    type: String,
    default: "",
  },
  language: {
    type: String,
    default: "",
  },
  loadingText: {
    type: String,
    default: "",
  },
  darkMode: {
    type: Boolean,
    default: false,
  },
});

const emit = defineEmits(["load", "error"]);

// 使用统一的文本预览逻辑
const {
  textContent,
  detectedLanguage,
  currentEncoding,
  loading,
  error,
  loadTextContent: loadText,
  handleEncodingChange: changeEncoding,
} = useTextPreview({
  checkCancelled: false,
  emitEncodingChange: false,
});

// 统计信息计算
const lineCount = computed(() => {
  if (!textContent.value) return 0;
  return textContent.value.split("\n").length;
});

const characterCount = computed(() => {
  if (!textContent.value) return 0;
  return textContent.value.length;
});

// 使用文本获取 Composable（用于获取可用编码）
const { availableEncodings } = useFetchText();

// 适配数据结构
const adaptedFileData = computed(() => {
  if (!props.previewUrl) return null;

  return {
    name: props.filename || "text-file",
    filename: props.filename || "text-file",
    preview_url: props.previewUrl,
    contentType: "text/plain",
  };
});

// 加载文本内容 - 使用统一逻辑
const loadTextContent = async () => {
  if (!adaptedFileData.value) {
    console.warn("没有可用的文件数据");
    return;
  }

  await loadText(adaptedFileData.value, emit);
};

// 处理编码切换 - 使用统一逻辑
const handleEncodingChange = async () => {
  if (!adaptedFileData.value?.preview_url) return;

  await changeEncoding(currentEncoding.value, emit);
};

// 事件处理
const handleLoad = () => {
  // TextRenderer 的 load 事件，这里不需要额外处理
};

const handleError = (error) => {
  emit("error", error);
};

// 监听预览URL变化
watch(
  () => props.previewUrl,
  () => {
    if (props.previewUrl) {
      loadTextContent();
    }
  },
  { immediate: true }
);

// 组件挂载时加载内容
onMounted(() => {
  if (props.previewUrl) {
    loadTextContent();
  }
});
</script>
