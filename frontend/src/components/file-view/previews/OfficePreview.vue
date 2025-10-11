<template>
  <div class="office-preview rounded-lg overflow-hidden mb-2 flex-grow flex flex-col w-full">
    <div class="flex items-center justify-between p-2 bg-gray-100 dark:bg-gray-700 border-b border-gray-200 dark:border-gray-600">
      <span class="text-sm font-medium text-gray-700 dark:text-gray-300">
        {{ officeTypeDisplayName }}
      </span>
      <div>
        <button
          @click="toggleOfficePreviewService"
          class="text-xs px-2 py-1 rounded bg-blue-100 dark:bg-blue-700 text-blue-700 dark:text-blue-100 hover:bg-blue-200 dark:hover:bg-blue-600 transition-colors"
        >
          {{ useGoogleDocsPreview ? t("fileView.preview.office.useMicrosoft") : t("fileView.preview.office.useGoogle") }}
        </button>
      </div>
    </div>
    <div class="office-iframe flex-grow relative" style="height: calc(100vh - 400px); min-height: 300px; background-color: white">
      <iframe
        v-if="currentOfficePreviewUrl"
        :src="currentOfficePreviewUrl"
        frameborder="0"
        class="w-full h-full"
        sandbox="allow-scripts allow-same-origin allow-forms allow-popups allow-popups-to-escape-sandbox allow-top-navigation"
        @load="handleOfficePreviewLoad"
        @error="handleOfficePreviewError"
      ></iframe>
      <div v-else class="w-full h-full flex items-center justify-center">
        <div class="text-center p-4">
          <p class="text-gray-500 mb-2">{{ officePreviewError || t("fileView.preview.office.loading") }}</p>
          <div v-if="officePreviewError && officePreviewError.includes('401')">
            <p class="text-amber-500 text-sm mb-2">{{ t("fileView.preview.office.passwordIssue") }}</p>
            <ul class="text-left text-sm text-gray-600 dark:text-gray-300 list-disc pl-5 mb-2">
              <li>{{ t("fileView.preview.office.refreshAndRetry") }}</li>
              <li>{{ t("fileView.preview.office.confirmPassword") }}</li>
              <li>{{ t("fileView.preview.office.tryUrlPassword") }}</li>
            </ul>
          </div>
        </div>
      </div>

      <!-- 加载中状态遮罩 -->
      <div v-if="officePreviewLoading && currentOfficePreviewUrl" class="absolute inset-0 bg-white bg-opacity-70 flex items-center justify-center">
        <div class="text-center">
          <svg class="animate-spin h-8 w-8 text-blue-500 mx-auto mb-2" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
            <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
            <path
              class="opacity-75"
              fill="currentColor"
              d="M4 12a8 8 0 0 1 8-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 0 0 4 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"
            ></path>
          </svg>
          <p class="text-blue-600">{{ t("fileView.preview.office.loadingDetail") }}</p>
          <p class="text-gray-500 text-sm mt-1">
            {{ useGoogleDocsPreview ? t("fileView.preview.office.googleService") : t("fileView.preview.office.microsoftService") }}
            {{ useProxy ? t("fileView.preview.office.proxyMode") : t("fileView.preview.office.directMode") }}
          </p>
        </div>
      </div>
    </div>
    <div class="p-2 bg-gray-50 dark:bg-gray-800 text-xs text-gray-500 dark:text-gray-400 text-center">
      <p v-if="officePreviewError" class="text-red-500 mb-1">{{ officePreviewError }}</p>
      <p>
        {{ t("fileView.preview.office.previewTrouble") }}
        <button @click="updateOfficePreviewUrls" class="text-blue-500 hover:underline">{{ t("fileView.preview.office.refreshPreview") }}</button>
        {{ t("fileView.preview.office.switchService") }}
        <a :href="downloadUrl" class="text-blue-500 hover:underline" target="_blank">{{ t("fileView.preview.office.downloadFile") }}</a>
        {{ t("fileView.preview.office.afterDownload") }}
      </p>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, onMounted, onUnmounted } from "vue";
import { useI18n } from "vue-i18n";
import { getExtension } from "@/utils/fileTypes.js";

const { t } = useI18n();

const props = defineProps({
  microsoftOfficePreviewUrl: {
    type: String,
    default: "",
  },
  googleDocsPreviewUrl: {
    type: String,
    default: "",
  },
  mimetype: {
    type: String,
    default: "",
  },
  filename: {
    type: String,
    default: "",
  },
  useProxy: {
    type: Boolean,
    default: false,
  },
  downloadUrl: {
    type: String,
    default: "",
  },
});

const emit = defineEmits(["load", "error", "toggle-service", "update-urls"]);

const useGoogleDocsPreview = ref(false);
const officePreviewLoading = ref(true);
const officePreviewError = ref("");

// Office子类型判断 - 使用标准的 getExtension 函数
const isWordDocument = computed(() => {
  const ext = getExtension(props.filename || "");
  const mime = props.mimetype?.toLowerCase() || "";

  // 通过MIME类型判断
  if (
    mime === "application/msword" ||
    mime === "application/vnd.openxmlformats-officedocument.wordprocessingml.document" ||
    mime === "application/vnd.oasis.opendocument.text" ||
    mime === "application/rtf"
  ) {
    return true;
  }

  // 通过文件扩展名判断
  return ["doc", "docx", "odt", "rtf"].includes(ext);
});

const isSpreadsheet = computed(() => {
  const ext = getExtension(props.filename || "");
  const mime = props.mimetype?.toLowerCase() || "";

  // 通过MIME类型判断
  if (
    mime === "application/vnd.ms-excel" ||
    mime === "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" ||
    mime === "application/vnd.oasis.opendocument.spreadsheet" ||
    mime === "text/csv"
  ) {
    return true;
  }

  // 通过文件扩展名判断
  return ["xls", "xlsx", "ods", "csv"].includes(ext);
});

const isPresentation = computed(() => {
  const ext = getExtension(props.filename || "");
  const mime = props.mimetype?.toLowerCase() || "";

  // 通过MIME类型判断
  if (
    mime === "application/vnd.ms-powerpoint" ||
    mime === "application/vnd.openxmlformats-officedocument.presentationml.presentation" ||
    mime === "application/vnd.oasis.opendocument.presentation"
  ) {
    return true;
  }

  // 通过文件扩展名判断
  return ["ppt", "pptx", "odp"].includes(ext);
});

// Office类型显示名称
const officeTypeDisplayName = computed(() => {
  if (isWordDocument.value) return t("fileView.preview.office.wordPreview");
  if (isSpreadsheet.value) return t("fileView.preview.office.excelPreview");
  if (isPresentation.value) return t("fileView.preview.office.powerpointPreview");
  return t("fileView.preview.office.title");
});

const currentOfficePreviewUrl = computed(() => {
  return useGoogleDocsPreview.value ? props.googleDocsPreviewUrl : props.microsoftOfficePreviewUrl;
});

const toggleOfficePreviewService = () => {
  useGoogleDocsPreview.value = !useGoogleDocsPreview.value;
  officePreviewLoading.value = true;
  officePreviewError.value = "";
  emit("toggle-service", useGoogleDocsPreview.value);
};

const updateOfficePreviewUrls = () => {
  emit("update-urls");
};

const handleOfficePreviewLoad = () => {
  console.log("Office预览加载成功");
  officePreviewError.value = "";
  officePreviewLoading.value = false;
  emit("load");
};

const handleOfficePreviewError = (event) => {
  console.error("Office预览加载失败:", event);
  officePreviewError.value = t("fileView.preview.office.error");
  officePreviewLoading.value = false;
  emit("error", event);
};

onMounted(() => {
  if (currentOfficePreviewUrl.value) {
    officePreviewLoading.value = true;
  }
});

// 清理资源
onUnmounted(() => {
  // 清理可能的定时器或其他资源
  officePreviewLoading.value = false;
  officePreviewError.value = "";
});
</script>
