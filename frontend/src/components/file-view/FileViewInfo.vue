<template>
  <div class="file-info-container flex flex-col min-h-0 flex-grow">
    <!-- 文件头部信息 -->
    <div class="file-header mb-6">
      <div class="flex items-center gap-3">
        <!-- 文件图标 -->
        <div class="file-icon flex items-center justify-center w-12 h-12 rounded-xl bg-gray-100 dark:bg-gray-700">
          <svg xmlns="http://www.w3.org/2000/svg" class="h-6 w-6" :class="iconClass" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path
              stroke-linecap="round"
              stroke-linejoin="round"
              stroke-width="2"
              d="M7 21h10a2 2 0 002-2V9.414a1 1 0 00-.293-.707l-5.414-5.414A1 1 0 0012.586 3H7a2 2 0 00-2 2v14a2 2 0 002 2z"
            />
          </svg>
        </div>

        <!-- 文件名和类型 -->
        <div class="flex-1 min-w-0">
          <h1 class="text-xl font-bold truncate text-gray-900 dark:text-white">
            {{ fileInfo.filename }}
          </h1>
          <p class="text-sm text-gray-500 dark:text-gray-400">{{ formattedMimeType }} · {{ formattedSize }}</p>
        </div>
      </div>
    </div>

    <!-- 文件备注 -->
    <div v-if="fileInfo.remark" class="file-remark mb-6 px-4 py-3 rounded-lg bg-gray-100 dark:bg-gray-700 overflow-auto max-h-[300px]">
      <p class="text-blue-600 dark:text-blue-400 break-words whitespace-pre-wrap">{{ fileInfo.remark }}</p>
    </div>

    <!--使用动态组件进行文件预览 -->
    <div v-if="shouldShowPreview" class="file-preview mb-6 flex-grow flex flex-col justify-center items-center">
      <component
        :is="currentPreviewComponent"
        v-bind="previewComponentProps"
        @load="handlePreviewLoad"
        @error="handlePreviewError"
        @toggle-mode="handleToggleMode"
        @toggle-service="handleToggleService"
        @update-urls="handleUpdateUrls"
      />
    </div>

    <!-- 文件元数据 -->
    <div class="file-metadata grid grid-cols-1 md:grid-cols-2 gap-3 mb-6">
      <!-- 创建时间 -->
      <div class="metadata-item p-3 rounded-lg bg-gray-100 dark:bg-gray-700">
        <div class="flex items-center">
          <svg xmlns="http://www.w3.org/2000/svg" class="h-5 w-5 mr-2 text-gray-500 dark:text-gray-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8 7V3m8 4V3m-9 8h10M5 21h14a2 2 0 002-2V7a2 2 0 00-2-2H5a2 2 0 00-2 2v12a2 2 0 002 2z" />
          </svg>
          <span class="text-sm font-medium text-gray-600 dark:text-gray-200">{{ t("fileView.fileInfo.uploadTime") }}</span>
        </div>
        <p class="mt-1 text-sm pl-7 text-gray-800 dark:text-white">{{ formattedCreatedAt }}</p>
      </div>

      <!-- 访问次数 -->
      <div class="metadata-item p-3 rounded-lg bg-gray-100 dark:bg-gray-700">
        <div class="flex items-center">
          <svg xmlns="http://www.w3.org/2000/svg" class="h-5 w-5 mr-2 text-gray-500 dark:text-gray-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 12a3 3 0 11-6 0 3 3 0 016 0z" />
            <path
              stroke-linecap="round"
              stroke-linejoin="round"
              stroke-width="2"
              d="M2.458 12C3.732 7.943 7.523 5 12 5c4.478 0 8.268 2.943 9.542 7-1.274 4.057-5.064 7-9.542 7-4.477 0-8.268-2.943-9.542-7z"
            />
          </svg>
          <span class="text-sm font-medium text-gray-600 dark:text-gray-200">{{ t("fileView.fileInfo.accessCount") }}</span>
        </div>
        <p class="mt-1 text-sm pl-7 text-gray-800 dark:text-white">
          {{ fileInfo.views || 0 }}
          <span v-if="fileInfo.max_views" class="text-xs text-gray-500 dark:text-gray-400"> / {{ fileInfo.max_views }} ({{ t("fileView.fileInfo.limit") }}) </span>
        </p>
      </div>

      <!-- 过期时间 -->
      <div v-if="fileInfo.expires_at" class="metadata-item p-3 rounded-lg bg-gray-100 dark:bg-gray-700">
        <div class="flex items-center">
          <svg xmlns="http://www.w3.org/2000/svg" class="h-5 w-5 mr-2 text-gray-500 dark:text-gray-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z" />
          </svg>
          <span class="text-sm font-medium text-gray-600 dark:text-gray-200">{{ t("fileView.fileInfo.expiresAt") }}</span>
        </div>
        <p class="mt-1 text-sm pl-7 text-gray-800 dark:text-white">{{ formattedExpiresAt }}</p>
      </div>

      <!-- 访问模式 -->
      <div class="metadata-item p-3 rounded-lg bg-gray-100 dark:bg-gray-700">
        <div class="flex items-center">
          <svg xmlns="http://www.w3.org/2000/svg" class="h-5 w-5 mr-2 text-gray-500 dark:text-gray-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path
              stroke-linecap="round"
              stroke-linejoin="round"
              stroke-width="2"
              d="M9 12l2 2 4-4m5.618-4.016A11.955 11.955 0 0112 2.944a11.955 11.955 0 01-8.618 3.04A12.02 12.02 0 003 9c0 5.591 3.824 10.29 9 11.622 5.176-1.332 9-6.03 9-11.622 0-1.042-.133-2.052-.382-3.016z"
            />
          </svg>
          <span class="text-sm font-medium text-gray-600 dark:text-gray-200">{{ t("fileView.fileInfo.accessMode") }}</span>
        </div>
        <p class="mt-1 text-sm pl-7 text-gray-800 dark:text-white">
          <span :class="{ 'text-green-600 dark:text-green-400': fileInfo.use_proxy, 'text-blue-600 dark:text-blue-400': !fileInfo.use_proxy }">
            {{ fileInfo.use_proxy ? t("fileView.fileInfo.proxyAccess") : t("fileView.fileInfo.directAccess") }}
          </span>
        </p>
      </div>

      <!-- 文件短链接 -->
      <div class="metadata-item p-3 rounded-lg bg-gray-100 dark:bg-gray-700">
        <div class="flex items-center">
          <svg xmlns="http://www.w3.org/2000/svg" class="h-5 w-5 mr-2 text-gray-500 dark:text-gray-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path
              stroke-linecap="round"
              stroke-linejoin="round"
              stroke-width="2"
              d="M13.828 10.172a4 4 0 00-5.656 0l-4 4a4 4 0 105.656 5.656l1.102-1.101m-.758-4.899a4 4 0 005.656 0l4-4a4 4 0 00-5.656-5.656l-1.1 1.1"
            />
          </svg>
          <span class="text-sm font-medium text-gray-600 dark:text-gray-200">{{ t("fileView.fileInfo.fileLink") }}</span>
        </div>
        <div class="mt-1 pl-7 flex items-center relative">
          <p class="text-sm truncate flex-1 text-gray-800 dark:text-white">
            {{ shareUrl || t("fileView.fileInfo.needPassword") }}
          </p>
          <button
            v-if="shareUrl"
            @click="copyToClipboard(shareUrl)"
            class="ml-2 p-1 rounded hover:bg-opacity-80 transition-colors bg-gray-200 dark:bg-gray-600 text-gray-700 dark:text-gray-300 hover:bg-gray-300 dark:hover:bg-gray-500"
            :title="t('fileView.fileInfo.copyLink')"
          >
            <svg xmlns="http://www.w3.org/2000/svg" class="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path
                stroke-linecap="round"
                stroke-linejoin="round"
                stroke-width="2"
                d="M8 5H6a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2v-1M8 5a2 2 0 002 2h2a2 2 0 002-2M8 5a2 2 0 012-2h2a2 2 0 012 2m0 0h2a2 2 0 012 2v3m2 4H10m0 0l3-3m-3 3l3 3"
              />
            </svg>
          </button>

          <!-- 复制成功提示 -->
          <div
            v-if="showCopyToast"
            class="absolute right-0 -top-10 px-3 py-2 rounded-md shadow-md text-sm transition-opacity duration-300 bg-white dark:bg-gray-700 text-green-600 dark:text-green-400 border border-gray-200 dark:border-gray-600"
          >
            <div class="flex items-center">
              <svg xmlns="http://www.w3.org/2000/svg" class="h-4 w-4 mr-1" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7" />
              </svg>
              {{ t("fileView.fileInfo.linkCopied") }}
            </div>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { computed, ref, defineProps, onMounted, watch, onUnmounted } from "vue";
import { useI18n } from "vue-i18n";
import { api } from "@/api";

const { t } = useI18n();
import { getPreviewComponent, formatFileSize, FileType, getIconType } from "@/utils/fileTypes.js";
import { getPreviewModeFromFilename, PREVIEW_MODES } from "@/utils/textUtils.js";
import { formatDateTime } from "@/utils/timeUtils.js";
import { copyToClipboard as clipboardCopy } from "@/utils/clipboard";

//导入预览组件
import ImagePreview from "./previews/ImagePreview.vue";
import VideoPreview from "./previews/VideoPreview.vue";
import AudioPreview from "./previews/AudioPreview.vue";
import PdfPreview from "./previews/PdfPreview.vue";
import TextPreview from "./previews/TextPreview.vue";
import CodePreview from "./previews/CodePreview.vue";
import MarkdownPreview from "./previews/MarkdownPreview.vue";
import HtmlPreview from "./previews/HtmlPreview.vue";
import OfficePreview from "./previews/OfficePreview.vue";
import GenericPreview from "./previews/GenericPreview.vue";

const props = defineProps({
  fileInfo: {
    type: Object,
    required: true,
  },
  fileUrls: {
    type: Object,
    default: () => ({
      previewUrl: "",
      downloadUrl: "",
    }),
  },
  darkMode: {
    type: Boolean,
    default: false,
  },
});

// 分享链接 - 使用当前页面的URL
const shareUrl = computed(() => {
  return window.location.href;
});

// 复制成功提示状态
const showCopyToast = ref(false);

// 辅助函数：获取文件密码
const getFilePassword = () => {
  // 优先使用文件信息中存储的已验证密码
  if (props.fileInfo.currentPassword) {
    return props.fileInfo.currentPassword;
  }

  // 其次尝试从URL获取密码参数
  const currentUrl = new URL(window.location.href);
  const passwordParam = currentUrl.searchParams.get("password");
  if (passwordParam) {
    return passwordParam;
  }

  // 最后尝试从会话存储中获取密码
  try {
    const sessionPassword = sessionStorage.getItem(`file_password_${props.fileInfo.slug}`);
    if (sessionPassword) {
      return sessionPassword;
    }
  } catch (err) {
    console.error("从会话存储获取密码出错:", err);
  }

  return null;
};

// 处理预览URL，确保在使用Worker代理时携带密码参数
const processedPreviewUrl = computed(() => {
  let previewUrl = props.fileUrls.previewUrl;

  // 如果没有预览URL，则返回空字符串
  if (!previewUrl) {
    return "";
  }

  // 1. 添加密码参数(如果需要)
  const filePassword = getFilePassword();
  if (props.fileInfo.requires_password && filePassword && !previewUrl.includes("password=")) {
    previewUrl += (previewUrl.includes("?") ? "&" : "?") + `password=${encodeURIComponent(filePassword)}`;
  }

  return previewUrl;
});

// 格式化的文件大小
const formattedSize = computed(() => {
  return formatFileSize(props.fileInfo.size || 0);
});

// 格式化的MIME类型
const formattedMimeType = computed(() => {
  return props.fileInfo.filename || props.fileInfo.name || "";
});

// 格式化的创建时间
const formattedCreatedAt = computed(() => {
  return formatDateTime(props.fileInfo.created_at);
});

// 格式化的过期时间
const formattedExpiresAt = computed(() => {
  return formatDateTime(props.fileInfo.expires_at);
});

// 文件信息（直接使用后端type字段）

// 文件类型判断计算属性 - 遵循 usePreviewRenderers.js 的标准模式
const isOfficeFile = computed(() => props.fileInfo.type === FileType.OFFICE);
const isText = computed(() => props.fileInfo.type === FileType.TEXT);
const isImage = computed(() => props.fileInfo.type === FileType.IMAGE);
const isVideo = computed(() => props.fileInfo.type === FileType.VIDEO);
const isAudio = computed(() => props.fileInfo.type === FileType.AUDIO);

// 文本文件的细分类型判断 - 使用标准化的工具类函数
const textPreviewMode = computed(() => {
  if (!isText.value) return null;
  return getPreviewModeFromFilename(props.fileInfo.filename || "");
});

const isCode = computed(() => textPreviewMode.value === PREVIEW_MODES.CODE);
const isMarkdown = computed(() => textPreviewMode.value === PREVIEW_MODES.MARKDOWN);
const isHtml = computed(() => textPreviewMode.value === PREVIEW_MODES.HTML);

// PDF文件判断
const isPdf = computed(() => {
  return props.fileInfo.type === FileType.DOCUMENT;
});

// 文件图标类名 - 使用标准的 getIconType 函数
const iconClass = computed(() => {
  const iconType = getIconType(props.fileInfo);
  // 根据文件类型返回对应的图标颜色类
  const colorMap = {
    image: "text-green-500",
    video: "text-purple-500",
    audio: "text-blue-500",
    text: "text-yellow-500",
    document: "text-red-500",
    folder: "text-blue-500",
    file: "text-gray-500",
  };
  return colorMap[iconType] || "text-gray-500";
});

const currentPreviewComponent = computed(() => {
  const componentName = getPreviewComponent(props.fileInfo);

  // 组件映射
  const componentMap = {
    ImagePreview,
    VideoPreview,
    AudioPreview,
    PdfPreview,
    CodePreview,
    MarkdownPreview,
    HtmlPreview,
    TextPreview,
    OfficePreview,
    GenericPreview,
  };

  return componentMap[componentName] || GenericPreview;
});

// 是否应该显示预览
const shouldShowPreview = computed(() => {
  return props.fileUrls.previewUrl || isOfficeFile.value;
});

// 注意：预览能力检查现在通过 shouldShowPreview 计算属性处理

// 获取代码文件的语言类型
const getCodeLanguage = computed(() => {
  if (!props.fileInfo.filename) return t("fileView.preview.code.title");

  const extension = props.fileInfo.filename.split(".").pop().toLowerCase();
  const languageMap = {
    // 编程语言
    js: "JavaScript",
    ts: "TypeScript",
    py: "Python",
    java: "Java",
    c: "C",
    cpp: "C++",
    cs: "C#",
    go: "Go",
    php: "PHP",
    rb: "Ruby",
    swift: "Swift",
    kt: "Kotlin",
    rs: "Rust",
    sh: "Shell",
    sql: "SQL",
    // UI/前端
    css: "CSS",
    scss: "SCSS",
    less: "LESS",
    vue: "Vue",
    jsx: "JSX",
    tsx: "TSX",
  };

  return languageMap[extension] || t("fileView.preview.code.title");
});

// 删除getConfigLanguage - 不再区分配置文件

// 动态组件属性配置
const previewComponentProps = computed(() => {
  const baseProps = {
    previewUrl: processedPreviewUrl.value,
    filename: props.fileInfo.filename,
    mimetype: props.fileInfo.mimetype,
  };

  if (isText.value || isCode.value) {
    return {
      ...baseProps,
      title: isCode.value ? t("fileView.preview.code.title") : t("fileView.preview.text.title"),
      language: isCode.value ? getCodeLanguage.value : "",
      loadingText: isCode.value ? t("fileView.preview.code.loading") : t("fileView.preview.text.loading"),
      darkMode: props.darkMode,
    };
  }

  if (isMarkdown.value) {
    return {
      ...baseProps,
      darkMode: props.darkMode,
    };
  }

  if (isHtml.value) {
    return {
      ...baseProps,
      darkMode: props.darkMode,
    };
  }

  // PDF文件特殊处理
  if (isPdf.value) {
    return baseProps;
  }

  if (isOfficeFile.value) {
    return {
      microsoftOfficePreviewUrl: microsoftOfficePreviewUrl.value,
      googleDocsPreviewUrl: googleDocsPreviewUrl.value,
      mimetype: props.fileInfo.mimetype,
      filename: props.fileInfo.filename,
      useProxy: props.fileInfo.use_proxy,
      downloadUrl: props.fileUrls.downloadUrl,
    };
  }

  if (isImage.value || isAudio.value) {
    return baseProps;
  }

  if (isVideo.value) {
    return {
      ...baseProps,
      darkMode: props.darkMode,
    };
  }

  return {
    iconClass: iconClass.value,
    filename: props.fileInfo.filename,
    mimetype: props.fileInfo.mimetype,
  };
});

// Office预览URL状态
const microsoftOfficePreviewUrl = ref("");
const googleDocsPreviewUrl = ref("");

// 导入 LRU 缓存
import { officePreviewCache } from "../../utils/lruCache.js";

// Office预览错误状态
const officePreviewError = ref("");
// Office预览加载状态
const officePreviewLoading = ref(true);
// Office预览超时状态
const officePreviewTimedOut = ref(false);
// 预览超时计时器ID
const previewTimeoutId = ref(null);

// 是否使用Google Docs预览 (可以通过配置或自动检测确定)
const useGoogleDocsPreview = ref(false);

// Office在线预览服务配置
const officePreviewConfig = ref({
  // 默认使用Microsoft Office Online Viewer
  defaultService: "microsoft", // 'microsoft' 或 'google'
  // 自动故障转移到另一个服务
  enableAutoFailover: true,
  // 加载超时(毫秒)
  loadTimeout: 10000,
});

// 当前Office直接访问URL (用于Worker代理模式)
const officeDirectUrl = ref("");

// 动态组件事件处理
const handlePreviewLoad = () => {
  console.log("预览加载完成");
};

const handlePreviewError = (error) => {
  console.error("预览加载失败:", error);
};

const handleToggleMode = (mode) => {
  console.log("HTML预览模式切换:", mode);
};

const handleToggleService = (useGoogle) => {
  useGoogleDocsPreview.value = useGoogle;
};

const handleUpdateUrls = () => {
  if (isOfficeFile.value) {
    updateOfficePreviewUrls();
  }
};

// 复制到剪贴板函数
const copyToClipboard = async (text) => {
  try {
    const success = await clipboardCopy(text);

    if (success) {
      // 显示复制成功提示
      showCopyToast.value = true;
      // 3秒后自动隐藏提示
      setTimeout(() => {
        showCopyToast.value = false;
      }, 3000);
      console.log("复制成功");
    } else {
      throw new Error("复制失败");
    }
  } catch (err) {
    console.error("复制失败:", err);
    // 复制失败时也显示提示，但内容不同
    console.error("复制失败，需要手动复制");
    // 这里可以添加更友好的错误提示，比如显示一个模态框让用户手动复制
  }
};

// 确保密码被保存到会话存储
const savePasswordToSessionStorage = () => {
  if (!props.fileInfo.slug) return;

  try {
    // 获取潜在的密码源
    let password = null;

    // 1. 首先检查props.fileInfo.currentPassword
    if (props.fileInfo.currentPassword) {
      password = props.fileInfo.currentPassword;
    }
    // 2. 其次检查URL中的密码参数
    else {
      const currentUrl = new URL(window.location.href);
      const passwordParam = currentUrl.searchParams.get("password");
      if (passwordParam) {
        password = passwordParam;
      }
    }

    // 如果找到了密码，保存到会话存储
    if (password) {
      console.log("保存密码到会话存储", { slug: props.fileInfo.slug });
      sessionStorage.setItem(`file_password_${props.fileInfo.slug}`, password);
    }
  } catch (err) {
    console.error("保存密码到会话存储出错:", err);
  }
};

// 获取Office文件直接访问URL (用于Worker代理模式)
const getOfficeDirectUrlForPreview = async () => {
  if (!props.fileInfo.slug) return null;

  try {
    // 获取可能的密码
    const filePassword = getFilePassword();

    if (!filePassword && props.fileInfo.password) {
      // 如果文件确实需要密码但我们没有获取到，记录日志
      console.warn("文件需要密码，但无法获取到密码", {
        hasCurrentPassword: !!props.fileInfo.currentPassword,
        hasUrlPassword: !!new URL(window.location.href).searchParams.get("password"),
        hasSessionStorage: !!sessionStorage.getItem(`file_password_${props.fileInfo.slug}`),
      });
    }

    console.log("正在获取Office直接URL", { slug: props.fileInfo.slug, hasPassword: !!filePassword });

    // 使用统一的预览服务获取所有预览URL
    const previewUrls = await api.fileView.getOfficePreviewUrl(props.fileInfo.slug, {
      password: filePassword,
      returnAll: true,
    });

    // 缓存获取的直接URL
    officeDirectUrl.value = previewUrls.directUrl;

    // 确保密码被保存到会话存储中以便后续使用
    if (filePassword && props.fileInfo.slug) {
      try {
        sessionStorage.setItem(`file_password_${props.fileInfo.slug}`, filePassword);
      } catch (err) {
        console.error("保存密码到会话存储失败:", err);
      }
    }

    return previewUrls.directUrl;
  } catch (error) {
    console.error("获取Office直接URL出错:", error);
    officePreviewError.value = `获取预览失败: ${error.message || "未知错误"}`;
    return null;
  }
};

// 更新Office预览URL
const updateOfficePreviewUrls = async () => {
  // 重置加载状态
  officePreviewLoading.value = true;
  officePreviewError.value = "";

  // 记录密码状态
  const filePassword = getFilePassword();

  try {
    // 生成缓存键
    const cacheKey = `${props.fileInfo.slug}_${props.fileInfo.use_proxy ? "proxy" : "direct"}_${filePassword || "no_password"}`;

    // 检查缓存
    if (officePreviewCache.has(cacheKey)) {
      const cachedUrls = officePreviewCache.get(cacheKey);
      console.log("使用缓存的Office预览URL", { cacheKey });

      microsoftOfficePreviewUrl.value = cachedUrls.microsoft;
      googleDocsPreviewUrl.value = cachedUrls.google;

      // 开始预览加载超时计时
      startPreviewLoadTimeout();
      return;
    }

    // 如果是Worker代理模式，需要特殊处理
    if (props.fileInfo.use_proxy) {
      // 使用专门的API获取临时直接URL
      const directUrl = await getOfficeDirectUrlForPreview();

      if (directUrl) {
        // 使用统一的预览服务
        const previewUrls = await api.fileView.getOfficePreviewUrl({ directUrl }, { returnAll: true });

        microsoftOfficePreviewUrl.value = previewUrls.microsoft;
        googleDocsPreviewUrl.value = previewUrls.google;

        // 缓存URL（代理模式的URL有时效性，缓存时间较短 - 10分钟）
        officePreviewCache.set(
          cacheKey,
          {
            microsoft: previewUrls.microsoft,
            google: previewUrls.google,
          },
          10 * 60 * 1000
        );

        console.log("缓存Office预览URL (代理模式)", { cacheKey });
      } else {
        microsoftOfficePreviewUrl.value = "";
        googleDocsPreviewUrl.value = "";
        officePreviewError.value = "获取Office预览URL失败";
      }
    } else {
      // S3直链模式，正常处理
      if (!processedPreviewUrl.value) {
        microsoftOfficePreviewUrl.value = "";
        googleDocsPreviewUrl.value = "";
        return;
      }

      // 确保URL是完整的绝对URL
      let url = processedPreviewUrl.value;
      if (!url.startsWith("http://") && !url.startsWith("https://")) {
        // 如果是相对URL，转换为绝对URL
        const baseUrl = window.location.origin;
        url = url.startsWith("/") ? `${baseUrl}${url}` : `${baseUrl}/${url}`;
      }

      // 使用统一的预览服务
      const previewUrls = await api.fileView.getOfficePreviewUrl({ directUrl: url }, { returnAll: true });

      microsoftOfficePreviewUrl.value = previewUrls.microsoft;
      googleDocsPreviewUrl.value = previewUrls.google;

      // 缓存URL（直链模式的URL相对稳定，可以缓存更长时间 - 30分钟）
      officePreviewCache.set(
        cacheKey,
        {
          microsoft: previewUrls.microsoft,
          google: previewUrls.google,
        },
        30 * 60 * 1000
      );

      console.log("缓存Office预览URL (直链模式)", { cacheKey });
    }

    // 开始预览加载超时计时
    startPreviewLoadTimeout();
  } catch (error) {
    console.error("更新Office预览URL出错:", error);
    officePreviewError.value = `更新预览URL失败: ${error.message || "未知错误"}`;
    officePreviewLoading.value = false;
  }
};

// 开始预览加载超时计时
const startPreviewLoadTimeout = () => {
  // 清除可能存在的上一个计时器
  if (previewTimeoutId.value) {
    clearTimeout(previewTimeoutId.value);
  }

  // 重置超时状态
  officePreviewTimedOut.value = false;

  // 设置新的超时计时器
  previewTimeoutId.value = setTimeout(() => {
    console.warn("Office预览加载超时");
    officePreviewError.value = "预览加载超时，请尝试切换预览服务或下载文件后查看。";
    officePreviewTimedOut.value = true;
    officePreviewLoading.value = false;
  }, officePreviewConfig.value.loadTimeout);
};

// 清理过期的Office预览URL缓存（现在由 LRU 缓存自动处理）
const cleanExpiredCache = () => {
  const cleaned = officePreviewCache.cleanup();
  if (cleaned > 0) {
    console.log(`清理了 ${cleaned} 个过期的Office预览URL缓存项`);
  }
};

// 初始化
onMounted(() => {
  // 根据默认配置设置预览服务
  useGoogleDocsPreview.value = officePreviewConfig.value.defaultService === "google";

  // 确保密码被保存到会话存储
  savePasswordToSessionStorage();

  // 清理过期缓存
  cleanExpiredCache();

  // 如果是Office文件，更新预览URL
  if (isOfficeFile.value) {
    updateOfficePreviewUrls();
  }
});

// 监听预览URL变化（预览组件会自动响应URL变化）
watch(
  () => processedPreviewUrl.value,
  (newUrl) => {
    console.log("预览URL变化:", newUrl);
  },
  { immediate: true }
);

// 组件卸载时清理资源
onUnmounted(() => {
  // 清理预览超时计时器
  if (previewTimeoutId.value) {
    clearTimeout(previewTimeoutId.value);
    previewTimeoutId.value = null;
  }

  // 清理复制提示定时器
  if (showCopyToast.value) {
    showCopyToast.value = false;
  }
});
</script>
