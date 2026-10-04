<template>
  <div class="folder-share-container flex flex-col flex-1 pt-6 sm:pt-8">
    <!-- 面包屑导航标题 -->
    <div class="max-w-6xl mx-auto w-full px-3 sm:px-6">
      <div class="py-3 text-sm text-gray-500 dark:text-gray-400 border-b border-gray-200 dark:border-gray-700 mb-4">
        <a href="/" class="hover:text-primary-600 dark:hover:text-primary-400">{{ t("nav.home") }}</a>
        <span class="mx-2">/</span>
        <span class="text-gray-700 dark:text-gray-300">{{ t("fileView.folder.title") }}</span>
      </div>
    </div>

    <!-- 错误状态（过期 / 不存在 / 其它） -->
    <div v-if="errorKind" class="error-container py-12 px-3 sm:px-6 max-w-6xl mx-auto text-center">
      <IconExclamation class="h-16 w-16 mx-auto mb-4 text-red-600 dark:text-red-500" />
      <h2 class="text-2xl font-bold mb-2 text-gray-900 dark:text-white">
        {{ errorKind === "expired" ? t("fileView.folder.errors.expired") : errorKind === "notFound" ? t("fileView.folder.errors.notFound") : t("fileView.folder.errorState") }}
      </h2>
      <p class="text-lg mb-6 text-gray-600 dark:text-gray-300">{{ errorMessage }}</p>
      <a
        href="/"
        class="inline-flex items-center justify-center px-4 py-2 border border-transparent rounded-md shadow-sm text-base font-medium text-white bg-blue-600 hover:bg-blue-700"
      >
        {{ t("common.back") }}
      </a>
    </div>

    <!-- 加载中 -->
    <div v-else-if="loading" class="loading-container py-12 px-3 sm:px-6 max-w-6xl mx-auto text-center">
      <LoadingIndicator
        :text="t('fileView.folder.loading')"
        :dark-mode="darkMode"
        size="4xl"
        :icon-class="darkMode ? 'text-blue-400' : 'text-blue-600'"
        :text-class="darkMode ? 'text-gray-300' : 'text-gray-600'"
      />
    </div>

    <div v-else class="folder-content flex-1 flex flex-col py-6 px-3 sm:px-6 max-w-6xl mx-auto w-full">
      <!-- 密码验证界面 -->
      <div v-if="requiresPassword" class="password-container flex-1 flex items-start justify-center pt-8">
        <div class="max-w-sm w-full mx-auto p-5 border rounded-lg shadow-sm bg-white dark:bg-gray-800 border-gray-200 dark:border-gray-700">
          <h3 class="text-lg font-medium mb-4 text-gray-900 dark:text-white">{{ t("fileView.folder.password.title") }}</h3>
          <p class="mb-4 text-sm text-gray-600 dark:text-gray-300">{{ t("fileView.folder.password.description") }}</p>

          <form @submit.prevent="handlePasswordSubmit" class="space-y-4">
            <div>
              <label for="folder-password" class="block text-sm font-medium mb-1 text-gray-700 dark:text-gray-300">
                {{ t("fileView.folder.password.label") }}
              </label>
              <input
                id="folder-password"
                type="password"
                autocomplete="current-password"
                v-model="passwordInput"
                :placeholder="t('fileView.folder.password.placeholder')"
                class="block w-full px-3 py-2 rounded-md shadow-sm border border-gray-300 dark:border-gray-600 text-gray-900 dark:text-white dark:bg-gray-700 focus:ring-2 focus:ring-primary-500 focus:border-primary-500"
                :disabled="passwordLoading"
              />
              <p v-if="passwordError" class="mt-2 text-sm text-red-500 dark:text-red-400">{{ passwordError }}</p>
            </div>

            <button
              type="submit"
              class="w-full px-4 py-2 text-sm font-medium text-white bg-primary-600 rounded-md hover:bg-primary-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500 dark:focus:ring-offset-gray-800 disabled:opacity-60"
              :disabled="passwordLoading || !passwordInput"
            >
              <span v-if="passwordLoading">{{ t("fileView.folder.password.loading") }}</span>
              <span v-else>{{ t("fileView.folder.password.submit") }}</span>
            </button>
          </form>
        </div>
      </div>

      <!-- 目录浏览界面 -->
      <div v-else class="folder-browser flex flex-col flex-1">
        <!-- 头部：名称 + 备注 + 元信息 -->
        <div class="folder-header mb-6">
          <div class="flex items-center gap-3">
            <div class="flex items-center justify-center w-12 h-12 rounded-xl bg-gray-100 dark:bg-gray-700">
              <IconFolder class="h-6 w-6 text-blue-500" />
            </div>
            <div class="flex-1 min-w-0">
              <h1 class="text-xl font-bold truncate text-gray-900 dark:text-white">{{ meta.filename || slug }}</h1>
              <p class="text-sm text-gray-500 dark:text-gray-400">
                {{ t("fileView.folder.itemCount", { count: items.length }) }}
              </p>
            </div>
          </div>

          <div v-if="meta.remark" class="mt-4 px-4 py-3 rounded-lg bg-gray-100 dark:bg-gray-700 overflow-auto max-h-[300px]">
            <p class="text-blue-600 dark:text-blue-400 break-words whitespace-pre-wrap">{{ meta.remark }}</p>
          </div>

          <!-- 元信息：剩余下载次数 / 过期时间 -->
          <div class="mt-4 grid grid-cols-1 sm:grid-cols-2 gap-3">
            <div v-if="hasDownloadLimit" class="flex items-center gap-2 p-3 rounded-lg bg-gray-100 dark:bg-gray-700">
              <IconDownload size="md" class="text-gray-500 dark:text-gray-400" />
              <div>
                <div class="text-sm font-medium text-gray-600 dark:text-gray-200">{{ t("fileView.folder.meta.remainingDownloads") }}</div>
                <div class="text-sm text-gray-800 dark:text-white">
                  {{ remainingDownloads }}
                  <span class="text-xs text-gray-500 dark:text-gray-400">/ {{ meta.max_views }}</span>
                </div>
              </div>
            </div>
            <div v-if="meta.expires_at" class="flex items-center gap-2 p-3 rounded-lg bg-gray-100 dark:bg-gray-700">
              <IconClock size="md" class="text-gray-500 dark:text-gray-400" />
              <div>
                <div class="text-sm font-medium text-gray-600 dark:text-gray-200">{{ t("fileView.folder.meta.expiresAt") }}</div>
                <div class="text-sm text-gray-800 dark:text-white">{{ formattedExpiresAt }}</div>
              </div>
            </div>
          </div>
        </div>

        <!-- 目录内面包屑 -->
        <div class="flex items-center flex-wrap gap-1 mb-3 text-sm">
          <button
            type="button"
            class="inline-flex items-center px-2 py-1 rounded hover:bg-gray-100 dark:hover:bg-gray-700 text-gray-700 dark:text-gray-200"
            :class="{ 'font-semibold text-primary-600 dark:text-primary-400': breadcrumbSegments.length === 0 }"
            @click="navigateTo('')"
          >
            <IconHome size="sm" class="mr-1" />
            {{ meta.filename || slug }}
          </button>
          <template v-for="(segment, index) in breadcrumbSegments" :key="index">
            <IconChevronRight size="sm" class="text-gray-400" />
            <button
              type="button"
              class="px-2 py-1 rounded hover:bg-gray-100 dark:hover:bg-gray-700 text-gray-700 dark:text-gray-200"
              :class="{ 'font-semibold text-primary-600 dark:text-primary-400': index === breadcrumbSegments.length - 1 }"
              @click="navigateTo(breadcrumbPath(index))"
            >
              {{ segment }}
            </button>
          </template>
          <div class="ml-auto flex items-center gap-2">
            <ViewModeToggle
              v-model="viewMode"
              :options="viewModeOptions"
              :dark-mode="darkMode"
              size="sm"
            />
            <button
              type="button"
              class="inline-flex items-center px-2 py-1 rounded text-gray-500 dark:text-gray-400 hover:bg-gray-100 dark:hover:bg-gray-700"
              :disabled="listLoading"
              @click="refreshListing"
            >
              <IconRefresh size="sm" class="mr-1" :class="{ 'animate-spin': listLoading }" />
              {{ t("fileView.folder.actions.refresh") }}
            </button>
          </div>
        </div>

        <!-- 条目列表 -->
        <div class="folder-list flex-1 rounded-lg border border-gray-200 dark:border-gray-700 overflow-hidden">
          <div v-if="listLoading" class="py-10 text-center text-gray-500 dark:text-gray-400">
            <LoadingIndicator :text="t('fileView.folder.loading')" :dark-mode="darkMode" size="2xl" />
          </div>

          <div v-else-if="sortedItems.length === 0" class="py-10 text-center text-gray-500 dark:text-gray-400">
            {{ t("fileView.folder.emptyFolder") }}
          </div>

          <ul v-else-if="viewMode === 'list'" class="divide-y divide-gray-200 dark:divide-gray-700">
            <li
              v-for="item in sortedItems"
              :key="item.path"
              class="flex items-center gap-3 px-3 py-2.5 hover:bg-gray-50 dark:hover:bg-gray-800"
            >
              <!-- 图标 + 名称 -->
              <button
                v-if="item.isDirectory"
                type="button"
                class="flex items-center gap-3 flex-1 min-w-0 text-left"
                @click="navigateTo(item.path)"
              >
                <IconFolder class="h-5 w-5 flex-shrink-0 text-blue-500" />
                <span class="truncate text-gray-900 dark:text-white">{{ item.name }}</span>
              </button>
              <div v-else class="flex items-center gap-3 flex-1 min-w-0">
                <IconDocumentText class="h-5 w-5 flex-shrink-0 text-gray-400" />
                <button
                  type="button"
                  class="truncate text-left text-gray-900 dark:text-white hover:text-primary-600 dark:hover:text-primary-400"
                  :title="item.name"
                  @click="openPreview(item)"
                >
                  {{ item.name }}
                </button>
              </div>

              <!-- 大小 / 时间 -->
              <span class="hidden sm:block w-24 text-right text-xs text-gray-500 dark:text-gray-400 flex-shrink-0">
                {{ item.isDirectory ? "-" : formatSize(item.size) }}
              </span>
              <span class="hidden md:block w-40 text-right text-xs text-gray-500 dark:text-gray-400 flex-shrink-0">
                {{ item.modified_at ? formatModified(item.modified_at) : "-" }}
              </span>

              <!-- 操作 -->
              <div class="flex items-center gap-1 flex-shrink-0">
                <template v-if="!item.isDirectory">
                  <button
                    v-if="canPreview(item.name)"
                    type="button"
                    class="p-1.5 rounded text-gray-500 dark:text-gray-400 hover:bg-gray-200 dark:hover:bg-gray-700"
                    :title="t('fileView.folder.actions.preview')"
                    @click="openPreview(item)"
                  >
                    <IconEye size="sm" />
                  </button>
                  <button
                    type="button"
                    class="p-1.5 rounded text-gray-500 dark:text-gray-400 hover:bg-gray-200 dark:hover:bg-gray-700"
                    :title="t('fileView.folder.actions.download')"
                    @click="downloadFile(item)"
                  >
                    <IconDownload size="sm" />
                  </button>
                </template>
                <button
                  v-else
                  type="button"
                  class="p-1.5 rounded text-gray-500 dark:text-gray-400 hover:bg-gray-200 dark:hover:bg-gray-700"
                  :title="t('fileView.folder.actions.openFolder')"
                  @click="navigateTo(item.path)"
                >
                  <IconChevronRight size="sm" />
                </button>
              </div>
            </li>
          </ul>

          <!-- 宫格视图 -->
          <div v-else class="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-5 gap-3 p-3">
            <button
              v-for="item in sortedItems"
              :key="item.path"
              type="button"
              class="group flex flex-col items-center gap-2 p-2 rounded-lg border border-gray-200 dark:border-gray-700 hover:bg-gray-50 dark:hover:bg-gray-800 text-center"
              :title="item.name"
              @click="handleItemClick(item)"
            >
              <div class="w-full aspect-square flex items-center justify-center overflow-hidden rounded-md bg-gray-100 dark:bg-gray-700">
                <img
                  v-if="!item.isDirectory && detectPreviewKind(item.name) === 'image'"
                  :src="buildThumbUrl(item)"
                  :alt="item.name"
                  class="w-full h-full object-cover"
                  loading="lazy"
                />
                <IconFolder v-else-if="item.isDirectory" class="h-10 w-10 text-blue-500" />
                <IconDocumentText v-else class="h-10 w-10 text-gray-400 dark:text-gray-500" />
              </div>
              <span class="w-full truncate text-xs text-gray-900 dark:text-white">{{ item.name }}</span>
            </button>
          </div>
        </div>
      </div>
    </div>

    <!-- 预览模态框 -->
    <div v-if="previewTarget" class="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black bg-opacity-60" @click.self="closePreview">
      <div class="w-full max-w-4xl max-h-[90vh] flex flex-col rounded-lg shadow-xl bg-white dark:bg-gray-800 overflow-hidden">
        <div class="flex items-center justify-between px-4 py-3 border-b border-gray-200 dark:border-gray-700">
          <h3 class="text-base font-medium truncate text-gray-900 dark:text-white">
            {{ previewTarget.name }}
          </h3>
          <div class="flex items-center gap-2">
            <button
              type="button"
              class="p-1.5 rounded text-gray-500 dark:text-gray-400 hover:bg-gray-100 dark:hover:bg-gray-700"
              :title="t('fileView.folder.preview.download')"
              @click="downloadFile(previewTarget)"
            >
              <IconDownload size="sm" />
            </button>
            <button
              type="button"
              class="p-1.5 rounded text-gray-500 dark:text-gray-400 hover:bg-gray-100 dark:hover:bg-gray-700"
              :title="t('fileView.folder.preview.close')"
              @click="closePreview"
            >
              <IconX size="sm" />
            </button>
          </div>
        </div>

        <div class="flex-1 min-h-0 overflow-auto p-3 bg-gray-50 dark:bg-gray-900 flex items-center justify-center">
          <img v-if="previewKind === 'image'" :src="previewUrl" :alt="previewTarget.name" class="max-w-full max-h-[75vh] object-contain" />
          <video v-else-if="previewKind === 'video'" :src="previewUrl" controls class="max-w-full max-h-[75vh]"></video>
          <audio v-else-if="previewKind === 'audio'" :src="previewUrl" controls class="w-full"></audio>
          <iframe v-else-if="previewKind === 'pdf'" :src="previewUrl" class="w-full h-[75vh] bg-white"></iframe>
          <template v-else-if="previewKind === 'text'">
            <div v-if="previewLoading" class="text-gray-500 dark:text-gray-400 py-10">{{ t("fileView.folder.preview.loading") }}</div>
            <div v-else-if="previewError" class="text-red-500 dark:text-red-400 py-10">{{ previewError }}</div>
            <pre v-else class="w-full text-xs text-gray-800 dark:text-gray-200 whitespace-pre-wrap break-words text-left">{{ previewText }}</pre>
          </template>
          <div v-else class="text-gray-500 dark:text-gray-400 py-10 text-center">
            <p class="mb-3">{{ t("fileView.folder.preview.notSupported") }}</p>
            <button
              type="button"
              class="inline-flex items-center px-4 py-2 rounded-md text-sm font-medium text-white bg-blue-600 hover:bg-blue-700"
              @click="downloadFile(previewTarget)"
            >
              <IconDownload size="sm" class="mr-2" />
              {{ t("fileView.folder.preview.download") }}
            </button>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, onMounted, watch } from "vue";
import { useI18n } from "vue-i18n";
import { useFileshareService } from "@/modules/fileshare/fileshareService.js";
import { getFilePassword, setFilePassword } from "@/utils/filePasswordUtils.js";
import { formatFileSize, lookupMimeType } from "@/utils/fileTypes.js";
import { formatDateTime } from "@/utils/timeUtils.js";
import { createLogger } from "@/utils/logger.js";
import LoadingIndicator from "@/components/common/LoadingIndicator.vue";
import ViewModeToggle from "@/components/common/ViewModeToggle.vue";
import {
  IconChevronRight,
  IconClock,
  IconDocumentText,
  IconDownload,
  IconExclamation,
  IconEye,
  IconFolder,
  IconHome,
  IconRefresh,
  IconX,
} from "@/components/icons";

const { t } = useI18n();
const log = createLogger("FolderShareView");
const fileshareService = useFileshareService();

const props = defineProps({
  slug: {
    type: String,
    required: true,
  },
  darkMode: {
    type: Boolean,
    default: false,
  },
});

const PREVIEW_KINDS = ["image", "video", "audio", "pdf", "text"];
const TEXT_PREVIEW_MAX_CHARS = 200000;
const VIEW_MODE_STORAGE_KEY = "folderShare:viewMode";

const readStoredViewMode = () => {
  try {
    return window.localStorage.getItem(VIEW_MODE_STORAGE_KEY) === "grid" ? "grid" : "list";
  } catch {
    return "list";
  }
};

const slug = ref(props.slug);
const meta = ref({});
const items = ref([]);
const viewMode = ref(readStoredViewMode());
const viewModeOptions = [
  { value: "list", icon: "list", titleKey: "fileView.folder.viewMode.list" },
  { value: "grid", icon: "grid", titleKey: "fileView.folder.viewMode.grid" },
];
const loading = ref(true);
const listLoading = ref(false);
const errorKind = ref("");
const errorMessage = ref("");

const requiresPassword = ref(false);
const passwordInput = ref("");
const passwordLoading = ref(false);
const passwordError = ref("");
const currentPassword = ref("");
const currentPath = ref("");

const previewTarget = ref(null);
const previewKind = ref("");
const previewText = ref("");
const previewLoading = ref(false);
const previewError = ref("");

const hasDownloadLimit = computed(() => Number(meta.value.max_views) > 0);
const remainingDownloads = computed(() => {
  if (!hasDownloadLimit.value) return null;
  const remaining = Number(meta.value.max_views) - Number(meta.value.views || 0);
  return remaining > 0 ? remaining : 0;
});
const formattedExpiresAt = computed(() => (meta.value.expires_at ? formatDateTime(meta.value.expires_at) : ""));
const breadcrumbSegments = computed(() => (currentPath.value ? currentPath.value.split("/").filter(Boolean) : []));
const sortedItems = computed(() => {
  return [...items.value].sort((a, b) => {
    if (!!a.isDirectory !== !!b.isDirectory) return a.isDirectory ? -1 : 1;
    return String(a.name || "").localeCompare(String(b.name || ""));
  });
});
const previewUrl = computed(() => {
  if (!previewTarget.value) return "";
  // 预览使用内联入口（不强制下载），避免 PDF 等被浏览器当成附件下载
  return fileshareService.buildFolderPreviewUrl(slug.value, previewTarget.value.path, currentPassword.value);
});

/** 将错误映射为页面级状态 */
const classifyError = (err) => {
  const code = err?.code;
  const status = err?.status ?? err?.response?.status;
  const msg = err?.message || "";
  if (code === "GONE" || status === 410 || msg.includes("已过期") || msg.includes("下载次数")) return "expired";
  if (code === "NOT_FOUND" || status === 404 || msg.includes("不存在")) return "notFound";
  return "generic";
};

const formatSize = (size) => (typeof size === "number" && size > 0 ? formatFileSize(size) : "-");
const formatModified = (value) => formatDateTime(value);

/** 应用列表数据（元信息 + 当前路径 + 条目） */
const applyListing = (data) => {
  if (!data) return;
  meta.value = { ...meta.value, ...data };
  items.value = Array.isArray(data.items) ? data.items : [];
  currentPath.value = data.path || "";
};

/** 使用密码验证并加载根目录 */
const doVerify = async (password) => {
  const data = await fileshareService.verifyFolderPassword(slug.value, password);
  currentPassword.value = password;
  setFilePassword(slug.value, password);
  requiresPassword.value = false;
  applyListing(data);
  return data;
};

/** 加载分享元信息 */
const loadMeta = async () => {
  loading.value = true;
  errorKind.value = "";
  errorMessage.value = "";

  try {
    const data = await fileshareService.fetchFolderBySlug(slug.value);
    if (!data) {
      throw new Error(t("fileView.folder.errors.loadFailed"));
    }
    meta.value = data;

    if (data.requires_password) {
      const stored = getFilePassword({ slug: slug.value });
      if (stored) {
        try {
          await doVerify(stored);
        } catch (err) {
          const kind = classifyError(err);
          if (kind === "expired" || kind === "notFound") {
            errorKind.value = kind;
            errorMessage.value = err?.message || t(`fileView.folder.errors.${kind}`);
          } else {
            // 缓存的密码已失效：清除并展示密码输入
            setFilePassword(slug.value, "");
            currentPassword.value = "";
            requiresPassword.value = true;
          }
        }
      } else {
        requiresPassword.value = true;
      }
    } else {
      currentPassword.value = "";
      applyListing(data);
    }
  } catch (err) {
    log.error("加载文件夹分享失败:", err);
    errorKind.value = classifyError(err);
    errorMessage.value = err?.message || t("fileView.folder.errors.loadFailed");
  } finally {
    loading.value = false;
  }
};

/** 提交密码 */
const handlePasswordSubmit = async () => {
  if (!passwordInput.value) return;
  passwordLoading.value = true;
  passwordError.value = "";

  try {
    await doVerify(passwordInput.value);
  } catch (err) {
    const kind = classifyError(err);
    if (kind === "expired" || kind === "notFound") {
      errorKind.value = kind;
      errorMessage.value = err?.message || t(`fileView.folder.errors.${kind}`);
    } else {
      passwordError.value = t("fileView.folder.errors.wrongPassword");
    }
  } finally {
    passwordLoading.value = false;
  }
};

/** 进入指定相对路径（"" 为分享根目录） */
const navigateTo = async (relPath) => {
  const target = relPath || "";
  if (target === currentPath.value) return;
  await loadListing(target);
};

/** 重新加载当前目录 */
const refreshListing = () => loadListing(currentPath.value);

const loadListing = async (relPath) => {
  const target = relPath || "";
  listLoading.value = true;
  try {
    const data = await fileshareService.fetchFolderList(slug.value, {
      path: target,
      password: currentPassword.value,
    });
    applyListing(data);
    window.scrollTo({ top: 0, behavior: "smooth" });
  } catch (err) {
    log.error("加载目录失败:", err);
    const kind = classifyError(err);
    if (kind === "expired" || kind === "notFound") {
      errorKind.value = kind;
      errorMessage.value = err?.message || t(`fileView.folder.errors.${kind}`);
    }
  } finally {
    listLoading.value = false;
  }
};

const breadcrumbPath = (index) => breadcrumbSegments.value.slice(0, index + 1).join("/");

/** 下载单个文件（走后端代理，可能消耗下载次数） */
const downloadFile = (item) => {
  if (!item || item.isDirectory) return;
  const url = fileshareService.buildFolderDownloadUrl(slug.value, item.path, currentPassword.value);
  if (!url) return;
  const link = document.createElement("a");
  link.href = url;
  link.rel = "noopener";
  document.body.appendChild(link);
  link.click();
  setTimeout(() => document.body.removeChild(link), 100);
};

/** 宫格视图缩略图 URL（复用内联预览入口） */
const buildThumbUrl = (item) => fileshareService.buildFolderPreviewUrl(slug.value, item.path, currentPassword.value);

/** 宫格条目点击：目录进入、可预览文件打开预览、其余直接下载 */
const handleItemClick = (item) => {
  if (item.isDirectory) return navigateTo(item.path);
  if (canPreview(item.name)) return openPreview(item);
  downloadFile(item);
};

/** 依据扩展名/MIME 判定可预览类型 */
const detectPreviewKind = (name) => {
  const mime = lookupMimeType(name) || "";
  if (mime.startsWith("image/")) return "image";
  if (mime.startsWith("video/")) return "video";
  if (mime.startsWith("audio/")) return "audio";
  if (mime === "application/pdf") return "pdf";
  if (mime.startsWith("text/") || /(json|javascript|xml|yaml|csv)/.test(mime)) return "text";
  return "";
};

const canPreview = (name) => PREVIEW_KINDS.includes(detectPreviewKind(name));

const openPreview = async (item) => {
  if (!item || item.isDirectory) return;
  previewTarget.value = item;
  previewKind.value = detectPreviewKind(item.name);
  previewText.value = "";
  previewError.value = "";
  previewLoading.value = false;

  if (previewKind.value === "text") {
    previewLoading.value = true;
    try {
      const res = await fetch(previewUrl.value);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const text = await res.text();
      previewText.value =
        text.length > TEXT_PREVIEW_MAX_CHARS ? `${text.slice(0, TEXT_PREVIEW_MAX_CHARS)}\n...` : text;
    } catch (err) {
      log.error("预览文本失败:", err);
      previewError.value = t("fileView.folder.preview.error");
    } finally {
      previewLoading.value = false;
    }
  }
};

const closePreview = () => {
  previewTarget.value = null;
  previewKind.value = "";
  previewText.value = "";
  previewError.value = "";
  previewLoading.value = false;
};

onMounted(() => {
  loadMeta();
});

watch(viewMode, (mode) => {
  try {
    window.localStorage.setItem(VIEW_MODE_STORAGE_KEY, mode);
  } catch {
    // 忽略存储不可用（隐私模式等）
  }
});

watch(
  () => props.slug,
  (newSlug) => {
    if (newSlug && newSlug !== slug.value) {
      slug.value = newSlug;
      // 切换分享时重置浏览状态（密码改由会话存储按 slug 恢复）
      items.value = [];
      currentPath.value = "";
      currentPassword.value = "";
      requiresPassword.value = false;
      closePreview();
      loadMeta();
    }
  }
);
</script>
