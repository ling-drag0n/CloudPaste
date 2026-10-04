<template>
  <div class="file-view-container flex flex-col flex-1 pt-6 sm:pt-8">
    <!-- 添加面包屑导航标题，与文本分享页面风格一致 -->
    <div class="max-w-6xl mx-auto w-full px-3 sm:px-6">
      <div class="py-3 text-sm text-gray-500 dark:text-gray-400 border-b border-gray-200 dark:border-gray-700 mb-4">
        <a href="/" class="hover:text-primary-600 dark:hover:text-primary-400">{{ t("nav.home") }}</a>
        <span class="mx-2">/</span>
        <span class="text-gray-700 dark:text-gray-300">{{ t("fileView.title") }}</span>
      </div>
    </div>

    <div v-if="error" class="error-container py-12 px-3 sm:px-6 max-w-6xl mx-auto text-center">
      <IconExclamation class="h-16 w-16 mx-auto mb-4 text-red-600 dark:text-red-500" />
      <h2 class="text-2xl font-bold mb-2 text-gray-900 dark:text-white">{{ t("fileView.error") }}</h2>
      <p class="text-lg mb-6 text-gray-600 dark:text-gray-300">{{ error }}</p>
      <a
        href="/"
        class="inline-flex items-center justify-center px-4 py-2 border border-transparent rounded-md shadow-sm text-base font-medium text-white bg-blue-600 hover:bg-blue-700"
      >
        {{ t("common.back") }}
      </a>
    </div>

    <!-- 删除成功提示 -->
    <div v-else-if="showDeleteSuccess" class="success-container py-12 px-3 sm:px-6 max-w-6xl mx-auto text-center">
      <IconCheck class="h-16 w-16 mx-auto mb-4 text-green-600 dark:text-green-500" />
      <h2 class="text-2xl font-bold mb-2 text-gray-900 dark:text-white">{{ t("fileView.actions.deleteSuccess") }}</h2>
      <p class="text-lg mb-6 text-gray-600 dark:text-gray-300">{{ t("fileView.actions.redirectMessage") }}</p>
      <div class="animate-pulse text-gray-500 dark:text-gray-400">{{ redirectCountdown }} {{ t("fileView.actions.redirecting") }}</div>
    </div>

    <div v-else-if="loading" class="loading-container py-12 px-3 sm:px-6 max-w-6xl mx-auto text-center">
      <LoadingIndicator
        :text="t('fileView.loading')"
        :dark-mode="darkMode"
        size="4xl"
        :icon-class="darkMode ? 'text-blue-400' : 'text-blue-600'"
        :text-class="darkMode ? 'text-gray-300' : 'text-gray-600'"
      />
    </div>

    <div v-else class="file-container flex-1 flex flex-col py-8 px-4 max-w-4xl mx-auto w-full">
      <!-- 密码验证界面 -->
      <div v-if="requiresPassword" class="password-container flex-1 flex items-start justify-center pt-8">
        <FileViewPassword :fileId="fileInfo.slug" @verified="handlePasswordVerified" />
      </div>

      <!-- 文件信息和操作界面 -->
      <div v-else class="file-content flex flex-col flex-1">
        <!-- 文件信息 -->
        <FileViewInfo :fileInfo="fileInfo" :darkMode="darkMode" />

        <!-- 文件操作按钮 -->
        <FileViewActions :fileInfo="fileInfo" :darkMode="darkMode" @edit="openEditModal" @delete="handleFileDeleted" @refresh-file-info="refreshFileInfo" />
      </div>

      <!-- 编辑模态框 (仅管理员可见) -->
      <FileEditModal v-if="showEditModal" :file="fileInfo" @close="closeEditModal" @save="saveFileChanges" />
    </div>

  </div>
</template>

<script setup>
import { ref, computed, onMounted, onUnmounted, watch } from "vue";
import { useIntervalFn } from "@vueuse/core";
import { useRouter, useRoute } from "vue-router";
import { useI18n } from "vue-i18n";
import { useAuthStore } from "@/stores/authStore.js";
import { useFileshareService } from "@/modules/fileshare/fileshareService.js";
import { useFileShareStore } from "@/modules/fileshare/fileShareStore.js";
import { useGlobalMessage } from "@/composables/core/useGlobalMessage.js";
import { setFilePassword } from "@/utils/filePasswordUtils.js";
import { createLogger } from "@/utils/logger.js";
import { IconCheck, IconExclamation } from "@/components/icons";
import LoadingIndicator from "@/components/common/LoadingIndicator.vue";

const { t } = useI18n();
const log = createLogger("FileView");
const fileshareService = useFileshareService();
const fileShareStore = useFileShareStore();
const { showError, showSuccess } = useGlobalMessage();

// 导入子组件
import FileViewInfo from "@/modules/fileshare/public/components/FileViewInfo.vue";
import FileViewPassword from "@/modules/fileshare/public/components/FileViewPassword.vue";
import FileViewActions from "@/modules/fileshare/public/components/FileViewActions.vue";
import FileEditModal from "@/components/file/FileEditModal.vue";

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

const router = useRouter();
const route = useRoute();

// 状态变量
const slug = ref(props.slug);
const fileInfo = ref({});
const loading = ref(true);
const error = ref("");
const requiresPassword = ref(false);
const showEditModal = ref(false);

// 删除成功状态
const showDeleteSuccess = ref(false);
const redirectCountdown = ref(3);
const { pause: stopRedirectCountdown, resume: startRedirectCountdown } = useIntervalFn(
  () => {
    redirectCountdown.value--;
    if (redirectCountdown.value <= 0) {
      stopRedirectCountdown();
      window.location.href = "/";
    }
  },
  1000,
  { immediate: false }
);

// 使用认证Store
const authStore = useAuthStore();

// 从Store获取权限状态的计算属性
const isAdmin = computed(() => authStore.isAdmin);

/**
 * 重新加载文件信息
 * 当预签名URL过期时，可以调用此方法刷新获取新的URL
 */
const refreshFileInfo = async () => {
  // 如果文件已通过密码验证，记录当前密码以便在刷新后使用
  if (fileInfo.value && fileInfo.value.passwordVerified && fileInfo.value.currentPassword) {
    try {
      // 确保当前密码被保存到会话存储
      setFilePassword(fileInfo.value.slug, fileInfo.value.currentPassword);
    } catch (err) {
      log.error("无法保存密码到会话存储:", err);
    }
  }

  // 重新加载文件信息
  await loadFileInfo(true);
};

/**
 * 加载文件信息
 */
const loadFileInfo = async (force = false) => {
  loading.value = true;
  error.value = "";

  try {
    const fileSlug = slug.value;
    if (!fileSlug) {
      error.value = t("fileView.errors.missingSlug");
      loading.value = false;
      return;
    }

    // force=true 时跳过缓存，确保拿到最新数据
    const data = await fileShareStore.fetchBySlug(fileSlug, { useCache: !force });

    // 文件夹分享：重定向到公开只读的文件夹分享页
    if (data && data.target_type === "folder") {
      await router.replace({ name: "FolderShareView", params: { slug: fileSlug } });
      return;
    }

    fileInfo.value = {
      ...data,
      slug: fileSlug,
    };

    requiresPassword.value = !!data.requires_password;
  } catch (err) {
    log.error("加载文件信息失败:", err);
    error.value = err.message || t("fileView.errors.loadFailed");
  } finally {
    loading.value = false;
  }
};


/**
 * 处理密码验证成功事件
 * @param {Object} data - 包含文件URLs和信息的对象
 */
const handlePasswordVerified = (data) => {
  const updated = {
    ...fileInfo.value,
    ...data,
    passwordVerified: true,
    currentPassword: data.currentPassword,
  };

  fileInfo.value = updated;

  if (data.currentPassword) {
    try {
      setFilePassword(fileInfo.value.slug, data.currentPassword);
    } catch (err) {
      log.error("无法保存密码到会话存储:", err);
    }
  }

  requiresPassword.value = false;

  // 更新缓存中的文件信息，保持 admin/public 视图一致
  fileShareStore.updateCachedFile(updated);
};

/**
 * 打开编辑模态框
 */
const openEditModal = async () => {
  try {
    // 只有当文件有ID时才尝试获取详情
    if (fileInfo.value.id) {
      const prev = fileInfo.value;
      const detail = await fileShareStore.fetchById(fileInfo.value.id, { useCache: false });
      // 合并数据时，只保留运行时状态字段，不保留可编辑字段
      fileInfo.value = {
        ...detail,
        // 保留运行时状态字段
        slug: prev.slug,
        type: prev.type,
        requires_password: prev.requires_password,
        passwordVerified: prev.passwordVerified,
        currentPassword: prev.currentPassword,
        // 移除 use_proxy 的旧值保留，使用从后端获取的最新值
        previewUrl: prev.previewUrl,
        downloadUrl: prev.downloadUrl,
        linkType: prev.linkType,
        previewSelection: prev.previewSelection,
      };
    }

    // 显示编辑模态框
    showEditModal.value = true;
  } catch (err) {
    log.error("获取文件详情出错:", err);
    showError(`${t("fileView.errors.getDetailsFailed")}: ${t("fileView.errors.getDetailsFailedMessage")}`);
    showEditModal.value = true;
  }
};

/**
 * 关闭编辑模态框
 */
const closeEditModal = () => {
  showEditModal.value = false;
};

/**
 * 保存文件修改
 * @param {Object} updatedFile - 更新后的文件信息
 */
const saveFileChanges = async (updatedFile) => {
  try {
    if (!updatedFile?.id) {
      showError(`${t("fileView.errors.updateFailed")}: ${t("fileView.errors.missingId")}`);
      return;
    }

    // 使用 fileshareService 统一更新文件元数据
    const result = await fileshareService.updateFileMetadata(updatedFile.id, updatedFile);
    const updatedSlug = result && typeof result === "object" && result.slug ? result.slug : fileInfo.value.slug;
    const slugChanged = updatedSlug && updatedSlug !== fileInfo.value.slug;

    if (slugChanged) {
      slug.value = updatedSlug;
    }

    // 更新成功，重新加载文件信息并关闭弹窗（强制刷新，避免使用旧缓存）
    await loadFileInfo(true);
    if (slugChanged) {
      try {
        await router.replace({
          name: "FileView",
          params: { slug: updatedSlug },
          query: route.query,
          hash: route.hash,
        });
      } catch (replaceError) {
        log.warn("跳转新链接失败", replaceError);
      }
    }
    closeEditModal();
    showSuccess(t("fileView.actions.updateSuccess"));
  } catch (err) {
    log.error("更新文件错误:", err);
    const msg = err?.message || t("fileView.errors.unknown");
    showError(`${t("fileView.errors.updateFailed")}: ${msg}`);
  }
};

/**
 * 处理文件删除事件
 */
const handleFileDeleted = () => {
  // 显示删除成功提示
  showDeleteSuccess.value = true;

  // 开始倒计时
  redirectCountdown.value = 3;

  stopRedirectCountdown();
  startRedirectCountdown();
};

// 组件挂载时加载文件信息
onMounted(() => {
  loadFileInfo();
});

// 组件卸载时清除计时器
onUnmounted(() => {
  stopRedirectCountdown();
});

watch(
  () => props.slug,
  (newSlug) => {
    if (newSlug && newSlug !== slug.value) {
      slug.value = newSlug;
      loadFileInfo(true);
    }
  }
);
</script>
