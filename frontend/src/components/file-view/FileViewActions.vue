<template>
  <div class="file-actions flex flex-wrap gap-3">
    <!-- 预览按钮 -->
    <button
      v-if="fileUrls.previewUrl"
      @click="previewFile"
      class="action-button flex items-center justify-center px-4 py-2 rounded-lg transition-colors focus:outline-none focus:ring-2 focus:ring-offset-2 bg-indigo-600 hover:bg-indigo-700 text-white focus:ring-indigo-500 focus:ring-offset-white dark:focus:ring-offset-gray-800"
    >
      <svg xmlns="http://www.w3.org/2000/svg" class="h-5 w-5 mr-2" fill="none" viewBox="0 0 24 24" stroke="currentColor">
        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 12a3 3 0 11-6 0 3 3 0 016 0z" />
        <path
          stroke-linecap="round"
          stroke-linejoin="round"
          stroke-width="2"
          d="M2.458 12C3.732 7.943 7.523 5 12 5c4.478 0 8.268 2.943 9.542 7-1.274 4.057-5.064 7-9.542 7-4.477 0-8.268-2.943-9.542-7z"
        />
      </svg>
      <span>{{ t("fileView.actions.preview") }}</span>
    </button>

    <!-- 下载按钮 -->
    <button
      v-if="fileUrls.downloadUrl"
      @click="downloadFile"
      class="action-button flex items-center justify-center px-4 py-2 rounded-lg transition-colors focus:outline-none focus:ring-2 focus:ring-offset-2 bg-blue-600 hover:bg-blue-700 text-white focus:ring-blue-500 focus:ring-offset-white dark:focus:ring-offset-gray-800"
    >
      <svg xmlns="http://www.w3.org/2000/svg" class="h-5 w-5 mr-2" fill="none" viewBox="0 0 24 24" stroke="currentColor">
        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-4l-4 4m0 0l-4-4m4 4V4" />
      </svg>
      <span>{{ t("fileView.actions.download") }}</span>
    </button>

    <!-- 编辑按钮 (管理员可见所有文件，API密钥用户只能看到自己的文件) -->
    <button
      v-if="(isAdmin || (hasApiKey && hasFilePermission && isCreator)) && fileInfo.id"
      @click="$emit('edit')"
      class="action-button flex items-center justify-center px-4 py-2 rounded-lg transition-colors focus:outline-none focus:ring-2 focus:ring-offset-2 bg-gray-200 dark:bg-gray-700 hover:bg-gray-300 dark:hover:bg-gray-600 text-gray-800 dark:text-white focus:ring-gray-400 dark:focus:ring-gray-500 focus:ring-offset-white dark:focus:ring-offset-gray-800"
    >
      <svg xmlns="http://www.w3.org/2000/svg" class="h-5 w-5 mr-2" fill="none" viewBox="0 0 24 24" stroke="currentColor">
        <path
          stroke-linecap="round"
          stroke-linejoin="round"
          stroke-width="2"
          d="M11 5H6a2 2 0 00-2 2v11a2 2 0 002 2h11a2 2 0 002-2v-5m-1.414-9.414a2 2 0 112.828 2.828L11.828 15H9v-2.828l8.586-8.586z"
        />
      </svg>
      <span>{{ t("fileView.actions.edit") }}</span>
    </button>

    <!-- 分享按钮 -->
    <button
      @click="openShareModal"
      class="action-button flex items-center justify-center px-4 py-2 rounded-lg transition-colors focus:outline-none focus:ring-2 focus:ring-offset-2 bg-green-600 hover:bg-green-700 text-white focus:ring-green-500 focus:ring-offset-white dark:focus:ring-offset-gray-800"
    >
      <svg xmlns="http://www.w3.org/2000/svg" class="h-5 w-5 mr-2" fill="none" viewBox="0 0 24 24" stroke="currentColor">
        <path
          stroke-linecap="round"
          stroke-linejoin="round"
          stroke-width="2"
          d="M8.684 13.342C8.886 12.938 9 12.482 9 12c0-.482-.114-.938-.316-1.342m0 2.684a3 3 0 110-2.684m0 2.684l6.632 3.316m-6.632-6l6.632-3.316m0 0a3 3 0 105.367-2.684 3 3 0 00-5.367 2.684zm0 9.316a3 3 0 105.368 2.684 3 3 0 00-5.368-2.684z"
        />
      </svg>
      <span>{{ t("fileView.actions.share") }}</span>
    </button>

    <!-- 删除按钮 (管理员可见所有文件，API密钥用户只能看到自己的文件) -->
    <button
      v-if="(isAdmin || (hasApiKey && hasFilePermission && isCreator)) && fileInfo.id"
      @click="confirmDelete"
      class="action-button flex items-center justify-center px-4 py-2 rounded-lg transition-colors focus:outline-none focus:ring-2 focus:ring-offset-2 bg-red-600 hover:bg-red-700 text-white focus:ring-red-500 focus:ring-offset-white dark:focus:ring-offset-gray-800"
    >
      <svg xmlns="http://www.w3.org/2000/svg" class="h-5 w-5 mr-2" fill="none" viewBox="0 0 24 24" stroke="currentColor">
        <path
          stroke-linecap="round"
          stroke-linejoin="round"
          stroke-width="2"
          d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16"
        />
      </svg>
      <span>{{ t("fileView.actions.delete") }}</span>
    </button>

    <!-- 删除确认模态框 -->
    <div v-if="showDeleteConfirm" class="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black bg-opacity-50">
      <div class="rounded-lg p-6 max-w-sm w-full shadow-xl bg-white dark:bg-gray-800">
        <h3 class="text-lg font-medium mb-4 text-gray-900 dark:text-white">{{ t("fileView.actions.delete") }}</h3>
        <p class="mb-6 text-gray-600 dark:text-gray-300">
          {{ t("fileView.actions.deleteConfirm") }}
        </p>
        <div class="flex justify-end space-x-3">
          <button
            @click="showDeleteConfirm = false"
            class="px-4 py-2 rounded-md text-sm font-medium transition-colors bg-gray-200 dark:bg-gray-700 hover:bg-gray-300 dark:hover:bg-gray-600 text-gray-700 dark:text-white"
          >
            {{ t("common.cancel") }}
          </button>
          <button @click="deleteFile" class="px-4 py-2 rounded-md text-sm font-medium transition-colors bg-red-600 hover:bg-red-700 text-white" :disabled="deleting">
            {{ deleting ? t("fileView.actions.deleting") : t("common.confirm") }}
          </button>
        </div>
      </div>
    </div>

    <!-- 复制成功提示 -->
    <div
      v-if="showCopyToast"
      class="fixed top-4 right-4 px-4 py-3 rounded-lg shadow-lg z-50 flex items-center transition-opacity duration-200 bg-white dark:bg-gray-800 text-green-600 dark:text-green-400 border border-gray-200 dark:border-gray-700"
    >
      <svg xmlns="http://www.w3.org/2000/svg" class="h-5 w-5 mr-2" fill="none" viewBox="0 0 24 24" stroke="currentColor">
        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7" />
      </svg>
      <span>{{ t("fileView.fileInfo.linkCopied") }}</span>
    </div>

    <!-- 错误提示组件 -->
    <ErrorToast
      :visible="showErrorToast"
      :title="errorTitle"
      :message="errorMessage"
      :action-text="errorActionText"
      :action-handler="errorActionHandler"
      @close="closeErrorToast"
    />

    <!-- 分享模态框 -->
    <ShareModal :visible="showShareModal" :file-info="fileInfo" :dark-mode="darkMode" @close="closeShareModal" />
  </div>
</template>

<script setup>
import { ref, defineProps, defineEmits, computed } from "vue";
import { useI18n } from "vue-i18n";
import { api } from "@/api";
import { ApiStatus } from "../../api/ApiStatus";
import { copyToClipboard } from "@/utils/clipboard";
import ErrorToast from "../common/ErrorToast.vue";
import ShareModal from "./ShareModal.vue";
import { useAuthStore } from "@/stores/authStore.js";
import { FileType } from "@/utils/fileTypes.js";

const { t } = useI18n();

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

const emit = defineEmits(["edit", "delete", "refresh-file-info"]);

// 使用认证Store
const authStore = useAuthStore();

// 从Store获取权限状态的计算属性
const isAdmin = computed(() => authStore.isAdmin);
const hasApiKey = computed(() => authStore.authType === "apikey" && !!authStore.apiKey);
const hasFilePermission = computed(() => authStore.hasFilePermission);

// 删除确认状态
const showDeleteConfirm = ref(false);
const deleting = ref(false);

// 复制提示状态
const showCopyToast = ref(false);

// 分享模态框状态
const showShareModal = ref(false);

// 错误提示状态
const showErrorToast = ref(false);
const errorTitle = ref("");
const errorMessage = ref("");
const errorActionText = ref("");
const errorActionHandler = ref(null);

// 显示错误提示
const showError = (title, message, actionText = "", actionHandler = null) => {
  errorTitle.value = title;
  errorMessage.value = message;
  errorActionText.value = actionText;
  errorActionHandler.value = actionHandler;
  showErrorToast.value = true;
};

// 关闭错误提示
const closeErrorToast = () => {
  showErrorToast.value = false;
  errorTitle.value = "";
  errorMessage.value = "";
  errorActionText.value = "";
  errorActionHandler.value = null;
};

// 计算属性：判断当前用户是否为文件创建者
const isCreator = computed(() => {
  return authStore.isFileCreator(props.fileInfo);
});

// 辅助函数：获取文件密码
const getFilePassword = () => {
  // 1. 优先使用文件信息中存储的已验证密码
  if (props.fileInfo.currentPassword) {
    console.log("使用已验证的密码");
    return props.fileInfo.currentPassword;
  }

  // 2. 尝试从会话存储中获取密码（用户在当前会话中输入过）
  try {
    const sessionPassword = sessionStorage.getItem(`file_password_${props.fileInfo.slug}`);
    if (sessionPassword) {
      console.log("使用会话存储中的密码");
      return sessionPassword;
    }
  } catch (err) {
    console.error("从会话存储获取密码出错:", err);
  }

  // 3. 最后尝试从URL获取密码参数
  try {
    const currentUrl = new URL(window.location.href);
    const passwordParam = currentUrl.searchParams.get("password");
    if (passwordParam) {
      console.log("使用URL参数中的密码");
      return passwordParam;
    }
  } catch (err) {
    console.error("从URL获取密码参数出错:", err);
  }

  return null;
};

/**
 * 预览文件
 * 在新窗口中打开预览链接，为Office文件使用在线预览服务
 */
const previewFile = async () => {
  if (!props.fileUrls.previewUrl) return;

  try {
    // 判断是否是需要Office在线预览的文件
    const isOffice = props.fileInfo.type === FileType.OFFICE;
    const needsOfficePreview = isOffice;

    // 如果是需要Office在线预览的文件，需要特殊处理
    if (needsOfficePreview) {
      let officePreviewUrl;

      // 使用统一的文件分享API处理Office预览
      const urlInfo = api.fileView.parseFileShareUrl(props.fileUrls.previewUrl);
      if (urlInfo.isFileShare) {
        // Worker代理模式：通过API获取Office预览URL
        console.log("Office文件预览 - Worker代理模式");

        const filePassword = getFilePassword();

        try {
          // 使用统一的预览服务
          officePreviewUrl = await api.fileView.getOfficePreviewUrl(urlInfo.slug, {
            password: filePassword,
            provider: "microsoft",
          });
        } catch (error) {
          console.error("Office预览失败:", error);

          // 根据状态码选择合适的错误信息
          let errorMessage;
          if (error.status) {
            const errorKey = api.fileView.getErrorKeyByStatus(error.status);
            errorMessage = t(errorKey);
          } else {
            errorMessage = error.message || t("fileView.errors.serverError");
          }

          showError(t("fileView.actions.previewFailed"), errorMessage, t("fileView.actions.retry"), () => emit("refresh-file-info"));
          return;
        }
      } else {
        // S3直链模式：直接使用previewUrl
        console.log("Office文件预览 - S3直链模式");

        let previewUrl = props.fileUrls.previewUrl;

        // 确保URL是完整的绝对URL
        if (!previewUrl.startsWith("http://") && !previewUrl.startsWith("https://")) {
          const baseUrl = window.location.origin;
          previewUrl = previewUrl.startsWith("/") ? `${baseUrl}${previewUrl}` : `${baseUrl}/${previewUrl}`;
        }

        // 将S3 URL包装到Office在线预览服务中
        officePreviewUrl = `https://view.officeapps.live.com/op/view.aspx?src=${encodeURIComponent(previewUrl)}`;
      }

      // 打开Office在线预览
      console.log("正在打开Office在线预览:", officePreviewUrl);
      window.open(officePreviewUrl, "_blank");
      return;
    }

    // 非Office文件使用原有逻辑
    // 检查是否是代理URL并添加密码参数
    let previewUrl = props.fileUrls.previewUrl;

    // 使用统一的文件分享API处理预览URL
    const urlInfo = api.fileView.parseFileShareUrl(previewUrl);
    if (urlInfo.isFileShare && urlInfo.type === "preview") {
      const filePassword = getFilePassword();

      // 如果需要密码且URL中没有密码参数，则添加密码参数
      // 注意：不论是否为代理模式，都直接使用后端返回的URL，只添加密码参数
      if (filePassword && !urlInfo.password) {
        previewUrl = api.fileView.addPasswordToUrl(previewUrl, filePassword);
        console.log("已通过API服务添加密码参数到预览URL");
      }
    }

    // 在新窗口打开预览链接
    window.open(previewUrl, "_blank");
  } catch (err) {
    console.error("预览文件失败:", err);
    showError(t("fileView.actions.previewFailed"), err.message || t("fileView.errors.unknown"), t("fileView.actions.retry"), () => emit("refresh-file-info"));
  }
};

/**
 * 下载文件
 * 通过创建隐藏的a元素并模拟点击下载文件
 */
const downloadFile = () => {
  if (!props.fileUrls.downloadUrl) return;

  try {
    console.log("开始下载文件:", props.fileInfo.filename);

    // 提取文件名，用于下载时的文件命名
    const fileName = props.fileInfo.filename || t("fileView.actions.downloadFile");

    // 使用统一的文件分享API处理下载URL
    let downloadUrl = props.fileUrls.downloadUrl;

    // 检查是否为文件分享URL并统一处理
    const urlInfo = api.fileView.parseFileShareUrl(downloadUrl);
    if (urlInfo.isFileShare && urlInfo.type === "download") {
      const filePassword = getFilePassword();
      if (filePassword && !urlInfo.password) {
        // 使用API服务添加密码参数
        downloadUrl = api.fileView.addPasswordToUrl(downloadUrl, filePassword);
        console.log("已通过API服务添加密码参数到下载URL");
      }
    }

    // 创建一个隐藏的a标签
    const link = document.createElement("a");
    link.href = downloadUrl;
    link.download = fileName; // 设置下载文件名
    document.body.appendChild(link);

    // 模拟点击下载
    link.click();

    // 移除临时创建的元素
    setTimeout(() => {
      document.body.removeChild(link);
    }, 100);
  } catch (err) {
    console.error("下载文件失败:", err);

    // 如果直接下载失败，尝试在新窗口打开下载链接
    // 注意：这里也需要使用添加了密码的URL
    let downloadUrl = props.fileUrls.downloadUrl;

    // 判断是否是代理URL
    if (downloadUrl.includes("/api/file-download/")) {
      // 获取文件密码
      const filePassword = getFilePassword();

      // 如果有密码，并且下载URL中还没有包含密码参数
      if (filePassword && !downloadUrl.includes("password=")) {
        // 添加密码参数到下载URL
        downloadUrl = downloadUrl.includes("?") ? `${downloadUrl}&password=${encodeURIComponent(filePassword)}` : `${downloadUrl}?password=${encodeURIComponent(filePassword)}`;
      }

      // 不再需要添加counted=true参数，下载端点默认不增加计数
    }

    window.open(downloadUrl, "_blank");

    // 如果是授权问题，提示刷新页面
    if (
      err.status === ApiStatus.FORBIDDEN ||
      err.response?.status === ApiStatus.FORBIDDEN ||
      err.status === ApiStatus.UNAUTHORIZED ||
      err.response?.status === ApiStatus.UNAUTHORIZED ||
      err.code === ApiStatus.FORBIDDEN ||
      err.code === ApiStatus.UNAUTHORIZED ||
      (err.message && (err.message.includes(ApiStatus.FORBIDDEN.toString()) || err.message.includes(ApiStatus.UNAUTHORIZED.toString())))
    ) {
      showError(t("fileView.actions.downloadFailed"), t("fileView.actions.downloadExpired"), t("fileView.actions.refresh"), () => emit("refresh-file-info"));
    } else {
      showError(t("fileView.actions.downloadFailed"), err.message || t("fileView.errors.unknown"));
    }
  }
};

/**
 * 打开分享模态框
 */
const openShareModal = () => {
  showShareModal.value = true;
};

/**
 * 关闭分享模态框
 */
const closeShareModal = () => {
  showShareModal.value = false;
};

/**
 * 显示删除确认对话框
 */
const confirmDelete = () => {
  showDeleteConfirm.value = true;
};

/**
 * 删除文件
 */
const deleteFile = async () => {
  if (!props.fileInfo.id) return;

  deleting.value = true;

  try {
    let response;

    // 使用统一的批量删除 API
    if (isAdmin.value || (hasApiKey.value && hasFilePermission.value && isCreator.value)) {
      response = await api.file.batchDeleteFiles([props.fileInfo.id]);
    } else {
      throw new Error(t("fileView.actions.noPermission"));
    }

    if (response.success) {
      // 检查批量删除结果
      if (response.data && response.data.failed && response.data.failed.length > 0) {
        // 删除失败
        const failedItem = response.data.failed[0];
        throw new Error(failedItem.error || "删除失败");
      }

      // 关闭确认对话框
      showDeleteConfirm.value = false;
      // 通知父组件文件已删除
      emit("delete", props.fileInfo.id);
    } else {
      // 处理删除失败的情况
      console.error("删除文件失败:", response.message);
      showError(t("fileView.actions.deleteFailed"), `${t("fileView.actions.deleteFailed")}: ${response.message}`);
    }
  } catch (err) {
    console.error("删除文件错误:", err);
    showError(t("fileView.actions.deleteFailed"), err.message || t("fileView.errors.unknown"));
  } finally {
    deleting.value = false;
  }
};
</script>
