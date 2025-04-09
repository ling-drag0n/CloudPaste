<template>
  <div class="file-view-container flex flex-col flex-1 bg-white dark:bg-gray-900">
    <!-- 添加面包屑导航标题，与文本分享页面风格一致 -->
    <div class="max-w-4xl mx-auto w-full px-4 mt-4">
      <div class="py-3 text-sm text-gray-500 dark:text-gray-400 border-b border-gray-200 dark:border-gray-700 mb-4">
        <a href="/" class="hover:text-primary-600 dark:hover:text-primary-400">{{ t("common.back") }}</a>
        <span class="mx-2">/</span>
        <span class="text-gray-700 dark:text-gray-300">{{ t("file.view") }}</span>
      </div>
    </div>

    <div v-if="error" class="error-container py-12 px-4 max-w-4xl mx-auto text-center">
      <svg xmlns="http://www.w3.org/2000/svg" class="h-16 w-16 mx-auto mb-4 text-red-600 dark:text-red-500" fill="none" viewBox="0 0 24 24" stroke="currentColor">
        <path
            stroke-linecap="round"
            stroke-linejoin="round"
            stroke-width="1.5"
            d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z"
        />
      </svg>
      <h2 class="text-2xl font-bold mb-2 text-gray-900 dark:text-white">{{ t("file.error") }}</h2>
      <p class="text-lg mb-6 text-gray-600 dark:text-gray-300">{{ error }}</p>
      <a
          href="/"
          class="inline-flex items-center justify-center px-4 py-2 border border-transparent rounded-md shadow-sm text-base font-medium text-white bg-blue-600 hover:bg-blue-700"
      >
        {{ t("common.back") }}
      </a>
    </div>

    <!-- 删除成功提示 -->
    <div v-else-if="showDeleteSuccess" class="success-container py-12 px-4 max-w-4xl mx-auto text-center">
      <svg xmlns="http://www.w3.org/2000/svg" class="h-16 w-16 mx-auto mb-4 text-green-600 dark:text-green-500" fill="none" viewBox="0 0 24 24" stroke="currentColor">
        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.5" d="M5 13l4 4L19 7" />
      </svg>
      <h2 class="text-2xl font-bold mb-2 text-gray-900 dark:text-white">{{ t("file.deletedSuccess") }}</h2>
      <p class="text-lg mb-6 text-gray-600 dark:text-gray-300">{{ t("file.deletedRedirectMsg") }}</p>
      <div class="animate-pulse text-gray-500 dark:text-gray-400">{{ t("file.redirectCountdown", { seconds: redirectCountdown }) }}</div>
    </div>

    <div v-else-if="loading" class="loading-container py-12 px-4 max-w-4xl mx-auto text-center">
      <div class="animate-spin rounded-full h-16 w-16 border-t-4 border-b-4 mx-auto mb-4 border-blue-600 dark:border-blue-500"></div>
      <p class="text-lg text-gray-600 dark:text-gray-300">{{ t("file.loading") }}</p>
    </div>

    <div v-else class="file-container flex-1 flex flex-col py-8 px-4 max-w-4xl mx-auto">
      <!-- 密码验证界面 -->
      <div v-if="requiresPassword && !fileUrls.previewUrl" class="password-container flex-1 flex items-start justify-center pt-8">
        <FileViewPassword :fileId="fileInfo.slug" @verified="handlePasswordVerified" />
      </div>

      <!-- 文件信息和操作界面 -->
      <div v-else class="file-content">
        <!-- 文件信息 -->
        <FileViewInfo :fileInfo="fileInfo" :fileUrls="fileUrls" class="mb-8" />

        <!-- 文件操作按钮 -->
        <FileViewActions :fileInfo="fileInfo" :fileUrls="fileUrls" @edit="openEditModal" @delete="handleFileDeleted" @refresh-file-info="refreshFileInfo" />
      </div>

      <!-- 编辑模态框 (仅管理员可见) -->
      <FileEditModal v-if="showEditModal" :file="fileInfo" @close="closeEditModal" @save="saveFileChanges" />
    </div>
  </div>
</template>

<script setup>
import { ref, onMounted, defineProps, onUnmounted } from "vue";
import { useI18n } from "vue-i18n";
import { api } from "../api";
import { getAuthStatus } from "./file-view/FileViewUtils";

// 导入子组件
import FileViewInfo from "./file-view/FileViewInfo.vue";
import FileViewPassword from "./file-view/FileViewPassword.vue";
import FileViewActions from "./file-view/FileViewActions.vue";
import FileEditModal from "./adminManagement/files-management/FileEditModal.vue";

// 初始化i18n
const { t } = useI18n();

const props = defineProps({
  slug: {
    type: String,
    required: true,
  },
});

// 状态变量
const slug = ref(props.slug);
const fileInfo = ref({});
const fileUrls = ref({
  previewUrl: "",
  downloadUrl: "",
});
const loading = ref(true);
const error = ref("");
const requiresPassword = ref(false);
const showEditModal = ref(false);

// 删除成功状态
const showDeleteSuccess = ref(false);
const redirectCountdown = ref(3);
let countdownTimer = null;

// 获取用户权限
const { isAdmin } = getAuthStatus();

/**
 * 重新加载文件信息
 * 当预签名URL过期时，可以调用此方法刷新获取新的URL
 */
const refreshFileInfo = async () => {
  console.log("重新加载文件信息");

  // 如果文件已通过密码验证，记录当前密码以便在刷新后使用
  if (fileInfo.value && fileInfo.value.passwordVerified && fileInfo.value.currentPassword) {
    try {
      // 确保当前密码被保存到会话存储
      sessionStorage.setItem(`file_password_${fileInfo.value.slug}`, fileInfo.value.currentPassword);
      console.log("已保存当前密码到会话存储以便刷新");
    } catch (err) {
      console.error("无法保存密码到会话存储:", err);
    }
  }

  // 重新加载文件信息
  await loadFileInfo();
};

/**
 * 加载文件信息
 */
const loadFileInfo = async () => {
  loading.value = true;
  error.value = "";

  try {
    // 确保使用计算属性获取slug
    const fileSlug = slug.value;
    if (!fileSlug) {
      error.value = "缺少文件标识符";
      loading.value = false;
      return;
    }

    const response = await api.file.getPublicFile(fileSlug);

    if (response.success) {
      // 更新文件信息，并确保包含slug
      fileInfo.value = {
        ...response.data,
        slug: fileSlug, // 确保slug被包含在fileInfo中
      };

      // 检查是否需要密码
      if (response.data.requires_password) {
        requiresPassword.value = true;
        fileUrls.value = {
          previewUrl: "",
          downloadUrl: "",
        };
      } else {
        requiresPassword.value = false;

        // 检查并修改预览URL，确保代理URL包含密码参数
        let previewUrl = response.data.previewUrl;
        let downloadUrl = response.data.downloadUrl;

        // 对于不需要密码的文件，检查session中是否有存储的密码
        // 这可能是用户之前访问过需要密码的文件，而后端修改了密码要求
        let savedPassword = null;
        try {
          savedPassword = sessionStorage.getItem(`file_password_${fileSlug}`);
        } catch (err) {
          console.error("从会话存储获取密码出错:", err);
        }

        // 如果有代理URL，并且有保存的密码，添加密码参数
        if (previewUrl && previewUrl.includes("/api/file-view/") && savedPassword && !previewUrl.includes("password=")) {
          previewUrl = previewUrl.includes("?") ? `${previewUrl}&password=${encodeURIComponent(savedPassword)}` : `${previewUrl}?password=${encodeURIComponent(savedPassword)}`;
          console.log("从会话存储中为代理预览URL添加密码参数");
        }

        // 同样处理下载URL
        if (downloadUrl && downloadUrl.includes("/api/file-download/") && savedPassword && !downloadUrl.includes("password=")) {
          downloadUrl = downloadUrl.includes("?") ? `${downloadUrl}&password=${encodeURIComponent(savedPassword)}` : `${downloadUrl}?password=${encodeURIComponent(savedPassword)}`;
        }

        fileUrls.value = {
          previewUrl: previewUrl,
          downloadUrl: downloadUrl,
        };
      }
    } else {
      // 处理错误情况
      error.value = response.message || "无法加载文件信息";
    }
  } catch (err) {
    console.error("加载文件出错:", err);
    error.value = err.message || "加载文件时发生错误";
  } finally {
    loading.value = false;
  }
};

/**
 * 处理密码验证成功事件
 * @param {Object} data - 包含文件URLs和信息的对象
 */
const handlePasswordVerified = (data) => {
  // 检查并修改预览URL，确保代理URL包含密码参数
  let previewUrl = data.previewUrl;
  if (previewUrl && previewUrl.includes("/api/file-view/")) {
    // 确保URL包含密码参数
    if (data.currentPassword && !previewUrl.includes("password=")) {
      // 添加密码参数到预览URL
      previewUrl = previewUrl.includes("?")
          ? `${previewUrl}&password=${encodeURIComponent(data.currentPassword)}`
          : `${previewUrl}?password=${encodeURIComponent(data.currentPassword)}`;
      console.log("已在验证阶段为代理预览URL添加密码参数");
    }
  }

  // 更新文件URLs
  fileUrls.value = {
    previewUrl: previewUrl, // 使用可能修改过的预览URL
    downloadUrl: data.downloadUrl,
  };

  // 更新文件信息，保留所有从API返回的属性
  fileInfo.value = {
    ...fileInfo.value, // 保留现有属性
    ...data, // 添加所有从密码验证返回的属性
    passwordVerified: true, // 标记密码已验证
    currentPassword: data.currentPassword, // 保存当前使用的密码
  };

  // 存储验证过的密码到sessionStorage，避免页面刷新后需要重新输入
  // 注意：这会在当前会话中保存密码，当窗口关闭时会自动清除
  if (data.currentPassword) {
    try {
      // 使用文件slug作为键，以支持在同一会话中访问多个文件
      sessionStorage.setItem(`file_password_${fileInfo.value.slug}`, data.currentPassword);
    } catch (err) {
      console.error("无法保存密码到会话存储:", err);
    }
  }

  // 更新状态，显示文件内容而不是密码验证界面
  requiresPassword.value = false;
};

/**
 * 打开编辑模态框
 */
const openEditModal = async () => {
  try {
    // 只有当文件有ID时才尝试获取详情
    if (fileInfo.value.id) {
      let response;

      // 根据用户类型选择合适的API函数
      if (isAdmin) {
        response = await api.file.getFile(fileInfo.value.id);
      } else {
        // 如果不是管理员，则尝试使用用户的API获取文件
        response = await api.file.getUserFile(fileInfo.value.id);
      }

      if (response.success) {
        // 更新文件信息
        fileInfo.value = {
          ...response.data,
          slug: fileInfo.value.slug, // 保留原来的slug
        };
      } else {
        console.error("获取文件详情失败:", response.message);
        alert("获取文件详情失败，将使用当前显示的信息");
      }
    }

    // 显示编辑模态框
    showEditModal.value = true;
  } catch (err) {
    console.error("获取文件详情出错:", err);
    alert("获取文件详情时发生错误，将使用当前显示的信息");
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
    let response;

    // 根据用户类型选择合适的API函数
    if (isAdmin) {
      response = await api.file.updateFile(updatedFile.id, updatedFile);
    } else {
      // 如果不是管理员，则尝试使用用户的API更新文件
      response = await api.file.updateUserFile(updatedFile.id, updatedFile);
    }

    if (response.success) {
      // 更新成功，重新加载文件信息
      await loadFileInfo();
      // 关闭编辑模态框
      closeEditModal();
    } else {
      // 处理错误情况
      console.error("更新文件信息失败:", response.message);
      alert(`更新文件信息失败: ${response.message}`);
    }
  } catch (err) {
    console.error("更新文件错误:", err);
    alert("更新文件时发生错误，请稍后重试");
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

  // 清除可能存在的旧定时器
  if (countdownTimer) {
    clearInterval(countdownTimer);
  }

  // 设置倒计时定时器
  countdownTimer = setInterval(() => {
    redirectCountdown.value--;

    if (redirectCountdown.value <= 0) {
      clearInterval(countdownTimer);
      countdownTimer = null;

      // 直接使用window.location进行重定向
      window.location.href = "/";
    }
  }, 1000);
};

// 组件挂载时加载文件信息
onMounted(() => {
  loadFileInfo();
});

// 组件卸载时清除计时器
onUnmounted(() => {
  if (countdownTimer) {
    clearInterval(countdownTimer);
    countdownTimer = null;
  }
});
</script>

// 密码错误清除令牌 //
