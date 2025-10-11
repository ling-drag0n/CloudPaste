<template>
  <div class="file-upload-container mx-auto px-3 sm:px-6 flex-1 flex flex-col pt-6 sm:pt-8 w-full max-w-full sm:max-w-6xl">
    <div class="header mb-4 border-b pb-2" :class="darkMode ? 'border-gray-700' : 'border-gray-200'">
      <h2 class="text-xl font-semibold">{{ t("file.uploadPageTitle") }}</h2>
    </div>

    <!-- 权限管理组件 -->
    <PermissionManager
      :dark-mode="darkMode"
      permission-type="file"
      :permission-required-text="$t('file.permissionRequired')"
      :login-auth-text="$t('file.loginOrAuth')"
      @permission-change="handlePermissionChange"
      @navigate-to-admin="navigateToAdmin"
    />

    <div class="main-content" v-if="hasPermission">
      <!-- 文件上传区域 -->
      <div class="card mb-6 p-4 sm:p-6">
        <FileUploader
          :dark-mode="darkMode"
          :s3-configs="s3Configs"
          :loading="loadingConfigs"
          :is-admin="isAdmin"
          @upload-success="handleUploadSuccess"
          @upload-error="handleUploadError"
        />
      </div>

      <!-- 错误消息 -->
      <div
        v-if="message"
        class="mb-4 p-3 rounded-md"
        :class="
          message.type === 'success'
            ? darkMode
              ? 'bg-green-900/40 border border-green-800 text-green-200'
              : 'bg-green-50 border border-green-200 text-green-800'
            : message.type === 'warning'
            ? darkMode
              ? 'bg-yellow-900/40 border border-yellow-800 text-yellow-200'
              : 'bg-yellow-50 border border-yellow-200 text-yellow-800'
            : darkMode
            ? 'bg-red-900/40 border border-red-800 text-red-200'
            : 'bg-red-50 border border-red-200 text-red-800'
        "
      >
        <div class="flex items-center">
          <!-- 成功图标 -->
          <svg v-if="message.type === 'success'" class="h-5 w-5 mr-2" :class="darkMode ? 'text-green-300' : 'text-green-500'" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7" />
          </svg>

          <!-- 存储容量不足图标 -->
          <svg
            v-else-if="message.icon === 'storage-full'"
            class="h-5 w-5 mr-2"
            :class="darkMode ? 'text-yellow-300' : 'text-yellow-500'"
            fill="none"
            viewBox="0 0 24 24"
            stroke="currentColor"
          >
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M20 12V8a2 2 0 00-2-2H6a2 2 0 00-2 2v4M8 18h8M12 6v5m-2 3h4" />
          </svg>

          <!-- 普通警告图标 -->
          <svg
            v-else-if="message.type === 'warning'"
            class="h-5 w-5 mr-2"
            :class="darkMode ? 'text-yellow-300' : 'text-yellow-500'"
            fill="none"
            viewBox="0 0 24 24"
            stroke="currentColor"
          >
            <path
              stroke-linecap="round"
              stroke-linejoin="round"
              stroke-width="2"
              d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z"
            />
          </svg>

          <!-- 错误图标 -->
          <svg v-else class="h-5 w-5 mr-2" :class="darkMode ? 'text-red-300' : 'text-red-500'" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 8v4m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
          </svg>

          {{ message.content }}
        </div>
      </div>

      <!-- 最近上传记录 -->
      <div v-if="recentFiles.length > 0" class="card p-4 sm:p-6">
        <div class="flex justify-between items-center mb-4">
          <h3 class="text-lg font-medium">{{ t("file.recentUploads") }}</h3>
          <span class="text-sm" :class="darkMode ? 'text-gray-400' : 'text-gray-500'">{{ t("file.showingRecent") }}</span>
        </div>
        <FileList :dark-mode="darkMode" :files="recentFiles" :loading="loadingFiles" :user-type="isAdmin ? 'admin' : 'apikey'" @refresh="loadFiles" />
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, onMounted, computed, onBeforeUnmount } from "vue";
import { api } from "@/api";
import FileUploader from "../components/file-upload/FileUploader.vue";
import FileList from "../components/file-upload/FileList.vue";
import PermissionManager from "../components/common/PermissionManager.vue";
import { useI18n } from "vue-i18n"; // 导入i18n
import { useAuthStore } from "@/stores/authStore.js";

const { t } = useI18n(); // 初始化i18n

// 使用认证Store
const authStore = useAuthStore();

const props = defineProps({
  darkMode: {
    type: Boolean,
    default: false,
  },
});

// 数据状态
const s3Configs = ref([]);
const files = ref([]);
const loadingConfigs = ref(false);
const loadingFiles = ref(false);
const message = ref(null);

// 从Store获取权限状态的计算属性
const hasPermission = computed(() => authStore.hasFilePermission);

// 计算最近3条记录
const recentFiles = computed(() => {
  return files.value.slice(0, 3);
});

// API密钥验证逻辑已移至认证Store

// 导航到管理页面
const navigateToAdmin = () => {
  // 使用 Vue Router 进行导航
  import("../router").then(({ routerUtils }) => {
    routerUtils.navigateTo("admin");
  });
};

// 权限变化处理 - 当权限状态改变时执行相应的业务逻辑
const handlePermissionChange = async (hasPermissionValue) => {
  console.log("FileUpload: 权限状态变化", hasPermissionValue);

  if (hasPermissionValue) {
    console.log("用户获得权限，开始加载配置和文件列表");
    await Promise.all([loadS3Configs(), loadFiles()]);
  } else {
    console.log("用户失去权限，清空数据");
    // 清空相关数据
    s3Configs.value = [];
    files.value = [];
  }
};

// 事件处理函数
const handleAuthStateChange = async (e) => {
  console.log("FileUpload: 接收到认证状态变化事件:", e.detail);
  // 权限状态会自动更新，这里只需要重新加载数据
  if (hasPermission.value) {
    await Promise.all([loadS3Configs(), loadFiles()]);
  }
};

// 加载S3配置
const loadS3Configs = async () => {
  if (!hasPermission.value) return;

  loadingConfigs.value = true;
  try {
    const response = await api.file.getS3Configs();
    if (response.success && response.data) {
      s3Configs.value = response.data;
    }
  } catch (error) {
    console.error("加载S3配置失败:", error);
  } finally {
    loadingConfigs.value = false;
  }
};

// 加载已上传文件列表
const loadFiles = async () => {
  if (!hasPermission.value) return;

  loadingFiles.value = true;
  try {
    // 使用统一的API，只请求5个文件（显示3个，留2个余量）
    const response = await api.file.getFiles(5, 0);

    if (response.success && response.data) {
      // 确保按时间倒序排序，最新的在前面
      files.value = (response.data.files || []).sort((a, b) => {
        return new Date(b.created_at) - new Date(a.created_at);
      });
    }
  } catch (error) {
    console.error("加载文件列表失败:", error);
    // 显示错误消息，但不清除权限
    message.value = {
      type: "error",
      content: `${t("file.messages.getFileDetailFailed")}: ${error.message || t("file.messages.unknownError")}`,
    };

    // 4秒后清除消息
    setTimeout(() => {
      message.value = null;
    }, 4000);
  } finally {
    loadingFiles.value = false;
  }
};

// 处理上传成功事件
const handleUploadSuccess = (fileData) => {
  // 刷新文件列表
  loadFiles();

  // 显示成功消息
  message.value = {
    type: "success",
    content: t("file.uploadSuccessful"),
  };

  // 4秒后清除消息
  setTimeout(() => {
    message.value = null;
  }, 4000);
};

// 处理上传错误事件
const handleUploadError = (error) => {
  // 检查错误消息是否与存储容量相关
  if (
    error &&
    error.message &&
    (error.message.includes("存储空间不足") || error.message.includes("insufficient storage") || error.message.includes("容量") || error.message.includes("storage limit"))
  ) {
    // 存储容量不足错误，使用特殊样式
    message.value = {
      type: "warning", // 使用警告类型
      content: `${error.message}`,
      icon: "storage-full", // 自定义图标标识
    };
  } else {
    // 其他类型的错误
    message.value = {
      type: "error",
      content: error.message || t("file.messages.unknownError"),
    };
  }

  // 6秒后清除消息（给用户更多时间阅读错误信息）
  setTimeout(() => {
    message.value = null;
  }, 6000);
};

// 组件挂载时初始化
onMounted(async () => {
  console.log("FileUpload组件已挂载");

  // 监听认证状态变化事件
  window.addEventListener("auth-state-changed", handleAuthStateChange);

});

// 组件卸载时清理
onBeforeUnmount(() => {
  // 移除认证状态变化事件监听
  window.removeEventListener("auth-state-changed", handleAuthStateChange);
});
</script>

<style scoped>
/* 响应式设计 */
@media (max-width: 640px) {
  .file-upload-container {
    padding-top: 1rem;
  }
}
</style>
