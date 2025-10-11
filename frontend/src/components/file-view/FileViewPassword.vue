<template>
  <div class="max-w-sm w-full mx-auto p-5 border rounded-lg shadow-sm bg-white dark:bg-gray-800 border-gray-200 dark:border-gray-700">
    <h3 class="text-lg font-medium mb-4 text-gray-900 dark:text-white">{{ t("fileView.password.title") }}</h3>
    <p class="mb-4 text-sm text-gray-600 dark:text-gray-300">{{ t("fileView.password.description") }}</p>

    <form @submit.prevent="verifyPassword" class="space-y-4">
      <!-- 密码输入框 -->
      <div>
        <label for="password" class="block text-sm font-medium mb-1 text-gray-700 dark:text-gray-300">{{ t("fileView.password.label") }}</label>
        <div class="relative">
          <input
            :type="showPassword ? 'text' : 'password'"
            id="password"
            autocomplete="current-password"
            v-model="password"
            :placeholder="t('fileView.password.placeholder')"
            class="block w-full px-3 py-2 rounded-md shadow-sm border-gray-300 dark:border-gray-600 text-gray-900 dark:text-white dark:bg-gray-700 focus:ring-2 focus:ring-primary-500 focus:border-primary-500 dark:focus:ring-offset-gray-800 password-input"
            :disabled="loading"
          />
          <button
            type="button"
            @click="togglePasswordVisibility"
            class="absolute right-3 top-1/2 transform -translate-y-1/2 text-gray-600 dark:text-gray-400 hover:text-gray-800 dark:hover:text-gray-300"
          >
            <svg v-if="showPassword" xmlns="http://www.w3.org/2000/svg" class="h-5 w-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path
                stroke-linecap="round"
                stroke-linejoin="round"
                stroke-width="2"
                d="M13.875 18.825A10.05 10.05 0 0112 19c-4.478 0-8.268-2.943-9.543-7a9.97 9.97 0 011.563-3.029m5.858.908a3 3 0 114.243 4.243M9.878 9.878l4.242 4.242M9.88 9.88l-3.29-3.29m7.532 7.532l3.29 3.29M3 3l18 18"
              />
            </svg>
            <svg v-else xmlns="http://www.w3.org/2000/svg" class="h-5 w-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 12a3 3 0 11-6 0 3 3 0 016 0z" />
              <path
                stroke-linecap="round"
                stroke-linejoin="round"
                stroke-width="2"
                d="M2.458 12C3.732 7.943 7.523 5 12 5c4.478 0 8.268 2.943 9.542 7-1.274 4.057-5.064 7-9.542 7-4.477 0-8.268-2.943-9.542-7z"
              />
            </svg>
          </button>
        </div>
        <!-- 错误提示 -->
        <p v-if="error" class="mt-2 text-sm text-red-500 dark:text-red-400">{{ error }}</p>
      </div>

      <!-- 提交按钮 -->
      <button
        type="submit"
        class="w-full px-4 py-2 text-sm font-medium text-white bg-primary-600 rounded-md hover:bg-primary-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500 dark:focus:ring-offset-gray-800"
        :disabled="loading || !password"
      >
        <span v-if="loading">
          <svg class="animate-spin h-5 w-5 mr-2 inline-block" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
            <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
            <path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
          </svg>
          {{ t("fileView.password.loading") }}
        </span>
        <span v-else>{{ t("fileView.password.submit") }}</span>
      </button>
    </form>
  </div>
</template>

<script setup>
import { ref, watch, defineProps, defineEmits } from "vue";
import { useI18n } from "vue-i18n";
import { api } from "@/api"; // 导入api模块
import { ApiStatus } from "../../api/ApiStatus"; // 导入API状态码常量

const { t } = useI18n();

const props = defineProps({
  fileId: {
    type: String,
    required: true,
  },
  appUrl: {
    type: String,
    required: false, // 不再需要appUrl
  },
  darkMode: {
    type: Boolean,
    default: false,
  },
  fileThumbnail: {
    type: String,
    default: "",
  },
});

const emit = defineEmits(["verified"]);

const password = ref("");
const loading = ref(false);
const error = ref("");
const showPassword = ref(false);

// 切换密码可见性
const togglePasswordVisibility = () => {
  showPassword.value = !showPassword.value;
};

// 验证密码
const verifyPassword = async () => {
  if (!password.value) return;

  loading.value = true;
  error.value = "";

  try {
    // 使用api.file模块的verifyFilePassword方法进行密码验证
    const response = await api.file.verifyFilePassword(props.fileId, password.value);

    if (response.success) {
      // 密码验证成功，将用户输入的密码与API返回的URL一起传递给父组件
      emit("verified", {
        ...response.data, // 传递API返回的所有数据
        currentPassword: password.value, // 传递当前输入的密码，用于后续操作
      });
    } else {
      // 密码验证失败
      error.value = response.message || t("fileView.password.error");
    }
  } catch (err) {
    console.error("验证密码时出错:", err);
    // 优先使用HTTP状态码判断错误类型，更可靠
    if (err.status === ApiStatus.UNAUTHORIZED || err.response?.status === ApiStatus.UNAUTHORIZED || err.code === ApiStatus.UNAUTHORIZED) {
      // 401 Unauthorized - 密码错误
      error.value = "密码错误，请重新输入";
    } else if (err.status === ApiStatus.GONE || err.response?.status === ApiStatus.GONE || err.code === ApiStatus.GONE) {
      // 410 Gone - 资源已过期
      error.value = "此文件已过期或不可访问";
    } else if (err.status === ApiStatus.NOT_FOUND || err.response?.status === ApiStatus.NOT_FOUND || err.code === ApiStatus.NOT_FOUND) {
      // 404 Not Found - 资源不存在
      error.value = "此文件不存在或已被删除";
    } else {
      // 后备判断：基于错误消息内容判断错误类型（保持兼容性）
      if (err.message && (err.message.includes("密码错误") || err.message.includes("密码不正确") || err.message.includes("401"))) {
        error.value = t("fileView.password.error");
      } else if (err.message && (err.message.includes("已过期") || err.message.includes("410"))) {
        error.value = t("fileView.errors.forbidden");
      } else if (err.message && (err.message.includes("找不到") || err.message.includes("不存在") || err.message.includes("404"))) {
        error.value = t("fileView.errors.notFound");
      } else {
        error.value = err.message || t("fileView.errors.unknown");
      }
    }
  } finally {
    loading.value = false;
  }
};

// 清除表单
watch(
  () => props.fileId,
  () => {
    password.value = "";
    error.value = "";
    loading.value = false;
  }
);
</script>

<style>
/* 隐藏密码框的浏览器自带眼睛图标 */
.password-input::-ms-reveal,
.password-input::-ms-clear {
  display: none !important;
}

/* 确保表单不会自动填充 */
input:-webkit-autofill,
input:-webkit-autofill:hover,
input:-webkit-autofill:focus {
  transition: background-color 5000s ease-in-out 0s;
}
</style>
