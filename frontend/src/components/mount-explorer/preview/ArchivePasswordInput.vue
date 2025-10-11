<template>
  <div class="archive-password-container p-6 bg-white dark:bg-gray-800 rounded-lg shadow-lg">
    <div class="text-center mb-6">
      <!-- 锁图标 -->
      <div class="mx-auto w-16 h-16 bg-yellow-100 dark:bg-yellow-900/20 rounded-full flex items-center justify-center mb-4">
        <svg class="w-8 h-8 text-yellow-600 dark:text-yellow-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path
            stroke-linecap="round"
            stroke-linejoin="round"
            stroke-width="2"
            d="M12 15v2m-6 4h12a2 2 0 002-2v-6a2 2 0 00-2-2H6a2 2 0 00-2 2v6a2 2 0 002 2zm10-10V7a4 4 0 00-8 0v4h8z"
          />
        </svg>
      </div>

      <h3 class="text-lg font-semibold mb-2" :class="darkMode ? 'text-gray-200' : 'text-gray-800'">压缩文件已加密</h3>
      <p class="text-sm" :class="darkMode ? 'text-gray-400' : 'text-gray-600'">请输入密码以解压文件内容</p>
    </div>

    <form @submit.prevent="handleSubmit" class="space-y-4">
      <!-- 密码输入框 -->
      <div>
        <label for="archive-password" class="block text-sm font-medium mb-2" :class="darkMode ? 'text-gray-300' : 'text-gray-700'"> 密码 </label>
        <div class="relative">
          <input
            :type="showPassword ? 'text' : 'password'"
            id="archive-password"
            autocomplete="current-password"
            v-model="passwordInput"
            placeholder="请输入解压密码"
            class="block w-full px-3 py-2 pr-10 rounded-md shadow-sm border focus:ring-2 focus:ring-primary-500 focus:border-primary-500"
            :class="[
              darkMode ? 'bg-gray-700 border-gray-600 text-white focus:ring-offset-gray-800' : 'border-gray-300 text-gray-900 focus:ring-offset-white',
              passwordError ? 'border-red-500' : '',
            ]"
            :disabled="isValidating"
            ref="passwordInputRef"
          />
          <!-- 密码可见性切换按钮 -->
          <button type="button" @click="togglePasswordVisibility" class="absolute inset-y-0 right-0 pr-3 flex items-center" :disabled="isValidating">
            <svg class="w-5 h-5" :class="darkMode ? 'text-gray-400' : 'text-gray-500'" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path v-if="!showPassword" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 12a3 3 0 11-6 0 3 3 0 016 0z" />
              <path
                v-if="!showPassword"
                stroke-linecap="round"
                stroke-linejoin="round"
                stroke-width="2"
                d="M2.458 12C3.732 7.943 7.523 5 12 5c4.478 0 8.268 2.943 9.542 7-1.274 4.057-5.064 7-9.542 7-4.477 0-8.268-2.943-9.542-7z"
              />
              <path
                v-if="showPassword"
                stroke-linecap="round"
                stroke-linejoin="round"
                stroke-width="2"
                d="M13.875 18.825A10.05 10.05 0 0112 19c-4.478 0-8.268-2.943-9.543-7a9.97 9.97 0 011.563-3.029m5.858.908a3 3 0 114.243 4.243M9.878 9.878l4.242 4.242M9.878 9.878L3 3m6.878 6.878L21 21"
              />
            </svg>
          </button>
        </div>

        <!-- 错误提示 -->
        <div v-if="passwordError" class="mt-2 text-sm text-red-600 dark:text-red-400">
          {{ passwordError }}
        </div>
      </div>

      <!-- 操作按钮 -->
      <div class="flex space-x-3 pt-4">
        <button
          type="submit"
          :disabled="!passwordInput || isValidating"
          class="flex-1 bg-primary-600 hover:bg-primary-700 disabled:bg-gray-400 text-white font-medium py-2 px-4 rounded-md transition-colors focus:outline-none focus:ring-2 focus:ring-primary-500 focus:ring-offset-2"
          :class="darkMode ? 'focus:ring-offset-gray-800' : 'focus:ring-offset-white'"
        >
          <span v-if="!isValidating">确认</span>
          <span v-else class="flex items-center justify-center">
            <svg class="animate-spin -ml-1 mr-2 h-4 w-4 text-white" fill="none" viewBox="0 0 24 24">
              <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
              <path
                class="opacity-75"
                fill="currentColor"
                d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"
              ></path>
            </svg>
            验证中...
          </span>
        </button>

        <button
          type="button"
          @click="handleCancel"
          :disabled="isValidating"
          class="px-4 py-2 border border-gray-300 dark:border-gray-600 rounded-md text-sm font-medium focus:outline-none focus:ring-2 focus:ring-primary-500 focus:ring-offset-2 transition-colors"
          :class="[darkMode ? 'text-gray-300 hover:bg-gray-700 focus:ring-offset-gray-800' : 'text-gray-700 hover:bg-gray-50 focus:ring-offset-white']"
        >
          取消
        </button>
      </div>
    </form>
  </div>
</template>

<script setup>
import { ref, onMounted, nextTick } from "vue";

// Props
const props = defineProps({
  darkMode: {
    type: Boolean,
    default: false,
  },
  passwordError: {
    type: String,
    default: "",
  },
  isValidating: {
    type: Boolean,
    default: false,
  },
});

// Emits
const emit = defineEmits(["submit", "cancel"]);

// 响应式数据
const passwordInput = ref("");
const showPassword = ref(false);
const passwordInputRef = ref(null);

// 切换密码可见性
const togglePasswordVisibility = () => {
  showPassword.value = !showPassword.value;
};

// 提交密码
const handleSubmit = () => {
  if (passwordInput.value && !props.isValidating) {
    emit("submit", passwordInput.value);
  }
};

// 取消操作
const handleCancel = () => {
  emit("cancel");
};

// 组件挂载后自动聚焦
onMounted(async () => {
  await nextTick();
  if (passwordInputRef.value) {
    passwordInputRef.value.focus();
  }
});
</script>

<style scoped>
/* 自定义样式 */
.archive-password-container {
  max-width: 400px;
  margin: 0 auto;
}
</style>
