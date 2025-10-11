<script setup>
import { ref, computed } from "vue";
import { api } from "@/api";
import { useI18n } from "vue-i18n";
import { useAuthStore } from "@/stores/authStore";

// 使用i18n和认证Store
const { t } = useI18n();
const authStore = useAuthStore();

// 检测用户类型
const isAdmin = computed(() => authStore.isAdmin);
const isApiKeyUser = computed(() => authStore.authType === "apikey");

// 定义props，接收父组件传递的darkMode
const props = defineProps({
  darkMode: {
    type: Boolean,
    required: true,
  },
});

// 定义事件，用于通知父组件需要退出登录
const emit = defineEmits(["logout"]);

// 密码更改表单
const passwordForm = ref({
  currentPassword: "",
  newPassword: "",
  newUsername: "",
});

// 密码更改状态
const passwordChangeStatus = ref({
  loading: false,
  success: false,
  error: "",
});

// 倒计时计数器
const countdown = ref(3);
// 倒计时定时器ID
let countdownTimer = null;

// 更改密码
const handleChangePassword = async (event) => {
  event.preventDefault();

  // 验证表单
  if (!passwordForm.value.currentPassword) {
    passwordChangeStatus.value.error = t("admin.account.messages.passwordRequired");
    // 设置3秒后自动清除错误信息
    setTimeout(() => {
      passwordChangeStatus.value.error = "";
    }, 3000);
    return;
  }

  // 确保新密码或新用户名至少填写一个
  if (!passwordForm.value.newPassword && !passwordForm.value.newUsername) {
    passwordChangeStatus.value.error = t("admin.account.messages.newFieldRequired");
    // 设置3秒后自动清除错误信息
    setTimeout(() => {
      passwordChangeStatus.value.error = "";
    }, 3000);
    return;
  }

  // 如果新密码与当前密码相同，给出提示（虽然后端也会验证，但前端提前验证可以减少无效请求）
  if (passwordForm.value.newPassword && passwordForm.value.newPassword === passwordForm.value.currentPassword) {
    passwordChangeStatus.value.error = t("admin.account.messages.samePassword");
    // 设置3秒后自动清除错误信息
    setTimeout(() => {
      passwordChangeStatus.value.error = "";
    }, 3000);
    return;
  }

  passwordChangeStatus.value = {
    loading: true,
    success: false,
    error: "",
  };

  try {
    // 调用API修改密码
    await api.admin.changePassword(passwordForm.value.currentPassword, passwordForm.value.newPassword, passwordForm.value.newUsername);

    // 更新成功
    passwordChangeStatus.value.success = true;
    passwordForm.value = {
      currentPassword: "",
      newPassword: "",
      newUsername: "",
    };

    // 重置倒计时
    countdown.value = 3;

    // 清除之前的倒计时
    if (countdownTimer) {
      clearInterval(countdownTimer);
    }

    // 启动倒计时
    countdownTimer = setInterval(() => {
      countdown.value -= 1;
      if (countdown.value <= 0) {
        clearInterval(countdownTimer);
        passwordChangeStatus.value.success = false;
        // 由于后端会删除所有token，所以自动触发登出
        emit("logout");
      }
    }, 1000);
  } catch (error) {
    // 发生错误时，仅显示错误消息，不执行登出
    passwordChangeStatus.value.error = error.message || t("admin.account.messages.updateFailed");

    // 3秒后自动清除错误消息
    setTimeout(() => {
      passwordChangeStatus.value.error = "";
    }, 3000);
  } finally {
    passwordChangeStatus.value.loading = false;
  }
};
</script>

<template>
  <div class="flex-1 flex flex-col overflow-y-auto">
    <!-- 页面标题和说明 -->
    <div class="mb-6">
      <h1 class="text-2xl font-bold mb-2" :class="darkMode ? 'text-white' : 'text-gray-800'">
        {{ isAdmin ? t("admin.account.title") : t("admin.account.apiKeyTitle") }}
      </h1>
      <p class="text-base" :class="darkMode ? 'text-gray-300' : 'text-gray-600'">
        {{ isAdmin ? t("admin.account.description") : t("admin.account.apiKeyDescription") }}
      </p>
    </div>

    <!-- 状态消息 -->
    <div v-if="passwordChangeStatus.success || passwordChangeStatus.error" class="mb-6">
      <div
        v-if="passwordChangeStatus.success"
        class="rounded-lg p-4 border"
        :class="darkMode ? 'bg-green-900/20 border-green-800/40 text-green-200' : 'bg-green-50 border-green-200 text-green-800'"
      >
        <div class="flex items-center">
          <svg class="h-5 w-5 mr-2 flex-shrink-0" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20" fill="currentColor">
            <path
              fill-rule="evenodd"
              d="M10 18a8 8 0 100-16 8 8 0 000 16zm3.707-9.293a1 1 0 00-1.414-1.414L9 10.586 7.707 9.293a1 1 0 00-1.414 1.414l2 2a1 1 0 001.414 0l4-4z"
              clip-rule="evenodd"
            />
          </svg>
          <p class="text-sm font-medium">
            管理员信息更新成功，即将自动退出登录
            <span class="ml-1 inline-flex items-center justify-center bg-green-100 dark:bg-green-800 text-green-800 dark:text-green-200 rounded-full h-5 w-5 text-xs font-bold">{{
              countdown
            }}</span>
          </p>
        </div>
      </div>

      <div
        v-if="passwordChangeStatus.error"
        class="rounded-lg p-4 border"
        :class="darkMode ? 'bg-red-900/20 border-red-800/40 text-red-200' : 'bg-red-50 border-red-200 text-red-800'"
      >
        <div class="flex items-center">
          <svg class="h-5 w-5 mr-2 flex-shrink-0" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20" fill="currentColor">
            <path
              fill-rule="evenodd"
              d="M10 18a8 8 0 100-16 8 8 0 000 16zM8.707 7.293a1 1 0 00-1.414 1.414L8.586 10l-1.293 1.293a1 1 0 101.414 1.414L10 11.414l1.293 1.293a1 1 0 001.414-1.414L11.414 10l1.293-1.293a1 1 0 00-1.414-1.414L10 8.586 8.707 7.293z"
              clip-rule="evenodd"
            />
          </svg>
          <p class="text-sm font-medium">{{ passwordChangeStatus.error }}</p>
        </div>
      </div>
    </div>

    <!-- 设置表单 -->
    <div class="space-y-6">
      <!-- API密钥用户信息显示 -->
      <div v-if="isApiKeyUser" class="setting-group bg-white dark:bg-gray-800 rounded-lg p-6 border border-gray-200 dark:border-gray-700 max-w-2xl">
        <h2 class="text-lg font-medium mb-4 pb-2 border-b border-gray-200 dark:border-gray-700">{{ t("admin.account.apiKeyInfo.title") }}</h2>
        <div class="space-y-4">
          <div class="grid grid-cols-1 md:grid-cols-3 gap-4 items-center">
            <div class="md:col-span-1">
              <span class="block text-sm font-medium" :class="darkMode ? 'text-gray-200' : 'text-gray-700'">{{ t("admin.account.apiKeyInfo.keyName") }}</span>
            </div>
            <div class="md:col-span-2">
              <span class="text-sm" :class="darkMode ? 'text-gray-300' : 'text-gray-600'">{{ authStore.apiKeyInfo?.name || t("common.unknown") }}</span>
            </div>
          </div>

          <div class="grid grid-cols-1 md:grid-cols-3 gap-4 items-center">
            <div class="md:col-span-1">
              <span class="block text-sm font-medium" :class="darkMode ? 'text-gray-200' : 'text-gray-700'">{{ t("admin.account.apiKeyInfo.basicPath") }}</span>
            </div>
            <div class="md:col-span-2">
              <span class="text-sm" :class="darkMode ? 'text-gray-300' : 'text-gray-600'">{{ authStore.apiKeyInfo?.basic_path || "/" }}</span>
            </div>
          </div>
        </div>
      </div>

      <!-- 管理员信息修改设置组 -->
      <div v-if="isAdmin" class="setting-group bg-white dark:bg-gray-800 rounded-lg p-6 border border-gray-200 dark:border-gray-700 max-w-2xl">
        <h2 class="text-lg font-medium mb-4 pb-2 border-b border-gray-200 dark:border-gray-700">{{ t("admin.account.adminInfo.title") }}</h2>
        <div class="space-y-4">
          <p class="text-sm" :class="darkMode ? 'text-gray-400' : 'text-gray-500'">{{ t("admin.account.adminInfo.description") }}</p>

          <form @submit="handleChangePassword" class="space-y-6">
            <!-- 新用户名 -->
            <div class="setting-item grid grid-cols-1 md:grid-cols-3 gap-4 items-start">
              <div class="md:col-span-1">
                <label for="newUsername" class="block text-sm font-medium" :class="darkMode ? 'text-gray-200' : 'text-gray-700'">{{
                  t("admin.account.adminInfo.newUsernameLabel")
                }}</label>
              </div>
              <div class="md:col-span-2">
                <div class="relative">
                  <input
                    type="text"
                    name="newUsername"
                    id="newUsername"
                    v-model="passwordForm.newUsername"
                    class="block w-full rounded border shadow-sm pl-3 pr-10 py-2 text-sm"
                    :class="
                      darkMode
                        ? 'bg-gray-700 border-gray-600 text-white placeholder-gray-400 focus:ring-blue-500 focus:border-blue-500'
                        : 'bg-white border-gray-300 text-gray-900 placeholder-gray-500 focus:ring-blue-500 focus:border-blue-500'
                    "
                    :placeholder="t('admin.account.adminInfo.newUsernamePlaceholder')"
                  />
                  <div class="absolute inset-y-0 right-0 flex items-center pr-3 pointer-events-none">
                    <svg
                      class="h-5 w-5"
                      :class="darkMode ? 'text-gray-500' : 'text-gray-400'"
                      xmlns="http://www.w3.org/2000/svg"
                      fill="none"
                      viewBox="0 0 24 24"
                      stroke="currentColor"
                    >
                      <path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.5" d="M16 7a4 4 0 11-8 0 4 4 0 018 0zM12 14a7 7 0 00-7 7h14a7 7 0 00-7-7z" />
                    </svg>
                  </div>
                </div>
                <p class="mt-2 text-xs" :class="darkMode ? 'text-gray-400' : 'text-gray-500'">{{ t("admin.account.adminInfo.newUsernameHint") }}</p>
              </div>
            </div>

            <!-- 当前密码 -->
            <div class="setting-item grid grid-cols-1 md:grid-cols-3 gap-4 items-start">
              <div class="md:col-span-1">
                <label for="currentPassword" class="block text-sm font-medium" :class="darkMode ? 'text-gray-200' : 'text-gray-700'">
                  {{ t("admin.account.adminInfo.currentPasswordLabel") }} <span class="text-red-500">*</span>
                </label>
              </div>
              <div class="md:col-span-2">
                <div class="relative">
                  <input
                    type="password"
                    name="currentPassword"
                    id="currentPassword"
                    autocomplete="current-password"
                    v-model="passwordForm.currentPassword"
                    class="block w-full rounded border shadow-sm pl-3 pr-10 py-2 text-sm"
                    :class="
                      darkMode
                        ? 'bg-gray-700 border-gray-600 text-white placeholder-gray-400 focus:ring-blue-500 focus:border-blue-500'
                        : 'bg-white border-gray-300 text-gray-900 placeholder-gray-500 focus:ring-blue-500 focus:border-blue-500'
                    "
                    :placeholder="t('admin.account.adminInfo.currentPasswordPlaceholder')"
                    required
                  />
                  <div class="absolute inset-y-0 right-0 flex items-center pr-3 pointer-events-none">
                    <svg
                      class="h-5 w-5"
                      :class="darkMode ? 'text-gray-500' : 'text-gray-400'"
                      xmlns="http://www.w3.org/2000/svg"
                      fill="none"
                      viewBox="0 0 24 24"
                      stroke="currentColor"
                    >
                      <path
                        stroke-linecap="round"
                        stroke-linejoin="round"
                        stroke-width="1.5"
                        d="M15 7a2 2 0 012 2m4 0a6 6 0 01-7.743 5.743L11 17H9v2H7v2H4a1 1 0 01-1-1v-2.586a1 1 0 01.293-.707l5.964-5.964A6 6 0 1 1 21 9z"
                      />
                    </svg>
                  </div>
                </div>
                <p class="mt-1 text-xs" :class="darkMode ? 'text-gray-400' : 'text-gray-500'">
                  {{ t("admin.account.adminInfo.currentPasswordHint") }}
                </p>
              </div>
            </div>

            <!-- 新密码 -->
            <div class="setting-item grid grid-cols-1 md:grid-cols-3 gap-4 items-start">
              <div class="md:col-span-1">
                <label for="newPassword" class="block text-sm font-medium" :class="darkMode ? 'text-gray-200' : 'text-gray-700'">{{
                  t("admin.account.adminInfo.newPasswordLabel")
                }}</label>
              </div>
              <div class="md:col-span-2">
                <div class="relative">
                  <input
                    type="password"
                    name="newPassword"
                    id="newPassword"
                    autocomplete="new-password"
                    v-model="passwordForm.newPassword"
                    class="block w-full rounded border shadow-sm pl-3 pr-10 py-2 text-sm"
                    :class="
                      darkMode
                        ? 'bg-gray-700 border-gray-600 text-white placeholder-gray-400 focus:ring-blue-500 focus:border-blue-500'
                        : 'bg-white border-gray-300 text-gray-900 placeholder-gray-500 focus:ring-blue-500 focus:border-blue-500'
                    "
                    :placeholder="t('admin.account.adminInfo.newPasswordPlaceholder')"
                  />
                  <div class="absolute inset-y-0 right-0 flex items-center pr-3 pointer-events-none">
                    <svg
                      class="h-5 w-5"
                      :class="darkMode ? 'text-gray-500' : 'text-gray-400'"
                      xmlns="http://www.w3.org/2000/svg"
                      fill="none"
                      viewBox="0 0 24 24"
                      stroke="currentColor"
                    >
                      <path
                        stroke-linecap="round"
                        stroke-linejoin="round"
                        stroke-width="1.5"
                        d="M12 15v2m-6 4h12a2 2 0 002-2v-6a2 2 0 00-2-2H6a2 2 0 00-2 2v6a2 2 0 002 2zm10-10V7a4 4 0 00-8 0v4h8z"
                      />
                    </svg>
                  </div>
                </div>
                <p class="mt-1 text-xs" :class="darkMode ? 'text-gray-400' : 'text-gray-500'">
                  {{ t("admin.account.adminInfo.newPasswordHint") }}
                </p>
              </div>
            </div>

            <div class="flex justify-end pt-4">
              <button
                type="submit"
                :disabled="passwordChangeStatus.loading"
                class="inline-flex items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md shadow-sm text-white bg-blue-600 hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500 disabled:opacity-50 disabled:cursor-not-allowed"
              >
                <span v-if="passwordChangeStatus.loading" class="flex items-center">
                  <svg class="animate-spin -ml-0.5 mr-2 h-4 w-4" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                    <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
                    <path
                      class="opacity-75"
                      fill="currentColor"
                      d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"
                    ></path>
                  </svg>
                  {{ t("admin.account.buttons.updating") }}
                </span>
                <span v-else>{{ t("admin.account.buttons.updateAccount") }}</span>
              </button>
            </div>
          </form>

          <!-- 警告提示 -->
          <div class="mt-6 p-4 rounded-md border" :class="darkMode ? 'bg-amber-900/20 border-amber-800/40' : 'bg-amber-50 border-amber-200'">
            <div class="flex items-start">
              <svg xmlns="http://www.w3.org/2000/svg" class="h-5 w-5 mr-2 mt-0.5 text-amber-500" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path
                  stroke-linecap="round"
                  stroke-linejoin="round"
                  stroke-width="2"
                  d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z"
                />
              </svg>
              <p class="text-sm" :class="darkMode ? 'text-amber-200' : 'text-amber-800'">{{ t("admin.account.adminInfo.warningMessage") }}</p>
            </div>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<style scoped>
.setting-group {
  background: transparent;
}

.setting-item {
  padding: 1rem 0;
}

.setting-item:not(:last-child) {
  border-bottom: 1px solid;
  border-color: inherit;
}

/* 移动端适配 */
@media (max-width: 768px) {
  .setting-item {
    grid-template-columns: 1fr;
  }
}
</style>
