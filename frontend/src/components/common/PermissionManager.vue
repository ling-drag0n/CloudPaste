<template>
  <div v-if="!hasPermission" class="permission-warning">
    <div
      class="mb-4 p-3 rounded-md border"
      :class="
        isApiKeyUserWithoutPermission
          ? 'bg-red-50 border-red-200 text-red-800 dark:bg-red-900/30 dark:border-red-700/50 dark:text-red-200'
          : 'bg-yellow-50 border-yellow-200 text-yellow-800 dark:bg-yellow-900/30 dark:border-yellow-700/50 dark:text-yellow-200'
      "
    >
      <div class="flex items-center">
        <svg xmlns="http://www.w3.org/2000/svg" class="h-5 w-5 mr-2" fill="none" viewBox="0 0 24 24" stroke="currentColor">
          <path
            stroke-linecap="round"
            stroke-linejoin="round"
            stroke-width="2"
            :d="
              isApiKeyUserWithoutPermission
                ? 'M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-2.5L13.732 4c-.77-.833-1.964-.833-2.732 0L4.082 16.5c-.77.833.192 2.5 1.732 2.5z'
                : 'M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z'
            "
          />
        </svg>
        <span v-if="isApiKeyUserWithoutPermission">
          {{ $t("common.noPermission") }}
        </span>
        <span v-else>
          {{ permissionRequiredText }}
          <a href="#" @click.prevent="navigateToAdmin" class="font-medium underline">{{ loginAuthText }}</a
          >。
        </span>
      </div>
    </div>
  </div>
</template>

<script setup>
import { computed, onMounted, onUnmounted } from "vue";
import { useAuthStore } from "@/stores/authStore.js";

// Props
const props = defineProps({
  darkMode: {
    type: Boolean,
    default: false,
  },
  permissionType: {
    type: String,
    default: "text", // text, file, mount
  },
  permissionRequiredText: {
    type: String,
    default: "",
  },
  loginAuthText: {
    type: String,
    default: "",
  },
});

// Emits
const emit = defineEmits(["permission-change", "navigate-to-admin"]);

// 使用认证Store
const authStore = useAuthStore();

// 从Store获取权限状态的计算属性
const isAdmin = computed(() => authStore.isAdmin);
const hasApiKey = computed(() => authStore.authType === "apikey" && !!authStore.apiKey);
const hasTextPermission = computed(() => authStore.hasTextPermission);
const hasFilePermission = computed(() => authStore.hasFilePermission);
const hasMountPermission = computed(() => authStore.hasMountPermission);

// 根据权限类型动态计算权限状态
const hasPermission = computed(() => {
  switch (props.permissionType) {
    case "file":
      return authStore.hasFilePermission;
    case "mount":
      return authStore.hasMountPermission;
    case "text":
    default:
      return authStore.hasTextPermission;
  }
});

// 判断是否为已登录但无权限的API密钥用户
const isApiKeyUserWithoutPermission = computed(() => {
  return authStore.isAuthenticated && authStore.authType === "apikey" && !hasPermission.value;
});

// 检查用户权限状态（简化版，主要用于触发事件）
const checkPermissionStatus = async () => {
  console.log("PermissionManager: 检查用户权限状态...");

  // 如果需要重新验证，则进行验证
  if (authStore.needsRevalidation) {
    console.log("PermissionManager: 需要重新验证认证状态");
    await authStore.validateAuth();
  }

  console.log("PermissionManager: 用户权限:", hasPermission.value ? "有权限" : "无权限");
  emit("permission-change", hasPermission.value);
};


// 导航到管理员登录页面
const navigateToAdmin = () => {
  emit("navigate-to-admin");
};

// 事件处理函数
const handleAuthStateChange = async (e) => {
  console.log("PermissionManager: 接收到认证状态变化事件:", e.detail);
  emit("permission-change", hasPermission.value);
};

// 组件挂载
onMounted(async () => {
  await checkPermissionStatus();

  // 监听认证状态变化事件
  window.addEventListener("auth-state-changed", handleAuthStateChange);
});

// 组件卸载
onUnmounted(() => {
  window.removeEventListener("auth-state-changed", handleAuthStateChange);
});

// 暴露方法和状态
defineExpose({
  hasPermission,
  isAdmin,
  hasApiKey,
  hasTextPermission,
  hasFilePermission,
  hasMountPermission,
  isApiKeyUserWithoutPermission,
  checkPermissionStatus,
});
</script>

<style scoped>
.permission-warning {
  margin-bottom: 1rem;
}
</style>
