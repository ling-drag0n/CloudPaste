<template>
  <div class="h-screen" :class="darkMode ? 'bg-gray-900' : 'bg-gray-100'">
    <AdminSidebar
      :dark-mode="darkMode"
      :permissions="userPermissions"
      :is-mobile-sidebar-open="isMobileSidebarOpen"
      @close-mobile-sidebar="closeMobileSidebar"
      @sidebar-toggle="handleSidebarToggle"
      @logout="handleLogout"
    />

    <!-- 移动端顶部栏 -->
    <AdminHeader :dark-mode="darkMode" @toggle-mobile-sidebar="toggleMobileSidebar" />

    <!-- 主内容区域 - 混合导航模式，响应侧边栏收缩状态 -->
    <main
      class="fixed left-0 top-32 md:top-16 right-0 bottom-0 overflow-y-auto focus:outline-none z-40 transition-all duration-300"
      :class="[sidebarCollapsed ? 'md:left-16' : 'md:left-64', darkMode ? 'bg-gray-900' : 'bg-gray-100']"
    >
      <!-- 内容区域 -->
      <div class="mx-auto w-full px-2 sm:px-4 md:px-6 lg:px-8 mt-2 md:mt-4 pb-4" style="max-width: 1280px">
        <div class="rounded-lg flex-1 flex flex-col" :class="darkMode ? 'bg-gray-800' : 'bg-white border border-gray-200'">
          <!-- 页面内容 -->
          <div class="p-2 md:p-4 flex-1 flex flex-col">
            <router-view :dark-mode="darkMode" :permissions="userPermissions" @logout="handleLogout" />
          </div>
        </div>
      </div>
    </main>
  </div>
</template>

<script setup>
import { ref, computed, onMounted, onBeforeUnmount } from "vue";
import { useRouter } from "vue-router";
import AdminSidebar from "@/components/admin/AdminSidebar.vue";
import AdminHeader from "@/components/admin/AdminHeader.vue";
import { useAuthStore } from "@/stores/authStore.js";

const props = defineProps({
  darkMode: {
    type: Boolean,
    required: true,
  },
});

// 使用认证Store和路由
const authStore = useAuthStore();
const router = useRouter();


// 计算用户权限
const userPermissions = computed(() => ({
  isAdmin: authStore.isAdmin,
  text: authStore.hasTextPermission,
  file: authStore.hasFilePermission,
  mount: authStore.hasMountPermission,
}));

const isMobileSidebarOpen = ref(false);
const sidebarCollapsed = ref(false);

const toggleMobileSidebar = () => {
  isMobileSidebarOpen.value = !isMobileSidebarOpen.value;
};

const closeMobileSidebar = () => {
  isMobileSidebarOpen.value = false;
};

// 处理侧边栏收缩状态变化
const handleSidebarToggle = (event) => {
  sidebarCollapsed.value = event.collapsed;
};

// 在组件加载时验证认证状态
onMounted(async () => {
  // 如果需要重新验证，则进行验证
  if (authStore.needsRevalidation) {
    console.log("AdminLayout: 需要重新验证认证状态");
    await authStore.validateAuth();
  }

  // 监听认证状态变化事件
  window.addEventListener("auth-state-changed", handleAuthStateChange);
});

// 组件卸载时移除事件监听
onBeforeUnmount(() => {
  window.removeEventListener("auth-state-changed", handleAuthStateChange);
});

// 处理认证状态变化
const handleAuthStateChange = (event) => {
  console.log("AdminLayout: 认证状态变化", event.detail);
  // 由于使用了响应式的计算属性，UI会自动更新
};

const emit = defineEmits(["logout"]);

// 处理登出
const handleLogout = async () => {
  console.log("AdminLayout: 执行登出");
  await authStore.logout();
  // 登出后重定向到首页
  router.push({ name: "Home" });
};
</script>
