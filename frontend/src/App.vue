<script setup>
import { ref, watchEffect, onMounted, onBeforeUnmount, computed } from "vue";
import { useRoute } from "vue-router";
import EnvSwitcher from "./components/EnvSwitcher.vue";
import LanguageSwitcher from "./components/LanguageSwitcher.vue";
import PWAInstallPrompt from "./components/PWAInstallPrompt.vue";
import { useAuthStore } from "./stores/authStore.js";
import { useSiteConfigStore } from "./stores/siteConfigStore.js";
import FooterMarkdownRenderer from "./components/admin/FooterMarkdownRenderer.vue";

const route = useRoute();

// 使用认证Store和站点配置Store
const authStore = useAuthStore();
const siteConfigStore = useSiteConfigStore();

// 初始化主题模式状态
const prefersDark = window.matchMedia("(prefers-color-scheme: dark)").matches;
const savedThemeMode = localStorage.getItem("themeMode");

// 主题模式: "auto", "light", "dark"
const themeMode = ref(savedThemeMode || "auto");
// 实际的暗色模式状态
const isDarkMode = ref(themeMode.value === "auto" ? prefersDark : themeMode.value === "dark");

// 计算当前页面 - 基于路由
const activePage = computed(() => {
  return route.meta?.originalPage || "home";
});

// 过渡状态，用于页面切换动画
const transitioning = ref(false);

// 移动端菜单状态
const isMobileMenuOpen = ref(false);

// 环境切换器显示状态
const showEnvSwitcher = ref(false);

// GitHub 链接
const githubUrl = "https://github.com/ling-drag0n/CloudPaste";

// 检查是否为开发环境
const isDev = import.meta.env.DEV;

// 计算是否显示页脚
const shouldShowFooter = computed(() => {
  // 管理面板页面不显示页脚
  if (activePage.value === "admin") {
    return false;
  }

  const footerMarkdown = siteConfigStore.siteFooterMarkdown;
  return footerMarkdown && footerMarkdown.trim();
});

// 系统主题媒体查询
let darkModeMediaQuery;
// 系统主题变化的处理函数
const darkModeHandler = (e) => {
  if (themeMode.value === "auto") {
    isDarkMode.value = e.matches;
    updateTheme();
  }
};

// 切换移动端菜单状态
const toggleMobileMenu = () => {
  isMobileMenuOpen.value = !isMobileMenuOpen.value;
};

// 切换主题模式
const toggleThemeMode = () => {
  const modes = ["auto", "light", "dark"];
  const currentIndex = modes.indexOf(themeMode.value);
  const nextIndex = (currentIndex + 1) % modes.length;
  themeMode.value = modes[nextIndex];

  // 保存到本地存储
  localStorage.setItem("themeMode", themeMode.value);

  // 更新主题
  updateTheme();
};

// 更新主题函数
const updateTheme = () => {
  if (themeMode.value === "auto") {
    isDarkMode.value = window.matchMedia("(prefers-color-scheme: dark)").matches;
  } else {
    isDarkMode.value = themeMode.value === "dark";
  }

  // 更新 DOM 类
  if (isDarkMode.value) {
    document.documentElement.classList.add("dark");
    document.body.classList.add("bg-custom-bg-900", "text-custom-text-dark");
    document.body.classList.remove("bg-custom-bg-50", "text-custom-text");
  } else {
    document.documentElement.classList.remove("dark");
    document.body.classList.add("bg-custom-bg-50", "text-custom-text");
    document.body.classList.remove("bg-custom-bg-900", "text-custom-text-dark");
  }

  // 保存主题模式到本地存储
  localStorage.setItem("themeMode", themeMode.value);
};

// 监听并更新 DOM 的主题类
watchEffect(() => {
  updateTheme();
});

// 组件挂载时初始化
onMounted(() => {
  // 初始化系统主题媒体查询
  darkModeMediaQuery = window.matchMedia("(prefers-color-scheme: dark)");

  // 为媒体查询添加监听器
  darkModeMediaQuery.addEventListener("change", darkModeHandler);

  // 初始化主题
  updateTheme();

  // 在开发环境中始终显示环境切换器
  if (isDev) {
    showEnvSwitcher.value = true;
  } else {
    // 在生产环境中，只有在明确的条件下才显示：
    // 1. 存在管理员token
    // 2. URL中有特定的参数 (showEnvSwitcher) 如："https://域名.com?showEnvSwitcher"
    const hasAdminToken = authStore.isAdmin;
    const urlParams = new URLSearchParams(window.location.search);
    const hasEnvParam = urlParams.has("showEnvSwitcher");

    // 确保在生产环境中默认不显示
    showEnvSwitcher.value = hasAdminToken && hasEnvParam;
  }

  console.log("应用初始化完成");
});

// 组件卸载时移除事件监听
onBeforeUnmount(() => {
  if (darkModeMediaQuery) {
    if (darkModeMediaQuery.removeEventListener) {
      darkModeMediaQuery.removeEventListener("change", darkModeHandler);
    } else {
      darkModeMediaQuery.removeListener(darkModeHandler);
    }
  }
});
</script>

<template>
  <div :class="['app-container min-h-screen transition-colors duration-200', isDarkMode ? 'bg-custom-bg-900 text-custom-text-dark' : 'bg-custom-bg-50 text-custom-text']">
    <header :class="['sticky top-0 z-50 shadow-sm transition-colors', isDarkMode ? 'bg-custom-surface-dark' : 'bg-custom-surface']">
      <div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div class="flex justify-between h-16">
          <div class="flex">
            <div class="flex-shrink-0 flex items-center">
              <h1 class="text-xl font-bold">{{ siteConfigStore.siteTitle || $t("app.title") }}</h1>
            </div>
            <nav class="hidden sm:ml-6 sm:flex sm:space-x-8">
              <router-link
                to="/"
                :class="[
                  activePage === 'home' ? 'border-primary-500 text-current' : 'border-transparent hover:border-gray-300',
                  'inline-flex items-center px-1 pt-1 border-b-2 text-sm font-medium transition-colors duration-200',
                  activePage !== 'home' && isDarkMode ? 'text-gray-300 hover:text-gray-100' : activePage !== 'home' ? 'text-gray-500 hover:text-gray-700' : '',
                ]"
              >
                {{ $t("nav.home") }}
              </router-link>
              <router-link
                to="/upload"
                :class="[
                  activePage === 'upload' ? 'border-primary-500 text-current' : 'border-transparent hover:border-gray-300',
                  'inline-flex items-center px-1 pt-1 border-b-2 text-sm font-medium transition-colors duration-200',
                  activePage !== 'upload' && isDarkMode ? 'text-gray-300 hover:text-gray-100' : activePage !== 'upload' ? 'text-gray-500 hover:text-gray-700' : '',
                ]"
              >
                {{ $t("nav.upload") }}
              </router-link>
              <router-link
                to="/mount-explorer"
                :class="[
                  activePage === 'mount-explorer' ? 'border-primary-500 text-current' : 'border-transparent hover:border-gray-300',
                  'inline-flex items-center px-1 pt-1 border-b-2 text-sm font-medium transition-colors duration-200',
                  activePage !== 'mount-explorer' && isDarkMode ? 'text-gray-300 hover:text-gray-100' : activePage !== 'mount-explorer' ? 'text-gray-500 hover:text-gray-700' : '',
                ]"
              >
                {{ $t("nav.mountExplorer") }}
              </router-link>
              <router-link
                to="/admin"
                :class="[
                  activePage === 'admin' ? 'border-primary-500 text-current' : 'border-transparent hover:border-gray-300',
                  'inline-flex items-center px-1 pt-1 border-b-2 text-sm font-medium transition-colors duration-200',
                  activePage !== 'admin' && isDarkMode ? 'text-gray-300 hover:text-gray-100' : activePage !== 'admin' ? 'text-gray-500 hover:text-gray-700' : '',
                ]"
              >
                {{ $t("nav.admin") }}
              </router-link>
            </nav>
          </div>
          <div class="hidden sm:ml-6 sm:flex sm:items-center space-x-2">
            <a
              :href="githubUrl"
              target="_blank"
              rel="noopener noreferrer"
              :class="[
                'p-2 rounded-full focus:outline-none transition-colors',
                isDarkMode ? 'text-gray-300 hover:text-white hover:bg-gray-700' : 'text-gray-500 hover:text-gray-700 hover:bg-gray-100',
              ]"
              aria-label="GitHub"
              title="GitHub"
            >
              <svg class="h-5 w-5" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="currentColor">
                <path
                  d="M12 0c-6.626 0-12 5.373-12 12 0 5.302 3.438 9.8 8.207 11.387.599.111.793-.261.793-.577v-2.234c-3.338.726-4.033-1.416-4.033-1.416-.546-1.387-1.333-1.756-1.333-1.756-1.089-.745.083-.729.083-.729 1.205.084 1.839 1.237 1.839 1.237 1.07 1.834 2.807 1.304 3.492.997.107-.775.418-1.305.762-1.604-2.665-.305-5.467-1.334-5.467-5.931 0-1.311.469-2.381 1.236-3.221-.124-.303-.535-1.524.117-3.176 0 0 1.008-.322 3.301 1.23.957-.266 1.983-.399 3.003-.404 1.02.005 2.047.138 3.006.404 2.291-1.552 3.297-1.23 3.297-1.23.653 1.653.242 2.874.118 3.176.77.84 1.235 1.911 1.235 3.221 0 4.609-2.807 5.624-5.479 5.921.43.372.823 1.102.823 2.222v3.293c0 .319.192.694.801.576 4.765-1.589 8.199-6.086 8.199-11.386 0-6.627-5.373-12-12-12z"
                />
              </svg>
            </a>

            <LanguageSwitcher :darkMode="isDarkMode" />

            <button
              type="button"
              @click="toggleThemeMode"
              :class="[
                'p-2 rounded-full focus:outline-none transition-colors mr-2',
                isDarkMode ? 'text-yellow-300 hover:text-yellow-200 hover:bg-gray-700' : 'text-gray-400 hover:text-gray-500 hover:bg-gray-100',
              ]"
            >
              <span class="sr-only">{{ $t("theme.toggle") }}</span>
              <!-- 自动模式图标 - 半亮半暗 -->
              <svg v-if="themeMode === 'auto'" class="h-5 w-5" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <!-- 简单的圆圈，左半亮色右半暗色 -->
                <circle cx="12" cy="12" r="9" fill="none" stroke="currentColor" stroke-width="2" />
                <!-- 左半部分：亮色 -->
                <path d="M12 3 A 9 9 0 0 0 12 21 Z" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" />
                <!-- 右半部分：暗色 -->
                <path d="M12 3 A 9 9 0 0 1 12 21 Z" fill="currentColor" stroke="currentColor" stroke-width="2" stroke-linecap="round" />
              </svg>
              <!-- 暗色模式图标 -->
              <svg v-else-if="themeMode === 'dark'" class="h-5 w-5" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M20.354 15.354A9 9 0 018.646 3.646 9.003 9.003 0 0012 21a9.003 9.003 0 008.354-5.646z" />
              </svg>
              <!-- 亮色模式图标 -->
              <svg v-else class="h-5 w-5" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path
                  stroke-linecap="round"
                  stroke-linejoin="round"
                  stroke-width="2"
                  d="M12 3v1m0 16v1m9-9h-1M4 12H3m15.364 6.364l-.707-.707M6.343 6.343l-.707-.707m12.728 0l-.707.707M6.343 17.657l-.707.707M16 12a4 4 0 11-8 0 4 4 0 018 0z"
                />
              </svg>
            </button>
          </div>

          <!-- 移动端菜单按钮 -->
          <div class="flex items-center sm:hidden">
            <a
              :href="githubUrl"
              target="_blank"
              rel="noopener noreferrer"
              :class="[
                'p-2 rounded-full focus:outline-none transition-colors mr-2',
                isDarkMode ? 'text-gray-300 hover:text-white hover:bg-gray-700' : 'text-gray-500 hover:text-gray-700 hover:bg-gray-100',
              ]"
              aria-label="GitHub"
              title="GitHub"
            >
              <svg class="h-5 w-5" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="currentColor">
                <path
                  d="M12 0c-6.626 0-12 5.373-12 12 0 5.302 3.438 9.8 8.207 11.387.599.111.793-.261.793-.577v-2.234c-3.338.726-4.033-1.416-4.033-1.416-.546-1.387-1.333-1.756-1.333-1.756-1.089-.745.083-.729.083-.729 1.205.084 1.839 1.237 1.839 1.237 1.07 1.834 2.807 1.304 3.492.997.107-.775.418-1.305.762-1.604-2.665-.305-5.467-1.334-5.467-5.931 0-1.311.469-2.381 1.236-3.221-.124-.303-.535-1.524.117-3.176 0 0 1.008-.322 3.301 1.23.957-.266 1.983-.399 3.003-.404 1.02.005 2.047.138 3.006.404 2.291-1.552 3.297-1.23 3.297-1.23.653 1.653.242 2.874.118 3.176.77.84 1.235 1.911 1.235 3.221 0 4.609-2.807 5.624-5.479 5.921.43.372.823 1.102.823 2.222v3.293c0 .319.192.694.801.576 4.765-1.589 8.199-6.086 8.199-11.386 0-6.627-5.373-12-12-12z"
                />
              </svg>
            </a>

            <LanguageSwitcher :darkMode="isDarkMode" class="mr-2" />

            <button
              type="button"
              @click="toggleThemeMode"
              :class="[
                'p-2 rounded-full focus:outline-none transition-colors mr-2',
                isDarkMode ? 'text-yellow-300 hover:text-yellow-200 hover:bg-gray-700' : 'text-gray-400 hover:text-gray-500 hover:bg-gray-100',
              ]"
              :aria-label="$t('theme.toggle')"
            >
              <span class="sr-only">{{ $t("theme.toggle") }}</span>
              <!-- 自动模式图标 - 半亮半暗 -->
              <svg v-if="themeMode === 'auto'" class="h-5 w-5" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <!-- 简单的圆圈，左半亮色右半暗色 -->
                <circle cx="12" cy="12" r="9" fill="none" stroke="currentColor" stroke-width="2" />
                <!-- 左半部分：亮色 -->
                <path d="M12 3 A 9 9 0 0 0 12 21 Z" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" />
                <!-- 右半部分：暗色 -->
                <path d="M12 3 A 9 9 0 0 1 12 21 Z" fill="currentColor" stroke="currentColor" stroke-width="2" stroke-linecap="round" />
              </svg>
              <!-- 暗色模式图标 -->
              <svg v-else-if="themeMode === 'dark'" class="h-5 w-5" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M20.354 15.354A9 9 0 018.646 3.646 9.003 9.003 0 0012 21a9.003 9.003 0 008.354-5.646z" />
              </svg>
              <!-- 亮色模式图标 -->
              <svg v-else class="h-5 w-5" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path
                  stroke-linecap="round"
                  stroke-linejoin="round"
                  stroke-width="2"
                  d="M12 3v1m0 16v1m9-9h-1M4 12H3m15.364 6.364l-.707-.707M6.343 6.343l-.707-.707m12.728 0l-.707.707M6.343 17.657l-.707.707M16 12a4 4 0 11-8 0 4 4 0 018 0z"
                />
              </svg>
            </button>
            <button
              type="button"
              @click="toggleMobileMenu"
              :class="[
                'inline-flex items-center justify-center p-2 rounded-full focus:outline-none transition-all duration-200',
                isMobileMenuOpen
                  ? isDarkMode
                    ? 'bg-gray-700 text-white'
                    : 'bg-gray-200 text-gray-900'
                  : isDarkMode
                  ? 'text-gray-300 hover:text-white hover:bg-gray-700'
                  : 'text-gray-500 hover:text-gray-700 hover:bg-gray-100',
              ]"
              :aria-expanded="isMobileMenuOpen"
              :aria-label="$t('nav.menu')"
            >
              <!-- 菜单图标 -->
              <svg v-if="!isMobileMenuOpen" class="h-6 w-6" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 6h16M4 12h16M4 18h16" />
              </svg>
              <!-- 关闭图标 -->
              <svg v-else class="h-6 w-6" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
              </svg>
            </button>
          </div>
        </div>
      </div>

      <!-- 移动端菜单面板 -->
      <div class="sm:hidden overflow-hidden transition-all duration-300 ease-in-out" :class="[isMobileMenuOpen ? 'max-h-80' : 'max-h-0']">
        <div :class="['py-3 border-t transition-colors', isDarkMode ? 'bg-gray-800 border-gray-700' : 'bg-white border-gray-200']">
          <router-link
            to="/"
            @click="isMobileMenuOpen = false"
            :class="[
              'flex items-center px-4 py-3 transition-colors duration-200',
              activePage === 'home'
                ? isDarkMode
                  ? 'bg-gray-700 text-white'
                  : 'bg-gray-100 text-gray-900'
                : isDarkMode
                ? 'text-gray-300 hover:bg-gray-700 hover:text-white'
                : 'text-gray-600 hover:bg-gray-50 hover:text-gray-900',
            ]"
          >
            <span class="ml-2">{{ $t("nav.home") }}</span>
          </router-link>
          <router-link
            to="/upload"
            @click="isMobileMenuOpen = false"
            :class="[
              'flex items-center px-4 py-3 transition-colors duration-200',
              activePage === 'upload'
                ? isDarkMode
                  ? 'bg-gray-700 text-white'
                  : 'bg-gray-100 text-gray-900'
                : isDarkMode
                ? 'text-gray-300 hover:bg-gray-700 hover:text-white'
                : 'text-gray-600 hover:bg-gray-50 hover:text-gray-900',
            ]"
          >
            <span class="ml-2">{{ $t("nav.upload") }}</span>
          </router-link>
          <router-link
            to="/mount-explorer"
            @click="isMobileMenuOpen = false"
            :class="[
              'flex items-center px-4 py-3 transition-colors duration-200',
              activePage === 'mount-explorer'
                ? isDarkMode
                  ? 'bg-gray-700 text-white'
                  : 'bg-gray-100 text-gray-900'
                : isDarkMode
                ? 'text-gray-300 hover:bg-gray-700 hover:text-white'
                : 'text-gray-600 hover:bg-gray-50 hover:text-gray-900',
            ]"
          >
            <span class="ml-2">{{ $t("nav.mountExplorer") }}</span>
          </router-link>
          <router-link
            to="/admin"
            @click="isMobileMenuOpen = false"
            :class="[
              'flex items-center px-4 py-3 transition-colors duration-200',
              activePage === 'admin'
                ? isDarkMode
                  ? 'bg-gray-700 text-white'
                  : 'bg-gray-100 text-gray-900'
                : isDarkMode
                ? 'text-gray-300 hover:bg-gray-700 hover:text-white'
                : 'text-gray-600 hover:bg-gray-50 hover:text-gray-900',
            ]"
          >
            <span class="ml-2">{{ $t("nav.admin") }}</span>
          </router-link>
        </div>
      </div>
    </header>

    <main class="flex-1 flex flex-col" :class="activePage !== 'admin' ? 'pb-8' : ''">
      <router-view :dark-mode="isDarkMode" class="transition-opacity duration-300 flex-1 flex flex-col" :class="{ 'opacity-0': transitioning }" />
    </main>

    <footer v-if="shouldShowFooter" :class="['border-t transition-colors mt-auto', isDarkMode ? 'bg-custom-surface-dark border-gray-700' : 'bg-custom-surface border-gray-200']">
      <div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 text-center py-6">
        <FooterMarkdownRenderer :content="siteConfigStore.siteFooterMarkdown" :dark-mode="isDarkMode" />
      </div>
    </footer>

    <!-- 添加环境切换器组件 (在开发环境或管理员登录状态下显示) -->
    <!-- <EnvSwitcher v-if="showEnvSwitcher" /> -->

    <!-- PWA 安装提示组件 -->
    <PWAInstallPrompt :dark-mode="isDarkMode" />
  </div>
</template>

<style>
/* 全局样式 */
.app-container {
  display: flex;
  flex-direction: column;
  min-height: 100vh;
}

main {
  flex: 1;
  display: flex;
  flex-direction: column;
}

/* 移动端菜单过渡动画 */
.sm\:hidden {
  transition: all 0.3s ease-in-out;
}

/* 按钮触摸反馈 */
@media (max-width: 640px) {
  button,
  a {
    -webkit-tap-highlight-color: transparent;
  }
}

/* 禁用移动端点击时的蓝色高亮 */
* {
  -webkit-tap-highlight-color: transparent;
}
</style>
