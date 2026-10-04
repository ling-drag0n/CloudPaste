<template>
  <div>
    <!-- 桌面端侧边栏 - 为全局header留出空间，支持收缩 -->
    <div
      class="hidden md:block fixed left-0 top-16 border-r shadow-md z-30 transition-all duration-300"
      :class="[isCollapsed ? 'w-16' : 'w-64', 'h-[calc(100vh-4rem)]', darkMode ? 'bg-gray-800 border-gray-700' : 'bg-white border-gray-200']"
    >
      <div class="flex flex-col h-full">
        <!-- 桌面端头部区域 - Logo + 标题 + 收缩按钮 -->
        <div class="h-16 flex-shrink-0 border-b flex items-center" :class="darkMode ? 'border-gray-700' : 'border-gray-200'">
          <!-- 统一容器，避免重复创建img元素 -->
          <div class="relative w-full h-full flex items-center" :class="isCollapsed ? 'justify-center' : 'px-4'">
            <!-- 站点图标 - 单一元素，通过CSS控制位置 -->
            <div class="flex-shrink-0 w-8 h-8" :class="isCollapsed ? '' : 'mr-3'">
              <img
                :src="siteFaviconUrl || '/cloudpaste.svg'"
                :alt="siteTitle"
                class="w-8 h-8 object-contain"
                :title="isCollapsed ? `${siteTitle} - ${userTypeText}` : siteTitle"
                @error="handleImageError"
              />
            </div>

            <!-- 标题信息 - 展开状态显示 -->
            <transition
              name="fade-slide"
              enter-active-class="transition-all duration-300 delay-100"
              leave-active-class="transition-all duration-200"
              enter-from-class="opacity-0 transform translate-x-2"
              enter-to-class="opacity-100 transform translate-x-0"
              leave-from-class="opacity-100 transform translate-x-0"
              leave-to-class="opacity-0 transform translate-x-2"
            >
              <div v-if="!isCollapsed" class="flex-1 min-w-0">
                <div class="font-semibold text-sm truncate" :class="darkMode ? 'text-white' : 'text-gray-900'">
                  {{ siteTitle }}
                </div>
                <div class="text-xs opacity-75 truncate" :class="darkMode ? 'text-gray-300' : 'text-gray-600'">
                  {{ userTypeText }}
                </div>
              </div>
            </transition>

            <!-- 收缩按钮 - 展开状态显示 -->
            <transition
              name="fade-slide"
              enter-active-class="transition-all duration-300 delay-100"
              leave-active-class="transition-all duration-200"
              enter-from-class="opacity-0 transform translate-x-2"
              enter-to-class="opacity-100 transform translate-x-0"
              leave-from-class="opacity-100 transform translate-x-0"
              leave-to-class="opacity-0 transform translate-x-2"
            >
              <button
                v-if="!isCollapsed"
                @click="toggleCollapse"
                class="p-1.5 rounded-md transition-colors ml-2 flex-shrink-0"
                :class="darkMode ? 'text-gray-400 hover:text-gray-200 hover:bg-gray-700' : 'text-gray-500 hover:text-gray-700 hover:bg-gray-100'"
                :title="t('admin.sidebar.collapse')"
              >
                <IconChevronLeft size="sm" aria-hidden="true" />
              </button>
            </transition>

            <!-- 展开按钮 - 收缩状态显示，位于侧边栏右边缘 -->
            <button
              v-if="isCollapsed"
              @click="toggleCollapse"
              class="absolute -right-3 top-1/2 transform -translate-y-1/2 w-6 h-6 rounded-full border shadow-sm transition-all flex items-center justify-center"
              :class="
                darkMode
                  ? 'bg-gray-800 border-gray-600 text-gray-300 hover:text-white hover:bg-gray-700'
                  : 'bg-white border-gray-200 text-gray-600 hover:text-gray-900 hover:bg-gray-50'
              "
              :title="t('admin.sidebar.expand')"
            >
              <IconChevronRight size="xs" aria-hidden="true" />
            </button>
          </div>
        </div>

        <div class="flex-1 flex flex-col overflow-y-auto pt-4">
          <nav class="flex-1 px-2 space-y-1">
            <template v-for="item in visibleMenuItems">
              <!-- 普通菜单项 -->
              <router-link
                v-if="item.type === 'item'"
                :key="`item-${item.id}`"
                :to="{ name: item.routeName }"
                :class="[
                  $route.name === item.routeName
                    ? darkMode
                      ? 'bg-gray-900 text-white'
                      : 'bg-gray-100 text-gray-900'
                    : darkMode
                    ? 'text-gray-300 hover:bg-gray-700 hover:text-white'
                    : 'text-gray-600 hover:bg-gray-50 hover:text-gray-900',
                  'group flex items-center text-sm font-medium rounded-md transition-colors',
                  isCollapsed ? 'px-3 py-3 justify-center' : 'px-3 py-2.5',
                ]"
                :title="isCollapsed ? item.name : ''"
              >
                <component
                  :is="getMenuIconComponent(item.icon)"
                  size="lg"
                  class="flex-shrink-0"
                  :class="[
                    isCollapsed ? 'mx-auto' : 'mr-3',
                    $route.name === item.routeName ? 'text-primary-500' : darkMode ? 'text-gray-400 group-hover:text-gray-300' : 'text-gray-400 group-hover:text-gray-500',
                  ]"
                  aria-hidden="true"
                />
                <transition
                  name="fade-slide"
                  enter-active-class="transition-all duration-300 delay-100"
                  leave-active-class="transition-all duration-200"
                  enter-from-class="opacity-0 transform translate-x-2"
                  enter-to-class="opacity-100 transform translate-x-0"
                  leave-from-class="opacity-100 transform translate-x-0"
                  leave-to-class="opacity-0 transform translate-x-2"
                >
                  <span v-if="!isCollapsed" class="whitespace-nowrap">
                    {{ item.name }}
                  </span>
                </transition>
              </router-link>

              <!-- 带子菜单的菜单组 -->
              <div v-else-if="item.type === 'group'" :key="`group-${item.id}`" class="space-y-1">
                <!-- 收缩状态：显示为图标，点击展开侧边栏 -->
                <button
                  v-if="isCollapsed"
                  @click="handleGroupItemClick"
                  :class="[
                    darkMode ? 'text-gray-300 hover:bg-gray-700 hover:text-white' : 'text-gray-600 hover:bg-gray-50 hover:text-gray-900',
                    'w-full group flex items-center px-3 py-3 justify-center text-sm font-medium rounded-md cursor-pointer transition-colors',
                  ]"
                  :title="item.name"
                >
                  <component
                    :is="getMenuIconComponent(item.icon)"
                    size="lg"
                    class="mx-auto"
                    :class="darkMode ? 'text-gray-400 group-hover:text-gray-300' : 'text-gray-400 group-hover:text-gray-500'"
                    aria-hidden="true"
                  />
                </button>

                <!-- 展开状态：显示完整的组菜单 -->
                <template v-else>
                  <!-- 主菜单项 -->
                  <button
                    @click="item.id === 'system-settings' ? toggleSystemSettings() : toggleTaskManagement()"
                    :class="[
                      darkMode ? 'text-gray-300 hover:bg-gray-700 hover:text-white' : 'text-gray-600 hover:bg-gray-50 hover:text-gray-900',
                      'w-full group flex items-center justify-between px-3 py-2.5 text-sm font-medium rounded-md cursor-pointer',
                    ]"
                  >
                    <div class="flex items-center">
                      <component
                        :is="getMenuIconComponent(item.icon)"
                        size="lg"
                        class="flex-shrink-0"
                        :class="[isCollapsed ? 'mx-auto' : 'mr-3', darkMode ? 'text-gray-400 group-hover:text-gray-300' : 'text-gray-400 group-hover:text-gray-500']"
                        aria-hidden="true"
                      />
                      <transition
                        name="fade-slide"
                        enter-active-class="transition-all duration-300 delay-100"
                        leave-active-class="transition-all duration-200"
                        enter-from-class="opacity-0 transform translate-x-2"
                        enter-to-class="opacity-100 transform translate-x-0"
                        leave-from-class="opacity-100 transform translate-x-0"
                        leave-to-class="opacity-0 transform translate-x-2"
                      >
                        <span v-if="!isCollapsed" class="whitespace-nowrap">
                          {{ item.name }}
                        </span>
                      </transition>
                    </div>
                    <!-- 展开/收起箭头 -->
                    <transition
                      name="fade-slide"
                      enter-active-class="transition-all duration-300 delay-100"
                      leave-active-class="transition-all duration-200"
                      enter-from-class="opacity-0 transform translate-x-2"
                      enter-to-class="opacity-100 transform translate-x-0"
                      leave-from-class="opacity-100 transform translate-x-0"
                      leave-to-class="opacity-0 transform translate-x-2"
                    >
                      <IconChevronDown
                        v-if="!isCollapsed"
                        class="transition-transform duration-200"
                        :class="[(item.id === 'system-settings' ? isSystemSettingsExpanded : isTaskManagementExpanded) ? 'transform rotate-180' : '', darkMode ? 'text-gray-400' : 'text-gray-500']"
                        aria-hidden="true"
                      />
                    </transition>
                  </button>

                  <!-- 子菜单项 -->
                  <div v-if="item.id === 'system-settings' ? isSystemSettingsExpanded : isTaskManagementExpanded" class="ml-6 space-y-1">
                    <router-link
                      v-for="child in item.children"
                      :key="child.id"
                      :to="{ name: child.routeName }"
                      :class="[
                        $route.name === child.routeName
                          ? darkMode
                            ? 'bg-gray-900 text-white'
                            : 'bg-gray-100 text-gray-900'
                          : darkMode
                          ? 'text-gray-400 hover:bg-gray-700 hover:text-white'
                          : 'text-gray-500 hover:bg-gray-50 hover:text-gray-900',
                        'group flex items-center px-3 py-2 text-sm font-medium rounded-md',
                      ]"
                    >
                      <component
                        :is="getMenuIconComponent(child.icon)"
                        class="flex-shrink-0"
                        :class="[
                          isCollapsed ? 'mx-auto' : 'mr-3',
                          $route.name === child.routeName ? 'text-primary-500' : darkMode ? 'text-gray-500 group-hover:text-gray-400' : 'text-gray-400 group-hover:text-gray-500',
                        ]"
                        aria-hidden="true"
                      />
                      <transition
                        name="fade-slide"
                        enter-active-class="transition-all duration-300 delay-100"
                        leave-active-class="transition-all duration-200"
                        enter-from-class="opacity-0 transform translate-x-2"
                        enter-to-class="opacity-100 transform translate-x-0"
                        leave-from-class="opacity-100 transform translate-x-0"
                        leave-to-class="opacity-0 transform translate-x-2"
                      >
                        <span v-if="!isCollapsed" class="whitespace-nowrap">
                          {{ child.name }}
                        </span>
                      </transition>
                    </router-link>
                  </div>
                </template>
              </div>
            </template>

            <!-- 退出登录按钮 -->
            <div class="pt-4 mt-4 border-t" :class="darkMode ? 'border-gray-700' : 'border-gray-200'">
              <a
                @click="handleLogout"
                :class="[
                  darkMode ? 'text-gray-300 hover:bg-gray-700 hover:text-white' : 'text-gray-600 hover:bg-gray-50 hover:text-gray-900',
                  'group flex items-center text-sm font-medium rounded-md cursor-pointer transition-colors',
                  isCollapsed ? 'px-3 py-3 justify-center' : 'px-3 py-2.5',
                ]"
                :title="isCollapsed ? logoutText : ''"
              >
                <IconLogout
                  size="lg"
                  class="flex-shrink-0"
                  :class="[isCollapsed ? 'mx-auto' : 'mr-3', 'text-gray-400']"
                  aria-hidden="true"
                />
                <transition
                  name="fade-slide"
                  enter-active-class="transition-all duration-300 delay-100"
                  leave-active-class="transition-all duration-200"
                  enter-from-class="opacity-0 transform translate-x-2"
                  enter-to-class="opacity-100 transform translate-x-0"
                  leave-from-class="opacity-100 transform translate-x-0"
                  leave-to-class="opacity-0 transform translate-x-2"
                >
                  <span v-if="!isCollapsed" class="whitespace-nowrap">
                    {{ logoutText }}
                  </span>
                </transition>
              </a>

              <!-- 文档链接 -->
              <div class="flex justify-center mt-2">
                <a
                  :href="DOC_URL"
                  target="_blank"
                  rel="noopener noreferrer"
                  :class="[
                    darkMode ? 'text-gray-400 hover:text-gray-300' : 'text-gray-400 hover:text-gray-500',
                    'inline-flex items-center justify-center p-2 rounded-md transition-colors duration-200',
                ]"
                title="Document"
              >
                  <IconBookOpen class="h-7 w-7" aria-hidden="true" />
                </a>
              </div>
            </div>
          </nav>
          <div class="h-6"></div>
        </div>
      </div>
    </div>

    <!-- 移动端侧边栏覆盖层 - 混合导航模式下的层级管理 -->
    <transition name="slide">
      <div v-if="isMobileSidebarOpen" class="md:hidden fixed inset-0 z-[60] flex">
        <!-- 侧边栏背景遮罩 -->
        <div class="fixed inset-0 bg-gray-900 bg-opacity-50 transition-opacity" @click="$emit('close-mobile-sidebar')"></div>

        <!-- 侧边栏内容 -->
        <div class="relative flex-1 flex flex-col w-full max-w-xs shadow-xl transform transition-transform ease-in-out duration-300" :class="darkMode ? 'bg-gray-800' : 'bg-white'">
          <!-- 移动端侧边栏Logo + 标题和关闭按钮 -->
          <div class="flex items-center justify-between p-3 h-14 border-b" :class="darkMode ? 'border-gray-700' : 'border-gray-200'">
            <!-- Logo + 标题 -->
            <div class="flex items-center flex-1 min-w-0">
              <!-- 站点图标 -->
              <div class="flex-shrink-0 w-8 h-8 mr-3">
                <img :src="siteFaviconUrl || '/cloudpaste.svg'" :alt="siteTitle" class="w-8 h-8 object-contain" @error="handleImageError" />
              </div>

              <!-- 标题信息 -->
              <div class="flex-1 min-w-0">
                <div class="font-semibold text-base truncate" :class="darkMode ? 'text-white' : 'text-gray-900'">
                  {{ siteTitle }}
                </div>
                <div class="text-sm opacity-75 truncate" :class="darkMode ? 'text-gray-300' : 'text-gray-600'">
                  {{ userTypeText }}
                </div>
              </div>
            </div>
            <button
              type="button"
              @click="$emit('close-mobile-sidebar')"
              class="ml-1 flex items-center justify-center h-10 w-10 rounded-full focus:outline-none focus:ring-2 focus:ring-inset focus:ring-white"
            >
              <span class="sr-only">{{ t("admin.sidebar.closeMenu") }}</span>
              <IconClose size="lg" :class="darkMode ? 'text-white' : 'text-gray-600'" aria-hidden="true" />
            </button>
          </div>

          <!-- 移动端菜单内容 -->
          <div class="flex-1 overflow-y-auto pt-4">
            <nav class="px-4 space-y-2">
              <!-- 这里复制桌面端的菜单结构，但使用移动端样式 -->
              <template v-for="item in visibleMenuItems">
                <!-- 普通菜单项 -->
                <router-link
                  v-if="item.type === 'item'"
                  :key="item.id"
                  :to="{ name: item.routeName }"
                  :class="[
                    $route.name === item.routeName
                      ? darkMode
                        ? 'bg-gray-900 text-white'
                        : 'bg-gray-100 text-gray-900'
                      : darkMode
                      ? 'text-gray-300 hover:bg-gray-700 hover:text-white'
                      : 'text-gray-600 hover:bg-gray-50 hover:text-gray-900',
                    'group flex items-center px-3 py-2.5 text-sm font-medium rounded-md',
                  ]"
                  @click="$emit('close-mobile-sidebar')"
                >
                  <component
                    :is="getMenuIconComponent(item.icon)"
                    size="lg"
                    class="mr-3 flex-shrink-0"
                    :class="$route.name === item.routeName ? 'text-primary-500' : darkMode ? 'text-gray-400 group-hover:text-gray-300' : 'text-gray-400 group-hover:text-gray-500'"
                    aria-hidden="true"
                  />
                  {{ item.name }}
                </router-link>

                <!-- 带子菜单的菜单组 -->
                <div v-else-if="item.type === 'group'" :key="`mobile-group-${item.id}`" class="space-y-1">
                  <!-- 主菜单项 -->
                  <a
                    @click="item.id === 'system-settings' ? toggleSystemSettings() : toggleTaskManagement()"
                    :class="[
                      darkMode ? 'text-gray-300 hover:bg-gray-700 hover:text-white' : 'text-gray-600 hover:bg-gray-50 hover:text-gray-900',
                      'group flex items-center justify-between px-3 py-2.5 text-sm font-medium rounded-md cursor-pointer',
                    ]"
                  >
                    <div class="flex items-center">
                      <component
                        :is="getMenuIconComponent(item.icon)"
                        size="lg"
                        class="mr-3 flex-shrink-0"
                        :class="darkMode ? 'text-gray-400 group-hover:text-gray-300' : 'text-gray-400 group-hover:text-gray-500'"
                        aria-hidden="true"
                      />
                      {{ item.name }}
                    </div>
                    <!-- 展开/收起箭头 -->
                    <IconChevronDown
                      class="transition-transform duration-200"
                      :class="[(item.id === 'system-settings' ? isSystemSettingsExpanded : isTaskManagementExpanded) ? 'transform rotate-180' : '', darkMode ? 'text-gray-400' : 'text-gray-500']"
                      aria-hidden="true"
                    />
                  </a>

                  <!-- 子菜单项 -->
                  <div v-if="item.id === 'system-settings' ? isSystemSettingsExpanded : isTaskManagementExpanded" class="ml-6 space-y-1">
                    <router-link
                      v-for="child in item.children"
                      :key="child.id"
                      :to="{ name: child.routeName }"
                      :class="[
                        $route.name === child.routeName
                          ? darkMode
                            ? 'bg-gray-900 text-white'
                            : 'bg-gray-100 text-gray-900'
                          : darkMode
                          ? 'text-gray-400 hover:bg-gray-700 hover:text-white'
                          : 'text-gray-500 hover:bg-gray-50 hover:text-gray-900',
                        'group flex items-center px-3 py-2 text-sm font-medium rounded-md',
                      ]"
                      @click="$emit('close-mobile-sidebar')"
                    >
                      <component
                        :is="getMenuIconComponent(child.icon)"
                        class="mr-3 flex-shrink-0"
                        :class="
                          $route.name === child.routeName ? 'text-primary-500' : darkMode ? 'text-gray-500 group-hover:text-gray-400' : 'text-gray-400 group-hover:text-gray-500'
                        "
                        aria-hidden="true"
                      />
                      {{ child.name }}
                    </router-link>
                  </div>
                </div>
              </template>

              <!-- 退出登录按钮 -->
              <div class="pt-4 mt-4 border-t" :class="darkMode ? 'border-gray-700' : 'border-gray-200'">
                <a
                  @click="handleLogout"
                  :class="[
                    darkMode ? 'text-gray-300 hover:bg-gray-700 hover:text-white' : 'text-gray-600 hover:bg-gray-50 hover:text-gray-900',
                    'group flex items-center px-3 py-2.5 text-sm font-medium rounded-md cursor-pointer',
                  ]"
                >
                  <IconLogout size="lg" class="mr-3 flex-shrink-0 text-gray-400" aria-hidden="true" />
                  {{ logoutText }}
                </a>

                <!-- 文档链接 -->
                <div class="flex justify-center mt-2">
                  <a
                    :href="DOC_URL"
                    target="_blank"
                    rel="noopener noreferrer"
                    :class="[
                      darkMode ? 'text-gray-400 hover:text-gray-300' : 'text-gray-400 hover:text-gray-500',
                      'inline-flex items-center justify-center p-2 rounded-md transition-colors duration-200',
                    ]"
                    title="Document"
                    @click="$emit('close-mobile-sidebar')"
                  >
                    <IconBookOpen class="h-7 w-7" aria-hidden="true" />
                  </a>
                </div>
              </div>
            </nav>
            <div class="h-6"></div>
          </div>
        </div>
      </div>
    </transition>
  </div>
</template>

<script setup>
import { ref, computed, onMounted } from "vue";
import { useLocalStorage } from "@vueuse/core";
import { useI18n } from "vue-i18n";
import { useSiteConfigStore } from "@/stores/siteConfigStore.js";
import { IconBellAlert, IconBookOpen, IconChartBar, IconChevronDown, IconChevronLeft, IconChevronRight, IconCircleStack, IconCloud, IconClose, IconDocumentText, IconEye, IconFolder, IconGlobeAlt, IconHome, IconInformationCircle, IconKey, IconLink, IconLogout, IconMenu, IconSearch, IconServerStack, IconSettings, IconShare, IconTaskList, IconUser, IconList } from "@/components/icons";

// 使用i18n和站点配置Store
const { t } = useI18n();
const siteConfigStore = useSiteConfigStore();

const props = defineProps({
  darkMode: {
    type: Boolean,
    required: true,
  },
  permissions: {
    type: Object,
    required: true,
  },
  isMobileSidebarOpen: {
    type: Boolean,
    default: false,
  },
});

const emit = defineEmits(["close-mobile-sidebar", "logout", "sidebar-toggle"]);

// 侧边栏收缩状态
const isCollapsed = useLocalStorage("admin-sidebar-collapsed", false);

// 初始化时通知父组件
onMounted(() => {
  emit("sidebar-toggle", { collapsed: isCollapsed.value });
});

// 菜单组的展开状态
const isSystemSettingsExpanded = ref(false);
const isTaskManagementExpanded = ref(false);

// 站点图标相关计算属性
const siteFaviconUrl = computed(() => siteConfigStore.siteFaviconUrl);
const siteTitle = computed(() => siteConfigStore.siteTitle || "CloudPaste");

// 提取重复的权限判断逻辑
const userTypeText = computed(() => (props.permissions.isAdmin ? t("admin.sidebar.menuTitle.admin") : t("admin.sidebar.menuTitle.user")));
const logoutText = computed(() => (props.permissions.isAdmin ? t("admin.sidebar.logout") : t("admin.sidebar.logoutAuth")));

// 图标错误处理 - 直接切换到默认图标
const handleImageError = (event) => {
  event.target.src = "/cloudpaste.svg";
};

// 常量
const DOC_URL = "https://doc.cloudpaste.qzz.io/";

// 统一侧边栏图标：从图标名映射到统一出口 @/components/icons 的组件
const menuIconMap = {
  'chart-bar': IconChartBar,
  'document-text': IconDocumentText,
  folder: IconFolder,
  cloud: IconCloud,
  server: IconServerStack,
  'information-circle': IconInformationCircle,
  'clipboard-list': IconTaskList,
  'bell-alert': IconBellAlert,
  'list-bullet': IconList,
  share: IconShare,
  key: IconKey,
  user: IconUser,
  'circle-stack': IconCircleStack,
  cog: IconSettings,
  globe: IconGlobeAlt,
  eye: IconEye,
  'cloud-webdav': IconLink,
  home: IconHome,
  logout: IconLogout,
  'chevron-down': IconChevronDown,
  search: IconSearch,
};

const getMenuIconComponent = (iconName) => {
  return menuIconMap[iconName] || IconMenu;
};

// 根据登录类型和权限计算可见的菜单项
const visibleMenuItems = computed(() => {
  // 管理员可见所有菜单
  if (props.permissions.isAdmin) {
    return [
      { id: "dashboard", name: t("admin.sidebar.dashboard"), icon: "chart-bar", type: "item", routeName: "AdminDashboard" },
      { id: "text-management", name: t("admin.sidebar.textManagement"), icon: "document-text", type: "item", routeName: "AdminTextManagement" },
      { id: "file-management", name: t("admin.sidebar.fileManagement"), icon: "folder", type: "item", routeName: "AdminFileManagement" },
      { id: "share-management", name: t("admin.sidebar.shareManagement"), icon: "share", type: "item", routeName: "AdminShareManagement" },
      { id: "storage", name: t("admin.sidebar.storageConfig"), icon: "cloud", type: "item", routeName: "AdminStorage" },
      { id: "mount-management", name: t("admin.sidebar.mountManagement"), icon: "server", type: "item", routeName: "AdminMountManagement" },
      { id: "fs-meta-management", name: t("admin.sidebar.fsMetaManagement"), icon: "information-circle", type: "item", routeName: "AdminFsMetaManagement" },
      {
        id: "task-management",
        name: t("admin.sidebar.taskManagement"),
        icon: "clipboard-list",
        type: "group",
        children: [
          { id: "scheduled-jobs", name: t("admin.sidebar.scheduledJobs"), icon: "bell-alert", type: "item", routeName: "AdminScheduledJobs" },
          { id: "tasks", name: t("admin.sidebar.tasks"), icon: "list-bullet", type: "item", routeName: "AdminTasks" },
        ],
      },
      { id: "fs-index-management", name: t("admin.sidebar.fsIndexManagement"), icon: "search", type: "item", routeName: "AdminFsIndexManagement" },
      { id: "key-management", name: t("admin.sidebar.keyManagement"), icon: "key", type: "item", routeName: "AdminKeyManagement" },
      { id: "account-management", name: t("admin.sidebar.accountManagement"), icon: "user", type: "item", routeName: "AdminAccountManagement" },
      { id: "backup", name: t("admin.sidebar.backup"), icon: "circle-stack", type: "item", routeName: "AdminBackup" },
      {
        id: "system-settings",
        name: t("admin.sidebar.systemSettings"),
        icon: "cog",
        type: "group",
        children: [
          { id: "settings/global", name: t("admin.sidebar.globalSettings"), icon: "globe", type: "item", routeName: "AdminGlobalSettings" },
          { id: "settings/preview", name: t("admin.sidebar.previewSettings"), icon: "eye", type: "item", routeName: "AdminPreviewSettings" },
          { id: "settings/webdav", name: t("admin.sidebar.webdavSettings"), icon: "cloud-webdav", type: "item", routeName: "AdminWebDAVSettings" },
          { id: "settings/site", name: t("admin.sidebar.siteSettings"), icon: "home", type: "item", routeName: "AdminSiteSettings" },
        ],
      },
    ];
  }

  // API密钥用户根据权限显示菜单
  const items = [];

  if (props.permissions.text) {
    items.push({ id: "text-management", name: t("admin.sidebar.textManagement"), icon: "document-text", type: "item", routeName: "AdminTextManagement" });
  }

  if (props.permissions.file) {
    items.push({ id: "file-management", name: t("admin.sidebar.fileManagement"), icon: "folder", type: "item", routeName: "AdminFileManagement" });
    items.push({ id: "share-management", name: t("admin.sidebar.shareManagement"), icon: "share", type: "item", routeName: "AdminShareManagement" });
  }

  if (props.permissions.mount) {
    items.push({ id: "mount-management", name: t("admin.sidebar.mountManagement"), icon: "server", type: "item", routeName: "AdminMountManagement" });
  }

  // 任务管理：有挂载权限即可访问，具体任务根据权限类型在列表内过滤
  if (props.permissions.mount) {
    items.push({ id: "tasks", name: t("admin.sidebar.tasks"), icon: "list-bullet", type: "item", routeName: "AdminTasks" });
  }

  // 所有API密钥用户都可以访问账户管理（用于查看信息和登出）
  items.push({ id: "account-management", name: t("admin.sidebar.accountManagement"), icon: "user", type: "item", routeName: "AdminAccountManagement" });

  return items;
});

// 切换菜单组的展开状态
const toggleSystemSettings = () => {
  isSystemSettingsExpanded.value = !isSystemSettingsExpanded.value;
};

const toggleTaskManagement = () => {
  isTaskManagementExpanded.value = !isTaskManagementExpanded.value;
};

// 收缩/展开切换函数
const toggleCollapse = () => {
  isCollapsed.value = !isCollapsed.value;
  emit("sidebar-toggle", { collapsed: isCollapsed.value });
};

// 处理收缩状态下的组菜单项点击 - 展开侧边栏并展开子菜单
const handleGroupItemClick = () => {
  isCollapsed.value = false;
  emit("sidebar-toggle", { collapsed: false });
  // 展开所有菜单组
  isSystemSettingsExpanded.value = true;
  isTaskManagementExpanded.value = true;
};

// 退出登录
const handleLogout = () => {
  emit("logout");
};
</script>

<style scoped>
.slide-enter-active,
.slide-leave-active {
  transition: all 0.3s ease-in-out;
}

.slide-enter-from {
  transform: translateX(-100%);
}

.slide-leave-to {
  transform: translateX(-100%);
}
</style>
