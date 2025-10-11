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
                <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 19l-7-7 7-7" />
                </svg>
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
              <svg class="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5l7 7-7 7" />
              </svg>
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
                <svg
                  class="flex-shrink-0 h-6 w-6"
                  :class="[
                    isCollapsed ? 'mx-auto' : 'mr-3',
                    $route.name === item.routeName ? 'text-primary-500' : darkMode ? 'text-gray-400 group-hover:text-gray-300' : 'text-gray-400 group-hover:text-gray-500',
                  ]"
                  xmlns="http://www.w3.org/2000/svg"
                  fill="none"
                  viewBox="0 0 24 24"
                  stroke="currentColor"
                  aria-hidden="true"
                >
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" :d="getIconPath(item.icon)" />
                </svg>
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
                  <svg
                    class="h-6 w-6 mx-auto"
                    :class="darkMode ? 'text-gray-400 group-hover:text-gray-300' : 'text-gray-400 group-hover:text-gray-500'"
                    xmlns="http://www.w3.org/2000/svg"
                    fill="none"
                    viewBox="0 0 24 24"
                    stroke="currentColor"
                    aria-hidden="true"
                  >
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" :d="getIconPath(item.icon)" />
                  </svg>
                </button>

                <!-- 展开状态：显示完整的组菜单 -->
                <template v-else>
                  <!-- 主菜单项 -->
                  <button
                    @click="toggleSystemSettings"
                    :class="[
                      darkMode ? 'text-gray-300 hover:bg-gray-700 hover:text-white' : 'text-gray-600 hover:bg-gray-50 hover:text-gray-900',
                      'w-full group flex items-center justify-between px-3 py-2.5 text-sm font-medium rounded-md cursor-pointer',
                    ]"
                  >
                    <div class="flex items-center">
                      <svg
                        class="flex-shrink-0 h-6 w-6"
                        :class="[isCollapsed ? 'mx-auto' : 'mr-3', darkMode ? 'text-gray-400 group-hover:text-gray-300' : 'text-gray-400 group-hover:text-gray-500']"
                        xmlns="http://www.w3.org/2000/svg"
                        fill="none"
                        viewBox="0 0 24 24"
                        stroke="currentColor"
                        aria-hidden="true"
                      >
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" :d="getIconPath(item.icon)" />
                      </svg>
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
                      <svg
                        v-if="!isCollapsed"
                        class="h-5 w-5 transition-transform duration-200"
                        :class="[isSystemSettingsExpanded ? 'transform rotate-180' : '', darkMode ? 'text-gray-400' : 'text-gray-500']"
                        xmlns="http://www.w3.org/2000/svg"
                        fill="none"
                        viewBox="0 0 24 24"
                        stroke="currentColor"
                      >
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" :d="getIconPath('chevron-down')" />
                      </svg>
                    </transition>
                  </button>

                  <!-- 子菜单项 -->
                  <div v-if="isSystemSettingsExpanded" class="ml-6 space-y-1">
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
                      <svg
                        class="flex-shrink-0 h-5 w-5"
                        :class="[
                          isCollapsed ? 'mx-auto' : 'mr-3',
                          $route.name === child.routeName ? 'text-primary-500' : darkMode ? 'text-gray-500 group-hover:text-gray-400' : 'text-gray-400 group-hover:text-gray-500',
                        ]"
                        xmlns="http://www.w3.org/2000/svg"
                        fill="none"
                        viewBox="0 0 24 24"
                        stroke="currentColor"
                        aria-hidden="true"
                      >
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" :d="getIconPath(child.icon)" />
                      </svg>
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
                <svg
                  class="flex-shrink-0 h-6 w-6"
                  :class="[isCollapsed ? 'mx-auto' : 'mr-3', 'text-gray-400']"
                  xmlns="http://www.w3.org/2000/svg"
                  fill="none"
                  viewBox="0 0 24 24"
                  stroke="currentColor"
                  aria-hidden="true"
                >
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" :d="getIconPath('logout')" />
                </svg>
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
                  <svg class="h-7 w-7" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor" aria-hidden="true">
                    <path
                      stroke-linecap="round"
                      stroke-linejoin="round"
                      stroke-width="2"
                      d="M12 6.253v13m0-13C10.832 5.477 9.246 5 7.5 5S4.168 5.477 3 6.253v13C4.168 18.477 5.754 18 7.5 18s3.332.477 4.5 1.253m0-13C13.168 5.477 14.754 5 16.5 5c1.746 0 3.332.477 4.5 1.253v13C19.832 18.477 18.246 18 16.5 18c-1.746 0-3.332.477-4.5 1.253"
                    />
                  </svg>
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
              <svg
                class="h-6 w-6"
                :class="darkMode ? 'text-white' : 'text-gray-600'"
                xmlns="http://www.w3.org/2000/svg"
                fill="none"
                viewBox="0 0 24 24"
                stroke="currentColor"
                aria-hidden="true"
              >
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
              </svg>
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
                  <svg
                    class="mr-3 flex-shrink-0 h-6 w-6"
                    :class="$route.name === item.routeName ? 'text-primary-500' : darkMode ? 'text-gray-400 group-hover:text-gray-300' : 'text-gray-400 group-hover:text-gray-500'"
                    xmlns="http://www.w3.org/2000/svg"
                    fill="none"
                    viewBox="0 0 24 24"
                    stroke="currentColor"
                    aria-hidden="true"
                  >
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" :d="getIconPath(item.icon)" />
                  </svg>
                  {{ item.name }}
                </router-link>

                <!-- 带子菜单的菜单组 -->
                <div v-else-if="item.type === 'group'" :key="`mobile-group-${item.id}`" class="space-y-1">
                  <!-- 主菜单项 -->
                  <a
                    @click="toggleSystemSettings"
                    :class="[
                      darkMode ? 'text-gray-300 hover:bg-gray-700 hover:text-white' : 'text-gray-600 hover:bg-gray-50 hover:text-gray-900',
                      'group flex items-center justify-between px-3 py-2.5 text-sm font-medium rounded-md cursor-pointer',
                    ]"
                  >
                    <div class="flex items-center">
                      <svg
                        class="mr-3 flex-shrink-0 h-6 w-6"
                        :class="darkMode ? 'text-gray-400 group-hover:text-gray-300' : 'text-gray-400 group-hover:text-gray-500'"
                        xmlns="http://www.w3.org/2000/svg"
                        fill="none"
                        viewBox="0 0 24 24"
                        stroke="currentColor"
                        aria-hidden="true"
                      >
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" :d="getIconPath(item.icon)" />
                      </svg>
                      {{ item.name }}
                    </div>
                    <!-- 展开/收起箭头 -->
                    <svg
                      class="h-5 w-5 transition-transform duration-200"
                      :class="[isSystemSettingsExpanded ? 'transform rotate-180' : '', darkMode ? 'text-gray-400' : 'text-gray-500']"
                      xmlns="http://www.w3.org/2000/svg"
                      fill="none"
                      viewBox="0 0 24 24"
                      stroke="currentColor"
                    >
                      <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" :d="getIconPath('chevron-down')" />
                    </svg>
                  </a>

                  <!-- 子菜单项 -->
                  <div v-if="isSystemSettingsExpanded" class="ml-6 space-y-1">
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
                      <svg
                        class="mr-3 flex-shrink-0 h-5 w-5"
                        :class="
                          $route.name === child.routeName ? 'text-primary-500' : darkMode ? 'text-gray-500 group-hover:text-gray-400' : 'text-gray-400 group-hover:text-gray-500'
                        "
                        xmlns="http://www.w3.org/2000/svg"
                        fill="none"
                        viewBox="0 0 24 24"
                        stroke="currentColor"
                        aria-hidden="true"
                      >
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" :d="getIconPath(child.icon)" />
                      </svg>
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
                  <svg class="mr-3 flex-shrink-0 h-6 w-6 text-gray-400" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor" aria-hidden="true">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" :d="getIconPath('logout')" />
                  </svg>
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
                    <svg class="h-7 w-7" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor" aria-hidden="true">
                      <path
                        stroke-linecap="round"
                        stroke-linejoin="round"
                        stroke-width="2"
                        d="M12 6.253v13m0-13C10.832 5.477 9.246 5 7.5 5S4.168 5.477 3 6.253v13C4.168 18.477 5.754 18 7.5 18s3.332.477 4.5 1.253m0-13C13.168 5.477 14.754 5 16.5 5c1.746 0 3.332.477 4.5 1.253v13C19.832 18.477 18.246 18 16.5 18c-1.746 0-3.332.477-4.5 1.253"
                      />
                    </svg>
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
import { useI18n } from "vue-i18n";
import { useSiteConfigStore } from "@/stores/siteConfigStore.js";

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
const isCollapsed = ref(false);

// 恢复保存的收缩状态
onMounted(() => {
  const saved = localStorage.getItem("admin-sidebar-collapsed");
  if (saved !== null) {
    isCollapsed.value = JSON.parse(saved);
    // 初始化时也要通知父组件
    emit("sidebar-toggle", { collapsed: isCollapsed.value });
  }
});

// 系统设置菜单的展开状态
const isSystemSettingsExpanded = ref(false);

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

// 提取localStorage操作
const saveCollapseState = (collapsed) => {
  localStorage.setItem("admin-sidebar-collapsed", JSON.stringify(collapsed));
};

// 常量
const DOC_URL = "https://doc.cloudpaste.qzz.io/";

// 根据登录类型和权限计算可见的菜单项
const visibleMenuItems = computed(() => {
  // 管理员可见所有菜单
  if (props.permissions.isAdmin) {
    return [
      { id: "dashboard", name: t("admin.sidebar.dashboard"), icon: "chart-bar", type: "item", routeName: "AdminDashboard" },
      { id: "text-management", name: t("admin.sidebar.textManagement"), icon: "document-text", type: "item", routeName: "AdminTextManagement" },
      { id: "file-management", name: t("admin.sidebar.fileManagement"), icon: "folder", type: "item", routeName: "AdminFileManagement" },
      { id: "storage-config", name: t("admin.sidebar.storageConfig"), icon: "cloud", type: "item", routeName: "AdminStorageConfig" },
      { id: "mount-management", name: t("admin.sidebar.mountManagement"), icon: "server", type: "item", routeName: "AdminMountManagement" },
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
  }

  if (props.permissions.mount) {
    items.push({ id: "mount-management", name: t("admin.sidebar.mountManagement"), icon: "server", type: "item", routeName: "AdminMountManagement" });
  }

  // 所有API密钥用户都可以访问账户管理（用于查看信息和登出）
  items.push({ id: "account-management", name: t("admin.sidebar.accountManagement"), icon: "user", type: "item", routeName: "AdminAccountManagement" });

  return items;
});

// 切换系统设置菜单的展开状态
const toggleSystemSettings = () => {
  isSystemSettingsExpanded.value = !isSystemSettingsExpanded.value;
};

// 收缩/展开切换函数
const toggleCollapse = () => {
  isCollapsed.value = !isCollapsed.value;
  saveCollapseState(isCollapsed.value);
  emit("sidebar-toggle", { collapsed: isCollapsed.value });
};

// 处理收缩状态下的组菜单项点击 - 展开侧边栏并展开子菜单
const handleGroupItemClick = () => {
  isCollapsed.value = false;
  saveCollapseState(false);
  emit("sidebar-toggle", { collapsed: false });
  isSystemSettingsExpanded.value = true;
};

// 退出登录
const handleLogout = () => {
  emit("logout");
};

// 根据图标名称返回SVG路径数据
const getIconPath = (iconName) => {
  switch (iconName) {
    case "chart-bar":
      return "M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z";
    case "document-text":
      return "M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z";
    case "folder":
      return "M3 7v10a2 2 0 002 2h14a2 2 0 002-2V9a2 2 0 00-2-2h-6l-2-2H5a2 2 0 00-2 2z";
    case "cloud":
      return "M8 16a5 5 0 01-.916-9.916A5.002 5.002 0 0113 6c2.761 0 5 2.239 5 5 0 .324-.024.64-.075.947 1.705.552 2.668 2.176 2.668 3.833 0 1.598-1.425 3.22-3 3.22h-2.053V14.53c0-.282-.112-.55-.308-.753a1 1 0 00-1.412-.002l-2.332 2.332c-.39.39-.39 1.024 0 1.414l2.331 2.331c.392.391 1.025.39 1.414-.001a1.06 1.06 0 00.307-.752V17h2.053a5.235 5.235 0 003.626-8.876A7.002 7.002 0 0013 4a7.002 7.002 0 00-6.929 5.868A6.998 6.998 0 008 16z";
    case "cloud-webdav":
      return "M3 17a4 4 0 01.899-7.899 5.002 5.002 0 019.802-1.902A4 4 0 0117 17H7a4 4 0 01-4-4z";
    case "key":
      return "M15 7a2 2 0 012 2m4 0a6 6 0 01-7.743 5.743L11 17H9v2H7v2H4a1 1 0 01-1-1v-2.586a1 1 0 01.293-.707l5.964-5.964A6 6 0 1121 9z";
    case "cog":
      return "M10.325 4.317c.426-1.756 2.924-1.756 3.35 0a1.724 1.724 0 002.573 1.066c1.543-.94 3.31.826 2.37 2.37a1.724 1.724 0 001.065 2.572c1.756.426 1.756 2.924 0 3.35a1.724 1.724 0 00-1.066 2.573c.94 1.543-.826 3.31-2.37 2.37a1.724 1.724 0 00-2.572 1.065c-.426 1.756-2.924 1.756-3.35 0a1.724 1.724 0 00-2.573-1.066c-1.543.94-3.31-.826-2.37-2.37a1.724 1.724 0 00-1.065-2.572c-1.756-.426-1.756-2.924 0-3.35a1.724 1.724 0 001.066-2.573c-.94-1.543.826-3.31 2.37-2.37.996.608 2.296.07 2.572-1.065z";
    case "logout":
      return "M17 16l4-4m0 0l-4-4m4 4H7m6 4v1a3 3 0 01-3 3H6a3 3 0 01-3-3V7a3 3 0 013-3h4a3 3 0 013 3v1";
    case "server":
      return "M5 12H3v8h18v-8H5zm0 0a2 2 0 100-4h14a2 2 0 100 4M5 8a2 2 0 100-4h14a2 2 0 100 4";
    case "chevron-down":
      return "M19 9l-7 7-7-7";
    case "globe":
      return "M21 12a9 9 0 01-9 9m9-9a9 9 0 00-9-9m9 9H3m9 9a9 9 0 01-9-9m9 9c1.657 0 3-4.03 3-9s-1.343-9-3-9m0 18c-1.657 0-3-4.03-3-9s1.343-9 3-9m-9 9a9 9 0 019-9";
    case "user":
      return "M16 7a4 4 0 11-8 0 4 4 0 018 0zM12 14a7 7 0 00-7 7h14a7 7 0 00-7-7z";
    case "eye":
      return "M15 12a3 3 0 11-6 0 3 3 0 016 0z M2.458 12C3.732 7.943 7.523 5 12 5c4.478 0 8.268 2.943 9.542 7-1.274 4.057-5.064 7-9.542 7-4.477 0-8.268-2.943-9.542-7z";
    case "home":
      return "M10.707 2.293a1 1 0 00-1.414 0l-7 7a1 1 0 001.414 1.414L4 10.414V17a1 1 0 001 1h2a1 1 0 001-1v-2a1 1 0 011-1h2a1 1 0 011 1v2a1 1 0 001 1h2a1 1 0 001-1v-6.586l.293.293a1 1 0 001.414-1.414l-7-7z";
    case "archive":
      return "M5 8a2 2 0 012-2h6a2 2 0 012 2v1a2 2 0 002 2h2a2 2 0 012 2v5a2 2 0 01-2 2H5a2 2 0 01-2-2v-5a2 2 0 012-2h2a2 2 0 002-2V8z";
    case "circle-stack":
      return "M20.25 6.375c0 2.278-3.694 4.125-8.25 4.125S3.75 8.653 3.75 6.375m16.5 0c0-2.278-3.694-4.125-8.25-4.125S3.75 4.097 3.75 6.375m16.5 0v11.25c0 2.278-3.694 4.125-8.25 4.125s-8.25-1.847-8.25-4.125V6.375m16.5 0v3.75c0 2.278-3.694 4.125-8.25 4.125s-8.25-1.847-8.25-4.125V6.375";
    default:
      return "";
  }
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
