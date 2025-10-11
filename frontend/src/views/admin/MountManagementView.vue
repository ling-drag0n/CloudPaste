<script setup>
import { onMounted } from "vue";
import MountForm from "@/components/admin/MountForm.vue";
import CommonPagination from "@/components/common/CommonPagination.vue";
import { useMountManagement } from "@/composables/admin-management";

/**
 * 挂载管理视图
 */

// 组件接收的属性定义
const props = defineProps({
  darkMode: {
    type: Boolean,
    required: true,
  },
  userType: {
    type: String,
    default: "admin",
    validator: (value) => ["admin", "apikey"].includes(value),
  },
});

// 使用挂载管理 composable
const {
  // 状态
  loading,
  error,
  successMessage,
  showForm,
  currentMount,
  searchQuery,
  filteredMounts,
  pagination,
  lastRefreshTime,

  // 权限状态
  isAdmin,
  isApiKeyUser,

  // 方法
  loadMounts,
  loadS3Configs,
  handleOffsetChange,
  openCreateForm,
  openEditForm,
  closeForm,
  handleFormSaveSuccess,
  confirmDelete,
  toggleActive,

  // 工具方法
  formatDate,
  formatCreator,
  getCreatorClass,
  formatStorageType,
} = useMountManagement();

// 组件挂载时加载数据
onMounted(() => {
  loadS3Configs(); // 先加载S3配置列表
  loadMounts(); // 然后加载挂载点列表
});
</script>

<template>
  <div class="p-4 flex-1 flex flex-col">
    <!-- 顶部操作栏 -->
    <div class="flex flex-col sm:flex-row sm:justify-between mb-4">
      <div class="mb-2 sm:mb-0">
        <h2 class="text-lg font-semibold" :class="darkMode ? 'text-white' : 'text-gray-900'">
          {{ isApiKeyUser ? $t("admin.mount.accessibleMounts") : $t("admin.mount.title") }}
        </h2>
      </div>
      <div class="flex flex-wrap gap-2">
        <!-- 只有管理员可以添加挂载点 -->
        <button
          v-if="isAdmin"
          @click="openCreateForm"
          class="px-3 py-1.5 rounded-md text-sm font-medium transition-colors duration-200 shadow-sm hover:shadow"
          :class="darkMode ? 'bg-blue-600 hover:bg-blue-700 text-white' : 'bg-blue-500 hover:bg-blue-600 text-white'"
          :disabled="loading"
        >
          <span class="flex items-center">
            <svg xmlns="http://www.w3.org/2000/svg" class="h-4 w-4 mr-1.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 6v6m0 0v6m0-6h6m-6 0H6" />
            </svg>
            {{ $t("admin.mount.createMount") }}
          </span>
        </button>
        <button
          @click="loadMounts"
          class="px-3 py-1.5 rounded-md text-sm font-medium transition-colors duration-200 shadow-sm hover:shadow"
          :class="darkMode ? 'bg-blue-600 hover:bg-blue-700 text-white' : 'bg-blue-500 hover:bg-blue-600 text-white'"
        >
          <span class="flex items-center">
            <svg xmlns="http://www.w3.org/2000/svg" class="h-4 w-4 mr-1.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path
                stroke-linecap="round"
                stroke-linejoin="round"
                stroke-width="2"
                d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"
              />
            </svg>
            {{ $t("admin.mount.refresh") }}
          </span>
        </button>
      </div>
    </div>

    <!-- 错误和成功消息提示 -->
    <div v-if="error" class="mb-4 p-3 bg-red-100 text-red-600 rounded-md dark:bg-red-900/50 dark:text-red-200 dark:border dark:border-red-800">
      <p>{{ error }}</p>
    </div>
    <div v-if="successMessage" class="mb-4 p-3 bg-green-100 text-green-600 rounded-md dark:bg-green-900/50 dark:text-green-200 dark:border dark:border-green-800">
      <p>{{ successMessage }}</p>
    </div>

    <!-- 上次刷新时间显示 -->
    <div class="flex justify-between items-center mb-3 sm:mb-4" v-if="lastRefreshTime">
      <div class="text-xs sm:text-sm text-gray-500 dark:text-gray-400">
        <span class="inline-flex items-center">
          <svg xmlns="http://www.w3.org/2000/svg" class="h-3 w-3 sm:h-4 sm:w-4 mr-1" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z" />
          </svg>
          {{ $t("admin.mount.info.lastRefresh") }}: {{ lastRefreshTime }}
        </span>
      </div>

      <!-- 搜索框 -->
      <div class="max-w-md">
        <div class="relative rounded-md shadow-sm">
          <input
            type="text"
            v-model="searchQuery"
            :placeholder="$t('admin.mount.search')"
            class="w-full px-3 py-1.5 rounded-md focus:outline-none focus:ring-2"
            :class="
              darkMode
                ? 'bg-gray-700 text-white placeholder-gray-400 focus:ring-blue-500 focus:border-blue-500 border border-gray-600'
                : 'bg-white text-gray-700 placeholder-gray-400 focus:ring-blue-500 focus:border-blue-500 border border-gray-300'
            "
          />
          <div class="absolute inset-y-0 right-0 pr-3 flex items-center pointer-events-none">
            <svg class="h-4 w-4" :class="darkMode ? 'text-gray-400' : 'text-gray-400'" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
            </svg>
          </div>
        </div>
      </div>
    </div>

    <!-- 加载中指示器 -->
    <div v-if="loading" class="flex justify-center my-8">
      <svg class="animate-spin h-8 w-8" :class="darkMode ? 'text-blue-400' : 'text-blue-500'" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
        <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
        <path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
      </svg>
    </div>

    <!-- 挂载点列表为空 -->
    <div v-else-if="filteredMounts.length === 0" class="flex-grow flex items-center justify-center p-6">
      <div class="text-center">
        <svg
          class="mx-auto h-12 w-12 mb-4"
          :class="darkMode ? 'text-gray-500' : 'text-gray-400'"
          xmlns="http://www.w3.org/2000/svg"
          fill="none"
          viewBox="0 0 24 24"
          stroke="currentColor"
        >
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 12H3v8h18v-8H5zm0 0a2 2 0 100-4h14a2 2 0 100 4M5 8a2 2 0 100-4h14a2 2 0 100 4" />
        </svg>
        <h3 class="text-lg font-medium mb-2" :class="darkMode ? 'text-white' : 'text-gray-900'">
          {{ searchQuery ? $t("admin.mount.searchResults.noResults") : isApiKeyUser ? $t("admin.mount.empty.title") : $t("admin.mount.empty.title") }}
        </h3>
        <p class="text-sm mb-4" :class="darkMode ? 'text-gray-400' : 'text-gray-500'">
          {{ searchQuery ? "尝试使用不同的搜索条件" : isApiKeyUser ? "当前API密钥没有可访问的挂载点，请联系管理员配置权限" : $t("admin.mount.empty.description") }}
        </p>
        <button
          v-if="searchQuery"
          @click="searchQuery = ''"
          class="inline-flex items-center px-3 py-2 border rounded-md shadow-sm text-sm font-medium focus:outline-none focus:ring-2 focus:ring-offset-2 transition-colors duration-200"
          :class="
            darkMode
              ? 'border-gray-600 bg-gray-700 text-gray-200 hover:bg-gray-600 focus:ring-blue-500 focus:ring-offset-gray-800'
              : 'border-gray-300 bg-white text-gray-700 hover:bg-gray-50 focus:ring-blue-500'
          "
        >
          {{ $t("admin.mount.searchResults.clearSearch") }}
        </button>
      </div>
    </div>

    <!-- 挂载点列表 -->
    <div v-else class="flex-grow overflow-auto">
      <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
        <!-- 分页显示的挂载点 -->
        <div
          v-for="mount in filteredMounts.slice(pagination.offset, pagination.offset + pagination.limit)"
          :key="mount.id"
          class="rounded-lg shadow-md overflow-hidden transition-all duration-200 hover:shadow-lg flex flex-col"
          :class="darkMode ? 'bg-gray-700 border border-gray-600 hover:border-blue-500/50' : 'bg-white border border-gray-200 hover:border-blue-400/50'"
        >
          <!-- 卡片内容区域 - 使用flex-col布局 -->
          <div class="px-4 py-4 flex-1 flex flex-col justify-between">
            <!-- 卡片顶部信息区 -->
            <div>
              <!-- 挂载点标题和状态 -->
              <div class="flex justify-between items-start mb-3">
                <h3 class="text-base font-medium truncate" :class="darkMode ? 'text-white' : 'text-gray-900'" :title="mount.name">
                  {{ mount.name }}
                </h3>
                <span
                  class="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium transition-colors duration-200"
                  :class="
                    mount.is_active
                      ? darkMode
                        ? 'bg-green-900/50 text-green-200 border border-green-800/50'
                        : 'bg-green-100 text-green-800 border border-green-200'
                      : darkMode
                      ? 'bg-gray-800 text-gray-300 border border-gray-700'
                      : 'bg-gray-100 text-gray-800 border border-gray-200'
                  "
                >
                  <span class="flex items-center">
                    <span class="w-1.5 h-1.5 rounded-full mr-1" :class="mount.is_active ? 'bg-green-400' : 'bg-gray-400'"></span>
                    {{ mount.is_active ? $t("admin.mount.status.enabled") : $t("admin.mount.status.disabled") }}
                  </span>
                </span>
              </div>

              <!-- 挂载路径 - 改进显示 -->
              <div class="mb-3">
                <div class="flex items-center">
                  <svg
                    class="flex-shrink-0 mr-1.5 h-4 w-4"
                    :class="darkMode ? 'text-gray-400' : 'text-gray-500'"
                    xmlns="http://www.w3.org/2000/svg"
                    fill="none"
                    viewBox="0 0 24 24"
                    stroke="currentColor"
                  >
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3 7v10a2 2 0 002 2h14a2 2 0 002-2V9a2 2 0 00-2-2h-6l-2-2H5a2 2 0 00-2 2z" />
                  </svg>
                  <p class="text-sm font-mono truncate" :class="darkMode ? 'text-gray-300' : 'text-gray-600'" :title="mount.mount_path">
                    {{ mount.mount_path }}
                  </p>
                </div>
              </div>

              <!-- 存储类型和配置 - 改进显示 -->
              <div class="mb-3">
                <div class="flex items-center text-sm" :class="darkMode ? 'text-gray-300' : 'text-gray-600'">
                  <svg
                    class="flex-shrink-0 mr-1.5 h-4 w-4"
                    :class="darkMode ? 'text-gray-400' : 'text-gray-500'"
                    xmlns="http://www.w3.org/2000/svg"
                    fill="none"
                    viewBox="0 0 24 24"
                    stroke="currentColor"
                  >
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 12H3v8h18v-8H5zm0 0a2 2 0 100-4h14a2 2 0 100 4M5 8a2 2 0 100-4h14a2 2 0 100 4" />
                  </svg>
                  <span class="truncate" :title="formatStorageType(mount)">{{ formatStorageType(mount) }}</span>
                </div>
              </div>

              <!-- 备注说明 - 固定高度确保对齐 -->
              <div class="mb-3 min-h-[24px]">
                <div v-if="mount.remark" class="flex items-start">
                  <svg
                    class="flex-shrink-0 mr-1.5 h-4 w-4 mt-0.5"
                    :class="darkMode ? 'text-gray-400' : 'text-gray-500'"
                    xmlns="http://www.w3.org/2000/svg"
                    fill="none"
                    viewBox="0 0 24 24"
                    stroke="currentColor"
                  >
                    <path
                      stroke-linecap="round"
                      stroke-linejoin="round"
                      stroke-width="2"
                      d="M7 8h10M7 12h4m1 8l-4-4H5a2 2 0 01-2-2V6a2 2 0 012-2h14a2 2 0 012 2v8a2 2 0 01-2 2h-3l-4 4z"
                    />
                  </svg>
                  <p class="text-sm truncate" :class="darkMode ? 'text-gray-400' : 'text-gray-500'" :title="mount.remark">
                    {{ mount.remark }}
                  </p>
                </div>
              </div>
            </div>

            <!-- 卡片底部信息区 -->
            <div>
              <!-- 代理和签名状态 -->
              <div v-if="mount.web_proxy" class="mb-2 flex flex-wrap gap-1">
                <span
                  class="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium"
                  :class="darkMode ? 'bg-blue-900/50 text-blue-200 border border-blue-800/50' : 'bg-blue-100 text-blue-800 border border-blue-200'"
                >
                  <svg class="w-3 h-3 mr-1" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M21 12a9 9 0 01-9 9m9-9a9 9 0 00-9-9m9 9H3m9 9v-9m0-9v9"></path>
                  </svg>
                  {{ $t("admin.mount.status.proxy") }}
                </span>
                <span
                  v-if="mount.enable_sign"
                  class="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium"
                  :class="darkMode ? 'bg-green-900/50 text-green-200 border border-green-800/50' : 'bg-green-100 text-green-800 border border-green-200'"
                >
                  <svg class="w-3 h-3 mr-1" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path
                      stroke-linecap="round"
                      stroke-linejoin="round"
                      stroke-width="2"
                      d="M9 12l2 2 4-4m5.618-4.016A11.955 11.955 0 0112 2.944a11.955 11.955 0 01-8.618 3.04A12.02 12.02 0 003 9c0 5.591 3.824 10.29 9 11.622 5.176-1.332 9-6.03 9-11.622 0-1.042-.133-2.052-.382-3.016z"
                    ></path>
                  </svg>
                  {{ $t("admin.mount.status.signature") }}
                </span>
              </div>

              <!-- 创建时间 -->
              <div class="text-xs mb-3" :class="darkMode ? 'text-gray-400' : 'text-gray-500'">
                <div class="flex items-center">
                  <svg
                    class="flex-shrink-0 mr-1.5 h-3.5 w-3.5"
                    :class="darkMode ? 'text-gray-500' : 'text-gray-400'"
                    xmlns="http://www.w3.org/2000/svg"
                    fill="none"
                    viewBox="0 0 24 24"
                    stroke="currentColor"
                  >
                    <path
                      stroke-linecap="round"
                      stroke-linejoin="round"
                      stroke-width="2"
                      d="M8 7V3m8 4V3m-9 8h10M5 21h14a2 2 0 002-2V7a2 2 0 00-2-2H5a2 2 0 00-2 2v12a2 2 0 002 2z"
                    />
                  </svg>
                  <span>{{ $t("admin.mount.info.createdAt") }}: {{ formatDate(mount.created_at) }}</span>
                </div>
              </div>

              <!-- 创建者信息 -->
              <div class="text-xs mb-3" :class="darkMode ? 'text-gray-400' : 'text-gray-500'">
                <div class="flex items-center">
                  <svg
                    class="flex-shrink-0 mr-1.5 h-3.5 w-3.5"
                    :class="darkMode ? 'text-gray-500' : 'text-gray-400'"
                    xmlns="http://www.w3.org/2000/svg"
                    fill="none"
                    viewBox="0 0 24 24"
                    stroke="currentColor"
                  >
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M16 7a4 4 0 11-8 0 4 4 0 018 0zM12 14a7 7 0 00-7 7h14a7 7 0 00-7-7z" />
                  </svg>
                  <span>{{ $t("admin.mount.info.createdBy") }}: </span>
                  <span class="ml-1 px-1.5 py-0.5 text-xs rounded" :class="getCreatorClass(mount)">
                    {{ formatCreator(mount) }}
                  </span>
                </div>
              </div>

              <!-- 操作按钮组 - 根据用户类型显示不同按钮 -->
              <div class="flex justify-end space-x-2">
                <!-- 管理员可以进行所有操作 -->
                <template v-if="isAdmin">
                  <button
                    @click="openEditForm(mount)"
                    class="inline-flex items-center px-2.5 py-1.5 rounded-md text-xs font-medium transition-all duration-200 focus:outline-none focus:ring-2 focus:ring-offset-1"
                    :class="
                      darkMode
                        ? 'bg-gray-600 hover:bg-gray-500 text-gray-200 focus:ring-blue-500 focus:ring-offset-gray-800'
                        : 'bg-gray-100 hover:bg-gray-200 text-gray-700 focus:ring-blue-500 focus:ring-offset-white'
                    "
                  >
                    <svg class="h-3.5 w-3.5 mr-1" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                      <path
                        stroke-linecap="round"
                        stroke-linejoin="round"
                        stroke-width="2"
                        d="M11 5H6a2 2 0 00-2 2v11a2 2 0 002 2h11a2 2 0 002-2v-5m-1.414-9.414a2 2 0 112.828 2.828L11.828 15H9v-2.828l8.586-8.586z"
                      />
                    </svg>
                    {{ $t("admin.mount.actions.edit") }}
                  </button>
                  <!-- 启用/禁用切换按钮 -->
                  <button
                    @click="toggleActive(mount)"
                    class="inline-flex items-center px-2.5 py-1.5 rounded-md text-xs font-medium transition-all duration-200 focus:outline-none focus:ring-2 focus:ring-offset-1"
                    :class="
                      darkMode
                        ? mount.is_active
                          ? 'bg-yellow-700 hover:bg-yellow-600 text-yellow-100 focus:ring-yellow-500 focus:ring-offset-gray-800'
                          : 'bg-green-700 hover:bg-green-600 text-green-100 focus:ring-green-500 focus:ring-offset-gray-800'
                        : mount.is_active
                        ? 'bg-yellow-100 hover:bg-yellow-200 text-yellow-800 focus:ring-yellow-500 focus:ring-offset-white'
                        : 'bg-green-100 hover:bg-green-200 text-green-800 focus:ring-green-500 focus:ring-offset-white'
                    "
                  >
                    <svg v-if="mount.is_active" class="h-3.5 w-3.5 mr-1" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                      <path
                        stroke-linecap="round"
                        stroke-linejoin="round"
                        stroke-width="2"
                        d="M18.364 18.364A9 9 0 005.636 5.636m12.728 12.728A9 9 0 015.636 5.636m12.728 12.728L5.636 5.636"
                      />
                    </svg>
                    <svg v-else class="h-3.5 w-3.5 mr-1" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                      <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z" />
                    </svg>
                    {{ mount.is_active ? $t("admin.mount.actions.disable") : $t("admin.mount.actions.enable") }}
                  </button>
                  <button
                    @click="confirmDelete(mount.id)"
                    class="inline-flex items-center px-2.5 py-1.5 rounded-md text-xs font-medium transition-all duration-200 focus:outline-none focus:ring-2 focus:ring-offset-1"
                    :class="
                      darkMode
                        ? 'bg-red-700 hover:bg-red-600 text-red-100 focus:ring-red-500 focus:ring-offset-gray-800'
                        : 'bg-red-100 hover:bg-red-200 text-red-800 focus:ring-red-500 focus:ring-offset-white'
                    "
                  >
                    <svg class="h-3.5 w-3.5 mr-1" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                      <path
                        stroke-linecap="round"
                        stroke-linejoin="round"
                        stroke-width="2"
                        d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16"
                      />
                    </svg>
                    {{ $t("admin.mount.actions.delete") }}
                  </button>
                </template>
                <!-- API密钥用户只能查看，显示只读提示 -->
                <template v-else>
                  <span class="inline-flex items-center px-2.5 py-1.5 rounded-md text-xs font-medium" :class="darkMode ? 'bg-gray-700 text-gray-400' : 'bg-gray-100 text-gray-500'">
                    <svg class="h-3.5 w-3.5 mr-1" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                      <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 12a3 3 0 11-6 0 3 3 0 016 0z" />
                      <path
                        stroke-linecap="round"
                        stroke-linejoin="round"
                        stroke-width="2"
                        d="M2.458 12C3.732 7.943 7.523 5 12 5c4.478 0 8.268 2.943 9.542 7-1.274 4.057-5.064 7-9.542 7-4.477 0-8.268-2.943-9.542-7z"
                      />
                    </svg>
                    {{ $t("admin.mount.actions.view") }}
                  </span>
                </template>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>

    <!-- 分页组件 -->
    <div class="mt-2 mb-4 sm:mt-4 sm:mb-0">
      <CommonPagination :dark-mode="darkMode" :pagination="pagination" mode="offset" @offset-changed="handleOffsetChange" />
    </div>

    <!-- 新建/编辑挂载点表单 -->
    <MountForm v-if="showForm" :dark-mode="darkMode" :mount="currentMount" :user-type="userType" @close="closeForm" @save-success="handleFormSaveSuccess" />
  </div>
</template>

<style scoped>
/* 自定义滚动条样式 */
::-webkit-scrollbar {
  width: 8px;
  height: 8px;
}
::-webkit-scrollbar-track {
  background: transparent;
}
::-webkit-scrollbar-thumb {
  background-color: rgba(156, 163, 175, 0.5);
  border-radius: 4px;
}
.dark ::-webkit-scrollbar-thumb {
  background-color: rgba(75, 85, 99, 0.5);
}
::-webkit-scrollbar-thumb:hover {
  background-color: rgba(107, 114, 128, 0.5);
}
.dark ::-webkit-scrollbar-thumb:hover {
  background-color: rgba(107, 114, 128, 0.5);
}

/* 卡片过渡效果 */
.mount-card-enter-active,
.mount-card-leave-active {
  transition: opacity 0.3s, transform 0.3s;
}
.mount-card-enter-from,
.mount-card-leave-to {
  opacity: 0;
  transform: translateY(10px);
}
</style>
