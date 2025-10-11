<script setup>
import { ref, computed } from "vue";
import { useI18n } from "vue-i18n";
import { api } from "@/api";
import { copyToClipboard } from "@/utils/clipboard";
import { Permission, PermissionChecker } from "@/constants/permissions.js";

// 获取权限标签列表
const getPermissionTags = (permissions) => {
  const tags = [];

  if (PermissionChecker.hasPermission(permissions, Permission.TEXT)) {
    tags.push({ key: "text", label: "admin.keyManagement.permissions.text", color: "blue" });
  }

  if (PermissionChecker.hasPermission(permissions, Permission.FILE_SHARE)) {
    tags.push({ key: "file_share", label: "admin.keyManagement.permissions.file_share", color: "green" });
  }

  if (PermissionChecker.hasPermission(permissions, Permission.MOUNT_VIEW)) {
    tags.push({ key: "mount_view", label: "admin.keyManagement.permissions.mount_view", color: "purple" });
  }

  if (PermissionChecker.hasPermission(permissions, Permission.MOUNT_UPLOAD)) {
    tags.push({ key: "mount_upload", label: "admin.keyManagement.permissions.mount_upload", color: "indigo" });
  }

  if (PermissionChecker.hasPermission(permissions, Permission.MOUNT_COPY)) {
    tags.push({ key: "mount_copy", label: "admin.keyManagement.permissions.mount_copy", color: "pink" });
  }

  if (PermissionChecker.hasPermission(permissions, Permission.MOUNT_RENAME)) {
    tags.push({ key: "mount_rename", label: "admin.keyManagement.permissions.mount_rename", color: "yellow" });
  }

  if (PermissionChecker.hasPermission(permissions, Permission.MOUNT_DELETE)) {
    tags.push({ key: "mount_delete", label: "admin.keyManagement.permissions.mount_delete", color: "red" });
  }

  if (PermissionChecker.hasPermission(permissions, Permission.WEBDAV_READ)) {
    tags.push({ key: "webdav_read", label: "admin.keyManagement.permissions.webdav_read", color: "cyan" });
  }

  if (PermissionChecker.hasPermission(permissions, Permission.WEBDAV_MANAGE)) {
    tags.push({ key: "webdav_manage", label: "admin.keyManagement.permissions.webdav_manage", color: "orange" });
  }

  return tags;
};

// 获取标签颜色类
const getTagColorClass = (color) => {
  const colorMap = {
    blue: "bg-blue-900/50 text-blue-300",
    green: "bg-green-900/50 text-green-300",
    purple: "bg-purple-900/50 text-purple-300",
    indigo: "bg-indigo-900/50 text-indigo-300",
    pink: "bg-pink-900/50 text-pink-300",
    yellow: "bg-yellow-900/50 text-yellow-300",
    red: "bg-red-900/50 text-red-300",
    cyan: "bg-cyan-900/50 text-cyan-300",
    orange: "bg-orange-900/50 text-orange-300",
  };

  const lightColorMap = {
    blue: "bg-blue-100 text-blue-800",
    green: "bg-green-100 text-green-800",
    purple: "bg-purple-100 text-purple-800",
    indigo: "bg-indigo-100 text-indigo-800",
    pink: "bg-pink-100 text-pink-800",
    yellow: "bg-yellow-100 text-yellow-800",
    red: "bg-red-100 text-red-800",
    cyan: "bg-cyan-100 text-cyan-800",
    orange: "bg-orange-100 text-orange-800",
  };

  return props.darkMode ? colorMap[color] || colorMap.blue : lightColorMap[color] || lightColorMap.blue;
};

// i18n
const { t } = useI18n();

// Props
const props = defineProps({
  darkMode: {
    type: Boolean,
    required: true,
  },
  apiKeys: {
    type: Array,
    required: true,
  },
  isLoading: {
    type: Boolean,
    default: false,
  },
  availableMounts: {
    type: Array,
    default: () => [],
  },
  isMobile: {
    type: Boolean,
    default: false,
  },
});

// Emits
const emit = defineEmits(["refresh", "edit", "success", "error", "selected-keys-change"]);

// 状态管理
const error = ref(null);
const successMessage = ref(null);
const selectedKeys = ref([]);

// 切换选择状态
const toggleSelectKey = (keyId) => {
  const index = selectedKeys.value.indexOf(keyId);
  if (index === -1) {
    selectedKeys.value.push(keyId);
  } else {
    selectedKeys.value.splice(index, 1);
  }
  emit("selected-keys-change", selectedKeys.value);
};

// 全选/取消全选
const toggleSelectAll = () => {
  if (selectedKeys.value.length === props.apiKeys.length) {
    selectedKeys.value = [];
  } else {
    selectedKeys.value = props.apiKeys.map((key) => key.id);
  }
  emit("selected-keys-change", selectedKeys.value);
};

// 删除单个密钥
const handleDeleteKey = async (keyId) => {
  if (!confirm(t("admin.keyManagement.deleteConfirm"))) {
    return;
  }

  try {
    const result = await api.admin.deleteApiKey(keyId);

    if (result.success) {
      // 从选中列表中移除
      const index = selectedKeys.value.indexOf(keyId);
      if (index !== -1) {
        selectedKeys.value.splice(index, 1);
        emit("selected-keys-change", selectedKeys.value);
      }

      emit("success", t("admin.keyManagement.success.deleted"));
      emit("refresh");
    } else {
      error.value = result.message || t("admin.keyManagement.error.deleteFailed");
      emit("error", error.value);
    }
  } catch (e) {
    console.error("删除API密钥失败:", e);
    error.value = t("admin.keyManagement.error.deleteFailed");
    emit("error", error.value);
  }
};

// 打开编辑模态框
const openEditModal = (key) => {
  emit("edit", key);
};

// 复制密钥到剪贴板
const copyKeyToClipboard = async (key) => {
  try {
    const success = await copyToClipboard(key);

    if (success) {
      emit("success", t("admin.keyManagement.success.copied"));
    } else {
      throw new Error(t("admin.keyManagement.error.copyFailed"));
    }
  } catch (e) {
    console.error("复制到剪贴板失败:", e);
    error.value = t("admin.keyManagement.error.copyFailed");
    emit("error", error.value);
  }
};

// 导入统一的时间处理工具
import { formatDateTime } from "@/utils/timeUtils.js";

// 格式化日期显示
const formatDate = (dateString) => {
  if (!dateString) return t("admin.keyManagement.neverExpires");

  // 检查是否是远未来日期（表示永不过期）
  if (dateString.startsWith("9999-")) {
    return t("admin.keyManagement.neverExpires");
  }

  return formatDateTime(dateString);
};

// 获取基本路径显示文本
const getDisplayPath = (path) => {
  if (!path || path === "/") {
    return `${t("admin.keyManagement.pathSelector.rootDirectory", "根目录")} (/)`;
  }

  // 查找对应的挂载点名称
  const mount = props.availableMounts.find((m) => m.mount_path === path);
  if (mount) {
    return `${mount.name} (${path})`;
  }

  // 处理长路径，当路径超过25个字符时进行省略
  if (path.length > 25) {
    const segments = path.split("/");

    // 如果段数小于等于2（如 /abc），则不进行省略
    if (segments.length <= 2) {
      return path;
    }

    // 保留第一段和最后一段，中间用省略号代替
    const firstPart = segments[1]; // 第一段（不包含空字符串）
    const lastPart = segments[segments.length - 1];

    // 如果最后一段是空字符串（路径以/结尾），则使用倒数第二段
    const last = lastPart === "" ? segments[segments.length - 2] : lastPart;

    return `/${firstPart}/.../${last}${lastPart === "" ? "/" : ""}`;
  }

  return path;
};

// 清除选中的密钥
const clearSelectedKeys = () => {
  selectedKeys.value = [];
  emit("selected-keys-change", selectedKeys.value);
};

// 对外暴露方法
defineExpose({
  clearSelectedKeys,
  getSelectedKeys: () => selectedKeys.value,
});
</script>

<template>
  <div class="flex flex-col">
    <!-- 加载状态 -->
    <div v-if="isLoading && apiKeys.length === 0" class="flex flex-col items-center justify-center h-40">
      <div class="animate-spin rounded-full h-8 w-8 border-b-2" :class="darkMode ? 'border-white' : 'border-primary-500'"></div>
      <p class="mt-3 text-sm" :class="darkMode ? 'text-gray-300' : 'text-gray-600'">{{ $t("admin.keyManagement.loadingKeys") }}</p>
    </div>

    <!-- 空状态 -->
    <div v-else-if="apiKeys.length === 0" class="text-center p-8 border rounded-md" :class="darkMode ? 'border-gray-700 text-gray-400' : 'border-gray-200 text-gray-500'">
      <svg xmlns="http://www.w3.org/2000/svg" class="h-12 w-12 mx-auto mb-3 opacity-40" fill="none" viewBox="0 0 24 24" stroke="currentColor">
        <path
          stroke-linecap="round"
          stroke-linejoin="round"
          stroke-width="1.5"
          d="M15 7a2 2 0 012 2m4 0a6 6 0 01-7.743 5.743L11 17H9v2H7v2H4a1 1 0 01-1-1v-2.586a1 1 0 01.293-.707l5.964-5.964A6 6 0 1121 9z"
        />
      </svg>
      <p class="text-lg font-medium mb-1">{{ $t("admin.keyManagement.noKeysTitle") }}</p>
      <p class="mb-4">{{ $t("admin.keyManagement.noKeysDescription") }}</p>
    </div>

    <!-- 数据展示 -->
    <div v-else>
      <!-- 桌面端表格视图 -->
      <div class="hidden md:block flex-1 overflow-auto">
        <table class="w-full border-collapse" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">
          <thead>
            <tr :class="darkMode ? 'bg-gray-700/50' : 'bg-gray-50'">
              <!-- 选择列 -->
              <th class="py-3 px-4 text-left text-xs font-medium" :class="darkMode ? 'text-gray-400' : 'text-gray-500'">
                <input
                  type="checkbox"
                  :checked="selectedKeys.length === apiKeys.length"
                  :indeterminate="selectedKeys.length > 0 && selectedKeys.length < apiKeys.length"
                  @change="toggleSelectAll"
                  class="rounded border-gray-300 text-primary-600 focus:ring-primary-500"
                  :class="darkMode ? 'bg-gray-700 border-gray-600' : 'bg-white border-gray-300'"
                />
              </th>
              <th class="py-3 px-4 text-left text-xs font-medium" :class="darkMode ? 'text-gray-400' : 'text-gray-500'">
                {{ $t("admin.keyManagement.keyName") }}
              </th>
              <th class="text-left py-3 px-4 font-medium text-sm" :class="darkMode ? 'text-gray-200' : 'text-gray-600'">
                {{ $t("admin.keyManagement.key") }}
              </th>
              <th class="text-left py-3 px-4 font-medium text-sm" :class="darkMode ? 'text-gray-200' : 'text-gray-600'">
                {{ $t("admin.keyManagement.permissionsColumn") }}
              </th>
              <th class="text-left py-3 px-4 font-medium text-sm" :class="darkMode ? 'text-gray-200' : 'text-gray-600'">
                {{ $t("admin.keyManagement.basicPath", "基本路径") }}
              </th>
              <th class="text-left py-3 px-4 font-medium text-sm" :class="darkMode ? 'text-gray-200' : 'text-gray-600'">
                {{ $t("admin.keyManagement.createdAt") }}
              </th>
              <th class="text-left py-3 px-4 font-medium text-sm" :class="darkMode ? 'text-gray-200' : 'text-gray-600'">
                {{ $t("admin.keyManagement.expiresAt") }}
              </th>
              <th class="text-left py-3 px-4 font-medium text-sm" :class="darkMode ? 'text-gray-200' : 'text-gray-600'">
                {{ $t("admin.keyManagement.lastUsed") }}
              </th>
              <th class="text-center py-3 px-4 font-medium text-sm" :class="darkMode ? 'text-gray-200' : 'text-gray-600'">
                {{ $t("admin.keyManagement.actions") }}
              </th>
            </tr>
          </thead>
          <tbody class="divide-y" :class="darkMode ? 'divide-gray-700' : 'divide-gray-200'">
            <tr v-for="key in apiKeys" :key="key.id" :class="darkMode ? 'hover:bg-gray-700/50' : 'hover:bg-gray-50'">
              <!-- 选择框 -->
              <td class="py-3 px-4">
                <input
                  type="checkbox"
                  :checked="selectedKeys.includes(key.id)"
                  @change="toggleSelectKey(key.id)"
                  class="rounded border-gray-300 text-primary-600 focus:ring-primary-500"
                  :class="darkMode ? 'bg-gray-700 border-gray-600' : 'bg-white border-gray-300'"
                />
              </td>
              <td class="py-3 px-4">{{ key.name }}</td>
              <td class="py-3 px-4">
                <div class="flex items-center">
                  <span class="font-mono text-sm mr-2">{{ key.key_masked }}</span>
                  <button
                    @click="copyKeyToClipboard(key.key)"
                    class="text-xs px-2 py-0.5 rounded"
                    :class="darkMode ? 'bg-gray-700 hover:bg-gray-600 text-gray-300' : 'bg-gray-200 hover:bg-gray-300 text-gray-700'"
                    :title="$t('admin.keyManagement.copyKeyFull')"
                  >
                    {{ $t("admin.keyManagement.copyKey") }}
                  </button>
                </div>
              </td>
              <td class="py-3 px-4">
                <div class="flex flex-wrap gap-1">
                  <span v-for="tag in getPermissionTags(key.permissions || 0)" :key="tag.key" class="px-2 py-0.5 text-xs rounded" :class="getTagColorClass(tag.color)">
                    {{ $t(tag.label) }}
                  </span>
                  <span
                    v-if="getPermissionTags(key.permissions || 0).length === 0"
                    class="px-2 py-0.5 text-xs rounded"
                    :class="darkMode ? 'bg-gray-800 text-gray-400' : 'bg-gray-100 text-gray-500'"
                  >
                    {{ $t("admin.keyManagement.permissions.none") }}
                  </span>
                </div>
              </td>
              <td class="py-3 px-4 text-sm" :class="darkMode ? 'text-gray-400' : 'text-gray-500'">
                <div class="max-w-[200px] overflow-hidden text-ellipsis whitespace-nowrap" :title="key.basic_path">
                  {{ getDisplayPath(key.basic_path) }}
                </div>
              </td>
              <td class="py-3 px-4 text-sm" :class="darkMode ? 'text-gray-400' : 'text-gray-500'">{{ formatDate(key.created_at) }}</td>
              <td class="py-3 px-4 text-sm" :class="darkMode ? 'text-gray-400' : 'text-gray-500'">{{ formatDate(key.expires_at) }}</td>
              <td class="py-3 px-4 text-sm" :class="darkMode ? 'text-gray-400' : 'text-gray-500'">
                {{ key.last_used ? formatDate(key.last_used) : $t("admin.keyManagement.neverUsed") }}
              </td>
              <td class="py-3 px-4 text-center">
                <div class="flex justify-center space-x-2">
                  <button
                    @click="openEditModal(key)"
                    class="text-sm px-2 py-1 rounded transition-colors duration-150"
                    :class="darkMode ? 'hover:bg-blue-900/40 text-blue-400' : 'hover:bg-blue-100 text-blue-600'"
                    :disabled="isLoading"
                  >
                    {{ $t("admin.keyManagement.edit") }}
                  </button>
                  <button
                    @click="handleDeleteKey(key.id)"
                    class="text-sm px-2 py-1 rounded transition-colors duration-150"
                    :class="darkMode ? 'hover:bg-red-900/40 text-red-400' : 'hover:bg-red-100 text-red-600'"
                    :disabled="isLoading"
                  >
                    {{ $t("admin.keyManagement.delete") }}
                  </button>
                </div>
              </td>
            </tr>
          </tbody>
        </table>
      </div>

      <!-- 移动端卡片视图 -->
      <div class="md:hidden flex-1 overflow-auto">
        <div v-if="isLoading" class="p-4 text-center" :class="darkMode ? 'text-gray-300' : 'text-gray-500'">
          <div class="flex justify-center">
            <svg class="animate-spin h-5 w-5" :class="darkMode ? 'text-white' : 'text-primary-500'" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
              <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
              <path
                class="opacity-75"
                fill="currentColor"
                d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"
              ></path>
            </svg>
            <span class="ml-2">{{ $t("admin.keyManagement.loading") }}</span>
          </div>
        </div>

        <!-- 移动端：每个密钥显示为一个卡片 -->
        <div class="p-2 space-y-3">
          <div v-for="key in apiKeys" :key="key.id" class="border rounded-lg p-3 relative" :class="darkMode ? 'border-gray-700 bg-gray-800/50' : 'border-gray-200 bg-white'">
            <!-- 密钥名称和操作按钮 -->
            <div class="flex items-start justify-between mb-2">
              <div class="flex items-center space-x-2">
                <!-- 选择框 -->
                <input
                  type="checkbox"
                  :checked="selectedKeys.includes(key.id)"
                  @change="toggleSelectKey(key.id)"
                  class="rounded border-gray-300 text-primary-600 focus:ring-primary-500"
                  :class="darkMode ? 'bg-gray-700 border-gray-600' : 'bg-white border-gray-300'"
                />
                <h3 class="font-medium" :class="darkMode ? 'text-white' : 'text-gray-900'">{{ key.name }}</h3>
              </div>
              <div class="flex items-center space-x-2">
                <!-- 复制按钮 -->
                <button
                  @click="copyKeyToClipboard(key.key)"
                  class="text-xs px-2 py-1 rounded"
                  :class="darkMode ? 'bg-gray-700 hover:bg-gray-600 text-gray-300' : 'bg-gray-200 hover:bg-gray-300 text-gray-700'"
                >
                  {{ $t("admin.keyManagement.copyKeyFull") }}
                </button>
              </div>
            </div>

            <!-- 密钥信息 -->
            <div class="text-sm mb-3" :class="darkMode ? 'text-gray-400' : 'text-gray-600'">
              <div class="font-mono mb-1">{{ key.key_masked }}</div>

              <!-- 权限标签 -->
              <div class="flex flex-wrap gap-2 mt-1 mb-2">
                <span v-for="tag in getPermissionTags(key.permissions || 0)" :key="tag.key" class="px-2 py-0.5 text-xs rounded" :class="getTagColorClass(tag.color)">
                  {{ $t(tag.label) }}
                </span>
                <span
                  v-if="getPermissionTags(key.permissions || 0).length === 0"
                  class="px-2 py-0.5 text-xs rounded"
                  :class="darkMode ? 'bg-gray-800 text-gray-400' : 'bg-gray-100 text-gray-500'"
                >
                  {{ $t("admin.keyManagement.permissions.none") }}
                </span>
              </div>

              <!-- 时间信息 -->
              <div class="grid grid-cols-2 gap-1">
                <div>
                  {{ $t("admin.keyManagement.basicPath", "基本路径") }}:
                  <span class="block overflow-hidden text-ellipsis whitespace-nowrap" :title="key.basic_path">
                    {{ getDisplayPath(key.basic_path) }}
                  </span>
                </div>
                <div>{{ $t("admin.keyManagement.createdAt") }}: {{ formatDate(key.created_at) }}</div>
                <div>{{ $t("admin.keyManagement.expiresAt") }}: {{ formatDate(key.expires_at) }}</div>
                <div>{{ $t("admin.keyManagement.lastUsed") }}: {{ key.last_used ? formatDate(key.last_used) : $t("admin.keyManagement.neverUsed") }}</div>
              </div>
            </div>

            <!-- 操作按钮 -->
            <div class="flex justify-end space-x-2">
              <button
                @click="openEditModal(key)"
                class="text-sm px-2 py-1 rounded transition-colors duration-150"
                :class="darkMode ? 'hover:bg-blue-900/40 text-blue-400' : 'hover:bg-blue-100 text-blue-600'"
              >
                {{ $t("admin.keyManagement.edit") }}
              </button>
              <button
                @click="handleDeleteKey(key.id)"
                class="text-sm px-2 py-1 rounded transition-colors duration-150"
                :class="darkMode ? 'hover:bg-red-900/40 text-red-400' : 'hover:bg-red-100 text-red-600'"
              >
                {{ $t("admin.keyManagement.delete") }}
              </button>
            </div>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>
