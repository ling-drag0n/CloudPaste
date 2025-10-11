<template>
  <div class="archive-preview-container">
    <!-- 压缩文件信息展示 -->
    <div v-if="!isExtracted" class="archive-info flex items-center justify-center min-h-[400px]">
      <!-- 密码输入界面 -->
      <ArchivePasswordInput
        v-if="isPasswordRequired"
        :dark-mode="darkMode"
        :password-error="passwordError"
        :is-validating="isValidatingPassword"
        @submit="handlePasswordSubmit"
        @cancel="handlePasswordCancel"
      />

      <!-- 简化的文件信息 -->
      <div v-else class="file-info-content text-center">
        <!-- 文件图标 -->
        <div class="flex justify-center mb-6">
          <div v-html="fileIcon" class="w-16 h-16"></div>
        </div>

        <!-- 文件信息 -->
        <h3 class="text-xl font-semibold mb-2" :class="darkMode ? 'text-gray-200' : 'text-gray-800'">
          {{ file.name }}
        </h3>
        <p class="text-sm mb-1" :class="darkMode ? 'text-gray-400' : 'text-gray-600'">
          {{ archiveInfo.description }}
        </p>
        <p class="text-sm mb-8" :class="darkMode ? 'text-gray-400' : 'text-gray-600'">
          {{ formatFileSize(file.size) }}
        </p>

        <!-- 在线解压按钮 -->
        <button
          v-if="archiveInfo.supported"
          @click="handleExtractArchive"
          :disabled="isExtracting"
          class="inline-flex items-center px-8 py-3 rounded-lg font-medium transition-all duration-200"
          :class="[
            darkMode
              ? 'bg-blue-600 hover:bg-blue-700 text-white disabled:bg-gray-600 disabled:text-gray-400'
              : 'bg-blue-500 hover:bg-blue-600 text-white disabled:bg-gray-400 disabled:text-gray-600',
            isExtracting ? 'cursor-not-allowed' : 'cursor-pointer hover:shadow-lg',
          ]"
        >
          <svg v-if="isExtracting" class="animate-spin -ml-1 mr-3 h-5 w-5" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
            <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
            <path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
          </svg>
          <svg v-else class="w-5 h-5 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M7 16a4 4 0 01-.88-7.903A5 5 0 1115.9 6L16 6a5 5 0 011 9.9M9 19l3 3m0 0l3-3m-3 3V10"></path>
          </svg>
          {{ isExtracting ? `正在${currentStage}... ${extractProgress.toFixed(1)}%` : "在线解压查看" }}
        </button>

        <!-- 不支持格式的提示 -->
        <div v-if="!archiveInfo.supported" class="mt-6 p-3 rounded-lg" :class="darkMode ? 'bg-yellow-900/20 border border-yellow-700' : 'bg-yellow-50 border border-yellow-200'">
          <div class="flex items-center justify-center">
            <svg class="w-5 h-5 mr-2" :class="darkMode ? 'text-yellow-400' : 'text-yellow-600'" fill="currentColor" viewBox="0 0 20 20">
              <path
                fill-rule="evenodd"
                d="M8.257 3.099c.765-1.36 2.722-1.36 3.486 0l5.58 9.92c.75 1.334-.213 2.98-1.742 2.98H4.42c-1.53 0-2.493-1.646-1.743-2.98l5.58-9.92zM11 13a1 1 0 11-2 0 1 1 0 012 0zm-1-8a1 1 0 00-1 1v3a1 1 0 002 0V6a1 1 0 00-1-1z"
                clip-rule="evenodd"
              ></path>
            </svg>
            <span class="text-sm" :class="darkMode ? 'text-yellow-200' : 'text-yellow-800'"> 暂不支持 {{ archiveInfo.name }} 格式的在线解压 </span>
          </div>
        </div>

        <!-- 错误提示 -->
        <div v-if="extractError" class="mt-6 p-3 rounded-lg" :class="darkMode ? 'bg-red-900/20 border border-red-700' : 'bg-red-50 border border-red-200'">
          <div class="flex items-center justify-center">
            <svg class="w-5 h-5 mr-2" :class="darkMode ? 'text-red-400' : 'text-red-600'" fill="currentColor" viewBox="0 0 20 20">
              <path
                fill-rule="evenodd"
                d="M10 18a8 8 0 100-16 8 8 0 000 16zM8.707 7.293a1 1 0 00-1.414 1.414L8.586 10l-1.293 1.293a1 1 0 101.414 1.414L10 11.414l1.293 1.293a1 1 0 001.414-1.414L11.414 10l1.293-1.293a1 1 0 00-1.414-1.414L10 8.586 8.707 7.293z"
                clip-rule="evenodd"
              ></path>
            </svg>
            <span class="text-sm" :class="darkMode ? 'text-red-200' : 'text-red-800'">{{ extractError }}</span>
          </div>
        </div>
      </div>
    </div>

    <!-- 解压后的文件列表 -->
    <div v-else class="archive-content">
      <!-- 头部工具栏 -->
      <div class="archive-header px-4 py-2 border-b" :class="darkMode ? 'border-gray-700 bg-gray-800' : 'border-gray-200 bg-gray-50'">
        <div class="flex items-center justify-between">
          <div class="flex items-center space-x-3">
            <button
              @click="handleBackToInfo"
              class="p-1.5 rounded-lg transition-colors"
              :class="darkMode ? 'hover:bg-gray-700 text-gray-300' : 'hover:bg-gray-200 text-gray-600'"
              title="返回文件信息"
            >
              <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 19l-7-7 7-7"></path>
              </svg>
            </button>
            <div class="flex-shrink-0">
              <div v-html="fileIcon" class="w-5 h-5"></div>
            </div>
            <div class="min-w-0 flex-1">
              <h3 class="text-base font-semibold truncate" :class="darkMode ? 'text-gray-200' : 'text-gray-800'">
                {{ file.name }}
              </h3>
              <p class="text-xs truncate" :class="darkMode ? 'text-gray-400' : 'text-gray-600'">{{ archiveEntries.length }} 个文件，{{ formatFileSize(totalSize) }}</p>
            </div>
          </div>

          <!-- 工具栏按钮 -->
          <div class="flex items-center space-x-2">
            <button
              @click="handleDownload"
              class="inline-flex items-center px-2 py-1 text-xs rounded-lg transition-colors"
              :class="darkMode ? 'bg-gray-700 hover:bg-gray-600 text-gray-200' : 'bg-gray-100 hover:bg-gray-200 text-gray-700'"
              title="下载原文件"
            >
              <svg class="w-3 h-3 mr-1" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path
                  stroke-linecap="round"
                  stroke-linejoin="round"
                  stroke-width="2"
                  d="M12 10v6m0 0l-3-3m3 3l3-3m2 8H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z"
                ></path>
              </svg>
              <span class="hidden sm:inline">下载</span>
            </button>
          </div>
        </div>
      </div>

      <!-- 文件列表表格 -->
      <div class="archive-file-list flex-1 overflow-auto">
        <div v-if="archiveEntries.length === 0" class="text-center py-12">
          <svg class="w-12 h-12 mx-auto mb-4" :class="darkMode ? 'text-gray-500' : 'text-gray-400'" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path
              stroke-linecap="round"
              stroke-linejoin="round"
              stroke-width="1.5"
              d="M7 21h10a2 2 0 002-2V9.414a1 1 0 00-.293-.707l-5.414-5.414A1 1 0 0012.586 3H7a2 2 0 00-2 2v14a2 2 0 002 2z"
            ></path>
          </svg>
          <p class="text-lg font-medium mb-2" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">压缩文件为空</p>
          <p class="text-sm" :class="darkMode ? 'text-gray-400' : 'text-gray-500'">此压缩文件不包含任何内容</p>
        </div>

        <!-- 表格式文件列表 -->
        <div v-else class="min-w-full">
          <!-- 表头 - 响应式设计 -->
          <div
            class="grid items-center py-2 px-3 border-b border-t"
            :class="[darkMode ? 'bg-gray-700/50 border-gray-600' : 'bg-gray-100 border-gray-200', 'grid-cols-[1fr_auto] sm:grid-cols-[1fr_auto_auto_auto]']"
          >
            <div class="font-medium pl-16" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">名称</div>
            <div class="w-24 text-center font-medium hidden sm:block" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">大小</div>
            <div class="w-32 text-center font-medium hidden sm:block" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">类型</div>
            <div class="min-w-[80px] sm:min-w-32 text-center font-medium" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">操作</div>
          </div>

          <!-- 树状文件列表 -->
          <div class="divide-y" :class="darkMode ? 'divide-gray-700' : 'divide-gray-200'">
            <div
              v-for="entry in visibleFileList"
              :key="entry.path"
              @click="handleRowClick(entry)"
              class="grid items-center py-2 px-3 hover:cursor-pointer transition-colors"
              :class="[
                darkMode ? 'hover:bg-gray-700/50' : 'hover:bg-gray-100',
                'grid-cols-[1fr_auto] sm:grid-cols-[1fr_auto_auto_auto]',
                selectedEntry?.path === entry.path ? (darkMode ? 'bg-blue-900/20' : 'bg-blue-50') : '',
              ]"
            >
              <!-- 文件名列 - 包含图标、层级缩进和名称 -->
              <div class="flex items-center space-x-2 min-w-0" :style="{ paddingLeft: `${entry.level * 16}px` }">
                <!-- 展开/折叠箭头（仅文件夹显示） -->
                <div
                  v-if="entry.isDirectory"
                  @click.stop="toggleFolder(entry.path.replace(/\/$/, ''))"
                  class="flex-shrink-0 w-4 h-4 flex items-center justify-center cursor-pointer rounded transition-colors"
                  :class="darkMode ? 'hover:bg-gray-600' : 'hover:bg-gray-200'"
                  :title="isFolderExpanded(entry.path.replace(/\/$/, '')) ? '折叠文件夹' : '展开文件夹'"
                >
                  <svg
                    class="w-3 h-3 transition-transform duration-200"
                    :class="isFolderExpanded(entry.path.replace(/\/$/, '')) ? 'rotate-90' : ''"
                    fill="none"
                    stroke="currentColor"
                    viewBox="0 0 24 24"
                  >
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5l7 7-7 7"></path>
                  </svg>
                </div>
                <!-- 占位符（非文件夹） -->
                <div v-else class="w-4 h-4 flex-shrink-0"></div>

                <!-- 文件图标 -->
                <div class="flex-shrink-0 w-5 sm:w-6 h-5 sm:h-6">
                  <span v-html="getFileIconSvg(entry)"></span>
                </div>

                <!-- 文件名 + 移动端信息重组 -->
                <div class="flex-grow truncate">
                  <div class="font-medium truncate" :class="darkMode ? 'text-gray-200' : 'text-gray-700'">
                    {{ entry.name }}
                    <!-- 文件夹文件数量显示 -->
                    <span v-if="entry.isDirectory && entry.fileCount > 0" class="text-xs ml-1 font-normal" :class="darkMode ? 'text-gray-500' : 'text-gray-400'">
                      ({{ entry.fileCount }})
                    </span>
                  </div>
                  <!-- 移动端显示大小和类型信息 -->
                  <div class="text-xs block sm:hidden mt-0.5 flex items-center space-x-2" :class="darkMode ? 'text-gray-400' : 'text-gray-500'">
                    <span v-if="!entry.isDirectory" class="flex-shrink-0">{{ formatFileSize(entry.size || 0) }}</span>
                    <span class="truncate" :title="getMimeTypeDescription(entry)">{{ getMimeTypeDescription(entry) }}</span>
                  </div>
                </div>
              </div>

              <!-- 桌面端大小列 - 固定宽度 -->
              <div class="w-24 text-center text-sm hidden sm:block" :class="darkMode ? 'text-gray-400' : 'text-gray-500'">
                <span v-if="entry.isDirectory">-</span>
                <span v-else>{{ formatFileSize(entry.size || 0) }}</span>
              </div>

              <!-- 桌面端类型列 - 固定宽度，内容过长时省略 -->
              <div class="w-32 text-center text-sm hidden sm:block" :class="darkMode ? 'text-gray-400' : 'text-gray-500'">
                <span class="block truncate px-1" :title="getMimeTypeDescription(entry)">
                  {{ getMimeTypeDescription(entry) }}
                </span>
              </div>

              <!-- 操作按钮  -->
              <div class="min-w-[80px] sm:min-w-32 text-center">
                <div class="flex justify-end sm:justify-center space-x-0.5 sm:space-x-1">
                  <!-- 预览按钮（仅文件显示） -->
                  <button
                    v-if="!entry.isDirectory && canPreviewFile(entry, true)"
                    @click.stop="handlePreviewFile(entry)"
                    class="p-1.5 sm:p-2 rounded-full transition-colors"
                    :class="darkMode ? 'hover:bg-gray-600 text-blue-400 hover:text-blue-300' : 'hover:bg-gray-200 text-blue-600 hover:text-blue-700'"
                    title="预览文件"
                  >
                    <svg class="w-4 h-4 sm:w-5 sm:h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                      <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 12a3 3 0 11-6 0 3 3 0 016 0z"></path>
                      <path
                        stroke-linecap="round"
                        stroke-linejoin="round"
                        stroke-width="2"
                        d="M2.458 12C3.732 7.943 7.523 5 12 5c4.478 0 8.268 2.943 9.542 7-1.274 4.057-5.064 7-9.542 7-4.477 0-8.268-2.943-9.542-7z"
                      ></path>
                    </svg>
                  </button>

                  <!-- 下载按钮（仅文件显示） -->
                  <button
                    v-if="!entry.isDirectory"
                    @click.stop="handleDownloadFile(entry)"
                    class="p-1.5 sm:p-2 rounded-full transition-colors"
                    :class="darkMode ? 'hover:bg-gray-600 text-green-400 hover:text-green-300' : 'hover:bg-gray-200 text-green-600 hover:text-green-700'"
                    title="下载文件"
                  >
                    <svg class="w-4 h-4 sm:w-5 sm:h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                      <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-4l-4 4m0 0l-4-4m4 4V4"></path>
                    </svg>
                  </button>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { computed, ref, nextTick, onBeforeUnmount } from "vue";
import { getArchiveType, formatFileSize, canPreviewFile, createMockFileObject, getMimeTypeDescription } from "@/utils/fileTypes.js";
import { getFileIcon } from "@/utils/fileTypeIcons.js";
import { useArchivePreview } from "@/composables/archive/useArchivePreview.js";
import ArchivePasswordInput from "./ArchivePasswordInput.vue";

// Props
const props = defineProps({
  file: {
    type: Object,
    required: true,
  },
  darkMode: {
    type: Boolean,
    default: false,
  },
  authenticatedPreviewUrl: {
    type: String,
    default: "",
  },
});

// Emits
const emit = defineEmits(["download", "loaded", "error"]);

// 使用压缩文件预览 composable
const { isExtracting, extractError, archiveEntries, isExtracted, extractProgress, currentStage, totalSize, extractArchive, previewFile, downloadFile, resetState } =
  useArchivePreview();

// 本地状态
const selectedEntry = ref(null);
const expandedFolders = ref(new Set()); // 展开的文件夹状态

// 密码相关状态
const isPasswordRequired = ref(false); // 是否需要密码
const passwordError = ref(""); // 密码错误信息
const isValidatingPassword = ref(false); // 是否正在验证密码

// 计算属性
const archiveInfo = computed(() => {
  return getArchiveType(props.file.name);
});

// 文件图标计算属性
const fileIcon = computed(() => {
  if (!props.file) return "";
  return getFileIcon(props.file, props.darkMode);
});

// 树状结构数据
const treeStructure = computed(() => {
  if (!archiveEntries.value || !archiveEntries.value.length) return [];

  // 将扁平数组转换为树状结构
  const tree = [];
  const pathMap = new Map();

  // 为根目录创建虚拟容器
  pathMap.set("", { children: tree });

  // 按路径深度排序，确保父目录先处理
  const sortedEntries = [...archiveEntries.value].sort((a, b) => {
    const depthA = a.name.split("/").length;
    const depthB = b.name.split("/").length;
    return depthA - depthB;
  });

  sortedEntries.forEach((entry) => {
    // 边界情况检查
    if (!entry || !entry.name) return;

    const pathParts = entry.name.split("/").filter((part) => part); // 移除空字符串

    if (pathParts.length === 0) return; // 跳过空路径

    const fileName = pathParts[pathParts.length - 1];
    const parentPath = pathParts.slice(0, -1).join("/");

    // 跳过无效的文件名
    if (!fileName) return;

    // 确保父目录存在
    if (parentPath && !pathMap.has(parentPath)) {
      // 创建缺失的父目录节点
      const parentParts = parentPath.split("/");
      let currentPath = "";

      parentParts.forEach((part) => {
        const prevPath = currentPath;
        currentPath = currentPath ? `${currentPath}/${part}` : part;

        if (!pathMap.has(currentPath)) {
          const parentContainer = pathMap.get(prevPath);
          const folderNode = {
            name: part,
            path: currentPath,
            fullPath: `${currentPath}/`,
            isDirectory: true,
            size: 0,
            children: [],
            entry: null,
            isVirtual: true, // 标记为虚拟节点
          };

          parentContainer.children.push(folderNode);
          pathMap.set(currentPath, folderNode);
        }
      });
    }

    // 添加当前节点
    const parent = pathMap.get(parentPath);
    const node = {
      name: fileName,
      path: entry.name,
      fullPath: entry.name,
      isDirectory: entry.isDirectory,
      size: entry.size || 0,
      lastModDate: entry.lastModDate,
      entry: entry.entry,
      children: entry.isDirectory ? [] : undefined,
      isVirtual: false, // 标记为真实节点
    };

    if (entry.isDirectory) {
      // 如果已存在虚拟节点，替换为真实节点
      const normalizedPath = entry.name.replace(/\/$/, "");
      const existingNode = pathMap.get(normalizedPath);
      if (existingNode && existingNode.isVirtual) {
        // 保留虚拟节点的 children，更新为真实节点
        node.children = existingNode.children;
        // 从父容器中移除虚拟节点
        const index = parent.children.findIndex((child) => child.path.replace(/\/$/, "") === normalizedPath);
        if (index !== -1) {
          parent.children.splice(index, 1); // 移除虚拟节点
        }
      }
      pathMap.set(normalizedPath, node);
    }

    // 添加节点到父容器（统一处理，避免重复添加）
    parent.children.push(node);
  });

  return tree;
});

// 工具函数
const validateEntry = (entry) => {
  if (entry.isDirectory) {
    return { valid: false, reason: "directory" };
  }
  if (entry.isVirtual || !entry.entry) {
    return { valid: false, reason: "virtual" };
  }
  return { valid: true };
};

const createOriginalEntry = (entry) => ({
  name: entry.fullPath,
  isDirectory: entry.isDirectory,
  size: entry.size,
  entry: entry.entry,
});

const resetPasswordState = () => {
  isPasswordRequired.value = false;
  passwordError.value = "";
  isValidatingPassword.value = false;
};

// 方法
const handleExtractArchive = async () => {
  if (!archiveInfo.value.supported) return;

  // 检查预览URL是否有效
  if (!props.authenticatedPreviewUrl) {
    console.error("预览URL无效，无法进行解压操作");
    return;
  }

  // 第一次尝试解压
  await attemptExtraction(null);
};

// 尝试解压的核心方法
const attemptExtraction = async (password) => {
  try {
    console.log("开始解压压缩文件:", props.file.name);
    console.log("使用预览URL:", props.authenticatedPreviewUrl);

    if (password) {
      console.log("使用密码进行解压");
    } else {
      console.log("尝试流式检测解压");
    }

    // 使用composable进行解压，传递密码
    await extractArchive(props.authenticatedPreviewUrl, props.file.name, password);

    // 解压成功，隐藏密码输入界面
    resetPasswordState();

    // 自动展开第一层目录
    await nextTick();
    expandedFolders.value.clear();

    // 自动展开第一层目录
    if (archiveEntries.value && Array.isArray(archiveEntries.value)) {
      archiveEntries.value.forEach((node) => {
        if (node && node.isDirectory && node.path) {
          expandedFolders.value.add(node.path.replace(/\/$/, ""));
        }
      });
    }

    emit("loaded");
  } catch (error) {
    console.error("解压失败:", error);

    // 检查是否是加密检测错误
    if (error.message && error.message.includes("ENCRYPTED_ARCHIVE_DETECTED")) {
      // 检测到加密文件，文件已预下载完成，显示密码输入界面
      console.log("检测到加密文件，文件已预下载完成，显示密码输入界面");
      isPasswordRequired.value = true;
      passwordError.value = "";
      isValidatingPassword.value = false;
    } else if (error.message && error.message.includes("INVALID_ARCHIVE_PASSWORD")) {
      // 密码错误，重新显示密码输入界面
      console.log("密码错误，重新显示密码输入界面");
      isPasswordRequired.value = true;
      passwordError.value = "密码错误，请重新输入";
      isValidatingPassword.value = false;
    } else {
      // 其他错误，正常处理
      emit("error", error);
    }
  }
};

// 密码处理方法
const handlePasswordSubmit = async (inputPassword) => {
  console.log("用户提交密码，开始验证...");

  // 隐藏密码输入界面，显示正常的解压进度界面
  resetPasswordState();

  // 使用提交的密码重新尝试解压（会显示正常的进度条）
  await attemptExtraction(inputPassword);
};

const handlePasswordCancel = () => {
  console.log("用户取消密码输入");
  resetPasswordState();
};

const handleBackToInfo = () => {
  // 重置状态，返回到文件信息页面
  resetState();
  expandedFolders.value.clear(); // 清理展开状态
  selectedEntry.value = null; // 清理选择状态

  // 重置密码相关状态
  resetPasswordState();

  console.log("已重置压缩文件预览状态，返回文件信息页面");
};

// 统一的行点击处理（避免移动端点击事件冲突）
const handleRowClick = (entry) => {
  if (entry.isDirectory) {
    // 文件夹：展开/折叠
    toggleFolder(entry.path.replace(/\/$/, ""));
  } else {
    // 文件：选择高亮
    selectedEntry.value = entry;
    console.log("选择文件:", entry.name);
  }
};

const handlePreviewFile = async (entry) => {
  const validation = validateEntry(entry);

  if (!validation.valid) {
    if (validation.reason === "directory") {
      toggleFolder(entry.path.replace(/\/$/, ""));
      return;
    }
    if (validation.reason === "virtual") {
      console.warn("无法预览虚拟节点:", entry.name);
      return;
    }
  }

  const originalEntry = createOriginalEntry(entry);
  await previewFile(originalEntry);
};

// 移除弹窗相关处理函数，改为新页面预览

const handleDownloadFile = async (entry) => {
  const validation = validateEntry(entry);

  if (!validation.valid) {
    if (validation.reason === "directory") {
      console.warn("无法下载目录:", entry.name);
      return;
    }
    if (validation.reason === "virtual") {
      console.warn("无法下载虚拟节点:", entry.name);
      return;
    }
  }

  const originalEntry = createOriginalEntry(entry);
  await downloadFile(originalEntry);
};

const handleDownload = () => {
  emit("download");
};

// 树状结构相关方法
const toggleFolder = (folderPath) => {
  if (expandedFolders.value.has(folderPath)) {
    expandedFolders.value.delete(folderPath);
  } else {
    expandedFolders.value.add(folderPath);
  }
};

const isFolderExpanded = (folderPath) => {
  return expandedFolders.value.has(folderPath);
};

// 渲染树状节点的方法
const renderTreeNode = (node, level = 0) => {
  return {
    ...node,
    level,
    isExpanded: node.isDirectory ? isFolderExpanded(node.path.replace(/\/$/, "")) : false,
  };
};

// 获取可见的树状节点
const getVisibleTreeNodes = (nodes, level = 0) => {
  if (!nodes || !Array.isArray(nodes)) return [];

  const result = [];

  // 排序：文件夹在前，文件在后
  const sortedNodes = [...nodes].sort((a, b) => {
    // 文件夹优先
    if (a.isDirectory && !b.isDirectory) return -1;
    if (!a.isDirectory && b.isDirectory) return 1;
    // 同类型按名称排序
    return a.name.localeCompare(b.name, undefined, { numeric: true, sensitivity: "base" });
  });

  sortedNodes.forEach((node) => {
    if (!node) return; // 跳过无效节点

    // 添加当前节点（包含文件数量信息）
    const nodeWithCount = {
      ...renderTreeNode(node, level),
      fileCount: node.isDirectory ? getDirectoryFileCount(node) : 0,
    };
    result.push(nodeWithCount);

    // 如果是展开的文件夹，递归添加子节点
    if (node.isDirectory && node.children && Array.isArray(node.children) && isFolderExpanded(node.path.replace(/\/$/, ""))) {
      result.push(...getVisibleTreeNodes(node.children, level + 1));
    }
  });

  return result;
};

// 计算文件夹内的文件数量（递归统计）
const getDirectoryFileCount = (directory) => {
  if (!directory || !directory.children || !Array.isArray(directory.children)) {
    return 0;
  }

  let count = 0;
  directory.children.forEach((child) => {
    if (child.isDirectory) {
      count += getDirectoryFileCount(child); // 递归统计子文件夹
    } else {
      count += 1; // 文件计数
    }
  });

  return count;
};

// 可见的文件列表（用于渲染）
const visibleFileList = computed(() => {
  return getVisibleTreeNodes(treeStructure.value);
});

// 工具函数 - 获取文件图标
const getFileIconSvg = (entry) => {
  if (!entry) return "";

  // 使用适配器创建模拟文件对象，完全复用现有图标系统
  const mockFile = createMockFileObject(entry);
  return getFileIcon(mockFile, props.darkMode);
};

// 🧹 组件卸载时清理缓存
onBeforeUnmount(() => {
  console.log("🧹 ArchivePreview组件卸载，清理缓存");
  resetState(); // 清理所有缓存数据

  if (props.authenticatedPreviewUrl) {
    const { archiveService } = useArchivePreview();

    // 清理文件Blob缓存（原始压缩文件）
    archiveService.clearFileBlobCache(props.authenticatedPreviewUrl);
    console.log("🧹 已清理文件Blob缓存");

    // 清理解压结果缓存
    archiveService.clearFileCache(props.authenticatedPreviewUrl, props.file.name);
    console.log("🧹 已清理解压结果缓存");
  }
});
</script>

<style scoped>
.archive-preview-container {
  display: flex;
  flex-direction: column;
  height: 100%;
}

.archive-content {
  display: flex;
  flex-direction: column;
  height: 100%;
}

.archive-file-list {
  flex: 1;
  overflow: auto;
  max-height: 400px;
}

.archive-file-preview {
  flex-shrink: 0;
  max-height: 300px;
}

.preview-content {
  overflow: auto;
}

/* 移动端响应式优化 - 应用 DirectoryList 设计模式 */
@media (max-width: 768px) {
  /* 头部工具栏紧凑化 */
  .archive-header {
    padding: 0.5rem 0.75rem !important;
  }

  .archive-header h3 {
    font-size: 0.875rem !important;
  }

  .archive-header p {
    font-size: 0.75rem !important;
  }

  /* 文件信息页面优化 */
  .archive-info {
    min-height: 300px !important;
    padding: 1rem !important;
  }

  .file-info-content .w-16 {
    width: 3rem !important;
    height: 3rem !important;
  }

  /* 移动端文件列表优化 - 增大触摸目标 */
  .archive-file-list .grid {
    min-height: 48px; /* 符合移动端触摸标准 */
  }

  /* 移动端按钮触摸优化 */
  .archive-file-list button {
    min-width: 36px;
    min-height: 36px;
  }
}
</style>
