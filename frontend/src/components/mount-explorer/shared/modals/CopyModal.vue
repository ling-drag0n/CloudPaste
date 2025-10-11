<template>
  <div v-if="isOpen" class="fixed inset-0 z-[60] overflow-auto bg-black bg-opacity-50 flex items-center justify-center p-2 sm:p-4 pt-20 sm:pt-4">
    <div
      class="relative w-full max-w-sm sm:max-w-lg bg-white dark:bg-gray-800 rounded-lg shadow-xl max-h-[85vh] sm:max-h-[80vh] overflow-hidden"
      :class="darkMode ? 'bg-gray-800' : 'bg-white'"
    >
      <!-- 标题栏 -->
      <div class="px-4 py-3 border-b flex justify-between items-center" :class="darkMode ? 'border-gray-700' : 'border-gray-200'">
        <h3 class="text-lg font-medium" :class="darkMode ? 'text-gray-100' : 'text-gray-900'">{{ t("mount.copyModal.title") }}</h3>
        <button @click="closeModal" class="text-gray-400 hover:text-gray-500 dark:text-gray-500 dark:hover:text-gray-400">
          <svg class="h-5 w-5" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
          </svg>
        </button>
      </div>

      <!-- 内容区 -->
      <div class="p-3 sm:p-4 overflow-y-auto" style="max-height: calc(85vh - 140px)">
        <!-- 已选项目信息 -->
        <div class="mb-3 text-sm" :class="darkMode ? 'text-gray-300' : 'text-gray-600'">
          {{
            t("mount.copyModal.selectedInfo", {
              count: selectedItems.length,
              folders: selectedItems.filter((item) => item.isDirectory).length,
              files: selectedItems.filter((item) => !item.isDirectory).length,
            })
          }}
        </div>

        <!-- 当前路径 -->
        <div class="mb-3 text-sm font-medium" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">
          {{ t("mount.copyModal.targetLocation") }} <span class="font-bold">{{ currentPath }}</span>
        </div>

        <!-- 警告信息 -->
        <div v-if="pathWarning" class="mb-3 p-2 bg-yellow-50 dark:bg-yellow-900/30 border border-yellow-200 dark:border-yellow-700 rounded-md">
          <div class="flex items-start">
            <svg
              class="h-5 w-5 text-yellow-500 dark:text-yellow-400 mt-0.5 mr-2 flex-shrink-0"
              xmlns="http://www.w3.org/2000/svg"
              fill="none"
              viewBox="0 0 24 24"
              stroke="currentColor"
            >
              <path
                stroke-linecap="round"
                stroke-linejoin="round"
                stroke-width="2"
                d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z"
              />
            </svg>
            <span class="text-sm text-yellow-700 dark:text-yellow-300">{{ pathWarning }}</span>
          </div>
        </div>

        <!-- 目录树 -->
        <div class="border rounded-md overflow-hidden mb-4 h-64" :class="darkMode ? 'border-gray-700' : 'border-gray-300'">
          <!-- 加载状态 -->
          <div v-if="loading" class="h-full flex justify-center items-center" :class="darkMode ? 'text-gray-400' : 'text-gray-500'">
            <svg class="animate-spin h-5 w-5 mr-2" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
              <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
              <path
                class="opacity-75"
                fill="currentColor"
                d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"
              ></path>
            </svg>
            <span>{{ t("mount.copyModal.loading") }}</span>
          </div>

          <div v-else class="h-full overflow-y-auto p-1">
            <!-- 目录树结构 -->
            <div class="file-tree">
              <!-- 根目录 -->
              <div class="tree-item" :class="{ selected: currentPath === userBasicPath }" @click="selectDestination(userBasicPath)">
                <div class="flex items-center py-2 px-2 rounded hover:bg-gray-100 dark:hover:bg-gray-700 cursor-pointer">
                  <svg
                    class="h-4 w-4 flex-shrink-0 mr-2"
                    :class="darkMode ? 'text-blue-400' : 'text-blue-600'"
                    xmlns="http://www.w3.org/2000/svg"
                    fill="none"
                    viewBox="0 0 24 24"
                    stroke-width="2"
                    stroke="currentColor"
                  >
                    <path
                      stroke-linecap="round"
                      stroke-linejoin="round"
                      d="M3 12l2-2m0 0l7-7 7 7M5 10v10a1 1 0 001 1h3m10-11l2 2m-2-2v10a1 1 0 01-1 1h-3m-6 0a1 1 0 001-1v-4a1 1 0 011-1h2a1 1 0 011 1v4a1 1 0 001 1m-6 0h6"
                    />
                  </svg>
                  <span class="truncate" :class="darkMode ? 'text-gray-200' : 'text-gray-700'">{{ rootDisplayName }}</span>
                </div>
              </div>

              <!-- 递归渲染目录树 -->
              <div v-for="item in rootDirectories" :key="item.path" class="folder-item">
                <DirectoryItemVue :item="item" :current-path="currentPath" :dark-mode="darkMode" :fs-api="fsApi" :level="0" @select="selectDestination" />
              </div>
            </div>
          </div>
        </div>

        <!-- 操作按钮 -->
        <div class="flex justify-end space-x-3">
          <button
            @click="closeModal"
            class="px-4 py-2 text-sm font-medium rounded-md shadow-sm focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-gray-300"
            :class="darkMode ? 'bg-gray-700 text-gray-200 hover:bg-gray-600' : 'bg-gray-100 text-gray-700 hover:bg-gray-200'"
          >
            {{ t("mount.copyModal.cancel") }}
          </button>
          <button
            @click="confirmCopy"
            class="px-4 py-2 text-sm font-medium rounded-md shadow-sm focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500"
            :class="[darkMode ? 'bg-blue-600 hover:bg-blue-700 text-white' : 'bg-blue-600 hover:bg-blue-700 text-white', copying ? 'opacity-70 cursor-not-allowed' : '']"
            :disabled="copying"
          >
            {{ copying ? t("mount.copyModal.copying") : t("mount.copyModal.confirmCopy") }}
          </button>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, watch, computed, shallowRef } from "vue";
import { useI18n } from "vue-i18n";
import { api } from "../../../../api";
import { useTaskManager, TaskType, TaskStatus } from "../../../../utils/taskManager";

const { t } = useI18n();

// 注册递归组件
import { h } from "vue";

// 目录缓存对象，用于存储已加载的目录内容
const directoryCache = shallowRef(new Map());

// 创建目录项组件
const DirectoryItemVue = {
  name: "DirectoryItemVue",
  props: {
    item: {
      type: Object,
      required: true,
    },
    currentPath: {
      type: String,
      required: true,
    },
    darkMode: {
      type: Boolean,
      default: false,
    },
    fsApi: {
      type: Object,
      required: true,
    },
    level: {
      type: Number,
      default: 0,
    },
  },
  emits: ["select"],
  setup(props, { emit }) {
    const expanded = ref(false);
    const children = shallowRef([]);
    const loading = ref(false);

    // 监听当前路径变化
    watch(
      () => props.currentPath,
      (newPath) => {
        // 如果当前路径是此节点的子路径，自动展开
        if (newPath.startsWith(props.item.path + "/") && newPath !== props.item.path + "/") {
          expanded.value = true;
          if (children.value.length === 0) {
            loadChildren();
          }
        }
      },
      { immediate: true }
    );

    // 计算是否被选中
    const isSelected = computed(() => {
      return props.currentPath === props.item.path + "/";
    });

    // 切换展开/折叠状态
    const toggleExpand = (event) => {
      event.stopPropagation();

      if (expanded.value) {
        expanded.value = false;
        return;
      }

      expanded.value = true;
      if (children.value.length === 0) {
        loadChildren();
      }
    };

    // 加载子目录
    const loadChildren = async () => {
      // 检查缓存中是否已有此目录的数据
      const cacheKey = props.item.path;
      if (directoryCache.value.has(cacheKey)) {
        children.value = directoryCache.value.get(cacheKey);
        return;
      }

      loading.value = true;
      try {
        const response = await props.fsApi.getDirectoryList(props.item.path);
        if (response.success) {
          // 只显示目录
          const dirItems = response.data.items.filter((item) => item.isDirectory);

          // 更新组件状态
          children.value = dirItems;

          // 更新缓存
          directoryCache.value.set(cacheKey, dirItems);
        } else {
          children.value = [];
        }
      } catch (error) {
        children.value = [];
      } finally {
        loading.value = false;
      }
    };

    // 选择当前目录
    const selectFolder = () => {
      emit("select", props.item.path);
    };

    return {
      expanded,
      children,
      loading,
      isSelected,
      toggleExpand,
      selectFolder,
    };
  },
  render() {
    // 使用渲染函数代替模板，以确保递归组件正确渲染
    return h("div", { class: "directory-item" }, [
      h(
        "div",
        {
          class: ["tree-item", { selected: this.isSelected }],
          onClick: this.selectFolder,
        },
        [
          h(
            "div",
            {
              class: "flex items-center py-2 px-2 rounded hover:bg-gray-100 dark:hover:bg-gray-700 cursor-pointer",
              style: { paddingLeft: `${this.level * 0.75 + 0.5}rem` }, // 增加层级缩进值
            },
            [
              h(
                "div",
                {
                  class: "folder-toggle",
                  onClick: (e) => {
                    e.stopPropagation();
                    this.toggleExpand(e);
                  },
                },
                [
                  this.expanded
                    ? h(
                        "svg",
                        {
                          class: "h-4 w-4",
                          xmlns: "http://www.w3.org/2000/svg",
                          fill: "none",
                          viewBox: "0 0 24 24",
                          stroke: "currentColor",
                          "stroke-width": "2",
                        },
                        [
                          h("path", {
                            "stroke-linecap": "round",
                            "stroke-linejoin": "round",
                            d: "M19 9l-7 7-7-7",
                          }),
                        ]
                      )
                    : h(
                        "svg",
                        {
                          class: "h-4 w-4",
                          xmlns: "http://www.w3.org/2000/svg",
                          fill: "none",
                          viewBox: "0 0 24 24",
                          stroke: "currentColor",
                          "stroke-width": "2",
                        },
                        [
                          h("path", {
                            "stroke-linecap": "round",
                            "stroke-linejoin": "round",
                            d: "M9 5l7 7-7 7",
                          }),
                        ]
                      ),
                ]
              ),
              h(
                "svg",
                {
                  class: ["h-4 w-4 flex-shrink-0 mr-2", this.darkMode ? "text-yellow-400" : "text-yellow-600"],
                  xmlns: "http://www.w3.org/2000/svg",
                  fill: "none",
                  viewBox: "0 0 24 24",
                  stroke: "currentColor",
                  "stroke-width": "2",
                },
                [
                  h("path", {
                    "stroke-linecap": "round",
                    "stroke-linejoin": "round",
                    d: "M3 7v10a2 2 0 002 2h14a2 2 0 002-2V9a2 2 0 00-2-2h-6l-2-2H5a2 2 0 00-2 2z",
                  }),
                ]
              ),
              h(
                "span",
                {
                  class: ["truncate", this.darkMode ? "text-gray-200" : "text-gray-700"],
                },
                this.item.name
              ),
            ]
          ),
        ]
      ),
      this.expanded
        ? h("div", { class: "folder-children" }, [
            this.loading
              ? h(
                  "div",
                  {
                    class: "folder-loading",
                    style: { paddingLeft: `${(this.level + 1) * 0.75 + 0.75}rem` },
                  },
                  [
                    h(
                      "svg",
                      {
                        class: "animate-spin h-3 w-3 mr-1",
                        xmlns: "http://www.w3.org/2000/svg",
                        fill: "none",
                        viewBox: "0 0 24 24",
                      },
                      [
                        h("circle", {
                          class: "opacity-25",
                          cx: "12",
                          cy: "12",
                          r: "10",
                          stroke: "currentColor",
                          "stroke-width": "4",
                        }),
                        h("path", {
                          class: "opacity-75",
                          fill: "currentColor",
                          d: "M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z",
                        }),
                      ]
                    ),
                    h("span", { class: "text-xs" }, t("mount.copyModal.loading")),
                  ]
                )
              : this.children.length === 0
              ? null // 如果没有子目录，不显示任何内容
              : this.children.map((child) =>
                  h("div", { class: "folder-item", key: child.path }, [
                    h(DirectoryItemVue, {
                      item: child,
                      currentPath: this.currentPath,
                      darkMode: this.darkMode,
                      fsApi: this.fsApi,
                      level: this.level + 1, // 增加层级深度
                      onSelect: (path) => this.$emit("select", path),
                    }),
                  ])
                ),
          ])
        : null,
    ]);
  },
};

const props = defineProps({
  isOpen: {
    type: Boolean,
    default: false,
  },
  darkMode: {
    type: Boolean,
    default: false,
  },
  selectedItems: {
    type: Array,
    default: () => [],
  },
  sourcePath: {
    type: String,
    default: "/",
  },
  isAdmin: {
    type: Boolean,
    default: true,
  },
  apiKeyInfo: {
    type: Object,
    default: null,
  },
});

const emit = defineEmits(["close", "copy-started", "copy-complete"]);

// 使用统一的文件系统API
const fsApi = api.fs;

// 计算用户的基本路径
const userBasicPath = computed(() => {
  if (props.isAdmin) {
    return "/";
  }
  return props.apiKeyInfo?.basic_path || "/";
});

// 计算根路径显示名称
const rootDisplayName = computed(() => {
  if (props.isAdmin) {
    return t("mount.copyModal.rootDirectory");
  }
  // 对于API密钥用户，显示基本路径的最后一段作为根目录名称
  const basicPath = userBasicPath.value;
  if (basicPath === "/") {
    return t("mount.copyModal.rootDirectory");
  }
  const pathParts = basicPath.split("/").filter((part) => part);
  return pathParts.length > 0 ? pathParts[pathParts.length - 1] : t("mount.copyModal.rootDirectory");
});

// 状态变量
const currentPath = ref("/");
const rootDirectories = shallowRef([]);
const loading = ref(false);
const copying = ref(false);
const pathWarning = ref(""); // 添加路径警告状态

// 清除目录缓存的函数，在模态窗口关闭时调用
const clearDirectoryCache = () => {
  directoryCache.value.clear();
};

// 监听模态窗口打开状态
watch(
  () => props.isOpen,
  (newValue) => {
    if (newValue) {
      // 当模态窗口打开时，设置初始路径为用户的基本路径
      currentPath.value = userBasicPath.value;
      loadRootDirectories();
    } else {
      // 当模态窗口关闭时，清除目录缓存
      clearDirectoryCache();
    }
  }
);

// 关闭模态窗口
const closeModal = () => {
  if (copying.value) return; // 如果正在复制，不允许关闭
  emit("close");
};

// 加载根目录内容
const loadRootDirectories = async () => {
  const rootPath = userBasicPath.value;

  // 检查缓存中是否已有根目录数据
  const cacheKey = rootPath;
  if (directoryCache.value.has(cacheKey)) {
    rootDirectories.value = directoryCache.value.get(cacheKey);
    return;
  }

  loading.value = true;
  try {
    const response = await fsApi.getDirectoryList(rootPath);
    if (response.success) {
      // 只显示目录
      const dirItems = response.data.items.filter((item) => item.isDirectory);

      // 更新组件状态
      rootDirectories.value = dirItems;

      // 更新缓存
      directoryCache.value.set(cacheKey, dirItems);
    } else {
      rootDirectories.value = [];
    }
  } catch (error) {
    rootDirectories.value = [];
  } finally {
    loading.value = false;
  }
};

// 选择目标位置
const selectDestination = (path) => {
  // 确保路径格式正确
  let formattedPath = path;
  if (!formattedPath.endsWith("/")) {
    formattedPath = formattedPath + "/";
  }

  // 对于API密钥用户，确保选择的路径在基本路径范围内
  if (!props.isAdmin) {
    const basicPath = userBasicPath.value;
    const normalizedBasicPath = basicPath === "/" ? "/" : basicPath.replace(/\/+$/, "");
    const normalizedSelectedPath = formattedPath.replace(/\/+$/, "") || "/";

    // 特殊处理：如果基本路径是根目录，允许选择任何路径
    if (normalizedBasicPath !== "/") {
      // 检查选择的路径是否在基本路径范围内
      if (normalizedSelectedPath !== normalizedBasicPath && !normalizedSelectedPath.startsWith(normalizedBasicPath + "/")) {
        // 如果选择的路径不在基本路径范围内，重置为基本路径
        formattedPath = basicPath.endsWith("/") ? basicPath : basicPath + "/";
      }
    }
  }

  currentPath.value = formattedPath;

  // 清除之前的警告
  pathWarning.value = "";

  // 验证路径
  validateDestinationPath();
};

// 验证目标路径
const validateDestinationPath = () => {
  // 检查是否有选择的项目
  if (!props.selectedItems || props.selectedItems.length === 0) return;

  // 检查是否存在将文件夹复制到其自身或其子目录的情况
  for (const item of props.selectedItems) {
    if (item.isDirectory) {
      const sourcePath = item.path.endsWith("/") ? item.path : item.path + "/";

      // 检查目标路径是否是源路径的子目录
      if (currentPath.value.startsWith(sourcePath)) {
        pathWarning.value = t("mount.copyModal.warnings.recursiveCopy");
        return;
      }

      // 检查是否将文件夹复制到其自身
      if (currentPath.value === sourcePath) {
        pathWarning.value = t("mount.copyModal.warnings.selfCopy");
        return;
      }
    }
  }
};

// 准备复制项目
const prepareCopyItems = () => {
  return props.selectedItems.map((item) => ({
    sourcePath: item.path,
    targetPath: `${currentPath.value}${item.name}`,
  }));
};

// 创建复制任务
const createCopyTask = (itemCount) => {
  const taskManager = useTaskManager();
  const taskId = taskManager.addTask(TaskType.COPY, t("mount.taskManager.copyTaskName", { count: itemCount, path: currentPath.value }), itemCount);

  // 更新任务状态为运行中
  taskManager.updateTaskProgress(taskId, 0, {
    total: itemCount,
    processed: 0,
  });

  return { taskManager, taskId };
};

// 创建进度回调函数
const createProgressCallback = (taskManager, taskId, itemCount) => {
  return (phase, progress, details = {}) => {
    // 更新任务进度
    // phase可能是"downloading"或"uploading"或普通数值
    let progressValue = progress;

    // 如果是跨S3复制，phase会提供阶段信息
    if (typeof phase === "string") {
      // 根据阶段调整进度显示
      if (phase === "downloading") {
        // 下载阶段占总进度的40%
        progressValue = progress * 0.4;
      } else if (phase === "uploading") {
        // 上传阶段占总进度的40%，加上之前的40%
        progressValue = 40 + progress * 0.4;
      }
    }

    // 确保进度在0-100之间
    progressValue = Math.min(Math.max(progressValue, 0), 100);

    // 准备任务详情
    const taskDetails = {
      total: itemCount,
      processed: Math.ceil((itemCount * progressValue) / 100),
      phase: phase,
    };

    // 如果有details参数，合并到taskDetails中
    if (details && typeof details === "object") {
      // 选择性合并，保留重要的任务信息
      if (details.currentFile) taskDetails.currentFile = details.currentFile;
      if (details.currentFileProgress) taskDetails.currentFileProgress = details.currentFileProgress;

      // 确保processed和total值在合理范围内
      if (details.processedFiles !== undefined && details.totalFiles !== undefined) {
        // 优先使用文件计数而非字节计数
        taskDetails.processed = details.processedFiles;
        taskDetails.total = details.totalFiles;

        // 动态更新任务的总数，确保进度显示一致
        const currentTask = taskManager.getTasks().find((t) => t.id === taskId);
        if (currentTask && currentTask.total !== details.totalFiles) {
          // 更新任务的总数为实际文件数量
          currentTask.total = details.totalFiles;
        }
      }

      // 如果details中包含percentage字段，优先使用它作为进度值
      if (details.percentage !== undefined) {
        progressValue = details.percentage;
      }
    }

    taskManager.updateTaskProgress(taskId, progressValue, taskDetails);
  };
};

// 创建取消检查函数
const createCancelCallback = (taskManager, taskId) => {
  return () => {
    const task = taskManager.getTasks().find((t) => t.id === taskId);
    if (task && task.status === TaskStatus.CANCELLED) {
      // 检测到取消状态后，立即主动中止所有进行中的操作
      return true;
    }
    return false;
  };
};

// 处理复制完成
const handleCopyCompletion = (taskManager, taskId, response) => {
  // 检查响应数据
  const data = response.data || {};

  const successCount = Array.isArray(data.success) ? data.success.length : data.success || 0;
  const failedCount = Array.isArray(data.failed) ? data.failed.length : data.failed || 0;
  const skippedCount = data.skipped || 0;
  const totalProcessed = successCount + failedCount + skippedCount;

  // 根据操作类型生成统一的消息
  let operationMessage;
  if (data.crossStorage === true || data.requiresClientSideCopy === true) {
    operationMessage = "跨存储复制完成";
  } else {
    operationMessage = "同存储复制完成";
  }

  // 准备任务详情
  const taskDetails = {
    total: totalProcessed, // 使用实际处理的文件数量
    processed: totalProcessed,
    successCount,
    failedCount,
    skippedCount,
    message: operationMessage,
  };

  // 如果是跨S3复制，添加相关信息
  if (data.requiresClientSideCopy) {
    taskDetails.crossStorage = true;
  }

  // 根据结果状态决定任务状态
  if (response.success) {
    // 完全成功 - 没有失败项
    taskManager.completeTask(taskId, taskDetails);
  } else if (successCount > 0) {
    // 部分成功 - 有成功的项目，但也有失败的
    taskManager.completeTask(taskId, {
      ...taskDetails,
      partialSuccess: true,
      status: t("mount.taskManager.partialComplete"),
    });
  } else {
    // 完全失败 - 没有成功的项目
    taskManager.failTask(taskId, response.message, taskDetails);
  }

  // 在复制完成后发出事件，触发父组件刷新目录
  // 注意：模态框已经在任务创建时关闭，这里只需要触发目录刷新
  emit("copy-complete", {
    success: response.success,
    message: response.success ? operationMessage : response.message,
    targetPath: currentPath.value,
    taskId: taskId,
    showTaskManager: true,
    successCount,
    failedCount,
    skippedCount,
    modalAlreadyClosed: true, // 标记模态框已经关闭
  });
};

// 处理复制错误
const handleCopyError = (error) => {
  // 获取任务管理器并更新任务状态为失败
  try {
    const taskManager = useTaskManager();
    const tasks = taskManager.getTasks().filter((t) => t.type === TaskType.COPY && t.status !== TaskStatus.COMPLETED && t.status !== TaskStatus.FAILED);

    if (tasks.length > 0) {
      // 找到最近的复制任务并标记为失败
      const latestTask = tasks.sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt))[0];

      taskManager.failTask(latestTask.id, error.message || t("mount.messages.copyFailed", { message: t("common.unknown") }));
    }
  } catch (e) {
    console.error("Update task status failed:", e);
  }
};

// 异步执行复制任务
const executeCopyTask = async (taskManager, taskId, copyItems) => {
  try {
    // 创建回调函数
    const onProgress = createProgressCallback(taskManager, taskId, props.selectedItems.length);
    const onCancel = createCancelCallback(taskManager, taskId);

    // 调用批量复制API
    const response = await fsApi.batchCopyItems(copyItems, false, {
      onProgress,
      onCancel,
    });

    // 处理复制完成
    handleCopyCompletion(taskManager, taskId, response);
  } catch (error) {
    handleCopyError(error);
  }
};

// 确认复制
const confirmCopy = async () => {
  if (copying.value) return;

  // 先验证路径
  validateDestinationPath();

  // 如果有警告，需要用户确认
  if (pathWarning.value) {
    if (!confirm(t("mount.copyModal.confirmPotentialIssue", { warning: pathWarning.value }))) {
      return;
    }
  }

  copying.value = true;

  try {
    // 准备复制项目
    const copyItems = prepareCopyItems();

    // 创建任务
    const { taskManager, taskId } = createCopyTask(props.selectedItems.length);

    // 任务创建成功后立即发出复制开始事件
    emit("copy-started", {
      message: t("mount.taskManager.copyStarted", {
        count: props.selectedItems.length,
        path: currentPath.value,
      }),
      taskId: taskId,
      itemCount: props.selectedItems.length,
      targetPath: currentPath.value,
    });

    // 任务创建成功后立即关闭模态框，让任务在后台执行
    emit("close");

    // 重置 copying 状态，因为模态框已关闭
    copying.value = false;

    // 异步执行复制任务，不阻塞模态框关闭
    executeCopyTask(taskManager, taskId, copyItems);
  } catch (error) {
    // 如果任务创建失败，保持模态框打开并显示错误
    handleCopyError(error);
    copying.value = false;
  }
};
</script>

<style scoped>
.file-tree {
  @apply text-sm;
}

.tree-item {
  @apply mb-1;
}

.tree-item.selected > div {
  @apply bg-blue-50 dark:bg-blue-900/30 border-l-4 border-blue-500 dark:border-blue-400;
}

.tree-item.selected > div span {
  @apply text-blue-700 dark:text-blue-300 font-medium;
}

/* 目录项组件样式 */
.directory-item {
  @apply w-full;
}

/* 文件夹组件样式 */
.folder-item {
  @apply relative;
}

/* 添加层级连接线 */
.folder-item::before {
  content: "";
  @apply absolute border-l border-gray-300 dark:border-gray-600;
  height: calc(100% - 1.5rem);
  left: 1rem;
  top: 1.5rem;
}

/* 最后一个子项不需要显示连接线到底部 */
.folder-item:last-child::before {
  height: 0;
}

.folder-toggle {
  @apply w-5 h-5 flex items-center justify-center flex-shrink-0 mr-1 text-gray-500 dark:text-gray-400;
}

.folder-icon {
  @apply h-4 w-4 flex-shrink-0 mr-2;
}

.folder-children {
  @apply mt-1 relative;
}

.folder-loading {
  @apply py-1 text-xs text-gray-500 dark:text-gray-400 flex items-center;
}
</style>
