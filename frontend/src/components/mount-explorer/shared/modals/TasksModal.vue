<template>
  <div v-if="isOpen" class="fixed inset-0 z-[60] overflow-auto bg-black bg-opacity-50 flex items-center justify-center p-2 sm:p-4 pt-20 sm:pt-4">
    <div class="relative w-full max-w-sm sm:max-w-md bg-white dark:bg-gray-800 rounded-lg shadow-xl max-h-[85vh] sm:max-h-[80vh] overflow-hidden">
      <!-- 标题栏 -->
      <div class="px-4 py-3 border-b flex justify-between items-center" :class="darkMode ? 'border-gray-700' : 'border-gray-200'">
        <h3 class="text-lg font-medium" :class="darkMode ? 'text-gray-100' : 'text-gray-900'">{{ t("mount.taskManager.title") }}</h3>
        <button @click="close" class="text-gray-400 hover:text-gray-500 dark:text-gray-500 dark:hover:text-gray-400">
          <svg class="h-5 w-5" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
          </svg>
        </button>
      </div>

      <!-- 内容区 -->
      <div class="p-3 sm:p-4 overflow-y-auto" style="max-height: calc(85vh - 140px)">
        <div v-if="activeTasks.length === 0 && completedTasks.length === 0" class="text-center py-6" :class="darkMode ? 'text-gray-400' : 'text-gray-600'">
          <svg class="h-12 w-12 mx-auto mb-3 opacity-30" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path
              stroke-linecap="round"
              stroke-linejoin="round"
              stroke-width="2"
              d="M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2"
            />
          </svg>
          <p>{{ t("mount.taskManager.noTasksDescription") }}</p>
        </div>

        <div v-else class="space-y-4 max-h-80 overflow-y-auto">
          <!-- 活动任务标题 -->
          <div v-if="activeTasks.length > 0" class="text-sm font-medium mb-2" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">
            {{ t("mount.taskManager.activeTasks", { count: activeTasks.length }) }}
          </div>

          <!-- 活动任务列表 -->
          <div v-for="task in activeTasks" :key="task.id" class="task-item border rounded-lg p-3" :class="getTaskCardClass(task)">
            <!-- 任务标题和类型 -->
            <div class="flex justify-between items-start mb-2">
              <div class="flex items-start">
                <!-- 任务类型图标 -->
                <div class="mr-2 mt-0.5 flex-shrink-0" :class="darkMode ? 'text-gray-400' : 'text-gray-600'" v-html="getTaskTypeIcon(task.type)"></div>

                <div>
                  <div class="font-medium" :class="darkMode ? 'text-gray-200' : 'text-gray-800'">{{ task.name }}</div>
                  <div class="text-xs mt-0.5" :class="darkMode ? 'text-gray-400' : 'text-gray-500'">{{ getTaskTypeText(task.type) }} · {{ getTimeAgo(task.createdAt) }}</div>
                </div>
              </div>
              <div class="flex items-center">
                <!-- 任务状态标签 -->
                <span class="px-2 py-0.5 rounded-full text-xs font-medium" :class="getStatusClass(task.status, task)">
                  {{ getStatusText(task.status, task) }}
                </span>

                <!-- 任务操作按钮 -->
                <button
                  v-if="task.status === TaskStatus.RUNNING || task.status === TaskStatus.PENDING"
                  @click="cancelTask(task.id)"
                  class="ml-2 text-gray-400 hover:text-red-500 dark:text-gray-500 dark:hover:text-red-400"
                >
                  <svg class="h-4 w-4" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
                  </svg>
                </button>
              </div>
            </div>

            <!-- 进度条 -->
            <div v-if="task.status === TaskStatus.RUNNING || task.status === TaskStatus.PENDING" class="mt-2">
              <div class="flex justify-between items-center mb-1">
                <div class="text-xs" :class="darkMode ? 'text-gray-400' : 'text-gray-500'">
                  <span v-if="task.details && task.details.phase === 'downloading'" class="text-blue-500 dark:text-blue-400">{{ t("mount.taskManager.downloading") }}</span>
                  <span v-else-if="task.details && task.details.phase === 'uploading'" class="text-green-500 dark:text-green-400">{{ t("mount.taskManager.uploading") }}</span>
                  <span v-else>{{ t("mount.taskManager.processing") }}</span>
                </div>
                <div class="text-xs font-medium" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">{{ Math.round(task.progress) }}%</div>
              </div>
              <div class="w-full bg-gray-200 rounded-full h-2 dark:bg-gray-700">
                <div class="h-2 rounded-full transition-width duration-300 ease-out" :class="getProgressBarClass(task)" :style="{ width: `${task.progress}%` }"></div>
              </div>
            </div>

            <!-- 任务详情 -->
            <div v-if="task.details && Object.keys(task.details).length > 0" class="mt-2 text-xs" :class="darkMode ? 'text-gray-400' : 'text-gray-500'">
              <div v-if="task.details.processed && task.details.total">{{ t("mount.taskManager.processed", { current: task.details.processed, total: task.details.total }) }}</div>

              <!-- 并行下载详情显示 -->
              <div v-if="task.details.showParallelDetails && task.details.parallelFiles" class="mt-1">
                <!-- 总体信息和展开按钮 -->
                <div class="flex justify-between items-center cursor-pointer" @click="toggleTaskDetails(task.id)">
                  <span class="truncate">{{ t("mount.taskManager.currentFile", { fileName: task.details.currentFile || t("mount.taskManager.fileStatus.preparing") }) }}</span>
                  <div class="flex items-center space-x-2">
                    <span class="text-xs">{{ task.details.processed }}/{{ task.details.total }} {{ t("mount.taskManager.filesCount") }}</span>
                    <!-- 现代化展开/收起箭头 -->
                    <svg
                      class="w-4 h-4 transition-transform duration-200 text-slate-500"
                      :class="{ 'rotate-180': isTaskExpanded(task.id) }"
                      fill="none"
                      stroke="currentColor"
                      viewBox="0 0 24 24"
                      stroke-width="2"
                    >
                      <path stroke-linecap="round" stroke-linejoin="round" d="m19.5 8.25-7.5 7.5-7.5-7.5" />
                    </svg>
                  </div>
                </div>

                <!-- 可折叠的文件详情列表 -->
                <div v-if="isTaskExpanded(task.id)" class="mt-2 space-y-1 max-h-40 overflow-y-auto">
                  <div
                    v-for="file in task.details.parallelFiles"
                    :key="file.path"
                    class="flex items-center justify-between text-xs p-2 rounded"
                    :class="darkMode ? 'bg-gray-800' : 'bg-gray-50'"
                  >
                    <div class="flex items-center space-x-2 flex-1 min-w-0">
                      <!-- 文件状态图标 (现代Heroicons风格) -->
                      <div class="flex-shrink-0">
                        <!-- 已完成 - 绿色勾选圆圈 -->
                        <svg v-if="file.status === 'completed'" class="w-4 h-4 text-emerald-500" fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2">
                          <path stroke-linecap="round" stroke-linejoin="round" d="M9 12.75L11.25 15 15 9.75M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
                        </svg>
                        <!-- 失败 - 红色X圆圈 -->
                        <svg v-else-if="file.status === 'failed'" class="w-4 h-4 text-red-500" fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2">
                          <path stroke-linecap="round" stroke-linejoin="round" d="m9.75 9.75 4.5 4.5m0-4.5-4.5 4.5M21 12a9 9 0 1 1-18 0 9 9 0 0 1 18 0Z" />
                        </svg>
                        <!-- 下载中 - 蓝色下载箭头动画 -->
                        <svg
                          v-else-if="file.status === 'downloading'"
                          class="w-4 h-4 text-blue-500 animate-pulse"
                          fill="none"
                          stroke="currentColor"
                          viewBox="0 0 24 24"
                          stroke-width="2"
                        >
                          <path
                            stroke-linecap="round"
                            stroke-linejoin="round"
                            d="M3 16.5v2.25A2.25 2.25 0 0 0 5.25 21h13.5A2.25 2.25 0 0 0 21 18.75V16.5M16.5 12 12 16.5m0 0L7.5 12m4.5 4.5V3"
                          />
                        </svg>
                        <!-- 等待中 - 灰色时钟 -->
                        <svg v-else class="w-4 h-4 text-slate-400" fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2">
                          <path stroke-linecap="round" stroke-linejoin="round" d="M12 6v6h4.5m4.5 0a9 9 0 1 1-18 0 9 9 0 0 1 18 0Z" />
                        </svg>
                      </div>

                      <!-- 文件名 -->
                      <span class="truncate font-medium">{{ file.name }}</span>
                    </div>

                    <!-- 文件进度信息 (现代化颜色) -->
                    <div class="flex items-center space-x-2 flex-shrink-0">
                      <span v-if="file.status === 'downloading'" class="text-blue-600 font-medium">{{ file.progress }}%</span>
                      <span v-else-if="file.status === 'completed'" class="text-emerald-600 font-medium">100%</span>
                      <span v-else-if="file.status === 'failed'" class="text-red-600 font-medium">{{ t("mount.taskManager.fileStatus.failed") }}</span>
                      <span v-else class="text-slate-500">{{ t("mount.taskManager.fileStatus.pending") }}</span>

                      <span v-if="file.totalBytes > 0" class="text-slate-600 dark:text-slate-400 text-xs">
                        {{ formatBytes(file.receivedBytes) }}/{{ formatBytes(file.totalBytes) }}
                      </span>
                    </div>
                  </div>
                </div>
              </div>

              <!-- 单文件下载进度显示（非并行模式） -->
              <div v-else-if="task.details.currentFile" class="mt-1">
                <div class="truncate">{{ t("mount.taskManager.currentFile", { fileName: task.details.currentFile }) }}</div>

                <!-- 单文件进度条 -->
                <div v-if="task.details.fileProgress !== undefined && task.details.receivedBytes !== undefined" class="mt-1 text-xs">
                  <div class="flex justify-between items-center">
                    <span>{{ t("mount.taskManager.fileProgress") }}: {{ task.details.fileProgress }}%</span>
                    <div class="flex items-center">
                      <span v-if="task.details.totalBytes">{{ formatBytes(task.details.receivedBytes) }} / {{ formatBytes(task.details.totalBytes) }}</span>
                      <span v-else>{{ formatBytes(task.details.receivedBytes) }}</span>
                    </div>
                  </div>
                  <!-- 文件级别的进度条 -->
                  <div class="w-full bg-gray-200 rounded-full h-1 mt-1 dark:bg-gray-600">
                    <div class="h-1 rounded-full bg-blue-400 transition-width duration-300 ease-out" :style="{ width: `${task.details.fileProgress}%` }"></div>
                  </div>
                </div>
              </div>

              <!-- 复制操作统计信息 - 活动任务也可能有统计 -->
              <div v-if="task.details.successCount !== undefined || task.details.failedCount !== undefined || task.details.skippedCount !== undefined" class="mt-1 space-x-2">
                <span v-if="task.details.successCount !== undefined" class="text-green-500 dark:text-green-400">{{
                  t("mount.taskManager.success", { count: task.details.successCount })
                }}</span>
                <span v-if="task.details.skippedCount !== undefined" class="text-orange-500 dark:text-orange-400">{{
                  t("mount.taskManager.skipped", { count: task.details.skippedCount })
                }}</span>
                <span v-if="task.details.failedCount !== undefined" class="text-red-500 dark:text-red-400">{{
                  t("mount.taskManager.failed", { count: task.details.failedCount })
                }}</span>
              </div>
            </div>

            <!-- 错误信息 -->
            <div v-if="task.status === TaskStatus.FAILED && task.error" class="mt-2 text-xs text-red-500 dark:text-red-400">
              {{ task.error }}
            </div>
          </div>

          <!-- 已完成任务标题 -->
          <div v-if="completedTasks.length > 0" class="text-sm font-medium mt-4 mb-2" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">
            {{ t("mount.taskManager.completedTasks", { count: completedTasks.length }) }}
          </div>

          <!-- 已完成任务列表 -->
          <div v-for="task in completedTasks" :key="task.id" class="border rounded-lg p-3" :class="getTaskCardClass(task)">
            <!-- 任务标题和类型 -->
            <div class="flex justify-between items-start">
              <div class="flex items-start">
                <!-- 任务类型图标 -->
                <div class="mr-2 mt-0.5 flex-shrink-0" :class="darkMode ? 'text-gray-400' : 'text-gray-600'" v-html="getTaskTypeIcon(task.type)"></div>

                <div>
                  <div class="font-medium" :class="darkMode ? 'text-gray-200' : 'text-gray-800'">{{ task.name }}</div>
                  <div class="text-xs mt-0.5" :class="darkMode ? 'text-gray-400' : 'text-gray-500'">{{ getTaskTypeText(task.type) }} · {{ getTimeAgo(task.updatedAt) }}</div>
                </div>
              </div>
              <div class="flex items-center">
                <!-- 任务状态标签 -->
                <span class="px-2 py-0.5 rounded-full text-xs font-medium" :class="getStatusClass(task.status, task)">
                  {{ getStatusText(task.status, task) }}
                </span>

                <!-- 展开/折叠按钮 -->
                <button @click="toggleTaskDetails(task.id)" class="ml-2 text-gray-400 hover:text-gray-600 dark:text-gray-500 dark:hover:text-gray-300">
                  <svg
                    class="h-4 w-4 transition-transform"
                    :class="{ 'transform rotate-180': isTaskExpanded(task.id) }"
                    xmlns="http://www.w3.org/2000/svg"
                    fill="none"
                    viewBox="0 0 24 24"
                    stroke="currentColor"
                  >
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7" />
                  </svg>
                </button>
              </div>
            </div>

            <!-- 任务详情 - 仅在展开时显示 -->
            <div v-if="isTaskExpanded(task.id)" class="mt-2 pt-2 border-t" :class="darkMode ? 'border-gray-700' : 'border-gray-200'">
              <!-- 完成时间 -->
              <div class="text-xs" :class="darkMode ? 'text-gray-400' : 'text-gray-500'">{{ t("mount.taskManager.completedAt", { time: formatDateTime(task.updatedAt) }) }}</div>

              <!-- 任务详情 -->
              <div v-if="task.details && Object.keys(task.details).length > 0" class="mt-1 text-xs" :class="darkMode ? 'text-gray-400' : 'text-gray-500'">
                <div v-if="task.details.processed && task.details.total">
                  {{ t("mount.taskManager.processedItems", { current: task.details.processed, total: task.details.total }) }}
                </div>

                <!-- 复制操作统计信息 -->
                <div
                  v-if="
                    task.status === TaskStatus.COMPLETED &&
                    (task.details.successCount !== undefined || task.details.failedCount !== undefined || task.details.skippedCount !== undefined)
                  "
                  class="mt-1 space-x-2"
                >
                  <span v-if="task.details.successCount !== undefined" class="text-green-500 dark:text-green-400">{{
                    t("mount.taskManager.success", { count: task.details.successCount })
                  }}</span>
                  <span v-if="task.details.skippedCount !== undefined" class="text-orange-500 dark:text-orange-400">{{
                    t("mount.taskManager.skipped", { count: task.details.skippedCount })
                  }}</span>
                  <span v-if="task.details.failedCount !== undefined" class="text-red-500 dark:text-red-400">{{
                    t("mount.taskManager.failed", { count: task.details.failedCount })
                  }}</span>
                </div>

                <!-- 部分成功状态提示 -->
                <div v-if="task.details.partialSuccess" class="mt-1">
                  <span class="text-orange-500 dark:text-orange-400 text-xs">{{ task.details.status || t("mount.taskManager.partialComplete") }}</span>
                </div>

                <!-- 任务消息 -->
                <div
                  v-if="task.details.message"
                  class="mt-1 text-xs"
                  :class="task.details.partialSuccess ? 'text-orange-600 dark:text-orange-400' : darkMode ? 'text-gray-400' : 'text-gray-500'"
                >
                  {{ task.details.message }}
                </div>
              </div>

              <!-- 错误信息 -->
              <div v-if="task.status === TaskStatus.FAILED && task.error" class="mt-1 text-xs text-red-500 dark:text-red-400">
                {{ t("mount.taskManager.error", { message: task.error }) }}
              </div>
            </div>

            <!-- 错误信息 - 简短版本，仅在未展开且有错误时显示 -->
            <div v-else-if="task.status === TaskStatus.FAILED && task.error" class="mt-2 text-xs text-red-500 dark:text-red-400 truncate">
              {{ task.error }}
            </div>
          </div>
        </div>

        <!-- 清除已完成按钮 -->
        <div v-if="completedTasks.length > 0" class="mt-4 flex justify-end space-x-2">
          <button
            @click="clearCompleted"
            class="text-xs px-3 py-1 rounded transition-colors"
            :class="darkMode ? 'text-gray-400 hover:bg-gray-700' : 'text-gray-600 hover:bg-gray-100'"
          >
            {{ t("mount.taskManager.clearCompleted") }}
          </button>
        </div>
      </div>
    </div>
  </div>
</template>

<style scoped>
/* 优化进度条动画性能，减少跳动 */
.transition-width {
  transition-property: width;
  will-change: width;
  transform: translateZ(0); /* 启用硬件加速 */
}

/* 减少文本更新时的闪烁 */
.text-xs {
  will-change: auto;
}

/* 优化任务列表的渲染性能 */
.task-item {
  contain: layout style paint;
}
</style>

<script setup>
import { computed, ref, shallowRef, onUnmounted } from "vue";
import { useI18n } from "vue-i18n";
import { useTaskManager, TaskStatus, TaskType } from "../../../../utils/taskManager";

const { t } = useI18n();

const props = defineProps({
  isOpen: {
    type: Boolean,
    default: false,
  },
  darkMode: {
    type: Boolean,
    default: false,
  },
});

const emit = defineEmits(["close"]);

// 获取任务管理器
const { state, cancelTask, clearCompletedTasks } = useTaskManager();

// 活动中的任务 - 使用shallowRef优化性能
const activeTasks = computed(() => {
  return [...state.tasks].filter((task) => task.status === TaskStatus.RUNNING || task.status === TaskStatus.PENDING).sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt));
});

// 已完成的任务 - 使用shallowRef优化性能
const completedTasks = computed(() => {
  return [...state.tasks]
    .filter((task) => task.status === TaskStatus.COMPLETED || task.status === TaskStatus.FAILED || task.status === TaskStatus.CANCELLED)
    .sort((a, b) => new Date(b.updatedAt) - new Date(a.updatedAt));
});

// 展开的任务ID集合
const expandedTaskIds = ref(new Set());

// 切换任务详情展开状态
const toggleTaskDetails = (taskId) => {
  if (expandedTaskIds.value.has(taskId)) {
    expandedTaskIds.value.delete(taskId);
  } else {
    expandedTaskIds.value.add(taskId);
  }
};

// 检查任务是否已展开
const isTaskExpanded = (taskId) => {
  return expandedTaskIds.value.has(taskId);
};

// 清除已完成任务
const clearCompleted = () => {
  clearCompletedTasks();
};

// 关闭弹窗
const close = () => {
  emit("close");
};

// 获取任务类型文本
const getTaskTypeText = (type) => {
  switch (type) {
    case TaskType.COPY:
      return t("mount.taskManager.copyTask");
    case TaskType.UPLOAD:
      return t("mount.taskManager.uploadTask");
    case TaskType.DELETE:
      return t("mount.taskManager.deleteTask");
    case TaskType.DOWNLOAD:
      return t("mount.taskManager.downloadTask");
    default:
      return t("mount.taskManager.unknownTask");
  }
};

// 获取任务类型图标
const getTaskTypeIcon = (type) => {
  switch (type) {
    case TaskType.COPY:
      return `<svg class="h-4 w-4" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8 16H6a2 2 0 01-2-2V6a2 2 0 012-2h8a2 2 0 012 2v2m-6 12h8a2 2 0 002-2v-8a2 2 0 00-2-2h-8a2 2 0 00-2 2v8a2 2 0 002 2z" />
              </svg>`;
    case TaskType.UPLOAD:
      return `<svg class="h-4 w-4" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-8l-4-4m0 0L8 8m4-4v12" />
              </svg>`;
    case TaskType.DELETE:
      return `<svg class="h-4 w-4" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16" />
              </svg>`;
    case TaskType.DOWNLOAD:
      return `<svg class="h-4 w-4" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-4l-4 4m0 0l-4-4m4 4V4" />
              </svg>`;
    default:
      return `<svg class="h-4 w-4" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2" />
              </svg>`;
  }
};

// 获取状态文本
const getStatusText = (status, task = null) => {
  // 检查是否为部分成功的任务
  if (status === TaskStatus.COMPLETED && task && task.details && task.details.partialSuccess) {
    return t("mount.taskManager.partialComplete");
  }

  switch (status) {
    case TaskStatus.PENDING:
      return t("mount.taskManager.status.pending");
    case TaskStatus.RUNNING:
      return t("mount.taskManager.status.running");
    case TaskStatus.COMPLETED:
      return t("mount.taskManager.status.completed");
    case TaskStatus.FAILED:
      return t("mount.taskManager.status.failed");
    case TaskStatus.CANCELLED:
      return t("mount.taskManager.status.cancelled");
    default:
      return t("mount.taskManager.unknown");
  }
};

// 状态样式类映射 - 缓存样式类以提高性能
const statusClassMap = {
  [TaskStatus.PENDING]: {
    light: "bg-yellow-100 text-yellow-800",
    dark: "text-white bg-yellow-700/50",
  },
  [TaskStatus.RUNNING]: {
    light: "bg-blue-100 text-blue-800",
    dark: "text-white bg-blue-700/50",
  },
  [TaskStatus.COMPLETED]: {
    light: "bg-green-100 text-green-800",
    dark: "text-white bg-green-700/50",
  },
  [TaskStatus.FAILED]: {
    light: "bg-red-100 text-red-800",
    dark: "text-white bg-red-700/50",
  },
  [TaskStatus.CANCELLED]: {
    light: "bg-gray-100 text-gray-800",
    dark: "text-white bg-gray-700/50",
  },
};

// 获取状态样式类
const getStatusClass = (status, task = null) => {
  // 检查是否为部分成功的任务，使用橙色样式
  if (status === TaskStatus.COMPLETED && task && task.details && task.details.partialSuccess) {
    return props.darkMode ? "text-white bg-orange-700/50" : "bg-orange-100 text-orange-800";
  }

  const statusClasses = statusClassMap[status] || statusClassMap[TaskStatus.CANCELLED];
  return props.darkMode ? statusClasses.dark : statusClasses.light;
};

// 任务卡片样式类映射 - 缓存样式类以提高性能
const taskCardClassMap = {
  [TaskStatus.RUNNING]: {
    light: "border-gray-200 bg-blue-50/50",
    dark: "border-gray-700 bg-blue-900/10",
  },
  [TaskStatus.COMPLETED]: {
    light: "border-gray-200 bg-green-50/50",
    dark: "border-gray-700 bg-green-900/10",
  },
  [TaskStatus.FAILED]: {
    light: "border-gray-200 bg-red-50/50",
    dark: "border-gray-700 bg-red-900/10",
  },
  default: {
    light: "border-gray-200",
    dark: "border-gray-700",
  },
};

// 获取任务卡片样式类
const getTaskCardClass = (task) => {
  const cardClasses = taskCardClassMap[task.status] || taskCardClassMap.default;
  return props.darkMode ? cardClasses.dark : cardClasses.light;
};

// 获取进度条样式类
const getProgressBarClass = (task) => {
  // 根据任务阶段设置不同的颜色
  if (task.details && task.details.phase) {
    if (task.details.phase === "downloading") {
      return "bg-blue-500";
    } else if (task.details.phase === "uploading") {
      return "bg-green-500";
    }
  }

  // 默认进度条颜色
  return task.progress > 95 ? "bg-green-500" : "bg-blue-500";
};

// 缓存时间单位常量，优化getTimeAgo函数
const TIME_UNITS = {
  MINUTE: 60,
  HOUR: 3600,
  DAY: 86400,
  WEEK: 604800,
};

// 导入统一的时间处理工具
import { formatRelativeTime, formatDateTime as formatDateTimeUtil } from "../../../../utils/timeUtils.js";

// 获取相对时间文本 - 使用统一的时间处理工具
const getTimeAgo = (dateString) => {
  return formatRelativeTime(dateString) || t("mount.taskManager.unknownTime");
};

// 格式化日期时间
const formatDateTime = (dateString) => {
  return formatDateTimeUtil(dateString) || t("mount.taskManager.unknownTime");
};

// 格式化进度数值
const formatProgress = (processed, total) => {
  // 检查值是否为数字
  const processedNum = Number(processed);
  const totalNum = Number(total);

  if (isNaN(processedNum) || isNaN(totalNum)) {
    return `${processed}/${total}`;
  }

  // 显示文件计数
  // 合理性检查 - 如果total超过10,000，可能是错误或者特殊情况
  if (totalNum > 10000) {
    // 显示简化的数字
    return `${formatNumber(processedNum)}/${formatNumber(totalNum)}`;
  }

  // 如果processed大于total，显示为total
  if (processedNum > totalNum) {
    return `${totalNum}/${totalNum}`;
  }

  return `${processedNum}/${totalNum}`;
};

// 格式化字节大小
const formatBytes = (bytes) => {
  if (bytes === 0) return "0 B";

  const k = 1024;
  const sizes = ["B", "KB", "MB", "GB", "TB"];
  const i = Math.floor(Math.log(bytes) / Math.log(k));

  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + " " + sizes[i];
};

// 格式化数字 (1000 -> 1K, 1000000 -> 1M)
const formatNumber = (num) => {
  if (num < 1000) {
    return num.toString();
  } else if (num < 1000000) {
    return (num / 1000).toFixed(1) + "K";
  } else {
    return (num / 1000000).toFixed(1) + "M";
  }
};

// 组件卸载时清理
onUnmounted(() => {
  // 清理展开状态
  expandedTaskIds.value.clear();

  console.log("TasksModal组件已卸载并清理");
});
</script>
