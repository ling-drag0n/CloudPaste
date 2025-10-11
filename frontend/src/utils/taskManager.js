import { reactive, readonly } from "vue";

// localStorage键名
const STORAGE_KEY = "cloudpaste_tasks";

// 配置项
const MAX_TASK_AGE_DAYS = 7; // 保留任务的最大天数

// 任务状态枚举
export const TaskStatus = {
  PENDING: "pending", // 待处理
  RUNNING: "running", // 运行中
  COMPLETED: "completed", // 已完成
  FAILED: "failed", // 失败
  CANCELLED: "cancelled", // 已取消
};

// 任务类型枚举
export const TaskType = {
  COPY: "copy", // 复制任务
  UPLOAD: "upload", // 上传任务
  DELETE: "delete", // 删除任务
  DOWNLOAD: "download", // 下载任务
};

// 创建任务状态存储
const state = reactive({
  tasks: [], // 任务列表
  nextId: 1, // 下一个任务ID
});

// 节流保存到localStorage（避免频繁保存导致性能问题）
let saveTimeout = null;
const saveTasksToStorage = () => {
  // 清除之前的定时器
  if (saveTimeout) {
    clearTimeout(saveTimeout);
  }

  // 延迟保存，避免频繁写入
  saveTimeout = setTimeout(() => {
    try {
      localStorage.setItem(
        STORAGE_KEY,
        JSON.stringify({
          tasks: state.tasks,
          nextId: state.nextId,
        })
      );
      console.log("任务已保存到本地存储");
    } catch (error) {
      console.error("保存任务到本地存储失败:", error);
    }
  }, 500); // 500ms延迟保存
};

// 清理过期任务
const cleanExpiredTasks = (tasks) => {
  if (!Array.isArray(tasks)) return [];

  const now = new Date();
  const cutoffDate = new Date(now.getTime() - MAX_TASK_AGE_DAYS * 24 * 60 * 60 * 1000);

  return tasks.filter((task) => {
    const taskDate = new Date(task.updatedAt);
    return taskDate >= cutoffDate;
  });
};

// 从localStorage加载任务
const loadTasksFromStorage = () => {
  try {
    const storedData = localStorage.getItem(STORAGE_KEY);
    if (storedData) {
      const parsedData = JSON.parse(storedData);

      // 恢复日期对象（JSON.parse会将日期字符串转为字符串）
      if (parsedData.tasks && Array.isArray(parsedData.tasks)) {
        parsedData.tasks.forEach((task) => {
          task.createdAt = new Date(task.createdAt);
          task.updatedAt = new Date(task.updatedAt);
        });

        // 清理过期任务
        const cleanedTasks = cleanExpiredTasks(parsedData.tasks);

        // 更新状态
        state.tasks = cleanedTasks;
        state.nextId = parsedData.nextId || 1;

        console.log(`从本地存储加载了 ${cleanedTasks.length} 个任务`);
      }
    }
  } catch (error) {
    console.error("从本地存储加载任务失败:", error);
  }
};

// 初始化时加载任务
loadTasksFromStorage();

// 任务管理器方法
const taskManager = {
  // 添加新任务
  addTask(taskType, name, total = 100) {
    const task = {
      id: state.nextId++,
      type: taskType,
      name,
      progress: 0,
      status: TaskStatus.PENDING,
      createdAt: new Date(),
      updatedAt: new Date(),
      total,
      error: null,
      details: {},
    };

    state.tasks.push(task);
    saveTasksToStorage(); // 保存到本地存储
    return task.id;
  },

  // 更新任务进度
  updateTaskProgress(taskId, progress, details = {}) {
    const task = state.tasks.find((t) => t.id === taskId);
    if (task) {
      task.progress = progress;
      task.status = TaskStatus.RUNNING;
      task.updatedAt = new Date();
      task.details = { ...task.details, ...details };
      saveTasksToStorage(); // 保存到本地存储
    }
  },

  // 完成任务
  completeTask(taskId, details = {}) {
    const task = state.tasks.find((t) => t.id === taskId);
    if (task) {
      task.progress = 100;
      task.status = TaskStatus.COMPLETED;
      task.updatedAt = new Date();
      task.details = { ...task.details, ...details };
      saveTasksToStorage(); // 保存到本地存储
    }
  },

  // 标记任务失败
  failTask(taskId, error, details = {}) {
    const task = state.tasks.find((t) => t.id === taskId);
    if (task) {
      task.status = TaskStatus.FAILED;
      task.error = error;
      task.updatedAt = new Date();
      task.details = { ...task.details, ...details };
      saveTasksToStorage(); // 保存到本地存储
    }
  },

  // 取消任务
  cancelTask(taskId) {
    const task = state.tasks.find((t) => t.id === taskId);
    if (task) {
      task.status = TaskStatus.CANCELLED;
      task.updatedAt = new Date();
      saveTasksToStorage(); // 保存到本地存储
    }
  },

  // 清除已完成的任务
  clearCompletedTasks() {
    state.tasks = state.tasks.filter((t) => t.status !== TaskStatus.COMPLETED && t.status !== TaskStatus.FAILED && t.status !== TaskStatus.CANCELLED);
    saveTasksToStorage(); // 保存到本地存储
  },

  // 清除所有任务
  clearAllTasks() {
    state.tasks = [];
    saveTasksToStorage();
  },

  // 手动保存任务到本地存储
  manualSaveToStorage() {
    saveTasksToStorage();
  },

  // 手动从本地存储加载任务
  manualLoadFromStorage() {
    loadTasksFromStorage();
  },

  // 获取任务存储统计信息
  getStorageStats() {
    try {
      const stats = {
        totalTasks: state.tasks.length,
        byStatus: {
          [TaskStatus.PENDING]: 0,
          [TaskStatus.RUNNING]: 0,
          [TaskStatus.COMPLETED]: 0,
          [TaskStatus.FAILED]: 0,
          [TaskStatus.CANCELLED]: 0,
        },
        byType: {
          [TaskType.COPY]: 0,
          [TaskType.UPLOAD]: 0,
          [TaskType.DELETE]: 0,
          [TaskType.DOWNLOAD]: 0,
        },
        oldestTask: null,
        newestTask: null,
        storageUsage: 0,
      };

      // 统计各状态和类型的任务数量
      state.tasks.forEach((task) => {
        stats.byStatus[task.status]++;
        stats.byType[task.type]++;
      });

      // 找出最早和最新的任务
      if (state.tasks.length > 0) {
        const sortedTasks = [...state.tasks].sort((a, b) => new Date(a.createdAt) - new Date(b.createdAt));
        stats.oldestTask = sortedTasks[0].createdAt;
        stats.newestTask = sortedTasks[sortedTasks.length - 1].createdAt;
      }

      // 估计存储使用量
      const storageData = JSON.stringify({
        tasks: state.tasks,
        nextId: state.nextId,
      });
      stats.storageUsage = new Blob([storageData]).size;

      return stats;
    } catch (error) {
      console.error("获取存储统计信息失败:", error);
      return null;
    }
  },

  // 获取所有任务
  getTasks() {
    return state.tasks;
  },
};

// 导出只读状态和方法
export const useTaskManager = () => {
  return {
    state: readonly(state),
    ...taskManager,
  };
};
