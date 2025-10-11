/**
 * 文件篮 Composable
 * 处理文件篮相关的业务逻辑，连接Store和组件
 */

import { computed, onUnmounted } from "vue";
import { useI18n } from "vue-i18n";
import { storeToRefs } from "pinia";
import { useFileBasketStore } from "../../stores/fileBasketStore.js";
import { useAuthStore } from "@/stores/authStore.js";
import { useTaskManager } from "../../utils/taskManager.js";
import { api } from "@/api";

export function useFileBasket() {
  const { t } = useI18n();
  const fileBasketStore = useFileBasketStore();
  const authStore = useAuthStore();
  const taskManager = useTaskManager();

  // ===== 全局清理状态跟踪 =====

  // 跟踪所有活动的XMLHttpRequest（全局级别）
  const globalActiveXHRs = new Set();

  // 跟踪所有事件监听器
  const globalEventListeners = new Set();

  // 全局清理函数
  const globalCleanup = () => {
    // 取消所有活动的XMLHttpRequest
    globalActiveXHRs.forEach((xhr) => {
      if (xhr.readyState !== XMLHttpRequest.DONE) {
        xhr.abort();
      }
    });
    globalActiveXHRs.clear();

    // 移除所有事件监听器
    globalEventListeners.forEach(({ target, event, handler }) => {
      target.removeEventListener(event, handler);
    });
    globalEventListeners.clear();

    console.log("文件篮composable已清理所有资源");
  };

  // ===== Store状态解构 =====
  const { collectedFiles, isBasketOpen, collectionCount, hasCollection, collectionTotalSize, collectionTotalSizeMB, filesByDirectory, directoryCount, isInitialized } =
    storeToRefs(fileBasketStore);

  // ===== 计算属性 =====

  /**
   * 文件篮按钮显示文本
   */
  const basketButtonText = computed(() => {
    try {
      if (collectionCount.value === 0) {
        return t("fileBasket.button.empty");
      }
      return t("fileBasket.button.withCount", { count: collectionCount.value });
    } catch (error) {
      console.warn("国际化函数调用失败，使用默认文本:", error);
      // 使用默认文本作为后备
      if (collectionCount.value === 0) {
        return "文件篮";
      }
      return `文件篮 (${collectionCount.value})`;
    }
  });

  /**
   * 文件篮摘要信息
   */
  const basketSummary = computed(() => {
    return {
      fileCount: collectionCount.value,
      directoryCount: directoryCount.value,
      totalSizeMB: collectionTotalSizeMB.value,
      isEmpty: !hasCollection.value,
    };
  });

  // ===== 文件篮操作方法 =====

  /**
   * 添加文件到篮子
   * @param {Object|Array} files - 文件或文件数组
   * @param {string} currentPath - 当前目录路径
   * @returns {Object} 操作结果
   */
  const addToBasket = (files, currentPath) => {
    try {
      const fileArray = Array.isArray(files) ? files : [files];
      const fileItems = fileArray.filter((item) => !item.isDirectory);

      if (fileItems.length === 0) {
        return {
          success: false,
          message: t("fileBasket.messages.noFilesToAdd"),
        };
      }

      fileBasketStore.addToBasket(fileItems, currentPath);

      return {
        success: true,
        message: t("fileBasket.messages.addSuccess", {
          count: fileItems.length,
          total: collectionCount.value,
        }),
      };
    } catch (error) {
      console.error("添加文件到篮子失败:", error);
      return {
        success: false,
        message: t("fileBasket.messages.addFailed"),
      };
    }
  };

  /**
   * 从篮子移除文件
   * @param {string|Array} filePaths - 文件路径
   * @returns {Object} 操作结果
   */
  const removeFromBasket = (filePaths) => {
    try {
      fileBasketStore.removeFromBasket(filePaths);
      return {
        success: true,
        message: t("fileBasket.messages.removeSuccess"),
      };
    } catch (error) {
      console.error("从篮子移除文件失败:", error);
      return {
        success: false,
        message: t("fileBasket.messages.removeFailed"),
      };
    }
  };

  /**
   * 切换文件在篮子中的状态
   * @param {Object} file - 文件对象
   * @param {string} currentPath - 当前目录路径
   * @returns {Object} 操作结果
   */
  const toggleFileInBasket = (file, currentPath) => {
    try {
      const isInBasket = fileBasketStore.isFileInBasket(file.path);
      fileBasketStore.toggleFileInBasket(file, currentPath);

      return {
        success: true,
        isInBasket: !isInBasket,
        message: isInBasket ? t("fileBasket.messages.removeSuccess") : t("fileBasket.messages.addSuccess", { count: 1, total: collectionCount.value }),
      };
    } catch (error) {
      console.error("切换文件篮状态失败:", error);
      return {
        success: false,
        message: t("fileBasket.messages.toggleFailed"),
      };
    }
  };

  /**
   * 批量添加选中文件到篮子
   * @param {Array} selectedFiles - 选中的文件列表
   * @param {string} currentPath - 当前目录路径
   * @returns {Object} 操作结果
   */
  const addSelectedToBasket = (selectedFiles, currentPath) => {
    try {
      const addedCount = fileBasketStore.addSelectedToBasket(selectedFiles, currentPath);

      if (addedCount === 0) {
        return {
          success: false,
          message: t("fileBasket.messages.noFilesToAdd"),
        };
      }

      return {
        success: true,
        addedCount,
        message: t("fileBasket.messages.batchAddSuccess", {
          count: addedCount,
          total: collectionCount.value,
        }),
      };
    } catch (error) {
      console.error("批量添加文件到篮子失败:", error);
      return {
        success: false,
        message: t("fileBasket.messages.batchAddFailed"),
      };
    }
  };

  // ===== 面板管理方法 =====

  /**
   * 打开文件篮面板
   */
  const openBasket = () => {
    fileBasketStore.openBasket();
  };

  /**
   * 关闭文件篮面板
   */
  const closeBasket = () => {
    fileBasketStore.closeBasket();
  };

  /**
   * 切换文件篮面板显示状态
   */
  const toggleBasket = () => {
    fileBasketStore.toggleBasket();
  };

  // ===== 清理方法 =====

  /**
   * 清空文件篮
   * @returns {Object} 操作结果
   */
  const clearBasket = () => {
    try {
      fileBasketStore.clearBasket();
      return {
        success: true,
        message: t("fileBasket.messages.clearSuccess"),
      };
    } catch (error) {
      console.error("清空文件篮失败:", error);
      return {
        success: false,
        message: t("fileBasket.messages.clearFailed"),
      };
    }
  };

  /**
   * 重置文件篮（清空并关闭面板）
   */
  const resetBasket = () => {
    fileBasketStore.resetBasket();
  };

  // ===== 打包下载方法 =====

  /**
   * 创建打包下载任务
   * @returns {Promise<Object>} 操作结果
   */
  const createPackTask = async () => {
    try {
      if (!hasCollection.value) {
        return {
          success: false,
          message: t("fileBasket.messages.emptyBasket"),
        };
      }

      // 创建任务
      const taskName = t("fileBasket.task.name", {
        count: collectionCount.value,
        directories: directoryCount.value,
      });

      const taskId = taskManager.addTask("download", taskName, collectionCount.value);

      // 启动异步打包处理
      processPackTask(taskId);

      return {
        success: true,
        taskId,
        message: t("fileBasket.messages.taskCreated", { taskName }),
      };
    } catch (error) {
      console.error("创建打包任务失败:", error);
      return {
        success: false,
        message: t("fileBasket.messages.taskCreateFailed"),
      };
    }
  };

  /**
   * 处理打包任务（异步）
   * @param {number} taskId - 任务ID
   */
  const processPackTask = async (taskId) => {
    // 初始化文件状态跟踪
    const fileStates = new Map();

    // 清理函数
    const cleanup = () => {
      if (fileStates) {
        fileStates.clear();
      }
    };

    try {
      // 更新任务状态
      taskManager.updateTaskProgress(taskId, 0, {
        status: t("fileBasket.task.preparing"),
        total: collectionCount.value,
        processed: 0,
        currentFile: "",
        startTime: new Date().toISOString(),
      });

      // 获取收集的文件
      const files = fileBasketStore.getCollectedFiles();

      // 动态导入zip.js和file-saver
      const { ZipWriter, BlobWriter, BlobReader, HttpReader } = await import("@zip.js/zip.js");
      const { saveAs } = await import("file-saver");

      // 创建 ZipWriter
      console.log(`处理 ${files.length} 个文件`);
      const blobWriter = new BlobWriter();
      const zipWriter = new ZipWriter(blobWriter, {
        keepOrder: true, // 保持文件顺序
        useWebWorkers: true, // 启用Web Workers
        useCompressionStream: true, // 使用原生压缩流
        bufferedWrite: false, // 不缓冲写入，减少内存占用
      });
      const failedFiles = [];
      const addedFiles = new Set();

      // 初始化文件状态
      files.forEach((file) => {
        fileStates.set(file.path, {
          name: file.name,
          path: file.path,
          size: file.size,
          status: "pending",
          progress: 0,
          receivedBytes: 0,
          totalBytes: file.size || 0,
        });
      });

      // 并发添加文件到ZIP
      const zipAddPromises = files.map(async (file) => {
        try {
          const downloadUrl = await getFileDownloadUrl(file);

          // 构建ZIP路径
          const directoryName = file.sourceDirectory.replace(/^\//, "").replace(/\//g, "_") || "root";
          const zipPath = `${directoryName}/${file.name}`;

          // 处理文件名冲突
          let finalZipPath = zipPath;
          let counter = 1;
          while (addedFiles.has(finalZipPath)) {
            const lastDotIndex = zipPath.lastIndexOf(".");
            if (lastDotIndex > 0) {
              const name = zipPath.substring(0, lastDotIndex);
              const ext = zipPath.substring(lastDotIndex);
              finalZipPath = `${name}_${counter}${ext}`;
            } else {
              finalZipPath = `${zipPath}_${counter}`;
            }
            counter++;
          }
          addedFiles.add(finalZipPath);

          // 更新文件状态
          let fileState = fileStates.get(file.path);
          if (fileState) {
            fileState.status = "downloading";
          }

          await zipWriter.add(
            finalZipPath,
            new HttpReader(downloadUrl, {
              preventHeadRequest: true, // 避免额外的HEAD请求
              useXHR: false, // 使用fetch API
              useCompressionStream: true, // 启用原生压缩流
              transferStreams: true, // 启用流传输到Web Workers
            }),
            {
              useWebWorkers: true, // 启用Web Workers
              useCompressionStream: true, // 使用原生压缩流
              transferStreams: true, // 启用流传输
              onprogress: (progress, total) => {
                if (fileState) {
                  fileState.progress = total > 0 ? Math.round((progress / total) * 100) : 0;
                  fileState.receivedBytes = progress;
                  fileState.totalBytes = total;

                  const completedFiles = Array.from(fileStates.values()).filter((f) => f.status === "completed").length;
                  const overallProgress = Math.round(((completedFiles + progress / total) / files.length) * 90);

                  taskManager.updateTaskProgress(taskId, overallProgress, {
                    status: t("fileBasket.task.downloading"),
                    currentFile: `${file.name} (${fileState.progress}%)`,
                    processed: completedFiles,
                    total: files.length,
                  });
                }
              },
            }
          );

          // 文件完成
          fileState = fileStates.get(file.path);
          if (fileState) {
            fileState.status = "completed";
            fileState.progress = 100;
          }

          return { success: true, fileName: file.name };
        } catch (error) {
          console.error(`添加文件 ${file.name} 失败:`, error);
          failedFiles.push({ fileName: file.name, path: file.path, error: error.message });

          const failedFileState = fileStates.get(file.path);
          if (failedFileState) {
            failedFileState.status = "failed";
            failedFileState.progress = 0;
          }

          return { success: false, fileName: file.name, error: error.message };
        }
      });

      // 等待所有文件完成
      await Promise.all(zipAddPromises);

      // 添加错误报告
      if (failedFiles.length > 0) {
        const errorReport = [t("fileBasket.task.failedFilesHeader"), "", ...failedFiles.map(({ fileName, path, error }) => `${path} (${fileName}): ${error}`)].join("\n");

        await zipWriter.add("下载失败文件列表.txt", new BlobReader(new Blob([errorReport], { type: "text/plain" })));
      }

      // 生成ZIP
      taskManager.updateTaskProgress(taskId, 95, {
        status: t("fileBasket.task.generating"),
        currentFile: "",
        processed: files.length,
        total: files.length,
      });

      const zipBlob = await zipWriter.close();

      // 下载ZIP文件
      const timestamp = new Date().toISOString().slice(0, 19).replace(/:/g, "-");
      const zipFileName = `CloudPaste_${timestamp}.zip`;
      saveAs(zipBlob, zipFileName);

      // 完成任务
      const successCount = files.length - failedFiles.length;
      taskManager.completeTask(taskId, {
        status: t("fileBasket.task.completed"),
        successCount,
        failedCount: failedFiles.length,
        zipFileName,
        endTime: new Date().toISOString(),
        summary:
          failedFiles.length > 0
            ? t("fileBasket.task.summaryWithFailures", { success: successCount, failed: failedFiles.length })
            : t("fileBasket.task.summarySuccess", { count: successCount }),
      });

      // 打包完成后自动清空文件篮
      fileBasketStore.clearBasket();
    } catch (error) {
      console.error("打包任务失败:", error);
      taskManager.failTask(taskId, {
        status: t("fileBasket.task.failed"),
        error: error.message,
        endTime: new Date().toISOString(),
      });
    } finally {
      cleanup();
    }
  };

  /**
   * 获取文件下载URL
   * @param {Object} file - 文件对象
   * @returns {Promise<string>} 下载URL
   */
  const getFileDownloadUrl = async (file) => {
    try {
      // 如果文件对象中已经有download_url，直接使用
      if (file.download_url) {
        return file.download_url;
      }

      // 使用统一API获取文件直链
      const getFileLinkApi = api.fs.getFileLink;
      const response = await getFileLinkApi(file.path, null, true); // 强制下载

      if (response.success && response.data) {
        // file-link API返回的是presignedUrl字段
        if (response.data.presignedUrl) {
          return response.data.presignedUrl;
        }
        // 兼容其他可能的字段名
        if (response.data.download_url) {
          return response.data.download_url;
        }
        if (response.data.proxy_download_url) {
          return response.data.proxy_download_url;
        }
      }

      throw new Error(t("fileBasket.errors.noDownloadUrl"));
    } catch (error) {
      console.error(`获取文件 ${file.name} 下载链接失败:`, error);
      throw error;
    }
  };

  // ===== 工具方法 =====

  /**
   * 检查文件是否在篮子中
   * @param {string} filePath - 文件路径
   * @returns {boolean}
   */
  const isFileInBasket = (filePath) => {
    return fileBasketStore.isFileInBasket(filePath);
  };

  /**
   * 获取篮子摘要信息
   * @returns {Object}
   */
  const getBasketSummary = () => {
    return fileBasketStore.getCollectionSummary();
  };

  // ===== 组件卸载清理 =====

  onUnmounted(() => {
    globalCleanup();
  });

  return {
    // Store状态
    collectedFiles,
    isBasketOpen,
    collectionCount,
    hasCollection,
    collectionTotalSize,
    collectionTotalSizeMB,
    filesByDirectory,
    directoryCount,
    isInitialized,

    // 计算属性
    basketButtonText,
    basketSummary,

    // 文件篮操作方法
    addToBasket,
    removeFromBasket,
    toggleFileInBasket,
    addSelectedToBasket,

    // 面板管理方法
    openBasket,
    closeBasket,
    toggleBasket,

    // 清理方法
    clearBasket,
    resetBasket,

    // 打包下载方法
    createPackTask,

    // 工具方法
    isFileInBasket,
    getBasketSummary,

    // 手动清理方法（用于紧急情况）
    globalCleanup,
  };
}
