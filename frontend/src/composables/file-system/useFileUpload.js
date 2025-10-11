/**
 * 文件上传 Composable
 * 彻底解决进度计算和状态管理问题
 *
 * @deprecated 此文件已废弃，请使用 UppyUploadModal.vue + Uppy.js
 * 原因：项目已迁移到 Uppy.js 作为主要上传解决方案
 * 替代方案：frontend/src/components/mount-explorer/shared/modals/UppyUploadModal.vue
 */

import { ref, computed } from "vue";
import { useI18n } from "vue-i18n";
import { api } from "@/api";
import { useAuthStore } from "@/stores/authStore.js";
import { useGlobalMessage } from "../core/useGlobalMessage.js";

export function useFileUpload() {
  const { t } = useI18n();
  const authStore = useAuthStore();
  const globalMessage = useGlobalMessage();

  // ===== 文件管理状态 =====
  const selectedFiles = ref([]);
  const fileItems = ref([]);
  const isDragging = ref(false);

  // ===== 上传状态 =====
  const isUploading = ref(false);
  const uploadProgress = ref(0);
  const totalProgress = ref(0);
  const currentUploadIndex = ref(-1);
  const cancelUploadFlag = ref(false);
  const uploadSpeed = ref("");

  // ===== 文件级别的进度状态管理 =====
  /**
   * 每个文件的速度计算状态
   * Map<fileIndex, { lastLoaded: number, lastTime: number, isInitialized: boolean }>
   */
  const fileSpeedStates = ref(new Map());

  /**
   * 全局进度计算状态
   */
  const progressState = ref({
    lastCalculatedTotalProgress: 0,
    progressHistory: [], // 用于防止进度倒退
    lastUpdateTime: 0,
  });

  // ===== 计算属性 =====
  const hasFilesToUpload = computed(() => {
    return selectedFiles.value.length > 0 && fileItems.value.some((item) => item.status !== "success");
  });

  const currentUploadInfo = computed(() => {
    if (currentUploadIndex.value >= 0 && currentUploadIndex.value < selectedFiles.value.length) {
      const fileName = selectedFiles.value[currentUploadIndex.value].name;
      const current = currentUploadIndex.value + 1;
      const total = selectedFiles.value.length;
      return t("mount.uploadModal.uploading", { current, total, fileName });
    }
    return "";
  });

  // ===== 核心进度计算引擎=====

  /**
   * 创建进度计算器
   */
  const createProgressCalculator = () => {
    return {
      /**
       * 安全的总进度计算 - 防止状态污染和进度倒退
       * @param {number} currentFileIndex - 当前文件索引
       * @param {number} currentFileProgress - 当前文件进度
       */
      calculateTotalProgress: (currentFileIndex = -1, currentFileProgress = 0) => {
        // 数组长度安全检查
        if (fileItems.value.length !== selectedFiles.value.length) {
          console.warn("文件数组长度不一致，跳过进度计算");
          return;
        }

        let totalSize = 0;
        let completedSize = 0;
        let currentFileContribution = 0;
        let validFilesCount = 0;

        for (let i = 0; i < fileItems.value.length; i++) {
          const fileItem = fileItems.value[i];
          const file = selectedFiles.value[i];

          if (fileItem.status === "error" || fileItem.status === "cancelled") {
            continue;
          }

          validFilesCount++;
          totalSize += file.size;

          if (fileItem.status === "success") {
            completedSize += file.size;
          } else if (fileItem.status === "uploading" && i === currentFileIndex) {
            // 当前正在上传的文件的贡献
            currentFileContribution = Math.max(0, (currentFileProgress / 100) * file.size);
          }
        }

        if (totalSize === 0 || validFilesCount === 0) {
          totalProgress.value = 0;
          return;
        }

        // 计算新进度
        const newProgress = Math.floor(((completedSize + currentFileContribution) / totalSize) * 100);

        // 防止进度倒退
        const finalProgress = Math.max(progressState.value.lastCalculatedTotalProgress, newProgress);

        // 更新状态
        totalProgress.value = Math.min(100, finalProgress);
        progressState.value.lastCalculatedTotalProgress = totalProgress.value;
        progressState.value.lastUpdateTime = Date.now();

        console.log(`进度计算: 文件${currentFileIndex}, 当前进度${currentFileProgress}%, 总进度${totalProgress.value}%`);
      },

      /**
       * 文件级别的速度计算
       * @param {number} currentFileIndex - 当前文件索引
       * @param {number} progress - 当前进度
       */
      calculateUploadSpeed: (currentFileIndex, progress) => {
        if (currentFileIndex < 0 || currentFileIndex >= selectedFiles.value.length) {
          return;
        }

        const now = Date.now();
        const file = selectedFiles.value[currentFileIndex];
        const loaded = (progress / 100) * file.size;

        // 获取或创建文件的速度状态
        let speedState = fileSpeedStates.value.get(currentFileIndex);

        if (!speedState || !speedState.isInitialized) {
          // 初始化文件的速度状态
          speedState = {
            lastLoaded: loaded,
            lastTime: now,
            isInitialized: true,
            fileSize: file.size,
            fileName: file.name,
          };
          fileSpeedStates.value.set(currentFileIndex, speedState);
          console.log(`初始化文件 ${file.name} 的速度计算状态`);
          return; // 第一次计算不显示速度
        }

        const timeElapsed = (now - speedState.lastTime) / 1000;

        // 速度计算频率控制
        if (timeElapsed >= 0.5) {
          const bytesUploaded = loaded - speedState.lastLoaded;

          // 防止负数速度（可能由于进度回调异常导致）
          if (bytesUploaded >= 0) {
            const speed = bytesUploaded / timeElapsed;
            uploadSpeed.value = formatUploadSpeed(speed);

            console.log(`文件 ${file.name} 速度: ${uploadSpeed.value}, 已上传: ${loaded}/${file.size}`);
          }

          // 更新状态
          speedState.lastLoaded = loaded;
          speedState.lastTime = now;
        }
      },

      /**
       * 重置文件的速度计算状态
       * @param {number} fileIndex - 文件索引
       */
      resetFileSpeedState: (fileIndex) => {
        if (fileSpeedStates.value.has(fileIndex)) {
          fileSpeedStates.value.delete(fileIndex);
          console.log(`重置文件 ${fileIndex} 的速度计算状态`);
        }
      },

      /**
       * 重置所有进度计算状态
       */
      resetAllProgressStates: () => {
        fileSpeedStates.value.clear();
        progressState.value = {
          lastCalculatedTotalProgress: 0,
          progressHistory: [],
          lastUpdateTime: 0,
        };
        totalProgress.value = 0;
        uploadSpeed.value = "";
        console.log("重置所有进度计算状态");
      },
    };
  };

  // 创建进度计算器实例
  const progressCalculator = createProgressCalculator();

  // ===== 统一的进度计算接口 =====

  /**
   * 统一的总进度计算
   * @param {number} currentFileIndex - 当前文件索引
   * @param {number} currentFileProgress - 当前文件进度
   */
  const calculateTotalProgress = (currentFileIndex = -1, currentFileProgress = 0) => {
    progressCalculator.calculateTotalProgress(currentFileIndex, currentFileProgress);
  };

  /**
   * 统一的上传速度计算
   * @param {number} currentFileIndex - 当前文件索引
   * @param {number} progress - 当前进度
   */
  const calculateUploadSpeed = (currentFileIndex, progress) => {
    progressCalculator.calculateUploadSpeed(currentFileIndex, progress);
  };

  /**
   * 统一的批量上传状态重置
   */
  const resetBatchUploadState = () => {
    isUploading.value = false;
    currentUploadIndex.value = -1;
    progressCalculator.resetAllProgressStates();
  };

  /**
   * 文件切换时的状态管理
   * @param {number} newFileIndex - 新文件索引
   */
  const handleFileSwitch = (newFileIndex) => {
    // 重置新文件的速度计算状态，确保干净的开始
    progressCalculator.resetFileSpeedState(newFileIndex);
    console.log(`切换到文件 ${newFileIndex}，重置速度计算状态`);
  };

  /**
   * 清理文件的所有上传相关引用和状态
   * @param {Object} fileItem - 文件项对象
   */
  const cleanupFileUploadReferences = (fileItem) => {
    // 清理所有可能的上传引用
    if (fileItem.multipartUploader) {
      try {
        fileItem.multipartUploader.abort();
      } catch (error) {
        console.warn("清理multipartUploader时出错:", error);
      }
      fileItem.multipartUploader = null;
    }

    if (fileItem.xhr) {
      try {
        fileItem.xhr.abort();
      } catch (error) {
        console.warn("清理xhr时出错:", error);
      }
      fileItem.xhr = null;
    }

    // 清理其他上传相关状态
    fileItem.uploadStartTime = null;
    fileItem.lastProgressTime = null;
  };

  // ===== 消息管理 - 使用全局消息系统 =====

  /**
   * 显示消息 - 兼容性包装器
   * @param {string} type - 消息类型 ('success', 'error', 'warning', 'info')
   * @param {string} content - 消息内容
   * @param {number} duration - 显示时长（毫秒）
   */
  const showMessage = (type, content, duration = 3000) => {
    return globalMessage.showMessage(type, content, duration);
  };

  /**
   * 格式化上传速度
   * @param {number} bytesPerSecond - 每秒字节数
   * @returns {string} 格式化的速度字符串
   */
  const formatUploadSpeed = (bytesPerSecond) => {
    if (bytesPerSecond < 1024) {
      return `${bytesPerSecond.toFixed(0)} B/s`;
    } else if (bytesPerSecond < 1024 * 1024) {
      return `${(bytesPerSecond / 1024).toFixed(1)} KB/s`;
    } else {
      return `${(bytesPerSecond / (1024 * 1024)).toFixed(1)} MB/s`;
    }
  };

  // ===== 文件管理方法 =====

  /**
   * 添加文件到上传列表
   * @param {File|FileList|Array} files - 要添加的文件
   */
  const addFiles = (files) => {
    const fileArray = Array.isArray(files) ? files : Array.from(files);

    fileArray.forEach((file) => {
      // 检查文件是否已经在列表中（基于名称和大小）
      const isFileAlreadyAdded = selectedFiles.value.some((existingFile) => existingFile.name === file.name && existingFile.size === file.size);

      if (!isFileAlreadyAdded) {
        selectedFiles.value.push(file);
        fileItems.value.push({
          file,
          progress: 0,
          status: "pending", // pending, uploading, success, error
          message: "",
          fileId: null,
          xhr: null,
        });
      }
    });

    // 文件添加不需要显示消息提示
  };

  /**
   * 处理拖拽放置
   * @param {DragEvent} event - 拖拽事件
   */
  const handleDrop = (event) => {
    event.preventDefault();
    isDragging.value = false;

    const files = Array.from(event.dataTransfer.files);
    if (files.length > 0) {
      addFiles(files);
    }
  };

  /**
   * 处理文件选择
   * @param {Event} event - 文件选择事件
   */
  const handleFileSelect = (event) => {
    const files = Array.from(event.target.files);
    if (files.length > 0) {
      addFiles(files);
    }
    // 清空input值，允许重复选择同一文件
    event.target.value = "";
  };

  /**
   * 处理粘贴事件
   * @param {ClipboardEvent} event - 粘贴事件
   */
  const handlePaste = (event) => {
    const items = Array.from(event.clipboardData.items);
    const files = items
      .filter((item) => item.kind === "file")
      .map((item) => item.getAsFile())
      .filter((file) => file !== null);

    if (files.length > 0) {
      addFiles(files);
    }
  };

  /**
   * 移除文件
   * @param {number} index - 文件索引
   */
  const removeFile = (index) => {
    if (index >= 0 && index < selectedFiles.value.length) {
      // 如果文件正在上传，先取消上传
      const fileItem = fileItems.value[index];
      if (fileItem.status === "uploading") {
        // 使用统一的清理函数
        cleanupFileUploadReferences(fileItem);
      }

      // 清理该文件的速度计算状态
      progressCalculator.resetFileSpeedState(index);

      selectedFiles.value.splice(index, 1);
      fileItems.value.splice(index, 1);

      // 重新计算索引后的文件状态映射
      const newSpeedStates = new Map();
      fileSpeedStates.value.forEach((state, fileIndex) => {
        if (fileIndex > index) {
          // 索引大于被删除文件的索引需要减1
          newSpeedStates.set(fileIndex - 1, state);
        } else if (fileIndex < index) {
          // 索引小于被删除文件的索引保持不变
          newSpeedStates.set(fileIndex, state);
        }
        // 等于index的状态已经被删除，不需要处理
      });
      fileSpeedStates.value = newSpeedStates;

      // 重新计算总进度
      if (selectedFiles.value.length > 0) {
        calculateTotalProgress(currentUploadIndex.value, 0);
      } else {
        progressCalculator.resetAllProgressStates();
      }

      // 移除文件不需要显示消息提示
    }
  };

  /**
   * 清空所有文件
   */
  const clearAllFiles = () => {
    // 取消所有正在上传的文件
    fileItems.value.forEach((item) => {
      if (item.status === "uploading") {
        // 使用统一的清理函数
        cleanupFileUploadReferences(item);
      }
    });

    selectedFiles.value = [];
    fileItems.value = [];
    uploadProgress.value = 0;
    currentUploadIndex.value = -1;
    cancelUploadFlag.value = false;

    // 彻底重置所有进度计算状态
    progressCalculator.resetAllProgressStates();

    // 清空文件不需要显示消息提示
  };

  /**
   * 清空已成功上传的文件
   */
  const clearSuccessfulFiles = () => {
    const indicesToRemove = [];
    fileItems.value.forEach((item, index) => {
      if (item.status === "success") {
        indicesToRemove.push(index);
      }
    });

    // 从后往前删除，避免索引变化
    indicesToRemove.reverse().forEach((index) => {
      // 清理该文件的速度计算状态
      progressCalculator.resetFileSpeedState(index);

      selectedFiles.value.splice(index, 1);
      fileItems.value.splice(index, 1);
    });

    // 重建速度状态映射，因为索引发生了变化
    if (indicesToRemove.length > 0) {
      const newSpeedStates = new Map();

      fileSpeedStates.value.forEach((state, fileIndex) => {
        // 计算有多少个小于当前索引的文件被删除了
        const removedBeforeThis = indicesToRemove.filter((removedIndex) => removedIndex < fileIndex).length;
        const newIndex = fileIndex - removedBeforeThis;

        if (newIndex >= 0 && newIndex < selectedFiles.value.length) {
          newSpeedStates.set(newIndex, state);
        }
      });

      fileSpeedStates.value = newSpeedStates;

      // 重新计算总进度
      if (selectedFiles.value.length > 0) {
        calculateTotalProgress(currentUploadIndex.value, 0);
      } else {
        progressCalculator.resetAllProgressStates();
      }
    }

    // 清除成功文件不需要显示消息提示
  };

  // ===== 上传控制方法 =====

  /**
   * 开始上传所有文件
   * @param {string} currentPath - 上传路径
   * @param {string} uploadMethod - 上传方法 ('presigned', 'direct', 'multipart')
   * @param {boolean} _isAdmin - 是否为管理员（兼容性参数，实际使用authStore.isAdmin）
   * @returns {Promise<Object>} 上传结果
   */
  const startUpload = async (currentPath, uploadMethod = "presigned", _isAdmin = null) => {
    if (selectedFiles.value.length === 0 || isUploading.value) {
      return { success: false, uploadResults: [], errors: [] };
    }

    // 重置取消标志，确保新的批量上传不受之前取消操作影响
    cancelUploadFlag.value = false;

    // 检查是否有文件可以上传（排除已上传成功的文件）
    const filesToUpload = fileItems.value.filter((item) => item.status !== "success");

    if (filesToUpload.length === 0) {
      showMessage("warning", t("mount.uploadModal.noFilesToUpload"));
      return { success: false, uploadResults: [], errors: [] };
    }

    try {
      isUploading.value = true;
      cancelUploadFlag.value = false;
      // 使用新的进度计算引擎重置状态
      progressCalculator.resetAllProgressStates();

      const uploadResults = [];
      const errors = [];

      // 使用统一的文件系统API
      const fsApi = api.fs;

      // 逐个上传文件
      for (let i = 0; i < fileItems.value.length; i++) {
        const fileItem = fileItems.value[i];
        const file = selectedFiles.value[i];

        // 跳过已成功上传的文件
        if (fileItem.status === "success") {
          continue;
        }

        // 检查是否被取消
        if (cancelUploadFlag.value) {
          break;
        }

        // 文件切换处理 - 根本性修复
        if (currentUploadIndex.value !== i) {
          handleFileSwitch(i);
        }

        currentUploadIndex.value = i;
        fileItem.status = "uploading";
        fileItem.progress = 0;
        fileItem.message = t("mount.uploadModal.uploading");

        try {
          let response;

          // 定义进度更新回调 - 使用重构后的计算函数
          const updateProgress = (progress) => {
            if (fileItem.status === "error") return;

            fileItem.progress = progress;

            // 使用重构后的统一进度和速度计算函数
            calculateTotalProgress(i, progress);
            calculateUploadSpeed(i, progress);
          };

          // 检查取消标志
          const checkCancel = () => cancelUploadFlag.value;

          // 根据上传方式选择对应的上传方法
          if (uploadMethod === "direct") {
            console.log(`使用直接上传模式上传文件: ${file.name}`);
            response = await fsApi.uploadFile(currentPath, file, false, (xhr) => {
              fileItem.xhr = xhr;
            });
            if (response) {
              updateProgress(100);
            }
          } else if (uploadMethod === "presigned") {
            console.log(`使用预签名URL直传上传文件: ${file.name}`);
            response = await fsApi.performPresignedUpload(file, currentPath, updateProgress, checkCancel, (xhr) => {
              fileItem.xhr = xhr;
            });
          } else {
            console.log(`使用分片上传方式上传文件: ${file.name}`);
            // 保存上传路径，用于取消时的abort操作
            fileItem.uploadPath = currentPath;

            response = await fsApi.performMultipartUpload(file, currentPath, updateProgress, checkCancel, (uploaderRef) => {
              // 保存分片上传器引用，用于取消操作
              if (uploaderRef && uploaderRef.multipartUploader) {
                fileItem.multipartUploader = uploaderRef.multipartUploader;
                // 保存uploadId，用于取消时的abort操作
                if (uploaderRef.multipartUploader.uploadId) {
                  fileItem.uploadId = uploaderRef.multipartUploader.uploadId;
                }
              } else {
                fileItem.xhr = uploaderRef;
              }
            });
          }

          if (response && response.success) {
            fileItem.status = "success";
            fileItem.progress = 100;
            fileItem.message = t("mount.uploadModal.fileStatus.success");
            fileItem.fileId = response.data?.file_id || response.data?.fileId;
            uploadResults.push({
              file: file.name,
              success: true,
              fileId: fileItem.fileId,
            });
          } else {
            throw new Error(response?.message || t("mount.uploadModal.uploadFailed"));
          }
        } catch (error) {
          console.error(`上传文件 ${file.name} 失败:`, error);
          fileItem.status = "error";
          fileItem.message = error.message || t("mount.uploadModal.uploadError");
          errors.push({
            file: file.name,
            error: error.message,
          });
        }
      }

      const success = errors.length === 0;
      if (success) {
        showMessage("success", t("mount.uploadModal.allFilesUploaded"));
      } else {
        showMessage("error", t("mount.uploadModal.someFilesFailed"));
      }

      return {
        success,
        uploadResults,
        errors,
      };
    } catch (error) {
      console.error("上传过程中发生错误:", error);
      showMessage("error", error.message || t("mount.uploadModal.uploadError"));
      return {
        success: false,
        uploadResults: [],
        errors: [{ error: error.message }],
      };
    } finally {
      resetBatchUploadState();
    }
  };

  /**
   * 取消所有上传
   */
  const cancelUpload = async () => {
    cancelUploadFlag.value = true;

    // 取消所有正在上传的文件
    for (let i = 0; i < fileItems.value.length; i++) {
      const item = fileItems.value[i];
      if (item.status === "uploading") {
        // 先清理S3分片（仅对multipart上传）
        if (item.multipartUploader && item.uploadId && item.uploadPath) {
          try {
            const file = selectedFiles.value[i];
            await api.fs.abortMultipartUpload(item.uploadPath, item.uploadId, file.name);
            console.log(`已中止分片上传: ${file.name}`);
          } catch (error) {
            console.warn("中止分片上传失败:", error);
          }
        }

        // 再清理前端引用（包括xhr.abort()）
        cleanupFileUploadReferences(item);
        item.status = "error";
        item.message = t("mount.uploadModal.uploadCancelled");
      }
    }

    resetBatchUploadState();
    showMessage("warning", t("mount.uploadModal.allUploadsCancelled"));
  };

  /**
   * 取消单个文件上传
   * @param {number} index - 文件索引
   */
  const cancelSingleUpload = async (index) => {
    if (index >= 0 && index < fileItems.value.length) {
      const fileItem = fileItems.value[index];

      if (fileItem.status === "uploading") {
        // 先清理S3分片（仅对multipart上传）
        if (fileItem.multipartUploader && fileItem.uploadId && fileItem.uploadPath) {
          try {
            const file = selectedFiles.value[index];
            await api.fs.abortMultipartUpload(fileItem.uploadPath, fileItem.uploadId, file.name);
            console.log(`已中止分片上传: ${file.name}`);
          } catch (error) {
            console.warn("中止分片上传失败:", error);
          }
        }

        // 再清理前端引用（包括xhr.abort()）
        cleanupFileUploadReferences(fileItem);

        fileItem.status = "error";
        fileItem.message = t("mount.uploadModal.uploadCancelled");

        // 单个文件取消不需要显示全局消息，状态变化已经足够反馈
      }
    }
  };

  /**
   * 重试上传单个文件
   * @param {number} index - 文件索引
   * @param {string} currentPath - 上传路径
   * @param {string} uploadMethod - 上传方法
   * @param {boolean} _isAdmin - 是否为管理员（兼容性参数，实际使用authStore.isAdmin）
   * @returns {Promise<Object>} 上传结果
   */
  const retryUpload = async (index, currentPath, uploadMethod = "presigned", _isAdmin = null) => {
    if (index < 0 || index >= fileItems.value.length) {
      return { success: false, error: "Invalid index" };
    }

    // 如果正在进行批量上传，不允许重试
    if (isUploading.value && currentUploadIndex.value !== -1 && currentUploadIndex.value !== index) {
      return { success: false, error: "Batch upload in progress" };
    }

    // 重置取消标志，确保重试不受之前取消操作影响
    cancelUploadFlag.value = false;

    const fileItem = fileItems.value[index];
    const file = selectedFiles.value[index];

    try {
      // 重试文件时的状态管理 - 根本性修复
      handleFileSwitch(index);

      // 完全清理之前的上传状态和引用，确保干净的重试环境
      cleanupFileUploadReferences(fileItem);

      fileItem.status = "uploading";
      fileItem.progress = 0;
      fileItem.message = t("mount.uploadModal.uploading");

      // 使用统一的文件系统API
      const fsApi = api.fs;

      // 重试时也要计算总进度和速度 - 修复用户体验问题
      const updateProgress = (progress) => {
        if (fileItem.status === "error") return;
        fileItem.progress = progress;

        // 重试时也要更新总进度和速度，保持一致的用户体验
        calculateTotalProgress(index, progress);
        calculateUploadSpeed(index, progress);
      };

      // 检查取消标志
      const checkCancel = () => cancelUploadFlag.value;

      let response;

      // 根据上传方式选择对应的上传方法
      if (uploadMethod === "direct") {
        console.log(`使用直接上传模式重试上传文件: ${file.name}`);
        response = await fsApi.uploadFile(currentPath, file, false, (xhr) => {
          fileItem.xhr = xhr;
        });
        if (response) {
          updateProgress(100);
        }
      } else if (uploadMethod === "presigned") {
        console.log(`使用预签名URL直传重试上传文件: ${file.name}`);
        response = await fsApi.performPresignedUpload(file, currentPath, updateProgress, checkCancel, (xhr) => {
          fileItem.xhr = xhr;
        });
      } else {
        console.log(`使用分片上传方式重试上传文件: ${file.name}`);
        // 保存上传路径，用于取消时的abort操作
        fileItem.uploadPath = currentPath;

        response = await fsApi.performMultipartUpload(file, currentPath, updateProgress, checkCancel, (uploaderRef) => {
          // 保存分片上传器引用，用于取消操作
          if (uploaderRef && uploaderRef.multipartUploader) {
            fileItem.multipartUploader = uploaderRef.multipartUploader;
            // 保存uploadId，用于取消时的abort操作
            if (uploaderRef.multipartUploader.uploadId) {
              fileItem.uploadId = uploaderRef.multipartUploader.uploadId;
            }
          } else {
            fileItem.xhr = uploaderRef;
          }
        });
      }

      if (response && response.success) {
        fileItem.status = "success";
        fileItem.progress = 100;
        fileItem.message = t("mount.uploadModal.fileStatus.success");
        fileItem.fileId = response.data?.file_id || response.data?.fileId;

        // 重试成功只显示简单的成功消息
        showMessage("success", t("mount.uploadModal.retryUploadSuccess", { fileName: file.name }));

        return {
          success: true,
          fileId: fileItem.fileId,
        };
      } else {
        throw new Error(response?.message || t("mount.uploadModal.uploadFailed"));
      }
    } catch (error) {
      console.error(`重试上传文件 ${file.name} 失败:`, error);
      fileItem.status = "error";
      fileItem.message = error.message || t("mount.uploadModal.uploadFailed");

      // 重试失败只显示简单的错误消息
      showMessage("error", t("mount.uploadModal.retryUploadFailed", { fileName: file.name, message: error.message }));

      return {
        success: false,
        error: error.message,
      };
    }
  };

  return {
    // 状态
    selectedFiles,
    fileItems,
    isDragging,
    isUploading,
    uploadProgress,
    totalProgress,
    currentUploadIndex,
    uploadSpeed,

    // 全局消息系统
    message: globalMessage.message,
    hasMessage: globalMessage.hasMessage,

    // 计算属性
    hasFilesToUpload,
    currentUploadInfo,

    // 方法
    showMessage,
    formatUploadSpeed,
    addFiles,
    handleDrop,
    handleFileSelect,
    handlePaste,
    removeFile,
    clearAllFiles,
    clearSuccessfulFiles,
    startUpload,
    cancelUpload,
    cancelSingleUpload,
    retryUpload,
  };
}
