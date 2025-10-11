/**
 * 文件操作 Composable
 * 处理文件操作相关的UI逻辑，使用统一的文件系统API
 */

import { ref } from "vue";
import { useI18n } from "vue-i18n";
import { api } from "@/api";
import { downloadFileWithAuth } from "@/utils/fileUtils.js";
import { copyToClipboard } from "@/utils/clipboard.js";

export function useFileOperations() {
  const { t } = useI18n();

  // 操作状态
  const loading = ref(false);
  const error = ref(null);
  const showLinkCopiedNotification = ref(false);

  // ===== 核心文件操作方法 =====

  /**
   * 下载文件
   * @param {string|Object} pathOrItem - 文件路径或文件项对象
   * @returns {Promise<Object>} 操作结果
   */
  const downloadFile = async (pathOrItem) => {
    const item = typeof pathOrItem === "string" ? { path: pathOrItem, name: pathOrItem.split("/").pop() } : pathOrItem;

    if (!item || item.isDirectory) {
      return { success: false, message: t("mount.messages.cannotDownloadDirectory") };
    }

    try {
      loading.value = true;
      error.value = null;

      // 注释掉权限检查，保持与原版本兼容
      // if (!auth.hasFileOperationPermission("read", item.path)) {
      //   throw new Error(t("mount.messages.noDownloadPermission"));
      // }

      // 优先使用文件信息中的download_url字段（S3直链）
      if (item.download_url) {
        console.log("下载使用文件信息中的download_url:", item.download_url);

        // 创建隐藏的下载链接
        const link = document.createElement("a");
        link.href = item.download_url;
        link.download = item.name;
        link.style.display = "none";
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);

        return { success: true, message: t("mount.messages.downloadStarted", { name: item.name }) };
      }

      // 如果没有download_url，尝试通过API获取下载链接
      const getFileInfo = api.fs.getFileInfo;
      const response = await getFileInfo(item.path);

      if (response.success && response.data?.download_url) {
        const link = document.createElement("a");
        link.href = response.data.download_url;
        link.download = item.name;
        link.style.display = "none";
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);

        return { success: true, message: t("mount.messages.downloadStarted", { name: item.name }) };
      }

      // 如果都没有，说明后端有问题
      console.error("下载：文件信息中没有download_url字段，请检查后端getFileInfo实现");
      throw new Error("文件信息中缺少download_url字段");
    } catch (err) {
      console.error("下载文件失败:", err);
      error.value = err.message;
      return { success: false, message: t("mount.messages.downloadFailed", { name: item.name, message: err.message }) };
    } finally {
      loading.value = false;
    }
  };

  /**
   * 重命名文件或文件夹
   * @param {string} oldPath - 原路径
   * @param {string} newPath - 新路径（完整路径）
   * @returns {Promise<Object>} 操作结果
   */
  const renameItem = async (oldPath, newPath) => {
    try {
      loading.value = true;
      error.value = null;

      const renameItemApi = api.fs.renameItem;
      const response = await renameItemApi(oldPath, newPath);

      if (response.success) {
        return { success: true, message: t("mount.messages.renameSuccess") };
      } else {
        throw new Error(response.message || t("mount.messages.renameFailed"));
      }
    } catch (err) {
      console.error("重命名失败:", err);
      error.value = err.message;
      return { success: false, message: err.message };
    } finally {
      loading.value = false;
    }
  };

  /**
   * 创建文件夹
   * @param {string} parentPath - 父目录路径
   * @param {string} folderName - 文件夹名称
   * @returns {Promise<Object>} 操作结果
   */
  const createFolder = async (parentPath, folderName) => {
    try {
      loading.value = true;
      error.value = null;

      // 构造完整路径，确保目录路径以 / 结尾
      const fullPath = parentPath.endsWith("/") ? `${parentPath}${folderName}/` : `${parentPath}/${folderName}/`;

      const createFolderApi = api.fs.createDirectory;
      const response = await createFolderApi(fullPath);

      if (response.success) {
        return { success: true, message: t("mount.messages.createFolderSuccess") };
      } else {
        throw new Error(response.message || t("mount.messages.createFolderFailed"));
      }
    } catch (err) {
      console.error("创建文件夹失败:", err);
      error.value = err.message;
      return { success: false, message: err.message };
    } finally {
      loading.value = false;
    }
  };

  /**
   * 批量删除项目（统一删除接口）
   * 支持单个项目或项目数组，文件直接删除，目录递归删除
   * @param {Object|Array|string} itemsOrItem - 要删除的项目、项目数组或路径字符串
   * @returns {Promise<Object>} 操作结果
   */
  const batchDeleteItems = async (itemsOrItem) => {
    // 统一处理不同的输入格式
    let items;
    if (typeof itemsOrItem === "string") {
      // 如果是字符串路径，转换为项目对象
      items = [{ path: itemsOrItem }];
    } else if (Array.isArray(itemsOrItem)) {
      items = itemsOrItem;
    } else {
      // 如果是单个项目对象
      items = [itemsOrItem];
    }

    if (!items || items.length === 0) {
      return { success: false, message: t("mount.messages.noItemsToDelete") };
    }

    try {
      loading.value = true;
      error.value = null;

      // 提取路径数组
      const paths = items.map((item) => item.path);

      // 使用统一的批量删除接口
      const result = await api.fs.batchDeleteItems(paths);

      // 简化的结果处理
      if (result.failed && result.failed.length > 0) {
        throw new Error(result.failed[0].error);
      }

      return {
        success: true,
        message: t("mount.messages.batchDeleteSuccess", { count: items.length }),
      };
    } catch (err) {
      console.error("批量删除失败:", err);
      error.value = err.message;
      return {
        success: false,
        message: t("mount.messages.batchDeleteFailed", { message: err.message }),
      };
    } finally {
      loading.value = false;
    }
  };

  /**
   * 获取文件直链并复制到剪贴板
   * @param {Object} item - 文件项
   * @param {number|null} expiresIn - 链接过期时间（秒），null使用默认值
   * @param {boolean} forceDownload - 是否强制下载
   * @returns {Promise<Object>} 操作结果
   */
  const getFileLink = async (item, expiresIn = null, forceDownload = true) => {
    if (item.isDirectory) {
      return {
        success: false,
        message: t("mount.messages.directoryNoLink"),
      };
    }

    try {
      loading.value = true;
      error.value = null;

      // 使用统一API函数
      const getFileLinkApi = api.fs.getFileLink;

      // 调用API获取直链
      const response = await getFileLinkApi(item.path, expiresIn, forceDownload);

      if (response.success && response.data?.presignedUrl) {
        // 复制链接到剪贴板
        const copySuccess = await copyToClipboard(response.data.presignedUrl);

        if (copySuccess) {
          // 显示成功通知
          showLinkCopiedNotification.value = true;
          setTimeout(() => {
            showLinkCopiedNotification.value = false;
          }, 3000);

          return {
            success: true,
            message: t("mount.messages.linkCopiedSuccess"),
            url: response.data.presignedUrl,
          };
        } else {
          throw new Error(t("mount.messages.copyFailed"));
        }
      } else {
        throw new Error(response.message || t("mount.messages.getFileLinkFailed"));
      }
    } catch (err) {
      console.error("获取文件直链错误:", err);
      error.value = err.message;
      return {
        success: false,
        message: err.message || t("mount.messages.getFileLinkError"),
      };
    } finally {
      loading.value = false;
    }
  };

  /**
   * 清除错误状态
   */
  const clearError = () => {
    error.value = null;
  };

  return {
    // 状态
    loading,
    error,
    showLinkCopiedNotification,

    // 方法
    downloadFile,
    renameItem,
    createFolder,
    batchDeleteItems,
    getFileLink,
    clearError,
  };
}
