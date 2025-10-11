import { ref, reactive } from "vue";
import QRCode from "qrcode";
import { api } from "@/api";
import { copyToClipboard } from "@/utils/clipboard.js";
import { useDeleteSettingsStore } from "@/stores/deleteSettingsStore.js";
import { useAdminBase } from "./useAdminBase.js";

/**
 * 文件管理专用composable
 * 基于useAdminBase，添加文件管理特有的逻辑
 */
export function useFileManagement(userType = "admin") {
  // 继承基础管理功能
  const base = useAdminBase();

  // 文件管理特有状态
  const files = ref([]);
  const editingFile = ref(null);
  const previewFile = ref(null);
  const showEdit = ref(false);
  const showPreview = ref(false);
  const showQRCodeModal = ref(false);
  const qrCodeDataURL = ref("");
  const qrCodeSlug = ref("");

  // 删除设置store
  const deleteSettingsStore = useDeleteSettingsStore();

  // 复制状态跟踪
  const copiedFiles = reactive({});
  const copiedPermanentFiles = reactive({});

  // 用户类型判断
  const isAdmin = () => userType === "admin";
  const isApiKeyUser = () => userType === "apikey";

  /**
   * 统一的API调用函数
   */
  const apiGetFiles = (limit, offset, options = {}) => {
    if (isAdmin()) {
      return api.file.getFiles(limit, offset, options);
    } else {
      return api.file.getFiles(limit, offset, options);
    }
  };

  const apiGetFile = (id) => api.file.getFile(id);
  const apiUpdateFile = (id, metadata) => api.file.updateFile(id, metadata);
  const apiBatchDeleteFiles = (ids) => api.file.batchDeleteFiles(ids);

  /**
   * 加载文件列表
   */
  const loadFiles = async () => {
    return await base.withLoading(async () => {
      const response = await apiGetFiles(base.pagination.limit, base.pagination.offset);

      if (response.success) {
        console.log("🔍 response.data:", response.data);

        // 处理文件数据
        if (response.data && Array.isArray(response.data.files)) {
          files.value = response.data.files;
        } else if (Array.isArray(response.data)) {
          files.value = response.data;
        } else {
          console.error("❌ 无效的文件列表数据格式:", response.data);
          files.value = [];
        }

        // 更新分页信息（文件管理使用offset模式）
        // 正确处理后端返回的分页数据结构
        const paginationData = response.data.pagination || {
          total: files.value.length,
          limit: base.pagination.limit,
          offset: base.pagination.offset,
          hasMore: false,
        };
        base.updatePagination(paginationData, "offset");
        base.updateLastRefreshTime();
      } else {
        base.showError(response.message || "加载数据失败");
        files.value = [];
      }
    });
  };

  /**
   * 处理分页变化
   */
  const handleOffsetChange = (newOffset) => {
    base.handlePaginationChange(newOffset, "offset");
    loadFiles();
  };

  /**
   * 删除单个文件
   */
  const handleFileDelete = async (file) => {
    if (!confirm("确定要删除此文件吗？此操作不可恢复。")) {
      return;
    }

    return await base.withLoading(async () => {
      const result = await apiBatchDeleteFiles([file.id]);

      if (result.success) {
        base.showSuccess("删除成功");
        await loadFiles();
      } else {
        base.showError(result.message || "删除失败");
      }
    });
  };

  /**
   * 批量删除文件
   */
  const handleBatchDelete = async () => {
    const selectedCount = base.selectedItems.value.length;
    if (selectedCount === 0) {
      base.showError("请先选择要删除的文件");
      return;
    }

    if (!confirm(`确定要删除选中的 ${selectedCount} 个文件吗？此操作不可恢复。`)) {
      return;
    }

    return await base.withLoading(async () => {
      const result = await api.file.batchDeleteFiles(base.selectedItems.value, deleteSettingsStore.getDeleteMode());

      if (result.success && result.data) {
        const { success: successCount, failed } = result.data;

        if (failed && failed.length > 0) {
          const failedCount = failed.length;
          base.showSuccess(`批量删除完成：成功 ${successCount} 个，失败 ${failedCount} 个`);

          const failedDetails = failed.map((item) => `ID: ${item.id} - ${item.error}`).join("\n");
          console.warn("部分文件删除失败:", failedDetails);
        } else {
          base.showSuccess(`成功删除 ${successCount} 个文件`);
        }
      } else {
        base.showSuccess(`成功删除 ${selectedCount} 个文件`);
      }

      base.clearSelection();
      await loadFiles();
    });
  };

  /**
   * 打开编辑弹窗
   */
  const openEditModal = async (file) => {
    try {
      const response = await apiGetFile(file.id);

      if (response.success) {
        editingFile.value = response.data;
        showEdit.value = true;
      } else {
        base.showError(response.message || "获取文件详情失败");
      }
    } catch (err) {
      console.error("获取文件详情失败:", err);
      base.showError(err.message || "获取文件详情失败，请重试");
    }
  };

  /**
   * 更新文件元数据
   */
  const updateFileMetadata = async (updatedFile) => {
    return await base.withLoading(async () => {
      const response = await apiUpdateFile(updatedFile.id, updatedFile);

      if (response.success) {
        base.showSuccess("文件信息更新成功");
        showEdit.value = false;
        editingFile.value = null;
        await loadFiles();
      } else {
        base.showError(response.message || "更新失败");
      }
    });
  };

  /**
   * 打开预览弹窗
   */
  const openPreviewModal = async (file) => {
    try {
      const response = await apiGetFile(file.id);

      if (response.success) {
        previewFile.value = response.data;
        showPreview.value = true;
      } else {
        base.showError(response.message || "获取文件详情失败");
      }
    } catch (err) {
      console.error("获取文件详情失败:", err);
      base.showError(err.message || "获取文件详情失败，请重试");
    }
  };

  /**
   * 生成QR码
   */
  const generateQRCode = async (file, darkMode = false) => {
    try {
      const baseUrl = window.location.origin;
      const fileUrl = `${baseUrl}/file/${file.slug}`;

      qrCodeDataURL.value = await QRCode.toDataURL(fileUrl, {
        width: 300,
        margin: 2,
        color: {
          dark: darkMode ? "#ffffff" : "#000000",
          light: darkMode ? "#000000" : "#ffffff",
        },
      });

      qrCodeSlug.value = file.slug;
      showQRCodeModal.value = true;
    } catch (err) {
      console.error("生成二维码失败:", err);
      base.showError("生成二维码失败");
    }
  };

  /**
   * 切换全选状态（文件管理专用）
   */
  const toggleSelectAll = () => {
    base.toggleSelectAll(files.value, "id");
  };

  /**
   * 复制文件分享链接
   */
  const copyFileLink = async (file) => {
    if (!file || !file.slug) {
      base.showError("该文件没有有效的分享链接");
      return;
    }

    try {
      const baseUrl = window.location.origin;
      const fileUrl = `${baseUrl}/file/${file.slug}`;

      await copyToClipboard(fileUrl);

      // 设置复制状态
      copiedFiles[file.id] = true;
      setTimeout(() => {
        copiedFiles[file.id] = false;
      }, 2000);
    } catch (err) {
      console.error("复制链接失败:", err);
      // 只在失败时显示错误提示，成功时不显示顶部提示
      base.showError("复制链接失败，请手动复制");
    }
  };

  /**
   * 复制文件永久下载链接
   */
  const copyPermanentLink = async (file) => {
    if (!file || !file.slug) {
      base.showError("该文件没有有效的永久链接");
      return;
    }

    try {
      let permanentDownloadUrl;
      let fileWithUrls = file;

      // 如果文件对象中没有urls属性或者proxyDownloadUrl，先获取完整的文件详情
      if (!file.urls || !file.urls.proxyDownloadUrl) {
        try {
          const response = await apiGetFile(file.id);

          if (response.success && response.data) {
            fileWithUrls = response.data;
          } else {
            throw new Error(response.message || "获取文件详情失败");
          }
        } catch (error) {
          console.error("获取文件详情失败:", error);
          base.showError("无法获取文件直链，请确认您已登录并刷新页面后重试");
          return;
        }
      }

      // 使用后端返回的代理URL
      if (fileWithUrls.urls && fileWithUrls.urls.proxyDownloadUrl) {
        permanentDownloadUrl = fileWithUrls.urls.proxyDownloadUrl;

        // 获取文件密码
        const filePassword = getFilePassword(fileWithUrls);

        // 如果文件有密码保护且URL中没有密码参数，添加密码参数
        if (fileWithUrls.has_password && filePassword && !permanentDownloadUrl.includes("password=")) {
          permanentDownloadUrl += permanentDownloadUrl.includes("?") ? `&password=${encodeURIComponent(filePassword)}` : `?password=${encodeURIComponent(filePassword)}`;
        }

        await copyToClipboard(permanentDownloadUrl);

        // 设置复制状态
        copiedPermanentFiles[file.id] = true;
        setTimeout(() => {
          copiedPermanentFiles[file.id] = false;
        }, 2000);
      } else {
        throw new Error("无法获取文件代理链接");
      }
    } catch (err) {
      console.error("复制永久链接失败:", err);
      // 只在失败时显示错误提示，成功时不显示顶部提示
      base.showError("复制永久链接失败，请重试");
    }
  };

  /**
   * 获取文件密码
   */
  const getFilePassword = (file) => {
    // 优先使用文件信息中存储的明文密码
    if (file.plain_password) {
      return file.plain_password;
    }

    // 其次检查当前密码字段
    if (file.currentPassword) {
      return file.currentPassword;
    }

    // 尝试从URL获取密码参数
    const currentUrl = new URL(window.location.href);
    const passwordParam = currentUrl.searchParams.get("password");
    if (passwordParam) {
      return passwordParam;
    }

    // 最后尝试从会话存储中获取密码
    try {
      if (file.slug) {
        const sessionPassword = sessionStorage.getItem(`file_password_${file.slug}`);
        if (sessionPassword) {
          return sessionPassword;
        }
      }
    } catch (err) {
      console.error("从会话存储获取密码出错:", err);
    }

    return null;
  };

  /**
   * 获取Office文件预览URL
   */
  const getOfficePreviewUrl = async (file) => {
    if (!file.slug) return null;

    try {
      // 获取文件密码
      const filePassword = getFilePassword(file);

      console.log("正在请求Office预览URL:", file.slug);

      // 使用统一的预览服务
      return await api.fileView.getOfficePreviewUrl(file.slug, {
        password: filePassword,
        provider: "microsoft",
      });
    } catch (error) {
      console.error("获取Office预览URL出错:", error);
      base.showError(`预览失败: ${error.message}`);
      return null;
    }
  };

  /**
   * 预览文件
   */
  const previewFileInNewWindow = async (file) => {
    if (!file.slug) {
      base.showError("无法预览：文件没有设置短链接");
      return;
    }

    try {
      // 检查是否为Office文件
      const { FileType } = await import("@/utils/fileTypes.js");
      if (file.type === FileType.OFFICE) {
        console.log("检测到Office文件，使用专用预览", {
          filename: file.filename,
          mimetype: file.mimetype,
        });

        // 获取Office预览URL
        const officePreviewUrl = await getOfficePreviewUrl(file);
        if (officePreviewUrl) {
          window.open(officePreviewUrl, "_blank");
        }
        return;
      }

      // 非Office文件使用普通预览方式
      const previewUrl = getPermanentViewUrl(file);
      window.open(previewUrl, "_blank");
    } catch (err) {
      console.error("预览文件失败:", err);
      base.showError("预览文件失败，请稍后重试");
    }
  };

  /**
   * 下载文件
   */
  const downloadFileDirectly = (file) => {
    try {
      // 检查是否有永久下载链接
      if (!file.slug) {
        base.showError("无法下载：文件没有设置短链接");
        return;
      }

      // 提取文件名，用于下载时的文件命名
      const fileName = file.filename || "下载文件";

      // 创建一个隐藏的a标签
      const link = document.createElement("a");
      link.href = getPermanentDownloadUrl(file);
      link.download = fileName; // 设置下载文件名
      link.setAttribute("target", "_blank"); // 在新窗口打开
      document.body.appendChild(link);

      // 模拟点击下载
      link.click();

      // 移除临时创建的元素
      setTimeout(() => {
        document.body.removeChild(link);
      }, 100);
    } catch (err) {
      console.error("下载文件失败:", err);
      // 如果直接下载失败，尝试在新窗口打开下载链接
      if (file.slug) {
        window.open(getPermanentDownloadUrl(file), "_blank");
      } else {
        window.open(file.s3_url, "_blank");
      }
    }
  };

  /**
   * 获取文件的永久下载链接
   */
  const getPermanentDownloadUrl = (file) => {
    if (!file.slug) return "";

    // 获取文件密码
    const filePassword = getFilePassword(file);

    // 检查文件是否有urls对象和代理URL
    if (file.urls && file.urls.proxyDownloadUrl) {
      // 使用后端返回的代理URL，始终采用worker代理，不受use_proxy影响
      let url = file.urls.proxyDownloadUrl;

      // 如果有密码保护且URL中没有密码参数，则添加密码
      if (file.has_password && filePassword && !url.includes("password=")) {
        url += url.includes("?") ? `&password=${encodeURIComponent(filePassword)}` : `?password=${encodeURIComponent(filePassword)}`;
      }

      return url;
    }

    // 使用统一的文件分享API构建下载URL
    return api.fileView.buildDownloadUrl(file.slug, file.has_password ? filePassword : null);
  };

  /**
   * 获取文件的永久预览链接
   */
  const getPermanentViewUrl = (file) => {
    if (!file.slug) return "";

    // 获取文件密码
    const filePassword = getFilePassword(file);

    // 检查文件是否有urls对象和代理URL
    if (file.urls && file.urls.proxyPreviewUrl) {
      // 使用后端返回的代理URL，始终采用worker代理，不受use_proxy影响
      let url = file.urls.proxyPreviewUrl;

      // 如果有密码保护且URL中没有密码参数，则添加密码
      if (file.has_password && filePassword && !url.includes("password=")) {
        url += url.includes("?") ? `&password=${encodeURIComponent(filePassword)}` : `?password=${encodeURIComponent(filePassword)}`;
      }

      return url;
    }

    // 使用统一的文件分享API构建预览URL
    return api.fileView.buildPreviewUrl(file.slug, file.has_password ? filePassword : null);
  };

  /**
   * 搜索文件
   * @param {string} searchTerm - 搜索关键词
   * @param {number} offset - 偏移量，默认为当前分页的offset
   * @returns {Promise<Object>} 搜索结果
   */
  const searchFiles = async (searchTerm, offset = null) => {
    if (!searchTerm || searchTerm.trim().length < 2) {
      return { files: [], pagination: { total: 0, limit: base.pagination.limit, offset: 0 } };
    }

    // 如果没有指定offset，使用当前分页的offset
    const searchOffset = offset !== null ? offset : base.pagination.offset;

    return await base.withLoading(async () => {
      const response = await apiGetFiles(base.pagination.limit, searchOffset, { search: searchTerm.trim() });

      if (response.success) {
        // 处理搜索结果数据结构
        let searchFiles = [];
        if (response.data && Array.isArray(response.data.files)) {
          searchFiles = response.data.files;
        } else if (Array.isArray(response.data)) {
          searchFiles = response.data;
        }

        return {
          files: searchFiles,
          pagination: response.data.pagination || {
            total: searchFiles.length,
            limit: base.pagination.limit,
            offset: searchOffset,
          },
        };
      } else {
        base.showError(response.message || "搜索失败");
        return { files: [], pagination: { total: 0, limit: base.pagination.limit, offset: searchOffset } };
      }
    });
  };

  /**
   * 关闭所有弹窗
   */
  const closeAllModals = () => {
    showEdit.value = false;
    showPreview.value = false;
    showQRCodeModal.value = false;
    editingFile.value = null;
    previewFile.value = null;
    qrCodeDataURL.value = "";
    qrCodeSlug.value = "";
  };

  return {
    // 继承基础功能
    ...base,

    // 文件管理特有状态
    files,
    editingFile,
    previewFile,
    showEdit,
    showPreview,
    showQRCodeModal,
    qrCodeDataURL,
    qrCodeSlug,
    copiedFiles,
    copiedPermanentFiles,

    // 文件管理方法
    loadFiles,
    searchFiles,
    handleOffsetChange,
    handleFileDelete,
    handleBatchDelete,
    openEditModal,
    updateFileMetadata,
    openPreviewModal,
    generateQRCode,
    copyFileLink,
    copyPermanentLink,
    getFilePassword,
    getOfficePreviewUrl,
    previewFileInNewWindow,
    downloadFileDirectly,
    getPermanentDownloadUrl,
    getPermanentViewUrl,
    toggleSelectAll,
    closeAllModals,
  };
}
