import { ref, computed } from "vue";
import { useAdminBase } from "./useAdminBase.js";
import { useAuthStore } from "@/stores/authStore.js";
import { useI18n } from "vue-i18n";
import { api } from "@/api";
import { formatDateTimeWithSeconds } from "@/utils/timeUtils.js";

/**
 * 挂载点管理专用composable
 */
export function useMountManagement() {
  // 继承基础管理功能独立的页面标识符
  const base = useAdminBase("mount");

  // 国际化
  const { t } = useI18n();

  // 使用认证Store
  const authStore = useAuthStore();

  // 挂载点管理特有状态
  const mounts = ref([]);
  const s3ConfigsList = ref([]);
  const apiKeyNames = ref({});
  const showForm = ref(false);
  const currentMount = ref(null);
  const searchQuery = ref("");

  // 重写分页默认设置为挂载点专用（每页6个）
  base.pagination.limit = 6;

  // 权限计算属性
  const isAdmin = computed(() => authStore.isAdmin);
  const isApiKeyUser = computed(() => authStore.authType === "apikey" && authStore.hasMountPermission);
  const isAuthorized = computed(() => isAdmin.value || isApiKeyUser.value);

  const apiUpdateMount = (id, mountData) => {
    if (isAdmin.value) {
      return api.mount.updateMount(id, mountData);
    } else {
      throw new Error("API密钥用户无权限更新挂载点");
    }
  };

  const apiDeleteMount = (id) => {
    if (isAdmin.value) {
      return api.mount.deleteMount(id);
    } else {
      throw new Error("API密钥用户无权限删除挂载点");
    }
  };

  /**
   * 搜索过滤计算属性
   */
  const filteredMounts = computed(() => {
    if (!searchQuery.value) {
      return mounts.value;
    }

    const query = searchQuery.value.toLowerCase();
    return mounts.value.filter(
      (mount) =>
        mount.name.toLowerCase().includes(query) ||
        mount.mount_path.toLowerCase().includes(query) ||
        mount.storage_type.toLowerCase().includes(query) ||
        (mount.remark && mount.remark.toLowerCase().includes(query))
    );
  });

  /**
   * 更新分页信息（挂载点专用逻辑）
   */
  const updateMountPagination = () => {
    base.pagination.total = filteredMounts.value.length;
    base.pagination.hasMore = base.pagination.offset + base.pagination.limit < base.pagination.total;
  };

  /**
   * 格式化日期显示
   */
  const formatDate = (dateString) => {
    if (!dateString) return "-";
    return formatDateTimeWithSeconds(dateString);
  };

  /**
   * 加载S3配置列表
   */
  const loadS3Configs = async () => {
    try {
      const response = await api.storage.getAllS3Configs();
      if (response.code === 200 && response.data) {
        s3ConfigsList.value = response.data;
      } else {
        console.error("加载S3配置列表失败:", response.message);
      }
    } catch (err) {
      console.error("加载S3配置列表错误:", err);
    }
  };

  /**
   * 根据ID获取S3配置
   */
  const getS3ConfigById = (configId) => {
    return s3ConfigsList.value.find((config) => config.id === configId) || null;
  };

  /**
   * 加载API密钥名称列表
   */
  const loadApiKeyNames = async () => {
    try {
      // 根据用户类型选择不同的加载方式
      if (isAdmin.value) {
        // 管理员用户 - 加载所有API密钥
        const response = await api.admin.getAllApiKeys();
        if (response.code === 200 && response.data) {
          // 构建API密钥ID到名称的映射
          const keyMap = {};
          response.data.forEach((key) => {
            keyMap[key.id] = key.name;
          });
          apiKeyNames.value = keyMap;
        }
      } else {
        // API密钥用户 - 只加载当前API密钥信息
        const keyInfo = authStore.apiKeyInfo;
        if (keyInfo && keyInfo.id) {
          const keyMap = {};
          keyMap[keyInfo.id] = keyInfo.name || t("admin.mount.currentApiKey");
          apiKeyNames.value = keyMap;
        }
      }
    } catch (err) {
      console.error("加载API密钥列表错误:", err);
    }
  };

  /**
   * 加载挂载点列表
   * 使用统一API，根据用户权限自动返回相应数据
   */
  const loadMounts = async () => {
    return await base.withLoading(async () => {
      try {
        // 使用统一的挂载点列表API
        const response = await api.mount.getMountsList();
        if (response.code === 200 && response.data) {
          mounts.value = response.data;
          if (mounts.value.length > 0) {
            // 更新最后刷新时间
            base.updateLastRefreshTime();
          }
          // 更新分页信息
          updateMountPagination();

          // 如果S3配置列表为空，尝试重新加载
          if (s3ConfigsList.value.length === 0) {
            loadS3Configs();
          }

          // 检查是否需要加载API密钥信息
          const needsApiKeys = mounts.value.some(
            (mount) => mount.created_by && (mount.created_by.startsWith("apikey:") || /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(mount.created_by))
          );

          if (needsApiKeys) {
            await loadApiKeyNames();
          }
        } else {
          base.showError(response.message || t("admin.mount.error.loadFailed"));
          mounts.value = [];
        }
      } catch (err) {
        console.error("加载挂载点列表错误:", err);
        base.showError(err.message || t("admin.mount.error.loadFailed"));
        mounts.value = [];
      }
    });
  };

  /**
   * 处理分页变化
   */
  const handleOffsetChange = (newOffset) => {
    base.handlePaginationChange(newOffset, "offset");
    loadMounts();
  };

  /**
   * 打开新建表单
   */
  const openCreateForm = () => {
    currentMount.value = null;
    showForm.value = true;
  };

  /**
   * 打开编辑表单
   */
  const openEditForm = (mount) => {
    currentMount.value = { ...mount };
    showForm.value = true;
  };

  /**
   * 关闭表单
   */
  const closeForm = () => {
    showForm.value = false;
    currentMount.value = null;
  };

  /**
   * 表单保存成功的处理函数
   */
  const handleFormSaveSuccess = (success = true, message = null) => {
    closeForm();

    // 如果操作失败，显示错误消息
    if (success === false) {
      base.showError(message || t("admin.mount.error.updateFailed"));
      return;
    }

    // 显示成功消息
    base.showSuccess(message || (currentMount.value ? t("admin.mount.success.updated") : t("admin.mount.success.created")));

    // 重新加载挂载点列表
    loadMounts();
  };

  /**
   * 删除挂载点
   */
  const confirmDelete = async (id) => {
    if (!confirm(t("admin.mount.confirmDelete.message", { name: "此挂载点" }))) {
      return;
    }

    return await base.withLoading(async () => {
      try {
        // 根据用户类型调用相应的API函数
        const response = await apiDeleteMount(id);
        if (response.code === 200) {
          base.showSuccess(t("admin.mount.success.deleted"));
          // 重新加载挂载点列表
          loadMounts();
        } else {
          base.showError(response.message || t("admin.mount.error.deleteFailed"));
        }
      } catch (err) {
        console.error("删除挂载点错误:", err);
        base.showError(err.message || t("admin.mount.error.deleteFailed"));
      }
    });
  };

  /**
   * 切换挂载点启用/禁用状态
   */
  const toggleActive = async (mount) => {
    // 确定操作类型（用于提示消息）
    const action = mount.is_active ? t("admin.mount.actions.disable") : t("admin.mount.actions.enable");

    return await base.withLoading(async () => {
      try {
        // 准备更新数据，只包含is_active字段
        const updateData = {
          is_active: !mount.is_active,
        };

        // 只有管理员可以切换挂载点状态
        if (isApiKeyUser.value) {
          base.showError(t("admin.mount.error.apiKeyNoPermission"));
          return;
        }

        const response = await apiUpdateMount(mount.id, updateData);

        // 处理响应
        if (response.code === 200) {
          base.showSuccess(mount.is_active ? t("admin.mount.success.disabled") : t("admin.mount.success.enabled"));
          // 重新加载挂载点列表
          loadMounts();
        } else {
          base.showError(response.message || (mount.is_active ? t("admin.mount.error.disableFailed") : t("admin.mount.error.enableFailed")));
        }
      } catch (err) {
        console.error(`${action}挂载点错误:`, err);
        base.showError(err.message || (mount.is_active ? t("admin.mount.error.disableFailed") : t("admin.mount.error.enableFailed")));
      }
    });
  };

  /**
   * 获取存储类型样式类
   */
  const getStorageTypeClass = (storageType, darkModeValue = false) => {
    switch (storageType) {
      case "S3":
        return darkModeValue ? "bg-blue-700 text-blue-100" : "bg-blue-100 text-blue-800";
      case "WebDAV":
        return darkModeValue ? "bg-green-700 text-green-100" : "bg-green-100 text-green-800";
      case "FTP":
        return darkModeValue ? "bg-purple-700 text-purple-100" : "bg-purple-100 text-purple-800";
      case "SMB":
        return darkModeValue ? "bg-orange-700 text-orange-100" : "bg-orange-100 text-orange-800";
      default:
        return darkModeValue ? "bg-gray-700 text-gray-300" : "bg-gray-100 text-gray-700";
    }
  };

  /**
   * 获取状态样式类
   */
  const getStatusClass = (isActive, darkModeValue = false) => {
    if (isActive) {
      return darkModeValue ? "bg-green-700 text-green-100" : "bg-green-100 text-green-800";
    } else {
      return darkModeValue ? "bg-red-700 text-red-100" : "bg-red-100 text-red-800";
    }
  };

  /**
   * 获取创建者类型，用于后续格式化和样式计算
   */
  const getCreatorType = (mount) => {
    if (!mount.created_by) {
      return "system";
    }

    if (mount.created_by === "admin") {
      return "admin";
    }

    // 处理带"apikey:"前缀的API密钥ID
    if (typeof mount.created_by === "string" && mount.created_by.startsWith("apikey:")) {
      return "apikey";
    }

    // 检查是否在已知的API密钥列表中的UUID
    if (/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(mount.created_by) && apiKeyNames.value && apiKeyNames.value[mount.created_by]) {
      return "apikey";
    }

    // UUID格式但不在已知API密钥列表中，则视为管理员
    if (/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(mount.created_by)) {
      return "admin";
    }

    // 默认为其他类型
    return "other";
  };

  /**
   * 格式化创建者信息显示
   */
  const formatCreator = (mount) => {
    const creatorType = getCreatorType(mount);

    if (creatorType === "system") {
      return t("admin.mount.creators.system", "系统");
    }

    if (creatorType === "admin") {
      return t("admin.mount.creators.admin", "管理员");
    }

    if (creatorType === "apikey") {
      // 获取API密钥ID
      let keyId = mount.created_by;
      if (mount.created_by.startsWith("apikey:")) {
        keyId = mount.created_by.substring(7);
      }

      // 显示API密钥名称或缩略ID
      if (apiKeyNames.value && apiKeyNames.value[keyId]) {
        return `${t("admin.mount.creators.apiKey", "密钥")}：${apiKeyNames.value[keyId]}`;
      } else if (keyId.length > 10) {
        return `${t("admin.mount.creators.apiKey", "密钥")}：${keyId.substring(0, 5)}...`;
      } else {
        return `${t("admin.mount.creators.apiKey", "密钥")}：${keyId}`;
      }
    }

    // 默认直接返回创建者值
    return mount.created_by;
  };

  /**
   * 获取创建者标签的样式类
   */
  const getCreatorClass = (mount, darkModeValue = false) => {
    const creatorType = getCreatorType(mount);

    // 根据创建者类型返回对应样式
    if (creatorType === "system") {
      return darkModeValue ? "bg-gray-700 text-gray-300" : "bg-gray-100 text-gray-700";
    }

    if (creatorType === "admin") {
      return darkModeValue ? "bg-green-900/50 text-green-200 border border-green-800/50" : "bg-green-100 text-green-800 border border-green-200";
    }

    if (creatorType === "apikey") {
      return darkModeValue ? "bg-blue-900/50 text-blue-200 border border-blue-800/50" : "bg-blue-100 text-blue-800 border border-blue-200";
    }

    // 默认样式
    return darkModeValue ? "bg-gray-700 text-gray-300" : "bg-gray-100 text-gray-700";
  };

  /**
   * 获取创建者显示名称（保持向后兼容）
   */
  const getCreatorDisplayName = (createdBy) => {
    if (!createdBy) return t("admin.mount.info.unknownCreator");

    // 如果是API密钥创建的
    if (createdBy.startsWith("apikey:")) {
      const keyId = createdBy.replace("apikey:", "");
      return apiKeyNames.value[keyId] || t("admin.mount.info.unknownApiKey");
    }

    // 如果是UUID格式（API密钥ID）
    if (/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(createdBy)) {
      return apiKeyNames.value[createdBy] || t("admin.mount.info.unknownApiKey");
    }

    // 否则认为是管理员创建的
    return t("admin.mount.info.admin");
  };

  /**
   * 格式化存储类型显示
   */
  const formatStorageType = (mount) => {
    // 基本验证
    if (!mount) return "-";

    if (mount.storage_type === "S3" && mount.storage_config_id) {
      // 如果S3配置列表尚未加载完成
      if (s3ConfigsList.value.length === 0) {
        return `${mount.storage_type} (加载中...)`;
      }

      const config = getS3ConfigById(mount.storage_config_id);
      if (config) {
        return `${config.name} (${config.provider_type})`;
      } else {
        // 未找到配置时显示配置ID
        return `${mount.storage_type} (ID: ${mount.storage_config_id})`;
      }
    }
    return mount.storage_type || "-";
  };

  return {
    // 继承基础功能
    ...base,

    // 挂载点管理特有状态
    mounts,
    s3ConfigsList,
    apiKeyNames,
    showForm,
    currentMount,
    searchQuery,
    filteredMounts,

    // 权限状态
    isAdmin,
    isApiKeyUser,
    isAuthorized,

    // 挂载点管理方法
    loadMounts,
    loadS3Configs,
    loadApiKeyNames,
    handleOffsetChange,
    openCreateForm,
    openEditForm,
    closeForm,
    handleFormSaveSuccess,
    confirmDelete,
    toggleActive,

    // 工具方法
    formatDate,
    getS3ConfigById,
    updateMountPagination,
    getStorageTypeClass,
    getStatusClass,
    getCreatorType,
    formatCreator,
    getCreatorClass,
    getCreatorDisplayName,
    formatStorageType,
  };
}
