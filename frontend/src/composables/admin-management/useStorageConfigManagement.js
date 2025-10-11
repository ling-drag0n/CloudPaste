import { ref } from "vue";
import { useAdminBase } from "./useAdminBase.js";
import { api } from "@/api";

/**
 * S3存储配置管理composable
 * 提供S3配置的CRUD操作、分页管理、测试功能等
 */
export function useStorageConfigManagement() {
  // 继承基础功能，使用独立的页面标识符
  const base = useAdminBase("storage");

  // S3配置特有状态
  const s3Configs = ref([]);
  const currentConfig = ref(null);
  const showAddForm = ref(false);
  const showEditForm = ref(false);
  const testResults = ref({});

  // 测试详情模态框状态
  const showTestDetails = ref(false);
  const selectedTestResult = ref(null);
  const showDetailedResults = ref(true);

  // S3配置专用的分页选项：4、8、12
  const pageSizeOptions = [4, 8, 12];

  // 重写默认页面大小，S3配置默认4个
  const getDefaultPageSize = () => {
    try {
      const saved = localStorage.getItem("admin-page-size");
      if (saved) {
        const pageSizes = JSON.parse(saved);
        const savedSize = pageSizes["storage"] || 4;
        // 确保保存的值在S3配置的选项范围内，否则使用默认值4
        return pageSizeOptions.includes(savedSize) ? savedSize : 4;
      }
    } catch (error) {
      console.warn("解析S3配置分页设置失败:", error);
    }
    return 4;
  };

  // 重新初始化分页状态
  base.pagination.limit = getDefaultPageSize();

  /**
   * 加载S3配置列表
   */
  const loadS3Configs = async () => {
    return await base.withLoading(async () => {
      const response = await api.storage.getAllS3Configs({
        page: base.pagination.page,
        limit: base.pagination.limit,
      });

      if (response.data) {
        s3Configs.value = response.data;
        // 使用标准的updatePagination方法
        base.updatePagination(
          {
            total: response.total || response.data.length,
          },
          "page"
        );
        base.updateLastRefreshTime();
        console.log(`S3配置列表加载完成，共 ${s3Configs.value.length} 条`);
      } else {
        base.showError(response.message || "加载数据失败");
        s3Configs.value = [];
      }
    });
  };

  /**
   * 处理页码变化
   */
  const handlePageChange = (page) => {
    base.handlePaginationChange(page, "page");
    loadS3Configs();
  };

  /**
   * 处理每页数量变化 - 使用标准方法
   */
  const handleLimitChange = (newLimit) => {
    base.changePageSize(newLimit);
    loadS3Configs();
  };

  /**
   * 删除S3配置
   */
  const handleDeleteConfig = async (configId) => {
    if (!confirm("确定要删除此存储配置吗？此操作不可恢复！")) {
      return;
    }

    return await base.withLoading(async () => {
      try {
        await api.storage.deleteS3Config(configId);
        base.showSuccess("删除成功");
        await loadS3Configs();
      } catch (err) {
        console.error("删除S3配置失败:", err);
        if (err.message && err.message.includes("有文件正在使用")) {
          base.showError(`无法删除此配置：${err.message}`);
        } else {
          base.showError(err.message || "删除S3配置失败，请稍后再试");
        }
      }
    });
  };

  /**
   * 编辑配置
   */
  const editConfig = (config) => {
    currentConfig.value = { ...config };
    showEditForm.value = true;
    showAddForm.value = false;
  };

  /**
   * 添加新配置
   */
  const addNewConfig = () => {
    currentConfig.value = null;
    showAddForm.value = true;
    showEditForm.value = false;
  };

  /**
   * 处理表单成功提交
   */
  const handleFormSuccess = async () => {
    showAddForm.value = false;
    showEditForm.value = false;
    await loadS3Configs();
  };

  /**
   * 设置默认配置
   */
  const handleSetDefaultConfig = async (configId) => {
    return await base.withLoading(async () => {
      try {
        await api.storage.setDefaultS3Config(configId);
        base.showSuccess("设置默认配置成功");
        await loadS3Configs();
      } catch (err) {
        console.error("设置默认S3配置失败:", err);
        base.showError(err.message || "无法设置为默认配置，请稍后再试");
      }
    });
  };

  /**
   * 测试结果处理器类
   */
  class TestResultProcessor {
    constructor(result) {
      this.result = result;
    }

    /**
     * 计算测试状态
     */
    calculateStatus() {
      const basicConnectSuccess = this.result.read?.success === true;
      const writeSuccess = this.result.write?.success === true;
      const corsSuccess = this.result.cors?.success === true;
      const frontendSimSuccess = this.result.frontendSim?.success === true;

      // 完全成功：读写权限都可用、CORS配置正确且前端模拟测试通过
      const isFullSuccess = basicConnectSuccess && writeSuccess && corsSuccess && frontendSimSuccess;
      // 部分成功：至少读权限可用
      const isPartialSuccess = basicConnectSuccess && (!writeSuccess || !corsSuccess || !frontendSimSuccess);
      // 整体成功状态：至少基础连接成功
      const isSuccess = basicConnectSuccess;

      return {
        isFullSuccess,
        isPartialSuccess,
        isSuccess,
      };
    }

    /**
     * 生成状态消息
     */
    generateStatusMessage() {
      const status = this.calculateStatus();

      if (status.isFullSuccess) {
        return "连接测试完全成功";
      } else if (status.isPartialSuccess) {
        return "连接测试部分成功";
      } else {
        return "连接测试失败";
      }
    }

    /**
     * 生成简洁的状态消息
     */
    generateDetailsMessage() {
      const details = [];

      // 读权限状态 - 简洁显示
      if (this.result.read?.success) {
        details.push("✓ 读权限正常");
      } else {
        details.push("✗ 读权限失败");
        if (this.result.read?.error) {
          details.push(`  ${this.result.read.error.split("\n")[0]}`);
        }
      }

      // 写权限状态 - 简洁显示
      if (this.result.write?.success) {
        details.push("✓ 写权限正常");
      } else {
        details.push("✗ 写权限失败");
        if (this.result.write?.error) {
          details.push(`  ${this.result.write.error.split("\n")[0]}`);
        }
      }

      // CORS配置状态 - 简洁显示
      if (this.result.cors?.success) {
        details.push("✓ CORS配置正确");
      } else {
        details.push("✗ CORS配置有问题");
        if (this.result.cors?.error) {
          details.push(`  ${this.result.cors.error.split("\n")[0]}`);
        }
      }

      return details.join("\n");
    }
  }

  /**
   * 测试S3配置连接
   */
  const testConnection = async (configId) => {
    try {
      testResults.value[configId] = { loading: true };
      const response = await api.storage.testS3Config(configId);

      // 使用测试结果处理器
      const processor = new TestResultProcessor(response.data?.result || {});
      const status = processor.calculateStatus();

      testResults.value[configId] = {
        success: status.isFullSuccess,
        partialSuccess: status.isPartialSuccess,
        message: processor.generateStatusMessage(),
        details: processor.generateDetailsMessage(),
        result: response.data?.result || {},
        loading: false,
      };
    } catch (err) {
      testResults.value[configId] = {
        success: false,
        partialSuccess: false,
        message: "测试连接失败",
        details: err.message || "无法连接到服务器",
        loading: false,
      };
    }
  };

  /**
   * 显示测试结果详情
   */
  const showTestDetailsModal = (configId) => {
    selectedTestResult.value = testResults.value[configId];
    showTestDetails.value = true;
    showDetailedResults.value = true;
  };

  /**
   * 获取提供商图标
   */
  const getProviderIcon = (providerType) => {
    switch (providerType) {
      case "Cloudflare R2":
        return "M11 16.5l11 7v-14.5m-11 7.5v-13l-11 6.5 11 6.5z";
      case "Backblaze B2":
        return "M4 4v16a2 2 0 002 2h12a2 2 0 002-2V8.342a2 2 0 00-.602-1.43l-4.44-4.342A2 2 0 0013.56 2H6a2 2 0 00-2 2zm5 9v-3a1 1 0 011-1h4a1 1 0 011 1v3a1 1 0 01-1 1h-4a1 1 0 01-1-1z";
      case "AWS S3":
        return "M5 16.577l2.194-2.195 2.194 2.195L5 20.772l-4.388-4.195 2.194-2.195 2.194 2.195zM5 4.822l2.194 2.195L5 9.211 2.806 7.017 5 4.822zM12 0l2.194 2.195L12 4.389 9.806 2.195 12 0zM5 11.211l2.194 2.195-2.194 2.194-2.194-2.194L5 11.211zM12 7.017l4.389-4.195 4.388 4.195-4.388 4.194-4.389-4.194z";
      case "Aliyun OSS":
        return "M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm-2 15l-5-5 1.41-1.41L10 14.17l7.59-7.59L19 8l-9 9z";
      default:
        return "M3 19h18a2 2 0 002-2V7a2 2 0 00-2-2H5a2 2 0 00-2 2v10a2 2 0 002 2z";
    }
  };

  return {
    // 继承基础功能
    ...base,

    // 重写分页选项为S3配置专用
    pageSizeOptions,

    // S3配置特有状态
    s3Configs,
    currentConfig,
    showAddForm,
    showEditForm,
    testResults,
    showTestDetails,
    selectedTestResult,
    showDetailedResults,

    // S3配置管理方法
    loadS3Configs,
    handlePageChange,
    handleLimitChange,
    handleDeleteConfig,
    editConfig,
    addNewConfig,
    handleFormSuccess,
    handleSetDefaultConfig,

    // 测试功能方法
    testConnection,
    showTestDetailsModal,

    // 工具方法
    getProviderIcon,
  };
}
