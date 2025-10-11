<script setup>
import { ref, computed, watch, shallowRef } from "vue";
import { h } from "vue"; //递归组件
import { useI18n } from "vue-i18n";
import { api } from "@/api";
import { Permission, PermissionChecker } from "@/constants/permissions.js";

// 目录缓存对象，用于存储已加载的目录内容
const directoryCache = shallowRef(new Map());

// 创建目录项组件，用于递归展示目录树
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

    // 计算是否被选中
    const isSelected = computed(() => {
      return props.currentPath === props.item.path + "/";
    });

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
        // 调用API获取目录内容
        let dirItems = [];

        // 如果是根目录，使用mountsList作为子目录
        if (props.item.path === "/") {
          dirItems = mountsList.value.map((mount) => ({
            name: mount.name,
            path: mount.mount_path,
            isDirectory: true,
          }));
        } else {
          // 对于其他路径，调用统一API获取子目录
          const response = await api.fs.getDirectoryList(props.item.path);
          if (response.success && response.data && response.data.items) {
            // 获取目录项，并只保留目录类型
            dirItems = response.data.items
              .filter((item) => item.isDirectory)
              .map((item) => ({
                name: item.name,
                path: props.item.path + "/" + item.name,
                isDirectory: true,
              }));

            // 确保路径格式正确（避免双斜杠）
            dirItems.forEach((item) => {
              item.path = item.path.replace(/\/\//g, "/");
            });
          }
        }

        // 更新组件状态
        children.value = dirItems;

        // 更新缓存
        directoryCache.value.set(cacheKey, dirItems);
      } catch (error) {
        console.error("加载目录失败:", error);
        children.value = [];
      } finally {
        loading.value = false;
      }
    };

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
              style: { paddingLeft: `${this.level * 0.75 + 0.5}rem` },
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
                    h("span", { class: "text-xs" }, "加载中..."),
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
                      level: this.level + 1,
                      onSelect: (path) => this.$emit("select", path),
                    }),
                  ])
                ),
          ])
        : null,
    ]);
  },
};

// i18n
const { t } = useI18n();

// Props
const props = defineProps({
  darkMode: {
    type: Boolean,
    required: true,
  },
  keyData: {
    type: Object,
    default: null,
  },
  isEditMode: {
    type: Boolean,
    default: false,
  },
  availableMounts: {
    type: Array,
    default: () => [],
  },
});

// Emits
const emit = defineEmits(["close", "updated", "created"]);

// 标签页状态
const activeTab = ref("basic"); // 'basic' 或 'path'

// Reactive state
const isLoading = ref(false);
const error = ref(null);
const keyName = ref("");
const customKey = ref("");
const useCustomKey = ref(false);
const expiration = ref("1d");
const customExpiration = ref("");
// 权限状态 - 支持所有 9 种权限
const permissions = ref({
  // 基础权限
  text: false,
  file_share: false,

  // 挂载页权限
  mount_view: false,
  mount_upload: false,
  mount_copy: false,
  mount_rename: false,
  mount_delete: false,

  // WebDAV权限
  webdav_read: false,
  webdav_manage: false,
});

const basicPath = ref("/");

// 路径选择器相关状态
const isLoadingMounts = ref(false);
const mountsList = ref([]);
const selectedPath = ref("/");
const rootDirectories = shallowRef([]); // 添加根目录列表状态

// 自定义过期时间选项
const expirationOptions = computed(() => {
  const baseKey = props.isEditMode ? "admin.keyManagement.editModal.expirationOptions" : "admin.keyManagement.createModal.expirationOptions";
  return [
    { value: "1d", label: t(`${baseKey}.1d`, "1天") },
    { value: "7d", label: t(`${baseKey}.7d`, "7天") },
    { value: "30d", label: t(`${baseKey}.30d`, "30天") },
    { value: "never", label: t(`${baseKey}.never`, "永不过期") },
    { value: "custom", label: t(`${baseKey}.custom`, "自定义") },
  ];
});

// 计算属性：是否为自定义过期时间
const isCustomExpiration = computed(() => {
  return expiration.value === "custom";
});

// 计算表单标题
const formTitle = computed(() => {
  return props.isEditMode ? t("admin.keyManagement.editModal.title", "编辑API密钥") : t("admin.keyManagement.createModal.title", "创建新API密钥");
});

// 计算保存按钮文本
const saveButtonText = computed(() => {
  return props.isEditMode ? t("admin.keyManagement.editModal.update", "更新") : t("admin.keyManagement.createModal.create", "创建");
});

// 计算处理中按钮文本
const processingButtonText = computed(() => {
  return props.isEditMode ? t("admin.keyManagement.editModal.processing", "更新中...") : t("admin.keyManagement.createModal.processing", "创建中...");
});

// 验证自定义密钥格式
const validateCustomKey = (key) => {
  if (!key) return false;
  const keyFormatRegex = /^[a-zA-Z0-9_-]+$/;
  return keyFormatRegex.test(key);
};

// 权限转换辅助函数 - 使用统一的PermissionChecker
const convertPermissionsFromBitFlag = (bitFlag) => {
  return PermissionChecker.convertFromBitFlag(bitFlag);
};

const convertPermissionsToBitFlag = (perms) => {
  return PermissionChecker.convertToBitFlag(perms);
};

// 重置表单 - 将此函数移到watch之前
const resetForm = () => {
  keyName.value = "";
  customKey.value = "";
  useCustomKey.value = false;
  expiration.value = "1d";
  customExpiration.value = "";
  // 重置所有权限
  permissions.value = {
    text: false,
    file_share: false,
    mount_view: false,
    mount_upload: false,
    mount_copy: false,
    mount_rename: false,
    mount_delete: false,
    webdav_read: false,
    webdav_manage: false,
  };
  basicPath.value = "/";
  selectedPath.value = "/";
  error.value = null;
  activeTab.value = "basic";
  // 清除目录缓存
  directoryCache.value.clear();
};

// 监听编辑模式变化，同步表单数据
watch(
  () => props.keyData,
  (newVal) => {
    if (newVal && props.isEditMode) {
      // 填充表单数据
      keyName.value = newVal.name;
      useCustomKey.value = false; // 编辑模式不能修改密钥本身
      customKey.value = "";
      permissions.value = convertPermissionsFromBitFlag(newVal.permissions);
      basicPath.value = newVal.basic_path || "/";
      selectedPath.value = newVal.basic_path || "/";

      // 设置过期时间
      if (newVal.expires_at) {
        // 检查是否是"永不过期"的特殊值
        if (newVal.expires_at === "never" || newVal.expires_at === null) {
          expiration.value = "never";
          customExpiration.value = "";
        } else {
          // 尝试解析日期
          const expiresAt = new Date(newVal.expires_at);

          // 检查日期是否有效
          if (!isNaN(expiresAt.getTime())) {
            // 检查是否是远未来日期（表示永不过期）
            const year = expiresAt.getFullYear();
            if (year >= 9999) {
              expiration.value = "never";
              customExpiration.value = "";
            } else {
              expiration.value = "custom";
              // 转换为本地日期时间格式 yyyy-MM-ddThh:mm
              const month = String(expiresAt.getMonth() + 1).padStart(2, "0");
              const day = String(expiresAt.getDate()).padStart(2, "0");
              const hours = String(expiresAt.getHours()).padStart(2, "0");
              const minutes = String(expiresAt.getMinutes()).padStart(2, "0");
              customExpiration.value = `${year}-${month}-${day}T${hours}:${minutes}`;
            }
          } else {
            // 日期无效，默认为永不过期
            expiration.value = "never";
            customExpiration.value = "";
          }
        }
      } else {
        expiration.value = "never";
        customExpiration.value = "";
      }
    } else {
      // 重置表单为创建模式
      resetForm();
    }
  },
  { immediate: true }
);

// 加载挂载点列表
const loadMounts = async () => {
  if (mountsList.value.length > 0) return; // 如果已经加载过，不再重复加载

  isLoadingMounts.value = true;
  try {
    // 先获取所有公开的S3配置
    let publicS3Configs = [];
    try {
      const s3Result = await api.storage.getAllS3Configs();
      if (s3Result.success && s3Result.data) {
        // 过滤出公开的S3配置
        publicS3Configs = s3Result.data.filter((config) => config.is_public === true || config.is_public === 1);
      }
    } catch (s3Error) {
      console.error("加载S3配置列表失败:", s3Error);
    }

    // 获取所有挂载点
    const result = await api.mount.getMountsList();
    if (result.success && result.data) {
      // 过滤挂载点：必须激活，且如果是S3类型，其配置ID必须在公开配置列表中
      mountsList.value = result.data.filter((mount) => {
        // 确保挂载点处于激活状态
        if (!mount.is_active) {
          return false;
        }

        // 对于S3类型的挂载点，检查其配置ID是否在公开配置列表中
        if (mount.storage_type === "S3" && mount.storage_config_id) {
          return publicS3Configs.some((config) => config.id === mount.storage_config_id);
        }

        // 非S3类型挂载点不显示
        return false;
      });

      // 构建根目录项
      const rootItem = {
        name: t("admin.keyManagement.pathSelector.rootDirectory", "根目录"),
        path: "/",
        isDirectory: true,
      };

      // 初始化根目录列表
      rootDirectories.value = mountsList.value.map((mount) => ({
        name: mount.name,
        path: mount.mount_path,
        isDirectory: true,
      }));
    } else {
      mountsList.value = [];
      rootDirectories.value = [];
    }
  } catch (error) {
    console.error("加载挂载点列表失败:", error);
    mountsList.value = [];
    rootDirectories.value = [];
  } finally {
    isLoadingMounts.value = false;
  }
};

// 切换到路径选择标签页
const switchToPathTab = async () => {
  await loadMounts(); // 确保已加载挂载点列表
  activeTab.value = "path";
};

// 选择路径
const selectPath = (path) => {
  selectedPath.value = path.endsWith("/") ? path : path + "/";
};

// 确认路径选择
const confirmPathSelection = () => {
  basicPath.value = selectedPath.value;
  activeTab.value = "basic"; // 返回基本信息标签页
};

// 处理表单提交
const handleSubmit = async () => {
  // 表单验证
  if (!keyName.value.trim()) {
    error.value = props.isEditMode
      ? t("admin.keyManagement.editModal.errors.nameRequired", "密钥名称不能为空")
      : t("admin.keyManagement.createModal.errors.nameRequired", "密钥名称不能为空");
    return;
  }

  // 验证自定义密钥（如果启用且在创建模式）
  if (!props.isEditMode && useCustomKey.value) {
    if (!customKey.value.trim()) {
      error.value = t("admin.keyManagement.createModal.errors.customKeyRequired", "自定义密钥不能为空");
      return;
    }

    if (!validateCustomKey(customKey.value)) {
      error.value = t("admin.keyManagement.createModal.errors.customKeyFormat", "自定义密钥格式不正确，只能包含字母、数字、下划线和短横线");
      return;
    }
  }

  let expiresAt = null;

  // 处理过期时间
  if (expiration.value !== "never") {
    if (expiration.value === "custom") {
      if (!customExpiration.value) {
        error.value = props.isEditMode
          ? t("admin.keyManagement.editModal.errors.expirationRequired", "自定义过期时间不能为空")
          : t("admin.keyManagement.createModal.errors.expirationRequired", "自定义过期时间不能为空");
        return;
      }

      // 验证自定义日期是否有效
      const customDate = new Date(customExpiration.value);
      if (isNaN(customDate.getTime())) {
        error.value = props.isEditMode
          ? t("admin.keyManagement.editModal.errors.invalidExpiration", "无效的过期时间")
          : t("admin.keyManagement.createModal.errors.invalidExpiration", "无效的过期时间");
        return;
      }

      expiresAt = customDate.toISOString();
    } else {
      // 处理1d, 7d, 30d
      const days = parseInt(expiration.value);
      if (isNaN(days) || days <= 0) {
        error.value = props.isEditMode
          ? t("admin.keyManagement.editModal.errors.invalidExpiration", "无效的过期时间")
          : t("admin.keyManagement.createModal.errors.invalidExpiration", "无效的过期时间");
        return;
      }

      const date = new Date();
      date.setDate(date.getDate() + days);
      expiresAt = date.toISOString();
    }
  }

  isLoading.value = true;
  error.value = null;

  try {
    if (props.isEditMode) {
      // 更新密钥 - 使用新的位标志权限系统
      const updateData = {
        name: keyName.value,
        permissions: convertPermissionsToBitFlag(permissions.value),
        basic_path: basicPath.value,
      };

      if (expiresAt) {
        updateData.expires_at = expiresAt;
      } else if (expiration.value === "never") {
        updateData.expires_at = "never";
      }

      const result = await api.admin.updateApiKey(props.keyData.id, updateData);

      if (result.success) {
        emit("updated", {
          ...props.keyData,
          ...updateData,
          expires_at: expiresAt,
        });
      } else {
        error.value = result.message || t("admin.keyManagement.editModal.errors.updateFailed", "更新密钥失败");
      }
    } else {
      // 创建密钥 - 使用新的位标志权限系统
      const customKeyValue = useCustomKey.value ? customKey.value : null;
      const permissionsBitFlag = convertPermissionsToBitFlag(permissions.value);
      const result = await api.admin.createApiKey(keyName.value, expiresAt, permissionsBitFlag, "GENERAL", customKeyValue, basicPath.value, false);

      if (result.success && result.data) {
        emit(
          "created",
          {
            id: result.data.id,
            name: result.data.name,
            key: result.data.key,
            key_masked: result.data.key.substring(0, 6) + "...",
            permissions: result.data.permissions,
            role: result.data.role,
            basic_path: basicPath.value,
            is_guest: result.data.is_guest,
            created_at: result.data.created_at,
            expires_at: expiresAt,
            last_used: null,
          },
          result.data.key
        );
        resetForm();
      } else {
        error.value = result.message || t("admin.keyManagement.createModal.errors.createFailed", "创建密钥失败");
      }
    }
  } catch (e) {
    console.error("API密钥操作失败:", e);
    error.value = props.isEditMode
      ? t("admin.keyManagement.editModal.errors.updateFailed", "更新密钥失败")
      : t("admin.keyManagement.createModal.errors.createFailed", "创建密钥失败");
  } finally {
    isLoading.value = false;
  }
};

// 取消并关闭表单
const handleCancel = () => {
  emit("close");
};

// 对外暴露方法
defineExpose({
  resetForm,
  setBasicPath: (path) => {
    basicPath.value = path;
    selectedPath.value = path;
  },
});
</script>

<template>
  <div
    class="relative rounded-lg shadow-xl w-full max-h-[85vh] sm:max-h-[85vh] overflow-hidden max-w-sm sm:max-w-xl flex flex-col"
    :class="darkMode ? 'bg-gray-800 text-white' : 'bg-white text-gray-800'"
  >
    <!-- 弹窗头部带关闭按钮 -->
    <div class="px-4 py-3 sm:py-4 border-b flex justify-between items-center sticky top-0 z-10" :class="[darkMode ? 'border-gray-700 bg-gray-800' : 'border-gray-200 bg-white']">
      <h3 class="text-lg leading-6 font-medium" :class="darkMode ? 'text-white' : 'text-gray-900'">
        {{ formTitle }}
      </h3>
      <button
        @click="handleCancel"
        class="rounded-md p-1 inline-flex items-center justify-center"
        :class="darkMode ? 'text-gray-400 hover:text-gray-200' : 'text-gray-500 hover:text-gray-700'"
      >
        <span class="sr-only">{{ $t("admin.keyManagement.createModal.close") }}</span>
        <svg xmlns="http://www.w3.org/2000/svg" class="h-5 w-5" viewBox="0 0 20 20" fill="currentColor">
          <path
            fill-rule="evenodd"
            d="M4.293 4.293a1 1 0 011.414 0L10 8.586l4.293-4.293a1 1 0 111.414 1.414L11.414 10l4.293 4.293a1 1 0 01-1.414 1.414L10 11.414l-4.293 4.293a1 1 0 01-1.414-1.414L8.586 10 4.293 5.707a1 1 0 010-1.414z"
            clip-rule="evenodd"
          />
        </svg>
      </button>
    </div>

    <!-- 标签导航 -->
    <div class="border-b" :class="darkMode ? 'border-gray-700' : 'border-gray-200'">
      <nav class="flex">
        <button
          @click="activeTab = 'basic'"
          class="px-4 py-2 text-sm font-medium"
          :class="[
            activeTab === 'basic'
              ? darkMode
                ? 'border-b-2 border-primary-500 text-primary-400'
                : 'border-b-2 border-primary-500 text-primary-600'
              : darkMode
              ? 'text-gray-400 hover:text-gray-200'
              : 'text-gray-500 hover:text-gray-700',
          ]"
        >
          {{ $t(isEditMode ? "admin.keyManagement.editModal.tabs.basic" : "admin.keyManagement.createModal.tabs.basic", "基本信息") }}
        </button>
        <button
          v-if="permissions.mount_view"
          @click="switchToPathTab"
          class="px-4 py-2 text-sm font-medium"
          :class="[
            activeTab === 'path'
              ? darkMode
                ? 'border-b-2 border-primary-500 text-primary-400'
                : 'border-b-2 border-primary-500 text-primary-600'
              : darkMode
              ? 'text-gray-400 hover:text-gray-200'
              : 'text-gray-500 hover:text-gray-700',
          ]"
        >
          {{ $t(isEditMode ? "admin.keyManagement.editModal.tabs.path" : "admin.keyManagement.createModal.tabs.path", "路径选择") }}
        </button>
      </nav>
    </div>

    <!-- 弹窗内容区 - 增加滚动条 -->
    <div class="px-3 sm:px-4 py-3 sm:py-4 overflow-y-auto flex-1">
      <!-- 基本信息标签页 -->
      <div v-if="activeTab === 'basic'" class="space-y-4">
        <!-- 密钥名称 -->
        <div>
          <label :for="isEditMode ? 'edit-key-name' : 'key-name'" class="block text-sm font-medium mb-1" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">
            {{ $t(isEditMode ? "admin.keyManagement.editModal.keyName" : "admin.keyManagement.createModal.keyName") }}
          </label>
          <input
            :id="isEditMode ? 'edit-key-name' : 'key-name'"
            v-model="keyName"
            type="text"
            :placeholder="$t(isEditMode ? 'admin.keyManagement.editModal.keyNamePlaceholder' : 'admin.keyManagement.createModal.keyNamePlaceholder')"
            class="w-full p-2 rounded-md border"
            :class="darkMode ? 'bg-gray-700 border-gray-600 text-white' : 'bg-white border-gray-300 text-gray-800'"
          />
          <p class="mt-1 text-sm" :class="darkMode ? 'text-gray-400' : 'text-gray-500'">
            {{ $t(isEditMode ? "admin.keyManagement.editModal.keyNameHelp" : "admin.keyManagement.createModal.keyNameHelp") }}
          </p>
        </div>

        <!-- 自定义密钥 - 仅在创建模式显示 -->
        <div v-if="!isEditMode" class="space-y-2">
          <div class="flex items-center space-x-2">
            <input
              id="use-custom-key"
              v-model="useCustomKey"
              type="checkbox"
              class="h-4 w-4 rounded"
              :class="darkMode ? 'bg-gray-700 border-gray-600 text-primary-600' : 'bg-white border-gray-300 text-primary-500'"
            />
            <label for="use-custom-key" class="text-sm font-medium" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">
              {{ $t("admin.keyManagement.createModal.useCustomKey") }}
            </label>
          </div>

          <div v-if="useCustomKey">
            <label for="custom-key" class="block text-sm font-medium mb-1" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">
              {{ $t("admin.keyManagement.createModal.customKey") }}
            </label>
            <input
              id="custom-key"
              v-model="customKey"
              type="text"
              :placeholder="$t('admin.keyManagement.createModal.customKeyPlaceholder')"
              class="w-full p-2 rounded-md border"
              :class="darkMode ? 'bg-gray-700 border-gray-600 text-white' : 'bg-white border-gray-300 text-gray-800'"
            />
            <p class="mt-1 text-sm" :class="darkMode ? 'text-gray-400' : 'text-gray-500'">
              {{ $t("admin.keyManagement.createModal.customKeyHelp") }}
            </p>
          </div>
        </div>

        <!-- 过期时间 -->
        <div>
          <label :for="isEditMode ? 'edit-key-expiration' : 'key-expiration'" class="block text-sm font-medium mb-1" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">
            {{ $t(isEditMode ? "admin.keyManagement.editModal.expiration" : "admin.keyManagement.createModal.expiration") }}
          </label>
          <select
            :id="isEditMode ? 'edit-key-expiration' : 'key-expiration'"
            v-model="expiration"
            class="w-full p-2 rounded-md border"
            :class="darkMode ? 'bg-gray-700 border-gray-600 text-white' : 'bg-white border-gray-300 text-gray-800'"
          >
            <option v-for="option in expirationOptions" :key="option.value" :value="option.value">
              {{ option.label }}
            </option>
          </select>
        </div>

        <!-- 自定义过期时间 -->
        <div v-if="isCustomExpiration">
          <label :for="isEditMode ? 'edit-custom-expiration' : 'custom-expiration'" class="block text-sm font-medium mb-1" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">
            {{ $t(isEditMode ? "admin.keyManagement.editModal.customExpiration" : "admin.keyManagement.createModal.customExpiration") }}
          </label>
          <input
            :id="isEditMode ? 'edit-custom-expiration' : 'custom-expiration'"
            v-model="customExpiration"
            type="datetime-local"
            class="w-full p-2 rounded-md border"
            :class="darkMode ? 'bg-gray-700 border-gray-600 text-white' : 'bg-white border-gray-300 text-gray-800'"
          />
        </div>

        <!-- 权限设置 -->
        <div class="space-y-4">
          <!-- 基础权限 -->
          <div>
            <h4 class="text-sm font-medium mb-2" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">
              {{ $t("admin.keyManagement.permissions.basic", "基础权限") }}
            </h4>
            <div class="grid grid-cols-1 gap-3">
              <!-- 文本权限 -->
              <div class="flex items-center space-x-2">
                <input
                  :id="isEditMode ? 'edit-text-permission' : 'text-permission'"
                  v-model="permissions.text"
                  type="checkbox"
                  class="h-4 w-4 rounded"
                  :class="darkMode ? 'bg-gray-700 border-gray-600 text-primary-600' : 'bg-white border-gray-300 text-primary-500'"
                />
                <label :for="isEditMode ? 'edit-text-permission' : 'text-permission'" class="text-sm font-medium" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">
                  {{ $t("admin.keyManagement.permissions.text", "文本分享") }}
                </label>
              </div>

              <!-- 文件权限 -->
              <div class="flex items-center space-x-2">
                <input
                  :id="isEditMode ? 'edit-file-permission' : 'file-permission'"
                  v-model="permissions.file_share"
                  type="checkbox"
                  class="h-4 w-4 rounded"
                  :class="darkMode ? 'bg-gray-700 border-gray-600 text-primary-600' : 'bg-white border-gray-300 text-primary-500'"
                />
                <label :for="isEditMode ? 'edit-file-permission' : 'file-permission'" class="text-sm font-medium" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">
                  {{ $t("admin.keyManagement.permissions.file_share", "文件分享") }}
                </label>
              </div>
            </div>
          </div>

          <!-- 挂载页权限 -->
          <div>
            <h4 class="text-sm font-medium mb-2" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">
              {{ $t("admin.keyManagement.permissions.mount", "挂载页权限") }}
            </h4>
            <div class="grid grid-cols-2 sm:grid-cols-3 gap-3">
              <!-- 查看权限 -->
              <div class="flex items-center space-x-2">
                <input
                  :id="isEditMode ? 'edit-mount-view' : 'mount-view'"
                  v-model="permissions.mount_view"
                  type="checkbox"
                  class="h-4 w-4 rounded"
                  :class="darkMode ? 'bg-gray-700 border-gray-600 text-primary-600' : 'bg-white border-gray-300 text-primary-500'"
                />
                <label :for="isEditMode ? 'edit-mount-view' : 'mount-view'" class="text-sm font-medium" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">
                  {{ $t("admin.keyManagement.permissions.mount_view", "查看") }}
                </label>
              </div>

              <!-- 上传权限 -->
              <div class="flex items-center space-x-2">
                <input
                  :id="isEditMode ? 'edit-mount-upload' : 'mount-upload'"
                  v-model="permissions.mount_upload"
                  type="checkbox"
                  class="h-4 w-4 rounded"
                  :class="darkMode ? 'bg-gray-700 border-gray-600 text-primary-600' : 'bg-white border-gray-300 text-primary-500'"
                />
                <label :for="isEditMode ? 'edit-mount-upload' : 'mount-upload'" class="text-sm font-medium" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">
                  {{ $t("admin.keyManagement.permissions.mount_upload", "上传") }}
                </label>
              </div>

              <!-- 复制权限 -->
              <div class="flex items-center space-x-2">
                <input
                  :id="isEditMode ? 'edit-mount-copy' : 'mount-copy'"
                  v-model="permissions.mount_copy"
                  type="checkbox"
                  class="h-4 w-4 rounded"
                  :class="darkMode ? 'bg-gray-700 border-gray-600 text-primary-600' : 'bg-white border-gray-300 text-primary-500'"
                />
                <label :for="isEditMode ? 'edit-mount-copy' : 'mount-copy'" class="text-sm font-medium" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">
                  {{ $t("admin.keyManagement.permissions.mount_copy", "复制") }}
                </label>
              </div>

              <!-- 重命名权限 -->
              <div class="flex items-center space-x-2">
                <input
                  :id="isEditMode ? 'edit-mount-rename' : 'mount-rename'"
                  v-model="permissions.mount_rename"
                  type="checkbox"
                  class="h-4 w-4 rounded"
                  :class="darkMode ? 'bg-gray-700 border-gray-600 text-primary-600' : 'bg-white border-gray-300 text-primary-500'"
                />
                <label :for="isEditMode ? 'edit-mount-rename' : 'mount-rename'" class="text-sm font-medium" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">
                  {{ $t("admin.keyManagement.permissions.mount_rename", "重命名") }}
                </label>
              </div>

              <!-- 删除权限 -->
              <div class="flex items-center space-x-2">
                <input
                  :id="isEditMode ? 'edit-mount-delete' : 'mount-delete'"
                  v-model="permissions.mount_delete"
                  type="checkbox"
                  class="h-4 w-4 rounded"
                  :class="darkMode ? 'bg-gray-700 border-gray-600 text-primary-600' : 'bg-white border-gray-300 text-primary-500'"
                />
                <label :for="isEditMode ? 'edit-mount-delete' : 'mount-delete'" class="text-sm font-medium" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">
                  {{ $t("admin.keyManagement.permissions.mount_delete", "删除") }}
                </label>
              </div>
            </div>
          </div>

          <!-- WebDAV权限 -->
          <div>
            <h4 class="text-sm font-medium mb-2" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">
              {{ $t("admin.keyManagement.permissions.webdav", "WebDAV权限") }}
            </h4>
            <div class="grid grid-cols-1 gap-3">
              <!-- WebDAV读取权限 -->
              <div class="flex items-center space-x-2">
                <input
                  :id="isEditMode ? 'edit-webdav-read' : 'webdav-read'"
                  v-model="permissions.webdav_read"
                  type="checkbox"
                  class="h-4 w-4 rounded"
                  :class="darkMode ? 'bg-gray-700 border-gray-600 text-primary-600' : 'bg-white border-gray-300 text-primary-500'"
                />
                <label :for="isEditMode ? 'edit-webdav-read' : 'webdav-read'" class="text-sm font-medium" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">
                  {{ $t("admin.keyManagement.permissions.webdav_read", "WebDAV读取") }}
                </label>
              </div>

              <!-- WebDAV管理权限 -->
              <div class="flex items-center space-x-2">
                <input
                  :id="isEditMode ? 'edit-webdav-manage' : 'webdav-manage'"
                  v-model="permissions.webdav_manage"
                  type="checkbox"
                  class="h-4 w-4 rounded"
                  :class="darkMode ? 'bg-gray-700 border-gray-600 text-primary-600' : 'bg-white border-gray-300 text-primary-500'"
                />
                <label :for="isEditMode ? 'edit-webdav-manage' : 'webdav-manage'" class="text-sm font-medium" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">
                  {{ $t("admin.keyManagement.permissions.webdav_manage", "WebDAV管理") }}
                </label>
              </div>
            </div>
          </div>
        </div>

        <!-- 基本路径 -->
        <div v-if="permissions.mount_view" class="mt-2">
          <label :for="isEditMode ? 'edit-basic-path' : 'basic-path'" class="block text-sm font-medium mb-1" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">
            {{ $t(isEditMode ? "admin.keyManagement.editModal.basicPath" : "admin.keyManagement.createModal.basicPath", "基本路径") }}
          </label>
          <div class="flex">
            <input
              :id="isEditMode ? 'edit-basic-path' : 'basic-path'"
              v-model="basicPath"
              type="text"
              :placeholder="$t(isEditMode ? 'admin.keyManagement.editModal.basicPathPlaceholder' : 'admin.keyManagement.createModal.basicPathPlaceholder', '/')"
              class="w-full p-2 rounded-l-md border"
              :class="darkMode ? 'bg-gray-700 border-gray-600 text-white' : 'bg-white border-gray-300 text-gray-800'"
              readonly
            />
            <button
              @click="switchToPathTab"
              class="px-2 py-0 rounded-r-md text-white h-[42px]"
              :class="darkMode ? 'bg-blue-600 hover:bg-blue-700' : 'bg-blue-500 hover:bg-blue-600'"
              :title="$t(isEditMode ? 'admin.keyManagement.editModal.selectPath' : 'admin.keyManagement.createModal.selectPath', '选择路径')"
            >
              <svg xmlns="http://www.w3.org/2000/svg" class="h-5 w-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2">
                <path stroke-linecap="round" stroke-linejoin="round" d="M3 7v10a2 2 0 002 2h14a2 2 0 002-2V9a2 2 0 00-2-2h-6l-2-2H5a2 2 0 00-2 2z" />
              </svg>
            </button>
          </div>
          <p class="mt-1 text-sm" :class="darkMode ? 'text-gray-400' : 'text-gray-500'">
            {{ $t(isEditMode ? "admin.keyManagement.editModal.basicPathHelp" : "admin.keyManagement.createModal.basicPathHelp", "设置API密钥可访问的基本路径，默认为根路径") }}
          </p>
        </div>

        <!-- 提示信息 -->
        <div class="p-3 rounded-md text-sm" :class="darkMode ? 'bg-gray-700 text-gray-300' : 'bg-gray-100 text-gray-700'">
          <p class="font-medium mb-1">{{ $t(isEditMode ? "admin.keyManagement.editModal.securityTip" : "admin.keyManagement.createModal.securityTip", "安全提示") }}</p>
          <p>
            {{
              $t(
                isEditMode ? "admin.keyManagement.editModal.securityMessage" : "admin.keyManagement.createModal.securityMessage",
                "请妥善保管您的API密钥，不要在公共场所或不安全的环境中使用。"
              )
            }}
          </p>
        </div>
      </div>

      <!-- 路径选择标签页 -->
      <div v-else-if="activeTab === 'path'" class="space-y-4">
        <!-- 当前路径 -->
        <div class="mb-3 text-sm font-medium" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">
          {{ $t(isEditMode ? "admin.keyManagement.editModal.pathSelector.currentPath" : "admin.keyManagement.createModal.pathSelector.currentPath", "当前选择") }}:
          <span class="font-bold">{{ selectedPath }}</span>
        </div>

        <!-- 目录树 -->
        <div class="border rounded-md overflow-hidden mb-4 h-64" :class="darkMode ? 'border-gray-700' : 'border-gray-300'">
          <!-- 加载状态 -->
          <div v-if="isLoadingMounts" class="h-full flex justify-center items-center" :class="darkMode ? 'text-gray-400' : 'text-gray-500'">
            <svg class="animate-spin h-5 w-5 mr-2" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
              <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
              <path
                class="opacity-75"
                fill="currentColor"
                d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"
              ></path>
            </svg>
            <span>{{ $t(isEditMode ? "admin.keyManagement.editModal.pathSelector.loading" : "admin.keyManagement.createModal.pathSelector.loading", "加载中...") }}</span>
          </div>

          <div v-else class="h-full overflow-y-auto p-1">
            <!-- 目录树结构 -->
            <div class="file-tree">
              <!-- 根目录 -->
              <div class="tree-item" :class="{ selected: selectedPath === '/' }" @click="selectPath('/')">
                <div
                  class="flex items-center py-2 px-2 rounded hover:bg-gray-100 dark:hover:bg-gray-700 cursor-pointer"
                  :class="{ 'bg-blue-50 dark:bg-blue-900/30': selectedPath === '/' }"
                >
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
                  <span class="truncate" :class="[darkMode ? 'text-gray-200' : 'text-gray-700', selectedPath === '/' ? 'font-medium text-blue-600 dark:text-blue-400' : '']">
                    {{ $t(isEditMode ? "admin.keyManagement.editModal.pathSelector.rootDirectory" : "admin.keyManagement.createModal.pathSelector.rootDirectory", "根目录") }}
                  </span>
                </div>
              </div>

              <!-- 递归渲染目录树 -->
              <div v-for="item in rootDirectories" :key="item.path" class="folder-item">
                <DirectoryItemVue :item="item" :current-path="selectedPath" :dark-mode="darkMode" :level="0" @select="selectPath" />
              </div>
            </div>
          </div>
        </div>

        <!-- 错误信息 - 路径标签页版本 -->
        <div v-if="error" class="p-3 rounded-md bg-red-50 dark:bg-red-900/30 border border-red-200 dark:border-red-800 text-red-800 dark:text-red-300 text-sm mb-4">
          {{ error }}
        </div>

        <!-- 路径选择确认按钮 -->
        <div class="flex justify-end">
          <button
            @click="confirmPathSelection"
            class="px-3 py-1.5 text-sm rounded-md text-white"
            :class="darkMode ? 'bg-blue-600 hover:bg-blue-700' : 'bg-blue-500 hover:bg-blue-600'"
          >
            {{ $t(isEditMode ? "admin.keyManagement.editModal.pathSelector.confirm" : "admin.keyManagement.createModal.pathSelector.confirm", "确认路径") }}
          </button>
        </div>
      </div>

      <!-- 错误信息 - 基本信息标签页版本 -->
      <div
        v-if="activeTab === 'basic' && error"
        class="p-3 rounded-md bg-red-50 dark:bg-red-900/30 border border-red-200 dark:border-red-800 text-red-800 dark:text-red-300 text-sm mt-4"
      >
        {{ error }}
      </div>
    </div>

    <!-- 固定按钮区域 -->
    <div
      class="px-3 sm:px-4 py-2 sm:py-3 border-t transition-colors duration-200 flex flex-col-reverse sm:flex-row sm:justify-end sm:space-x-3 space-y-2 space-y-reverse sm:space-y-0"
      :class="darkMode ? 'border-gray-700 bg-gray-800' : 'border-gray-200 bg-white'"
    >
      <button
        @click="handleCancel"
        class="w-full sm:w-auto px-4 py-2 rounded-md text-sm font-medium transition-colors duration-200"
        :class="darkMode ? 'bg-gray-700 hover:bg-gray-600 text-gray-200' : 'bg-gray-200 hover:bg-gray-300 text-gray-700'"
        :disabled="isLoading"
      >
        {{ $t(isEditMode ? "admin.keyManagement.editModal.cancel" : "admin.keyManagement.createModal.cancel") }}
      </button>
      <button
        @click="handleSubmit"
        type="button"
        class="w-full sm:w-auto flex justify-center items-center px-4 py-2 rounded-md text-sm font-medium transition-colors duration-200 text-white"
        :class="[isLoading ? 'opacity-70 cursor-not-allowed' : 'hover:bg-primary-600', darkMode ? 'bg-primary-600' : 'bg-primary-500']"
        :disabled="isLoading"
      >
        <span v-if="isLoading">{{ processingButtonText }}</span>
        <span v-else>{{ saveButtonText }}</span>
      </button>
    </div>
  </div>
</template>

<style scoped>
/* 路径选择器样式 */
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
