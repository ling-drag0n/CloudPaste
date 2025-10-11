<script setup>
import { ref, computed, watch } from "vue";
import { api } from "@/api";

// 接收属性
const props = defineProps({
  darkMode: {
    type: Boolean,
    required: true,
  },
  config: {
    type: Object,
    default: null,
  },
  isEdit: {
    type: Boolean,
    default: false,
  },
});

// 定义事件
const emit = defineEmits(["close", "success"]);

// 表单数据
const formData = ref({
  name: "",
  provider_type: "Cloudflare R2",
  endpoint_url: "",
  bucket_name: "",
  region: "",
  access_key_id: "",
  secret_access_key: "",
  path_style: false,
  default_folder: "",
  is_public: false,
  total_storage_bytes: null,
  custom_host: "",
  signature_expires_in: 3600,
});

// 提供商列表
const providerTypes = [
  { value: "Cloudflare R2", label: "Cloudflare R2" },
  { value: "Backblaze B2", label: "Backblaze B2" },
  { value: "AWS S3", label: "AWS S3" },
  { value: "Aliyun OSS", label: "阿里云OSS" },
  { value: "Other", label: "其他S3兼容服务" },
];

// 存储容量单位列表
const storageUnits = [
  { value: 1, label: "B" },
  { value: 1024, label: "KB" },
  { value: 1024 * 1024, label: "MB" },
  { value: 1024 * 1024 * 1024, label: "GB" },
  { value: 1024 * 1024 * 1024 * 1024, label: "TB" },
];

// 存储容量相关变量
const storageSize = ref("");
const storageUnit = ref(1024 * 1024 * 1024); // 默认单位为GB

// 获取默认存储容量（根据提供商类型）
const getDefaultStorageByProvider = (provider) => {
  switch (provider) {
    case "Cloudflare R2":
      return 10 * 1024 * 1024 * 1024; // 10GB
    case "Backblaze B2":
      return 10 * 1024 * 1024 * 1024; // 10GB
    case "Aliyun OSS":
      return 5 * 1024 * 1024 * 1024; // 5GB
    default:
      return 5 * 1024 * 1024 * 1024; // 5GB
  }
};

// 转换存储字节为可读格式并设置相应的值
const setStorageSizeFromBytes = (bytes) => {
  if (!bytes || bytes <= 0) {
    storageSize.value = "";
    return;
  }

  // 找到合适的单位
  let unitIndex = 0;
  while (bytes >= 1024 && unitIndex < storageUnits.length - 1) {
    bytes /= 1024;
    unitIndex++;
  }

  storageSize.value = bytes.toFixed(2);
  storageUnit.value = storageUnits[unitIndex].value;
};

// 计算存储容量字节数
const calculateStorageBytes = () => {
  if (!storageSize.value || isNaN(storageSize.value) || storageSize.value <= 0) {
    formData.value.total_storage_bytes = null;
    return;
  }
  formData.value.total_storage_bytes = Math.floor(parseFloat(storageSize.value) * storageUnit.value);
};

// 表单状态
const loading = ref(false);
const error = ref("");
const success = ref("");

// 计算表单标题
const formTitle = computed(() => {
  return props.isEdit ? "编辑S3存储配置" : "添加S3存储配置";
});

// 输入处理函数
const trimInput = (field) => {
  if (formData.value[field]) {
    formData.value[field] = formData.value[field].trim();
  }
};

const formatUrl = (field) => {
  if (formData.value[field]) {
    let url = formData.value[field].trim();

    // 移除末尾的斜杠
    url = url.replace(/\/+$/, "");

    formData.value[field] = url;
  }
};

const formatBucketName = () => {
  if (formData.value.bucket_name) {
    // 只去除空格
    formData.value.bucket_name = formData.value.bucket_name.trim();
  }
};

// URL格式验证
const isValidUrl = (url) => {
  if (!url) return true; // 空值由required验证处理
  try {
    const urlObj = new URL(url);
    return urlObj.protocol === "https:" || urlObj.protocol === "http:";
  } catch {
    return false;
  }
};

// 表单验证
const formValid = computed(() => {
  // 基本必填字段检查
  const basicFieldsValid = formData.value.name && formData.value.provider_type && formData.value.endpoint_url && formData.value.bucket_name;

  // URL格式验证
  const urlValid = isValidUrl(formData.value.endpoint_url) && isValidUrl(formData.value.custom_host);

  // 编辑模式下不要求密钥字段必填
  if (props.isEdit) {
    return basicFieldsValid && urlValid;
  }

  // 新建模式下需要检查密钥字段
  return basicFieldsValid && urlValid && formData.value.access_key_id && formData.value.secret_access_key;
});

// 根据提供商类型预填默认端点
const updateEndpoint = () => {
  const type = formData.value.provider_type;

  if (formData.value.endpoint_url) {
    return; // 如果已有值，不覆盖
  }

  switch (type) {
    case "Cloudflare R2":
      formData.value.endpoint_url = "https://<accountid>.r2.cloudflarestorage.com";
      formData.value.region = "auto";
      formData.value.path_style = false;
      break;
    case "Backblaze B2":
      formData.value.endpoint_url = "https://s3.us-west-000.backblazeb2.com";
      formData.value.region = "";
      formData.value.path_style = true;
      break;
    case "AWS S3":
      formData.value.endpoint_url = "https://s3.amazonaws.com";
      formData.value.path_style = false;
      break;
    case "Aliyun OSS":
      formData.value.endpoint_url = "https://oss-cn-hangzhou.aliyuncs.com";
      formData.value.region = "oss-cn-hangzhou";
      formData.value.path_style = false;
      break;
    default:
      // 其他S3兼容服务使用标准设置
      formData.value.endpoint_url = "https://your-s3-endpoint.com";
      formData.value.path_style = false;
      break;
  }
};

// 监听提供商变化
watch(() => formData.value.provider_type, updateEndpoint);

// 监听编辑的配置变化
watch(
  () => props.config,
  () => {
    if (props.config) {
      // 编辑模式下，复制现有配置到表单
      formData.value.name = props.config.name;
      formData.value.provider_type = props.config.provider_type;
      formData.value.endpoint_url = props.config.endpoint_url;
      formData.value.bucket_name = props.config.bucket_name;
      formData.value.region = props.config.region || "";
      formData.value.default_folder = props.config.default_folder || "";
      formData.value.path_style = props.config.path_style === 1 || props.config.path_style === true;
      formData.value.is_public = props.config.is_public === 1 || props.config.is_public === true;

      // 新增字段
      formData.value.custom_host = props.config.custom_host || "";
      formData.value.signature_expires_in = props.config.signature_expires_in || 3600;

      // 敏感信息在编辑时默认为空，只在主动填写时才提交
      formData.value.access_key_id = "";
      formData.value.secret_access_key = "";

      // 设置存储容量显示
      if (props.config.total_storage_bytes) {
        setStorageSizeFromBytes(props.config.total_storage_bytes);
      } else {
        // 使用默认值
        setStorageSizeFromBytes(getDefaultStorageByProvider(props.config.provider_type));
      }
    } else {
      // 添加模式下重置表单
      formData.value = {
        name: "",
        provider_type: "Cloudflare R2",
        endpoint_url: "",
        bucket_name: "",
        region: "",
        access_key_id: "",
        secret_access_key: "",
        path_style: false,
        default_folder: "",
        is_public: false,
        total_storage_bytes: getDefaultStorageByProvider("Cloudflare R2"),
        custom_host: "",
        signature_expires_in: 3600,
      };
      updateEndpoint();

      // 设置默认存储容量显示
      setStorageSizeFromBytes(formData.value.total_storage_bytes);
    }
  },
  { immediate: true }
);

// 监听provider_type变化，自动设置默认存储容量
watch(
  () => formData.value.provider_type,
  (newProvider) => {
    if (!formData.value.total_storage_bytes) {
      const defaultBytes = getDefaultStorageByProvider(newProvider);
      formData.value.total_storage_bytes = defaultBytes;
      setStorageSizeFromBytes(defaultBytes);
    }
  }
);

// 监听存储大小和单位的变化
watch([storageSize, storageUnit], () => {
  calculateStorageBytes();
});

// 提交表单
const submitForm = async () => {
  if (!formValid.value) {
    error.value = "请填写所有必填字段";
    return;
  }

  loading.value = true;
  error.value = "";
  success.value = "";

  // 确保存储容量字段已更新
  calculateStorageBytes();

  try {
    let response;
    if (props.isEdit && props.config?.id) {
      // 更新现有配置
      // 创建一个新对象，避免修改原始表单数据
      const updateData = { ...formData.value };

      // 如果是编辑模式，且密钥字段为空，则从更新数据中移除这些字段
      // 这样后端将保留原有的密钥值
      if (!updateData.access_key_id || updateData.access_key_id.trim() === "") {
        delete updateData.access_key_id;
      }

      if (!updateData.secret_access_key || updateData.secret_access_key.trim() === "") {
        delete updateData.secret_access_key;
      }

      response = await api.storage.updateS3Config(props.config.id, updateData);
    } else {
      // 创建新配置
      response = await api.storage.createS3Config(formData.value);
    }

    if (response.success) {
      success.value = props.isEdit ? "S3配置更新成功！" : "S3配置创建成功！";
      emit("success", response.data);
      setTimeout(() => {
        emit("close");
      }, 1000);
    } else {
      throw new Error(response.message || "操作失败");
    }
  } catch (err) {
    console.error("S3配置操作失败:", err);
    error.value = err.message || "操作失败，请重试";
  } finally {
    loading.value = false;
  }
};

// 处理关闭模态框
const closeModal = () => {
  emit("close");
};
</script>

<template>
  <div class="fixed inset-0 z-[60] flex items-center justify-center p-2 sm:p-4 pt-20 sm:pt-4 bg-black bg-opacity-50 overflow-y-auto" @click.self="closeModal">
    <div
      class="w-full max-w-md sm:max-w-xl rounded-lg shadow-xl overflow-hidden transition-colors max-h-[85vh] sm:max-h-[80vh] flex flex-col"
      :class="darkMode ? 'bg-gray-800 text-white' : 'bg-white text-gray-900'"
      @click.stop
    >
      <div class="px-4 sm:px-6 py-3 sm:py-4 border-b" :class="darkMode ? 'border-gray-700' : 'border-gray-200'">
        <h2 class="text-base sm:text-lg font-semibold">{{ formTitle }}</h2>
      </div>

      <div class="p-4 sm:p-6 space-y-3 sm:space-y-4 flex-1 overflow-y-auto">
        <div v-if="success" class="p-3 rounded-md text-sm font-medium mb-4 bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-300">
          {{ success }}
        </div>
        <div v-if="error" class="p-3 rounded-md text-sm font-medium mb-4 bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-300">
          {{ error }}
        </div>

        <!-- 表单字段 -->
        <form @submit.prevent="submitForm" class="space-y-6">
          <!-- 基本信息 -->
          <div class="space-y-4">
            <h3 class="text-sm font-medium border-b pb-2" :class="darkMode ? 'text-gray-200 border-gray-600' : 'text-gray-700 border-gray-200'">基本信息</h3>

            <!-- 配置名称 -->
            <div>
              <label for="name" class="block text-sm font-medium mb-1" :class="darkMode ? 'text-gray-200' : 'text-gray-700'"> 配置名称 <span class="text-red-500">*</span> </label>
              <input
                type="text"
                id="name"
                v-model="formData.name"
                required
                @blur="trimInput('name')"
                class="block w-full px-3 py-2 rounded-md text-sm transition-colors duration-200"
                :class="darkMode ? 'bg-gray-700 border-gray-600 text-white placeholder-gray-400' : 'border-gray-300 text-gray-900 placeholder-gray-500'"
                placeholder="例如：我的备份存储"
              />
              <p class="mt-1 text-xs" :class="darkMode ? 'text-gray-400' : 'text-gray-500'">为此配置指定一个易于识别的名称</p>
            </div>

            <div class="grid grid-cols-1 sm:grid-cols-2 gap-4">
              <div>
                <label for="provider_type" class="block text-sm font-medium mb-1" :class="darkMode ? 'text-gray-200' : 'text-gray-700'">
                  提供商类型 <span class="text-red-500">*</span>
                </label>
                <select
                  id="provider_type"
                  v-model="formData.provider_type"
                  required
                  class="block w-full px-3 py-2 rounded-md text-sm transition-colors duration-200"
                  :class="darkMode ? 'bg-gray-700 border-gray-600 text-white' : 'border-gray-300 text-gray-900'"
                >
                  <option v-for="provider in providerTypes" :key="provider.value" :value="provider.value">
                    {{ provider.label }}
                  </option>
                </select>
              </div>

              <div>
                <label for="bucket_name" class="block text-sm font-medium mb-1" :class="darkMode ? 'text-gray-200' : 'text-gray-700'">
                  存储桶名称 <span class="text-red-500">*</span>
                </label>
                <input
                  type="text"
                  id="bucket_name"
                  v-model="formData.bucket_name"
                  required
                  @blur="formatBucketName"
                  class="block w-full px-3 py-2 rounded-md text-sm transition-colors duration-200"
                  :class="darkMode ? 'bg-gray-700 border-gray-600 text-white placeholder-gray-400' : 'border-gray-300 text-gray-900 placeholder-gray-500'"
                  placeholder="my-bucket"
                />
              </div>
            </div>

            <div>
              <label for="storage_size" class="block text-sm font-medium mb-1" :class="darkMode ? 'text-gray-200' : 'text-gray-700'"> 存储容量限制 </label>
              <div class="flex space-x-2">
                <input
                  type="number"
                  id="storage_size"
                  v-model="storageSize"
                  min="0"
                  step="0.01"
                  class="block w-2/3 px-3 py-2 rounded-md text-sm transition-colors duration-200"
                  :class="darkMode ? 'bg-gray-700 border-gray-600 text-white placeholder-gray-400' : 'border-gray-300 text-gray-900 placeholder-gray-500'"
                  placeholder="例如：10"
                />
                <select
                  v-model="storageUnit"
                  class="block w-1/3 px-3 py-2 rounded-md text-sm transition-colors duration-200"
                  :class="darkMode ? 'bg-gray-700 border-gray-600 text-white' : 'border-gray-300 text-gray-900'"
                >
                  <option v-for="unit in storageUnits" :key="unit.value" :value="unit.value">{{ unit.label }}</option>
                </select>
              </div>
              <p class="mt-1 text-xs" :class="darkMode ? 'text-gray-400' : 'text-gray-500'">
                {{ formData.provider_type === "Cloudflare R2" ? "默认为10GB" : formData.provider_type === "Backblaze B2" ? "默认为10GB" : "默认为5GB" }}
              </p>
            </div>
          </div>

          <!-- 连接配置 -->
          <div class="space-y-4">
            <h3 class="text-sm font-medium border-b pb-2" :class="darkMode ? 'text-gray-200 border-gray-600' : 'text-gray-700 border-gray-200'">连接配置</h3>

            <div>
              <label for="endpoint_url" class="block text-sm font-medium mb-1" :class="darkMode ? 'text-gray-200' : 'text-gray-700'">
                端点URL <span class="text-red-500">*</span>
              </label>
              <input
                type="text"
                id="endpoint_url"
                v-model="formData.endpoint_url"
                required
                @blur="formatUrl('endpoint_url')"
                class="block w-full px-3 py-2 rounded-md text-sm transition-colors duration-200"
                :class="[
                  darkMode ? 'bg-gray-700 border-gray-600 text-white placeholder-gray-400' : 'border-gray-300 text-gray-900 placeholder-gray-500',
                  formData.endpoint_url && !isValidUrl(formData.endpoint_url) ? 'border-red-500' : '',
                ]"
                placeholder="https://endpoint.example.com"
              />
              <p v-if="formData.endpoint_url && !isValidUrl(formData.endpoint_url)" class="mt-1 text-xs text-red-500">请输入有效的URL格式，如 https://xxx.com</p>
              <p v-else class="mt-1 text-xs" :class="darkMode ? 'text-gray-400' : 'text-gray-500'">S3 API的完整端点URL，例如：https://endpoint.example.com</p>
            </div>

            <div class="grid grid-cols-1 sm:grid-cols-2 gap-4">
              <div>
                <label for="region" class="block text-sm font-medium mb-1" :class="darkMode ? 'text-gray-200' : 'text-gray-700'"> 区域 </label>
                <input
                  type="text"
                  id="region"
                  v-model="formData.region"
                  @blur="trimInput('region')"
                  class="block w-full px-3 py-2 rounded-md text-sm transition-colors duration-200"
                  :class="darkMode ? 'bg-gray-700 border-gray-600 text-white placeholder-gray-400' : 'border-gray-300 text-gray-900 placeholder-gray-500'"
                  placeholder="us-east-1"
                />
              </div>

              <div>
                <label for="default_folder" class="block text-sm font-medium mb-1" :class="darkMode ? 'text-gray-200' : 'text-gray-700'"> 默认上传路径 </label>
                <input
                  type="text"
                  id="default_folder"
                  v-model="formData.default_folder"
                  @blur="trimInput('default_folder')"
                  class="block w-full px-3 py-2 rounded-md text-sm transition-colors duration-200"
                  :class="darkMode ? 'bg-gray-700 border-gray-600 text-white placeholder-gray-400' : 'border-gray-300 text-gray-900 placeholder-gray-500'"
                  placeholder="如：uploads/ ，留空表示根目录"
                />
              </div>
            </div>
          </div>

          <!-- 认证信息 -->
          <div class="space-y-4">
            <h3 class="text-sm font-medium border-b pb-2" :class="darkMode ? 'text-gray-200 border-gray-600' : 'text-gray-700 border-gray-200'">认证信息</h3>

            <div class="grid grid-cols-1 sm:grid-cols-2 gap-4">
              <div>
                <label for="access_key_id" class="block text-sm font-medium mb-1" :class="darkMode ? 'text-gray-200' : 'text-gray-700'">
                  访问密钥ID <span class="text-red-500">{{ !isEdit ? "*" : "" }}</span>
                </label>
                <input
                  type="text"
                  id="access_key_id"
                  v-model="formData.access_key_id"
                  :required="!isEdit"
                  @blur="trimInput('access_key_id')"
                  class="block w-full px-3 py-2 rounded-md text-sm transition-colors duration-200"
                  :class="darkMode ? 'bg-gray-700 border-gray-600 text-white placeholder-gray-400' : 'border-gray-300 text-gray-900 placeholder-gray-500'"
                  placeholder="AKIAXXXXXXXXXXXXXXXX"
                />
                <p class="mt-1 text-xs" :class="darkMode ? 'text-gray-400' : 'text-gray-500'">
                  {{ isEdit ? "留空表示保持不变" : "S3访问密钥ID" }}
                </p>
              </div>

              <div>
                <label for="secret_access_key" class="block text-sm font-medium mb-1" :class="darkMode ? 'text-gray-200' : 'text-gray-700'">
                  秘密访问密钥 <span class="text-red-500">{{ !isEdit ? "*" : "" }}</span>
                </label>
                <input
                  type="password"
                  id="secret_access_key"
                  autocomplete="new-password"
                  v-model="formData.secret_access_key"
                  :required="!isEdit"
                  @blur="trimInput('secret_access_key')"
                  class="block w-full px-3 py-2 rounded-md text-sm transition-colors duration-200"
                  :class="darkMode ? 'bg-gray-700 border-gray-600 text-white placeholder-gray-400' : 'border-gray-300 text-gray-900 placeholder-gray-500'"
                  placeholder="••••••••••••••••••••••••••••••"
                />
                <p class="mt-1 text-xs" :class="darkMode ? 'text-gray-400' : 'text-gray-500'">
                  {{ isEdit ? "留空表示保持不变" : "S3秘密访问密钥" }}
                </p>
              </div>
            </div>
          </div>

          <!-- 高级配置 -->
          <div class="space-y-4">
            <h3 class="text-sm font-medium border-b pb-2" :class="darkMode ? 'text-gray-200 border-gray-600' : 'text-gray-700 border-gray-200'">高级配置</h3>

            <!-- 自定义域名 -->
            <div>
              <label for="custom_host" class="block text-sm font-medium mb-1" :class="darkMode ? 'text-gray-200' : 'text-gray-700'"> 自定义HOST/CDN域名 </label>
              <input
                type="text"
                id="custom_host"
                v-model="formData.custom_host"
                @blur="formatUrl('custom_host')"
                class="block w-full px-3 py-2 rounded-md text-sm transition-colors duration-200"
                :class="[
                  darkMode ? 'bg-gray-700 border-gray-600 text-white placeholder-gray-400' : 'border-gray-300 text-gray-900 placeholder-gray-500',
                  formData.custom_host && !isValidUrl(formData.custom_host) ? 'border-red-500' : '',
                ]"
                placeholder="https://cdn.example.com"
              />
              <p v-if="formData.custom_host && !isValidUrl(formData.custom_host)" class="mt-1 text-xs text-red-500">请输入有效的URL格式，如 https://xxx.com</p>
              <p v-else class="mt-1 text-xs" :class="darkMode ? 'text-gray-400' : 'text-gray-500'">可选：配置CDN加速域名或自定义域名，留空使用原始S3端点</p>
            </div>

            <div class="grid grid-cols-1 sm:grid-cols-2 gap-4">
              <!-- 签名有效期 -->
              <div>
                <label for="signature_expires_in" class="block text-sm font-medium mb-1" :class="darkMode ? 'text-gray-200' : 'text-gray-700'"> 签名有效期（秒） </label>
                <input
                  type="number"
                  id="signature_expires_in"
                  v-model="formData.signature_expires_in"
                  min="1"
                  class="block w-full px-3 py-2 rounded-md text-sm transition-colors duration-200"
                  :class="darkMode ? 'bg-gray-700 border-gray-600 text-white placeholder-gray-400' : 'border-gray-300 text-gray-900 placeholder-gray-500'"
                  placeholder="3600"
                />
                <p class="mt-1 text-xs" :class="darkMode ? 'text-gray-400' : 'text-gray-500'">控制预签名URL的默认有效期，默认3600秒</p>
              </div>
            </div>
          </div>

          <!-- 其他选项 -->
          <div class="space-y-4">
            <h3 class="text-sm font-medium border-b pb-2" :class="darkMode ? 'text-gray-200 border-gray-600' : 'text-gray-700 border-gray-200'">其他选项</h3>

            <div class="grid grid-cols-1 sm:grid-cols-2 gap-4">
              <!-- 路径样式 -->
              <div>
                <label class="block text-sm font-medium mb-1" :class="darkMode ? 'text-gray-200' : 'text-gray-700'"> 访问样式 </label>
                <div class="flex items-center h-10 px-3 py-2 rounded-md border" :class="darkMode ? 'border-gray-600' : 'border-gray-300'">
                  <input
                    type="checkbox"
                    id="path_style"
                    v-model="formData.path_style"
                    class="h-4 w-4 rounded border-gray-300 text-primary-600 focus:ring-primary-500"
                    :class="darkMode ? 'bg-gray-700 border-gray-600' : ''"
                  />
                  <label for="path_style" class="ml-2 text-sm" :class="darkMode ? 'text-gray-200' : 'text-gray-700'"> 使用路径样式访问 </label>
                </div>
                <p class="mt-1 text-xs" :class="darkMode ? 'text-gray-400' : 'text-gray-500'">使用 endpoint.com/bucket 格式</p>
              </div>

              <!-- API密钥权限 -->
              <div>
                <label class="block text-sm font-medium mb-1" :class="darkMode ? 'text-gray-200' : 'text-gray-700'"> API密钥权限 </label>
                <div class="flex items-center h-10 px-3 py-2 rounded-md border" :class="darkMode ? 'border-gray-600' : 'border-gray-300'">
                  <input
                    type="checkbox"
                    id="is_public"
                    v-model="formData.is_public"
                    class="h-4 w-4 rounded border-gray-300 text-primary-600 focus:ring-primary-500"
                    :class="darkMode ? 'bg-gray-700 border-gray-600' : ''"
                  />
                  <label for="is_public" class="ml-2 text-sm" :class="darkMode ? 'text-gray-200' : 'text-gray-700'"> 允许API密钥用户使用 </label>
                </div>
                <p class="mt-1 text-xs" :class="darkMode ? 'text-gray-400' : 'text-gray-500'">允许API密钥用户使用此存储</p>
              </div>
            </div>
          </div>
        </form>
      </div>

      <div
        class="px-3 sm:px-4 py-2 sm:py-3 border-t transition-colors duration-200 flex flex-col-reverse sm:flex-row sm:justify-end sm:space-x-3 space-y-2 space-y-reverse sm:space-y-0"
        :class="darkMode ? 'border-gray-700' : 'border-gray-200'"
      >
        <button
          @click="closeModal"
          class="w-full sm:w-auto px-4 py-2 rounded-md text-sm font-medium transition-colors duration-200"
          :class="darkMode ? 'bg-gray-700 hover:bg-gray-600 text-gray-200' : 'bg-gray-200 hover:bg-gray-300 text-gray-700'"
        >
          取消
        </button>

        <button
          @click="submitForm"
          :disabled="!formValid || loading"
          class="w-full sm:w-auto flex justify-center items-center px-4 py-2 rounded-md text-sm font-medium transition-colors duration-200 bg-primary-500 hover:bg-primary-600 text-white"
          :class="{ 'opacity-50 cursor-not-allowed': !formValid || loading }"
        >
          <svg v-if="loading" class="animate-spin -ml-1 mr-2 h-4 w-4 text-white" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
            <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
            <path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
          </svg>
          {{ loading ? "保存中..." : "保存配置" }}
        </button>
      </div>
    </div>
  </div>
</template>
