<template>
  <div class="fixed inset-0 z-[60] overflow-auto bg-black bg-opacity-50 flex items-center justify-center p-2 sm:p-4 pt-20 sm:pt-4">
    <div class="relative rounded-lg max-w-sm sm:max-w-lg w-full mx-auto shadow-xl overflow-hidden bg-white dark:bg-gray-800 max-h-[95vh] sm:max-h-[85vh]">
      <!-- 标题栏 -->
      <div class="px-4 sm:px-6 py-3 sm:py-4 border-b border-gray-200 dark:border-gray-700">
        <h3 class="text-base sm:text-lg font-medium text-gray-900 dark:text-white">编辑文件信息</h3>
      </div>

      <!-- 表单内容 -->
      <div class="px-4 sm:px-6 py-3 sm:py-4 overflow-y-auto" style="max-height: calc(95vh - 160px)">
        <div v-if="error" class="mb-4 p-3 rounded bg-red-100 dark:bg-red-900/20 text-red-600 dark:text-red-400">
          {{ error }}
        </div>

        <form @submit.prevent="saveChanges">
          <!-- 文件名(只读) -->
          <div class="mb-4">
            <label class="block text-sm font-medium mb-1 text-gray-700 dark:text-gray-300"> 文件名 </label>
            <input
              type="text"
              class="w-full px-3 py-2 border rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500 sm:text-sm disabled:opacity-70 bg-white dark:bg-gray-700 border-gray-300 dark:border-gray-600 text-gray-900 dark:text-white"
              disabled
              :value="fileData.filename"
            />
          </div>

          <!-- 短链接 -->
          <div class="mb-4">
            <label class="block text-sm font-medium mb-1 text-gray-700 dark:text-gray-300"> 自定义短链接 </label>
            <div class="flex items-center">
              <span class="text-sm mr-1 text-gray-600 dark:text-gray-400">/</span>
              <input
                type="text"
                class="w-full px-3 py-2 border rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500 sm:text-sm bg-white dark:bg-gray-700 border-gray-300 dark:border-gray-600 text-gray-900 dark:text-white"
                v-model="fileData.slug"
                placeholder="例如: my-file"
              />
            </div>
            <p class="mt-1 text-xs text-gray-500 dark:text-gray-400">留空则系统自动生成</p>
          </div>

          <!-- 备注 -->
          <div class="mb-4">
            <label class="block text-sm font-medium mb-1 text-gray-700 dark:text-gray-300"> 备注信息 </label>
            <textarea
              class="w-full px-3 py-2 border rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500 sm:text-sm bg-white dark:bg-gray-700 border-gray-300 dark:border-gray-600 text-gray-900 dark:text-white"
              v-model="fileData.remark"
              rows="2"
              placeholder="文件的描述信息"
            ></textarea>
          </div>

          <!-- 最大查看次数 -->
          <div class="mb-4">
            <label class="block text-sm font-medium mb-1 text-gray-700 dark:text-gray-300"> 最大查看次数 </label>
            <input
              type="number"
              class="w-full px-3 py-2 border rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500 sm:text-sm bg-white dark:bg-gray-700 border-gray-300 dark:border-gray-600 text-gray-900 dark:text-white"
              v-model="fileData.max_views"
              min="0"
              placeholder="留空表示无限制"
            />
          </div>

          <!-- 过期时间 -->
          <div class="mb-4">
            <label class="block text-sm font-medium mb-1 text-gray-700 dark:text-gray-300"> 过期时间 </label>
            <select
              v-model="expiryOption"
              class="w-full px-3 py-2 border rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500 sm:text-sm bg-white dark:bg-gray-700 border-gray-300 dark:border-gray-600 text-gray-900 dark:text-white"
            >
              <option value="1">1小时</option>
              <option value="24">1天</option>
              <option value="168">7天</option>
              <option value="720">30天</option>
              <option value="0">永不过期</option>
            </select>
          </div>

          <!-- 访问密码 -->
          <div class="mb-4">
            <label class="block text-sm font-medium mb-1 text-gray-700 dark:text-gray-300"> 访问密码 </label>
            <input
              type="text"
              class="w-full px-3 py-2 border rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500 sm:text-sm bg-white dark:bg-gray-700 border-gray-300 dark:border-gray-600 text-gray-900 dark:text-white"
              v-model="fileData.password"
              placeholder="留空则不修改密码"
              :disabled="clearPassword"
            />
            <!-- 添加清除密码功能 -->
            <div class="mt-2 flex items-center">
              <input
                type="checkbox"
                id="clear_password"
                v-model="clearPassword"
                class="rounded border-gray-300 text-blue-600 shadow-sm focus:border-blue-500 focus:ring focus:ring-blue-500 focus:ring-opacity-50 dark:border-gray-600 dark:bg-gray-700"
                :disabled="fileData.password.length > 0"
              />
              <label for="clear_password" class="ml-2 block text-sm text-gray-700 dark:text-gray-300"> 清除密码保护 </label>
            </div>
            <p v-if="file.has_password && !fileData.password && !clearPassword" class="mt-1 text-xs text-yellow-500">留空将保持原密码不变</p>
            <p v-if="clearPassword" class="mt-1 text-xs text-red-500">警告：勾选此项将移除密码保护</p>
          </div>

          <!-- 使用Worker代理 -->
          <div class="mb-4">
            <div class="flex items-center">
              <input
                type="checkbox"
                id="use_proxy"
                v-model="fileData.use_proxy"
                class="rounded border-gray-300 text-blue-600 shadow-sm focus:border-blue-500 focus:ring focus:ring-blue-500 focus:ring-opacity-50 dark:border-gray-600 dark:bg-gray-700"
              />
              <label for="use_proxy" class="ml-2 block text-sm text-gray-700 dark:text-gray-300"> 使用Worker代理访问 </label>
            </div>
            <p class="mt-1 text-xs text-gray-500 dark:text-gray-400">启用后，预览页将通过Worker代理，否则使用S3直链</p>
          </div>

          <!-- 按钮组 -->
          <div class="flex justify-end space-x-3 mt-6">
            <button
              type="button"
              @click="$emit('close')"
              class="px-4 py-2 rounded-md text-sm font-medium transition-colors bg-gray-200 dark:bg-gray-700 hover:bg-gray-300 dark:hover:bg-gray-600 text-gray-700 dark:text-white"
            >
              取消
            </button>
            <button type="submit" class="px-4 py-2 rounded-md text-sm font-medium transition-colors bg-blue-600 hover:bg-blue-700 text-white" :disabled="saving">
              {{ saving ? "保存中..." : "保存" }}
            </button>
          </div>
        </form>
      </div>
    </div>
  </div>
</template>

<script setup>
import { defineProps, defineEmits, reactive, ref, watch } from "vue";

const props = defineProps({
  file: {
    type: Object,
    required: true,
  },
  darkMode: {
    type: Boolean,
    default: false,
  },
});

const emit = defineEmits(["close", "save"]);

// 错误信息
const error = ref("");
// 保存中状态
const saving = ref(false);
// 添加清除密码标记
const clearPassword = ref(false);

/**
 * 格式化日期为input[type=datetime-local]所需的格式
 * @param {Date} date - 日期对象
 * @returns {string} 格式化后的日期字符串
 */
const formatDateForInput = (date) => {
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  const hours = String(date.getHours()).padStart(2, "0");
  const minutes = String(date.getMinutes()).padStart(2, "0");

  return `${year}-${month}-${day}T${hours}:${minutes}`;
};

// 文件编辑数据
const fileData = reactive({
  id: "",
  filename: "",
  slug: "",
  remark: "",
  max_views: null,
  expires_at: null,
  password: "",
  use_proxy: false,
});

// 过期时间选项
const expiryOption = ref("0"); // 默认为永不过期

// 监听文件数据变化，初始化编辑表单
watch(
  () => props.file,
  (newFile) => {
    if (newFile) {
      // 复制文件数据
      fileData.id = newFile.id;
      fileData.filename = newFile.filename;
      fileData.slug = newFile.slug || "";
      fileData.remark = newFile.remark || "";
      fileData.max_views = newFile.max_views || null;
      fileData.expires_at = newFile.expires_at || null;
      fileData.password = ""; // 出于安全考虑不回显密码
      // 确保 use_proxy 值正确转换为布尔值，数据库中可能存储为 0 或 1
      fileData.use_proxy = newFile.use_proxy === 1 || newFile.use_proxy === true;

      // 如果有过期时间，根据到期日期计算最接近的选项
      if (newFile.expires_at) {
        const now = new Date();
        const expiresAt = new Date(newFile.expires_at);

        // 计算小时差
        const hoursDiff = Math.round((expiresAt - now) / (1000 * 60 * 60));

        // 根据小时差选择最接近的选项
        if (hoursDiff <= 0) {
          // 如果已经过期，默认设置为1小时
          expiryOption.value = "1";
        } else if (hoursDiff <= 12) {
          expiryOption.value = "1"; // 1小时
        } else if (hoursDiff <= 96) {
          expiryOption.value = "24"; // 1天
        } else if (hoursDiff <= 336) {
          expiryOption.value = "168"; // 7天
        } else if (hoursDiff <= 1440) {
          expiryOption.value = "720"; // 30天
        } else {
          expiryOption.value = "720"; // 默认为30天，如果超过30天
        }
      } else {
        expiryOption.value = "0"; // 永不过期
      }
    }
  },
  { immediate: true }
);

/**
 * 保存更改
 * 将表单数据提交给父组件
 */
const saveChanges = () => {
  // 清除错误
  error.value = "";
  saving.value = true;

  try {
    // 准备提交的数据
    const updatedFile = {
      id: fileData.id,
      slug: fileData.slug.trim() || null,
      remark: fileData.remark.trim() || null,
      max_views: fileData.max_views !== null ? parseInt(fileData.max_views, 10) : null,
      use_proxy: fileData.use_proxy,
    };

    // 处理密码 - 三种情况：不变、更新、清除
    if (fileData.password) {
      updatedFile.password = fileData.password;
    } else if (clearPassword.value) {
      updatedFile.password = ""; // 明确设置为空字符串表示清除密码
    }
    // 如果既没有新密码也没有勾选清除密码，则不修改密码

    // 处理过期时间
    const expiryHours = parseInt(expiryOption.value);

    if (expiryHours > 0) {
      // 计算过期日期
      const expiresAt = new Date();
      expiresAt.setHours(expiresAt.getHours() + expiryHours);
      updatedFile.expires_at = expiresAt.toISOString();
    } else {
      // 永不过期
      updatedFile.expires_at = null;
    }

    // 提交到父组件
    emit("save", updatedFile);
  } catch (err) {
    console.error("处理表单数据出错:", err);
    error.value = "数据处理错误，请检查输入";
  } finally {
    saving.value = false;
  }
};
</script>
