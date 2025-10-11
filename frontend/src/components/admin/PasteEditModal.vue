<script setup>
import { reactive, watch } from "vue";
// 编辑文本属性的弹窗

// 组件接收的属性定义
const props = defineProps({
  darkMode: {
    type: Boolean,
    required: true,
  },
  showEdit: {
    type: Boolean,
    required: true,
  },
  paste: {
    type: Object,
    default: null,
  },
});

// 定义组件要触发的事件
const emit = defineEmits(["close", "save"]);

// 编辑文本属性的弹窗
/**
 * 编辑表单数据对象，用于存储用户修改的内容
 * 使用reactive使其具有响应性
 */
const editForm = reactive({
  remark: "", // 备注信息
  password: "", // 不回显密码，如果填写则更新密码
  clearPassword: false, // 是否清除密码的标记
  expiry_time: "0", // 过期时间（小时），0表示永不过期
  max_views: 0, // 最大查看次数，0表示无限制
  slug: "", // 链接后缀，为空则系统自动生成
});

/**
 * 监听paste属性变化，当要编辑的paste对象变化时，更新表单数据
 * immediate设为true确保一开始就执行一次
 */
watch(
  () => props.paste,
  (newPaste) => {
    if (!newPaste) return;

    // 填充表单数据，但密码不回显（出于安全考虑）
    editForm.remark = newPaste.remark || "";
    editForm.password = ""; // 密码不回显
    editForm.clearPassword = false;
    editForm.slug = newPaste.slug || ""; // 设置当前slug

    // 处理过期时间 - 根据剩余小时数选择最接近的选项
    if (newPaste.expires_at) {
      const expiryDate = new Date(newPaste.expires_at);
      const now = new Date();
      const diffHours = Math.round((expiryDate - now) / (1000 * 60 * 60));

      if (diffHours <= 1) {
        editForm.expiry_time = "1";
      } else if (diffHours <= 24) {
        editForm.expiry_time = "24";
      } else if (diffHours <= 168) {
        editForm.expiry_time = "168";
      } else if (diffHours <= 720) {
        editForm.expiry_time = "720";
      } else {
        editForm.expiry_time = "0"; // 设置为永不过期
      }
    } else {
      editForm.expiry_time = "0"; // 永不过期
    }

    // 设置最大查看次数
    editForm.max_views = newPaste.max_views || 0;
  },
  { immediate: true }
);

/**
 * 验证并处理可打开次数的输入
 * 确保输入的是有效的非负整数
 * @param {Event} event - 输入事件对象
 */
const validateMaxViews = (event) => {
  // 获取输入的值
  const value = event.target.value;

  // 如果是负数，则设置为0
  if (value < 0) {
    editForm.max_views = 0;
    return;
  }

  // 如果包含小数点，截取整数部分
  if (value.toString().includes(".")) {
    editForm.max_views = parseInt(value);
  }

  // 确保值为有效数字
  if (isNaN(value) || value === "") {
    editForm.max_views = 0;
  } else {
    // 确保是整数
    editForm.max_views = parseInt(value);
  }
};

/**
 * 关闭修改弹窗
 * 触发close事件通知父组件
 */
const closeEditModal = () => {
  emit("close");
};

/**
 * 保存修改
 * 收集表单数据，进行验证，然后触发save事件将数据传递给父组件
 */
const saveEdit = () => {
  // 验证可打开次数不能为负数
  if (editForm.max_views < 0) {
    // 通过emit传递错误信息
    emit("save", { error: "可打开次数不能为负数" });
    return;
  }

  // 准备更新数据
  const updateData = {
    id: props.paste?.id, // 添加ID
    slug: props.paste?.slug, // 当前slug（用于API请求路径）
    content: props.paste?.content, // 使用原内容
    remark: editForm.remark || null,
    max_views: editForm.max_views === 0 ? null : parseInt(editForm.max_views),
  };

  // 只有当用户修改了slug时，才包含newSlug参数
  if (editForm.slug !== props.paste?.slug) {
    updateData.newSlug = editForm.slug || null; // 新的slug（为空则系统自动生成）
  }

  // 处理密码 - 三种情况：不变、更新、清除
  if (editForm.password) {
    updateData.password = editForm.password;
  } else if (editForm.clearPassword) {
    updateData.clearPassword = true;
  }

  // 处理过期时间 - 将小时数转换为ISO日期字符串
  if (editForm.expiry_time !== "0") {
    const hours = parseInt(editForm.expiry_time);
    const expiresAt = new Date();
    expiresAt.setHours(expiresAt.getHours() + hours);
    updateData.expires_at = expiresAt.toISOString();
  } else {
    updateData.expires_at = null; // 永不过期
  }

  // 传递更新数据给父组件
  emit("save", { data: updateData });
};
</script>

<template>
  <!-- 编辑弹窗 - 仅在showEdit为true时显示 -->
  <div v-if="showEdit" class="fixed inset-0 z-[60] overflow-y-auto" aria-labelledby="edit-modal-title" role="dialog" aria-modal="true">
    <div class="flex items-center justify-center min-h-screen pt-20 sm:pt-2 px-2 sm:px-4 pb-4 sm:pb-20 text-center sm:p-0">
      <!-- 背景蒙层 - 点击时关闭弹窗 -->
      <div class="fixed inset-0 bg-gray-500 bg-opacity-75 dark:bg-gray-900 dark:bg-opacity-75 transition-opacity" aria-hidden="true" @click="closeEditModal"></div>

      <!-- 使对话框居中的辅助元素 -->
      <span class="hidden sm:inline-block sm:align-middle sm:h-screen" aria-hidden="true">&#8203;</span>

      <!-- 弹窗主体内容 -->
      <div
        class="inline-block align-middle sm:align-middle bg-white dark:bg-gray-800 rounded-lg text-left overflow-hidden shadow-xl transform transition-all w-full max-w-sm sm:max-w-lg max-h-[95vh] sm:max-h-[85vh] my-1 sm:my-8"
        :class="darkMode ? 'dark' : ''"
      >
        <!-- 弹窗头部带关闭按钮 -->
        <div class="bg-white dark:bg-gray-800 px-4 py-3 sm:py-4 border-b dark:border-gray-700 flex justify-between items-center">
          <h3 class="text-lg leading-6 font-medium text-gray-900 dark:text-gray-100" id="edit-modal-title">修改文本分享属性</h3>
          <button
            type="button"
            @click="closeEditModal"
            class="rounded-md p-1 inline-flex items-center justify-center text-gray-400 hover:text-gray-500 dark:hover:text-gray-300 focus:outline-none"
          >
            <span class="sr-only">关闭</span>
            <svg class="h-5 w-5" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
            </svg>
          </button>
        </div>

        <!-- 弹窗内容区 - 带滚动条 -->
        <div class="bg-white dark:bg-gray-800 px-3 sm:px-4 py-3 sm:py-4 overflow-y-auto" style="max-height: calc(95vh - 160px); min-height: 200px">
          <!-- 修改表单 -->
          <form @submit.prevent="saveEdit" class="space-y-4">
            <!-- 链接后缀（slug）字段 -->
            <div class="mb-4">
              <label class="block text-sm font-medium mb-1 text-gray-700 dark:text-gray-300">链接后缀</label>
              <div class="flex items-center">
                <span class="text-sm mr-1 text-gray-600 dark:text-gray-400">/paste/</span>
                <input
                  type="text"
                  v-model="editForm.slug"
                  class="w-full px-3 py-2 border rounded-md shadow-sm focus:ring-primary-500 focus:border-primary-500 sm:text-sm"
                  :class="darkMode ? 'bg-gray-700 border-gray-600 text-white' : 'bg-white border-gray-300 text-gray-900'"
                  placeholder="留空则自动生成"
                />
              </div>
              <p class="mt-1 text-xs text-gray-500 dark:text-gray-400">当前链接：{{ props.paste?.slug }}，留空则自动生成新链接</p>
            </div>

            <!-- 备注 -->
            <div class="mb-4">
              <label class="block text-sm font-medium mb-1 text-gray-700 dark:text-gray-300">备注信息</label>
              <textarea
                v-model="editForm.remark"
                rows="2"
                class="w-full px-3 py-2 border rounded-md shadow-sm focus:ring-primary-500 focus:border-primary-500 sm:text-sm"
                :class="darkMode ? 'bg-gray-700 border-gray-600 text-white' : 'bg-white border-gray-300 text-gray-900'"
                placeholder="添加备注信息..."
              ></textarea>
            </div>
            <!-- 可打开次数输入区域 -->
            <div class="mb-4">
              <label class="block text-sm font-medium mb-1 text-gray-700 dark:text-gray-300">可打开次数</label>
              <input
                type="number"
                v-model.number="editForm.max_views"
                min="0"
                step="1"
                class="w-full px-3 py-2 border rounded-md shadow-sm focus:ring-primary-500 focus:border-primary-500 sm:text-sm"
                :class="darkMode ? 'bg-gray-700 border-gray-600 text-white' : 'bg-white border-gray-300 text-gray-900'"
                placeholder="0表示无限制"
                @input="validateMaxViews"
              />
              <p class="mt-1 text-xs text-gray-500 dark:text-gray-400">0表示无限制</p>
            </div>

            <!-- 过期时间选择器 -->
            <div class="mb-4">
              <label class="block text-sm font-medium mb-1 text-gray-700 dark:text-gray-300">过期时间</label>
              <select
                v-model="editForm.expiry_time"
                class="w-full px-3 py-2 border rounded-md shadow-sm focus:ring-primary-500 focus:border-primary-500 sm:text-sm"
                :class="darkMode ? 'bg-gray-700 border-gray-600 text-white' : 'bg-white border-gray-300 text-gray-900'"
              >
                <option value="1">1小时</option>
                <option value="24">1天</option>
                <option value="168">7天</option>
                <option value="720">30天</option>
                <option value="0">永不过期</option>
              </select>
            </div>
            <!-- 密码保护设置区域 -->
            <div class="mb-4">
              <label class="block text-sm font-medium mb-1 text-gray-700 dark:text-gray-300">密码保护</label>
              <!-- 密码输入框 -->
              <input
                type="password"
                autocomplete="new-password"
                v-model="editForm.password"
                class="w-full px-3 py-2 border rounded-md shadow-sm focus:ring-primary-500 focus:border-primary-500 sm:text-sm"
                :class="darkMode ? 'bg-gray-700 border-gray-600 text-white' : 'bg-white border-gray-300 text-gray-900'"
                placeholder="留空则不修改密码"
                :disabled="editForm.clearPassword"
              />
              <!-- 清除密码复选框 -->
              <div class="mt-2 flex items-center">
                <input
                  type="checkbox"
                  id="clearPassword"
                  v-model="editForm.clearPassword"
                  class="h-4 w-4 rounded border-gray-300 text-primary-600 focus:ring-primary-500 focus:border-primary-500 cursor-pointer"
                  :disabled="editForm.password.length > 0"
                />
                <label for="clearPassword" class="ml-2 block text-sm text-gray-700 dark:text-gray-300"> 清除密码保护 </label>
              </div>
              <p v-if="props.paste?.has_password && !editForm.password && !editForm.clearPassword" class="mt-1 text-xs text-yellow-500">留空将保持原密码不变</p>
              <p v-if="editForm.clearPassword" class="mt-1 text-xs text-red-500">警告：勾选此项将移除密码保护</p>
            </div>
          </form>
        </div>

        <!-- 弹窗底部按钮区 -->
        <div class="bg-gray-50 dark:bg-gray-700 px-4 py-3 sm:px-6 flex flex-col sm:flex-row-reverse sm:space-x-reverse space-y-2 sm:space-y-0 sm:space-x-3">
          <!-- 保存按钮 -->
          <button
            type="button"
            class="w-full inline-flex justify-center rounded-md border border-transparent shadow-sm px-4 py-2 bg-primary-600 text-base font-medium text-white hover:bg-primary-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500 sm:w-auto sm:text-sm"
            @click="saveEdit"
          >
            保存修改
          </button>
          <!-- 取消按钮 -->
          <button
            type="button"
            class="w-full inline-flex justify-center rounded-md border border-gray-300 dark:border-gray-600 shadow-sm px-4 py-2 bg-white dark:bg-gray-800 text-base font-medium text-gray-700 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500 sm:w-auto sm:text-sm"
            @click="closeEditModal"
          >
            取消
          </button>
        </div>
      </div>
    </div>
  </div>
</template>
