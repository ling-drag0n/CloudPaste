<template>
  <div class="paste-view-editor-wrapper">
    <!-- 主要编辑器内容 -->
    <div class="paste-view-editor">
      <!-- 成功通知提示 -->
      <div v-if="notification" class="fixed bottom-4 right-4 z-50 px-4 py-2 rounded-lg bg-green-500 text-white shadow-lg notification-toast flex items-center">
        <svg class="w-5 h-5 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg">
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7"></path>
        </svg>
        {{ notification }}
      </div>

      <!-- 添加隐藏的文件输入控件用于导入Markdown文件 -->
      <input type="file" ref="markdownImporter" accept=".md,.markdown,.mdown,.mkd" style="display: none" @change="importMarkdownFile" />

      <!-- 编辑器模式切换按钮 -->
      <div class="mb-1 flex justify-end">
        <button
          class="px-1.5 py-0.5 text-xs rounded-md border transition-colors"
          :class="darkMode ? 'bg-gray-700 hover:bg-gray-600 text-gray-200 border-gray-600' : 'bg-gray-50 hover:bg-gray-100 text-gray-700 border-gray-200'"
          @click="toggleEditorMode"
          :title="isPlainTextMode ? '切换到Markdown模式' : '切换到纯文本模式'"
        >
          <span class="inline-flex items-center">
            <svg class="w-3 h-3 mr-0.5" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
              <path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm-1 15v-4H8l4-8v4h3l-4 8z" fill="currentColor" />
            </svg>
            {{ isPlainTextMode ? "切换MD" : "切换TXT" }}
          </span>
        </button>
      </div>

      <div class="editor-wrapper">
        <!-- 编辑器区域 -->
        <div class="flex flex-col gap-2">
          <!-- 纯文本编辑器 (在纯文本模式下显示) -->
          <textarea
            v-if="isPlainTextMode"
            class="w-full h-[500px] p-4 font-mono text-base border rounded-lg resize-y focus:outline-none focus:ring-2"
            :class="darkMode ? 'bg-gray-800 border-gray-700 text-gray-100 focus:ring-primary-600' : 'bg-white border-gray-300 text-gray-900 focus:ring-primary-500'"
            v-model="plainTextContent"
            placeholder="在此输入纯文本内容..."
            @input="syncContentFromPlainText"
          ></textarea>

          <!-- Markdown编辑器 (在Markdown模式下显示) -->
          <VditorUnified
            v-else
            ref="editorRef"
            :dark-mode="darkMode"
            :is-plain-text-mode="false"
            v-model="editorContent"
            @editor-ready="handleEditorReady"
            @content-change="handleContentChange"
            @import-file="triggerImportFile"
            @clear-content="clearEditorContent"
            @show-copy-formats="showCopyFormatsMenu"
          />
        </div>
      </div>

      <!-- 元数据编辑表单 - 允许编辑备注、过期时间等 -->
      <div class="mt-6 border-t pt-4" :class="darkMode ? 'border-gray-700' : 'border-gray-200'">
        <div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-5 gap-4">
          <!-- 链接后缀 - 不可修改 -->
          <div class="form-group">
            <label class="form-label block mb-1 text-sm font-medium" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">链接后缀</label>
            <input
              type="text"
              class="form-input w-full rounded-md shadow-sm cursor-not-allowed opacity-75"
              :class="getInputClasses(darkMode)"
              placeholder="不可修改"
              v-model="editForm.customLink"
              disabled
            />
            <p class="mt-1 text-xs" :class="darkMode ? 'text-gray-500' : 'text-gray-400'">后缀不可修改，仅支持字母、数字、-和_</p>
          </div>

          <!-- 备注信息 -->
          <div class="form-group">
            <label class="form-label block mb-1 text-sm font-medium" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">备注(可选)</label>
            <input type="text" class="form-input w-full rounded-md shadow-sm" :class="getInputClasses(darkMode)" placeholder="添加备注信息..." v-model="editForm.remark" />
          </div>

          <!-- 过期时间选择 -->
          <div class="form-group">
            <label class="form-label block mb-1 text-sm font-medium" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">过期时间</label>
            <select class="form-input w-full rounded-md shadow-sm" :class="getInputClasses(darkMode)" v-model="editForm.expiryTime">
              <option value="1">1小时</option>
              <option value="24">1天</option>
              <option value="168">7天</option>
              <option value="720">30天</option>
              <option value="0">永不过期</option>
            </select>
          </div>

          <!-- 可打开次数设置 -->
          <div class="form-group">
            <label class="form-label block mb-1 text-sm font-medium" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">可打开次数(0表示无限制)</label>
            <input
              type="number"
              min="0"
              step="1"
              pattern="\d*"
              class="form-input w-full rounded-md shadow-sm"
              :class="getInputClasses(darkMode)"
              placeholder="0表示无限制"
              v-model.number="editForm.maxViews"
              @input="validateMaxViews"
            />
          </div>

          <!-- 密码设置 -->
          <div class="form-group">
            <label class="form-label block mb-1 text-sm font-medium" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">访问密码</label>
            <div class="flex items-center space-x-2">
              <input
                :type="showPassword ? 'text' : 'password'"
                autocomplete="new-password"
                class="form-input w-full rounded-md shadow-sm"
                :class="getInputClasses(darkMode)"
                placeholder="设置访问密码..."
                v-model="editForm.password"
                :disabled="editForm.clearPassword"
              />
            </div>
            <div class="mt-2 flex items-center">
              <input
                type="checkbox"
                id="clear-password"
                class="h-4 w-4 rounded border-gray-300 text-primary-600 focus:ring-primary-500"
                :class="darkMode ? 'bg-gray-700 border-gray-600' : ''"
                v-model="editForm.clearPassword"
              />
              <label for="clear-password" class="ml-2 text-xs" :class="darkMode ? 'text-gray-400' : 'text-gray-600'"> 清除访问密码 </label>
            </div>
            <p class="mt-1 text-xs" :class="darkMode ? 'text-gray-500' : 'text-gray-400'">
              {{ editForm.clearPassword ? "将移除密码保护" : props.paste?.hasPassword ? "留空表示保持原密码不变" : "设置密码后，他人访问需要输入密码" }}
            </p>
          </div>
        </div>

        <!-- 保存和取消按钮 -->
        <div class="submit-section mt-6 flex flex-row items-center gap-4">
          <!-- 保存按钮 -->
          <button
            @click="saveEdit"
            class="btn-primary px-4 py-2 text-sm font-medium text-white bg-primary-600 rounded-md hover:bg-primary-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500 disabled:opacity-50"
            :disabled="loading"
          >
            {{ loading ? "保存中..." : "保存修改" }}
          </button>

          <!-- 取消按钮 -->
          <button
            @click="cancelEdit"
            class="px-4 py-2 text-sm font-medium border rounded-md transition-colors"
            :class="darkMode ? 'border-gray-600 text-gray-300 hover:bg-gray-800' : 'border-gray-300 text-gray-700 hover:bg-gray-50'"
            title="取消编辑并恢复原始内容"
          >
            取消
          </button>

          <!-- 状态提示信息 -->
          <div class="saving-status ml-auto text-sm" v-if="error">
            <span :class="[error.includes('成功') ? (darkMode ? 'text-green-400' : 'text-green-600') : darkMode ? 'text-red-400' : 'text-red-600']">
              {{ error }}
            </span>
          </div>
        </div>
      </div>
    </div>

    <!-- 复制格式菜单组件 -->
    <PasteCopyFormatMenu
      :visible="copyFormatMenuVisible"
      :position="copyFormatMenuPosition"
      :editor="editorRef"
      :dark-mode="darkMode"
      :is-plain-text-mode="isPlainTextMode"
      :plain-text-content="plainTextContent"
      :document-title="editForm.remark || 'CloudPaste文档'"
      @close="closeCopyFormatMenu"
      @status-message="handleStatusMessage"
    />
  </div>
</template>

<script setup>
import { ref, watch, onBeforeUnmount } from "vue";
import { getInputClasses } from "./PasteViewUtils";
import VditorUnified from "../common/VditorUnified.vue";
import PasteCopyFormatMenu from "./PasteCopyFormatMenu.vue";

// Props 定义
const props = defineProps({
  darkMode: { type: Boolean, required: true },
  content: { type: String, default: "" },
  paste: { type: Object, default: () => ({}) },
  loading: { type: Boolean, default: false },
  error: { type: String, default: "" },
  isDev: { type: Boolean, default: false },
  enableDebug: { type: Boolean, default: false },
  isPlainTextMode: { type: Boolean, default: false },
});

// Emits 定义
const emit = defineEmits(["save", "cancel", "update:error", "update:isPlainTextMode"]);

// 响应式数据
const editorRef = ref(null);
const editorContent = ref(props.content);
const notification = ref("");
const showPassword = ref(false);
const markdownImporter = ref(null);

// 纯文本模式变量
const isPlainTextMode = ref(props.isPlainTextMode);
// 纯文本内容
const plainTextContent = ref("");
// 原始纯文本内容（保留格式）
const originalPlainTextContent = ref("");

// 复制格式菜单状态
const copyFormatMenuVisible = ref(false);
const copyFormatMenuPosition = ref({ x: 0, y: 0 });

// 计算初始过期时间选项
const getInitialExpiryTime = (expiresAt) => {
  if (!expiresAt) return "0";

  const expiryDate = new Date(expiresAt);
  const now = new Date();
  const diffHours = Math.round((expiryDate - now) / (1000 * 60 * 60));

  if (diffHours <= 1) {
    return "1";
  } else if (diffHours <= 24) {
    return "24";
  } else if (diffHours <= 168) {
    return "168";
  } else if (diffHours <= 720) {
    return "720";
  } else {
    return "0"; // 设置为永不过期
  }
};

// 编辑表单数据
const editForm = ref({
  remark: props.paste?.remark || "",
  customLink: props.paste?.slug || "",
  expiryTime: getInitialExpiryTime(props.paste?.expires_at),
  maxViews: props.paste?.max_views || 0,
  password: "",
  clearPassword: false, // 新增是否清除密码的标志
});

// 工具函数
const showNotification = (message, duration = 2000) => {
  notification.value = message;
  setTimeout(() => {
    notification.value = "";
  }, duration);
};

// 切换编辑器模式
const toggleEditorMode = () => {
  isPlainTextMode.value = !isPlainTextMode.value;
  emit("update:isPlainTextMode", isPlainTextMode.value);

  if (isPlainTextMode.value) {
    // 切换到纯文本模式，保存当前Markdown内容
    if (editorRef.value) {
      plainTextContent.value = editorContent.value;
      originalPlainTextContent.value = editorContent.value;
    }
  } else {
    // 切换到Markdown模式，使用纯文本内容
    editorContent.value = plainTextContent.value || "";
  }
};

// 同步纯文本内容到编辑器
const syncContentFromPlainText = () => {
  // 同时更新原始纯文本内容，保留格式
  originalPlainTextContent.value = plainTextContent.value;
  editorContent.value = plainTextContent.value;
};

// 验证可打开次数输入，确保输入合法
const validateMaxViews = () => {
  const value = editForm.value.maxViews;
  if (value < 0) {
    editForm.value.maxViews = 0;
  }
  // 确保是整数
  if (!Number.isInteger(value)) {
    editForm.value.maxViews = Math.floor(value);
  }
};

// 编辑器事件处理
const handleEditorReady = () => {
  console.log("编辑器已准备就绪");
};

const handleContentChange = (content) => {
  editorContent.value = content;
};

// 触发导入文件功能
const triggerImportFile = () => {
  if (markdownImporter.value) {
    markdownImporter.value.click();
  }
};

// 清空编辑器内容
const clearEditorContent = () => {
  if (confirm("确定要清空所有内容吗？此操作不可撤销。")) {
    if (isPlainTextMode.value) {
      plainTextContent.value = "";
      originalPlainTextContent.value = "";
    } else {
      editorContent.value = "";
      if (editorRef.value) {
        editorRef.value.setValue("");
      }
    }
    showNotification("内容已清空");
  }
};

// 显示复制格式菜单
const showCopyFormatsMenu = (position = null) => {
  if (position) {
    // 如果传入了位置参数，直接使用（来自按钮点击事件）
    copyFormatMenuPosition.value = position;
  } else {
    // 如果没有传入位置，使用默认位置
    copyFormatMenuPosition.value = {
      x: 100,
      y: 100,
    };
  }

  // 显示菜单
  copyFormatMenuVisible.value = true;
};

// 关闭复制格式菜单
const closeCopyFormatMenu = () => {
  copyFormatMenuVisible.value = false;
};

// 处理状态消息
const handleStatusMessage = ({ message, type }) => {
  if (type === "error") {
    emit("update:error", message);
  } else {
    showNotification(message);
  }
};

// 监听 props 变化
watch(
  () => props.content,
  (newContent) => {
    editorContent.value = newContent;
  }
);

watch(
  () => props.paste,
  (newPaste) => {
    if (newPaste) {
      editForm.value.remark = newPaste.remark || "";
      editForm.value.customLink = newPaste.slug || "";
      editForm.value.maxViews = newPaste.max_views || 0;
      editForm.value.password = "";

      // 处理过期时间
      editForm.value.expiryTime = getInitialExpiryTime(newPaste.expires_at);
    }
  }
);

// 保存编辑内容，收集所有表单数据并触发保存事件
const saveEdit = async () => {
  // 根据当前模式获取内容
  let newContent;

  if (isPlainTextMode.value) {
    // 纯文本模式下，使用plainTextContent或originalPlainTextContent
    newContent = originalPlainTextContent.value || plainTextContent.value;
  } else if (editorRef.value) {
    // Markdown模式下，从编辑器获取内容
    newContent = editorContent.value;
  } else {
    emit("update:error", "编辑器未初始化");
    return;
  }

  // 检查文本内容是否为空
  if (!newContent || !newContent.trim()) {
    emit("update:error", "内容不能为空");
    return;
  }

  // 准备更新数据对象，包含内容和元数据
  const updateData = {
    content: newContent,
    remark: editForm.value.remark || null,
    max_views: editForm.value.maxViews === 0 ? null : parseInt(editForm.value.maxViews),
  };

  // 处理自定义链接 - 注意：链接后缀不可修改，所以不包含在更新数据中

  // 处理过期时间
  if (editForm.value.expiryTime !== "0") {
    const hours = parseInt(editForm.value.expiryTime);
    const expiresAt = new Date();
    expiresAt.setHours(expiresAt.getHours() + hours);
    updateData.expires_at = expiresAt.toISOString();
  } else {
    updateData.expires_at = null; // 永不过期
  }

  // 处理密码设置
  if (editForm.value.clearPassword) {
    // 如果选择清除密码，明确设置为null
    updateData.password = null;
    updateData.clearPassword = true;
  } else if (editForm.value.password && editForm.value.password.trim()) {
    // 如果设置了新密码
    updateData.password = editForm.value.password.trim();
  }
  // 如果密码字段为空且未选择清除，则不包含password字段，保持原密码不变

  // 触发保存事件，将数据传递给父组件
  emit("save", updateData);
};

// 取消编辑
const cancelEdit = () => {
  emit("cancel");
};

// 导入Markdown文件的函数
const importMarkdownFile = (event) => {
  const file = event.target.files[0];
  if (!file) return;

  // 检查文件类型
  const allowedTypes = [".md", ".markdown", ".mdown", ".mkd", ".txt"];
  const fileName = file.name.toLowerCase();
  const isValidType = allowedTypes.some((type) => fileName.endsWith(type));

  if (!isValidType) {
    emit("update:error", "请选择有效的Markdown文件（.md, .markdown, .mdown, .mkd, .txt）");
    return;
  }

  // 检查文件大小（限制为5MB）
  const maxSize = 5 * 1024 * 1024; // 5MB
  if (file.size > maxSize) {
    emit("update:error", "文件大小不能超过5MB");
    return;
  }

  const reader = new FileReader();
  reader.onload = (e) => {
    try {
      const content = e.target.result;

      if (isPlainTextMode.value) {
        // 纯文本模式下，直接设置纯文本内容
        plainTextContent.value = content;
        originalPlainTextContent.value = content;
      } else {
        // Markdown模式下，设置编辑器内容
        editorContent.value = content;
        if (editorRef.value) {
          editorRef.value.setValue(content);
        }
      }

      showNotification("文件导入成功");

      // 清空文件输入，允许重复导入同一文件
      if (markdownImporter.value) {
        markdownImporter.value.value = "";
      }
    } catch (error) {
      console.error("导入文件时出错:", error);
      emit("update:error", "导入文件失败");
    }
  };

  reader.onerror = () => {
    emit("update:error", "读取文件失败");
  };

  reader.readAsText(file, "UTF-8");
};

// 监听 isPlainTextMode 变化
watch(
  () => isPlainTextMode.value,
  (newMode) => {
    emit("update:isPlainTextMode", newMode);
  }
);

// 清理
onBeforeUnmount(() => {
  notification.value = "";
  copyFormatMenuVisible.value = false;
});
</script>

<style scoped>
.paste-view-editor {
  width: 100%;
}

.editor-wrapper {
  width: 100%;
  margin-bottom: 1rem;
}

.notification-toast {
  animation: slideIn 0.3s ease-out;
}

@keyframes slideIn {
  from {
    transform: translateX(100%);
    opacity: 0;
  }
  to {
    transform: translateX(0);
    opacity: 1;
  }
}
</style>
