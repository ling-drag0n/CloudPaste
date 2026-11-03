<template>
  <div class="editor-container mx-auto px-3 sm:px-6 flex-1 flex flex-col pt-6 sm:pt-8 w-full max-w-full sm:max-w-6xl">
    <!-- 隐藏的文件输入控件 -->
    <input type="file" ref="markdownImporter" accept=".md,.markdown,.mdown,.mkd" style="display: none" @change="importMarkdownFile" />

    <!-- 页面标题和模式切换 -->
    <div class="header mb-4 border-b pb-2" :class="darkMode ? 'border-gray-700' : 'border-gray-200'">
      <div class="flex justify-between items-center">
        <h2 class="text-xl font-semibold">{{ $t("markdown.title") }}</h2>
        <button
          class="px-2 py-1 text-sm rounded transition-colors"
          :class="darkMode ? 'bg-gray-700 hover:bg-gray-600 text-gray-200' : 'bg-gray-100 hover:bg-gray-200 text-gray-700'"
          @click="toggleEditorMode"
        >
          {{ isPlainTextMode ? $t("markdown.switchToMarkdown") : $t("markdown.switchToPlainText") }}
        </button>
      </div>
    </div>

    <!-- 权限管理组件 -->
    <PermissionManager
      :dark-mode="darkMode"
      permission-type="text"
      :permission-required-text="$t('markdown.permissionRequired')"
      :login-auth-text="$t('markdown.loginOrAuth')"
      @permission-change="handlePermissionChange"
      @navigate-to-admin="navigateToAdmin"
    />

    <!-- 编辑器组件 -->
    <div class="editor-wrapper">
      <div class="flex flex-col md:flex-row gap-4">
        <VditorUnified
          ref="editorRef"
          :dark-mode="darkMode"
          :is-plain-text-mode="isPlainTextMode"
          v-model="editorContent"
          @editor-ready="handleEditorReady"
          @content-change="handleContentChange"
          @import-file="triggerImportFile"
          @clear-content="clearEditorContent"
          @show-copy-formats="showCopyFormatsMenu"
        />
      </div>
    </div>

    <!-- 表单组件 -->
    <EditorForm
      ref="formRef"
      :dark-mode="darkMode"
      :has-permission="hasPermission"
      :is-submitting="isSubmitting"
      :saving-status="savingStatus"
      :show-tags="canManageTags"
      :available-tags="availableTags"
      :tags-loading="tagsLoading"
      :tags-error="tagsError"
      @submit="saveContent"
      @form-change="handleFormChange"
    />

    <!-- 分享链接组件 -->
    <ShareLinkBox
      ref="shareLinkRef"
      :dark-mode="darkMode"
      :label="t('markdown.shareLink')"
      :share-link="shareLink"
      :copy-tooltip="t('markdown.copyLink')"
      :copy-success-text="t('markdown.linkCopied')"
      :copy-failure-text="t('markdown.copyFailed')"
      :show-qr-button="true"
      :qr-tooltip="t('markdown.showQRCode')"
      :secondary-link="rawShareLink"
      :secondary-tooltip="t('markdown.copyRawLink')"
      :secondary-success-text="t('markdown.rawLinkCopied')"
      :secondary-failure-text="t('markdown.copyFailed')"
      :show-countdown="true"
      :countdown-seconds="15"
      :countdown-formatter="formatCountdownText"
      @show-qr-code="showQRCode"
      @status-message="handleStatusMessage"
      @countdown-end="handleCountdownEnd"
    />

    <!-- 二维码弹窗组件 -->
    <QRCodeModal :visible="showQRCodeModal" :share-link="shareLink" @close="closeQRCodeModal" @status-message="handleStatusMessage" />

    <!-- 复制格式菜单组件 -->
    <CopyFormatMenu
      :visible="copyFormatMenuVisible"
      :position="copyFormatMenuPosition"
      :editor="currentEditor"
      :dark-mode="darkMode"
      @close="closeCopyFormatMenu"
      @status-message="handleStatusMessage"
    />
  </div>
</template>

<script setup>
import { ref, computed, onMounted, onUnmounted, nextTick, watch } from "vue";
import { useDebounceFn, useEventListener, useLocalStorage, useTimeoutFn } from "@vueuse/core";
import { useI18n } from "vue-i18n";
import { useAuthStore } from "@/stores/authStore";
import { usePasteService } from "@/modules/paste";
import { useGlobalMessage } from "@/composables/core/useGlobalMessage.js";
import { createLogger } from "@/utils/logger.js";

// 导入子组件
import VditorUnified from "@/components/common/VditorUnified.vue";
import PermissionManager from "@/components/common/PermissionManager.vue";
import EditorForm from "@/modules/paste/editor/components/EditorForm.vue";
import ShareLinkBox from "@/components/common/ShareLinkBox.vue";
import QRCodeModal from "@/modules/paste/editor/components/QRCodeModal.vue";
import CopyFormatMenu from "@/modules/paste/editor/components/CopyFormatMenu.vue";

const { t } = useI18n();
const log = createLogger("MarkdownEditor");

// 使用认证Store
const authStore = useAuthStore();
const pasteService = usePasteService();
const { showSuccess, showError, showWarning, showInfo } = useGlobalMessage();

// Props
const props = defineProps({
  darkMode: {
    type: Boolean,
    default: false,
  },
});

// 组件引用
const editorRef = ref(null);
const formRef = ref(null);
const shareLinkRef = ref(null);
const markdownImporter = ref(null);

// 状态变量
const savingStatus = ref("");
const isSubmitting = ref(false);
const shareLink = ref("");
const currentSharePassword = ref("");
const draftContent = useLocalStorage("cloudpaste-content", "");

// savingStatus 自动清理
const clearSavingStatusDelayMs = ref(0);
const { start: startClearSavingStatus, stop: stopClearSavingStatus } = useTimeoutFn(
  () => {
    savingStatus.value = "";
  },
  clearSavingStatusDelayMs,
  { immediate: false }
);

const rawShareLink = computed(() => {
  if (!shareLink.value) return "";
  const slug = shareLink.value.split("/").pop();
  return slug ? pasteService.getRawPasteUrl(slug, currentSharePassword.value || null) : "";
});

const formatCountdownText = (seconds) => t("markdown.linkExpireIn", { seconds });
const isPlainTextMode = ref(false);
const editorContent = ref("");
const currentEditor = ref(null);

// 从Store获取权限状态的计算属性（文本分享创建权限）
const hasPermission = computed(() => authStore.hasTextSharePermission);
const canManageTags = computed(
  () => authStore.isAdmin || (authStore.authType === "apikey" && !authStore.isGuest && authStore.hasTextManagePermission)
);
const availableTags = ref([]);
const tagsLoading = ref(false);
const tagsError = ref("");

const loadTags = async () => {
  if (!canManageTags.value) {
    availableTags.value = [];
    tagsError.value = "";
    return;
  }

  tagsLoading.value = true;
  tagsError.value = "";
  try {
    const result = await pasteService.getTags();
    availableTags.value = result.tags;
  } catch (error) {
    log.error("加载标签失败:", error);
    tagsError.value = error?.message || t("markdown.form.tagsLoadFailed");
  } finally {
    tagsLoading.value = false;
  }
};

watch(canManageTags, loadTags, { immediate: true });

// 二维码弹窗状态
const showQRCodeModal = ref(false);

// 复制格式菜单状态
const copyFormatMenuVisible = ref(false);
const copyFormatMenuPosition = ref({ x: 0, y: 0 });

// 权限变化处理 - 当权限状态改变时执行相应的业务逻辑
const handlePermissionChange = (hasPermissionValue) => {
  log.debug("权限状态变化", hasPermissionValue);
  // 权限状态会自动更新，这里只需要记录日志
};

const handleEditorReady = (editor) => {
  currentEditor.value = editor;

  // 验证编辑器实例
  if (!editor || typeof editor.getValue !== "function" || typeof editor.getHTML !== "function") {
    log.error("Editor instance validation failed, missing required methods");
  }
};

const handleContentChange = (content) => {
  editorContent.value = content;
  autoSaveDebounce();
};

const handleFormChange = (formData) => {
  // 表单数据变化处理
  log.debug("Form data changed:", formData);
};

const handleStatusMessage = (payload) => {
  const message = typeof payload === "string" ? payload : payload?.message;
  const type = typeof payload === "object" && payload?.type ? payload.type : "info";

  if (!message) return;

  savingStatus.value = message;

  if (type === "error") {
    showError(message);
  } else if (type === "success") {
    showSuccess(message);
  } else if (type === "warning") {
    showWarning(message);
  } else {
    showInfo(message);
  }

  stopClearSavingStatus();
  clearSavingStatusDelayMs.value = type === "error" ? 4000 : 3000;
  startClearSavingStatus();
};

const handleCountdownEnd = () => {
  shareLink.value = "";
  currentSharePassword.value = "";
};

// 导航到管理页面
const navigateToAdmin = () => {
  // 使用全局 routerUtils 进行导航
  import("@/router").then(({ routerUtils }) => {
    routerUtils.navigateTo("admin");
  });
};

// 编辑器模式切换
const toggleEditorMode = () => {
  isPlainTextMode.value = !isPlainTextMode.value;
  log.debug("切换编辑器模式:", isPlainTextMode.value ? t("markdown.switchToPlainText") : t("markdown.switchToMarkdown"));
};

// 触发文件导入
const triggerImportFile = () => {
  if (markdownImporter.value) {
    markdownImporter.value.click();
  }
};

// 导入Markdown文件
const importMarkdownFile = (event) => {
  const file = event.target.files[0];
  if (!file) return;

  const reader = new FileReader();
  reader.onload = (e) => {
    const content = e.target.result;
    if (editorRef.value) {
      editorRef.value.setValue(content);
    }
    editorContent.value = content;
  };
  reader.readAsText(file);

  // 清空文件输入
  event.target.value = "";
};

// 清空编辑器内容
const clearEditorContent = () => {
  if (editorRef.value) {
    editorRef.value.clearContent();
  }
  editorContent.value = "";
};

// 根据当前工具栏按钮位置更新复制格式菜单的位置
const updateCopyFormatMenuPosition = () => {
  if (!copyFormatMenuVisible.value) {
    return;
  }

  const copyFormatBtn = document.querySelector('.vditor-toolbar .vditor-tooltipped[data-type="copy-formats"]');
  if (copyFormatBtn) {
    const rect = copyFormatBtn.getBoundingClientRect();
    copyFormatMenuPosition.value = {
      x: rect.left,
      y: rect.bottom + 5,
    };
  }
};

// 显示复制格式菜单
const showCopyFormatsMenu = (position) => {
  if (!currentEditor.value) return;

  // 如果传入了位置参数，直接使用
  if (position && position.x !== undefined && position.y !== undefined) {
    copyFormatMenuPosition.value = position;
  } else {
    // 否则尝试获取工具栏中复制格式按钮的位置
    const copyFormatBtn = document.querySelector('.vditor-toolbar .vditor-tooltipped[data-type="copy-formats"]');
    if (copyFormatBtn) {
      const rect = copyFormatBtn.getBoundingClientRect();
      copyFormatMenuPosition.value = {
        x: rect.left,
        y: rect.bottom + 5,
      };
    } else {
      // 使用默认位置
      copyFormatMenuPosition.value = { x: 100, y: 100 };
    }
  }

  copyFormatMenuVisible.value = true;
};

// 关闭复制格式菜单
const closeCopyFormatMenu = () => {
  copyFormatMenuVisible.value = false;
};

// 窗口尺寸变化时，保持菜单跟随工具栏按钮（自动清理）
useEventListener(window, "resize", updateCopyFormatMenuPosition);

// 显示二维码
const showQRCode = () => {
  showQRCodeModal.value = true;
};

// 关闭二维码弹窗
const closeQRCodeModal = () => {
  showQRCodeModal.value = false;
};

// 保存内容
const saveContent = async (formData) => {
  if (!hasPermission.value) {
    handleStatusMessage(t("common.noPermission"));
    return;
  }

  if (!editorContent.value.trim()) {
    handleStatusMessage(t("markdown.messages.contentEmpty"));
    return;
  }

  isSubmitting.value = true;
  handleStatusMessage(t("markdown.messages.creating"));

  try {
    // 准备要提交的数据 - 只传递有值的字段
    const pasteData = {
      content: editorContent.value,
    };

    // 只有当字段有值时才添加到请求中
    if (formData.customLink && formData.customLink.trim()) {
      pasteData.slug = formData.customLink.trim();
    }

    if (formData.title && formData.title.trim()) {
      pasteData.title = formData.title.trim();
    }

    if (formData.remark && formData.remark.trim()) {
      pasteData.remark = formData.remark.trim();
    }

    if (formData.password && formData.password.trim()) {
      pasteData.password = formData.password.trim();
    }

    if (formData.max_views && parseInt(formData.max_views) > 0) {
      pasteData.max_views = parseInt(formData.max_views);
    }

    // 始终包含 is_public 字段（布尔值）
    if (typeof formData.is_public === "boolean") {
      pasteData.is_public = formData.is_public;
    }

    if (canManageTags.value && Array.isArray(formData.tag_ids)) {
      pasteData.tag_ids = formData.tag_ids;
    }

    // 处理过期时间
    const expiryHours = parseInt(formData.expiry_time);
    if (expiryHours > 0) {
      const expiresAt = new Date();
      expiresAt.setHours(expiresAt.getHours() + expiryHours);
      pasteData.expires_at = expiresAt.toISOString();
    }

    log.debug("创建分享，数据:", pasteData);

    // 调用领域 service，统一返回 slug
    const slug = await pasteService.createPaste(pasteData);
    log.debug("创建分享结果 slug:", slug);

    // 构建分享链接
    shareLink.value = `${window.location.origin}/paste/${slug}`;
    currentSharePassword.value = formData.password || "";

    // 启动倒计时
    if (shareLinkRef.value) {
      shareLinkRef.value.startCountdown();
    }

    // 重置表单
    if (formRef.value) {
      formRef.value.resetForm();
    }

    handleStatusMessage(t("markdown.messages.createSuccess"));
  } catch (error) {
    log.error("保存失败:", error);

    // 根据错误消息内容进行分类处理
    if (error.message && error.message.includes("已被占用")) {
      handleStatusMessage(t("markdown.messages.linkOccupied"));
    } else if (error.message && error.message.includes("权限")) {
      handleStatusMessage(t("common.noPermission"));
    } else if (error.message && error.message.includes("内容过大")) {
      handleStatusMessage(t("markdown.messages.contentTooLarge"));
    } else {
      handleStatusMessage(`${t("markdown.messages.createFailed")}: ${error.message || t("markdown.messages.unknownError")}`);
    }
  } finally {
    isSubmitting.value = false;
  }
};

// 自动保存
const autoSaveDebounce = useDebounceFn(() => {
  try {
    draftContent.value = editorContent.value;
  } catch (e) {
    log.warn(t("markdown.messages.autoSaveFailed"), e);
  }
}, 1000);

// 组件挂载
onMounted(async () => {
  // 恢复保存的内容
  try {
    if (draftContent.value) {
      editorContent.value = draftContent.value;
    }
  } catch (e) {
    log.warn(t("markdown.messages.restoreContentFailed"), e);
  }
});

// 组件卸载
onUnmounted(() => {
  stopClearSavingStatus();
  autoSaveDebounce.cancel?.();
});
</script>

<style scoped>
.editor-container {
  min-height: 700px;
  box-sizing: border-box; /* 确保内边距不增加元素实际宽度 */
}

/* 移动端优化 */
@media (max-width: 640px) {
  .editor-container {
    padding-left: 0.5rem;
    padding-right: 0.5rem;
    width: 100%;
    overflow-x: hidden;
  }

  .form-input,
  .form-label {
    width: 100%;
    max-width: 100%;
  }

  .form-group {
    margin-bottom: 0.75rem;
  }

  /* 确保分享链接区域不溢出 */
  .share-link-box {
    max-width: 100%;
    overflow-x: hidden;
  }
}

/* 添加表单响应式样式 */
.form-input {
  width: 100%;
  max-width: 100%;
  padding: 0.5rem;
  border-width: 1px;
  border-radius: 0.375rem;
}

.form-label {
  display: block;
  margin-bottom: 0.5rem;
  font-size: 0.875rem;
}

/* 添加新的过渡动画 */
@keyframes slideDown {
  from {
    opacity: 0;
    transform: translateY(-8px);
  }
  to {
    opacity: 1;
    transform: translateY(0);
  }
}

/* 应用动画到分享链接区域 */
.mt-4 {
  animation: slideDown 0.25s ease-out;
}

.btn-primary {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  padding: 0.5rem 1rem;
  font-weight: 500;
  border-radius: 0.375rem;
  background-color: v-bind('props.darkMode ? "#3b82f6" : "#2563eb"');
  color: white;
  transition: background-color 0.2s;
}

.btn-primary:hover {
  background-color: v-bind('props.darkMode ? "#2563eb" : "#1d4ed8"');
}

.btn-primary:disabled {
  opacity: 0.7;
  cursor: not-allowed;
}

/* 分享链接样式 */
.share-link-box {
  animation: fadeIn 0.3s ease-out;
  border: 1px solid v-bind('props.darkMode ? "rgba(75, 85, 99, 0.3)" : "rgba(229, 231, 235, 0.8)"');
}

.link-text {
  text-decoration: none;
  word-break: break-all;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.link-text:hover {
  text-decoration: underline;
}

@keyframes fadeIn {
  from {
    opacity: 0;
  }
  to {
    opacity: 1;
  }
}

/* 复制格式菜单样式 */
#copyFormatMenu {
  min-width: 180px;
  box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06);
  transition: opacity 0.2s ease-in-out, transform 0.2s ease-in-out;
  transform-origin: top left;
}

#copyFormatMenu div {
  transition: background-color 0.15s ease-in-out;
}
</style>
