<template>
  <div
    v-if="visible"
    id="pasteCopyFormatMenu"
    class="absolute bg-white dark:bg-gray-800 rounded-lg shadow-lg py-1 z-50 border border-gray-200 dark:border-gray-700"
    :style="{ top: `${position.y}px`, left: `${position.x}px` }"
  >
    <div class="px-4 py-2 hover:bg-gray-100 dark:hover:bg-gray-700 cursor-pointer flex items-center" @click="copyAsMarkdown">
      <svg class="h-4 w-4 mr-2" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
        <path d="M14 3v4a1 1 0 0 0 1 1h4" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" />
        <path d="M17 21H7a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h7l5 5v11a2 2 0 0 1-2 2z" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" />
        <path d="M9 9h1v6h1" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" />
        <path d="M15 15h-2v-6" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" />
      </svg>
      <span>复制为Markdown</span>
    </div>
    <div class="px-4 py-2 hover:bg-gray-100 dark:hover:bg-gray-700 cursor-pointer flex items-center" @click="copyAsHTML">
      <svg class="h-4 w-4 mr-2" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
        <path
          d="M9 16H5a2 2 0 0 1-2-2V6a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2v8a2 2 0 0 1-2 2h-4l-4 4z"
          stroke="currentColor"
          stroke-width="2"
          stroke-linecap="round"
          stroke-linejoin="round"
        />
        <path d="M8 9l3 3-3 3" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" />
        <path d="M16 15l-3-3 3-3" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" />
      </svg>
      <span>复制为HTML</span>
    </div>
    <div class="px-4 py-2 hover:bg-gray-100 dark:hover:bg-gray-700 cursor-pointer flex items-center" @click="copyAsPlainText">
      <svg class="h-4 w-4 mr-2" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
        <path d="M14 3v4a1 1 0 0 0 1 1h4" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" />
        <path d="M17 21H7a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h7l5 5v11a2 2 0 0 1-2 2z" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" />
        <path d="M9 9h6" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" />
        <path d="M9 13h6" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" />
        <path d="M9 17h4" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" />
      </svg>
      <span>复制为纯文本</span>
    </div>
    <div class="px-4 py-2 hover:bg-gray-100 dark:hover:bg-gray-700 cursor-pointer flex items-center" @click="exportWordDocument">
      <svg class="h-4 w-4 mr-2" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
        <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8l-6-6z" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" />
        <path d="M14 2v6h6" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" />
        <path d="M16 13H8" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" />
        <path d="M16 17H8" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" />
        <path d="M10 9H8" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" />
      </svg>
      <span>导出为Word文档</span>
    </div>
    <div class="px-4 py-2 hover:bg-gray-100 dark:hover:bg-gray-700 cursor-pointer flex items-center" @click="exportAsPng">
      <svg class="h-4 w-4 mr-2" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
        <path d="M14 3v4a1 1 0 0 0 1 1h4" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" />
        <path d="M17 21H7a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h7l5 5v11a2 2 0 0 1-2 2z" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" />
      </svg>
      <span>导出为PNG图片</span>
    </div>
  </div>
</template>

<script setup>
// PasteCopyFormatMenu组件 - paste-view专用的复制格式菜单
// 基于原始PasteViewEditor.vue.bak的复制功能实现
import { ref, watch, onBeforeUnmount } from "vue";
import markdownToWord from "../../utils/markdownToWord";
import { saveAs } from "file-saver";
import htmlToImage from "../../utils/htmlToImage";
import { copyToClipboard as clipboardCopy } from "@/utils/clipboard";

// Props 定义
const props = defineProps({
  visible: { type: Boolean, default: false },
  position: { type: Object, default: () => ({ x: 0, y: 0 }) },
  editor: { type: Object, default: null }, // VditorUnified 组件的 ref
  darkMode: { type: Boolean, default: false },
  isPlainTextMode: { type: Boolean, default: false },
  plainTextContent: { type: String, default: "" },
  documentTitle: { type: String, default: "CloudPaste文档" },
});

// Emits 定义
const emit = defineEmits(["close", "status-message"]);

// 显示状态消息
const showStatusMessage = (message, type = "success") => {
  emit("status-message", { message, type });
};

// 获取编辑器内容
const getEditorContent = () => {
  if (props.isPlainTextMode) {
    return props.plainTextContent || "";
  }
  
  if (props.editor && props.editor.getValue) {
    return props.editor.getValue();
  }
  
  return "";
};

// 获取编辑器HTML内容
const getEditorHTML = () => {
  if (props.isPlainTextMode) {
    // 纯文本模式下，将换行转换为<br>
    return props.plainTextContent.replace(/\n/g, '<br>');
  }
  
  if (props.editor && props.editor.getHTML) {
    return props.editor.getHTML();
  }
  
  return "";
};

// 复制为Markdown格式
const copyAsMarkdown = async () => {
  const content = getEditorContent();
  if (!content) {
    showStatusMessage("没有可复制的内容", "error");
    return;
  }
  
  try {
    await clipboardCopy(content);
    showStatusMessage("已复制为Markdown格式");
    emit("close");
  } catch (error) {
    showStatusMessage("复制失败", "error");
  }
};

// 复制为HTML格式
const copyAsHTML = async () => {
  const htmlContent = getEditorHTML();
  if (!htmlContent) {
    showStatusMessage("没有可复制的内容", "error");
    return;
  }
  
  try {
    await clipboardCopy(htmlContent);
    showStatusMessage("已复制为HTML格式");
    emit("close");
  } catch (error) {
    showStatusMessage("复制失败", "error");
  }
};

// 复制为纯文本格式
const copyAsPlainText = async () => {
  if (props.isPlainTextMode) {
    // 纯文本模式下，直接复制纯文本内容
    try {
      await clipboardCopy(props.plainTextContent);
      showStatusMessage("已复制为纯文本格式");
      emit("close");
    } catch (error) {
      showStatusMessage("复制失败", "error");
    }
    return;
  }
  
  const htmlContent = getEditorHTML();
  if (!htmlContent) {
    showStatusMessage("没有可复制的内容", "error");
    return;
  }
  
  try {
    // 创建一个临时元素来去除HTML标签
    const tempDiv = document.createElement("div");
    tempDiv.innerHTML = htmlContent;
    const plainText = tempDiv.textContent || tempDiv.innerText || "";
    await clipboardCopy(plainText);
    showStatusMessage("已复制为纯文本格式");
    emit("close");
  } catch (error) {
    showStatusMessage("复制失败", "error");
  }
};

// 导出为Word文档
const exportWordDocument = async () => {
  const content = getEditorContent();
  if (!content) {
    showStatusMessage("没有内容可导出", "error");
    return;
  }

  try {
    showStatusMessage("正在生成Word文档...");
    
    const blob = await markdownToWord(content, props.documentTitle);
    saveAs(blob, `${props.documentTitle}.docx`);
    showStatusMessage("Word文档导出成功");
    emit("close");
  } catch (error) {
    console.error("导出Word失败:", error);
    showStatusMessage("导出Word文档失败", "error");
  }
};

// 导出为PNG图片
const exportAsPng = async () => {
  if (props.isPlainTextMode) {
    showStatusMessage("纯文本模式下无法导出为PNG图片", "error");
    return;
  }

  if (!props.editor || !props.editor.editor) {
    showStatusMessage("编辑器未初始化", "error");
    return;
  }

  try {
    showStatusMessage("正在生成PNG图片...");
    
    const result = await htmlToImage.editorContentToPng(props.editor.editor(), {
      title: props.documentTitle,
      filename: `${props.documentTitle}.png`,
      autoSave: false,
      imageOptions: {
        quality: 0.95,
        backgroundColor: props.darkMode ? "#1f2937" : "#ffffff",
      },
    });
    
    if (result.success) {
      saveAs(result.blob, `${props.documentTitle}.png`);
      showStatusMessage("PNG图片导出成功");
    } else {
      throw new Error(result.error);
    }
    emit("close");
  } catch (error) {
    console.error("导出PNG失败:", error);
    showStatusMessage("导出PNG图片失败", "error");
  }
};

// 全局点击处理，用于关闭菜单
const handleGlobalClick = (event) => {
  if (props.visible) {
    const menu = document.getElementById('pasteCopyFormatMenu');
    if (menu && !menu.contains(event.target)) {
      emit("close");
    }
  }
};

// 监听菜单显示状态
watch(
  () => props.visible,
  (visible) => {
    if (visible) {
      // 延迟添加事件监听器，避免立即触发
      setTimeout(() => {
        document.addEventListener('click', handleGlobalClick);
      }, 0);
    } else {
      document.removeEventListener('click', handleGlobalClick);
    }
  }
);

// 清理
onBeforeUnmount(() => {
  document.removeEventListener('click', handleGlobalClick);
});
</script>

<style scoped>
#pasteCopyFormatMenu {
  min-width: 200px;
  box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06);
  transition: opacity 0.2s ease-in-out, transform 0.2s ease-in-out;
  transform-origin: top left;
}

#pasteCopyFormatMenu div {
  transition: background-color 0.15s ease-in-out;
}
</style>
