<template>
  <div
    v-if="visible"
    id="copyFormatMenu"
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
      <span>{{ $t("markdown.copyAsMarkdown") }}</span>
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
      <span>{{ $t("markdown.copyAsHTML") }}</span>
    </div>
    <div class="px-4 py-2 hover:bg-gray-100 dark:hover:bg-gray-700 cursor-pointer flex items-center" @click="copyAsPlainText">
      <svg class="h-4 w-4 mr-2" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
        <path d="M14 3v4a1 1 0 0 0 1 1h4" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" />
        <path d="M17 21H7a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h7l5 5v11a2 2 0 0 1-2 2z" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" />
        <path d="M9 9h6" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" />
        <path d="M9 13h6" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" />
        <path d="M9 17h4" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" />
      </svg>
      <span>{{ $t("markdown.copyAsPlainText") }}</span>
    </div>
    <div class="px-4 py-2 hover:bg-gray-100 dark:hover:bg-gray-700 cursor-pointer flex items-center" @click="exportWordDocument">
      <svg class="h-4 w-4 mr-2" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
        <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8l-6-6z" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" />
        <path d="M14 2v6h6" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" />
        <path d="M16 13H8" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" />
        <path d="M16 17H8" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" />
        <path d="M10 9H8" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" />
      </svg>
      <span>{{ $t("markdown.exportAsWord") }}</span>
    </div>
    <div class="px-4 py-2 hover:bg-gray-100 dark:hover:bg-gray-700 cursor-pointer flex items-center" @click="exportAsPng">
      <svg class="h-4 w-4 mr-2" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
        <path d="M19 21H5a2 2 0 01-2-2V5a2 2 0 012-2h11l5 5v11a2 2 0 01-2 2z" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" />
        <path d="M17 21v-8h-8v8" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" />
        <path d="M7 3v5h5" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" />
      </svg>
      <span>{{ $t("markdown.exportAsPng") }}</span>
    </div>
  </div>
</template>

<script setup>
import { ref, onMounted, onUnmounted } from "vue";
import { useI18n } from "vue-i18n";
import { copyToClipboard as clipboardCopy } from "@/utils/clipboard";
import markdownToWord from "../../utils/markdownToWord";
import { saveAs } from "file-saver";
import htmlToImage from "@/utils/htmlToImage";

const { t } = useI18n();

// Props
const props = defineProps({
  visible: {
    type: Boolean,
    default: false,
  },
  position: {
    type: Object,
    default: () => ({ x: 0, y: 0 }),
  },
  editor: {
    type: Object,
    default: null,
  },
  darkMode: {
    type: Boolean,
    default: false,
  },
});

// Emits
const emit = defineEmits(["close", "status-message"]);

// 复制为Markdown格式
const copyAsMarkdown = () => {
  if (!props.editor || typeof props.editor.getValue !== "function") {
    emit("status-message", t("markdown.messages.editorNotReady"));
    return;
  }
  const mdContent = props.editor.getValue();
  copyToClipboard(mdContent, t("markdown.markdownCopied"));
  emit("close");
};

// 复制为HTML格式
const copyAsHTML = () => {
  if (!props.editor || typeof props.editor.getHTML !== "function") {
    emit("status-message", t("markdown.messages.editorNotReady"));
    return;
  }
  const htmlContent = props.editor.getHTML();
  copyToClipboard(htmlContent, t("markdown.htmlCopied"));
  emit("close");
};

// 复制为纯文本格式
const copyAsPlainText = () => {
  if (!props.editor || typeof props.editor.getHTML !== "function") {
    emit("status-message", t("markdown.messages.editorNotReady"));
    return;
  }
  const htmlContent = props.editor.getHTML();
  const tempDiv = document.createElement("div");
  tempDiv.innerHTML = htmlContent;
  const plainText = tempDiv.textContent || tempDiv.innerText || "";
  copyToClipboard(plainText, t("markdown.plainTextCopied"));
  emit("close");
};

// 导出为Word文档
const exportWordDocument = async () => {
  if (!props.editor || typeof props.editor.getValue !== "function") {
    emit("status-message", t("markdown.messages.editorNotReady"));
    return;
  }

  emit("status-message", t("markdown.messages.generatingWord"));

  try {
    const markdownContent = props.editor.getValue();

    if (!markdownContent) {
      emit("status-message", t("markdown.messages.contentEmpty"));
      return;
    }

    const blob = await markdownToWord(markdownContent, {
      title: t("markdown.exportDocumentTitle"),
    });

    const now = new Date();
    const dateStr = now.toISOString().slice(0, 10);
    const timeStr = now.toTimeString().slice(0, 8).replace(/:/g, "-");
    const fileName = `markdown-${dateStr}-${timeStr}.docx`;

    saveAs(blob, fileName);
    emit("status-message", t("markdown.messages.wordExported"));
  } catch (error) {
    console.error("导出Word文档时出错:", error);
    emit("status-message", t("markdown.messages.wordExportFailed"));
  } finally {
    emit("close");
  }
};

// 导出为PNG图片
const exportAsPng = async () => {
  if (!props.editor || typeof props.editor.getValue !== "function") {
    console.error("导出PNG失败：编辑器实例不存在");
    emit("status-message", t("markdown.messages.editorNotReady"));
    return;
  }

  emit("status-message", t("markdown.messages.exportingPng"));

  try {
    const now = new Date();
    const dateStr = now.toISOString().slice(0, 10);
    const timeStr = now.toTimeString().slice(0, 8).replace(/:/g, "-");
    const fileName = `markdown-${dateStr}-${timeStr}.png`;

    const editorContainer = document.getElementById("vditor");
    await new Promise((resolve) => setTimeout(resolve, 1000));

    const result = await htmlToImage.editorContentToPng(props.editor, {
      filename: fileName,
      imageOptions: {
        quality: 1.0,
        backgroundColor: props.darkMode ? "#1e1e1e" : "#ffffff",
        style: {
          "max-width": "100%",
          width: "auto",
        },
        cacheBust: true,
        imagePlaceholder: "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNkYAAAAAYAAjCB0C8AAAAASUVORK5CYII=",
        skipFonts: false,
        pixelRatio: 4,
        canvasWidth: editorContainer ? editorContainer.offsetWidth : undefined,
        canvasHeight: undefined,
      },
      beforeCapture: async (targetElement) => {
        const tempStyle = document.createElement("style");
        tempStyle.id = "temp-export-style";
        tempStyle.textContent = `
          .vditor-reset {
            padding: 20px !important;
            box-sizing: border-box !important;
          }
          .vditor-reset pre {
            white-space: pre-wrap !important;
            word-break: break-all !important;
            overflow: visible !important;
            background-color: #f6f8fa !important;
            border-radius: 4px !important;
            padding: 12px 16px !important;
            margin: 1em 0 !important;
          }
          .vditor-reset pre code {
            font-family: monospace, Consolas, "Courier New", monospace !important;
            font-size: 13px !important;
            line-height: 1.5 !important;
            white-space: pre-wrap !important;
            tab-size: 4 !important;
            word-break: keep-all !important;
          }
          .vditor-reset img {
            max-width: 100% !important;
            image-rendering: auto !important;
          }
          .vditor-reset table {
            display: table !important;
            width: auto !important;
            max-width: 100% !important;
            overflow: visible !important;
            border-collapse: collapse !important;
            margin: 1em 0 !important;
          }
          .vditor-reset table th,
          .vditor-reset table td {
            border: 1px solid #ddd !important;
            padding: 8px 12px !important;
          }
        `;
        document.head.appendChild(tempStyle);

        const images = targetElement.querySelectorAll("img");
        images.forEach((img) => {
          if (!img.hasAttribute("crossorigin")) {
            img.setAttribute("crossorigin", "anonymous");
          }
        });

        await new Promise((resolve) => setTimeout(resolve, 500));
        return Promise.resolve();
      },
      afterCapture: (targetElement) => {
        const tempStyle = document.getElementById("temp-export-style");
        if (tempStyle) {
          document.head.removeChild(tempStyle);
        }
        return Promise.resolve();
      },
      onSuccess: (dataUrl, blob) => {
        emit("status-message", t("markdown.messages.pngExported"));
      },
      onError: (error) => {
        console.error("导出PNG图片时出错:", error);

        if (error instanceof Event && error.type === "error" && error.target instanceof HTMLImageElement) {
          emit("status-message", t("markdown.messages.corsImageError"));
        } else {
          emit("status-message", t("markdown.messages.pngExportFailed") + ": " + (error.message || t("markdown.messages.unknownError")));
        }
      },
    });

    if (!result || !result.success) {
      const errorMsg =
        result && result.error instanceof Event && result.error.type === "error" && result.error.target instanceof HTMLImageElement
          ? t("markdown.messages.corsImageError")
          : t("markdown.messages.pngExportFailed");

      throw result?.error || new Error(errorMsg);
    }
  } catch (error) {
    console.error("导出PNG图片过程中发生错误:", error);

    if (error instanceof Event && error.type === "error") {
      emit("status-message", t("markdown.messages.corsImageError"));
    } else {
      emit("status-message", t("markdown.messages.pngExportFailed"));
    }
  } finally {
    emit("close");
  }
};

// 通用复制到剪贴板函数
const copyToClipboard = async (text, successMessage) => {
  if (!text) {
    emit("status-message", t("markdown.messages.contentEmpty"));
    return;
  }

  try {
    const success = await clipboardCopy(text);

    if (success) {
      emit("status-message", successMessage);
    } else {
      throw new Error(t("markdown.copyFailed"));
    }
  } catch (e) {
    console.error("复制失败:", e);
    emit("status-message", t("markdown.copyFailed"));
  }
};

// 全局点击事件处理
const handleGlobalClick = (event) => {
  const menu = document.getElementById("copyFormatMenu");
  if (menu && !menu.contains(event.target) && !event.target.closest('.vditor-toolbar button[data-type="copy-formats"]') && props.visible) {
    emit("close");
  }
};

// 组件挂载
onMounted(() => {
  document.addEventListener("click", handleGlobalClick);
});

// 组件卸载
onUnmounted(() => {
  document.removeEventListener("click", handleGlobalClick);
});
</script>

<style scoped>
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
