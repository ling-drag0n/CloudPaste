<script setup>
// HTML预览弹窗组件 - 用于在弹窗中安全预览HTML代码
// 该组件使用iframe实现HTML的安全渲染，并提供复制代码、在新窗口打开等功能
import { ref, watch, onMounted, nextTick, onUnmounted, computed } from "vue";

// 定义组件接受的属性
const props = defineProps({
  // HTML 代码内容
  htmlContent: {
    type: String,
    default: "",
  },
  // 是否显示弹窗
  show: {
    type: Boolean,
    default: false,
  },
  // 暗色模式
  darkMode: {
    type: Boolean,
    default: false,
  },
  // 内容类型
  contentType: {
    type: String,
    default: "html",
  },
});

// 定义组件事件
const emit = defineEmits(["close", "open-external"]);

// iframe 引用
const iframeRef = ref(null);
// 渲染状态
const renderState = ref("idle"); // 'idle', 'loading', 'rendered', 'error'
// 错误信息
const errorMessage = ref("");

// 全屏状态
const isFullscreen = ref(false);

// 计算HTML模板
const htmlTemplate = computed(() => {
  if (!props.htmlContent) return "";

  return `
<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>HTML预览</title>
  <style>
    body {
      margin: 0;
      padding: 16px;
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
      line-height: 1.6;
      color: ${props.darkMode ? "#d4d4d4" : "#333"};
      background-color: ${props.darkMode ? "#1a1a1a" : "#fff"};
    }

    /* 响应式设计 */
    @media (max-width: 768px) {
      body {
        padding: 8px;
        font-size: 14px;
      }
    }

    /* 基础样式重置 */
    * {
      box-sizing: border-box;
    }

    img {
      max-width: 100%;
      height: auto;
    }

    table {
      width: 100%;
      border-collapse: collapse;
      margin: 1em 0;
    }

    th, td {
      border: 1px solid ${props.darkMode ? "#444" : "#ddd"};
      padding: 8px;
      text-align: left;
    }

    th {
      background-color: ${props.darkMode ? "#333" : "#f5f5f5"};
    }

    pre {
      background-color: ${props.darkMode ? "#2d2d2d" : "#f5f5f5"};
      padding: 1em;
      border-radius: 4px;
      overflow-x: auto;
    }

    code {
      background-color: ${props.darkMode ? "#2d2d2d" : "#f5f5f5"};
      padding: 2px 4px;
      border-radius: 2px;
      font-family: 'Courier New', monospace;
    }
  </style>
</head>
<body>
  ${props.htmlContent}
</body>
</html>
  `.trim();
});

// 切换全屏
const toggleFullscreen = () => {
  isFullscreen.value = !isFullscreen.value;
};

// 监听ESC键
onMounted(() => {
  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape" && isFullscreen.value) {
      isFullscreen.value = false;
    }
  });
});

// 组件卸载时移除事件监听
onUnmounted(() => {
  document.removeEventListener("keydown", () => {});
});

// 监听 show 和 htmlContent 变化
watch([() => props.show, () => props.htmlContent], ([newShow, newHtml]) => {
  if (newShow && newHtml) {
    nextTick(() => renderHtml());
  }
});

// 处理关闭弹窗
const closeModal = () => {
  emit("close");
};

// 渲染 HTML 内容
const renderHtml = () => {
  if (!props.htmlContent) return;

  try {
    renderState.value = "loading";
    // 使用nextTick确保DOM更新后再设置状态
    nextTick(() => {
      renderState.value = "rendered";
    });
  } catch (error) {
    console.error("渲染 HTML 时出错:", error);
    errorMessage.value = error.message || "渲染 HTML 时发生错误";
    renderState.value = "error";
  }
};

// 在新窗口中打开
const openInNewWindow = () => {
  emit("open-external", props.htmlContent);
};

// 复制 HTML 代码
const copyHtml = () => {
  navigator.clipboard
    .writeText(props.htmlContent)
    .then(() => {
      // 显示复制成功提示
      const copyButton = document.querySelector(".copy-button");
      if (copyButton) {
        copyButton.textContent = "已复制";
        setTimeout(() => {
          copyButton.textContent = "复制代码";
        }, 2000);
      }
    })
    .catch((err) => {
      console.error("复制失败:", err);
    });
};

// 组件挂载时，如果弹窗显示就渲染内容
onMounted(() => {
  if (props.show && props.htmlContent) {
    renderHtml();
  }
});
</script>

<template>
  <div v-if="show" class="html-preview-modal">
    <div class="modal-overlay" @click="closeModal"></div>
    <div class="modal-container" :class="{ 'dark-mode': darkMode, fullscreen: isFullscreen }">
      <div class="modal-header">
        <h3>{{ contentType === "svg" ? "SVG 预览" : "HTML 预览" }}</h3>
        <div class="modal-actions">
          <button class="action-button copy-button" @click="copyHtml">复制代码</button>
          <button class="action-button" @click="openInNewWindow">在新窗口打开</button>
          <button class="action-button" @click="toggleFullscreen">
            <svg
              xmlns="http://www.w3.org/2000/svg"
              width="16"
              height="16"
              viewBox="0 0 24 24"
              fill="none"
              stroke="currentColor"
              stroke-width="2"
              stroke-linecap="round"
              stroke-linejoin="round"
            >
              <path d="M8 3H5a2 2 0 0 0-2 2v3m18 0V5a2 2 0 0 0-2-2h-3m0 18h3a2 2 0 0 0 2-2v-3M3 16v3a2 2 0 0 0 2 2h3"></path>
            </svg>
          </button>
          <button class="close-button" @click="closeModal">
            <svg
              xmlns="http://www.w3.org/2000/svg"
              width="20"
              height="20"
              viewBox="0 0 24 24"
              fill="none"
              stroke="currentColor"
              stroke-width="2"
              stroke-linecap="round"
              stroke-linejoin="round"
            >
              <line x1="18" y1="6" x2="6" y2="18"></line>
              <line x1="6" y1="6" x2="18" y2="18"></line>
            </svg>
          </button>
        </div>
      </div>

      <div class="modal-content">
        <!-- 加载状态 -->
        <div v-if="renderState === 'loading'" class="loading-state">
          <svg class="spinner" viewBox="0 0 50 50">
            <circle class="path" cx="25" cy="25" r="20" fill="none" stroke-width="5"></circle>
          </svg>
          <p>正在渲染 HTML...</p>
        </div>

        <!-- 错误状态 -->
        <div v-else-if="renderState === 'error'" class="error-state">
          <p>渲染失败: {{ errorMessage }}</p>
        </div>

        <!-- iframe 预览 -->
        <iframe
          v-show="renderState === 'rendered'"
          ref="iframeRef"
          class="preview-iframe"
          :srcdoc="htmlTemplate"
          sandbox="allow-same-origin allow-scripts"
          title="HTML 预览"
          @load="renderState = 'rendered'"
        ></iframe>
      </div>
    </div>
  </div>
</template>

<style scoped>
.html-preview-modal {
  position: fixed;
  top: 0;
  left: 0;
  width: 100%;
  height: 100%;
  z-index: 1060;
  display: flex;
  justify-content: center;
  align-items: center;
  padding: 8px;
  padding-top: 80px;
}

@media (min-width: 640px) {
  .html-preview-modal {
    padding: 16px;
    padding-top: 16px;
  }
}

.modal-overlay {
  position: absolute;
  top: 0;
  left: 0;
  width: 100%;
  height: 100%;
  background-color: rgba(0, 0, 0, 0.7);
  z-index: -1;
}

.modal-container {
  background-color: #ffffff;
  border-radius: 8px;
  box-shadow: 0 4px 16px rgba(0, 0, 0, 0.2);
  width: 100%;
  max-width: 320px;
  height: 85%;
  max-height: 600px;
  display: flex;
  flex-direction: column;
  overflow: hidden;
  transition: all 0.3s ease;
}

@media (min-width: 640px) {
  .modal-container {
    max-width: 900px;
    height: 80%;
    max-height: 700px;
  }
}

.modal-container.dark-mode {
  background-color: #1e1e1e;
  color: #d4d4d4;
  border: 1px solid #333;
}

.modal-container.fullscreen {
  width: 100%;
  height: 100%;
  max-width: none;
  max-height: none;
  border-radius: 0;
  position: fixed;
  top: 0;
  left: 0;
  z-index: 1001;
}

.modal-header {
  padding: 16px;
  border-bottom: 1px solid #e5e7eb;
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.dark-mode .modal-header {
  border-color: #333;
}

.modal-header h3 {
  margin: 0;
  font-size: 18px;
  font-weight: 600;
}

.modal-actions {
  display: flex;
  gap: 8px;
  align-items: center;
}

.action-button {
  background-color: #f3f4f6;
  border: none;
  padding: 8px 16px;
  border-radius: 4px;
  font-size: 14px;
  cursor: pointer;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  color: #374151;
  min-height: 36px;
  transition: all 0.2s ease;
}

.dark-mode .action-button {
  background-color: #2d2d2d;
  color: #d4d4d4;
}

.action-button:hover {
  background-color: #e5e7eb;
}

.dark-mode .action-button:hover {
  background-color: #3d3d3d;
}

.action-button svg {
  width: 16px;
  height: 16px;
}

.close-button {
  background: none;
  border: none;
  cursor: pointer;
  color: #6b7280;
  padding: 4px;
  display: flex;
  align-items: center;
  justify-content: center;
  border-radius: 4px;
}

.close-button:hover {
  background-color: #f3f4f6;
  color: #374151;
}

.dark-mode .close-button {
  color: #9ca3af;
}

.dark-mode .close-button:hover {
  background-color: #2d2d2d;
  color: #d4d4d4;
}

.modal-content {
  flex: 1;
  display: flex;
  justify-content: center;
  align-items: center;
  background-color: #f9fafb;
  position: relative;
}

.dark-mode .modal-content {
  background-color: #121212;
}

.preview-iframe {
  width: 100%;
  height: 100%;
  border: none;
}

.loading-state,
.error-state {
  display: flex;
  flex-direction: column;
  justify-content: center;
  align-items: center;
  width: 100%;
  height: 100%;
}

.dark-mode .loading-state,
.dark-mode .error-state {
  color: #d4d4d4;
}

.error-state {
  color: #ef4444;
  padding: 24px;
  text-align: center;
}

.dark-mode .error-state {
  color: #f87171;
}

/* 加载动画 */
.spinner {
  animation: rotate 2s linear infinite;
  width: 40px;
  height: 40px;
  margin-bottom: 16px;
}

.path {
  stroke: #3b82f6;
  stroke-linecap: round;
  animation: dash 1.5s ease-in-out infinite;
}

.dark-mode .path {
  stroke: #60a5fa;
}

@keyframes rotate {
  100% {
    transform: rotate(360deg);
  }
}

@keyframes dash {
  0% {
    stroke-dasharray: 1, 150;
    stroke-dashoffset: 0;
  }
  50% {
    stroke-dasharray: 90, 150;
    stroke-dashoffset: -35;
  }
  100% {
    stroke-dasharray: 90, 150;
    stroke-dashoffset: -124;
  }
}

@media (max-width: 640px) {
  .modal-container {
    width: 95%;
    height: 90%;
  }

  .modal-header {
    flex-direction: column;
    align-items: flex-start;
    padding: 12px;
  }

  .modal-actions {
    margin-top: 8px;
    width: 100%;
    justify-content: space-between;
    flex-wrap: wrap;
    gap: 6px;
  }

  .action-button {
    flex: 1;
    min-width: 80px;
    padding: 10px;
    font-size: 13px;
    min-height: 40px;
  }

  .close-button {
    padding: 8px;
  }
}
</style>
