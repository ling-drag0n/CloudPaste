<template>
  <div class="code-display" :class="{ 'code-display-dark': darkMode }">
    <!-- 加载状态 -->
    <div v-if="loading" class="loading-state">
      <div class="loading-spinner"></div>
      <p class="loading-text">{{ $t("textPreview.loadingHighlight") }}</p>
    </div>
    <!-- 错误状态 -->
    <div v-else-if="error" class="error-state">
      <p class="error-text">{{ error }}</p>
      <pre class="fallback-code">{{ content }}</pre>
    </div>
    <!-- 代码内容 -->
    <div v-else ref="codeContainer" class="code-container" v-html="highlightedCode"></div>
  </div>
</template>

<script setup>
import { ref, computed, watch, watchEffect, nextTick } from "vue";
import { useI18n } from "vue-i18n";
import { useCodeHighlight } from "@/composables/text-preview/useCodeHighlight.js";

const { t } = useI18n();

// Props
const props = defineProps({
  content: {
    type: String,
    required: true,
  },
  language: {
    type: String,
    default: "",
  },
  darkMode: {
    type: Boolean,
    default: false,
  },
  showLineNumbers: {
    type: Boolean,
    default: true,
  },
  filename: {
    type: String,
    default: "",
  },
});

// 响应式数据
const codeContainer = ref(null);
const loading = ref(false);
const error = ref(null);
const highlightedCode = ref("");

// 使用代码高亮 Composable
const { highlightCode, detectLanguageFromFilename, loadTheme } = useCodeHighlight();

// 渲染代码高亮
const renderCode = async () => {
  if (!props.content) {
    highlightedCode.value = "";
    return;
  }

  try {
    loading.value = true;
    error.value = null;

    // 检测语言
    let language = props.language;
    if (!language && props.filename) {
      language = detectLanguageFromFilename(props.filename);
    }

    // 加载主题
    await loadTheme(null, props.darkMode);

    // 高亮代码
    const result = await highlightCode(props.content, language);

    if (result.success) {
      // 构建HTML字符串
      let html = `<pre class="hljs-pre"><code class="hljs language-${result.language}">${result.html}</code></pre>`;

      // 添加行号
      if (props.showLineNumbers && props.content.split("\n").length > 1) {
        html = addLineNumbersToHTML(html, props.content);
      }

      highlightedCode.value = html;
    } else {
      throw new Error(result.error || "Code highlighting failed");
    }
  } catch (err) {
    console.error("代码高亮渲染失败:", err);
    error.value = err.message;

    // 降级显示原始代码
    highlightedCode.value = `<pre class="hljs-pre"><code class="hljs">${escapeHtml(props.content)}</code></pre>`;
  } finally {
    loading.value = false;
  }
};

/**
 * 为代码块添加行号
 * @param {HTMLElement} preElement - pre 元素
 */
const addLineNumbers = (preElement) => {
  const codeElement = preElement.querySelector("code");
  if (!codeElement) return;

  const lines = codeElement.innerHTML.split("\n");
  const lineCount = lines.length;

  // 创建行号容器
  const lineNumbersContainer = document.createElement("div");
  lineNumbersContainer.className = "hljs-line-numbers";

  // 生成行号
  for (let i = 1; i <= lineCount; i++) {
    const lineNumber = document.createElement("span");
    lineNumber.className = "hljs-line-number";
    lineNumber.textContent = i.toString();
    lineNumbersContainer.appendChild(lineNumber);
  }

  // 包装代码内容
  const codeWrapper = document.createElement("div");
  codeWrapper.className = "hljs-code-wrapper";
  codeWrapper.appendChild(codeElement.cloneNode(true));

  // 重新组织结构
  preElement.innerHTML = "";
  preElement.className += " hljs-with-line-numbers";
  preElement.appendChild(lineNumbersContainer);
  preElement.appendChild(codeWrapper);
};

/**
 * 为HTML添加行号
 * @param {string} html - 原始HTML
 * @param {string} content - 原始内容
 * @returns {string} 添加行号后的HTML
 */
const addLineNumbersToHTML = (html, content) => {
  //早期返回，避免不必要的计算
  if (!props.showLineNumbers) {
    return html;
  }

  const lineCount = content.split("\n").length;

  // 数组join
  const lineNumbers = [];
  lineNumbers.push('<div class="hljs-line-numbers">');

  // 批量生成行号
  for (let i = 1; i <= lineCount; i++) {
    lineNumbers.push(`<div class="hljs-line-number">${i}</div>`);
  }

  lineNumbers.push("</div>");
  const lineNumbersHTML = lineNumbers.join("");

  const wrappedHTML = html.replace(
    /<pre class="hljs-pre"><code class="([^"]*)">([\s\S]*?)<\/code><\/pre>/,
    `<pre class="hljs-pre hljs-with-line-numbers">${lineNumbersHTML}<div class="hljs-code-wrapper"><code class="$1">$2</code></div></pre>`
  );

  return wrappedHTML;
};

/**
 * 转义HTML字符
 * @param {string} text - 原始文本
 * @returns {string} 转义后的文本
 */
const escapeHtml = (text) => {
  const div = document.createElement("div");
  div.textContent = text;
  return div.innerHTML;
};

//  watchEffect 避免重复渲染
// 添加防抖
let renderTimer = null;

const debouncedRender = () => {
  if (renderTimer) {
    clearTimeout(renderTimer);
  }
  renderTimer = setTimeout(() => {
    renderCode();
    renderTimer = null;
  }, 100); // 100ms 防抖
};

// 使用 watchEffect 统一监听所有相关属性
watchEffect(() => {
  // 监听所有影响渲染的属性
  const { content, language, darkMode, showLineNumbers, filename } = props;

  // 如果没有内容，直接清空
  if (!content) {
    highlightedCode.value = "";
    return;
  }

  // 使用防抖渲染
  debouncedRender();
});
</script>

<style scoped>
.code-display {
  @apply w-full h-full overflow-auto;
}

.loading-state,
.error-state {
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  padding: 2rem;
  min-height: 200px;
}

.loading-spinner {
  @apply w-6 h-6 border-2 border-blue-500 border-t-transparent rounded-full animate-spin mb-2;
}

.loading-text,
.error-text {
  @apply text-sm text-gray-600;
}

.code-display-dark .loading-text,
.code-display-dark .error-text {
  @apply text-gray-400;
}

.fallback-code {
  @apply mt-4 p-4 text-sm font-mono bg-gray-100 rounded;
  @apply text-gray-800 whitespace-pre-wrap break-words;
}

.code-display-dark .fallback-code {
  @apply bg-gray-700 text-gray-200;
}

.code-container {
  width: 100%;
  height: 100%;
  position: relative;
}

/* 代码高亮样式 */
:deep(.hljs-pre) {
  @apply m-0 p-2 text-sm font-mono overflow-auto;
  @apply bg-transparent border-0;
  line-height: 1.5;
  border-radius: 0;
}

:deep(.hljs-with-line-numbers) {
  @apply flex;
}

:deep(.hljs-line-numbers) {
  @apply flex flex-col text-right pr-2;
  @apply text-gray-400;
  min-width: 2rem;
  padding: 0.5rem 0.25rem;
  margin-left: -0.7rem;
  user-select: none;
  background: transparent;
}

:deep(.hljs-line-number) {
  @apply block;
  line-height: 1.5;
  font-size: 0.875rem;
  font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
}

:deep(.hljs-code-wrapper) {
  @apply flex-1 overflow-auto;
}

:deep(.hljs-code-wrapper code) {
  @apply block p-2;
  background: transparent !important;
}

/* 暗色模式下的行号样式 */
.code-display-dark :deep(.hljs-line-numbers) {
  @apply text-gray-500;
  background: transparent;
}

.code-display-dark :deep(.hljs-line-number) {
  color: #6b7280;
}

/* 滚动条样式 */
.code-display::-webkit-scrollbar {
  width: 8px;
  height: 8px;
}

.code-display::-webkit-scrollbar-track {
  background-color: #f3f4f6;
}

.code-display-dark::-webkit-scrollbar-track {
  background-color: #374151;
}

.code-display::-webkit-scrollbar-thumb {
  background-color: #d1d5db;
  border-radius: 4px;
}

.code-display-dark::-webkit-scrollbar-thumb {
  background-color: #6b7280;
}

.code-display::-webkit-scrollbar-thumb:hover {
  background-color: #9ca3af;
}

.code-display-dark::-webkit-scrollbar-thumb:hover {
  background-color: #9ca3af;
}
</style>
