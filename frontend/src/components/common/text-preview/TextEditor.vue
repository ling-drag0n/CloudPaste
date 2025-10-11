<template>
  <div class="text-editor" :class="{ 'text-editor-dark': darkMode }">
    <!-- Monaco编辑器容器 - 始终渲染 -->
    <div ref="editorContainer" class="editor-container">
      <!-- 加载状态覆盖层 -->
      <div v-if="loading" class="loading-overlay">
        <div class="loading-spinner"></div>
        <p class="loading-text">{{ $t("textPreview.loadingEditor") }}</p>
      </div>

      <!-- 错误状态覆盖层 -->
      <div v-else-if="error" class="error-overlay">
        <p class="error-text">{{ error }}</p>
        <div class="fallback-editor">
          <textarea
            v-model="localContent"
            class="fallback-textarea"
            :class="{ 'fallback-dark': darkMode }"
            :readonly="readOnly"
            @input="handleInput"
            :placeholder="$t('textPreview.fallbackEditor')"
          />
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, onMounted, onBeforeUnmount, watch, nextTick } from "vue";
import { useI18n } from "vue-i18n";
import loader from "@monaco-editor/loader";

const { t } = useI18n();

// CDN 配置列表（按优先级排序）
const CDN_LIST = [
  "https://cdnjs.znnu.com/monaco-editor/0.52.2/min/vs",  // cdnjs CDN
  "https://s4.zstatic.net/npm/monaco-editor@0.52.2/min/vs",  // ZStatic
  "https://fastly.jsdelivr.net/npm/monaco-editor@0.52.2/min/vs", // JSDelivr
  "https://unpkg.com/monaco-editor@0.52.2/min/vs", // unpkg
];

// Props
const props = defineProps({
  content: {
    type: String,
    default: "",
  },
  language: {
    type: String,
    default: "plaintext",
  },
  darkMode: {
    type: Boolean,
    default: false,
  },
  readOnly: {
    type: Boolean,
    default: false,
  },
  height: {
    type: [Number, String],
    default: 400,
  },
  fontSize: {
    type: Number,
    default: 14,
  },
  wordWrap: {
    type: String,
    default: "on", // "on" | "off" | "wordWrapColumn" | "bounded"
  },
  minimap: {
    type: Boolean,
    default: false,
  },
});

// Emits
const emit = defineEmits(["change", "focus", "blur"]);

// 响应式状态
const loading = ref(true);
const error = ref(null);
const editorContainer = ref(null);
const localContent = ref(props.content);

// Monaco编辑器实例
let monacoEditor = null;
let monaco = null;

// 监听器清理
let contentChangeDisposable = null;
let focusDisposable = null;
let blurDisposable = null;

const addEditorActions = (editor, monaco) => {
  // 1. 切换自动换行
  editor.addAction({
    id: "toggle-word-wrap",
    label: "切换自动换行",
    keybindings: [monaco.KeyMod.Alt | monaco.KeyCode.KeyZ],
    contextMenuGroupId: "view",
    contextMenuOrder: 1,
    run: function (ed) {
      const currentWordWrap = ed.getOption(monaco.editor.EditorOption.wordWrap);
      ed.updateOptions({
        wordWrap: currentWordWrap === "on" ? "off" : "on",
      });
    },
  });

  // 2. 插入时间戳
  editor.addAction({
    id: "insert-timestamp",
    label: "插入时间戳",
    contextMenuGroupId: "modification",
    contextMenuOrder: 1,
    run: function (ed) {
      const timestamp = new Date().toLocaleString("zh-CN");
      const selection = ed.getSelection();
      ed.executeEdits("", [
        {
          range: selection,
          text: timestamp,
        },
      ]);
    },
  });

  // 3. 文本统计信息
  editor.addAction({
    id: "show-text-stats",
    label: "显示文本统计",
    contextMenuGroupId: "view",
    contextMenuOrder: 2,
    run: function (ed) {
      const content = ed.getValue();
      const lines = content.split("\n").length;
      const chars = content.length;
      const words = content.trim() ? content.trim().split(/\s+/).length : 0;

      // 使用更友好的提示方式
      const message = `行数: ${lines}\n字符数: ${chars}\n单词数: ${words}`;
      if (window.confirm) {
        alert(message);
      } else {
        console.log("文本统计:", { lines, chars, words });
      }
    },
  });

  // 4. 转换为大写
  editor.addAction({
    id: "convert-to-uppercase",
    label: "转换为大写",
    contextMenuGroupId: "modification",
    contextMenuOrder: 2,
    run: function (ed) {
      const selection = ed.getSelection();
      const selectedText = ed.getModel().getValueInRange(selection);
      if (selectedText) {
        ed.executeEdits("", [
          {
            range: selection,
            text: selectedText.toUpperCase(),
          },
        ]);
      }
    },
  });

  // 5. 转换为小写
  editor.addAction({
    id: "convert-to-lowercase",
    label: "转换为小写",
    contextMenuGroupId: "modification",
    contextMenuOrder: 3,
    run: function (ed) {
      const selection = ed.getSelection();
      const selectedText = ed.getModel().getValueInRange(selection);
      if (selectedText) {
        ed.executeEdits("", [
          {
            range: selection,
            text: selectedText.toLowerCase(),
          },
        ]);
      }
    },
  });

  // 6. 切换代码缩略图（Minimap）
  editor.addAction({
    id: "toggle-minimap",
    label: "切换代码缩略图",
    keybindings: [monaco.KeyMod.CtrlCmd | monaco.KeyMod.Shift | monaco.KeyCode.KeyM],
    contextMenuGroupId: "view",
    contextMenuOrder: 3,
    run: function (ed) {
      const currentMinimap = ed.getOption(monaco.editor.EditorOption.minimap);
      ed.updateOptions({
        minimap: {
          ...currentMinimap,
          enabled: !currentMinimap.enabled,
        },
      });
    },
  });
};

/**
 * 初始化Monaco编辑器
 */
const initMonacoEditor = async () => {
  try {
    loading.value = true;
    error.value = null;

    // 等待DOM更新
    await nextTick();

    // 添加重试机制，确保DOM容器准备就绪
    let retryCount = 0;
    const maxRetries = 5;

    while (!editorContainer.value && retryCount < maxRetries) {
      console.log(`等待编辑器容器准备就绪... (${retryCount + 1}/${maxRetries})`);
      await new Promise((resolve) => setTimeout(resolve, 50));
      retryCount++;
    }

    if (!editorContainer.value) {
      throw new Error(`编辑器容器不存在 (重试${retryCount}次后仍然失败)`);
    }

    console.log("编辑器容器准备就绪，开始初始化Monaco编辑器");

    // 加载Monaco编辑器（带CDN切换）
    console.log("开始加载Monaco编辑器...");

    let lastError = null;
    for (let i = 0; i < CDN_LIST.length; i++) {
      try {
        const cdnUrl = CDN_LIST[i];
        console.log(`尝试 CDN ${i + 1}/${CDN_LIST.length}: ${cdnUrl}`);

        // 配置当前 CDN
        loader.config({
          paths: {
            vs: cdnUrl,
          },
        });

        // 尝试加载
        monaco = await loader.init();

        if (monaco && monaco.editor) {
          console.log(`Monaco编辑器加载成功，使用 CDN: ${cdnUrl}`);
          break;
        } else {
          throw new Error("Monaco编辑器对象无效");
        }
      } catch (error) {
        lastError = error;
        console.warn(`CDN ${i + 1} 加载失败:`, error.message);

        if (i === CDN_LIST.length - 1) {
          throw new Error(`所有 CDN 都无法加载 Monaco Editor。最后错误: ${lastError?.message}`);
        }

        // 继续尝试下一个 CDN
        continue;
      }
    }

    // 创建编辑器实例
    monacoEditor = monaco.editor.create(editorContainer.value, {
      value: props.content,
      language: props.language,
      theme: props.darkMode ? "vs-dark" : "vs",
      readOnly: props.readOnly,
      fontSize: props.fontSize,
      wordWrap: props.wordWrap,
      // 启用 minimap（代码缩略图）
      minimap: {
        enabled: true,
        side: "right",
        showSlider: "mouseover",
        renderCharacters: true,
        maxColumn: 120,
        scale: 1,
      },
      scrollBeyondLastLine: false,
      automaticLayout: true,
      lineNumbers: "on",
      renderWhitespace: "selection",
      tabSize: 2,
      insertSpaces: true,
      folding: true,
      lineDecorationsWidth: 10,
      lineNumbersMinChars: 3,
      glyphMargin: false,
      contextmenu: true,
      mouseWheelZoom: true,
      smoothScrolling: true,
      cursorBlinking: "blink",
      cursorSmoothCaretAnimation: "on",
      renderLineHighlight: "line",
      selectionHighlight: true,
      occurrencesHighlight: "singleFile",
      codeLens: false,
      suggest: {
        showKeywords: true,
        showSnippets: true,
      },
    });

    // 监听内容变化
    let changeTimer = null;
    let lastEmittedValue = ""; // 避免重复emit相同内容

    contentChangeDisposable = monacoEditor.onDidChangeModelContent(() => {
      if (changeTimer) clearTimeout(changeTimer);

      changeTimer = setTimeout(() => {
        const value = monacoEditor.getValue();

        // 避免重复emit相同内容
        if (value !== lastEmittedValue) {
          lastEmittedValue = value;
          localContent.value = value;
          emit("change", value);

          // 只在开发环境输出日志
          if (process.env.NODE_ENV === "development") {
            console.log("Monaco编辑器内容变化:", value.length, "字符");
          }
        }

        changeTimer = null;
      }, 300); // 300ms防抖，平衡响应性和性能
    });

    // 监听焦点事件
    focusDisposable = monacoEditor.onDidFocusEditorText(() => {
      emit("focus");
    });

    blurDisposable = monacoEditor.onDidBlurEditorText(() => {
      emit("blur");
    });

    // 添加有用的编辑器功能
    addEditorActions(monacoEditor, monaco);

    loading.value = false;
    console.log("Monaco编辑器初始化成功", {
      language: props.language,
      theme: props.darkMode ? "vs-dark" : "vs",
      readOnly: props.readOnly,
    });
  } catch (err) {
    console.error("Monaco编辑器初始化失败:", err);
    console.error("错误详情:", {
      containerExists: !!editorContainer.value,
      containerElement: editorContainer.value,
      props: {
        content: props.content?.length || 0,
        language: props.language,
        darkMode: props.darkMode,
        readOnly: props.readOnly,
      },
    });
    error.value = err.message || t("textPreview.editorInitFailed");
    loading.value = false;
  }
};

/**
 * 处理fallback textarea输入
 */
const handleInput = (event) => {
  const value = event.target.value;
  localContent.value = value;
  emit("change", value);
};

/**
 * 设置编辑器内容
 */
const setValue = (value) => {
  if (monacoEditor) {
    monacoEditor.setValue(value || "");
  } else {
    localContent.value = value || "";
  }
};

/**
 * 获取编辑器内容
 */
const getValue = () => {
  if (monacoEditor) {
    return monacoEditor.getValue();
  }
  return localContent.value;
};

/**
 * 设置编辑器语言
 */
const setLanguage = (language) => {
  if (monacoEditor && monaco) {
    const model = monacoEditor.getModel();
    if (model) {
      monaco.editor.setModelLanguage(model, language);
    }
  }
};

/**
 * 聚焦编辑器
 */
const focus = () => {
  if (monacoEditor) {
    monacoEditor.focus();
  }
};

/**
 * 调整编辑器大小
 */
const resize = () => {
  if (monacoEditor) {
    monacoEditor.layout();
  }
};

// 监听props变化
watch(
  () => props.content,
  (newContent) => {
    if (newContent !== getValue()) {
      setValue(newContent);
    }
  }
);

watch(
  () => props.language,
  (newLanguage) => {
    setLanguage(newLanguage);
  }
);

watch(
  () => props.darkMode,
  (newDarkMode) => {
    if (monacoEditor) {
      monacoEditor.updateOptions({
        theme: newDarkMode ? "vs-dark" : "vs",
      });
    }
  }
);

watch(
  () => props.readOnly,
  (newReadOnly) => {
    if (monacoEditor) {
      monacoEditor.updateOptions({
        readOnly: newReadOnly,
      });
    }
  }
);

// 组件挂载
onMounted(async () => {
  await nextTick();

  // 使用requestIdleCallback优化初始化时机，确保DOM完全准备好
  const initializeEditor = async () => {
    try {
      await initMonacoEditor();
    } catch (error) {
      console.error("初始化编辑器时出错:", error);
    }
  };

  if (window.requestIdleCallback) {
    window.requestIdleCallback(initializeEditor, { timeout: 1000 });
  } else {
    // 降级方案
    setTimeout(initializeEditor, 100);
  }
});

// 组件销毁
onBeforeUnmount(() => {
  // 清理监听器
  if (contentChangeDisposable) {
    contentChangeDisposable.dispose();
    contentChangeDisposable = null;
  }
  if (focusDisposable) {
    focusDisposable.dispose();
    focusDisposable = null;
  }
  if (blurDisposable) {
    blurDisposable.dispose();
    blurDisposable = null;
  }

  // 清理编辑器实例
  if (monacoEditor) {
    monacoEditor.dispose();
    monacoEditor = null;
  }
});

// 暴露方法
defineExpose({
  setValue,
  getValue,
  setLanguage,
  focus,
  resize,
  getEditor: () => monacoEditor,
});
</script>

<style scoped>
.text-editor {
  width: 100%;
  height: 100%;
  display: flex;
  flex-direction: column;
  border: 1px solid #e5e7eb;
  border-radius: 0.375rem;
  overflow: hidden;
}

.text-editor-dark {
  border-color: #374151;
}

.editor-container {
  flex: 1;
  min-height: 400px;
  position: relative;
}

/* 加载状态覆盖层 */
.loading-overlay {
  position: absolute;
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  background-color: rgba(255, 255, 255, 0.9);
  z-index: 10;
}

.text-editor-dark .loading-overlay {
  background-color: rgba(31, 41, 55, 0.9);
}

.loading-spinner {
  width: 2rem;
  height: 2rem;
  border: 2px solid #e5e7eb;
  border-top: 2px solid #3b82f6;
  border-radius: 50%;
  animation: spin 1s linear infinite;
}

.loading-text {
  margin-top: 1rem;
  color: #6b7280;
  font-size: 0.875rem;
}

/* 错误状态覆盖层 */
.error-overlay {
  position: absolute;
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  padding: 1rem;
  background-color: rgba(255, 255, 255, 0.95);
  z-index: 10;
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: flex-start;
  overflow: auto;
}

.text-editor-dark .error-overlay {
  background-color: rgba(31, 41, 55, 0.95);
}

.error-text {
  color: #ef4444;
  font-size: 0.875rem;
  margin-bottom: 1rem;
}

.fallback-editor {
  width: 100%;
  flex: 1;
  display: flex;
  flex-direction: column;
  margin-top: 1rem;
}

.fallback-textarea {
  width: 100%;
  flex: 1;
  min-height: 300px;
  padding: 1rem;
  font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
  font-size: 0.875rem;
  line-height: 1.5;
  border: 1px solid #d1d5db;
  border-radius: 0.375rem;
  background-color: #ffffff;
  color: #1f2937;
  resize: none;
  outline: none;
}

.fallback-textarea:focus {
  border-color: #3b82f6;
  box-shadow: 0 0 0 3px rgba(59, 130, 246, 0.1);
}

.fallback-dark {
  background-color: #1f2937;
  color: #e5e7eb;
  border-color: #374151;
}

.fallback-dark:focus {
  border-color: #60a5fa;
  box-shadow: 0 0 0 3px rgba(96, 165, 250, 0.1);
}

/* 动画 */
@keyframes spin {
  0% {
    transform: rotate(0deg);
  }
  100% {
    transform: rotate(360deg);
  }
}
</style>
