<template>
  <div 
    ref="contentRef" 
    :class="['footer-markdown-content', darkMode ? 'footer-dark' : 'footer-light']"
  />
</template>

<script setup>
import { ref, watch, onMounted, onBeforeUnmount, nextTick } from "vue";

// Props
const props = defineProps({
  content: {
    type: String,
    required: true,
  },
  darkMode: {
    type: Boolean,
    default: false,
  },
});

// Refs
const contentRef = ref(null);

// 组件销毁状态
const isDestroyed = ref(false);

// 加载 Vditor CSS
const loadVditorCSS = async () => {
  const existingLink = document.querySelector('link[href="/assets/vditor/dist/index.css"]');
  if (!existingLink) {
    const link = document.createElement("link");
    link.rel = "stylesheet";
    link.href = "/assets/vditor/dist/index.css";
    document.head.appendChild(link);
  }
};

// 使用 Vditor 渲染 Markdown 内容
const renderContent = async () => {
  if (!contentRef.value || !props.content || isDestroyed.value) return;

  try {
    // 加载 CSS
    await loadVditorCSS();

    // 确保DOM更新后再渲染
    await nextTick();

    // 再次检查组件状态
    if (isDestroyed.value || !contentRef.value) {
      return;
    }

    // 清空之前的内容
    contentRef.value.innerHTML = "";

    // 动态导入 Vditor
    let Vditor;
    if (window.Vditor) {
      Vditor = window.Vditor;
    } else {
      // 如果Vditor还没加载，动态加载
      const script = document.createElement("script");
      script.src = "/assets/vditor/dist/index.min.js";
      
      await new Promise((resolve, reject) => {
        script.onload = () => {
          Vditor = window.Vditor;
          resolve();
        };
        script.onerror = reject;
        document.head.appendChild(script);
      });
    }

    // 再次检查组件状态
    if (isDestroyed.value || !contentRef.value) {
      return;
    }

    // 使用 Vditor 的预览功能渲染 Markdown
    Vditor.preview(contentRef.value, props.content, {
      cdn: "/assets/vditor",
      theme: {
        current: props.darkMode ? "dark" : "light",
        path: "/assets/vditor/dist/css/content-theme",
      },
      hljs: {
        lineNumber: false, // 页脚不需要行号
        style: props.darkMode ? "vs2015" : "github",
      },
      markdown: {
        toc: false, // 页脚不需要目录
        mark: true, // 支持标记
        footnotes: false, // 页脚不需要脚注
        autoSpace: true, // 自动空格
        listStyle: true, // 支持列表样式
        task: true, // 支持任务列表
        paragraphBeginningSpace: true,
        fixTermTypo: true,
        media: false, // 页脚不需要媒体支持
      },
      math: {
        engine: "KaTeX",
        inlineDigit: true,
      },
      after: () => {
        // 渲染完成后的回调
        if (isDestroyed.value || !contentRef.value) {
          return;
        }
        
        // 确保主题类正确应用
        if (props.darkMode) {
          contentRef.value.classList.add("vditor-reset--dark");
          contentRef.value.classList.remove("vditor-reset--light");
        } else {
          contentRef.value.classList.add("vditor-reset--light");
          contentRef.value.classList.remove("vditor-reset--dark");
        }
      },
    });
  } catch (error) {
    console.error("页脚Markdown渲染失败:", error);
    // 渲染失败时显示原始文本
    if (contentRef.value && !isDestroyed.value) {
      contentRef.value.textContent = props.content;
    }
  }
};

// 监听内容变化
watch(() => props.content, renderContent);

// 监听暗色模式变化
watch(() => props.darkMode, renderContent);

// 组件挂载时渲染
onMounted(() => {
  if (props.content) {
    renderContent();
  }
});

// 组件销毁时清理
onBeforeUnmount(() => {
  isDestroyed.value = true;
  
  // 清理 DOM
  if (contentRef.value) {
    contentRef.value.innerHTML = "";
  }
});
</script>

<style scoped>
.footer-markdown-content {
  /* 页脚特定的样式 */
  font-size: 0.875rem; /* text-sm */
  line-height: 1.25rem;
}

.footer-light {
  color: rgb(107 114 128); /* text-gray-500 */
}

.footer-dark {
  color: rgb(156 163 175); /* text-gray-400 */
}

/* 确保Vditor渲染的内容适配页脚样式 */
.footer-markdown-content :deep(.vditor-reset) {
  font-size: inherit;
  line-height: inherit;
  color: inherit;
}

/* 链接样式适配 */
.footer-markdown-content :deep(.vditor-reset a) {
  color: rgb(59 130 246); /* blue-500 */
  text-decoration: underline;
}

.footer-dark .footer-markdown-content :deep(.vditor-reset a) {
  color: rgb(96 165 250); /* blue-400 */
}

.footer-markdown-content :deep(.vditor-reset a:hover) {
  color: rgb(37 99 235); /* blue-600 */
}

.footer-dark .footer-markdown-content :deep(.vditor-reset a:hover) {
  color: rgb(147 197 253); /* blue-300 */
}

/* 移除不必要的边距 */
.footer-markdown-content :deep(.vditor-reset p) {
  margin: 0;
}

.footer-markdown-content :deep(.vditor-reset) {
  padding: 0;
  margin: 0;
}
</style>
