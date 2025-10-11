<script setup>
// PasteViewPreview组件 - 用于渲染Markdown内容并提供预览功能
// 该组件使用Vditor库渲染复杂的Markdown内容，支持代码高亮、数学公式等高级特性
import { ref, onMounted, watch, nextTick, onBeforeUnmount } from "vue";
import { debugLog } from "./PasteViewUtils";
import HtmlPreviewModal from "./preview/HtmlPreviewModal.vue"; // 引入HTML预览弹窗组件

// 懒加载Vditor和CSS
let VditorClass = null;
let vditorCSSLoaded = false;

const loadVditor = async () => {
  if (!VditorClass) {
    await loadVditorCSS();

    // 从本地vditor目录加载Vditor
    const script = document.createElement("script");
    script.src = "/assets/vditor/dist/index.min.js";

    return new Promise((resolve, reject) => {
      script.onload = () => {
        VditorClass = window.Vditor;
        resolve(VditorClass);
      };
      script.onerror = reject;
      document.head.appendChild(script);
    });
  }
  return VditorClass;
};

const loadVditorCSS = async () => {
  if (!vditorCSSLoaded) {
    const link = document.createElement("link");
    link.rel = "stylesheet";
    link.href = "/assets/vditor/dist/index.css";
    document.head.appendChild(link);
    vditorCSSLoaded = true;
  }
};

// 定义组件接收的属性
const props = defineProps({
  // 是否为暗色模式，控制渲染主题
  darkMode: {
    type: Boolean,
    required: true,
  },
  // 要渲染的Markdown内容
  content: {
    type: String,
    default: "",
  },
  // 是否为开发环境
  isDev: {
    type: Boolean,
    default: false,
  },
  // 是否启用调试日志
  enableDebug: {
    type: Boolean,
    default: false,
  },
  // 是否为纯文本模式（不渲染Markdown）
  isPlainTextMode: {
    type: Boolean,
    default: false,
  },
});

// HTML 预览相关的状态变量
const showHtmlPreview = ref(false);
const previewHtmlContent = ref("");
// SVG 预览相关的状态变量
const showSvgPreview = ref(false);
const previewSvgContent = ref("");

// 定义组件可触发的事件
const emit = defineEmits(["rendered"]);
// 用于保存预览元素的引用
const previewElement = ref(null);
// 跟踪内容是否已经渲染
const contentRendered = ref(false);
// 存储滚动位置
const savedScrollPosition = ref({ window: 0, content: 0 });
// MutationObserver实例，用于清理
let mutationObserver = null;
// 存储定时器ID，用于清理
const timeoutIds = new Set();

// 安全的setTimeout，会自动清理
const safeSetTimeout = (callback, delay) => {
  const id = setTimeout(() => {
    timeoutIds.delete(id);
    callback();
  }, delay);
  timeoutIds.add(id);
  return id;
};

// 清理所有定时器
const clearAllTimeouts = () => {
  timeoutIds.forEach((id) => clearTimeout(id));
  timeoutIds.clear();
};

// 保存当前滚动位置
const saveScrollPosition = () => {
  savedScrollPosition.value = {
    window: window.scrollY,
    content: previewElement.value ? previewElement.value.scrollTop : 0,
  };
  debugLog(props.enableDebug, props.isDev, "保存滚动位置:", savedScrollPosition.value);
};

// 恢复保存的滚动位置
const restoreScrollPosition = () => {
  nextTick(() => {
    // 恢复窗口滚动位置
    window.scrollTo(0, savedScrollPosition.value.window);

    // 恢复内容滚动位置（如果预览元素有滚动）
    if (previewElement.value) {
      previewElement.value.scrollTop = savedScrollPosition.value.content;
    }

    debugLog(props.enableDebug, props.isDev, "恢复滚动位置:", savedScrollPosition.value);
  });
};

// 监听暗色模式变化，当主题改变时重新渲染内容
watch(
  () => props.darkMode,
  () => {
    if (props.content) {
      // 在重新渲染前保存滚动位置
      saveScrollPosition();

      // 暗色模式变化时重新渲染
      nextTick(() => {
        renderContent(props.content);
      });
    }
  }
);

// 缓存上次渲染的内容，避免重复渲染
let lastRenderedContent = "";
// DOM查询缓存
let diagramContainersCache = null;
let checkboxesCache = null;
let imagesCache = null;
let codeBlocksCache = null;

// 监听内容变化，当内容改变时重新渲染
watch(
  () => props.content,
  (newContent, oldContent) => {
    // 避免重复渲染相同内容
    if (newContent && newContent !== lastRenderedContent && newContent !== oldContent) {
      lastRenderedContent = newContent;
      contentRendered.value = false;
      nextTick(() => {
        renderContent(newContent);
      });
    }
  }
);

// 监听纯文本模式变化
watch(
  () => props.isPlainTextMode,
  (newMode) => {
    // 如果从纯文本模式切换到Markdown模式，需要重新渲染
    if (!newMode && props.content) {
      contentRendered.value = false;
      nextTick(() => {
        renderContent(props.content);
      });
    }
    debugLog(props.enableDebug, props.isDev, `显示模式切换: ${newMode ? "纯文本模式" : "Markdown渲染模式"}`);
  }
);

// 渲染内容的方法，处理DOM可用性和兼容性问题
const renderContent = (content) => {
  // 如果是纯文本模式，不需要渲染
  if (props.isPlainTextMode) {
    contentRendered.value = true; // 标记为已渲染，避免显示加载动画
    emit("rendered"); // 触发渲染完成事件
    return;
  }

  if (!content) {
    console.warn("没有内容可渲染");
    return;
  }

  // 延迟检查预览元素，确保DOM已挂载
  if (!previewElement.value) {
    debugLog(props.enableDebug, props.isDev, "预览元素尚未准备好，将在下一个渲染周期尝试");
    // 使用nextTick等待DOM更新后再尝试渲染
    nextTick(() => {
      if (previewElement.value) {
        debugLog(props.enableDebug, props.isDev, "预览元素已就绪，现在开始渲染");
        renderContentInternal(content);
      } else {
        console.error("预览元素始终不可用，无法渲染内容");
      }
    });
    return;
  }

  renderContentInternal(content);
};

// 内部渲染实现，使用Vditor的API渲染Markdown
const renderContentInternal = async (content) => {
  // 清空之前的内容，避免重复渲染
  if (previewElement.value) {
    // 完全清除之前的内容和样式
    previewElement.value.innerHTML = "";
    // 移除可能残留的主题相关类
    previewElement.value.classList.remove("vditor-reset--dark", "vditor-reset--light");

    // 清理DOM缓存，强制重新查询
    diagramContainersCache = null;
    checkboxesCache = null;
    imagesCache = null;
    codeBlocksCache = null;

    try {
      // 懒加载Vditor
      const VditorConstructor = await loadVditor();

      // 安全的预览渲染
      safeSetTimeout(() => {
        try {
          // 使用 Vditor 的预览 API 渲染内容
          // 配置了主题、代码高亮、数学公式等渲染选项
          VditorConstructor.preview(previewElement.value, content, {
            mode: "dark-light", // 支持明暗主题
            theme: {
              current: props.darkMode ? "dark" : "light", // 根据darkMode设置主题
              path: "/assets/vditor/dist/css/content-theme",
            },
            cdn: "/assets/vditor",
            hljs: {
              lineNumber: true, // 代码块显示行号
              style: props.darkMode ? "vs2015" : "github", // 代码高亮样式
            },
            markdown: {
              toc: true, // 启用目录
              mark: true, // 启用标记
              footnotes: true, // 启用脚注
              autoSpace: true, // 自动空格
              media: true, // 启用媒体链接解析（视频、音频等）
              listStyle: true, // 启用列表样式支持
              // 添加任务列表支持
              task: true, // 启用任务列表
              // 图表渲染相关配置
              mermaid: {
                theme: "default", // 使用固定的主题，不跟随暗色模式变化
                useMaxWidth: false, // 不使用最大宽度限制
              },
              flowchart: {
                theme: "default", // 使用固定的主题
              },
              // 固定图表样式
              fixDiagramTheme: true, // 自定义属性，用于CSS选择器中识别
            },
            math: {
              engine: "KaTeX", // 数学公式渲染引擎
              inlineDigit: true, // 启用行内数学公式
            },
            after: () => {
              // 渲染完成后的回调
              debugLog(props.enableDebug, props.isDev, "Markdown 内容渲染完成");

              // 强制添加对应主题的类
              if (props.darkMode) {
                previewElement.value.classList.add("vditor-reset--dark");
                previewElement.value.classList.remove("vditor-reset--light");
              } else {
                previewElement.value.classList.add("vditor-reset--light");
                previewElement.value.classList.remove("vditor-reset--dark");
              }

              // 为所有图表容器添加固定样式类 - 使用缓存优化
              if (!diagramContainersCache || diagramContainersCache.length === 0) {
                diagramContainersCache = previewElement.value.querySelectorAll(".language-mermaid, .language-flow, .language-plantuml, .language-gantt");
              }
              diagramContainersCache.forEach((container) => {
                container.classList.add("diagram-fixed-theme");
              });

              // 添加任务列表的交互功能
              const setupTaskListInteraction = () => {
                // 添加非空检查，如果预览元素不存在则直接返回
                if (!previewElement.value) {
                  console.warn("setupTaskListInteraction: 预览元素不存在，跳过任务列表交互设置");
                  return;
                }

                // 根据用户反馈，使用已知有效的选择器 - 使用缓存优化
                if (!checkboxesCache || checkboxesCache.length === 0) {
                  checkboxesCache = previewElement.value.querySelectorAll('.vditor-task input[type="checkbox"]');
                }

                // 处理所有找到的复选框
                checkboxesCache.forEach((checkbox) => {
                  // 强制确保可交互性
                  checkbox.disabled = false;
                  checkbox.style.pointerEvents = "auto";
                  checkbox.style.cursor = "pointer";

                  // 找到父级li元素
                  const parentLi = checkbox.closest("li");
                  if (parentLi) {
                    // 如果已经勾选，添加样式属性
                    if (checkbox.checked) {
                      parentLi.setAttribute("data-task-checked", "true");
                    }

                    // 添加change事件处理
                    checkbox.addEventListener("change", (e) => {
                      const isChecked = e.target.checked;
                      // 更新父元素的数据属性，启用样式
                      parentLi.setAttribute("data-task-checked", isChecked.toString());
                    });
                  }
                });
              };

              // 运行初始设置
              setupTaskListInteraction();

              // 清理之前的观察器
              if (mutationObserver) {
                mutationObserver.disconnect();
              }

              // 设置DOM观察器，处理动态变化 - 优化性能
              mutationObserver = new MutationObserver((mutations) => {
                // 只在有相关变化时才处理
                const hasRelevantChanges = mutations.some((mutation) => mutation.type === "childList" && (mutation.addedNodes.length > 0 || mutation.removedNodes.length > 0));

                if (hasRelevantChanges) {
                  // 清理缓存，强制重新查询
                  checkboxesCache = null;
                  diagramContainersCache = null;
                  codeBlocksCache = null;
                  // 延迟短暂时间后运行，确保DOM更新完成
                  safeSetTimeout(setupTaskListInteraction, 50);
                }
              });

              // 监听DOM变化 - 只监听必要的变化
              if (previewElement.value) {
                mutationObserver.observe(previewElement.value, {
                  childList: true,
                  subtree: true,
                  attributes: false, // 不监听属性变化
                  characterData: false, // 不监听文本变化
                });
              } else {
                console.warn("无法设置MutationObserver：预览元素不存在");
              }

              // 添加代码块折叠功能
              setupCodeBlockCollapse();

              // 自定义图片点击放大功能 - 使用事件委托优化性能
              setupImagePreview();

              // 标记内容已渲染，并触发渲染完成事件
              contentRendered.value = true;
              emit("rendered");

              // 恢复滚动位置
              restoreScrollPosition();
            },
          });
        } catch (previewError) {
          console.error("Vditor预览渲染失败:", previewError);
          // 降级到基本文本显示
          if (previewElement.value) {
            const safeContent = content.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;").replace(/'/g, "&#039;");
            previewElement.value.innerHTML = `<pre class="whitespace-pre-wrap">${safeContent}</pre>`;
            contentRendered.value = true;
            emit("rendered");
            restoreScrollPosition();
          }
        }
      }, 100); // 增加延迟确保DOM稳定
    } catch (e) {
      console.error("渲染 Markdown 内容时发生错误:", e);
      // 回退到基本的文本显示
      if (previewElement.value) {
        // 内容转义，避免XSS风险
        const safeContent = content.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;").replace(/'/g, "&#039;");
        previewElement.value.innerHTML = `<pre class="whitespace-pre-wrap">${safeContent}</pre>`;
        // 即使是回退渲染，也标记为已渲染
        contentRendered.value = true;
        emit("rendered");

        // 恢复滚动位置
        restoreScrollPosition();
      }
    }
  } else {
    debugLog(props.enableDebug, props.isDev, "renderContentInternal: 预览元素不存在");
    // 在DOM可用时重试（仅重试一次，避免无限循环）
    safeSetTimeout(() => {
      if (previewElement.value && !contentRendered.value) {
        debugLog(props.enableDebug, props.isDev, "延迟后重试渲染");
        renderContentInternal(content);
      }
    }, 150);
  }
};

// 检查内容是否已渲染，提供多种判断方法确保准确性
const isContentRendered = () => {
  // 优先使用明确的渲染状态标记
  if (contentRendered.value) {
    return true;
  }

  // 以下是备用检查逻辑
  if (!previewElement.value) return false;

  // 检查预览元素是否有内容
  const innerHTML = previewElement.value.innerHTML || "";
  // 更可靠的渲染检测 - 检查是否包含vditor特有的元素或类
  const hasVditorContent = innerHTML.includes('class="vditor') || previewElement.value.querySelectorAll(".vditor-reset").length > 0;

  // 如果有内容，也标记为已渲染
  if (hasVditorContent || innerHTML.trim().length > 50) {
    contentRendered.value = true;
    return true;
  }

  return false;
};

// 对外暴露方法，允许父组件调用
defineExpose({
  renderContent,
  isContentRendered,
});

// 组件挂载时尝试渲染内容
onMounted(() => {
  if (props.content) {
    renderContent(props.content);
  }
});

// 设置图片预览功能 - 使用事件委托优化性能
const setupImagePreview = () => {
  if (!previewElement.value) return;

  // 移除之前的事件监听器（如果存在）
  previewElement.value.removeEventListener("click", handleImageClick);

  // 添加事件委托
  previewElement.value.addEventListener("click", handleImageClick);

  // 为所有图片添加可点击样式
  const images = previewElement.value.querySelectorAll("img");
  images.forEach((img) => {
    img.style.cursor = "pointer";
  });
};

// 处理图片点击事件
const handleImageClick = (e) => {
  if (e.target.tagName === "IMG") {
    e.preventDefault();

    const img = e.target;

    // 创建图片预览容器
    const previewContainer = document.createElement("div");
    previewContainer.className = "image-preview-container";
    previewContainer.style.position = "fixed";
    previewContainer.style.top = "0";
    previewContainer.style.left = "0";
    previewContainer.style.width = "100%";
    previewContainer.style.height = "100%";
    previewContainer.style.backgroundColor = "rgba(0, 0, 0, 0.8)";
    previewContainer.style.zIndex = "10000";
    previewContainer.style.display = "flex";
    previewContainer.style.justifyContent = "center";
    previewContainer.style.alignItems = "center";
    previewContainer.style.cursor = "zoom-out";

    // 创建图片元素
    const previewImg = document.createElement("img");
    previewImg.src = img.src;
    previewImg.style.maxWidth = "90%";
    previewImg.style.maxHeight = "90%";
    previewImg.style.objectFit = "contain";
    previewImg.style.boxShadow = "0 4px 12px rgba(0, 0, 0, 0.5)";
    previewImg.style.transition = "transform 0.3s ease";

    // 添加点击关闭功能
    previewContainer.addEventListener("click", () => {
      document.body.removeChild(previewContainer);
    });

    // 防止点击图片时冒泡事件关闭预览
    previewImg.addEventListener("click", (e) => {
      e.stopPropagation();
    });

    // 添加到DOM
    previewContainer.appendChild(previewImg);
    document.body.appendChild(previewContainer);
  }
};

// 组件卸载时清理资源
onBeforeUnmount(() => {
  // 清理所有定时器
  clearAllTimeouts();

  // 清理MutationObserver
  if (mutationObserver) {
    mutationObserver.disconnect();
    mutationObserver = null;
  }

  // 清理图片预览事件监听器
  if (previewElement.value) {
    previewElement.value.removeEventListener("click", handleImageClick);
  }

  // 清理DOM缓存
  diagramContainersCache = null;
  checkboxesCache = null;
  imagesCache = null;
  codeBlocksCache = null;
});

// 设置代码块折叠功能
const setupCodeBlockCollapse = () => {
  if (!previewElement.value) return;

  // 查找所有代码块 - 使用缓存优化性能
  if (!codeBlocksCache || codeBlocksCache.length === 0) {
    codeBlocksCache = previewElement.value.querySelectorAll('pre code[class*="language-"]');
  }
  const codeBlocks = codeBlocksCache;

  codeBlocks.forEach((codeBlock) => {
    // 避免重复处理
    if (codeBlock.parentElement.getAttribute("data-collapsible") === "true") {
      return;
    }

    // 标记已处理
    codeBlock.parentElement.setAttribute("data-collapsible", "true");

    // 获取语言类名
    const languageClass = Array.from(codeBlock.classList).find((cls) => cls.startsWith("language-"));

    // 提取语言名称
    const language = languageClass ? languageClass.replace("language-", "") : "代码";

    // 计算代码行数，如果超过一定行数或长度则默认折叠
    const codeText = codeBlock.textContent || "";
    const lineCount = (codeText.match(/\n/g) || []).length + 1;
    const isLargeCodeBlock = lineCount > 15 || codeText.length > 2000;

    // 创建details和summary元素
    const details = document.createElement("details");
    details.className = "code-block-collapsible";
    // 根据大小决定是否默认展开
    details.open = !isLargeCodeBlock; // 小代码块默认展开，大代码块默认折叠

    const summary = document.createElement("summary");
    summary.className = "code-block-summary";

    // 创建语言标签
    const langLabel = document.createElement("span");
    langLabel.className = "code-block-language";
    langLabel.textContent = language;

    // 添加代码行数信息
    const lineInfo = document.createElement("span");
    lineInfo.className = "code-block-line-info";
    lineInfo.textContent = `${lineCount}行`;

    // 创建操作文本
    const actionText = document.createElement("span");
    actionText.className = "code-block-action-text";
    actionText.textContent = "折叠";

    // 创建折叠提示
    const collapseHint = document.createElement("span");
    collapseHint.className = "code-block-collapse-hint";
    collapseHint.innerHTML =
      '<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="currentColor" viewBox="0 0 16 16"><path fill-rule="evenodd" d="M1.646 4.646a.5.5 0 0 1 .708 0L8 10.293l5.646-5.647a.5.5 0 0 1 .708.708l-6 6a.5.5 0 0 1-.708 0l-6-6a.5.5 0 0 1 0-.708z"/></svg>';

    // 将左侧信息（语言和行数）添加到summary
    const leftContainer = document.createElement("div");
    leftContainer.className = "code-block-info";
    leftContainer.appendChild(langLabel);
    leftContainer.appendChild(lineInfo);
    summary.appendChild(leftContainer);

    // 将右侧操作（折叠文本和图标）添加到summary
    const actionsContainer = document.createElement("div");
    actionsContainer.className = "code-block-actions";

    // 为 HTML 代码块添加预览按钮
    if (language.toLowerCase() === "html") {
      const previewButton = document.createElement("button");
      previewButton.className = "code-block-preview-button";
      previewButton.innerHTML =
        '<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="currentColor" viewBox="0 0 16 16"><path d="M10.5 8a2.5 2.5 0 1 1-5 0 2.5 2.5 0 0 1 5 0z"/><path d="M0 8s3-5.5 8-5.5S16 8 16 8s-3 5.5-8 5.5S0 8 0 8zm8 3.5a3.5 3.5 0 1 0 0-7 3.5 3.5 0 0 0 0 7z"/></svg>';
      previewButton.title = "预览 HTML";

      // 添加点击事件
      previewButton.addEventListener("click", (e) => {
        e.stopPropagation(); // 阻止事件冒泡

        // 获取 HTML 代码内容并设置到预览变量
        previewHtmlContent.value = codeText;
        showHtmlPreview.value = true;
      });

      actionsContainer.appendChild(previewButton);
    }

    // 为 SVG 代码块添加预览按钮
    if (language.toLowerCase() === "svg") {
      const previewButton = document.createElement("button");
      previewButton.className = "code-block-preview-button";
      previewButton.innerHTML =
        '<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="currentColor" viewBox="0 0 16 16"><path d="M10.5 8a2.5 2.5 0 1 1-5 0 2.5 2.5 0 0 1 5 0z"/><path d="M0 8s3-5.5 8-5.5S16 8 16 8s-3 5.5-8 5.5S0 8 0 8zm8 3.5a3.5 3.5 0 1 0 0-7 3.5 3.5 0 0 0 0 7z"/></svg>';
      previewButton.title = "预览 SVG";

      // 添加点击事件
      previewButton.addEventListener("click", (e) => {
        e.stopPropagation(); // 阻止事件冒泡

        // 获取 SVG 代码内容并设置到预览变量
        previewSvgContent.value = codeText;
        showSvgPreview.value = true;
      });

      actionsContainer.appendChild(previewButton);
    }

    actionsContainer.appendChild(actionText);
    actionsContainer.appendChild(collapseHint);
    summary.appendChild(actionsContainer);

    // 获取代码块的父元素(pre标签)
    const preElement = codeBlock.parentElement;
    const preParent = preElement.parentElement;

    // 将pre放入details中
    details.appendChild(summary);
    preParent.replaceChild(details, preElement);
    details.appendChild(preElement);

    // 添加点击事件处理
    summary.addEventListener("click", (e) => {
      // 阻止事件冒泡，避免影响其他点击行为
      e.stopPropagation();

      // 更新操作文本
      safeSetTimeout(() => {
        actionText.textContent = details.open ? "折叠" : "展开";
      }, 0);
    });

    // 设置初始文本
    actionText.textContent = details.open ? "折叠" : "展开";
  });
};

// 在外部浏览器中打开 HTML
const openHtmlInExternalBrowser = (htmlContent) => {
  // 创建包含完整 HTML 结构的文档
  const htmlTemplate = `
    <!DOCTYPE html>
    <html>
      <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>HTML 预览</title>
        <style>
          body {
            font-family: system-ui, -apple-system, sans-serif;
            margin: 0;
            padding: 0;
          }
        </style>
      </head>
      <body>
        ${htmlContent}
      </body>
    </html>
  `;

  // 使用 Blob 创建 URL
  const blob = new Blob([htmlTemplate], { type: "text/html" });
  const url = URL.createObjectURL(blob);

  // 在新窗口中打开
  window.open(url, "_blank");

  // 清理 Blob URL
  safeSetTimeout(() => URL.revokeObjectURL(url), 100);
};

// 在外部浏览器中打开 SVG
const openSvgInExternalBrowser = (svgContent) => {
  // 创建包含完整 SVG 结构的文档
  const svgTemplate = `
    <!DOCTYPE html>
    <html>
      <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>SVG 预览</title>
        <style>
          body {
            font-family: system-ui, -apple-system, sans-serif;
            margin: 0;
            padding: 0;
            display: flex;
            justify-content: center;
            align-items: center;
            min-height: 100vh;
            background-color: ${props.darkMode ? "#1a1a1a" : "#ffffff"};
          }
          svg {
            max-width: 100%;
            max-height: 100vh;
          }
        </style>
      </head>
      <body>
        ${svgContent}
      </body>
    </html>
  `;

  // 使用 Blob 创建 URL
  const blob = new Blob([svgTemplate], { type: "text/html" });
  const url = URL.createObjectURL(blob);

  // 在新窗口中打开
  window.open(url, "_blank");

  // 清理 Blob URL
  safeSetTimeout(() => URL.revokeObjectURL(url), 100);
};
</script>

<template>
  <div class="paste-view-preview">
    <!-- 内容渲染中的加载动画 -->
    <div v-if="props.content && !contentRendered && !props.isPlainTextMode" class="py-10 flex justify-center items-center">
      <svg class="animate-spin h-8 w-8" :class="props.darkMode ? 'text-blue-400' : 'text-primary-500'" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
        <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="3"></circle>
        <path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
      </svg>
      <span class="ml-3 text-sm" :class="props.darkMode ? 'text-gray-300' : 'text-gray-500'">正在渲染内容...</span>
    </div>

    <!-- 纯文本模式显示 -->
    <div v-if="props.isPlainTextMode && props.content" class="plain-text-container">
      <pre class="whitespace-pre-wrap p-4 font-mono text-base overflow-auto min-h-[300px]" :class="props.darkMode ? 'text-gray-200' : 'text-gray-800'">{{ props.content }}</pre>
    </div>

    <!-- Markdown渲染模式显示 -->
    <div v-else>
      <!-- Vditor 预览容器 - 这里是实际的Markdown渲染区域 -->
      <div ref="previewElement" class="vditor-reset markdown-body min-h-[300px]" :class="{ 'opacity-0': !contentRendered }"></div>

      <!-- 后备内容显示 - 当预览元素为空或渲染失败时显示原始Markdown -->
      <div
        v-if="props.content && !isContentRendered() && previewElement && contentRendered === false"
        class="mt-4 p-3 border rounded"
        :class="props.darkMode ? 'border-yellow-600 bg-yellow-900/20' : 'border-yellow-500 bg-yellow-50'"
      >
        <p class="text-sm mb-2" :class="props.darkMode ? 'text-yellow-300' : 'text-yellow-700'">Markdown 渲染失败，显示原始内容：</p>
        <pre class="whitespace-pre-wrap overflow-auto max-h-[600px] p-3 rounded" :class="props.darkMode ? 'text-gray-200 bg-gray-800' : 'text-gray-800 bg-gray-100'">{{
          props.content
        }}</pre>
      </div>
    </div>

    <!-- 无内容提示 -->
    <p v-if="!props.content" :class="props.darkMode ? 'text-gray-400' : 'text-gray-500'">无内容</p>

    <!-- HTML 预览弹窗组件 -->
    <HtmlPreviewModal
      :show="showHtmlPreview"
      :html-content="previewHtmlContent"
      :dark-mode="props.darkMode"
      :content-type="'html'"
      @close="showHtmlPreview = false"
      @open-external="openHtmlInExternalBrowser"
    />

    <!-- SVG 预览弹窗组件 -->
    <HtmlPreviewModal
      :show="showSvgPreview"
      :html-content="previewSvgContent"
      :dark-mode="props.darkMode"
      :content-type="'svg'"
      @close="showSvgPreview = false"
      @open-external="openSvgInExternalBrowser"
    />
  </div>
</template>

<style scoped>
/* Vditor预览样式定制 */
/* 这些样式会被应用到Vditor渲染的内容上，使其更美观并适应暗色/亮色模式 */
:deep(.vditor-reset) {
  font-size: 16px;
  line-height: 1.7;
  padding: 0.5rem;
  transition: all 0.3s ease; /* 增加所有属性的过渡效果 */
  color: v-bind('props.darkMode ? "#d4d4d4" : "#374151"'); /* 显式设置文本颜色 */
  background-color: v-bind('props.darkMode ? "transparent" : "transparent"'); /* 确保背景透明 */
}

/* 确保暗色模式下的特定样式 */
:deep(.vditor-reset--dark) {
  color: #d4d4d4;
  background-color: transparent;
}

/* 确保亮色模式下的特定样式 */
:deep(.vditor-reset--light) {
  color: #374151;
  background-color: transparent;
}

/* 标题样式 */
:deep(.vditor-reset h1, .vditor-reset h2) {
  border-bottom: 1px solid v-bind('props.darkMode ? "#30363d" : "#e5e7eb"');
  padding-bottom: 0.3em;
  margin-top: 1.8em;
  margin-bottom: 1em;
}

:deep(.vditor-reset h1) {
  font-size: 2em;
}

:deep(.vditor-reset h2) {
  font-size: 1.6em;
}

:deep(.vditor-reset h3) {
  font-size: 1.4em;
  margin-top: 1.5em;
  margin-bottom: 0.75em;
}

/* 段落样式 */
:deep(.vditor-reset p) {
  margin-top: 0.75em;
  margin-bottom: 0.75em;
}

/* 行内代码样式 */
:deep(.vditor-reset code:not(.hljs)) {
  background-color: v-bind('props.darkMode ? "#252526" : "#f3f4f6"');
  color: v-bind('props.darkMode ? "#ce9178" : "#ef4444"');
  padding: 0.2em 0.4em;
  border-radius: 3px;
  font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
}

/* 引用块样式 */
:deep(.vditor-reset blockquote) {
  border-left: 4px solid v-bind('props.darkMode ? "#4b5563" : "#e5e7eb"');
  padding: 0.5em 1em;
  margin-left: 0;
  margin-right: 0;
  margin-top: 1em;
  margin-bottom: 1em;
  color: v-bind('props.darkMode ? "#9ca3af" : "#6b7280"');
  background-color: v-bind('props.darkMode ? "#1a1a1a" : "#f9fafb"');
  border-radius: 0.25rem;
}

/* 链接样式 */
:deep(.vditor-reset a) {
  color: v-bind('props.darkMode ? "#3b82f6" : "#2563eb"');
  text-decoration: none;
}

:deep(.vditor-reset a:hover) {
  text-decoration: underline;
}

/* 表格样式 */
:deep(.vditor-reset table) {
  border-collapse: collapse;
  margin: 1.25em 0;
  width: 100%;
}

/* 添加表格容器，确保在小屏幕上可以滚动 */
:deep(.vditor-reset div:has(> table)) {
  overflow-x: auto;
  max-width: 100%;
}

:deep(.vditor-reset table th, .vditor-reset table td) {
  border: 1px solid v-bind('props.darkMode ? "#30363d" : "#e5e7eb"');
  padding: 0.6em 1em;
}

:deep(.vditor-reset table th) {
  background-color: v-bind('props.darkMode ? "#252526" : "#f3f4f6"');
  font-weight: 600;
  color: v-bind('props.darkMode ? "#e2e8f0" : "#374151"');
}

:deep(.vditor-reset table td) {
  background-color: v-bind('props.darkMode ? "#1e1e1e" : "#ffffff"');
}

:deep(.vditor-reset table tr:nth-child(even) td) {
  background-color: v-bind('props.darkMode ? "#252526" : "#f9fafb"');
}

:deep(.vditor-reset table tr:hover td) {
  background-color: v-bind('props.darkMode ? "#2c2c2d" : "#f3f4f6"');
}

/* 代码块样式 */
:deep(.vditor-reset pre) {
  margin: 1.25em 0;
  padding: 1.25em;
  border-radius: 0; /* 移除圆角 */
  background-color: transparent; /* 移除背景色，使用透明背景 */
  overflow-x: auto;
  line-height: 1.5;
  border: none; /* 确保没有边框 */
  box-shadow: none; /* 移除阴影 */
}

/* 列表样式 */
:deep(.vditor-reset ul, .vditor-reset ol) {
  padding-left: 2em;
  margin: 1em 0;
}

/* 多级列表样式 */
/* 有序列表样式 */
:deep(.vditor-reset ol) {
  list-style-type: decimal;
}

:deep(.vditor-reset ol ol) {
  list-style-type: decimal;
}

:deep(.vditor-reset ol ol ol) {
  list-style-type: decimal;
}

/* 无序列表样式 - 确保普通无序列表显示圆点 */
:deep(.vditor-reset ul) {
  list-style-type: disc;
}

:deep(.vditor-reset ul ul) {
  list-style-type: circle;
}

:deep(.vditor-reset ul ul ul) {
  list-style-type: square;
}

/* 图片样式 */
:deep(.vditor-reset img) {
  max-width: 100%;
  margin: 1.25em 0;
  border-radius: 0.25rem;
}

/* 图表样式固定 - 不受暗色模式影响 */
:deep(.diagram-fixed-theme) {
  background-color: white !important; /* 强制使用白色背景 */
  color: #333 !important; /* 强制使用深色文本 */
  filter: none !important; /* 移除任何可能的过滤器效果 */
}

/* 确保mermaid图表使用固定样式 */
:deep(.language-mermaid) {
  background-color: white !important;
  padding: 10px !important;
  border-radius: 4px !important;
  border: 1px solid #e5e7eb !important;
  box-shadow: 0 2px 4px rgba(0, 0, 0, 0.05) !important;
}

/* 流程图样式固定 */
:deep(.language-flow) {
  background-color: white !important;
  padding: 10px !important;
  border-radius: 4px !important;
  border: 1px solid #e5e7eb !important;
}

/* PlantUML图表样式固定 */
:deep(.language-plantuml) {
  background-color: white !important;
  padding: 10px !important;
  border-radius: 4px !important;
  border: 1px solid #e5e7eb !important;
}

/* 甘特图样式固定 */
:deep(.language-gantt) {
  background-color: white !important;
  padding: 10px !important;
  border-radius: 4px !important;
  border: 1px solid #e5e7eb !important;
}

/* 图表内的SVG元素样式固定 */
:deep(.diagram-fixed-theme svg) {
  background-color: white !important;
}

/* HTML 预览按钮样式 */
:deep(.code-block-preview-button) {
  background: none;
  border: none;
  cursor: pointer;
  color: inherit;
  padding: 4px;
  margin-right: 8px;
  border-radius: 4px;
  display: flex;
  align-items: center;
  justify-content: center;
  transition: background-color 0.2s;
}

:deep(.code-block-preview-button:hover) {
  background-color: v-bind('props.darkMode ? "rgba(255, 255, 255, 0.1)" : "rgba(0, 0, 0, 0.05)"');
}

:deep(.code-block-preview-button svg) {
  width: 16px;
  height: 16px;
}

/* 针对手机屏幕的响应式调整 */
@media (max-width: 640px) {
  :deep(.vditor-reset) {
    font-size: 15px;
    padding: 0.25rem;
  }

  :deep(.vditor-reset pre) {
    padding: 0.5em;
  }

  :deep(.code-block-collapsible pre) {
    padding: 0.5em !important; /* 在移动设备上使用更小的padding */
  }

  :deep(.vditor-reset table) {
    font-size: 14px;
  }
}

/* 中等屏幕 */
@media (min-width: 641px) and (max-width: 1024px) {
  :deep(.vditor-reset) {
    padding: 0.75rem;
  }
}

/* 大屏幕额外优化 */
@media (min-width: 1280px) {
  :deep(.vditor-reset) {
    font-size: 17px;
    padding: 1rem;
  }

  :deep(.vditor-reset pre) {
    margin: 1.5em 0;
    padding: 1.5em;
  }
}

/* 任务列表样式增强 */
:deep(.vditor-reset ul li input[type="checkbox"]) {
  cursor: pointer !important;
  pointer-events: auto !important;
  margin-right: 0.5em;
  width: 1.2em;
  height: 1.2em;
  vertical-align: middle;
  position: relative;
  top: -0.1em;
}

/* 只对任务列表项应用list-style-type: none */
:deep(.vditor-reset ul li.vditor-task) {
  list-style-type: none !important;
}

/* 确保高优先级覆盖所有嵌套级别的任务列表 */
:deep(.vditor-reset ul li.vditor-task),
:deep(.vditor-reset ul ul li.vditor-task),
:deep(.vditor-reset ul ul ul li.vditor-task) {
  list-style-type: none !important;
}

/* 确保普通列表项的样式不受影响 */
:deep(.vditor-reset ul li:not(.vditor-task)) {
  list-style-type: disc;
}

:deep(.vditor-reset ul ul li:not(.vditor-task)) {
  list-style-type: circle;
}

:deep(.vditor-reset ul ul ul li:not(.vditor-task)) {
  list-style-type: square;
}

/* 确保任务列表项交互状态更明显 */
:deep(.vditor-reset ul li[data-task-checked="true"]) {
  text-decoration: line-through;
  color: v-bind('props.darkMode ? "#6b7280" : "#9ca3af"');
}

:deep(.vditor-reset ul li input[type="checkbox"]:hover),
:deep(.vditor-reset ul li input[type="checkbox"]:focus) {
  outline: 1px solid v-bind('props.darkMode ? "#3b82f6" : "#2563eb"');
  box-shadow: 0 0 0 2px rgba(59, 130, 246, 0.3);
}

/* 确保任务列表在暗色模式下有正确的颜色 */
:deep(.vditor-reset--dark ul li input[type="checkbox"]) {
  background-color: #1e1e1e;
  border-color: #4b5563;
}

:deep(.vditor-reset--light ul li input[type="checkbox"]) {
  background-color: #ffffff;
  border-color: #d1d5db;
}

/* 代码块折叠功能相关样式 */
:deep(.code-block-collapsible) {
  width: 100%;
  margin: 1.25em 0;
  border-radius: 0.375rem;
  overflow: hidden;
  border: 1px solid v-bind('props.darkMode ? "#30363d" : "#e5e7eb"');
  background-color: v-bind('props.darkMode ? "#1a1a1a" : "#f8f9fa"');
  box-shadow: 0 1px 2px rgba(0, 0, 0, 0.05);
}

:deep(.code-block-summary) {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 0.5rem 1rem;
  font-size: 0.875rem;
  cursor: pointer;
  user-select: none;
  border-bottom: 1px solid v-bind('props.darkMode ? "#30363d" : "#e5e7eb"');
  background-color: v-bind('props.darkMode ? "#252526" : "#f1f3f5"');
  color: v-bind('props.darkMode ? "#e2e8f0" : "#4b5563"');
  font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
}

:deep(.code-block-summary:hover) {
  background-color: v-bind('props.darkMode ? "#2c2c2d" : "#e9ecef"');
}

:deep(.code-block-language) {
  font-weight: 600;
  color: v-bind('props.darkMode ? "#3b82f6" : "#2563eb"');
}

:deep(.code-block-info) {
  display: flex;
  align-items: center;
  gap: 0.5rem;
}

:deep(.code-block-line-info) {
  font-size: 0.75rem;
  color: v-bind('props.darkMode ? "#9ca3af" : "#6b7280"');
  opacity: 0.8;
}

:deep(.code-block-actions) {
  display: flex;
  align-items: center;
  gap: 0.5rem;
}

:deep(.code-block-action-text) {
  font-size: 0.75rem;
  opacity: 0.8;
}

:deep(.code-block-collapse-hint) {
  display: flex;
  align-items: center;
  transition: transform 0.2s ease;
}

:deep(.code-block-collapsible[open] .code-block-collapse-hint) {
  transform: rotate(180deg);
}

/* 强制调整代码块内部的样式 */
:deep(.code-block-collapsible pre) {
  margin: 0 !important; /* 移除margins，因为我们现在有外部容器 */
  padding: 0.75em !important; /* 减小padding值，缩小内容与容器间距 */
  border-radius: 0 !important; /* 移除圆角，让它与外部容器融合 */
  border: none !important; /* 移除边框 */
}

/* 增强代码块内部滚动条样式 */
:deep(.code-block-collapsible pre::-webkit-scrollbar) {
  height: 8px;
}

:deep(.code-block-collapsible pre::-webkit-scrollbar-track) {
  background: v-bind('props.darkMode ? "#1e1e1e" : "#f1f1f1"');
}

:deep(.code-block-collapsible pre::-webkit-scrollbar-thumb) {
  background-color: v-bind('props.darkMode ? "#4b5563" : "#c1c9d6"');
  border-radius: 4px;
}

:deep(.code-block-collapsible pre::-webkit-scrollbar-thumb:hover) {
  background-color: v-bind('props.darkMode ? "#6b7280" : "#a1a9b6"');
}

/* 移动端适配 */
@media (max-width: 640px) {
  :deep(.code-block-summary) {
    padding: 0.375rem 0.75rem;
    font-size: 0.75rem;
  }

  :deep(.code-block-action-text) {
    font-size: 0.7rem;
  }

  :deep(.code-block-collapse-hint svg) {
    width: 14px;
    height: 14px;
  }
}

/* 纯文本容器样式 - 与Markdown渲染风格保持一致 */
.plain-text-container pre {
  font-size: 16px;
  line-height: 1.7;
  transition: all 0.3s ease;
  background-color: transparent;
}

/* 针对手机屏幕的响应式调整 */
@media (max-width: 640px) {
  .plain-text-container pre {
    font-size: 15px;
    padding: 0.25rem;
  }
}

/* 中等屏幕 */
@media (min-width: 641px) and (max-width: 1024px) {
  .plain-text-container pre {
    padding: 0.75rem;
  }
}

/* 大屏幕额外优化 */
@media (min-width: 1280px) {
  .plain-text-container pre {
    font-size: 17px;
    padding: 1rem;
  }
}
</style>
