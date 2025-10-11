/**
 * 代码高亮 Composable
 * 基于 highlight.js 实现代码语法高亮
 */

import { ref, computed } from "vue";

export function useCodeHighlight() {
  // 状态
  const isHighlightLoaded = ref(false);
  const highlightInstance = ref(null);
  const loadingError = ref(null);

  // 支持的语言列表
  const supportedLanguages = ref([
    "javascript",
    "typescript",
    "python",
    "java",
    "cpp",
    "c",
    "csharp",
    "php",
    "ruby",
    "go",
    "rust",
    "swift",
    "kotlin",
    "scala",
    "dart",
    "r",
    "html",
    "css",
    "scss",
    "less",
    "sass",
    "xml",
    "json",
    "yaml",
    "toml",
    "sql",
    "bash",
    "shell",
    "powershell",
    "dockerfile",
    "nginx",
    "markdown",
    "latex",
    "makefile",
    "cmake",
    "gradle",
    "maven",
    "vue",
    "jsx",
    "tsx",
    "svelte",
    "angular",
    "react",
    "perl",
    "lua",
    "vim",
    "ini",
    "properties",
    "diff",
    "patch",
  ]);

  // 主题列表
  const availableThemes = ref([
    { value: "github", label: "GitHub", description: "经典GitHub风格" },
    { value: "vs2015", label: "VS2015", description: "Visual Studio 2015暗色主题" },
    { value: "atom-one-dark", label: "Atom One Dark", description: "Atom编辑器暗色主题" },
    { value: "atom-one-light", label: "Atom One Light", description: "Atom编辑器亮色主题" },
    { value: "monokai", label: "Monokai", description: "经典Monokai主题" },
    { value: "dracula", label: "Dracula", description: "Dracula暗色主题" },
    { value: "tomorrow", label: "Tomorrow", description: "Tomorrow亮色主题" },
    { value: "tomorrow-night", label: "Tomorrow Night", description: "Tomorrow暗色主题" },
    { value: "solarized-light", label: "Solarized Light", description: "Solarized亮色主题" },
    { value: "solarized-dark", label: "Solarized Dark", description: "Solarized暗色主题" },
  ]);

  // 计算属性
  const isReady = computed(() => isHighlightLoaded.value && !loadingError.value);

  /**
   * 加载 highlight.js 库（复用 Vditor 的 highlight.js）
   * @returns {Promise<Object>} highlight.js 实例
   */
  const loadHighlightJs = async () => {
    if (highlightInstance.value) {
      return highlightInstance.value;
    }

    try {
      loadingError.value = null;

      // 检查是否已经加载了 highlight.js（通过 Vditor）
      if (window.hljs) {
        console.log("使用已加载的 highlight.js 实例");
        highlightInstance.value = window.hljs;
        isHighlightLoaded.value = true;
        return highlightInstance.value;
      }

      // 如果没有加载，则通过 Vditor 的方式加载
      console.log("通过 Vditor 方式加载 highlight.js");

      // 加载 highlight.js 主文件
      await loadScript("/assets/vditor/dist/js/highlight.js/highlight.min.js");

      // 加载第三方语言包
      await loadScript("/assets/vditor/dist/js/highlight.js/third-languages.js");

      // 等待 hljs 全局变量可用
      await waitForGlobal("hljs", 5000);

      highlightInstance.value = window.hljs;
      isHighlightLoaded.value = true;

      console.log("highlight.js 加载成功，支持语言:", window.hljs.listLanguages());

      return highlightInstance.value;
    } catch (error) {
      console.error("加载 highlight.js 失败:", error);
      loadingError.value = error.message;
      throw error;
    }
  };

  /**
   * 动态加载脚本
   * @param {string} src - 脚本路径
   * @returns {Promise<void>}
   */
  const loadScript = (src) => {
    return new Promise((resolve, reject) => {
      // 检查是否已经加载
      const existingScript = document.querySelector(`script[src="${src}"]`);
      if (existingScript) {
        resolve();
        return;
      }

      const script = document.createElement("script");
      script.src = src;
      script.onload = () => resolve();
      script.onerror = () => reject(new Error(`无法加载脚本: ${src}`));
      document.head.appendChild(script);
    });
  };

  /**
   * 等待全局变量可用
   * @param {string} globalName - 全局变量名
   * @param {number} timeout - 超时时间（毫秒）
   * @returns {Promise<void>}
   */
  const waitForGlobal = (globalName, timeout = 5000) => {
    return new Promise((resolve, reject) => {
      const startTime = Date.now();

      const check = () => {
        if (window[globalName]) {
          resolve();
        } else if (Date.now() - startTime > timeout) {
          reject(new Error(`等待全局变量 ${globalName} 超时`));
        } else {
          setTimeout(check, 100);
        }
      };

      check();
    });
  };

  /**
   * 高亮代码
   * @param {string} code - 代码内容
   * @param {string} language - 语言类型
   * @param {Object} options - 高亮选项
   * @returns {Promise<Object>} 高亮结果
   */
  const highlightCode = async (code, language = "", options = {}) => {
    if (!code || typeof code !== "string") {
      return {
        success: false,
        html: "",
        language: "",
        error: "代码内容为空或无效",
      };
    }

    try {
      const hljs = await loadHighlightJs();

      let result;

      if (language && supportedLanguages.value.includes(language.toLowerCase())) {
        // 指定语言高亮
        try {
          result = hljs.highlight(code, { language: language.toLowerCase() });
        } catch (err) {
          console.warn(`指定语言 ${language} 高亮失败，尝试自动检测:`, err);
          result = hljs.highlightAuto(code);
        }
      } else {
        // 自动检测语言
        result = hljs.highlightAuto(code);
      }

      return {
        success: true,
        html: result.value,
        language: result.language || "plaintext",
        relevance: result.relevance || 0,
        error: null,
      };
    } catch (error) {
      console.error("代码高亮失败:", error);

      return {
        success: false,
        html: escapeHtml(code), // 返回转义的原始代码
        language: language || "plaintext",
        relevance: 0,
        error: error.message,
      };
    }
  };

  /**
   * 根据文件扩展名推测语言
   * @param {string} filename - 文件名
   * @returns {string} 语言类型
   */
  const detectLanguageFromFilename = (filename) => {
    if (!filename) return "";

    const ext = filename.split(".").pop()?.toLowerCase();

    const extensionMap = {
      // JavaScript 系列
      js: "javascript",
      jsx: "javascript",
      ts: "typescript",
      tsx: "typescript",
      vue: "vue",
      mjs: "javascript",
      cjs: "javascript",

      // Python
      py: "python",
      pyw: "python",
      pyi: "python",

      // Java 系列
      java: "java",
      kt: "kotlin",
      kts: "kotlin",
      scala: "scala",

      // C 系列
      c: "c",
      h: "c",
      cpp: "cpp",
      cxx: "cpp",
      cc: "cpp",
      hpp: "cpp",
      hxx: "cpp",
      cs: "csharp",

      // Web 技术
      html: "html",
      htm: "html",
      xhtml: "html",
      css: "css",
      scss: "scss",
      sass: "sass",
      less: "less",

      // 数据格式
      json: "json",
      xml: "xml",
      yaml: "yaml",
      yml: "yaml",
      toml: "toml",
      ini: "ini",
      cfg: "ini",
      conf: "ini",

      // 脚本语言
      php: "php",
      rb: "ruby",
      go: "go",
      rs: "rust",
      swift: "swift",
      dart: "dart",
      r: "r",
      R: "r",
      perl: "perl",
      pl: "perl",
      lua: "lua",

      // Shell 脚本
      sh: "bash",
      bash: "bash",
      zsh: "bash",
      fish: "bash",
      ps1: "powershell",
      psm1: "powershell",

      // 数据库
      sql: "sql",

      // 配置文件
      dockerfile: "dockerfile",
      makefile: "makefile",
      cmake: "cmake",
      gradle: "gradle",
      maven: "xml",

      // 文档
      md: "markdown",
      markdown: "markdown",
      tex: "latex",
      latex: "latex",

      // 其他
      vim: "vim",
      diff: "diff",
      patch: "diff",
    };

    return extensionMap[ext] || "";
  };

  /**
   * 加载主题样式
   * @param {string} theme - 主题名称
   * @param {boolean} darkMode - 是否暗色模式
   * @returns {Promise<void>}
   */
  const loadTheme = async (theme, darkMode = false) => {
    // 根据暗色模式自动选择主题
    if (!theme) {
      theme = darkMode ? "vs2015" : "github";
    }

    try {
      // 移除旧的主题样式
      const oldStyle = document.getElementById("hljs-theme");
      if (oldStyle) {
        oldStyle.remove();
      }

      // 检查主题文件是否存在的路径列表
      const possiblePaths = [`/assets/vditor/dist/js/highlight.js/styles/${theme}.min.css`, `/assets/vditor/dist/js/highlight.js/styles/${theme}.css`];

      // 尝试加载主题
      let loaded = false;
      for (const path of possiblePaths) {
        try {
          await loadStylesheet(path, "hljs-theme");
          console.log(`成功加载主题: ${theme} from ${path}`);
          loaded = true;
          break;
        } catch (err) {
          console.warn(`无法从 ${path} 加载主题 ${theme}:`, err.message);
        }
      }

      if (!loaded) {
        console.warn(`所有路径都无法加载主题 ${theme}，使用默认样式`);
        // 不抛出错误，允许使用默认样式
      }
    } catch (error) {
      console.error("加载主题失败:", error);
      // 不抛出错误，允许使用默认样式
    }
  };

  /**
   * 动态加载样式表
   * @param {string} href - 样式表路径
   * @param {string} id - 样式表ID
   * @returns {Promise<void>}
   */
  const loadStylesheet = (href, id) => {
    return new Promise((resolve, reject) => {
      const link = document.createElement("link");
      link.id = id;
      link.rel = "stylesheet";
      link.href = href;

      link.onload = () => resolve();
      link.onerror = () => reject(new Error(`无法加载样式表: ${href}`));

      document.head.appendChild(link);
    });
  };

  /**
   * 转义 HTML 字符
   * @param {string} text - 原始文本
   * @returns {string} 转义后的文本
   */
  const escapeHtml = (text) => {
    const div = document.createElement("div");
    div.textContent = text;
    return div.innerHTML;
  };

  /**
   * 获取推荐主题
   * @param {boolean} darkMode - 是否暗色模式
   * @returns {string} 主题名称
   */
  const getRecommendedTheme = (darkMode) => {
    return darkMode ? "vs2015" : "github";
  };

  return {
    // 状态
    isHighlightLoaded,
    loadingError,
    supportedLanguages,
    availableThemes,

    // 计算属性
    isReady,

    // 方法
    loadHighlightJs,
    highlightCode,
    detectLanguageFromFilename,
    loadTheme,
    getRecommendedTheme,
  };
}
