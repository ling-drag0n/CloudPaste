<script setup>
// PasteViewEditor组件 - 提供Markdown编辑及相关配置功能
// 该组件使用Vditor作为编辑器，允许用户修改内容并设置过期时间等元数据
import { ref, onMounted, watch, onBeforeUnmount } from "vue";
import Vditor from "vditor";
import "vditor/dist/index.css";
import { getInputClasses, debugLog } from "./PasteViewUtils";
// 导入Word导出服务
import markdownToWord from "../../utils/markdownToWord";
// 导入FileSaver用于下载文件
import { saveAs } from "file-saver";

// 定义组件接收的属性
const props = defineProps({
  // 是否为暗色模式，控制编辑器主题
  darkMode: {
    type: Boolean,
    required: true,
  },
  // 要编辑的Markdown内容
  content: {
    type: String,
    default: "",
  },
  // 文本分享对象，包含元数据
  paste: {
    type: Object,
    default: () => ({}),
  },
  // 是否处于加载状态
  loading: {
    type: Boolean,
    default: false,
  },
  // 错误信息
  error: {
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
});

// 定义组件可触发的事件
const emit = defineEmits(["save", "cancel", "update:error"]);

// 编辑器实例引用
const vditorInstance = ref(null);
// 编辑表单数据，包含元数据设置
const editForm = ref({
  remark: props.paste?.remark || "",
  customLink: props.paste?.slug || "",
  expiryTime: "24", // 默认为1天
  maxViews: props.paste?.maxViews || 0,
  password: "", // 新增密码字段
  clearPassword: false, // 新增是否清除密码的标志
});

// 密码可见性控制
const showPassword = ref(false);

// 复制为其他格式功能逻辑
const copyFormatMenuVisible = ref(false);
const copyFormatMenuPosition = ref({ x: 0, y: 0 });
// 成功通知消息
const notification = ref("");
// 存储最后一次使用的按钮元素引用，用于重新定位菜单
const lastCopyFormatsBtnElement = ref(null);

// 切换密码可见性
const togglePasswordVisibility = () => {
  showPassword.value = !showPassword.value;
};

// 监听paste对象变化，更新表单数据
watch(
    () => props.paste,
    (newPaste) => {
      if (newPaste) {
        // 初始化编辑表单数据
        editForm.value.remark = newPaste.remark || "";
        editForm.value.customLink = newPaste.slug || "";
        // 密码字段重置为空字符串
        editForm.value.password = "";
        editForm.value.clearPassword = false;

        // 处理过期时间 - 将ISO日期转换为选择项值
        if (newPaste.expiresAt) {
          const expiryDate = new Date(newPaste.expiresAt);
          const now = new Date();
          const diffHours = Math.round((expiryDate - now) / (1000 * 60 * 60));

          // 根据剩余时间选择最接近的预设选项
          if (diffHours <= 1) {
            editForm.value.expiryTime = "1";
          } else if (diffHours <= 24) {
            editForm.value.expiryTime = "24";
          } else if (diffHours <= 168) {
            editForm.value.expiryTime = "168";
          } else if (diffHours <= 720) {
            editForm.value.expiryTime = "720";
          } else {
            editForm.value.expiryTime = "0"; // 设置为永不过期
          }
        } else {
          editForm.value.expiryTime = "0"; // 永不过期
        }

        // 最大查看次数
        editForm.value.maxViews = newPaste.maxViews || 0;
      }
    },
    { immediate: true }
);

// 初始化Vditor编辑器，配置主题、工具栏等选项
const initEditor = () => {
  if (vditorInstance.value) return;

  const editorElement = document.getElementById("vditor-editor");
  if (!editorElement) {
    console.error("编辑器元素不存在");
    return;
  }

  // 创建并配置Vditor实例
  vditorInstance.value = new Vditor("vditor-editor", {
    height: 500,
    minHeight: 400,
    value: props.content, // 设置初始内容
    theme: props.darkMode ? "dark" : "classic", // 根据主题设置
    mode: "ir", // 即时渲染模式，兼顾编辑体验和所见即所得
    cdn: "https://cdn.jsdelivr.net/npm/vditor@3.10.9", // 添加CDN配置，确保资源正确加载
    resize: {
      enable: true,
      position: "bottom", // 只允许底部拖动
    },
    preview: {
      theme: {
        current: props.darkMode ? "dark" : "light",
      },
      hljs: {
        style: props.darkMode ? "vs2015" : "github",
        lineNumber: true,
      },
      actions: ["desktop", "tablet", "mobile", "both"],
      markdown: {
        toc: true, // 启用目录
        mark: true, // 启用标记
        footnotes: true, // 启用脚注
        autoSpace: true, // 自动空格
        media: true, // 启用媒体链接解析
        listStyle: true, // 确保开启列表样式
        task: true, // 启用任务列表交互
        paragraphBeginningSpace: true, // 段落开头空格支持
        fixTermTypo: true, // 术语修正
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
        engine: "KaTeX", // 数学公式引擎
        inlineDigit: true,
      },
    },
    typewriterMode: true, // 启用打字机模式，光标总在屏幕中间
    outline: {
      enable: false, // 默认关闭大纲，避免与自定义大纲冲突
      position: "left",
    },
    counter: {
      enable: true, // 启用计数器
      type: "text", // 文本类型计数
    },
    tab: "\t", // 按Tab键时插入制表符而非缩进
    indent: {
      tab: "\t", // 使用制表符进行缩进
      codeBlock: 4, // 代码块的缩进为4个空格
    },
    hint: {
      emoji: {
        // 表情符号 - 基本表情
        slight_smile: "🙂",
        smile: "😊",
        joy: "😂",
        rofl: "🤣",
        laughing: "😆",
        wink: "😉",
        blush: "😊",
        heart_eyes: "😍",
        kissing_heart: "😘",
        kissing: "😗",
        kissing_smiling_eyes: "😙",
        kissing_closed_eyes: "😚",
        yum: "😋",
        stuck_out_tongue: "😛",
        stuck_out_tongue_winking_eye: "😜",
        stuck_out_tongue_closed_eyes: "😝",
        grin: "😁",
        satisfied: "😌",
        sweat_smile: "😅",

        // 情绪表情
        thinking: "🤔",
        confused: "😕",
        worried: "😟",
        frowning: "😦",
        persevere: "😣",
        confounded: "😖",
        tired_face: "😫",
        weary: "😩",
        cry: "😢",
        sob: "😭",
        angry: "😠",
        rage: "😡",
        triumph: "😤",
        sleepy: "😪",
        yawning: "🥱",
        mask: "😷",
        sunglasses: "😎",
        dizzy_face: "😵",
        exploding_head: "🤯",
        flushed: "😳",

        // 手势表情
        thumbsup: "👍",
        thumbsdown: "👎",
        ok_hand: "👌",
        punch: "👊",
        fist: "✊",
        v: "✌️",
        wave: "👋",
        raised_hand: "✋",
        clap: "👏",
        muscle: "💪",
        pray: "🙏",
        point_up: "☝️",
        point_down: "👇",
        point_left: "👈",
        point_right: "👉",

        // 心形表情
        heart: "❤️",
        orange_heart: "🧡",
        yellow_heart: "💛",
        green_heart: "💚",
        blue_heart: "💙",
        purple_heart: "💜",
        black_heart: "🖤",
        broken_heart: "💔",
        sparkling_heart: "💖",
        heartbeat: "💓",
        heartpulse: "💗",

        // 动物表情
        dog: "🐶",
        cat: "🐱",
        mouse: "🐭",
        hamster: "🐹",
        rabbit: "🐰",
        fox: "🦊",
        bear: "🐻",
        panda: "🐼",
        koala: "🐨",
        tiger: "🐯",
        lion: "🦁",

        // 食物表情
        apple: "🍎",
        pizza: "🍕",
        hamburger: "🍔",
        fries: "🍟",
        sushi: "🍣",
        ramen: "🍜",
        doughnut: "🍩",
        cake: "🍰",
        coffee: "☕",
        beer: "🍺",

        // 活动表情
        soccer: "⚽",
        basketball: "🏀",
        football: "🏈",
        baseball: "⚾",
        tennis: "🎾",

        // 物体表情
        gift: "🎁",
        book: "📚",
        computer: "💻",
        bulb: "💡",
        rocket: "🚀",
        hourglass: "⌛",
        watch: "⌚",
        moneybag: "💰",

        // 符号表情
        check: "✅",
        x: "❌",
        warning: "⚠️",
        question: "❓",
        exclamation: "❗",
        star: "⭐",
        sparkles: "✨",
        fire: "🔥",
        zap: "⚡",
      },
    },
    // 配置工具栏按钮
    toolbar: [
      "emoji",
      "headings",
      "bold",
      "italic",
      "strike",
      "link",
      "|",
      "list",
      "ordered-list",
      "check",
      "outdent",
      "indent",
      "|",
      "quote",
      "line",
      "code",
      "inline-code",
      "insert-before",
      "insert-after",
      "|",
      "table",
      "|",
      "undo",
      "redo",
      "|",
      {
        name: "copy-formats",
        icon: '<svg viewBox="0 0 24 24" width="16" height="16" xmlns="http://www.w3.org/2000/svg"><path fill="currentColor" d="M19,21H8V7H19M19,5H8A2,2 0 0,0 6,7V21A2,2 0 0,0 8,23H19A2,2 0 0,0 21,21V7A2,2 0 0,0 19,5M16,1H4A2,2 0 0,0 2,3V17H4V3H16V1Z"></path></svg>',
        tip: "复制为其他格式",
        click() {
          showCopyFormatsMenu();
        },
      },
      "|",
      "fullscreen",
      "outline", // 保留大纲按钮，用户可以手动开启
      "edit-mode",
      "both",
      "preview",
      "export",
      "help",
    ],
    cache: {
      enable: false, // 禁用缓存，避免数据混乱
    },
    after: () => {
      debugLog(props.enableDebug, props.isDev, "编辑器初始化完成");

      // 添加一个延迟，确保所有图表渲染完成后应用固定样式
      setTimeout(() => {
        // 添加固定样式类到所有图表容器
        const diagramContainers = document.querySelectorAll(".language-mermaid, .language-flow, .language-plantuml, .language-gantt");
        diagramContainers.forEach((container) => {
          container.classList.add("diagram-fixed-theme");
        });
      }, 1000);
    },
  });
};

// 监听暗色模式变化，实时更新编辑器主题
watch(
    () => props.darkMode,
    (newDarkMode) => {
      if (vditorInstance.value) {
        vditorInstance.value.setTheme(newDarkMode ? "dark" : "classic", newDarkMode ? "dark" : "light");
      }
    }
);

// 保存编辑内容，收集所有表单数据并触发保存事件
const saveEdit = async () => {
  if (!vditorInstance.value) return;

  // 获取编辑器当前内容
  const newContent = vditorInstance.value.getValue();
  // 检查文本内容是否为空
  if (!newContent.trim()) {
    emit("update:error", "内容不能为空");
    return;
  }

  // 准备更新数据对象，包含内容和元数据
  const updateData = {
    content: newContent,
    remark: editForm.value.remark || null,
    maxViews: editForm.value.maxViews === 0 ? null : parseInt(editForm.value.maxViews),
  };

  // 处理密码
  if (editForm.value.password.trim()) {
    updateData.password = editForm.value.password;
  } else if (editForm.value.clearPassword) {
    updateData.clearPassword = true;
  }

  // 处理过期时间 - 将选择值转换为ISO日期
  if (editForm.value.expiryTime !== "0") {
    const hours = parseInt(editForm.value.expiryTime);
    const expiresAt = new Date();
    expiresAt.setHours(expiresAt.getHours() + hours);
    updateData.expiresAt = expiresAt.toISOString();
  } else {
    updateData.expiresAt = null; // 永不过期
  }

  // 触发保存事件，将数据传递给父组件
  emit("save", updateData);
};

// 取消编辑，返回到预览模式
const cancelEdit = () => {
  emit("cancel");
};

// 验证可打开次数输入，确保输入合法
const validateMaxViews = (event) => {
  const value = editForm.value.maxViews;

  // 如果是负数，则设置为0
  if (value < 0) {
    editForm.value.maxViews = 0;
    return;
  }

  // 确保值为有效数字
  if (isNaN(value) || value === "") {
    editForm.value.maxViews = 0;
  } else {
    // 确保是整数
    editForm.value.maxViews = parseInt(value);
  }
};

// 获取当前编辑内容的辅助方法
const getCurrentContent = () => {
  if (vditorInstance.value) {
    return vditorInstance.value.getValue();
  }
  return props.content;
};

// 暴露方法供父组件调用
defineExpose({
  getCurrentContent,
});

// 组件挂载时初始化编辑器
onMounted(() => {
  initEditor();

  // 添加全局点击事件监听器
  document.addEventListener("click", handleGlobalClick);

  // 添加窗口大小调整事件监听器
  window.addEventListener("resize", updateCopyFormatMenuPosition);

  // 添加编辑器容器滚动事件监听器
  setTimeout(() => {
    const editorContainer = document.querySelector(".vditor-content");
    if (editorContainer) {
      editorContainer.addEventListener("scroll", updateCopyFormatMenuPosition);
    }
  }, 1000); // 给编辑器足够的初始化时间
});

// 组件卸载时销毁编辑器实例，避免内存泄漏
onBeforeUnmount(() => {
  if (vditorInstance.value) {
    vditorInstance.value.destroy();
    vditorInstance.value = null;
  }

  // 移除全局点击事件监听器
  document.removeEventListener("click", handleGlobalClick);

  // 移除窗口大小调整事件监听器
  window.removeEventListener("resize", updateCopyFormatMenuPosition);

  // 移除编辑器容器滚动事件监听器
  const editorContainer = document.querySelector(".vditor-content");
  if (editorContainer) {
    editorContainer.removeEventListener("scroll", updateCopyFormatMenuPosition);
  }
});

// 显示复制格式菜单
const showCopyFormatsMenu = () => {
  if (!vditorInstance.value) return;

  // 获取工具栏中复制格式按钮的位置
  const copyFormatBtn = document.querySelector('.vditor-toolbar button[data-type="copy-formats"]');
  if (!copyFormatBtn) return;

  // 保存按钮元素引用
  lastCopyFormatsBtnElement.value = copyFormatBtn;

  const rect = copyFormatBtn.getBoundingClientRect();

  // 设置菜单位置
  copyFormatMenuPosition.value = {
    x: rect.left,
    y: rect.bottom + 5,
  };

  // 显示菜单
  copyFormatMenuVisible.value = true;

  // 添加点击事件监听器，点击外部区域关闭菜单
  setTimeout(() => {
    document.addEventListener("click", closeCopyFormatMenu);
  }, 0);
};

// 更新复制格式菜单位置（当窗口调整大小时）
const updateCopyFormatMenuPosition = () => {
  if (!copyFormatMenuVisible.value) return;

  // 如果按钮引用无效，尝试重新获取
  if (!lastCopyFormatsBtnElement.value || !document.body.contains(lastCopyFormatsBtnElement.value)) {
    const newBtn = document.querySelector('.vditor-toolbar button[data-type="copy-formats"]');
    if (!newBtn) {
      // 如果找不到按钮，隐藏菜单并返回
      copyFormatMenuVisible.value = false;
      return;
    }
    lastCopyFormatsBtnElement.value = newBtn;
  }

  const rect = lastCopyFormatsBtnElement.value.getBoundingClientRect();

  // 更新菜单位置
  copyFormatMenuPosition.value = {
    x: rect.left,
    y: rect.bottom + 5,
  };
};

// 关闭复制格式菜单
const closeCopyFormatMenu = (event) => {
  // 如果没有传入event（如从复制函数中直接调用）
  if (!event) {
    copyFormatMenuVisible.value = false;
    document.removeEventListener("click", closeCopyFormatMenu);
    return;
  }

  const menu = document.getElementById("copyFormatMenu");
  // 更新选择器以匹配自定义按钮
  const copyFormatBtn = document.querySelector('.vditor-toolbar button[data-type="copy-formats"]')?.parentElement;

  if (menu && !menu.contains(event.target) && (!copyFormatBtn || !copyFormatBtn.contains(event.target))) {
    copyFormatMenuVisible.value = false;
    document.removeEventListener("click", closeCopyFormatMenu);
  }
};

// 复制为Markdown格式
const copyAsMarkdown = () => {
  if (!vditorInstance.value) return;
  const mdContent = vditorInstance.value.getValue();
  copyToClipboard(mdContent, "已复制为Markdown格式");
  closeCopyFormatMenu();
};

// 复制为HTML格式
const copyAsHTML = () => {
  if (!vditorInstance.value) return;
  const htmlContent = vditorInstance.value.getHTML();
  copyToClipboard(htmlContent, "已复制为HTML格式");
  closeCopyFormatMenu();
};

// 复制为纯文本格式
const copyAsPlainText = () => {
  if (!vditorInstance.value) return;
  const htmlContent = vditorInstance.value.getHTML();
  // 创建一个临时元素来去除HTML标签
  const tempDiv = document.createElement("div");
  tempDiv.innerHTML = htmlContent;
  const plainText = tempDiv.textContent || tempDiv.innerText || "";
  copyToClipboard(plainText, "已复制为纯文本格式");
  closeCopyFormatMenu();
};

// 导出为Word文档
const exportWordDocument = async () => {
  if (!vditorInstance.value) return;

  // 显示状态消息
  notification.value = "正在生成Word文档...";

  try {
    // 获取Markdown内容
    const markdownContent = vditorInstance.value.getValue();

    if (!markdownContent) {
      notification.value = "没有内容可导出";
      setTimeout(() => {
        notification.value = "";
      }, 3000);
      return;
    }

    // 使用文档标题（如果有）或默认名称
    const docTitle = props.paste?.remark || "Markdown导出文档";

    // 使用服务转换成Word文档
    const blob = await markdownToWord(markdownContent, {
      title: docTitle,
    });

    // 生成文件名 - 使用日期和时间
    const now = new Date();
    const dateStr = now.toISOString().slice(0, 10);
    const timeStr = now.toTimeString().slice(0, 8).replace(/:/g, "-");
    const fileName = `markdown-${dateStr}-${timeStr}.docx`;

    // 使用file-saver保存文件
    saveAs(blob, fileName);

    // 显示成功消息
    notification.value = "Word文档已生成并下载";
    setTimeout(() => {
      notification.value = "";
    }, 3000);
  } catch (error) {
    console.error("导出Word文档时出错:", error);
    notification.value = "导出失败，请稍后重试";
    setTimeout(() => {
      notification.value = "";
    }, 3000);
  } finally {
    closeCopyFormatMenu();
  }
};

// 通用复制到剪贴板函数
const copyToClipboard = (text, successMessage) => {
  if (!text) {
    emit("update:error", "没有可复制的内容");
    return;
  }

  try {
    navigator.clipboard
        .writeText(text)
        .then(() => {
          // 使用notification显示成功消息，而非error
          notification.value = successMessage;
          setTimeout(() => {
            notification.value = "";
          }, 3000);
        })
        .catch((err) => {
          console.error("复制失败:", err);
          emit("update:error", "复制失败，请手动选择内容复制");
          setTimeout(() => {
            emit("update:error", "");
          }, 3000);
        });
  } catch (e) {
    console.error("复制API不可用:", e);
    // 降级方案：创建临时文本区域
    const textarea = document.createElement("textarea");
    textarea.value = text;
    textarea.style.position = "fixed";
    textarea.style.opacity = "0";
    document.body.appendChild(textarea);
    textarea.select();
    try {
      document.execCommand("copy");
      // 使用notification显示成功消息，而非error
      notification.value = successMessage;
      setTimeout(() => {
        notification.value = "";
      }, 3000);
    } catch (err) {
      console.error("复制失败:", err);
      emit("update:error", "复制失败，请手动选择内容复制");
      setTimeout(() => {
        emit("update:error", "");
      }, 3000);
    }
    document.body.removeChild(textarea);
  }
};

const handleGlobalClick = (event) => {
  // 如果点击事件不是来自复制格式菜单，并且复制格式菜单可见，则关闭菜单
  const menu = document.getElementById("copyFormatMenu");
  if (
      menu &&
      !menu.contains(event.target) &&
      // 更新选择器以匹配自定义按钮
      !event.target.closest('.vditor-toolbar button[data-type="copy-formats"]') &&
      copyFormatMenuVisible.value
  ) {
    closeCopyFormatMenu();
  }
};
</script>

<template>
  <div class="paste-view-editor">
    <!-- 成功通知提示 -->
    <div v-if="notification" class="fixed bottom-4 right-4 z-50 px-4 py-2 rounded-lg bg-green-500 text-white shadow-lg notification-toast flex items-center">
      <svg class="w-5 h-5 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg">
        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7"></path>
      </svg>
      <span>{{ notification }}</span>
    </div>

    <div class="editor-wrapper">
      <!-- 编辑器区域 - Vditor实例将挂载到这个div -->
      <div class="flex flex-col gap-4">
        <!-- Markdown编辑器容器 -->
        <div id="vditor-editor" class="w-full"></div>
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
          <p class="mt-1 text-xs" :class="darkMode ? 'text-gray-500' : 'text-gray-400'">后缀不可修改，仅支持字母、-和_</p>
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

    <!-- 复制格式菜单 -->
    <div
        v-if="copyFormatMenuVisible"
        id="copyFormatMenu"
        class="absolute bg-white dark:bg-gray-800 rounded-lg shadow-lg py-1 z-50 border border-gray-200 dark:border-gray-700"
        :style="{ top: `${copyFormatMenuPosition.y}px`, left: `${copyFormatMenuPosition.x}px` }"
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
    </div>
  </div>
</template>

<style scoped>
/* 编辑器容器样式 */
.paste-view-editor {
  width: 100%;
}

.editor-wrapper {
  width: 100%;
}

/* 编辑器容器适应屏幕宽度，添加边框和圆角 */
:deep(#vditor-editor) {
  width: 100%;
  border: 1px solid v-bind('props.darkMode ? "#4B5563" : "#E5E7EB"');
  border-radius: 0.5rem;
  margin-bottom: 1rem;
  overflow: hidden;
}

/* 工具栏样式调整，适应明暗主题 */
:deep(.vditor-toolbar) {
  background-color: v-bind('props.darkMode ? "#374151" : "#F9FAFB"');
  border-bottom: 1px solid v-bind('props.darkMode ? "#4B5563" : "#E5E7EB"');
}

:deep(.vditor-toolbar__item) {
  color: v-bind('props.darkMode ? "#E5E7EB" : "#4B5563"');
}

:deep(.vditor-toolbar__item--current) {
  background-color: v-bind('props.darkMode ? "#1F2937" : "#F3F4F6"');
}

:deep(.vditor-toolbar__item:hover) {
  background-color: v-bind('props.darkMode ? "#4B5563" : "#E5E7EB"');
}

/* 表单和按钮通用样式 */
.form-input {
  display: block;
  width: 100%;
  padding: 0.5rem 0.75rem;
  font-size: 0.875rem;
  line-height: 1.25rem;
  border-width: 1px;
  border-radius: 0.375rem;
  transition: all 0.2s;
}

/* 主按钮样式 */
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

/* 编辑器样式定制 */
:deep(.vditor) {
  border-radius: 0.375rem;
  /* 移除整体过渡效果，避免拖动时的延迟感 */
  transition: border-color 0.2s, background-color 0.2s;
}

/* 确保编辑器图表有固定样式 */
:deep(.diagram-fixed-theme) {
  background-color: white !important; /* 强制使用白色背景 */
  color: #333 !important; /* 强制使用深色文本 */
  filter: none !important; /* 移除任何可能的过滤器效果 */
}

/* 编辑器高度调整拖动区域样式 */
:deep(.vditor-resize) {
  padding: 3px 0;
  cursor: row-resize;
  user-select: none;
  position: absolute;
  width: 100%;
  z-index: 20; /* 提高z-index确保在最上层 */
  transition: none; /* 移除过渡动画，避免拖动时的延迟感 */
}

:deep(.vditor-resize > div) {
  height: 3px;
  background-color: v-bind('props.darkMode ? "#3f3f3f" : "#e5e7eb"');
  border-radius: 3px;
  transition: background-color 0.15s; /* 只保留背景色的过渡效果 */
}

:deep(.vditor-resize:hover > div) {
  background-color: v-bind('props.darkMode ? "#007acc" : "#d1d5db"');
}

:deep(.vditor-resize:active > div) {
  background-color: v-bind('props.darkMode ? "#2563eb" : "#3b82f6"');
}

/* 移动端响应式优化 */
@media (max-width: 640px) {
  /* 移动设备下编辑器工具栏精简 */
  :deep(.vditor-toolbar) {
    overflow-x: auto;
    white-space: nowrap;
    padding: 0.25rem;
  }
}

/* 编辑器样式定制 */
:deep(.vditor) {
  border-radius: 0.375rem;
  /* 移除整体过渡效果，避免拖动时的延迟感 */
  transition: border-color 0.2s, background-color 0.2s;
}

/* 确保编辑器图表有固定样式 */
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

/* 添加多级列表样式支持 */
/* 有序列表样式 */
:deep(.vditor-reset ol) {
  list-style-type: decimal;
  padding-left: 2em;
}

:deep(.vditor-reset ol ol) {
  list-style-type: decimal;
}

:deep(.vditor-reset ol ol ol) {
  list-style-type: decimal;
}

/* 无序列表样式 */
:deep(.vditor-reset ul) {
  list-style-type: disc;
  padding-left: 2em;
}

:deep(.vditor-reset ul ul) {
  list-style-type: circle;
}

:deep(.vditor-reset ul ul ul) {
  list-style-type: square;
}

/* 预览模式列表样式 */
:deep(.vditor-preview ol) {
  list-style-type: decimal;
  padding-left: 2em;
}

:deep(.vditor-preview ol ol) {
  list-style-type: decimal;
}

:deep(.vditor-preview ol ol ol) {
  list-style-type: decimal;
}

:deep(.vditor-preview ul) {
  list-style-type: disc;
  padding-left: 2em;
}

:deep(.vditor-preview ul ul) {
  list-style-type: circle;
}

:deep(.vditor-preview ul ul ul) {
  list-style-type: square;
}

/* 确保即时渲染模式的列表也正确显示 */
:deep(.vditor-ir ol) {
  list-style-type: decimal;
  padding-left: 2em;
}

:deep(.vditor-ir ol ol) {
  list-style-type: decimal;
}

:deep(.vditor-ir ol ol ol) {
  list-style-type: decimal;
}

:deep(.vditor-ir ul) {
  list-style-type: disc;
  padding-left: 2em;
}

:deep(.vditor-ir ul ul) {
  list-style-type: circle;
}

:deep(.vditor-ir ul ul ul) {
  list-style-type: square;
}

/* 制表符样式支持 */
:deep(.vditor-reset) {
  tab-size: 4;
  -moz-tab-size: 4;
}

:deep(.vditor-ir) {
  tab-size: 4;
  -moz-tab-size: 4;
}

:deep(.vditor-sv) {
  tab-size: 4;
  -moz-tab-size: 4;
}

:deep(.vditor-wysiwyg) {
  tab-size: 4;
  -moz-tab-size: 4;
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

/* 通知提示动画 */
@keyframes fadeInOut {
  0% {
    opacity: 0;
    transform: translateY(-10px);
  }
  10% {
    opacity: 1;
    transform: translateY(0);
  }
  90% {
    opacity: 1;
    transform: translateY(0);
  }
  100% {
    opacity: 0;
    transform: translateY(-10px);
  }
}

.notification-toast {
  animation: fadeInOut 3s ease-in-out forwards;
  box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06);
}

/* 添加多级列表样式支持 */
/* 有序列表样式 */
</style>
