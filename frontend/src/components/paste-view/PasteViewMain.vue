<script setup>
// PasteViewMain组件 - 主组件，整合各个功能模块
// 负责协调预览、大纲和编辑功能，管理全局状态和数据流
import { ref, computed, onMounted, onBeforeUnmount, watch } from "vue";

import PasteViewPreview from "./PasteViewPreview.vue";
import PasteViewOutline from "./PasteViewOutline.vue";
import PasteViewEditor from "./PasteViewEditor.vue";
import { formatExpiry, debugLog } from "./PasteViewUtils";
import { isExpired } from "@/utils/timeUtils.js";
import { api } from "@/api";
import { ApiStatus } from "../../api/ApiStatus";
import { copyToClipboard } from "@/utils/clipboard";
import { useAuthStore } from "@/stores/authStore.js";

// 定义环境变量
const isDev = import.meta.env.DEV;
// 是否启用调试 - 默认禁用，即使在开发环境
const enableDebug = ref(false);
// 开发模式下的特殊选项 - 强制显示编辑按钮用于测试
const forceShowEditButton = ref(false);

// 接收父组件传递的属性
const props = defineProps({
  // 是否为暗色模式
  darkMode: {
    type: Boolean,
    required: true,
  },
  // 文本分享的唯一标识符
  slug: {
    type: String,
    required: true,
  },
});

// 主要状态变量
const paste = ref(null); // 文本分享数据
const loading = ref(true); // 加载状态
const error = ref(""); // 错误信息
const passwordInput = ref(""); // 密码输入框的值
const needPassword = ref(false); // 是否需要密码
const showPassword = ref(false); // 是否显示密码
let mounted = false; // 组件是否已挂载
const previewRef = ref(null); // 预览组件引用
const editContent = ref(""); // 编辑内容

// 视图模式设置
const viewMode = ref("preview"); // 'preview', 'outline', 'edit'
// 纯文本模式标志 - 控制是否显示为纯文本而非渲染的Markdown
const isPlainTextMode = ref(false);

// 使用认证Store
const authStore = useAuthStore();

// 从Store获取权限状态的计算属性
const isAdmin = computed(() => authStore.isAdmin);
const hasApiKey = computed(() => authStore.authType === "apikey" && !!authStore.apiKey);
const hasTextPermission = computed(() => authStore.hasTextPermission);
const isCreator = ref(false); // 这个需要根据具体文本分享的创建者信息判断
// 大纲相关数据
const outlineData = ref([]); // 扁平结构大纲数据
const outlineTreeData = ref([]); // 树形结构大纲数据

// 标记是否通过取消按钮退出编辑模式
const isCancelling = ref(false);

// 权限检查逻辑已移至认证Store，这里只需要检查创建者状态

// 检查创建者状态
const checkCreatorStatus = () => {
  // 检查是否是创建者 - 如果文本分享已加载
  if (paste.value && hasApiKey.value) {
    try {
      debugLog(enableDebug.value, isDev, "检查API密钥用户是否为创建者，文本created_by:", paste.value.created_by);

      if (!paste.value.created_by) {
        debugLog(enableDebug.value, isDev, "文本没有创建者信息");
        isCreator.value = false;
        return;
      }

      // 从认证Store获取API密钥信息
      const keyInfo = authStore.apiKeyInfo;
      if (keyInfo) {
        debugLog(enableDebug.value, isDev, "API密钥信息:", keyInfo);

        // 处理created_by字段，后端返回的格式是"apikey:密钥ID"
        const createdBy = paste.value.created_by;

        // 如果created_by以"apikey:"开头，提取实际的ID部分
        if (typeof createdBy === "string" && createdBy.startsWith("apikey:")) {
          const actualKeyId = createdBy.substring(7); // 移除"apikey:"前缀
          isCreator.value = keyInfo.id === actualKeyId;
          debugLog(enableDebug.value, isDev, `比较创建者ID "${actualKeyId}" 与当前API密钥ID "${keyInfo.id}": ${isCreator.value ? "匹配" : "不匹配"}`);
        } else {
          // 直接比较(兼容其他格式)
          isCreator.value = keyInfo.id === createdBy;
          debugLog(enableDebug.value, isDev, `直接比较创建者 "${createdBy}" 与API密钥ID "${keyInfo.id}": ${isCreator.value ? "匹配" : "不匹配"}`);
        }
        return;
      }

      // 如果没有密钥信息，但有文本权限，放宽条件
      if (hasTextPermission.value) {
        debugLog(enableDebug.value, isDev, "尝试使用API密钥直接验证创建者身份");

        const createdBy = paste.value.created_by;
        if (typeof createdBy === "string" && createdBy.startsWith("apikey:")) {
          // 没有足够信息时，简单假设有权限的API密钥用户可以编辑API密钥创建的内容
          isCreator.value = true;
          debugLog(enableDebug.value, isDev, "放宽条件: 允许API密钥用户编辑任何API密钥创建的内容");
        } else {
          isCreator.value = false;
        }
      } else {
        isCreator.value = false;
      }
    } catch (e) {
      console.error("检查创建者状态失败:", e);
      isCreator.value = false;
    }
  } else {
    isCreator.value = false;
  }

  debugLog(enableDebug.value, isDev, "API密钥用户最终状态:", {
    hasApiKey: hasApiKey.value,
    hasTextPermission: hasTextPermission.value,
    isCreator: isCreator.value,
  });
};

// 监听slug的变化，当它变化时重新加载内容
watch(
  () => props.slug,
  (newSlug) => {
    if (mounted && newSlug) {
      debugLog(enableDebug.value, isDev, "PasteView: 检测到slug变化，重新加载", newSlug);
      loadPaste();
    }
  }
);

// 获取文本分享内容 - 主要数据加载函数
const loadPaste = async (password = null) => {
  if (!mounted) return;

  loading.value = true;
  error.value = "";

  try {
    debugLog(enableDebug.value, isDev, "PasteView: 开始加载内容", props.slug);
    // 调用API获取文本分享数据
    const result = await api.paste.getPaste(props.slug, password);
    paste.value = result;

    // 检查返回的完整数据用于调试
    debugLog(enableDebug.value, isDev, "文本分享加载成功", {
      slug: result.slug,
      hasPassword: result.hasPassword,
      created_by: result.created_by || "无",
      contentLength: result.content?.length || 0,
    });

    if (!result.created_by) {
      debugLog(enableDebug.value, isDev, "警告: 文本分享没有创建者信息");
    }

    // 检查是否需要密码访问
    if (result.requiresPassword) {
      needPassword.value = true;
      loading.value = false;
      return;
    }

    needPassword.value = false;

    // 保存编辑内容原始值，用于编辑模式
    editContent.value = result.content || "";

    // 如果需要重新验证认证状态，则进行验证
    if (authStore.needsRevalidation) {
      console.log("PasteViewMain: 需要重新验证认证状态");
      await authStore.validateAuth();
    }

    // 由于文本加载完成，此时可以正确检查创建者状态
    checkCreatorStatus();
  } catch (err) {
    console.error("获取文本分享失败:", err);
    // 优先使用HTTP状态码判断错误类型，更可靠
    if (err.status === ApiStatus.UNAUTHORIZED || err.response?.status === ApiStatus.UNAUTHORIZED || err.code === ApiStatus.UNAUTHORIZED) {
      // 401 Unauthorized - 密码错误
      error.value = "密码验证失败，请重试";
      // 确保密码输入框仍然显示
      needPassword.value = true;
    } else if (err.status === ApiStatus.GONE || err.response?.status === ApiStatus.GONE || err.code === ApiStatus.GONE) {
      // 410 Gone - 资源已过期或达到最大查看次数
      error.value = "此文本分享已不可访问：达到最大查看次数或已过期";
      needPassword.value = false;
    } else if (err.status === ApiStatus.NOT_FOUND || err.response?.status === ApiStatus.NOT_FOUND || err.code === ApiStatus.NOT_FOUND) {
      // 404 Not Found - 资源不存在
      error.value = "此文本分享不存在或已被删除";
      needPassword.value = false;
    } else {
      // 后备判断：基于错误消息内容判断错误类型（保持兼容性）
      if (err.message && (err.message.includes("密码错误") || err.message.includes("密码不正确") || err.message.includes("401"))) {
        error.value = "密码验证失败，请重试";
        // 确保密码输入框仍然显示
        needPassword.value = true;
      } else if (err.message && (err.message.includes("文本分享已过期") || err.message.includes("最大查看次数") || err.message.includes("410"))) {
        error.value = "此文本分享已不可访问：达到最大查看次数或已过期";
        needPassword.value = false;
      } else if (err.message && (err.message.includes("找不到") || err.message.includes("不存在") || err.message.includes("404"))) {
        error.value = "此文本分享不存在或已被删除";
        needPassword.value = false;
      } else {
        error.value = err.message || "获取文本分享失败";
        // 除非明确知道是401密码错误，其他错误默认不显示密码输入框
        needPassword.value = false;
      }
    }
  } finally {
    loading.value = false;
  }
};

// 提取文档大纲 - 由预览组件渲染完成后调用
// 分析DOM结构生成大纲数据
const extractOutline = () => {
  // 使用更安全的DOM查询，避免在组件卸载时出错
  if (!mounted) return;

  const previewElement = document.querySelector(".vditor-reset");
  if (!previewElement) return;

  // 获取所有标题元素
  const headings = previewElement.querySelectorAll("h1, h2, h3, h4, h5, h6");
  const outline = [];

  // 构建嵌套大纲结构
  const outlineTree = [];
  let currentParents = []; // 用于存储每个级别的当前父项

  headings.forEach((heading) => {
    // 获取标题级别、文本和ID
    const level = parseInt(heading.tagName.substring(1));
    const text = heading.textContent;
    const id = heading.id;

    // 创建大纲项对象
    const item = {
      level,
      text,
      id,
      children: [],
      expanded: true, // 默认展开
    };

    // 确定此标题在大纲中的位置 - 构建层级关系
    if (level === 1) {
      // 一级标题直接添加到大纲树根
      outlineTree.push(item);
      currentParents = [item]; // 重置所有父级
    } else {
      // 找到当前标题的父标题
      let parentLevel = level - 1;
      while (parentLevel >= 1) {
        if (currentParents[parentLevel - 1]) {
          // 将当前项添加到找到的父项的子项中
          currentParents[parentLevel - 1].children.push(item);
          break;
        }
        parentLevel--;
      }

      // 如果没有找到合适的父级，则添加到根级
      if (parentLevel < 1) {
        outlineTree.push(item);
      }

      // 更新当前级别的父项
      currentParents[level - 1] = item;
      // 清除所有更高级别的父项
      for (let i = level; i < 6; i++) {
        currentParents[i] = null;
      }
    }

    // 同时保留扁平结构用于兼容性
    outline.push(item);
  });

  // 更新大纲数据状态
  outlineData.value = outline;
  outlineTreeData.value = outlineTree;
  debugLog(enableDebug.value, isDev, "提取大纲数据:", outline.length, "项");
};

// 处理预览组件内容渲染完成事件
const handlePreviewRendered = () => {
  extractOutline();
};

// 切换大纲/预览/编辑模式
const switchViewMode = (mode) => {
  // 如果当前是编辑模式，只能通过保存或取消按钮切换到预览模式
  if (viewMode.value === "edit" && mode !== "preview") {
    return;
  }

  if (viewMode.value === mode) return;

  // 从编辑模式切换到预览模式时
  if (viewMode.value === "edit" && mode === "preview") {
    // 如果是点击取消按钮，设置标记
    isCancelling.value = true;
  }

  viewMode.value = mode;
};

// 点击大纲项时滚动到对应位置
const scrollToHeading = (id) => {
  if (!id) return;

  const element = document.getElementById(id);
  if (element) {
    element.scrollIntoView({ behavior: "smooth", block: "start" });
  }
};

// 保存编辑内容 - 调用API更新文本分享
const saveEdit = async (updateData) => {
  if (!paste.value) return;

  loading.value = true;
  error.value = "";

  try {
    // 检查编辑权限
    if (!isAdmin.value && !(hasApiKey.value && hasTextPermission.value && isCreator.value)) {
      throw new Error("您没有编辑权限");
    }

    // 获取文本分享的唯一标识
    const slug = paste.value.slug;
    if (!slug) {
      throw new Error("无法获取文本分享的唯一标识");
    }

    // 检查是否修改了密码
    const passwordChanged = updateData.password || updateData.clearPassword;

    // 使用统一API更新内容（自动根据认证信息处理权限）
    await api.paste.updatePaste(slug, updateData);

    // 更新本地内容状态
    paste.value.content = updateData.content;
    // 保存成功后，更新编辑内容的原始值，这样取消按钮可以恢复到最新保存的内容
    editContent.value = updateData.content;
    paste.value.remark = updateData.remark;
    paste.value.max_views = updateData.max_views;
    paste.value.expires_at = updateData.expires_at;

    // 切换回预览模式
    switchViewMode("preview");

    // 显示成功消息
    error.value = "保存成功";
    setTimeout(() => {
      error.value = "";
    }, 3000);

    // 如果修改了密码，需要重新加载内容以获取新的验证状态
    if (passwordChanged) {
      // 清空密码输入框
      passwordInput.value = "";

      // 显示正在刷新状态
      setTimeout(() => {
        // 清空当前内容，重新加载
        debugLog(enableDebug.value, isDev, "密码已修改，重新加载内容以更新验证状态");
        loadPaste(); // 重新加载会触发密码验证界面（如果需要）
      });
    }
  } catch (err) {
    console.error("保存内容失败:", err);
    error.value = err.message || "保存失败，请重试";
  } finally {
    loading.value = false;
  }
};

// 取消编辑 - 恢复原始内容并返回预览模式
const cancelEdit = () => {
  isCancelling.value = true;
  switchViewMode("preview");
  isCancelling.value = false;
};

// 提交密码 - 处理密码保护的文本分享
const submitPassword = async () => {
  if (!passwordInput.value) {
    error.value = "请输入密码";
    return;
  }

  loading.value = true;
  error.value = "";

  try {
    await loadPaste(passwordInput.value);

    // 如果验证通过（没有触发requiresPassword），清空密码字段
    if (!needPassword.value) {
      passwordInput.value = "";
    }
  } catch (err) {
    // 错误处理已在loadPaste中完成，不需要额外处理
    // 但为了确保界面体验良好，在这里结束loading状态
    loading.value = false;
  }
};

// 复制内容到剪贴板 - 提供两种实现方式以确保兼容性
const copyContentToClipboard = async () => {
  if (!paste.value || !paste.value.content) {
    error.value = "没有可复制的内容";
    return;
  }

  try {
    const success = await copyToClipboard(paste.value.content);

    if (success) {
      error.value = "复制成功：内容已复制到剪贴板";
      setTimeout(() => {
        error.value = "";
      }, 3000);
    } else {
      throw new Error("复制失败");
    }
  } catch (e) {
    console.error("复制失败:", e);
    error.value = "复制失败，请手动选择内容复制";
  }
};

// 复制原始文本链接到剪贴板
const copyRawLink = async () => {
  if (!paste.value || !paste.value.slug) {
    error.value = "没有可复制的原始链接";
    return;
  }

  try {
    // 从getRawPasteUrl获取原始链接
    const rawLink = api.paste.getRawPasteUrl(paste.value.slug, paste.value.plain_password || null);

    // 复制链接到剪贴板
    const success = await copyToClipboard(rawLink);

    if (success) {
      error.value = "复制成功：原始链接已复制到剪贴板";
      setTimeout(() => {
        error.value = "";
      }, 3000);
    } else {
      throw new Error("复制失败");
    }
  } catch (e) {
    console.error("复制失败:", e);
    error.value = "复制失败，请手动复制原始链接";
  }
};

// 切换调试信息显示 - 开发辅助功能
const toggleDebug = () => {
  enableDebug.value = !enableDebug.value;
  debugLog(true, isDev, enableDebug.value ? "调试模式已开启" : "调试模式已关闭");
};

// 切换强制显示编辑按钮选项 - 仅开发模式使用
const toggleForceEditButton = () => {
  if (isDev) {
    forceShowEditButton.value = !forceShowEditButton.value;
    debugLog(true, isDev, forceShowEditButton.value ? "已强制显示编辑按钮" : "已禁用强制显示编辑按钮");
  }
};

// 切换密码可见性
const togglePasswordVisibility = () => {
  showPassword.value = !showPassword.value;
};

// 切换纯文本/Markdown渲染模式
const toggleTextMode = (isPlainText) => {
  isPlainTextMode.value = isPlainText;
  debugLog(enableDebug.value, isDev, isPlainText ? "切换到TXT模式" : "切换到MD渲染模式");
};

// 组件挂载时加载文本分享
onMounted(async () => {
  debugLog(enableDebug.value, isDev, "PasteView: 组件挂载", props.slug);
  mounted = true;

  // 监听认证状态变化事件
  window.addEventListener("auth-state-changed", handleAuthStateChange);

  // 延迟加载以确保DOM已准备就绪
  setTimeout(async () => {
    await loadPaste();
  }, 150);
});

// 处理认证状态变化
const handleAuthStateChange = (event) => {
  debugLog(enableDebug.value, isDev, "PasteViewMain: 认证状态变化", event.detail);
  // 重新检查创建者状态
  checkCreatorStatus();
};

// 组件卸载时清理资源
onBeforeUnmount(() => {
  debugLog(enableDebug.value, isDev, "PasteView: 组件卸载");
  mounted = false;
  paste.value = null;
  // 移除事件监听
  window.removeEventListener("auth-state-changed", handleAuthStateChange);
});
</script>

<template>
  <div class="paste-view max-w-6xl mx-auto px-3 sm:px-6 flex-1 flex flex-col pt-6 sm:pt-8">
    <div class="mb-6">
      <!-- 将原来的大标题替换为更简洁的面包屑样式导航 -->
      <div class="py-3 text-sm text-gray-500 dark:text-gray-400 border-b border-gray-200 dark:border-gray-700 mb-4">
        <a href="/" class="hover:text-primary-600 dark:hover:text-primary-400">首页</a>
        <span class="mx-2">/</span>
        <span class="text-gray-700 dark:text-gray-300">文本分享</span>
      </div>

      <!-- 加载中状态显示 -->
      <div v-if="loading" class="py-16 flex justify-center">
        <svg class="animate-spin h-12 w-12 text-primary-500" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
          <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
          <path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
        </svg>
      </div>

      <!-- 错误信息显示区域 - 仅在有错误且不是密码错误时显示 -->
      <div v-else-if="error && !needPassword && !error.includes('成功')" class="error-container py-12 px-4 max-w-4xl mx-auto text-center">
        <svg xmlns="http://www.w3.org/2000/svg" class="h-16 w-16 mx-auto mb-4 text-red-600 dark:text-red-500" fill="none" viewBox="0 0 24 24" stroke="currentColor">
          <path
            stroke-linecap="round"
            stroke-linejoin="round"
            stroke-width="1.5"
            d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z"
          />
        </svg>
        <h2 class="text-2xl font-bold mb-2 text-gray-900 dark:text-white">文本访问错误</h2>
        <p class="text-lg mb-6 text-gray-600 dark:text-gray-300">{{ error }}</p>
        <a
          href="/"
          class="inline-flex items-center justify-center px-4 py-2 border border-transparent rounded-md shadow-sm text-base font-medium text-white bg-blue-600 hover:bg-blue-700"
        >
          返回首页
        </a>
      </div>

      <!-- 密码输入区域 - 用于访问受密码保护的内容 -->
      <div v-else-if="needPassword" class="py-6 px-4 flex justify-center">
        <div class="max-w-sm w-full p-5 border rounded-lg shadow-sm" :class="darkMode ? 'bg-gray-800 border-gray-700' : 'bg-white border-gray-200'">
          <h3 class="text-lg font-medium mb-4" :class="darkMode ? 'text-white' : 'text-gray-900'">此内容需要密码访问</h3>
          <p class="mb-4 text-sm" :class="darkMode ? 'text-gray-300' : 'text-gray-600'">此文本分享已被密码保护，请输入密码查看内容</p>

          <div class="mb-4">
            <label for="password" class="block text-sm font-medium mb-1" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">密码</label>
            <div class="relative">
              <input
                :type="showPassword ? 'text' : 'password'"
                id="password"
                autocomplete="current-password"
                v-model="passwordInput"
                @keyup.enter="submitPassword"
                class="block w-full px-3 py-2 rounded-md shadow-sm focus:ring-2 focus:ring-primary-500 focus:border-primary-500 password-input"
                :class="darkMode ? 'bg-gray-700 border-gray-600 text-white focus:ring-offset-gray-800' : 'border-gray-300 text-gray-900 focus:ring-offset-white'"
                placeholder="请输入密码"
              />
              <button
                type="button"
                @click="togglePasswordVisibility"
                class="absolute right-3 top-1/2 transform -translate-y-1/2 text-gray-600 dark:text-gray-400 hover:text-gray-800 dark:hover:text-gray-300"
              >
                <svg v-if="showPassword" xmlns="http://www.w3.org/2000/svg" class="h-5 w-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path
                    stroke-linecap="round"
                    stroke-linejoin="round"
                    stroke-width="2"
                    d="M13.875 18.825A10.05 10.05 0 0112 19c-4.478 0-8.268-2.943-9.543-7a9.97 9.97 0 011.563-3.029m5.858.908a3 3 0 114.243 4.243M9.878 9.878l4.242 4.242M9.88 9.88l-3.29-3.29m7.532 7.532l3.29 3.29M3 3l18 18"
                  />
                </svg>
                <svg v-else xmlns="http://www.w3.org/2000/svg" class="h-5 w-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 12a3 3 0 11-6 0 3 3 0 016 0z" />
                  <path
                    stroke-linecap="round"
                    stroke-linejoin="round"
                    stroke-width="2"
                    d="M2.458 12C3.732 7.943 7.523 5 12 5c4.478 0 8.268 2.943 9.542 7-1.274 4.057-5.064 7-9.542 7-4.477 0-8.268-2.943-9.542-7z"
                  />
                </svg>
              </button>
            </div>
            <!-- 显示密码错误消息 -->
            <p v-if="error" class="mt-2 text-sm text-red-500 dark:text-red-400">{{ error }}</p>
          </div>

          <button
            @click="submitPassword"
            class="w-full px-4 py-2 text-sm font-medium text-white bg-primary-600 rounded-md hover:bg-primary-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500"
            :class="darkMode ? 'focus:ring-offset-gray-800' : 'focus:ring-offset-white'"
          >
            提交
          </button>
        </div>
      </div>

      <!-- 成功消息显示 - 独立显示，不影响内容区域 -->
      <div v-if="error && error.includes('成功')" class="mt-4 p-4 rounded-md" :class="darkMode ? 'bg-green-900/30' : 'bg-green-50'">
        <div class="text-sm" :class="darkMode ? 'text-green-200' : 'text-green-700'">{{ error }}</div>
      </div>

      <!-- 文本内容显示区域 - 主要内容容器 -->
      <div v-if="paste && !needPassword && !(error && !error.includes('成功') && !needPassword)" class="mt-6">
        <!-- 元信息显示区域 - 显示过期时间和剩余查看次数 -->
        <div class="mb-6 p-5 border rounded-lg" :class="darkMode ? 'bg-gray-800 border-gray-700' : 'bg-white border-gray-200'">
          <div class="grid grid-cols-1 gap-4 text-sm">
            <div v-if="paste.expires_at">
              <span :class="darkMode ? 'text-gray-400' : 'text-gray-500'">过期时间:</span>
              <span class="ml-2" :class="[darkMode ? 'text-white' : 'text-gray-900', isExpired(paste.expires_at) ? 'text-red-500' : '']">{{ formatExpiry(paste.expires_at) }}</span>
            </div>
            <div v-if="paste.max_views">
              <span :class="darkMode ? 'text-gray-400' : 'text-gray-500'">剩余查看次数:</span>
              <span
                class="ml-2"
                :class="[
                  darkMode ? 'text-white' : 'text-gray-900',
                  paste.max_views - paste.views <= 5 ? 'text-amber-500' : '',
                  paste.max_views - paste.views <= 1 ? 'text-red-500' : '',
                ]"
              >
                {{ paste.max_views - paste.views }}
              </span>
            </div>
          </div>
        </div>

        <!-- 视图模式切换和操作按钮区域 -->
        <div class="mb-4 flex items-center justify-between flex-wrap gap-2">
          <!-- 视图模式切换按钮组 - 在编辑模式下隐藏 -->
          <div v-if="viewMode !== 'edit'" class="flex border rounded-md overflow-hidden" :class="darkMode ? 'border-gray-700' : 'border-gray-200'">
            <button
              @click="switchViewMode('preview')"
              class="px-3 py-1.5 text-sm font-medium"
              :class="[
                viewMode === 'preview'
                  ? darkMode
                    ? 'bg-gray-700 text-white'
                    : 'bg-gray-100 text-gray-900'
                  : darkMode
                  ? 'bg-gray-800 text-gray-300 hover:bg-gray-700'
                  : 'bg-white text-gray-700 hover:bg-gray-50',
              ]"
            >
              预览
            </button>
            <button
              @click="switchViewMode('outline')"
              class="px-3 py-1.5 text-sm font-medium"
              :class="[
                viewMode === 'outline'
                  ? darkMode
                    ? 'bg-gray-700 text-white'
                    : 'bg-gray-100 text-gray-900'
                  : darkMode
                  ? 'bg-gray-800 text-gray-300 hover:bg-gray-700'
                  : 'bg-white text-gray-700 hover:bg-gray-50',
              ]"
            >
              大纲
            </button>
            <!-- 纯文本/Markdown切换按钮组 -->
            <button
              @click="toggleTextMode(false)"
              class="px-3 py-1.5 text-sm font-medium"
              :class="[
                !isPlainTextMode
                  ? darkMode
                    ? 'bg-gray-700 text-white'
                    : 'bg-gray-100 text-gray-900'
                  : darkMode
                  ? 'bg-gray-800 text-gray-300 hover:bg-gray-700'
                  : 'bg-white text-gray-700 hover:bg-gray-50',
              ]"
            >
              MD
            </button>
            <button
              @click="toggleTextMode(true)"
              class="px-3 py-1.5 text-sm font-medium"
              :class="[
                isPlainTextMode
                  ? darkMode
                    ? 'bg-gray-700 text-white'
                    : 'bg-gray-100 text-gray-900'
                  : darkMode
                  ? 'bg-gray-800 text-gray-300 hover:bg-gray-700'
                  : 'bg-white text-gray-700 hover:bg-gray-50',
              ]"
            >
              TXT
            </button>
          </div>
          <!-- 编辑模式下显示固定标题 -->
          <div v-else class="text-sm font-medium" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">编辑模式 - 请使用下方按钮保存或取消</div>

          <!-- 管理员编辑按钮和复制按钮 - 在编辑模式下隐藏 -->
          <div v-if="viewMode !== 'edit'" class="flex items-center gap-2">
            <!-- 复制内容按钮 -->
            <button
              v-if="paste && paste.content"
              @click="copyContentToClipboard"
              class="px-4 py-1.5 text-sm font-medium border rounded-md"
              :class="darkMode ? 'border-gray-600 text-gray-300 hover:bg-gray-800' : 'border-gray-300 text-gray-700 hover:bg-gray-50'"
            >
              复制内容
            </button>

            <!-- 复制原始链接按钮 -->
            <button
              v-if="paste && paste.slug"
              @click="copyRawLink"
              class="px-4 py-1.5 text-sm font-medium border rounded-md"
              :class="darkMode ? 'border-gray-600 text-gray-300 hover:bg-gray-800' : 'border-gray-300 text-gray-700 hover:bg-gray-50'"
            >
              raw
            </button>

            <!-- 编辑按钮 - 管理员和有权限的API密钥创建者可见 -->
            <button
              v-if="isAdmin || (hasApiKey && hasTextPermission && isCreator) || (isDev && forceShowEditButton)"
              @click="switchViewMode('edit')"
              class="px-4 py-1.5 text-sm font-medium border rounded-md"
              :class="darkMode ? 'border-gray-600 text-gray-300 hover:bg-gray-800' : 'border-gray-300 text-gray-700 hover:bg-gray-50'"
            >
              编辑内容
            </button>
          </div>

          <!-- 调试按钮 (仅在开发模式显示) -->
          <div v-if="isDev" class="ml-auto">
            <button
              @click="toggleDebug"
              class="text-xs px-2 py-1 mr-2 rounded transition-colors"
              :class="enableDebug ? 'bg-yellow-500 text-white' : 'bg-gray-200 dark:bg-gray-700 text-gray-700 dark:text-gray-300'"
            >
              {{ enableDebug ? "隐藏调试" : "调试" }}
            </button>
            <button
              @click="toggleForceEditButton"
              class="text-xs px-2 py-1 rounded transition-colors"
              :class="forceShowEditButton ? 'bg-pink-500 text-white' : 'bg-gray-200 dark:bg-gray-700 text-gray-700 dark:text-gray-300'"
            >
              {{ forceShowEditButton ? "取消强制编辑" : "强制编辑" }}
            </button>
          </div>
        </div>

        <!-- 调试信息区域 (仅在明确启用调试时显示) -->
        <div v-if="enableDebug && paste && paste.content" class="mb-4 p-3 bg-yellow-100 dark:bg-yellow-900/30 text-xs rounded">
          <details open>
            <summary class="cursor-pointer text-yellow-700 dark:text-yellow-300">调试信息</summary>
            <div class="mt-2 whitespace-pre-wrap text-gray-700 dark:text-gray-300 break-all">
              <div>内容长度: {{ paste.content?.length || 0 }}字符</div>
              <div>预览组件: {{ previewRef ? "已引用" : "未引用" }}</div>
              <div>大纲项数: {{ outlineData.length }}</div>
              <div>是否管理员: {{ isAdmin ? "是" : "否" }}</div>
              <div>是否有API密钥: {{ hasApiKey ? "是" : "否" }}</div>
              <div>是否有文本权限: {{ hasTextPermission ? "是" : "否" }}</div>
              <div>是否为创建者: {{ isCreator ? "是" : "否" }}</div>
              <div>强制显示编辑按钮: {{ forceShowEditButton ? "是" : "否" }}</div>
              <div>当前模式: {{ viewMode }}</div>
              <div>显示格式: {{ isPlainTextMode ? "TXT模式" : "MD渲染模式" }}</div>
              <div>内容前20字符: {{ paste.content?.substring(0, 20) }}...</div>
              <div>创建者信息: {{ paste.created_by || "无" }}</div>
            </div>
          </details>
        </div>

        <!-- 内容区域 - 根据不同模式显示不同的内容 -->
        <div class="border rounded-lg shadow-sm overflow-hidden" :class="darkMode ? 'bg-gray-800 border-gray-700 text-white' : 'bg-white border-gray-200 text-gray-900'">
          <!-- 常规预览模式 - 显示Markdown渲染结果 -->
          <div v-if="viewMode === 'preview'" class="p-6">
            <PasteViewPreview
              ref="previewRef"
              :dark-mode="darkMode"
              :content="paste.content"
              :is-dev="isDev"
              :enable-debug="enableDebug"
              :is-plain-text-mode="isPlainTextMode"
              @rendered="handlePreviewRendered"
            />
          </div>

          <!-- 大纲预览模式 - 左侧显示大纲，右侧显示内容 -->
          <div v-else-if="viewMode === 'outline'">
            <PasteViewOutline
              :dark-mode="darkMode"
              :outline-data="outlineData"
              :outline-tree-data="outlineTreeData"
              :content="paste.content"
              :is-dev="isDev"
              :enable-debug="enableDebug"
              @heading-click="scrollToHeading"
            >
              <template #content>
                <div class="content-scroll flex-1 p-4 overflow-y-auto md:absolute md:inset-0">
                  <PasteViewPreview
                    :dark-mode="darkMode"
                    :content="paste.content"
                    :is-dev="isDev"
                    :enable-debug="enableDebug"
                    :is-plain-text-mode="isPlainTextMode"
                    @rendered="handlePreviewRendered"
                  />
                </div>
              </template>
            </PasteViewOutline>
          </div>

          <!-- 编辑模式 - 显示编辑器和相关配置表单 -->
          <div v-else-if="viewMode === 'edit'" class="flex flex-col p-6">
            <PasteViewEditor
              :dark-mode="darkMode"
              :content="editContent"
              :paste="paste"
              :loading="loading"
              :error="error"
              :is-dev="isDev"
              :enable-debug="enableDebug"
              @save="saveEdit"
              @cancel="cancelEdit"
              @update:error="(val) => (error = val)"
            />
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<style scoped>
/* 主容器样式 */
.paste-view {
  min-height: 500px;
  width: 100%;
}

/* 隐藏密码框的浏览器自带眼睛图标 */
.password-input::-ms-reveal,
.password-input::-ms-clear {
  display: none !important;
}

/* 大屏幕额外优化 - 增加内容区域高度 */
@media (min-width: 1280px) {
  .paste-view {
    min-height: 600px;
  }
}
</style>
