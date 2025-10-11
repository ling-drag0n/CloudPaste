/**
 * 全局消息管理系统 
 */

import { ref, computed } from "vue";

// 全局消息状态 - 使用单例模式
let globalMessageState = null;

export function useGlobalMessage() {
  // 确保全局只有一个消息管理实例
  if (!globalMessageState) {
    globalMessageState = createMessageState();
  }

  return globalMessageState;
}

/**
 * 创建消息状态管理 
 */
function createMessageState() {
  // ===== 核心状态 =====
  const message = ref(null);

  // ===== 计算属性 =====
  const hasMessage = computed(() => !!message.value);
  const messageType = computed(() => message.value?.type || null);
  const messageContent = computed(() => message.value?.content || null);

  // ===== 核心方法 =====

  let currentTimeoutId = null; // 用于管理自动清除的定时器

  /**
   * 显示消息
   * @param {string} type - 消息类型 ('success', 'error', 'warning', 'info')
   * @param {string} content - 消息内容
   * @param {number} duration - 显示时长（毫秒），默认4000ms
   */
  const showMessage = (type, content, duration = 4000) => {
    // 清除之前的定时器
    if (currentTimeoutId) {
      clearTimeout(currentTimeoutId);
      currentTimeoutId = null;
    }

    // 设置新消息
    message.value = {
      type,
      content,
      timestamp: Date.now(),
    };

    // 自动清除消息
    if (duration > 0) {
      currentTimeoutId = setTimeout(() => {
        clearMessage();
      }, duration);
    }

    console.log(`[GlobalMessage] 显示消息: ${type} - ${content}`);
  };

  /**
   * 清除当前消息
   */
  const clearMessage = () => {
    if (currentTimeoutId) {
      clearTimeout(currentTimeoutId);
      currentTimeoutId = null;
    }
    message.value = null;
    console.log("[GlobalMessage] 清除消息");
  };

  // ===== 便捷方法 =====

  /**
   * 显示成功消息
   */
  const showSuccess = (content, duration = 3000) => {
    showMessage("success", content, duration);
  };

  /**
   * 显示错误消息
   */
  const showError = (content, duration = 5000) => {
    showMessage("error", content, duration);
  };

  /**
   * 显示警告消息
   */
  const showWarning = (content, duration = 4000) => {
    showMessage("warning", content, duration);
  };

  /**
   * 显示信息消息
   */
  const showInfo = (content, duration = 4000) => {
    showMessage("info", content, duration);
  };

  return {
    // 状态 - 匹配现有组件期望的结构
    message,

    // 计算属性
    hasMessage,
    messageType,
    messageContent,

    // 核心方法
    showMessage,
    clearMessage,

    // 便捷方法
    showSuccess,
    showError,
    showWarning,
    showInfo,

    // 兼容性别名
    clearCurrentMessage: clearMessage,
  };
}
