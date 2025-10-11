/**
 * 通用复制文本到剪贴板函数
 * 支持现代Clipboard API和传统方法作为回退
 *
 * @param {string} text - 要复制的文本
 * @returns {Promise<boolean>} 是否复制成功
 */
export const copyToClipboard = async (text) => {
  try {
    // 检查是否支持现代Clipboard API
    if (navigator.clipboard) {
      try {
        await navigator.clipboard.writeText(text);
        return true;
      } catch (err) {
        console.warn("Clipboard API失败，尝试回退方法:", err);
        // 如果Clipboard API失败（例如权限问题），尝试传统方法
        return fallbackCopyTextToClipboard(text);
      }
    } else {
      // 使用传统方法
      return fallbackCopyTextToClipboard(text);
    }
  } catch (err) {
    console.error("复制到剪贴板失败:", err);
    return false;
  }
};

/**
 * 传统的复制到剪贴板方法
 * 使用document.execCommand('copy')
 *
 * @param {string} text - 要复制的文本
 * @returns {boolean} 是否复制成功
 */
function fallbackCopyTextToClipboard(text) {
  try {
    // 创建临时文本区域
    const textArea = document.createElement("textarea");
    textArea.value = text;

    // 设置样式使其不可见
    textArea.style.top = "0";
    textArea.style.left = "0";
    textArea.style.position = "fixed";
    textArea.style.opacity = "0";
    textArea.style.pointerEvents = "none";
    textArea.style.zIndex = "-1";

    // 添加到文档
    document.body.appendChild(textArea);

    // 选择文本
    textArea.focus();
    textArea.select();

    // 执行复制命令
    const successful = document.execCommand("copy");

    // 清理
    document.body.removeChild(textArea);

    return successful;
  } catch (err) {
    console.error("回退复制方法失败:", err);
    return false;
  }
}
