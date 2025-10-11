/**
 * 离线状态下的用户提示工具
 */

let toastContainer = null;
let currentToast = null;

// 创建Toast容器
function createToastContainer() {
  if (toastContainer) return toastContainer;
  
  toastContainer = document.createElement('div');
  toastContainer.className = 'offline-toast-container';
  toastContainer.style.cssText = `
    position: fixed;
    top: 20px;
    left: 50%;
    transform: translateX(-50%);
    z-index: 9999;
    pointer-events: none;
  `;
  document.body.appendChild(toastContainer);
  return toastContainer;
}

// 显示离线提示
export function showOfflineToast(message = '页面暂时无法访问，您当前处于离线状态') {
  // 如果已有Toast，先移除
  if (currentToast) {
    hideOfflineToast();
  }
  
  const container = createToastContainer();
  
  // 创建Toast元素
  const toast = document.createElement('div');
  toast.className = 'offline-toast';
  toast.style.cssText = `
    background: #f59e0b;
    color: white;
    padding: 12px 20px;
    border-radius: 8px;
    box-shadow: 0 4px 12px rgba(0, 0, 0, 0.15);
    font-size: 14px;
    font-weight: 500;
    max-width: 400px;
    text-align: center;
    pointer-events: auto;
    transform: translateY(-20px);
    opacity: 0;
    transition: all 0.3s ease-out;
    display: flex;
    align-items: center;
    gap: 8px;
  `;
  
  // 添加图标
  const icon = document.createElement('span');
  icon.innerHTML = '⚠️';
  icon.style.fontSize = '16px';
  
  // 添加文本
  const text = document.createElement('span');
  text.textContent = message;
  
  toast.appendChild(icon);
  toast.appendChild(text);
  container.appendChild(toast);
  
  // 触发动画
  requestAnimationFrame(() => {
    toast.style.transform = 'translateY(0)';
    toast.style.opacity = '1';
  });
  
  currentToast = toast;
  
  // 3秒后自动隐藏
  setTimeout(() => {
    hideOfflineToast();
  }, 3000);
}

// 隐藏离线提示
export function hideOfflineToast() {
  if (!currentToast) return;
  
  currentToast.style.transform = 'translateY(-20px)';
  currentToast.style.opacity = '0';
  
  setTimeout(() => {
    if (currentToast && currentToast.parentNode) {
      currentToast.parentNode.removeChild(currentToast);
    }
    currentToast = null;
  }, 300);
}

// 显示页面不可用提示
export function showPageUnavailableToast(pageName = '此页面') {
  showOfflineToast(`${pageName}暂时无法访问，请检查网络连接`);
}

// 清理所有Toast
export function clearAllToasts() {
  hideOfflineToast();
  if (toastContainer && toastContainer.parentNode) {
    toastContainer.parentNode.removeChild(toastContainer);
    toastContainer = null;
  }
}
