// 在文件开头添加常量声明
const MAX_FILE_SIZE = 98 * 1024 * 1024; // 文件大小限制 (98MB)
const MAX_TOTAL_STORAGE = 5 * 1024 * 1024 * 1024; // 总存储限制 (5GB)

// 工具函数
const utils = {
  // 生成随机ID
  generateId(length = 8) {
    const chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    let result = "";
    for (let i = 0; i < length; i++) {
      result += chars.charAt(Math.floor(Math.random() * chars.length));
    }
    return result;
  },

  // 密码加密
  async hashPassword(password) {
    const encoder = new TextEncoder();
    const data = encoder.encode(password);
    const hash = await crypto.subtle.digest("SHA-256", data);
    return btoa(String.fromCharCode(...new Uint8Array(hash)));
  },

  // 验证密码
  async verifyPassword(password, hash) {
    const inputHash = await utils.hashPassword(password);
    return inputHash === hash;
  },

  // 计算过期时间
  calculateExpiryTime(duration) {
    if (duration === "never") return null; // 永久不过期返回 null

    const now = new Date();
    switch (duration) {
      case "1h":
        return new Date(now.getTime() + 60 * 60 * 1000);
      case "1d":
        return new Date(now.getTime() + 24 * 60 * 60 * 1000);
      case "7d":
        return new Date(now.getTime() + 7 * 24 * 60 * 60 * 1000);
      case "30d":
        return new Date(now.getTime() + 30 * 24 * 60 * 60 * 1000);
      default:
        return new Date(now.getTime() + 24 * 60 * 60 * 1000);
    }
  },

  // 检查是否过期
  isExpired(expiryTime) {
    if (!expiryTime) return false; // 如果没有过期时间,则永不过期
    return new Date() > new Date(expiryTime);
  },
};

// CSS 样式
const styles = `
/* GitHub 图标样式 */
.github-link {
  position: fixed;
  top: 20px;
  right: 20px;
  z-index: 1000;
  color: var(--text-color);  /* 使用主题颜色 */
  transition: color 0.3s ease;
}

.github-link:hover {
  color: var(--primary-color);
}

.github-icon {
  transition: transform 0.3s ease;
}

.github-link:hover .github-icon {
  transform: scale(1.1);
}

:root {
  --primary-color: #3498db;
  --bg-color: #f5f6fa;
  --border-color: #dcdde1;
  --text-color: #2d3436;
  --card-bg: white;
  --error-bg: #ffebee;
  --error-border: #ef5350;
  --error-text: #c62828;
  --code-bg: #f6f8fa;
  --hover-bg: rgba(0,0,0,0.05);
  --secondary-bg: #f8f9fa;
  --secondary-text: #666;
  --admin-panel-bg: white;
  --markdown-preview-bg: white;
  --markdown-code-bg: #f6f8fa;
  --markdown-blockquote-bg: #f8f9fa;
  --markdown-blockquote-border: #3498db;
  --btn-secondary-bg: #95a5a6;
  --btn-secondary-text: white;
  --input-bg: white;
  --input-text: #2d3436;
  --input-placeholder: #999;
  --scrollbar-thumb: rgba(0,0,0,0.2);
  --scrollbar-track: transparent;
  --markdown-bg: white;
  --markdown-text: #24292e;
  --markdown-heading-text: #1a1a1a;
  --markdown-link: #0366d6;
  --markdown-link-hover: #0550ae;
  --markdown-code-bg: #f6f8fa;
  --markdown-code-text: #24292e;
  --markdown-code-block-bg: #f6f8fa;
  --markdown-blockquote-bg: #f8f9fa;
  --markdown-blockquote-text: #6a737d;
  --markdown-blockquote-border: #3498db;
  --markdown-table-border: #dfe2e5;
  --markdown-table-bg: white;
  --markdown-table-alt-bg: #f8f9fa;
  --markdown-hr: #eaecef;
  --markdown-list-marker: #24292e;
}

[data-theme="dark"] {
  --primary-color: #5dade2;
  --bg-color: #1a1a1a;
  --border-color: #333;
  --text-color: #e0e0e0;
  --card-bg: #242424;
  --error-bg: #421c1c;
  --error-border: #b71c1c;
  --error-text: #ff5252;
  --code-bg: #2d2d2d;
  --hover-bg: rgba(255,255,255,0.05);
  /* 暗色主题特有变量 */
  --secondary-bg: #2d2d2d;
  --secondary-text: #aaa;
  --admin-panel-bg: #242424;
  --markdown-preview-bg: #2d2d2d;
  --markdown-code-bg: #2d333b;
  --markdown-blockquote-bg: #363636;
  --markdown-blockquote-border: #5dade2;
  --btn-secondary-bg: #4a4a4a;
  --btn-secondary-text: #e0e0e0;
  --input-bg: #363636;
  --input-text: #e0e0e0;
  --input-placeholder: #666;
  --scrollbar-thumb: rgba(255,255,255,0.2);
  --scrollbar-track: rgba(255,255,255,0.05);
  --markdown-bg: #22272e;
  --markdown-text: #adbac7;
  --markdown-heading-text: #e6edf3;
  --markdown-link: #539bf5;
  --markdown-link-hover: #6cb6ff;
  --markdown-code-bg: #2d333b;
  --markdown-code-text: #adbac7;
  --markdown-code-block-bg: #2d333b;
  --markdown-blockquote-bg: #2d333b;
  --markdown-blockquote-text: #768390;
  --markdown-blockquote-border: #444c56;
  --markdown-table-border: #444c56;
  --markdown-table-bg: #22272e;
  --markdown-table-alt-bg: #2d333b;
  --markdown-hr: #444c56;
  --markdown-list-marker: #768390;
}

* {
  box-sizing: border-box;
  margin: 0;
  padding: 0;
}

body {
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
  background: var(--bg-color);
  color: var(--text-color);
  line-height: 1.6;
}

.card {
  background: var(--card-bg);
  border-radius: 8px;
  box-shadow: 0 2px 4px rgba(0,0,0,0.1);
  padding: 2rem;
  margin-bottom: 1rem;
  width: fit-content;
  min-width: 1200px; /* 设置默认最小宽度 */
  overflow: visible;
  transition: width 0.3s ease; /* 添加过渡效果 */
}

.tabs {
  display: flex;
  gap: 1rem;
  margin-bottom: 1rem;
}

.tab {
  padding: 0.5rem 1rem;
  border: none;
  background: none;
  cursor: pointer;
  border-bottom: 2px solid transparent;
  position: relative;
}

.tab::after {
  content: '';
  position: absolute;
  bottom: 0;
  left: 50%;
  width: 0;
  height: 2px;
  background: var(--primary-color);
  transition: all 0.3s ease;
  transform: translateX(-50%);
}

.tab.active {
  border-bottom-color: var(--primary-color);
  color: var(--primary-color);
}

.tab.active::after {
  width: 100%;
}

.settings {
  margin-top: 1rem;
  display: grid;
  gap: 1rem;
  grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
}

.input-group {
  margin-bottom: 1rem;
}

.input-group label {
  display: block;
  margin-bottom: 0.5rem;
}

.input-group input, .input-group select {
  width: 100%;
  padding: 0.5rem;
  border: 1px solid var(--border-color);
  border-radius: 4px;
}

.btn {
  padding: 0.5rem 1rem;
  border: none;
  border-radius: 4px;
  background: var(--primary-color);
  color: white;
  cursor: pointer;
  transition: opacity 0.2s;
  position: relative;
  overflow: hidden;
}

.btn:hover {
  opacity: 0.9;
}

.btn:active {
  transform: translateY(1px);
}

.btn::after {
  content: '';
  position: absolute;
  top: 50%;
  left: 50%;
  width: 5px;
  height: 5px;
  background: rgba(255, 255, 255, 0.5);
  opacity: 0;
  border-radius: 100%;
  transform: scale(1, 1) translate(-50%);
  transform-origin: 50% 50%;
}

.btn:active::after {
  animation: ripple 0.6s ease-out;
}

@keyframes ripple {
  0% {
    transform: scale(0, 0);
    opacity: 0.5;
  }
  100% {
    transform: scale(40, 40);
    opacity: 0;
  }
}

.file-drop {
  border: 2px dashed var(--border-color);
  border-radius: 4px;
  padding: 2rem;
  text-align: center;
  cursor: pointer;
  transition: all 0.3s ease;
  position: relative;
  overflow: hidden;
}

.file-drop.dragging {
  border-color: var(--primary-color);
  background: rgba(52, 152, 219, 0.1);
  transform: scale(1.02);
  box-shadow: 0 0 15px rgba(52, 152, 219, 0.2);
}

.file-list {
  margin-top: 1rem;
}

.file-item {
  display: flex;
  align-items: center;
  padding: 0.5rem;
  border: 1px solid var(--border-color);
  border-radius: 4px;
  margin-bottom: 0.5rem;
  animation: file-item-in 0.3s ease-out;
}

@keyframes file-item-in {
  from {
    opacity: 0;
    transform: translateY(-10px);
  }
  to {
    opacity: 1;
    transform: translateY(0);
  }
}

.file-item .name {
  flex: 1;
}

.file-item .size {
  color: #7f8c8d;
  margin: 0 1rem;
}

.result {
  margin-top: 1rem;
  padding: 1rem;
  background: #f1f2f6;
  border-radius: 4px;
}

.error {
  color: #e74c3c;
  padding: 0.8rem;
  border-radius: 4px;
  background: #ffebee;
  border-left: 4px solid #ef5350;
  margin: 1rem 0;
  animation: error-in 0.3s ease-out;
}

@keyframes error-in {
  from {
    opacity: 0;
    transform: translateY(-10px);
  }
  to {
    opacity: 1;
    transform: translateY(0);
  }
}

.link {
  display: flex;
  align-items: center;
  gap: 1rem;
  margin: 0.5rem 0;
}

.link a {
  color: var(--primary-color);
  text-decoration: none;
  word-break: break-all;
}

.content {
  margin-bottom: 1rem;
  padding: 1rem;
  border: 1px solid var(--border-color);
  border-radius: 4px;
  line-height: 1.5;  // 减小行高
  overflow-wrap: break-word;
  word-wrap: break-word;
  word-break: break-all;
  white-space: pre-wrap;       /* 保留换行符并自动换行 */
  max-width: 100%;            /* 限制最大宽度 */
}

.content input[type="checkbox"] {
  appearance: none;
  -webkit-appearance: none;
  width: 16px;
  height: 16px;
  border: 2px solid var(--primary-color);
  border-radius: 3px;
  margin: 0;
  position: relative;
  top: 3px;
  margin-right: 8px;
  cursor: pointer;
  vertical-align: top;
}

.content input[type="checkbox"]:checked {
  background-color: var(--primary-color);
}

.content input[type="checkbox"]:checked::after {
  content: "✓";
  color: white;
  position: absolute;
  left: 50%;
  top: 50%;
  transform: translate(-50%, -50%);
  font-size: 12px;
  line-height: 1;
}

.content ul {
  list-style: none;
  padding-left: 0;
  margin: 0.3em 0;  // 减小列表整体间距
}

.content li {
  display: flex;
  align-items: flex-start;
  margin: 0.2em 0;  // 减小列表项间距
  line-height: 1.5;  // 减小行高
  margin: 0.5em 0;              /* 增加列表项间距 */
  display: block;             /* 改为块级显示 */
}

.content li label {
  display: inline-flex;
  align-items: flex-start;
  margin: 0;
  cursor: pointer;
}

.content p {
  margin: 0.3em 0;  // 进一步减小段落间距
  line-height: 1.5;  // 减小行高
  overflow-wrap: break-word;
  word-wrap: break-word;
  word-break: break-all;
}

.expiry-info {
  color: #7f8c8d;
  font-size: 0.9rem;
}

/* 二维码弹窗样式 */
.qr-dialog {
  position: fixed;
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  background: rgba(0, 0, 0, 0.5);
  display: flex;
  align-items: center;
  justify-content: center;
  z-index: 1100;
}

.qr-content {
  background: var(--card-bg);
  padding: 2rem;
  border-radius: 8px;
  box-shadow: 0 2px 20px rgba(0, 0, 0, 0.2);
  max-width: 300px;
  width: 90%;
  text-align: center;
}

.qr-content h3 {
  margin: 0 0 1rem 0;
  color: var(--text-color);
}

.qr-actions {
  display: flex;
  gap: 1rem;
  margin-top: 1.5rem;
  justify-content: center;
}

/* 链接操作按钮组样式 */
.link-actions {
  display: flex;
  gap: 0.5rem;
  align-items: center;
}

/* 修改 qr-btn 样式，使其同时支持 button 和 a 标签 */
.qr-btn {
  padding: 0.5rem;
  background: none;
  border: none;
  cursor: pointer;
  color: var(--text-color);
  opacity: 0.7;
  transition: all 0.3s ease;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  text-decoration: none;
}

.qr-btn:hover {
  opacity: 1;
  transform: scale(1.1);
}

.qr-btn svg {
  width: 20px;
  height: 20px;
}

/* 确保链接形式的 qr-btn 继承颜色 */
a.qr-btn {
  color: inherit;
}

.qr-content #qr-container {
  margin: 1rem auto;
  width: 200px;
  height: 200px;
  display: flex;
  align-items: center;
  justify-content: center;
}

.qr-content #qr-container img {
  max-width: 100%;
  height: auto;
  display: block;
}

/* 添加直链按钮样式 */
.direct-link-btn {
  width: 32px;
  height: 32px;
  padding: 6px;
  border: none;
  background: none;
  cursor: pointer;
  color: var(--text-color);
  opacity: 0.7;
  transition: all 0.3s ease;
  margin-left: 0.5rem;
}

.direct-link-btn:hover {
  opacity: 1;
  transform: scale(1.1);
  color: var(--primary-color);
}

.direct-link-btn svg {
  width: 100%;
  height: 100%;
}

/* 管理面板组件 */
.admin-panel {
  position: fixed;
  top: 20px;
  right: 70px;  
  z-index: 1000;
}

.admin-login {
  background: white;
  padding: 1.5rem;
  border-radius: 8px;
  box-shadow: 0 2px 15px rgba(0,0,0,0.1);
  min-width: 300px;
}

.admin-content {
  position: fixed;
  top: 0;
  right: 0;
  bottom: 0;
  width: 350px;
  background: var(--admin-panel-bg);
  box-shadow: -2px 0 15px rgba(0,0,0,0.1);
  padding: 1.5rem;
  overflow-y: auto;
}

/* 管理面板头部 */
.admin-header {
  display: flex;
  flex-direction: column;
  gap: 0.8rem;
  margin-bottom: 1.5rem;
  padding-bottom: 1rem;
  border-bottom: 1px solid var(--border-color);
}

.admin-header-top {
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.admin-header-top h3 {
  font-size: 1.2rem;
  font-weight: 600;
  color: #2c3e50;
  margin: 0;
}

/* 管理面板控制按钮 */
.admin-controls {
  display: flex;
  gap: 0.8rem;
  align-items: center;
  flex-wrap: wrap;
}

.control-btn {
  min-width: 100px;
  height: 32px;
  padding: 0 0.8rem;
  border: 1px solid var(--border-color);
  border-radius: 4px;
  background: white;
  cursor: pointer;
  font-size: 0.9rem;
  transition: all 0.2s;
  display: flex;
  align-items: center;
  justify-content: center;
  color: #666;
  white-space: nowrap;
}

.control-btn.active {
  background: var(--primary-color);
  color: white;
  border-color: var(--primary-color);
}

.control-btn:hover {
  border-color: var(--primary-color);
  color: var(--primary-color);
}

.control-btn.active:hover {
  opacity: 0.9;
  color: white;
}

.refresh-btn {
  height: 32px;
  padding: 0 0.8rem;
  border: 1px solid var(--border-color);
  border-radius: 4px;
  background: white;
  cursor: pointer;
  font-size: 0.9rem;
  transition: all 0.2s;
  display: flex;
  align-items: center;
  justify-content: center;
  color: #666;
  white-space: nowrap;
}

.refresh-btn:hover {
  border-color: var(--primary-color);
  color: var(--primary-color);
}

.refresh-btn:disabled {
  opacity: 0.6;
  cursor: not-allowed;
}

.close-btn {
  width: 32px;
  height: 32px;
  padding: 0;
  border: 1px solid var(--border-color);
  border-radius: 4px;
  background: white;
  cursor: pointer;
  font-size: 1.2rem;
  transition: all 0.2s;
  display: flex;
  align-items: center;
  justify-content: center;
  color: #666;
}

.close-btn:hover {
  border-color: #e74c3c;
  color: #e74c3c;
}

/* 分享列表 */
.share-list {
  margin-top: 1rem;
}

/* 修改分享项布局 */
.share-item {
  position: relative;  /* 添加相对定位 */
  padding: 1rem;
  background: var(--secondary-bg);
  border-radius: 8px;
  margin-bottom: 1rem;
  border: 1px solid var(--border-color);
}

/* 修改标题样式 */
.share-item .title {
  display: flex;
  align-items: center;
  justify-content: space-between;  /* 添加这行，让内容两端对齐 */
  margin-bottom: 0.5rem;
  font-weight: 600;
  color: var(--text-color);
}

/* 修改左侧标题区域样式 */
.title-left {
  display: flex;
  align-items: center;
  gap: 1rem; /* 增加文字和锁图标的间距 */
}

/* 标题链接样式 */
.title-link {
  text-decoration: none;
  color: var(--text-color);
  transition: all 0.2s ease;
  padding: 0.15rem 0.3rem;
  border-radius: 4px;
  position: relative;
  margin-left: -0.4rem;
  display: inline-block; /* 让链接紧贴文字内容 */
  line-height: 1.2; /* 减小行高 */
}

.title-link:hover {
  background: var(--hover-bg);
  color: var(--primary-color);
}

/* 锁图标样式 */
.lock-icon {
  font-size: 0.9em;
  opacity: 0.7;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 1.2em;
  height: 1.2em;
  transition: opacity 0.2s ease;
  margin-left: 0.3rem; /* 微调锁图标的间距 */
}

/* 添加点击波纹效果 */
.title-link::after {
  content: '';
  position: absolute;
  inset: 0;
  border-radius: 4px;
  background: var(--primary-color);
  opacity: 0;
  transform: scale(0.6);
  transition: all 0.2s ease;
}

.title-link:active::after {
  opacity: 0.1;
  transform: scale(1);
}

/* 统一图标按钮样式 */
.icon-btn {
  width: 20px;
  height: 20px;
  padding: 2px;
  border: none;
  background: none;
  cursor: pointer;
  color: var(--text-color);
  opacity: 0.7;
  transition: all 0.3s ease;
}

.icon-btn:hover {
  opacity: 1;
  transform: scale(1.1);
  color: var(--primary-color);
}

.icon-btn svg {
  width: 100%;
  height: 100%;
}

/* 调整密码保护标签样式 */
.badge {
  font-size: 0.8em;
  padding: 2px 6px;
  border-radius: 4px;
  background: var(--primary-color);
  color: white;
}

.share-item .info {
  font-size: 0.9em;
  color: var(--secondary-text);
  margin-bottom: 0.5rem;
  padding-right: 2.5rem;  /* 避免与二维码重叠 */
}

.share-item .info div {
  margin: 0.25rem 0;
}

/* 操作按钮组样式 */
.share-item .actions {
  display: flex;
  gap: 0.5rem;
  margin-top: 1rem;
  align-items: center;
  flex-wrap: wrap;
}

/* 二维码按钮样式 */
.share-item .qr-btn {
  position: absolute;
  top: 1rem;
  right: 1rem;
  padding: 0.4rem;
  background: none;
  border: none;
  cursor: pointer;
  color: var(--text-color);
  opacity: 0.6;
  transition: all 0.2s;
  display: flex;
  align-items: center;
  justify-content: center;
}

.share-item .qr-btn:hover {
  opacity: 1;
}

.share-item .qr-btn svg {
  width: 18px;
  height: 18px;
}

/* 确保标题文字不会与图标重叠 */
.share-item .title-text {
  margin-right: 80px;  /* 为图标预留空间 */
}

/* 统计卡片 */
.admin-stats {
  display: grid;
  grid-template-columns: repeat(2, 1fr);
  gap: 1rem;
  margin-bottom: 1.5rem;
}

.stat-card {
  background: var(--secondary-bg);
  padding: 1rem;
  border-radius: 8px;
  text-align: center;
}

.stat-value {
  font-size: 1.5rem;
  font-weight: bold;
  color: var(--primary-color);
}

.stat-label {
  font-size: 0.9rem;
  color: var(--secondary-text);
}

/* 筛选和状态 */
.filter-bar {
  margin-bottom: 1rem;
  display: flex;
  gap: 0.5rem;
}

.filter-bar select {
  padding: 0.5rem;
  border: 1px solid var(--border-color);
  border-radius: 4px;
  background: white;
}

.upload-status {
  margin: 1rem 0;
  padding: 0.5rem;
  border-radius: 4px;
  background: var(--secondary-bg);
  color: var(--text-color);
  text-align: center;
}

.upload-status.error {
  background: #ffebee;
  color: #c62828;
}


/* 密码输入组件 */
.password-input-group {
  position: relative;
  display: flex;
  align-items: center;
}

.password-toggle {
  position: absolute;
  right: 10px;
  background: none;
  border: none;
  cursor: pointer;
  color: #666;
  padding: 5px;
  opacity: 0.6;
  transition: opacity 0.2s;
}

.password-toggle:hover {
  opacity: 1;
}

/* 管理员登录按钮 */
.admin-login .actions {
  display: flex;
  gap: 1rem;  /* 添加按钮之间的间距 */
  margin-top: 1rem;
}

.admin-login .actions .btn {
  flex: 1;  /* 让按钮平均分配空间 */
}

.admin-login .actions .btn:last-child {
  background: #95a5a6;  /* 取消按钮使用不同的颜色 */
}


/* 确认对话框样式 */
.confirm-dialog {
  position: fixed;
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  background: rgba(0, 0, 0, 0.5);
  display: flex;
  align-items: center;
  justify-content: center;
  z-index: 1100;
}

.confirm-content {
  background: var(--card-bg);
  padding: 2rem;
  border-radius: 8px;
  box-shadow: 0 2px 20px rgba(0, 0, 0, 0.2);
  max-width: 400px;
  width: 90%;
}

.confirm-content h3 {
  margin: 0 0 1rem 0;
  color: var(--text-color);
}

.confirm-content .warning {
  color: #e74c3c;
  font-weight: 500;
  margin: 1rem 0;
}

.confirm-actions {
  display: flex;
  gap: 1rem;
  margin-top: 1.5rem;
}

.confirm-actions .btn {
  flex: 1;
}

.confirm-actions .btn.cancel {
  background: #95a5a6;
}

.delete-btn {
  background: #e74c3c;
}

.delete-btn:hover {
  background: #c0392b;
}


/* Markdown 复选框 */
.markdown-toggle {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  margin-bottom: 1rem;
}

.markdown-toggle input[type="checkbox"] {
  margin: 0;
}

.markdown-toggle label {
  margin: 0;
  cursor: pointer;
}

/* 编辑页滚动优化 */
.editor-container {
  display: flex;
  gap: 1rem;
  height: 600px;
  min-height: 400px;
  width: 100%;         /* 限制最大宽度 */
  max-width: 100%;     /* 限制最大宽度 */
  position: relative;
  resize: both;
  overflow: auto;
  margin: 1rem 0;
}

.editor, .preview {
  flex: 1;
  padding: 1rem;
  border: 1px solid var(--border-color);
  border-radius: 4px;
  overflow-y: auto;
  position: relative;
  word-wrap: break-word;
  overflow-wrap: break-word;
  white-space: pre-wrap;
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
  font-size: 16px;
  line-height: 1.6;
  width: 0;             /* 强制flex子项不超出容器 */
  min-width: 0;         /* 允许flex子项收缩 */
  min-width: calc(50% - 0.5rem); /* 确保最小宽度不会小于容器的一半减去间距 */
}

.editor textarea {
  width: 100%;
  height: 100%;
  border: none;
  resize: none;
  outline: none;
  font-family: inherit;
  font-size: inherit;
  line-height: inherit;
  padding: 0;
  margin: 0;
  background: transparent;
  white-space: pre-wrap;       /*  保留换行符并自动换行 */
  word-wrap: break-word;       /*  允许单词内换行 */
  overflow-wrap: break-word;   /*  确保长单词会换行 */
}

/* 拉伸手柄样式优化 */
.editor-container::after {
  content: '';
  position: absolute;
  bottom: 0;
  right: 0;
  width: 25px;
  height: 25px;
  cursor: nw-resize;
  background: linear-gradient(135deg, transparent 50%, var(--border-color) 50%);
  border-radius: 0 0 4px 0;
}

/* 优化滚动条样式 */
.editor::-webkit-scrollbar,
.preview::-webkit-scrollbar {
  width: 12px;  
  height: 12px;  
}

.editor::-webkit-scrollbar-track,
.preview::-webkit-scrollbar-track {
  background: transparent;
}

.editor::-webkit-scrollbar-thumb,
.preview::-webkit-scrollbar-thumb {
  background: var(--scrollbar-thumb);
  border-radius: 6px;  
  border: 3px solid transparent;  
  background-clip: padding-box;
}

.editor::-webkit-scrollbar-thumb:hover,
.preview::-webkit-scrollbar-thumb:hover {
  background: var(--scrollbar-thumb);
  border: 3px solid transparent;  
  background-clip: padding-box;
}

/* 防止文本选择时的闪烁 */
.editor textarea::selection {
  background: rgba(52, 152, 219, 0.2);
}

/* 优化文本渲染 */
.editor, .preview {
  text-rendering: optimizeLegibility;
  -webkit-text-size-adjust: 100%;
  -webkit-tap-highlight-color: transparent;
}


/* Markdown 内容样式优化 */
.content {
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
  font-size: 16px;
  line-height: 1.6;
  color: var(--text-color);
  padding: 2rem;
  max-width: 100%;
  overflow-x: auto;
}

/* 列表样式优化 */
.content ul {
  list-style: none;
  padding-left: 2em;
  margin: 1em 0;
}

.content ul li {
  position: relative;
  margin: 0.5em 0;
  padding-left: 0.5em;
  line-height: 1.6;
  display: flex; /* 使用 flex 布局 */
  flex-wrap: wrap; /* 允许内容换行 */
  align-items: baseline; /* 基线对齐 */
}

.content ul li::before {
  content: "•";
  position: absolute;
  left: -1.5em;
  color: var(--text-color);
}

/* 列表项内容样式 */
.content li > * {
  margin: 0.1em 0;  // 减小列表项内容间距
  line-height: inherit;
}

.content li p {
  display: inline;
  margin: 0.1em 0;  // 减小列表项段落间距
  line-height: inherit;
}

.content li strong {
  font-weight: 600;
  color: #2c3e50;
  margin-right: 0.25em; /* 加粗文本右侧添加小间距 */
}

/* 标点符号样式优化 */
.content li strong + *,
.content li p + * {
  margin-left: 0; /* 移除标点符号前的间距 */
}

/* 中文标点符号对齐 */
.content li:lang(zh),
.content li:lang(zh) * {
  text-align: justify;
  text-justify: inter-ideograph;
}

/* 代码块在列表项中的样式 */
.content li pre {
  width: 100%; /* 确保代码块占满宽度 */
  margin: 1em 0;
  display: block;
}

/* 行内代码样式优化 */
.content code {
  padding: 0.2em 0.4em;
  margin: 0;
  font-size: 0.9em;
  background-color: rgba(27,31,35,0.05);
  border-radius: 3px;
  font-family: 'SFMono-Regular', Consolas, 'Liberation Mono', Menlo, monospace;
  color: #476582;
  vertical-align: baseline; /* 确保行内代码垂直对齐 */
}

/* 标点符号和特殊字符对齐 */
.content li strong:last-child {
  margin-right: 0;
}

.content li > p:first-child {
  margin-right: 0;
}

/* 确保列表项内的所有元素垂直对齐 */
.content li * {
  vertical-align: baseline;
}

/* 优化中文冒号对齐 */
.content li:lang(zh) strong + :not(pre):not(ul):not(ol)::before {
  content: "";
  white-space: normal;
  display: inline;
}

/* 列表样式优化 */
.content ul {
  list-style: none;
  padding-left: 2em;
  margin: 1em 0;
}

.content ul li {
  position: relative;
  margin: 0.5em 0;
  padding-left: 0.5em;
  line-height: 1.6;
}

.content ul li::before {
  content: "•";
  position: absolute;
  left: -1.5em;
  color: var(--text-color);
}

/* 列表项内容样式 */
.content li > * {
  margin: 0.5em 0;
}

.content li p {
  display: inline;
  margin: 0;
}

.content li > pre {
  display: block;
  margin: 0.5em 0;  // 减小代码块间距
}

/* 加粗文本样式 */
.content strong {
  font-weight: 600;
  color: #2c3e50;
  display: inline;
  white-space: normal;       /* 允许正常换行 */
}

/* 代码块样式 */
.content pre {
  margin: 1.5em 0;
  padding: 1.5em;
  background: var(--markdown-code-bg);
  border-radius: 6px;
  overflow-x: auto;          /* 允许横向滚动 */
  white-space: pre-wrap;     /* 允许自动换行 */
  word-wrap: break-word;     /* 允许长单词换行 */
  word-break: break-all;     /* 允许在任意字符处换行 */
  max-width: 100%;          /* 限制最大宽度 */
  font-family: 'SFMono-Regular', Consolas, 'Liberation Mono', Menlo, monospace;
  font-size: 0.9em;
  line-height: 1.6;
  position: relative;
  border: 1px solid #eaecef;
}

.content pre code {
  padding: 0;
  margin: 0;
  background: none;
  border-radius: 0;
  color: inherit;
  font-size: 1em;
  white-space: inherit;      /* 继承父元素的 white-space 属性 */
  word-wrap: inherit;        /* 继承父元素的 word-wrap 属性 */
  word-break: inherit;       /* 继承父元素的 word-break 属性 */
  overflow-wrap: inherit;    /* 继承父元素的 overflow-wrap 属性 */
}

/* 标题样式 */
.content h1, .content h2, .content h3, .content h4, .content h5, .content h6 {
  margin: 0.8em 0 0.2em;  // 减小标题下方间距
  font-weight: 600;
  line-height: 1.2;  // 减小标题行高
  color: #1a202c;
}

.content h1 { 
  font-size: 2em; 
  border-bottom: 2px solid #eaecef; 
  padding-bottom: 0.5em;
  margin-top: 1em;
}

.content h2 { 
  font-size: 1.5em; 
  border-bottom: 1px solid #eaecef; 
  padding-bottom: 0.4em;
}

.content h3 { font-size: 1.25em; }
.content h4 { font-size: 1em; }
.content h5 { font-size: 0.875em; }
.content h6 { font-size: 0.85em; color: #6a737d; }

/* 引用块样式 */
.content blockquote {
  margin: 0;  /* 移除外边距 */
  padding: 0.4em 1em;  /* 使用相等的上下内边距 */
  color: var(--markdown-blockquote-text);
  border-left: 0.25em solid var(--markdown-blockquote-border);
  background: var(--markdown-blockquote-bg);
  border-radius: 0 4px 4px 0;
}

.content blockquote > :first-child { margin-top: 0; }
.content blockquote > :last-child { margin-bottom: 0; }

/* 引用块内的段落样式 */
.content blockquote p {
  margin: 0;  /* 移除段落边距 */
  line-height: 1.6;
}

/* 列表项中的引用块特殊处理 */
.content li > blockquote {
  margin: 0.3em 0;  /* 在列表项中添加小的外边距 */
  width: calc(100% - 1em);
}

/* 连续引用块的处理 */
.content blockquote + blockquote {
  margin-top: 0.2em;  /* 连续引用块之间的间距 */
}

/* 表格样式优化 */
.content table {
  margin: 1.5em 0;
  border-collapse: collapse;
  width: 100%;
  max-width: 100%;
  border: 1px solid var(--border-color);
  display: table;
  table-layout: fixed;
  font-size: 0.95em;
  line-height: 1.5;
}

.content table th,
.content table td {
  padding: 0.8em 1em;
  border: 1px solid var(--border-color);
  text-align: left;
  vertical-align: top;
  line-height: 1.6;
  word-wrap: break-word;
  overflow-wrap: break-word;
}

/* 设置列宽比例 */
.content table th:nth-child(1),
.content table td:nth-child(1) {
  width: 15%;
}

.content table th:nth-child(2),
.content table td:nth-child(2) {
  width: 25%;
}

.content table th:nth-child(3),
.content table td:nth-child(3) {
  width: 60%;
}

/* 表头样式 */
.content table th {
  background: var(--secondary-bg);
  color: var(--text-color);
}

/* 表格行样式 */
.content table tr {
  background-color: #ffffff;
  border-top: 1px solid var(--border-color);
}

.content table tr:nth-child(2n) {
  background-color: var(--secondary-bg);
}

/* 表格单元格内容样式 */
.content table td strong,
.content table th strong {
  font-weight: 600;
  color: #24292e;
}

/* 确保表格内容紧凑 */
.content table p {
  margin: 0;
  padding: 0;
}

/* 确保表格边框完整性 */
.content table thead {
  border-bottom: 2px solid var(--border-color);
}

.content table tbody tr:last-child {
  border-bottom: none;
}

/* 优化表格内链接样式 */
.content table a {
  color: #0366d6;
  text-decoration: none;
}

.content table a:hover {
  text-decoration: underline;
}

/* 链接样式 */
.content a {
  color: var(--primary-color);
  text-decoration: none;
  font-weight: 500;
  border-bottom: 1px solid transparent;
  transition: border-color 0.2s;
}

.content a:hover {
  border-bottom-color: #3498db;
}

/* 图片样式 */
.content img {
  max-width: 100%;
  height: auto;
  margin: 1.5em auto;
  display: block;
  border-radius: 4px;
  box-shadow: 0 2px 8px rgba(0,0,0,0.1);
}

/* 水平线样式 */
.content hr {
  height: 2px;
  margin: 2.5em 0;
  background-color: #eaecef;
  border: none;
}

/* 列表基础样式优化 */
.content ul,
.content ol {
  padding-left: 2em;
  margin: 0.2em 0;  // 减小列表整体间距
}

/* 无序列表样式 */
.content ul {
  list-style: none;
  padding-left: 2em;
  margin: 0.2em 0;  // 减小列表整体间距
}

.content ul li {
  position: relative;
  padding-left: 0.5em;
  margin: 0.5em 0;
  line-height: 1.6;
}

.content ul li::before {
  content: "•";
  position: absolute;
  left: -1em;
  color: #2c3e50;
}

/* 二级无序列表样式 */
.content ul ul {
  margin: 0.5em 0;
  padding-left: 1.5em;
}

.content ul ul li::before {
  content: "•";
  font-size: 0.85em;
  left: -1.2em;
  top: 0.1em;
}

/* 有序列表样式 */
.content ol {
  list-style: none;
  counter-reset: item;
}

.content ol li {
  position: relative;
  padding-left: 0.5em;
  margin: 0.5em 0;
  line-height: 1.6;
  counter-increment: item;
}

.content ol li::before {
  content: counter(item) ".";
  position: absolute;
  left: -2em;
  width: 1.5em;
  text-align: right;
  color: #3498db;
  font-weight: 600;
}

/* 二级有序列表样式 */
.content ol ol {
  counter-reset: subitem;
  margin: 0.5em 0;
}

.content ol ol li {
  counter-increment: subitem;
}

.content ol ol li::before {
  content: counter(subitem) ".";
  left: -2em;
  color: #666;
}

/* 确保列表项内容对齐 */
.content li > * {
  margin: 0;
  line-height: inherit;
}

.content li p {
  display: inline;
  margin: 0;
}

/* 代码块复制按钮容器基础样式 */
.code-block-wrapper {
  position: relative;
  margin: 1.5em 0;
}

.code-block-wrapper pre {
  margin: 0;
  position: relative;
}

/* 复制按钮基础样式 */
.code-copy-btn {
  position: absolute;
  top: 0.5rem;
  right: 0.5rem;
  padding: 0.3rem 0.6rem;
  font-size: 0.8rem;
  color: #666;
  background: rgba(255, 255, 255, 0.8);
  border: 1px solid #ddd;
  border-radius: 4px;
  cursor: pointer;
  opacity: 0;
  transition: opacity 0.2s, background-color 0.2s;
  z-index: 10;
}

/* 复制按钮交互状态 */
.code-block-wrapper:hover .code-copy-btn {
  opacity: 1;
}

.code-copy-btn:hover {
  background: rgba(255, 255, 255, 0.95);
}

.code-copy-btn.copied {
  color: #2ecc71;
}

/* 暗色主题样式 */
[data-theme="dark"] .code-copy-btn {
  background: rgba(45, 45, 45, 0.8);
  border-color: #444;
  color: #eee;
}

[data-theme="dark"] .code-copy-btn:hover {
  background: rgba(45, 45, 45, 0.95);
}

[data-theme="dark"] .code-copy-btn.copied {
  color: #2ecc71;
}

/* 管理面板操作按钮 */
.admin-actions {
    display: flex;
    gap: 0.5rem;
    align-items: center;
}

/* 控制按钮基础样式 */
.control-btn {
    padding: 0.4rem 0.8rem;
    border: 1px solid var(--border-color);
    border-radius: 4px;
    background: white;
    cursor: pointer;
    font-size: 0.9rem;
    transition: all 0.2s;
}

.control-btn.active {
    background: var(--primary-color);
    color: white;
    border-color: var(--primary-color);
}

/* 上传禁用状态 */
.upload-disabled {
    text-align: center;
    padding: 2rem;
    background: #f8f9fa;
    border-radius: 8px;
    color: #666;
}

.upload-disabled p {
    margin: 0;
    font-size: 1.1rem;
}

/* 复制提示样式 */
.copy-toast {
  position: fixed;
  bottom: 20px;
  left: 50%;
  transform: translateX(-50%);
  background: rgba(0, 0, 0, 0.8);
  color: white;
  padding: 8px 16px;
  border-radius: 4px;
  font-size: 14px;
  z-index: 1000;
  opacity: 0;
  transition: opacity 0.3s ease;
  transform: translate(-50%, 20px);
  transition: all 0.3s cubic-bezier(0.68, -0.55, 0.265, 1.55);
}

.copy-toast.show {
  opacity: 1;
  transform: translate(-50%, 0);
}

/* 存储信息组件 */
.storage-info {
  margin: 1rem 0;
  padding: 1rem;
  background: var(--secondary-bg);
  border-radius: 8px;
}

.storage-progress {
  width: 100%;
  height: 20px;
  background-color: #f0f0f0;
  border-radius: 10px;
  overflow: hidden;
  margin: 10px 0;
}

.storage-progress-inner {
  height: 100%;
  background-color: var(--primary-color);
  transition: width 0.3s ease, background-color 0.3s ease;
  border-radius: 10px;
}

/* 状态类 */
.storage-progress-inner.warning {
  background-color: #f1c40f;
}

.storage-progress-inner.danger {
  background-color: #e74c3c;
}

.storage-details {
  display: flex;
  justify-content: space-between;
  font-size: 0.9rem;
  color: #666;
}

/* 加载状态组件 */
.loading-spinner {
  display: inline-block;
  width: 20px;
  height: 20px;
  border: 2px solid #f3f3f3;
  border-top: 2px solid var(--primary-color);
  border-radius: 50%;
  animation: spin 1s linear infinite;
  margin-right: 8px;
  vertical-align: middle;
}

@keyframes spin {
  0% { transform: rotate(0deg); }
  100% { transform: rotate(360deg); }
}

/* 按钮交互效果 */
.btn {
  position: relative;
  overflow: hidden;
}

.btn:active {
  transform: translateY(1px);
}

.btn::after {
  content: '';
  position: absolute;
  top: 50%;
  left: 50%;
  width: 5px;
  height: 5px;
  background: rgba(255, 255, 255, 0.5);
  opacity: 0;
  border-radius: 100%;
  transform: scale(1, 1) translate(-50%);
  transform-origin: 50% 50%;
}

.btn:active::after {
  animation: ripple 0.6s ease-out;
}

@keyframes ripple {
  0% {
    transform: scale(0, 0);
    opacity: 0.5;
  }
  100% {
    transform: scale(40, 40);
    opacity: 0;
  }
}

/* 文件拖放交互 */
.file-drop {
  transition: all 0.3s ease;
}

.file-drop.dragging {
  transform: scale(1.02);
  box-shadow: 0 0 15px rgba(52, 152, 219, 0.2);
}

/* 复制成功提示样式 */
.copy-toast {
  transform: translate(-50%, 20px);
  transition: all 0.3s cubic-bezier(0.68, -0.55, 0.265, 1.55);
}

.copy-toast.show {
  transform: translate(-50%, 0);
  opacity: 1;
}

/* 输入框交互 */
.input-group input:focus,
.input-group select:focus {
  outline: none;
  border-color: var(--primary-color);
  box-shadow: 0 0 0 2px rgba(52, 152, 219, 0.2);
}

/* 错误提示样式 */
.error {
  padding: 0.8rem;
  border-radius: 4px;
  background: #ffebee;
  border-left: 4px solid #ef5350;
  margin: 1rem 0;
  animation: error-in 0.3s ease-out;
}

@keyframes error-in {
  from {
    opacity: 0;
    transform: translateY(-10px);
  }
  to {
    opacity: 1;
    transform: translateY(0);
  }
}

/* 滚动条样式 */
::-webkit-scrollbar {
  width: 12px;  
  height: 12px;  
}

::-webkit-scrollbar-track {
  background: transparent;
}

::-webkit-scrollbar-thumb {
  background: var(--scrollbar-thumb);
  border-radius: 6px;  
  border: 3px solid transparent;  
  background-clip: padding-box;
}

::-webkit-scrollbar-thumb:hover {
  background: var(--scrollbar-thumb);
  border: 3px solid transparent;  
  background-clip: padding-box;
}



/* 文件拖放提示优化 */
.file-drop::before {
  content: '';
  position: absolute;
  inset: 0;
  border: 2px dashed var(--border-color);
  border-radius: inherit;
  pointer-events: none;
  opacity: 0;
  transition: opacity 0.3s;
}

.file-drop.dragging::before {
  opacity: 1;
  animation: border-dance 1s linear infinite;
}

@keyframes border-dance {
  0% { stroke-dashoffset: 0; }
  100% { stroke-dashoffset: 100; }
}

/* 编辑器工具栏 */
.editor-toolbar {
  display: flex;
  gap: 0.5rem;
  padding: 0.5rem;
  background: #f8f9fa;
  border: 1px solid var(--border-color);
  border-bottom: none;
  border-radius: 4px 4px 0 0;
}

.editor-toolbar button {
  padding: 0.25rem 0.5rem;
  border: 1px solid var(--border-color);
  border-radius: 4px;
  background: white;
  color: #666;
  cursor: pointer;
  transition: all 0.2s;
}

.editor-toolbar button:hover {
  background: #f1f2f6;
  border-color: #ccc;
}

/* 文件上传状态优化 */
.upload-item {
  display: flex;
  align-items: center;
  padding: 0.5rem;
  background: #f8f9fa;
  border-radius: 4px;
  margin: 0.5rem 0;
}

.upload-item .name {
  flex: 1;
  margin-right: 1rem;
}

.upload-item .status {
  font-size: 0.9em;
  color: #666;
}

.upload-item .status.success {
  color: #2ecc71;
}

.upload-item .status.error {
  color: #e74c3c;
}

/* 密码强度指示器 */
.password-strength {
  height: 3px;
  margin-top: 4px;
  border-radius: 2px;
  transition: all 0.3s;
}

.password-strength.weak {
  background: #e74c3c;
  width: 33.33%;
}

.password-strength.medium {
  background: #f1c40f;
  width: 66.66%;
}

.password-strength.strong {
  background: #2ecc71;
  width: 100%;
}

/* 文件大小限制提示 */
.size-limit-warning {
  display: flex;
  align-items: center;
  padding: 0.5rem;
  background: #fff3cd;
  border: 1px solid #ffeeba;
  border-radius: 4px;
  color: #856404;
  margin: 0.5rem 0;
  font-size: 0.9em;
}

.size-limit-warning::before {
  content: '⚠️';
  margin-right: 0.5rem;
}

/* 文本输入增强 */
.auto-resize-textarea {
  min-height: 100px;
  max-height: 500px;
  resize: vertical;
  transition: height 0.2s;
}

/* 文件类型图标 */
.file-type-icon {
  width: 24px;
  height: 24px;
  margin-right: 0.5rem;
  background-size: contain;
  background-repeat: no-repeat;
  background-position: center;
}

.file-type-icon.image { background-image: url('data:image/svg+xml,...'); }
.file-type-icon.document { background-image: url('data:image/svg+xml,...'); }
.file-type-icon.archive { background-image: url('data:image/svg+xml,...'); }

/* 链接复制成功动画 */
@keyframes copy-success {
  0% { transform: scale(1); }
  50% { transform: scale(1.2); }
  100% { transform: scale(1); }
}

.copy-success {
  animation: copy-success 0.3s ease-in-out;
}

/* 标签切换过渡效果 */
.tab-content {
  opacity: 0;
  transform: translateY(10px);
  transition: all 0.3s ease-out;
}

.tab-content.active {
  opacity: 1;
  transform: translateY(0);
}

/* 文件上传错误抖动效果 */
@keyframes upload-error-shake {
  0%, 100% { transform: translateX(0); }
  25% { transform: translateX(-5px); }
  75% { transform: translateX(5px); }
}

.upload-error {
  animation: upload-error-shake 0.3s ease-in-out;
}

/* 添加文件上传拖拽时的动画效果 */
.file-drop {
  position: relative;
  overflow: hidden;
}

.file-drop::after {
  content: '';
  position: absolute;
  inset: 0;
  background: linear-gradient(45deg, 
    rgba(52, 152, 219, 0.1) 25%,
    transparent 25%,
    transparent 50%,
    rgba(52, 152, 219, 0.1) 50%,
    rgba(52, 152, 219, 0.1) 75%,
    transparent 75%
  );
  background-size: 20px 20px;
  opacity: 0;
  transition: opacity 0.3s;
  pointer-events: none;
  animation: bg-scroll 0.5s linear infinite;
}

.file-drop.dragging::after {
  opacity: 1;
}

@keyframes bg-scroll {
  0% { background-position: 0 0; }
  100% { background-position: 20px 20px; }
}



/* 优化文件列表项动画 */
.file-item {
  animation: file-item-in 0.3s ease-out;
}

@keyframes file-item-in {
  from {
    opacity: 0;
    transform: translateY(-10px);
  }
  to {
    opacity: 1;
    transform: translateY(0);
  }
}

/* 优化复制按钮反馈 */
.copy-btn {
  position: relative;
}

.copy-btn::before {
  content: '已复制!';
  position: absolute;
  top: -30px;
  left: 50%;
  transform: translateX(-50%);
  background: rgba(0,0,0,0.8);
  color: white;
  padding: 4px 8px;
  border-radius: 4px;
  font-size: 12px;
  opacity: 0;
  transition: opacity 0.3s;
  pointer-events: none;
}

.copy-btn.copied::before {
  opacity: 1;
}

/* 优化密码输入框切换按钮 */
.password-toggle {
  opacity: 0.6;
  transition: opacity 0.2s;
}

.password-toggle:hover {
  opacity: 1;
}

/* 优化标签切换动画 */
.tab {
  position: relative;
}

.tab::after {
  content: '';
  position: absolute;
  bottom: 0;
  left: 50%;
  width: 0;
  height: 2px;
  background: var(--primary-color);
  transition: all 0.3s ease;
  transform: translateX(-50%);
}

.tab.active::after {
  width: 100%;
}

/* 优化错误提示动画 */
.error {
  animation: error-in 0.3s ease-out;
}

@keyframes error-in {
  from {
    opacity: 0;
    transform: translateY(-10px);
  }
  to {
    opacity: 1;
    transform: translateY(0);
  }
}

/* 优化加载状态显示 */
.loading {
  display: inline-flex;
  align-items: center;
  gap: 8px;
  color: #666;
}

.loading::after {
  content: '';
  width: 16px;
  height: 16px;
  border: 2px solid #f3f3f3;
  border-top: 2px solid var(--primary-color);
  border-radius: 50%;
  animation: spin 1s linear infinite;
}

@keyframes spin {
  0% { transform: rotate(0deg); }
  100% { transform: rotate(360deg); }
}

/* 优化按钮禁用状态 */
.btn:disabled {
  opacity: 0.6;
  cursor: not-allowed;
  position: relative;
}

.btn:disabled::after {
  content: '';
  position: absolute;
  inset: 0;
  background: repeating-linear-gradient(
    45deg,
    transparent,
    transparent 5px,
    rgba(255,255,255,0.1) 5px,
    rgba(255,255,255,0.1) 10px
  );
}

/* 文件预览样式 */
.file-preview {
  margin: 1rem 0;
  padding: 1rem;
  background: #f8f9fa;
  border-radius: 8px;
  border: 1px solid var(--border-color);
}

.file-preview img {
  max-width: 100%;
  max-height: 200px;
  object-fit: contain;
  margin: 0 auto;
  display: block;
}

.file-preview audio,
.file-preview video {
  width: 100%;
  max-width: 500px;
  margin: 0 auto;
  display: block;
}

/* 文件类型图标 */
.file-type-icon {
  width: 24px;
  height: 24px;
  margin-right: 0.5rem;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  background: #f1f2f6;
  border-radius: 4px;
  font-size: 14px;
}

/* 文件预览样式 */
.file-preview {
  margin: 1rem 0;
  padding: 1rem;
  background: #f8f9fa;
  border-radius: 8px;
  border: 1px solid var(--border-color);
}

.file-preview img {
  max-width: 100%;
  max-height: 200px;
  object-fit: contain;
  margin: 0 auto;
  display: block;
}

.file-preview audio,
.file-preview video {
  width: 100%;
  max-width: 500px;
  margin: 0 auto;
  display: block;
}

/* 文件类型图标 */
.file-type-icon {
  width: 24px;
  height: 24px;
  margin-right: 0.5rem;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  background: #f1f2f6;
  border-radius: 4px;
  font-size: 14px;
}

/* 基础进度条组件 */
.progress-bar {
  width: 100%;
  height: 8px;
  background: #e9ecef;
  border-radius: 4px;
  overflow: hidden;
  margin: 8px 0;
  box-shadow: inset 0 1px 2px rgba(0,0,0,0.05);
}

.progress-bar-inner {
  height: 100%;
  background: var(--primary-color);
  border-radius: inherit;
  transition: width 0.3s cubic-bezier(0.4, 0, 0.2, 1);
}

/* 带动画效果的上传进度条 */
.upload-progress-wrapper {
  position: relative;
  margin: 1rem 0;
  padding: 1rem;
  background: #f8f9fa;
  border-radius: 8px;
  border: 1px solid var(--border-color);
}

.upload-progress-bar {
  width: 100%;
  height: 6px;
  background: #e9ecef;
  border-radius: 3px;
  overflow: hidden;
  margin-bottom: 0.75rem;
}

.upload-progress-inner {
  height: 100%;
  background: var(--primary-color);
  border-radius: inherit;
  transition: width 0.3s cubic-bezier(0.4, 0, 0.2, 1);
  background-image: linear-gradient(
    45deg,
    rgba(255, 255, 255, .15) 25%,
    transparent 25%,
    transparent 50%,
    rgba(255, 255, 255, .15) 50%,
    rgba(255, 255, 255, .15) 75%,
    transparent 75%,
    transparent
  );
  background-size: 1rem 1rem;
  animation: progress-bar-stripes 1s linear infinite;
}

.upload-progress-info {
  display: flex;
  justify-content: space-between;
  align-items: center;
  font-size: 0.875rem;
  color: #666;
}

/* 上传状态文本 */
.progress-text {
  font-size: 0.875rem;
  color: #666;
  margin-top: 0.5rem;
  text-align: center;
}

/* 取消按钮 */
.cancel-btn {
  padding: 0.25rem 0.75rem;
  font-size: 0.875rem;
  color: #fff;
  background: #95a5a6;
  border: none;
  border-radius: 4px;
  cursor: pointer;
  transition: background-color 0.2s ease;
}

.cancel-btn:hover {
  background: #7f8c8d;
}

.cancel-btn:disabled {
  background: #bdc3c7;
  cursor: not-allowed;
  opacity: 0.7;
}

/* 进度条动画 */
@keyframes progress-bar-stripes {
  from { background-position: 1rem 0; }
  to { background-position: 0 0; }
}

/* 编辑器组件 */
.editor {
  flex: 1;
  padding: 1rem;
  border: 1px solid var(--border-color);
  border-radius: 4px;
  overflow-y: auto;
  position: relative;
  word-wrap: break-word;
  overflow-wrap: break-word;
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
  font-size: 16px;
  line-height: 1.6;
  white-space: pre-wrap;       /* 保留换行符并自动换行 */
  max-width: 100%;            /*  限制最大宽度 */
}

/* 预览区域的引用块样式 */
.preview blockquote {
  margin: 0;  /* 移除外边距 */
  padding: 0.4em 1em;  /* 使用相等的上下内边距 */
  color: var(--markdown-blockquote-text);
  border-left: 0.25em solid var(--markdown-blockquote-border);
  background: var(--markdown-blockquote-bg);
  border-radius: 0 4px 4px 0;
}

.preview blockquote p {
  margin: 0;  /* 移除段落边距 */
  line-height: 1.6;
}

/* 预览区域列表中的引用块 */
.preview li > blockquote {
  margin: 0.3em 0;  /* 在列表项中添加小的外边距 */
  width: calc(100% - 1em);
}

/* 预览区域连续引用块 */
.preview blockquote + blockquote {
  margin-top: 0.2em;
}

/* 预览区域基础样式 */
.preview {
  flex: 1;
  padding: 1rem;
  border: 1px solid var(--border-color);
  border-radius: 4px;
  overflow-y: auto;
  background: var(--markdown-preview-bg);
  color: var(--text-color);
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
  font-size: 16px;
  line-height: 1.6;
}

/* 确保预览区域的列表样式正确 */
.preview ul {
  list-style: none;
  padding-left: 2em;
  margin: 1em 0;
}

.preview ul li {
  position: relative;
  margin: 0.5em 0;
  padding-left: 0.5em;
  line-height: 1.6;
}

.preview ul li::before {
  content: "•";
  position: absolute;
  left: -1.5em;
  color: var(--text-color);
}

/* 添加文本输入框滚动条样式 */
.editor textarea::-webkit-scrollbar {
  width: 12px;  
  height: 12px;  
}

.editor textarea::-webkit-scrollbar-track {
  background: transparent;
}

.editor textarea::-webkit-scrollbar-thumb {
  background: var(--scrollbar-thumb);
  border-radius: 6px;  
  border: 3px solid transparent;  
  background-clip: padding-box;
}

.editor textarea::-webkit-scrollbar-thumb:hover {
  background: var(--scrollbar-thumb);
  border: 3px solid transparent;  
  background-clip: padding-box;
}

/* 添加主题切换按钮样式 */
.theme-toggle {
  position: fixed;
  top: 10px;
  left: 10px;  /* 改为左侧 */
  z-index: 1000;
  background: none;
  border: none;
  cursor: pointer;
  color: var(--text-color);
  font-size: 24px;
  padding: 8px;
  border-radius: 50%;
  transition: background-color 0.3s;
}

.theme-toggle:hover {
  background: var(--hover-bg);
}

/* 确保基础容器在所有尺寸下都能正常工作 */
.container {
  width: 100%;
  max-width: 1400px;
  margin: 0 auto;
  padding: clamp(1rem, 3vw, 2rem);  /* 使用 clamp 实现响应式内边距 */
  min-height: 100vh;                 /* 确保容器至少占满视口高度 */
  display: flex;
  flex-direction: column;
  align-items: center;               /* 内容水平居中 */
}

/* 响应式布局断点系统 */
/* 超大屏幕 (≥ 1400px) */
@media (min-width: 1400px) {
  .card {
    min-width: 1200px;
  }
}

/* 大屏幕 (1200-1399px) */
@media (max-width: 1399px) and (min-width: 1200px) {
  .card {
    min-width: 1000px;
  }
}

/* 中等屏幕 (992-1199px) */
@media (max-width: 1199px) and (min-width: 992px) {
  .card {
    min-width: 900px;
  }
}

/* 小屏幕 (768-991px) */
@media (max-width: 991px) and (min-width: 768px) {
  .card {
    min-width: 700px;
  }
  
  .editor-container {
    height: 500px;
  }
}

/* 平板和手机端通用样式 (≤ 768px) */
@media (max-width: 768px) {
  /* 容器和卡片样式 */
  .container {
    padding: 1rem;
  }

  .card {
    width: 100%;
    max-width: 100%;
    min-width: auto;
    padding: 1rem;
  }

  /* 编辑器相关 */
  .editor-container {
    flex-direction: column;
    height: auto;
    min-height: 200px;
    resize: vertical;
  }

  .editor, .preview {
    height: 300px;
    min-width: auto;
    width: 100%;
  }

  /* 内容样式 */
  .content {
    padding: 1rem;
  }

  .content pre {
    padding: 1rem;
    font-size: 0.85em;
  }

  .content table {
    font-size: 0.9em;
  }

  .content table th,
  .content table td {
    padding: 0.6em 0.8em;
  }
  
  .content table th:nth-child(1),
  .content table td:nth-child(1),
  .content table th:nth-child(2),
  .content table td:nth-child(2),
  .content table th:nth-child(3),
  .content table td:nth-child(3) {
    width: auto;
  }

  /* 其他UI组件 */
  .settings {
    grid-template-columns: 1fr;
  }

  .file-item {
    flex-direction: column;
    align-items: flex-start;
    gap: 0.5rem;
  }

  .file-item .size {
    margin: 0;
  }

  .admin-panel {
    right: 70px;  
  }

  .admin-content {
    width: 75%;
  }

  .share-list {
    margin-top: 0.8rem;
  }

  .share-item {
    padding: 0.8rem;
    margin-bottom: 0.8rem;
  }

  .share-item .type {
    font-size: 0.8rem;
  }

  .share-item .info {
    font-size: 0.8rem;
  }

  .share-item .actions {
    flex-direction: row;
    gap: 0.5rem;
  }

  .share-item .actions button {
    flex: 1;  /* 平均分配宽度 */
    padding: 0.4rem;  /* 减小内边距使按钮更紧凑 */
    font-size: 0.85rem;  /* 稍微减小字体大小 */
  }

  .link {
    flex-direction: column;
    gap: 0.5rem;
  }

  .link button {
    width: 100%;
  }

  .github-link {
    top: 10px;
    right: 10px;
  }
}

/* 平板设备 (481-767px) */
@media (max-width: 767px) and (min-width: 481px) {
  .card {
    min-width: auto;
    width: 95%;
    padding: 1.5rem;
  }

  .editor-container {
    flex-direction: column;
    height: 400px;
    min-height: 300px;  // 可以调整这个值
  }

  .editor, .preview {
    height: 400px;     // 可以调整这个值
    min-width: auto;
    width: 100%;
  }

  
  .admin-content {
    width: 55%;
    right: 0;
    padding: 1rem;
  }

  .admin-header {
    margin-bottom: 1rem;
  }

  .admin-header h2 {
    font-size: 1.2rem;
  }

  .storage-stats {
    padding: 0.8rem;
  }

  .storage-stats h3 {
    font-size: 0.9rem;
  }

  .storage-info {
    font-size: 0.8rem;
  }

  .share-list {
    margin-top: 0.8rem;
  }

  .share-item {
    padding: 0.8rem;
    margin-bottom: 0.8rem;
  }

  .share-item .type {
    font-size: 0.8rem;
  }

  .share-item .info {
    font-size: 0.8rem;
  }

  .share-item .actions {
    flex-direction: row;
    gap: 0.5rem;
  }

  .share-item .actions button {
    flex: 1;  /* 平均分配宽度 */
    padding: 0.4rem;  /* 减小内边距使按钮更紧凑 */
    font-size: 0.85rem;  /* 稍微减小字体大小 */
  }
}

/* 手机设备 (≤ 480px) */
@media (max-width: 480px) {

  .content {
    overflow-x: auto;
  }
  
  .content table {
    min-width: 100%;
  }

  /* 卡片样式 */
  .card {
    min-width: auto;
    width: 95%;
    padding: 0.8rem;
  }

  /* 编辑器相关 */
  .editor-container {
    height: 300px;
    min-height: 300px;
  }

  .editor, .preview {
    height: 300px;
    min-width: auto;
  }

  /* 标签页 */
  .tabs {
    flex-direction: column;
    gap: 0.5rem;
  }

  .tab {
    width: 100%;
    text-align: center;
  }

  /* 其他UI组件 */
  .file-drop {
    padding: 1rem;
  }

  .admin-content {
    width: 80%;
    right: 0;
    padding: 1rem;
  }

  .admin-header {
    margin-bottom: 1rem;
  }

  .admin-header h2 {
    font-size: 1.2rem;
  }

  .upload-buttons {
    flex-direction: column;
    gap: 0.5rem;
  }

  .upload-buttons button {
    width: 100%;
    padding: 0.8rem;
  }

  .refresh-list {
    padding: 0.8rem;
  }

  .admin-stats {
    grid-template-columns: repeat(2, 1fr);
    gap: 0.5rem;
  }

  .stat-card {
    padding: 0.8rem;
  }

  .stat-value {
    font-size: 1.2rem;
  }

  .stat-label {
    font-size: 0.8rem;
  }

  .storage-stats {
    padding: 0.8rem;
  }

  .storage-stats h3 {
    font-size: 0.9rem;
  }

  .storage-info {
    font-size: 0.8rem;
  }

  .share-list {
    margin-top: 0.8rem;
  }

  .share-item {
    padding: 0.8rem;
    margin-bottom: 0.8rem;
  }

  .share-item .type {
    font-size: 0.8rem;
  }

  .share-item .info {
    font-size: 0.8rem;
  }

  .share-item .actions {
    flex-direction: row;
    gap: 0.5rem;
  }

  .share-item .actions button {
    flex: 1;  /* 平均分配宽度 */
    padding: 0.4rem;  /* 减小内边距使按钮更紧凑 */
    font-size: 0.85rem;  /* 稍微减小字体大小 */
  }

  .file-item {
    flex-direction: column;
    align-items: flex-start;
  }
  
  .file-item .actions {
    width: 100%;
    margin-top: 0.5rem;
  }
  
  .file-preview {
    padding: 0.5rem;
  }
}
`;

// Vue 应用代码
const appScript = `
const { createApp, ref, computed, onMounted, nextTick, onUnmounted, watch } = Vue;  // 添加 onMounted, onUnmounted 和 watch

createApp({
  setup() {
    const activeTab = ref('paste');
    const content = ref('');
    const isMarkdown = ref(true);
    const password = ref('');
    const expiresIn = ref('1d');
    const files = ref([]);
    const isDragging = ref(false);
    const result = ref(null);
    const error = ref(null);
    const uploadStatus = ref('');  // 用于显示上传状态
    const uploadProgress = ref(0);  // 用于显示上传进度
    const isUploading = ref(false);  // 添加这行
    const uploadingFiles = ref([]);  // 添加这行
    // 添加管理员相关的状态
    const isAdmin = ref(false);
    const showAdminLogin = ref(false);
    const adminUsername = ref('');
    const adminPassword = ref('');
    const adminError = ref('');
    const shares = ref([]);
    const shareFilter = ref('all');  // 用于过滤分享列表
    const showPassword = ref(false);  // 控制密码显示/隐藏
    const editorRef = ref(null);// 添加编辑器和预览区域的引用
    const previewRef = ref(null);
    let isEditorScrolling = false;
    let isPreviewScrolling = false;
    const showDeleteConfirm = ref(false);//确认删除
    const deleteTarget = ref(null);
    const isRefreshing = ref(false);  // 添加刷新状态
    const customId = ref(''); // 添加自定义ID输入框的值
    // 添加新的状态变量
    const allowTextUpload = ref(false);  // 控制文本上传
    const allowFileUpload = ref(false);  // 控制文件上传
    const showPasswordDialog = ref(false);
    const passwordTarget = ref(null);
    const newPassword = ref('');
    const passwordError = ref('');
    // 添加自动保存的状态和方法
    const lastSavedContent = ref(''); // 添加最后保存的内容
    const isFileEditing = ref(false);
    const editFileExpiresIn = ref('1d');
    const editFileMaxViews = ref('0');

    // 在 setup() 函数中添加新的状态
    const storageInfo = ref({
      used: 0,
      total: ${MAX_TOTAL_STORAGE},
      percentage: 0
    });

    // 添加计算存储空间的方法
    const calculateStorage = async () => {
      try {
        const credentials = localStorage.getItem('adminCredentials');
        if (!credentials) {
          throw new Error('未登录');
        }

        const response = await fetch('/api/admin/storage', {
          headers: {
            'Authorization': 'Basic ' + credentials
          }
        });

        if (!response.ok) {
          throw new Error('获取存储信息失败');
        }

        const data = await response.json();
        storageInfo.value = data.storage;
      } catch (e) {
        console.error('Error calculating storage:', e);
      }
    };

    // 打开修改密码对话框
    const showChangePassword = (share) => {
      passwordTarget.value = share;
      newPassword.value = '';
      passwordError.value = '';
      showPasswordDialog.value = true;
    };

    // 修改密码
    const changePassword = async () => {
      if (!passwordTarget.value) return;
      
      try {
        const credentials = localStorage.getItem('adminCredentials');
        if (!credentials) {
          throw new Error('未登录');
        }
        const response = await fetch('/api/admin/' + passwordTarget.value.type + '/' + passwordTarget.value.id + '/password', {
          method: 'PUT',
          headers: {
            'Authorization': 'Basic ' + credentials,
            'Content-Type': 'application/json'
          },
          body: JSON.stringify({
            password: newPassword.value
          })
        });

        if (!response.ok) {
          if (response.status === 401) {
            isAdmin.value = false;
            localStorage.removeItem('adminCredentials');
            throw new Error('登录已过期，请重新登录');
          }
          throw new Error('修改密码失败');
        }

        showPasswordDialog.value = false;
        passwordTarget.value = null;
        await fetchShares();
      } catch (err) {
        passwordError.value = err.message;
      }
    };

    // 修改刷新函数
    const refreshShares = async () => {
        if (isRefreshing.value) return;
        
        try {
            isRefreshing.value = true;
            
            // 并行执行两个请求
            const credentials = localStorage.getItem('adminCredentials');
            if (!credentials) {
                throw new Error('未登录');
            }

            const [sharesResponse, statusResponse] = await Promise.all([
                // 获取分享列表
                fetch('/api/admin/shares', {
                    headers: {
                        'Authorization': 'Basic ' + credentials
                    }
                }),
                // 获取上传状态
                fetch('/api/admin/upload-status', {
                    headers: {
                        'Authorization': 'Basic ' + credentials
                    }
                })
            ]);

            // 处理分享列表响应
            if (!sharesResponse.ok) {
                if (sharesResponse.status === 401) {
                    isAdmin.value = false;
                    localStorage.removeItem('adminCredentials');
                    throw new Error('登录已过期，请重新登录');
                }
                throw new Error('获取分享列表失败');
            }
            const sharesData = await sharesResponse.json();
            shares.value = sharesData.shares;

            // 处理上传状态响应
            if (statusResponse.ok) {
                const statusData = await statusResponse.json();
                // 更新按钮状态
                const adminControls = document.querySelectorAll('.control-btn');
                adminControls.forEach(btn => {
                    if (btn.textContent.includes('文本上传')) {
                        allowTextUpload.value = statusData.textUpload;
                        btn.classList.toggle('active', statusData.textUpload);
                    } else if (btn.textContent.includes('文件上传')) {
                        allowFileUpload.value = statusData.fileUpload;
                        btn.classList.toggle('active', statusData.fileUpload);
                    }
                });
            }

        } catch (err) {
            adminError.value = err.message;
        } finally {
            isRefreshing.value = false;
        }
    };

    // 修改 onMounted，移除自动刷新
    onMounted(async () => {
        const credentials = localStorage.getItem('adminCredentials');
        if (credentials) {
            isAdmin.value = true;
            
            try {
              // 获取上传状态
              const response = await fetch('/api/admin/upload-status', {
                headers: {
                  'Authorization': 'Basic ' + credentials
                }
              });
              
              if (response.ok) {
                const statusData = await response.json();
                allowTextUpload.value = statusData.textUpload;
                allowFileUpload.value = statusData.fileUpload;
                
                // 更新按钮状态
                const adminControls = document.querySelectorAll('.control-btn');
                adminControls.forEach(btn => {
                  if (btn.textContent.includes('文本上传')) {
                    btn.classList.toggle('active', statusData.textUpload);
                  } else if (btn.textContent.includes('文件上传')) {
                    btn.classList.toggle('active', statusData.fileUpload);
                  }
                });
              }
              
              // 获取分享列表
              await fetchShares();
            } catch (err) {
              console.error('Error fetching initial status:', err);
            }
        }
    });




    // 确认删除
    const confirmDelete = (share) => {
        deleteTarget.value = share;
        showDeleteConfirm.value = true;
    };

    const executeDelete = async () => {
        if (!deleteTarget.value) return;
        
        try {
            await deleteShare(deleteTarget.value.id, deleteTarget.value.type);
            showDeleteConfirm.value = false;
            deleteTarget.value = null;
        } catch (err) {
            adminError.value = err.message;
        }
    };

    // 在 appScript 中修改滚动处理函数
    const handleEditorScroll = (e) => {
    requestAnimationFrame(() => {
        const editor = e.target;
        const preview = previewRef.value;
        if (!preview) return;

        const percentage = editor.scrollTop / (editor.scrollHeight - editor.clientHeight);
        preview.scrollTop = percentage * (preview.scrollHeight - preview.clientHeight);
    });
    };

    // 处理预览区域滚动
    const handlePreviewScroll = () => {
        // 预览框滚动时不做任何处理
    };


    // 在 appScript 中添加防抖函数
    const debounce = (fn, delay) => {
    let timer = null;
    return function(...args) {
        if (timer) clearTimeout(timer);
        timer = setTimeout(() => {
            fn.apply(this, args);
        }, delay);
    };
    };

    // 使用防抖处理预览内容更新
    const preview = computed(() => {
    if (!content.value) return '';
    if (!isMarkdown.value) return content.value;
    
    try {
        const rendered = marked.parse(content.value);
        // 使用 nextTick 确保在 DOM 更新后应用代码高亮和数学公式渲染
        nextTick(() => {
            // 获取预览容器
            const previewContainer = document.querySelector('.preview');
            if (!previewContainer) return;

            // 检查是否为暗色主题
            const isDarkTheme = document.documentElement.getAttribute('data-theme') === 'dark';
            
            // 设置预览容器的基本样式
            previewContainer.style.background = isDarkTheme ? 'var(--markdown-bg)' : 'var(--markdown-preview-bg)';
            previewContainer.style.color = isDarkTheme ? 'var(--markdown-text)' : 'var(--text-color)';

            // 代码块样式处理
            previewContainer.querySelectorAll('pre').forEach(pre => {
                // 直接设置 pre 元素的背景色
                pre.style.background = isDarkTheme ? 'var(--markdown-code-block-bg)' : 'var(--markdown-code-bg)';
                
                // 处理内部的 code 元素
                const code = pre.querySelector('code');
                if (code) {
                    hljs.highlightBlock(code);
                    code.style.color = isDarkTheme ? 'var(--markdown-code-text)' : 'var(--text-color)';
                    code.style.background = 'transparent'; // 确保 code 元素背景是透明的
                }
            });

            // 其他 Markdown 元素的样式
            if (isDarkTheme) {
                previewContainer.querySelectorAll('h1, h2, h3, h4, h5, h6').forEach(heading => {
                    heading.style.color = 'var(--markdown-heading-text)';
                });

                previewContainer.querySelectorAll('a').forEach(link => {
                    link.style.color = 'var(--markdown-link)';
                });

                previewContainer.querySelectorAll('blockquote').forEach(quote => {
                    quote.style.background = 'var(--markdown-blockquote-bg)';
                    quote.style.color = 'var(--markdown-blockquote-text)';
                    quote.style.borderLeftColor = 'var(--markdown-blockquote-border)';
                });

                previewContainer.querySelectorAll('table').forEach(table => {
                    table.style.borderColor = 'var(--markdown-table-border)';
                    table.querySelectorAll('tr:nth-child(2n)').forEach(row => {
                        row.style.background = 'var(--markdown-table-alt-bg)';
                    });
                });
            }

            // 渲染数学公式
            renderMathInElement(previewContainer, {
                delimiters: [
                    {left: "$$", right: "$$", display: true},
                    {left: "$", right: "$", display: false}
                ],
                throwOnError: false
            });
        });
        return rendered;
    } catch (err) {
        return '渲染出错: ' + err.message;
    }
    });


// 管理员登录
    const adminLogin = async function() {
      try {
        adminError.value = '';
        const response = await fetch('/api/admin/login', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json'
          },
          body: JSON.stringify({
            username: adminUsername.value,
            password: adminPassword.value
          })
        });

        const data = await response.json();
        if (!response.ok) {
          throw new Error(data.message || '登录失败');
        }

        const credentials = btoa(adminUsername.value + ':' + adminPassword.value);
        localStorage.setItem('adminCredentials', credentials);
        isAdmin.value = true;
        showAdminLogin.value = false;
        
        // 获取上传状态
        const statusResponse = await fetch('/api/admin/upload-status', {
          headers: {
            'Authorization': 'Basic ' + credentials
          }
        });
        
        if (statusResponse.ok) {
          const statusData = await statusResponse.json();
          allowTextUpload.value = statusData.textUpload;
          allowFileUpload.value = statusData.fileUpload;
        }
        
        await fetchShares();
      } catch (err) {
        adminError.value = err.message;
      }
    };

    // 获取分享列表
    const fetchShares = async function() {
      try {
        const credentials = localStorage.getItem('adminCredentials');
        if (!credentials) {
          throw new Error('未登录');
        }

        console.log('Fetching shares with credentials');  // 添加调试日志

        const response = await fetch('/api/admin/shares', {
          headers: {
            'Authorization': 'Basic ' + credentials,
            'Content-Type': 'application/json'
          }
        });

        console.log('Shares response status:', response.status);  // 添加调试日志
       
        if (!response.ok) {
          if (response.status === 401) {
            isAdmin.value = false;
            localStorage.removeItem('adminCredentials');
            throw new Error('登录已过期，请重新登录');
          }
          throw new Error('获取分享列表失败');
        }

        const data = await response.json();
        console.log('Shares data:', data);  // 添加调试日志
        shares.value = data.shares;
        await calculateStorage(); // 计算存储空间
      } catch (err) {
        console.error('Fetch shares error:', err);  // 添加调试日志
        adminError.value = err.message;
      }
    };

    // 删除分享
    const deleteShare = async function(id, type) {
      try {
        const credentials = localStorage.getItem('adminCredentials');
        if (!credentials) {
          throw new Error('未登录');
        }

        const response = await fetch('/api/admin/' + type + '/' + id, {
          method: 'DELETE',
          headers: {
            'Authorization': 'Basic ' + credentials
          }
        });

        if (!response.ok) {
          if (response.status === 401) {
            isAdmin.value = false;
            localStorage.removeItem('adminCredentials');
            throw new Error('登录已过期，请重新登录');
          }
          throw new Error('删除失败');
        }

        shares.value = shares.value.filter(s => s.id !== id);
      } catch (err) {
        adminError.value = err.message;
      }
    };

    // 退出登录
    const adminLogout = function() {
      isAdmin.value = false;
      shares.value = [];
      adminError.value = '';
      localStorage.removeItem('adminCredentials');
      // 不要清除上传控制状态
      // localStorage.removeItem('allowTextUpload');
      // localStorage.removeItem('allowFileUpload');
      showAdminPanel.value = false; // 关闭面板
    };

    // 检查是否已登录
    onMounted(function() {
    const credentials = localStorage.getItem('adminCredentials');
      if (credentials) {
        isAdmin.value = true;
        fetchShares();
      }
    });

    // 过滤分享列表
    const filteredShares = computed(() => {
      if (shareFilter.value === 'all') return shares.value;
      return shares.value.filter(s => s.type === shareFilter.value);
    });

    // 格式化时间
    const formatDate = function(dateStr) {
      // 如果日期为 null 或 undefined，返回"永不过期"
      if (!dateStr) {
        return '永不过期';
      }
      
      const date = new Date(dateStr);
      // 检查是否为有效日期
      if (isNaN(date.getTime())) {
        return '永不过期';
      }
      
      return date.toLocaleString();
    };




    // 添加 isExpired 函数
    const isExpired = (expiryTime) => {
      return new Date() > new Date(expiryTime);
    };
    // 修改有效分享数量的计算
    const validSharesCount = computed(() => {
      return shares.value.filter(s => !isExpired(s.expiresAt)).length;
    });


    // 格式化文件大小
    const formatSize = (bytes) => {
      const units = ['B', 'KB', 'MB', 'GB'];
      let size = bytes;
      let unitIndex = 0;
      while (size >= 1024 && unitIndex < units.length - 1) {
        size /= 1024;
        unitIndex++;
      }
      return size.toFixed(1) + ' ' + units[unitIndex];
    };

    // 处理文件拖放
    const handleDrop = (e) => {
      e.preventDefault();
      isDragging.value = false;
      
      // 在添加新文件前先清理当前预览
      clearPreview();
      
      const droppedFiles = Array.from(e.dataTransfer.files);
      files.value = [...files.value, ...droppedFiles];
      
      // 生成预览
      droppedFiles.forEach(file => {
        if (file.type.startsWith('image/')) {
          previewUrl.value = URL.createObjectURL(file);
          previewType.value = 'image';
        } else if (file.type.startsWith('audio/')) {
          previewUrl.value = URL.createObjectURL(file);
          previewType.value = 'audio';
        } else if (file.type.startsWith('video/')) {
          previewUrl.value = URL.createObjectURL(file);
          previewType.value = 'video';
        }
      });
    };

    // 在 setup() 函数中添加检查文件大小的函数
    const checkFilesSize = (fileList) => {
      const totalSize = fileList.reduce((sum, file) => sum + file.size, 0);
      if (totalSize > MAX_FILE_SIZE) {
        const maxSizeMB = (MAX_FILE_SIZE / 1024 / 1024).toFixed(1);
        error.value = '文件总大小超过限制(' + maxSizeMB + 'MB)';
        return false;
      }
      return true;
    };

    // 修改 handleFileSelect 函数
    const handleFileSelect = (e) => {
      const selectedFiles = Array.from(e.target.files);
      
      // 在添加新文件前先清理当前预览
      clearPreview();
      
      files.value = [...files.value, ...selectedFiles];
      
      // 生成预览
      selectedFiles.forEach(file => {
        if (file.type.startsWith('image/')) {
          previewUrl.value = URL.createObjectURL(file);
          previewType.value = 'image';
        } else if (file.type.startsWith('audio/')) {
          previewUrl.value = URL.createObjectURL(file);
          previewType.value = 'audio';
        } else if (file.type.startsWith('video/')) {
          previewUrl.value = URL.createObjectURL(file);
          previewType.value = 'video';
        }
      });
    };

    // 移除文件
    const removeFile = (index) => {
      // 如果要移除的是当前正在预览的文件,清理预览
      const file = files.value[index];
      if (file && (
        (file.type.startsWith('image/') && previewType.value === 'image') ||
        (file.type.startsWith('audio/') && previewType.value === 'audio') ||
        (file.type.startsWith('video/') && previewType.value === 'video')
      )) {
        clearPreview();
      }
      
      // 移除文件
      files.value.splice(index, 1);
      
      // 如果没有文件了,确保清理预览
      if (files.value.length === 0) {
        clearPreview();
      }
    };

    // 提交粘贴内容
    const submitPaste = async () => {
      // 检查是否允许上传
      if (!allowTextUpload.value) {
        error.value = '文本上传功能已关闭';
        return;
      }
      try {
        error.value = null;
        const response = await fetch('/api/paste', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({
            content: content.value,
            password: password.value,
            expiresIn: expiresIn.value,
            isMarkdown: isMarkdown.value,
            customId: customId.value,
            maxViews: maxViews.value ? parseInt(maxViews.value) : 0 // 添加这行
          }),
        });

        const data = await response.json();
        
        if (!response.ok) {
          error.value = data.message || '提交失败';
          return;
        }
        
        result.value = {
          type: 'paste',
          url: window.location.origin + '/share/paste/' + data.id,
        };
        
        // 成功后立即刷新分享列表
        if (isAdmin.value) {
          await fetchShares();
        }
        
        // 成功提交后清除保存的内容
        localStorage.removeItem('autosaved_content');
        lastSavedContent.value = '';
      } catch (err) {
        error.value = err.message;
      }
    };


    


    // 上传文件
    const uploadFiles = async () => {
      // 检查是否允许上传
      if (!allowFileUpload.value) {
        error.value = '文件上传功能已关闭';
        return;
      }
      try {
        error.value = null;
        
        // 添加: 验证自定义ID的格式
        if (customId.value && !/^[a-zA-Z0-9-_]+$/.test(customId.value)) {
          error.value = '自定义链接后缀只能包含字母、数字、横线和下划线';
          return;
        }
        
        // 检查文件总大小
        const totalSize = files.value.reduce((sum, file) => sum + file.size, 0);
        if (totalSize > window.APP_CONFIG.MAX_FILE_SIZE) {
          const maxSizeMB = (window.APP_CONFIG.MAX_FILE_SIZE / 1024 / 1024).toFixed(1);
          error.value = '文件总大小超过限制(' + maxSizeMB + 'MB)';
          return;
        }

        if (!files.value || files.value.length === 0) {
          error.value = '请选择要上传的文件';
          return;
        }

        // 如果是多文件上传提供了自定义ID，显示错误
        if (files.value.length > 1 && customId.value) {
          error.value = '多文件上传时不支持自定义链接';
          return;
        }

        // 添加: 如果提供了自定义ID，先检查是否已存在
        if (customId.value) {
          try {
            // 先检查文本分享
            const pasteResponse = await fetch('/api/paste/' + customId.value);
            // 修改这里：同时处理 200 和 401 状态码
            if (pasteResponse.ok || pasteResponse.status === 401) {
              error.value = '该链接后缀已被用于文本分享，请更换一个';
              isUploading.value = false;
              uploadStatus.value = '';
              uploadProgress.value = 0;
              return;
            }
            
            // 再检查文件分享
            const fileResponse = await fetch('/api/file/' + customId.value);
            // 修改这里：同时处理 200 和 401 状态码
            if (fileResponse.ok || fileResponse.status === 401) {
              error.value = '该链接后缀已被用于文件分享，请更换一个';
              isUploading.value = false;
              uploadStatus.value = '';
              uploadProgress.value = 0;
              return;
            }
          } catch (e) {
            // 如果是404错误，说明ID不存在，可以继续
            if (!e.response || e.response.status !== 404) {
              error.value = '检查链接后缀时出错，请重试';
              isUploading.value = false;
              uploadStatus.value = '';
              uploadProgress.value = 0;
              return;
            }
          }
        }

        uploadStatus.value = '正在上传...';
        isUploading.value = true;
        uploadProgress.value = 0;

        // 初始化上传列表
        uploadingFiles.value = files.value.map(file => ({
          name: file.name,
          status: 'loading',
          statusText: '准备上传...'
        }));

        const formData = new FormData();
        files.value.forEach(file => formData.append('files', file));
        
        if (password.value) {
          formData.append('password', password.value);
        }
        formData.append('expiresIn', expiresIn.value);
        if (customId.value && files.value.length === 1) {
          formData.append('customId', customId.value);
        }
        formData.append('maxViews', maxViews.value || '0'); // 添加这行

        // 创建 XMLHttpRequest 来监控上传进度
        const xhr = new XMLHttpRequest();
        uploadXHR.value = xhr; // 保存 xhr 引用以便取消上传
        
        // 添加取消上传的处理
        xhr.upload.addEventListener('abort', () => {
          isUploading.value = false;
          uploadStatus.value = '已取消上传';
          uploadProgress.value = 0;
          setTimeout(() => {
            uploadStatus.value = '';
          }, 2000);
        });

        xhr.upload.onprogress = (e) => {
          if (e.lengthComputable) {
            if (uploadStartTime.value === 0) {
              uploadStartTime.value = Date.now();
            }
            uploadProgress.value = (e.loaded / e.total) * 100;
            calculateSpeed(e.loaded);
          }
        };

        // 包装成 Promise
        const response = await new Promise((resolve, reject) => {
          xhr.onload = () => {
            if (xhr.status >= 200 && xhr.status < 300) {
              resolve(new Response(xhr.response, {
                status: xhr.status,
                headers: {
                  'Content-Type': 'application/json'
                }
              }));
            } else {
              try {
                const errorData = JSON.parse(xhr.response);
                if(errorData.message && errorData.message.includes('存储空间')) {
                  reject(new Error(errorData.message));
                } else {
                  reject(new Error('Upload failed'));
                }
              } catch(e) {
                reject(new Error('Upload failed'));
              }
            }
          };
          xhr.onerror = () => reject(new Error('Network error'));
          xhr.open('POST', '/api/file');
          xhr.send(formData);
        });

        const data = await response.json();
        
        if (!response.ok || data.status === 'error') {
        throw new Error(data.message || '上传失败');
        }

        // 更新上传状态
        data.files.forEach(file => {
        const uploadingFile = uploadingFiles.value.find(f => f.name === file.filename);
        if (uploadingFile) {
            uploadingFile.status = file.status;
            uploadingFile.statusText = file.status === 'success' ? '上传成功' : file.error;
        }
        });

        const successFiles = data.files.filter(f => f.status === 'success');
        

        if (successFiles.length > 0) {
            result.value = {
                type: 'files',
                files: successFiles.map(file => ({
                    url: window.location.origin + '/share/file/' + file.fileId,
                    filename: file.filename,
                    directDownloadUrl: window.location.origin + '/download/' + file.fileId  // 直链
                }))
            };
        }

        // 成功后立即刷新分享列表
        if (isAdmin.value) {
          await fetchShares();
        }

        if (successFiles.length === 0) {
        throw new Error('没有文件上传成功');
        }
        
        uploadStatus.value = '上传成功！';
        
        result.value = {
            type: 'files',
            files: successFiles.map(file => ({
                url: window.location.origin + '/share/file/' + file.fileId,
                filename: file.filename,
                directDownloadUrl: window.location.origin + '/download/' + file.fileId  // 直链
            }))
        };

        // 清空文件列表
        setTimeout(() => {
        files.value = [];
        uploadingFiles.value = [];
        uploadStatus.value = '';
        }, 3000);

        if (successFiles.length > 0) {
          await calculateStorage(); // 计算存储空间
        }

      } catch (err) {
        error.value = err.message;
        uploadStatus.value = '上传失败: ' + err.message;
      } finally {
        isUploading.value = false;
        uploadStartTime.value = 0;
        uploadSpeed.value = 0;
        uploadXHR.value = null; // 清除 xhr 引用
      }
    };

    // 复制链接
    const copyUrl = async (url) => {
      try {
        await navigator.clipboard.writeText(url);
        
        // 创建提示元素
        const toast = document.createElement('div');
        toast.className = 'copy-toast';
        toast.textContent = '链接已复制到剪贴板';
        document.body.appendChild(toast);
        
        // 显示提示
        setTimeout(() => toast.classList.add('show'), 10);
        
        // 2秒后隐藏并移除提示
        setTimeout(() => {
          toast.classList.remove('show');
          setTimeout(() => document.body.removeChild(toast), 300);
        }, 2000);
      } catch (err) {
        // 复制失败时仍然使用 alert
        alert('复制失败，请手动复制');
      }
    };

    // 添加控制函数
    const toggleTextUpload = async () => {
      try {
        const newStatus = !allowTextUpload.value;
        const credentials = localStorage.getItem('adminCredentials');
        if (credentials) {
          const response = await fetch('/api/admin/upload-status', {
            method: 'PUT',
            headers: {
              'Authorization': 'Basic ' + credentials,
              'Content-Type': 'application/json'
            },
            body: JSON.stringify({
              textUpload: newStatus,
              fileUpload: allowFileUpload.value
            })
          });
          
          if (!response.ok) {
            throw new Error('更新状态失败');
          }
          allowTextUpload.value = newStatus;
          // 更新按钮状态
          const btn = document.querySelector('.control-btn:nth-child(1)');
          if (btn) {
              btn.classList.toggle('active', newStatus);
          }
        }
      } catch (err) {
        console.error('Error updating text upload status:', err);
      }
    };

    const toggleFileUpload = async () => {
      try {
        const newStatus = !allowFileUpload.value;
        const credentials = localStorage.getItem('adminCredentials');
        if (credentials) {
          const response = await fetch('/api/admin/upload-status', {
            method: 'PUT',
            headers: {
              'Authorization': 'Basic ' + credentials,
              'Content-Type': 'application/json'
            },
            body: JSON.stringify({
              textUpload: allowFileUpload.value,
              fileUpload: newStatus
            })
          });
          
          if (!response.ok) {
            throw new Error('更新状态失败');
          }
          allowFileUpload.value = newStatus;
          // 更新按钮状态
          const btn = document.querySelector('.control-btn:nth-child(2)');
          if (btn) {
              btn.classList.toggle('active', newStatus);
          }
        }
      } catch (err) {
        console.error('Error updating file upload status:', err);
      }
    };

    // 添加格式化存储空间的方法
    const formatStorageSize = (bytes) => {
      const units = ['B', 'KB', 'MB', 'GB'];
      let size = bytes;
      let unitIndex = 0;
      while (size >= 1024 && unitIndex < units.length - 1) {
        size /= 1024;
        unitIndex++;
      }
      return size.toFixed(2) + ' ' + units[unitIndex];
    };

    // 在 setup() 中添加新的状态
    const showAdminPanel = ref(false); // 默认不显示面板

    // 添加文件预览相关状态
    const previewUrl = ref('');
    const previewType = ref('');
    
    // 获取文件类型图标
    const getFileTypeIcon = (file) => {
      const type = file.type.split('/')[0];
      switch(type) {
        case 'image': return '🖼️';
        case 'audio': return '🎵';
        case 'video': return '🎬';
        case 'text': return '📄';
        case 'application': return '📦';
        default: return '📎';
      }
    };
    
    // 清理预览
    const clearPreview = () => {
      if (previewUrl.value) {
        URL.revokeObjectURL(previewUrl.value);
        previewUrl.value = '';
        previewType.value = '';
      }
    };
    
    onUnmounted(() => {
      clearPreview();
    });

    // 在 setup() 函数中添加上传速率相关的状态
    const uploadSpeed = ref(0);
    const uploadStartTime = ref(0);
    const uploadedSize = ref(0);

    // 在 setup() 函数中添加计算上传速率的函数
    const calculateSpeed = (loaded) => {
      const now = Date.now();
      const duration = (now - uploadStartTime.value) / 1000; // 转换为秒
      if (duration > 0) {
        // 计算这个时间段内的平均速度
        uploadSpeed.value = loaded / duration;
      }
    };

    // 在 setup() 函数中添加格式化速率的函数
    const formatSpeed = function(bytesPerSecond) {
      if (bytesPerSecond < 1024) {
        return bytesPerSecond.toFixed(1) + ' B/s';
      }
      if (bytesPerSecond < 1024 * 1024) {
        return (bytesPerSecond / 1024).toFixed(1) + ' KB/s';
      }
      return (bytesPerSecond / (1024 * 1024)).toFixed(1) + ' MB/s';
    };

    // 在 setup() 函数中添加错误提示计时器
    const errorTimer = ref(null);

    // 添加设置错误信息的函数
    const setError = (message) => {
      // 清除之前的计时器
      if (errorTimer.value) {
        clearTimeout(errorTimer.value);
      }
      
      // 设置错误信息
      error.value = message;
      
      // 3秒后自动清除错误信息
      errorTimer.value = setTimeout(() => {
        error.value = null;
        errorTimer.value = null;
      }, 3000);
    };


    // 在组件卸载时清理计时器
    onUnmounted(() => {
      if (errorTimer.value) {
        clearTimeout(errorTimer.value);
      }
    });

    // 修改 error 的定义，添加 watch
    watch(error, (newError) => {
      if (newError) {
        // 清除之前的计时器
        if (errorTimer.value) {
          clearTimeout(errorTimer.value);
        }
        
        // 设置新的计时器，3秒后清除错误信息
        errorTimer.value = setTimeout(() => {
          error.value = null;
          errorTimer.value = null;
        }, 3000);
      }
    });

    // 在 setup() 函数中添加状态
    const abortController = ref(null); // 添加用于取消上传的控制器

    // 添加取消上传的方法
    const cancelUpload = () => {
      if (uploadXHR.value) {
        uploadXHR.value.abort();
        uploadStatus.value = '已取消上传';
        isUploading.value = false;
        uploadProgress.value = 0;
        setTimeout(() => {
          uploadStatus.value = '';
        }, 2000);
      }
    };

    // 在 setup() 中添加 uploadXHR ref
    const uploadXHR = ref(null);

    // 在 Vue 应用的 setup() 函数中添加状态
    const maxViews = ref(''); // 添加可打开次数的状态

    // 在 shareAppScript 中的 setup() 函数里添加复制功能
    const copyContent = async () => {
      try {
        await navigator.clipboard.writeText(content.value);
        
        // 创建提示元素
        const toast = document.createElement('div');
        toast.className = 'copy-toast';
        toast.textContent = '内容已复制到剪贴板';
        document.body.appendChild(toast);
        
        // 显示提示
        setTimeout(() => toast.classList.add('show'), 10);
        
        // 2秒后隐藏并移除提示
        setTimeout(() => {
          toast.classList.remove('show');
          setTimeout(() => document.body.removeChild(toast), 300);
        }, 2000);
      } catch (err) {
        error.value = '复制失败，请手动复制';
      }
    };

    // 在 appScript 的 setup 函数中添加，默认主题为light
    const currentTheme = ref(localStorage.getItem('theme') || 'light'); 

    // 检测系统主题
    const prefersDark = window.matchMedia('(prefers-color-scheme: dark)');

    // 设置主题的函数
    const setTheme = (theme) => {
      if (theme === 'auto') {
        // 跟随系统
        document.documentElement.setAttribute('data-theme', 
          prefersDark.matches ? 'dark' : 'light'
        );
      } else {
        document.documentElement.setAttribute('data-theme', theme);
      }
      localStorage.setItem('theme', theme);
      currentTheme.value = theme;

      // 主题切换后重新处理代码块
      setTimeout(() => {
        // 处理主内容区域的代码块
        const contentContainer = document.querySelector('.content');
        if (contentContainer) {
          contentContainer.querySelectorAll('pre code').forEach((block) => {
            // 检查是否已经添加了复制按钮
            if (!block.parentElement.parentElement.classList.contains('code-block-wrapper')) {
              const wrapper = document.createElement('div');
              wrapper.className = 'code-block-wrapper';
              
              const pre = block.parentElement;
              pre.parentNode.insertBefore(wrapper, pre);
              wrapper.appendChild(pre);
              
              const copyButton = document.createElement('button');
              copyButton.className = 'code-copy-btn';
              copyButton.textContent = '复制';
              wrapper.appendChild(copyButton);
              
              copyButton.addEventListener('click', async () => {
                try {
                  await navigator.clipboard.writeText(block.textContent);
                  copyButton.textContent = '已复制!';
                  copyButton.classList.add('copied');
                  
                  setTimeout(() => {
                    copyButton.textContent = '复制';
                    copyButton.classList.remove('copied');
                  }, 2000);
                } catch (err) {
                  console.error('Failed to copy code:', err);
                  copyButton.textContent = '复制失败';
                  setTimeout(() => {
                    copyButton.textContent = '复制';
                  }, 2000);
                }
              });
            }

            // 应用代码高亮
            hljs.highlightBlock(block);
          });
        }

        // 处理预览区域的代码块
        const previewContainer = document.querySelector('.preview');
        if (previewContainer) {
          previewContainer.querySelectorAll('pre code').forEach((block) => {
            if (!block.parentElement.parentElement.classList.contains('code-block-wrapper')) {
              const wrapper = document.createElement('div');
              wrapper.className = 'code-block-wrapper';
              
              const pre = block.parentElement;
              pre.parentNode.insertBefore(wrapper, pre);
              wrapper.appendChild(pre);
              
              const copyButton = document.createElement('button');
              copyButton.className = 'code-copy-btn';
              copyButton.textContent = '复制';
              wrapper.appendChild(copyButton);
              
              copyButton.addEventListener('click', async () => {
                try {
                  await navigator.clipboard.writeText(block.textContent);
                  copyButton.textContent = '已复制!';
                  copyButton.classList.add('copied');
                  
                  setTimeout(() => {
                    copyButton.textContent = '复制';
                    copyButton.classList.remove('copied');
                  }, 2000);
                } catch (err) {
                  console.error('Failed to copy code:', err);
                  copyButton.textContent = '复制失败';
                  setTimeout(() => {
                    copyButton.textContent = '复制';
                  }, 2000);
                }
              });
            }

            // 应用代码高亮
            hljs.highlightBlock(block);
          });
        }
      }, 0);
    };

    // 在 onMounted 中初始化主题
    onMounted(() => {
      setTheme(currentTheme.value); // 会使用 'light' 作为默认值
      
      // 监听系统主题变化
      prefersDark.addEventListener('change', (e) => {
        if (currentTheme.value === 'auto') {
          setTheme('auto');
        }
      });
    });

    // 切换主题的函数
    const toggleTheme = () => {
      const themes = ['light', 'dark', 'auto'];
      const currentIndex = themes.indexOf(currentTheme.value);
      const nextTheme = themes[(currentIndex + 1) % themes.length];
      setTheme(nextTheme);
    };

    // 添加二维码相关状态
    const showQRCode = ref(false);
    const currentQRUrl = ref('');
    
    // 显示二维码的方法
    const showQR = async (url) => {
      currentQRUrl.value = url;
      showQRCode.value = true;
      
      // 等待 DOM 更新后生成二维码
      await nextTick();
      const qrContainer = document.getElementById('qr-container');
      if (qrContainer) {
        // 清空容器
        qrContainer.innerHTML = '';
        
        // 使用 QRCode 构造函数生成二维码
        new QRCode(qrContainer, {
          text: url,
          width: 200,
          height: 200,
          colorDark: getComputedStyle(document.documentElement).getPropertyValue('--text-color').trim(),
          colorLight: getComputedStyle(document.documentElement).getPropertyValue('--card-bg').trim(),
          correctLevel: QRCode.CorrectLevel.H
        });
      }
    };
    
    // 下载二维码的方法
    const downloadQR = () => {
      const qrContainer = document.getElementById('qr-container');
      if (qrContainer) {
        const img = qrContainer.querySelector('img');
        if (img) {
          const link = document.createElement('a');
          link.download = 'qrcode.png';
          link.href = img.src;
          link.click();
        }
      }
    };

    // 在 setup() 函数中添加一个新的 computed 属性
    const themeIcon = computed(() => {
      if (currentTheme.value === 'auto') return '🌗';
      return currentTheme.value === 'dark' ? '🌙' : '☀️';
    });

    // 在 setup() 函数中添加一个计算属性来处理 GitHub 图标
    const githubIconSvg = computed(() => ({
      __html: '<svg height="32" width="32" viewBox="0 0 16 16" class="github-icon"><path fill="currentColor" d="M8 0C3.58 0 0 3.58 0 8c0 3.54 2.29 6.53 5.47 7.59.4.07.55-.17.55-.38 0-.19-.01-.82-.01-1.49-2.01.37-2.53-.49-2.69-.94-.09-.23-.48-.94-.82-1.13-.28-.15-.68-.52-.01-.53.63-.01 1.08.58 1.23.82.72 1.21 1.87.87 2.33.66.07-.52.28-.87.51-1.07-1.78-.2-3.64-.89-3.64-3.95 0-.87.31-1.59.82-2.15-.08-.2-.36-1.02.08-2.12 0 0 .67-.21 2.2.82.64-.18 1.32-.27 2-.27.68 0 1.36.09 2 .27 1.53-1.04 2.2-.82 2.2-.82.44 1.1.16 1.92.08 2.12.51.56.82 1.27.82 2.15 0 3.07-1.87 3.75-3.65 3.95.29.25.54.73.54 1.48 0 1.07-.01 1.93-.01 2.2 0 .21.15.46.55.38A8.013 8.013 0 0016 8c0-4.42-3.58-8-8-8z"/></svg>'
    }));
    
    // 添加自动保存方法
    const autoSave = () => {
      if (content.value && content.value !== lastSavedContent.value) {
        try {
          localStorage.setItem('autosaved_content', content.value);
          lastSavedContent.value = content.value;
          console.log('Content auto-saved:', new Date().toLocaleString());
        } catch (e) {
          console.error('Auto-save failed:', e);
        }
      }
    };

    // 在组件挂载时检查是否有自动保存的内容
    onMounted(() => {
      // 检查是否有保存的内容
      const savedContent = localStorage.getItem('autosaved_content');
      if (savedContent) {
        // 如果有保存的内容，提示用户是否恢复
        if (confirm('发现未保存的内容，是否恢复？')) {
          content.value = savedContent;
        }
        // 无论是否恢复，都清除保存的内容
        localStorage.removeItem('autosaved_content');
      }

      // 添加页面关闭前的保存事件
      window.addEventListener('beforeunload', saveContent);
    });

    // 在组件卸载时清理事件监听
    onUnmounted(() => {
      window.removeEventListener('beforeunload', saveContent);
    });

    // 添加保存方法
    const saveContent = () => {
      if (content.value && content.value !== lastSavedContent.value) {
        try {
          localStorage.setItem('autosaved_content', content.value);
          lastSavedContent.value = content.value;
          console.log('Content saved:', new Date().toLocaleString());
        } catch (e) {
          console.error('Save failed:', e);
        }
      }
    };


    // 修改 startFileEdit 方法
    const startFileEdit = async () => {
      try {
        const pathParts = window.location.pathname.split('/');
        const id = pathParts[pathParts.length - 1];
        const credentials = localStorage.getItem('adminCredentials');
        
        if (!credentials) {
          throw new Error('未登录');
        }

        const response = await fetch('/api/file/' + id, {
          headers: {
            'Authorization': 'Basic ' + credentials
          }
        });

        if (!response.ok) {
          throw new Error('获取文件信息失败');
        }

        const data = await response.json();
        
        // 设置编辑状态
        editFileExpiresIn.value = '1d'; // 默认值
        editFileMaxViews.value = (data.maxViews || 0).toString(); // 设置当前的下载次数限制
        isFileEditing.value = true;
      } catch (err) {
        console.error('Edit error:', err);
        error.value = err.message;
      }
    };

    // 修改 saveFileEdit 方法
    const saveFileEdit = async () => {
      try {
        error.value = null;
        const pathParts = window.location.pathname.split('/');
        const id = pathParts[pathParts.length - 1];
        const credentials = localStorage.getItem('adminCredentials');
        
        if (!credentials) {
          throw new Error('未登录');
        }

        const response = await fetch('/api/admin/file/' + id + '/settings', {
          method: 'PUT',
          headers: {
            'Authorization': 'Basic ' + credentials,
            'Content-Type': 'application/json'
          },
          body: JSON.stringify({
            expiresIn: editFileExpiresIn.value,
            maxViews: parseInt(editFileMaxViews.value) || 0
          })
        });

        if (!response.ok) {
          throw new Error('保存失败');
        }

        const data = await response.json();
        
        // 更新文件信息
        fileInfo.value = {
          ...fileInfo.value,
          expiresAt: data.expiresAt,
          maxViews: data.maxViews,
          viewCount: data.viewCount
        };
        
        isFileEditing.value = false;
      } catch (err) {
        error.value = err.message;
      }
    };

    // 添加取消编辑方法
    const cancelFileEdit = () => {
      isFileEditing.value = false;
    };

    return {
      activeTab,
      content,
      isMarkdown,
      password,
      expiresIn,
      files,
      isDragging,
      result,
      error,
      uploadStatus,
      uploadProgress,
      isUploading,
      uploadingFiles,
      isAdmin,
      showAdminLogin,
      adminUsername,
      adminPassword,
      adminError,
      shares,
      shareFilter,
      filteredShares,
      preview,
      formatSize,
      handleDrop,
      handleFileSelect,
      removeFile,
      submitPaste,
      uploadFiles,
      copyUrl,
      adminLogin,
      adminLogout,
      deleteShare,
      formatDate,
      isExpired,  
      validSharesCount,
      showPassword,
      editorRef,
      previewRef,
      handleEditorScroll,
      handlePreviewScroll,
      showDeleteConfirm,
      deleteTarget,
      confirmDelete,
      isRefreshing,
      refreshShares,
      customId,
      allowTextUpload,
      allowFileUpload,
      toggleTextUpload,
      toggleFileUpload,
      showPasswordDialog,
      passwordTarget,
      newPassword,
      passwordError,
      showChangePassword,
      changePassword,
      executeDelete,
      storageInfo,
      formatStorageSize,
      showAdminPanel,
      previewUrl,
      previewType,
      getFileTypeIcon,
      clearPreview,
      uploadSpeed,
      formatSpeed,
      setError,
      abortController,
      cancelUpload,
      uploadXHR,
      maxViews,
      copyContent, // 添加这行
      currentTheme,
      prefersDark,
      setTheme,
      toggleTheme,
      showQRCode,
      currentQRUrl,
      showQR,
      downloadQR,
      themeIcon,
      githubIconSvg,
      lastSavedContent,
      saveContent,
      isFileEditing,
      editFileExpiresIn,
      editFileMaxViews,
      startFileEdit,
      saveFileEdit,
      cancelFileEdit,
    };
  },

  template: \`
  <div class="container">

  <a href="https://github.com/ling-drag0n/CloudPaste" 
     target="_blank" 
     class="github-link" 
     title="Visit GitHub"
     v-html="githubIconSvg.__html">
  </a>

  <button 
    class="theme-toggle" 
    @click="toggleTheme" 
    :title="'当前主题: ' + currentTheme"
    v-text="themeIcon"
  >
  </button>

    <div class="card">
      <!-- 标签页切换 -->
      <div class="tabs">
        <button 
          :class="['tab', activeTab === 'paste' ? 'active' : '']"
          @click="activeTab = 'paste'"
        >文本粘贴</button>
        <button 
          :class="['tab', activeTab === 'file' ? 'active' : '']"
          @click="activeTab = 'file'"
        >文件上传</button>
      </div>

      <!-- 文本粘贴部分 -->
      <div v-if="activeTab === 'paste'">
        <div class="editor-container">
          <!-- 编辑器区域 -->
          <div class="editor">
            <textarea 
              ref="editorRef"
              v-model="content"
              placeholder="在此输入文本内容..."
              @scroll="handleEditorScroll"
            ></textarea>
          </div>
          <!-- Markdown 预览区域 -->
          <div 
            v-if="isMarkdown" 
            class="preview" 
            ref="previewRef"
            @scroll="handlePreviewScroll"
            v-html="preview"
          ></div>
        </div>
        
        <!-- 设置选项 -->
        <div class="settings">
        <div class="markdown-toggle">
            <input type="checkbox" id="markdown-toggle" v-model="isMarkdown">
            <label for="markdown-toggle">启用 Markdown</label>
        </div>
        <div class="input-group">
            <label>密码保护</label>
            <input 
              type="password" 
              v-model="password" 
              placeholder="可选"
              autocomplete="new-password"
            >
        </div>
        <div class="input-group">
            <label>过期时间</label>
            <select v-model="expiresIn">
            <option value="1h">1小时</option>
            <option value="1d">1天</option>
            <option value="7d">7天</option>
            <option value="30d">30天</option>
            <option value="never">永不过期</option>
            </select>
        </div>
        <div class="input-group">
          <label>自定义链接后缀 (可选)</label>
          <input 
            type="text" 
            v-model="customId"
            placeholder="留空则自动生成"
            pattern="[a-zA-Z0-9-_]+"
            title="只能使用字母、数字、横线和下划线"
            :disabled="files.length > 1"
            autocomplete="new-password"
          >
          <small v-if="files.length > 1" style="color: #666;">
            多文件上传时不支持自定义链接
          </small>
        </div>
        <div class="input-group">
          <label>可打开次数 (0表示无限制)</label>
          <input 
            type="number" 
            v-model="maxViews"
            min="0"
            placeholder="0"
            title="设置分享可以被打开的次数，0或留空表示无限制"
          >
        </div>
        </div>

        <button class="btn" @click="submitPaste">创建分享</button>
      </div>

      <!-- 文件上传部分 -->
      <div v-if="activeTab === 'file'">
        <!-- 文件拖放区域 -->
        <div
          class="file-drop"
          :class="{ dragging: isDragging }"
          @dragenter.prevent="isDragging = true"
          @dragover.prevent="isDragging = true"
          @dragleave.prevent="isDragging = false"
          @drop.prevent="handleDrop"
        >
          <input
            type="file"
            multiple
            @change="handleFileSelect"
            style="display: none"
            ref="fileInput"
          >
          <div @click="$refs.fileInput.click()">
            点击或拖放文件到此处上传
          </div>
        </div>

        <!-- 文件列表 -->
        <div class="file-list" v-if="files.length">
          <!-- 添加预览区域 -->
          <div v-if="previewUrl" class="file-preview">
            <img v-if="previewType === 'image'" :src="previewUrl" alt="预览">
            <audio v-if="previewType === 'audio'" :src="previewUrl" controls></audio>
            <video v-if="previewType === 'video'" :src="previewUrl" controls></video>
          </div>
          
          <div v-for="(file, index) in files" :key="index" class="file-item">
            <span class="file-type-icon">{{ getFileTypeIcon(file) }}</span>
            <span class="name">{{ file.name }}</span>
            <span class="size">{{ formatSize(file.size) }}</span>
            <div class="actions">
              <button class="btn" @click="removeFile(index)">移除</button>
            </div>
          </div>
        </div>

        <!-- 文件上传设置 -->
        <div class="settings">
          <!-- 添加自定义链接后缀输入框 -->
          <div class="input-group">
            <label>自定义链接后缀 (可选)</label>
            <input 
              type="text" 
              v-model="customId"
              placeholder="留空则自动生成"
              pattern="[a-zA-Z0-9-_]+"
              title="只能使用字母、数字、横线和下划线"
              :disabled="files.length > 1"
              autocomplete="new-password"
            >
            <small v-if="files.length > 1" style="color: #666;">
              多文件上传时不支持自定义链接
            </small>
          </div>

          <div class="input-group">
            <label>密码保护</label>
            <input type="password" v-model="password" placeholder="可选" autocomplete="new-password">
          </div>
          <div class="input-group">
            <label>过期时间</label>
            <select v-model="expiresIn">
              <option value="1h">1小时</option>
              <option value="1d">1天</option>
              <option value="7d">7天</option>
              <option value="30d">30天</option>
              <option value="never">永不过期</option>
            </select>
          </div>

          <!-- 添加可打开次数输入框 -->
          <div class="input-group">
            <label>可下载次数 (0表示无限制)</label>
            <input 
              type="number" 
              v-model="maxViews"
              min="0"
              placeholder="0"
              title="设置文件可以被下载的次数，0或留空表示无限制"
            >
          </div>
        </div>

        <!-- 上传状态显示 -->
        <div v-if="uploadStatus" class="upload-status" :class="{ error: error }">
          {{ uploadStatus }}
        </div>

        <!-- 上传进度显示 -->
        <div v-if="isUploading" class="upload-progress-wrapper">
          <div class="upload-progress-bar">
            <div class="upload-progress-inner" :style="{ width: uploadProgress + '%' }"></div>
          </div>
          <div class="upload-progress-info">
            <span>{{ uploadProgress.toFixed(1) }}%</span>
            <span>{{ formatSpeed(uploadSpeed) }}</span>
            <button class="btn cancel-btn" @click="cancelUpload">
              取消上传
            </button>
          </div>
        </div>

        <button class="btn" @click="uploadFiles" :disabled="!files.length || isUploading">
          {{ isUploading ? '上传中...' : '上传文件' }}
        </button>
      </div>

      <!-- 结果显示部分 -->
      <div v-if="result" class="result">
        <div v-if="result.type === 'paste'">
          <p>分享链接：</p>
          <div class="link">
            <a :href="result.url" target="_blank">{{ result.url }}</a>
            <div class="link-actions">
              <button class="btn" @click="copyUrl(result.url)">复制链接</button>
              <button class="qr-btn" @click="showQR(result.url)" title="显示二维码">
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                  <path d="M3 3h6v6H3zM15 3h6v6h-6zM3 15h6v6H3z"/>
                  <path d="M15 15h2v2h-2zM19 15h2v2h-2zM15 19h2v2h-2zM19 19h2v2h-2z"/>
                </svg>
              </button>
            </div>
          </div>
        </div>
        <div v-else>
          <p>分享链接：</p>
          <div v-for="file in result.files" :key="file.url" class="link">
            <a :href="file.url" target="_blank">{{ file.url }}</a>
            <div class="link-actions">
              <button class="btn" @click="copyUrl(file.url)">复制链接</button>
              <!-- 修改直链按钮，点击复制直接下载链接 -->
              <button 
                class="qr-btn" 
                @click="copyUrl(file.directDownloadUrl)" 
                title="复制直接下载链接"
              >
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                  <path d="M10 13a5 5 0 0 0 7.54.54l3-3a5 5 0 0 0-7.07-7.07l-1.72 1.71"/>
                  <path d="M14 11a5 5 0 0 0-7.54-.54l-3 3a5 5 0 0 0 7.07 7.07l1.71-1.71"/>
                </svg>
              </button>
              <button class="qr-btn" @click="showQR(file.url)" title="显示二维码">
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                  <path d="M3 3h6v6H3zM15 3h6v6h-6zM3 15h6v6H3z"/>
                  <path d="M15 15h2v2h-2zM19 15h2v2h-2zM15 19h2v2h-2zM19 19h2v2h-2z"/>
                </svg>
              </button>
            </div>
          </div>
        </div>
      </div>

      <!-- 错误信息显示 -->
      <div v-if="error" class="error">
        {{ error }}
      </div>
    </div>

    <!-- 管理员面板 -->
    <div class="admin-panel">
      <!-- 管理员登录按钮 -->
      <button v-if="(!isAdmin || !showAdminPanel) && !showAdminLogin" 
              class="btn" 
              @click="() => {
                if(isAdmin) {
                  showAdminPanel = true;
                } else {
                  showAdminLogin = true;
                }
              }">
        {{ isAdmin ? '管理面板' : '管理员登录' }}
      </button>

      <!-- 管理员登录表单 -->
      <div v-if="showAdminLogin" class="admin-login">
        <div class="input-group">
          <label>用户名</label>
          <input type="text" v-model="adminUsername">
        </div>
        <div class="input-group">
          <label>密码</label>
          <div class="password-input-group">
            <input 
              :type="showPassword ? 'text' : 'password'" 
              v-model="adminPassword" 
              @keyup.enter="adminLogin"
            >
            <button 
              class="password-toggle" 
              @click="showPassword = !showPassword"
              type="button"
            >
              {{ showPassword ? '👁️' : '👁️‍🗨️' }}
            </button>
          </div>
        </div>
        <div v-if="adminError" class="error">{{ adminError }}</div>
        <div class="actions">
          <button class="btn" @click="adminLogin">确定</button>
          <button class="btn" @click="showAdminLogin = false">取消</button>
        </div>
      </div>

      <!-- 管理员内容面板 -->
      <div v-if="isAdmin && showAdminPanel" class="admin-content">
        <div class="admin-header">
          <div class="admin-header-top">
            <h3>分享管理</h3>
            <button class="close-btn" @click="showAdminPanel = false">&times;</button>
          </div>
          <div class="admin-controls">
            <button 
              class="control-btn" 
              :class="{ active: allowTextUpload }"
              @click="toggleTextUpload"
              title="允许/禁止文本上传"
            >
              文本上传: {{ allowTextUpload ? '开' : '关' }}
            </button>
            <button 
              class="control-btn" 
              :class="{ active: allowFileUpload }"
              @click="toggleFileUpload"
              title="允许/禁止文件上传"
            >
              文件上传: {{ allowFileUpload ? '开' : '关' }}
            </button>
            <button 
              class="refresh-btn" 
              @click="refreshShares" 
              :disabled="isRefreshing"
            >
              {{ isRefreshing ? '刷新中...' : '刷新列表' }}
            </button>
          </div>
        </div>

        <!-- 统计信息 -->
        <div class="admin-stats">
          <div class="stat-card">
            <div class="stat-value">{{ shares.length }}</div>
            <div class="stat-label">总分享数</div>
          </div>
          <div class="stat-card">
            <div class="stat-value">
              {{ shares.filter(s => !isExpired(s.expiresAt)).length }}
            </div>
            <div class="stat-label">有效分享</div>
          </div>
        </div>

        <!-- 在 admin-stats 后面添加 -->
        <div class="storage-info">
          <h4 style="margin: 0 0 0.5rem 0;">R2存储空间使用情况</h4>
          <div class="storage-progress">
            <div 
              class="storage-progress-inner"
              :class="{
                'warning': storageInfo.percentage >= 70 && storageInfo.percentage < 90,
                'danger': storageInfo.percentage >= 90
              }"
              :style="{ width: storageInfo.percentage + '%' }"
            ></div>
          </div>
          <div class="storage-details">
            <span>已用: {{ formatStorageSize(storageInfo.used) }}</span>
            <span>总容量: {{ formatStorageSize(storageInfo.total) }}</span>
            <span>使用率: {{ storageInfo.percentage.toFixed(1) }}%</span>
          </div>
        </div>

        <!-- 分享类型筛选 -->
        <div class="filter-bar">
          <select v-model="shareFilter">
            <option value="all">全部</option>
            <option value="paste">文本</option>
            <option value="file">文件</option>
          </select>
        </div>

        <!-- 分享列表 -->
        <div class="share-list">
          <div v-for="share in filteredShares" :key="share.id" class="share-item">
            <div class="title">
              <div class="title-left">
              <a 
                :href="share.url" 
                target="_blank" 
                  class="title-link"
                :title="'点击打开' + (share.type === 'paste' ? '文本' : '文件') + '分享'"
              >
                <span class="title-text">
                {{ share.type === 'paste' ? '文本分享' : '文件分享' }}
                  <span v-if="share.hasPassword" class="lock-icon" title="密码保护">🔒</span>
                </span>
              </a>
              </div>
              <div class="title-icons">
                <!-- 添加直链按钮 -->
                <button 
                  v-if="share.type === 'file'" 
                  class="icon-btn" 
                  @click="copyUrl(share.directDownloadUrl)" 
                  title="复制直接下载链接"
                >
                  <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                    <path d="M10 13a5 5 0 0 0 7.54.54l3-3a5 5 0 0 0-7.07-7.07l-1.72 1.71"/>
                    <path d="M14 11a5 5 0 0 0-7.54-.54l-3 3a5 5 0 0 0 7.07 7.07l1.71-1.71"/>
                  </svg>
                </button>
                
                <!-- 二维码按钮 -->
                <button class="icon-btn" @click="showQR(share.url)" title="显示二维码">
                  <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                    <path d="M3 3h6v6H3zM15 3h6v6h-6zM3 15h6v6H3z"/>
                    <path d="M15 15h2v2h-2zM19 15h2v2h-2zM15 19h2v2h-2zM19 19h2v2h-2z"/>
                  </svg>
                </button>
              </div>
            </div>
            <div class="info">
              <div>ID: {{ share.id }}</div>
              <div>创建时间: {{ formatDate(share.createdAt) }}</div>
              <div>过期时间: {{ share.expiresAt ? formatDate(share.expiresAt) : '永不过期' }}</div>
              <div v-if="share.type === 'file'">文件名: {{ share.filename }}</div>
              <!-- 根据分享类型显示不同的文本 -->
              <div v-if="share.maxViews > 0">
                {{ share.type === 'paste' ? '剩余访问次数' : '剩余下载次数' }}: 
                {{ share.maxViews - share.viewCount }}
                (已{{ share.type === 'paste' ? '访问' : '下载' }} {{ share.viewCount }} 次)
              </div>
            </div>
            <div class="actions">
              <button class="btn" @click="copyUrl(share.url)">复制链接</button>
              <button class="btn" @click="showChangePassword(share)">修改密码</button>
              <button class="btn delete-btn" @click="confirmDelete(share)">删除</button>
            </div>
          </div>
        </div>

        <!-- 添加确认对话框 -->
        <div v-if="showDeleteConfirm" class="confirm-dialog">
            <div class="confirm-content">
                <h3>确认删除</h3>
                <p>确定要删除这个{{ deleteTarget?.type === 'paste' ? '文本' : '文件' }}分享吗？</p>
                <p class="warning">此操作不可恢复！</p>
                <div class="confirm-actions">
                    <button class="btn" @click="executeDelete">确定删除</button>
                    <button class="btn cancel" @click="showDeleteConfirm = false">取消</button>
                </div>
            </div>
        </div>

      </div>
    </div>

    <!-- 在管理员面板的最后添加修改密码对话框 -->
    <div v-if="showPasswordDialog" class="confirm-dialog">
      <div class="confirm-content">
        <h3>修改密码</h3>
        <p>{{ passwordTarget?.type === 'paste' ? '文本' : '文件' }}分享: {{ passwordTarget?.id }}</p>
        <div class="input-group">
          <label>新密码 (留空则移除密码保护)</label>
          <input 
            type="password" 
            v-model="newPassword"
            placeholder="输入密码"
          >
        </div>
        <div v-if="passwordError" class="error" style="margin: 10px 0;">{{ passwordError }}</div>
        <div class="confirm-actions">
          <button class="btn" @click="changePassword">确定</button>
          <button class="btn cancel" @click="showPasswordDialog = false">取消</button>
        </div>
      </div>
    </div>

    <!-- 添加二维码弹窗 -->
    <div v-if="showQRCode" class="qr-dialog" @click.self="showQRCode = false">
      <div class="qr-content">
        <h3>扫描二维码访问</h3>
        <div id="qr-container"></div>
        <div class="qr-actions">
          <button class="btn" @click="downloadQR">下载二维码</button>
          <button class="btn" style="background: var(--btn-secondary-bg)" @click="showQRCode = false">关闭</button>
        </div>
      </div>
    </div>
  </div>
  \`
}).mount('#app');`;

// 分享页面的 Vue 应用代码
const shareAppScript = `
const { createApp, ref, computed, onMounted } = Vue;

createApp({
  setup() {
    const content = ref('');
    const isMarkdown = ref(false);
    const needPassword = ref(false);
    const password = ref('');
    const error = ref(null);
    const loading = ref(true);
    const expiresAt = ref(null);
    const isFile = ref(false);
    const fileInfo = ref(null); // 添加文件信息
    const uploadProgress = ref(0);
    const isUploading = ref(false);
    const uploadingFiles = ref([]);
    // 添加管理员状态
    const isAdmin = ref(false);
    const isEditing = ref(false); // 添加编辑状态
    const editContent = ref(''); // 添加编辑内容
    const editMarkdown = ref(false); // 添加编辑时的 Markdown 开关状态
    const editExpiresIn = ref('1d'); // 添加编辑时的过期时间状态
    const editMaxViews = ref('0'); // 添加编辑时的访问次数状态
    const maxViews = ref(0); // 添加最大访问次数状态
    const viewCount = ref(0); // 添加已访问次数状态
    const isFileEditing = ref(false);
    const editFileExpiresIn = ref('1d');
    const editFileMaxViews = ref('0'); // 添加这行，初始化可下载次数

    // 添加检查管理员状态的方法
    const checkAdmin = () => {
      const credentials = localStorage.getItem('adminCredentials');
      isAdmin.value = !!credentials;
      return credentials;
    };

    // 添加开始编辑方法
    const startEdit = async () => {
      try {
        // 获取当前分享的最新信息
        const pathParts = window.location.pathname.split('/');
        const id = pathParts[pathParts.length - 1];
        const credentials = localStorage.getItem('adminCredentials');
        
        const response = await fetch('/api/paste/' + id, {
          headers: {
            'Authorization': 'Basic ' + credentials
          }
        });

        if (!response.ok) {
          throw new Error('获取分享信息失败');
        }

        const data = await response.json();
        
        // 设置编辑状态
        editContent.value = content.value;
        editMarkdown.value = isMarkdown.value;
        editExpiresIn.value = '1d';
        // 添加数据检查，使用默认值 0
        editMaxViews.value = (data.maxViews || 0).toString();
        maxViews.value = data.maxViews || 0;
        viewCount.value = data.viewCount || 0;
        isEditing.value = true;
      } catch (err) {
        console.error('Edit error:', err);
        error.value = err.message;
      }
    };

    // 添加保存编辑方法
    const saveEdit = async () => {
      try {
        error.value = null;
        const pathParts = window.location.pathname.split('/');
        const id = pathParts[pathParts.length - 1];
        const credentials = localStorage.getItem('adminCredentials');
        
        const response = await fetch('/api/admin/paste/' + id + '/content', {
          method: 'PUT',
          headers: {
            'Authorization': 'Basic ' + credentials,
            'Content-Type': 'application/json'
          },
          body: JSON.stringify({
            content: editContent.value,
            isMarkdown: editMarkdown.value,
            expiresIn: editExpiresIn.value,
            maxViews: parseInt(editMaxViews.value) || 0 // 添加访问次数
          })
        });

        if (!response.ok) {
          throw new Error('保存失败');
        }

        const data = await response.json(); // 获取响应数据
        
        // 更新内容和状态
        content.value = editContent.value;
        isMarkdown.value = editMarkdown.value;
        // 更新过期时间
        expiresAt.value = data.expiresAt ? new Date(data.expiresAt) : null;
        maxViews.value = data.maxViews; // 更新访问次数
        viewCount.value = data.viewCount; // 更新已访问次数
        isEditing.value = false;
      } catch (err) {
        error.value = err.message;
      }
    };

    // 添加取消编辑方法
    const cancelEdit = () => {
      isEditing.value = false;
      editContent.value = '';
      
      // 在取消编辑后重新处理代码块
      setTimeout(() => {
        const contentContainer = document.querySelector('.content');
        if (!contentContainer) return;

        // 处理代码块
        contentContainer.querySelectorAll('pre code').forEach((block) => {
          // 检查是否已经添加了复制按钮
          if (!block.parentElement.parentElement.classList.contains('code-block-wrapper')) {
            // 创建包装容器
            const wrapper = document.createElement('div');
            wrapper.className = 'code-block-wrapper';
            
            // 获取 pre 标签
            const pre = block.parentElement;
            pre.parentNode.insertBefore(wrapper, pre);
            wrapper.appendChild(pre);
            
            // 创建复制按钮
            const copyButton = document.createElement('button');
            copyButton.className = 'code-copy-btn';
            copyButton.textContent = '复制';
            wrapper.appendChild(copyButton);
            
            // 添加复制功能
            copyButton.addEventListener('click', async () => {
              try {
                await navigator.clipboard.writeText(block.textContent);
                copyButton.textContent = '已复制!';
                copyButton.classList.add('copied');
                
                setTimeout(() => {
                  copyButton.textContent = '复制';
                  copyButton.classList.remove('copied');
                }, 2000);
              } catch (err) {
                console.error('Failed to copy code:', err);
                copyButton.textContent = '复制失败';
                setTimeout(() => {
                  copyButton.textContent = '复制';
                }, 2000);
              }
            });
          }

          // 应用代码高亮
          hljs.highlightBlock(block);
        });

        // 检查是否为暗色主题
        const isDarkTheme = document.documentElement.getAttribute('data-theme') === 'dark';
        
        if (isDarkTheme) {
          // 在暗色主题下应用样式
          contentContainer.style.background = 'var(--markdown-bg)';
          contentContainer.style.color = 'var(--markdown-text)';
          
          // 代码高亮
          contentContainer.querySelectorAll('pre code').forEach((block) => {
            hljs.highlightBlock(block);
            const pre = block.parentElement;
            if (pre) {
              pre.style.background = 'var(--markdown-code-block-bg)';
            }
            block.style.color = 'var(--markdown-code-text)';
          });

          // 应用其他暗色主题样式
          contentContainer.querySelectorAll('h1, h2, h3, h4, h5, h6').forEach(heading => {
            heading.style.color = 'var(--markdown-heading-text)';
          });

          contentContainer.querySelectorAll('a').forEach(link => {
            link.style.color = 'var(--markdown-link)';
          });

          contentContainer.querySelectorAll('blockquote').forEach(quote => {
            quote.style.background = 'var(--markdown-blockquote-bg)';
            quote.style.color = 'var(--markdown-blockquote-text)';
            quote.style.borderLeftColor = 'var(--markdown-blockquote-border)';
          });

          contentContainer.querySelectorAll('table').forEach(table => {
            table.style.borderColor = 'var(--markdown-table-border)';
            table.querySelectorAll('tr:nth-child(2n)').forEach(row => {
              row.style.background = 'var(--markdown-table-alt-bg)';
            });
          });
        }
        
        // 重新渲染数学公式
        renderMathInElement(contentContainer, {
          delimiters: [
            {left: "$$", right: "$$", display: true},
            {left: "$", right: "$", display: false}
          ],
          throwOnError: false
        });
      }, 0);
    };

    // 修改 onMounted,添加管理员检查
    onMounted(() => {
      checkAdmin();
      fetchContent();
    });

    // 添加下载文件的方法
    const downloadFile = async () => {
      try {
        error.value = null;
        const pathParts = window.location.pathname.split('/');
        const id = pathParts[pathParts.length - 1];
        
        const response = await fetch('/api/file/' + id + '?download=true', {
          headers: password.value ? {
            'X-Password': password.value
          } : {}
        });

        if (!response.ok) {
          throw new Error('下载失败');
        }

        // 触发浏览器下载
        const blob = await response.blob();
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = fileInfo.value.filename;
        document.body.appendChild(a);
        a.click();
        window.URL.revokeObjectURL(url);
        document.body.removeChild(a);
      } catch (err) {
        error.value = err.message;
      }
    };

    const fetchContent = async () => {
      try {
        error.value = null;
        const pathParts = window.location.pathname.split('/');
        const id = pathParts[pathParts.length - 1];
        const isFilePath = window.location.pathname.includes('/share/file/');
        isFile.value = isFilePath;
        
        const apiUrl = isFilePath ? '/api/file/' + id : '/api/paste/' + id;
        
        // 构建请求头
        const headers = {};
        
        // 如果有密码,添加密码头
        if (password.value) {
          headers['X-Password'] = password.value;
        }
        
        // 如果是管理员,添加认证头
        const credentials = localStorage.getItem('adminCredentials');
        if (credentials) {
          headers['Authorization'] = 'Basic ' + credentials;
        }

        const response = await fetch(apiUrl, { headers });

        if (response.status === 401) {
          needPassword.value = true;
          loading.value = false;
          error.value = null;
          return;
        }

        if (response.status === 403) {
          needPassword.value = true;
          loading.value = false;
          error.value = '密码错误，请重试';
          password.value = '';
          return;
        }

        if (!response.ok) {
          const data = await response.json();
          throw new Error(data.message || '加载失败');
        }

          const data = await response.json();
        if (isFilePath) {
          fileInfo.value = data;
          content.value = '文件信息已加载';
        } else {
          content.value = data.content;
          isMarkdown.value = data.isMarkdown;
          expiresAt.value = data.expiresAt ? new Date(data.expiresAt) : null;
          maxViews.value = parseInt(data.maxViews) || 0;
          viewCount.value = parseInt(data.viewCount) || 0;
        }
        loading.value = false;
        needPassword.value = false;
      } catch (err) {
        error.value = err.message;
        loading.value = false;
      }
    };

    const submitPassword = async () => {
      if (!password.value) {
        error.value = '请输入密码';
        return;
      }
      loading.value = true;
      await fetchContent();
    };

    const formatExpiryTime = computed(() => {
      // 文件分享的处理
      if (isFile.value && fileInfo.value) {
        if (!fileInfo.value.expiresAt) return '永不过期';
        
        try {
          const now = new Date();
          const expiry = new Date(fileInfo.value.expiresAt);
          if (isNaN(expiry.getTime())) return '永不过期';
          
          const diff = expiry - now;
          if (diff <= 0) return '已过期';
          
          const hours = Math.floor(diff / (1000 * 60 * 60));
          if (hours < 24) {
            return '将在 ' + hours + ' 小时后过期';
          }
          const days = Math.floor(hours / 24);
          return '将在 ' + days + ' 天后过期';
        } catch (e) {
          return '永不过期';
        }
      }
      
      // 文本分享的处理
      // 如果 expiresAt.value 为 null，直接返回永不过期
      if (expiresAt.value === null) return '永不过期';
      
      try {
        const now = new Date();
        const diff = expiresAt.value - now;
        
        if (diff <= 0) return '已过期';
        
        const hours = Math.floor(diff / (1000 * 60 * 60));
        if (hours < 24) {
          return '将在 ' + hours + ' 小时后过期';
        }
        const days = Math.floor(hours / 24);
        return '将在 ' + days + ' 天后过期';
      } catch (e) {
        return '永不过期';
      }
    });

    const renderedContent = computed(() => {
      if (!content.value) return '';
      if (!isMarkdown.value) return content.value;
      try {
        const rendered = marked.parse(content.value);
        setTimeout(() => {
          // 获取内容容器
          const contentContainer = document.querySelector('.content');
          if (!contentContainer) return;

          // 代码高亮和添加复制按钮
          contentContainer.querySelectorAll('pre code').forEach((block) => {
            // 检查是否已经添加了复制按钮
            if (!block.parentElement.parentElement.classList.contains('code-block-wrapper')) {
              // 创建包装容器
              const wrapper = document.createElement('div');
              wrapper.className = 'code-block-wrapper';
              
              // 获取 pre 标签
              const pre = block.parentElement;
              // 将 pre 标签包装在新容器中
              pre.parentNode.insertBefore(wrapper, pre);
              wrapper.appendChild(pre);
              
              // 创建复制按钮
              const copyButton = document.createElement('button');
              copyButton.className = 'code-copy-btn';
              copyButton.textContent = '复制';
              wrapper.appendChild(copyButton);
              
              // 添加复制功能
              copyButton.addEventListener('click', async () => {
                try {
                  await navigator.clipboard.writeText(block.textContent);
                  copyButton.textContent = '已复制!';
                  copyButton.classList.add('copied');
                  
                  setTimeout(() => {
                    copyButton.textContent = '复制';
                    copyButton.classList.remove('copied');
                  }, 2000);
                } catch (err) {
                  console.error('Failed to copy code:', err);
                  copyButton.textContent = '复制失败';
                  setTimeout(() => {
                    copyButton.textContent = '复制';
                  }, 2000);
                }
              });
            }

            // 应用代码高亮
            hljs.highlightBlock(block);
          });

          // 检查是否为暗色主题
          const isDarkTheme = document.documentElement.getAttribute('data-theme') === 'dark';
          
          if (isDarkTheme) {
            contentContainer.style.background = 'var(--markdown-bg)';
            contentContainer.style.color = 'var(--markdown-text)';
            
            contentContainer.querySelectorAll('pre code').forEach((block) => {
              const pre = block.parentElement;
              if (pre) {
                pre.style.background = 'var(--markdown-code-block-bg)';
              }
              block.style.color = 'var(--markdown-code-text)';
            });

            contentContainer.querySelectorAll('h1, h2, h3, h4, h5, h6').forEach(heading => {
              heading.style.color = 'var(--markdown-heading-text)';
            });

            contentContainer.querySelectorAll('a').forEach(link => {
              link.style.color = 'var(--markdown-link)';
            });

            contentContainer.querySelectorAll('blockquote').forEach(quote => {
              quote.style.background = 'var(--markdown-blockquote-bg)';
              quote.style.color = 'var(--markdown-blockquote-text)';
              quote.style.borderLeftColor = 'var(--markdown-blockquote-border)';
            });

            contentContainer.querySelectorAll('table').forEach(table => {
              table.style.borderColor = 'var(--markdown-table-border)';
              table.querySelectorAll('tr:nth-child(2n)').forEach(row => {
                row.style.background = 'var(--markdown-table-alt-bg)';
              });
            });
          }
          
          // 渲染数学公式
          renderMathInElement(contentContainer, {
            delimiters: [
              {left: "$$", right: "$$", display: true},
              {left: "$", right: "$", display: false}
            ],
            throwOnError: false
          });
        }, 0);
        return rendered;
      } catch (err) {
        return '渲染出错: ' + err.message;
      }
    });

    // 添加编辑预览计算属性
    const editPreview = computed(() => {
      if (!editContent.value) return '';
      if (!editMarkdown.value) return editContent.value;
      
      try {
        const rendered = marked.parse(editContent.value);
        setTimeout(() => {
          const previewContainer = document.querySelector('.preview');
          if (!previewContainer) return;

          // 检查是否为暗色主题
          const isDarkTheme = document.documentElement.getAttribute('data-theme') === 'dark';
          
          if (isDarkTheme) {
            // 仅在暗色主题下应用这些样式
            previewContainer.style.background = 'var(--markdown-bg)';
            previewContainer.style.color = 'var(--markdown-text)';
          }

          // 处理代码块
          previewContainer.querySelectorAll('pre code').forEach((block) => {
            // 检查是否已经添加了复制按钮
            if (!block.parentElement.parentElement.classList.contains('code-block-wrapper')) {
              const wrapper = document.createElement('div');
              wrapper.className = 'code-block-wrapper';
              
              const pre = block.parentElement;
              pre.parentNode.insertBefore(wrapper, pre);
              wrapper.appendChild(pre);
              
              const copyButton = document.createElement('button');
              copyButton.className = 'code-copy-btn';
              copyButton.textContent = '复制';
              wrapper.appendChild(copyButton);
              
              copyButton.addEventListener('click', async () => {
                try {
                  await navigator.clipboard.writeText(block.textContent);
                  copyButton.textContent = '已复制!';
                  copyButton.classList.add('copied');
                  
                  setTimeout(() => {
                    copyButton.textContent = '复制';
                    copyButton.classList.remove('copied');
                  }, 2000);
                } catch (err) {
                  console.error('Failed to copy code:', err);
                  copyButton.textContent = '复制失败';
                  setTimeout(() => {
                    copyButton.textContent = '复制';
                  }, 2000);
                }
              });
            }

            // 应用代码高亮
            hljs.highlightBlock(block);
            
            if (isDarkTheme) {
              const pre = block.parentElement;
              if (pre) {
                pre.style.background = 'var(--markdown-code-block-bg)';
              }
              block.style.color = 'var(--markdown-code-text)';
            }
          });

          if (isDarkTheme) {
            previewContainer.querySelectorAll('h1, h2, h3, h4, h5, h6').forEach(heading => {
              heading.style.color = 'var(--markdown-heading-text)';
            });

            previewContainer.querySelectorAll('a').forEach(link => {
              link.style.color = 'var(--markdown-link)';
            });

            previewContainer.querySelectorAll('blockquote').forEach(quote => {
              quote.style.background = 'var(--markdown-blockquote-bg)';
              quote.style.color = 'var(--markdown-blockquote-text)';
              quote.style.borderLeftColor = 'var(--markdown-blockquote-border)';
            });

            previewContainer.querySelectorAll('table').forEach(table => {
              table.style.borderColor = 'var(--markdown-table-border)';
              table.querySelectorAll('tr:nth-child(2n)').forEach(row => {
                row.style.background = 'var(--markdown-table-alt-bg)';
              });
            });
          }

          // 渲染数学公式
          renderMathInElement(previewContainer, {
            delimiters: [
              {left: "$$", right: "$$", display: true},
              {left: "$", right: "$", display: false}
            ],
            throwOnError: false
          });
        }, 0);
        return rendered;
      } catch (err) {
        return '渲染出错: ' + err.message;
      }
    });

    // 在 shareAppScript 中的 setup() 函数里添加复制功能
    const copyContent = async () => {
      try {
        await navigator.clipboard.writeText(content.value);
        
        // 创建提示元素
        const toast = document.createElement('div');
        toast.className = 'copy-toast';
        toast.textContent = '内容已复制到剪贴板';
        document.body.appendChild(toast);
        
        // 显示提示
        setTimeout(() => toast.classList.add('show'), 10);
        
        // 2秒后隐藏并移除提示
        setTimeout(() => {
          toast.classList.remove('show');
          setTimeout(() => document.body.removeChild(toast), 300);
        }, 2000);
      } catch (err) {
        error.value = '复制失败，请手动复制';
      }
    };

    // 在 shareAppScript 中也添加相同的主题切换逻辑，默认为light
    const currentTheme = ref(localStorage.getItem('theme') || 'light');
    const prefersDark = window.matchMedia('(prefers-color-scheme: dark)');

    // 设置主题的函数
    const setTheme = (theme) => {
      if (theme === 'auto') {
        document.documentElement.setAttribute('data-theme', 
          prefersDark.matches ? 'dark' : 'light'
        );
      } else {
        document.documentElement.setAttribute('data-theme', theme);
      }
      localStorage.setItem('theme', theme);
      currentTheme.value = theme;
    };

    // 在 onMounted 中初始化主题
    onMounted(() => {
      setTheme(currentTheme.value); // 会使用 'light' 作为默认值
      
      // 监听系统主题变化
      prefersDark.addEventListener('change', (e) => {
        if (currentTheme.value === 'auto') {
          setTheme('auto');
        }
      });
    });

    // 切换主题的函数
    const toggleTheme = () => {
      const themes = ['light', 'dark', 'auto'];
      const currentIndex = themes.indexOf(currentTheme.value);
      const nextTheme = themes[(currentIndex + 1) % themes.length];
      setTheme(nextTheme);
    };
    
    // 在 shareAppScript 中添加 themeIcon 计算属性
    const themeIcon = computed(() => {
      if (currentTheme.value === 'auto') return '🌗';
      return currentTheme.value === 'dark' ? '🌙' : '☀️';
    });

    // 添加代码块复制功能 onMounted 
    onMounted(() => {
      // 添加代码块复制功能
      setTimeout(() => {
        const codeBlocks = document.querySelectorAll('pre code');
        codeBlocks.forEach(codeBlock => {
          // 创建包装容器
          const wrapper = document.createElement('div');
          wrapper.className = 'code-block-wrapper';
          
          // 获取代码块的父元素(pre标签)
          const pre = codeBlock.parentElement;
          pre.parentNode.insertBefore(wrapper, pre);
          wrapper.appendChild(pre);
          
          // 创建复制按钮
          const copyButton = document.createElement('button');
          copyButton.className = 'code-copy-btn';
          copyButton.textContent = '复制';
          wrapper.appendChild(copyButton);
          
          // 添加点击事件
          copyButton.addEventListener('click', async () => {
            try {
              await navigator.clipboard.writeText(codeBlock.textContent);
              copyButton.textContent = '已复制!';
              copyButton.classList.add('copied');
              
              setTimeout(() => {
                copyButton.textContent = '复制';
                copyButton.classList.remove('copied');
              }, 2000);
            } catch (err) {
              console.error('Failed to copy code:', err);
              copyButton.textContent = '复制失败';
              setTimeout(() => {
                copyButton.textContent = '复制';
              }, 2000);
            }
          });
        });
      }, 100); // 给一个小延迟确保内容已渲染
    });

    // 添加开始编辑文件的方法
    const startFileEdit = async () => {
      try {
        const pathParts = window.location.pathname.split('/');
        const id = pathParts[pathParts.length - 1];
        const credentials = localStorage.getItem('adminCredentials');
        
        if (!credentials) {
          throw new Error('未登录');
        }

        const response = await fetch('/api/file/' + id, {
          headers: {
            'Authorization': 'Basic ' + credentials
          }
        });

        if (!response.ok) {
          throw new Error('获取文件信息失败');
        }

        const data = await response.json();
        
        // 设置编辑状态
        editFileExpiresIn.value = '1d'; // 默认值
        editFileMaxViews.value = (data.maxViews || 0).toString(); // 设置当前的下载次数限制
        isFileEditing.value = true;
      } catch (err) {
        console.error('Edit error:', err);
        error.value = err.message;
      }
    };

    // 添加保存文件编辑的方法
    const saveFileEdit = async () => {
      try {
        error.value = null;
        const pathParts = window.location.pathname.split('/');
        const id = pathParts[pathParts.length - 1];
        const credentials = localStorage.getItem('adminCredentials');
        
        if (!credentials) {
          throw new Error('未登录');
        }

        const response = await fetch('/api/admin/file/' + id + '/settings', {
          method: 'PUT',
          headers: {
            'Authorization': 'Basic ' + credentials,
            'Content-Type': 'application/json'
          },
          body: JSON.stringify({
            expiresIn: editFileExpiresIn.value,
            maxViews: parseInt(editFileMaxViews.value) || 0
          })
        });

        if (!response.ok) {
          throw new Error('保存失败');
        }

        const data = await response.json();
        
        // 更新文件信息
        fileInfo.value = {
          ...fileInfo.value,
          expiresAt: data.expiresAt,
          maxViews: data.maxViews,
          viewCount: data.viewCount
        };
        
        isFileEditing.value = false;
      } catch (err) {
        error.value = err.message;
      }
    };

    // 添加取消编辑方法
    const cancelFileEdit = () => {
      isFileEditing.value = false;
    };

    return {
      content,
      isMarkdown,
      needPassword,
      password,
      error,
      loading,
      renderedContent,
      formatExpiryTime,
      submitPassword,
      isFile,
      fileInfo,
      downloadFile, // 导出下载方法
      isAdmin,
      isEditing,
      editContent,
      startEdit,
      saveEdit,
      cancelEdit,
      editMarkdown,
      editPreview,
      copyContent, 
      currentTheme, // 添加暗色主题相关变量
      toggleTheme,
      themeIcon,  // 添加这行
      editExpiresIn, // 添加编辑时的过期时间状态
      editMaxViews, // 添加编辑时的访问次数状态
      maxViews, // 添加这行
      viewCount, // 添加这行
      isFileEditing,
      editFileExpiresIn,
      editFileMaxViews, // 添加这行
      startFileEdit,
      saveFileEdit,
      cancelFileEdit,
    };
  }
}).mount('#app');
`;

// HTML 模板
const html = `<!DOCTYPE html>
<html lang="zh">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>CloudPaste - 在线剪贴板</title>
    <script src="https://lf26-cdn-tos.bytecdntp.com/cdn/expire-1-M/vue/3.2.31/vue.global.prod.min.js"></script>
    <script src="https://lf26-cdn-tos.bytecdntp.com/cdn/expire-1-M/marked/4.0.2/marked.min.js"></script>
    <link rel="stylesheet" href="https://lf26-cdn-tos.bytecdntp.com/cdn/expire-1-M/highlight.js/11.4.0/styles/github.min.css">
    <script src="https://lf26-cdn-tos.bytecdntp.com/cdn/expire-1-M/highlight.js/11.4.0/highlight.min.js"></script>
    <script src="https://lf26-cdn-tos.bytecdntp.com/cdn/expire-1-M/qrcodejs/1.0.0/qrcode.min.js"></script>
    <!-- 添加 KaTeX 支持 -->
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/katex@0.16.9/dist/katex.min.css">
    <script src="https://cdn.jsdelivr.net/npm/katex@0.16.9/dist/katex.min.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/katex@0.16.9/dist/contrib/auto-render.min.js"></script>
    <style>${styles}</style>
</head>
<body>
    <div id="app">
       <a href="https://github.com/ling-drag0n/CloudPaste" 
            target="_blank" 
            class="github-link" 
            title="Visit GitHub"
            v-html="githubIconSvg.__html">
        </a>
    </div>
    <script>
        // 注入常量
        window.APP_CONFIG = {
            MAX_FILE_SIZE: ${MAX_FILE_SIZE},
            MAX_TOTAL_STORAGE: ${MAX_TOTAL_STORAGE}
        };
    </script>
    <script>${appScript}</script>
</body>
</html>`;

// 分享页面的 HTML 模板
const shareHtml = `<!DOCTYPE html>
<html lang="zh">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>CloudPaste - 分享内容</title>
    <script src="https://lf26-cdn-tos.bytecdntp.com/cdn/expire-1-M/vue/3.2.31/vue.global.prod.min.js"></script>
    <script src="https://lf26-cdn-tos.bytecdntp.com/cdn/expire-1-M/marked/4.0.2/marked.min.js"></script>
    <link rel="stylesheet" href="https://lf26-cdn-tos.bytecdntp.com/cdn/expire-1-M/highlight.js/11.4.0/styles/github.min.css">
    <script src="https://lf26-cdn-tos.bytecdntp.com/cdn/expire-1-M/highlight.js/11.4.0/highlight.min.js"></script>
    <!-- 添加 KaTeX 支持 -->
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/katex@0.16.9/dist/katex.min.css">
    <script src="https://cdn.jsdelivr.net/npm/katex@0.16.9/dist/katex.min.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/katex@0.16.9/dist/contrib/auto-render.min.js"></script>
    <style>${styles}</style>
</head>
<body>
    <div id="app">
      <div class="container">
        <!-- 添加主题切换按钮 -->
        <button 
          class="theme-toggle" 
          @click="toggleTheme" 
          :title="'当前主题: ' + currentTheme"
          v-text="themeIcon"
        >
        </button>

        <div class="card">
          <div v-if="loading">加载中...</div>
          <div v-else-if="needPassword">
            <div class="input-group">
              <label>此内容需要密码访问：</label>
              <input 
                type="password" 
                v-model="password"
                @keyup.enter="submitPassword"
                placeholder="请输入密码"
              >
            </div>
            <div v-if="error" class="error" style="margin: 10px 0;">{{ error }}</div>
            <button class="btn" @click="submitPassword">确认</button>
          </div>
          <div v-else>
            <div v-if="error" class="error">{{ error }}</div>
            <template v-else>
              <div v-if="isFile && fileInfo" class="content">
                <h3>文件信息</h3>
                <p>文件名: {{ fileInfo.filename }}</p>
                <p>文件大小: {{ (fileInfo.size / 1024 / 1024).toFixed(2) }} MB</p>
                <p>上传时间: {{ new Date(fileInfo.uploadedAt).toLocaleString() }}</p>
                <p>过期时间: {{ fileInfo.expiresAt ? new Date(fileInfo.expiresAt).toLocaleString() : '永不过期' }}</p>
                <!-- 添加剩余下载次数显示 -->
                <p v-if="fileInfo.maxViews > 0">
                  剩余下载次数: {{ fileInfo.maxViews - fileInfo.viewCount }}
                  (已下载 {{ fileInfo.viewCount }} 次)
                </p>
                
                <!-- 按钮容器 -->
                <div class="button-group" style="display: flex; gap: 1rem; margin-top: 1rem;">
                  <button class="btn" @click="downloadFile">下载文件</button>
                  <button v-if="isAdmin && !isFileEditing" 
                          class="btn" 
                          @click="startFileEdit">
                    编辑设置
                  </button>
                </div>
                  
                  <template v-if="isFileEditing">
                    <div class="settings" style="margin: 1rem 0;">
                      <div class="input-group">
                        <label>过期时间</label>
                        <select v-model="editFileExpiresIn" class="form-select">
                          <option value="1h">1小时</option>
                          <option value="1d">1天</option>
                          <option value="7d">7天</option>
                          <option value="30d">30天</option>
                          <option value="never">永不过期</option>
                        </select>
                    </div>
                    
                    <div class="input-group">
                      <label>可下载次数 (0表示无限制)</label>
                      <input 
                        type="number" 
                        v-model="editFileMaxViews"
                        min="0"
                        placeholder="0"
                        title="设置文件可以被下载的次数，0或留空表示无限制"
                      >
                      </div>
                    </div>
                    
                    <div class="actions" style="margin-top: 1rem;">
                      <button class="btn" 
                              @click="saveFileEdit" 
                              style="margin-right: 0.5rem;">
                        保存
                      </button>
                      <button class="btn" 
                              style="background: #95a5a6;" 
                              @click="cancelFileEdit">
                        取消
                      </button>
                    </div>
                </template>
              </div>
              <div v-else class="content">
                <!-- 添加控制按钮区域 -->
                <div class="content-controls" style="margin-bottom: 1rem;">
                  <!-- 复制按钮对所有人可见 -->
                  <button class="btn" 
                          @click="copyContent" 
                          style="margin-right: 0.5rem;"
                          v-if="!isFile">
                    复制内容
                  </button>
                  <!-- 编辑按钮仅管理员可见 -->
                  <button v-if="isAdmin && !isFile" 
                          class="btn" 
                          @click="startEdit" 
                          v-show="!isEditing">
                    编辑内容
                  </button>
                  <template v-if="isEditing">
                    <button class="btn" 
                            @click="saveEdit" 
                            style="margin-right: 0.5rem;">
                      保存
                    </button>
                    <button class="btn" 
                            style="background: #95a5a6;" 
                            @click="cancelEdit">
                      取消
                    </button>
                  </template>
                </div>

                <div v-if="isEditing">
                  <!-- 添加编辑器容器 -->
                  <div class="editor-container">
                    <div class="editor">
                      <textarea
                        v-model="editContent"
                        style="width: 100%; height: 100%; padding: 1rem; border: none; outline: none; resize: none;"
                      ></textarea>
                    </div>
                    <!-- Markdown 预览区域 -->
                    <div 
                      v-if="editMarkdown" 
                      class="preview"
                      v-html="editPreview"
                    ></div>
                  </div>

                  <!-- 底部控制区域 -->
                  <div class="settings" style="margin-top: 1rem;">
                    <!-- Markdown 开关 -->
                    <div class="input-group">
                      <div class="markdown-toggle" style="margin-bottom: 0;">
                        <input type="checkbox" id="edit-markdown-toggle" v-model="editMarkdown">
                        <label for="edit-markdown-toggle">启用 Markdown</label>
                      </div>
                    </div>
                    
                    <!-- 过期时间选择框 -->
                    <div class="input-group">
                      <label>过期时间</label>
                      <select v-model="editExpiresIn" class="form-select">
                        <option value="1h">1小时</option>
                        <option value="1d">1天</option>
                        <option value="7d">7天</option>
                        <option value="30d">30天</option>
                        <option value="never">永不过期</option>
                      </select>
                    </div>

                    <!-- 访问次数输入框 -->
                    <div class="input-group">
                      <label>可访问次数</label>
                      <input 
                        type="number" 
                        v-model="editMaxViews"
                        min="0"
                        placeholder="0表示无限制"
                        class="form-input"
                      >
                    </div>
                  </div>
                </div>
                <div v-else>
                  <div v-if="isMarkdown" v-html="renderedContent"></div>
                  <pre v-else>{{ content }}</pre>
                </div>
              </div>
              <div class="expiry-info">
                <span>{{ formatExpiryTime }}</span>
                <span v-if="isEditing && maxViews > 0" class="view-count-info">
                  · 剩余访问次数: {{ maxViews - viewCount }}
                  (已访问 {{ viewCount }} 次)
                </span>
              </div>
            </template>
          </div>
        </div>
      </div>
    </div>
    <script>${shareAppScript}</script>
</body>
</html>`;

// 处理粘贴内容
async function handlePaste(request, env) {
  const url = new URL(request.url);
  const pathParts = url.pathname.split("/");
  const pasteId = pathParts[pathParts.length - 1];

  switch (request.method) {
    case "POST": {
      const data = await request.json();
      const { content, password: inputPassword, expiresIn, isMarkdown = false, customId = "", maxViews = 0 } = data;

      if (!content) {
        return new Response(
          JSON.stringify({
            message: "Content is required",
            status: "error",
          }),
          {
            status: 400,
            headers: { "Content-Type": "application/json" },
          }
        );
      }

      // 验证自定义ID的格式
      if (customId && !/^[a-zA-Z0-9-_]+$/.test(customId)) {
        return new Response(
          JSON.stringify({
            message: "自定义链接后缀只能包含字母、数字、横线和下划线",
            status: "error",
          }),
          {
            status: 400,
            headers: { "Content-Type": "application/json" },
          }
        );
      }

      // 如果提供了自定义ID，先检查是否存在于文本分享中
      if (customId) {
        const existingPaste = await env.PASTE_STORE.get(customId);
        if (existingPaste) {
          return new Response(
            JSON.stringify({
              message: "该链接后缀已被用于文本分享，请更换一个",
              status: "error",
              usedBy: "paste",
            }),
            {
              status: 400,
              headers: { "Content-Type": "application/json" },
            }
          );
        }

        // 检查是否存在于文件分享中 - 只在 FILE_STORE 存在时检查
        if (env.FILE_STORE) {
          const existingFile = await env.FILE_STORE.get(customId);
          if (existingFile) {
            return new Response(
              JSON.stringify({
                message: "该链接后缀已被用于文件分享，请更换一个",
                status: "error",
                usedBy: "file",
              }),
              {
                status: 400,
                headers: { "Content-Type": "application/json" },
              }
            );
          }
        }
      }

      const id = customId || utils.generateId();
      const paste = {
        content,
        isMarkdown,
        createdAt: new Date().toISOString(),
        expiresAt: expiresIn === "never" ? null : utils.calculateExpiryTime(expiresIn)?.toISOString(),
        maxViews: parseInt(maxViews) || 0, // 添加这行
        viewCount: 0, // 添加这行
      };

      if (inputPassword) {
        paste.passwordHash = await utils.hashPassword(inputPassword);
      }

      await env.PASTE_STORE.put(id, JSON.stringify(paste));

      return new Response(
        JSON.stringify({
          id,
          status: "success",
        }),
        {
          headers: { "Content-Type": "application/json" },
          status: 201,
        }
      );
    }

    case "GET": {
      if (url.pathname === "/api/paste") {
        return new Response(
          JSON.stringify({
            message: "Invalid request",
            status: "error",
          }),
          {
            status: 400,
            headers: {
              "Content-Type": "application/json",
              "Cache-Control": "no-store",
            },
          }
        );
      }

      const storedPaste = await env.PASTE_STORE.get(pasteId);
      if (!storedPaste) {
        return new Response(
          JSON.stringify({
            message: "Paste not found",
            status: "error",
          }),
          {
            status: 404,
            headers: {
              "Content-Type": "application/json",
              "Cache-Control": "no-store",
            },
          }
        );
      }

      const paste = JSON.parse(storedPaste);

      if (utils.isExpired(paste.expiresAt)) {
        await env.PASTE_STORE.delete(pasteId);
        return new Response(
          JSON.stringify({
            message: "Paste has expired",
            status: "error",
          }),
          {
            status: 404,
            headers: {
              "Content-Type": "application/json",
              "Cache-Control": "no-store",
            },
          }
        );
      }

      // 先检查是否是管理员
      const isAdmin = await verifyAdmin(request, env);

      // 如果是管理员,跳过密码验证
      if (!isAdmin) {
        const inputPassword = request.headers.get("X-Password");
        if (paste.passwordHash) {
          if (!inputPassword) {
            return new Response(
              JSON.stringify({
                message: "Password required",
                status: "error",
              }),
              {
                status: 401,
                headers: {
                  "Content-Type": "application/json",
                  "Cache-Control": "no-store",
                },
              }
            );
          }
          if (!(await utils.verifyPassword(inputPassword, paste.passwordHash))) {
            return new Response(
              JSON.stringify({
                message: "Invalid password",
                status: "error",
              }),
              {
                status: 403,
                headers: {
                  "Content-Type": "application/json",
                  "Cache-Control": "no-store",
                },
              }
            );
          }
        }
      }

      // 检查访问次数
      if (paste.maxViews > 0) {
        paste.viewCount = (paste.viewCount || 0) + 1;
        await env.PASTE_STORE.put(pasteId, JSON.stringify(paste));

        // 如果达到最大访问次数，删除分享并返回过期信息
        if (paste.viewCount > paste.maxViews) {
          await env.PASTE_STORE.delete(pasteId);
          return new Response(
            JSON.stringify({
              message: "分享已达到最大访问次数",
              status: "error",
            }),
            {
              status: 404,
              headers: {
                "Content-Type": "application/json",
                "Cache-Control": "no-store",
              },
            }
          );
        }
      }

      return new Response(
        JSON.stringify({
          content: paste.content,
          isMarkdown: paste.isMarkdown,
          createdAt: paste.createdAt,
          expiresAt: paste.expiresAt,
          maxViews: paste.maxViews || 0, // 确保返回数值
          viewCount: paste.viewCount || 0, // 确保返回数值
          status: "success",
        }),
        {
          headers: {
            "Content-Type": "application/json",
            "Cache-Control": "no-store, no-cache, must-revalidate, proxy-revalidate",
          },
        }
      );
    }

    default:
      return new Response(
        JSON.stringify({
          message: "Method not allowed",
          status: "error",
        }),
        {
          status: 405,
          headers: { "Content-Type": "application/json" },
        }
      );
  }
}

// 处理文件上传和下载
async function handleFile(request, env, ctx) {
  const url = new URL(request.url);
  const pathParts = url.pathname.split("/");
  const fileId = pathParts[pathParts.length - 1];

  switch (request.method) {
    case "POST": {
      try {
        // 计算当前已使用的存储空间
        let currentStorage = 0;
        const fileList = await env.FILE_STORE.list();
        for (const object of fileList.objects || []) {
          try {
            const file = await env.FILE_STORE.get(object.key);
            if (file && file.customMetadata) {
              currentStorage += parseInt(file.customMetadata.size) || 0;
            }
          } catch (e) {
            console.error("Error calculating storage for file:", object.key, e);
          }
        }

        const formData = await request.formData();
        const files = formData.getAll("files");
        const maxViews = parseInt(formData.get("maxViews")) || 0; // 获取最大访问次数

        // 计算新文件的总大小
        const newFilesSize = files.reduce((total, file) => total + file.size, 0);

        // 检查是否会超出总存储限制
        if (currentStorage + newFilesSize > MAX_TOTAL_STORAGE) {
          return new Response(
            JSON.stringify({
              files: files.map((file) => ({
                filename: file.name,
                error: `上传失败: 总存储空间将超出限制(${(MAX_TOTAL_STORAGE / 1024 / 1024 / 1024).toFixed(1)}GB)`,
                status: "error",
              })),
              message: "存储空间不足",
              status: "error",
              currentStorage: currentStorage,
              maxStorage: MAX_TOTAL_STORAGE,
            }),
            {
              status: 400,
              headers: {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*",
              },
            }
          );
        }

        const customId = formData.get("customId");
        const expiresIn = formData.get("expiresIn") || "1d";
        const inputPassword = formData.get("password");

        // 验证自定义ID的格式
        if (customId && !/^[a-zA-Z0-9-_]+$/.test(customId)) {
          return new Response(
            JSON.stringify({
              files: [
                {
                  filename: files[0].name,
                  error: "自定义链接后缀只能包含字母、数字、横线和下划线",
                  status: "error",
                },
              ],
              message: "自定义链接后缀格式不正确",
              status: "error",
            }),
            {
              status: 400,
              headers: {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*",
              },
            }
          );
        }

        // 如果提供了自定义ID，先检查是否存在于文本分享中
        if (customId) {
          const existingPaste = await env.PASTE_STORE.get(customId);
          if (existingPaste) {
            return new Response(
              JSON.stringify({
                files: [
                  {
                    filename: files[0].name,
                    error: "该链接后缀已被用于文本分享，请更换一个",
                    status: "error",
                    usedBy: "paste",
                  },
                ],
                message: "链接后缀已被使用",
                status: "error",
              }),
              {
                status: 400,
                headers: {
                  "Content-Type": "application/json",
                  "Access-Control-Allow-Origin": "*",
                },
              }
            );
          }

          // 检查是否存在于文件分享中
          const existingFile = await env.FILE_STORE.get(customId);
          if (existingFile) {
            return new Response(
              JSON.stringify({
                files: [
                  {
                    filename: files[0].name,
                    error: "该链接后缀已被用于文件分享，请更换一个",
                    status: "error",
                    usedBy: "file",
                  },
                ],
                message: "链接后缀已被使用",
                status: "error",
              }),
              {
                status: 400,
                headers: {
                  "Content-Type": "application/json",
                  "Access-Control-Allow-Origin": "*",
                },
              }
            );
          }
        }

        console.log("Files received:", {
          count: files.length,
          fileInfo: files.map((f) => ({
            name: f.name,
            size: f.size,
            type: f.type,
          })),
        });

        if (!files || files.length === 0) {
          return new Response(
            JSON.stringify({
              files: [],
              message: "请选择要上传的文件",
              status: "error",
            }),
            {
              status: 400,
              headers: {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*",
              },
            }
          );
        }

        const uploadResults = [];
        let hasSuccess = false;

        for (const file of files) {
          try {
            if (!file || !(file instanceof File)) {
              console.log("Invalid file object:", file);
              continue;
            }

            console.log("Processing file:", {
              name: file.name,
              size: file.size,
              type: file.type,
            });

            if (file.size > MAX_FILE_SIZE) {
              uploadResults.push({
                filename: file.name,
                error: `文件大小超过限制(${MAX_FILE_SIZE / 1024 / 1024}MB)`,
                status: "error",
              });
              continue;
            }

            // 生成或使用自定义文件ID
            const id = customId || utils.generateId(12);

            // 检查自定义ID是否已存在
            if (customId) {
              // 检查是否存在于文本分享中
              const existingPaste = await env.PASTE_STORE.get(id);
              if (existingPaste) {
                uploadResults.push({
                  filename: file.name,
                  error: "该链接后缀已被用于文本分享，请更换一个",
                  status: "error",
                });
                continue;
              }

              // 检查是否存在于文件分享中
              const existingFile = await env.FILE_STORE.get(id);
              if (existingFile) {
                uploadResults.push({
                  filename: file.name,
                  error: "该链接后缀已被用于文件分享，请更换一个",
                  status: "error",
                });
                continue;
              }
            }

            // 准备元数据
            const metadata = {
              filename: file.name,
              type: file.type || "application/octet-stream",
              size: file.size,
              uploadedAt: new Date().toISOString(),
              expiresAt: expiresIn === "never" ? null : utils.calculateExpiryTime(expiresIn)?.toISOString(),
              maxViews: maxViews, // 添加最大访问次数
              viewCount: 0, // 初始化访问计数
            };

            if (inputPassword) {
              metadata.passwordHash = await utils.hashPassword(inputPassword);
            }

            // 读取文件内容
            const arrayBuffer = await file.arrayBuffer();

            // 检查文件内容是否有效
            if (!arrayBuffer || arrayBuffer.byteLength === 0) {
              throw new Error("文件内容无效");
            }

            console.log("Uploading file to R2:", {
              id,
              size: arrayBuffer.byteLength,
              metadata,
            });

            // 上传到 R2
            await env.FILE_STORE.put(id, arrayBuffer, {
              customMetadata: metadata,
            });

            console.log("File uploaded successfully:", id);

            uploadResults.push({
              fileId: id,
              filename: file.name,
              expiresAt: metadata.expiresAt,
              maxViews: metadata.maxViews, // 在返回结果中添加最大访问次数
              viewCount: 0,
              status: "success",
              url: `${url.origin}/share/file/${id}`,
              directDownloadUrl: `${url.origin}/download/${id}`, // 添加直接下载链接
            });
            hasSuccess = true;
          } catch (uploadError) {
            console.error("File upload error:", {
              filename: file.name,
              error: uploadError.message,
              stack: uploadError.stack,
            });

            uploadResults.push({
              filename: file.name,
              error: uploadError.message,
              status: "error",
            });
          }
        }

        return new Response(
          JSON.stringify({
            files: uploadResults,
            status: hasSuccess ? "success" : "error",
            message: hasSuccess ? "上传成功" : "所有文件上传失败",
            successCount: uploadResults.filter((r) => r.status === "success").length,
            totalCount: files.length,
          }),
          {
            status: hasSuccess ? 201 : 400,
            headers: {
              "Content-Type": "application/json",
              "Access-Control-Allow-Origin": "*",
            },
          }
        );
      } catch (error) {
        console.error("Upload handler error:", {
          message: error.message,
          stack: error.stack,
        });

        return new Response(
          JSON.stringify({
            files: [],
            message: "上传失败: " + error.message,
            status: "error",
          }),
          {
            status: 500,
            headers: {
              "Content-Type": "application/json",
              "Access-Control-Allow-Origin": "*",
            },
          }
        );
      }
    }

    case "GET": {
      if (url.pathname === "/api/file") {
        return new Response("Invalid request", { status: 400 });
      }

      try {
        const file = await env.FILE_STORE.get(fileId);

        if (!file) {
          return new Response(
            JSON.stringify({
              message: "File not found",
              status: "error",
            }),
            {
              status: 404,
              headers: { "Content-Type": "application/json" },
            }
          );
        }

        const metadata = file.customMetadata;

        if (utils.isExpired(metadata.expiresAt)) {
          await env.FILE_STORE.delete(fileId);
          return new Response(
            JSON.stringify({
              message: "File has expired",
              status: "error",
            }),
            {
              status: 404,
              headers: { "Content-Type": "application/json" },
            }
          );
        }

        // 先检查是否是管理员
        const isAdmin = await verifyAdmin(request, env);

        // 如果是管理员,跳过密码验证
        if (!isAdmin) {
          const inputPassword = request.headers.get("X-Password");
          if (metadata.passwordHash) {
            if (!inputPassword) {
              return new Response(
                JSON.stringify({
                  message: "Password required",
                  status: "error",
                }),
                {
                  status: 401,
                  headers: { "Content-Type": "application/json" },
                }
              );
            }
            if (!(await utils.verifyPassword(inputPassword, metadata.passwordHash))) {
              return new Response(
                JSON.stringify({
                  message: "Invalid password",
                  status: "error",
                }),
                {
                  status: 403,
                  headers: { "Content-Type": "application/json" },
                }
              );
            }
          }
        }

        // 添加一个查询参数来区分是获取文件信息还是下载文件
        const isDownload = url.searchParams.get("download") === "true";

        if (!isDownload) {
          // 返回文件信息
          return new Response(
            JSON.stringify({
              filename: metadata.filename,
              type: metadata.type,
              size: metadata.size,
              uploadedAt: metadata.uploadedAt,
              expiresAt: metadata.expiresAt,
              maxViews: parseInt(metadata.maxViews) || 0,
              viewCount: parseInt(metadata.viewCount) || 0,
              status: "success",
              url: `${url.origin}/share/file/${fileId}`,
              directDownloadUrl: `${url.origin}/download/${fileId}`, // 添加直接下载链接
            }),
            {
              headers: {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*",
              },
            }
          );
        }

        // 处理文件下载...
        const arrayBuffer = await file.arrayBuffer(); // 先获取文件内容

        // 添加: 如果是下载请求，检查并处理下载次数
        if (isDownload && metadata.maxViews > 0) {
          const currentCount = parseInt(metadata.viewCount) || 0;
          const newCount = currentCount + 1;

          // 准备更新后的元数据
          const updatedMetadata = {
            ...metadata,
            viewCount: newCount.toString(),
          };

          // 先准备下载响应
          const stream = new Response(arrayBuffer).body;
          const contentLength = metadata.size;

          // 处理文件名
          const filename = metadata.filename;
          const isASCII = /^[\x00-\x7F]*$/.test(filename);
          let contentDisposition;

          if (isASCII) {
            contentDisposition = `attachment; filename="${filename}"`;
          } else {
            const encodedFilename = encodeURIComponent(filename).replace(/['()]/g, escape);
            contentDisposition = `attachment; filename*=UTF-8''${encodedFilename}`;
          }

          const progress = new TransformStream({
            start(controller) {
              this.loaded = 0;
            },
            transform(chunk, controller) {
              this.loaded += chunk.byteLength;
              controller.enqueue(chunk);
            },
          });

          // 创建响应对象
          const response = new Response(stream.pipeThrough(progress), {
            headers: {
              "Content-Type": metadata.type || "application/octet-stream",
              "Content-Disposition": contentDisposition,
              "Content-Length": contentLength,
              "Access-Control-Allow-Origin": "*",
              "Access-Control-Expose-Headers": "Download-Progress",
            },
          });

          // 在返回响应之前更新计数
          if (newCount >= parseInt(metadata.maxViews)) {
            // 如果这是最后一次下载，设置删除标记
            ctx.waitUntil(
              (async () => {
                // 先更新元数据
                await env.FILE_STORE.put(fileId, arrayBuffer, {
                  customMetadata: updatedMetadata,
                });
                // 等待一小段时间后删除文件
                await new Promise((resolve) => setTimeout(resolve, 1000));
                await env.FILE_STORE.delete(fileId);
              })()
            );
          } else {
            // 如果还有剩余次数，只更新计数
            ctx.waitUntil(
              env.FILE_STORE.put(fileId, arrayBuffer, {
                customMetadata: updatedMetadata,
              })
            );
          }

          return response;
        } else {
          // 非下载请求或无下载限制的处理
          const stream = new Response(arrayBuffer).body;
          const contentLength = metadata.size;

          // 处理文件名
          const filename = metadata.filename;
          const isASCII = /^[\x00-\x7F]*$/.test(filename);
          let contentDisposition;

          if (isASCII) {
            contentDisposition = `attachment; filename="${filename}"`;
          } else {
            const encodedFilename = encodeURIComponent(filename).replace(/['()]/g, escape);
            contentDisposition = `attachment; filename*=UTF-8''${encodedFilename}`;
          }

          const progress = new TransformStream({
            start(controller) {
              this.loaded = 0;
            },
            transform(chunk, controller) {
              this.loaded += chunk.byteLength;
              controller.enqueue(chunk);
            },
          });

          return new Response(stream.pipeThrough(progress), {
            headers: {
              "Content-Type": metadata.type || "application/octet-stream",
              "Content-Disposition": contentDisposition,
              "Content-Length": contentLength,
              "Access-Control-Allow-Origin": "*",
              "Access-Control-Expose-Headers": "Download-Progress",
            },
          });
        }
      } catch (error) {
        return new Response(
          JSON.stringify({
            message: "Download failed: " + error.message,
            status: "error",
          }),
          {
            status: 500,
            headers: {
              "Content-Type": "application/json",
              "Access-Control-Allow-Origin": "*",
            },
          }
        );
      }
    }

    default:
      return new Response(
        JSON.stringify({
          message: "Method not allowed",
          status: "error",
        }),
        {
          status: 405,
          headers: {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
          },
        }
      );
  }
}

// 验证管理员权限的辅助函数
async function verifyAdmin(request, env) {
  try {
    const authHeader = request.headers.get("Authorization");
    if (!authHeader || !authHeader.startsWith("Basic ")) {
      console.log("Missing or invalid Authorization header");
      return false;
    }

    const credentials = atob(authHeader.slice(6));
    const [username, password] = credentials.split(":");

    console.log("Verifying admin credentials:", { username });

    const isValid = username === env.ADMIN_USERNAME && password === env.ADMIN_PASSWORD;
    console.log("Admin verification result:", isValid);

    return isValid;
  } catch (error) {
    console.error("Admin verification error:", error);
    return false;
  }
}

// 添加清理过期内容的函数
async function cleanupExpiredContent(env) {
  try {
    const now = new Date();
    let cleanedCount = 0;
    const errors = [];

    // 并行处理文本和文件清理
    const [pasteResults, fileResults] = await Promise.allSettled([
      // 清理过期的文本分享
      (async () => {
        const pasteList = await env.PASTE_STORE.list();
        let pasteCount = 0;

        for (const key of pasteList.keys) {
          try {
            if (key.name === "last_cleanup") continue; // 跳过清理记录键

            const paste = JSON.parse(await env.PASTE_STORE.get(key.name));

            // 检查过期时间或访问次数限制
            const isExpired = paste.expiresAt && new Date(paste.expiresAt) < now;
            const isMaxViewsReached = paste.maxViews > 0 && paste.viewCount >= paste.maxViews;

            if (isExpired || isMaxViewsReached) {
              await env.PASTE_STORE.delete(key.name);
              pasteCount++;
              console.log(`Deleted expired paste: ${key.name}`);
            }
          } catch (e) {
            errors.push(`Error cleaning paste ${key.name}: ${e.message}`);
          }
        }
        return pasteCount;
      })(),

      // 清理过期的文件
      (async () => {
        const fileList = await env.FILE_STORE.list();
        let fileCount = 0;

        for (const object of fileList.objects || []) {
          try {
            const file = await env.FILE_STORE.get(object.key);
            if (!file) continue;

            const metadata = file.customMetadata;
            if (!metadata) continue;

            // 检查过期时间或下载次数限制
            const isExpired = metadata.expiresAt && new Date(metadata.expiresAt) < now;
            const isMaxViewsReached = parseInt(metadata.maxViews) > 0 && parseInt(metadata.viewCount) >= parseInt(metadata.maxViews);

            if (isExpired || isMaxViewsReached) {
              await env.FILE_STORE.delete(object.key);
              fileCount++;
              console.log(`Deleted expired file: ${object.key}`);
            }
          } catch (e) {
            errors.push(`Error cleaning file ${object.key}: ${e.message}`);
          }
        }
        return fileCount;
      })(),
    ]);

    // 计算总清理数量
    const pasteCount = pasteResults.status === "fulfilled" ? pasteResults.value : 0;
    const fileCount = fileResults.status === "fulfilled" ? fileResults.value : 0;
    cleanedCount = pasteCount + fileCount;

    // 记录错误
    if (errors.length > 0) {
      console.error("Cleanup errors:", errors);
    }

    console.log(`Cleanup completed: ${cleanedCount} items removed (${pasteCount} pastes, ${fileCount} files)`);
    return cleanedCount;
  } catch (e) {
    console.error("Cleanup error:", e);
    return 0;
  }
}

// 在 handleFile 和 handlePaste 函数之间添加新的函数
async function handleUploadStatus(request, env) {
  // 验证管理员权限
  if (!(await verifyAdmin(request, env))) {
    return new Response(
      JSON.stringify({
        status: "error",
        message: "未授权访问",
      }),
      {
        status: 401,
        headers: { "Content-Type": "application/json" },
      }
    );
  }

  switch (request.method) {
    case "GET": {
      // 获取上传状态
      const textStatus = (await env.UPLOAD_STATUS.get("textUpload")) || "false";
      const fileStatus = (await env.UPLOAD_STATUS.get("fileUpload")) || "false";

      return new Response(
        JSON.stringify({
          status: "success",
          textUpload: textStatus === "true",
          fileUpload: fileStatus === "true",
        }),
        {
          headers: { "Content-Type": "application/json" },
        }
      );
    }

    case "PUT": {
      // 更新上传状态
      const { textUpload, fileUpload } = await request.json();

      await env.UPLOAD_STATUS.put("textUpload", String(textUpload));
      await env.UPLOAD_STATUS.put("fileUpload", String(fileUpload));

      return new Response(
        JSON.stringify({
          status: "success",
          message: "上传状态已更新",
        }),
        {
          headers: { "Content-Type": "application/json" },
        }
      );
    }

    default:
      return new Response("Method not allowed", { status: 405 });
  }
}

// Worker 导出
// Worker 导出
export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    // 修改清理逻辑，使其更可靠
    try {
      // 获取上次清理时间
      const lastCleanup = await env.PASTE_STORE.get("last_cleanup");
      const now = new Date();

      // 如果没有上次清理记录，或者距离上次清理已经过去了至少1小时
      if (!lastCleanup || now - new Date(lastCleanup) >= 60 * 60 * 1000) {
        // 使用 waitUntil 确保清理操作在响应返回后继续执行
        ctx.waitUntil(
          (async () => {
            try {
              const cleanedCount = await cleanupExpiredContent(env);
              console.log(`Cleaned up ${cleanedCount} expired items at ${now.toISOString()}`);
              // 更新最后清理时间
              await env.PASTE_STORE.put("last_cleanup", now.toISOString());
            } catch (error) {
              console.error("Cleanup error:", error);
            }
          })()
        );
      }
    } catch (error) {
      console.error("Error checking cleanup status:", error);
    }

    // 处理管理员 API
    if (url.pathname.startsWith("/api/admin/")) {
      // 处理管理员登录
      if (url.pathname === "/api/admin/login") {
        if (request.method !== "POST") {
          return new Response("Method not allowed", { status: 405 });
        }

        const { username, password } = await request.json();
        if (username === env.ADMIN_USERNAME && password === env.ADMIN_PASSWORD) {
          return new Response(
            JSON.stringify({
              status: "success",
              message: "登录成功",
              credentials: btoa(`${username}:${password}`), // 添加这行
            }),
            {
              headers: {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "GET, POST, DELETE, OPTIONS",
                "Access-Control-Allow-Headers": "Content-Type, Authorization",
              },
            }
          );
        }

        return new Response(
          JSON.stringify({
            status: "error",
            message: "用户名或密码错误",
          }),
          {
            status: 401,
            headers: {
              "Content-Type": "application/json",
              "Access-Control-Allow-Origin": "*",
            },
          }
        );
      }

      // 获取所有分享列表
      if (url.pathname === "/api/admin/shares") {
        if (request.method !== "GET") {
          return new Response("Method not allowed", { status: 405 });
        }

        try {
          // 验证管理员权限
          if (!(await verifyAdmin(request, env))) {
            return new Response(
              JSON.stringify({
                status: "error",
                message: "未授权访问",
              }),
              {
                status: 401,
                headers: {
                  "Content-Type": "application/json",
                  "Access-Control-Allow-Origin": "*",
                  "Access-Control-Allow-Headers": "Content-Type, Authorization",
                },
              }
            );
          }

          const shares = [];

          // 并行获取文本和文件分享
          await Promise.all([
            // 获取文本分享
            (async () => {
              try {
                const pasteList = await env.PASTE_STORE.list();
                // 并行获取所有文本分享的内容
                const pastesPromises = pasteList.keys.map(async (key) => {
                  try {
                    const paste = JSON.parse(await env.PASTE_STORE.get(key.name));
                    return {
                      id: key.name,
                      type: "paste",
                      content: paste.content?.substring(0, 100) + "...", // 只获取前100个字符
                      createdAt: paste.createdAt,
                      expiresAt: paste.expiresAt,
                      hasPassword: !!paste.passwordHash,
                      isMarkdown: paste.isMarkdown,
                      url: `${url.origin}/share/paste/${key.name}`,
                      maxViews: paste.maxViews || 0,
                      viewCount: paste.viewCount || 0,
                    };
                  } catch (e) {
                    console.error("Error processing paste:", key.name, e);
                    return null; // 返回 null 表示处理失败
                  }
                });

                // 等待所有文本分享处理完成并过滤掉失败的
                const validPastes = (await Promise.all(pastesPromises)).filter((paste) => paste !== null);
                shares.push(...validPastes);
              } catch (e) {
                console.error("Error listing pastes:", e);
              }
            })(),

            // 获取文件分享
            (async () => {
              try {
                const fileList = await env.FILE_STORE.list();
                // 并行处理所有文件
                const filePromises = (fileList.objects || []).map(async (object) => {
                  try {
                    const file = await env.FILE_STORE.get(object.key);
                    if (!file || !file.customMetadata) return null;

                    const metadata = file.customMetadata;
                    return {
                      id: object.key,
                      type: "file",
                      filename: metadata.filename || object.key,
                      size: metadata.size || object.size,
                      createdAt: metadata.uploadedAt || object.uploaded,
                      expiresAt: metadata.expiresAt,
                      hasPassword: !!metadata.passwordHash,
                      url: `${url.origin}/share/file/${object.key}`,
                      directDownloadUrl: `${url.origin}/download/${object.key}`, // 添加直接下载链接
                      maxViews: parseInt(metadata.maxViews) || 0,
                      viewCount: parseInt(metadata.viewCount) || 0,
                    };
                  } catch (e) {
                    console.error("Error processing file:", object.key, e);
                    return null;
                  }
                });

                // 等待所有文件处理完成并过滤掉失败的
                const validFiles = (await Promise.all(filePromises)).filter((file) => file !== null);
                shares.push(...validFiles);
              } catch (e) {
                console.error("Error listing files:", e);
              }
            })(),
          ]);

          // 按创建时间排序
          shares.sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt));

          return new Response(
            JSON.stringify({
              status: "success",
              shares,
            }),
            {
              headers: {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "Content-Type, Authorization",
                "Cache-Control": "no-store",
              },
            }
          );
        } catch (error) {
          console.error("Get shares error:", error);
          return new Response(
            JSON.stringify({
              status: "error",
              message: "获取分享列表失败: " + error.message,
            }),
            {
              status: 500,
              headers: {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "Content-Type, Authorization",
              },
            }
          );
        }
      }

      // 删除分享
      if (url.pathname.match(/^\/api\/admin\/(paste|file)\/[a-zA-Z0-9]+$/)) {
        if (request.method !== "DELETE") {
          return new Response("Method not allowed", { status: 405 });
        }

        // 验证管理员权限
        if (!(await verifyAdmin(request, env))) {
          return new Response("Unauthorized", { status: 401 });
        }

        try {
          const pathParts = url.pathname.split("/");
          const type = pathParts[pathParts.length - 2];
          const id = pathParts[pathParts.length - 1];

          if (type === "paste") {
            await env.PASTE_STORE.delete(id);
          } else {
            await env.FILE_STORE.delete(id);
          }

          return new Response(
            JSON.stringify({
              status: "success",
              message: "删除成功",
            }),
            {
              headers: { "Content-Type": "application/json" },
            }
          );
        } catch (error) {
          return new Response(
            JSON.stringify({
              status: "error",
              message: "删除失败",
            }),
            {
              status: 500,
              headers: { "Content-Type": "application/json" },
            }
          );
        }
      }
    } // 删除多余的大括号,只保留这一个

    // 处理CORS预检请求
    if (request.method === "OPTIONS") {
      return new Response(null, {
        headers: {
          "Access-Control-Allow-Origin": "*",
          "Access-Control-Allow-Methods": "GET, POST, DELETE, PUT, OPTIONS", // 添加 PUT 方法
          "Access-Control-Allow-Headers": "Content-Type, X-Password, Authorization",
          "Access-Control-Max-Age": "86400",
        },
      });
    }

    // 处理 API 请求 - 移到前面，优先处理
    if (url.pathname.startsWith("/api/")) {
      try {
        let response;
        const corsHeaders = {
          "Access-Control-Allow-Origin": "*",
          "Access-Control-Allow-Methods": "GET, POST, DELETE, PUT, OPTIONS",
          "Access-Control-Allow-Headers": "Content-Type, X-Password, Authorization",
        };

        if (url.pathname === "/api/admin/upload-status") {
          response = await handleUploadStatus(request, env);
        } else if (url.pathname.startsWith("/api/paste")) {
          response = await handlePaste(request, env);
        } else if (url.pathname.startsWith("/api/file")) {
          response = await handleFile(request, env, ctx); // 修改这里，传入 ctx
        } else if (url.pathname.startsWith("/api/admin/")) {
          // 添加对管理员 API 的处理
          if (url.pathname.match(/^\/api\/admin\/(paste|file)\/[a-zA-Z0-9-_]+\/password$/)) {
            if (request.method !== "PUT") {
              return new Response("Method not allowed", {
                status: 405,
                headers: corsHeaders,
              });
            }

            // 验证管理员权限
            if (!(await verifyAdmin(request, env))) {
              return new Response("Unauthorized", {
                status: 401,
                headers: corsHeaders,
              });
            }

            try {
              const pathParts = url.pathname.split("/");
              const type = pathParts[pathParts.length - 3];
              const id = pathParts[pathParts.length - 2];
              const { password } = await request.json();

              if (type === "paste") {
                const storedPaste = await env.PASTE_STORE.get(id);
                if (!storedPaste) {
                  return new Response(
                    JSON.stringify({
                      status: "error",
                      message: "分享不存在",
                    }),
                    {
                      status: 404,
                      headers: { ...corsHeaders, "Content-Type": "application/json" },
                    }
                  );
                }

                const paste = JSON.parse(storedPaste);
                if (password) {
                  paste.passwordHash = await utils.hashPassword(password);
                } else {
                  delete paste.passwordHash;
                }

                await env.PASTE_STORE.put(id, JSON.stringify(paste));
              } else {
                const file = await env.FILE_STORE.get(id);
                if (!file) {
                  return new Response(
                    JSON.stringify({
                      status: "error",
                      message: "分享不存在",
                    }),
                    {
                      status: 404,
                      headers: { ...corsHeaders, "Content-Type": "application/json" },
                    }
                  );
                }

                const metadata = file.customMetadata;
                if (password) {
                  metadata.passwordHash = await utils.hashPassword(password);
                } else {
                  delete metadata.passwordHash;
                }

                await env.FILE_STORE.put(id, await file.arrayBuffer(), {
                  customMetadata: metadata,
                });
              }

              return new Response(
                JSON.stringify({
                  status: "success",
                  message: "密码修改成功",
                }),
                {
                  headers: { ...corsHeaders, "Content-Type": "application/json" },
                }
              );
            } catch (error) {
              return new Response(
                JSON.stringify({
                  status: "error",
                  message: "修改密码失败",
                }),
                {
                  status: 500,
                  headers: { ...corsHeaders, "Content-Type": "application/json" },
                }
              );
            }
          }

          // 添加处理更新文本内容的路由
          if (url.pathname.match(/^\/api\/admin\/paste\/[a-zA-Z0-9-_]+\/content$/)) {
            if (request.method !== "PUT") {
              return new Response("Method not allowed", {
                status: 405,
                headers: corsHeaders,
              });
            }

            // 验证管理员权限
            if (!(await verifyAdmin(request, env))) {
              return new Response("Unauthorized", {
                status: 401,
                headers: corsHeaders,
              });
            }

            try {
              const pathParts = url.pathname.split("/");
              const id = pathParts[pathParts.length - 2];
              const { content, isMarkdown, expiresIn, maxViews } = await request.json();

              const storedPaste = await env.PASTE_STORE.get(id);
              if (!storedPaste) {
                return new Response(
                  JSON.stringify({
                    status: "error",
                    message: "分享不存在",
                  }),
                  {
                    status: 404,
                    headers: { ...corsHeaders, "Content-Type": "application/json" },
                  }
                );
              }

              const paste = JSON.parse(storedPaste);
              paste.content = content;
              paste.isMarkdown = isMarkdown;
              paste.expiresAt = expiresIn === "never" ? null : utils.calculateExpiryTime(expiresIn)?.toISOString();

              // 如果修改了最大访问次数，重置访问计数
              const newMaxViews = parseInt(maxViews) || 0;
              if (paste.maxViews !== newMaxViews) {
                paste.maxViews = newMaxViews;
                paste.viewCount = 0; // 重置访问计数
              }

              await env.PASTE_STORE.put(id, JSON.stringify(paste));

              return new Response(
                JSON.stringify({
                  status: "success",
                  message: "内容已更新",
                  expiresAt: paste.expiresAt,
                  maxViews: paste.maxViews,
                  viewCount: paste.viewCount,
                }),
                {
                  headers: {
                    ...corsHeaders,
                    "Content-Type": "application/json",
                    "Cache-Control": "no-store, no-cache, must-revalidate, proxy-revalidate",
                  },
                }
              );
            } catch (error) {
              return new Response(
                JSON.stringify({
                  status: "error",
                  message: "更新失败",
                }),
                {
                  status: 500,
                  headers: { ...corsHeaders, "Content-Type": "application/json" },
                }
              );
            }
          }

          // 在 Worker 中添加一个新的 API 路由来获取存储信息
          if (url.pathname === "/api/admin/storage") {
            if (request.method !== "GET") {
              return new Response("Method not allowed", { status: 405 });
            }

            try {
              // 验证管理员权限
              if (!(await verifyAdmin(request, env))) {
                return new Response(
                  JSON.stringify({
                    status: "error",
                    message: "未授权访问",
                  }),
                  {
                    status: 401,
                    headers: {
                      "Content-Type": "application/json",
                      "Access-Control-Allow-Origin": "*",
                    },
                  }
                );
              }

              // 计算当前已使用的存储空间
              let currentStorage = 0;
              const fileList = await env.FILE_STORE.list();
              for (const object of fileList.objects || []) {
                try {
                  const file = await env.FILE_STORE.get(object.key);
                  if (file && file.customMetadata) {
                    currentStorage += parseInt(file.customMetadata.size) || 0;
                  }
                } catch (e) {
                  console.error("Error calculating storage for file:", object.key, e);
                }
              }

              return new Response(
                JSON.stringify({
                  status: "success",
                  storage: {
                    used: currentStorage,
                    total: MAX_TOTAL_STORAGE,
                    percentage: (currentStorage / MAX_TOTAL_STORAGE) * 100,
                  },
                }),
                {
                  headers: {
                    "Content-Type": "application/json",
                    "Access-Control-Allow-Origin": "*",
                  },
                }
              );
            } catch (error) {
              return new Response(
                JSON.stringify({
                  status: "error",
                  message: "获取存储信息失败: " + error.message,
                }),
                {
                  status: 500,
                  headers: {
                    "Content-Type": "application/json",
                    "Access-Control-Allow-Origin": "*",
                  },
                }
              );
            }
          }

          // 在 handleFile 函数中添加新的路由处理
          if (url.pathname.match(/^\/api\/admin\/file\/[a-zA-Z0-9-_]+\/settings$/)) {
            if (request.method !== "PUT") {
              return new Response("Method not allowed", { status: 405 });
            }

            // 验证管理员权限
            if (!(await verifyAdmin(request, env))) {
              return new Response("Unauthorized", { status: 401 });
            }

            try {
              const pathParts = url.pathname.split("/");
              const id = pathParts[pathParts.length - 2];
              const { expiresIn, maxViews } = await request.json();

              const file = await env.FILE_STORE.get(id);
              if (!file) {
                return new Response(
                  JSON.stringify({
                    status: "error",
                    message: "文件不存在",
                  }),
                  {
                    status: 404,
                    headers: { "Content-Type": "application/json" },
                  }
                );
              }

              const metadata = file.customMetadata;
              metadata.expiresAt = expiresIn === "never" ? null : utils.calculateExpiryTime(expiresIn)?.toISOString();
              // 如果修改了最大下载次数，重置下载计数
              const newMaxViews = parseInt(maxViews) || 0;
              if (metadata.maxViews !== newMaxViews) {
                metadata.maxViews = newMaxViews;
                metadata.viewCount = 0;
              }

              await env.FILE_STORE.put(id, await file.arrayBuffer(), {
                customMetadata: metadata,
              });

              return new Response(
                JSON.stringify({
                  status: "success",
                  message: "设置已更新",
                  expiresAt: metadata.expiresAt,
                  maxViews: metadata.maxViews,
                  viewCount: metadata.viewCount,
                }),
                {
                  headers: { "Content-Type": "application/json" },
                }
              );
            } catch (error) {
              return new Response(
                JSON.stringify({
                  status: "error",
                  message: "更新失败",
                }),
                {
                  status: 500,
                  headers: { "Content-Type": "application/json" },
                }
              );
            }
          }

          response = new Response("Not Found", { status: 404 });
        }

        return new Response(response.body, {
          status: response.status,
          statusText: response.statusText,
          headers: { ...Object.fromEntries(response.headers), ...corsHeaders },
        });
      } catch (err) {
        return new Response(
          JSON.stringify({
            message: err.message,
            status: "error",
          }),
          {
            status: 500,
            headers: { "Content-Type": "application/json" },
          }
        );
      }
    }

    // 处理分享页面
    if (url.pathname.startsWith("/share/paste/") || url.pathname.startsWith("/share/file/")) {
      return new Response(shareHtml, {
        headers: {
          "Content-Type": "text/html",
          "Access-Control-Allow-Origin": "*",
        },
      });
    }

    // 重定向 API 直接访问到分享页面
    if (url.pathname.match(/^\/paste\/[a-zA-Z0-9]+$/)) {
      const id = url.pathname.split("/").pop();
      return Response.redirect(`${url.origin}/share/paste/${id}`, 301);
    }

    if (url.pathname.match(/^\/file\/[a-zA-Z0-9]+$/)) {
      const id = url.pathname.split("/").pop();
      return Response.redirect(`${url.origin}/share/file/${id}`, 301);
    }

    // 添加直接下载路由
    if (url.pathname.match(/^\/download\/[a-zA-Z0-9]+$/)) {
      const id = url.pathname.split("/").pop();
      const modifiedRequest = new Request(`${url.origin}/api/file/${id}?download=true`, {
        method: "GET",
        headers: request.headers,
      });
      return handleFile(modifiedRequest, env, ctx);
    }

    // 处理主页
    return new Response(html, {
      headers: {
        "Content-Type": "text/html",
        "Access-Control-Allow-Origin": "*",
      },
    });
  },
};
