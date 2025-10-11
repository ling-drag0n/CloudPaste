export default {
  pwa: {
    // 状态指示器
    status: {
      title: "PWA 状态详情",
      offline: "离线模式",
      updateAvailable: "有新版本",
      updating: "更新中",
      installable: "可安装",
      ready: "PWA 就绪",
    },

    // 状态标签
    labels: {
      networkStatus: "网络状态",
      installStatus: "安装状态",
      serviceWorker: "Service Worker",
      updateStatus: "更新状态",
      notificationPermission: "通知权限",
      backgroundSync: "后台同步",
      appVersion: "应用版本",
    },

    // 网络状态
    network: {
      online: "在线",
      offline: "离线",
    },

    // 安装状态
    install: {
      installed: "已安装",
      notInstalled: "未安装",
      installable: "可安装",
      installing: "安装中",
      installApp: "安装应用",
      installPrompt: "将 CloudPaste 添加到主屏幕，获得更好的使用体验",
    },

    // Service Worker 状态
    serviceWorker: {
      unknown: "未知",
      installing: "安装中",
      waiting: "等待中",
      active: "活跃",
      redundant: "已废弃",
    },

    // 更新状态
    update: {
      latest: "最新版本",
      available: "有更新",
      updating: "更新中",
      updateApp: "立即更新",
      checkUpdate: "检查更新",
      checking: "检查中",
      updatePrompt: "发现新版本，点击更新获得最新功能",
      updateSuccess: "更新成功",
      updateFailed: "更新失败",
    },

    // 通知权限
    notification: {
      default: "未设置",
      granted: "已授权",
      denied: "已拒绝",
      enable: "启用通知",
      requesting: "请求中",
      requestFailed: "请求通知权限失败",
    },

    // 后台同步
    backgroundSync: {
      supported: "支持",
      notSupported: "不支持",
      syncing: "同步中",
      syncComplete: "同步完成",
      syncFailed: "同步失败",
    },

    // 离线提示
    offline: {
      message: "您已离线，部分功能可能受限",
      restored: "网络已恢复，正在同步数据...",
      pageUnavailable: "{page}暂时无法访问，请检查网络连接",
    },

    // 安装提示
    installPrompt: {
      title: "安装应用",
      message: "将 CloudPaste 添加到主屏幕，获得更好的使用体验",
      install: "安装",
      later: "稍后",
      dismiss: "稍后",
    },

    // 更新提示
    updatePrompt: {
      title: "新版本可用",
      message: "发现新版本，点击更新获得最新功能",
      update: "更新",
      later: "稍后",
      dismiss: "稍后",
    },

    // 操作按钮
    actions: {
      install: "安装",
      update: "更新",
      checkUpdate: "检查更新",
      enableNotification: "启用通知",
      close: "关闭",
      later: "稍后",
      dismiss: "稍后",
    },

    // 错误消息
    errors: {
      installFailed: "安装失败",
      updateFailed: "更新失败",
      notificationFailed: "通知权限请求失败",
      syncFailed: "数据同步失败，请稍后重试",
      notSupported: "您的浏览器不支持此功能",
    },

    // 成功消息
    success: {
      installed: "应用已成功安装",
      updated: "应用已更新到最新版本",
      notificationEnabled: "通知权限已启用",
      syncComplete: "数据同步完成",
    },
  },
};
