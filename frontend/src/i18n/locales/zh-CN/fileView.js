export default {
  fileView: {
    title: "文件预览",
    loading: "加载中...",
    error: "加载失败",
    notFound: "文件不存在",

    // 文件信息
    fileInfo: {
      filename: "文件名",
      fileSize: "文件大小",
      uploadTime: "上传时间",
      mimetype: "文件类型",
      fileLink: "文件链接",
      needPassword: "需要密码访问",
      copyLink: "复制链接",
      linkCopied: "链接已复制到剪贴板",
      copyFailed: "复制失败",
      accessCount: "访问次数",
      expiresAt: "过期时间",
      accessMode: "访问模式",
      proxyAccess: "Worker代理访问",
      directAccess: "S3/CDN直链访问",
      limit: "限制",
    },

    // 密码验证
    password: {
      title: "请输入访问密码",
      description: "此文件已被密码保护，请输入密码查看内容",
      label: "密码",
      placeholder: "请输入密码",
      submit: "确认",
      error: "密码错误，请重试",
      loading: "验证中...",
      showPassword: "显示密码",
      hidePassword: "隐藏密码",
    },

    // 文件操作
    actions: {
      download: "下载文件",
      downloadFile: "下载文件",
      downloadFailed: "下载失败",
      downloadExpired: "下载链接可能已过期，请尝试刷新获取新的下载链接",
      share: "分享链接",
      shareFileText: "分享文件",
      edit: "编辑信息",
      delete: "删除文件",
      preview: "预览文件",
      previewFailed: "预览失败",
      getPreviewUrlFailed: "获取预览URL失败",
      noUrlInResponse: "返回数据中没有URL",
      shareSuccess: "分享成功",
      shareFailed: "分享失败",
      deleteConfirm: "确定要删除这个文件吗？此操作无法撤销。",
      deleteSuccess: "文件删除成功",
      deleteFailed: "删除失败",
      deleting: "删除中...",
      noPermission: "没有足够的权限",
      redirecting: "秒后自动跳转",
      redirectMessage: "文件已成功删除，即将跳转到首页",
      retry: "重试",
      refresh: "刷新",
      manualCopy: "无法自动复制，请手动复制链接",
      copyLink: "复制链接",
      copied: "已复制",
      nativeShare: "原生分享",
      shareToSocial: "分享到社交平台",
      qrCode: "二维码",
      generating: "生成中...",
      scanToShare: "扫码分享",
      qrCodeError: "生成失败",
      shareFileText: "分享文件：{filename}",
    },

    // 文件预览
    preview: {
      loading: "加载预览中...",
      error: "预览加载失败",
      notSupported: "此文件类型不支持预览",
      downloadToView: "请下载文件查看",

      // 通用预览
      generic: {
        applicationFile: "应用程序文件",
        fontFile: "字体文件",
        modelFile: "3D模型文件",
        unsupportedType: "此文件类型不支持在线预览",
        downloadAndExtract: "请下载后使用解压软件打开",
        downloadAndInstall: "请下载后安装或运行",
        downloadAndOpenWithDb: "请下载后使用数据库工具打开",
        downloadAndInstallFont: "请下载后安装字体",
        downloadAndMount: "请下载后挂载或刻录",
        downloadAndOpenWith: "请下载后使用相应的应用程序打开",
        showDetails: "显示详细信息",
        hideDetails: "隐藏详细信息",
        fileInfo: "文件信息",
        filename: "文件名",
        mimeType: "MIME类型",
        fileExtension: "文件扩展名",
        suggestedApps: "建议使用",
      },

      // 文本预览
      text: {
        title: "文本文件预览",
        loading: "加载文本内容中...",
        error: "加载文本内容失败",
        tooLarge: "文件过大，为了性能考虑，请下载后查看完整内容",
        truncated: "内容已截断，请下载查看完整文件",
      },

      // 代码预览
      code: {
        title: "代码预览",
        loading: "加载代码内容中...",
      },

      // 配置文件预览
      config: {
        title: "配置文件预览",
        loading: "加载配置文件中...",
      },

      // Markdown预览
      markdown: {
        title: "Markdown预览",
        loading: "加载Markdown内容中...",
        error: "Markdown预览加载失败",
      },

      // HTML预览
      html: {
        title: "HTML预览",
        loading: "加载HTML内容中...",
        loadingSource: "加载HTML源码中...",
        error: "HTML加载失败",
        viewSource: "查看源码",
        viewRendered: "查看渲染",
      },

      // PDF预览
      pdf: {
        title: "PDF预览",
        loading: "加载PDF中...",
        error: "PDF加载失败",
      },

      // 图片预览
      image: {
        title: "图片预览",
        loading: "加载图片中...",
        error: "图片加载失败",
      },

      // 视频预览
      video: {
        title: "视频预览",
        loading: "加载视频中...",
        error: "视频加载失败",
        notSupported: "您的浏览器不支持视频标签",
      },

      // 音频预览
      audio: {
        title: "音频预览",
        loading: "加载音频中...",
        error: "音频加载失败",
        notSupported: "您的浏览器不支持音频标签",
      },

      // Office预览
      office: {
        title: "Office预览",
        loading: "加载预览中...",
        loadingDetail: "加载Office预览中，请稍候...",
        error: "Office预览加载失败",
        useMicrosoft: "使用Microsoft预览",
        useGoogle: "使用Google预览",
        refreshPreview: "刷新预览",
        downloadFile: "下载文件",
        previewTrouble: "如果预览不正常，请尝试",
        switchService: "或切换预览服务，或",
        afterDownload: "后查看",
        wordPreview: "Word文档预览",
        excelPreview: "Excel表格预览",
        powerpointPreview: "PowerPoint演示文稿预览",
        passwordIssue: "似乎是密码验证问题，请尝试：",
        refreshAndRetry: "刷新页面后重新输入密码",
        confirmPassword: "确认您输入的密码正确",
        tryUrlPassword: "尝试在URL中直接添加密码参数",
        googleService: "使用Google Docs服务",
        microsoftService: "使用Microsoft Office服务",
        proxyMode: " (Worker代理模式)",
        directMode: " (直接访问模式)",
      },
    },

    // 错误消息
    errors: {
      networkError: "网络错误，请检查网络连接",
      serverError: "服务器错误，请稍后重试",
      unauthorized: "未授权访问",
      forbidden: "访问被禁止",
      notFound: "文件不存在",
      unknown: "未知错误",
      missingSlug: "缺少文件标识符",
      loadFailed: "无法加载文件信息",
      getDetailsFailed: "获取详情失败",
      getDetailsFailedMessage: "获取文件详情失败，将使用当前显示的信息",
      updateFailed: "更新失败",
    },
  },
};
