export default {
  fileView: {
    title: "文件预览",
    loading: "加载中...",
    error: "加载失败",
    notFound: "文件不存在",

    // 文件夹分享（公开只读）
    folder: {
      title: "文件夹分享",
      loading: "加载中...",
      errorState: "无法访问分享",
      emptyFolder: "此文件夹为空",
      itemCount: "共 {count} 项",
      back: "返回",
      meta: {
        remainingDownloads: "剩余下载次数",
        downloadsUsed: "已下载 {views} 次",
        unlimitedDownloads: "不限次数",
        expiresAt: "过期时间",
        noExpiry: "永久有效",
      },
      password: {
        title: "请输入访问密码",
        description: "此文件夹已被密码保护，请输入密码查看内容",
        label: "密码",
        placeholder: "请输入密码",
        submit: "确认",
        loading: "验证中...",
      },
      table: {
        name: "名称",
        size: "大小",
        modified: "修改时间",
        actions: "操作",
      },
      actions: {
        download: "下载",
        preview: "预览",
        openFolder: "打开",
        refresh: "刷新",
      },
      viewMode: {
        list: "列表视图",
        grid: "宫格视图",
      },
      preview: {
        title: "文件预览",
        loading: "加载中...",
        error: "预览加载失败",
        notSupported: "此文件类型不支持在线预览，请下载后查看",
        download: "下载文件",
        close: "关闭",
      },
      errors: {
        expired: "分享已过期或已达下载次数上限",
        notFound: "分享不存在或已被删除",
        wrongPassword: "密码错误，请重试",
        loadFailed: "无法加载分享信息",
        listFailed: "加载目录失败",
      },
    },

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
      accessCount: "下载次数",
      expiresAt: "过期时间",
      accessMode: "访问模式",
      proxyAccess: "本地代理访问",
      directAccess: "直链/URL代理访问",
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
      updateSuccess: "文件信息更新成功",
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
      fullscreen: "全屏",
      exitFullscreen: "退出全屏",

      // 文本预览模式切换（统一归属“文本预览”）
      modes: {
        text: "文本",
        code: "代码",
        markdown: "Markdown",
        html: "HTML",
      },

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
        loading: "加载中...",
        error: "加载文本内容失败",
        tooLarge: "文件过大，为了性能考虑，请下载后查看完整内容",
        truncated: "内容已截断，请下载查看完整文件",
      },

      // 代码预览
      code: {
        title: "代码预览",
        loading: "加载中...",
      },

      // 配置文件预览
      config: {
        title: "配置文件预览",
        loading: "加载中...",
      },

      // Markdown预览
      markdown: {
        title: "Markdown预览",
        loading: "加载中...",
        error: "Markdown预览加载失败",
      },

      // HTML预览
      html: {
        title: "HTML预览",
        loading: "加载中...",
        loadingSource: "加载HTML源码中...",
        error: "HTML加载失败",
        viewSource: "查看源码",
        viewRendered: "查看渲染",
      },

      // PDF预览
      pdf: {
        title: "PDF预览",
        loading: "加载中...",
        error: "PDF加载失败",
        browserNative: "原生预览",
        pdfjsLabel: "PDF.js",
      },

      // EPUB预览
      epub: {
        title: "EPUB预览",
        loading: "加载中...",
        error: "EPUB加载失败",
        browserNative: "本地预览",
        toc: "目录",
        closeToc: "关闭目录",
        noToc: "无目录",
        readingMode: "阅读模式",
        modePaginated: "翻页",
        modeScrolled: "滚动",
        prevPage: "上一页",
        nextPage: "下一页",
        page: "第",
        bookmarks: "书签",
        addBookmark: "添加书签",
        removeBookmark: "移除书签",
        untitledBookmark: "未命名书签",
        historyBack: "返回",
        historyForward: "前进",
        progressSaved: "进度已保存",
        progressRestored: "已恢复到上次阅读位置",
      },

      // 图片预览
      image: {
        title: "图片预览",
        loading: "加载中...",
        error: "图片加载失败",
      },

      // 视频预览
      video: {
        title: "视频预览",
        loading: "加载中...",
        error: "视频加载失败",
        notSupported: "您的浏览器不支持视频标签",
        hlsShareNotSupportedTitle: "分享页不支持 m3u8 切片视频",
        hlsShareNotSupportedTip:
          "这是 m3u8 清单文件，分享页（单文件）无法播放。请回到“挂载浏览”页面配合分片播放",
      },

      // 直链预览提示
      directNotSupported: "当前存储不支持直链预览，请切换为代理模式。",

      // 音频预览
      audio: {
        title: "音频预览",
        loading: "加载中...",
        error: "音频加载失败",
        notSupported: "您的浏览器不支持音频标签",
      },

      // Office预览
      office: {
        title: "Office预览",
        loading: "加载中...",
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
        providerMicrosoft: "Microsoft Office Online",
        providerGoogle: "Google Docs",
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
      missingId: "缺少文件 ID",
      loadFailed: "无法加载文件信息",
      getDetailsFailed: "获取详情失败",
      getDetailsFailedMessage: "获取文件详情失败，将使用当前显示的信息",
      updateFailed: "更新失败",
    },
  },
};
