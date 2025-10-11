export default {
  // 全局设置页面
  global: {
    title: "全局设置",
    description: "管理系统全局配置和代理签名设置",
    uploadSettings: {
      title: "文件上传页限制设置",
      description: "设置文件上传的大小限制和默认代理",
      maxUploadSizeLabel: "最大上传文件大小",
      maxUploadSizePlaceholder: "输入数字",
      unitKB: "KB",
      unitMB: "MB",
      unitGB: "GB",
      validationError: "请输入有效的上传大小限制",
      defaultUseProxyLabel: "新文件默认使用代理",
      defaultUseProxyHint: "启用后新文件默认使用Worker代理，禁用后默认使用直链",
      fileOverwriteModeLabel: "文件覆盖模式",
      fileOverwriteModeHint: "开启后直接覆盖同名文件，关闭则使用随机后缀避免冲突（如 document-a1B2c3.pdf）",
    },
    proxySignSettings: {
      title: "代理签名设置",
      description: "配置文件访问的代理签名功能",
      signAllLabel: "签名所有请求",
      signAllHint: "启用后，所有文件访问请求都将使用代理签名",
      expiresLabel: "签名过期时间",
      expiresHint: "设置代理签名的过期时间，0表示永不过期",
      expiresUnit: "秒",
    },
    buttons: {
      updateSettings: "更新设置",
      updating: "更新中...",
    },
    messages: {
      updateSuccess: "设置更新成功",
      updateFailed: "更新设置失败",
    },
  },

  // 账号管理页面
  account: {
    title: "账号管理",
    description: "管理管理员账户信息，包括用户名和密码修改",
    apiKeyTitle: "账户信息",
    apiKeyDescription: "查看您的账户信息",
    apiKeyInfo: {
      title: "账户信息",
      keyName: "密钥名称",
      basicPath: "基础路径",
    },
    adminInfo: {
      title: "管理员信息修改",
      description: "修改管理员用户名和密码",
      newUsernameLabel: "新用户名",
      newUsernameHint: "留空则不修改用户名",
      currentPasswordLabel: "当前密码",
      currentPasswordHint: "验证身份需要输入当前密码",
      newPasswordLabel: "新密码",
      newPasswordHint: "留空则不修改密码",
      newUsernamePlaceholder: "请输入新用户名",
      currentPasswordPlaceholder: "请输入当前密码",
      newPasswordPlaceholder: "请输入新密码",
      warningMessage: "修改后将自动退出登录，需要重新登录",
      updateButton: "更新账号信息",
      updating: "更新中...",
    },
    buttons: {
      updateAccount: "更新账号信息",
      updating: "更新中...",
    },
    messages: {
      updateSuccess: "账号信息更新成功",
      updateFailed: "更新账号信息失败",
      passwordRequired: "请输入当前密码",
      newPasswordRequired: "请输入新密码",
      newUsernameRequired: "请输入新用户名",
      newFieldRequired: "请至少填写新用户名或新密码中的一项",
      samePassword: "新密码不能与当前密码相同",
      logoutCountdown: "将在 {seconds} 秒后自动退出登录",
    },
  },

  // WebDAV设置页面
  webdav: {
    title: "WebDAV设置",
    description: "配置WebDAV协议相关的功能和参数",
    uploadSettings: {
      title: "WebDAV上传设置",
      description: "配置WebDAV客户端的上传处理方式",
      uploadModeLabel: "WebDAV上传模式",
      uploadModeHint: "选择WebDAV客户端的上传处理方式",
      modes: {
        direct: "直接上传",
        multipart: "分片上传",
      },
    },
    protocolInfo: {
      title: "WebDAV协议信息",
      description: "WebDAV服务的基本信息和使用说明",
      webdavUrlLabel: "WebDAV地址",
      webdavUrlHint: "在WebDAV客户端中使用此地址连接",
      authMethodLabel: "认证方式",
      adminAuth: "管理员：用户名/密码",
      apiKeyAuth: "API密钥: 密钥/密钥",
      authHint: "推荐使用API密钥认证，更加安全",
    },
    buttons: {
      updateSettings: "更新设置",
      updating: "更新中...",
    },
    messages: {
      updateSuccess: "WebDAV设置更新成功",
      updateFailed: "更新WebDAV设置失败",
    },
  },

  // 预览设置页面
  preview: {
    title: "预览设置",
    description: "配置不同文件类型的预览支持",
    loadError: "加载预览设置失败",

    textTypes: "文本文件类型",
    textTypesLabel: "支持的文本文件扩展名",
    textTypesPlaceholder:
      "txt,htm,html,xml,java,properties,sql,js,md,json,conf,ini,vue,php,py,bat,yml,go,sh,c,cpp,h,hpp,tsx,vtt,srt,ass,rs,lrc,dockerfile,makefile,gitignore,license,readme",
    textTypesHelp: "用逗号分隔的文本文件扩展名，这些文件将使用文本预览器显示",

    imageTypes: "图片文件类型",
    imageTypesLabel: "支持的图片文件扩展名",
    imageTypesPlaceholder: "jpg,tiff,jpeg,png,gif,bmp,svg,ico,swf,webp",
    imageTypesHelp: "用逗号分隔的图片文件扩展名，这些文件将使用图片预览器显示",

    videoTypes: "视频文件类型",
    videoTypesLabel: "支持的视频文件扩展名",
    videoTypesPlaceholder: "mp4,htm,html,mkv,avi,mov,rmvb,webm,flv,m3u8",
    videoTypesHelp: "用逗号分隔的视频文件扩展名，这些文件将使用视频播放器预览(能否播放要取决于浏览器是否支持，一般只支格式为 h.264 (mp4) 的编码格式)",

    audioTypes: "音频文件类型",
    audioTypesLabel: "支持的音频文件扩展名",
    audioTypesPlaceholder: "mp3,flac,ogg,m4a,wav,opus,wma",
    audioTypesHelp: "用逗号分隔的音频文件扩展名，这些文件将使用音频播放器预览",

    officeTypes: "Office文件类型",
    officeTypesLabel: "支持的Office文件扩展名（需要在线转换）",
    officeTypesPlaceholder: "doc,docx,xls,xlsx,ppt,pptx,rtf",
    officeTypesHelp: "用逗号分隔的Office文件扩展名，这些文件将通过第三方服务进行在线转换预览",

    documentTypes: "文档文件类型",
    documentTypesLabel: "支持的文档文件扩展名（可直接预览）",
    documentTypesPlaceholder: "pdf",
    documentTypesHelp: "用逗号分隔的文档文件扩展名，这些文件可以直接在浏览器中预览",

    resetDefaults: "重置为默认",
    resetConfirm: "确定要重置为默认设置吗？这将覆盖当前的所有配置。",
    saveSuccess: "预览设置保存成功",
  },

  // 站点设置页面
  site: {
    title: "站点设置",
    description: "配置站点相关功能",
    groups: {
      basic: "基本设置",
      announcement: "公告设置",
    },
    siteTitle: {
      label: "站点标题",
      hint: "显示在浏览器标签页和页面标题中",
      placeholder: "请输入站点标题",
    },
    favicon: {
      label: "站点图标",
      hint: "支持 http://、https:// 链接或 data: base64 格式，留空使用默认图标",
      placeholder: "https://example.com/favicon.svg",
    },
    footer: {
      label: "页脚内容",
      hint: "支持 Markdown 格式，可以使用链接、粗体等语法，留空则不显示页脚",
      placeholder: "© 2025 CloudPaste. 保留所有权利。",
    },
    announcement: {
      enableLabel: "启用公告栏",
      enableHint: "开启后将在首页显示公告栏",
      contentLabel: "公告内容",
      contentHint: "支持 Markdown 格式，可以使用粗体、斜体、链接等",
      contentPlaceholder: "请输入公告内容，支持 Markdown 格式...",
    },
    customHead: "自定义头部",
    customHeadPlaceholder: "<!-- 在此输入自定义头部内容 -->",
    customHeadHelp: "在此处设置的任何内容都会自动放置在网页头部的开头",
    customBody: "自定义body",
    customBodyPlaceholder: "<!-- 在此输入自定义body内容 -->",
    customBodyHelp: "在此处设置的任何内容都会自动放置在网页正文的末尾",
    buttons: {
      updateSettings: "更新设置",
      updating: "更新中...",
      reset: "重置",
    },
    messages: {
      updateSuccess: "站点设置更新成功",
      updateFailed: "更新站点设置失败",
      confirmReset: "确定要重置所有站点设置吗？",
    },
  },
};

// 公告弹窗
export const announcement = {
  title: "网站公告",
  dontShowAgain: "不再显示此公告",
  gotIt: "我知道了",
};
