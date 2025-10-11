export default {
  // Global settings page
  global: {
    title: "Global Settings",
    description: "Manage system global configuration and proxy signature settings",
    uploadSettings: {
      title: "File Upload Page Limit Settings",
      description: "Set file upload size limits and default proxy",
      maxUploadSizeLabel: "Maximum upload file size",
      maxUploadSizePlaceholder: "Enter a number",
      unitKB: "KB",
      unitMB: "MB",
      unitGB: "GB",
      validationError: "Please enter a valid upload size limit",
      defaultUseProxyLabel: "Use proxy by default for new files",
      defaultUseProxyHint: "When enabled, new files will use Worker proxy by default; when disabled, direct links will be used by default",
      fileOverwriteModeLabel: "File Overwrite Mode",
      fileOverwriteModeHint:
        "When enabled, files with the same name will be directly overwritten; when disabled, random suffixes will be used to avoid conflicts (e.g., document-a1B2c3.pdf)",
    },
    proxySignSettings: {
      title: "Proxy Signature Settings",
      description: "Configure proxy signature functionality for file access",
      signAllLabel: "Sign All Requests",
      signAllHint: "When enabled, all file access requests will use proxy signatures",
      expiresLabel: "Signature Expiration Time",
      expiresHint: "Set proxy signature expiration time, 0 means never expires",
      expiresUnit: "seconds",
    },
    buttons: {
      updateSettings: "Update Settings",
      updating: "Updating...",
    },
    messages: {
      updateSuccess: "Settings updated successfully",
      updateFailed: "Failed to update settings",
    },
  },

  // Account management page
  account: {
    title: "Account Management",
    description: "Manage administrator account information, including username and password modification",
    apiKeyTitle: "Account Information",
    apiKeyDescription: "View your account information",
    apiKeyInfo: {
      title: "Account Information",
      keyName: "Key Name",
      basicPath: "Basic Path",
    },
    adminInfo: {
      title: "Administrator Information Modification",
      description: "Modify administrator username and password",
      newUsernameLabel: "New Username",
      newUsernameHint: "Leave blank to keep current username",
      currentPasswordLabel: "Current Password",
      currentPasswordHint: "Current password is required for identity verification",
      newPasswordLabel: "New Password",
      newPasswordHint: "Leave blank to keep current password",
      newUsernamePlaceholder: "Please enter new username",
      currentPasswordPlaceholder: "Please enter current password",
      newPasswordPlaceholder: "Please enter new password",
      warningMessage: "You will be automatically logged out after modification and need to log in again",
      updateButton: "Update Account Information",
      updating: "Updating...",
    },
    buttons: {
      updateAccount: "Update Account Information",
      updating: "Updating...",
    },
    messages: {
      updateSuccess: "Account information updated successfully",
      updateFailed: "Failed to update account information",
      passwordRequired: "Please enter current password",
      newPasswordRequired: "Please enter new password",
      newUsernameRequired: "Please enter new username",
      newFieldRequired: "Please fill in at least one of new username or new password",
      samePassword: "New password cannot be the same as current password",
      logoutCountdown: "Will automatically log out in {seconds} seconds",
    },
  },

  // WebDAV settings page
  webdav: {
    title: "WebDAV Settings",
    description: "Configure WebDAV protocol related functions and parameters",
    uploadSettings: {
      title: "WebDAV Upload Settings",
      description: "Configure WebDAV client upload processing method",
      uploadModeLabel: "WebDAV Upload Mode",
      uploadModeHint: "Select the upload processing method for the WebDAV client.",
      modes: {
        direct: "Direct Upload",
        multipart: "Multipart Upload",
      },
    },
    protocolInfo: {
      title: "WebDAV Protocol Information",
      description: "Basic information and usage instructions for WebDAV service",
      webdavUrlLabel: "WebDAV Address",
      webdavUrlHint: "Use this address to connect in WebDAV client",
      authMethodLabel: "Authentication Method",
      adminAuth: "Administrator: Username/Password",
      apiKeyAuth: "API Key: Key/Key",
      authHint: "API key authentication is recommended for better security",
    },
    buttons: {
      updateSettings: "Update Settings",
      updating: "Updating...",
    },
    messages: {
      updateSuccess: "WebDAV settings updated successfully",
      updateFailed: "Failed to update WebDAV settings",
    },
  },

  // Preview settings page
  preview: {
    title: "Preview Settings",
    description: "Configure preview support for different file types",
    loadError: "Failed to load preview settings",

    textTypes: "Text File Types",
    textTypesLabel: "Supported text file extensions",
    textTypesPlaceholder:
      "txt,htm,html,xml,java,properties,sql,js,md,json,conf,ini,vue,php,py,bat,yml,go,sh,c,cpp,h,hpp,tsx,vtt,srt,ass,rs,lrc,dockerfile,makefile,gitignore,license,readme",
    textTypesHelp: "Comma-separated text file extensions that will be displayed using the text previewer",

    imageTypes: "Image File Types",
    imageTypesLabel: "Supported image file extensions",
    imageTypesPlaceholder: "jpg,tiff,jpeg,png,gif,bmp,svg,ico,swf,webp",
    imageTypesHelp: "Comma-separated image file extensions that will be displayed using the image previewer",

    videoTypes: "Video File Types",
    videoTypesLabel: "Supported video file extensions",
    videoTypesPlaceholder: "mp4,htm,html,mkv,avi,mov,rmvb,webm,flv,m3u8",
    videoTypesHelp:
      "Video file extensions separated by commas, which will be previewed using a video player (playback depends on browser support, generally only supporting h.264 (mp4) encoded formats).",

    audioTypes: "Audio File Types",
    audioTypesLabel: "Supported audio file extensions",
    audioTypesPlaceholder: "mp3,flac,ogg,m4a,wav,opus,wma",
    audioTypesHelp: "Comma-separated audio file extensions that will be previewed using the audio player",

    officeTypes: "Office File Types",
    officeTypesLabel: "Supported Office file extensions (require online conversion)",
    officeTypesPlaceholder: "doc,docx,xls,xlsx,ppt,pptx,rtf",
    officeTypesHelp: "Comma-separated Office file extensions that will be converted and previewed through third-party services",

    documentTypes: "Document File Types",
    documentTypesLabel: "Supported document file extensions (direct preview)",
    documentTypesPlaceholder: "pdf",
    documentTypesHelp: "Comma-separated document file extensions that can be directly previewed in the browser",

    resetDefaults: "Reset to Defaults",
    resetConfirm: "Are you sure you want to reset to default settings? This will overwrite all current configurations.",
    saveSuccess: "Preview settings saved successfully",
  },

  // Site settings page
  site: {
    title: "Site Settings",
    description: "Configure site-related features",
    groups: {
      basic: "Basic Settings",
      announcement: "Announcement Settings",
    },
    siteTitle: {
      label: "Site Title",
      hint: "Displayed in browser tabs and page titles",
      placeholder: "Enter site title",
    },
    favicon: {
      label: "Site Icon",
      hint: "Supports http://, https:// links or data: base64 format, leave empty for default icon",
      placeholder: "https://example.com/favicon.svg",
    },
    footer: {
      label: "Footer Content",
      hint: "Supports Markdown format, you can use links, bold text, etc. Leave empty to hide footer",
      placeholder: "© 2025 CloudPaste. All rights reserved.",
    },
    announcement: {
      enableLabel: "Enable Announcement Banner",
      enableHint: "When enabled, the announcement banner will be displayed on the homepage",
      contentLabel: "Announcement Content",
      contentHint: "Supports Markdown format, you can use bold, italic, links, etc.",
      contentPlaceholder: "Please enter announcement content, Markdown format supported...",
    },
    customHead: "Custom Head",
    customHeadPlaceholder: "<!-- Enter your custom head content here -->",
    customHeadHelp: "Any content set here will be automatically placed at the beginning of the webpage head",
    customBody: "Custom Body",
    customBodyPlaceholder: "<!-- Enter your custom body content here -->",
    customBodyHelp: "Any content set here will be automatically placed at the end of the webpage body",
    buttons: {
      updateSettings: "Update Settings",
      updating: "Updating...",
      reset: "Reset",
    },
    messages: {
      updateSuccess: "Site settings updated successfully",
      updateFailed: "Failed to update site settings",
      confirmReset: "Are you sure you want to reset all site settings?",
    },
  },
};
