export default {
  fileView: {
    title: "File Preview",
    loading: "Loading...",
    error: "Load Failed",
    notFound: "File Not Found",

    // File information
    fileInfo: {
      filename: "Filename",
      fileSize: "File Size",
      uploadTime: "Upload Time",
      mimetype: "File Type",
      fileLink: "File Link",
      needPassword: "Password Required",
      copyLink: "Copy Link",
      linkCopied: "Link copied to clipboard",
      copyFailed: "Copy failed",
      accessCount: "Access Count",
      expiresAt: "Expires At",
      accessMode: "Access Mode",
      proxyAccess: "Worker Proxy Access",
      directAccess: "S3/CDN Direct Access",
      limit: "limit",
    },

    // Password verification
    password: {
      title: "Please enter access password",
      description: "This file is password protected, please enter the password to view the content",
      label: "Password",
      placeholder: "Enter password",
      submit: "Confirm",
      error: "Incorrect password, please try again",
      loading: "Verifying...",
      showPassword: "Show password",
      hidePassword: "Hide password",
    },

    // File actions
    actions: {
      download: "Download File",
      downloadFile: "Download File",
      downloadFailed: "Download failed",
      downloadExpired: "Download link may have expired, please try refreshing to get a new download link",
      share: "Share Link",
      shareFileText: "Share file",
      edit: "Edit Info",
      delete: "Delete File",
      preview: "Preview File",
      previewFailed: "Preview failed",
      getPreviewUrlFailed: "Failed to get preview URL",
      noUrlInResponse: "No URL in response data",
      shareSuccess: "Share successful",
      shareFailed: "Share failed",
      deleteConfirm: "Are you sure you want to delete this file? This action cannot be undone.",
      deleteSuccess: "File deleted successfully",
      deleteFailed: "Delete failed",
      deleting: "Deleting...",
      noPermission: "Insufficient permissions",
      redirecting: "seconds until automatic redirect",
      redirectMessage: "File has been successfully deleted, redirecting to homepage",
      retry: "Retry",
      refresh: "Refresh",
      manualCopy: "Unable to copy automatically, please manually copy the link",
      copyLink: "Copy Link",
      copied: "Copied",
      nativeShare: "Native Share",
      shareToSocial: "Share to Social",
      qrCode: "QR Code",
      generating: "Generating...",
      scanToShare: "Scan to share",
      qrCodeError: "Generation Failed",
      shareFileText: "Share file: {filename}",
    },

    // File preview
    preview: {
      loading: "Loading preview...",
      error: "Preview load failed",
      notSupported: "This file type does not support preview",
      downloadToView: "Please download the file to view",

      // Generic preview
      generic: {
        applicationFile: "Application File",
        fontFile: "Font File",
        modelFile: "3D Model File",
        unsupportedType: "This file type does not support online preview",
        downloadAndExtract: "Please download and use decompression software to open",
        downloadAndInstall: "Please download and install or run",
        downloadAndOpenWithDb: "Please download and use database tools to open",
        downloadAndInstallFont: "Please download and install font",
        downloadAndMount: "Please download and mount or burn",
        downloadAndOpenWith: "Please download and use appropriate application to open",
        showDetails: "Show Details",
        hideDetails: "Hide Details",
        fileInfo: "File Information",
        filename: "Filename",
        mimeType: "MIME Type",
        fileExtension: "File Extension",
        suggestedApps: "Suggested Apps",
      },

      // Text preview
      text: {
        title: "Text File Preview",
        loading: "Loading text content...",
        error: "Failed to load text content",
        tooLarge: "File is too large, please download to view complete content for performance reasons",
        truncated: "Content has been truncated, please download to view complete file",
      },

      // Code preview
      code: {
        title: "Code Preview",
        loading: "Loading code content...",
      },

      // Config file preview
      config: {
        title: "Config File Preview",
        loading: "Loading config file...",
      },

      // Markdown preview
      markdown: {
        title: "Markdown Preview",
        loading: "Loading Markdown content...",
        error: "Markdown preview load failed",
      },

      // HTML preview
      html: {
        title: "HTML Preview",
        loading: "Loading HTML content...",
        loadingSource: "Loading HTML source...",
        error: "HTML load failed",
        viewSource: "View Source",
        viewRendered: "View Rendered",
      },

      // PDF preview
      pdf: {
        title: "PDF Preview",
        loading: "Loading PDF...",
        error: "PDF load failed",
      },

      // Image preview
      image: {
        title: "Image Preview",
        loading: "Loading image...",
        error: "Image load failed",
      },

      // Video preview
      video: {
        title: "Video Preview",
        loading: "Loading video...",
        error: "Video load failed",
        notSupported: "Your browser does not support video tag",
      },

      // Audio preview
      audio: {
        title: "Audio Preview",
        loading: "Loading audio...",
        error: "Audio load failed",
        notSupported: "Your browser does not support audio tag",
      },

      // Office preview
      office: {
        title: "Office Preview",
        loading: "Loading preview...",
        loadingDetail: "Loading Office preview, please wait...",
        error: "Office preview load failed",
        useMicrosoft: "Use Microsoft Preview",
        useGoogle: "Use Google Preview",
        refreshPreview: "Refresh Preview",
        downloadFile: "Download File",
        previewTrouble: "If preview is not working properly, please try",
        switchService: "or switch preview service, or",
        afterDownload: "to view",
        wordPreview: "Word Document Preview",
        excelPreview: "Excel Spreadsheet Preview",
        powerpointPreview: "PowerPoint Presentation Preview",
        passwordIssue: "Seems to be a password verification issue, please try:",
        refreshAndRetry: "Refresh the page and re-enter password",
        confirmPassword: "Confirm your password is correct",
        tryUrlPassword: "Try adding password parameter directly in URL",
        googleService: "Using Google Docs service",
        microsoftService: "Using Microsoft Office service",
        proxyMode: " (Worker proxy mode)",
        directMode: " (Direct access mode)",
      },
    },

    // Error messages
    errors: {
      networkError: "Network error, please check your connection",
      serverError: "Server error, please try again later",
      unauthorized: "Unauthorized access",
      forbidden: "Access forbidden",
      notFound: "File not found",
      unknown: "Unknown error",
      missingSlug: "Missing file identifier",
      loadFailed: "Unable to load file information",
      getDetailsFailed: "Failed to get details",
      getDetailsFailedMessage: "Failed to get file details, will use currently displayed information",
      updateFailed: "Update failed",
    },
  },
};
