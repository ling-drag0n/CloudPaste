export default {
  fileBasket: {
    // 按钮文本
    button: {
      empty: "文件篮",
      withCount: "文件篮 ({count})",
    },

    // 面板标题和标签
    panel: {
      title: "文件篮",
      summary: "{fileCount} 个文件，来自 {directoryCount} 个目录",
      totalSize: "总大小：{size}MB",
      empty: "文件篮为空",
      emptyDescription: "在文件浏览器中选择文件并添加到文件篮",
    },

    // 操作按钮
    actions: {
      packDownload: "打包下载",
      clear: "清空篮子",
      close: "关闭",
      remove: "移除",
      addToBasket: "加入文件篮",
      removeFromBasket: "从文件篮移除",
      batchAdd: "批量加入",
      // 移动端短文本
      mobile: {
        batchAdd: "加入",
      },
    },

    // 消息提示
    messages: {
      addSuccess: "已添加 {count} 个文件到篮子，当前共 {total} 个文件",
      addFailed: "添加文件到篮子失败",
      removeSuccess: "已从篮子移除文件",
      removeFailed: "从篮子移除文件失败",
      toggleFailed: "切换文件篮状态失败",
      batchAddSuccess: "已批量添加 {count} 个文件到篮子，当前共 {total} 个文件",
      batchAddFailed: "批量添加文件到篮子失败",
      clearSuccess: "已清空文件篮",
      clearFailed: "清空文件篮失败",
      noFilesToAdd: "没有可添加的文件（文件夹将被忽略）",
      emptyBasket: "文件篮为空，请先添加文件",
      taskCreated: "已创建任务：{taskName}",
      taskCreateFailed: "创建打包任务失败",
      cancelled: "操作已取消",
    },

    // 警告信息
    warnings: {
      large: "选中文件总大小约 {size}MB，打包可能需要一些时间",
      veryLarge: "选中文件总大小约 {size}MB，打包可能需要较长时间且消耗较多内存",
    },

    // 确认对话框
    confirmations: {
      continueAnyway: "是否继续？",
      clearBasket: "确定要清空文件篮吗？这将移除所有已收集的文件。",
    },

    // 任务相关
    task: {
      name: "打包下载 {count} 个文件（来自 {directories} 个目录）",
      preparing: "正在准备下载...",
      downloading: "正在下载文件...",
      generating: "正在生成压缩包...",
      completed: "打包下载完成",
      failed: "打包下载失败",
      summarySuccess: "成功下载 {count} 个文件",
      summaryWithFailures: "成功: {success}，失败: {failed}",
      failedFilesHeader: "以下文件下载失败：",
    },

    // 错误信息
    errors: {
      noDownloadUrl: "无法获取文件下载链接",
      downloadFailed: "文件下载失败",
      zipGenerationFailed: "压缩包生成失败",
    },

    // 文件信息
    fileInfo: {
      fileName: "文件名",
      fileSize: "大小",
      sourceDirectory: "来源目录",
      addedTime: "添加时间",
    },

    // 状态文本
    status: {
      inBasket: "已在篮子中",
      notInBasket: "未在篮子中",
      processing: "处理中...",
      completed: "已完成",
      failed: "失败",
    },
  },
};
