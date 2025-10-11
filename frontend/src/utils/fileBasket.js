/**
 * 文件篮工具函数
 * 提供文件篮相关的纯函数工具
 */

/**
 * 验证文件是否可以添加到篮子
 * @param {Object} file - 文件对象
 * @returns {Object} 验证结果
 */
export function validateFileForBasket(file) {
  if (!file) {
    return {
      valid: false,
      reason: "FILE_NOT_EXISTS",
    };
  }

  if (file.isDirectory) {
    return {
      valid: false,
      reason: "IS_DIRECTORY",
    };
  }

  if (!file.name || !file.path) {
    return {
      valid: false,
      reason: "MISSING_REQUIRED_FIELDS",
    };
  }

  return {
    valid: true,
    reason: null,
  };
}

/**
 * 批量验证文件列表
 * @param {Array} files - 文件列表
 * @returns {Object} 验证结果
 */
export function validateFilesForBasket(files) {
  if (!Array.isArray(files)) {
    return {
      validFiles: [],
      invalidFiles: [],
      totalCount: 0,
      validCount: 0,
    };
  }

  const validFiles = [];
  const invalidFiles = [];

  files.forEach((file) => {
    const validation = validateFileForBasket(file);
    if (validation.valid) {
      validFiles.push(file);
    } else {
      invalidFiles.push({
        file,
        reason: validation.reason,
      });
    }
  });

  return {
    validFiles,
    invalidFiles,
    totalCount: files.length,
    validCount: validFiles.length,
  };
}

/**
 * 生成文件的唯一标识
 * @param {Object} file - 文件对象
 * @param {string} sourceDirectory - 来源目录
 * @returns {string} 唯一标识
 */
export function generateFileUniqueId(file, sourceDirectory) {
  return `${sourceDirectory}::${file.path}`;
}

/**
 * 计算文件列表的总大小
 * @param {Array} files - 文件列表
 * @returns {number} 总大小（字节）
 */
export function calculateTotalSize(files) {
  if (!Array.isArray(files)) return 0;

  return files.reduce((total, file) => {
    return total + (file.size || 0);
  }, 0);
}

/**
 * 格式化文件大小显示
 * @param {number} bytes - 字节数
 * @returns {string} 格式化后的大小
 */
export function formatFileSize(bytes) {
  if (!bytes || bytes === 0) return "0 B";

  const units = ["B", "KB", "MB", "GB", "TB"];
  const k = 1024;
  const i = Math.floor(Math.log(bytes) / Math.log(k));

  return `${(bytes / Math.pow(k, i)).toFixed(i === 0 ? 0 : 1)} ${units[i]}`;
}

/**
 * 按目录分组文件
 * @param {Array} files - 文件列表
 * @returns {Object} 按目录分组的文件
 */
export function groupFilesByDirectory(files) {
  if (!Array.isArray(files)) return {};

  const groups = {};

  files.forEach((file) => {
    const directory = file.sourceDirectory || "/";
    if (!groups[directory]) {
      groups[directory] = [];
    }
    groups[directory].push(file);
  });

  return groups;
}

/**
 * 生成ZIP文件名
 * @param {Object} options - 选项
 * @param {number} options.fileCount - 文件数量
 * @param {number} options.directoryCount - 目录数量
 * @param {string} options.prefix - 前缀
 * @returns {string} ZIP文件名
 */
export function generateZipFileName(options = {}) {
  const { fileCount = 0, directoryCount = 0, prefix = "CloudPaste" } = options;

  const timestamp = new Date().toISOString().slice(0, 19).replace(/:/g, "-");

  if (directoryCount > 1) {
    return `${prefix}_CrossDirectory_${fileCount}files_${directoryCount}dirs_${timestamp}.zip`;
  } else {
    return `${prefix}_${fileCount}files_${timestamp}.zip`;
  }
}

/**
 * 检查文件大小是否需要警告
 * @param {number} totalSizeBytes - 总大小（字节）
 * @returns {Object|null} 警告信息
 */
export function checkSizeWarning(totalSizeBytes) {
  const sizeMB = totalSizeBytes / (1024 * 1024);

  if (sizeMB > 500) {
    return {
      level: "danger",
      sizeMB: Math.round(sizeMB * 100) / 100,
      message: `文件总大小约 ${Math.round(sizeMB)}MB，打包可能需要较长时间且消耗较多内存`,
    };
  } else if (sizeMB > 100) {
    return {
      level: "warning",
      sizeMB: Math.round(sizeMB * 100) / 100,
      message: `文件总大小约 ${Math.round(sizeMB)}MB，打包可能需要一些时间`,
    };
  }

  return null;
}

/**
 * 创建错误报告内容
 * @param {Array} failedFiles - 失败的文件列表
 * @param {Object} options - 选项
 * @returns {string} 错误报告内容
 */
export function createErrorReport(failedFiles, options = {}) {
  const { title = "下载失败文件列表", includeTimestamp = true } = options;

  const lines = [title];

  if (includeTimestamp) {
    lines.push(`生成时间: ${new Date().toLocaleString()}`);
  }

  lines.push("");

  if (failedFiles.length === 0) {
    lines.push("所有文件下载成功！");
  } else {
    lines.push(`共 ${failedFiles.length} 个文件下载失败:`);
    lines.push("");

    failedFiles.forEach(({ fileName, path, error }, index) => {
      lines.push(`${index + 1}. ${path} (${fileName})`);
      lines.push(`   错误: ${error}`);
      lines.push("");
    });
  }

  return lines.join("\n");
}

/**
 * 处理文件名冲突
 * @param {string} originalPath - 原始路径
 * @param {Set} existingPaths - 已存在的路径集合
 * @returns {string} 处理后的路径
 */
export function resolvePathConflict(originalPath, existingPaths) {
  if (!existingPaths.has(originalPath)) {
    return originalPath;
  }

  const lastDotIndex = originalPath.lastIndexOf(".");
  let baseName, extension;

  if (lastDotIndex > 0) {
    baseName = originalPath.substring(0, lastDotIndex);
    extension = originalPath.substring(lastDotIndex);
  } else {
    baseName = originalPath;
    extension = "";
  }

  let counter = 1;
  let newPath;

  do {
    newPath = `${baseName}_${counter}${extension}`;
    counter++;
  } while (existingPaths.has(newPath));

  return newPath;
}

/**
 * 清理目录路径用于ZIP
 * @param {string} directoryPath - 目录路径
 * @returns {string} 清理后的路径
 */
export function sanitizeDirectoryForZip(directoryPath) {
  if (!directoryPath || directoryPath === "/") {
    return "root";
  }

  return directoryPath
    .replace(/^\/+/, "") // 移除开头的斜杠
    .replace(/\/+$/, "") // 移除结尾的斜杠
    .replace(/\/+/g, "_") // 将斜杠替换为下划线
    .replace(/[<>:"|?*]/g, "_"); // 替换Windows不支持的字符
}

/**
 * 验证文件篮状态
 * @param {Object} basketState - 文件篮状态
 * @returns {Object} 验证结果
 */
export function validateBasketState(basketState) {
  const errors = [];

  if (!basketState) {
    errors.push("文件篮状态为空");
    return { valid: false, errors };
  }

  if (!Array.isArray(basketState.collectedFiles)) {
    errors.push("收集的文件列表格式错误");
  }

  if (Array.isArray(basketState.collectedFiles) && basketState.collectedFiles.length === 0) {
    errors.push("文件篮为空");
  }

  // 检查文件对象的完整性
  if (Array.isArray(basketState.collectedFiles)) {
    basketState.collectedFiles.forEach((file, index) => {
      if (!file.path) {
        errors.push(`文件 ${index + 1} 缺少路径信息`);
      }
      if (!file.name) {
        errors.push(`文件 ${index + 1} 缺少名称信息`);
      }
      if (!file.sourceDirectory) {
        errors.push(`文件 ${index + 1} 缺少来源目录信息`);
      }
    });
  }

  return {
    valid: errors.length === 0,
    errors,
  };
}

/**
 * 创建任务摘要
 * @param {Object} taskResult - 任务结果
 * @returns {string} 任务摘要
 */
export function createTaskSummary(taskResult) {
  const { totalFiles = 0, successCount = 0, failedCount = 0, zipFileName = "", startTime, endTime } = taskResult;

  const lines = [];

  lines.push("=== 跨目录打包下载任务摘要 ===");
  lines.push("");
  lines.push(`总文件数: ${totalFiles}`);
  lines.push(`成功下载: ${successCount}`);
  lines.push(`下载失败: ${failedCount}`);
  lines.push(`成功率: ${totalFiles > 0 ? Math.round((successCount / totalFiles) * 100) : 0}%`);

  if (zipFileName) {
    lines.push(`生成文件: ${zipFileName}`);
  }

  if (startTime && endTime) {
    const duration = Math.round((new Date(endTime) - new Date(startTime)) / 1000);
    lines.push(`耗时: ${duration} 秒`);
  }

  lines.push("");
  lines.push(`完成时间: ${new Date().toLocaleString()}`);

  return lines.join("\n");
}
