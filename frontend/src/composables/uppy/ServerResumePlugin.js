/**
 * ServerResumePlugin - Uppy服务器端断点续传插件
 * 基于S3服务器状态的智能断点续传
 */

import { BasePlugin } from "@uppy/core";
import * as fsApi from "../../api/services/fsService.js";

export default class ServerResumePlugin extends BasePlugin {
  static VERSION = "1.0.0";

  constructor(uppy, opts) {
    super(uppy, {
      // 默认配置
      autoCheck: true, // 自动检测模式
      matchThreshold: 0.8, // 匹配阈值
      timeWindow: 24, // 时间窗口（小时）
      showConfirmDialog: true, // 显示确认对话框
      customMatcher: null, // 自定义匹配算法
      currentPath: "/", // 当前路径

      // 用户选择相关配置
      maxSelectionOptions: 5, // 最多显示几个选项
      showMatchScore: true, // 是否显示匹配分数
      ...opts,
    });

    this.type = "modifier";
    this.id = this.opts.id || "ServerResumePlugin";

    // 绑定方法
    this.prepareUpload = this.prepareUpload.bind(this);
    this.checkResumableUploads = this.checkResumableUploads.bind(this);
    this.findBestMatches = this.findBestMatches.bind(this);
    this.calculateMatchScore = this.calculateMatchScore.bind(this);

    // 国际化支持
    this.defaultLocale = {
      strings: {
        checkingResumableUploads: "检查可恢复的上传...",
        resumeUploadFound: "发现可恢复的上传",
        resumeUploadConfirm: "是否继续之前的上传？",
        resumingUpload: "恢复上传中...",
      },
    };

    this.i18nInit();
  }

  install() {
    // PreProcessor钩子
    this.uppy.addPreProcessor(this.prepareUpload);

    console.log("[ServerResumePlugin] 插件已安装");
  }

  uninstall() {
    this.uppy.removePreProcessor(this.prepareUpload);

    console.log("[ServerResumePlugin] 插件已卸载");
  }

  /**
   * 在上传前检查断点续传
   * @param {Array} fileIDs 文件ID数组
   * @returns {Promise} 处理完成的Promise
   */
  async prepareUpload(fileIDs) {
    if (!this.opts.autoCheck) {
      return Promise.resolve();
    }

    console.log("[ServerResumePlugin] 开始检查断点续传...");

    const promises = fileIDs.map(async (fileID) => {
      const file = this.uppy.getFile(fileID);

      // 检查文件是否会使用分片上传
      if (!this.shouldUseMultipart(file)) {
        console.log(`[ServerResumePlugin] 文件 ${file.name} 不使用分片上传，跳过断点续传检查`);
        this.uppy.emit("preprocess-complete", file);
        return;
      }

      // 显示检查进度
      this.uppy.emit("preprocess-progress", file, {
        mode: "indeterminate",
        message: this.i18n("checkingResumableUploads"),
      });

      try {
        // 检查可恢复的上传
        const resumableUploads = await this.checkResumableUploads(file);

        if (resumableUploads.length > 0) {
          console.log(`[ServerResumePlugin] 发现 ${resumableUploads.length} 个可恢复的上传`);

          if (this.opts.showConfirmDialog) {
            // 显示选择对话框
            const selectedUpload = await new Promise((resolve) => {
              this.showMultipleUploadsDialog(file, resumableUploads, resolve);
            });

            if (selectedUpload) {
              // 标记文件为可恢复状态
              this.uppy.setFileMeta(fileID, {
                resumable: true,
                existingUpload: selectedUpload,
                serverResume: true,
              });

              console.log(`[ServerResumePlugin] 文件 ${file.name} 已标记为可恢复`);
            }
          } else {
            // 自动选择最佳匹配
            this.uppy.setFileMeta(fileID, {
              resumable: true,
              existingUpload: resumableUploads[0],
              serverResume: true,
            });
          }
        }
      } catch (error) {
        console.error(`[ServerResumePlugin] 检查可恢复上传失败:`, error);
        // 不阻断正常上传流程
      }

      // 完成预处理
      this.uppy.emit("preprocess-complete", file);
    });

    return Promise.all(promises);
  }

  /**
   * 判断文件是否会使用分片上传
   * @param {Object} file Uppy文件对象
   * @returns {boolean} 是否使用分片上传
   */
  shouldUseMultipart(file) {
    // 尝试获取AWS S3插件的shouldUseMultipart配置
    const awsS3Plugin = this.uppy.getPlugin("AwsS3");
    if (awsS3Plugin && awsS3Plugin.opts.shouldUseMultipart) {
      if (typeof awsS3Plugin.opts.shouldUseMultipart === "function") {
        return awsS3Plugin.opts.shouldUseMultipart(file);
      } else {
        return awsS3Plugin.opts.shouldUseMultipart;
      }
    }

    // 如果没有找到AWS S3插件或配置，使用默认逻辑
    // 默认是100MB以上使用分片上传
    return file.size > 100 * 1024 * 1024;
  }

  /**
   * 检查可恢复的上传
   */
  async checkResumableUploads(file) {
    try {
      // 获取当前路径
      const currentPath = this.getCurrentPath();

      // 调用后端API列出进行中的上传
      const response = await fsApi.listMultipartUploads(currentPath);

      if (!response.success || !response.data.uploads) {
        return [];
      }

      // 使用智能匹配算法找到最佳匹配
      return this.findBestMatches(response.data.uploads, file);
    } catch (error) {
      console.error("[ServerResumePlugin] 检查可恢复上传失败:", error);
      return [];
    }
  }

  /**
   * 智能匹配算法 - 多维度评分
   */
  findBestMatches(uploads, file) {
    if (this.opts.customMatcher) {
      return this.opts.customMatcher(uploads, file);
    }

    return uploads
      .map((upload) => ({
        upload,
        score: this.calculateMatchScore(upload, file),
      }))
      .filter((item) => item.score >= this.opts.matchThreshold)
      .sort((a, b) => b.score - a.score)
      .map((item) => item.upload);
  }

  /**
   * 计算匹配分数
   */
  calculateMatchScore(upload, file) {
    let score = 0;

    // 文件名匹配 (40%)
    if (upload.key.endsWith(file.name)) {
      score += 0.4;

      // 完全路径匹配额外加分 (10%)
      const expectedPath = this.buildExpectedPath(file);
      if (upload.key === expectedPath) {
        score += 0.1;
      }
    }

    // 时间匹配 (30%) - 在时间窗口内
    const hoursDiff = this.getHoursDiff(upload.initiated);
    if (hoursDiff <= this.opts.timeWindow) {
      score += 0.3 * (1 - hoursDiff / this.opts.timeWindow);
    }

    // 文件名相似度 (20%)
    const similarity = this.calculateStringSimilarity(upload.key.split("/").pop(), file.name);
    score += 0.2 * similarity;

    return Math.min(score, 1.0); // 确保分数不超过1
  }

  /**
   * 显示多个上传选择对话框
   */
  showMultipleUploadsDialog(file, uploads, resolve) {
    // 限制显示的选项数量
    const limitedUploads = uploads.slice(0, this.opts.maxSelectionOptions);

    // 总是显示选择对话框，让用户选择
    this.showSelectDialog({
      file: file,
      uploads: limitedUploads,
      onSelect: (selectedUpload) => resolve(selectedUpload),
      onCancel: () => resolve(null),
    });
  }

  /**
   * 🔧 新增：显示选择对话框
   * 通过事件系统与 Vue 组件通信
   */
  async showSelectDialog(options) {
    // 为每个上传获取分片信息、计算匹配分数和添加文件大小信息
    const uploadsWithDetails = await Promise.all(
      options.uploads.map(async (upload) => {
        try {
          // 获取该上传的分片信息
          const currentPath = this.getCurrentPath();
          const fullPath = currentPath.endsWith("/") ? currentPath + options.file.name : currentPath + "/" + options.file.name;

          const partsResponse = await fsApi.listMultipartParts(fullPath, upload.uploadId, options.file.name);

          let uploadedParts = [];
          if (partsResponse.success && partsResponse.data.parts) {
            uploadedParts = partsResponse.data.parts;
            console.log(`[ServerResumePlugin] 上传 ${upload.uploadId.substring(0, 8)}... 有 ${uploadedParts.length} 个分片`);
          }

          return {
            ...upload,
            matchScore: this.calculateMatchScore(upload, options.file),
            fileSize: options.file.size,
            uploadedParts: uploadedParts, // 添加分片信息
          };
        } catch (error) {
          console.error(`[ServerResumePlugin] 获取上传 ${upload.uploadId} 的分片信息失败:`, error);
          return {
            ...upload,
            matchScore: this.calculateMatchScore(upload, options.file),
            fileSize: options.file.size,
            uploadedParts: [], // 失败时使用空数组
          };
        }
      })
    );

    this.uppy.emit("server-resume-select-dialog", {
      file: options.file,
      uploads: uploadsWithDetails,
      showMatchScore: this.opts.showMatchScore,
      onSelect: options.onSelect,
      onCancel: options.onCancel,
    });
  }

  /**
   * 获取当前路径
   */
  getCurrentPath() {
    // 优先使用配置中的路径
    if (this.opts.currentPath && this.opts.currentPath !== "/") {
      return this.opts.currentPath;
    }

    // 从全局状态获取当前路径
    if (window.currentPath) {
      return window.currentPath;
    }

    // 默认根路径
    return "/";
  }

  /**
   * 构建期望的文件路径
   */
  buildExpectedPath(file) {
    const currentPath = this.getCurrentPath();
    const normalizedPath = currentPath.endsWith("/") ? currentPath : currentPath + "/";
    return (normalizedPath + file.name).replace(/^\/+/, "");
  }

  /**
   * 计算时间差（小时）
   */
  getHoursDiff(initiated) {
    const uploadTime = new Date(initiated);
    const now = new Date();
    return (now - uploadTime) / (1000 * 60 * 60);
  }

  /**
   * 计算字符串相似度
   */
  calculateStringSimilarity(str1, str2) {
    if (str1 === str2) return 1.0;

    const longer = str1.length > str2.length ? str1 : str2;
    const shorter = str1.length > str2.length ? str2 : str1;

    if (longer.length === 0) return 1.0;

    const editDistance = this.levenshteinDistance(longer, shorter);
    return (longer.length - editDistance) / longer.length;
  }

  /**
   * 计算编辑距离
   */
  levenshteinDistance(str1, str2) {
    const matrix = [];

    for (let i = 0; i <= str2.length; i++) {
      matrix[i] = [i];
    }

    for (let j = 0; j <= str1.length; j++) {
      matrix[0][j] = j;
    }

    for (let i = 1; i <= str2.length; i++) {
      for (let j = 1; j <= str1.length; j++) {
        if (str2.charAt(i - 1) === str1.charAt(j - 1)) {
          matrix[i][j] = matrix[i - 1][j - 1];
        } else {
          matrix[i][j] = Math.min(matrix[i - 1][j - 1] + 1, matrix[i][j - 1] + 1, matrix[i - 1][j] + 1);
        }
      }
    }

    return matrix[str2.length][str1.length];
  }
}
