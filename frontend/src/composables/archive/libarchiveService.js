/**
 * libarchive文件处理服务
 * 基于libarchive.js，专门处理RAR、7Z、TAR等格式文件
 */

import { Archive } from "libarchive.js";
import { sharedFileBlobCache, getOrDownloadFileBlob, isWebAssemblySupported, countTotalFiles, countExtractedFiles } from "./archiveUtils.js";

/**
 * libarchive文件处理服务类
 */
class LibarchiveService {
  constructor() {
    this.libarchiveInitialized = false;
    this.initPromise = null;

    // 简单配置对象
    this.config = {
      workerUrl: "/libarchive.js/dist/worker-bundle.js",
    };

    // 使用共享的文件Blob缓存
    this.fileBlobCache = sharedFileBlobCache;
  }

  /**
   * 统一的libarchive文件解压接口
   * @param {Blob|File|string} fileBlobOrUrl - 压缩文件 Blob 对象或URL
   * @param {string} filename - 文件名
   * @param {string} fileUrl - 文件URL（用于缓存键）
   * @param {Function} progressCallback - 进度回调函数 (progress: 0-100)
   * @param {string|null} password - 可选的解压密码
   * @param {Object} archiveType - 压缩格式信息
   * @returns {Promise<Array>} 统一格式的文件列表
   */
  async extractArchive(fileBlobOrUrl, filename, fileUrl = "", progressCallback = null, password = null, archiveType) {
    console.log(`开始处理 ${archiveType.name} 格式文件:`, filename);

    // libarchive格式：密码检测和分支逻辑
    let hasEncrypted = false;
    let fileBlob = null;

    if (typeof fileBlobOrUrl === "string" && fileBlobOrUrl.startsWith("http")) {
      // 远程文件：先下载用于检测
      fileBlob = await getOrDownloadFileBlob(fileBlobOrUrl, progressCallback, 0, 50, "下载中");
      hasEncrypted = await this.libarchiveEncryptionCheck(fileBlob, archiveType, progressCallback);
    } else {
      hasEncrypted = await this.libarchiveEncryptionCheck(fileBlobOrUrl, archiveType, progressCallback);
      fileBlob = fileBlobOrUrl;
    }

    // 第二步：根据加密状态和密码情况选择处理方式
    if (hasEncrypted && !password) {
      throw new Error("ENCRYPTED_ARCHIVE_DETECTED");
    } else if (hasEncrypted && password) {
      // 加密文件需要密码：fileBlob应该已经在检测阶段准备好了
      if (!fileBlob) {
        console.warn("密码解压时fileBlob为空，尝试重新获取");
        if (typeof fileBlobOrUrl === "string" && fileBlobOrUrl.startsWith("http")) {
          fileBlob = await getOrDownloadFileBlob(fileBlobOrUrl, progressCallback, 60, 70, "重新下载");
        } else {
          fileBlob = fileBlobOrUrl;
        }
      }

      if (progressCallback) progressCallback(70, "准备密码解压");
      return await this.extractLibarchiveWithPassword(fileBlob, archiveType, password, progressCallback);
    } else {
      return await this.extractWithLibarchiveStream(fileBlob, archiveType, progressCallback);
    }
  }

  /**
   * 初始化 libarchive.js（懒加载）
   * @returns {Promise<boolean>} 是否初始化成功
   */
  async initLibarchive() {
    if (this.libarchiveInitialized) {
      return true;
    }

    // 避免重复初始化
    if (this.initPromise) {
      return await this.initPromise;
    }

    this.initPromise = this._performInit();
    return await this.initPromise;
  }

  async _performInit() {
    try {
      // 检查 WebAssembly 支持
      if (!isWebAssemblySupported()) {
        console.warn("当前浏览器不支持 WebAssembly，libarchive.js 功能将不可用");
        return false;
      }

      console.log("正在初始化 libarchive.js WebWorker:", this.config.workerUrl);

      Archive.init({
        workerUrl: this.config.workerUrl,
      });

      this.libarchiveInitialized = true;
      console.log("libarchive.js 初始化成功，WebWorker已配置");
      return true;
    } catch (error) {
      console.warn("libarchive.js 初始化失败，将降级到仅支持 ZIP:", error);
      return false;
    }
  }

  /**
   * libarchive加密检测
   * @param {Blob} fileBlob - 已下载的文件Blob
   * @param {Object} archiveType - 压缩格式信息
   * @param {Function} progressCallback - 进度回调函数
   * @returns {Promise<boolean>} true表示检测到加密，false表示无加密
   */
  async libarchiveEncryptionCheck(fileBlob, archiveType, progressCallback = null) {
    const initialized = await this.initLibarchive();

    if (!initialized) {
      throw new Error(`libarchive.js 未初始化，无法检测 ${archiveType.name} 格式`);
    }

    try {
      console.log(`开始检测 ${archiveType.name} 文件是否加密...`);
      if (progressCallback) progressCallback(55, "检测加密");

      // 使用官方API打开压缩文件
      const archive = await Archive.open(fileBlob);

      try {
        // 检查是否有加密数据
        const hasEncrypted = await archive.hasEncryptedData();

        if (progressCallback) {
          progressCallback(60, hasEncrypted ? "发现加密" : "无加密");
        }

        console.log(`${archiveType.name} 加密检测完成:`, hasEncrypted === true ? "有加密" : "无加密");
        return hasEncrypted === true;
      } finally {
        // 关闭archive释放worker
        try {
          await archive.close();
        } catch (closeError) {
          console.warn("关闭archive时出错:", closeError);
        }
      }
    } catch (error) {
      console.warn(`⚠️ ${archiveType.name} 加密检测失败:`, error.message);
      return false;
    }
  }

  /**
   * libarchive密码解压
   * @param {Blob} fileBlob - 已下载的文件Blob
   * @param {Object} archiveType - 压缩格式信息
   * @param {string} password - 解压密码
   * @param {Function} progressCallback - 进度回调函数
   * @returns {Promise<Array>} 统一格式的文件列表
   */
  async extractLibarchiveWithPassword(fileBlob, archiveType, password, progressCallback = null) {
    const initialized = await this.initLibarchive();

    if (!initialized) {
      throw new Error(`libarchive.js 未初始化，无法处理 ${archiveType.name} 格式`);
    }

    let archive = null;
    try {
      console.log(`开始libarchive密码解压 ${archiveType.name} 文件...`);
      if (progressCallback) progressCallback(75, "解析中");

      // 打开压缩文件
      archive = await Archive.open(fileBlob);
      // 设置密码
      await archive.usePassword(password);
      console.log(`${archiveType.name} 文件密码已设置`);

      // 全量解压所有文件
      const extractedFiles = await archive.extractFiles((entry) => {
        if (progressCallback && entry.path) {
          console.log(`正在解压: ${entry.path}`);
        }
      });
      console.log(`${archiveType.name} 全量解压完成`);

      const result = [];
      let processedFiles = 0;
      const totalFiles = countExtractedFiles(extractedFiles);

      // 转换为统一格式（全量解压，内容已缓存）
      const processFiles = (obj, basePath = "") => {
        for (const [name, item] of Object.entries(obj)) {
          const fullPath = basePath ? `${basePath}/${name}` : name;

          if (item instanceof File) {
            // 这是一个已解压的File对象
            result.push({
              name: fullPath,
              size: item.size || 0,
              compressedSize: 0,
              isDirectory: false,
              lastModDate: item.lastModified ? new Date(item.lastModified) : new Date(),
              entry: {
                entry: item,
                type: "libarchive-password",
                cachedContent: null, // 将在下面设置
                async getContent() {
                  // 直接返回缓存的ArrayBuffer
                  if (!this.cachedContent) {
                    this.cachedContent = await item.arrayBuffer();
                  }
                  return this.cachedContent;
                },
              },
            });

            // 立即缓存内容
            item.arrayBuffer().then((buffer) => {
              result[result.length - 1].entry.cachedContent = buffer;
            });
          } else if (typeof item === "object" && item !== null) {
            // 这是一个目录，递归处理
            result.push({
              name: fullPath + "/",
              size: 0,
              compressedSize: 0,
              isDirectory: true,
              lastModDate: new Date(),
              entry: {
                entry: null,
                type: "libarchive-password",
                async getContent() {
                  throw new Error("Cannot extract directory");
                },
              },
            });
            processFiles(item, fullPath);
          }

          // 更新解压进度
          processedFiles++;
          if (progressCallback) {
            // 解压进度：75-100%
            const extractProgress = 75 + (processedFiles / totalFiles) * 25;
            progressCallback(Math.min(extractProgress, 100), "解压中");
          }
        }
      };

      processFiles(extractedFiles);

      if (progressCallback) progressCallback(100, "完成");

      console.log(`libarchive密码解压完成，处理了 ${result.length} 个项目`);
      return result;
    } catch (error) {
      console.error(`libarchive密码解压 ${archiveType.name} 失败:`, error);
      throw new Error(`${archiveType.name} 密码解压失败: ${error.message}`);
    } finally {
      // 确保关闭archive释放worker
      if (archive) {
        try {
          await archive.close();
        } catch (closeError) {
          console.warn("关闭archive时出错:", closeError);
        }
      }
    }
  }

  /**
   * 使用 libarchive.js 按需解压模式
   * @param {Blob|File} fileBlob - 压缩文件 Blob 对象
   * @param {Object} archiveType - 压缩格式信息
   * @param {Function} progressCallback - 进度回调函数 (progress: 0-100)
   * @returns {Promise<Array>} 统一格式的文件列表
   */
  async extractWithLibarchiveStream(fileBlob, archiveType, progressCallback = null) {
    const initialized = await this.initLibarchive();

    if (!initialized) {
      throw new Error(`libarchive.js 未初始化，无法处理 ${archiveType.name} 格式`);
    }

    let archive = null;
    try {
      // 使用官方API打开压缩文件
      archive = await Archive.open(fileBlob);
      // 获取文件列表（不解压内容，按需解压模式）
      const filesObject = await archive.getFilesObject();

      console.log(`libarchive.js 获取 ${archiveType.name} 文件列表完成`);
      console.log(`文件内容将按需解压，不占用大量内存`);

      // 转换为统一格式（按需解压）
      const result = [];
      let processedFiles = 0;
      const totalFiles = countTotalFiles(filesObject); // 计算总文件数

      const processFiles = (obj, basePath = "") => {
        for (const [name, item] of Object.entries(obj)) {
          const fullPath = basePath ? `${basePath}/${name}` : name;

          if (item && typeof item.extract === "function") {
            // 这是一个 CompressedFile，支持按需解压
            result.push({
              name: fullPath,
              size: item.size || 0,
              compressedSize: item.compressedSize || 0,
              isDirectory: false,
              lastModDate: new Date(),
              entry: {
                entry: item,
                type: "libarchive",
                async getContent() {
                  // 按需解压：只有在需要时才解压单个文件
                  console.log(`按需解压文件: ${fullPath}`);
                  try {
                    const file = await item.extract();
                    return await file.arrayBuffer();
                  } catch (error) {
                    console.error(`解压文件 ${fullPath} 失败:`, error);
                    throw new Error(`解压文件失败: ${error.message}`);
                  }
                },
              },
            });
          } else if (typeof item === "object" && item !== null) {
            // 这是一个目录，递归处理
            result.push({
              name: fullPath + "/",
              size: 0,
              compressedSize: 0,
              isDirectory: true,
              lastModDate: new Date(),
              entry: {
                entry: null,
                type: "libarchive",
                async getContent() {
                  throw new Error("Cannot extract directory");
                },
              },
            });
            processFiles(item, fullPath);
          }

          // 更新文件列表处理进度
          processedFiles++;
          if (progressCallback) {
            // 文件列表处理进度
            const progress = (processedFiles / totalFiles) * 100;
            progressCallback(Math.min(progress, 100), "分析文件");
          }
        }
      };

      processFiles(filesObject);

      console.log(`libarchive按需解压准备完成，处理了 ${result.length} 个项目`);
      return result;
    } catch (error) {
      console.error(`libarchive.js 解压 ${archiveType.name} 失败:`, error);
      throw new Error(`${archiveType.name} 文件解压失败: ${error.message}`);
    }
  }

  /**
   * 清除文件Blob缓存
   * @param {string} fileUrl - 文件URL
   */
  clearFileBlobCache(fileUrl) {
    // 共享缓存由archiveUtils管理
    if (!fileUrl) return;

    this.fileBlobCache.delete(fileUrl);
    console.log("已清除libarchive服务文件Blob缓存:", fileUrl);
  }
}

// 导出单例实例
export const libarchiveService = new LibarchiveService();
export default libarchiveService;
