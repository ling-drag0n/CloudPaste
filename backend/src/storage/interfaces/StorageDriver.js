/**
 * 存储驱动抽象接口
 * 定义所有存储驱动必须实现的标准接口
 * 基于 alist 设计理念，为不同存储类型提供统一的操作接口
 */

export class StorageDriver {
  /**
   * 构造函数
   * @param {Object} config - 存储配置对象
   */
  constructor(config) {
    this.config = config;
    this.type = "base"; // 子类应该重写此属性
    this.initialized = false;
  }

  /**
   * 初始化存储驱动
   * @returns {Promise<void>}
   */
  async initialize() {
    throw new Error("initialize方法必须在子类中实现");
  }

  /**
   * 检查存储驱动是否已初始化
   * @returns {boolean}
   */
  isInitialized() {
    return this.initialized;
  }

  /**
   * 获取存储类型
   * @returns {string}
   */
  getType() {
    return this.type;
  }

  /**
   * 列出目录内容
   * @param {string} path - 目录路径
   * @param {Object} options - 选项参数
   * @returns {Promise<Array>} 文件和目录列表
   */
  async listDirectory(path, options = {}) {
    throw new Error("listDirectory方法必须在子类中实现");
  }

  /**
   * 获取文件信息
   * @param {string} path - 文件路径
   * @param {Object} options - 选项参数
   * @returns {Promise<Object>} 文件信息对象
   */
  async getFileInfo(path, options = {}) {
    throw new Error("getFileInfo方法必须在子类中实现");
  }

  /**
   * 下载文件
   * @param {string} path - 文件路径
   * @param {Object} options - 选项参数
   * @returns {Promise<Response>} 文件响应对象
   */
  async downloadFile(path, options = {}) {
    throw new Error("downloadFile方法必须在子类中实现");
  }

  /**
   * 上传文件
   * @param {string} path - 目标路径
   * @param {File} file - 文件对象
   * @param {Object} options - 选项参数
   * @returns {Promise<Object>} 上传结果
   */
  async uploadFile(path, file, options = {}) {
    throw new Error("uploadFile方法必须在子类中实现");
  }

  /**
   * 创建目录
   * @param {string} path - 目录路径
   * @param {Object} options - 选项参数
   * @returns {Promise<Object>} 创建结果
   */
  async createDirectory(path, options = {}) {
    throw new Error("createDirectory方法必须在子类中实现");
  }

  /**
   * 重命名文件或目录
   * @param {string} oldPath - 原路径
   * @param {string} newPath - 新路径
   * @param {Object} options - 选项参数
   * @returns {Promise<Object>} 重命名结果
   */
  async renameItem(oldPath, newPath, options = {}) {
    throw new Error("renameItem方法必须在子类中实现");
  }

  /**
   * 批量删除文件
   * @param {Array<string>} paths - 路径数组
   * @param {Object} options - 选项参数
   * @returns {Promise<Object>} 批量删除结果
   */
  async batchRemoveItems(paths, options = {}) {
    throw new Error("batchRemoveItems方法必须在子类中实现");
  }

  /**
   * 复制文件或目录
   * @param {string} sourcePath - 源路径
   * @param {string} targetPath - 目标路径
   * @param {Object} options - 选项参数
   * @returns {Promise<Object>} 复制结果
   */
  async copyItem(sourcePath, targetPath, options = {}) {
    throw new Error("copyItem方法必须在子类中实现");
  }

  /**
   * 批量复制文件
   * @param {Array<Object>} items - 复制项数组，每项包含 sourcePath 和 targetPath
   * @param {Object} options - 选项参数
   * @returns {Promise<Object>} 批量复制结果
   */
  async batchCopyItems(items, options = {}) {
    throw new Error("batchCopyItems方法必须在子类中实现");
  }

  /**
   * 生成预签名URL
   * @param {string} path - 文件路径
   * @param {Object} options - 选项参数
   * @param {string} options.operation - 操作类型：'download' 或 'upload'
   * @returns {Promise<Object>} 预签名URL信息
   */
  async generatePresignedUrl(path, options = {}) {
    throw new Error("generatePresignedUrl方法必须在子类中实现");
  }

  /**
   * 更新文件内容
   * @param {string} path - 文件路径
   * @param {string} content - 新内容
   * @param {Object} options - 选项参数
   * @returns {Promise<Object>} 更新结果
   */
  async updateFile(path, content, options = {}) {
    throw new Error("updateFile方法必须在子类中实现");
  }

  /**
   * 处理跨存储复制
   * @param {string} sourcePath - 源路径
   * @param {string} targetPath - 目标路径
   * @param {Object} options - 选项参数
   * @returns {Promise<Object>} 跨存储复制结果
   */
  async handleCrossStorageCopy(sourcePath, targetPath, options = {}) {
    throw new Error("handleCrossStorageCopy方法必须在子类中实现");
  }

  /**
   * 检查路径是否存在
   * @param {string} path - 路径
   * @param {Object} options - 选项参数
   * @returns {Promise<boolean>} 是否存在
   */
  async exists(path, options = {}) {
    throw new Error("exists方法必须在子类中实现");
  }

  /**
   * 获取存储统计信息
   * @returns {Promise<Object>} 统计信息
   */
  async getStats() {
    throw new Error("getStats方法必须在子类中实现");
  }

  // /**
  //  * 初始化后端分片上传 - 已废弃，项目使用前端分片上传
  //  * @deprecated 使用前端分片上传 initializeFrontendMultipartUpload 替代
  //  * @param {string} path - 目标路径
  //  * @param {Object} options - 选项参数
  //  * @returns {Promise<Object>} 初始化结果
  //  */
  // async initializeBackendMultipartUpload(path, options = {}) {
  //   throw new Error("initializeBackendMultipartUpload方法必须在子类中实现");
  // }

  // /**
  //  * 上传后端分片 - 已废弃，项目使用前端分片上传
  //  * @deprecated 使用前端分片上传替代
  //  * @param {string} path - 目标路径
  //  * @param {Object} options - 选项参数
  //  * @returns {Promise<Object>} 上传结果
  //  */
  // async uploadBackendPart(path, options = {}) {
  //   throw new Error("uploadBackendPart方法必须在子类中实现");
  // }

  // /**
  //  * 完成后端分片上传 - 已废弃，项目使用前端分片上传
  //  * @deprecated 使用前端分片上传 completeFrontendMultipartUpload 替代
  //  * @param {string} path - 目标路径
  //  * @param {Object} options - 选项参数
  //  * @returns {Promise<Object>} 完成结果
  //  */
  // async completeBackendMultipartUpload(path, options = {}) {
  //   throw new Error("completeBackendMultipartUpload方法必须在子类中实现");
  // }

  /**
   * 中止后端分片上传
   * @param {string} path - 目标路径
   * @param {Object} options - 选项参数
   * @returns {Promise<Object>} 中止结果
   */
  async abortBackendMultipartUpload(path, options = {}) {
    throw new Error("abortBackendMultipartUpload方法必须在子类中实现");
  }

  /**
   * 清理资源
   * @returns {Promise<void>}
   */
  async cleanup() {
    this.initialized = false;
  }
}
