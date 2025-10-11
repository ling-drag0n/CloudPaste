/**
 * 写入能力接口
 * 定义存储驱动的写入操作能力
 * 支持此能力的驱动可以进行文件和目录的创建、更新、删除操作
 */

export class WriterCapable {
  /**
   * 上传文件
   * @param {string} path - 目标路径
   * @param {File|Buffer|Stream} data - 文件数据
   * @param {Object} options - 选项参数
   * @param {string} options.contentType - 内容类型
   * @param {number} options.contentLength - 内容长度
   * @param {Object} options.metadata - 文件元数据
   * @returns {Promise<Object>} 上传结果
   */
  async put(path, data, options = {}) {
    throw new Error("put方法必须在实现WriterCapable的类中实现");
  }

  /**
   * 创建目录
   * @param {string} path - 目录路径
   * @param {Object} options - 选项参数
   * @param {boolean} options.recursive - 是否递归创建父目录
   * @returns {Promise<Object>} 创建结果
   */
  async mkdir(path, options = {}) {
    throw new Error("mkdir方法必须在实现WriterCapable的类中实现");
  }

  /**
   * 批量删除文件和目录
   * @param {Array<string>} paths - 路径数组
   * @param {Object} options - 选项参数
   * @param {boolean} options.continueOnError - 遇到错误时是否继续
   * @returns {Promise<Object>} 批量删除结果
   */
  async batchRemoveItems(paths, options = {}) {
    throw new Error("batchRemoveItems方法必须在实现WriterCapable的类中实现");
  }

  /**
   * 更新文件内容
   * @param {string} path - 文件路径
   * @param {string|Buffer} content - 新内容
   * @param {Object} options - 选项参数
   * @param {string} options.contentType - 内容类型
   * @returns {Promise<Object>} 更新结果
   */
  async update(path, content, options = {}) {
    // 默认实现：使用put方法
    return await this.put(path, content, options);
  }
}

/**
 * 检查对象是否实现了WriterCapable接口
 * @param {Object} obj - 要检查的对象
 * @returns {boolean} 是否实现了WriterCapable接口
 */
export function isWriterCapable(obj) {
  return obj && typeof obj.put === "function" && typeof obj.mkdir === "function" && typeof obj.batchRemoveItems === "function";
}

/**
 * WriterCapable能力的标识符
 */
export const WRITER_CAPABILITY = "WriterCapable";
