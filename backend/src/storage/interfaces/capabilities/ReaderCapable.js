/**
 * 读取能力接口
 * 定义存储驱动的读取操作能力
 * 支持此能力的驱动可以进行文件和目录的读取操作
 */

export class ReaderCapable {
  /**
   * 列出目录内容
   * @param {string} path - 目录路径
   * @param {Object} options - 选项参数
   * @param {number} options.maxKeys - 最大返回项数
   * @param {string} options.continuationToken - 分页令牌
   * @param {boolean} options.recursive - 是否递归列出
   * @returns {Promise<Object>} 目录内容对象
   */
  async list(path, options = {}) {
    throw new Error("list方法必须在实现ReaderCapable的类中实现");
  }

  /**
   * 获取文件内容
   * @param {string} path - 文件路径
   * @param {Object} options - 选项参数
   * @param {string} options.range - HTTP Range头，用于部分内容请求
   * @param {boolean} options.forceDownload - 是否强制下载
   * @returns {Promise<Response>} 文件内容响应
   */
  async get(path, options = {}) {
    throw new Error("get方法必须在实现ReaderCapable的类中实现");
  }

  /**
   * 获取文件信息（不包含内容）
   * @param {string} path - 文件路径
   * @param {Object} options - 选项参数
   * @returns {Promise<Object>} 文件信息对象
   */
  async getInfo(path, options = {}) {
    throw new Error("getInfo方法必须在实现ReaderCapable的类中实现");
  }

  /**
   * 搜索文件和目录
   * @param {string} query - 搜索查询
   * @param {Object} options - 选项参数
   * @param {string} options.path - 搜索路径范围
   * @param {boolean} options.recursive - 是否递归搜索
   * @param {Array<string>} options.types - 文件类型过滤
   * @returns {Promise<Array>} 搜索结果数组
   */
  async search(query, options = {}) {
    // 默认实现：不支持搜索
    throw new Error("此存储驱动不支持搜索功能");
  }
}

/**
 * 检查对象是否实现了ReaderCapable接口
 * @param {Object} obj - 要检查的对象
 * @returns {boolean} 是否实现了ReaderCapable接口
 */
export function isReaderCapable(obj) {
  return (
    obj &&
    typeof obj.list === "function" &&
    typeof obj.get === "function" &&
    typeof obj.getInfo === "function"
  );
}

/**
 * ReaderCapable能力的标识符
 */
export const READER_CAPABILITY = "ReaderCapable";
