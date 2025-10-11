/**
 * 基础存储驱动接口
 * 所有存储驱动必须实现的核心基础接口
 * 基于alist设计理念，提供最小化的必需接口
 */

export class BaseDriver {
  /**
   * 构造函数
   * @param {Object} config - 存储配置对象
   */
  constructor(config) {
    this.config = config;
    this.type = "base"; // 子类应该重写此属性
    this.initialized = false;
    this.capabilities = []; // 子类需要声明支持的能力
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
   * 获取支持的能力列表
   * @returns {Array<string>} 能力名称数组
   */
  getCapabilities() {
    return [...this.capabilities];
  }

  /**
   * 检查是否支持特定能力
   * @param {string} capability - 能力名称
   * @returns {boolean}
   */
  hasCapability(capability) {
    return this.capabilities.includes(capability);
  }

  /**
   * 获取文件或目录的状态信息
   * @param {string} path - 文件或目录路径
   * @param {Object} options - 选项参数
   * @returns {Promise<Object>} 状态信息对象
   */
  async stat(path, options = {}) {
    throw new Error("stat方法必须在子类中实现");
  }

  /**
   * 检查文件或目录是否存在
   * @param {string} path - 文件或目录路径
   * @param {Object} options - 选项参数
   * @returns {Promise<boolean>} 是否存在
   */
  async exists(path, options = {}) {
    throw new Error("exists方法必须在子类中实现");
  }

  /**
   * 清理资源
   * @returns {Promise<void>}
   */
  async cleanup() {
    // 默认实现：什么都不做
    // 子类可以重写此方法来清理特定资源
  }

  /**
   * 获取存储统计信息
   * @returns {Promise<Object>} 统计信息对象
   */
  async getStats() {
    // 默认实现：返回基础信息
    return {
      type: this.type,
      initialized: this.initialized,
      capabilities: this.capabilities,
    };
  }

  /**
   * 确保驱动已初始化
   * @private
   */
  _ensureInitialized() {
    if (!this.initialized) {
      throw new Error(`存储驱动未初始化: ${this.type}`);
    }
  }
}
