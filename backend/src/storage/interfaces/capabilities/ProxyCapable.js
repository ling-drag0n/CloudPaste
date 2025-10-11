/**
 * 代理能力接口
 * 定义存储驱动的代理访问能力
 * 支持此能力的驱动可以生成代理URL，提供无认证的公开访问
 */

export class ProxyCapable {
  /**
   * 生成代理URL
   * @param {string} path - 文件路径
   * @param {Object} options - 选项参数
   * @param {Object} options.mount - 挂载点信息
   * @param {Request} options.request - 请求对象（用于构建完整URL）
   * @param {boolean} options.download - 是否为下载模式
   * @returns {Promise<Object>} 代理URL对象
   */
  async generateProxyUrl(path, options = {}) {
    throw new Error("generateProxyUrl方法必须在实现ProxyCapable的类中实现");
  }

  /**
   * 检查是否支持代理模式
   * @param {Object} mount - 挂载点信息
   * @returns {boolean} 是否支持代理模式
   */
  supportsProxyMode(mount) {
    // 默认实现：检查挂载点的web_proxy配置（兼容数据库的0/1和布尔值）
    return mount && !!mount.web_proxy;
  }

  /**
   * 获取代理配置
   * @param {Object} mount - 挂载点信息
   * @returns {Object} 代理配置对象
   */
  getProxyConfig(mount) {
    return {
      enabled: this.supportsProxyMode(mount),
      webdavPolicy: mount?.webdav_policy || "302_redirect",
    };
  }
}

/**
 * 检查对象是否实现了ProxyCapable接口
 * @param {Object} obj - 要检查的对象
 * @returns {boolean} 是否实现了ProxyCapable接口
 */
export function isProxyCapable(obj) {
  return obj && typeof obj.generateProxyUrl === "function" && typeof obj.supportsProxyMode === "function";
}

/**
 * ProxyCapable能力的标识符
 */
export const PROXY_CAPABILITY = "ProxyCapable";
