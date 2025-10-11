/**
 * 预签名URL能力接口
 * 定义存储驱动的预签名URL生成能力
 * 主要用于S3等支持预签名URL的存储服务
 * 可以生成临时的、安全的直接访问URL
 */

export class PresignedCapable {
  /**
   * 生成预签名下载URL
   * @param {string} path - 文件路径
   * @param {Object} options - 选项参数
   * @param {number} options.expiresIn - URL过期时间（秒），默认3600秒
   * @param {boolean} options.forceDownload - 是否强制下载
   * @param {string} options.responseContentType - 响应内容类型
   * @param {string} options.responseContentDisposition - 响应内容处置
   * @returns {Promise<Object>} 预签名下载URL信息
   */
  async generateDownloadUrl(path, options = {}) {
    throw new Error("generateDownloadUrl方法必须在实现PresignedCapable的类中实现");
  }

  /**
   * 生成预签名上传URL
   * @param {string} path - 目标路径
   * @param {Object} options - 选项参数
   * @param {number} options.expiresIn - URL过期时间（秒），默认3600秒
   * @param {string} options.contentType - 内容类型
   * @param {number} options.contentLength - 内容长度限制
   * @param {Object} options.metadata - 文件元数据
   * @returns {Promise<Object>} 预签名上传URL信息
   */
  async generateUploadUrl(path, options = {}) {
    throw new Error("generateUploadUrl方法必须在实现PresignedCapable的类中实现");
  }

  /**
   * 生成预签名URL（通用方法）
   * @param {string} path - 文件路径
   * @param {string} operation - 操作类型：'download' 或 'upload'
   * @param {Object} options - 选项参数
   * @returns {Promise<Object>} 预签名URL信息
   */
  async generatePresignedUrl(path, operation, options = {}) {
    switch (operation) {
      case "download":
        return await this.generateDownloadUrl(path, options);
      case "upload":
        return await this.generateUploadUrl(path, options);
      default:
        throw new Error(`不支持的预签名URL操作类型: ${operation}`);
    }
  }

  /**
   * 批量生成预签名下载URL
   * @param {Array<string>} paths - 文件路径数组
   * @param {Object} options - 选项参数
   * @returns {Promise<Array>} 预签名URL信息数组
   */
  async batchGenerateDownloadUrls(paths, options = {}) {
    // 默认实现：逐个生成
    const results = [];
    const errors = [];

    for (const path of paths) {
      try {
        const result = await this.generateDownloadUrl(path, options);
        results.push({ path, success: true, ...result });
      } catch (error) {
        errors.push({ path, success: false, error: error.message });
        if (!options.continueOnError) {
          throw error;
        }
      }
    }

    return {
      success: errors.length === 0,
      results,
      errors,
      total: paths.length,
      succeeded: results.length,
      failed: errors.length,
    };
  }

  /**
   * 验证预签名URL是否有效
   * @param {string} url - 预签名URL
   * @param {Object} options - 选项参数
   * @returns {Promise<boolean>} 是否有效
   */
  async validatePresignedUrl(url, options = {}) {
    // 默认实现：简单的URL格式检查
    try {
      const urlObj = new URL(url);
      return urlObj.protocol === "https:" || urlObj.protocol === "http:";
    } catch {
      return false;
    }
  }
}

/**
 * 检查对象是否实现了PresignedCapable接口
 * @param {Object} obj - 要检查的对象
 * @returns {boolean} 是否实现了PresignedCapable接口
 */
export function isPresignedCapable(obj) {
  return (
    obj &&
    typeof obj.generateDownloadUrl === "function" &&
    typeof obj.generateUploadUrl === "function" &&
    typeof obj.generatePresignedUrl === "function"
  );
}

/**
 * PresignedCapable能力的标识符
 */
export const PRESIGNED_CAPABILITY = "PresignedCapable";
