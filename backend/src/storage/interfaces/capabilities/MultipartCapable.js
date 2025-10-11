/**
 * 分片上传能力接口
 * 定义存储驱动的分片上传操作能力
 * 主要用于S3等支持分片上传的存储服务
 * 支持大文件的高效上传和断点续传
 */

export class MultipartCapable {
  /**
   * 初始化分片上传
   * @param {string} path - 目标路径
   * @param {Object} options - 选项参数
   * @param {string} options.contentType - 内容类型
   * @param {number} options.fileSize - 文件总大小
   * @param {string} options.fileName - 文件名
   * @param {Object} options.metadata - 文件元数据
   * @returns {Promise<Object>} 初始化结果，包含uploadId
   */
  async initializeMultipartUpload(path, options = {}) {
    throw new Error("initializeMultipartUpload方法必须在实现MultipartCapable的类中实现");
  }

  /**
   * 上传分片
   * @param {string} uploadId - 上传ID
   * @param {number} partNumber - 分片编号（从1开始）
   * @param {Buffer|Stream} partData - 分片数据
   * @param {Object} options - 选项参数
   * @param {string} options.path - 文件路径（用于某些存储服务）
   * @returns {Promise<Object>} 上传结果，包含ETag
   */
  async uploadPart(uploadId, partNumber, partData, options = {}) {
    throw new Error("uploadPart方法必须在实现MultipartCapable的类中实现");
  }

  /**
   * 完成分片上传
   * @param {string} uploadId - 上传ID
   * @param {Array<Object>} parts - 分片信息数组
   * @param {Object} options - 选项参数
   * @param {string} options.path - 文件路径
   * @returns {Promise<Object>} 完成结果
   */
  async completeMultipartUpload(uploadId, parts, options = {}) {
    throw new Error("completeMultipartUpload方法必须在实现MultipartCapable的类中实现");
  }

  /**
   * 取消分片上传
   * @param {string} uploadId - 上传ID
   * @param {Object} options - 选项参数
   * @param {string} options.path - 文件路径
   * @returns {Promise<Object>} 取消结果
   */
  async abortMultipartUpload(uploadId, options = {}) {
    throw new Error("abortMultipartUpload方法必须在实现MultipartCapable的类中实现");
  }

  /**
   * 列出已上传的分片
   * @param {string} uploadId - 上传ID
   * @param {Object} options - 选项参数
   * @param {string} options.path - 文件路径
   * @param {number} options.maxParts - 最大返回分片数
   * @param {number} options.partNumberMarker - 分片编号标记
   * @returns {Promise<Object>} 分片列表
   */
  async listParts(uploadId, options = {}) {
    throw new Error("listParts方法必须在实现MultipartCapable的类中实现");
  }

  /**
   * 列出进行中的分片上传
   * @param {Object} options - 选项参数
   * @param {string} options.prefix - 路径前缀
   * @param {number} options.maxUploads - 最大返回上传数
   * @param {string} options.keyMarker - 键标记
   * @param {string} options.uploadIdMarker - 上传ID标记
   * @returns {Promise<Object>} 上传列表
   */
  async listMultipartUploads(options = {}) {
    throw new Error("listMultipartUploads方法必须在实现MultipartCapable的类中实现");
  }

  /**
   * 生成分片上传的预签名URL列表
   * @param {string} path - 目标路径
   * @param {Object} options - 选项参数
   * @param {number} options.partCount - 分片数量
   * @param {number} options.partSize - 分片大小
   * @param {number} options.expiresIn - URL过期时间（秒）
   * @param {string} options.contentType - 内容类型
   * @returns {Promise<Object>} 预签名URL列表和上传信息
   */
  async generateMultipartPresignedUrls(path, options = {}) {
    // 默认实现：不支持预签名分片上传
    throw new Error("此存储驱动不支持预签名分片上传");
  }

  /**
   * 计算推荐的分片大小
   * @param {number} fileSize - 文件大小（字节）
   * @param {Object} options - 选项参数
   * @param {number} options.minPartSize - 最小分片大小，默认5MB
   * @param {number} options.maxPartSize - 最大分片大小，默认100MB
   * @param {number} options.maxParts - 最大分片数，默认10000
   * @returns {Object} 推荐的分片配置
   */
  calculateOptimalPartSize(fileSize, options = {}) {
    const minPartSize = options.minPartSize || 5 * 1024 * 1024; // 5MB
    const maxPartSize = options.maxPartSize || 100 * 1024 * 1024; // 100MB
    const maxParts = options.maxParts || 10000;

    // 计算基于文件大小的分片大小
    let partSize = Math.ceil(fileSize / maxParts);
    
    // 确保分片大小在合理范围内
    partSize = Math.max(partSize, minPartSize);
    partSize = Math.min(partSize, maxPartSize);
    
    // 计算实际分片数
    const partCount = Math.ceil(fileSize / partSize);

    return {
      partSize,
      partCount,
      fileSize,
      lastPartSize: fileSize % partSize || partSize,
    };
  }
}

/**
 * 检查对象是否实现了MultipartCapable接口
 * @param {Object} obj - 要检查的对象
 * @returns {boolean} 是否实现了MultipartCapable接口
 */
export function isMultipartCapable(obj) {
  return (
    obj &&
    typeof obj.initializeMultipartUpload === "function" &&
    typeof obj.uploadPart === "function" &&
    typeof obj.completeMultipartUpload === "function" &&
    typeof obj.abortMultipartUpload === "function"
  );
}

/**
 * MultipartCapable能力的标识符
 */
export const MULTIPART_CAPABILITY = "MultipartCapable";
