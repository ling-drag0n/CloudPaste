/**
 * 文件分享系统统一抽象层
 * 严格按照FileSystem.js的架构模式设计
 * 负责分享相关的存储操作抽象
 * 委托具体操作给存储驱动实现
 */

import { HTTPException } from "hono/http-exception";
import { ApiStatus } from "../../constants/index.js";
import { CAPABILITIES } from "../interfaces/capabilities/index.js";
import { StorageFactory } from "../factory/StorageFactory.js";
import { StorageConfigUtils } from "../utils/StorageConfigUtils.js";

export class FileShareSystem {
  /**
   * 构造函数 - 严格按照FileSystem.js的模式
   * @param {Object} db - 数据库实例
   * @param {string} encryptionSecret - 加密密钥
   */
  constructor(db, encryptionSecret) {
    this.db = db;
    this.encryptionSecret = encryptionSecret;
  }

  /**
   * 生成预签名上传URL - 纯抽象层方法
   * @param {string} storagePath - 存储路径
   * @param {string} filename - 文件名
   * @param {string|Object} userIdOrInfo - 用户ID或API密钥信息
   * @param {string} userType - 用户类型
   * @param {Object} options - 选项参数
   * @returns {Promise<Object>} 预签名上传URL结果
   */
  async generatePresignedUploadUrl(storagePath, filename, userIdOrInfo, userType, options = {}) {
    // 1. 获取存储配置
    const config = await this._getStorageConfig("S3", options.storageConfigId);

    // 2. 创建存储驱动
    const driver = await this._createDriver("S3", config);

    // 3. 检查驱动是否支持预签名上传能力
    if (!driver.hasCapability(CAPABILITIES.PRESIGNED)) {
      throw new HTTPException(ApiStatus.NOT_IMPLEMENTED, {
        message: `存储驱动 S3 不支持预签名上传`,
      });
    }

    // 4. 委托给驱动执行 - 纯粹的抽象层委托
    return await driver.generatePresignedUrl(storagePath, {
      subPath: storagePath,
      operation: "upload",
      fileName: filename,
      fileSize: options.fileSize,
      expiresIn: options.expiresIn || 3600,
    });
  }

  /**
   * 生成预签名上传URL（使用已验证的配置） - 避免重复权限检查
   * @param {string} storagePath - 存储路径
   * @param {string} filename - 文件名
   * @param {Object} config - 已验证的存储配置
   * @param {Object} options - 选项参数
   * @returns {Promise<Object>} 预签名上传URL结果
   */
  async generatePresignedUploadUrlWithConfig(storagePath, filename, config, options = {}) {
    // 1. 创建存储驱动（使用已验证的配置）
    const driver = await this._createDriver("S3", config);

    // 2. 检查驱动是否支持预签名上传能力
    if (!driver.hasCapability(CAPABILITIES.PRESIGNED)) {
      throw new HTTPException(ApiStatus.NOT_IMPLEMENTED, {
        message: `存储驱动 S3 不支持预签名上传`,
      });
    }

    // 3. 委托给驱动执行 - 纯粹的抽象层委托
    return await driver.generatePresignedUrl(storagePath, {
      subPath: storagePath,
      operation: "upload",
      fileName: filename,
      fileSize: options.fileSize,
      expiresIn: options.expiresIn || 3600,
    });
  }

  /**
   * 直接上传文件到存储（使用已验证的配置）
   * @param {string} storagePath - 存储路径
   * @param {File|Object} file - 文件对象或模拟文件对象
   * @param {Object} config - 已验证的存储配置
   * @param {Object} options - 选项参数
   * @returns {Promise<Object>} 上传结果
   */
  async uploadFileWithConfig(storagePath, file, config, options = {}) {
    // 1. 创建存储驱动（使用已验证的配置）
    const driver = await this._createDriver("S3", config);

    // 2. 检查驱动是否支持写入能力（包含上传功能）
    if (!driver.hasCapability(CAPABILITIES.WRITER)) {
      throw new HTTPException(ApiStatus.NOT_IMPLEMENTED, {
        message: `存储驱动 S3 不支持文件写入`,
      });
    }

    // 3. 委托给驱动执行 - 纯粹的抽象层委托
    return await driver.uploadFile(storagePath, file, {
      subPath: storagePath,
      useMultipart: false, // 直接上传模式
      ...options,
    });
  }

  /**
   * 初始化分片上传
   * @param {string} storagePath - 存储路径
   * @param {string} filename - 文件名
   * @param {Object} config - 已验证的存储配置
   * @param {Object} options - 选项参数
   * @returns {Promise<Object>} 分片上传初始化结果
   */
  async initializeMultipartUploadWithConfig(storagePath, filename, config, options = {}) {
    // 1. 创建存储驱动（使用已验证的配置）
    const driver = await this._createDriver("S3", config);

    // 2. 检查驱动是否支持分片上传能力
    if (!driver.hasCapability(CAPABILITIES.MULTIPART)) {
      throw new HTTPException(ApiStatus.NOT_IMPLEMENTED, {
        message: `存储驱动 S3 不支持分片上传`,
      });
    }

    // 3. 委托给驱动执行 - 纯粹的抽象层委托
    return await driver.initializeFrontendMultipartUpload(storagePath, {
      fileName: filename,
      fileSize: options.fileSize,
      partSize: options.partSize,
      partCount: options.partCount,
    });
  }

  /**
   * 完成分片上传
   * @param {string} storagePath - 存储路径
   * @param {string} uploadId - 上传ID
   * @param {Array} parts - 分片信息数组
   * @param {Object} config - 已验证的存储配置
   * @param {Object} options - 选项参数
   * @returns {Promise<Object>} 分片上传完成结果
   */
  async completeMultipartUploadWithConfig(storagePath, uploadId, parts, config, options = {}) {
    // 1. 创建存储驱动
    const driver = await this._createDriver("S3", config);

    // 2. 检查驱动是否支持分片上传能力
    if (!driver.hasCapability(CAPABILITIES.MULTIPART)) {
      throw new HTTPException(ApiStatus.NOT_IMPLEMENTED, {
        message: `存储驱动 S3 不支持分片上传`,
      });
    }

    // 3. 委托给驱动执行
    return await driver.completeFrontendMultipartUpload(storagePath, {
      uploadId,
      parts,
      fileName: options.fileName,
      fileSize: options.fileSize,
    });
  }

  /**
   * 中止分片上传
   * @param {string} storagePath - 存储路径
   * @param {string} uploadId - 上传ID
   * @param {Object} config - 已验证的存储配置
   * @param {Object} options - 选项参数
   * @returns {Promise<Object>} 中止结果
   */
  async abortMultipartUploadWithConfig(storagePath, uploadId, config, options = {}) {
    // 1. 创建存储驱动（使用已验证的配置）
    const driver = await this._createDriver("S3", config);

    // 2. 检查驱动是否支持分片上传能力
    if (!driver.hasCapability(CAPABILITIES.MULTIPART)) {
      throw new HTTPException(ApiStatus.NOT_IMPLEMENTED, {
        message: `存储驱动 S3 不支持分片上传`,
      });
    }

    // 3. 委托给驱动执行
    return await driver.abortFrontendMultipartUpload(storagePath, {
      uploadId,
      fileName: options.fileName,
    });
  }

  /**
   * 获取存储配置 - 私有方法，按照MountManager的模式
   * @private
   * @param {string} storageType - 存储类型
   * @param {string} storageConfigId - 存储配置ID
   * @returns {Promise<Object>} 存储配置
   */
  async _getStorageConfig(storageType, storageConfigId) {
    return await StorageConfigUtils.getStorageConfig(this.db, storageType, storageConfigId);
  }

  /**
   * 创建存储驱动 - 私有方法，按照MountManager的模式
   * @private
   * @param {string} storageType - 存储类型
   * @param {Object} config - 存储配置
   * @returns {Promise<StorageDriver>} 存储驱动实例
   */
  async _createDriver(storageType, config) {
    return await StorageFactory.createDriver(storageType, config, this.encryptionSecret);
  }
}
