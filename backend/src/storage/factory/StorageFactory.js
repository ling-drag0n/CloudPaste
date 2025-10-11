/**
 * 存储驱动工厂类
 * 负责根据存储类型创建对应的存储驱动实例
 * 基于工厂模式，支持动态创建和管理不同类型的存储驱动
 */

import { S3StorageDriver } from "../drivers/s3/S3StorageDriver.js";

export class StorageFactory {
  /**
   * 支持的存储类型
   */
  static SUPPORTED_TYPES = {
    S3: "S3",
    // 未来扩展：
    // WEBDAV: "WebDAV",
    // LOCAL: "Local"
  };

  /**
   * 创建存储驱动实例
   * @param {string} storageType - 存储类型
   * @param {Object} config - 存储配置
   * @param {string} encryptionSecret - 加密密钥
   * @returns {Promise<StorageDriver>} 存储驱动实例
   */
  static async createDriver(storageType, config, encryptionSecret) {
    if (!storageType) {
      throw new Error("存储类型不能为空");
    }

    if (!config) {
      throw new Error("存储配置不能为空");
    }

    switch (storageType) {
      case StorageFactory.SUPPORTED_TYPES.S3:
        const s3Driver = new S3StorageDriver(config, encryptionSecret);
        await s3Driver.initialize();
        return s3Driver;

      // 未来扩展：
      // case StorageFactory.SUPPORTED_TYPES.WEBDAV:
      //   const webdavDriver = new WebDAVStorageDriver(config, encryptionSecret);
      //   await webdavDriver.initialize();
      //   return webdavDriver;

      // case StorageFactory.SUPPORTED_TYPES.LOCAL:
      //   const localDriver = new LocalStorageDriver(config, encryptionSecret);
      //   await localDriver.initialize();
      //   return localDriver;

      default:
        throw new Error(`不支持的存储类型: ${storageType}`);
    }
  }

  /**
   * 获取支持的存储类型列表
   * @returns {Array<string>} 支持的存储类型
   */
  static getSupportedTypes() {
    return Object.values(StorageFactory.SUPPORTED_TYPES);
  }

  /**
   * 检查存储类型是否支持
   * @param {string} storageType - 存储类型
   * @returns {boolean} 是否支持
   */
  static isTypeSupported(storageType) {
    return Object.values(StorageFactory.SUPPORTED_TYPES).includes(storageType);
  }

  /**
   * 获取存储类型的显示名称
   * @param {string} storageType - 存储类型
   * @returns {string} 显示名称
   */
  static getTypeDisplayName(storageType) {
    const displayNames = {
      [StorageFactory.SUPPORTED_TYPES.S3]: "S3 兼容存储",
      // 未来扩展：
      // [StorageFactory.SUPPORTED_TYPES.WEBDAV]: "WebDAV 存储",
      // [StorageFactory.SUPPORTED_TYPES.LOCAL]: "本地存储"
    };

    return displayNames[storageType] || storageType;
  }

  /**
   * 验证存储配置
   * @param {string} storageType - 存储类型
   * @param {Object} config - 存储配置
   * @returns {Object} 验证结果 { valid: boolean, errors: Array<string> }
   */
  static validateConfig(storageType, config) {
    const errors = [];

    if (!StorageFactory.isTypeSupported(storageType)) {
      errors.push(`不支持的存储类型: ${storageType}`);
      return { valid: false, errors };
    }

    switch (storageType) {
      case StorageFactory.SUPPORTED_TYPES.S3:
        return StorageFactory._validateS3Config(config);

      // 未来扩展其他存储类型的验证

      default:
        errors.push(`未实现的配置验证: ${storageType}`);
        return { valid: false, errors };
    }
  }

  /**
   * 验证S3配置
   * @private
   * @param {Object} config - S3配置
   * @returns {Object} 验证结果
   */
  static _validateS3Config(config) {
    const errors = [];
    const requiredFields = ["id", "name", "provider_type", "endpoint_url", "bucket_name", "access_key_id", "secret_access_key"];

    for (const field of requiredFields) {
      if (!config[field]) {
        errors.push(`S3配置缺少必填字段: ${field}`);
      }
    }

    // 验证URL格式
    if (config.endpoint_url) {
      try {
        new URL(config.endpoint_url);
      } catch (error) {
        errors.push("endpoint_url 格式无效");
      }
    }

    // 验证bucket名称格式
    if (config.bucket_name && !/^[a-z0-9.-]+$/.test(config.bucket_name)) {
      errors.push("bucket_name 格式无效，只能包含小写字母、数字、点和连字符");
    }

    return {
      valid: errors.length === 0,
      errors,
    };
  }
}
