import crypto from "crypto";
import { RepositoryFactory } from "../repositories/index.js";

/**
 * 代理签名服务
 * 支持两层签名策略：全局签名所有 + 存储级签名
 */
export class ProxySignatureService {
  constructor(db, encryptionSecret) {
    this.db = db;
    this.secret = encryptionSecret;
    this.configCache = new Map(); // 配置缓存

    // 初始化Repository
    const repositoryFactory = new RepositoryFactory(db);
    this.systemRepository = repositoryFactory.getSystemRepository();
  }

  /**
   * 判断挂载点是否需要签名
   * @param {Object} mount - 挂载点配置
   * @returns {Promise<Object>} 签名需求结果
   */
  async needsSignature(mount) {
    // 1. 检查全局"签名所有"设置
    const signAll = await this._getSystemSetting("proxy_sign_all");
    if (signAll === "true") {
      return {
        required: true,
        reason: "sign_all_enabled",
        level: "global",
        description: "全局签名所有已启用",
      };
    }

    // 2. 检查存储级别的签名设置
    if (mount.enable_sign === 1 || mount.enable_sign === true) {
      return {
        required: true,
        reason: "storage_sign_enabled",
        level: "storage",
        description: "存储启用签名",
      };
    }

    // 3. 不需要签名
    return {
      required: false,
      reason: "no_sign_required",
      level: "none",
      description: "无签名要求",
    };
  }

  /**
   * 获取签名过期时间
   * @param {Object} mount - 挂载点配置
   * @returns {Promise<number>} 过期时间（秒），0表示永不过期
   */
  async getSignatureExpiration(mount) {
    // 1. 挂载点自定义时间
    if (mount.sign_expires !== null && mount.sign_expires !== undefined) {
      return mount.sign_expires;
    }

    // 2. 使用全局默认时间
    const globalExpires = await this._getSystemSetting("proxy_sign_expires");
    return parseInt(globalExpires) || 0;
  }

  /**
   * 生成签名
   * @param {string} path - 文件路径
   * @param {Object} mount - 挂载点配置
   * @param {Object} options - 额外选项
   * @returns {Promise<Object>} 签名信息
   */
  async generateStorageSignature(path, mount, options = {}) {
    // 获取过期时间
    const expiresIn = options.expiresIn !== undefined ? options.expiresIn : await this.getSignatureExpiration(mount);

    // 0表示永不过期
    const expireTimestamp = expiresIn > 0 ? Math.floor(Date.now() / 1000) + expiresIn : 0;

    // 生成签名数据：路径 + 过期时间戳
    const signData = `${path}:${expireTimestamp}`;

    // 使用HMAC-SHA256生成签名
    const hmac = crypto.createHmac("sha256", this.secret);
    hmac.update(signData);
    const hash = hmac.digest("base64");

    return {
      signature: `${hash}:${expireTimestamp}`,
      requestTimestamp: Date.now(),
      expiresAt: expireTimestamp,
      expiresIn: expiresIn,
      isTemporary: expireTimestamp > 0,
    };
  }

  /**
   * 验证签名
   * @param {string} path - 文件路径
   * @param {string} signature - 签名值
   * @returns {Object} 验证结果
   */
  verifyStorageSignature(path, signature) {
    try {
      const [hash, timestampStr] = signature.split(":");
      const expireTimestamp = parseInt(timestampStr);

      // 检查签名是否过期（0表示永不过期）
      if (expireTimestamp > 0 && Math.floor(Date.now() / 1000) > expireTimestamp) {
        return {
          valid: false,
          reason: "signature_expired",
          expiredAt: expireTimestamp,
        };
      }

      // 重新生成签名进行比较
      const signData = `${path}:${expireTimestamp}`;
      const hmac = crypto.createHmac("sha256", this.secret);
      hmac.update(signData);
      const expectedHash = hmac.digest("base64");

      const isValid = hash === expectedHash;
      return {
        valid: isValid,
        reason: isValid ? "valid" : "invalid_signature",
        expireTimestamp,
      };
    } catch (error) {
      return {
        valid: false,
        reason: "malformed_signature",
        error: error.message,
      };
    }
  }

  /**
   * 获取系统设置（带缓存）
   * @param {string} key - 设置键
   * @returns {Promise<string>} 设置值
   */
  async _getSystemSetting(key) {
    const cacheKey = `setting_${key}`;

    // 检查缓存
    if (this.configCache.has(cacheKey)) {
      return this.configCache.get(cacheKey);
    }

    try {
      const setting = await this.systemRepository.getSettingMetadata(key);
      const value = setting ? setting.value : "";

      // 缓存5分钟
      this.configCache.set(cacheKey, value);
      setTimeout(() => this.configCache.delete(cacheKey), 5 * 60 * 1000);

      return value;
    } catch (error) {
      console.error(`获取系统设置 ${key} 失败:`, error);
      return "";
    }
  }

  /**
   * 清除配置缓存
   */
  clearCache() {
    this.configCache.clear();
  }

  /**
   * 获取全局签名配置（用于管理界面）
   * @returns {Promise<Object>} 全局配置
   */
  async getGlobalSignConfig() {
    return await this.systemRepository.getProxySignConfig();
  }

  /**
   * 更新全局签名配置
   * @param {Object} config - 配置对象
   * @returns {Promise<void>}
   */
  async updateGlobalSignConfig(config) {
    await this.systemRepository.updateProxySignConfig(config);

    // 清除缓存
    this.clearCache();
  }
}
