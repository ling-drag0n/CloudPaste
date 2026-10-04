import crypto from "crypto";
import { ensureRepositoryFactory } from "../utils/repositories.js";
import { timingSafeEqualStrings } from "../utils/fileEncryption.js";

/**
 * 代理签名服务
 * 支持两层签名策略：
 * - 全局签名所有：proxy_sign_all = true 时，所有代理流量都启用签名（与挂载配置无关）
 * - 挂载级签名：enable_sign = true 且挂载开启 web_proxy 时，对该挂载下的代理流量启用签名
 */
export class ProxySignatureService {
  constructor(db, encryptionSecret, repositoryFactory = null) {
    this.db = db;
    this.secret = encryptionSecret;
    this.configCache = new Map(); // 配置缓存

    const factory = ensureRepositoryFactory(db, repositoryFactory);
    this.systemRepository = factory.getSystemRepository();
  }

  /**
   * 是否启用了“全局签名所有”
   * - 这是“强制模式”：一旦开启，就认为所有代理签名策略都应以全局配置为准
   * @returns {Promise<boolean>}
   */
  async isGlobalSignAllEnabled() {
    const signAll = await this._getSystemSetting("proxy_sign_all");
    return signAll === "true";
  }

  /**
   * 判断挂载点是否需要签名
   * @param {Object} mount - 挂载点配置
   * @returns {Promise<Object>} 签名需求结果
   */
  async needsSignature(mount) {
    // 1. 检查全局"签名所有"设置
    if (await this.isGlobalSignAllEnabled()) {
      return {
        required: true,
        reason: "sign_all_enabled",
        level: "global",
        description: "全局签名所有已启用",
      };
    }

    // 2. 挂载信息缺失时，不启用挂载级签名（仅全局策略生效）
    if (!mount || typeof mount !== "object") {
      return {
        required: false,
        reason: "mount_not_provided",
        level: "none",
        description: "未提供挂载配置，仅全局签名策略有效",
      };
    }

    // 3. 仅在挂载开启 web_proxy 时才允许挂载级签名生效
    const webProxyEnabled = mount.web_proxy === 1 || mount.web_proxy === true;
    if (!webProxyEnabled) {
      return {
        required: false,
        reason: "web_proxy_disabled",
        level: "none",
        description: "挂载未开启 web_proxy，忽略挂载级签名配置",
      };
    }

    // 4. 检查挂载级别的签名设置
    if (mount.enable_sign === 1 || mount.enable_sign === true) {
      return {
        required: true,
        reason: "storage_sign_enabled",
        level: "storage",
        description: "存储启用签名",
      };
    }

    // 5. 不需要签名
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
   * @param {{ signAllEnabled?: boolean }} [options] - 可选：外部可传入 signAllEnabled 避免重复读配置
   * @returns {Promise<number>} 过期时间（秒），0表示永不过期
   */
  async getSignatureExpiration(mount, options = {}) {
    const signAllEnabled = typeof options.signAllEnabled === "boolean" ? options.signAllEnabled : await this.isGlobalSignAllEnabled();

    // 强制模式：全局 sign_all 开启时，过期时间也应统一由全局 proxy_sign_expires 决定（不允许挂载覆盖）
    if (signAllEnabled) {
      const globalExpires = await this._getSystemSetting("proxy_sign_expires");
      return parseInt(globalExpires) || 0;
    }

    // 非强制模式：挂载点可覆盖过期时间（NULL 表示使用全局设置）
    if (mount && mount.sign_expires !== null && mount.sign_expires !== undefined) {
      return mount.sign_expires;
    }

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
    // 强制模式：全局 sign_all 开启时，过期时间统一以全局为准
    const signAllEnabled = await this.isGlobalSignAllEnabled();
    const configuredExpiresIn = await this.getSignatureExpiration(mount, { signAllEnabled });

    let expiresIn = configuredExpiresIn;
    if (options.expiresIn !== undefined && options.expiresIn !== null) {
      const requested = Number(options.expiresIn);
      if (!Number.isNaN(requested) && requested >= 0) {
        if (!signAllEnabled) {
          expiresIn = requested;
        } else if (configuredExpiresIn === 0) {
          // 全局永不过期时：允许按需生成短期签名
          expiresIn = requested;
        } else {
          // 全局有上限时：不允许超过全局上限
          expiresIn = Math.min(requested, configuredExpiresIn);
        }
      }
    }

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
  verifyStorageSignature(path, signature, options = {}) {
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

      // 常量时间比较，避免时序侧信道
      const isValid = timingSafeEqualStrings(hash, expectedHash);
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
