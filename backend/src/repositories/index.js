/**
 * Repository层统一导出文件
 * 提供所有Repository类的统一入口
 */

export { BaseRepository } from "./BaseRepository.js";
export { FileRepository } from "./FileRepository.js";
export { MountRepository } from "./MountRepository.js";
export { S3ConfigRepository } from "./S3ConfigRepository.js";
export { AdminRepository } from "./AdminRepository.js";
export { ApiKeyRepository } from "./ApiKeyRepository.js";
export { PasteRepository } from "./PasteRepository.js";
export { SystemRepository } from "./SystemRepository.js";

// 导入所有Repository类用于工厂类
import { BaseRepository } from "./BaseRepository.js";
import { FileRepository } from "./FileRepository.js";
import { MountRepository } from "./MountRepository.js";
import { S3ConfigRepository } from "./S3ConfigRepository.js";
import { AdminRepository } from "./AdminRepository.js";
import { ApiKeyRepository } from "./ApiKeyRepository.js";
import { PasteRepository } from "./PasteRepository.js";
import { SystemRepository } from "./SystemRepository.js";

/**
 * Repository工厂类
 * 用于创建和管理Repository实例
 */
export class RepositoryFactory {
  /**
   * 构造函数
   * @param {D1Database} db - 数据库实例
   */
  constructor(db) {
    this.db = db;
    this._repositories = new Map();
  }

  /**
   * 获取FileRepository实例
   * @returns {FileRepository} FileRepository实例
   */
  getFileRepository() {
    if (!this._repositories.has("file")) {
      this._repositories.set("file", new FileRepository(this.db));
    }
    return this._repositories.get("file");
  }

  /**
   * 获取MountRepository实例
   * @returns {MountRepository} MountRepository实例
   */
  getMountRepository() {
    if (!this._repositories.has("mount")) {
      this._repositories.set("mount", new MountRepository(this.db));
    }
    return this._repositories.get("mount");
  }

  /**
   * 获取S3ConfigRepository实例
   * @returns {S3ConfigRepository} S3ConfigRepository实例
   */
  getS3ConfigRepository() {
    if (!this._repositories.has("s3config")) {
      this._repositories.set("s3config", new S3ConfigRepository(this.db));
    }
    return this._repositories.get("s3config");
  }

  /**
   * 获取AdminRepository实例
   * @returns {AdminRepository} AdminRepository实例
   */
  getAdminRepository() {
    if (!this._repositories.has("admin")) {
      this._repositories.set("admin", new AdminRepository(this.db));
    }
    return this._repositories.get("admin");
  }

  /**
   * 获取ApiKeyRepository实例
   * @returns {ApiKeyRepository} ApiKeyRepository实例
   */
  getApiKeyRepository() {
    if (!this._repositories.has("apikey")) {
      this._repositories.set("apikey", new ApiKeyRepository(this.db));
    }
    return this._repositories.get("apikey");
  }

  /**
   * 获取PasteRepository实例
   * @returns {PasteRepository} PasteRepository实例
   */
  getPasteRepository() {
    if (!this._repositories.has("paste")) {
      this._repositories.set("paste", new PasteRepository(this.db));
    }
    return this._repositories.get("paste");
  }

  /**
   * 获取SystemRepository实例
   * @returns {SystemRepository} SystemRepository实例
   */
  getSystemRepository() {
    if (!this._repositories.has("system")) {
      this._repositories.set("system", new SystemRepository(this.db));
    }
    return this._repositories.get("system");
  }

  /**
   * 清理所有Repository实例缓存
   */
  clearCache() {
    this._repositories.clear();
  }

  /**
   * 获取所有Repository实例
   * @returns {Object} 包含所有Repository实例的对象
   */
  getAllRepositories() {
    return {
      file: this.getFileRepository(),
      mount: this.getMountRepository(),
      s3Config: this.getS3ConfigRepository(),
      admin: this.getAdminRepository(),
      apiKey: this.getApiKeyRepository(),
      paste: this.getPasteRepository(),
      system: this.getSystemRepository(),
    };
  }
}
