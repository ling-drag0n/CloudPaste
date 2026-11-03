/**
 * Repository层统一导出文件
 * 提供所有Repository类的统一入口
 */

export { BaseRepository } from "./BaseRepository.js";
export { FileRepository } from "./FileRepository.js";
export { MountRepository } from "./MountRepository.js";
export { StorageConfigRepository } from "./StorageConfigRepository.js";
export { AdminRepository } from "./AdminRepository.js";
export { ApiKeyRepository } from "./ApiKeyRepository.js";
export { PasteRepository } from "./PasteRepository.js";
export { PasteTagRepository } from "./PasteTagRepository.js";
export { SystemRepository } from "./SystemRepository.js";
export { PrincipalStorageAclRepository } from "./PrincipalStorageAclRepository.js";
export { FsMetaRepository } from "./FsMetaRepository.js";
export { UploadPartsRepository } from "./UploadPartsRepository.js";
export { VfsNodesRepository } from "./VfsNodesRepository.js";
export { MetricsCacheRepository } from "./MetricsCacheRepository.js";

// 导入所有Repository类用于工厂类
import { BaseRepository } from "./BaseRepository.js";
import { FileRepository } from "./FileRepository.js";
import { MountRepository } from "./MountRepository.js";
import { StorageConfigRepository } from "./StorageConfigRepository.js";
import { AdminRepository } from "./AdminRepository.js";
import { ApiKeyRepository } from "./ApiKeyRepository.js";
import { PasteRepository } from "./PasteRepository.js";
import { PasteTagRepository } from "./PasteTagRepository.js";
import { SystemRepository } from "./SystemRepository.js";
import { PrincipalStorageAclRepository } from "./PrincipalStorageAclRepository.js";
import { FsMetaRepository } from "./FsMetaRepository.js";
import { UploadPartsRepository } from "./UploadPartsRepository.js";
import { VfsNodesRepository } from "./VfsNodesRepository.js";
import { MetricsCacheRepository } from "./MetricsCacheRepository.js";
import { createDbRuntime } from "../db/runtime.js";

/**
 * Repository工厂类
 * 用于创建和管理Repository实例
 */
export class RepositoryFactory {
  /**
   * 构造函数
   * @param {D1Database} db - 数据库实例
   * @param {{ env?: any, providerName?: string }} [options]
   */
  constructor(db, options = {}) {
    const { env = {}, providerName = null } = options || {};
    const runtime = createDbRuntime({ db, env, providerName });
    this.db = runtime.db;
    this.dialect = runtime.dialect;
    this.providerName = runtime.providerName;
    this._repositories = new Map();
  }

  /**
   * 获取FileRepository实例
   * @returns {FileRepository} FileRepository实例
   */
  getFileRepository() {
    if (!this._repositories.has("file")) {
      this._repositories.set("file", new FileRepository(this.db, this.dialect));
    }
    return this._repositories.get("file");
  }

  /**
   * 获取MountRepository实例
   * @returns {MountRepository} MountRepository实例
   */
  getMountRepository() {
    if (!this._repositories.has("mount")) {
      this._repositories.set("mount", new MountRepository(this.db, this.dialect));
    }
    return this._repositories.get("mount");
  }

  /**
   * 获取StorageConfigRepository实例（通用）
   * @returns {StorageConfigRepository}
   */
  getStorageConfigRepository() {
    if (!this._repositories.has("storageconfig")) {
      this._repositories.set("storageconfig", new StorageConfigRepository(this.db, this.dialect));
    }
    return this._repositories.get("storageconfig");
  }

  /**
   * 获取AdminRepository实例
   * @returns {AdminRepository} AdminRepository实例
   */
  getAdminRepository() {
    if (!this._repositories.has("admin")) {
      this._repositories.set("admin", new AdminRepository(this.db, this.dialect));
    }
    return this._repositories.get("admin");
  }

  /**
   * 获取ApiKeyRepository实例
   * @returns {ApiKeyRepository} ApiKeyRepository实例
   */
  getApiKeyRepository() {
    if (!this._repositories.has("apikey")) {
      this._repositories.set("apikey", new ApiKeyRepository(this.db, this.dialect));
    }
    return this._repositories.get("apikey");
  }

  /**
   * 获取PasteRepository实例
   * @returns {PasteRepository} PasteRepository实例
   */
  getPasteRepository() {
    if (!this._repositories.has("paste")) {
      this._repositories.set("paste", new PasteRepository(this.db, this.dialect));
    }
    return this._repositories.get("paste");
  }

  /**
   * 获取PasteTagRepository实例
   * @returns {PasteTagRepository}
   */
  getPasteTagRepository() {
    if (!this._repositories.has("pasteTag")) {
      this._repositories.set("pasteTag", new PasteTagRepository(this.db, this.dialect));
    }
    return this._repositories.get("pasteTag");
  }

  /**
   * 获取SystemRepository实例
   * @returns {SystemRepository} SystemRepository实例
   */
  getSystemRepository() {
    if (!this._repositories.has("system")) {
      this._repositories.set("system", new SystemRepository(this.db, this.dialect));
    }
    return this._repositories.get("system");
  }

  /**
   * 获取 PrincipalStorageAclRepository 实例
   * @returns {PrincipalStorageAclRepository} PrincipalStorageAclRepository 实例
   */
  getPrincipalStorageAclRepository() {
    if (!this._repositories.has("principalStorageAcl")) {
      this._repositories.set("principalStorageAcl", new PrincipalStorageAclRepository(this.db, this.dialect));
    }
    return this._repositories.get("principalStorageAcl");
  }

  /**
   * 获取 UploadPartsRepository 实例
   * @returns {UploadPartsRepository}
   */
  getUploadPartsRepository() {
    if (!this._repositories.has("uploadParts")) {
      this._repositories.set("uploadParts", new UploadPartsRepository(this.db, this.dialect));
    }
    return this._repositories.get("uploadParts");
  }

  /**
   * 获取 VfsNodesRepository 实例
   * @returns {VfsNodesRepository}
   */
  getVfsNodesRepository() {
    if (!this._repositories.has("vfsNodes")) {
      this._repositories.set("vfsNodes", new VfsNodesRepository(this.db, this.dialect));
    }
    return this._repositories.get("vfsNodes");
  }

  /**
   * 获取 MetricsCacheRepository 实例
   * @returns {MetricsCacheRepository}
   */
  getMetricsCacheRepository() {
    if (!this._repositories.has("metricsCache")) {
      this._repositories.set("metricsCache", new MetricsCacheRepository(this.db, this.dialect));
    }
    return this._repositories.get("metricsCache");
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
      storageConfig: this.getStorageConfigRepository(),
      admin: this.getAdminRepository(),
      apiKey: this.getApiKeyRepository(),
      paste: this.getPasteRepository(),
      pasteTag: this.getPasteTagRepository(),
      system: this.getSystemRepository(),
      principalStorageAcl: this.getPrincipalStorageAclRepository(),
      fsMeta: this.getFsMetaRepository(),
      uploadParts: this.getUploadPartsRepository(),
      vfsNodes: this.getVfsNodesRepository(),
      metricsCache: this.getMetricsCacheRepository(),
    };
  }

  /**
   * 获取 FsMetaRepository 实例
   * @returns {FsMetaRepository}
   */
  getFsMetaRepository() {
    if (!this._repositories.has("fsMeta")) {
      this._repositories.set("fsMeta", new FsMetaRepository(this.db, this.dialect));
    }
    return this._repositories.get("fsMeta");
  }
}
