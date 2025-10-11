/**
 * 存储挂载配置服务
 */
import { ApiStatus } from "../constants/index.js";
import { HTTPException } from "hono/http-exception";
import { generateUUID } from "../utils/common.js";
import { MountRepository, S3ConfigRepository } from "../repositories/index.js";

/**
 * 挂载点服务类
 * 职责：纯粹的挂载点业务逻辑，通过Repository访问数据
 */
class MountService {
  /**
   * 构造函数
   * @param {D1Database} db - 数据库实例
   */
  constructor(db) {
    this.mountRepository = new MountRepository(db);
    this.s3ConfigRepository = new S3ConfigRepository(db);
  }

  /**
   * 获取管理员的挂载点列表
   * @param {string} adminId - 管理员ID
   * @param {boolean} includeInactive - 是否包含禁用的挂载点
   * @returns {Promise<Array>} 挂载点列表
   */
  async getMountsByAdmin(adminId, includeInactive = false) {
    return await this.mountRepository.findByAdmin(adminId, includeInactive);
  }

  /**
   * 获取所有挂载点列表（管理员专用）
   * @param {boolean} includeInactive - 是否包含禁用的挂载点
   * @returns {Promise<Array>} 所有挂载点列表
   */
  async getAllMounts(includeInactive = true) {
    return await this.mountRepository.findAll(includeInactive);
  }

  /**
   * 验证挂载点数据
   * @param {Object} mountData - 挂载点数据
   * @param {string} creatorId - 创建者ID
   * @throws {HTTPException} 400 - 参数错误
   */
  async validateMountData(mountData, creatorId) {
    // 验证必填字段
    const requiredFields = ["name", "storage_type", "mount_path"];
    for (const field of requiredFields) {
      if (!mountData[field]) {
        throw new HTTPException(ApiStatus.BAD_REQUEST, { message: `缺少必填字段: ${field}` });
      }
    }

    // 验证挂载路径格式
    this.validateMountPath(mountData.mount_path);

    // 检查挂载路径是否已存在
    const existingMount = await this.mountRepository.findByMountPath(mountData.mount_path);
    if (existingMount) {
      throw new HTTPException(ApiStatus.CONFLICT, { message: "挂载路径已被使用" });
    }

    // 如果是S3类型，验证storage_config_id是否存在
    if (mountData.storage_type === "S3" && mountData.storage_config_id) {
      const s3Config = await this.s3ConfigRepository.findById(mountData.storage_config_id);
      if (!s3Config) {
        throw new HTTPException(ApiStatus.BAD_REQUEST, { message: "指定的S3配置不存在" });
      }
      console.log(`创建挂载点: ${mountData.name}, 类型: S3, 使用S3配置ID: ${mountData.storage_config_id}`);
    } else if (mountData.storage_type === "S3" && !mountData.storage_config_id) {
      console.log(`创建S3类型挂载点: ${mountData.name}，但未指定S3配置ID`);
    } else {
      console.log(`创建挂载点: ${mountData.name}, 类型: ${mountData.storage_type}, 路径: ${mountData.mount_path}`);
    }
  }

  /**
   * 验证挂载路径格式
   * @param {string} mountPath - 挂载路径
   * @throws {HTTPException} 400 - 路径格式错误
   */
  validateMountPath(mountPath) {
    if (!mountPath.startsWith("/")) {
      throw new HTTPException(ApiStatus.BAD_REQUEST, { message: "挂载路径必须以 / 开头" });
    }

    if (mountPath.includes("//")) {
      throw new HTTPException(ApiStatus.BAD_REQUEST, { message: "挂载路径不能包含连续的 /" });
    }

    if (mountPath.length > 1 && mountPath.endsWith("/")) {
      throw new HTTPException(ApiStatus.BAD_REQUEST, { message: "挂载路径不能以 / 结尾（根路径除外）" });
    }

    // 检查是否包含非法字符
    const invalidChars = /[<>:"|?*\x00-\x1f]/;
    if (invalidChars.test(mountPath)) {
      throw new HTTPException(ApiStatus.BAD_REQUEST, { message: "挂载路径包含非法字符" });
    }
  }

  /**
   * 创建挂载点
   * @param {Object} mountData - 挂载点数据
   * @param {string} creatorId - 创建者ID
   * @returns {Promise<Object>} 创建的挂载点完整信息
   * @throws {HTTPException} 400/409 - 参数错误或冲突
   */
  async createMount(mountData, creatorId) {
    // 验证挂载点数据
    await this.validateMountData(mountData, creatorId);

    // 生成唯一ID
    const id = generateUUID();

    // 设置默认值
    const isActive = mountData.is_active !== undefined ? mountData.is_active : true;
    const sortOrder = mountData.sort_order || 0;
    const cacheTtl = mountData.cache_ttl || 300;
    const webProxy = mountData.web_proxy || false;
    const webdavPolicy = mountData.webdav_policy || "302_redirect";
    const enableSign = mountData.enable_sign || false;
    const signExpires = mountData.sign_expires !== undefined ? mountData.sign_expires : null;

    // 准备数据
    const createData = {
      id,
      name: mountData.name,
      storage_type: mountData.storage_type,
      storage_config_id: mountData.storage_config_id || null,
      mount_path: mountData.mount_path,
      remark: mountData.remark || null,
      is_active: isActive,
      created_by: creatorId,
      sort_order: sortOrder,
      cache_ttl: cacheTtl,
      web_proxy: webProxy,
      webdav_policy: webdavPolicy,
      enable_sign: enableSign,
      sign_expires: signExpires,
    };

    // 创建挂载点
    await this.mountRepository.createMount(createData);

    // 返回创建的挂载点信息
    return await this.mountRepository.findById(id);
  }

  /**
   * 更新挂载点
   * @param {string} mountId - 挂载点ID
   * @param {Object} updateData - 更新数据
   * @param {string} updaterId - 更新者ID
   * @param {boolean} isAdmin - 是否为管理员操作
   * @returns {Promise<Object>} 更新后的挂载点信息
   * @throws {HTTPException} 404/400/409 - 不存在、参数错误或冲突
   */
  async updateMount(mountId, updateData, updaterId, isAdmin = false) {
    // 检查挂载点是否存在
    const existingMount = await this.mountRepository.findById(mountId);
    if (!existingMount) {
      throw new HTTPException(ApiStatus.NOT_FOUND, { message: "挂载点不存在" });
    }

    // 权限检查：非管理员只能修改自己创建的挂载点
    if (!isAdmin && existingMount.created_by !== updaterId) {
      throw new HTTPException(ApiStatus.FORBIDDEN, { message: "没有权限修改此挂载点" });
    }

    // 如果更新挂载路径，需要验证
    if (updateData.mount_path && updateData.mount_path !== existingMount.mount_path) {
      this.validateMountPath(updateData.mount_path);

      // 检查新路径是否已被其他挂载点使用
      const pathExists = await this.mountRepository.existsByMountPath(updateData.mount_path, mountId);
      if (pathExists) {
        throw new HTTPException(ApiStatus.CONFLICT, { message: "挂载路径已被使用" });
      }
    }

    // 如果更新S3配置，需要验证
    if (updateData.storage_config_id && updateData.storage_config_id !== existingMount.storage_config_id) {
      const s3Config = await this.s3ConfigRepository.findById(updateData.storage_config_id);
      if (!s3Config) {
        throw new HTTPException(ApiStatus.BAD_REQUEST, { message: "指定的S3配置不存在" });
      }
    }

    // 更新挂载点
    await this.mountRepository.updateMount(mountId, updateData);

    // 返回更新后的挂载点信息
    return await this.mountRepository.findById(mountId);
  }

  /**
   * 删除挂载点
   * @param {string} mountId - 挂载点ID
   * @param {string} deleterId - 删除者ID
   * @param {boolean} isAdmin - 是否为管理员操作
   * @returns {Promise<Object>} 删除结果
   * @throws {HTTPException} 404 - 挂载点不存在
   */
  async deleteMount(mountId, deleterId, isAdmin = false) {
    // 检查挂载点是否存在
    const existingMount = await this.mountRepository.findById(mountId);
    if (!existingMount) {
      throw new HTTPException(ApiStatus.NOT_FOUND, { message: "挂载点不存在" });
    }

    // 权限检查：非管理员只能删除自己创建的挂载点
    if (!isAdmin && existingMount.created_by !== deleterId) {
      throw new HTTPException(ApiStatus.FORBIDDEN, { message: "没有权限删除此挂载点" });
    }

    // 删除挂载点
    const result = await this.mountRepository.deleteMount(mountId);

    return {
      success: true,
      message: "挂载点删除成功",
      deletedCount: result.meta?.changes || 0,
    };
  }

  /**
   * 更新挂载点最后使用时间
   * @param {string} mountId - 挂载点ID
   * @returns {Promise<Object>} 更新结果
   */
  async updateMountLastUsed(mountId) {
    return await this.mountRepository.updateLastUsed(mountId);
  }
}

/**
 * 获取管理员的挂载点列表
 * @param {D1Database} db - D1数据库实例
 * @param {string} adminId - 管理员ID
 * @param {boolean} includeInactive - 是否包含禁用的挂载点，默认为false
 * @returns {Promise<Array>} 挂载点列表
 * @throws {Error} 数据库操作错误
 */
export async function getMountsByAdmin(db, adminId, includeInactive = false) {
  const mountService = new MountService(db);
  return await mountService.getMountsByAdmin(adminId, includeInactive);
}

/**
 * 获取所有挂载点列表（管理员专用）
 * @param {D1Database} db - D1数据库实例
 * @param {boolean} includeInactive - 是否包含禁用的挂载点，默认为true（管理员界面需要看到所有挂载点）
 * @returns {Promise<Array>} 所有挂载点列表
 * @throws {Error} 数据库操作错误
 */
export async function getAllMounts(db, includeInactive = true) {
  const mountService = new MountService(db);
  return await mountService.getAllMounts(includeInactive);
}

/**
 * 创建挂载点
 * @param {D1Database} db - D1数据库实例
 * @param {Object} mountData - 挂载点数据
 * @param {string} mountData.name - 挂载点名称
 * @param {string} mountData.storage_type - 存储类型（如'S3'）
 * @param {string} mountData.mount_path - 挂载路径
 * @param {string} [mountData.storage_config_id] - 存储配置ID（对S3类型必需）
 * @param {string} [mountData.remark] - 备注
 * @param {boolean} [mountData.is_active=true] - 是否激活
 * @param {number} [mountData.sort_order=0] - 排序顺序
 * @param {number} [mountData.cache_ttl=300] - 缓存时间（秒）
 * @param {boolean} [mountData.web_proxy=false] - 是否启用网页代理
 * @param {string} [mountData.webdav_policy='302_redirect'] - WebDAV策略
 * @param {boolean} [mountData.enable_sign=false] - 是否启用代理签名
 * @param {number|null} [mountData.sign_expires=null] - 签名过期时间（秒），null表示使用全局设置
 * @param {string} creatorId - 创建者ID
 * @returns {Promise<Object>} 创建的挂载点完整信息
 * @throws {HTTPException} 400 - 参数错误，包括缺少必填字段、路径格式错误等
 * @throws {HTTPException} 409 - 挂载路径已存在冲突
 * @throws {Error} 数据库操作错误
 */
export async function createMount(db, mountData, creatorId) {
  const mountService = new MountService(db);
  return await mountService.createMount(mountData, creatorId);
}

/**
 * 更新挂载点
 * @param {D1Database} db - D1数据库实例
 * @param {string} id - 挂载点ID
 * @param {Object} updateData - 更新数据
 * @param {string} [updateData.name] - 挂载点名称
 * @param {string} [updateData.storage_type] - 存储类型
 * @param {string} [updateData.mount_path] - 挂载路径
 * @param {string|null} [updateData.storage_config_id] - 存储配置ID
 * @param {string} [updateData.remark] - 备注
 * @param {boolean} [updateData.is_active] - 是否激活
 * @param {number} [updateData.sort_order] - 排序顺序
 * @param {number} [updateData.cache_ttl] - 缓存时间（秒）
 * @param {boolean} [updateData.web_proxy] - 是否启用网页代理
 * @param {string} [updateData.webdav_policy] - WebDAV策略
 * @param {boolean} [updateData.enable_sign] - 是否启用代理签名
 * @param {number|null} [updateData.sign_expires] - 签名过期时间（秒），null表示使用全局设置
 * @param {string} creatorId - 创建者ID或管理员ID
 * @param {boolean} isAdmin - 是否为管理员操作，为true时不检查创建者
 * @returns {Promise<void>}
 * @throws {HTTPException} 400 - 参数错误，包括路径格式错误、S3配置不存在等
 * @throws {HTTPException} 404 - 挂载点不存在或无权限修改
 * @throws {HTTPException} 409 - 挂载路径已存在冲突
 * @throws {Error} 数据库操作错误
 */
export async function updateMount(db, id, updateData, creatorId, isAdmin = false) {
  const mountService = new MountService(db);
  return await mountService.updateMount(id, updateData, creatorId, isAdmin);
}

/**
 * 删除挂载点
 * @param {D1Database} db - D1数据库实例
 * @param {string} id - 挂载点ID
 * @param {string} creatorId - 创建者ID或管理员ID
 * @param {boolean} isAdmin - 是否为管理员操作，为true时不检查创建者
 * @returns {Promise<void>}
 * @throws {HTTPException} 404 - 挂载点不存在或无权限删除
 * @throws {Error} 数据库操作错误
 */
export async function deleteMount(db, id, creatorId, isAdmin = false) {
  const mountService = new MountService(db);
  return await mountService.deleteMount(id, creatorId, isAdmin);
}

/**
 * 更新挂载点最后使用时间
 * @param {D1Database} db - D1数据库实例
 * @param {string} id - 挂载点ID
 * @returns {Promise<void>}
 * @throws {Error} 数据库操作错误
 */
export async function updateMountLastUsed(db, id) {
  const mountService = new MountService(db);
  return await mountService.updateMountLastUsed(id);
}
