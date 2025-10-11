import { generateRandomString, createErrorResponse } from "../utils/common.js";
import { ApiStatus, DbTables } from "../constants/index.js";
import { RepositoryFactory } from "../repositories/index.js";
import { Permission, PermissionChecker } from "../constants/permissions.js";

/**
 * 检查并删除过期的API密钥
 * @param {D1Database} db - D1数据库实例
 * @param {Object} key - API密钥对象
 * @returns {Promise<boolean>} 是否已过期并删除
 */
export async function checkAndDeleteExpiredApiKey(db, key) {
  if (!key) return true;

  const now = new Date();

  // 检查过期时间
  if (key.expires_at && new Date(key.expires_at) < now) {
    console.log(`API密钥(${key.id})已过期，自动删除`);

    // 使用 ApiKeyRepository 删除过期密钥
    const repositoryFactory = new RepositoryFactory(db);
    const apiKeyRepository = repositoryFactory.getApiKeyRepository();

    await apiKeyRepository.deleteApiKey(key.id);
    return true;
  }

  return false;
}

/**
 * 获取所有API密钥
 * @param {D1Database} db - D1数据库实例
 * @returns {Promise<Array>} API密钥列表
 */
export async function getAllApiKeys(db) {
  // 使用 ApiKeyRepository
  const repositoryFactory = new RepositoryFactory(db);
  const apiKeyRepository = repositoryFactory.getApiKeyRepository();

  // 先清理过期的API密钥
  await apiKeyRepository.deleteExpired();

  // 获取所有密钥列表
  const keys = await apiKeyRepository.findAll({ orderBy: "created_at DESC" });

  // 为每个密钥添加掩码字段和权限信息
  return keys.map((key) => {
    const permissions = key.permissions || 0;

    return {
      ...key,
      key_masked: key.key.substring(0, 6) + "...",
      permissions, // 位标志权限
      // 权限描述（用于前端显示）
      permission_names: PermissionChecker.getPermissionDescriptions(permissions),
    };
  });
}

/**
 * 创建新的API密钥
 * @param {D1Database} db - D1数据库实例
 * @param {Object} keyData - API密钥数据
 * @returns {Promise<Object>} 创建的API密钥
 */
export async function createApiKey(db, keyData) {
  // 必需参数：名称验证
  if (!keyData.name || keyData.name.trim() === "") {
    throw new Error("密钥名称不能为空");
  }

  // 如果用户提供了自定义密钥，验证其格式
  if (keyData.custom_key) {
    // 验证密钥格式：只允许字母、数字、横杠和下划线
    const keyFormatRegex = /^[a-zA-Z0-9_-]+$/;
    if (!keyFormatRegex.test(keyData.custom_key)) {
      throw new Error("密钥只能包含字母、数字、横杠和下划线");
    }
  }

  // 使用 ApiKeyRepository
  const repositoryFactory = new RepositoryFactory(db);
  const apiKeyRepository = repositoryFactory.getApiKeyRepository();

  // 检查名称是否已存在
  const nameExists = await apiKeyRepository.existsByName(keyData.name.trim());
  if (nameExists) {
    throw new Error("密钥名称已存在");
  }

  // 生成唯一ID
  const id = crypto.randomUUID();

  // 生成API密钥，如果有自定义密钥则使用自定义密钥
  const key = keyData.custom_key ? keyData.custom_key : generateRandomString(12);

  // 检查密钥是否已存在
  const keyExists = await apiKeyRepository.existsByKey(key);
  if (keyExists) {
    throw new Error("密钥已存在，请重新生成");
  }

  // 处理过期时间，默认为1天后
  const now = new Date();
  let expiresAt;

  if (keyData.expires_at === null || keyData.expires_at === "never") {
    // 永不过期 - 使用远未来日期（9999-12-31）
    expiresAt = new Date("9999-12-31T23:59:59Z");
  } else if (keyData.expires_at) {
    expiresAt = new Date(keyData.expires_at);
  } else {
    expiresAt = new Date();
    expiresAt.setDate(now.getDate() + 1); // 默认一天后过期
  }

  // 确保日期是有效的
  if (isNaN(expiresAt.getTime())) {
    throw new Error("无效的过期时间");
  }

  // 直接使用传入的位标志权限
  const permissions = keyData.permissions || 0;

  // 验证权限值的有效性
  if (typeof permissions !== "number" || permissions < 0) {
    throw new Error("权限值必须是非负整数");
  }

  // 准备API密钥数据
  const apiKeyData = {
    id,
    name: keyData.name.trim(),
    key,
    permissions, // 位标志权限
    role: keyData.role || "GENERAL",
    basic_path: keyData.basic_path || "/",
    is_guest: keyData.is_guest || 0,
    expires_at: expiresAt.toISOString(),
  };

  // 使用 Repository 创建密钥
  await apiKeyRepository.createApiKey(apiKeyData);

  // 准备响应数据
  return {
    id,
    name: apiKeyData.name,
    key,
    key_masked: key.substring(0, 6) + "...",
    permissions: apiKeyData.permissions, // 位标志权限
    role: apiKeyData.role,
    basic_path: apiKeyData.basic_path,
    is_guest: apiKeyData.is_guest,
    permission_names: PermissionChecker.getPermissionDescriptions(apiKeyData.permissions),
    created_at: apiKeyData.created_at,
    expires_at: apiKeyData.expires_at,
  };
}

/**
 * 更新API密钥
 * @param {D1Database} db - D1数据库实例
 * @param {string} id - API密钥ID
 * @param {Object} updateData - 更新数据
 * @returns {Promise<void>}
 */
export async function updateApiKey(db, id, updateData) {
  // 使用 ApiKeyRepository
  const repositoryFactory = new RepositoryFactory(db);
  const apiKeyRepository = repositoryFactory.getApiKeyRepository();

  // 检查密钥是否存在
  const keyExists = await apiKeyRepository.findById(id);
  if (!keyExists) {
    throw new Error("密钥不存在");
  }

  // 验证名称
  if (updateData.name && !updateData.name.trim()) {
    throw new Error("密钥名称不能为空");
  }

  // 检查名称是否已存在（排除当前密钥）
  if (updateData.name && updateData.name !== keyExists.name) {
    const nameExists = await apiKeyRepository.existsByName(updateData.name.trim(), id);
    if (nameExists) {
      throw new Error("密钥名称已存在");
    }
  }

  // 处理过期时间
  let processedUpdateData = { ...updateData };

  if (updateData.expires_at === null || updateData.expires_at === "never") {
    // 永不过期 - 使用远未来日期（9999-12-31）
    processedUpdateData.expires_at = new Date("9999-12-31T23:59:59Z").toISOString();
  } else if (updateData.expires_at) {
    const expiresAt = new Date(updateData.expires_at);
    // 确保日期是有效的
    if (isNaN(expiresAt.getTime())) {
      throw new Error("无效的过期时间");
    }
    processedUpdateData.expires_at = expiresAt.toISOString();
  }

  // 验证权限值（如果提供）
  if (updateData.permissions !== undefined) {
    if (typeof updateData.permissions !== "number" || updateData.permissions < 0) {
      throw new Error("权限值必须是非负整数");
    }
    processedUpdateData.permissions = updateData.permissions;
  }

  // 清理名称
  if (processedUpdateData.name !== undefined) {
    processedUpdateData.name = processedUpdateData.name.trim();
  }

  // 检查是否有有效的更新字段
  const validFields = ["name", "permissions", "role", "basic_path", "is_guest", "expires_at"];
  const hasValidUpdates = validFields.some((field) => processedUpdateData[field] !== undefined);

  if (!hasValidUpdates) {
    throw new Error("没有提供有效的更新字段");
  }

  // 使用 Repository 更新密钥
  await apiKeyRepository.updateApiKey(id, processedUpdateData);
}

/**
 * 删除API密钥
 * @param {D1Database} db - D1数据库实例
 * @param {string} id - API密钥ID
 * @returns {Promise<void>}
 */
export async function deleteApiKey(db, id) {
  // 使用 ApiKeyRepository
  const repositoryFactory = new RepositoryFactory(db);
  const apiKeyRepository = repositoryFactory.getApiKeyRepository();

  // 检查密钥是否存在
  const keyExists = await apiKeyRepository.findById(id);
  if (!keyExists) {
    throw new Error("密钥不存在");
  }

  // 删除密钥
  await apiKeyRepository.deleteApiKey(id);
}

/**
 * 获取API密钥信息
 * @param {D1Database} db - D1数据库实例
 * @param {string} key - API密钥
 * @returns {Promise<Object|null>} API密钥信息
 */
export async function getApiKeyByKey(db, key) {
  if (!key) return null;

  // 使用 ApiKeyRepository
  const repositoryFactory = new RepositoryFactory(db);
  const apiKeyRepository = repositoryFactory.getApiKeyRepository();

  return await apiKeyRepository.findByKey(key);
}

/**
 * 根据API密钥的基本路径筛选可访问的挂载点
 * @param {D1Database} db - D1数据库实例
 * @param {string} basicPath - API密钥的基本路径
 * @returns {Promise<Array>} 可访问的挂载点列表
 */
export async function getAccessibleMountsByBasicPath(db, basicPath) {
  // 使用 Repository 获取数据
  const repositoryFactory = new RepositoryFactory(db);
  const mountRepository = repositoryFactory.getMountRepository();
  const s3ConfigRepository = repositoryFactory.getS3ConfigRepository();

  // 获取所有活跃的挂载点
  const allMounts = await mountRepository.findMany(DbTables.STORAGE_MOUNTS, { is_active: 1 }, { orderBy: "sort_order ASC, name ASC" });

  if (!allMounts || allMounts.length === 0) return [];

  // 为每个挂载点获取S3配置信息
  const mountsWithS3Info = await Promise.all(
    allMounts.map(async (mount) => {
      if (mount.storage_type === "S3" && mount.storage_config_id) {
        const s3Config = await s3ConfigRepository.findById(mount.storage_config_id);
        return {
          ...mount,
          is_public: s3Config?.is_public || 0,
        };
      }
      return {
        ...mount,
        is_public: 1, // 非S3类型默认为公开
      };
    })
  );

  // 根据基本路径和S3配置公开性筛选可访问的挂载点
  const inaccessibleMounts = []; // 收集无法访问的挂载点信息
  const accessibleMounts = mountsWithS3Info.filter((mount) => {
    // 首先检查S3配置的公开性
    // 对于S3类型的挂载点，必须使用公开的S3配置
    if (mount.storage_type === "S3" && mount.is_public !== 1) {
      inaccessibleMounts.push(mount.name);
      return false;
    }

    // 然后检查路径权限
    const normalizedBasicPath = basicPath === "/" ? "/" : basicPath.replace(/\/+$/, "");
    const normalizedMountPath = mount.mount_path.replace(/\/+$/, "") || "/";

    // 情况1：基本路径是根路径，允许访问所有公开配置的挂载点
    if (normalizedBasicPath === "/") {
      return true;
    }

    // 情况2：基本路径允许访问挂载点路径（基本路径是挂载点的父级或相同）
    if (normalizedMountPath === normalizedBasicPath || normalizedMountPath.startsWith(normalizedBasicPath + "/")) {
      return true;
    }

    // 情况3：挂载点路径是基本路径的父级（基本路径是挂载点的子目录）
    if (normalizedBasicPath.startsWith(normalizedMountPath + "/")) {
      return true;
    }

    return false;
  });

  // 如果有无法访问的挂载点，统一输出一条日志
  if (inaccessibleMounts.length > 0) {
    console.log(`API密钥用户无法访问 ${inaccessibleMounts.length} 个非公开S3配置的挂载点: ${inaccessibleMounts.join(", ")}`);
  }

  return accessibleMounts;
}

/**
 * 更新API密钥最后使用时间
 * @param {D1Database} db - D1数据库实例
 * @param {string} id - API密钥ID
 * @returns {Promise<void>}
 */
export async function updateApiKeyLastUsed(db, id) {
  // 使用 ApiKeyRepository
  const repositoryFactory = new RepositoryFactory(db);
  const apiKeyRepository = repositoryFactory.getApiKeyRepository();

  await apiKeyRepository.updateLastUsed(id);
}
