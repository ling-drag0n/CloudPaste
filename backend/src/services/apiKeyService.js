import { generateRandomString } from "../utils/common.js";
import { ApiStatus, DbTables } from "../constants/index.js";
import { ensureRepositoryFactory } from "../utils/repositories.js";
import { Permission, PermissionChecker } from "../constants/permissions.js";
import { ValidationError, ConflictError, NotFoundError, AuthorizationError } from "../http/errors.js";

const resolveRepositoryFactory = ensureRepositoryFactory;

/**
 * 检查并删除过期的API密钥
 * @param {D1Database} db - D1数据库实例
 * @param {Object} key - API密钥对象
 * @returns {Promise<boolean>} 是否已过期并删除
 */
export async function checkAndDeleteExpiredApiKey(db, key, repositoryFactory) {
  if (!key) return true;

  const now = new Date();

  // 检查过期时间
  if (key.expires_at && new Date(key.expires_at) < now) {
    console.log(`API密钥(${key.id})已过期，自动删除`);

    // 使用 ApiKeyRepository 删除过期密钥
    const factory = resolveRepositoryFactory(db, repositoryFactory);
    const apiKeyRepository = factory.getApiKeyRepository();

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
export async function getAllApiKeys(db, repositoryFactory) {
  // 使用 ApiKeyRepository
  const factory = resolveRepositoryFactory(db, repositoryFactory);
  const apiKeyRepository = factory.getApiKeyRepository();

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
export async function createApiKey(db, keyData, repositoryFactory) {
  // 必需参数：名称验证
  if (!keyData.name || keyData.name.trim() === "") {
    throw new ValidationError("密钥名称不能为空");
  }

  // 如果用户提供了自定义密钥，验证其格式
  if (keyData.custom_key) {
    // 验证密钥格式：只允许字母、数字、横杠和下划线
    const keyFormatRegex = /^[a-zA-Z0-9_-]+$/;
    if (!keyFormatRegex.test(keyData.custom_key)) {
      throw new ValidationError("密钥只能包含字母、数字、横杠和下划线");
    }
  }

  // 使用 ApiKeyRepository
  const factory = resolveRepositoryFactory(db, repositoryFactory);
  const apiKeyRepository = factory.getApiKeyRepository();

  // 对于 role='GUEST' 的 Key，保证全局唯一
  const role = keyData.role || "GENERAL";
  if (role === "GUEST") {
    const existingGuests = await apiKeyRepository.findAll({});
    const hasGuest = Array.isArray(existingGuests) && existingGuests.some((k) => (k.role || "GENERAL") === "GUEST");
    if (hasGuest) {
      throw new ConflictError("已存在 GUEST 角色的 API 密钥，系统仅允许一个游客密钥");
    }
  }

  // 检查名称是否已存在
  const nameExists = await apiKeyRepository.existsByName(keyData.name.trim());
  if (nameExists) {
    throw new ConflictError("密钥名称已存在");
  }

  // 生成唯一ID
  const id = crypto.randomUUID();

  // 生成API密钥
  let key;
  let name = keyData.name.trim();

  if (role === "GUEST") {
    // 游客密钥固定使用 guest / guest
    name = "guest";
    key = "guest";
  } else {
    // 非 GUEST：如果有自定义密钥则使用自定义密钥
    key = keyData.custom_key ? keyData.custom_key : generateRandomString(12);
  }

  // 检查密钥是否已存在
  const keyExists = await apiKeyRepository.existsByKey(key);
  if (keyExists) {
    throw new ConflictError("密钥已存在，请重新生成");
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
    throw new ValidationError("无效的过期时间");
  }

  // 直接使用传入的位标志权限
  const permissions = keyData.permissions || 0;

  // 验证权限值的有效性
  if (typeof permissions !== "number" || permissions < 0) {
    throw new ValidationError("权限值必须是非负整数");
  }

  // 准备API密钥数据
  const apiKeyData = {
    id,
    name,
    key,
    permissions, // 位标志权限
    role,
    basic_path: keyData.basic_path || "/",
    // 启用位：默认禁用，需在管理界面显式开启
    is_enable: typeof keyData.is_enable === "number" ? keyData.is_enable : 0,
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
    is_enable: apiKeyData.is_enable,
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
export async function updateApiKey(db, id, updateData, repositoryFactory) {
  // 使用 ApiKeyRepository
  const factory = resolveRepositoryFactory(db, repositoryFactory);
  const apiKeyRepository = factory.getApiKeyRepository();

  // 检查密钥是否存在
  const keyExists = await apiKeyRepository.findById(id);
  if (!keyExists) {
    throw new NotFoundError("密钥不存在");
  }

  // 验证名称
  if (updateData.name && !updateData.name.trim()) {
    throw new ValidationError("密钥名称不能为空");
  }

  // 锁死 GUEST 密钥的 name/role，不允许修改；过期时间字段会在后续统一忽略
  if ((keyExists.role || "GENERAL") === "GUEST") {
    if (updateData.name !== undefined && updateData.name.trim() !== "guest") {
      throw new ConflictError("游客密钥名称固定为 'guest'，不允许修改");
    }
    if (updateData.role !== undefined && updateData.role !== "GUEST") {
      throw new ConflictError("游客密钥角色固定为 'GUEST'，不允许修改");
    }
  }

  // 检查名称是否已存在（排除当前密钥）
  if (updateData.name && updateData.name !== keyExists.name) {
    const nameExists = await apiKeyRepository.existsByName(updateData.name.trim(), id);
    if (nameExists) {
      throw new ConflictError("密钥名称已存在");
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
      throw new ValidationError("无效的过期时间");
    }
    processedUpdateData.expires_at = expiresAt.toISOString();
  }

  // 验证权限值（如果提供）
  if (updateData.permissions !== undefined) {
    if (typeof updateData.permissions !== "number" || updateData.permissions < 0) {
      throw new ValidationError("权限值必须是非负整数");
    }
    processedUpdateData.permissions = updateData.permissions;
  }

  // 清理名称
  if (processedUpdateData.name !== undefined) {
    processedUpdateData.name = processedUpdateData.name.trim();
  }

  if ((keyExists.role || "GENERAL") === "GUEST" && processedUpdateData.expires_at !== undefined) {
    delete processedUpdateData.expires_at;
  }

  // 检查是否有有效的更新字段
  const validFields = ["name", "permissions", "role", "basic_path", "is_enable", "expires_at"];
  const hasValidUpdates = validFields.some((field) => processedUpdateData[field] !== undefined);

  if (!hasValidUpdates) {
    throw new ValidationError("没有提供有效的更新字段");
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
export async function deleteApiKey(db, id, repositoryFactory) {
  // 使用 ApiKeyRepository
  const factory = resolveRepositoryFactory(db, repositoryFactory);
  const apiKeyRepository = factory.getApiKeyRepository();
  const principalStorageAclRepository = factory.getPrincipalStorageAclRepository
    ? factory.getPrincipalStorageAclRepository()
    : null;

  // 检查密钥是否存在
  const keyExists = await apiKeyRepository.findById(id);
  if (!keyExists) {
    throw new NotFoundError("密钥不存在");
  }
  if ((keyExists.role || "GENERAL") === "GUEST") {
    throw new ConflictError("游客密钥不允许删除，请通过禁用或修改权限控制访问");
  }

  // 删除该密钥关联的存储 ACL（subject_type = 'API_KEY'）
  if (principalStorageAclRepository) {
    try {
      await principalStorageAclRepository.replaceBindings("API_KEY", id, []);
    } catch (error) {
      console.warn("删除 API Key 关联的存储 ACL 失败，将继续删除密钥本身：", error);
    }
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
export async function getApiKeyByKey(db, key, repositoryFactory) {
  if (!key) return null;

  // 使用 ApiKeyRepository
  const factory = resolveRepositoryFactory(db, repositoryFactory);
  const apiKeyRepository = factory.getApiKeyRepository();

  return await apiKeyRepository.findByKey(key);
}

/**
 * 根据 API 密钥的 basicPath + 存储 ACL 筛选可访问的挂载点
 * @param {D1Database} db - D1数据库实例
 * @param {string} basicPath - API 密钥的基础路径
 * @param {string|null} subjectType - 主体类型，例如 'API_KEY'，用于存储 ACL（可选）
 * @param {string|null} subjectId - 主体 ID，例如 api_keys.id，用于存储 ACL（可选）
 * @param {import("../repositories").RepositoryFactory} [repositoryFactory] - Repository 工厂（可选）
 * @returns {Promise<Array>} 可访问的挂载点列表
 */
export async function getAccessibleMountsByBasicPath(db, basicPath, subjectType, subjectId, repositoryFactory) {
  // 使用 Repository 获取数据
  const factory = resolveRepositoryFactory(db, repositoryFactory);
  const mountRepository = factory.getMountRepository();
  const storageConfigRepository = factory.getStorageConfigRepository();
  const principalStorageAclRepository = factory.getPrincipalStorageAclRepository
    ? factory.getPrincipalStorageAclRepository()
    : null;

  // 获取所有活跃的挂载点
  const allMounts = await mountRepository.findMany(DbTables.STORAGE_MOUNTS, { is_active: 1 }, { orderBy: "sort_order ASC, name ASC" });

  if (!allMounts || allMounts.length === 0) return [];

  // 为每个挂载点获取存储配置的公开性信息（当前主要针对对象存储）
  const mountsWithStorageInfo = await Promise.all(
    allMounts.map(async (mount) => {
      if (mount.storage_config_id) {
        const storageConfig = await storageConfigRepository.findById(mount.storage_config_id);
        return {
          ...mount,
          is_public: storageConfig?.is_public || 0,
        };
      }
      return {
        ...mount,
        is_public: 1, // 无存储配置引用时默认视为公开
      };
    })
  );

  // 若提供主体信息，则加载该主体的 storage_config 白名单（存储 ACL）
  let allowedConfigIdsSet = null;
  if (principalStorageAclRepository && subjectType && subjectId) {
    try {
      const allowedConfigIds = await principalStorageAclRepository.findConfigIdsBySubject(subjectType, subjectId);
      if (Array.isArray(allowedConfigIds) && allowedConfigIds.length > 0) {
        allowedConfigIdsSet = new Set(allowedConfigIds);
      }
    } catch (error) {
      console.error("加载存储 ACL 失败，拒绝扩大存储访问范围：", error);
      throw new AuthorizationError("无法验证存储访问权限");
    }
  }

  // 根据 basicPath + 存储公开性 + 存储 ACL 筛选可访问的挂载点
  const inaccessibleMounts = []; // 收集无法访问的挂载点信息
  const accessibleMounts = mountsWithStorageInfo.filter((mount) => {
    // 首先检查存储配置是否“公开可用”
    // 对于对象存储类挂载点，必须使用 is_public = 1 的配置
    if (mount.storage_config_id && mount.is_public !== 1) {
      inaccessibleMounts.push(mount.name);
      return false;
    }

    // 然后检查是否命中主体的存储 ACL 白名单（如果有）
    if (allowedConfigIdsSet && mount.storage_config_id && !allowedConfigIdsSet.has(mount.storage_config_id)) {
      return false;
    }

    // 最后检查路径权限（basicPath 与挂载路径的父子关系）
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
    console.log(
      `API密钥用户无法访问 ${inaccessibleMounts.length} 个非公开存储配置的挂载点: ${inaccessibleMounts.join(", ")}`
    );
  }

  return accessibleMounts;
}

/**
 * 更新API密钥最后使用时间
 * @param {D1Database} db - D1数据库实例
 * @param {string} id - API密钥ID
 * @returns {Promise<void>}
 */
export async function updateApiKeyLastUsed(db, id, repositoryFactory) {
  // 使用 ApiKeyRepository
  const factory = resolveRepositoryFactory(db, repositoryFactory);
  const apiKeyRepository = factory.getApiKeyRepository();

  await apiKeyRepository.updateLastUsed(id);
}
