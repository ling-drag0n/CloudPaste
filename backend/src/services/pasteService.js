import { ApiStatus } from "../constants/index.js";
import { generateRandomString, createErrorResponse, validateSlugFormat } from "../utils/common.js";
import { hashPassword, verifyPassword } from "../utils/crypto.js";
import { AppError, ValidationError, ConflictError, NotFoundError, AuthenticationError } from "../http/errors.js";
import { ensureRepositoryFactory } from "../utils/repositories.js";
import { hydratePastesWithTags, validatePasteTagIds } from "./pasteTagService.js";

const resolveRepositoryFactory = ensureRepositoryFactory;

/**
 * 生成唯一的文本分享短链接slug
 * @param {D1Database} db - D1数据库实例
 * @param {string} customSlug - 自定义短链接
 * @returns {Promise<string>} 生成的唯一slug
 */
export async function generateUniqueSlug(db, customSlug = null, repositoryFactory = null) {
  // 使用 PasteRepository
  const factory = resolveRepositoryFactory(db, repositoryFactory);
  const pasteRepository = factory.getPasteRepository();

  if (customSlug) {
    // 添加格式验证：只允许字母、数字、连字符、下划线、点号
    if (!validateSlugFormat(customSlug)) {
      throw new ValidationError("链接后缀格式无效，只允许使用字母、数字、连字符(-)、下划线(_)和点号(.)");
    }

    // 检查自定义slug是否已存在
    const slugExists = await pasteRepository.existsBySlug(customSlug);
    if (!slugExists) {
      return customSlug;
    }
    // 如果自定义slug已存在，抛出特定错误
    throw new ConflictError("链接后缀已被占用，请尝试其他后缀");
  }

  // 生成随机slug
  const attempts = 5;
  for (let i = 0; i < attempts; i++) {
    const slug = generateRandomString(6);

    // 检查随机slug是否已存在
    const slugExists = await pasteRepository.existsBySlug(slug);
    if (!slugExists) {
      return slug;
    }
  }

  throw new AppError("无法生成唯一链接，请稍后再试", { status: ApiStatus.INTERNAL_ERROR, code: "INTERNAL_ERROR", expose: false });
}

/**
 * 原子化增加文本分享查看次数并检查状态
 * @param {D1Database} db - D1数据库实例
 * @param {string} pasteId - 文本分享ID
 * @param {number} maxViews - 最大查看次数
 * @returns {Promise<Object>} 包含isDeleted、paste、isLastView、isLastNormalAccess的结果对象
 */
export async function incrementAndCheckPasteViews(db, pasteId, maxViews, repositoryFactory = null) {
  // 使用 PasteRepository
  const factory = resolveRepositoryFactory(db, repositoryFactory);
  const pasteRepository = factory.getPasteRepository();

  // 先获取当前paste信息，检查这是否是最后一次正常访问
  const currentPaste = await pasteRepository.findById(pasteId);
  if (!currentPaste) {
    return { isDeleted: true, paste: null, isLastView: false, isLastNormalAccess: false };
  }

  // 检查这是否是最后一次正常访问（访问后刚好达到限制）
  const isLastNormalAccess = maxViews && maxViews > 0 && currentPaste.views + 1 === maxViews;

  // 原子增加查看次数
  await pasteRepository.incrementViews(pasteId);

  // 重新获取更新后的paste信息
  const updatedPaste = await pasteRepository.findById(pasteId);

  if (!updatedPaste) {
    return { isDeleted: true, paste: null, isLastView: false, isLastNormalAccess: false };
  }

  // 检查是否需要删除
  let isDeleted = false;
  let isLastView = false;

  if (maxViews && maxViews > 0 && updatedPaste.views >= maxViews) {
    console.log(`文本分享(${pasteId})已达到最大查看次数(${maxViews})，自动删除`);
    await pasteRepository.deletePaste(pasteId);
    isDeleted = true;
    isLastView = true;
  }

  return {
    isDeleted,
    paste: updatedPaste,
    isLastView,
    isLastNormalAccess,
  };
}

/**
 * 检查并删除过期的文本分享
 * @param {D1Database} db - D1数据库实例
 * @param {Object} paste - 文本分享对象
 * @returns {Promise<boolean>} 是否已过期并删除
 */
export async function checkAndDeleteExpiredPaste(db, paste, repositoryFactory = null) {
  if (!paste) return false;

  const now = new Date();

  // 使用 PasteRepository
  const factory = resolveRepositoryFactory(db, repositoryFactory);
  const pasteRepository = factory.getPasteRepository();

  // 检查过期时间
  if (paste.expires_at && new Date(paste.expires_at) < now) {
    console.log(`文本分享(${paste.id})已过期，自动删除`);
    await pasteRepository.deletePaste(paste.id);
    return true;
  }

  // 检查最大查看次数
  if (paste.max_views && paste.views >= paste.max_views) {
    console.log(`文本分享(${paste.id})已达到最大查看次数，自动删除`);
    await pasteRepository.deletePaste(paste.id);
    return true;
  }

  return false;
}

/**
 * 检查文本分享是否可访问
 * @param {Object} paste - 文本分享对象
 * @returns {boolean} 是否可访问
 */
export function isPasteAccessible(paste) {
  if (!paste) return false;

  const now = new Date();

  // 检查过期时间
  if (paste.expires_at && new Date(paste.expires_at) < now) {
    return false;
  }

  // 检查最大查看次数
  if (paste.max_views && paste.max_views > 0 && paste.views >= paste.max_views) {
    return false;
  }

  return true;
}

/**
 * 创建新的文本分享
 * @param {D1Database} db - D1数据库实例
 * @param {Object} pasteData - 文本分享数据
 * @param {string} created_by - 创建者标识
 * @returns {Promise<Object>} 创建的文本分享
 */
export async function createPaste(db, pasteData, created_by, repositoryFactory = null) {
  // 必须提供内容
  if (!pasteData.content) {
    throw new ValidationError("内容不能为空");
  }

  // 验证可打开次数不能为负数
  if (pasteData.max_views !== null && pasteData.max_views !== undefined && parseInt(pasteData.max_views) < 0) {
    throw new ValidationError("可打开次数不能为负数");
  }

  // 使用 PasteRepository
  const factory = resolveRepositoryFactory(db, repositoryFactory);
  const pasteRepository = factory.getPasteRepository();

  // 生成唯一slug
  const slug = await generateUniqueSlug(db, pasteData.slug, factory);
  const pasteId = crypto.randomUUID();

  // 处理密码 (如果提供)
  let passwordHash = null;
  if (pasteData.password) {
    passwordHash = await hashPassword(pasteData.password);
  }

  // 创建时间
  const now = new Date();
  const createdAt = now.toISOString();

  // 如果没有提供标题，使用slug作为标题
  const title = pasteData.title || slug;

  // 准备文本分享数据
  const pasteDataForRepo = {
    id: pasteId,
    slug,
    content: pasteData.content,
    title: title,
    remark: pasteData.remark || null,
    password: passwordHash,
    expires_at: pasteData.expires_at || null,
    max_views: pasteData.max_views || null,
    is_public: typeof pasteData.is_public === "boolean" ? (pasteData.is_public ? 1 : 0) : 1,
    created_by: created_by,
    created_at: createdAt,
    updated_at: createdAt,
  };

  // 使用 Repository 创建文本分享（包含密码处理）
  await pasteRepository.createPasteWithPassword(pasteDataForRepo, pasteData.password);

  // 返回创建结果
  return {
    id: pasteId,
    slug,
    title: title,
    remark: pasteData.remark,
    expires_at: pasteData.expires_at,
    max_views: pasteData.max_views,
    is_public: typeof pasteData.is_public === "boolean" ? pasteData.is_public : true,
    hasPassword: !!passwordHash,
    created_at: createdAt,
  };
}

/**
 * 获取文本分享
 * @param {D1Database} db - D1数据库实例
 * @param {string} slug - 唯一标识
 * @returns {Promise<Object>} 文本分享
 */
export async function getPasteBySlug(db, slug, repositoryFactory = null) {
  // 使用 PasteRepository
  const factory = resolveRepositoryFactory(db, repositoryFactory);
  const pasteRepository = factory.getPasteRepository();

  // 查询文本分享
  const paste = await pasteRepository.findBySlug(slug);

  // 如果不存在则返回404
  if (!paste) {
    throw new NotFoundError("文本分享不存在或已过期");
  }

  // 添加 has_password 字段
  const pasteWithPasswordFlag = {
    ...paste,
    has_password: !!paste.password,
  };

  // 检查是否过期并删除
  if (await checkAndDeleteExpiredPaste(db, pasteWithPasswordFlag)) {
    throw new AppError("文本分享已过期或超过最大查看次数", { status: ApiStatus.GONE, code: "PASTE_GONE", expose: true });
  }

  return pasteWithPasswordFlag;
}

/**
 * 验证文本密码
 * @param {D1Database} db - D1数据库实例
 * @param {string} slug - 唯一标识
 * @param {string} password - 密码
 * @param {boolean} incrementViews - 是否增加查看次数，默认为true
 * @returns {Promise<Object>} 文本分享
 */
export async function verifyPastePassword(db, slug, password, incrementViews = true, repositoryFactory = null) {
  // 使用 PasteRepository
  const factory = resolveRepositoryFactory(db, repositoryFactory);
  const pasteRepository = factory.getPasteRepository();

  // 查询文本分享
  const paste = await pasteRepository.findBySlug(slug);

  // 如果不存在则返回404
  if (!paste) {
    throw new NotFoundError("文本分享不存在");
  }

  // 添加 has_password 字段用于过期检查
  const pasteWithPasswordFlag = {
    ...paste,
    has_password: !!paste.password,
  };

  // 检查是否过期并删除
  if (await checkAndDeleteExpiredPaste(db, pasteWithPasswordFlag)) {
    throw new AppError("文本分享已过期或超过最大查看次数", { status: ApiStatus.GONE, code: "PASTE_GONE", expose: true });
  }

  // 如果没有密码
  if (!paste.password) {
    throw new ValidationError("此文本分享无需密码");
  }

  // 验证密码
  const isValid = await verifyPassword(password, paste.password);
  if (!isValid) {
    throw new AuthenticationError("密码错误");
  }

  // 查询明文密码
  const plainPassword = await pasteRepository.findPasswordByPasteId(paste.id);

  // 增加查看次数（如果需要）
  if (incrementViews) {
    const result = await incrementAndCheckPasteViews(db, paste.id, paste.max_views);
    // 如果文本被删除，抛出错误
    if (result.isDeleted) {
      throw new AppError("文本分享已达到最大查看次数", { status: ApiStatus.GONE, code: "PASTE_GONE", expose: true });
    }
  }

  // 返回包含明文密码的完整paste对象
  return {
    ...paste,
    plain_password: plainPassword,
  };
}

/**
 * 获取所有文本分享列表（管理员用）
 * @param {D1Database} db - D1数据库实例
 * @param {number} page - 页码（兼容性参数）
 * @param {number} limit - 每页条数
 * @param {string} created_by - 创建者筛选
 * @param {string} search - 搜索关键词
 * @param {number} offset - 偏移量（优先使用）
 * @returns {Promise<Object>} 分页结果
 */
export async function getAllPastes(db, page = 1, limit = 10, created_by = null, search = null, offset = null, repositoryFactory = null, filters = {}) {
  // 使用 Repository
  const factory = resolveRepositoryFactory(db, repositoryFactory);
  const pasteRepository = factory.getPasteRepository();
  const apiKeyRepository = factory.getApiKeyRepository();

  // 使用 PasteRepository 获取管理员列表数据，支持offset模式
  const pasteData = await pasteRepository.findAllForAdmin({
    page,
    limit,
    offset,
    created_by,
    search,
    tagIds: filters.tagIds,
    untagged: filters.untagged,
  });

  // 处理查询结果，为API密钥创建者添加密钥名称
  let results = pasteData.results;
  // 收集所有需要查询名称的密钥ID
  const apiKeyIds = results.filter((paste) => paste.created_by && paste.created_by.startsWith("apikey:")).map((paste) => paste.created_by.substring(7));

  // 如果有需要查询名称的密钥
  if (apiKeyIds.length > 0) {
    // 使用Set去重
    const uniqueKeyIds = [...new Set(apiKeyIds)];
    const keyNamesMap = new Map();

    // 为每个唯一的密钥ID查询名称
    for (const keyId of uniqueKeyIds) {
      const keyInfo = await apiKeyRepository.findById(keyId);
      if (keyInfo) {
        keyNamesMap.set(keyId, keyInfo.name);
      }
    }

    // 为每个结果添加key_name字段
    results = results.map((paste) => {
      if (paste.created_by && paste.created_by.startsWith("apikey:")) {
        const keyId = paste.created_by.substring(7);
        const keyName = keyNamesMap.get(keyId);
        if (keyName) {
          return { ...paste, key_name: keyName };
        }
      }
      return paste;
    });
  }

  results = await hydratePastesWithTags(db, results, factory);

  return {
    results,
    pagination: pasteData.pagination,
  };
}

/**
 * 获取用户文本列表
 * @param {D1Database} db - D1数据库实例
 * @param {string} apiKeyId - API密钥ID
 * @param {number} limit - 每页条数
 * @param {number} offset - 偏移量
 * @param {string} search - 搜索关键词
 * @returns {Promise<Object>} 分页结果
 */
export async function getUserPastes(db, apiKeyId, limit = 30, offset = 0, search = null, repositoryFactory = null, filters = {}) {
  // 使用 Repository
  const factory = resolveRepositoryFactory(db, repositoryFactory);
  const pasteRepository = factory.getPasteRepository();
  const apiKeyRepository = factory.getApiKeyRepository();

  // 使用 PasteRepository 获取用户文本列表
  const created_by = `apikey:${apiKeyId}`;
  const pasteData = await pasteRepository.findByCreatorWithPagination(created_by, {
    limit,
    offset,
    search,
    tagIds: filters.tagIds,
    untagged: filters.untagged,
  });

  // 如果有created_by字段并且以apikey:开头，查询密钥名称
  let results = pasteData.results;

  // 收集所有需要查询名称的密钥ID
  const apiKeyIds = results.filter((paste) => paste.created_by && paste.created_by.startsWith("apikey:")).map((paste) => paste.created_by.substring(7));

  // 如果有需要查询名称的密钥
  if (apiKeyIds.length > 0) {
    // 使用Set去重
    const uniqueKeyIds = [...new Set(apiKeyIds)];
    const keyNamesMap = new Map();

    // 为每个唯一的密钥ID查询名称
    for (const keyId of uniqueKeyIds) {
      const keyInfo = await apiKeyRepository.findById(keyId);
      if (keyInfo) {
        keyNamesMap.set(keyId, keyInfo.name);
      }
    }

    // 为每个结果添加key_name字段
    results = results.map((paste) => {
      if (paste.created_by && paste.created_by.startsWith("apikey:")) {
        const keyId = paste.created_by.substring(7);
        const keyName = keyNamesMap.get(keyId);
        if (keyName) {
          return { ...paste, key_name: keyName };
        }
      }
      return paste;
    });
  }

  results = await hydratePastesWithTags(db, results, factory);

  return {
    results,
    pagination: pasteData.pagination,
  };
}

/**
 * 获取单个文本详情（管理员用）
 * @param {D1Database} db - D1数据库实例
 * @param {string} id - 文本ID
 * @returns {Promise<Object>} 文本详情
 */
export async function getPasteById(db, id, repositoryFactory = null) {
  // 使用 Repository
  const factory = resolveRepositoryFactory(db, repositoryFactory);
  const pasteRepository = factory.getPasteRepository();
  const apiKeyRepository = factory.getApiKeyRepository();

  // 查询文本分享记录（包含密码信息）
  const paste = await pasteRepository.findByIdWithPassword(id, true);

  if (!paste) {
    throw new NotFoundError("文本分享不存在");
  }

  // 添加 has_password 字段
  const result = {
    ...paste,
    has_password: !!paste.password,
  };

  // 如果文本由API密钥创建，获取密钥名称
  if (paste.created_by && paste.created_by.startsWith("apikey:")) {
    const keyId = paste.created_by.substring(7);
    const keyInfo = await apiKeyRepository.findById(keyId);

    if (keyInfo) {
      result.key_name = keyInfo.name;
    }
  }

  const [hydrated] = await hydratePastesWithTags(db, [result], factory);
  return hydrated;
}

/**
 * 删除文本分享
 * @param {D1Database} db - D1数据库实例
 * @param {string} id - 文本ID
 * @returns {Promise<void>}
 */
export async function deletePaste(db, id, repositoryFactory = null) {
  // 使用 PasteRepository
  const factory = resolveRepositoryFactory(db, repositoryFactory);
  const pasteRepository = factory.getPasteRepository();

  // 检查分享是否存在
  const paste = await pasteRepository.findById(id);

  if (!paste) {
    throw new NotFoundError("文本分享不存在");
  }

  // 删除分享
  await pasteRepository.deletePaste(id);
}

/**
 * 批量删除文本分享
 * @param {D1Database} db - D1数据库实例
 * @param {Array<string>} ids - 文本ID数组
 * @param {boolean} clearExpired - 是否清理过期内容
 * @returns {Promise<number>} 删除的数量
 */
export async function batchDeletePastes(db, ids, clearExpired = false, repositoryFactory = null) {
  // 使用 PasteRepository
  const factory = resolveRepositoryFactory(db, repositoryFactory);
  const pasteRepository = factory.getPasteRepository();

  // 如果指定了清理过期内容
  if (clearExpired) {
    const result = await pasteRepository.deleteExpired();
    return result.deletedCount;
  }

  // 否则按照指定的ID删除
  if (!ids || !Array.isArray(ids) || ids.length === 0) {
    throw new ValidationError("请提供有效的ID数组");
  }

  // 执行批量删除
  const result = await pasteRepository.batchDelete(ids);
  return result.deletedCount;
}

/**
 * 批量删除用户的文本分享
 * @param {D1Database} db - D1数据库实例
 * @param {Array<string>} ids - 文本ID数组
 * @param {string} apiKeyId - API密钥ID
 * @returns {Promise<number>} 删除的数量
 */
export async function batchDeleteUserPastes(db, ids, apiKeyId, repositoryFactory = null) {
  // 验证请求数据
  if (!ids || !Array.isArray(ids) || ids.length === 0) {
    throw new ValidationError("请提供有效的ID数组");
  }

  // 使用 PasteRepository
  const factory = resolveRepositoryFactory(db, repositoryFactory);
  const pasteRepository = factory.getPasteRepository();

  // 构建创建者标识
  const created_by = `apikey:${apiKeyId}`;

  // 执行批量删除（只删除属于该API密钥用户的文本）
  const result = await pasteRepository.batchDeleteByCreator(ids, created_by);

  return result.deletedCount;
}

/**
 * 更新文本分享
 * @param {D1Database} db - D1数据库实例
 * @param {string} slug - 唯一标识
 * @param {Object} updateData - 更新数据
 * @param {string} created_by - 创建者标识（可选，用于权限检查）
 * @returns {Promise<Object>} 更新后的信息
 */
export async function updatePaste(db, slug, updateData, created_by = null, repositoryFactory = null) {
  // 使用 Repository
  const factory = resolveRepositoryFactory(db, repositoryFactory);
  const pasteRepository = factory.getPasteRepository();
  const hasTagPayload = Object.prototype.hasOwnProperty.call(updateData || {}, "tag_ids");
  const tagIds = hasTagPayload ? await validatePasteTagIds(db, updateData.tag_ids, factory) : null;

  // 检查分享是否存在
  const paste = await pasteRepository.findBySlugForUpdate(slug, created_by);

  if (!paste) {
    throw new NotFoundError("文本分享不存在或无权修改");
  }

  // 检查是否过期
  if (await checkAndDeleteExpiredPaste(db, paste, factory)) {
    throw new AppError("文本分享已过期或超过最大查看次数，无法修改", { status: ApiStatus.GONE, code: "PASTE_GONE", expose: true });
  }

  // 验证内容
  if (!updateData.content) {
    throw new ValidationError("内容不能为空");
  }

  // 验证可打开次数
  if (updateData.max_views !== null && updateData.max_views !== undefined && parseInt(updateData.max_views) < 0) {
    throw new ValidationError("可打开次数不能为负数");
  }

  // 处理密码更新
  let passwordHash = null;
  let clearPassword = false;
  let newPlainPassword = null;

  if (updateData.password) {
    // 如果提供了新密码，则更新
    passwordHash = await hashPassword(updateData.password);
    newPlainPassword = updateData.password;
  } else if (updateData.clearPassword) {
    // 如果指定了清除密码
    clearPassword = true;
  }

  // 处理slug更新
  let newSlug = paste.slug; // 默认保持原slug不变

  // 如果提供了新的slug，则生成唯一slug
  if (updateData.newSlug !== undefined) {
    try {
      // 如果newSlug为空或null，则自动生成随机slug
      newSlug = await generateUniqueSlug(db, updateData.newSlug || null, factory);
    } catch (error) {
      // 如果slug已被占用，返回409冲突错误
      if (error.message.includes("链接后缀已被占用")) {
        throw new ConflictError(error.message);
      }
      throw error;
    }
  }

  // 简化重置策略：调用更新接口就重置views为0
  let resetViews = true;
  let newViewsValue = 0;
  console.log(`文本分享(${paste.id})已更新，重置访问次数为0`);

  // 如果标题为空，使用新slug作为标题
  const finalTitle = updateData.title || newSlug;

  // 准备更新数据
  const updateDataForRepo = {
    title: finalTitle,
    content: updateData.content,
    remark: updateData.remark,
    expires_at: updateData.expires_at,
    max_views: updateData.max_views,
    is_public: updateData.is_public,
  };

  // 准备更新选项
  const updateOptions = {
    newSlug: newSlug !== paste.slug ? newSlug : null,
    passwordHash,
    clearPassword,
    resetViews,
    newViewsValue, // 传递新的views值
  };

  // 使用 Repository 执行复杂更新
  await pasteRepository.updatePasteComplex(paste.id, updateDataForRepo, updateOptions);

  // 处理密码记录的更新
  if (newPlainPassword) {
    await pasteRepository.upsertPasswordRecord(paste.id, newPlainPassword);
  } else if (clearPassword) {
    await pasteRepository.deletePasswordRecord(paste.id);
  }

  // 标签在文本和密码更新完成后单独保存
  if (hasTagPayload) {
    await factory.getPasteTagRepository().replacePasteTags(paste.id, tagIds);
  }

  // 返回更新结果
  const result = {
    id: paste.id,
    slug: newSlug, // 返回更新后的slug（可能已更改）
  };
  if (hasTagPayload) {
    const tagMap = await factory.getPasteTagRepository().getTagsForPasteIds([paste.id]);
    result.tags = tagMap.get(paste.id) || [];
  }
  return result;
}
