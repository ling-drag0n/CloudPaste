/**
 * 通用工具函数
 */

/**
 * 生成随机字符串
 * @param {number} length - 字符串长度
 * @returns {string} 随机字符串
 */
export function generateRandomString(length = 8) {
  const chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
  let result = "";
  const randomValues = new Uint8Array(length);
  crypto.getRandomValues(randomValues);
  randomValues.forEach((val) => (result += chars[val % chars.length]));
  return result;
}

/**
 * 统一错误响应工具函数
 * @param {number} statusCode - HTTP状态码
 * @param {string} message - 错误消息
 * @returns {object} 标准错误响应对象
 */
export function createErrorResponse(statusCode, message) {
  return {
    code: statusCode,
    message: message,
    success: false,
    data: null,
  };
}

// getLocalTimeString() 函数已被移除
// 现在所有时间处理都使用 CURRENT_TIMESTAMP 以支持更好的国际化

/**
 * 格式化文件大小
 * @param {number} bytes 文件大小（字节）
 * @returns {string} 格式化后的文件大小
 */
export function formatFileSize(bytes) {
  if (bytes === 0) return "0 B";

  const sizes = ["B", "KB", "MB", "GB", "TB"];
  const i = Math.floor(Math.log(bytes) / Math.log(1024));

  return (bytes / Math.pow(1024, i)).toFixed(2) + " " + sizes[i];
}

/**
 * 处理每周数据，确保有7天的数据
 * @param {Array} data - 包含日期和数量的数据
 * @returns {Array} 处理后的数据
 */
export function processWeeklyData(data) {
  const result = new Array(7).fill(0);

  if (!data || data.length === 0) return result;

  // 获取过去7天的日期
  const dates = [];
  for (let i = 6; i >= 0; i--) {
    const date = new Date();
    date.setDate(date.getDate() - i);
    dates.push(date.toISOString().split("T")[0]); // 格式：YYYY-MM-DD
  }

  // 将数据映射到对应日期
  data.forEach((item) => {
    const itemDate = item.date.split("T")[0]; // 处理可能的时间部分
    const index = dates.indexOf(itemDate);
    if (index !== -1) {
      result[index] = item.count;
    }
  });

  return result;
}

/**
 * 生成通用UUID
 * @returns {string} 生成的UUID，符合RFC4122 v4标准
 */
export function generateUUID() {
  return crypto.randomUUID();
}

/**
 * 生成唯一文件ID
 * @returns {string} 生成的文件ID
 */
export function generateFileId() {
  return crypto.randomUUID();
}

/**
 * 生成唯一的S3配置ID
 * @returns {string} 生成的S3配置ID
 */
export function generateS3ConfigId() {
  return crypto.randomUUID();
}

/**
 * 生成短ID作为文件路径前缀
 * @returns {string} 生成的短ID
 */
export function generateShortId() {
  // 生成6位随机ID
  const charset = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
  let result = "";

  // 使用 crypto.getRandomValues 获取加密安全的随机值
  const randomValues = new Uint8Array(6);
  crypto.getRandomValues(randomValues);

  for (let i = 0; i < 6; i++) {
    result += charset[randomValues[i] % charset.length];
  }

  return result;
}

/**
 * 根据系统设置决定是否使用随机后缀
 * @param {D1Database} db - 数据库实例
 * @returns {Promise<boolean>} 是否使用随机后缀
 */
export async function shouldUseRandomSuffix(db) {
  try {
    // 动态导入避免循环依赖
    const { getSettingMetadata } = await import("../services/systemService.js");

    // 获取文件命名策略设置，默认为覆盖模式
    const setting = await getSettingMetadata(db, "file_naming_strategy");
    const strategy = setting ? setting.value : "overwrite";

    // 返回是否使用随机后缀
    return strategy === "random_suffix";
  } catch (error) {
    console.warn("获取文件命名策略失败，使用默认覆盖模式:", error);
    // 出错时默认使用覆盖模式（不使用随机后缀）
    return false;
  }
}

/**
 * 从文件名中获取文件名和扩展名
 * @param {string} filename - 文件名
 * @returns {Object} 包含文件名和扩展名的对象
 */
export function getFileNameAndExt(filename) {
  const lastDotIndex = filename.lastIndexOf(".");
  if (lastDotIndex > -1) {
    return {
      name: filename.substring(0, lastDotIndex),
      ext: filename.substring(lastDotIndex),
    };
  }
  return {
    name: filename,
    ext: "",
  };
}

/**
 * 生成安全的文件名（移除非法字符）
 * @param {string} fileName - 原始文件名
 * @returns {string} 安全的文件名
 */
export function getSafeFileName(fileName) {
  // 只过滤真正有害的字符：
  // - 控制字符 (\x00-\x1F, \x7F)
  // - 路径分隔符 (/ \)
  // - Windows保留字符 (< > : " | ? *)
  // 保留所有其他Unicode字符，包括中文标点符号
  return fileName.replace(/[<>:"|?*\\/\x00-\x1F\x7F]/g, "_");
}

/**
 * 验证 slug 格式
 * @param {string} slug - 要验证的 slug
 * @returns {boolean} 是否有效
 */
export function validateSlugFormat(slug) {
  if (!slug) return false;
  const slugRegex = /^[a-zA-Z0-9._-]+$/;
  return slugRegex.test(slug);
}

/**
 * 生成唯一的文件slug
 * @param {D1Database} db - D1数据库实例
 * @param {string} customSlug - 自定义slug
 * @param {boolean} override - 是否覆盖已存在的slug
 * @param {Object} overrideContext - 覆盖操作的上下文信息（当override=true时需要）
 * @returns {Promise<string>} 生成的唯一slug
 */
export async function generateUniqueFileSlug(db, customSlug = null, override = false, overrideContext = null) {
  // 动态导入DbTables以避免循环依赖
  const { DbTables } = await import("../constants/index.js");

  // 如果提供了自定义slug，验证其格式并检查是否已存在
  if (customSlug) {
    // 验证slug格式：只允许字母、数字、横杠、下划线和点号
    if (!validateSlugFormat(customSlug)) {
      throw new Error("链接后缀格式无效，只能使用字母、数字、下划线、横杠和点号");
    }

    // 检查slug是否已存在
    const existingFile = await db.prepare(`SELECT * FROM ${DbTables.FILES} WHERE slug = ?`).bind(customSlug).first();

    // 如果存在并且不覆盖，抛出错误
    if (existingFile && !override) {
      throw new Error("链接后缀已被占用，请使用其他链接后缀");
    } else if (existingFile && override) {
      // 处理文件覆盖逻辑
      await handleFileOverride(existingFile, overrideContext);
      console.log(`允许覆盖已存在的链接后缀: ${customSlug}`);
    }

    return customSlug;
  }

  // 生成随机slug (6个字符)
  let attempts = 0;
  const maxAttempts = 10;
  while (attempts < maxAttempts) {
    const randomSlug = generateShortId();

    // 检查是否已存在
    const existingFile = await db.prepare(`SELECT id FROM ${DbTables.FILES} WHERE slug = ?`).bind(randomSlug).first();
    if (!existingFile) {
      return randomSlug;
    }

    attempts++;
  }

  throw new Error("无法生成唯一链接后缀，请稍后再试");
}

/**
 * 处理文件覆盖逻辑的辅助函数
 * @private
 */
async function handleFileOverride(existingFile, overrideContext) {
  if (!overrideContext) {
    throw new Error("覆盖操作需要提供上下文信息");
  }

  const { userIdOrInfo, userType, encryptionSecret, repositoryFactory } = overrideContext;

  console.log(`覆盖模式：删除已存在的文件记录 Slug: ${existingFile.slug}`);

  // 检查当前用户是否为文件创建者
  const currentCreator = userType === "admin" ? userIdOrInfo : `apikey:${userIdOrInfo}`;
  if (existingFile.created_by !== currentCreator) {
    console.log(`覆盖操作被拒绝：用户 ${currentCreator} 尝试覆盖 ${existingFile.created_by} 创建的文件`);
    const { HTTPException } = await import("hono/http-exception");
    const { ApiStatus } = await import("../constants/index.js");
    throw new HTTPException(ApiStatus.FORBIDDEN, {
      message: "您无权覆盖其他用户创建的文件",
    });
  }

  try {
    const fileRepository = repositoryFactory.getFileRepository();

    // 获取S3配置以便删除实际文件（仅对S3存储类型）
    if (existingFile.storage_type === "S3" && existingFile.storage_config_id) {
      const s3ConfigRepository = repositoryFactory.getS3ConfigRepository();
      const s3Config = await s3ConfigRepository.findById(existingFile.storage_config_id);

      if (s3Config) {
        const { deleteFileFromS3 } = await import("../utils/s3Utils.js");
        const deleteResult = await deleteFileFromS3(s3Config, existingFile.storage_path, encryptionSecret);
        if (deleteResult) {
          console.log(`成功从S3删除文件: ${existingFile.storage_path}`);
        } else {
          console.warn(`无法从S3删除文件: ${existingFile.storage_path}，但将继续删除数据库记录`);
        }
      }
    }

    // 删除旧文件的数据库记录
    await fileRepository.deleteFile(existingFile.id);

    // 删除关联的密码记录（如果有）
    await fileRepository.deleteFilePasswordRecord(existingFile.id);

    // 清除与文件相关的缓存（仅对S3存储类型）
    if (existingFile.storage_type === "S3" && existingFile.storage_config_id) {
      const { clearDirectoryCache } = await import("../cache/index.js");
      // 需要传递db参数，从repositoryFactory获取
      const db = repositoryFactory.db || repositoryFactory._db;
      await clearDirectoryCache({ db, s3ConfigId: existingFile.storage_config_id });
    }
  } catch (deleteError) {
    console.error(`删除旧文件记录时出错: ${deleteError.message}`);
    // 继续流程，不中断上传
  }
}
