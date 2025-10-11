/**
 * S3存储配置服务
 */
import { ApiStatus, S3ProviderTypes } from "../constants/index.js";
import { HTTPException } from "hono/http-exception";
import { createErrorResponse, generateS3ConfigId, formatFileSize } from "../utils/common.js";
import { encryptValue, decryptValue } from "../utils/crypto.js";
import { createS3Client } from "../utils/s3Utils.js";
import { S3Client, ListObjectsV2Command, PutObjectCommand, DeleteObjectCommand } from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";
import { S3ConfigRepository, FileRepository, RepositoryFactory } from "../repositories/index.js";

/**
 * S3配置服务类
 */
class S3ConfigService {
  /**
   * 构造函数
   * @param {D1Database} db - 数据库实例
   */
  constructor(db) {
    this.s3ConfigRepository = new S3ConfigRepository(db);
    this.fileRepository = new FileRepository(db);
    this.db = db; // 保留db引用，用于复杂查询和测试功能
  }

  /**
   * 获取管理员的S3配置列表（支持分页）
   * @param {string} adminId - 管理员ID
   * @param {Object} options - 查询选项
   * @param {number} [options.page] - 页码
   * @param {number} [options.limit] - 每页数量
   * @returns {Promise<Object>} S3配置列表和分页信息
   */
  async getS3ConfigsByAdmin(adminId, options = {}) {
    if (options.page !== undefined || options.limit !== undefined) {
      // 明确的分页查询
      return await this.s3ConfigRepository.findByAdminWithPagination(adminId, options);
    } else {
      // 兼容性：无分页参数时返回所有数据
      const configs = await this.s3ConfigRepository.findByAdmin(adminId);
      return { configs, total: configs.length };
    }
  }

  /**
   * 获取公开的S3配置列表
   * @returns {Promise<Array>} 公开的S3配置列表
   */
  async getPublicS3Configs() {
    return await this.s3ConfigRepository.findPublic();
  }

  /**
   * 通过ID获取S3配置（管理员访问）
   * @param {string} id - 配置ID
   * @param {string} adminId - 管理员ID
   * @returns {Promise<Object>} S3配置对象
   * @throws {HTTPException} 404 - 如果配置不存在
   */
  async getS3ConfigByIdForAdmin(id, adminId) {
    const config = await this.s3ConfigRepository.findByIdAndAdmin(id, adminId);
    if (!config) {
      throw new HTTPException(ApiStatus.NOT_FOUND, { message: "S3配置不存在" });
    }
    return config;
  }

  /**
   * 通过ID获取公开的S3配置
   * @param {string} id - 配置ID
   * @returns {Promise<Object>} S3配置对象
   * @throws {HTTPException} 404 - 如果配置不存在
   */
  async getPublicS3ConfigById(id) {
    const config = await this.s3ConfigRepository.findPublicById(id);
    if (!config) {
      throw new HTTPException(ApiStatus.NOT_FOUND, { message: "S3配置不存在" });
    }
    return config;
  }

  /**
   * 创建S3配置
   * @param {Object} configData - 配置数据
   * @param {string} adminId - 管理员ID
   * @param {string} encryptionSecret - 加密密钥
   * @returns {Promise<Object>} 创建的S3配置
   * @throws {HTTPException} 400 - 参数错误
   */
  async createS3Config(configData, adminId, encryptionSecret) {
    // 验证必填字段
    const requiredFields = ["name", "provider_type", "endpoint_url", "bucket_name", "access_key_id", "secret_access_key"];
    for (const field of requiredFields) {
      if (!configData[field]) {
        throw new HTTPException(ApiStatus.BAD_REQUEST, { message: `缺少必填字段: ${field}` });
      }
    }

    // 生成唯一ID
    const id = generateS3ConfigId();

    // 加密敏感字段
    const encryptedAccessKey = await encryptValue(configData.access_key_id, encryptionSecret);
    const encryptedSecretKey = await encryptValue(configData.secret_access_key, encryptionSecret);

    // 获取可选字段或设置默认值
    const region = configData.region || "";
    const pathStyle = configData.path_style === true ? 1 : 0;
    const defaultFolder = configData.default_folder || "";
    const isPublic = configData.is_public === true ? 1 : 0;

    // 处理新增的自定义域名相关字段
    const customHost = configData.custom_host || null;
    const signatureExpiresIn = parseInt(configData.signature_expires_in) || 3600;

    // 处理存储总容量
    let totalStorageBytes = null;
    if (configData.total_storage_bytes !== undefined) {
      // 如果用户提供了总容量，则直接使用
      const storageValue = parseInt(configData.total_storage_bytes);
      if (!isNaN(storageValue) && storageValue > 0) {
        totalStorageBytes = storageValue;
      }
    }

    // 如果未提供存储容量，根据不同的存储提供商设置合理的默认值
    if (totalStorageBytes === null) {
      if (configData.provider_type === S3ProviderTypes.R2) {
        totalStorageBytes = 10 * 1024 * 1024 * 1024; // 10GB默认值
      } else if (configData.provider_type === S3ProviderTypes.B2) {
        totalStorageBytes = 10 * 1024 * 1024 * 1024; // 10GB默认值
      } else {
        totalStorageBytes = 5 * 1024 * 1024 * 1024; // 5GB默认值
      }
      console.log(`未提供存储容量限制，为${configData.provider_type}设置默认值: ${formatFileSize(totalStorageBytes)}`);
    }

    // 准备创建数据
    const createData = {
      id,
      name: configData.name,
      provider_type: configData.provider_type,
      endpoint_url: configData.endpoint_url,
      bucket_name: configData.bucket_name,
      region,
      access_key_id: encryptedAccessKey,
      secret_access_key: encryptedSecretKey,
      path_style: pathStyle,
      default_folder: defaultFolder,
      is_public: isPublic,
      admin_id: adminId,
      total_storage_bytes: totalStorageBytes,
      custom_host: customHost,
      signature_expires_in: signatureExpiresIn,
    };

    // 创建S3配置
    await this.s3ConfigRepository.createConfig(createData);

    // 返回创建成功响应（不包含敏感字段）
    return {
      id,
      name: configData.name,
      provider_type: configData.provider_type,
      endpoint_url: configData.endpoint_url,
      bucket_name: configData.bucket_name,
      region,
      path_style: pathStyle === 1,
      default_folder: defaultFolder,
      is_public: isPublic === 1,
      total_storage_bytes: totalStorageBytes,
      custom_host: customHost,
      signature_expires_in: signatureExpiresIn,
    };
  }

  /**
   * 更新S3配置
   * @param {string} id - 配置ID
   * @param {Object} updateData - 更新数据
   * @param {string} adminId - 管理员ID
   * @param {string} encryptionSecret - 加密密钥
   * @returns {Promise<void>}
   * @throws {HTTPException} 404 - 配置不存在
   */
  async updateS3Config(id, updateData, adminId, encryptionSecret) {
    // 查询配置是否存在
    const config = await this.s3ConfigRepository.findByIdAndAdmin(id, adminId);
    if (!config) {
      throw new HTTPException(ApiStatus.NOT_FOUND, { message: "S3配置不存在" });
    }

    // 准备更新数据
    const repoUpdateData = {};

    // 处理存储容量字段
    if (updateData.total_storage_bytes !== undefined) {
      // 如果用户提供了总容量参数
      if (updateData.total_storage_bytes === null) {
        // 为null表示使用默认值，根据提供商类型设置
        let defaultStorageBytes;
        if (config.provider_type === S3ProviderTypes.R2) {
          defaultStorageBytes = 10 * 1024 * 1024 * 1024; // 10GB 默认值
        } else if (config.provider_type === S3ProviderTypes.B2) {
          defaultStorageBytes = 10 * 1024 * 1024 * 1024; // 10GB 默认值
        } else if (config.provider_type === S3ProviderTypes.ALIYUN_OSS) {
          defaultStorageBytes = 5 * 1024 * 1024 * 1024; // 5GB 默认值
        } else {
          defaultStorageBytes = 5 * 1024 * 1024 * 1024; // 5GB 默认值
        }

        repoUpdateData.total_storage_bytes = defaultStorageBytes;
        console.log(`重置存储容量限制，为${config.provider_type}设置默认值: ${formatFileSize(defaultStorageBytes)}`);
      } else {
        // 用户提供了具体数值
        const storageValue = parseInt(updateData.total_storage_bytes);
        if (!isNaN(storageValue) && storageValue > 0) {
          repoUpdateData.total_storage_bytes = storageValue;
        }
      }
    }

    // 更新名称
    if (updateData.name !== undefined) {
      repoUpdateData.name = updateData.name;
    }

    // 更新提供商类型
    if (updateData.provider_type !== undefined) {
      repoUpdateData.provider_type = updateData.provider_type;
    }

    // 更新端点URL
    if (updateData.endpoint_url !== undefined) {
      repoUpdateData.endpoint_url = updateData.endpoint_url;
    }

    // 更新桶名称
    if (updateData.bucket_name !== undefined) {
      repoUpdateData.bucket_name = updateData.bucket_name;
    }

    // 更新区域
    if (updateData.region !== undefined) {
      repoUpdateData.region = updateData.region;
    }

    // 更新访问密钥ID（需要加密）
    if (updateData.access_key_id !== undefined) {
      const encryptedAccessKey = await encryptValue(updateData.access_key_id, encryptionSecret);
      repoUpdateData.access_key_id = encryptedAccessKey;
    }

    // 更新秘密访问密钥（需要加密）
    if (updateData.secret_access_key !== undefined) {
      const encryptedSecretKey = await encryptValue(updateData.secret_access_key, encryptionSecret);
      repoUpdateData.secret_access_key = encryptedSecretKey;
    }

    // 更新路径样式
    if (updateData.path_style !== undefined) {
      repoUpdateData.path_style = updateData.path_style === true ? 1 : 0;
    }

    // 更新默认文件夹
    if (updateData.default_folder !== undefined) {
      repoUpdateData.default_folder = updateData.default_folder;
    }

    // 更新是否公开
    if (updateData.is_public !== undefined) {
      repoUpdateData.is_public = updateData.is_public === true ? 1 : 0;
    }

    // 更新自定义域名
    if (updateData.custom_host !== undefined) {
      repoUpdateData.custom_host = updateData.custom_host || null;
    }

    // 更新签名有效期
    if (updateData.signature_expires_in !== undefined) {
      const expiresIn = parseInt(updateData.signature_expires_in);
      repoUpdateData.signature_expires_in = !isNaN(expiresIn) && expiresIn > 0 ? expiresIn : 3600;
    }

    // 如果没有更新字段，直接返回成功
    if (Object.keys(repoUpdateData).length === 0) {
      return;
    }

    // 执行更新
    await this.s3ConfigRepository.updateConfig(id, repoUpdateData);
  }

  /**
   * 删除S3配置
   * @param {string} id - 配置ID
   * @param {string} adminId - 管理员ID
   * @returns {Promise<void>}
   * @throws {HTTPException} 404/409 - 配置不存在或有文件使用
   */
  async deleteS3Config(id, adminId) {
    // 查询配置是否存在
    const existingConfig = await this.s3ConfigRepository.findByIdAndAdmin(id, adminId);
    if (!existingConfig) {
      throw new HTTPException(ApiStatus.NOT_FOUND, { message: "S3配置不存在" });
    }

    // 检查是否有文件使用此配置
    const filesCount = await this.fileRepository.countByStorageConfigId(id, "S3");
    if (filesCount > 0) {
      throw new HTTPException(ApiStatus.CONFLICT, { message: `无法删除此配置，因为有${filesCount}个文件正在使用它` });
    }

    // 执行删除操作
    await this.s3ConfigRepository.deleteConfig(id);
  }

  /**
   * 设置默认S3配置
   * @param {string} id - 配置ID
   * @param {string} adminId - 管理员ID
   * @returns {Promise<void>}
   * @throws {HTTPException} 404 - 配置不存在
   */
  async setDefaultS3Config(id, adminId) {
    // 查询配置是否存在
    const config = await this.s3ConfigRepository.findByIdAndAdmin(id, adminId);
    if (!config) {
      throw new HTTPException(ApiStatus.NOT_FOUND, { message: "S3配置不存在" });
    }

    // 设置默认配置（Repository会处理原子操作）
    await this.s3ConfigRepository.setAsDefault(id, adminId);
  }

  /**
   * 获取带使用情况的S3配置列表
   * @returns {Promise<Array>} S3配置列表
   */
  async getS3ConfigsWithUsage() {
    return await this.s3ConfigRepository.findAllWithUsage();
  }
}

/**
 * S3存储提供商测试策略基类
 */
class S3TestStrategy {
  constructor(config, s3Client, requestOrigin) {
    this.config = config;
    this.s3Client = s3Client;
    this.requestOrigin = requestOrigin;
  }

  /**
   * 获取提供商特定的CORS请求头
   */
  getCorsHeaders() {
    return {
      Origin: this.requestOrigin,
      "Access-Control-Request-Method": "PUT",
      "Access-Control-Request-Headers": "content-type,x-amz-content-sha256,x-amz-date,authorization",
    };
  }

  /**
   * 获取提供商特定的上传参数
   */
  getUploadParams(testKey, testContent) {
    return {
      Bucket: this.config.bucket_name,
      Key: testKey,
      Body: testContent,
      ContentType: "text/plain",
      Metadata: {
        "test-purpose": "cloudpaste-s3-test",
        "test-timestamp": `${Date.now()}`,
      },
    };
  }

  /**
   * 获取提供商特定的错误处理提示
   */
  getErrorTroubleshooting(errorType) {
    const guides = {
      cors: "请检查您的S3兼容服务提供商的CORS配置说明，确保允许来自您前端域名的请求。",
      upload: "上传失败通常与CORS配置、权限设置或预签名URL过期有关。请检查服务配置。",
    };
    return guides[errorType] || guides.upload;
  }

  /**
   * 是否跳过写入测试
   */
  shouldSkipWriteTest() {
    return false;
  }

  /**
   * 获取跳过写入测试的结果
   */
  getSkipWriteResult() {
    return {
      success: true,
      uploadTime: 0,
      testFile: "(此提供商跳过测试写入)",
      note: "由于提供商特性，跳过测试写入。实际上传功能正常工作。",
    };
  }
}

/**
 * Backblaze B2测试策略
 */
class B2TestStrategy extends S3TestStrategy {
  getCorsHeaders() {
    const headers = super.getCorsHeaders();
    headers["Access-Control-Request-Headers"] += ",x-bz-content-sha1,x-requested-with";
    return headers;
  }

  getUploadParams(testKey, testContent) {
    const params = super.getUploadParams(testKey, testContent);
    params.Metadata["test-provider"] = "b2";
    return params;
  }

  getErrorTroubleshooting(errorType) {
    const guides = {
      cors: "对于B2，需要在存储桶设置中配置CORS。确保允许来源包含您的域名或*，方法包含PUT，以及所有必要的头部。",
      upload: "B2上传失败可能与Content-SHA1头部有关，确保已正确配置CORS并允许此头部。",
    };
    return guides[errorType] || guides.upload;
  }

  shouldSkipWriteTest() {
    return true; // B2跳过直接写入测试
  }

  getSkipWriteResult() {
    return {
      success: true,
      uploadTime: 0,
      testFile: "(B2存储服务不进行测试写入)",
      note: "由于B2 S3兼容层的特性，跳过测试写入。实际上传功能正常工作。",
    };
  }
}

/**
 * Cloudflare R2测试策略
 */
class R2TestStrategy extends S3TestStrategy {
  getErrorTroubleshooting(errorType) {
    const guides = {
      cors: "在Cloudflare R2控制台的存储桶设置中启用CORS，添加适当的来源和方法。",
      upload: "R2上传失败通常与CORS配置或权限有关，请检查R2存储桶的CORS设置和访问策略。",
    };
    return guides[errorType] || guides.upload;
  }
}

/**
 * AWS S3测试策略
 */
class AWSS3TestStrategy extends S3TestStrategy {
  getErrorTroubleshooting(errorType) {
    const guides = {
      cors: "在AWS S3控制台的存储桶属性中配置CORS设置，添加适当的跨域规则。",
      upload: "AWS S3上传失败通常与IAM权限、存储桶策略或CORS配置有关。",
    };
    return guides[errorType] || guides.upload;
  }
}

/**
 * 测试策略工厂
 */
class S3TestStrategyFactory {
  static create(config, s3Client, requestOrigin) {
    switch (config.provider_type) {
      case S3ProviderTypes.B2:
        return new B2TestStrategy(config, s3Client, requestOrigin);
      case S3ProviderTypes.R2:
        return new R2TestStrategy(config, s3Client, requestOrigin);
      case S3ProviderTypes.AWS:
        return new AWSS3TestStrategy(config, s3Client, requestOrigin);
      default:
        return new S3TestStrategy(config, s3Client, requestOrigin);
    }
  }
}

/**
 * 测试S3配置连接
 * @param {D1Database} db - D1数据库实例
 * @param {string} id - 配置ID
 * @param {string} adminId - 管理员ID
 * @param {string} encryptionSecret - 加密密钥
 * @param {string} requestOrigin - 请求来源
 * @returns {Promise<Object>} 测试结果
 */
export async function testS3Connection(db, id, adminId, encryptionSecret, requestOrigin) {
  // 使用 S3ConfigRepository 获取配置（需要包含敏感字段用于测试）
  const repositoryFactory = new RepositoryFactory(db);
  const s3ConfigRepository = repositoryFactory.getS3ConfigRepository();

  const config = await s3ConfigRepository.findByIdAndAdminWithSecrets(id, adminId);

  if (!config) {
    throw new HTTPException(ApiStatus.NOT_FOUND, { message: "S3配置不存在" });
  }

  // 创建S3客户端和测试策略
  const s3Client = await createS3Client(config, encryptionSecret);
  const strategy = S3TestStrategyFactory.create(config, s3Client, requestOrigin);

  // 初始化测试结果
  const testResult = {
    read: { success: false, error: null, note: "后端直接测试，不代表前端访问" },
    write: { success: false, error: null, note: "后端直接测试，不代表前端上传" },
    cors: { success: false, error: null, note: "测试S3存储桶的CORS配置是否支持跨域请求" },
    frontendSim: { success: false, error: null, note: "完整模拟前端预签名URL上传流程，最接近真实使用场景" },
    connectionInfo: {
      bucket: config.bucket_name,
      endpoint: config.endpoint_url || "默认",
      region: config.region || "默认",
      pathStyle: config.path_style ? "是" : "否",
      provider: config.provider_type,
      defaultFolder: config.default_folder || "",
      customHost: config.custom_host || "未配置",
      signatureExpiresIn: `${config.signature_expires_in || 3600}秒`,
    },
  };

  // 执行测试步骤
  await executeReadTest(testResult, strategy);
  await executeWriteTest(testResult, strategy);
  await executeCorsTest(testResult, strategy);
  await executeFrontendSimulationTest(testResult, strategy);

  // 更新最后使用时间
  await updateLastUsedTime(db, id);

  // 生成测试结果摘要
  const summary = generateTestSummary(testResult);

  return {
    success: summary.overallSuccess,
    message: summary.message,
    result: testResult,
  };
}

/**
 * 更新S3配置的最后使用时间
 */
async function updateLastUsedTime(db, configId) {
  // 使用 S3ConfigRepository
  const repositoryFactory = new RepositoryFactory(db);
  const s3ConfigRepository = repositoryFactory.getS3ConfigRepository();

  await s3ConfigRepository.updateLastUsed(configId);
}

/**
 * 生成测试结果摘要
 */
function generateTestSummary(testResult) {
  let message = "S3配置测试";

  // 基础连接成功条件：读权限必须可用
  const basicConnectSuccess = testResult.read.success;
  // 前端可用条件：CORS配置和前端模拟测试都成功
  const frontendUsable = testResult.cors.success && testResult.frontendSim.success;

  // 判断整体成功状态：优先考虑前端可用性
  let overallSuccess = basicConnectSuccess && frontendUsable;

  if (basicConnectSuccess) {
    if (testResult.write.success) {
      if (frontendUsable) {
        message += "成功 (读写权限均可用，前端上传测试通过)";
      } else if (testResult.cors.success) {
        message += "部分成功 (读写权限可用，CORS配置正确，但前端上传模拟失败)";
      } else {
        message += "部分成功 (读写权限可用，但CORS配置有问题)";
      }
    } else {
      if (testResult.cors.success) {
        message += "部分成功 (仅读权限可用，CORS配置正确)";
      } else {
        message += "部分成功 (仅读权限可用，CORS配置有问题)";
      }
    }
  } else {
    message += "失败 (读取权限不可用)";
  }

  // 添加全局提示说明
  testResult.globalNote = "读写测试验证基本连接和权限；CORS测试验证跨域配置；前端模拟测试是判断前端能否直接上传的最终依据";

  return {
    overallSuccess,
    message,
  };
}

/**
 * 执行读取权限测试
 */
async function executeReadTest(testResult, strategy) {
  try {
    const defaultFolder = strategy.config.default_folder || "";
    const prefix = defaultFolder ? (defaultFolder.endsWith("/") ? defaultFolder : defaultFolder + "/") : "";

    const command = new ListObjectsV2Command({
      Bucket: strategy.config.bucket_name,
      MaxKeys: 10,
      Prefix: prefix,
    });

    const response = await strategy.s3Client.send(command);
    testResult.read.success = true;
    testResult.read.objectCount = response.Contents?.length || 0;
    testResult.read.prefix = prefix || "(根目录)";
    testResult.read.note = "此测试通过后端SDK直接访问S3，成功不代表前端可访问";

    if (response.Contents && response.Contents.length > 0) {
      testResult.read.firstObjects = response.Contents.slice(0, 3).map((obj) => ({
        key: obj.Key,
        size: formatFileSize(obj.Size),
        lastModified: new Date(obj.LastModified).toISOString(),
      }));
    }
  } catch (error) {
    testResult.read.success = false;
    testResult.read.error = error.message;
    testResult.read.code = error.Code || error.code;
  }
}

/**
 * 执行写入权限测试
 */
async function executeWriteTest(testResult, strategy) {
  try {
    const timestamp = Date.now();
    const defaultFolder = strategy.config.default_folder || "";
    const prefix = defaultFolder ? (defaultFolder.endsWith("/") ? defaultFolder : defaultFolder + "/") : "";
    const testKey = `${prefix}__test_${timestamp}.txt`;
    const testContent = "CloudPaste S3测试文件 - " + new Date().toISOString();

    if (strategy.shouldSkipWriteTest()) {
      Object.assign(testResult.write, strategy.getSkipWriteResult());
      return;
    }

    // 使用策略获取上传参数
    const uploadParams = strategy.getUploadParams(testKey, testContent);
    const putCommand = new PutObjectCommand(uploadParams);

    // 执行上传测试
    const uploadStartTime = performance.now();
    await strategy.s3Client.send(putCommand);
    const uploadEndTime = performance.now();

    testResult.write.success = true;
    testResult.write.uploadTime = Math.round(uploadEndTime - uploadStartTime);
    testResult.write.testFile = testKey;
    testResult.write.note = "此测试通过后端SDK直接上传，成功不代表前端可上传";

    // 清理测试文件
    await cleanupTestFile(strategy, testKey, testResult.write);
  } catch (error) {
    testResult.write.success = false;
    testResult.write.error = error.message;
    testResult.write.code = error.Code || error.code;
  }
}

/**
 * 清理测试文件
 */
async function cleanupTestFile(strategy, testKey, writeResult) {
  try {
    const deleteCommand = new DeleteObjectCommand({
      Bucket: strategy.config.bucket_name,
      Key: testKey,
    });
    await strategy.s3Client.send(deleteCommand);
    writeResult.cleaned = true;
  } catch (cleanupError) {
    writeResult.cleaned = false;
    writeResult.cleanupError = cleanupError.message;
  }
}

/**
 * 执行CORS配置测试
 * 使用预检请求（Preflight Request）方法，这是AWS官方推荐的标准CORS测试方法
 */
async function executeCorsTest(testResult, strategy) {
  try {
    const timestamp = Date.now();
    const defaultFolder = strategy.config.default_folder || "";
    const prefix = defaultFolder ? (defaultFolder.endsWith("/") ? defaultFolder : defaultFolder + "/") : "";
    const testKey = `${prefix}__cors_test_${timestamp}.txt`;

    // 生成预签名URL用于CORS预检测试
    const putCommand = new PutObjectCommand({
      Bucket: strategy.config.bucket_name,
      Key: testKey,
      ContentType: "text/plain",
    });

    const presignedUrl = await getSignedUrl(strategy.s3Client, putCommand, { expiresIn: strategy.config.signature_expires_in || 300 });

    // 使用策略获取CORS预检请求头
    const corsRequestHeaders = strategy.getCorsHeaders();

    // 执行CORS预检请求（OPTIONS方法）
    const optionsResponse = await fetch(presignedUrl, {
      method: "OPTIONS",
      headers: corsRequestHeaders,
    });

    // 分析预检响应头
    const allowOrigin = optionsResponse.headers.get("access-control-allow-origin");
    const allowMethods = optionsResponse.headers.get("access-control-allow-methods");
    const allowHeaders = optionsResponse.headers.get("access-control-allow-headers");
    const maxAge = optionsResponse.headers.get("access-control-max-age");

    if (allowOrigin) {
      testResult.cors.success = true;
      testResult.cors.allowOrigin = allowOrigin;
      testResult.cors.allowMethods = allowMethods;
      testResult.cors.allowHeaders = allowHeaders;
      testResult.cors.maxAge = maxAge;
      testResult.cors.statusCode = optionsResponse.status;
      testResult.cors.note = "CORS预检请求测试通过，存储桶支持跨域请求";

      // 检查是否支持PUT方法（文件上传必需）
      const supportsPut = allowMethods && allowMethods.toLowerCase().includes("put");
      const supportsOrigin = allowOrigin === "*" || allowOrigin === strategy.requestOrigin;

      if (supportsPut && supportsOrigin) {
        testResult.cors.detail = "CORS配置完全支持跨域文件上传";
        testResult.cors.uploadSupported = true;
      } else {
        testResult.cors.detail = "CORS配置存在但可能不完全支持文件上传";
        testResult.cors.uploadSupported = false;
        testResult.cors.warning = `缺少支持: ${!supportsPut ? "PUT方法 " : ""}${!supportsOrigin ? "来源域名匹配" : ""}`;
      }
    } else {
      testResult.cors.success = false;
      testResult.cors.error = "预检请求未返回Access-Control-Allow-Origin头";
      testResult.cors.statusCode = optionsResponse.status;
      testResult.cors.note = "存储桶可能未配置CORS或CORS配置不正确";
      testResult.cors.configGuide = strategy.getErrorTroubleshooting("cors");

      // 收集所有响应头用于诊断
      testResult.cors.responseHeaders = {};
      for (const [key, value] of optionsResponse.headers.entries()) {
        testResult.cors.responseHeaders[key] = value;
      }
    }
  } catch (error) {
    testResult.cors.success = false;
    testResult.cors.error = "CORS预检请求失败: " + error.message;
    testResult.cors.note = "无法执行CORS测试，可能是网络问题或存储服务不可达";
    testResult.cors.configGuide = strategy.getErrorTroubleshooting("cors");
  }
}

/**
 * 执行前端上传流程模拟测试
 * 完整模拟前端预签名URL上传流程，包括获取预签名URL、上传文件、提交元数据
 */
async function executeFrontendSimulationTest(testResult, strategy) {
  // 初始化测试步骤
  testResult.frontendSim.steps = {
    step1: { name: "获取预签名URL", success: false, duration: 0 },
    step2: { name: "模拟文件上传", success: false, duration: 0 },
    step3: { name: "验证上传结果", success: false, duration: 0 },
  };

  try {
    const timestamp = Date.now();
    const defaultFolder = strategy.config.default_folder || "";
    const prefix = defaultFolder ? (defaultFolder.endsWith("/") ? defaultFolder : defaultFolder + "/") : "";
    const testFileName = `frontend_test_${timestamp}.txt`;
    const testKey = `${prefix}${testFileName}`;
    const testContent = `CloudPaste前端上传模拟测试\n时间: ${new Date().toISOString()}\n提供商: ${strategy.config.provider_type}\n测试ID: ${timestamp}`;
    const testContentType = "text/plain";

    // 步骤1: 获取预签名URL（模拟前端调用后端API）
    const step1StartTime = performance.now();
    testResult.frontendSim.steps.step1.note = "模拟前端调用后端API获取预签名URL";

    try {
      const putCommand = new PutObjectCommand({
        Bucket: strategy.config.bucket_name,
        Key: testKey,
        ContentType: testContentType,
        Metadata: {
          "test-purpose": "cloudpaste-frontend-simulation",
          "test-timestamp": `${timestamp}`,
          "test-provider": strategy.config.provider_type,
        },
      });

      const presignedUrl = await getSignedUrl(strategy.s3Client, putCommand, { expiresIn: strategy.config.signature_expires_in || 300 });
      const step1EndTime = performance.now();

      testResult.frontendSim.steps.step1.success = true;
      testResult.frontendSim.steps.step1.duration = Math.round(step1EndTime - step1StartTime);
      testResult.frontendSim.steps.step1.presignedUrl = presignedUrl.substring(0, 80) + "...";
      testResult.frontendSim.steps.step1.note = "成功获取预签名URL，模拟前端API调用";
    } catch (step1Error) {
      testResult.frontendSim.steps.step1.success = false;
      testResult.frontendSim.steps.step1.error = step1Error.message;
      testResult.frontendSim.steps.step1.note = "获取预签名URL失败，前端无法进行上传";
      throw step1Error;
    }

    // 步骤2: 模拟前端XHR上传到S3
    const step2StartTime = performance.now();
    testResult.frontendSim.steps.step2.note = "模拟前端使用XHR直接上传到S3存储";

    try {
      // 获取预签名URL
      const presignedUrl = await getSignedUrl(
        strategy.s3Client,
        new PutObjectCommand({
          Bucket: strategy.config.bucket_name,
          Key: testKey,
          ContentType: testContentType,
        }),
        { expiresIn: strategy.config.signature_expires_in || 300 }
      );

      // 模拟前端上传请求头（根据不同提供商定制）
      const uploadHeaders = {
        "Content-Type": testContentType,
        Origin: strategy.requestOrigin,
      };

      // 根据提供商添加特定头部
      if (strategy.config.provider_type === S3ProviderTypes.B2) {
        uploadHeaders["X-Bz-Content-Sha1"] = "do_not_verify";
        uploadHeaders["X-Requested-With"] = "XMLHttpRequest";
      } else if (strategy.config.provider_type === S3ProviderTypes.ALIYUN_OSS) {
        // 阿里云OSS通常不需要特殊头部，使用标准S3头部即可
        // 如果需要特殊处理，可以在这里添加
      }

      // 执行模拟上传
      const uploadResponse = await fetch(presignedUrl, {
        method: "PUT",
        headers: uploadHeaders,
        body: testContent,
      });

      const step2EndTime = performance.now();

      if (uploadResponse.ok) {
        const etag = uploadResponse.headers.get("ETag");

        testResult.frontendSim.steps.step2.success = true;
        testResult.frontendSim.steps.step2.duration = Math.round(step2EndTime - step2StartTime);
        testResult.frontendSim.steps.step2.statusCode = uploadResponse.status;
        testResult.frontendSim.steps.step2.etag = etag ? etag.replace(/"/g, "") : null;
        testResult.frontendSim.steps.step2.uploadSpeed = `${(testContent.length / ((step2EndTime - step2StartTime) / 1000)).toFixed(2)} B/s`;
        testResult.frontendSim.steps.step2.note = "成功模拟前端XHR上传，文件已上传到S3";

        // 记录提供商特定的响应头
        testResult.frontendSim.steps.step2.providerHeaders = {};
        if (strategy.config.provider_type === S3ProviderTypes.B2) {
          testResult.frontendSim.steps.step2.providerHeaders.fileId = uploadResponse.headers.get("x-bz-file-id");
          testResult.frontendSim.steps.step2.providerHeaders.sha1 = uploadResponse.headers.get("x-bz-content-sha1");
        }
      } else {
        testResult.frontendSim.steps.step2.success = false;
        testResult.frontendSim.steps.step2.statusCode = uploadResponse.status;
        testResult.frontendSim.steps.step2.statusText = uploadResponse.statusText;
        testResult.frontendSim.steps.step2.error = `HTTP ${uploadResponse.status}: ${uploadResponse.statusText}`;
        testResult.frontendSim.steps.step2.note = "前端XHR上传失败，可能是CORS配置问题";

        try {
          testResult.frontendSim.steps.step2.responseBody = await uploadResponse.text();
        } catch (e) {
          testResult.frontendSim.steps.step2.responseBody = "无法读取响应内容";
        }

        throw new Error(`上传失败: HTTP ${uploadResponse.status}`);
      }
    } catch (step2Error) {
      testResult.frontendSim.steps.step2.success = false;
      testResult.frontendSim.steps.step2.error = step2Error.message;
      testResult.frontendSim.steps.step2.troubleshooting = strategy.getErrorTroubleshooting("upload");
      throw step2Error;
    }

    // 步骤3: 验证上传结果（模拟前端验证上传是否成功）
    const step3StartTime = performance.now();
    testResult.frontendSim.steps.step3.note = "验证文件是否成功上传到S3存储";

    try {
      // 验证文件是否存在
      const headCommand = new (await import("@aws-sdk/client-s3")).HeadObjectCommand({
        Bucket: strategy.config.bucket_name,
        Key: testKey,
      });

      const headResponse = await strategy.s3Client.send(headCommand);
      const step3EndTime = performance.now();

      testResult.frontendSim.steps.step3.success = true;
      testResult.frontendSim.steps.step3.duration = Math.round(step3EndTime - step3StartTime);
      testResult.frontendSim.steps.step3.fileSize = headResponse.ContentLength;
      testResult.frontendSim.steps.step3.lastModified = headResponse.LastModified?.toISOString();
      testResult.frontendSim.steps.step3.contentType = headResponse.ContentType;
      testResult.frontendSim.steps.step3.note = "成功验证文件已上传到S3，前端上传流程完整";

      // 清理测试文件
      try {
        const deleteCommand = new DeleteObjectCommand({
          Bucket: strategy.config.bucket_name,
          Key: testKey,
        });
        await strategy.s3Client.send(deleteCommand);
        testResult.frontendSim.steps.step3.fileCleaned = true;
      } catch (cleanupError) {
        testResult.frontendSim.steps.step3.fileCleaned = false;
        testResult.frontendSim.steps.step3.cleanupError = cleanupError.message;
      }
    } catch (step3Error) {
      testResult.frontendSim.steps.step3.success = false;
      testResult.frontendSim.steps.step3.error = step3Error.message;
      testResult.frontendSim.steps.step3.note = "无法验证上传结果，可能文件未成功上传";
      throw step3Error;
    }

    // 所有步骤成功
    testResult.frontendSim.success = true;
    testResult.frontendSim.note = "前端上传流程模拟完全成功，配置可用于实际前端上传";
    testResult.frontendSim.totalDuration = testResult.frontendSim.steps.step1.duration + testResult.frontendSim.steps.step2.duration + testResult.frontendSim.steps.step3.duration;
    testResult.frontendSim.testFile = testKey;
  } catch (error) {
    testResult.frontendSim.success = false;
    testResult.frontendSim.error = error.message;
    testResult.frontendSim.note = "前端上传流程模拟失败，配置可能不适用于前端直传";

    // 确定失败阶段
    if (!testResult.frontendSim.steps.step1.success) {
      testResult.frontendSim.failedAt = "获取预签名URL";
    } else if (!testResult.frontendSim.steps.step2.success) {
      testResult.frontendSim.failedAt = "文件上传";
    } else {
      testResult.frontendSim.failedAt = "验证上传结果";
    }

    testResult.frontendSim.troubleshooting = strategy.getErrorTroubleshooting("upload");
  }
}

// ==================== 向后兼容的导出函数 ====================

/**
 * 获取S3配置列表（向后兼容，支持分页）
 * @param {D1Database} db - D1数据库实例
 * @param {string} adminId - 管理员ID
 * @param {Object} [options] - 查询选项
 * @returns {Promise<Object>} S3配置列表和分页信息
 */
export async function getS3ConfigsByAdmin(db, adminId, options = {}) {
  const s3ConfigService = new S3ConfigService(db);
  return await s3ConfigService.getS3ConfigsByAdmin(adminId, options);
}

/**
 * 获取公开的S3配置列表（向后兼容）
 * @param {D1Database} db - D1数据库实例
 * @returns {Promise<Array>} 公开的S3配置列表
 */
export async function getPublicS3Configs(db) {
  const s3ConfigService = new S3ConfigService(db);
  return await s3ConfigService.getPublicS3Configs();
}

/**
 * 通过ID获取S3配置（管理员访问，向后兼容）
 * @param {D1Database} db - D1数据库实例
 * @param {string} id - 配置ID
 * @param {string} adminId - 管理员ID
 * @returns {Promise<Object>} S3配置对象
 */
export async function getS3ConfigByIdForAdmin(db, id, adminId) {
  const s3ConfigService = new S3ConfigService(db);
  return await s3ConfigService.getS3ConfigByIdForAdmin(id, adminId);
}

/**
 * 通过ID获取公开的S3配置（向后兼容）
 * @param {D1Database} db - D1数据库实例
 * @param {string} id - 配置ID
 * @returns {Promise<Object>} S3配置对象
 */
export async function getPublicS3ConfigById(db, id) {
  const s3ConfigService = new S3ConfigService(db);
  return await s3ConfigService.getPublicS3ConfigById(id);
}

/**
 * 创建S3配置（向后兼容）
 * @param {D1Database} db - D1数据库实例
 * @param {Object} configData - 配置数据
 * @param {string} adminId - 管理员ID
 * @param {string} encryptionSecret - 加密密钥
 * @returns {Promise<Object>} 创建的S3配置
 */
export async function createS3Config(db, configData, adminId, encryptionSecret) {
  const s3ConfigService = new S3ConfigService(db);
  return await s3ConfigService.createS3Config(configData, adminId, encryptionSecret);
}

/**
 * 更新S3配置（向后兼容）
 * @param {D1Database} db - D1数据库实例
 * @param {string} id - 配置ID
 * @param {Object} updateData - 更新数据
 * @param {string} adminId - 管理员ID
 * @param {string} encryptionSecret - 加密密钥
 * @returns {Promise<void>}
 */
export async function updateS3Config(db, id, updateData, adminId, encryptionSecret) {
  const s3ConfigService = new S3ConfigService(db);
  return await s3ConfigService.updateS3Config(id, updateData, adminId, encryptionSecret);
}

/**
 * 删除S3配置（向后兼容）
 * @param {D1Database} db - D1数据库实例
 * @param {string} id - 配置ID
 * @param {string} adminId - 管理员ID
 * @returns {Promise<void>}
 */
export async function deleteS3Config(db, id, adminId) {
  const s3ConfigService = new S3ConfigService(db);
  return await s3ConfigService.deleteS3Config(id, adminId);
}

/**
 * 设置默认S3配置（向后兼容）
 * @param {D1Database} db - D1数据库实例
 * @param {string} id - 配置ID
 * @param {string} adminId - 管理员ID
 * @returns {Promise<void>}
 */
export async function setDefaultS3Config(db, id, adminId) {
  const s3ConfigService = new S3ConfigService(db);
  return await s3ConfigService.setDefaultS3Config(id, adminId);
}

/**
 * 获取带使用情况的S3配置列表（向后兼容）
 * @param {D1Database} db - D1数据库实例
 * @returns {Promise<Array>} S3配置列表
 */
export async function getS3ConfigsWithUsage(db) {
  const s3ConfigService = new S3ConfigService(db);
  return await s3ConfigService.getS3ConfigsWithUsage();
}
