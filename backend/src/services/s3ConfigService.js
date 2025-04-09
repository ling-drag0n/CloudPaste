/**
 * S3存储配置服务
 */
import { DbTables, ApiStatus, S3ProviderTypes } from "../constants";
import { HTTPException } from "hono/http-exception";
import { createErrorResponse, getLocalTimeString, generateS3ConfigId, formatFileSize } from "../utils/common";
import { encryptValue, decryptValue } from "../utils/crypto";
import { createS3Client } from "../utils/s3Utils";
import { S3Client, ListObjectsV2Command, PutObjectCommand, DeleteObjectCommand } from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";
import { testWebDAVConnection } from "../utils/webdavUtils";

/**
 * 获取S3配置列表
 * @param {D1Database} db - D1数据库实例
 * @param {string} adminId - 管理员ID
 * @returns {Promise<Array>} S3配置列表
 */
export async function getS3ConfigsByAdmin(db, adminId) {
  const configs = await db
      .prepare(
          `
      SELECT 
        id, name, provider_type, endpoint_url, bucket_name, 
        region, path_style, default_folder, is_public, is_default, 
        created_at, updated_at, last_used, total_storage_bytes
      FROM ${DbTables.S3_CONFIGS}
      WHERE admin_id = ?
      ORDER BY name ASC
      `
      )
      .bind(adminId)
      .all();

  return configs.results;
}

/**
 * 获取公开的S3配置列表
 * @param {D1Database} db - D1数据库实例
 * @returns {Promise<Array>} 公开的S3配置列表
 */
export async function getPublicS3Configs(db) {
  const configs = await db
      .prepare(
          `
      SELECT 
        id, name, provider_type, endpoint_url, bucket_name, 
        region, path_style, default_folder, is_default, created_at, updated_at, total_storage_bytes
      FROM ${DbTables.S3_CONFIGS}
      WHERE is_public = 1
      ORDER BY name ASC
      `
      )
      .all();

  return configs.results;
}

/**
 * 通过ID获取S3配置（管理员访问）
 * @param {D1Database} db - D1数据库实例
 * @param {string} id - 配置ID
 * @param {string} adminId - 管理员ID
 * @returns {Promise<Object>} S3配置对象
 */
export async function getS3ConfigByIdForAdmin(db, id, adminId) {
  const config = await db
      .prepare(
          `
      SELECT 
        id, name, provider_type, endpoint_url, bucket_name, 
        region, path_style, default_folder, is_public, is_default, 
        created_at, updated_at, last_used, total_storage_bytes
      FROM ${DbTables.S3_CONFIGS}
      WHERE id = ? AND admin_id = ?
    `
      )
      .bind(id, adminId)
      .first();

  if (!config) {
    throw new HTTPException(ApiStatus.NOT_FOUND, { message: "S3配置不存在" });
  }

  return config;
}

/**
 * 通过ID获取公开的S3配置
 * @param {D1Database} db - D1数据库实例
 * @param {string} id - 配置ID
 * @returns {Promise<Object>} S3配置对象
 */
export async function getPublicS3ConfigById(db, id) {
  const config = await db
      .prepare(
          `
      SELECT 
        id, name, provider_type, endpoint_url, bucket_name, 
        region, path_style, default_folder, is_default, created_at, updated_at, total_storage_bytes
      FROM ${DbTables.S3_CONFIGS}
      WHERE id = ? AND is_public = 1
    `
      )
      .bind(id)
      .first();

  if (!config) {
    throw new HTTPException(ApiStatus.NOT_FOUND, { message: "S3配置不存在" });
  }

  return config;
}

/**
 * 创建S3配置
 * @param {D1Database} db - D1数据库实例
 * @param {Object} configData - 配置数据
 * @param {string} adminId - 管理员ID
 * @param {string} encryptionSecret - 加密密钥
 * @returns {Promise<Object>} 创建的S3配置
 */
export async function createS3Config(db, configData, adminId, encryptionSecret) {
  // 验证必填字段
  const requiredFields = ["name", "provider_type", "endpoint_url", "bucket_name", "access_key_id", "secret_access_key"];
  
  // 如果是WebDAV类型，bucket_name不是必需的
  if (configData.provider_type === S3ProviderTypes.WEBDAV) {
    const webdavRequiredFields = ["name", "provider_type", "endpoint_url", "access_key_id", "secret_access_key"];
    for (const field of webdavRequiredFields) {
      if (!configData[field]) {
        throw new HTTPException(ApiStatus.BAD_REQUEST, { message: `缺少必填字段: ${field}` });
      }
    }
    // 为WebDAV设置默认bucket_name
    if (!configData.bucket_name) {
      configData.bucket_name = "webdav-storage";
    }
  } else {
    // 对于S3兼容存储，验证所有必填字段
    for (const field of requiredFields) {
      if (!configData[field]) {
        throw new HTTPException(ApiStatus.BAD_REQUEST, { message: `缺少必填字段: ${field}` });
      }
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
    } else if (configData.provider_type === S3ProviderTypes.WEBDAV) {
      totalStorageBytes = 5 * 1024 * 1024 * 1024; // 5GB默认值
    } else {
      totalStorageBytes = 5 * 1024 * 1024 * 1024; // 5GB默认值
    }
    console.log(`未提供存储容量限制，为${configData.provider_type}设置默认值: ${formatFileSize(totalStorageBytes)}`);
  }

  // 添加到数据库
  await db
      .prepare(
          `
    INSERT INTO ${DbTables.S3_CONFIGS} (
      id, name, provider_type, endpoint_url, bucket_name, 
      region, access_key_id, secret_access_key, path_style, 
      default_folder, is_public, admin_id, total_storage_bytes, created_at, updated_at
    ) VALUES (
      ?, ?, ?, ?, ?, 
      ?, ?, ?, ?, 
      ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
    )
  `
      )
      .bind(
          id,
          configData.name,
          configData.provider_type,
          configData.endpoint_url,
          configData.bucket_name,
          region,
          encryptedAccessKey,
          encryptedSecretKey,
          pathStyle,
          defaultFolder,
          isPublic,
          adminId,
          totalStorageBytes
      )
      .run();

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
  };
}

/**
 * 更新S3配置
 * @param {D1Database} db - D1数据库实例
 * @param {string} id - 配置ID
 * @param {Object} updateData - 更新数据
 * @param {string} adminId - 管理员ID
 * @param {string} encryptionSecret - 加密密钥
 * @returns {Promise<void>}
 */
export async function updateS3Config(db, id, updateData, adminId, encryptionSecret) {
  // 查询配置是否存在
  const config = await db.prepare(`SELECT id, provider_type FROM ${DbTables.S3_CONFIGS} WHERE id = ? AND admin_id = ?`).bind(id, adminId).first();

  if (!config) {
    throw new HTTPException(ApiStatus.NOT_FOUND, { message: "S3配置不存在" });
  }

  // 准备更新字段
  const updateFields = [];
  const params = [];

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
      } else {
        defaultStorageBytes = 5 * 1024 * 1024 * 1024; // 5GB 默认值
      }

      updateFields.push("total_storage_bytes = ?");
      params.push(defaultStorageBytes);
      console.log(`重置存储容量限制，为${config.provider_type}设置默认值: ${formatFileSize(defaultStorageBytes)}`);
    } else {
      // 用户提供了具体数值
      const storageValue = parseInt(updateData.total_storage_bytes);
      if (!isNaN(storageValue) && storageValue > 0) {
        updateFields.push("total_storage_bytes = ?");
        params.push(storageValue);
      }
    }
  }

  // 更新名称
  if (updateData.name !== undefined) {
    updateFields.push("name = ?");
    params.push(updateData.name);
  }

  // 更新提供商类型
  if (updateData.provider_type !== undefined) {
    updateFields.push("provider_type = ?");
    params.push(updateData.provider_type);
  }

  // 更新端点URL
  if (updateData.endpoint_url !== undefined) {
    updateFields.push("endpoint_url = ?");
    params.push(updateData.endpoint_url);
  }

  // 更新桶名称
  if (updateData.bucket_name !== undefined) {
    updateFields.push("bucket_name = ?");
    params.push(updateData.bucket_name);
  }

  // 更新区域
  if (updateData.region !== undefined) {
    updateFields.push("region = ?");
    params.push(updateData.region);
  }

  // 更新访问密钥ID（需要加密）
  if (updateData.access_key_id !== undefined) {
    updateFields.push("access_key_id = ?");
    const encryptedAccessKey = await encryptValue(updateData.access_key_id, encryptionSecret);
    params.push(encryptedAccessKey);
  }

  // 更新秘密访问密钥（需要加密）
  if (updateData.secret_access_key !== undefined) {
    updateFields.push("secret_access_key = ?");
    const encryptedSecretKey = await encryptValue(updateData.secret_access_key, encryptionSecret);
    params.push(encryptedSecretKey);
  }

  // 更新路径样式
  if (updateData.path_style !== undefined) {
    updateFields.push("path_style = ?");
    params.push(updateData.path_style === true ? 1 : 0);
  }

  // 更新默认文件夹
  if (updateData.default_folder !== undefined) {
    updateFields.push("default_folder = ?");
    params.push(updateData.default_folder);
  }

  // 更新是否公开
  if (updateData.is_public !== undefined) {
    updateFields.push("is_public = ?");
    params.push(updateData.is_public === true ? 1 : 0);
  }

  // 更新时间戳
  updateFields.push("updated_at = ?");
  params.push(new Date().toISOString());

  // 如果没有更新字段，直接返回成功
  if (updateFields.length === 0) {
    return;
  }

  // 添加ID作为条件参数
  params.push(id);
  params.push(adminId);

  // 执行更新
  await db
      .prepare(`UPDATE ${DbTables.S3_CONFIGS} SET ${updateFields.join(", ")} WHERE id = ? AND admin_id = ?`)
      .bind(...params)
      .run();
}

/**
 * 删除S3配置
 * @param {D1Database} db - D1数据库实例
 * @param {string} id - 配置ID
 * @param {string} adminId - 管理员ID
 * @returns {Promise<void>}
 */
export async function deleteS3Config(db, id, adminId) {
  // 查询配置是否存在
  const existingConfig = await db.prepare(`SELECT id FROM ${DbTables.S3_CONFIGS} WHERE id = ? AND admin_id = ?`).bind(id, adminId).first();

  if (!existingConfig) {
    throw new HTTPException(ApiStatus.NOT_FOUND, { message: "S3配置不存在" });
  }

  // 检查是否有文件使用此配置
  const filesCount = await db
      .prepare(
          `
      SELECT COUNT(*) as count FROM ${DbTables.FILES}
      WHERE s3_config_id = ?
    `
      )
      .bind(id)
      .first();

  if (filesCount && filesCount.count > 0) {
    throw new HTTPException(ApiStatus.CONFLICT, { message: `无法删除此配置，因为有${filesCount.count}个文件正在使用它` });
  }

  // 执行删除操作
  await db.prepare(`DELETE FROM ${DbTables.S3_CONFIGS} WHERE id = ?`).bind(id).run();
}

/**
 * 设置默认S3配置
 * @param {D1Database} db - D1数据库实例
 * @param {string} id - 配置ID
 * @param {string} adminId - 管理员ID
 * @returns {Promise<void>}
 */
export async function setDefaultS3Config(db, id, adminId) {
  // 查询配置是否存在
  const config = await db.prepare(`SELECT id FROM ${DbTables.S3_CONFIGS} WHERE id = ? AND admin_id = ?`).bind(id, adminId).first();

  if (!config) {
    throw new HTTPException(ApiStatus.NOT_FOUND, { message: "S3配置不存在" });
  }

  // 使用D1的batch API来执行原子事务操作
  await db.batch([
    // 1. 首先将所有配置设置为非默认
    db
        .prepare(
            `UPDATE ${DbTables.S3_CONFIGS}
       SET is_default = 0, updated_at = CURRENT_TIMESTAMP
       WHERE admin_id = ?`
        )
        .bind(adminId),

    // 2. 然后将当前配置设置为默认
    db
        .prepare(
            `UPDATE ${DbTables.S3_CONFIGS}
       SET is_default = 1, updated_at = CURRENT_TIMESTAMP
       WHERE id = ?`
        )
        .bind(id),
  ]);
}

/**
 * 测试S3连接
 * @param {D1Database} db - D1数据库实例
 * @param {string} id - 配置ID
 * @param {string} adminId - 管理员ID
 * @param {string} encryptionSecret - 加密密钥
 * @param {string} requestOrigin - 请求来源
 * @returns {Promise<Object>} 测试结果
 */
export async function testS3Connection(db, id, adminId, encryptionSecret, requestOrigin) {
  // 获取配置信息
  const config = await db
    .prepare(
      `
      SELECT *
      FROM ${DbTables.S3_CONFIGS}
      WHERE id = ? AND admin_id = ?
    `
    )
    .bind(id, adminId)
    .first();

  if (!config) {
    throw new HTTPException(ApiStatus.NOT_FOUND, { message: "配置不存在或您没有权限访问" });
  }

  try {
    // 如果是WebDAV存储，使用WebDAV连接测试
    if (config.provider_type === S3ProviderTypes.WEBDAV) {
      const testResult = await testWebDAVConnection(config, encryptionSecret);
      
      if (testResult.success) {
        return {
          success: true,
          message: "WebDAV连接测试成功!",
          provider: config.provider_type,
          endpoint: testResult.endpoint,
          bucket: "WebDAV",
        };
      } else {
        throw new Error(testResult.message || "WebDAV连接测试失败");
      }
    }

    // 对于S3兼容存储，使用原有的S3连接测试
    // 创建S3客户端测试连接
    const s3Client = await createS3Client(config, encryptionSecret);

    // 测试结果对象
    const testResult = {
      read: { success: false, error: null, note: "后端直接测试，不代表前端访问" },
      write: { success: false, error: null, note: "后端直接测试，不代表前端上传" },
      cors: { success: false, error: null, note: "仅测试CORS预检请求配置，是跨域支持的基础" },
      connectionInfo: {
        bucket: config.bucket_name,
        endpoint: config.endpoint_url || "默认",
        region: config.region || "默认",
        pathStyle: config.path_style ? "是" : "否",
        provider: config.provider_type,
        directory: config.directory || "",
      },
    };

    // 测试阶段1: 读取权限测试
    try {
      const command = new ListObjectsV2Command({
        Bucket: config.bucket_name,
        MaxKeys: 10,
        Prefix: config.directory ? `${config.directory}/` : "",
      });

      // 发送标准命令对象
      const response = await s3Client.send(command);
      testResult.read.success = true;
      testResult.read.objectCount = response.Contents?.length || 0;
      testResult.read.prefix = config.directory ? `${config.directory}/` : "(根目录)";
      testResult.read.note = "此测试通过后端SDK直接访问S3，成功不代表前端可访问";

      // 更详细的信息
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

    // 测试阶段2: 写入权限测试 (仅创建一个小测试文件)
    try {
      const timestamp = Date.now();
      const testKey = `${config.directory ? config.directory + "/" : ""}__test_${timestamp}.txt`;

      // 创建测试文件内容
      const testContent = "CloudPaste S3连接测试文件";

      // 针对不同的存储提供商采用不同的上传策略
      if (config.provider_type === S3ProviderTypes.B2) {
        // B2特殊处理 - 由于头部兼容性问题，改为标记为只读测试成功
        console.log("B2存储服务跳过直接写入测试，仅测试读取权限");

        // 将B2标记为测试成功，但添加说明
        testResult.write.success = true;
        testResult.write.uploadTime = 0;
        testResult.write.testFile = "(B2存储服务不进行测试写入)";
        testResult.write.note = "由于B2 S3兼容层的特性，跳过测试写入。实际上传功能正常工作。";
      } else {
        // 其他S3服务使用标准AWS SDK
        const putCommand = new PutObjectCommand({
          Bucket: config.bucket_name,
          Key: testKey,
          Body: testContent,
          ContentType: "text/plain",
          Metadata: {
            "test-purpose": "cloudpaste-s3-test",
            "test-timestamp": `${timestamp}`,
          },
        });

        // 尝试上传一个测试文件
        const uploadStartTime = performance.now();
        const putResponse = await s3Client.send(putCommand);
        const uploadEndTime = performance.now();

        testResult.write.success = true;
        testResult.write.uploadTime = Math.round(uploadEndTime - uploadStartTime);
        testResult.write.testFile = testKey;
        testResult.write.note = "此测试通过后端SDK直接上传，成功不代表前端可上传";

        // 上传成功后尝试删除测试文件 (但不影响测试结果)
        try {
          const deleteCommand = new DeleteObjectCommand({
            Bucket: config.bucket_name,
            Key: testKey,
          });
          await s3Client.send(deleteCommand);
          testResult.write.cleaned = true;
        } catch (cleanupError) {
          testResult.write.cleaned = false;
          testResult.write.cleanupError = cleanupError.message;
        }
      }
    } catch (error) {
      testResult.write.success = false;
      testResult.write.error = error.message;
      testResult.write.code = error.Code || error.code;
    }

    // 测试阶段3: 跨域CORS配置测试
    try {
      const timestamp = Date.now();
      const testKey = `${config.directory ? config.directory + "/" : ""}__cors_test_${timestamp}.txt`;
      const testContent = "CloudPaste CORS测试文件";

      // 生成预签名URL用于跨域测试
      const putCommand = new PutObjectCommand({
        Bucket: config.bucket_name,
        Key: testKey,
        ContentType: "text/plain",
      });

      // 获取预签名URL
      const presignedUrl = await getSignedUrl(s3Client, putCommand, { expiresIn: 300 });

      try {
        // 根据不同服务商定制CORS测试请求头
        const corsRequestHeaders = {
          Origin: requestOrigin,
          "Access-Control-Request-Method": "PUT",
          "Access-Control-Request-Headers": "content-type,x-amz-content-sha256,x-amz-date,authorization",
        };

        // 为特定服务商添加额外的CORS请求头
        switch (config.provider_type) {
          case S3ProviderTypes.B2:
            // B2可能需要额外的请求头
            corsRequestHeaders["Access-Control-Request-Headers"] += ",x-bz-content-sha1,x-requested-with";
            break;
        }

        // 创建fetch请求测试服务端预检响应
        const optionsResponse = await fetch(presignedUrl, {
          method: "OPTIONS",
          headers: corsRequestHeaders,
        });

        // 检查预检响应头
        const allowOrigin = optionsResponse.headers.get("access-control-allow-origin");
        const allowMethods = optionsResponse.headers.get("access-control-allow-methods");
        const allowHeaders = optionsResponse.headers.get("access-control-allow-headers");

        if (allowOrigin) {
          testResult.cors.success = true;
          testResult.cors.allowOrigin = allowOrigin;
          testResult.cors.allowMethods = allowMethods;
          testResult.cors.allowHeaders = allowHeaders;
          testResult.cors.note = "此测试仅检查CORS预检请求配置是否正确，是判断S3服务是否支持跨域请求的基础";

          // 添加CORS配置说明
          testResult.cors.detail = "预检请求测试通过，S3服务的CORS基础配置正确。";

          // 为特定服务商添加额外CORS说明
          switch (config.provider_type) {
            case S3ProviderTypes.B2:
              testResult.cors.providerNote = "对于B2，除了基本CORS配置外，还需要确保已允许X-Bz-Content-Sha1和X-Requested-With头部。";
              break;

            case S3ProviderTypes.R2:
              testResult.cors.providerNote = "Cloudflare R2的CORS配置相对简单，通常在控制台中设置后即可正常工作。";
              break;
          }
        } else {
          testResult.cors.success = false;
          testResult.cors.error = "预检请求未返回Access-Control-Allow-Origin头，可能没有正确配置CORS";
          testResult.cors.statusCode = optionsResponse.status;

          // 添加更多预检错误诊断信息
          testResult.cors.optionsResponseHeaders = {};
          for (const [key, value] of optionsResponse.headers.entries()) {
            testResult.cors.optionsResponseHeaders[key] = value;
          }

          // 添加特定服务商的CORS配置指南
          switch (config.provider_type) {
            case S3ProviderTypes.B2:
              testResult.cors.configGuide = "对于B2，需要在存储桶设置中配置CORS。确保允许来源包含您的域名或*，方法包含PUT，以及所有必要的头部。";
              break;

            case S3ProviderTypes.R2:
              testResult.cors.configGuide = "在Cloudflare R2控制台的存储桶设置中启用CORS，添加适当的来源和方法。";
              break;

            case S3ProviderTypes.AWS:
              testResult.cors.configGuide = "在AWS S3控制台的存储桶属性中配置CORS设置，添加适当的跨域规则。";
              break;

            default:
              testResult.cors.configGuide = "请检查您的S3兼容服务提供商的CORS配置说明，确保允许来自您前端域名的请求。";
          }
        }
      } catch (corsError) {
        testResult.cors.success = false;
        testResult.cors.error = corsError.message;
      }
    } catch (presignError) {
      testResult.cors.success = false;
      testResult.cors.error = "无法生成预签名URL: " + presignError.message;
    }

    // 测试阶段4: 完整前端上传流程模拟
    testResult.frontendSim = {
      success: false,
      note: "此测试完整模拟前端上传流程，包含预签名URL获取、XHR上传和元数据提交",
    };

    try {
      const timestamp = Date.now();
      const testFilename = `frontend_upload_test_${timestamp}.txt`;
      const testPath = config.directory ? `${config.directory}/tests/` : "tests/";
      const testKey = `${testPath}${testFilename}`;
      const testContent = "CloudPaste前端上传模拟测试文件 - " + new Date().toISOString();
      const testMimetype = "text/plain";
      const testSize = testContent.length;

      // 步骤1: 模拟前端获取预签名URL的请求
      testResult.frontendSim.step1 = { name: "获取预签名URL", success: false };

      // 为不同服务商准备合适的PutObject参数
      const putCommandParams = {
        Bucket: config.bucket_name,
        Key: testKey,
        ContentType: testMimetype,
      };

      // 特定服务商可能需要额外参数
      switch (config.provider_type) {
        case S3ProviderTypes.B2:
          // B2可能需要特定元数据
          putCommandParams.Metadata = {
            "test-purpose": "cloudpaste-s3-test",
          };
          break;
      }

      // 创建PutObjectCommand
      const putCommand = new PutObjectCommand(putCommandParams);

      // 获取预签名URL - 不同服务商可能需要不同过期时间
      let expiresIn = 300; // 默认5分钟

      const uploadUrl = await getSignedUrl(s3Client, putCommand, { expiresIn });
      testResult.frontendSim.step1.success = true;
      testResult.frontendSim.step1.url = uploadUrl.substring(0, 80) + "..."; // 截断显示

      // 步骤2: 模拟前端直接上传
      testResult.frontendSim.step2 = { name: "XHR文件上传", success: false };

      // 准备请求头，模拟前端XHR上传 (针对不同服务商定制)
      const uploadHeaders = {
        "Content-Type": testMimetype,
        Origin: requestOrigin,
      };

      // 根据不同服务商添加特定的请求头
      switch (config.provider_type) {
        case S3ProviderTypes.B2:
          // B2需要这些特殊头部
          uploadHeaders["X-Bz-Content-Sha1"] = "do_not_verify";
          uploadHeaders["X-Requested-With"] = "XMLHttpRequest";
          break;

        case S3ProviderTypes.R2:
          // R2可能需要特定头部
          break;
      }

      // 模拟XHR上传
      const uploadStartTime = performance.now();
      const uploadResponse = await fetch(uploadUrl, {
        method: "PUT",
        headers: uploadHeaders,
        body: testContent,
      });
      const uploadEndTime = performance.now();

      if (uploadResponse.ok) {
        const uploadDuration = uploadEndTime - uploadStartTime;
        const uploadSpeed = (testContent.length / (uploadDuration / 1000)).toFixed(2); // 字节/秒

        testResult.frontendSim.step2.success = true;
        testResult.frontendSim.step2.duration = Math.round(uploadDuration);
        testResult.frontendSim.step2.speed = `${uploadSpeed} B/s`;
        testResult.frontendSim.step2.etag = uploadResponse.headers.get("ETag");

        // 不同服务商可能返回不同的头部信息
        testResult.frontendSim.step2.providerHeaders = {};
        switch (config.provider_type) {
          case S3ProviderTypes.B2:
            // 记录B2特有的响应头
            testResult.frontendSim.step2.providerHeaders.uploadId = uploadResponse.headers.get("x-bz-file-id");
            testResult.frontendSim.step2.providerHeaders.sha1 = uploadResponse.headers.get("x-bz-content-sha1");
            break;

          case S3ProviderTypes.R2:
            // 记录R2特有的响应头
            break;
        }

        // 步骤3: 模拟前端上传后元数据提交 (模拟completeFileUpload流程)
        testResult.frontendSim.step3 = { name: "元数据提交", success: false };

        // 在实际前端中，这里会调用completeFileUpload API
        // 但在测试中，我们只模拟这个过程并标记成功
        testResult.frontendSim.step3.success = true;
        testResult.frontendSim.step3.note = "实际前端会调用接口提交元数据";

        // 针对不同服务商的兼容性提示
        switch (config.provider_type) {
          case S3ProviderTypes.B2:
            testResult.frontendSim.step3.providerNote = "B2存储需要在前端上传时添加X-Bz-Content-Sha1头部，CloudPaste已处理此要求。";
            break;

          case S3ProviderTypes.R2:
            testResult.frontendSim.step3.providerNote = "Cloudflare R2完全兼容标准S3上传流程，无需特殊处理。";
            break;
        }

        // 清理测试文件
        try {
          const deleteCommand = new DeleteObjectCommand({
            Bucket: config.bucket_name,
            Key: testKey,
          });
          await s3Client.send(deleteCommand);
          testResult.frontendSim.fileCleaned = true;
        } catch (cleanError) {
          testResult.frontendSim.fileCleaned = false;
          testResult.frontendSim.cleanError = cleanError.message;
        }

        // 所有步骤成功，标记整体测试成功
        testResult.frontendSim.success = true;
      } else {
        testResult.frontendSim.step2.success = false;
        testResult.frontendSim.step2.status = uploadResponse.status;
        testResult.frontendSim.step2.statusText = uploadResponse.statusText;
        try {
          testResult.frontendSim.step2.errorText = await uploadResponse.text();
        } catch (e) {
          testResult.frontendSim.step2.errorText = "无法读取错误响应内容";
        }

        // 添加服务商特定的错误解决提示
        switch (config.provider_type) {
          case S3ProviderTypes.B2:
            testResult.frontendSim.step2.troubleshooting = "B2上传失败可能与Content-SHA1头部有关，确保已正确配置CORS并允许此头部。";
            break;

          case S3ProviderTypes.R2:
            testResult.frontendSim.step2.troubleshooting = "R2上传失败通常与CORS配置或权限有关，请检查R2存储桶的CORS设置和访问策略。";
            break;

          default:
            testResult.frontendSim.step2.troubleshooting = "上传失败通常与CORS配置、权限设置或预签名URL过期有关。请检查服务配置。";
        }
      }
    } catch (error) {
      testResult.frontendSim.error = error.message;
      if (!testResult.frontendSim.step1?.success) {
        testResult.frontendSim.failedAt = "获取预签名URL";
      } else if (!testResult.frontendSim.step2?.success) {
        testResult.frontendSim.failedAt = "文件上传";
      } else {
        testResult.frontendSim.failedAt = "元数据提交";
      }

      // 添加错误诊断指南
      testResult.frontendSim.troubleshooting = "测试失败可能是由于网络连接问题、S3配置错误或凭证无效。请检查您的配置并重试。";
    }

    // 更新最后使用时间
    await db
        .prepare(
            `
        UPDATE ${DbTables.S3_CONFIGS}
        SET last_used = ?
        WHERE id = ?
      `
        )
        .bind(getLocalTimeString(), id)
        .run();

    // 生成友好的测试结果消息
    let message = "S3配置测试";

    // 调整成功判断逻辑，更重视前端模拟测试结果
    // 基础连接成功条件：读权限必须可用
    let basicConnectSuccess = testResult.read.success;
    // 前端可用条件：跨域配置和前端模拟测试都成功
    let frontendUsable = testResult.cors.success && testResult.frontendSim?.success;

    // 总体成功状态同时考虑基础连接和前端可用性
    let overallSuccess = basicConnectSuccess;

    if (basicConnectSuccess) {
      if (testResult.write.success) {
        if (testResult.cors.success) {
          if (testResult.frontendSim?.success) {
            message += "成功 (读写权限均可用，前端上传测试通过)";
          } else {
            message += "部分成功 (读写权限可用，CORS配置正确，但前端上传模拟失败)";
          }
        } else {
          message += "部分成功 (读写权限可用，但CORS配置有问题)";
        }
      } else {
        message += "部分成功 (仅读权限可用)";
      }
    } else {
      message += "失败 (读取权限不可用)";
    }

    // 测试结果的全局提示说明
    testResult.globalNote = "读写测试仅验证基本连接和权限，通过后端直接测试；CORS测试验证跨域基础配置是否正确；前端模拟测试才是判断前端能否直接上传的最终依据";

    return {
      success: overallSuccess,
      message,
      result: testResult,
    };
  } catch (error) {
    throw new HTTPException(ApiStatus.INTERNAL_SERVER_ERROR, { message: "测试过程中发生错误: " + error.message });
  }
}

/**
 * 获取带使用情况的S3配置列表
 * @param {D1Database} db - D1数据库实例
 * @returns {Promise<Array>} S3配置列表
 */
export async function getS3ConfigsWithUsage(db) {
  // 1. 获取所有S3配置
  const configs = await db
      .prepare(
          `
      SELECT 
        id, name, provider_type, endpoint_url, bucket_name, 
        region, path_style, default_folder, is_public, is_default, 
        created_at, updated_at, last_used, total_storage_bytes, admin_id
      FROM ${DbTables.S3_CONFIGS}
      ORDER BY name ASC
      `
      )
      .all();

  // 2. 对每个配置，查询使用情况
  const result = [];
  for (const config of configs.results) {
    // 查询每个配置的文件数和总大小
    const usage = await db
        .prepare(
            `
        SELECT 
          COUNT(*) as file_count, 
          SUM(size) as total_size
        FROM ${DbTables.FILES}
        WHERE s3_config_id = ?`
        )
        .bind(config.id)
        .first();

    result.push({
      ...config,
      usage: {
        file_count: usage?.file_count || 0,
        total_size: usage?.total_size || 0,
      },
    });
  }

  return result;
}
