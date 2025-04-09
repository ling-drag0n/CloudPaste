/**
 * S3存储配置服务
 */
import { DbTables, ApiStatus, S3ProviderTypes } from "../constants";
import { HTTPException } from "hono/http-exception";
import { createErrorResponse, getLocalTimeString, generateS3ConfigId, formatFileSize } from "../utils/common";
import { encryptValue, decryptValue } from "../utils/crypto";
import { createS3Client } from "../utils/s3Utils";
import { testWebDAVConnection } from "../utils/webdavUtils";
import { S3Client, ListObjectsV2Command, PutObjectCommand, DeleteObjectCommand } from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";

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
  const requiredFields = ["name", "provider_type", "endpoint_url"];
  
  // WebDAV不需要bucket_name，而S3需要
  if (configData.provider_type !== S3ProviderTypes.WEBDAV) {
    requiredFields.push("bucket_name");
  }
  
  // 所有提供商都需要认证信息
  requiredFields.push("access_key_id", "secret_access_key");
  
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
  
  // 对于WebDAV，bucket_name用作基础目录路径
  const bucketName = configData.bucket_name || (configData.provider_type === S3ProviderTypes.WEBDAV ? "" : ""); 

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
      totalStorageBytes = 10 * 1024 * 1024 * 1024; // WebDAV默认10GB
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
          bucketName,
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
    bucket_name: bucketName,
    region,
    path_style: !!pathStyle,
    default_folder: defaultFolder,
    is_public: !!isPublic,
    is_default: false,
    total_storage_bytes: totalStorageBytes,
    created_at: getLocalTimeString(),
    updated_at: getLocalTimeString(),
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
 * 测试S3配置连接
 * @param {D1Database} db - D1数据库实例
 * @param {string} id - 配置ID
 * @param {string} adminId - 管理员ID
 * @param {string} encryptionSecret - 加密密钥
 * @param {string} requestOrigin - 请求来源（用于CORS验证）
 * @returns {Promise<Object>} 测试结果
 */
export async function testS3Connection(db, id, adminId, encryptionSecret, requestOrigin) {
  // 查询配置
  const config = await db
    .prepare(
      `
      SELECT 
        id, name, provider_type, endpoint_url, bucket_name, 
        region, access_key_id, secret_access_key, path_style, 
        default_folder, is_public, is_default, admin_id
      FROM ${DbTables.S3_CONFIGS}
      WHERE id = ? AND admin_id = ?
    `
    )
    .bind(id, adminId)
    .first();

  if (!config) {
    throw new HTTPException(ApiStatus.NOT_FOUND, { message: "S3配置不存在" });
  }

  // 如果是WebDAV存储，使用WebDAV测试方法
  if (config.provider_type === S3ProviderTypes.WEBDAV) {
    try {
      const result = await testWebDAVConnection(config, encryptionSecret);
      return result;
    } catch (error) {
      console.error("WebDAV连接测试失败:", error);
      return {
        success: false,
        message: "WebDAV连接测试失败: " + error.message,
        error: error.message || "未知错误",
        config: {
          name: config.name,
          provider_type: config.provider_type,
          endpoint_url: config.endpoint_url,
          bucket_name: config.bucket_name,
        },
      };
    }
  }

  // 解密存储密钥
  const accessKeyId = await decryptValue(config.access_key_id, encryptionSecret);
  const secretAccessKey = await decryptValue(config.secret_access_key, encryptionSecret);

  // 创建S3客户端
  try {
    // 创建S3客户端
    const s3Client = await createS3Client(config, encryptionSecret);

    // 准备测试的文件名
    const testKey = `${config.default_folder || ""}cloudpaste-test-${Date.now()}.txt`;
    
    // 测试上传文件
    const putParams = {
      Bucket: config.bucket_name,
      Key: testKey,
      Body: "CloudPaste S3 connection test.",
      ContentType: "text/plain",
    };
    
    // 根据不同服务商可能需要特殊处理
    let uploadResult;
    try {
      // 尝试上传
      uploadResult = await s3Client.send(new PutObjectCommand(putParams));
      
      // 如果没有抛出错误，说明上传成功
      console.log(`测试上传成功: ${testKey}, ETag: ${uploadResult.ETag}`);
    } catch (uploadError) {
      console.error("测试上传失败:", uploadError);
      return {
        success: false,
        message: "上传测试失败: " + uploadError.message,
        error: uploadError.message,
        config: {
          name: config.name,
          provider_type: config.provider_type,
          endpoint_url: config.endpoint_url,
          bucket_name: config.bucket_name,
        },
      };
    }
    
    // 测试列出文件
    let listResult;
    try {
      // 列出存储桶内容
      const listParams = {
        Bucket: config.bucket_name,
        Prefix: config.default_folder || "",
        MaxKeys: 10,
      };
      
      listResult = await s3Client.send(new ListObjectsV2Command(listParams));
      console.log(`列出桶内容成功，找到${listResult.Contents?.length || 0}个对象`);
    } catch (listError) {
      console.error("列出桶内容失败:", listError);
      return {
        success: false,
        message: "列出桶内容失败: " + listError.message,
        error: listError.message,
        config: {
          name: config.name,
          provider_type: config.provider_type,
          endpoint_url: config.endpoint_url,
          bucket_name: config.bucket_name,
        },
      };
    }
    
    // 测试生成预签名URL
    let presignedResult = null;
    try {
      const getCommand = new GetObjectCommand({
        Bucket: config.bucket_name,
        Key: testKey,
      });
      
      const url = await getSignedUrl(s3Client, getCommand, { expiresIn: 3600 });
      console.log("生成预签名URL成功:", url.substring(0, 100) + "...");
      presignedResult = { url: url.substring(0, 100) + "..." };
    } catch (presignedError) {
      console.error("生成预签名URL失败:", presignedError);
      presignedResult = { error: presignedError.message };
    }
    
    // 测试CORS（如果提供了请求源）
    let corsResult = null;
    if (requestOrigin) {
      try {
        // 构造CORS测试的URL
        const corsTestUrl = new URL(presignedResult.url || "");
        const headers = new Headers();
        headers.append("Origin", requestOrigin);
        
        // 发送OPTIONS请求测试CORS
        const corsResponse = await fetch(corsTestUrl.toString(), {
          method: "OPTIONS",
          headers,
        });
        
        const corsHeaders = {
          "access-control-allow-origin": corsResponse.headers.get("access-control-allow-origin"),
          "access-control-allow-methods": corsResponse.headers.get("access-control-allow-methods"),
          "access-control-allow-headers": corsResponse.headers.get("access-control-allow-headers"),
        };
        
        corsResult = {
          status: corsResponse.status,
          allowOrigin: corsHeaders["access-control-allow-origin"],
          allowMethods: corsHeaders["access-control-allow-methods"],
          allowHeaders: corsHeaders["access-control-allow-headers"],
          corsEnabled: !!corsHeaders["access-control-allow-origin"],
        };
        
        console.log("CORS测试结果:", corsResult);
      } catch (corsError) {
        console.error("CORS测试失败:", corsError);
        corsResult = { error: corsError.message };
      }
    }
    
    // 清理测试文件
    try {
      await s3Client.send(
        new DeleteObjectCommand({
          Bucket: config.bucket_name,
          Key: testKey,
        })
      );
      console.log("成功删除测试文件");
    } catch (deleteError) {
      console.error("删除测试文件失败:", deleteError);
      // 不返回失败，因为这只是清理步骤
    }
    
    // 返回成功结果
    return {
      success: true,
      message: "S3连接测试成功",
      details: {
        upload: !!uploadResult,
        list: {
          success: !!listResult,
          objects: listResult?.Contents?.length || 0,
        },
        presignedUrl: presignedResult,
        cors: corsResult,
      },
      config: {
        name: config.name,
        provider_type: config.provider_type,
        endpoint_url: config.endpoint_url,
        bucket_name: config.bucket_name,
      },
    };
  } catch (error) {
    console.error("S3连接测试失败:", error);
    return {
      success: false,
      message: "S3连接测试失败: " + error.message,
      error: error.message || "未知错误",
      config: {
        name: config.name,
        provider_type: config.provider_type,
        endpoint_url: config.endpoint_url,
        bucket_name: config.bucket_name,
      },
    };
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
