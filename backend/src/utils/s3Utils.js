/**
 * S3存储操作相关工具函数
 */

import { S3Client, PutObjectCommand, GetObjectCommand, ListObjectsV2Command, DeleteObjectCommand } from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";
import { ConfiguredRetryStrategy } from "@smithy/util-retry";
import { decryptValue } from "./crypto";
import { S3ProviderTypes } from "../constants";
import { buildWebDAVUrl, generateWebDAVPutUrl, generateWebDAVUrl, deleteFileFromWebDAV } from "./webdavUtils";
import btoa from "btoa";

/**
 * 创建S3客户端
 * @param {Object} config - S3配置对象
 * @param {string} encryptionSecret - 用于解密凭证的密钥
 * @returns {Promise<S3Client>} S3客户端实例
 */
export async function createS3Client(config, encryptionSecret) {
  // 解密敏感配置
  const accessKeyId = await decryptValue(config.access_key_id, encryptionSecret);
  const secretAccessKey = await decryptValue(config.secret_access_key, encryptionSecret);

  // 创建S3客户端配置
  const clientConfig = {
    endpoint: config.endpoint_url,
    region: config.region || "auto",
    credentials: {
      accessKeyId,
      secretAccessKey,
    },
    forcePathStyle: config.path_style === 1, // 使用路径样式访问
  };

  // 设置适当的超时时间
  clientConfig.requestTimeout = 30000; // 全局默认超时30秒

  // 设置默认重试策略
  let maxRetries = 3; // 默认最大重试次数
  let retryBackoffStrategy = (attempt) => Math.min(Math.pow(2, attempt) * 500, 10000); // 默认指数退避策略

  // 为不同服务商设置特定配置
  switch (config.provider_type) {
    case S3ProviderTypes.B2:
      // Backblaze B2特定配置
      clientConfig.signatureVersion = "v4";
      clientConfig.customUserAgent = "CloudPaste/1.0";
      clientConfig.requestTimeout = 60000;
      maxRetries = 4;
      break;

    case S3ProviderTypes.R2:
      // Cloudflare R2配置
      clientConfig.requestTimeout = 30000;
      break;

    case S3ProviderTypes.OTHER:
      clientConfig.signatureVersion = "v4";
      break;
  }

  // 应用重试策略
  clientConfig.retryStrategy = new ConfiguredRetryStrategy(maxRetries, retryBackoffStrategy);

  // 日志记录所选服务商和配置
  console.log(
      `正在创建S3客户端 (${config.provider_type}), endpoint: ${config.endpoint_url}, region: ${config.region || "auto"}, pathStyle: ${
          config.path_style ? "是" : "否"
      }, maxRetries: ${maxRetries}`
  );

  // 返回创建的S3客户端
  return new S3Client(clientConfig);
}

/**
 * 构建S3文件公共访问URL
 * @param {Object} s3Config - S3配置
 * @param {string} storagePath - S3存储路径
 * @returns {string} 访问URL
 */
export function buildS3Url(s3Config, storagePath) {
  // 如果是WebDAV存储，使用WebDAV URL构建逻辑
  if (s3Config.provider_type === S3ProviderTypes.WEBDAV) {
    return buildWebDAVUrl(s3Config, storagePath);
  }

  const bucketName = s3Config.bucket_name;
  const endpointUrl = s3Config.endpoint_url;

  // 去除endpoint_url末尾的斜杠(如果有)
  const endpoint = endpointUrl.endsWith("/") ? endpointUrl.slice(0, -1) : endpointUrl;

  // 确保storagePath不以斜杠开始
  const normalizedPath = storagePath.startsWith("/") ? storagePath.slice(1) : storagePath;

  // 根据配置选择合适的URL格式(路径样式vs虚拟主机样式)
  if (s3Config.path_style === 1) {
    // 路径样式: https://endpoint/bucket/key
    return `${endpoint}/${bucketName}/${normalizedPath}`;
  } else {
    // 虚拟主机样式: https://bucket.endpoint/key

    // 提取endpoint的域名部分
    let domain = endpoint;
    try {
      const url = new URL(endpoint);
      domain = url.host;
    } catch (e) {
      // 处理无效URL，保持原样
    }

    return `${endpoint.split("//")[0]}//${bucketName}.${domain}/${normalizedPath}`;
  }
}

/**
 * 生成S3文件的上传预签名URL
 * @param {Object} s3Config - S3配置
 * @param {string} storagePath - S3存储路径
 * @param {string} mimetype - 文件的MIME类型
 * @param {string} encryptionSecret - 用于解密凭证的密钥
 * @param {number} expiresIn - URL过期时间（秒），默认为1小时
 * @returns {Promise<string>} 预签名URL
 */
export async function generatePresignedPutUrl(s3Config, storagePath, mimetype, encryptionSecret, expiresIn = 3600) {
  // 如果是WebDAV存储，使用WebDAV上传URL生成逻辑
  if (s3Config.provider_type === S3ProviderTypes.WEBDAV) {
    const webdavInfo = await generateWebDAVPutUrl(s3Config, storagePath, encryptionSecret);
    // 创建Basic认证头
    const authHeader = `Basic ${btoa(`${webdavInfo.auth.username}:${webdavInfo.auth.password}`)}`;
    // 将WebDAV信息转换为前端可以使用的格式
    return {
      url: webdavInfo.url,
      method: webdavInfo.method,
      headers: {
        'Authorization': authHeader,
        'Content-Type': mimetype
      }
    };
  }

  try {
    // 创建S3客户端
    const s3Client = await createS3Client(s3Config, encryptionSecret);

    // 确保storagePath不以斜杠开始
    const normalizedPath = storagePath.startsWith("/") ? storagePath.slice(1) : storagePath;

    // 创建PutObjectCommand
    const command = new PutObjectCommand({
      Bucket: s3Config.bucket_name,
      Key: normalizedPath,
      ContentType: mimetype,
    });

    // 针对不同服务商添加特定头部或参数
    const commandOptions = { expiresIn };

    // 某些服务商可能对预签名URL有不同处理
    switch (s3Config.provider_type) {
      case S3ProviderTypes.B2:
        // B2特殊处理 - 某些情况可能需要添加特定头部
        // 例如Content-SHA1处理，但一般在前端上传时添加
        break;

      case S3ProviderTypes.OTHER:
        break;
    }

    // 生成预签名URL，应用服务商特定选项
    const url = await getSignedUrl(s3Client, command, commandOptions);

    return url;
  } catch (error) {
    console.error("生成上传预签名URL出错:", error);
    throw new Error("无法生成文件上传链接: " + (error.message || "未知错误"));
  }
}

/**
 * 生成S3文件的下载预签名URL
 * @param {Object} s3Config - S3配置
 * @param {string} storagePath - S3存储路径
 * @param {string} encryptionSecret - 用于解密凭证的密钥
 * @param {number} expiresIn - URL过期时间（秒），默认为1小时
 * @param {boolean} forceDownload - 是否强制下载（而非预览）
 * @returns {Promise<string>} 预签名URL
 */
export async function generatePresignedUrl(s3Config, storagePath, encryptionSecret, expiresIn = 3600, forceDownload = false) {
  // 如果是WebDAV存储，使用WebDAV下载URL生成逻辑
  if (s3Config.provider_type === S3ProviderTypes.WEBDAV) {
    const webdavInfo = await generateWebDAVUrl(s3Config, storagePath, encryptionSecret, forceDownload);
    // 安全序列化认证信息
    const authData = btoa(JSON.stringify(webdavInfo.auth));
    // 构造特殊格式URL，包含认证信息
    return `${webdavInfo.url}#webdav:${authData}`;
  }

  try {
    // 创建S3客户端
    const s3Client = await createS3Client(s3Config, encryptionSecret);

    // 确保storagePath不以斜杠开始
    const normalizedPath = storagePath.startsWith("/") ? storagePath.slice(1) : storagePath;

    // 提取文件名，用于Content-Disposition头
    const fileName = normalizedPath.split("/").pop();

    // 创建GetObjectCommand
    const commandParams = {
      Bucket: s3Config.bucket_name,
      Key: normalizedPath,
    };

    // 如果需要强制下载，添加相应的响应头
    if (forceDownload) {
      commandParams.ResponseContentDisposition = `attachment; filename="${encodeURIComponent(fileName)}"`;
    }

    // 针对特定服务商添加额外参数
    switch (s3Config.provider_type) {
      case S3ProviderTypes.B2:
        // B2可能需要特殊响应头
        break;
    }

    const command = new GetObjectCommand(commandParams);

    // 生成预签名URL
    const url = await getSignedUrl(s3Client, command, { expiresIn });

    return url;
  } catch (error) {
    console.error("生成预签名URL出错:", error);
    throw new Error("无法生成文件下载链接: " + (error.message || "未知错误"));
  }
}

/**
 * 从S3存储中删除文件
 * @param {Object} s3Config - S3配置信息
 * @param {string} storagePath - 存储路径
 * @param {string} encryptionSecret - 加密密钥
 * @returns {Promise<boolean>} 是否成功删除
 */
export async function deleteFileFromS3(s3Config, storagePath, encryptionSecret) {
  // 如果是WebDAV存储，使用WebDAV删除逻辑
  if (s3Config.provider_type === S3ProviderTypes.WEBDAV) {
    return await deleteFileFromWebDAV(s3Config, storagePath, encryptionSecret);
  }

  try {
    const s3Client = await createS3Client(s3Config, encryptionSecret);

    const deleteParams = {
      Bucket: s3Config.bucket_name,
      Key: storagePath,
    };

    await s3Client.send(new DeleteObjectCommand(deleteParams));
    console.log(`成功从S3存储中删除文件: ${storagePath}`);
    return true;
  } catch (error) {
    console.error(`从S3删除文件错误: ${error.message || error}`);
    return false;
  }
}
