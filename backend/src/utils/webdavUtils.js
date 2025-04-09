/**
 * WebDAV存储操作相关工具函数
 */

import { createClient } from "webdav";
import { decryptValue } from "./crypto";

/**
 * 创建WebDAV客户端
 * @param {Object} config - WebDAV配置对象
 * @param {string} encryptionSecret - 用于解密凭证的密钥
 * @returns {Promise<Object>} WebDAV客户端实例
 */
export async function createWebDAVClient(config, encryptionSecret) {
  try {
    // 解密敏感配置
    const username = await decryptValue(config.access_key_id, encryptionSecret);
    const password = await decryptValue(config.secret_access_key, encryptionSecret);

    // 格式化WebDAV服务器URL
    let serverUrl = config.endpoint_url;
    if (serverUrl.endsWith("/")) {
      serverUrl = serverUrl.slice(0, -1);
    }

    // 创建WebDAV客户端
    const client = createClient(serverUrl, {
      username,
      password,
      maxBodyLength: 100 * 1024 * 1024, // 100MB默认上传限制
      maxContentLength: 100 * 1024 * 1024,
    });

    return client;
  } catch (error) {
    console.error("创建WebDAV客户端错误:", error);
    throw new Error("无法创建WebDAV客户端: " + (error.message || "未知错误"));
  }
}

/**
 * 构建WebDAV文件公共访问URL
 * @param {Object} config - WebDAV配置
 * @param {string} storagePath - 存储路径
 * @returns {string} 访问URL
 */
export function buildWebDAVUrl(config, storagePath) {
  let serverUrl = config.endpoint_url;
  if (serverUrl.endsWith("/")) {
    serverUrl = serverUrl.slice(0, -1);
  }

  // 确保存储路径不以斜杠开始
  let path = storagePath;
  if (path.startsWith("/")) {
    path = path.slice(1);
  }

  // 如果有bucket_name（作为基础目录），则添加到路径中
  if (config.bucket_name && config.bucket_name.length > 0) {
    let bucketPath = config.bucket_name;
    if (bucketPath.startsWith("/")) {
      bucketPath = bucketPath.slice(1);
    }
    if (!bucketPath.endsWith("/")) {
      bucketPath += "/";
    }
    path = bucketPath + path;
  }

  return `${serverUrl}/${path}`;
}

/**
 * 生成WebDAV文件上传URL
 * 注意：WebDAV不支持预签名URL，需要在服务器端处理上传
 * @param {Object} config - WebDAV配置
 * @param {string} storagePath - 存储路径
 * @param {string} encryptionSecret - 加密密钥
 * @returns {Promise<string>} 上传URL (WebDAV的URL主要用于服务器端操作)
 */
export async function getWebDAVUploadUrl(config, storagePath, encryptionSecret) {
  // 构建上传路径，返回标准格式URL
  // WebDAV需要在服务器端完成上传，这里仅用于生成最终URL
  const url = buildWebDAVUrl(config, storagePath);
  return url;
}

/**
 * 上传文件到WebDAV服务器
 * @param {Object} config - WebDAV配置
 * @param {string} storagePath - 存储路径
 * @param {Buffer|ReadableStream} fileContent - 文件内容
 * @param {string} encryptionSecret - 加密密钥  
 * @returns {Promise<boolean>} 是否上传成功
 */
export async function uploadFileToWebDAV(config, storagePath, fileContent, encryptionSecret) {
  try {
    const client = await createWebDAVClient(config, encryptionSecret);

    // 确保目录存在
    const dirPath = storagePath.substring(0, storagePath.lastIndexOf("/"));
    if (dirPath && dirPath.length > 0) {
      try {
        // 尝试创建目录，忽略如果目录已存在的错误
        await client.createDirectory(dirPath, { recursive: true });
      } catch (err) {
        // 忽略目录已存在的错误
        console.log(`目录创建错误(可能已存在): ${err.message}`);
      }
    }

    // 上传文件
    await client.putFileContents(storagePath, fileContent, { overwrite: true });
    console.log(`成功上传文件到WebDAV: ${storagePath}`);
    return true;
  } catch (error) {
    console.error(`上传文件到WebDAV错误: ${error.message || error}`);
    throw new Error("上传文件到WebDAV失败: " + (error.message || "未知错误"));
  }
}

/**
 * 从WebDAV下载文件
 * @param {Object} config - WebDAV配置
 * @param {string} storagePath - 存储路径 
 * @param {string} encryptionSecret - 加密密钥
 * @returns {Promise<{data: Buffer|ReadableStream, stats: Object}>} 文件内容和统计信息
 */
export async function downloadFileFromWebDAV(config, storagePath, encryptionSecret) {
  try {
    const client = await createWebDAVClient(config, encryptionSecret);

    // 获取文件内容
    const data = await client.getFileContents(storagePath);
    
    // 获取文件统计信息
    const stats = await client.stat(storagePath);

    return { data, stats };
  } catch (error) {
    console.error(`从WebDAV下载文件错误: ${error.message || error}`);
    throw new Error("从WebDAV下载文件失败: " + (error.message || "未知错误"));
  }
}

/**
 * 从WebDAV删除文件
 * @param {Object} config - WebDAV配置
 * @param {string} storagePath - 存储路径
 * @param {string} encryptionSecret - 加密密钥
 * @returns {Promise<boolean>} 是否删除成功
 */
export async function deleteFileFromWebDAV(config, storagePath, encryptionSecret) {
  try {
    const client = await createWebDAVClient(config, encryptionSecret);
    
    // 删除文件
    await client.deleteFile(storagePath);
    console.log(`成功从WebDAV删除文件: ${storagePath}`);
    return true;
  } catch (error) {
    console.error(`从WebDAV删除文件错误: ${error.message || error}`);
    return false;
  }
}

/**
 * 生成WebDAV文件下载URL
 * @param {Object} config - WebDAV配置
 * @param {string} storagePath - 存储路径
 * @param {string} encryptionSecret - 加密密钥
 * @param {boolean} forceDownload - 是否强制下载
 * @returns {Promise<string>} 下载URL
 */
export async function generateWebDAVDownloadUrl(config, storagePath, encryptionSecret, forceDownload = false) {
  try {
    // 简单返回WebDAV URL，实际访问时会通过Worker代理
    return buildWebDAVUrl(config, storagePath);
  } catch (error) {
    console.error("生成WebDAV下载URL错误:", error);
    throw new Error("无法生成WebDAV下载链接: " + (error.message || "未知错误"));
  }
}

/**
 * 检查WebDAV连接和权限
 * @param {Object} config - WebDAV配置
 * @param {string} encryptionSecret - 加密密钥
 * @returns {Promise<Object>} 连接测试结果
 */
export async function testWebDAVConnection(config, encryptionSecret) {
  try {
    const client = await createWebDAVClient(config, encryptionSecret);
    
    // 测试目录列表权限
    const testDir = config.bucket_name || "/";
    const dirContents = await client.getDirectoryContents(testDir);
    
    // 测试写入权限
    const testFilePath = `${testDir.endsWith("/") ? testDir : testDir + "/"}cloudpaste-test-file-${Date.now()}.txt`;
    await client.putFileContents(testFilePath, "CloudPaste WebDAV Test", { overwrite: true });
    
    // 测试读取权限
    const testContent = await client.getFileContents(testFilePath, { format: "text" });
    
    // 测试删除权限
    await client.deleteFile(testFilePath);
    
    return {
      success: true,
      message: "WebDAV连接测试成功",
      details: {
        dirListWorks: Array.isArray(dirContents),
        writeWorks: true,
        readWorks: testContent === "CloudPaste WebDAV Test",
        deleteWorks: true,
      }
    };
  } catch (error) {
    console.error("WebDAV连接测试失败:", error);
    return {
      success: false,
      message: "WebDAV连接测试失败: " + error.message,
      error: error.message || "未知错误"
    };
  }
} 