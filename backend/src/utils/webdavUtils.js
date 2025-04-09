/**
 * WebDAV存储操作相关工具函数
 */

import { decryptValue } from "./crypto";
// 导入btoa用于Base64编码
import btoa from "btoa";

/**
 * 创建基本认证头
 * @param {string} username - 用户名
 * @param {string} password - 密码
 * @returns {string} 基本认证头值
 */
function createBasicAuthHeader(username, password) {
  return `Basic ${btoa(`${username}:${password}`)}`;
}

/**
 * 创建WebDAV客户端配置
 * @param {Object} config - WebDAV配置对象
 * @param {string} encryptionSecret - 用于解密凭证的密钥
 * @returns {Promise<Object>} WebDAV客户端配置
 */
export async function createWebDAVConfig(config, encryptionSecret) {
  // 解密敏感配置
  const username = await decryptValue(config.access_key_id, encryptionSecret);
  const password = await decryptValue(config.secret_access_key, encryptionSecret);

  // 确保endpoint_url以斜杠结尾
  let endpoint = config.endpoint_url;
  if (!endpoint.endsWith("/")) {
    endpoint += "/";
  }

  // 确保default_folder不以斜杠开始，但以斜杠结尾(如果不为空)
  let folder = config.default_folder || "";
  if (folder) {
    folder = folder.startsWith("/") ? folder.substring(1) : folder;
    folder = folder.endsWith("/") ? folder : folder + "/";
  }

  return {
    endpoint,
    username,
    password,
    folder
  };
}

/**
 * 构建WebDAV文件URL
 * @param {Object} webdavConfig - WebDAV配置
 * @param {string} storagePath - 存储路径
 * @returns {string} 文件URL
 */
export function buildWebDAVUrl(webdavConfig, storagePath) {
  // 确保storagePath不以斜杠开始
  const normalizedPath = storagePath.startsWith("/") ? storagePath.slice(1) : storagePath;
  
  // 构建完整URL
  return `${webdavConfig.endpoint}${normalizedPath}`;
}

/**
 * 生成WebDAV文件的上传URL
 * @param {Object} webdavConfig - WebDAV配置
 * @param {string} storagePath - 存储路径
 * @param {string} encryptionSecret - 用于解密凭证的密钥
 * @returns {Promise<Object>} 包含URL和认证信息的对象
 */
export async function generateWebDAVPutUrl(webdavConfig, storagePath, encryptionSecret) {
  try {
    const config = await createWebDAVConfig(webdavConfig, encryptionSecret);
    
    // 确保storagePath不以斜杠开始
    const normalizedPath = storagePath.startsWith("/") ? storagePath.slice(1) : storagePath;
    
    // 构建上传URL
    const url = `${config.endpoint}${normalizedPath}`;
    
    // 返回WebDAV上传信息
    return {
      url,
      method: "PUT",
      auth: {
        username: config.username,
        password: config.password
      }
    };
  } catch (error) {
    console.error("生成WebDAV上传URL出错:", error);
    throw new Error("无法生成WebDAV文件上传链接: " + (error.message || "未知错误"));
  }
}

/**
 * 生成WebDAV文件的下载URL和认证信息
 * @param {Object} webdavConfig - WebDAV配置
 * @param {string} storagePath - 存储路径
 * @param {string} encryptionSecret - 用于解密凭证的密钥
 * @param {boolean} forceDownload - 是否强制下载（而非预览）
 * @returns {Promise<Object>} 包含URL和认证信息的对象
 */
export async function generateWebDAVUrl(webdavConfig, storagePath, encryptionSecret, forceDownload = false) {
  try {
    const config = await createWebDAVConfig(webdavConfig, encryptionSecret);
    
    // 确保storagePath不以斜杠开始
    const normalizedPath = storagePath.startsWith("/") ? storagePath.slice(1) : storagePath;
    
    // 提取文件名，用于Content-Disposition头
    const fileName = normalizedPath.split("/").pop();
    
    // 构建下载URL
    const url = `${config.endpoint}${normalizedPath}`;
    
    // 返回WebDAV下载信息
    return {
      url,
      auth: {
        username: config.username,
        password: config.password
      },
      fileName,
      forceDownload
    };
  } catch (error) {
    console.error("生成WebDAV下载URL出错:", error);
    throw new Error("无法生成WebDAV文件下载链接: " + (error.message || "未知错误"));
  }
}

/**
 * 从WebDAV服务器删除文件
 * @param {Object} webdavConfig - WebDAV配置
 * @param {string} storagePath - 存储路径
 * @param {string} encryptionSecret - 用于解密凭证的密钥
 * @returns {Promise<boolean>} 删除操作是否成功
 */
export async function deleteFileFromWebDAV(webdavConfig, storagePath, encryptionSecret) {
  try {
    const config = await createWebDAVConfig(webdavConfig, encryptionSecret);
    
    // 确保storagePath不以斜杠开始
    const normalizedPath = storagePath.startsWith("/") ? storagePath.slice(1) : storagePath;
    
    // 构建文件URL
    const url = `${config.endpoint}${normalizedPath}`;
    
    // 创建认证头
    const authHeader = createBasicAuthHeader(config.username, config.password);
    
    // 准备DELETE请求
    const response = await fetch(url, {
      method: 'DELETE',
      headers: {
        'Authorization': authHeader
      }
    });
    
    if (response.ok || response.status === 404) {
      console.log(`成功从WebDAV存储中删除文件: ${storagePath}`);
      return true;
    } else {
      throw new Error(`WebDAV删除文件失败: HTTP状态码 ${response.status}`);
    }
  } catch (error) {
    console.error(`从WebDAV删除文件错误: ${error.message || error}`);
    return false;
  }
}

/**
 * 测试WebDAV服务器连接
 * @param {Object} webdavConfig - WebDAV配置
 * @param {string} encryptionSecret - 用于解密凭证的密钥
 * @returns {Promise<Object>} 测试结果对象
 */
export async function testWebDAVConnection(webdavConfig, encryptionSecret) {
  try {
    const config = await createWebDAVConfig(webdavConfig, encryptionSecret);
    
    // 创建认证头
    const authHeader = createBasicAuthHeader(config.username, config.password);
    
    // 准备PROPFIND请求(查询根目录)
    const response = await fetch(config.endpoint, {
      method: 'PROPFIND',
      headers: {
        'Authorization': authHeader,
        'Depth': '0',
        'Content-Type': 'application/xml'
      }
    });
    
    if (response.ok) {
      return { 
        success: true, 
        message: "WebDAV连接测试成功",
        endpoint: config.endpoint
      };
    } else {
      throw new Error(`WebDAV连接测试失败: HTTP状态码 ${response.status}`);
    }
  } catch (error) {
    console.error("WebDAV连接测试错误:", error);
    return {
      success: false,
      message: `WebDAV连接测试失败: ${error.message || "未知错误"}`,
      error: error.message || "未知错误"
    };
  }
}

/**
 * 创建WebDAV目录
 * @param {Object} webdavConfig - WebDAV配置
 * @param {string} directoryPath - 目录路径
 * @param {string} encryptionSecret - 用于解密凭证的密钥
 * @returns {Promise<boolean>} 创建操作是否成功
 */
export async function createWebDAVDirectory(webdavConfig, directoryPath, encryptionSecret) {
  try {
    const config = await createWebDAVConfig(webdavConfig, encryptionSecret);
    
    // 确保directoryPath不以斜杠开始，但以斜杠结尾
    let normalizedPath = directoryPath.startsWith("/") ? directoryPath.slice(1) : directoryPath;
    normalizedPath = normalizedPath.endsWith("/") ? normalizedPath : normalizedPath + "/";
    
    // 构建目录URL
    const url = `${config.endpoint}${normalizedPath}`;
    
    // 创建认证头
    const authHeader = createBasicAuthHeader(config.username, config.password);
    
    // 准备MKCOL请求
    const response = await fetch(url, {
      method: 'MKCOL',
      headers: {
        'Authorization': authHeader
      }
    });
    
    if (response.ok || response.status === 201 || response.status === 405) {
      // 201: Created, 405: Method Not Allowed (可能目录已存在)
      console.log(`WebDAV目录已创建或已存在: ${directoryPath}`);
      return true;
    } else {
      throw new Error(`WebDAV创建目录失败: HTTP状态码 ${response.status}`);
    }
  } catch (error) {
    console.error(`WebDAV创建目录错误: ${error.message || error}`);
    return false;
  }
} 