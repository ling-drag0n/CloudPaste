/**
 * WebDAV PROPFIND方法实现
 * 符合RFC 4918标准，支持propname、allprop、prop请求类型
 * 在单个文件内修复所有标准违反问题
 */

import { handleWebDAVError } from "../utils/errorUtils.js";
import { MountManager } from "../../storage/managers/MountManager.js";
import { FileSystem } from "../../storage/fs/FileSystem.js";
import { authGateway } from "../../middlewares/authGatewayMiddleware.js";
import { getMimeTypeFromFilename } from "../../utils/fileUtils.js";
import { lockManager } from "../utils/LockManager.js";
import { buildLockDiscoveryXML } from "../utils/lockUtils.js";
import { WEBDAV_BASE_PATH } from "../auth/config/WebDAVConfig.js";
import { getWebDAVMultiStatusHeaders } from "../utils/headerUtils.js";

// 导入虚拟目录处理函数
import { isVirtualPath, getVirtualDirectoryListing } from "../../storage/fs/utils/VirtualDirectory.js";

// ===== RFC 4918标准实现：请求类型枚举 =====
const PropfindRequestType = {
  PROPNAME: "propname",
  ALLPROP: "allprop",
  PROP: "prop",
};

// ===== RFC 4918标准实现：属性状态枚举 =====
const PropertyStatus = {
  OK: 200,
  NOT_FOUND: 404,
  FORBIDDEN: 403,
  CONFLICT: 409,
};

// ===== RFC 4918标准实现：请求体解析函数 =====

/**
 * 解析PROPFIND请求体
 * 符合RFC 4918标准，支持propname、allprop、prop三种请求类型
 * @param {string} requestBody - XML请求体
 * @returns {Object} 解析结果
 */
function parsePropfindRequest(requestBody) {
  // 如果请求体为空，按照RFC 4918标准应该当作allprop处理
  if (!requestBody || requestBody.trim() === "") {
    return {
      type: PropfindRequestType.ALLPROP,
      properties: [],
      include: [],
    };
  }

  try {
    const cleanBody = requestBody.trim();

    // 检查是否为propname请求
    if (cleanBody.includes("<D:propname") || cleanBody.includes("<propname")) {
      return {
        type: PropfindRequestType.PROPNAME,
        properties: [],
        include: [],
      };
    }

    // 检查是否为allprop请求
    if (cleanBody.includes("<D:allprop") || cleanBody.includes("<allprop")) {
      return {
        type: PropfindRequestType.ALLPROP,
        properties: [],
        include: [],
      };
    }

    // 检查是否为prop请求（指定属性）
    if (cleanBody.includes("<D:prop") || cleanBody.includes("<prop")) {
      const properties = parseRequestedProperties(cleanBody);
      return {
        type: PropfindRequestType.PROP,
        properties: properties,
        include: [],
      };
    }

    // 如果无法识别，默认当作allprop处理
    return {
      type: PropfindRequestType.ALLPROP,
      properties: [],
      include: [],
    };
  } catch (error) {
    console.error("解析PROPFIND请求体失败:", error);
    return {
      type: PropfindRequestType.ALLPROP,
      properties: [],
      include: [],
    };
  }
}

/**
 * 解析prop元素中请求的属性
 * @param {string} xmlBody - XML请求体
 * @returns {Array} 请求的属性列表
 */
function parseRequestedProperties(xmlBody) {
  const properties = [];

  try {
    // 查找prop元素
    const propMatch = xmlBody.match(/<D:prop[^>]*>(.*?)<\/D:prop>/s) || xmlBody.match(/<prop[^>]*>(.*?)<\/prop>/s);

    if (propMatch) {
      const propContent = propMatch[1];
      // 提取属性名称
      const propertyMatches = propContent.match(/<[^\/][^>]*>/g) || [];

      for (const match of propertyMatches) {
        const propertyName = extractPropertyName(match);
        if (propertyName) {
          properties.push(propertyName);
        }
      }
    }
  } catch (error) {
    console.error("解析请求属性失败:", error);
  }

  return properties;
}

/**
 * 从XML标签中提取属性名称
 * @param {string} xmlTag - XML标签
 * @returns {string|null} 属性名称
 */
function extractPropertyName(xmlTag) {
  try {
    const tagContent = xmlTag.replace(/[<>]/g, "");
    const cleanTag = tagContent.replace(/\/$/, "");
    const tagName = cleanTag.split(/\s+/)[0];
    const localName = tagName.includes(":") ? tagName.split(":")[1] : tagName;
    return localName || null;
  } catch (error) {
    console.error("提取属性名称失败:", error, xmlTag);
    return null;
  }
}

// ===== RFC 4918标准实现：属性管理函数 =====

/**
 * 获取所有标准DAV属性
 * 符合RFC 4918标准
 * @param {Object} item - 文件/目录项
 * @param {string} path - 资源路径
 * @returns {Object} 属性映射
 */
function getAllProperties(item, path) {
  const isDirectory = item.isDirectory || false;

  return {
    resourcetype: {
      value: isDirectory ? "<D:collection/>" : "",
      status: PropertyStatus.OK,
    },
    displayname: {
      value: escapeXmlChars(item.name || ""),
      status: PropertyStatus.OK,
    },
    getlastmodified: {
      value: item.modified ? new Date(item.modified).toUTCString() : null,
      status: item.modified ? PropertyStatus.OK : PropertyStatus.NOT_FOUND,
    },
    creationdate: {
      value: item.created ? new Date(item.created).toISOString() : null,
      status: item.created ? PropertyStatus.OK : PropertyStatus.NOT_FOUND,
    },
    getetag: {
      value: generateETag(item, path),
      status: PropertyStatus.OK,
    },
    getcontentlength: {
      value: !isDirectory ? (item.size || 0).toString() : null,
      status: !isDirectory ? PropertyStatus.OK : PropertyStatus.NOT_FOUND,
    },
    getcontenttype: {
      value: !isDirectory ? getMimeTypeFromFilename(item.name || "") : null,
      status: !isDirectory ? PropertyStatus.OK : PropertyStatus.NOT_FOUND,
    },
    getcontentlanguage: {
      value: null,
      status: PropertyStatus.NOT_FOUND,
    },
    lockdiscovery: {
      value: (() => {
        // 获取当前路径的锁定信息
        const lockInfo = lockManager.getLockByPath(item.path || path);
        return buildLockDiscoveryXML(lockInfo);
      })(),
      status: PropertyStatus.OK,
    },
    supportedlock: {
      value: `<D:lockentry>
        <D:lockscope><D:exclusive/></D:lockscope>
        <D:locktype><D:write/></D:locktype>
      </D:lockentry>`,
      status: PropertyStatus.OK,
    },
    source: {
      value: null,
      status: PropertyStatus.NOT_FOUND,
    },
  };
}

/**
 * 获取指定的属性
 * @param {Object} item - 文件/目录项
 * @param {string} path - 资源路径
 * @param {Array} requestedProperties - 请求的属性列表
 * @returns {Object} 属性映射
 */
function getRequestedProperties(item, path, requestedProperties) {
  const allProperties = getAllProperties(item, path);
  const result = {};

  for (const propertyName of requestedProperties) {
    if (allProperties[propertyName]) {
      result[propertyName] = allProperties[propertyName];
    } else {
      // 未知属性返回404
      result[propertyName] = {
        value: null,
        status: PropertyStatus.NOT_FOUND,
      };
    }
  }

  return result;
}

/**
 * 获取属性名称列表（用于propname请求）
 * @param {Object} item - 文件/目录项
 * @param {string} path - 资源路径
 * @returns {Object} 属性名称映射
 */
function getPropertyNames(item, path) {
  const allProperties = getAllProperties(item, path);
  const result = {};

  for (const [propertyName, propertyInfo] of Object.entries(allProperties)) {
    // 只返回存在的属性名称
    if (propertyInfo.status === PropertyStatus.OK) {
      result[propertyName] = {
        value: "", // propname请求不返回值，只返回名称
        status: PropertyStatus.OK,
      };
    }
  }

  return result;
}

// ===== RFC 4918标准实现：工具函数 =====

/**
 * 生成ETag
 * @param {Object} item - 文件/目录项
 * @param {string} path - 资源路径
 * @returns {string} ETag值
 */
function generateETag(item, path) {
  // 基于路径、修改时间和大小生成稳定的ETag
  const pathHash = simpleHash(path);
  const timeHash = item.modified ? new Date(item.modified).getTime().toString(16) : "0";
  const sizeHash = (item.size || 0).toString(16);

  return `"${pathHash}-${timeHash}-${sizeHash}"`;
}

/**
 * 简单哈希函数
 * @param {string} str - 输入字符串
 * @returns {string} 哈希值
 */
function simpleHash(str) {
  let hash = 0;
  for (let i = 0; i < str.length; i++) {
    const char = str.charCodeAt(i);
    hash = (hash << 5) - hash + char;
    hash = hash & hash; // 转换为32位整数
  }
  return Math.abs(hash).toString(16);
}

/**
 * 转义XML特殊字符
 * @param {string} text - 需要转义的文本
 * @returns {string} 转义后的文本
 */
function escapeXmlChars(text) {
  if (typeof text !== "string") return "";
  return text.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;").replace(/'/g, "&apos;");
}

/**
 * 编码URI路径
 * @param {string} path - 路径
 * @returns {string} 编码后的路径
 */
function encodeUriPath(path) {
  return path
    .split("/")
    .map((segment) => encodeURIComponent(segment))
    .join("/");
}

/**
 * 规范化路径（确保目录以/结尾，文件不以/结尾）
 * @param {string} path - 原始路径
 * @param {boolean} isDirectory - 是否为目录
 * @returns {string} 规范化后的路径
 */
function normalizePath(path, isDirectory) {
  if (isDirectory && !path.endsWith("/")) {
    return path + "/";
  } else if (!isDirectory && path.endsWith("/")) {
    return path.slice(0, -1);
  }
  return path;
}

/**
 * 根据请求类型获取属性
 * @param {Object} item - 文件/目录项
 * @param {string} path - 路径
 * @param {Object} requestInfo - 请求信息
 * @returns {Object} 属性映射
 */
function getPropertiesForRequest(item, path, requestInfo) {
  switch (requestInfo.type) {
    case PropfindRequestType.PROPNAME:
      return getPropertyNames(item, path);

    case PropfindRequestType.PROP:
      return getRequestedProperties(item, path, requestInfo.properties);

    case PropfindRequestType.ALLPROP:
    default:
      const allProperties = getAllProperties(item, path);

      // 如果有include属性，添加额外的属性
      if (requestInfo.include && requestInfo.include.length > 0) {
        const includeProperties = getRequestedProperties(item, path, requestInfo.include);
        return { ...allProperties, ...includeProperties };
      }

      return allProperties;
  }
}

// ===== RFC 4918标准实现：Multi-Status响应构建 =====

/**
 * 构建Multi-Status XML响应
 * 符合RFC 4918标准，支持属性级错误处理
 * @param {Array} responses - 响应数组
 * @returns {string} Multi-Status XML
 */
function buildMultiStatusXML(responses) {
  let xml = `<?xml version="1.0" encoding="utf-8"?>\n<D:multistatus xmlns:D="DAV:">`;

  for (const response of responses) {
    xml += buildResponseXML(response);
  }

  xml += `\n</D:multistatus>`;

  return xml;
}

/**
 * 构建单个响应的XML
 * @param {Object} response - 响应对象
 * @returns {string} 响应XML
 */
function buildResponseXML(response) {
  let xml = `
  <D:response>
    <D:href>${encodeUriPath(response.href)}</D:href>`;

  if (response.status) {
    // 资源级别的错误
    xml += `
    <D:status>HTTP/1.1 ${response.status} ${getStatusText(response.status)}</D:status>`;
  } else if (response.properties) {
    // 属性级别的响应
    const propstatGroups = groupPropertiesByStatus(response.properties);

    for (const propstat of propstatGroups) {
      xml += buildPropstatXML(propstat);
    }
  }

  xml += `
  </D:response>`;

  return xml;
}

/**
 * 按状态码分组属性
 * @param {Object} properties - 属性映射
 * @returns {Array} propstat组列表
 */
function groupPropertiesByStatus(properties) {
  const statusGroups = {};

  // 按状态码分组
  for (const [propertyName, propertyInfo] of Object.entries(properties)) {
    const status = propertyInfo.status;

    if (!statusGroups[status]) {
      statusGroups[status] = [];
    }

    statusGroups[status].push({
      name: propertyName,
      value: propertyInfo.value,
    });
  }

  // 转换为propstat数组
  const propstats = [];
  for (const [status, props] of Object.entries(statusGroups)) {
    propstats.push({
      properties: props,
      status: parseInt(status),
    });
  }

  return propstats;
}

/**
 * 构建propstat的XML
 * @param {Object} propstat - propstat对象
 * @returns {string} propstat XML
 */
function buildPropstatXML(propstat) {
  let xml = `
    <D:propstat>
      <D:prop>`;

  for (const property of propstat.properties) {
    if (property.value !== null && property.value !== undefined) {
      xml += `
        <D:${property.name}>${property.value}</D:${property.name}>`;
    } else {
      // 空属性（用于propname或不存在的属性）
      xml += `
        <D:${property.name}/>`;
    }
  }

  xml += `
      </D:prop>
      <D:status>HTTP/1.1 ${propstat.status} ${getStatusText(propstat.status)}</D:status>
    </D:propstat>`;

  return xml;
}

/**
 * 获取HTTP状态码对应的文本
 * @param {number} status - HTTP状态码
 * @returns {string} 状态文本
 */
function getStatusText(status) {
  const statusTexts = {
    200: "OK",
    403: "Forbidden",
    404: "Not Found",
    409: "Conflict",
    422: "Unprocessable Entity",
    423: "Locked",
    424: "Failed Dependency",
    507: "Insufficient Storage",
  };

  return statusTexts[status] || "Unknown";
}

/**
 * 创建标准错误响应
 * @param {string} href - 资源URI
 * @param {number} status - HTTP状态码
 * @param {string} message - 错误消息
 * @returns {Response} HTTP响应对象
 */
function createErrorResponse(href, status, message) {
  // 404等错误，直接返回对应的状态码
  // 客户端才能正确理解资源不存在后继续后续操作（如PUT上传）
  if (status === 404) {
    return new Response(message, {
      status: 404,
      headers: {
        "Content-Type": "text/plain; charset=utf-8",
        DAV: "1, 2",
      },
    });
  }

  // 对于其他错误，使用Multi-Status格式
  const xml = buildMultiStatusXML([
    {
      href: href,
      status: status,
      description: message,
    },
  ]);

  return new Response(xml, {
    status: status >= 400 ? status : 207,
    headers: {
      "Content-Type": "text/xml; charset=utf-8",
      DAV: "1, 2",
    },
  });
}

// ===== RFC 4918标准实现：主要处理函数 =====

/**
 * 主要的PROPFIND处理函数
 * 符合RFC 4918标准，修复所有标准违反问题
 * @param {Object} c - Hono上下文
 * @param {string} path - 请求路径（由WebDAV中间件解析）
 * @param {string|Object} userId - 用户ID或信息
 * @param {string} userType - 用户类型
 * @param {D1Database} db - 数据库实例
 * @returns {Response} HTTP响应
 */
export async function handlePropfind(c, path, userId, userType, db) {
  try {
    // 修复：Depth默认值应该是"infinity"（符合RFC 4918标准）
    const depth = c.req.header("Depth") || "infinity";

    // 验证depth值
    if (!["0", "1", "infinity"].includes(depth)) {
      return createErrorResponse(WEBDAV_BASE_PATH + path, 400, "Invalid Depth header value");
    }

    console.log(`WebDAV PROPFIND - 路径: ${path}, 深度: ${depth}`);

    // 修复：解析请求体（支持propname、allprop、prop三种类型）
    let requestBody = "";
    try {
      requestBody = await c.req.text();
    } catch (error) {
      console.log("PROPFIND请求体解析失败，当作allprop处理");
    }

    const requestInfo = parsePropfindRequest(requestBody);
    requestInfo.depth = depth;

    console.log("PROPFIND请求解析结果:", requestInfo);

    // 获取用户信息（适配WebDAV中间件的格式）
    let userIdOrInfo, actualUserType;
    if (userType === "admin") {
      userIdOrInfo = userId;
      actualUserType = "admin";
    } else if (userType === "apiKey") {
      // 对于API密钥用户，userId应该是完整的信息对象
      if (typeof userId === "object" && userId !== null) {
        userIdOrInfo = {
          id: userId.id,
          name: userId.name,
          basicPath: userId.basicPath,
          permissions: userId.permissions || {},
        };
      } else {
        return createErrorResponse("/dav" + path, 401, "API密钥信息格式错误");
      }
      actualUserType = "apiKey";
    } else {
      return createErrorResponse("/dav" + path, 401, "未知用户类型");
    }

    return await processPropfindRequest(path, requestInfo, userIdOrInfo, actualUserType, db, c.env.ENCRYPTION_SECRET);
  } catch (error) {
    console.error("PROPFIND处理失败:", error);
    return handleWebDAVError(error, "PROPFIND");
  }
}

/**
 * 处理PROPFIND请求的核心逻辑
 * @param {string} path - 请求路径
 * @param {Object} requestInfo - 解析后的请求信息
 * @param {string|Object} userIdOrInfo - 用户ID或信息
 * @param {string} actualUserType - 实际用户类型
 * @param {D1Database} db - 数据库实例
 * @param {string} encryptionSecret - 加密密钥
 * @returns {Response} HTTP响应
 */
async function processPropfindRequest(path, requestInfo, userIdOrInfo, actualUserType, db, encryptionSecret) {
  try {
    // 检查API密钥用户的路径权限
    if (actualUserType === "apiKey") {
      if (!authGateway.utils.checkPathPermissionForNavigation(userIdOrInfo.basicPath, path)) {
        return createErrorResponse(WEBDAV_BASE_PATH + path, 403, "没有权限访问此路径");
      }
    }

    // 获取用户可访问的挂载点列表
    const mounts = await authGateway.utils.getAccessibleMounts(db, userIdOrInfo, actualUserType);

    // 检查是否为虚拟路径
    if (isVirtualPath(path, mounts)) {
      // 处理虚拟目录
      const basicPath = actualUserType === "apiKey" ? userIdOrInfo.basicPath : null;
      return await handleVirtualDirectoryPropfind(mounts, path, basicPath, requestInfo);
    }

    // 处理实际存储路径
    const mountManager = new MountManager(db, encryptionSecret);
    const fileSystem = new FileSystem(mountManager);

    return await handleStoragePropfind(fileSystem, path, requestInfo, userIdOrInfo, actualUserType);
  } catch (error) {
    console.error("处理PROPFIND请求失败:", error);

    if (error.message && error.message.includes("权限")) {
      return createErrorResponse(WEBDAV_BASE_PATH + path, 403, "权限不足");
    } else if (error.message && error.message.includes("不存在")) {
      return createErrorResponse(WEBDAV_BASE_PATH + path, 404, "资源不存在");
    } else {
      return createErrorResponse(WEBDAV_BASE_PATH + path, 500, "内部服务器错误");
    }
  }
}

/**
 * 处理虚拟目录的PROPFIND请求
 * @param {Object} mounts - 挂载点列表
 * @param {string} path - 请求路径
 * @param {string} basicPath - 基础路径（API密钥用户）
 * @param {Object} requestInfo - 请求信息
 * @returns {Response} HTTP响应
 */
async function handleVirtualDirectoryPropfind(mounts, path, basicPath, requestInfo) {
  try {
    const result = await getVirtualDirectoryListing(mounts, path, basicPath);

    const responses = [];

    // 处理当前虚拟目录
    const webdavPath = "/dav" + normalizePath(path, true);
    const properties = getPropertiesForRequest(result, path, requestInfo);
    responses.push({
      href: webdavPath,
      properties: properties,
    });

    // 如果depth=1，添加子项
    if (requestInfo.depth === "1" && result.items) {
      for (const item of result.items) {
        const itemPath = WEBDAV_BASE_PATH + normalizePath(item.path, item.isDirectory);
        const itemProperties = getPropertiesForRequest(item, item.path, requestInfo);
        responses.push({
          href: itemPath,
          properties: itemProperties,
        });
      }
    }

    const xml = buildMultiStatusXML(responses);

    return new Response(xml, {
      status: 207, // Multi-Status
      headers: getWebDAVMultiStatusHeaders(),
    });
  } catch (error) {
    console.error("处理虚拟目录PROPFIND失败:", error);
    return createErrorResponse(WEBDAV_BASE_PATH + path, 500, "内部服务器错误");
  }
}

/**
 * 处理实际存储的PROPFIND请求
 * @param {Object} fileSystem - 文件系统实例
 * @param {string} path - 请求路径
 * @param {Object} requestInfo - 请求信息
 * @param {string|Object} userIdOrInfo - 用户ID或信息
 * @param {string} actualUserType - 实际用户类型
 * @returns {Response} HTTP响应
 */
async function handleStoragePropfind(fileSystem, path, requestInfo, userIdOrInfo, actualUserType) {
  try {
    let result;

    if (requestInfo.depth === "0") {
      // 只获取当前资源信息
      try {
        const fileInfo = await fileSystem.getFileInfo(path, userIdOrInfo, actualUserType);
        result = {
          path: path,
          isDirectory: fileInfo.isDirectory,
          name: fileInfo.name,
          size: fileInfo.size,
          modified: fileInfo.modified,
          created: fileInfo.created,
          items: [], // depth=0时不包含子项
        };
      } catch (error) {
        throw error;
      }
    } else if (requestInfo.depth === "1") {
      // 获取当前资源和直接子项
      try {
        result = await fileSystem.listDirectory(path, userIdOrInfo, actualUserType);
      } catch (error) {
        throw error;
      }
    } else {
      // infinity深度 - 大多数服务器会拒绝此请求
      return createErrorResponse("/dav" + path, 403, "Depth infinity is not supported for performance reasons");
    }

    const responses = [];

    // 处理当前资源
    // 修复：根据返回的数据结构判断是否为目录
    const isCurrentDirectory = result.type === "directory" || result.isDirectory === true;
    const webdavPath = "/dav" + normalizePath(path, isCurrentDirectory);

    // 修复：为当前资源构建正确的属性对象
    const currentResource = {
      ...result,
      isDirectory: isCurrentDirectory,
      name: result.name || path.split("/").filter(Boolean).pop() || "",
    };

    const properties = getPropertiesForRequest(currentResource, path, requestInfo);
    responses.push({
      href: webdavPath,
      properties: properties,
    });

    // 如果depth=1且是目录，处理子项
    if (requestInfo.depth === "1" && isCurrentDirectory && result.items) {
      for (const item of result.items) {
        // 修复：确保子项有正确的isDirectory属性
        const itemIsDirectory = item.isDirectory === true;
        const itemPath = "/dav" + normalizePath(item.path, itemIsDirectory);

        const itemProperties = getPropertiesForRequest(item, item.path, requestInfo);
        responses.push({
          href: itemPath,
          properties: itemProperties,
        });
      }
    }

    const xml = buildMultiStatusXML(responses);

    // // 调试：输出完整的XML内容（仅在depth=0时，避免过多日志）
    // if (requestInfo.depth === "0") {
    //   console.log(`WebDAV PROPFIND 完整XML响应:\n${xml}`);
    // } else {
    //   console.log(`WebDAV PROPFIND XML响应预览: ${xml.substring(0, 200)}...`);
    // }

    return new Response(xml, {
      status: 207, // Multi-Status
      headers: getWebDAVMultiStatusHeaders(),
    });
  } catch (error) {
    console.error("处理存储PROPFIND失败:", error);

    if (error.message && error.message.includes("权限")) {
      return createErrorResponse(WEBDAV_BASE_PATH + path, 403, "权限不足");
    } else if (error.message && error.message.includes("不存在")) {
      return createErrorResponse(WEBDAV_BASE_PATH + path, 404, "资源不存在");
    } else {
      return createErrorResponse(WEBDAV_BASE_PATH + path, 500, "内部服务器错误");
    }
  }
}
