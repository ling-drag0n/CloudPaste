/**
 * WebDAV锁定工具函数
 * 提供锁定相关的工具函数，包括令牌生成、XML解析和响应构建
 */

import { XMLParser } from "fast-xml-parser";

/**
 * 生成WebDAV锁令牌
 * @param {number} tokenId - 令牌ID
 * @returns {string} 锁令牌
 */
export function generateLockToken(tokenId) {
  // 生成UUID风格的令牌
  const timestamp = Date.now().toString(36);
  const random = Math.random().toString(36).substring(2);
  const id = tokenId.toString(36);

  return `opaquelocktoken:${timestamp}-${random}-${id}`;
}

/**
 * 解析LOCK请求的XML内容
 * @param {string} xmlBody - XML请求体
 * @returns {Object} 解析后的锁定信息
 */
export function parseLockXML(xmlBody) {
  if (!xmlBody || typeof xmlBody !== "string") {
    throw new Error("无效的XML请求体");
  }

  try {
    const parser = new XMLParser({
      ignoreAttributes: false,
      attributeNamePrefix: "@_",
      textNodeName: "#text",
      parseAttributeValue: true,
      trimValues: true,
    });

    const parsed = parser.parse(xmlBody);
    console.log("LOCK XML解析结果:", JSON.stringify(parsed, null, 2));

    // 查找lockinfo元素
    let lockinfo = null;

    // 递归查找lockinfo元素，支持任意命名空间前缀
    function findElementByLocalName(obj, localName) {
      if (!obj || typeof obj !== "object") return null;

      for (const key in obj) {
        // 检查是否是目标元素（支持带命名空间前缀）
        if (key === localName || key.endsWith(":" + localName)) {
          return obj[key];
        }

        // 递归查找
        if (typeof obj[key] === "object") {
          const found = findElementByLocalName(obj[key], localName);
          if (found) return found;
        }
      }
      return null;
    }

    lockinfo = findElementByLocalName(parsed, "lockinfo");

    if (!lockinfo) {
      throw new Error("无效的LOCK请求：缺少lockinfo元素");
    }

    // 解析锁定范围
    let scope = "exclusive"; // 默认为exclusive
    const scopeElement = findElementByLocalName(lockinfo, "lockscope");
    if (scopeElement) {
      const sharedElement = findElementByLocalName(scopeElement, "shared");
      if (sharedElement) {
        scope = "shared";
      }
    }

    // 解析锁定类型
    let type = "write"; // 默认为write
    const typeElement = findElementByLocalName(lockinfo, "locktype");
    if (typeElement) {
      const readElement = findElementByLocalName(typeElement, "read");
      if (readElement) {
        type = "read";
      }
    }

    // 解析所有者信息
    let owner = "unknown";
    const ownerElement = findElementByLocalName(lockinfo, "owner");
    if (ownerElement) {
      if (typeof ownerElement === "string") {
        owner = ownerElement;
      } else if (ownerElement["#text"]) {
        owner = ownerElement["#text"];
      } else {
        const hrefElement = findElementByLocalName(ownerElement, "href");
        if (hrefElement) {
          owner = typeof hrefElement === "string" ? hrefElement : hrefElement["#text"] || "unknown";
        }
      }
    }

    return {
      scope,
      type,
      owner: String(owner).trim() || "unknown",
    };
  } catch (error) {
    console.error("LOCK XML解析错误:", error);
    throw new Error(`XML解析失败: ${error.message}`);
  }
}

/**
 * 解析Timeout头
 * @param {string} timeoutHeader - Timeout头的值
 * @returns {number} 超时时间（秒）
 */
export function parseTimeoutHeader(timeoutHeader) {
  if (!timeoutHeader) {
    return 600; // 默认10分钟
  }

  // 解析格式：Second-3600 或 Infinite
  if (timeoutHeader.toLowerCase() === "infinite") {
    return 7200; // 最大2小时
  }

  const match = timeoutHeader.match(/Second-(\d+)/i);
  if (match) {
    const seconds = parseInt(match[1], 10);
    // 限制最大超时时间为2小时
    return Math.min(seconds, 7200);
  }

  return 600; // 默认10分钟
}

/**
 * 解析Depth头
 * @param {string} depthHeader - Depth头的值
 * @returns {string} 深度值
 */
export function parseDepthHeader(depthHeader) {
  if (!depthHeader) {
    return "0"; // 默认深度为0
  }

  const depth = depthHeader.toLowerCase();
  if (depth === "infinity") {
    return "infinity";
  }

  return "0"; // 只支持0和infinity
}

/**
 * 构建LOCK响应XML
 * @param {string} path - 资源路径
 * @param {Object} lockInfo - 锁定信息
 * @returns {string} XML响应
 */
export function buildLockResponseXML(path, lockInfo) {
  const href = escapeXmlChars(path);
  const token = escapeXmlChars(lockInfo.token);
  const owner = escapeXmlChars(lockInfo.owner);
  const timeout = `Second-${lockInfo.timeoutSeconds}`;

  return `<?xml version="1.0" encoding="utf-8"?>
<D:prop xmlns:D="DAV:">
  <D:lockdiscovery>
    <D:activelock>
      <D:locktype><D:${lockInfo.type}/></D:locktype>
      <D:lockscope><D:${lockInfo.scope}/></D:lockscope>
      <D:depth>${lockInfo.depth}</D:depth>
      <D:owner>${owner}</D:owner>
      <D:timeout>${timeout}</D:timeout>
      <D:locktoken>
        <D:href>${token}</D:href>
      </D:locktoken>
      <D:lockroot>
        <D:href>${href}</D:href>
      </D:lockroot>
    </D:activelock>
  </D:lockdiscovery>
</D:prop>`;
}

/**
 * 构建lockdiscovery属性XML（用于PROPFIND）
 * 注意：只返回内容，不包含外层<D:lockdiscovery>标签，因为会被PROPFIND自动包装
 * @param {Object} lockInfo - 锁定信息
 * @returns {string} lockdiscovery内容XML
 */
export function buildLockDiscoveryXML(lockInfo) {
  if (!lockInfo) {
    return ""; // 返回空内容，外层会生成<D:lockdiscovery/>
  }

  const token = escapeXmlChars(lockInfo.token);
  const owner = escapeXmlChars(lockInfo.owner);
  const timeout = `Second-${lockInfo.timeoutSeconds}`;

  return `
  <D:activelock>
    <D:locktype><D:${lockInfo.type}/></D:locktype>
    <D:lockscope><D:${lockInfo.scope}/></D:lockscope>
    <D:depth>${lockInfo.depth}</D:depth>
    <D:owner>${owner}</D:owner>
    <D:timeout>${timeout}</D:timeout>
    <D:locktoken>
      <D:href>${token}</D:href>
    </D:locktoken>
  </D:activelock>
`;
}

/**
 * 解析Lock-Token头
 * @param {string} lockTokenHeader - Lock-Token头的值
 * @returns {string|null} 锁令牌
 */
export function parseLockTokenHeader(lockTokenHeader) {
  if (!lockTokenHeader) {
    return null;
  }

  // 格式：<opaquelocktoken:token>
  const match = lockTokenHeader.match(/<([^>]+)>/);
  if (match) {
    return match[1];
  }

  return null;
}

/**
 * 转义XML特殊字符
 * @param {string} str - 要转义的字符串
 * @returns {string} 转义后的字符串
 */
function escapeXmlChars(str) {
  if (typeof str !== "string") {
    return String(str);
  }

  return str.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;").replace(/'/g, "&#39;");
}

/**
 * 验证锁定冲突
 * @param {Object} existingLock - 现有锁定
 * @param {string} requestedScope - 请求的锁定范围
 * @returns {boolean} 是否有冲突
 */
export function hasLockConflict(existingLock, requestedScope) {
  if (!existingLock) {
    return false;
  }

  // exclusive锁与任何锁都冲突
  if (existingLock.scope === "exclusive" || requestedScope === "exclusive") {
    return true;
  }

  // shared锁之间不冲突
  return false;
}

/**
 * 格式化锁定信息用于日志
 * @param {Object} lockInfo - 锁定信息
 * @returns {string} 格式化的字符串
 */
export function formatLockInfo(lockInfo) {
  if (!lockInfo) {
    return "无锁定";
  }

  const remainingTime = Math.max(0, Math.floor((lockInfo.expiresAt - Date.now()) / 1000));
  return `${lockInfo.scope} ${lockInfo.type} lock by ${lockInfo.owner} (${remainingTime}s remaining)`;
}

/**
 * 解析RFC 4918标准的If头
 * 支持格式：If: </path> (<locktoken>) (["etag"]) 或 If: (<locktoken>)
 * @param {string} ifHeader - If头的值
 * @returns {Object} 解析结果
 */
export function parseIfHeaderRFC4918(ifHeader) {
  if (!ifHeader || typeof ifHeader !== "string") {
    return { conditions: [] };
  }

  const conditions = [];

  try {
    // 移除多余的空格
    const cleanHeader = ifHeader.trim();

    // 简化的解析：主要提取锁令牌
    // 支持格式：If: (<locktoken>) 或 If: </path> (<locktoken>)
    const tokenRegex = /<(opaquelocktoken:[^>]+)>/g;
    const pathRegex = /<(\/[^>]*)>/g;

    let match;
    const tokens = [];
    const paths = [];

    // 提取所有锁令牌
    while ((match = tokenRegex.exec(cleanHeader)) !== null) {
      tokens.push(match[1]);
    }

    // 提取所有路径
    tokenRegex.lastIndex = 0; // 重置正则
    while ((match = pathRegex.exec(cleanHeader)) !== null) {
      const value = match[1];
      // 如果不是锁令牌，则认为是路径
      if (!value.startsWith("opaquelocktoken:")) {
        paths.push(value);
      }
    }

    // 构建条件对象
    if (tokens.length > 0) {
      conditions.push({
        path: paths.length > 0 ? paths[0] : null,
        tokens: tokens,
        etags: [], // 暂不支持ETag解析
      });
    }

    return { conditions };
  } catch (error) {
    console.error("If头解析错误:", error);
    return { conditions: [] };
  }
}

/**
 * 检查If头条件是否满足
 * @param {Object} parsedIf - 解析后的If头
 * @param {string} resourcePath - 资源路径
 * @param {string} lockToken - 当前资源的锁令牌
 * @returns {boolean} 条件是否满足
 */
export function checkIfConditions(parsedIf, resourcePath, lockToken) {
  if (!parsedIf || !parsedIf.conditions || parsedIf.conditions.length === 0) {
    return true; // 没有条件，允许操作
  }

  for (const condition of parsedIf.conditions) {
    // 检查路径匹配（如果指定了路径）
    if (condition.path && condition.path !== resourcePath) {
      continue; // 路径不匹配，检查下一个条件
    }

    // 检查锁令牌匹配
    if (condition.tokens && condition.tokens.length > 0) {
      if (condition.tokens.includes(lockToken)) {
        return true; // 找到匹配的令牌
      }
    }
  }

  return false; // 没有满足的条件
}

/**
 * 检查路径的锁定状态并验证权限
 * 用于在WebDAV写操作前进行锁定检查
 * @param {Object} lockManager - 锁定管理器实例
 * @param {string} path - 资源路径
 * @param {string} ifHeader - If头的值（可选）
 * @param {string} operation - 操作名称（用于日志）
 * @returns {Object|null} 如果有锁定冲突返回错误信息，否则返回null
 */
export function checkLockPermission(lockManager, path, ifHeader = null, operation = "操作") {
  const lockInfo = lockManager.checkLock(path, ifHeader);
  if (lockInfo) {
    console.log(`${operation}被锁定阻止 - 路径: ${path}, 锁定信息: ${formatLockInfo(lockInfo)}`);
    return {
      status: 423,
      message: `资源被锁定: ${formatLockInfo(lockInfo)}`,
      lockToken: lockInfo.token,
      multiStatus: createLockConflictMultiStatus(path, lockInfo),
    };
  }

  return null;
}

/**
 * 创建锁定冲突的多状态响应
 * @param {string} path - 资源路径
 * @param {Object} lockInfo - 锁定信息
 * @returns {string} 多状态XML响应
 */
export function createLockConflictMultiStatus(path, lockInfo) {
  const href = escapeXmlChars(path);
  const lockToken = escapeXmlChars(lockInfo.token);
  const owner = escapeXmlChars(lockInfo.owner);

  return `<?xml version="1.0" encoding="utf-8"?>
<D:multistatus xmlns:D="DAV:">
  <D:response>
    <D:href>${href}</D:href>
    <D:status>HTTP/1.1 423 Locked</D:status>
    <D:error>
      <D:lock-token-submitted>
        <D:href>${lockToken}</D:href>
      </D:lock-token-submitted>
    </D:error>
    <D:responsedescription>Resource is locked by ${owner}</D:responsedescription>
  </D:response>
</D:multistatus>`;
}
