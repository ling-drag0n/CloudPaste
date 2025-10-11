/**
 * 统一认证服务
 * 职责：具体的认证逻辑、身份识别、权限检查
 */

import { Permission, PermissionChecker } from "../constants/permissions.js";
import { validateAdminToken } from "./adminService.js";
import { checkAndDeleteExpiredApiKey } from "./apiKeyService.js";
import { verifyPassword } from "../utils/crypto.js";
import { RepositoryFactory } from "../repositories/index.js";

/**
 * 认证结果类
 * 简洁设计，只包含网关需要的核心属性和方法
 */
export class AuthResult {
  constructor({ isAuthenticated = false, userId = null, permissions = 0, basicPath = "/", isAdmin = false, keyInfo = null } = {}) {
    this.isAuthenticated = isAuthenticated;
    this.userId = userId;
    this.permissions = permissions;
    this.basicPath = basicPath;
    this._isAdmin = isAdmin;
    this.keyInfo = keyInfo;
  }

  /**
   * 检查是否有指定权限
   * 网关需要的核心方法
   */
  hasPermission(permissionFlag) {
    // 管理员拥有所有权限
    if (this._isAdmin) {
      return true;
    }
    return PermissionChecker.hasPermission(this.permissions, permissionFlag);
  }

  /**
   * 检查是否为管理员
   * 网关需要的核心方法
   */
  isAdmin() {
    return this._isAdmin;
  }

  /**
   * 获取用户ID
   * 网关工具函数需要的方法
   */
  getUserId() {
    return this.userId;
  }

  /**
   * 获取用户类型
   * WebDAV认证需要的方法
   */
  getUserType() {
    if (this._isAdmin) {
      return "admin";
    }
    if (this.keyInfo) {
      return "apiKey";
    }
    return "unknown";
  }

  /**
   * 检查是否有任一权限
   * 网关权限验证需要的方法
   */
  hasAnyPermission(permissionFlags) {
    // 管理员拥有所有权限
    if (this._isAdmin) {
      return true;
    }
    return PermissionChecker.hasAnyPermission(this.permissions, permissionFlags);
  }

  /**
   * 检查是否有所有权限
   * 网关权限验证需要的方法
   */
  hasAllPermissions(permissionFlags) {
    // 管理员拥有所有权限
    if (this._isAdmin) {
      return true;
    }
    return PermissionChecker.hasAllPermissions(this.permissions, permissionFlags);
  }
}

/**
 * 认证服务类 - 基于位标志权限系统
 */
export class AuthService {
  constructor(db) {
    this.db = db;
  }

  /**
   * 解析认证头
   */
  parseAuthHeader(authHeader) {
    if (!authHeader) {
      return { type: null, token: null };
    }

    const parts = authHeader.split(" ");
    if (parts.length !== 2) {
      return { type: null, token: null };
    }

    const [type, token] = parts;
    return {
      type: type.toLowerCase(),
      token: token,
    };
  }

  /**
   * 验证管理员认证
   */
  async validateAdminAuth(token) {
    try {
      const adminId = await validateAdminToken(this.db, token);
      if (!adminId) {
        return new AuthResult();
      }

      return new AuthResult({
        isAuthenticated: true,
        userId: adminId,
        permissions: 0,
        isAdmin: true,
      });
    } catch (error) {
      console.error("管理员认证失败:", error);
      return new AuthResult();
    }
  }

  /**
   * 验证API密钥认证
   */
  async validateApiKeyAuth(apiKey) {
    try {
      const repositoryFactory = new RepositoryFactory(this.db);
      const apiKeyRepository = repositoryFactory.getApiKeyRepository();
      const keyRecord = await apiKeyRepository.findByKey(apiKey);

      if (!keyRecord) {
        return new AuthResult();
      }

      // 检查是否过期
      if (await checkAndDeleteExpiredApiKey(this.db, keyRecord)) {
        return new AuthResult();
      }

      // 更新最后使用时间
      await apiKeyRepository.updateLastUsed(keyRecord.id);

      return new AuthResult({
        isAuthenticated: true,
        userId: keyRecord.id,
        permissions: keyRecord.permissions || 0,
        basicPath: keyRecord.basic_path || "/",
        keyInfo: {
          id: keyRecord.id,
          name: keyRecord.name,
          key: keyRecord.key,
          basicPath: keyRecord.basic_path || "/",
          permissions: keyRecord.permissions || 0,
          role: keyRecord.role || "GENERAL",
          isGuest: keyRecord.is_guest === 1,
        },
      });
    } catch (error) {
      console.error("API密钥认证失败:", error);
      return new AuthResult();
    }
  }

  /**
   * 验证Basic认证（用于WebDAV）
   */
  async validateBasicAuth(token) {
    try {
      const credentials = Buffer.from(token, "base64").toString("utf-8");
      const [username, password] = credentials.split(":");

      if (!username || !password) {
        return new AuthResult();
      }

      // 尝试管理员认证
      try {
        const repositoryFactory = new RepositoryFactory(this.db);
        const adminRepository = repositoryFactory.getAdminRepository();
        const adminRecord = await adminRepository.findByUsername(username);

        if (adminRecord && (await verifyPassword(password, adminRecord.password))) {
          return new AuthResult({
            isAuthenticated: true,
            userId: adminRecord.id,
            permissions: 0,
            isAdmin: true,
          });
        }
      } catch (error) {
        console.error("WebDAV认证: 管理员验证过程出错", error);
      }

      // 尝试API密钥认证
      try {
        const repositoryFactory = new RepositoryFactory(this.db);
        const apiKeyRepository = repositoryFactory.getApiKeyRepository();
        const keyRecord = await apiKeyRepository.findByKey(username);

        if (keyRecord) {
          // 检查WebDAV权限 - 至少需要读取权限才能进行WebDAV认证
          const hasWebDAVPermission = PermissionChecker.hasPermission(keyRecord.permissions || 0, Permission.WEBDAV_READ);

          if (hasWebDAVPermission) {
            // 对于API密钥，用户名和密码应相同
            if (username === password) {
              // 检查是否过期
              if (await checkAndDeleteExpiredApiKey(this.db, keyRecord)) {
                return new AuthResult();
              }

              // 更新最后使用时间
              await apiKeyRepository.updateLastUsed(keyRecord.id);

              return new AuthResult({
                isAuthenticated: true,
                userId: keyRecord.id,
                permissions: keyRecord.permissions || 0,
                basicPath: keyRecord.basic_path || "/",
                keyInfo: {
                  id: keyRecord.id,
                  name: keyRecord.name,
                  key: keyRecord.key,
                  basicPath: keyRecord.basic_path || "/",
                  permissions: keyRecord.permissions || 0,
                  role: keyRecord.role || "GENERAL",
                  isGuest: keyRecord.is_guest === 1,
                },
              });
            }
          }
        }
      } catch (error) {
        console.error("WebDAV认证: API密钥验证过程出错", error);
      }

      return new AuthResult();
    } catch (error) {
      console.error("Basic认证验证失败:", error);
      return new AuthResult();
    }
  }

  /**
   * 统一认证方法
   * 网关需要的核心方法
   */
  async authenticate(authHeader) {
    const { type, token } = this.parseAuthHeader(authHeader);

    if (!type || !token) {
      return new AuthResult();
    }

    let result;
    switch (type) {
      case "bearer":
        result = await this.validateAdminAuth(token);
        break;
      case "apikey":
        result = await this.validateApiKeyAuth(token);
        break;
      case "basic":
        result = await this.validateBasicAuth(token);
        break;
      default:
        result = new AuthResult();
    }

    return result;
  }

  /**
   * 检查路径权限
   * 网关需要的核心方法
   */
  checkPathPermission(authResult, requestPath) {
    if (!authResult.isAuthenticated) {
      return false;
    }

    // 管理员有所有路径权限
    if (authResult.isAdmin()) {
      return true;
    }

    // API密钥检查基础路径权限
    const basicPath = authResult.basicPath || "/";
    return this.checkBasicPathPermission(basicPath, requestPath);
  }

  /**
   * 检查基础路径权限
   */
  checkBasicPathPermission(basicPath, requestPath) {
    if (!basicPath || !requestPath) {
      return false;
    }

    // 标准化路径
    const normalizeBasicPath = basicPath === "/" ? "/" : basicPath.replace(/\/+$/, "");
    const normalizeRequestPath = requestPath.replace(/\/+$/, "") || "/";

    // 如果基本路径是根路径，允许所有访问
    if (normalizeBasicPath === "/") {
      return true;
    }

    // 检查请求路径是否在基本路径范围内
    return normalizeRequestPath === normalizeBasicPath || normalizeRequestPath.startsWith(normalizeBasicPath + "/");
  }
}

/**
 * 创建认证服务实例
 */
export function createAuthService(db) {
  return new AuthService(db);
}
