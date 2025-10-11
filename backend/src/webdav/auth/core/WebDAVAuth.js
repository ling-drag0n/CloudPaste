/**
 * WebDAV认证核心
 */

import { getMethodPermission } from "../config/WebDAVConfig.js";
import { authGateway } from "../../../middlewares/authGatewayMiddleware.js";
import { MountManager } from "../../../storage/managers/MountManager.js";
import { processWebDAVPath } from "../../utils/webdavUtils.js";

/**
 * 认证结果类型枚举
 */
export const AuthResultType = {
  SUCCESS: "success",
  CHALLENGE: "challenge",
  FORBIDDEN: "forbidden",
  UNAUTHORIZED: "unauthorized",
  ERROR: "error",
};

/**
 * WebDAV认证核心类
 */
export class WebDAVAuth {
  constructor(db) {
    this.db = db;
  }

  /**
   * 权限检查
   * @param {Object} authResult - 认证结果
   * @param {string} method - HTTP方法
   * @returns {boolean} 是否有权限
   */
  checkWebDAVPermission(authResult, method) {
    // 管理员绕过所有检查
    if (authResult.isAdmin()) {
      return true;
    }

    // 获取所需权限
    const requiredPermission = getMethodPermission(method);
    if (!requiredPermission) {
      return false;
    }

    // 检查权限
    return authResult.hasPermission(requiredPermission);
  }

  /**
   * 验证WebDAV路径权限
   * 检查API密钥用户是否有权限访问指定路径
   * @param {Object} keyInfo - API密钥信息
   * @param {string} path - 请求路径
   * @param {string} method - HTTP方法
   * @param {Object} c - Hono上下文
   * @returns {Promise<boolean>} 是否有权限
   */
  async validateWebDAVPathPermission(keyInfo, path, method, c) {
    try {
      // 1. 检查基础路径权限
      const basicPath = keyInfo.basicPath || "/";
      if (!this.checkBasicPathPermission(basicPath, path)) {
        console.log(`WebDAV基础路径权限检查失败: basicPath=${basicPath}, requestPath=${path}`);
        return false;
      }

      // 2. 检查挂载点权限
      const mountManager = new MountManager(this.db, c.env.ENCRYPTION_SECRET);

      try {
        const { mount } = await mountManager.getDriverByPath(path, keyInfo, "apiKey");

        // 3. 验证API密钥是否有权限访问该挂载点
        const accessibleMounts = await authGateway.utils.getAccessibleMounts(this.db, keyInfo, "apiKey");
        const isAccessible = accessibleMounts.some((m) => m.id === mount.id);

        if (!isAccessible) {
          console.log(`WebDAV挂载点权限检查失败: 用户无权限访问挂载点 ${mount.name}`);
          return false;
        }

        return true;
      } catch (mountError) {
        console.log(`WebDAV挂载点检查失败: ${mountError.message}`);
        return false;
      }
    } catch (error) {
      console.error("WebDAV路径权限检查失败:", error);
      return false;
    }
  }

  /**
   * 检查基础路径权限
   * @param {string} basicPath - 用户的基础路径
   * @param {string} requestPath - 请求的路径
   * @returns {boolean} 是否有权限
   */
  checkBasicPathPermission(basicPath, requestPath) {
    if (!basicPath || basicPath === "/") {
      return true; // 根路径权限
    }

    // 规范化路径
    const normalizedBasicPath = basicPath.endsWith("/") ? basicPath : basicPath + "/";
    const normalizedRequestPath = requestPath.startsWith("/") ? requestPath : "/" + requestPath;

    // 检查请求路径是否在基础路径范围内
    return normalizedRequestPath.startsWith(normalizedBasicPath) || normalizedRequestPath === basicPath;
  }

  /**
   * 生成认证挑战 - 符合RFC 4918 WebDAV标准
   * 发送Basic认证挑战
   * @returns {Object} 认证挑战结果
   */
  generateAuthChallenge() {
    return {
      type: AuthResultType.CHALLENGE,
      message: "需要认证",
      headers: {
        "WWW-Authenticate": 'Basic realm="WebDAV"',
      },
    };
  }

  /**
   * 创建中间件
   * @returns {Function} 中间件函数
   */
  createMiddleware() {
    return async (c, next) => {
      try {
        // 获取并处理请求路径
        const url = new URL(c.req.url);
        const rawPath = url.pathname;
        let requestPath = this.processPath(rawPath);

        // OPTIONS 方法特殊处理 - 允许未认证访问进行能力发现
        if (c.req.method === "OPTIONS") {
          // 直接跳过认证，不设置用户类型（保持undefined状态）
          return await next();
        }

        // 统一认证处理
        const authResult = await this.performUnifiedAuth(c, requestPath);

        if (authResult.type === AuthResultType.SUCCESS) {
          // 设置认证信息到上下文
          c.set("webdavAuth", authResult);
          c.set("userType", authResult.userType);
          c.set("userId", authResult.userId);
          return await next();
        } else if (authResult.type === AuthResultType.CHALLENGE) {
          // 返回认证挑战
          return new Response("Unauthorized", {
            status: 401,
            headers: authResult.headers,
          });
        } else {
          // 认证失败
          return new Response(authResult.message, {
            status: authResult.type === AuthResultType.FORBIDDEN ? 403 : 401,
          });
        }
      } catch (error) {
        console.error("WebDAV中间件错误:", error);
        return new Response("Internal Server Error", { status: 500 });
      }
    };
  }

  /**
   * 统一路径处理 - 使用统一的路径处理函数
   * @param {string} rawPath - 原始路径
   * @returns {string} 处理后的路径
   */
  processPath(rawPath) {
    const processedPath = processWebDAVPath(rawPath, false);
    return processedPath || rawPath; // 如果处理失败，返回原始路径
  }

  /**
   * 统一认证处理
   * @param {Object} c - Hono上下文
   * @param {string} requestPath - 请求路径
   * @returns {Promise<Object>} 认证结果
   */
  async performUnifiedAuth(c, requestPath) {
    try {
      // 执行基础认证
      await authGateway.performAuth(c);
      const authResult = authGateway.utils.getAuthResult(c);

      if (!authResult || !authResult.isAuthenticated) {
        return this.generateAuthChallenge();
      }

      // 方法权限检查
      if (!this.checkWebDAVPermission(authResult, c.req.method)) {
        return {
          type: AuthResultType.FORBIDDEN,
          message: "方法权限不足",
        };
      }

      // 路径权限检查（仅对非管理员用户）
      if (!authResult.isAdmin() && authResult.keyInfo) {
        const hasPathPermission = await this.validateWebDAVPathPermission(authResult.keyInfo, requestPath, c.req.method, c);
        if (!hasPathPermission) {
          return {
            type: AuthResultType.FORBIDDEN,
            message: "路径权限不足",
          };
        }
      }

      // 准备用户信息
      const userType = authGateway.utils.getUserType(c);
      let userId = authGateway.utils.getUserId(c);

      // 对于API密钥用户，传递完整的keyInfo对象
      if (userType === "apiKey" && authResult.keyInfo) {
        userId = authResult.keyInfo;
      }

      return {
        type: AuthResultType.SUCCESS,
        authResult: authResult,
        userType: userType,
        userId: userId,
      };
    } catch (error) {
      console.error("WebDAV统一认证错误:", error);
      return {
        type: AuthResultType.ERROR,
        message: "认证失败",
      };
    }
  }
}

/**
 * 创建WebDAV认证实例
 * @param {D1Database} db - 数据库实例
 * @returns {WebDAVAuth} 认证实例
 */
export function createWebDAVAuth(db) {
  return new WebDAVAuth(db);
}
