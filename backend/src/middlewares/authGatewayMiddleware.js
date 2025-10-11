/**
 * 统一认证网关中间件
 *
 * - 分层验证：认证 → 权限 → 路径 → 自定义检查
 */

import { createAuthService } from "../services/authService.js";
import { Permission, PermissionChecker } from "../constants/permissions.js";
import { ApiStatus } from "../constants/index.js";
import { HTTPException } from "hono/http-exception";

/**
 * 统一认证网关中间件
 * 所有权限检查的统一入口
 *
 * @param {Object} config - 权限配置对象
 * @param {boolean} config.requireAuth - 是否需要认证（默认true）
 * @param {boolean} config.allowAdmin - 是否允许管理员绕过权限检查（默认true）
 * @param {Array} config.permissions - 需要的权限列表
 * @param {string} config.permissionMode - 权限检查模式：'any'（任一满足）或'all'（全部满足）
 * @param {boolean} config.pathCheck - 是否检查路径权限
 * @param {string} config.customPath - 自定义路径（用于路径权限检查）
 * @param {Function} config.customCheck - 自定义权限检查函数
 * @returns {Function} Hono中间件函数
 */
export const authGateway = (config = {}) => {
  return async (c, next) => {
    const { requireAuth = true, allowAdmin = true, permissions = [], permissionMode = "any", pathCheck = false, customPath = null, customCheck = null } = config;

    try {
      // 第一层：统一认证处理
      const authResult = await performAuthentication(c);

      // 第二层：认证检查
      if (requireAuth && !authResult.isAuthenticated) {
        throw new HTTPException(ApiStatus.UNAUTHORIZED, {
          message: "需要认证访问",
        });
      }

      // 第三层：管理员绕过检查
      if (allowAdmin && authResult.isAdmin()) {
        // 管理员绕过所有权限检查，直接通过
        await next();
        return;
      }

      // 第四层：权限验证
      if (permissions.length > 0) {
        const hasPermission = validatePermissions(authResult, permissions, permissionMode);
        if (!hasPermission) {
          const permissionNames = getPermissionNames(permissions);
          throw new HTTPException(ApiStatus.FORBIDDEN, {
            message: `需要以下权限: ${permissionNames.join(", ")}`,
          });
        }
      }

      // 第五层：路径权限检查
      if (pathCheck) {
        const pathPermissionResult = validatePathPermissions(authResult, c, customPath);
        if (!pathPermissionResult.success) {
          throw new HTTPException(ApiStatus.FORBIDDEN, {
            message: pathPermissionResult.message,
          });
        }
      }

      // 第六层：自定义权限检查
      if (customCheck && typeof customCheck === "function") {
        const authService = c.get("authService");
        const customResult = await customCheck(authResult, authService, c);
        if (!customResult) {
          throw new HTTPException(ApiStatus.FORBIDDEN, {
            message: "自定义权限检查失败",
          });
        }
      }

      await next();
    } catch (error) {
      if (error instanceof HTTPException) {
        throw error;
      }
      console.error("认证网关错误:", error);
      throw new HTTPException(ApiStatus.INTERNAL_ERROR, {
        message: "认证处理失败: " + error.message,
      });
    }
  };
};

/**
 * 统一认证处理
 * 调用认证服务，缓存认证结果
 */
async function performAuthentication(c) {
  // 尝试从上下文获取已有的认证结果（避免重复认证）
  let authResult = c.get("authResult");
  let authService = c.get("authService");

  if (!authResult) {
    // 创建认证服务实例
    authService = createAuthService(c.env.DB);

    // 委托给认证服务进行具体的认证处理
    // 认证服务负责：验证token、密码、API密钥等具体逻辑
    const authHeader = c.req.header("Authorization");
    authResult = await authService.authenticate(authHeader);

    // 如果标准认证失败，尝试自定义授权头
    if (!authResult.isAuthenticated) {
      const customAuthKey = c.req.header("X-Custom-Auth-Key");
      if (customAuthKey) {
        const customAuthHeader = `ApiKey ${customAuthKey}`;
        authResult = await authService.authenticate(customAuthHeader);
      }
    }

    // 网关职责：缓存认证结果到上下文，避免重复认证
    c.set("authResult", authResult);
    c.set("authService", authService);
  }

  return authResult;
}

/**
 * 权限验证
 * 委托给 PermissionChecker 进行权限检查，避免重复实现
 */
function validatePermissions(authResult, requiredPermissions, mode) {
  if (!authResult.isAuthenticated) {
    return false;
  }

  const userPermissions = authResult.permissions;

  if (mode === "all") {
    // 需要所有权限 - 使用 PermissionChecker
    return PermissionChecker.hasAllPermissions(userPermissions, requiredPermissions);
  } else {
    // 任一权限满足即可（默认模式）- 使用 PermissionChecker
    return PermissionChecker.hasAnyPermission(userPermissions, requiredPermissions);
  }
}

/**
 * 路径权限验证
 * 网关职责：调用认证服务的路径权限检查，不重复实现路径检查逻辑
 */
function validatePathPermissions(authResult, c, customPath) {
  if (!authResult.isAuthenticated) {
    return { success: false, message: "未认证用户无法进行路径权限检查" };
  }

  const authService = c.get("authService");
  if (!authService) {
    return { success: false, message: "认证服务不可用" };
  }

  // 确定要检查的路径
  const requestPath = customPath || c.req.param("path") || c.req.query("path") || "/";

  // 委托给认证服务进行路径权限检查
  // 认证服务负责：具体的路径权限检查逻辑
  if (!authService.checkPathPermission(authResult, requestPath)) {
    return { success: false, message: "路径权限不足" };
  }

  return { success: true };
}

/**
 * 获取权限名称
 * 将权限位标志转换为可读的权限名称
 */
function getPermissionNames(permissions) {
  const nameMap = {
    [Permission.TEXT]: "文本权限",
    [Permission.FILE_SHARE]: "文件分享权限",
    [Permission.MOUNT_VIEW]: "挂载页查看权限",
    [Permission.MOUNT_UPLOAD]: "上传权限",
    [Permission.MOUNT_COPY]: "复制权限",
    [Permission.MOUNT_RENAME]: "重命名权限",
    [Permission.MOUNT_DELETE]: "删除权限",
    [Permission.WEBDAV_READ]: "WebDAV读取权限",
    [Permission.WEBDAV_MANAGE]: "WebDAV管理权限",
  };

  return permissions.map((permission) => nameMap[permission] || `权限${permission}`);
}

// ==================== 网关工具函数 ====================

/**
 * 网关工具函数：从上下文获取认证信息
 */
const gatewayUtils = {
  /**
   * 从上下文获取认证结果
   */
  getAuthResult: (c) => c.get("authResult"),

  /**
   * 从上下文获取认证服务
   */
  getAuthService: (c) => c.get("authService"),

  /**
   * 获取用户ID
   */
  getUserId: (c) => {
    const authResult = c.get("authResult");
    return authResult ? authResult.getUserId() : null;
  },

  /**
   * 获取基础路径
   */
  getBasicPath: (c) => {
    const authResult = c.get("authResult");
    return authResult ? authResult.basicPath : "/";
  },

  /**
   * 检查是否为管理员
   */
  isAdmin: (c) => {
    const authResult = c.get("authResult");
    return authResult ? authResult.isAdmin() : false;
  },

  /**
   * 检查是否已认证
   */
  isAuthenticated: (c) => {
    const authResult = c.get("authResult");
    return authResult ? authResult.isAuthenticated : false;
  },

  /**
   * 获取认证类型
   */
  getAuthType: (c) => {
    const authResult = c.get("authResult");
    if (!authResult || !authResult.isAuthenticated) {
      return null;
    }
    return authResult.isAdmin() ? "admin" : "apikey";
  },

  /**
   * 获取用户类型（类型系统用）
   */
  getUserType: (c) => {
    const authResult = c.get("authResult");
    if (!authResult || !authResult.isAuthenticated) {
      return null;
    }
    return authResult.getUserType();
  },

  /**
   * 检查操作权限（委托给认证服务）
   */
  checkOperationPermission: (c, requestPath) => {
    const authResult = c.get("authResult");
    const authService = c.get("authService");
    if (!authResult || !authService) return false;
    return authService.checkPathPermission(authResult, requestPath);
  },

  /**
   * 获取API密钥信息
   */
  getApiKeyInfo: (c) => {
    const authResult = c.get("authResult");
    return authResult ? authResult.keyInfo : null;
  },

  /**
   * 检查路径权限
   */
  checkPathPermissionForOperation: (c, basicPath, requestPath) => {
    const authService = c.get("authService");
    if (!authService) return false;
    return authService.checkBasicPathPermission(basicPath, requestPath);
  },

  /**
   * 检查导航路径权限
   * 允许访问从根路径到基本路径的所有父级路径，以便用户能够导航
   */
  checkPathPermissionForNavigation: (basicPath, requestPath) => {
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

    // 允许访问基本路径本身
    if (normalizeRequestPath === normalizeBasicPath) {
      return true;
    }

    // 允许访问基本路径的子路径
    if (normalizeRequestPath.startsWith(normalizeBasicPath + "/")) {
      return true;
    }

    // 特殊处理：允许访问从根路径到基本路径的所有父级路径，以便用户能够导航
    // 例如：如果基本路径是 /folder/subfolder，则允许访问 / 和 /folder
    const basicPathParts = normalizeBasicPath.split("/").filter((part) => part);
    const requestPathParts = normalizeRequestPath.split("/").filter((part) => part);

    // 检查请求路径是否是基本路径的父路径
    if (requestPathParts.length < basicPathParts.length) {
      const requestPathStr = "/" + requestPathParts.join("/");
      const basicPathPrefix = "/" + basicPathParts.slice(0, requestPathParts.length).join("/");
      return requestPathStr === basicPathPrefix;
    }

    return false;
  },

  /**
   * 获取可访问的挂载点列表
   */
  getAccessibleMounts: async (db, userIdOrInfo, userType) => {
    if (userType === "admin") {
      // 管理员可以访问所有挂载点
      const { RepositoryFactory } = await import("../repositories/index.js");
      const repositoryFactory = new RepositoryFactory(db);
      const mountRepository = repositoryFactory.getMountRepository();
      return await mountRepository.findAll(false); // false = 只获取活跃的挂载点
    } else if (userType === "apiKey") {
      // API密钥用户根据基本路径获取可访问的挂载点
      const { getAccessibleMountsByBasicPath } = await import("../services/apiKeyService.js");
      return await getAccessibleMountsByBasicPath(db, userIdOrInfo.basicPath);
    }

    return [];
  },

  /**
   * 检查是否有指定权限
   */
  hasPermission: (c, permission) => {
    const authResult = c.get("authResult");
    return authResult ? authResult.hasPermission(permission) : false;
  },

  /**
   * 检查是否有任一权限
   */
  hasAnyPermission: (c, permissions) => {
    const authResult = c.get("authResult");
    return authResult ? authResult.hasAnyPermission(permissions) : false;
  },

  /**
   * 检查是否有所有权限
   */
  hasAllPermissions: (c, permissions) => {
    const authResult = c.get("authResult");
    return authResult ? authResult.hasAllPermissions(permissions) : false;
  },
};

// ==================== 快捷中间件 ====================
// 基于统一认证网关的常用权限检查快捷方式

/**
 * 快捷中间件：只需要认证
 */
authGateway.requireAuth = () =>
  authGateway({
    requireAuth: true,
  });

/**
 * 快捷中间件：需要管理员权限
 */
authGateway.requireAdmin = () =>
  authGateway({
    requireAuth: true,
    allowAdmin: false,
    customCheck: (authResult) => authResult.isAdmin(),
  });

/**
 * 快捷中间件：需要文本权限
 */
authGateway.requireText = (options = {}) =>
  authGateway({
    requireAuth: true,
    permissions: [Permission.TEXT],
    ...options,
  });

/**
 * 快捷中间件：需要文件分享权限
 */
authGateway.requireFile = (options = {}) =>
  authGateway({
    requireAuth: true,
    permissions: [Permission.FILE_SHARE],
    ...options,
  });

/**
 * 快捷中间件：需要挂载页查看权限
 */
authGateway.requireMount = (options = {}) =>
  authGateway({
    requireAuth: true,
    permissions: [Permission.MOUNT_VIEW],
    ...options,
  });

/**
 * 快捷中间件：需要挂载页上传权限
 */
authGateway.requireMountUpload = (options = {}) =>
  authGateway({
    requireAuth: true,
    permissions: [Permission.MOUNT_UPLOAD],
    ...options,
  });

/**
 * 快捷中间件：需要挂载页复制权限
 */
authGateway.requireMountCopy = (options = {}) =>
  authGateway({
    requireAuth: true,
    permissions: [Permission.MOUNT_COPY],
    ...options,
  });

/**
 * 快捷中间件：需要挂载页重命名权限
 */
authGateway.requireMountRename = (options = {}) =>
  authGateway({
    requireAuth: true,
    permissions: [Permission.MOUNT_RENAME],
    ...options,
  });

/**
 * 快捷中间件：需要挂载页删除权限
 */
authGateway.requireMountDelete = (options = {}) =>
  authGateway({
    requireAuth: true,
    permissions: [Permission.MOUNT_DELETE],
    ...options,
  });

/**
 * 快捷中间件：需要WebDAV读取权限
 */
authGateway.requireWebDAVRead = (options = {}) =>
  authGateway({
    requireAuth: true,
    permissions: [Permission.WEBDAV_READ],
    ...options,
  });

/**
 * 快捷中间件：需要WebDAV管理权限
 */
authGateway.requireWebDAVManage = (options = {}) =>
  authGateway({
    requireAuth: true,
    permissions: [Permission.WEBDAV_MANAGE],
    ...options,
  });

// ==================== 工具函数导出 ====================

/**
 * 认证网关工具函数
 */
authGateway.utils = gatewayUtils;

/**
 * 创建认证服务实例
 */
authGateway.createAuthService = createAuthService;

/**
 * 执行认证处理 - 供WebDAV等协议层调用
 */
authGateway.performAuth = performAuthentication;
