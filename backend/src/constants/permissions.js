/**
 * 位标志权限系统常量定义
 * 基于位运算的高性能权限管理系统
 */

/**
 * 权限位标志常量
 * 使用位移操作定义权限，支持高效的位运算检查
 */
export const Permission = {
  // 基础权限 (0-7位)
  TEXT: 1 << 0, // 0x0001 - 文本权限
  FILE_SHARE: 1 << 1, // 0x0002 - 文件分享权限

  // 挂载页权限 (8-15位)
  MOUNT_VIEW: 1 << 8, // 0x0100 - 挂载页查看
  MOUNT_UPLOAD: 1 << 9, // 0x0200 - 上传/创建目录
  MOUNT_COPY: 1 << 10, // 0x0400 - 复制
  MOUNT_RENAME: 1 << 11, // 0x0800 - 重命名
  MOUNT_DELETE: 1 << 12, // 0x1000 - 删除

  // WebDAV权限 (16-23位)
  WEBDAV_READ: 1 << 16, // 0x10000 - WebDAV读取
  WEBDAV_MANAGE: 1 << 17, // 0x20000 - WebDAV管理
};

/**
 * 权限组合常量
 * 预定义的权限组合，便于快速分配
 */
export const PermissionGroup = {
  // 基础权限组合
  BASIC_TEXT: Permission.TEXT,
  BASIC_FILE: Permission.FILE_SHARE,

  // 挂载页权限组合
  MOUNT_READ_ONLY: Permission.MOUNT_VIEW,
  MOUNT_BASIC: Permission.MOUNT_VIEW | Permission.MOUNT_UPLOAD,
  MOUNT_FULL: Permission.MOUNT_VIEW | Permission.MOUNT_UPLOAD | Permission.MOUNT_COPY | Permission.MOUNT_RENAME | Permission.MOUNT_DELETE,

  // WebDAV权限组合
  WEBDAV_READ_ONLY: Permission.WEBDAV_READ,
  WEBDAV_FULL: Permission.WEBDAV_READ | Permission.WEBDAV_MANAGE,

  // 完整权限组合
  ALL_PERMISSIONS:
    Permission.TEXT |
    Permission.FILE_SHARE |
    Permission.MOUNT_VIEW |
    Permission.MOUNT_UPLOAD |
    Permission.MOUNT_COPY |
    Permission.MOUNT_RENAME |
    Permission.MOUNT_DELETE |
    Permission.WEBDAV_READ |
    Permission.WEBDAV_MANAGE,
};

/**
 * 角色预设和权限模板
 * 用于用户创建时的权限模板选择
 */
export const Role = {
  // 访客角色 - 可以设置为免密访问
  GUEST: {
    name: "GUEST",
    displayName: "访客",
    permissions: Permission.MOUNT_VIEW,
    description: "访客用户，可以设置为免密访问，权限可自定义",
  },

  // 普通用户角色 - 基础功能权限模板
  GENERAL: {
    name: "GENERAL",
    displayName: "普通用户",
    permissions: Permission.TEXT | Permission.FILE_SHARE | PermissionGroup.MOUNT_BASIC | Permission.WEBDAV_READ,
    description: "可以使用文本分享、文件分享、基础挂载页操作和WebDAV读取功能",
  },

  // 管理员角色 - 所有权限
  ADMIN: {
    name: "ADMIN",
    displayName: "管理员",
    permissions: PermissionGroup.ALL_PERMISSIONS,
    description: "拥有所有权限，可以进行任何操作",
  },
};

/**
 * 权限模板
 * 用于前端界面的快速权限设置
 */
export const PermissionTemplate = {
  // 只读模板 - 只能查看
  READ_ONLY: {
    name: "READ_ONLY",
    displayName: "只读权限",
    permissions: Permission.MOUNT_VIEW,
    description: "只能查看挂载页内容，无法进行任何操作",
  },

  // 基础模板 - 常用功能
  BASIC: {
    name: "BASIC",
    displayName: "基础权限",
    permissions: Permission.TEXT | Permission.FILE_SHARE | PermissionGroup.MOUNT_BASIC,
    description: "可以使用文本分享、文件分享和基础挂载页操作",
  },

  // 完整模板 - 除管理外的所有功能
  FULL: {
    name: "FULL",
    displayName: "完整权限",
    permissions: Permission.TEXT | Permission.FILE_SHARE | PermissionGroup.MOUNT_FULL | PermissionGroup.WEBDAV_FULL,
    description: "可以使用所有功能，包括文件管理和WebDAV访问",
  },
};

/**
 * 权限检查工具函数
 */
export class PermissionChecker {
  /**
   * 检查用户是否拥有指定权限
   * @param {number} userPermissions - 用户权限位标志
   * @param {number} requiredPermission - 需要的权限位标志
   * @returns {boolean} 是否拥有权限
   */
  static hasPermission(userPermissions, requiredPermission) {
    return (userPermissions & requiredPermission) === requiredPermission;
  }

  /**
   * 检查用户是否拥有任一权限
   * @param {number} userPermissions - 用户权限位标志
   * @param {number[]} requiredPermissions - 需要的权限位标志数组
   * @returns {boolean} 是否拥有任一权限
   */
  static hasAnyPermission(userPermissions, requiredPermissions) {
    return requiredPermissions.some((permission) => this.hasPermission(userPermissions, permission));
  }

  /**
   * 检查用户是否拥有所有权限
   * @param {number} userPermissions - 用户权限位标志
   * @param {number[]} requiredPermissions - 需要的权限位标志数组
   * @returns {boolean} 是否拥有所有权限
   */
  static hasAllPermissions(userPermissions, requiredPermissions) {
    return requiredPermissions.every((permission) => this.hasPermission(userPermissions, permission));
  }

  /**
   * 添加权限
   * @param {number} currentPermissions - 当前权限位标志
   * @param {number} newPermission - 要添加的权限位标志
   * @returns {number} 新的权限位标志
   */
  static addPermission(currentPermissions, newPermission) {
    return currentPermissions | newPermission;
  }

  /**
   * 移除权限
   * @param {number} currentPermissions - 当前权限位标志
   * @param {number} permissionToRemove - 要移除的权限位标志
   * @returns {number} 新的权限位标志
   */
  static removePermission(currentPermissions, permissionToRemove) {
    return currentPermissions & ~permissionToRemove;
  }

  /**
   * 获取权限描述
   * @param {number} permissions - 权限位标志
   * @returns {string[]} 权限描述数组
   */
  static getPermissionDescriptions(permissions) {
    const descriptions = [];

    if (this.hasPermission(permissions, Permission.TEXT)) {
      descriptions.push("文本分享");
    }
    if (this.hasPermission(permissions, Permission.FILE_SHARE)) {
      descriptions.push("文件分享");
    }
    if (this.hasPermission(permissions, Permission.MOUNT_VIEW)) {
      descriptions.push("挂载页查看");
    }
    if (this.hasPermission(permissions, Permission.MOUNT_UPLOAD)) {
      descriptions.push("上传/创建目录");
    }
    if (this.hasPermission(permissions, Permission.MOUNT_COPY)) {
      descriptions.push("复制文件");
    }
    if (this.hasPermission(permissions, Permission.MOUNT_RENAME)) {
      descriptions.push("重命名文件");
    }
    if (this.hasPermission(permissions, Permission.MOUNT_DELETE)) {
      descriptions.push("删除文件");
    }
    if (this.hasPermission(permissions, Permission.WEBDAV_READ)) {
      descriptions.push("WebDAV读取");
    }
    if (this.hasPermission(permissions, Permission.WEBDAV_MANAGE)) {
      descriptions.push("WebDAV管理");
    }

    return descriptions;
  }
}
