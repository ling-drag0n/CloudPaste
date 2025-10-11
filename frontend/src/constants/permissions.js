/**
 * 前端权限系统常量定义
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
 * 前端权限检查工具函数
 * 与后端PermissionChecker保持一致的API
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
   * 获取权限描述（支持国际化）
   * @param {number} permissions - 权限位标志
   * @param {Function} t - 国际化翻译函数
   * @returns {string[]} 权限描述数组
   */
  static getPermissionDescriptions(permissions, t = null) {
    const descriptions = [];

    if (this.hasPermission(permissions, Permission.TEXT)) {
      descriptions.push(t ? t("permissions.text") : "文本分享");
    }
    if (this.hasPermission(permissions, Permission.FILE_SHARE)) {
      descriptions.push(t ? t("permissions.fileShare") : "文件分享");
    }
    if (this.hasPermission(permissions, Permission.MOUNT_VIEW)) {
      descriptions.push(t ? t("permissions.mountView") : "挂载页查看");
    }
    if (this.hasPermission(permissions, Permission.MOUNT_UPLOAD)) {
      descriptions.push(t ? t("permissions.mountUpload") : "上传/创建目录");
    }
    if (this.hasPermission(permissions, Permission.MOUNT_COPY)) {
      descriptions.push(t ? t("permissions.mountCopy") : "复制文件");
    }
    if (this.hasPermission(permissions, Permission.MOUNT_RENAME)) {
      descriptions.push(t ? t("permissions.mountRename") : "重命名文件");
    }
    if (this.hasPermission(permissions, Permission.MOUNT_DELETE)) {
      descriptions.push(t ? t("permissions.mountDelete") : "删除文件");
    }
    if (this.hasPermission(permissions, Permission.WEBDAV_READ)) {
      descriptions.push(t ? t("permissions.webdavRead") : "WebDAV读取");
    }
    if (this.hasPermission(permissions, Permission.WEBDAV_MANAGE)) {
      descriptions.push(t ? t("permissions.webdavManage") : "WebDAV管理");
    }

    return descriptions;
  }

  /**
   * 将位标志权限转换为布尔权限对象（用于前端表单）
   * @param {number} bitFlag - 位标志权限
   * @returns {Object} 布尔权限对象
   */
  static convertFromBitFlag(bitFlag) {
    return {
      text: this.hasPermission(bitFlag, Permission.TEXT),
      file_share: this.hasPermission(bitFlag, Permission.FILE_SHARE),
      mount_view: this.hasPermission(bitFlag, Permission.MOUNT_VIEW),
      mount_upload: this.hasPermission(bitFlag, Permission.MOUNT_UPLOAD),
      mount_copy: this.hasPermission(bitFlag, Permission.MOUNT_COPY),
      mount_rename: this.hasPermission(bitFlag, Permission.MOUNT_RENAME),
      mount_delete: this.hasPermission(bitFlag, Permission.MOUNT_DELETE),
      webdav_read: this.hasPermission(bitFlag, Permission.WEBDAV_READ),
      webdav_manage: this.hasPermission(bitFlag, Permission.WEBDAV_MANAGE),
    };
  }

  /**
   * 将布尔权限对象转换为位标志权限（用于前端表单）
   * @param {Object} permissions - 布尔权限对象
   * @returns {number} 位标志权限
   */
  static convertToBitFlag(permissions) {
    let bitFlag = 0;
    if (permissions.text) bitFlag |= Permission.TEXT;
    if (permissions.file_share) bitFlag |= Permission.FILE_SHARE;
    if (permissions.mount_view) bitFlag |= Permission.MOUNT_VIEW;
    if (permissions.mount_upload) bitFlag |= Permission.MOUNT_UPLOAD;
    if (permissions.mount_copy) bitFlag |= Permission.MOUNT_COPY;
    if (permissions.mount_rename) bitFlag |= Permission.MOUNT_RENAME;
    if (permissions.mount_delete) bitFlag |= Permission.MOUNT_DELETE;
    if (permissions.webdav_read) bitFlag |= Permission.WEBDAV_READ;
    if (permissions.webdav_manage) bitFlag |= Permission.WEBDAV_MANAGE;
    return bitFlag;
  }
}
