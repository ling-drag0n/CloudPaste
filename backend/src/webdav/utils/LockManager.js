/**
 * WebDAV锁管理器 
 */

import { HTTPException } from "hono/http-exception";
import { ApiStatus } from "../../constants/index.js";
import { generateLockToken } from "./lockUtils.js";

export class LockManager {
  constructor() {
    // 锁存储 - 保持双索引以支持按路径和令牌查找
    this.locksByPath = new Map(); // path -> lockInfo
    this.locksByToken = new Map(); // token -> lockInfo
    this.nextTokenId = 1;
  }

  /**
   * 创建锁定
   * @param {string} path - 资源路径
   * @param {string} owner - 锁定所有者
   * @param {number} timeoutSeconds - 超时时间（秒）
   * @param {string} depth - 锁定深度
   * @param {string} scope - 锁定范围
   * @param {string} type - 锁定类型
   * @returns {Object} 锁定信息
   */
  createLock(path, owner, timeoutSeconds = 600, depth = "0", scope = "exclusive", type = "write") {
    // 检查是否已锁定
    const existingLock = this.locksByPath.get(path);
    if (existingLock && !this.isExpired(existingLock)) {
      throw new HTTPException(ApiStatus.LOCKED, {
        message: `资源已被锁定: ${path}`,
        details: { lockToken: existingLock.token },
      });
    }

    // 创建锁信息
    const token = generateLockToken(this.nextTokenId++);
    const now = Date.now();
    const lockInfo = {
      token,
      path,
      owner,
      depth,
      scope,
      type,
      createdAt: now,
      expiresAt: now + timeoutSeconds * 1000,
      timeoutSeconds,
    };

    // 存储锁信息
    this.locksByPath.set(path, lockInfo);
    this.locksByToken.set(token, lockInfo);

    console.log(`WebDAV锁定创建: ${path} -> ${token}`);
    return lockInfo;
  }

  /**
   * 检查锁定状态
   * @param {string} path - 资源路径
   * @param {string} ifHeader - If头部
   * @returns {Object|null} 锁定信息或null
   */
  checkLock(path, ifHeader = null) {
    // 清理过期锁
    this.cleanupExpired();

    // 检查直接锁定
    const lock = this.locksByPath.get(path);
    if (lock && !this.isExpired(lock)) {
      // 如果有If头，验证条件
      if (ifHeader && this.checkIfCondition(ifHeader, lock.token)) {
        return null; // 条件满足，允许操作
      }
      return lock;
    }

    return null;
  }

  /**
   * 删除锁定
   * @param {string} token - 锁令牌
   * @returns {boolean} 是否成功删除
   */
  unlock(token) {
    const lockInfo = this.locksByToken.get(token);
    if (!lockInfo) {
      return false;
    }

    this.locksByPath.delete(lockInfo.path);
    this.locksByToken.delete(token);

    console.log(`WebDAV锁定删除: ${lockInfo.path} -> ${token}`);
    return true;
  }

  /**
   * 刷新锁定
   * @param {string} token - 锁令牌
   * @param {number} timeoutSeconds - 新的超时时间
   * @returns {Object|null} 更新后的锁信息
   */
  refreshLock(token, timeoutSeconds = 600) {
    const lockInfo = this.locksByToken.get(token);
    if (!lockInfo || this.isExpired(lockInfo)) {
      return null;
    }

    // 更新过期时间
    lockInfo.expiresAt = Date.now() + timeoutSeconds * 1000;
    lockInfo.timeoutSeconds = timeoutSeconds;

    console.log(`WebDAV锁定刷新: ${lockInfo.path} -> ${token}`);
    return lockInfo;
  }

  /**
   * 检查锁是否过期
   * @param {Object} lockInfo - 锁信息
   * @returns {boolean} 是否过期
   */
  isExpired(lockInfo) {
    return Date.now() > lockInfo.expiresAt;
  }

  /**
   * 简化的If条件检查
   * @param {string} ifHeader - If头部
   * @param {string} token - 锁令牌
   * @returns {boolean} 条件是否满足
   */
  checkIfCondition(ifHeader, token) {
    // 令牌匹配检查
    return ifHeader.includes(token);
  }

  /**
   * 清理过期锁定
   */
  cleanupExpired() {
    const now = Date.now();
    const expiredPaths = [];
    const expiredTokens = [];

    // 查找过期锁
    for (const [path, lockInfo] of this.locksByPath) {
      if (now > lockInfo.expiresAt) {
        expiredPaths.push(path);
        expiredTokens.push(lockInfo.token);
      }
    }

    // 删除过期锁
    for (const path of expiredPaths) {
      this.locksByPath.delete(path);
    }
    for (const token of expiredTokens) {
      this.locksByToken.delete(token);
    }

    if (expiredPaths.length > 0) {
      console.log(`WebDAV清理过期锁: ${expiredPaths.length}个`);
    }
  }

  /**
   * 获取锁信息
   * @param {string} token - 锁令牌
   * @returns {Object|null} 锁信息
   */
  getLockByToken(token) {
    const lockInfo = this.locksByToken.get(token);
    return lockInfo && !this.isExpired(lockInfo) ? lockInfo : null;
  }

  /**
   * 获取路径的锁信息
   * @param {string} path - 资源路径
   * @returns {Object|null} 锁信息
   */
  getLockByPath(path) {
    const lockInfo = this.locksByPath.get(path);
    return lockInfo && !this.isExpired(lockInfo) ? lockInfo : null;
  }

  /**
   * 获取统计信息
   * @returns {Object} 统计信息
   */
  getStats() {
    return {
      totalLocks: this.locksByPath.size,
      activeLocks: Array.from(this.locksByPath.values()).filter(lock => !this.isExpired(lock)).length,
    };
  }
}

// 创建全局实例
export const lockManager = new LockManager();
