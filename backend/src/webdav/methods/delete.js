/**
 * 处理WebDAV DELETE请求
 * 用于删除文件或目录
 */
import { MountManager } from "../../storage/managers/MountManager.js";
import { FileSystem } from "../../storage/fs/FileSystem.js";
import { handleWebDAVError, createWebDAVErrorResponse } from "../utils/errorUtils.js";
import { getStandardWebDAVHeaders } from "../utils/headerUtils.js";
import { clearDirectoryCache } from "../../cache/index.js";
import { lockManager } from "../utils/LockManager.js";
import { checkLockPermission } from "../utils/lockUtils.js";

/**
 * 处理DELETE请求
 * @param {Object} c - Hono上下文
 * @param {string} path - 请求路径
 * @param {string} userId - 用户ID
 * @param {string} userType - 用户类型 (admin 或 apiKey)
 * @param {D1Database} db - D1数据库实例
 */
export async function handleDelete(c, path, userId, userType, db) {
  try {
    // 检查锁定状态
    const ifHeader = c.req.header("If");
    const lockConflict = checkLockPermission(lockManager, path, ifHeader, "DELETE");
    if (lockConflict) {
      console.log(`WebDAV DELETE - 锁定冲突: ${path}`);
      return new Response(lockConflict.message, {
        status: lockConflict.status,
        headers: getStandardWebDAVHeaders({
          customHeaders: { "Content-Type": "text/plain" },
        }),
      });
    }

    // 提取挂载点路径部分和子路径部分
    const pathParts = path.split("/").filter((p) => p);

    // 不允许删除挂载点根目录（保持原有业务逻辑保护）
    if (pathParts.length === 1) {
      return new Response("不能删除挂载点根目录", {
        status: 405, // Method Not Allowed
        headers: getStandardWebDAVHeaders({
          customHeaders: { "Content-Type": "text/plain" },
        }),
      });
    }

    // 创建FileSystem实例
    const mountManager = new MountManager(db, c.env.ENCRYPTION_SECRET);
    const fileSystem = new FileSystem(mountManager);

    console.log(`WebDAV DELETE - 开始删除路径: ${path}, 用户类型: ${userType}`);

    // 使用FileSystem统一抽象层删除文件或目录
    // batchRemoveItems会自动处理文件和目录的删除，包括递归删除目录
    const result = await fileSystem.batchRemoveItems([path], userId, userType);

    console.log(`WebDAV DELETE - 删除结果: 成功=${result.success}, 失败=${result.failed?.length || 0}`);

    // 处理删除结果
    if (result.failed && result.failed.length > 0) {
      const failedItem = result.failed[0];
      console.warn(`WebDAV DELETE - 删除失败: ${failedItem.path} - ${failedItem.error}`);

      // 根据错误类型返回适当的HTTP状态码
      if (failedItem.error.includes("不存在") || failedItem.error.includes("not found")) {
        return createWebDAVErrorResponse("文件或目录不存在", 404, false);
      } else if (failedItem.error.includes("权限") || failedItem.error.includes("permission")) {
        return createWebDAVErrorResponse("权限不足", 403, false);
      } else {
        return createWebDAVErrorResponse(failedItem.error, 500, false);
      }
    }

    // 手动缓存清理（因为FileSystem不处理WebDAV特定的缓存）
    try {
      const { mount } = await mountManager.getDriverByPath(path, userId, userType);
      if (mount) {
        await clearDirectoryCache({ mountId: mount.id });
        console.log(`WebDAV DELETE - 已清理挂载点 ${mount.id} 的缓存`);
      }
    } catch (cacheError) {
      // 缓存清理失败不应该影响删除操作的成功响应
      console.warn(`WebDAV DELETE - 缓存清理失败: ${cacheError.message}`);
    }

    console.log(`WebDAV DELETE - 删除成功: ${path}`);

    // 返回成功响应（符合WebDAV DELETE标准）
    return new Response(null, {
      status: 204, // No Content
      headers: getStandardWebDAVHeaders({
        customHeaders: {
          "Content-Type": "text/plain",
          "Content-Length": "0",
        },
      }),
    });
  } catch (error) {
    console.error(`WebDAV DELETE - 处理错误: ${error.message}`, error);
    // 使用统一的WebDAV错误处理
    return handleWebDAVError("DELETE", error, false, false);
  }
}
