/**
 * 处理WebDAV MKCOL请求
 * 用于创建目录
 */
import { MountManager } from "../../storage/managers/MountManager.js";
import { FileSystem } from "../../storage/fs/FileSystem.js";
import { handleWebDAVError, createWebDAVErrorResponse } from "../utils/errorUtils.js";
import { getStandardWebDAVHeaders } from "../utils/headerUtils.js";
import { clearDirectoryCache } from "../../cache/index.js";

/**
 * 处理MKCOL请求
 * @param {Object} c - Hono上下文
 * @param {string} path - 请求路径
 * @param {string} userId - 用户ID
 * @param {string} userType - 用户类型 (admin 或 apiKey)
 * @param {D1Database} db - D1数据库实例
 */
export async function handleMkcol(c, path, userId, userType, db) {
  try {
    // 检查请求是否包含正文（基本MKCOL请求不应包含正文）
    // 符合RFC 4918标准：基本MKCOL不应包含请求体，扩展MKCOL可以包含XML
    const body = await c.req.text();
    if (body.length > 0) {
      return new Response("MKCOL请求不应包含正文", {
        status: 415, // Unsupported Media Type
        headers: { "Content-Type": "text/plain" },
      });
    }

    // 创建FileSystem实例
    const mountManager = new MountManager(db, c.env.ENCRYPTION_SECRET);
    const fileSystem = new FileSystem(mountManager);

    console.log(`WebDAV MKCOL - 开始创建目录: ${path}, 用户类型: ${userType}`);

    // 处理根目录特殊情况（符合WebDAV标准的特殊处理）
    const pathParts = path.split("/").filter((p) => p);
    if (pathParts.length === 1) {
      console.log(`WebDAV MKCOL - 检测到根目录请求，执行S3桶验证`);

      try {
        // 通过尝试获取挂载点信息来验证访问权限
        const { mount } = await mountManager.getDriverByPath(path, userId, userType);
        console.log(`WebDAV MKCOL - 成功验证根目录访问权限，挂载点: ${mount.id}`);

        // 对于根目录请求，直接返回成功状态码（符合WebDAV标准处理）
        return new Response(null, {
          status: 201, // Created
          headers: getStandardWebDAVHeaders({
            customHeaders: {
              "Content-Type": "text/plain",
              "Content-Length": "0",
            },
          }),
        });
      } catch (error) {
        console.warn(`WebDAV MKCOL - 根目录验证失败: ${error.message}`);
        return createWebDAVErrorResponse("根目录访问验证失败", 403, false);
      }
    }

    // 使用FileSystem统一抽象层创建目录
    try {
      await fileSystem.createDirectory(path, userId, userType);
      console.log(`WebDAV MKCOL - 目录创建成功: ${path}`);

      // 手动缓存清理（因为FileSystem已经处理了，但我们需要确保WebDAV缓存一致性）
      try {
        const { mount } = await mountManager.getDriverByPath(path, userId, userType);
        if (mount) {
          await clearDirectoryCache({ mountId: mount.id });
          console.log(`WebDAV MKCOL - 已清理挂载点 ${mount.id} 的缓存`);
        }
      } catch (cacheError) {
        // 缓存清理失败不应该影响创建操作的成功响应
        console.warn(`WebDAV MKCOL - 缓存清理失败: ${cacheError.message}`);
      }

      // 返回成功响应（符合WebDAV MKCOL标准）
      return new Response(null, {
        status: 201, // Created
        headers: getStandardWebDAVHeaders({
          customHeaders: {
            "Content-Type": "text/plain",
            "Content-Length": "0",
          },
        }),
      });
    } catch (error) {
      // 处理FileSystem的409错误，严格遵循RFC 4918标准
      if (error.status === 409) {
        if (error.message === "目录已存在") {
          // RFC 4918标准：目录已存在时返回405 Method Not Allowed
          console.log(`WebDAV MKCOL - 目录已存在，返回405 Method Not Allowed: ${path}`);
          return new Response("Collection already exists", {
            status: 405, // Method Not Allowed
            headers: getStandardWebDAVHeaders({
              customHeaders: {
                "Content-Type": "text/plain",
              },
            }),
          });
        } else if (error.message === "父目录不存在") {
          // RFC 4918标准：父目录不存在时返回409 Conflict
          console.log(`WebDAV MKCOL - 父目录不存在，返回409 Conflict: ${path}`);
          return new Response("Parent collection does not exist", {
            status: 409, // Conflict
            headers: getStandardWebDAVHeaders({
              customHeaders: {
                "Content-Type": "text/plain",
              },
            }),
          });
        }
      }

      // 其他错误直接抛出
      throw error;
    }
  } catch (error) {
    console.error(`WebDAV MKCOL - 处理错误: ${error.message}`, error);
    // 使用统一的WebDAV错误处理
    return handleWebDAVError("MKCOL", error, false, false);
  }
}
