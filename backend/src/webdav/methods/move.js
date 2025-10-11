/**
 * WebDAV MOVE方法实现
 * 基于RFC 4918标准和SabreDAV实现模式，采用"复制-删除"机制
 * - 复用COPY方法的跨存储传输逻辑
 * - 基于FileSystem抽象层的统一实现
 */

import { FileSystem } from "../../storage/fs/FileSystem.js";
import { MountManager } from "../../storage/managers/MountManager.js";
import { createWebDAVErrorResponse } from "../utils/errorUtils.js";
import { getStandardWebDAVHeaders } from "../utils/headerUtils.js";
import { clearDirectoryCache } from "../../cache/index.js";
import { lockManager } from "../utils/LockManager.js";
import { checkLockPermission } from "../utils/lockUtils.js";
import { parseDestinationPath } from "../utils/webdavUtils.js";

/**
 * 同步跨存储传输函数
 * 支持文件和目录的跨存储传输
 */
async function performSyncCrossStorageTransfer(crossStorageResult) {
  // 检查是否为目录复制
  if (crossStorageResult.isDirectory) {
    return await performDirectoryCrossStorageTransfer(crossStorageResult);
  } else {
    return await performFileCrossStorageTransfer(crossStorageResult);
  }
}

/**
 * 执行目录的跨存储传输
 */
async function performDirectoryCrossStorageTransfer(dirResult) {
  const { items, targetPath } = dirResult;

  try {
    console.log(`开始目录跨存储传输: ${targetPath}, 包含 ${items.length} 个文件`);

    // 配置并发传输参数
    const CONCURRENT_TRANSFERS = 3; // 限制同时传输3个文件，平衡性能和资源消耗
    const results = [];
    let successCount = 0;
    let failedCount = 0;

    // 分批并行处理文件
    for (let i = 0; i < items.length; i += CONCURRENT_TRANSFERS) {
      const batch = items.slice(i, i + CONCURRENT_TRANSFERS);
      console.log(`处理第 ${Math.floor(i / CONCURRENT_TRANSFERS) + 1} 批文件，包含 ${batch.length} 个文件`);

      // 并行传输当前批次的文件
      const batchPromises = batch.map(async (item) => {
        try {
          console.log(`开始传输文件: ${item.fileName}`);
          const result = await performFileCrossStorageTransfer(item);

          if (result.success) {
            console.log(`✅ 文件传输成功: ${item.fileName}`);
          } else {
            console.error(`❌ 文件传输失败: ${item.fileName} - ${result.error}`);
          }

          return {
            fileName: item.fileName,
            success: result.success,
            error: result.error,
          };
        } catch (error) {
          console.error(`❌ 文件传输异常: ${item.fileName} - ${error.message}`);
          return {
            fileName: item.fileName,
            success: false,
            error: error.message,
          };
        }
      });

      // 等待当前批次完成
      const batchResults = await Promise.allSettled(batchPromises);

      // 处理批次结果
      batchResults.forEach((promiseResult, index) => {
        if (promiseResult.status === "fulfilled") {
          const transferResult = promiseResult.value;
          results.push(transferResult);

          if (transferResult.success) {
            successCount++;
          } else {
            failedCount++;
          }
        } else {
          // Promise被拒绝的情况
          const item = batch[index];
          console.error(`❌ 文件传输Promise失败: ${item.fileName} - ${promiseResult.reason}`);
          results.push({
            fileName: item.fileName,
            success: false,
            error: promiseResult.reason?.message || "未知错误",
          });
          failedCount++;
        }
      });

      console.log(
        `第 ${Math.floor(i / CONCURRENT_TRANSFERS) + 1} 批完成: 成功 ${batchResults.filter((r) => r.status === "fulfilled" && r.value.success).length} 个，失败 ${
          batchResults.filter((r) => r.status === "rejected" || (r.status === "fulfilled" && !r.value.success)).length
        } 个`
      );
    }

    console.log(`目录跨存储传输完成: 成功 ${successCount} 个，失败 ${failedCount} 个`);

    // 根据结果判断整体成功性
    if (failedCount === 0) {
      return {
        success: true,
        message: `目录复制成功，共传输 ${successCount} 个文件`,
        details: results,
      };
    } else if (successCount === 0) {
      return {
        success: false,
        error: `目录复制失败，共 ${failedCount} 个文件传输失败`,
        details: results,
      };
    } else {
      // 部分成功的情况
      return {
        success: false,
        error: `目录复制部分成功，${successCount} 个成功，${failedCount} 个失败`,
        details: results,
      };
    }
  } catch (error) {
    console.error(`目录跨存储传输异常: ${error.message}`, error);
    return { success: false, error: `目录传输异常: ${error.message}` };
  }
}

/**
 * 执行单文件的跨存储传输
 */
async function performFileCrossStorageTransfer(fileItem) {
  const { downloadUrl, uploadUrl, contentType, fileName } = fileItem;

  try {
    // 1. 从源存储下载文件
    console.log(`下载文件: ${downloadUrl}`);
    const downloadResponse = await fetch(downloadUrl);
    if (!downloadResponse.ok) {
      throw new Error(`下载失败: ${downloadResponse.status} ${downloadResponse.statusText}`);
    }

    // 2. 获取文件内容长度
    const contentLength = downloadResponse.headers.get("Content-Length");
    console.log(`文件大小: ${contentLength} bytes`);

    // 3. 上传到目标存储
    console.log(`上传文件: ${uploadUrl}`);
    const uploadHeaders = {
      "Content-Type": contentType || "application/octet-stream",
    };

    // 如果有Content-Length，添加到上传头部
    if (contentLength) {
      uploadHeaders["Content-Length"] = contentLength;
    }

    const uploadResponse = await fetch(uploadUrl, {
      method: "PUT",
      body: downloadResponse.body, // 直接流式传输，避免内存占用
      headers: uploadHeaders,
    });

    if (!uploadResponse.ok) {
      throw new Error(`上传失败: ${uploadResponse.status} ${uploadResponse.statusText}`);
    }

    console.log(`文件跨存储传输成功: ${fileName}`);
    return { success: true };
  } catch (error) {
    console.error(`文件跨存储传输失败: ${fileName} - ${error.message}`, error);
    return { success: false, error: error.message };
  }
}

/**
 * 处理WebDAV MOVE请求
 * @param {Object} c - Hono上下文
 * @param {string} path - 请求路径
 * @param {string} userId - 用户ID
 * @param {string} userType - 用户类型
 * @param {D1Database} db - D1数据库实例
 * @returns {Promise<Response>} HTTP响应
 */
export async function handleMove(c, path, userId, userType, db) {
  try {
    console.log(`WebDAV MOVE - 开始处理: ${path}`);

    // 1. 解析WebDAV头部（与COPY方法完全一致）
    const destination = c.req.header("Destination");
    const overwrite = c.req.header("Overwrite") || "T";
    const depth = c.req.header("Depth") || "infinity";
    const ifHeader = c.req.header("If");

    console.log(`WebDAV MOVE - 请求头部: Destination=${destination}, Overwrite=${overwrite}, Depth=${depth}`);

    // 检查锁定状态

    // 检查源路径的锁定状态（MOVE操作会删除源资源）
    const sourceLockConflict = checkLockPermission(lockManager, path, ifHeader, "MOVE");
    if (sourceLockConflict) {
      console.log(`WebDAV MOVE - 源路径锁定冲突: ${path}`);
      return createWebDAVErrorResponse(sourceLockConflict.message, sourceLockConflict.status, false);
    }

    // 2. 验证必需的Destination头
    if (!destination) {
      console.warn(`WebDAV MOVE - 缺少Destination头`);
      return createWebDAVErrorResponse("缺少Destination头", 400, false);
    }

    // 3. 验证Depth头部（RFC 4918要求集合资源只能是infinity）
    if (depth !== "infinity") {
      console.error(`WebDAV MOVE - 不支持的Depth值: ${depth}`);
      return createWebDAVErrorResponse("MOVE操作只支持Depth: infinity", 412, false);
    }

    // 4. 解析目标路径
    const destPath = parseDestinationPath(destination);
    if (!destPath) {
      console.error(`WebDAV MOVE - 无效的Destination URL: ${destination}`);
      return createWebDAVErrorResponse("无效的Destination URL", 400, false);
    }

    console.log(`WebDAV MOVE - 目标路径: ${destPath}`);

    // 5. 验证源路径和目标路径不能相同（RFC 4918标准）
    if (path === destPath) {
      console.warn(`WebDAV MOVE - 源路径和目标路径相同: ${path}`);
      return createWebDAVErrorResponse("源路径和目标路径不能相同", 403, false);
    }

    // 6. 创建FileSystem实例
    const mountManager = new MountManager(db, c.env.ENCRYPTION_SECRET);
    const fileSystem = new FileSystem(mountManager);

    console.log(`WebDAV MOVE - 开始移动: ${path} -> ${destPath}, 用户类型: ${userType}`);

    // 7. 检查目标是否已存在（用于确定返回的状态码）
    let destExists = false;
    try {
      destExists = await fileSystem.exists(destPath, userId, userType);
      console.log(`WebDAV MOVE - 目标路径存在性检查: ${destPath} = ${destExists}`);
    } catch (error) {
      // exists方法出错，记录警告但继续执行
      console.warn(`WebDAV MOVE - 检查目标路径存在性失败: ${error.message}`);
    }

    // 8. 如果目标存在且不允许覆盖，直接返回错误
    if (destExists && overwrite === "F") {
      console.warn(`WebDAV MOVE - 目标已存在且不允许覆盖: ${destPath}`);
      return createWebDAVErrorResponse("目标已存在且不允许覆盖", 412, false); // Precondition Failed
    }

    // 9. 第一步：执行复制操作（复用COPY方法的完整逻辑）
    const copyResult = await fileSystem.copyItem(path, destPath, userId, userType, {
      skipExisting: overwrite === "F", // Overwrite: F 表示不覆盖，即跳过已存在的文件
    });

    console.log(`WebDAV MOVE - 复制结果:`, copyResult);

    // 10. 处理跨存储复制结果
    if (copyResult.crossStorage) {
      console.log(`WebDAV MOVE - 检测到跨存储移动，执行同步传输`);

      try {
        // 执行同步跨存储传输
        const transferResult = await performSyncCrossStorageTransfer(copyResult);

        if (transferResult.success) {
          console.log(`WebDAV MOVE - 跨存储复制成功: ${path} -> ${destPath}`);

          // 跨存储复制成功后，立即清理缓存
          try {
            const { mount: sourceMountResult } = await mountManager.getDriverByPath(path, userId, userType);
            const { mount: destMountResult } = await mountManager.getDriverByPath(destPath, userId, userType);

            if (sourceMountResult) {
              await clearDirectoryCache({ mountId: sourceMountResult.id });
              console.log(`WebDAV MOVE - 已清理源挂载点 ${sourceMountResult.id} 的缓存`);
            }

            if (destMountResult && destMountResult.id !== sourceMountResult?.id) {
              await clearDirectoryCache({ mountId: destMountResult.id });
              console.log(`WebDAV MOVE - 已清理目标挂载点 ${destMountResult.id} 的缓存`);
            }
          } catch (cacheError) {
            // 缓存清理失败不应该影响复制操作的成功响应
            console.warn(`WebDAV MOVE - 跨存储复制后缓存清理失败: ${cacheError.message}`);
          }
        } else {
          console.error(`WebDAV MOVE - 跨存储复制失败: ${transferResult.error}`);
          return createWebDAVErrorResponse(`跨存储复制失败: ${transferResult.error}`, 500, false);
        }
      } catch (error) {
        console.error(`WebDAV MOVE - 跨存储复制异常: ${error.message}`, error);
        return createWebDAVErrorResponse(`跨存储复制失败: ${error.message}`, 500, false);
      }
    }

    // 11. 第二步：删除源文件/目录（SabreDAV的"复制-删除"机制）
    console.log(`WebDAV MOVE - 第二步：删除源文件 ${path}`);

    try {
      const deleteResult = await fileSystem.batchRemoveItems([path], userId, userType);
      console.log(`WebDAV MOVE - 删除结果: 成功=${deleteResult.success}, 失败=${deleteResult.failed?.length || 0}`);

      if (deleteResult.failed && deleteResult.failed.length > 0) {
        // 删除失败，需要回滚已复制的文件
        console.error(`WebDAV MOVE - 删除源文件失败，尝试回滚: ${deleteResult.failed[0]?.error}`);

        try {
          // 回滚：删除已复制的目标文件
          await fileSystem.batchRemoveItems([destPath], userId, userType);
          console.log(`WebDAV MOVE - 回滚成功：已删除目标文件 ${destPath}`);
        } catch (rollbackError) {
          console.error(`WebDAV MOVE - 回滚失败: ${rollbackError.message}`, rollbackError);
        }

        return createWebDAVErrorResponse(`移动失败：无法删除源文件 - ${deleteResult.failed[0]?.error}`, 500, false);
      }
    } catch (deleteError) {
      console.error(`WebDAV MOVE - 删除源文件异常: ${deleteError.message}`, deleteError);

      try {
        // 回滚：删除已复制的目标文件
        await fileSystem.batchRemoveItems([destPath], userId, userType);
        console.log(`WebDAV MOVE - 回滚成功：已删除目标文件 ${destPath}`);
      } catch (rollbackError) {
        console.error(`WebDAV MOVE - 回滚失败: ${rollbackError.message}`, rollbackError);
      }

      return createWebDAVErrorResponse(`移动失败：删除源文件异常 - ${deleteError.message}`, 500, false);
    }

    // 12. 同存储移动的缓存清理（跨存储移动已在上面处理）
    if (!copyResult.crossStorage) {
      try {
        const { mount: sourceMountResult } = await mountManager.getDriverByPath(path, userId, userType);
        const { mount: destMountResult } = await mountManager.getDriverByPath(destPath, userId, userType);

        if (sourceMountResult) {
          await clearDirectoryCache({ mountId: sourceMountResult.id });
          console.log(`WebDAV MOVE - 已清理源挂载点 ${sourceMountResult.id} 的缓存`);
        }

        if (destMountResult && destMountResult.id !== sourceMountResult?.id) {
          await clearDirectoryCache({ mountId: destMountResult.id });
          console.log(`WebDAV MOVE - 已清理目标挂载点 ${destMountResult.id} 的缓存`);
        }
      } catch (cacheError) {
        // 缓存清理失败不应该影响移动操作的成功响应
        console.warn(`WebDAV MOVE - 缓存清理失败: ${cacheError.message}`);
      }
    }

    // 13. 根据RFC 4918标准返回适当的状态码
    if (destExists) {
      // 目标已存在，移动成功（覆盖了现有资源）
      console.log(`WebDAV MOVE - 移动成功（覆盖现有资源）: ${path} -> ${destPath}`);
      return new Response(null, {
        status: 204, // No Content
        headers: getStandardWebDAVHeaders({
          customHeaders: {
            "Content-Type": "text/plain",
            "Content-Length": "0",
          },
        }),
      });
    } else {
      // 目标不存在，移动成功（创建了新资源）
      console.log(`WebDAV MOVE - 移动成功（创建新资源）: ${path} -> ${destPath}`);
      return new Response(null, {
        status: 201, // Created
        headers: getStandardWebDAVHeaders({
          customHeaders: {
            "Content-Type": "text/plain",
            "Content-Length": "0",
            Location: destination,
          },
        }),
      });
    }
  } catch (error) {
    console.error(`WebDAV MOVE - 处理异常: ${error.message}`, error);
    return createWebDAVErrorResponse(`服务器内部错误: ${error.message}`, 500, false);
  }
}
