/**
 * 处理WebDAV COPY请求
 * 用于复制文件和目录
 */
import { MountManager } from "../../storage/managers/MountManager.js";
import { FileSystem } from "../../storage/fs/FileSystem.js";
import { handleWebDAVError, createWebDAVErrorResponse } from "../utils/errorUtils.js";
import { getStandardWebDAVHeaders } from "../utils/headerUtils.js";
import { parseDestinationPath } from "../utils/webdavUtils.js";
import { clearDirectoryCache } from "../../cache/index.js";
import { lockManager } from "../utils/LockManager.js";
import { checkLockPermission } from "../utils/lockUtils.js";

/**
 * 执行同步跨存储传输
 * @param {Object} crossStorageResult - 跨存储复制结果，包含下载和上传URL
 * @returns {Promise<Object>} 传输结果 {success: boolean, error?: string, message?: string}
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
 * @param {Object} dirResult - 目录跨存储复制结果
 * @returns {Promise<Object>} 传输结果
 */
async function performDirectoryCrossStorageTransfer(dirResult) {
  const { items, targetPath } = dirResult;

  try {
    console.log(`开始目录跨存储传输: ${targetPath}, 包含 ${items.length} 个文件`);

    // 配置并发传输参数
    const CONCURRENT_TRANSFERS = 3; // 限制同时传输3个文件
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
 * @param {Object} fileItem - 文件跨存储复制信息
 * @returns {Promise<Object>} 传输结果
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
 * 处理COPY请求
 * @param {Object} c - Hono上下文
 * @param {string} path - 请求路径
 * @param {string} userId - 用户ID
 * @param {string} userType - 用户类型 (admin 或 apiKey)
 * @param {D1Database} db - D1数据库实例
 */
export async function handleCopy(c, path, userId, userType, db) {
  try {
    // 1. 解析WebDAV头部
    const destination = c.req.header("Destination");
    const overwrite = c.req.header("Overwrite") || "T";
    const depth = c.req.header("Depth") || "infinity";
    const ifHeader = c.req.header("If");

    console.log(`WebDAV COPY - 请求头部: Destination=${destination}, Overwrite=${overwrite}, Depth=${depth}`);

    // 2. 验证必需的Destination头
    if (!destination) {
      console.warn(`WebDAV COPY - 缺少Destination头`);
      return createWebDAVErrorResponse("缺少Destination头", 400, false);
    }

    // 3. 解析目标路径
    const destPath = parseDestinationPath(destination);
    if (!destPath) {
      console.warn(`WebDAV COPY - 无效的Destination头: ${destination}`);
      return createWebDAVErrorResponse("无效的Destination头", 400, false);
    }

    // 检查目标路径的锁定状态（COPY操作会在目标位置创建新资源）
    const lockConflict = checkLockPermission(lockManager, destPath, ifHeader, "COPY");
    if (lockConflict) {
      console.log(`WebDAV COPY - 目标路径锁定冲突: ${destPath}`);
      return createWebDAVErrorResponse(lockConflict.message, lockConflict.status, false);
    }
    if (!destPath) {
      console.warn(`WebDAV COPY - 无效的Destination头: ${destination}`);
      return createWebDAVErrorResponse("无效的Destination头", 400, false);
    }

    // 4. 检查源路径和目标路径是否相同
    if (path === destPath) {
      console.warn(`WebDAV COPY - 源路径和目标路径相同: ${path}`);
      return createWebDAVErrorResponse("源路径和目标路径不能相同", 403, false);
    }

    // 5. 验证Depth头（对于集合资源）
    if (depth !== "0" && depth !== "infinity") {
      console.warn(`WebDAV COPY - 无效的Depth头: ${depth}`);
      return createWebDAVErrorResponse("无效的Depth头", 400, false);
    }

    // 6. 创建FileSystem实例
    const mountManager = new MountManager(db, c.env.ENCRYPTION_SECRET);
    const fileSystem = new FileSystem(mountManager);

    console.log(`WebDAV COPY - 开始复制: ${path} -> ${destPath}, 用户类型: ${userType}`);

    // 7. 检查目标是否已存在（用于确定返回的状态码）
    let destExists = false;
    try {
      destExists = await fileSystem.exists(destPath, userId, userType);
      console.log(`WebDAV COPY - 目标路径存在性检查: ${destPath} = ${destExists}`);
    } catch (error) {
      // exists方法出错，记录警告但继续执行
      console.warn(`WebDAV COPY - 检查目标路径存在性失败: ${error.message}`);
    }

    // 8. 如果目标存在且不允许覆盖，直接返回错误
    if (destExists && overwrite === "F") {
      console.warn(`WebDAV COPY - 目标已存在且不允许覆盖: ${destPath}`);
      return createWebDAVErrorResponse("目标已存在且不允许覆盖", 412, false); // Precondition Failed
    }

    // 9. 使用FileSystem统一抽象层执行复制
    // 将WebDAV的Overwrite头映射为FileSystem的skipExisting选项
    const result = await fileSystem.copyItem(path, destPath, userId, userType, {
      skipExisting: overwrite === "F", // Overwrite: F 表示不覆盖，即跳过已存在的文件
    });

    // console.log(`WebDAV COPY - 复制结果:`, result);

    // 10. 处理跨存储复制结果
    if (result.crossStorage) {
      // 根据RFC 2518 bis标准，跨存储COPY应该返回502 Bad Gateway
      // 但我们可以实现同步跨存储复制作为扩展功能
      console.log(`WebDAV COPY - 检测到跨存储复制，执行同步传输`);

      try {
        // 执行同步跨存储传输
        const transferResult = await performSyncCrossStorageTransfer(result);

        if (transferResult.success) {
          console.log(`WebDAV COPY - 跨存储复制成功: ${path} -> ${destPath}`);

          // 跨存储复制成功后，立即清理缓存
          try {
            const { mount: sourceMountResult } = await mountManager.getDriverByPath(path, userId, userType);
            const { mount: destMountResult } = await mountManager.getDriverByPath(destPath, userId, userType);

            if (sourceMountResult) {
              await clearDirectoryCache({ mountId: sourceMountResult.id });
              console.log(`WebDAV COPY - 已清理源挂载点 ${sourceMountResult.id} 的缓存`);
            }

            if (destMountResult && destMountResult.id !== sourceMountResult?.id) {
              await clearDirectoryCache({ mountId: destMountResult.id });
              console.log(`WebDAV COPY - 已清理目标挂载点 ${destMountResult.id} 的缓存`);
            }
          } catch (cacheError) {
            // 缓存清理失败不应该影响复制操作的成功响应
            console.warn(`WebDAV COPY - 跨存储复制后缓存清理失败: ${cacheError.message}`);
          }

          // 返回标准WebDAV成功响应
          return new Response(null, {
            status: 201, // Created - 跨存储复制总是创建新文件
            headers: getStandardWebDAVHeaders({
              customHeaders: {
                "Content-Type": "text/plain",
                "Content-Length": "0",
              },
            }),
          });
        } else {
          // 传输失败，返回错误
          console.error(`WebDAV COPY - 跨存储复制失败: ${transferResult.error}`);
          return createWebDAVErrorResponse(`跨存储复制失败: ${transferResult.error}`, 500, false);
        }
      } catch (error) {
        console.error(`WebDAV COPY - 跨存储复制异常: ${error.message}`, error);
        return createWebDAVErrorResponse(`跨存储复制失败: ${error.message}`, 500, false);
      }
    }

    // 11. 处理跳过的情况（这种情况理论上不应该发生，因为我们已经预先检查了）
    if (result.skipped === true || result.status === "skipped") {
      console.warn(`WebDAV COPY - 复制被跳过: ${path} -> ${destPath}`);
      return createWebDAVErrorResponse("目标已存在且不允许覆盖", 412, false); // Precondition Failed
    }

    // 12. 同存储复制的缓存清理（跨存储复制已在上面处理）
    try {
      const { mount: sourceMountResult } = await mountManager.getDriverByPath(path, userId, userType);
      const { mount: destMountResult } = await mountManager.getDriverByPath(destPath, userId, userType);

      if (sourceMountResult) {
        await clearDirectoryCache({ mountId: sourceMountResult.id });
        console.log(`WebDAV COPY - 已清理源挂载点 ${sourceMountResult.id} 的缓存`);
      }

      if (destMountResult && destMountResult.id !== sourceMountResult?.id) {
        await clearDirectoryCache({ mountId: destMountResult.id });
        console.log(`WebDAV COPY - 已清理目标挂载点 ${destMountResult.id} 的缓存`);
      }
    } catch (cacheError) {
      // 缓存清理失败不应该影响复制操作的成功响应
      console.warn(`WebDAV COPY - 缓存清理失败: ${cacheError.message}`);
    }

    console.log(`WebDAV COPY - 复制成功: ${path} -> ${destPath}`);

    // 13. 返回成功响应（符合WebDAV COPY标准）
    // 根据目标是否已存在返回正确的状态码
    const statusCode = destExists ? 204 : 201; // 204 No Content (覆盖) 或 201 Created (新建)
    const statusText = destExists ? "No Content" : "Created";

    console.log(`WebDAV COPY - 返回状态码: ${statusCode} (${statusText})`);

    return new Response(null, {
      status: statusCode,
      headers: getStandardWebDAVHeaders({
        customHeaders: {
          "Content-Type": "text/plain",
          "Content-Length": "0",
        },
      }),
    });
  } catch (error) {
    console.error(`WebDAV COPY - 处理错误: ${error.message}`, error);
    // 使用统一的WebDAV错误处理
    return handleWebDAVError("COPY", error, false, false);
  }
}
