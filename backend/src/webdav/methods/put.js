/**
 * 处理WebDAV PUT请求
 * 用于上传文件内容
 */
import { MountManager } from "../../storage/managers/MountManager.js";
import { FileSystem } from "../../storage/fs/FileSystem.js";
import { getEffectiveMimeType } from "../../utils/fileUtils.js";
import { handleWebDAVError } from "../utils/errorUtils.js";
import { getStandardWebDAVHeaders } from "../utils/headerUtils.js";
import { clearDirectoryCache } from "../../cache/index.js";
import { getSettingsByGroup } from "../../services/systemService.js";
import { lockManager } from "../utils/LockManager.js";
import { checkLockPermission } from "../utils/lockUtils.js";

/**
 * 获取WebDAV上传模式设置
 * @param {D1Database} db - 数据库实例
 * @returns {Promise<string>} 上传模式 ('direct' 或 'multipart')
 */
async function getWebDAVUploadMode(db) {
  try {
    // WebDAV设置组ID为3
    const settings = await getSettingsByGroup(db, 3, false);
    const uploadModeSetting = settings.find((setting) => setting.key === "webdav_upload_mode");
    return uploadModeSetting ? uploadModeSetting.value : "multipart"; // 默认分片上传
  } catch (error) {
    console.warn("获取WebDAV上传模式设置失败，使用默认值:", error);
    return "direct"; // 默认分片上传
  }
}

/**
 * 智能检测是否为真正的空文件
 * @param {number} contentLength - Content-Length头部值
 * @param {string|null} transferEncoding - Transfer-Encoding头部值
 * @returns {boolean} 是否为真正的空文件
 */
function isReallyEmptyFile(contentLength, transferEncoding) {
  // 如果Content-Length大于0，肯定不是空文件
  if (contentLength > 0) {
    return false;
  }

  // 如果有Transfer-Encoding: chunked，不能仅依赖Content-Length判断
  if (transferEncoding && transferEncoding.toLowerCase().includes("chunked")) {
    return false; // 分块传输时，使用流式分片上传
  }

  // Content-Length为0且没有分块传输，才是真正的空文件
  return true;
}

/**
 * 处理PUT请求
 * @param {Object} c - Hono上下文
 * @param {string} path - 请求路径
 * @param {string} userId - 用户ID
 * @param {string} userType - 用户类型 (admin 或 apiKey)
 * @param {D1Database} db - D1数据库实例
 */
export async function handlePut(c, path, userId, userType, db) {
  try {
    // 获取加密密钥
    const encryptionSecret = c.env.ENCRYPTION_SECRET;
    if (!encryptionSecret) {
      throw new Error("缺少加密密钥配置");
    }

    // 创建挂载管理器和文件系统
    const mountManager = new MountManager(db, encryptionSecret);
    const fileSystem = new FileSystem(mountManager);

    // 在PUT时自动创建父目录
    const parentPath = path.substring(0, path.lastIndexOf("/"));
    if (parentPath && parentPath !== "/" && parentPath !== "") {
      try {
        console.log(`WebDAV PUT - 确保父目录存在: ${parentPath}`);
        await fileSystem.createDirectory(parentPath, userId, userType);
        console.log(`WebDAV PUT - 父目录已确保存在: ${parentPath}`);
      } catch (error) {
        // 如果目录已存在（409 Conflict），这是正常情况，继续上传
        if (error.status === 409 || error.message?.includes("已存在") || error.message?.includes("exists")) {
          console.log(`WebDAV PUT - 父目录已存在，继续上传: ${parentPath}`);
        } else {
          // 其他错误（如权限不足、存储空间不足等）应该阻止上传
          console.error(`WebDAV PUT - 创建父目录失败: ${error.message}`);
          throw error;
        }
      }
    }

    // 获取WebDAV上传模式设置
    const uploadMode = await getWebDAVUploadMode(db);
    console.log(`WebDAV PUT - 使用配置的上传模式: ${uploadMode}`);

    // 检查锁定状态
    const ifHeader = c.req.header("If");
    const lockConflict = checkLockPermission(lockManager, path, ifHeader, "PUT");
    if (lockConflict) {
      return new Response(lockConflict.message, {
        status: lockConflict.status,
        headers: { "Content-Type": "text/plain" },
      });
    }

    // 获取请求头信息
    const contentLengthHeader = c.req.header("content-length");
    const clientContentType = c.req.header("content-type");
    const transferEncoding = c.req.header("transfer-encoding");
    const declaredContentLength = contentLengthHeader ? parseInt(contentLengthHeader, 10) : 0;

    // 智能MIME类型检测：优先使用文件名推断，客户端类型作为备选
    const contentType = getEffectiveMimeType(clientContentType, path);

    console.log(`WebDAV PUT - 开始处理: ${path}, 声明大小: ${declaredContentLength} 字节, 类型: ${contentType}`);

    // 获取请求体流
    const bodyStream = c.req.body;
    if (!bodyStream) {
      throw new Error("请求体为空");
    }

    const filename = path.split("/").pop();

    // 使用智能空文件检测
    const isEmptyFile = isReallyEmptyFile(declaredContentLength, transferEncoding);

    if (isEmptyFile) {
      console.log(`WebDAV PUT - 确认为空文件，使用FileSystem直接上传`);

      // 创建一个空的File对象
      const emptyFile = new File([""], filename, { type: contentType });

      // 使用FileSystem上传空文件
      const result = await fileSystem.uploadFile(path, emptyFile, userId, userType, {
        useMultipart: false,
      });

      // 清理缓存
      const { mount } = await mountManager.getDriverByPath(path, userId, userType);
      if (mount) {
        await clearDirectoryCache({ mountId: mount.id });
      }

      console.log(`WebDAV PUT - 空文件上传成功`);

      return new Response(null, {
        status: 201, // Created
        headers: getStandardWebDAVHeaders({
          customHeaders: {
            "Content-Type": "text/plain",
            "Content-Length": "0",
          },
        }),
      });
    }

    // 直接使用原始流
    const processedStream = bodyStream;

    // 对分块传输强制使用multipart
    let finalUploadMode = uploadMode;
    if (transferEncoding && transferEncoding.toLowerCase().includes("chunked")) {
      finalUploadMode = "multipart";
      console.log(`WebDAV PUT - 检测到分块传输，强制使用流式分片上传`);
    }

    // 根据最终决定的上传模式处理
    if (finalUploadMode === "direct") {
      console.log(`WebDAV PUT - 使用直接上传模式`);

      try {
        // 使用FileSystem抽象层的uploadStream方法
        const startTime = Date.now();
        const result = await fileSystem.uploadStream(path, processedStream, userId, userType, {
          filename: filename,
          contentType: contentType,
          contentLength: declaredContentLength,
          useMultipart: false, // direct模式使用直接上传
        });
        const duration = Date.now() - startTime;

        const speedMBps = declaredContentLength > 0 ? (declaredContentLength / 1024 / 1024 / (duration / 1000)).toFixed(2) : "未知";
        console.log(`WebDAV PUT - 直接上传成功，用时: ${duration}ms，速度: ${speedMBps}MB/s，ETag: ${result.etag}`);

        return new Response(null, {
          status: 201, // Created
          headers: getStandardWebDAVHeaders({
            customHeaders: {
              "Content-Type": "text/plain",
              "Content-Length": "0",
              ETag: result.etag || "",
            },
          }),
        });
      } catch (error) {
        console.error(`WebDAV PUT - 直接上传失败: ${error.message}`);
        throw error;
      }
    } else {
      // 使用分片上传模式（流式上传）
      console.log(`WebDAV PUT - 使用流式分片上传模式`);

      try {
        // 使用FileSystem抽象层的uploadStream方法
        const startTime = Date.now();
        const result = await fileSystem.uploadStream(path, processedStream, userId, userType, {
          filename: filename,
          contentType: contentType,
          contentLength: declaredContentLength,
          useMultipart: true, // multipart模式使用分片上传
        });
        const duration = Date.now() - startTime;

        const speedMBps = declaredContentLength > 0 ? (declaredContentLength / 1024 / 1024 / (duration / 1000)).toFixed(2) : "未知";
        console.log(`WebDAV PUT - 流式分片上传成功，用时: ${duration}ms，速度: ${speedMBps}MB/s，ETag: ${result.etag}`);

        return new Response(null, {
          status: 201, // Created
          headers: getStandardWebDAVHeaders({
            customHeaders: {
              "Content-Type": "text/plain",
              "Content-Length": "0",
              ETag: result.etag || "",
            },
          }),
        });
      } catch (error) {
        console.error(`WebDAV PUT - 流式分片上传失败: ${error.message}`);
        throw error;
      }
    }
  } catch (error) {
    console.error(`WebDAV PUT - 处理失败: ${error.message}`);
    return handleWebDAVError(error, `PUT ${path}`);
  }
}
