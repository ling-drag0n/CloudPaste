/**
 * 处理WebDAV GET请求
 * 用于获取文件内容
 * 重构版本：使用FileSystem统一抽象层替代直接S3调用
 */
import { MountManager } from "../../storage/managers/MountManager.js";
import { FileSystem } from "../../storage/fs/FileSystem.js";
import { handleWebDAVError, createWebDAVErrorResponse } from "../utils/errorUtils.js";
import { addWebDAVHeaders, getStandardWebDAVHeaders } from "../utils/headerUtils.js";
import { getEffectiveMimeType } from "../../utils/fileUtils.js";

/**
 * 处理GET请求
 * @param {Object} c - Hono上下文
 * @param {string} path - 请求路径
 * @param {string} userId - 用户ID
 * @param {string} userType - 用户类型 (admin 或 apiKey)
 * @param {D1Database} db - D1数据库实例
 */
export async function handleGet(c, path, userId, userType, db) {
  const isHead = c.req.method === "HEAD";

  try {
    // 创建FileSystem实例
    const mountManager = new MountManager(db, c.env.ENCRYPTION_SECRET);
    const fileSystem = new FileSystem(mountManager);

    // 获取文件名并统一从文件名推断MIME类型
    const fileName = path.split("/").pop();
    const contentType = getEffectiveMimeType(null, fileName);
    console.log(`WebDAV GET - 从文件名[${fileName}]推断MIME类型: ${contentType}`);

    // 处理条件请求头
    const ifNoneMatch = c.req.header("If-None-Match");
    const ifModifiedSince = c.req.header("If-Modified-Since");
    const ifMatch = c.req.header("If-Match");
    const ifUnmodifiedSince = c.req.header("If-Unmodified-Since");

    // 首先获取文件信息以检查条件请求
    let fileInfo;
    try {
      fileInfo = await fileSystem.getFileInfo(path, userId, userType);
    } catch (error) {
      if (error.status === 404) {
        return createWebDAVErrorResponse("文件不存在", 404);
      }
      throw error;
    }

    // 从文件信息中提取元数据
    const etag = fileInfo.etag ? `"${fileInfo.etag}"` : "";
    const lastModified = fileInfo.modified ? new Date(fileInfo.modified) : new Date();
    const lastModifiedStr = lastModified.toUTCString();
    const contentLength = fileInfo.size || 0;

    // 检查ETag匹配（如果提供了If-None-Match头）
    if (ifNoneMatch && etag) {
      // 移除引号以进行比较
      const clientEtag = ifNoneMatch.replace(/^"(.*)"$/, "$1");
      const serverEtag = etag.replace(/^"(.*)"$/, "$1");

      if (clientEtag === serverEtag || clientEtag === "*") {
        console.log(`GET请求: ETag匹配 ${etag}，返回304 Not Modified`);
        return new Response(null, {
          status: 304, // Not Modified
          headers: {
            ETag: etag,
            "Last-Modified": lastModifiedStr,
            "Cache-Control": "max-age=3600",
          },
        });
      }
    }

    // 检查修改时间（如果提供了If-Modified-Since头且没有If-None-Match头或ETag不匹配）
    if (ifModifiedSince && !ifNoneMatch) {
      try {
        const modifiedSinceDate = new Date(ifModifiedSince);

        // 将时间戳向下取整到秒，因为HTTP日期不包含毫秒
        const modifiedSinceTime = Math.floor(modifiedSinceDate.getTime() / 1000) * 1000;
        const lastModifiedTime = Math.floor(lastModified.getTime() / 1000) * 1000;

        if (lastModifiedTime <= modifiedSinceTime) {
          console.log(`GET请求: 文件未修改，返回304 Not Modified`);
          return new Response(null, {
            status: 304, // Not Modified
            headers: {
              ETag: etag,
              "Last-Modified": lastModifiedStr,
              "Cache-Control": "max-age=3600",
            },
          });
        }
      } catch (dateError) {
        console.warn(`GET请求: If-Modified-Since头格式无效: ${ifModifiedSince}`);
        // 如果日期格式无效，忽略此头，继续处理请求
      }
    }

    // 处理If-Match头（确保资源匹配）
    if (ifMatch && etag) {
      const clientEtag = ifMatch.replace(/^"(.*)"$/, "$1");
      const serverEtag = etag.replace(/^"(.*)"$/, "$1");

      if (clientEtag !== "*" && clientEtag !== serverEtag) {
        console.log(`GET请求: If-Match条件不满足 ${ifMatch} != ${etag}`);
        return createWebDAVErrorResponse("资源已被修改", 412); // Precondition Failed
      }
    }

    // 处理If-Unmodified-Since头
    if (ifUnmodifiedSince) {
      try {
        const unmodifiedSinceDate = new Date(ifUnmodifiedSince);

        // 将时间戳向下取整到秒
        const unmodifiedSinceTime = Math.floor(unmodifiedSinceDate.getTime() / 1000) * 1000;
        const lastModifiedTime = Math.floor(lastModified.getTime() / 1000) * 1000;

        if (lastModifiedTime > unmodifiedSinceTime) {
          console.log(`GET请求: If-Unmodified-Since条件不满足`);
          return createWebDAVErrorResponse("资源已被修改", 412); // Precondition Failed
        }
      } catch (dateError) {
        console.warn(`GET请求: If-Unmodified-Since头格式无效: ${ifUnmodifiedSince}`);
        // 如果日期格式无效，忽略此头，继续处理请求
      }
    }

    // 如果是HEAD请求，返回头信息
    if (isHead) {
      return new Response(null, {
        status: 200,
        headers: {
          "Content-Length": String(contentLength),
          "Content-Type": contentType,
          "Last-Modified": lastModifiedStr,
          ETag: etag,
          "Accept-Ranges": "bytes",
          "Cache-Control": "max-age=3600",
        },
      });
    }

    // 根据挂载点的webdav_policy配置决定处理方式
    const { mount } = await mountManager.getDriverByPath(path, userId, userType);

    if (mount.webdav_policy === "302_redirect") {
      // 302重定向模式：生成预签名URL并重定向
      console.log(`WebDAV GET - 使用302重定向模式: ${path}`);

      const presignedResult = await fileSystem.generatePresignedUrl(path, userId, userType, {
        operation: "download",
        forceDownload: false, // WebDAV通常不强制下载
      });

      return new Response(null, {
        status: 302,
        headers: getStandardWebDAVHeaders({
          customHeaders: {
            Location: presignedResult.presignedUrl,
            "Cache-Control": "no-cache",
          },
        }),
      });
    } else {
      // 代理模式（默认）：使用FileSystem下载文件
      console.log(`WebDAV GET - 使用代理模式: ${path}`);
      const fileResponse = await fileSystem.downloadFile(path, fileName, c.req, userId, userType);

      // 更新响应头以包含WebDAV特有的头信息
      const updatedHeaders = new Headers(fileResponse.headers);
      updatedHeaders.set("Content-Type", contentType);
      updatedHeaders.set("Last-Modified", lastModifiedStr);
      if (etag) {
        updatedHeaders.set("ETag", etag);
      }
      updatedHeaders.set("Accept-Ranges", "bytes");
      updatedHeaders.set("Cache-Control", "max-age=3600");

      // 创建响应并添加WebDAV标准头部
      const response = new Response(fileResponse.body, {
        status: fileResponse.status,
        headers: updatedHeaders,
      });

      return addWebDAVHeaders(response);
    }
  } catch (error) {
    // 使用统一的错误处理
    return handleWebDAVError("GET", error);
  }
}
