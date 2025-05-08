/**
 * 处理WebDAV GET/HEAD请求
 * 用于获取文件内容或头信息
 */
import { findMountPointByPath, updateMountLastUsed } from "../utils/webdavUtils.js";
import { createS3Client } from "../../utils/s3Utils.js";
import { GetObjectCommand, HeadObjectCommand } from "@aws-sdk/client-s3";
import { handleWebDAVError, createWebDAVErrorResponse } from "../utils/errorUtils.js"; // 假设这些存在

export async function handleGet(c, path, userId, userType, db) {
  const isHead = c.req.method === "HEAD";
  const operation = isHead ? "HEAD" : "GET";
  console.log(`[${operation}_DEBUG] Entered handleGet. webdavPath: "${path}", userId: "${userId}", userType: "${userType}"`);

  try {
    // 1. 找到挂载点
    const mountResult = await findMountPointByPath(db, path, userId, userType);
    if (mountResult.error) {
        console.error(`[${operation}_ERROR] Mount point not found or error for path "${path}": ${mountResult.error.status} ${mountResult.error.message}`);
        return createWebDAVErrorResponse(mountResult.error.message, mountResult.error.status);
    }
    const { mount, subPath } = mountResult;
    console.log(`[${operation}_DEBUG] Found matchingMount: id="${mount.id}", name="${mount.name}", mount_path="${mount.mount_path}", storage_config_id="${mount.storage_config_id}", calculated subPath: "${subPath}"`);

    // 2. 获取S3配置
    const s3Config = await db.prepare("SELECT * FROM s3_configs WHERE id = ?").bind(mount.storage_config_id).first();
    if (!s3Config) {
      console.error(`[${operation}_ERROR] Storage Configuration Not Found for mount_id: "${mount.id}", storage_config_id: "${mount.storage_config_id}"`);
      return createWebDAVErrorResponse("存储配置不存在", 404);
    }
     console.log(`[${operation}_DEBUG] Fetched s3Config: id="${s3Config.id}", bucket_name="${s3Config.bucket_name}", default_folder="${s3Config.default_folder || '(empty)'}"`);


    // 3. 创建S3客户端
    const s3Client = await createS3Client(s3Config, c.env.ENCRYPTION_SECRET);

    // 4. --- 修正后的S3对象键计算逻辑 ---
    let finalS3ObjectKey = "";
    if (s3Config.default_folder) {
        finalS3ObjectKey = s3Config.default_folder;
        if (!finalS3ObjectKey.endsWith('/')) finalS3ObjectKey += '/';
    }
    let mountPathSegment = mount.mount_path;
    if (mountPathSegment.startsWith('/')) mountPathSegment = mountPathSegment.substring(1);
    // 对于文件操作，挂载点段不应该强制加斜杠，除非它是路径的一部分
    if (mountPathSegment && mountPathSegment !== '/') { // 如果挂载点不是根"/"
        if (!finalS3ObjectKey.endsWith('/') && finalS3ObjectKey !== '') finalS3ObjectKey += '/'; // 确保前面部分以/结尾
        finalS3ObjectKey += mountPathSegment; // 添加挂载点段
    }


    let subPathSegment = subPath;
    if (subPathSegment.startsWith('/')) subPathSegment = subPathSegment.substring(1);
    // 只有在base非空时才在前加/
    if(finalS3ObjectKey && subPathSegment) finalS3ObjectKey += '/';
    finalS3ObjectKey += subPathSegment;

    finalS3ObjectKey = finalS3ObjectKey.replace(/\/+/g, "/"); // 规范化斜杠
    // 文件Key不应以/结尾，除非是根目录下的文件（这种情况很少见）
    if (finalS3ObjectKey.endsWith('/') && finalS3ObjectKey !== '/') {
        finalS3ObjectKey = finalS3ObjectKey.slice(0, -1);
    }
     // S3 Key 通常不以 / 开头
    if (finalS3ObjectKey.startsWith('/') && finalS3ObjectKey !== "/") {
        finalS3ObjectKey = finalS3ObjectKey.substring(1);
    }


    console.log(`[${operation}_DEBUG] Original WebDAV path: "${path}", Mount path: "${mount.mount_path}", Sub-path within mount: "${subPath}"`);
    console.log(`[${operation}_DEBUG] S3 default_folder: "${s3Config.default_folder || '(empty)'}"`);
    console.log(`[${operation}_DEBUG] Calculated final finalS3ObjectKey for S3 ${operation}: "${finalS3ObjectKey}"`);

    if (!finalS3ObjectKey) {
         console.error(`[${operation}_ERROR] Calculated S3 Key is empty for WebDAV path "${path}". This likely indicates an issue with mount/subpath logic.`);
         return createWebDAVErrorResponse("无法确定目标文件", 500);
    }
    // --- S3对象键计算逻辑结束 ---


    // 5. 更新最后使用时间
    await updateMountLastUsed(db, mount.id); // 放到S3操作前或后均可，这里放前面

    // 6. 执行S3 HEAD或GET操作
    try {
      const headParams = {
        Bucket: s3Config.bucket_name,
        Key: finalS3ObjectKey,
      };
      console.log(`[${operation}_DEBUG] S3 HeadObjectCommand params: Bucket='${s3Config.bucket_name}', Key='${finalS3ObjectKey}'`);
      const headResponse = await s3Client.send(new HeadObjectCommand(headParams));
      console.log(`[${operation}_DEBUG] S3 HeadObjectCommand successful. Size: ${headResponse.ContentLength}, Type: ${headResponse.ContentType}, ETag: ${headResponse.ETag}`);


      if (isHead) {
        // 返回HEAD响应
        return new Response(null, {
          status: 200,
          headers: {
            "Content-Length": String(headResponse.ContentLength || 0),
            "Content-Type": headResponse.ContentType || "application/octet-stream",
            "Last-Modified": headResponse.LastModified ? headResponse.LastModified.toUTCString() : new Date().toUTCString(),
            ETag: headResponse.ETag || "",
            "Accept-Ranges": "bytes", // 表明支持范围请求
          },
        });
      }

      // --- 处理GET请求 ---
      const getParams = {
        Bucket: s3Config.bucket_name,
        Key: finalS3ObjectKey,
      };

      // 处理Range请求
      const rangeHeader = c.req.header("Range");
      let isRangeRequest = false;
      if (rangeHeader) {
        console.log(`[GET_DEBUG] Received Range header: "${rangeHeader}"`);
        // 简单的 bytes=start-end 解析 (可能需要更健壮的解析库)
        const match = rangeHeader.match(/bytes=(\d+)-(\d*)/);
        if (match) {
          const start = parseInt(match[1], 10);
          const end = match[2] ? parseInt(match[2], 10) : undefined; // end是可选的
          // 验证范围的有效性 (可选但推荐)
          // if (!isNaN(start) && (end === undefined || (!isNaN(end) && end >= start))) {
             getParams.Range = `bytes=${start}-${end !== undefined ? end : ""}`;
             isRangeRequest = true;
             console.log(`[GET_DEBUG] Applying S3 Range: "${getParams.Range}"`);
          // } else {
          //    console.warn(`[GET_WARN] Invalid Range header format received: "${rangeHeader}"`);
          // }
        } else {
             console.warn(`[GET_WARN] Could not parse Range header: "${rangeHeader}"`);
        }

      }

      console.log(`[GET_DEBUG] S3 GetObjectCommand params: Bucket='${s3Config.bucket_name}', Key='${finalS3ObjectKey}'${getParams.Range ? `, Range='${getParams.Range}'` : ''}`);
      const getCommand = new GetObjectCommand(getParams);
      const getResponse = await s3Client.send(getCommand);
      console.log(`[GET_DEBUG] S3 GetObjectCommand successful. Status: ${getResponse.$metadata?.httpStatusCode}, ContentLength: ${getResponse.ContentLength}, ContentType: ${getResponse.ContentType}, ETag: ${getResponse.ETag}`);


      // 构建响应头
      const headers = {
        "Content-Type": getResponse.ContentType || "application/octet-stream",
        "Content-Length": String(getResponse.ContentLength || 0), // S3 SDK 应返回正确的长度，即使是范围请求
        "Last-Modified": getResponse.LastModified ? getResponse.LastModified.toUTCString() : new Date().toUTCString(),
        ETag: getResponse.ETag || "",
        "Accept-Ranges": "bytes", // 总是声明支持
      };

      let responseStatus = 200; // 默认OK

      // 处理分片响应 (206 Partial Content)
      if (isRangeRequest && getResponse.ContentRange) {
        headers["Content-Range"] = getResponse.ContentRange;
        responseStatus = 206;
        console.log(`[GET_DEBUG] Responding with 206 Partial Content. Content-Range: ${getResponse.ContentRange}`);
      } else if (isRangeRequest) {
          // 如果请求了范围但S3没有返回ContentRange（可能因为范围无效或S3不支持），则按200返回完整内容
           console.warn(`[GET_WARN] Range requested but ContentRange not present in S3 response. Returning 200 OK with full content.`);
      }


      // 返回文件流
      // 注意：getResponse.Body 应该是 ReadableStream
      return new Response(getResponse.Body, {
        status: responseStatus,
        headers,
      });

    } catch (error) {
      // 特别处理S3返回的404 Not Found错误
      if (error.name === 'NoSuchKey' || (error.$metadata && error.$metadata.httpStatusCode === 404)) {
        console.log(`[${operation}_INFO] S3 object not found for Key: "${finalS3ObjectKey}"`);
        return createWebDAVErrorResponse("文件不存在", 404);
      }
      // 处理无效范围请求的错误 (InvalidRange)
       if (error.name === 'InvalidRange') {
         console.warn(`[GET_WARN] S3 reported InvalidRange for Key: "${finalS3ObjectKey}", Range: "${getParams.Range}"`, error);
         // 返回 416 Range Not Satisfiable
         return createWebDAVErrorResponse("请求范围无效", 416, { 'Content-Range': `bytes */${headResponse.ContentLength || 0}` }); // Content-Length来自之前的HEAD
       }

      // 其他S3错误或网络错误
      console.error(`[${operation}_ERROR] Error during S3 operation for Key "${finalS3ObjectKey}":`, error, error.stack ? error.stack : '');
      throw error; // 重新抛出，让外层catch处理
    }
  } catch (error) {
    console.error(`[${operation}_ERROR] Unhandled error in handleGet for path "${path}":`, error, error.stack ? error.stack : '');
    // 使用统一的WebDAV错误处理（如果已定义）
    return handleWebDAVError ? handleWebDAVError(operation, error) : createWebDAVErrorResponse("内部服务器错误", 500);
  }
}