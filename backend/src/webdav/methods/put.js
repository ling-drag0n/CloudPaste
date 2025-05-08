/**
 * 处理WebDAV PUT请求
 * 用于上传文件内容
 */
import { findMountPointByPath, normalizeS3SubPath, updateMountLastUsed, checkDirectoryExists } from "../utils/webdavUtils.js"; // normalizeS3SubPath 可能不再直接使用
import { createS3Client } from "../../utils/s3Utils.js";
import { PutObjectCommand, CreateMultipartUploadCommand, UploadPartCommand, CompleteMultipartUploadCommand, AbortMultipartUploadCommand } from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";
import { getMimeType } from "../../utils/fileUtils.js";
import { initializeMultipartUpload, uploadPart, completeMultipartUpload, abortMultipartUpload } from "../../services/multipartUploadService.js"; // 假设这些服务存在
import { clearCacheAfterWebDAVOperation } from "../utils/cacheUtils.js"; // 假设缓存工具存在

// 分片上传阈值等常量...
const MULTIPART_THRESHOLD = 5 * 1024 * 1024;
const WINDOWS_CLIENT_MULTIPART_THRESHOLD = 1 * 1024 * 1024;
const MAX_RETRIES = 3;
const RETRY_DELAY_BASE = 1000;
const PROGRESS_LOG_INTERVAL = 20 * 1024 * 1024;

// ... (identifyClient, concatenateArrayBuffers, ensureArrayBuffer, uploadPartWithRetry, processStreamInChunks, checkSizeDifference 等辅助函数保持不变，除非需要调整) ...
// (确保这些辅助函数在此文件或导入的utils中可用)

/**
 * 处理PUT请求
 */
export async function handlePut(c, path, userId, userType, db) {
  const requestStartTime = Date.now();
  console.log(`[PUT_DEBUG] Entered handlePut. webdavPath: "${path}", userId: "${userId}", userType: "${userType}"`);

  try {
    const clientInfo = identifyClient(c); // 假设 identifyClient 函数存在
    console.log(`[PUT_DEBUG] Client User-Agent: ${clientInfo.userAgent}`);

    const contentLengthHeader = c.req.header("Content-Length");
    const transferEncodingHeader = c.req.header("Transfer-Encoding") || "";
    const isChunkedEncoding = transferEncodingHeader.toLowerCase().includes("chunked");
    const declaredContentLength = contentLengthHeader ? parseInt(contentLengthHeader, 10) : -1;
    console.log(`[PUT_DEBUG] Content-Length: ${declaredContentLength > -1 ? declaredContentLength + ' bytes' : 'Not provided'}, Transfer-Encoding: ${transferEncodingHeader || 'N/A'}`);

    // 1. 找到挂载点
    const mountResult = await findMountPointByPath(db, path, userId, userType);
    if (mountResult.error) {
        console.error(`[PUT_ERROR] Mount point not found or error for path "${path}": ${mountResult.error.status} ${mountResult.error.message}`);
        return new Response(mountResult.error.message, { status: mountResult.error.status });
    }
    const { mount, subPath } = mountResult;
    console.log(`[PUT_DEBUG] Found matchingMount: id="${mount.id}", name="${mount.name}", mount_path="${mount.mount_path}", storage_config_id="${mount.storage_config_id}", calculated subPath: "${subPath}"`);


    // 2. 获取S3配置
    const s3Config = await db.prepare("SELECT * FROM s3_configs WHERE id = ?").bind(mount.storage_config_id).first();
    if (!s3Config) {
      console.error(`[PUT_ERROR] Storage Configuration Not Found for mount_id: "${mount.id}", storage_config_id: "${mount.storage_config_id}"`);
      return new Response("存储配置不存在", { status: 404 });
    }
    console.log(`[PUT_DEBUG] Fetched s3Config: id="${s3Config.id}", bucket_name="${s3Config.bucket_name}", default_folder="${s3Config.default_folder || '(empty)'}"`);


    // 检查是否尝试向目录路径PUT (WebDAV路径以/结尾)
    if (path.endsWith("/")) {
      console.error(`[PUT_ERROR] Attempted to PUT content to a collection path: "${path}"`);
      return new Response("不能向目录路径上传文件内容", { status: 405 }); // Method Not Allowed
    }

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
    if (mountPathSegment && mountPathSegment !== '/') {
        if (!finalS3ObjectKey.endsWith('/') && finalS3ObjectKey !== '') finalS3ObjectKey += '/';
         // Fix: Ensure mountPathSegment itself doesn't force a trailing slash if it's the last component before the file
         // finalS3ObjectKey += mountPathSegment.endsWith('/') ? mountPathSegment : mountPathSegment + '/'; // No, mount segment should act like a folder prefix
        finalS3ObjectKey += mountPathSegment + (mountPathSegment.endsWith('/') ? '' : '/');

    }

    let subPathSegment = subPath; // e.g., "/file.txt" or "/folder/file.txt"
    if (subPathSegment.startsWith('/')) subPathSegment = subPathSegment.substring(1);
    // Sub path for PUT is a file, should not end with /
    if (subPathSegment.endsWith('/')) {
         console.error(`[PUT_ERROR] Calculated subPathSegment "${subPathSegment}" ends with a slash, but PUT target should be a file.`);
         // Optionally remove trailing slash for files? Or rely on upstream path check? Let's remove it defensively.
         subPathSegment = subPathSegment.slice(0,-1);
    }

    finalS3ObjectKey += subPathSegment;
    finalS3ObjectKey = finalS3ObjectKey.replace(/\/+/g, "/");
     if (finalS3ObjectKey.startsWith('/') && finalS3ObjectKey !== "/") {
        finalS3ObjectKey = finalS3ObjectKey.substring(1);
    }
    console.log(`[PUT_DEBUG] Original WebDAV path: "${path}", Mount path: "${mount.mount_path}", Sub-path within mount: "${subPath}"`);
    console.log(`[PUT_DEBUG] S3 default_folder: "${s3Config.default_folder || '(empty)'}"`);
    console.log(`[PUT_DEBUG] Calculated final finalS3ObjectKey for S3 PUT: "${finalS3ObjectKey}"`);

    if (!finalS3ObjectKey || finalS3ObjectKey.endsWith('/')) { // Key should not be empty or end with / for PUT file
         console.error(`[PUT_ERROR] Invalid calculated S3 Key "${finalS3ObjectKey}" for WebDAV path "${path}".`);
         return new Response("无法确定目标文件路径", 500);
    }
    // --- S3对象键计算逻辑结束 ---


    // 5. 获取Content-Type
    const filename = finalS3ObjectKey.split("/").pop();
    let contentType = c.req.header("Content-Type") || "application/octet-stream";
    if (contentType.includes(";")) contentType = contentType.split(";")[0].trim();
    if (!contentType || contentType === "application/octet-stream") {
      contentType = getMimeType(filename); // 假设 getMimeType 存在
    }
    console.log(`[PUT_DEBUG] Filename: "${filename}", Content-Type: "${contentType}"`);


    // 6. 检查并创建父目录 (使用修正后的路径逻辑)
    let s3ParentPrefix = "";
    if (finalS3ObjectKey.includes('/')) {
        s3ParentPrefix = finalS3ObjectKey.substring(0, finalS3ObjectKey.lastIndexOf('/') + 1);
        console.log(`[PUT_DEBUG] Checking if parent S3 prefix exists: "${s3ParentPrefix}"`);
        try {
            const parentExists = await checkDirectoryExists(s3Client, s3Config.bucket_name, s3ParentPrefix); // 假设 checkDirectoryExists 存在
            if (!parentExists) {
                console.log(`[PUT_INFO] Parent S3 prefix "${s3ParentPrefix}" does not exist. Attempting to create.`);
                const createDirParams = { Bucket: s3Config.bucket_name, Key: s3ParentPrefix, Body: "", ContentType: "application/x-directory" };
                await s3Client.send(new PutObjectCommand(createDirParams));
                console.log(`[PUT_INFO] Successfully created parent S3 prefix "${s3ParentPrefix}".`);
            } else {
                console.log(`[PUT_DEBUG] Parent S3 prefix "${s3ParentPrefix}" exists.`);
            }
        } catch (dirError) {
             console.error(`[PUT_ERROR] Failed to check or create parent S3 prefix "${s3ParentPrefix}":`, dirError, dirError.stack);
             // Decide whether to proceed or fail. Let's try proceeding.
             console.warn(`[PUT_WARN] Proceeding with PUT despite parent directory check/creation error.`);
        }
    } else {
        console.log(`[PUT_DEBUG] Target path "${finalS3ObjectKey}" is in the effective root. No parent directory check needed.`);
    }


    // 7. 根据设置和文件大小选择上传模式 (direct, proxy, multipart)
    // (这里的逻辑保持不变，但确保它使用的 S3 Key 是我们新计算的 finalS3ObjectKey)
    // ... 获取 webdavUploadMode 设置 ...
    const effectiveThreshold = clientInfo.isPotentiallyProblematicClient ? WINDOWS_CLIENT_MULTIPART_THRESHOLD : MULTIPART_THRESHOLD;
    let webdavUploadMode = "auto"; // Default
     try {
        const setting = await db.prepare("SELECT value FROM system_settings WHERE key = ?").bind("webdav_upload_mode").first();
        if (setting && setting.value) webdavUploadMode = setting.value;
     } catch (e) { console.warn("[PUT_WARN] Failed to get webdav_upload_mode setting", e);}
     console.log(`[PUT_DEBUG] WebDAV Upload Mode setting: ${webdavUploadMode}`);

    const DIRECT_THRESHOLD = 10 * 1024 * 1024;
    const PROXY_THRESHOLD = 50 * 1024 * 1024;

    const shouldUseProxy = webdavUploadMode === "proxy" || (webdavUploadMode === "auto" && declaredContentLength > PROXY_THRESHOLD);
    const shouldUseDirect = webdavUploadMode === "direct" || (webdavUploadMode === "auto" && declaredContentLength > 0 && declaredContentLength <= DIRECT_THRESHOLD);
    const isZeroByteFile = declaredContentLength === 0; // Handle 0 byte specifically


    // --- 上传逻辑分支 ---

    if (isZeroByteFile) {
        // 处理0字节文件
        console.log(`[PUT_DEBUG] Handling 0-byte file upload directly for Key: "${finalS3ObjectKey}"`);
        const putParams = { Bucket: s3Config.bucket_name, Key: finalS3ObjectKey, Body: "", ContentType: contentType };
        await s3Client.send(new PutObjectCommand(putParams));
    } else if (shouldUseProxy) {
        // 使用代理模式 (proxy or large file in auto)
        console.log(`[PUT_DEBUG] Using proxy upload mode for Key: "${finalS3ObjectKey}"`);
        try {
             const putCommand = new PutObjectCommand({ Bucket: s3Config.bucket_name, Key: finalS3ObjectKey, ContentType: contentType });
             const presignedUrl = await getSignedUrl(s3Client, putCommand, { expiresIn: 3600 });
             const proxyResponse = await proxyUploadToS3(c, presignedUrl, contentType); // 假设 proxyUploadToS3 存在
             if (proxyResponse.status < 200 || proxyResponse.status >= 300) {
                 // Propagate error if proxy failed
                 return proxyResponse;
             }
        } catch (proxyError) {
            console.error(`[PUT_ERROR] Proxy upload failed for Key "${finalS3ObjectKey}", falling back if possible:`, proxyError);
             // Decide if fallback is needed/possible. If mode was 'proxy', maybe fail here.
            if (webdavUploadMode === 'proxy') {
                 return new Response("Proxy upload failed", { status: 502 });
            }
            // If auto, maybe fall back to multipart? For now, let outer catch handle.
            throw proxyError;
        }
    } else if (shouldUseDirect) {
        // 使用直接上传模式 (direct or small file in auto)
        console.log(`[PUT_DEBUG] Using direct upload mode for Key: "${finalS3ObjectKey}"`);
        try {
             const bodyBuffer = await c.req.arrayBuffer(); // Read entire body - careful with large files if logic leads here incorrectly
             console.log(`[PUT_DEBUG] Read ${bodyBuffer.byteLength} bytes for direct upload.`);
             if (declaredContentLength > 0 && bodyBuffer.byteLength !== declaredContentLength) {
                 console.warn(`[PUT_WARN] Direct upload size mismatch: Declared=${declaredContentLength}, Actual=${bodyBuffer.byteLength}`);
             }
             const putParams = { Bucket: s3Config.bucket_name, Key: finalS3ObjectKey, Body: bodyBuffer, ContentType: contentType };
             await s3Client.send(new PutObjectCommand(putParams));
        } catch (directError) {
             console.error(`[PUT_ERROR] Direct upload failed for Key "${finalS3ObjectKey}", falling back if possible:`, directError);
              if (webdavUploadMode === 'direct') {
                 return new Response("Direct upload failed", { status: 500 });
              }
              // If auto, fall back to multipart? For now, let outer catch handle.
              throw directError;
        }

    } else {
        // 使用分片上传模式 (multipart or medium file in auto or fallback)
        console.log(`[PUT_DEBUG] Using multipart upload mode for Key: "${finalS3ObjectKey}"`);
        let uploadId = null; // Keep track for potential abort
        try {
            const initResult = await initializeMultipartUpload(db, path /* Use webdav path? */, contentType, declaredContentLength, userId, userType, c.env.ENCRYPTION_SECRET, finalS3ObjectKey /* Pass correct S3 Key */ );
            uploadId = initResult.uploadId;
             const s3KeyForMultipart = initResult.key; // Use key from init result
             if (s3KeyForMultipart !== finalS3ObjectKey) {
                 console.warn(`[PUT_WARN] S3 Key from multipart init ("${s3KeyForMultipart}") differs from calculated key ("${finalS3ObjectKey}"). Using key from init.`);
             }

            const recommendedPartSize = initResult.recommendedPartSize || effectiveThreshold;
            console.log(`[PUT_DEBUG] Multipart initialized. UploadId: ${uploadId}, PartSize: ${recommendedPartSize} bytes`);

            const uploadPartCallback = async (partNumber, partData) => {
                return await uploadPartWithRetry(db, path, uploadId, partNumber, partData, userId, userType, c.env.ENCRYPTION_SECRET, s3KeyForMultipart); // Use correct S3 Key
            };
            // 假设 processStreamInChunks 存在
            const { parts, totalProcessed } = await processStreamInChunks(c.req.body, recommendedPartSize, uploadPartCallback, { isSpecialClient: clientInfo.isPotentiallyProblematicClient, contentLength: declaredContentLength, originalStream: c.req.body});

             if (declaredContentLength > 0 && !checkSizeDifference(totalProcessed, declaredContentLength)) { // 假设 checkSizeDifference 存在
                console.warn(`[PUT_WARN] Multipart upload size difference significant: Declared=${declaredContentLength}, Actual=${totalProcessed}`);
             }

            console.log(`[PUT_DEBUG] Completing multipart upload. UploadId: ${uploadId}, Parts: ${parts.length}, TotalBytes: ${totalProcessed}`);
            await completeMultipartUpload(db, path, uploadId, parts, userId, userType, c.env.ENCRYPTION_SECRET, s3KeyForMultipart, contentType, totalProcessed, false);

        } catch(multipartError) {
             console.error(`[PUT_ERROR] Multipart upload failed for Key "${finalS3ObjectKey}":`, multipartError);
             if (uploadId) {
                 try {
                     console.log(`[PUT_INFO] Aborting multipart upload: ${uploadId}`);
                     // Pass the correct S3 key used for init
                     const keyForAbort = s3KeyForMultipart || finalS3ObjectKey;
                     await abortMultipartUpload(db, path, uploadId, userId, userType, c.env.ENCRYPTION_SECRET, keyForAbort);
                 } catch (abortError) {
                      console.error(`[PUT_ERROR] Failed to abort multipart upload ${uploadId}:`, abortError);
                 }
             }
             throw multipartError; // Re-throw
        }
    }

    // 8. 清理缓存并更新最后使用时间
    await updateMountLastUsed(db, mount.id);
    await finalizePutOperation(db, s3Client, s3Config, finalS3ObjectKey); // 假设 finalizePutOperation 存在

    const duration = Date.now() - requestStartTime;
    console.log(`[PUT_INFO] Successfully completed PUT for "${path}" (S3 Key: "${finalS3ObjectKey}") in ${duration}ms`);

    // 返回成功响应 (201 Created for new file, 204 No Content for overwrite - check if file existed before?)
    // For simplicity, often return 201 or 204. Let's use 201 as it implies resource state changed.
     return new Response(null, { status: 201 }); // Or 204 if preferred for overwrites

  } catch (error) {
    const errorId = Date.now().toString(36);
    console.error(`[PUT_ERROR] Unhandled error in handlePut for path "${path}" [ErrorID: ${errorId}]:`, error, error.stack ? error.stack : '');
    return new Response(`内部服务器错误 (错误ID: ${errorId})`, { status: 500 });
  }
}

// 假设 finalizePutOperation 存在且导入了
async function finalizePutOperation(db, s3Client, s3Config, s3Key) {
  try {
    await clearCacheAfterWebDAVOperation(db, s3Key, s3Config, false); // false because it's a file operation
  } catch (cacheError) {
    console.warn(`[PUT_WARN] Failed to clear cache for S3 key "${s3Key}":`, cacheError);
  }
}

// 假设 identifyClient, concatenateArrayBuffers, ensureArrayBuffer, uploadPartWithRetry,
// processStreamInChunks, checkSizeDifference, proxyUploadToS3, getMimeType,
// checkDirectoryExists, clearCacheAfterWebDAVOperation 等辅助函数都已定义或正确导入