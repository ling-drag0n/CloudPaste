/**
 * 处理WebDAV MKCOL请求
 * 用于创建目录
 */
import { findMountPointByPath, updateMountLastUsed, checkDirectoryExists } from "../utils/webdavUtils.js";
import { createS3Client } from "../../utils/s3Utils.js";
import { PutObjectCommand, ListObjectsV2Command } from "@aws-sdk/client-s3";
import { clearCacheAfterWebDAVOperation } from "../utils/cacheUtils.js"; // 假设缓存工具存在

export async function handleMkcol(c, path, userId, userType, db) {
  console.log(`[MKCOL_DEBUG] Entered handleMkcol. webdavPath: "${path}", userId: "${userId}", userType: "${userType}"`);

  try {
    // 1. 找到挂载点
    const mountResult = await findMountPointByPath(db, path, userId, userType);
    if (mountResult.error) {
      console.error(`[MKCOL_ERROR] Mount point not found or error for path "${path}": ${mountResult.error.status} ${mountResult.error.message}`);
      return new Response(mountResult.error.message, { status: mountResult.error.status });
    }
    const { mount, subPath } = mountResult; // subPath should end with / for MKCOL
     console.log(`[MKCOL_DEBUG] Found matchingMount: id="${mount.id}", name="${mount.name}", mount_path="${mount.mount_path}", storage_config_id="${mount.storage_config_id}", calculated subPath: "${subPath}"`);


    // 2. 检查请求体
    const body = await c.req.text();
    if (body.length > 0) {
      console.error(`[MKCOL_ERROR] MKCOL request for "${path}" contained a body.`);
      return new Response("MKCOL请求不应包含正文", { status: 415 });
    }

    // 3. 获取S3配置
    const s3Config = await db.prepare("SELECT * FROM s3_configs WHERE id = ?").bind(mount.storage_config_id).first();
    if (!s3Config) {
      console.error(`[MKCOL_ERROR] Storage Configuration Not Found for mount_id: "${mount.id}", storage_config_id: "${mount.storage_config_id}"`);
      return new Response("存储配置不存在", { status: 404 });
    }
     console.log(`[MKCOL_DEBUG] Fetched s3Config: id="${s3Config.id}", bucket_name="${s3Config.bucket_name}", default_folder="${s3Config.default_folder || '(empty)'}"`);


    // 4. 创建S3客户端
    const s3Client = await createS3Client(s3Config, c.env.ENCRYPTION_SECRET);

    // 5. --- 修正后的S3目录键计算逻辑 ---
    let finalS3DirectoryKey = "";
    if (s3Config.default_folder) {
        finalS3DirectoryKey = s3Config.default_folder;
        if (!finalS3DirectoryKey.endsWith('/')) finalS3DirectoryKey += '/';
    }
    let mountPathSegment = mount.mount_path;
    if (mountPathSegment.startsWith('/')) mountPathSegment = mountPathSegment.substring(1);
     if (mountPathSegment && mountPathSegment !== '/') {
        if (!finalS3DirectoryKey.endsWith('/') && finalS3DirectoryKey !== '') finalS3DirectoryKey += '/';
        finalS3DirectoryKey += mountPathSegment + (mountPathSegment.endsWith('/') ? '' : '/');
    }

    let subPathSegment = subPath; // e.g., "/newdir/"
    if (subPathSegment.startsWith('/')) subPathSegment = subPathSegment.substring(1);
    // Ensure trailing slash for directory key
    if (subPathSegment && !subPathSegment.endsWith('/')) {
       subPathSegment += '/';
    } else if (!subPathSegment && subPath === '/') { // Case for MKCOL on mount root itself (subPath="/")
       // finalS3DirectoryKey should already end with / from mountPathSegment or be empty if mountPath was /
       if (!finalS3DirectoryKey.endsWith('/') && finalS3DirectoryKey !== '') finalS3DirectoryKey += '/';
    }

    finalS3DirectoryKey += subPathSegment;
    finalS3DirectoryKey = finalS3DirectoryKey.replace(/\/+/g, "/");
     if (finalS3DirectoryKey.startsWith('/') && finalS3DirectoryKey !== "/") {
        finalS3DirectoryKey = finalS3DirectoryKey.substring(1);
    }
    console.log(`[MKCOL_DEBUG] Original WebDAV path: "${path}", Mount path: "${mount.mount_path}", Sub-path within mount: "${subPath}"`);
    console.log(`[MKCOL_DEBUG] S3 default_folder: "${s3Config.default_folder || '(empty)'}"`);
    console.log(`[MKCOL_DEBUG] Calculated final finalS3DirectoryKey for S3 MKCOL: "${finalS3DirectoryKey}"`);

    // MKCOL on the mount root itself needs careful handling - S3 doesn't really have root folders to "create"
    const isMkcolOnMountRoot = (subPath === '/');

    if (isMkcolOnMountRoot) {
        console.log(`[MKCOL_INFO] MKCOL request received for the mount root itself ("${path}"). Checking bucket accessibility.`);
        // Optionally check bucket access, then return success as the "folder" exists conceptually.
        try {
            await s3Client.send(new ListObjectsV2Command({ Bucket: s3Config.bucket_name, MaxKeys: 1 }));
            console.log(`[MKCOL_DEBUG] Bucket "${s3Config.bucket_name}" accessible.`);
        } catch (bucketError) {
             console.warn(`[MKCOL_WARN] Could not verify bucket accessibility for mount root MKCOL, but proceeding. Error: ${bucketError.message}`);
        }
        await updateMountLastUsed(db, mount.id);
        return new Response(null, { status: 201 }); // Created (conceptually)
    }


    if (!finalS3DirectoryKey || !finalS3DirectoryKey.endsWith('/')) {
         console.error(`[MKCOL_ERROR] Invalid calculated S3 Directory Key "${finalS3DirectoryKey}" for WebDAV path "${path}". Must end with '/'.`);
         return new Response("无法确定目标目录路径", 500);
    }
    // --- S3目录键计算逻辑结束 ---

    // 6. 检查目录是否已存在
    console.log(`[MKCOL_DEBUG] Checking if S3 directory key already exists: "${finalS3DirectoryKey}"`);
    const dirExists = await checkDirectoryExists(s3Client, s3Config.bucket_name, finalS3DirectoryKey); // 假设 checkDirectoryExists 存在

    if (dirExists) {
      console.log(`[MKCOL_INFO] Directory "${finalS3DirectoryKey}" already exists. Returning 201 as success (compatibility).`);
      return new Response(null, { status: 201 }); // Changed from 405 to 201 for compatibility
    } else {
        console.log(`[MKCOL_DEBUG] S3 directory key "${finalS3DirectoryKey}" does not exist yet.`);
    }

    // 7. 检查父目录是否存在 (使用修正后的路径逻辑)
     let s3ParentPrefix = "";
     if (finalS3DirectoryKey.includes('/')) {
         const pathParts = finalS3DirectoryKey.split('/').filter(p=>p);
         if (pathParts.length > 1) { // Check if there is a parent beyond the base
             s3ParentPrefix = pathParts.slice(0, -1).join('/') + '/';
              console.log(`[MKCOL_DEBUG] Checking if parent S3 prefix exists: "${s3ParentPrefix}"`);
              try {
                 const parentExists = await checkDirectoryExists(s3Client, s3Config.bucket_name, s3ParentPrefix);
                 if (!parentExists) {
                     console.log(`[MKCOL_INFO] Parent S3 prefix "${s3ParentPrefix}" does not exist. Attempting to create.`);
                     const createDirParams = { Bucket: s3Config.bucket_name, Key: s3ParentPrefix, Body: "", ContentType: "application/x-directory" };
                     await s3Client.send(new PutObjectCommand(createDirParams));
                     console.log(`[MKCOL_INFO] Successfully created parent S3 prefix "${s3ParentPrefix}".`);
                 } else {
                      console.log(`[MKCOL_DEBUG] Parent S3 prefix "${s3ParentPrefix}" exists.`);
                 }
              } catch(dirError) {
                   console.error(`[MKCOL_ERROR] Failed to check or create parent S3 prefix "${s3ParentPrefix}":`, dirError, dirError.stack);
                   // If parent creation fails, MKCOL should fail
                   return new Response("父目录创建失败", 409); // Conflict or 500? 409 seems appropriate
              }
         } else {
              console.log(`[MKCOL_DEBUG] Target directory "${finalS3DirectoryKey}" is directly under the effective root. No parent prefix check needed.`);
         }
     }


    // 8. 创建目录标记对象
    console.log(`[MKCOL_DEBUG] Creating S3 directory marker object with Key: "${finalS3DirectoryKey}"`);
    const putParams = {
      Bucket: s3Config.bucket_name,
      Key: finalS3DirectoryKey,
      Body: "",
      ContentLength: 0,
      ContentType: "application/x-directory", // Or application/octet-stream, S3 doesn't strictly enforce this for directories
    };
    await s3Client.send(new PutObjectCommand(putParams));

    // 9. 清理缓存并更新最后使用时间
    await updateMountLastUsed(db, mount.id);
    await clearCacheAfterWebDAVOperation(db, finalS3DirectoryKey, s3Config, true); // true for directory op

    console.log(`[MKCOL_INFO] Successfully completed MKCOL for "${path}" (S3 Key: "${finalS3DirectoryKey}")`);
    return new Response(null, { status: 201 }); // Created

  } catch (error) {
    const errorId = Date.now().toString(36);
    console.error(`[MKCOL_ERROR] Unhandled error in handleMkcol for path "${path}" [ErrorID: ${errorId}]:`, error, error.stack ? error.stack : '');
    return new Response(`内部服务器错误 (错误ID: ${errorId})`, { status: 500 });
  }
}

// 假设 clearCacheAfterWebDAVOperation 等辅助函数都已定义或正确导入