/**
 * 处理WebDAV DELETE请求
 * 用于删除文件或目录
 */
import { findMountPointByPath, updateMountLastUsed, checkDirectoryExists } from "../utils/webdavUtils.js";
import { createS3Client } from "../../utils/s3Utils.js";
import { DeleteObjectCommand, ListObjectsV2Command, HeadObjectCommand, DeleteObjectsCommand } from "@aws-sdk/client-s3"; // Added DeleteObjectsCommand
import { clearCacheAfterWebDAVOperation } from "../utils/cacheUtils.js"; // 假设缓存工具存在

export async function handleDelete(c, path, userId, userType, db) {
  console.log(`[DELETE_DEBUG] Entered handleDelete. webdavPath: "${path}", userId: "${userId}", userType: "${userType}"`);

  try {
    const originalWebdavPath = path; // Store original path

    // 1. 找到挂载点
    const mountResult = await findMountPointByPath(db, path, userId, userType);
    if (mountResult.error) {
      // If mountResult itself indicates not found, return that status
      if (mountResult.error.status === 404) {
           console.log(`[DELETE_INFO] Mount point not found for path "${path}". Returning 404.`);
           return new Response("资源不存在 (挂载点)", { status: 404 });
      }
      console.error(`[DELETE_ERROR] Mount point lookup error for path "${path}": ${mountResult.error.status} ${mountResult.error.message}`);
      return new Response(mountResult.error.message, { status: mountResult.error.status });
    }
    const { mount, subPath } = mountResult;
    console.log(`[DELETE_DEBUG] Found matchingMount: id="${mount.id}", name="${mount.name}", mount_path="${mount.mount_path}", storage_config_id="${mount.storage_config_id}", calculated subPath: "${subPath}"`);


    // 检查是否尝试删除挂载点根目录本身 (subPath会是"/")
    if (subPath === "/") {
      console.error(`[DELETE_ERROR] Attempted to delete the mount root itself: "${path}"`);
      return new Response("不能删除挂载点根目录", { status: 405 }); // Method Not Allowed or Forbidden? 405 seems appropriate
    }

    // 2. 获取S3配置
    const s3Config = await db.prepare("SELECT * FROM s3_configs WHERE id = ?").bind(mount.storage_config_id).first();
    if (!s3Config) {
      console.error(`[DELETE_ERROR] Storage Configuration Not Found for mount_id: "${mount.id}", storage_config_id: "${mount.storage_config_id}"`);
      return new Response("存储配置不存在", { status: 404 });
    }
    console.log(`[DELETE_DEBUG] Fetched s3Config: id="${s3Config.id}", bucket_name="${s3Config.bucket_name}", default_folder="${s3Config.default_folder || '(empty)'}"`);

    // 3. 创建S3客户端
    const s3Client = await createS3Client(s3Config, c.env.ENCRYPTION_SECRET);

    // 4. --- 修正后的S3对象键/前缀计算逻辑 ---
    const isDirectoryDelete = originalWebdavPath.endsWith('/');
    let finalS3Path = "";
    // Build base prefix
    if (s3Config.default_folder) {
        finalS3Path = s3Config.default_folder;
        if (!finalS3Path.endsWith('/')) finalS3Path += '/';
    }
    let mountPathSegment = mount.mount_path;
    if (mountPathSegment.startsWith('/')) mountPathSegment = mountPathSegment.substring(1);
     if (mountPathSegment && mountPathSegment !== '/') {
         if (!finalS3Path.endsWith('/') && finalS3Path !== '') finalS3Path += '/';
         finalS3Path += mountPathSegment + (mountPathSegment.endsWith('/') ? '' : '/');
    }
    // Add sub path segment
    let subPathSegment = subPath;
    if (subPathSegment.startsWith('/')) subPathSegment = subPathSegment.substring(1);
    finalS3Path += subPathSegment;
    // Normalize
    finalS3Path = finalS3Path.replace(/\/+/g, "/");
    // Remove leading slash if present
     if (finalS3Path.startsWith('/') && finalS3Path !== "/") {
        finalS3Path = finalS3Path.substring(1);
     }
    // Ensure trailing slash for directory prefix, remove for file key
    if (isDirectoryDelete) {
        if (finalS3Path && !finalS3Path.endsWith('/')) finalS3Path += '/';
    } else {
        if (finalS3Path.endsWith('/') && finalS3Path !== '/') finalS3Path = finalS3Path.slice(0, -1);
    }


    console.log(`[DELETE_DEBUG] Original WebDAV path: "${originalWebdavPath}", Mount path: "${mount.mount_path}", Sub-path within mount: "${subPath}"`);
    console.log(`[DELETE_DEBUG] S3 default_folder: "${s3Config.default_folder || '(empty)'}"`);
    console.log(`[DELETE_DEBUG] Calculated final finalS3Path for S3 DELETE (${isDirectoryDelete ? 'Directory' : 'File'}): "${finalS3Path}"`);

    if (!finalS3Path) { // Should not be empty unless trying to delete mount root (handled above) or path logic error
         console.error(`[DELETE_ERROR] Calculated S3 path is empty for WebDAV path "${originalWebdavPath}".`);
         return new Response("无法确定目标资源路径", 500);
    }
    // --- S3对象键/前缀计算逻辑结束 ---


    // 5. 执行删除操作
    if (isDirectoryDelete) {
        // --- 删除目录 ---
        const s3PrefixToDelete = finalS3Path; // Already ends with /
        console.log(`[DELETE_DEBUG] Attempting to delete directory (prefix): "${s3PrefixToDelete}"`);

        // 1. Check if the prefix actually exists (optional but good practice)
        // Note: checkDirectoryExists checks for *any* object OR explicit marker
        const dirExists = await checkDirectoryExists(s3Client, s3Config.bucket_name, s3PrefixToDelete);
        if (!dirExists) {
             console.log(`[DELETE_INFO] S3 prefix "${s3PrefixToDelete}" does not exist. Returning 404.`);
             return new Response("目录不存在", { status: 404 });
        }

        // 2. List all objects under the prefix
        let continuationToken = undefined;
        const keysToDelete = [];
        do {
            const listParams = { Bucket: s3Config.bucket_name, Prefix: s3PrefixToDelete, ContinuationToken: continuationToken };
            console.log(`[DELETE_DEBUG] Listing objects under prefix "${s3PrefixToDelete}" ${continuationToken ? ' (Continuation)' : ''}`);
            const listResponse = await s3Client.send(new ListObjectsV2Command(listParams));

            if (listResponse.Contents && listResponse.Contents.length > 0) {
                listResponse.Contents.forEach(item => keysToDelete.push({ Key: item.Key }));
                 console.log(`[DELETE_DEBUG] Found ${listResponse.Contents.length} objects to delete in this batch.`);
            } else if (!continuationToken) {
                 // If first batch is empty, maybe only the directory marker exists?
                 console.log(`[DELETE_DEBUG] No objects found under prefix "${s3PrefixToDelete}". Checking for directory marker itself.`);
            }

            continuationToken = listResponse.NextContinuationToken;
        } while (continuationToken);

         // 3. Also add the directory marker key itself to the delete list (if it wasn't listed as content)
         if (!keysToDelete.some(k => k.Key === s3PrefixToDelete)) {
             // Check if the explicit directory marker actually exists before adding
              try {
                   await s3Client.send(new HeadObjectCommand({ Bucket: s3Config.bucket_name, Key: s3PrefixToDelete }));
                   console.log(`[DELETE_DEBUG] Adding directory marker key "${s3PrefixToDelete}" to delete list.`);
                   keysToDelete.push({ Key: s3PrefixToDelete });
              } catch (headError) {
                   if (headError.name === 'NotFound' || headError.$metadata?.httpStatusCode === 404) {
                       console.log(`[DELETE_DEBUG] Directory marker key "${s3PrefixToDelete}" not found, not adding to delete list.`);
                   } else { throw headError; } // re-throw other errors
              }
         }


        // 4. Perform batch delete (more efficient) if there are keys to delete
        if (keysToDelete.length > 0) {
            console.log(`[DELETE_INFO] Deleting ${keysToDelete.length} objects/markers for prefix "${s3PrefixToDelete}"...`);
            // S3 DeleteObjects takes max 1000 keys per request
            const batchSize = 1000;
            for (let i = 0; i < keysToDelete.length; i += batchSize) {
                const batch = keysToDelete.slice(i, i + batchSize);
                const deleteParams = { Bucket: s3Config.bucket_name, Delete: { Objects: batch, Quiet: false } };
                console.log(`[DELETE_DEBUG] Sending DeleteObjects command for batch ${i/batchSize + 1} (size ${batch.length})`);
                const deleteResult = await s3Client.send(new DeleteObjectsCommand(deleteParams));
                console.log(`[DELETE_DEBUG] Batch delete successful.`);
                if (deleteResult.Errors && deleteResult.Errors.length > 0) {
                     console.error(`[DELETE_ERROR] Errors occurred during batch delete:`, deleteResult.Errors);
                     // Handle partial failure? Maybe return 500 or 207 Multi-Status? For simplicity, maybe just log and continue.
                     // throw new Error("部分对象删除失败");
                }
            }
        } else {
             console.log(`[DELETE_INFO] No objects or markers found to delete for prefix "${s3PrefixToDelete}".`);
             // If nothing was found, but checkDirectoryExists was true, it implies an inconsistency or race condition.
             // However, we can treat it as success as the end state (prefix gone) is achieved.
        }
        console.log(`[DELETE_INFO] Directory delete operation completed for "${s3PrefixToDelete}".`);

    } else {
        // --- 删除文件 ---
        const s3FileKey = finalS3Path; // Already ensured no trailing /
        console.log(`[DELETE_DEBUG] Attempting to delete file: "${s3FileKey}"`);

        // 1. Check if file exists using HEAD (optional but good practice, handles 404)
        try {
            console.log(`[DELETE_DEBUG] Sending HEAD request for Key: "${s3FileKey}"`);
            await s3Client.send(new HeadObjectCommand({ Bucket: s3Config.bucket_name, Key: s3FileKey }));
             console.log(`[DELETE_DEBUG] File "${s3FileKey}" exists. Proceeding with delete.`);

            // 2. Delete the file
            const deleteParams = { Bucket: s3Config.bucket_name, Key: s3FileKey };
             console.log(`[DELETE_DEBUG] Sending DeleteObject command for Key: "${s3FileKey}"`);
            await s3Client.send(new DeleteObjectCommand(deleteParams));
            console.log(`[DELETE_INFO] Successfully deleted file "${s3FileKey}".`);

        } catch (error) {
            if (error.name === 'NotFound' || (error.$metadata && error.$metadata.httpStatusCode === 404)) {
                console.log(`[DELETE_INFO] File "${s3FileKey}" not found during HEAD check. Returning 404.`);
                return new Response("文件不存在", { status: 404 });
            }
            console.error(`[DELETE_ERROR] Error during HEAD or DELETE for file "${s3FileKey}":`, error);
            throw error; // Re-throw other errors
        }
    }

    // 6. 清理缓存并更新最后使用时间
    await updateMountLastUsed(db, mount.id);
    await clearCacheAfterWebDAVOperation(db, finalS3Path, s3Config, isDirectoryDelete); // Pass directory flag

    // 7. 返回成功响应
    return new Response(null, { status: 204 }); // No Content

  } catch (error) {
    const errorId = Date.now().toString(36);
    console.error(`[DELETE_ERROR] Unhandled error in handleDelete for path "${path}" [ErrorID: ${errorId}]:`, error, error.stack ? error.stack : '');
    return new Response(`内部服务器错误 (错误ID: ${errorId})`, { status: 500 });
  }
}

// 假设 clearCacheAfterWebDAVOperation 等辅助函数都已定义或正确导入