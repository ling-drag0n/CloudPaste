// 文件顶部（如果还没有引入这些，确保有，虽然propfind.js本身可能不需要，但好的习惯）
// import { getMountsByAdmin, getMountsByApiKey } from "../../services/storageMountService.js"; // 应该已存在
// import { createS3Client } from "../../utils/s3Utils.js"; // 应该已存在
// import { S3Client, ListObjectsV2Command, HeadObjectCommand } from "@aws-sdk/client-s3"; // 应该已存在

// ... (文件原有的 getDirectoryStructure 函数等) ...

/**
 * 处理PROPFIND请求
 * @param {Object} c - Hono上下文
 * @param {string} path - 请求路径
 * @param {string} userId - 用户ID
 * @param {string} userType - 用户类型 (admin 或 apiKey)
 * @param {D1Database} db - D1数据库实例
 */
export async function handlePropfind(c, path, userId, userType, db) {
  // 获取请求头中的Depth (默认为infinity)
  const depth = c.req.header("Depth") || "infinity";
  if (depth !== "0" && depth !== "1" && depth !== "infinity") {
    return new Response("Bad Request: Invalid Depth Header", { status: 400 });
  }

  try {
    // 规范化路径
    path = path.startsWith("/") ? path : "/" + path;
    path = path.endsWith("/") ? path : path + "/";

    // 添加日志：记录进入函数时的参数
    console.log(`[PROPFIND_DEBUG] Entered handlePropfind. path: "${path}", userId: "${userId}", userType: "${userType}", depth: "${depth}"`);

    // 获取挂载点列表
    let mounts;
    if (userType === "admin") {
      mounts = await getMountsByAdmin(db, userId);
    } else if (userType === "apiKey") {
      mounts = await getMountsByApiKey(db, userId);
    } else {
      return new Response("Unauthorized", { status: 401 });
    }
    console.log(`[PROPFIND_DEBUG] Fetched mounts for user. Count: ${mounts ? mounts.length : 0}`);


    // 如果是根路径或者是虚拟目录路径,则返回虚拟目录列表
    let isVirtualPath = true;
    let matchingMount = null;
    let subPath = "";

    // 按照路径长度降序排序,以便优先匹配最长的路径
    mounts.sort((a, b) => b.mount_path.length - a.mount_path.length);

    // 检查是否匹配到实际的挂载点
    for (const mount of mounts) {
      const mountPath = mount.mount_path.startsWith("/") ? mount.mount_path : "/" + mount.mount_path;
      console.log(`[PROPFIND_DEBUG] Checking mount: mount.id="${mount.id}", mount.mount_path="${mount.mount_path}", mount.name="${mount.name}", normalized_mountPath_for_check="${mountPath}" against request path "${path}"`);

      if (path === mountPath + "/" || path.startsWith(mountPath + "/")) {
        matchingMount = mount;
        subPath = path.substring(mountPath.length);
        if (!subPath.startsWith("/")) {
          subPath = "/" + subPath;
        }
        isVirtualPath = false;
        console.log(`[PROPFIND_DEBUG] Found matchingMount: id="${matchingMount.id}", name="${matchingMount.name}", mount_path="${matchingMount.mount_path}", subPath: "${subPath}"`);
        break;
      }
    }

    if (!matchingMount && isVirtualPath) {
        console.log(`[PROPFIND_DEBUG] No direct matching mount found, path "${path}" is considered a virtual path. Responding with mounts/virtual dirs.`);
        // （注意：这里假设如果路径是 /dav/ 且没有子路径，或者是一个中间虚拟目录，会进入 respondWithMounts）
        // 如果 path 不是 / 且没有匹配到挂载点，且也不是一个已知的虚拟中间路径，可能也应返回 404 或其他，
        // 但当前逻辑是如果没匹配到实际挂载点，就认为是虚拟路径尝试 respondWithMounts。
        // respondWithMounts 本身也应有日志。
    }


    // 处理虚拟目录路径 (根目录或中间目录)
    if (isVirtualPath) {
      // 对于 /dav/ 这样的请求，或者中间的虚拟目录，会进入这里
      console.log(`[PROPFIND_DEBUG] Path "${path}" is virtual. Calling respondWithMounts.`);
      return await respondWithMounts(c, userId, userType, db, path); // respondWithMounts 内部也应有日志
    }

    // 处理实际挂载点路径
    // 获取挂载点对应的S3配置
    const s3Config = await db.prepare("SELECT * FROM s3_configs WHERE id = ?").bind(matchingMount.storage_config_id).first();

    if (!s3Config) {
      console.error(`[PROPFIND_ERROR] Storage Configuration Not Found for mount_id: "${matchingMount.id}", storage_config_id: "${matchingMount.storage_config_id}"`);
      return new Response("Storage Configuration Not Found", { status: 404 });
    }
    console.log(`[PROPFIND_DEBUG] Fetched s3Config: id="${s3Config.id}", bucket_name="${s3Config.bucket_name}", default_folder="${s3Config.default_folder || 'N/A'}"`);


    // 创建S3客户端 (createS3Client 内部也应该有错误处理和日志)
    const s3Client = await createS3Client(s3Config, c.env.ENCRYPTION_SECRET);

    // 规范化S3子路径
    let s3SubPath = subPath.startsWith("/") ? subPath.substring(1) : subPath; // Path relative to mount point

    // 如果有默认文件夹,添加到路径 (这是当前propfind.js的逻辑)
    if (s3Config.default_folder) {
      let defaultFolder = s3Config.default_folder;
      if (!defaultFolder.endsWith("/")) defaultFolder += "/";
      s3SubPath = defaultFolder + s3SubPath;
      console.log(`[PROPFIND_DEBUG] Applied s3Config.default_folder. s3SubPath is now: "${s3SubPath}"`);
    }

    // 规范化S3子路径,移除多余的斜杠
    s3SubPath = s3SubPath.replace(/\/+/g, "/");
    console.log(`[PROPFIND_DEBUG] Final calculated s3SubPath for S3 query: "${s3SubPath}" (This is the prefix that will be used for S3 listObjects)`);


    // 更新最后使用时间
    try {
      await db.prepare("UPDATE storage_mounts SET last_used = CURRENT_TIMESTAMP WHERE id = ?").bind(matchingMount.id).run();
    } catch (updateError) {
      console.warn(`[PROPFIND_WARN] Failed to update last_used for mount id: ${matchingMount.id}`, updateError);
    }

    // 构建响应
    return await buildPropfindResponse(c, s3Client, s3Config.bucket_name, s3SubPath, depth, path /* path is the original WebDAV relative path, e.g., /testmount/ */);
  } catch (error) {
    console.error("[PROPFIND_ERROR] Unhandled error in handlePropfind:", error, error.stack);
    return new Response("Internal Server Error: " + error.message, { status: 500 });
  }
}

/**
 * 响应挂载点列表 (也添加一些日志)
 */
async function respondWithMounts(c, userId, userType, db, path = "/") {
  console.log(`[PROPFIND_DEBUG] Entered respondWithMounts for path: "${path}"`);
  // ... (原 respondWithMounts 代码) ...
  // 可以在循环生成XML之前打印 structure.directories 和 structure.mounts 的内容
  // console.log(`[PROPFIND_DEBUG] respondWithMounts - structure for path "${path}": directories:`, JSON.stringify(structure.directories), `mounts:`, JSON.stringify(structure.mounts.map(m => ({id: m.id, name: m.name, path: m.mount_path}))));

  // ... (原 respondWithMounts 代码) ...
  // 在返回前打印最终的XML (可能会很长，选择性开启)
  // console.log(`[PROPFIND_DEBUG] respondWithMounts - XML Body for path "${path}":`, xmlBody);
  return new Response(xmlBody, {
    status: 207, // Multi-Status
    headers: {
      "Content-Type": "application/xml; charset=utf-8",
    },
  });
}


/**
 * 构建PROPFIND响应
 */
async function buildPropfindResponse(c, s3Client, bucketName, prefix, depth, requestPath /* requestPath is the original WebDAV relative path, e.g. /testmount/ */) {
  // 确保路径以斜杠结尾 (这部分逻辑是针对S3 prefix的，如果prefix是文件，不应加斜杠)
  // 如果 prefix 是文件，不应强制加斜杠，但PROPFIND通常是对目录操作，所以这里的prefix一般指目录
  // if (!prefix.endsWith("/") && prefix !== "") {
  //   prefix += "/";
  // }
  // 这段可能需要根据实际情况调整，但既然PROPFIND是列目录，传入的prefix应该已经是目录形式(带/)或空字符串

  // 删除开头的斜杠 (S3 prefix通常不以/开头，除非就是根目录的特殊表示)
  // if (prefix.startsWith("/")) {
  //   prefix = prefix.substring(1);
  // }
  // 这段也需要小心，S3 Prefix为空 "" 是有效的，代表根。如果是 "foo/" 也不应移除。

  console.log(`[PROPFIND_DEBUG] Entered buildPropfindResponse. bucketName: "${bucketName}", S3_query_prefix: "${prefix}", depth: "${depth}", webdav_requestPath: "${requestPath}"`);


  try {
    const listParams = {
      Bucket: bucketName,
      Prefix: prefix, // **这是传递给S3 SDK的Prefix**
      Delimiter: "/", // 通常用于模拟文件夹
    };

    console.log(`[PROPFIND_DEBUG] S3 ListObjectsV2Command params: Bucket='${bucketName}', Prefix='${prefix}', Delimiter='/'`);

    const listCommand = new ListObjectsV2Command(listParams);
    const listResponse = await s3Client.send(listCommand);

    console.log(`[PROPFIND_DEBUG] S3 ListObjectsV2Command response: Contents count: ${listResponse.Contents ? listResponse.Contents.length : 0}, CommonPrefixes count: ${listResponse.CommonPrefixes ? listResponse.CommonPrefixes.length : 0}`);
    // 可以选择性地打印更详细的S3响应内容，但要注意可能包含敏感信息或过长
    // if (listResponse.Contents && listResponse.Contents.length > 0) {
    //    console.log(`[PROPFIND_DEBUG] S3 Contents (first 5):`, JSON.stringify(listResponse.Contents.slice(0, 5).map(item => ({ Key: item.Key, Size: item.Size, LastModified: item.LastModified }))));
    // }
    // if (listResponse.CommonPrefixes && listResponse.CommonPrefixes.length > 0) {
    //    console.log(`[PROPFIND_DEBUG] S3 CommonPrefixes (first 5):`, JSON.stringify(listResponse.CommonPrefixes.slice(0, 5).map(item => ({ Prefix: item.Prefix }))));
    // }


    // 构建XML响应
    let xmlBody = `<?xml version="1.0" encoding="utf-8"?>
    <D:multistatus xmlns:D="DAV:">
      <D:response>
        <D:href>/dav${requestPath}</D:href> 
        <D:propstat>
          <D:prop>
            <D:resourcetype><D:collection/></D:resourcetype>
            <D:displayname>${requestPath.split("/").filter(Boolean).pop() || "/"}</D:displayname>
            <D:getlastmodified>${new Date().toUTCString()}</D:getlastmodified>
          </D:prop>
          <D:status>HTTP/1.1 200 OK</D:status>
        </D:propstat>
      </D:response>`;

    if (depth !== "0") {
      if (listResponse.CommonPrefixes) {
        for (const item of listResponse.CommonPrefixes) {
          // item.Prefix for S3 is like "actual_s3_prefix/folder_name/"
          // We need the folder_name part relative to the S3_query_prefix ("prefix" variable)
          let folderName = item.Prefix;
          if (prefix && folderName.startsWith(prefix)) { // Ensure it's a sub-item of current S3 query prefix
              folderName = folderName.substring(prefix.length); // Get part relative to current S3 prefix
          }
          folderName = folderName.replace(/\/$/, ""); // Remove trailing slash for display name part
          // folderName = folderName.split("/").filter(Boolean).pop(); // This might be too aggressive if prefix itself has slashes

          // href path construction:
          // requestPath is the WebDAV path we are listing, e.g., /testmount/
          // folderName is the "name" of the sub-collection found, e.g., "subfolder"
          // So, the href should be /dav/testmount/subfolder/
          const folderWebDAVPath = (requestPath.endsWith('/') ? requestPath : requestPath + '/') + folderName + '/';
          const encodedFolderWebDAVPath = encodeURI(folderWebDAVPath); // URI encode

          xmlBody += `
    <D:response>
      <D:href>/dav${encodedFolderWebDAVPath}</D:href>
      <D:propstat>
        <D:prop>
          <D:resourcetype><D:collection/></D:resourcetype>
          <D:displayname>${folderName}</D:displayname>
          <D:getlastmodified>${new Date().toUTCString()}</D:getlastmodified>
        </D:prop>
        <D:status>HTTP/1.1 200 OK</D:status>
      </D:propstat>
    </D:response>`;
        }
      }

      if (listResponse.Contents) {
        for (const item of listResponse.Contents) {
          // item.Key for S3 is like "actual_s3_prefix/file_name.txt" or "actual_s3_prefix/itself/"
          // Skip the current directory itself if it appears as an object (S3 sometimes does for empty prefixes representing directories)
          if (item.Key === prefix && item.Key.endsWith("/")) continue; 
          // Skip other "directory marker" objects if they are not actual files
          if (item.Key.endsWith("/") && item.Size === 0) continue;


          let fileName = item.Key;
          if (prefix && fileName.startsWith(prefix)) { // Ensure it's a sub-item of current S3 query prefix
             fileName = fileName.substring(prefix.length); // Get part relative to current S3 prefix
          }
          // fileName = fileName.split("/").pop(); // This might be too aggressive

          const fileWebDAVPath = (requestPath.endsWith('/') ? requestPath : requestPath + '/') + fileName;
          const encodedFileWebDAVPath = encodeURI(fileWebDAVPath);

          xmlBody += `
    <D:response>
      <D:href>/dav${encodedFileWebDAVPath}</D:href>
      <D:propstat>
        <D:prop>
          <D:resourcetype></D:resourcetype>
          <D:displayname>${fileName}</D:displayname>
          <D:getlastmodified>${new Date(item.LastModified).toUTCString()}</D:getlastmodified>
          <D:getcontentlength>${item.Size}</D:getcontentlength>
          <D:getcontenttype>${item.ContentType || 'application/octet-stream'}</D:getcontenttype>
        </D:prop>
        <D:status>HTTP/1.1 200 OK</D:status>
      </D:propstat>
    </D:response>`;
        }
      }
    }

    xmlBody += `</D:multistatus>`;

    // console.log(`[PROPFIND_DEBUG] buildPropfindResponse - Final XML Body:`, xmlBody); // Be careful, can be very long

    return new Response(xmlBody, {
      status: 207, // Multi-Status
      headers: {
        "Content-Type": "application/xml; charset=utf-8",
      },
    });
  } catch (error) {
    console.error("[PROPFIND_ERROR] Error in buildPropfindResponse:", error, error.stack);
    return new Response("Internal Server Error in buildPropfindResponse: " + error.message, { status: 500 });
  }
}
