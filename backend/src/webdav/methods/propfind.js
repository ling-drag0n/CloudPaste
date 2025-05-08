/**
 * 处理WebDAV PROPFIND请求
 * 用于获取文件和目录信息(列表)
 */
import { getMountsByAdmin, getMountsByApiKey } from "../../services/storageMountService.js"; // 确保导入了 getMountsByApiKey
import { createS3Client } from "../../utils/s3Utils.js";
import { S3Client, ListObjectsV2Command, HeadObjectCommand } from "@aws-sdk/client-s3"; // 确保这些也被导入，如果 buildPropfindResponse 需要它们

/**
 * 从挂载路径列表中获取指定路径下的目录结构
 * (这个函数来自您提供的 context source_id="6"，保持不变或根据您的实际版本调整)
 * @param {Array} mounts - 挂载点列表
 * @param {string} currentPath - 当前路径,以/开头且以/结尾
 * @returns {Object} 包含子目录和挂载点的对象
 */
function getDirectoryStructure(mounts, currentPath) {
  // 确保currentPath以/开头且以/结尾
  currentPath = currentPath.startsWith("/") ? currentPath : "/" + currentPath;
  currentPath = currentPath.endsWith("/") ? currentPath : currentPath + "/";

  const result = {
    directories: new Set(),
    mounts: [],
  };

  for (const mount of mounts) {
    let mountPath = mount.mount_path;
    mountPath = mountPath.startsWith("/") ? mountPath : "/" + mountPath;

    if (mountPath + "/" === currentPath || mountPath === currentPath) {
      result.mounts.push(mount);
      continue;
    }

    if (mountPath.startsWith(currentPath)) {
      const relativePath = mountPath.substring(currentPath.length);
      const firstDir = relativePath.split("/")[0];
      if (firstDir) {
        result.directories.add(firstDir);
      }
      continue;
    }

    if (currentPath !== "/" && mountPath.startsWith(currentPath)) {
      const relativePath = mountPath.substring(currentPath.length);
      const firstDir = relativePath.split("/")[0];
      if (firstDir) {
        result.directories.add(firstDir);
      }
    }
  }

  return {
    directories: Array.from(result.directories),
    mounts: result.mounts,
  };
}

/**
 * 处理PROPFIND请求
 * @param {Object} c - Hono上下文
 * @param {string} path - 请求路径
 * @param {string} userId - 用户ID
 * @param {string} userType - 用户类型 (admin 或 apiKey)
 * @param {D1Database} db - D1数据库实例
 */
export async function handlePropfind(c, path, userId, userType, db) {
  const depth = c.req.header("Depth") || "infinity";
  if (depth !== "0" && depth !== "1" && depth !== "infinity") {
    return new Response("Bad Request: Invalid Depth Header", { status: 400 });
  }

  try {
    path = path.startsWith("/") ? path : "/" + path;
    path = path.endsWith("/") ? path : path + "/";

    console.log(`[PROPFIND_DEBUG] Entered handlePropfind. path: "${path}", userId: "${userId}", userType: "${userType}", depth: "${depth}"`);

    let mounts;
    if (userType === "admin") {
      mounts = await getMountsByAdmin(db, userId);
    } else if (userType === "apiKey") {
      mounts = await getMountsByApiKey(db, userId); // 确保 getMountsByApiKey 已定义并导入
    } else {
      console.error(`[PROPFIND_ERROR] Invalid userType: "${userType}"`);
      return new Response("Unauthorized - Invalid user type", { status: 401 });
    }
    console.log(`[PROPFIND_DEBUG] Fetched mounts for user. Count: ${mounts ? mounts.length : 'N/A (mounts undefined)'}`);
    if (mounts && mounts.length > 0) {
        mounts.forEach(m => console.log(`[PROPFIND_DEBUG] Available mount: id=${m.id}, name=${m.name}, path=${m.mount_path}`));
    }


    let isVirtualPath = true;
    let matchingMount = null;
    let subPath = "";

    if (mounts) { // 添加检查，确保mounts已定义
        mounts.sort((a, b) => b.mount_path.length - a.mount_path.length);

        for (const mount of mounts) {
            const mountPathForCheck = mount.mount_path.startsWith("/") ? mount.mount_path : "/" + mount.mount_path;
            console.log(`[PROPFIND_DEBUG] Checking mount: mount.id="${mount.id}", mount.mount_path="${mount.mount_path}", normalized_mountPath_for_check="${mountPathForCheck}" against request path "${path}"`);

            if (path === mountPathForCheck + "/" || path.startsWith(mountPathForCheck + "/")) {
                matchingMount = mount;
                subPath = path.substring(mountPathForCheck.length);
                if (!subPath.startsWith("/")) {
                    subPath = "/" + subPath;
                }
                isVirtualPath = false;
                console.log(`[PROPFIND_DEBUG] Found matchingMount: id="${matchingMount.id}", name="${matchingMount.name}", mount_path="${matchingMount.mount_path}", storage_config_id="${matchingMount.storage_config_id}", subPath: "${subPath}"`);
                break;
            }
        }
    } else {
        console.warn("[PROPFIND_WARN] Mounts array is undefined or null after fetching.");
    }


    if (!matchingMount && isVirtualPath) {
        console.log(`[PROPFIND_DEBUG] No direct matching mount found for path "${path}", considering it a virtual path. Calling respondWithMounts.`);
    }

    if (isVirtualPath) {
      console.log(`[PROPFIND_DEBUG] Path "${path}" is virtual or no matching mount. Calling respondWithMounts.`);
      return await respondWithMounts(c, userId, userType, db, path);
    }

    const s3Config = await db.prepare("SELECT * FROM s3_configs WHERE id = ?").bind(matchingMount.storage_config_id).first();

    if (!s3Config) {
      console.error(`[PROPFIND_ERROR] Storage Configuration Not Found for mount_id: "${matchingMount.id}", storage_config_id: "${matchingMount.storage_config_id}"`);
      return new Response("Storage Configuration Not Found", { status: 404 });
    }
    console.log(`[PROPFIND_DEBUG] Fetched s3Config: id="${s3Config.id}", bucket_name="${s3Config.bucket_name}", default_folder="${s3Config.default_folder || '(empty)'}"`);

    const s3Client = await createS3Client(s3Config, c.env.ENCRYPTION_SECRET);

    // --- 关键的S3路径构建逻辑 ---
    let s3QueryPrefix = "";
    // 1. 如果S3配置有默认文件夹，以此为基础
    if (s3Config.default_folder) {
        s3QueryPrefix = s3Config.default_folder;
        if (!s3QueryPrefix.endsWith('/')) {
            s3QueryPrefix += '/';
        }
    }
    // 2. 将挂载点路径转换为S3前缀的一部分并追加
    // (例如，UI挂载点 /files 应该变成 files/ 追加到 s3QueryPrefix)
    let mountPathSegment = matchingMount.mount_path;
    if (mountPathSegment.startsWith('/')) {
        mountPathSegment = mountPathSegment.substring(1); // 移除开头的 /
    }
    if (mountPathSegment && !mountPathSegment.endsWith('/')) {
        mountPathSegment += '/'; // 确保挂载点段以 / 结尾
    }
    if (mountPathSegment) { // 只有当mountPathSegment非空时才添加，避免 "root//" 这种情况
        s3QueryPrefix += mountPathSegment;
    }

    // 3. 将挂载点内部的相对路径追加
    let subPathSegment = subPath; // subPath 来自于 findMountPointByPath, 例如 "/" 或 "/folder/" 或 "/file.txt"
    if (subPathSegment.startsWith('/')) {
        subPathSegment = subPathSegment.substring(1); // 移除开头的 /
    }
    // 如果subPathSegment是文件名，不应强制加斜杠。PROPFIND通常是对目录，所以subPathSegment预期是目录路径（带/）或空（代表挂载点本身）
    // 对于PROPFIND /dav/files/，subPath是 /，subPathSegment是 ""。
    // 对于PROPFIND /dav/files/folder/，subPath是 /folder/，subPathSegment是 "folder/"
    s3QueryPrefix += subPathSegment;

    // 4. 最终规范化，移除多余的斜杠，并确保如果不是根，不以斜杠开头 (S3前缀通常不以/开头)
    s3QueryPrefix = s3QueryPrefix.replace(/\/+/g, "/");
    // if (s3QueryPrefix.startsWith('/') && s3QueryPrefix !== "/") {
    //     s3QueryPrefix = s3QueryPrefix.substring(1);
    // }
    // 注意：S3前缀为空字符串 "" 是有效的，代表列出根目录。如果结果是 "foo/" 也是有效的。
    // 上面这行移除前导斜杠的逻辑可能需要谨慎，取决于S3客户端库如何处理。多数情况下，S3 prefix不应以/开头。
    // 但如果bucket本身就配置了某种根路径行为，或者createS3Client有特殊处理，则可能不同。
    // 为安全起见，暂时注释掉移除前导斜杠，因为 "foo/" 形式是标准S3前缀。

    console.log(`[PROPFIND_DEBUG] Original WebDAV path: "${path}", Mount path: "${matchingMount.mount_path}", Sub-path within mount: "${subPath}"`);
    console.log(`[PROPFIND_DEBUG] S3 default_folder: "${s3Config.default_folder || '(empty)'}"`);
    console.log(`[PROPFIND_DEBUG] Calculated final s3QueryPrefix for S3 ListObjects: "${s3QueryPrefix}"`);


    try {
      await db.prepare("UPDATE storage_mounts SET last_used = CURRENT_TIMESTAMP WHERE id = ?").bind(matchingMount.id).run();
    } catch (updateError) {
      console.warn(`[PROPFIND_WARN] Failed to update last_used for mount id: ${matchingMount.id}`, updateError);
    }

    return await buildPropfindResponse(c, s3Client, s3Config.bucket_name, s3QueryPrefix, depth, path);
  } catch (error) {
    console.error("[PROPFIND_ERROR] Unhandled error in handlePropfind:", error, error.stack ? error.stack : '(no stack trace)');
    return new Response("Internal Server Error: " + error.message, { status: 500 });
  }
}

async function respondWithMounts(c, userId, userType, db, path = "/") {
  console.log(`[PROPFIND_DEBUG] Entered respondWithMounts for WebDAV path: "${path}"`);
  let mounts;
  if (userType === "admin") {
    mounts = await getMountsByAdmin(db, userId);
  } else if (userType === "apiKey") {
    mounts = await getMountsByApiKey(db, userId);
  } else {
    return new Response("Unauthorized", { status: 401 });
  }

  path = path.startsWith("/") ? path : "/" + path;
  path = path.endsWith("/") ? path : path + "/";

  const structure = getDirectoryStructure(mounts, path);
  console.log(`[PROPFIND_DEBUG] respondWithMounts - structure for path "${path}": directories:`, JSON.stringify(structure.directories), `mounts:`, JSON.stringify(structure.mounts.map(m => ({id: m.id, name: m.name, path: m.mount_path}))));


  const pathParts = path.split("/").filter(Boolean);
  const displayName = path === "/" ? "/" : pathParts.length > 0 ? pathParts[pathParts.length - 1] : "/";

  let xmlBody = `<?xml version="1.0" encoding="utf-8"?>
  <D:multistatus xmlns:D="DAV:">
    <D:response>
      <D:href>/dav${path}</D:href>
      <D:propstat>
        <D:prop>
          <D:resourcetype><D:collection/></D:resourcetype>
          <D:displayname>${displayName}</D:displayname>
          <D:getlastmodified>${new Date().toUTCString()}</D:getlastmodified>
        </D:prop>
        <D:status>HTTP/1.1 200 OK</D:status>
      </D:propstat>
    </D:response>`;

  for (const dir of structure.directories) {
    const dirPath = path + dir + "/";
    const encodedPath = encodeURI(dirPath);
    xmlBody += `
    <D:response>
      <D:href>/dav${encodedPath}</D:href>
      <D:propstat>
        <D:prop>
          <D:resourcetype><D:collection/></D:resourcetype>
          <D:displayname>${dir}</D:displayname>
          <D:getlastmodified>${new Date().toUTCString()}</D:getlastmodified>
        </D:prop>
        <D:status>HTTP/1.1 200 OK</D:status>
      </D:propstat>
    </D:response>`;
  }

  for (const mount of structure.mounts) {
    const mountName = mount.name || mount.mount_path.split("/").filter(Boolean).pop() || mount.id;
    const mountPath = mount.mount_path.startsWith("/") ? mount.mount_path : "/" + mount.mount_path;
    const encodedPath = encodeURI(mountPath + "/");
    const relativePath = mountPath.substring(path.length);

    if (!relativePath.includes("/") || relativePath === "") {
      xmlBody += `
      <D:response>
        <D:href>/dav${encodedPath}</D:href>
        <D:propstat>
          <D:prop>
            <D:resourcetype><D:collection/></D:resourcetype>
            <D:displayname>${mountName}</D:displayname>
            <D:getlastmodified>${new Date(mount.updated_at || mount.created_at).toUTCString()}</D:getlastmodified>
          </D:prop>
          <D:status>HTTP/1.1 200 OK</D:status>
        </D:propstat>
      </D:response>`;
    }
  }

  xmlBody += `</D:multistatus>`;
  // console.log(`[PROPFIND_DEBUG] respondWithMounts - XML Body for path "${path}":`, xmlBody); // XML can be long

  return new Response(xmlBody, {
    status: 207,
    headers: { "Content-Type": "application/xml; charset=utf-8" },
  });
}

async function buildPropfindResponse(c, s3Client, bucketName, prefixForS3Query, depth, webdavRequestPath) {
  console.log(`[PROPFIND_DEBUG] Entered buildPropfindResponse. bucketName: "${bucketName}", S3_query_prefix: "${prefixForS3Query}", depth: "${depth}", webdav_requestPath: "${webdavRequestPath}"`);

  try {
    const listParams = {
      Bucket: bucketName,
      Prefix: prefixForS3Query,
      Delimiter: "/",
    };
    console.log(`[PROPFIND_DEBUG] S3 ListObjectsV2Command params: Bucket='${bucketName}', Prefix='${listParams.Prefix}', Delimiter='${listParams.Delimiter}'`);

    const listCommand = new ListObjectsV2Command(listParams);
    const listResponse = await s3Client.send(listCommand);

    console.log(`[PROPFIND_DEBUG] S3 ListObjectsV2Command response: Contents count: ${listResponse.Contents ? listResponse.Contents.length : 0}, CommonPrefixes count: ${listResponse.CommonPrefixes ? listResponse.CommonPrefixes.length : 0}`);
    if (listResponse.Contents && listResponse.Contents.length > 0) {
       console.log(`[PROPFIND_DEBUG] S3 Contents (first 5 of ${listResponse.Contents.length}):`, JSON.stringify(listResponse.Contents.slice(0, 5).map(item => ({ Key: item.Key, Size: item.Size, LastModified: item.LastModified }))));
    }
    if (listResponse.CommonPrefixes && listResponse.CommonPrefixes.length > 0) {
       console.log(`[PROPFIND_DEBUG] S3 CommonPrefixes (first 5 of ${listResponse.CommonPrefixes.length}):`, JSON.stringify(listResponse.CommonPrefixes.slice(0, 5).map(item => ({ Prefix: item.Prefix }))));
    }

    const baseWebDAVHref = `/dav${webdavRequestPath}`; // e.g., /dav/testmount/
    const displayNameForCollection = webdavRequestPath.split("/").filter(Boolean).pop() || "/";

    let xmlBody = `<?xml version="1.0" encoding="utf-8"?>
    <D:multistatus xmlns:D="DAV:">
      <D:response>
        <D:href>${baseWebDAVHref}</D:href>
        <D:propstat>
          <D:prop>
            <D:resourcetype><D:collection/></D:resourcetype>
            <D:displayname>${displayNameForCollection}</D:displayname>
            <D:getlastmodified>${new Date().toUTCString()}</D:getlastmodified>
          </D:prop>
          <D:status>HTTP/1.1 200 OK</D:status>
        </D:propstat>
      </D:response>`;

    if (depth !== "0") {
      if (listResponse.CommonPrefixes) {
        for (const commonPrefix of listResponse.CommonPrefixes) {
          // commonPrefix.Prefix is like "s3_base/mount_segment/sub_segment/folder_name/"
          // We need "folder_name" relative to the prefixForS3Query
          let folderDisplayName = commonPrefix.Prefix;
          if (prefixForS3Query && folderDisplayName.startsWith(prefixForS3Query)) {
            folderDisplayName = folderDisplayName.substring(prefixForS3Query.length);
          }
          folderDisplayName = folderDisplayName.replace(/\/$/, ""); // Remove trailing slash for display

          const folderWebDAVHref = encodeURI(baseWebDAVHref + folderDisplayName + "/");

          xmlBody += `
      <D:response>
        <D:href>${folderWebDAVHref}</D:href>
        <D:propstat>
          <D:prop>
            <D:resourcetype><D:collection/></D:resourcetype>
            <D:displayname>${folderDisplayName}</D:displayname>
            <D:getlastmodified>${new Date().toUTCString()}</D:getlastmodified>
          </D:prop>
          <D:status>HTTP/1.1 200 OK</D:status>
        </D:propstat>
      </D:response>`;
        }
      }

      if (listResponse.Contents) {
        for (const item of listResponse.Contents) {
          // Skip if item.Key is exactly the prefixForS3Query and ends with a slash (it's the "folder" object itself)
          if (item.Key === prefixForS3Query && item.Key.endsWith("/")) continue;
          // Skip other "directory marker" objects if they are not actual files (e.g. size 0 and ends with /)
          // However, allow 0 byte files that don't end with /
          if (item.Key.endsWith("/") && item.Size === 0 && item.Key !== prefixForS3Query) continue;


          let fileDisplayName = item.Key;
          if (prefixForS3Query && fileDisplayName.startsWith(prefixForS3Query)) {
            fileDisplayName = fileDisplayName.substring(prefixForS3Query.length);
          }

          // If fileDisplayName is now empty, it means item.Key was equal to prefixForS3Query but didn't end in a slash.
          // This usually shouldn't happen for files unless prefixForS3Query was a file path itself (not for PROPFIND on a collection).
          if (!fileDisplayName && item.Key === prefixForS3Query) continue;


          const fileWebDAVHref = encodeURI(baseWebDAVHref + fileDisplayName);

          xmlBody += `
      <D:response>
        <D:href>${fileWebDAVHref}</D:href>
        <D:propstat>
          <D:prop>
            <D:resourcetype></D:resourcetype>
            <D:displayname>${fileDisplayName}</D:displayname>
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
    // console.log(`[PROPFIND_DEBUG] buildPropfindResponse - Final XML Body for "${webdavRequestPath}":`, xmlBody);

    return new Response(xmlBody, {
      status: 207,
      headers: { "Content-Type": "application/xml; charset=utf-8" },
    });
  } catch (error) {
    console.error("[PROPFIND_ERROR] Error in buildPropfindResponse:", error, error.stack ? error.stack : '(no stack trace)');
    return new Response("Internal Server Error in buildPropfindResponse: " + error.message, { status: 500 });
  }
}
