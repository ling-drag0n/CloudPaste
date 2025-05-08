/**
 * 处理WebDAV PROPFIND请求
 * 用于获取文件和目录信息(列表)
 */
import { getMountsByAdmin, getMountsByApiKey } from "../../services/storageMountService.js";
import { createS3Client } from "../../utils/s3Utils.js";
import { S3Client, ListObjectsV2Command, HeadObjectCommand } from "@aws-sdk/client-s3";
import { updateMountLastUsed } from "../utils/webdavUtils.js"; // 假设updateMountLastUsed在webdavUtils中

// 假设 getDirectoryStructure 函数存在于某个地方（可能是此文件或utils）
// 如果它不在此文件，您需要确保它被正确导入或定义
// function getDirectoryStructure(...) { ... }

/**
 * 处理PROPFIND请求
 */
export async function handlePropfind(c, path, userId, userType, db) {
  const depth = c.req.header("Depth") || "infinity";
  if (depth !== "0" && depth !== "1" && depth !== "infinity") {
    return new Response("Bad Request: Invalid Depth Header", { status: 400 });
  }

  try {
    const originalWebdavPath = path; // 保留原始WebDAV相对路径供后续使用
    path = path.startsWith("/") ? path : "/" + path;
    path = path.endsWith("/") ? path : path + "/"; // PROPFIND 通常针对目录

    console.log(`[PROPFIND_DEBUG] Entered handlePropfind. webdavPath: "${originalWebdavPath}", userId: "${userId}", userType: "${userType}", depth: "${depth}"`);

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
    if (!mounts) {
         console.error("[PROPFIND_ERROR] Failed to fetch mounts.");
         // 即使没有挂载点，也应该能处理虚拟路径，所以不直接返回错误，除非下面逻辑依赖mounts数组
         mounts = []; // 设为空数组以防万一
    }
     if (mounts.length > 0) {
        mounts.forEach(m => console.log(`[PROPFIND_DEBUG] Available mount check: id=${m.id}, name=${m.name}, path=${m.mount_path}, active=${m.is_active}`));
     }


    let isVirtualPath = true;
    let matchingMount = null;
    let subPath = ""; // Path relative to the mount point's root

    mounts.sort((a, b) => b.mount_path.length - a.mount_path.length);

    for (const mount of mounts) {
      // 确保比较时，mount_path 和 path 都有或都没有结尾斜杠，或者用 startsWith
      const mountPathForCheck = mount.mount_path.startsWith("/") ? mount.mount_path : "/" + mount.mount_path;
      // 对比时考虑结尾斜杠: /files 应该匹配 /files/ 请求
      const mountPathWithSlash = mountPathForCheck.endsWith('/') ? mountPathForCheck : mountPathForCheck + '/';
      const mountPathWithoutSlash = mountPathForCheck.endsWith('/') ? mountPathForCheck.slice(0,-1) : mountPathForCheck;


      console.log(`[PROPFIND_DEBUG] Checking request_path "${path}" against mount_path "${mountPathForCheck}"`);

      // 匹配逻辑: 请求路径等于挂载点路径(带或不带斜杠)，或者以挂载点路径加斜杠开头
       if (path === mountPathWithSlash || (mountPathWithoutSlash !== '/' && path.startsWith(mountPathWithSlash)) ) {
          matchingMount = mount;
          // 计算subPath: path相对于mountPathForCheck的部分
          // 例如: path=/files/foo/, mountPathForCheck=/files -> subPath=/foo/
          // 例如: path=/files/, mountPathForCheck=/files -> subPath=/
          subPath = path.substring(mountPathForCheck.length);
          if (!subPath) subPath = "/"; // 如果完全匹配，subPath是根
          if (!subPath.startsWith("/")) {
            subPath = "/" + subPath; // 确保subPath以/开头
          }
          isVirtualPath = false;
          console.log(`[PROPFIND_DEBUG] Found matchingMount: id="${matchingMount.id}", name="${matchingMount.name}", mount_path="${matchingMount.mount_path}", storage_config_id="${matchingMount.storage_config_id}", calculated subPath: "${subPath}"`);
          break;
      }
    }

    // 如果没有找到匹配的物理挂载点，则认为是虚拟路径或根路径
    if (isVirtualPath) {
      console.log(`[PROPFIND_DEBUG] Path "${path}" is virtual or no matching physical mount found. Calling respondWithMounts.`);
      // respondWithMounts 用于列出根下的所有挂载点，或者如果路径是某个挂载点的父虚拟目录，则列出其下的挂载点/子目录
      return await respondWithMounts(c, userId, userType, db, path);
    }

    // --- 处理实际挂载点路径 ---
    const s3Config = await db.prepare("SELECT * FROM s3_configs WHERE id = ?").bind(matchingMount.storage_config_id).first();

    if (!s3Config) {
      console.error(`[PROPFIND_ERROR] Storage Configuration Not Found for mount_id: "${matchingMount.id}", storage_config_id: "${matchingMount.storage_config_id}"`);
      return new Response("Storage Configuration Not Found", { status: 404 });
    }
    console.log(`[PROPFIND_DEBUG] Fetched s3Config: id="${s3Config.id}", bucket_name="${s3Config.bucket_name}", default_folder="${s3Config.default_folder || '(empty)'}"`);

    const s3Client = await createS3Client(s3Config, c.env.ENCRYPTION_SECRET);

    // --- 修正后的S3查询前缀构建逻辑 ---
    let s3QueryPrefix = "";
    if (s3Config.default_folder) {
      s3QueryPrefix = s3Config.default_folder;
      if (!s3QueryPrefix.endsWith('/')) s3QueryPrefix += '/';
    }
    let mountPathSegment = matchingMount.mount_path;
    if (mountPathSegment.startsWith('/')) mountPathSegment = mountPathSegment.substring(1);
    if (mountPathSegment && !mountPathSegment.endsWith('/')) mountPathSegment += '/';
    if (mountPathSegment) s3QueryPrefix += mountPathSegment;

    let subPathSegment = subPath;
    if (subPathSegment.startsWith('/')) subPathSegment = subPathSegment.substring(1);
    s3QueryPrefix += subPathSegment; // subPath已经是目录形式（带/）或根("/")对应的空串

    s3QueryPrefix = s3QueryPrefix.replace(/\/+/g, "/"); // 规范化斜杠

    // S3 前缀通常不以 / 开头，除非是根目录 ""
    // if (s3QueryPrefix.startsWith('/') && s3QueryPrefix !== "/") {
    //     s3QueryPrefix = s3QueryPrefix.substring(1);
    // }
    // 保持原样，让 S3 SDK 处理空字符串 "" 代表根目录的情况

    console.log(`[PROPFIND_DEBUG] Original WebDAV relative path: "${originalWebdavPath}", Mount path: "${matchingMount.mount_path}", Sub-path within mount: "${subPath}"`);
    console.log(`[PROPFIND_DEBUG] S3 default_folder: "${s3Config.default_folder || '(empty)'}"`);
    console.log(`[PROPFIND_DEBUG] Calculated final s3QueryPrefix for S3 ListObjects: "${s3QueryPrefix}"`);
    // --- S3查询前缀构建逻辑结束 ---

    try {
      await updateMountLastUsed(db, matchingMount.id);
    } catch (updateError) {
      // 日志级别改为warn，不影响主流程
      console.warn(`[PROPFIND_WARN] Failed to update last_used for mount id: ${matchingMount.id}`, updateError);
    }

    // 使用计算出的 s3QueryPrefix 调用 buildPropfindResponse
    return await buildPropfindResponse(c, s3Client, s3Config.bucket_name, s3QueryPrefix, depth, path /* path是WebDAV相对路径，如 /testmount/ */);

  } catch (error) {
    console.error("[PROPFIND_ERROR] Unhandled error in handlePropfind:", error, error.stack ? error.stack : '(no stack trace)');
    return new Response("Internal Server Error: " + error.message, { status: 500 });
  }
}


// --- respondWithMounts 函数 ---
// (如果需要，确保 getDirectoryStructure 已定义或导入)
// (建议在此函数中也加入DEBUG日志，特别是打印 structure 的内容)
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
    if (!mounts) mounts = []; // 防御性编程

    path = path.startsWith("/") ? path : "/" + path;
    path = path.endsWith("/") ? path : path + "/";

    const structure = getDirectoryStructure(mounts, path); // 假设 getDirectoryStructure 已定义
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
      const dirWebDAVPath = path + dir + "/"; // WebDAV Path for display
      const encodedHrefPath = encodeURI(dirWebDAVPath);
      xmlBody += `
      <D:response>
        <D:href>/dav${encodedHrefPath}</D:href>
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
      const mountWebDAVPath = mount.mount_path.startsWith("/") ? mount.mount_path : "/" + mount.mount_path;
      // Ensure href path ends with / for collections
      const hrefPath = mountWebDAVPath.endsWith('/') ? mountWebDAVPath : mountWebDAVPath + '/';
      const encodedHrefPath = encodeURI(hrefPath);

      // This logic seems complex, ensure it correctly determines if a mount should be listed directly under 'path'
      const relativePathToCurrent = mountWebDAVPath.substring(path.length);
      if (!relativePathToCurrent.includes("/") || relativePathToCurrent === "") {
         xmlBody += `
        <D:response>
          <D:href>/dav${encodedHrefPath}</D:href>
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
    // console.log(`[PROPFIND_DEBUG] respondWithMounts - Final XML Body for path "${path}":`, xmlBody);

    return new Response(xmlBody, {
      status: 207,
      headers: { "Content-Type": "application/xml; charset=utf-8" },
    });
}


// --- buildPropfindResponse 函数 ---
// (建议在此函数中也加入DEBUG日志, 特别是打印 S3 listResponse 的内容)
async function buildPropfindResponse(c, s3Client, bucketName, prefixForS3Query, depth, webdavRequestPath) {
  console.log(`[PROPFIND_DEBUG] Entered buildPropfindResponse. bucketName: "${bucketName}", S3_query_prefix: "${prefixForS3Query}", depth: "${depth}", webdav_requestPath: "${webdavRequestPath}"`);

  try {
    const listParams = {
      Bucket: bucketName,
      Prefix: prefixForS3Query, // 使用正确计算的S3前缀
      Delimiter: "/",
    };
    console.log(`[PROPFIND_DEBUG] S3 ListObjectsV2Command params: Bucket='${bucketName}', Prefix='${listParams.Prefix}', Delimiter='${listParams.Delimiter}'`);

    const listCommand = new ListObjectsV2Command(listParams);
    const listResponse = await s3Client.send(listCommand);

    console.log(`[PROPFIND_DEBUG] S3 ListObjectsV2Command response: Contents count: ${listResponse.Contents ? listResponse.Contents.length : 0}, CommonPrefixes count: ${listResponse.CommonPrefixes ? listResponse.CommonPrefixes.length : 0}`);
    // 选择性地打印详细S3响应
    // if (listResponse.Contents && listResponse.Contents.length > 0) { console.log(`[PROPFIND_DEBUG] S3 Contents (first 5):`, JSON.stringify(listResponse.Contents.slice(0, 5).map(item => ({ Key: item.Key, Size: item.Size })))); }
    // if (listResponse.CommonPrefixes && listResponse.CommonPrefixes.length > 0) { console.log(`[PROPFIND_DEBUG] S3 CommonPrefixes (first 5):`, JSON.stringify(listResponse.CommonPrefixes.slice(0, 5).map(item => ({ Prefix: item.Prefix })))); }

    // --- XML构建逻辑（可能需要根据S3响应微调href和displayname）---
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
      // 处理目录 (CommonPrefixes)
      if (listResponse.CommonPrefixes) {
        for (const commonPrefix of listResponse.CommonPrefixes) {
          // commonPrefix.Prefix is like "combined_s3_prefix/folder_name/"
          let folderDisplayName = commonPrefix.Prefix;
          // Extract the name relative to the S3 query prefix
          if (prefixForS3Query && folderDisplayName.startsWith(prefixForS3Query)) {
            folderDisplayName = folderDisplayName.substring(prefixForS3Query.length);
          }
          folderDisplayName = folderDisplayName.replace(/\/$/, ""); // Remove trailing slash for display

          // Construct WebDAV href relative to the current request path
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

      // 处理文件 (Contents)
      if (listResponse.Contents) {
        for (const item of listResponse.Contents) {
          // Skip the directory object itself if S3 lists it
          if (item.Key === prefixForS3Query && item.Key.endsWith("/")) continue;
          // Skip other potential directory markers unless they are 0-byte files not ending in /
          if (item.Key.endsWith("/") && item.Size === 0 && item.Key !== prefixForS3Query) continue;

          let fileDisplayName = item.Key;
          // Extract the name relative to the S3 query prefix
          if (prefixForS3Query && fileDisplayName.startsWith(prefixForS3Query)) {
            fileDisplayName = fileDisplayName.substring(prefixForS3Query.length);
          }

          // Skip if the relative name is empty (means it was the prefix itself)
          if (!fileDisplayName) continue;


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

// --- getDirectoryStructure 函数定义 ---
// (需要确保此函数已定义或从正确位置导入)
function getDirectoryStructure(mounts, currentPath) {
  currentPath = currentPath.startsWith("/") ? currentPath : "/" + currentPath;
  currentPath = currentPath.endsWith("/") ? currentPath : currentPath + "/";
  const result = { directories: new Set(), mounts: [] };
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
      if (firstDir) result.directories.add(firstDir);
      continue;
    }
    // 这段逻辑可能也需要检查，是否正确处理了父级虚拟目录
    if (currentPath !== "/" && mountPath.startsWith(currentPath)) {
        const relativePath = mountPath.substring(currentPath.length);
        const firstDir = relativePath.split('/')[0];
        if(firstDir) result.directories.add(firstDir);
    }
  }
  return { directories: Array.from(result.directories), mounts: result.mounts };
}