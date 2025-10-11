/**
 * 虚拟目录处理工具
 * 提供虚拟目录的判断、列表生成等功能
 */

import { normalizePath } from "./PathResolver.js";

/**
 * 检查路径是否为虚拟路径
 * @param {string} path - 请求路径
 * @param {Array} mounts - 挂载点列表
 * @returns {boolean} 是否为虚拟路径
 */
export function isVirtualPath(path, mounts) {
  // 规范化路径
  path = path.startsWith("/") ? path : "/" + path;

  // 根路径总是虚拟路径
  if (path === "/" || path === "//") {
    return true;
  }

  // 按照路径长度降序排序，以便优先匹配最长的路径
  const sortedMounts = [...mounts].sort((a, b) => b.mount_path.length - a.mount_path.length);

  // 查找匹配的挂载点
  for (const mount of sortedMounts) {
    const mountPath = mount.mount_path.startsWith("/") ? mount.mount_path : "/" + mount.mount_path;

    // 如果请求路径完全匹配挂载点或者是挂载点的子路径
    if (path === mountPath || path === mountPath + "/" || path.startsWith(mountPath + "/")) {
      return false; // 匹配到实际挂载点，不是虚拟路径
    }
  }

  return true; // 没有匹配到挂载点，是虚拟路径
}

/**
 * 获取虚拟目录列表
 * 用于处理根路径和虚拟目录，返回挂载点列表作为虚拟目录结构
 * @param {Array} mounts - 挂载点列表
 * @param {string} path - 当前路径
 * @param {string|null} basicPath - API密钥的基本路径（用于过滤显示内容）
 * @returns {Promise<Object>} 虚拟目录内容
 */
export async function getVirtualDirectoryListing(mounts, path, basicPath = null) {
  // 确保路径格式正确
  path = normalizePath(path, true);

  // 检查当前路径是否在基本路径权限范围内
  let hasPermissionForCurrentPath = true;
  if (basicPath && basicPath !== "/") {
    const normalizedBasicPath = basicPath.replace(/\/+$/, "");
    const normalizedCurrentPath = path.replace(/\/+$/, "") || "/";

    // 只有当前路径是基本路径或其子路径时才有权限
    hasPermissionForCurrentPath = normalizedCurrentPath === normalizedBasicPath || normalizedCurrentPath.startsWith(normalizedBasicPath + "/");
  }

  const result = {
    path: path,
    isDirectory: true,
    isVirtual: true,
    items: [],
  };

  // 如果当前路径没有权限，返回空列表
  if (!hasPermissionForCurrentPath) {
    return result;
  }

  const directories = new Set();
  const mountEntries = [];

  // 处理挂载点
  for (const mount of mounts) {
    const mountPath = mount.mount_path.startsWith("/") ? mount.mount_path : "/" + mount.mount_path;
    const normalizedMountPath = normalizePath(mountPath, false);

    // 检查挂载点是否在当前路径下
    if (normalizedMountPath.startsWith(path)) {
      const relativePath = normalizedMountPath.substring(path.length);

      // 如果相对路径为空，说明当前路径就是挂载点
      if (relativePath === "" || relativePath === "/") {
        // 检查基本路径权限
        if (basicPath && basicPath !== "/") {
          const normalizedBasicPath = basicPath.replace(/\/+$/, "");
          const normalizedMountPath = normalizedMountPath.replace(/\/+$/, "") || "/";

          // 只有挂载点在基本路径范围内才显示
          if (normalizedMountPath === normalizedBasicPath || normalizedMountPath.startsWith(normalizedBasicPath + "/")) {
            mountEntries.push({
              name: mount.name,
              path: normalizedMountPath,
              isDirectory: true,
              isMount: true,
              mountId: mount.id,
              storageType: mount.storage_type,
            });
          }
        } else {
          // 没有基本路径限制，显示所有挂载点
          mountEntries.push({
            name: mount.name,
            path: normalizedMountPath,
            isDirectory: true,
            isMount: true,
            mountId: mount.id,
            storageType: mount.storage_type,
          });
        }
      } else {
        // 挂载点在更深的层级，需要创建中间虚拟目录
        const pathSegments = relativePath.split("/").filter((segment) => segment.length > 0);
        if (pathSegments.length > 0) {
          const firstDir = pathSegments[0];

          // 检查基本路径权限
          if (basicPath && basicPath !== "/") {
            const normalizedBasicPath = basicPath.replace(/\/+$/, "");
            const normalizedDirPath = (path + firstDir).replace(/\/+$/, "");

            // 检查目录路径是否在基本路径范围内
            if (normalizedDirPath === normalizedBasicPath || normalizedDirPath.startsWith(normalizedBasicPath + "/")) {
              directories.add(firstDir);
            }
            // 检查基本路径是否在目录路径范围内
            else if (normalizedBasicPath.startsWith(normalizedDirPath + "/")) {
              directories.add(firstDir);
            }
          } else {
            // 没有基本路径限制，显示所有目录
            directories.add(firstDir);
          }
        }
      }
    }
  }

  // 将目录添加到结果中
  for (const dir of directories) {
    result.items.push({
      name: dir,
      path: path + dir + "/",
      isDirectory: true,
      isVirtual: true,
      // 虚拟目录不设置modified字段，前端会显示"-"
    });
  }

  // 将挂载点添加到结果中
  for (const mountEntry of mountEntries) {
    result.items.push(mountEntry);
  }

  return result;
}
