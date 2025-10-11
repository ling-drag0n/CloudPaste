/**
 * 文件系统服务API
 */

import { get, post, del } from "../client";
import { API_BASE_URL } from "../config";
import { S3MultipartUploader } from "./S3MultipartUploader.js";

/******************************************************************************
 * 统一文件系统API函数
 ******************************************************************************/

/**
 * 获取目录列表
 * @param {string} path 请求路径
 * @param {Object} options 选项参数
 * @param {boolean} options.refresh 是否强制刷新，跳过缓存
 * @returns {Promise<Object>} 目录列表响应对象
 */
export async function getDirectoryList(path, options = {}) {
  const params = { path };
  if (options.refresh) {
    params.refresh = "true";
  }
  return get("/fs/list", { params });
}

/**
 * 获取文件信息
 * @param {string} path 文件路径
 * @returns {Promise<Object>} 文件信息响应对象
 */
export async function getFileInfo(path) {
  return get("/fs/get", { params: { path } });
}

/**
 * 搜索文件
 * @param {string} query 搜索查询字符串
 * @param {Object} searchParams 搜索参数对象
 * @param {string} searchParams.scope 搜索范围 ('global', 'mount', 'directory')
 * @param {string} searchParams.mountId 挂载点ID（当scope为'mount'时）
 * @param {string} searchParams.path 搜索路径（当scope为'directory'时）
 * @param {number} searchParams.limit 结果限制数量，默认50
 * @param {number} searchParams.offset 结果偏移量，默认0
 * @returns {Promise<Object>} 搜索结果响应对象
 */
export async function searchFiles(query, searchParams = {}) {
  const params = {
    q: query,
    scope: searchParams.scope || "global",
    limit: (searchParams.limit || 50).toString(),
    offset: (searchParams.offset || 0).toString(),
  };

  // 添加可选参数
  if (searchParams.mountId) {
    params.mount_id = searchParams.mountId;
  }
  if (searchParams.path) {
    params.path = searchParams.path;
  }

  return get("/fs/search", { params });
}

/**
 * 下载文件
 * @param {string} path 文件路径
 * @returns {string} 文件下载URL
 */
export function getFileDownloadUrl(path) {
  return `${API_BASE_URL}/api/fs/download?path=${encodeURIComponent(path)}`;
}

/**
 * 创建目录
 * @param {string} path 目录路径
 * @returns {Promise<Object>} 创建结果响应对象
 */
export async function createDirectory(path) {
  return post(`/fs/mkdir`, { path });
}

/**
 * 上传文件
 * @param {string} path 目标路径
 * @param {File} file 文件对象
 * @param {boolean} useMultipart 是否使用服务器分片上传，默认为true
 * @param {Function} onXhrCreated XHR创建后的回调，用于保存引用以便取消请求
 * @returns {Promise<Object>} 上传结果响应对象
 */
export async function uploadFile(path, file, useMultipart = true, onXhrCreated) {
  const formData = new FormData();
  formData.append("file", file);
  formData.append("path", path);
  formData.append("use_multipart", useMultipart.toString());

  return post(`/fs/upload`, formData, { onXhrCreated });
}

/**
 * 批量删除文件或目录（统一删除接口）
 * 支持单个路径或路径数组，文件直接删除，目录递归删除
 * @param {string|Array<string>} pathsOrPath 文件路径或路径数组
 * @returns {Promise<Object>} 删除结果响应对象
 */
export async function batchDeleteItems(pathsOrPath) {
  // 统一处理单个路径和路径数组
  const paths = Array.isArray(pathsOrPath) ? pathsOrPath : [pathsOrPath];

  // 统一使用批量删除接口，DELETE 方法带请求体
  return del(`/fs/batch-remove`, { paths });
}

/**
 * 重命名文件或目录
 * @param {string} oldPath 旧路径
 * @param {string} newPath 新路径
 * @returns {Promise<Object>} 重命名结果响应对象
 */
export async function renameItem(oldPath, newPath) {
  return post(`/fs/rename`, { oldPath, newPath });
}

/**
 * 更新文件内容
 * @param {string} path 文件路径
 * @param {string} content 新的文件内容
 * @returns {Promise<Object>} 更新结果响应对象
 */
export async function updateFile(path, content) {
  return post(`/fs/update`, { path, content });
}

/**
 * 获取文件直链
 * @param {string} path 文件路径
 * @param {number|null} expiresIn 过期时间（秒），null表示使用S3配置的默认签名时间
 * @param {boolean} forceDownload 是否强制下载而非预览
 * @returns {Promise<Object>} 包含预签名URL的响应对象
 */
export async function getFileLink(path, expiresIn = null, forceDownload = false) {
  const params = {
    path: path,
    force_download: forceDownload.toString(),
  };

  // 只有当expiresIn不为null时才添加expires_in参数
  if (expiresIn !== null) {
    params.expires_in = expiresIn.toString();
  }

  return get("/fs/file-link", { params });
}

/******************************************************************************
 * 分片上传相关API函数
 ******************************************************************************/

/**
 * 初始化前端分片上传（生成预签名URL列表）
 * @param {string} path 目标路径
 * @param {string} fileName 文件名
 * @param {number} fileSize 文件大小
 * @param {string} contentType 文件类型（可选，后端会推断）
 * @param {number} partSize 分片大小（默认5MB）
 * @returns {Promise<Object>} 初始化结果响应对象
 */
export async function initMultipartUpload(path, fileName, fileSize, contentType, partSize = 5 * 1024 * 1024) {
  const partCount = Math.ceil(fileSize / partSize);

  return post(`/fs/multipart/init`, {
    path,
    fileName,
    fileSize,
    partSize,
    partCount,
  });
}

/**
 * 完成前端分片上传
 * @param {string} path 文件路径
 * @param {string} uploadId 上传ID
 * @param {Array<{partNumber: number, etag: string}>} parts 所有已上传分片的信息
 * @param {string} fileName 文件名
 * @param {number} fileSize 文件大小（字节）
 * @returns {Promise<Object>} 完成上传结果响应对象
 */
export async function completeMultipartUpload(path, uploadId, parts, fileName, fileSize) {
  return post(`/fs/multipart/complete`, {
    path,
    uploadId,
    parts,
    fileName,
    fileSize,
  });
}

/**
 * 中止前端分片上传
 * @param {string} path 文件路径
 * @param {string} uploadId 上传ID
 * @param {string} fileName 文件名
 * @returns {Promise<Object>} 中止上传结果响应对象
 */
export async function abortMultipartUpload(path, uploadId, fileName) {
  return post(`/fs/multipart/abort`, { path, uploadId, fileName });
}

//============断点续传================

/**
 * 列出进行中的分片上传
 * @param {string} path 目标路径（可选，用于过滤特定文件的上传）
 * @returns {Promise<Object>} 进行中的上传列表响应对象
 */
export async function listMultipartUploads(path = "") {
  return post(`/fs/multipart/list-uploads`, { path });
}

/**
 * 列出已上传的分片
 * @param {string} path 文件路径
 * @param {string} uploadId 上传ID
 * @param {string} fileName 文件名
 * @returns {Promise<Object>} 已上传的分片列表响应对象
 */
export async function listMultipartParts(path, uploadId, fileName) {
  return post(`/fs/multipart/list-parts`, { path, uploadId, fileName });
}

/**
 * 为现有上传刷新预签名URL
 * @param {string} path 文件路径
 * @param {string} uploadId 现有的上传ID
 * @param {Array} partNumbers 需要刷新URL的分片编号数组
 * @returns {Promise<Object>} 刷新的预签名URL列表响应对象
 */
export async function refreshMultipartUrls(path, uploadId, partNumbers) {
  return post(`/fs/multipart/refresh-urls`, { path, uploadId, partNumbers });
}

/******************************************************************************
 * 预签名URL上传相关API函数
 ******************************************************************************/

/**
 * 获取预签名上传URL
 * @param {string} path 目标路径
 * @param {string} fileName 文件名
 * @param {string} contentType 文件类型
 * @param {number} fileSize 文件大小
 * @returns {Promise<Object>} 预签名URL响应对象
 */
export async function getPresignedUploadUrl(path, fileName, contentType, fileSize) {
  return post(`/fs/presign`, {
    path,
    fileName,
    contentType,
    fileSize,
  });
}

/**
 * 提交预签名URL上传完成
 * @param {Object} uploadInfo 上传信息对象
 * @param {string} etag ETag
 * @param {string} contentType 文件MIME类型
 * @param {number} fileSize 文件大小
 * @returns {Promise<Object>} 提交结果响应对象
 */
export async function commitPresignedUpload(uploadInfo, etag, contentType, fileSize) {
  return post(`/fs/presign/commit`, {
    ...uploadInfo,
    etag,
    contentType,
    fileSize,
  });
}

/******************************************************************************
 * 复制相关API函数
 ******************************************************************************/

/**
 * 批量复制文件或目录
 * @param {Array<{sourcePath: string, targetPath: string}>} items 要复制的项目数组，每项包含源路径和目标路径
 * @param {boolean} skipExisting 是否跳过已存在的文件，默认为true
 * @param {Object} options 额外选项
 * @param {Function} [options.onProgress] 进度回调函数
 * @param {Function} [options.onCancel] 取消检查函数
 * @returns {Promise<Object>} 批量复制结果响应对象
 */
export async function batchCopyItems(items, skipExisting = true, options = {}) {
  const { onProgress, onCancel } = options;

  // 首先调用服务器批量复制API
  const result = await post(`/fs/batch-copy`, { items, skipExisting });

  // 检查是否需要客户端处理的跨存储复制
  if (result.success && result.data && result.data.requiresClientSideCopy) {
    console.log("检测到需要客户端处理的批量跨存储复制", result.data);

    // 执行客户端复制流程
    return performClientSideCopy({
      copyResult: result.data,
      onProgress,
      onCancel,
    });
  }

  // 正常的服务器端复制，直接返回结果
  return result;
}

/**
 * 提交批量复制完成
 * @param {Object} data 批量复制完成数据
 * @param {string} data.targetMountId 目标挂载点ID
 * @param {Array<Object>} data.files 文件列表，每个对象包含 {targetPath, s3Path, contentType?, fileSize?, etag?}
 * @returns {Promise<Object>} 提交结果响应对象
 */
export async function commitBatchCopy(data) {
  return post(`/fs/batch-copy-commit`, data);
}

/**
 * 从文件系统创建分享链接
 * @param {string} path 文件路径
 * @returns {Promise<Object>} 创建结果响应对象
 */
export async function createShareFromFileSystem(path) {
  return post(`/fs/create-share`, { path });
}

/**
 * 复制文件或目录
 * @param {string} sourcePath 源路径
 * @param {string} targetPath 目标路径
 * @param {boolean} skipExisting 是否跳过已存在的文件，默认为true
 * @param {Object} options 额外选项
 * @param {Function} [options.onProgress] 进度回调函数
 * @param {Function} [options.onCancel] 取消检查函数
 * @returns {Promise<Object>} 复制结果响应对象
 */
export async function copyItem(sourcePath, targetPath, skipExisting = true, options = {}) {
  // 将单文件复制转换为批量复制格式
  const items = [{ sourcePath, targetPath }];
  return batchCopyItems(items, skipExisting, options);
}

/**
 * 提交复制完成信息
 * @param {Object} data 复制完成数据
 * @param {string} data.sourcePath 源文件路径
 * @param {string} data.targetPath 目标文件路径
 * @param {string} data.targetMountId 目标挂载点ID
 * @param {string} data.s3Path S3存储路径
 * @param {string} [data.etag] 文件ETag（可选）
 * @param {string} [data.contentType] 文件MIME类型（可选）
 * @param {number} [data.fileSize] 文件大小（字节）（可选）
 * @returns {Promise<Object>} 提交结果响应对象
 */
export async function commitCopy(data) {
  // 将单文件提交转换为批量提交格式
  return commitBatchCopy({
    targetMountId: data.targetMountId,
    files: [
      {
        targetPath: data.targetPath,
        s3Path: data.s3Path,
        contentType: data.contentType,
        fileSize: data.fileSize,
        etag: data.etag,
      },
    ],
  });
}

/******************************************************************************
 * 辅助上传功能API函数
 ******************************************************************************/

/**
 * 使用预签名URL上传文件
 * @param {string} url 预签名URL
 * @param {File|Blob|ArrayBuffer} data 要上传的数据
 * @param {string} contentType 文件MIME类型
 * @param {Function} onProgress 进度回调函数
 * @param {Function} onCancel 取消检查函数
 * @param {Function} setXhr 设置XHR引用的函数
 * @returns {Promise<Object>} 上传结果，包含etag和size
 */
export async function uploadWithPresignedUrl(url, data, contentType, onProgress, onCancel, setXhr) {
  return new Promise((resolve, reject) => {
    const xhr = new XMLHttpRequest();

    if (setXhr) {
      setXhr(xhr);
    }

    // 设置进度监听
    if (onProgress) {
      xhr.upload.addEventListener("progress", (event) => {
        if (event.lengthComputable) {
          const progress = Math.round((event.loaded / event.total) * 100);
          onProgress(progress, event.loaded, event.total);
        }
      });
    }

    xhr.onload = function () {
      if (cancelChecker) {
        clearInterval(cancelChecker);
      }

      if (xhr.status === 200) {
        const etag = xhr.getResponseHeader("ETag");
        resolve({
          etag: etag ? etag.replace(/"/g, "") : null,
          size: data.size || data.byteLength || 0,
        });
      } else {
        reject(new Error(`上传失败: HTTP ${xhr.status}`));
      }
    };

    xhr.onerror = function () {
      if (cancelChecker) {
        clearInterval(cancelChecker);
      }
      reject(new Error("上传过程中发生网络错误"));
    };

    xhr.onabort = function () {
      if (cancelChecker) {
        clearInterval(cancelChecker);
      }
      reject(new Error("上传已取消"));
    };

    // 定期检查取消状态
    let cancelChecker = null;
    if (onCancel) {
      cancelChecker = setInterval(() => {
        if (onCancel()) {
          if (cancelChecker) {
            clearInterval(cancelChecker);
          }
          xhr.abort();
        }
      }, 100);
    }

    // 开始上传
    xhr.open("PUT", url);
    // 设置Content-Type头部，使用后端推断的正确MIME类型
    if (contentType && contentType !== null) {
      xhr.setRequestHeader("Content-Type", contentType);
    }
    xhr.send(data);
  });
}

/**
 * 获取文件内容
 * @param {Object} options 获取选项
 * @param {string} options.url 文件URL
 * @param {Function} [options.onProgress] 进度回调函数
 * @param {Function} [options.onCancel] 取消检查函数
 * @param {Function} [options.setXhr] 设置XHR引用的函数
 * @returns {Promise<ArrayBuffer>} 文件内容
 */
export async function fetchFileContent(options) {
  const { url, onProgress, onCancel, setXhr } = options;

  return new Promise((resolve, reject) => {
    const xhr = new XMLHttpRequest();

    if (setXhr) {
      setXhr(xhr);
    }

    // 设置响应类型为ArrayBuffer
    xhr.responseType = "arraybuffer";

    // 设置进度监听
    if (onProgress) {
      xhr.addEventListener("progress", (event) => {
        if (event.lengthComputable) {
          const progress = Math.round((event.loaded / event.total) * 100);
          onProgress(progress, event.loaded, event.total);
        }
      });
    }

    // 设置状态变化监听
    xhr.addEventListener("readystatechange", () => {
      if (xhr.readyState === XMLHttpRequest.DONE) {
        if (xhr.status >= 200 && xhr.status < 300) {
          resolve(xhr.response);
        } else {
          reject(new Error(`下载失败: ${xhr.status} ${xhr.statusText}`));
        }
      }
    });

    // 设置错误监听
    xhr.addEventListener("error", () => {
      reject(new Error("网络错误"));
    });

    // 设置中止监听
    xhr.addEventListener("abort", () => {
      reject(new Error("下载已取消"));
    });

    // 定期检查取消状态
    if (onCancel) {
      const checkCancel = () => {
        if (onCancel()) {
          xhr.abort();
          return;
        }
        if (xhr.readyState !== XMLHttpRequest.DONE) {
          setTimeout(checkCancel, 100);
        }
      };
      setTimeout(checkCancel, 100);
    }

    // 开始下载
    xhr.open("GET", url);
    xhr.send();
  });
}

/**
 * 上传到预签名URL
 * @param {Object} options 上传选项
 * @param {string} options.url 预签名URL
 * @param {ArrayBuffer|Blob} options.data 要上传的数据
 * @param {string} options.contentType 文件MIME类型
 * @param {Function} [options.onProgress] 进度回调函数
 * @param {Function} [options.onCancel] 取消检查函数
 * @param {Function} [options.setXhr] 设置XHR引用的函数
 * @returns {Promise<Object>} 上传结果，包含etag和size
 */
export async function uploadToPresignedUrl(options) {
  const { url, data, contentType, onProgress, onCancel, setXhr } = options;
  return uploadWithPresignedUrl(url, data, contentType, onProgress, onCancel, setXhr);
}

/**
 * 执行客户端复制流程
 * @param {Object} options 复制选项
 * @param {Object} options.copyResult 初始复制请求的结果，包含下载URL和上传URL等信息
 * @param {Function} [options.onProgress] 进度回调，参数为(phase, progress)，phase可能是"downloading"或"uploading"
 * @param {Function} [options.onCancel] 取消检查函数，返回true时中止操作
 * @returns {Promise<Object>} 复制结果
 */
export async function performClientSideCopy(options) {
  const { copyResult, onProgress, onCancel } = options;

  console.log(`开始客户端复制流程`, copyResult);

  // 设置下载和上传的XHR引用，用于可能的取消操作
  let downloadXhr = null;
  let uploadXhr = null;

  try {
    // 检查是否为单文件复制
    if (!copyResult.crossStorageResults || copyResult.crossStorageResults.length === 0) {
      throw new Error("没有找到跨存储复制项目");
    }

    // 处理目录复制：如果有目录复制项目且包含 items 数组，需要展开处理
    let allCopyItems = [];
    let targetMountId = null;

    for (const result of copyResult.crossStorageResults) {
      // 检查是否为跳过的项目
      if (result.status === "skipped" || result.skipped === true) {
        console.log(`[客户端复制] 跳过已存在的文件: ${result.source} -> ${result.target}`);
        continue; // 跳过已存在的文件，不添加到复制列表
      }

      if (result.isDirectory && result.items && result.items.length > 0) {
        // 目录复制：将 items 数组中的文件添加到复制列表，并添加必要的元数据
        const itemsWithMetadata = result.items.map((item) => {
          // 正确构建目标路径，避免重复斜杠
          let targetPath = result.target;

          // 确保 targetPath 以斜杠结尾
          if (!targetPath.endsWith("/")) {
            targetPath += "/";
          }

          // 添加相对目录路径（如果存在）
          if (item.relativeDir) {
            targetPath += item.relativeDir + "/";
          }

          // 添加文件名
          targetPath += item.fileName;

          return {
            ...item,
            targetMount: result.targetMount,
            targetPath: targetPath,
          };
        });
        allCopyItems.push(...itemsWithMetadata);

        // 记录目标挂载点ID
        if (!targetMountId) {
          targetMountId = result.targetMount;
        }
      } else if (!result.isDirectory) {
        // 文件复制
        const fileItem = {
          ...result,
          targetPath: result.target,
        };
        allCopyItems.push(fileItem);

        // 记录目标挂载点ID
        if (!targetMountId) {
          targetMountId = result.targetMount;
        }
      }
    }

    // 统计跳过的文件数量
    const skippedCount = copyResult.crossStorageResults.filter((result) => result.status === "skipped" || result.skipped === true).length;

    if (allCopyItems.length === 0) {
      // 如果所有文件都被跳过
      if (skippedCount > 0) {
        return {
          success: true,
          message: "FILE_COPY_SUCCESS",
          data: {
            crossStorage: true, // 标记为跨存储复制
            skipped: skippedCount,
            success: 0,
            failed: 0,
          },
        };
      }
      throw new Error("没有找到需要复制的文件");
    }

    // 处理单文件复制
    if (allCopyItems.length === 1) {
      const singleFileCopy = allCopyItems[0];

      // 下载源文件
      console.log(`下载源文件: ${singleFileCopy.sourceS3Path || singleFileCopy.sourceKey}`);
      const fileContent = await fetchFileContent({
        url: singleFileCopy.downloadUrl,
        onProgress: (progress, loaded, total) => {
          if (onProgress) {
            onProgress("downloading", progress, {
              loaded,
              total,
              percentage: progress,
            });
          }
        },
        onCancel,
        setXhr: (xhr) => {
          downloadXhr = xhr;
        },
      });

      // 检查是否被取消
      if (onCancel && onCancel()) {
        throw new Error("操作已取消");
      }

      // 上传文件内容到目标位置
      console.log(`上传到目标位置: ${singleFileCopy.targetS3Path || singleFileCopy.targetKey}`);
      const uploadResult = await uploadToPresignedUrl({
        url: singleFileCopy.uploadUrl,
        data: fileContent,
        contentType: singleFileCopy.contentType || "application/octet-stream",
        onProgress: (progress, loaded, total) => {
          if (onProgress) {
            onProgress("uploading", progress, {
              loaded,
              total,
              percentage: progress,
            });
          }
        },
        onCancel,
        setXhr: (xhr) => {
          uploadXhr = xhr;
        },
      });

      // 提交复制完成信息
      const commitResult = await commitBatchCopy({
        targetMountId: targetMountId,
        files: [
          {
            targetPath: singleFileCopy.targetPath,
            s3Path: singleFileCopy.targetS3Path || singleFileCopy.targetKey,
            contentType: singleFileCopy.contentType,
            fileSize: fileContent.byteLength,
            etag: uploadResult.etag,
          },
        ],
      });

      return {
        success: true,
        message: "FILE_COPY_SUCCESS",
        data: {
          ...commitResult.data,
          crossStorage: true, // 标记为跨存储复制
        },
      };
    }

    // 处理批量文件复制
    const totalItems = allCopyItems.length;
    let completedItems = 0;
    const completedFiles = [];

    for (const item of allCopyItems) {
      // 检查是否被取消
      if (onCancel && onCancel()) {
        throw new Error("操作已取消");
      }

      // 下载源文件
      console.log(`下载源文件: ${item.sourceS3Path || item.sourceKey}`);
      const fileContent = await fetchFileContent({
        url: item.downloadUrl,
        onProgress: (progress) => {
          if (onProgress) {
            const itemProgress = (completedItems / totalItems) * 100;
            onProgress("downloading", progress, {
              currentFile: item.fileName || item.sourceKey,
              currentFileProgress: progress,
              totalProgress: itemProgress + progress / totalItems / 2,
              processedFiles: completedItems,
              totalFiles: totalItems,
              percentage: Math.round(itemProgress + progress / totalItems / 2),
            });
          }
        },
        setXhr: (xhr) => {
          downloadXhr = xhr;
        },
      });

      // 检查是否被取消
      if (onCancel && onCancel()) {
        throw new Error("操作已取消");
      }

      // 上传文件内容
      console.log(`上传到目标位置: ${item.targetS3Path || item.targetKey}`);
      const uploadResult = await uploadToPresignedUrl({
        url: item.uploadUrl,
        data: fileContent,
        contentType: item.contentType || "application/octet-stream",
        onProgress: (progress) => {
          if (onProgress) {
            const itemProgress = (completedItems / totalItems) * 100;
            onProgress("uploading", progress, {
              currentFile: item.fileName || item.sourceKey,
              currentFileProgress: progress,
              totalProgress: itemProgress + (50 + progress) / totalItems / 2,
              processedFiles: completedItems,
              totalFiles: totalItems,
              percentage: Math.round(itemProgress + (50 + progress) / totalItems / 2),
            });
          }
        },
        setXhr: (xhr) => {
          uploadXhr = xhr;
        },
      });

      // 记录完成的文件
      completedFiles.push({
        targetPath: item.targetPath,
        s3Path: item.targetS3Path || item.targetKey,
        contentType: item.contentType,
        fileSize: fileContent.byteLength,
        etag: uploadResult.etag,
      });

      completedItems++;
    }

    // 提交批量复制完成信息
    const commitResult = await commitBatchCopy({
      targetMountId: targetMountId,
      files: completedFiles,
    });

    return {
      success: true,
      message: "FILE_COPY_SUCCESS",
      data: {
        ...commitResult.data,
        crossStorage: true, // 标记为跨存储复制
        skipped: skippedCount,
        success: completedItems,
        failed: 0,
      },
    };
  } catch (error) {
    // 如果有正在进行的请求，尝试取消它们
    if (downloadXhr) {
      downloadXhr.abort();
    }
    if (uploadXhr) {
      uploadXhr.abort();
    }

    console.error("客户端复制流程失败:", error);
    throw error;
  }
}

/******************************************************************************
 * 高级功能API函数
 ******************************************************************************/

/**
 * 执行分片上传的完整流程
 * @param {File} file 要上传的文件
 * @param {string} path 目标路径
 * @param {Function} onProgress 进度回调函数，参数为(progress, loaded, total)
 * @param {Function} onCancel 取消检查函数，返回true时中止上传
 * @param {Function} onXhrCreated XHR创建后的回调，用于保存引用以便取消请求
 * @returns {Promise<Object>} 上传结果
 */
export async function performMultipartUpload(file, path, onProgress, onCancel, onXhrCreated) {
  const CHUNK_SIZE = 5 * 1024 * 1024; // 5MB per chunk

  let uploadId = null;
  let multipartUploader = null;
  let existingUpload = null;

  try {
    console.log(`开始分片上传: 文件=${file.name}, 大小=${file.size}, 路径=${path}`);

    // 1. 检查是否有进行中的上传（AWS S3标准流程）
    console.log("检查进行中的分片上传...");
    const uploadsResponse = await listMultipartUploads(path);

    if (uploadsResponse.success && uploadsResponse.data.uploads) {
      console.log(`S3中进行中的上传数量: ${uploadsResponse.data.uploads.length}`);

      // 查找匹配的上传（基于文件名和大小）
      const targetFileName = path.endsWith("/") ? path + file.name : path + "/" + file.name;
      const normalizedTargetPath = targetFileName.replace(/^\/+/, "");

      console.log(`查找目标文件: ${file.name}, 目标路径: ${normalizedTargetPath}`);

      // 列出所有进行中的上传，用于调试
      uploadsResponse.data.uploads.forEach((upload, index) => {
        console.log(`上传${index + 1}: key=${upload.key}, uploadId=${upload.uploadId?.substring(0, 20)}..., 创建时间=${upload.initiated}`);
      });

      // 匹配逻辑：文件名匹配
      existingUpload = uploadsResponse.data.uploads.find((upload) => {
        const keyEndsWith = upload.key.endsWith(file.name);
        const keyMatches = upload.key === normalizedTargetPath;

        console.log(`检查上传: key=${upload.key}, 文件名匹配=${keyEndsWith}, 路径匹配=${keyMatches}`);

        return keyEndsWith; // 先简化匹配逻辑，只要文件名匹配就认为是同一个文件
      });

      if (existingUpload) {
        console.log(`发现现有上传: uploadId=${existingUpload.uploadId}, key=${existingUpload.key}, 创建时间=${existingUpload.initiated}`);
        uploadId = existingUpload.uploadId;
      } else {
        console.log("没有找到匹配的现有上传");
      }
    }

    let presignedUrls;
    let uploadedParts = [];

    if (existingUpload) {
      // 2. 如果有现有上传，获取已上传的分片信息
      console.log("获取已上传的分片信息...");
      // 使用现有上传中的完整路径
      const fullPath = path.endsWith("/") ? path + file.name : path + "/" + file.name;
      console.log(`查询分片信息: path=${fullPath}, uploadId=${uploadId}, fileName=${file.name}`);
      const partsResponse = await listMultipartParts(fullPath, uploadId, file.name);

      if (partsResponse.success && partsResponse.data.parts) {
        uploadedParts = partsResponse.data.parts;
        console.log(`找到${uploadedParts.length}个已上传的分片`);
      }

      // 3. 为剩余分片生成新的预签名URL
      const totalParts = Math.ceil(file.size / CHUNK_SIZE);
      const uploadedPartNumbers = uploadedParts.map((p) => p.partNumber);
      const remainingPartNumbers = [];

      for (let i = 1; i <= totalParts; i++) {
        if (!uploadedPartNumbers.includes(i)) {
          remainingPartNumbers.push(i);
        }
      }

      console.log(`需要上传剩余分片: [${remainingPartNumbers.join(", ")}]`);

      // 检查是否还有剩余分片需要上传
      if (remainingPartNumbers.length > 0) {
        // 使用新的刷新URL API，为现有uploadId获取剩余分片的预签名URL
        console.log("为现有uploadId刷新剩余分片的预签名URL...");
        const refreshResponse = await refreshMultipartUrls(fullPath, uploadId, remainingPartNumbers);
        if (!refreshResponse.success) {
          throw new Error(refreshResponse.message || "刷新预签名URL失败");
        }

        // 将刷新的URL转换为原有格式
        presignedUrls = refreshResponse.data.presignedUrls;
        console.log(`成功为现有uploadId刷新了${presignedUrls.length}个分片的预签名URL`);
      } else {
        // 所有分片都已上传完成，直接完成上传
        console.log("所有分片都已上传完成，直接完成上传");
        const parts = uploadedParts.map((part) => ({
          PartNumber: part.partNumber,
          ETag: part.etag,
        }));

        const completeResponse = await completeMultipartUpload(path, uploadId, parts, file.name, file.size);
        if (!completeResponse.success) {
          throw new Error(completeResponse.message || "完成分片上传失败");
        }

        return {
          success: true,
          message: "文件上传完成（断点续传）",
          data: completeResponse.data,
        };
      }
    } else {
      // 4. 如果没有现有上传，创建新的上传
      console.log("创建新的分片上传...");
      const initResponse = await initMultipartUpload(path, file.name, file.size, file.type, CHUNK_SIZE);
      if (!initResponse.success) {
        throw new Error(initResponse.message || "初始化分片上传失败");
      }

      uploadId = initResponse.data.uploadId;
      presignedUrls = initResponse.data.presignedUrls;
      console.log(`创建新上传: uploadId=${uploadId}`);
    }

    // 检查是否已取消
    if (onCancel && onCancel()) {
      await abortMultipartUpload(path, uploadId, file.name);
      throw new Error("上传已取消");
    }

    // 2. 创建分片上传器
    multipartUploader = new S3MultipartUploader({
      maxConcurrentUploads: 3,
      onProgress: onProgress,
      onError: (error) => {
        console.error("分片上传错误:", error);
      },
    });

    // 设置文件内容和上传信息
    multipartUploader.setContent(file, CHUNK_SIZE);
    multipartUploader.setUploadInfo(uploadId, presignedUrls, `fs_${path}`);

    // 5. 如果有已上传的分片，恢复状态
    if (uploadedParts.length > 0) {
      console.log("恢复已上传的分片状态...");

      // 将S3的分片信息转换为S3MultipartUploader期望的格式
      const s3Parts = uploadedParts.map((part) => ({
        PartNumber: part.partNumber,
        ETag: part.etag,
        Size: part.size,
        LastModified: part.lastModified,
      }));

      // 调用S3MultipartUploader的恢复方法
      multipartUploader.restoreFromS3ListParts(s3Parts);

      console.log(`已恢复${uploadedParts.length}个分片的状态`);
    }

    // 如果提供了XHR创建回调，需要适配到分片上传器
    if (onXhrCreated) {
      // 注意：S3MultipartUploader内部管理多个XHR，这里只能提供一个引用
      // 实际的取消逻辑应该通过multipartUploader.abort()来处理
      onXhrCreated({
        abort: () => multipartUploader.abort(),
        multipartUploader: multipartUploader,
      });
    }

    // 3. 上传所有分片
    const parts = await multipartUploader.uploadAllParts();

    // 检查是否已取消
    if (onCancel && onCancel()) {
      await abortMultipartUpload(path, uploadId, file.name);
      throw new Error("上传已取消");
    }

    // 4. 完成分片上传
    const completeResponse = await completeMultipartUpload(path, uploadId, parts, file.name, file.size);
    if (!completeResponse.success) {
      throw new Error(completeResponse.message || "完成分片上传失败");
    }

    onProgress && onProgress(100);
    return completeResponse;
  } catch (error) {
    // 如果有uploadId，尝试中止上传
    if (uploadId) {
      try {
        await abortMultipartUpload(path, uploadId, file.name);
      } catch (abortError) {
        console.error("中止分片上传失败:", abortError);
      }
    }
    console.error("前端分片上传失败:", error);
    throw error;
  } finally {
    // 清理分片上传器
    if (multipartUploader) {
      multipartUploader.reset();
    }
  }
}

/**
 * 执行预签名URL上传的完整流程
 * @param {File} file 要上传的文件
 * @param {string} path 目标路径
 * @param {Function} onProgress 进度回调函数
 * @param {Function} onCancel 取消检查函数
 * @param {Function} onXhrCreated xhr创建回调函数
 * @returns {Promise<Object>} 上传结果
 */
export async function performPresignedUpload(file, path, onProgress, onCancel, onXhrCreated) {
  try {
    // 1. 获取预签名URL
    const presignResponse = await getPresignedUploadUrl(path, file.name, file.type, file.size);
    if (!presignResponse.success) {
      throw new Error(presignResponse.message || "获取预签名URL失败");
    }

    const { presignedUrl: uploadUrl, ...uploadInfo } = presignResponse.data;

    // 2. 使用 uploadWithPresignedUrl 上传到S3，支持进度和取消（使用后端推断的MIME类型）
    let uploadXhr = null;
    const uploadResult = await uploadWithPresignedUrl(uploadUrl, file, uploadInfo.contentType, onProgress, onCancel, (xhr) => {
      uploadXhr = xhr;
      // 如果提供了xhr创建回调，也调用它
      if (onXhrCreated) {
        onXhrCreated(xhr);
      }
    });

    const etag = uploadResult.etag;

    // 3. 提交上传完成
    const commitResponse = await commitPresignedUpload(uploadInfo, etag, uploadInfo.contentType, file.size);

    if (!commitResponse.success) {
      throw new Error(commitResponse.message || "提交预签名上传失败");
    }

    return commitResponse;
  } catch (error) {
    console.error("预签名URL上传失败:", error);
    throw error;
  }
}
