/**
 * 文件系统服务API
 */

import { get, post, del } from "../client";
// Legacy StorageMultipartUploader removed. Multipart flows are handled by Uppy AwsS3 plugin or driver engine.

/******************************************************************************
 * 统一文件系统API函数
 ******************************************************************************/

/**
 * 获取目录列表
 * @param {string} path 请求路径
 * @param {Object} options 选项参数
 * @param {boolean} options.refresh 是否强制刷新，跳过缓存
 * @param {AbortSignal} options.signal 用于取消请求的 AbortSignal
 * @returns {Promise<Object>} 目录列表响应对象
 */
export async function getDirectoryList(path, options = {}) {
  const params = { path };
  if (options.refresh) {
    params.refresh = "true";
  }
  if (options.cursor) {
    params.cursor = String(options.cursor);
  }
  if (options.limit) {
    params.limit = String(options.limit);
  }
  // 允许前端显式指定是否启用分页模式
  if (options.paged) {
    params.paged = "true";
  }
  const requestOptions = { params };
  if (options.headers) {
    requestOptions.headers = options.headers;
  }
  // 支持请求取消
  if (options.signal) {
    requestOptions.signal = options.signal;
  }
  return get("/fs/list", requestOptions);
}

/**
 * 获取文件信息
 * @param {string} path 文件路径
 * @param {{ headers?: Record<string,string>, signal?: AbortSignal }} [options] 可选请求配置（如路径密码token、取消信号等）
 * @returns {Promise<Object>} 文件信息响应对象
 */
export async function getFileInfo(path, options = {}) {
  /** @type {{ params: Record<string,string>, headers?: Record<string,string>, signal?: AbortSignal }} */
  const requestOptions = {
    params: { path },
  };
  if (options.headers) {
    requestOptions.headers = options.headers;
  }
  // 支持请求取消
  if (options.signal) {
    requestOptions.signal = options.signal;
  }
  return get("/fs/get", requestOptions);
}

/**
 * 搜索文件
 * @param {string} query 搜索查询字符串
 * @param {Object} searchParams 搜索参数对象
 * @param {string} searchParams.scope 搜索范围 ('global', 'mount', 'directory')
 * @param {string} searchParams.mountId 挂载点ID（当scope为'mount'时）
 * @param {string} searchParams.path 搜索路径（当scope为'directory'时）
 * @param {number} searchParams.limit 结果限制数量，默认50
 * @param {string|null} searchParams.cursor 分页游标（不透明字符串），默认null
 * @param {string} searchParams.pathToken 路径密码 token（可选）
 * @param {string[]} searchParams.pathTokens 路径密码 token 列表（可选）
 * @returns {Promise<Object>} 搜索结果响应对象
 */
export async function searchFiles(query, searchParams = {}) {
  const params = {
    q: query,
    scope: searchParams.scope || "global",
    limit: (searchParams.limit || 50).toString(),
  };

  // 添加可选参数
  if (searchParams.mountId) {
    params.mount_id = searchParams.mountId;
  }
  if (searchParams.path) {
    params.path = searchParams.path;
  }
  if (searchParams.cursor) {
    params.cursor = String(searchParams.cursor);
  }

  const headers = {};
  if (searchParams.pathToken) {
    headers["x-fs-path-token"] = searchParams.pathToken;
  }
  if (Array.isArray(searchParams.pathTokens) && searchParams.pathTokens.length > 0) {
    headers["x-fs-path-tokens"] = searchParams.pathTokens.join(",");
  }

  const requestOptions = { params };
  if (Object.keys(headers).length > 0) {
    requestOptions.headers = headers;
  }

  return get("/fs/search", requestOptions);
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
 * 上传文件（通过 /fs/upload，后端根据存储驱动自适应选择流式/表单实现）
 * @param {string} path 目标路径
 * @param {File} file 文件对象
 * @param {Function} onXhrCreated XHR创建后的回调，用于保存引用以便取消请求
 * @returns {Promise<Object>} 上传结果响应对象
 */
export async function uploadFile(path, file, onXhrCreated) {
  const formData = new FormData();
  formData.append("file", file);
  formData.append("path", path);

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
 * @param {number|null} expiresIn 过期时间（秒），null表示使用存储配置的默认签名时间
 * @param {boolean} forceDownload 是否强制下载而非预览
 * @param {{ headers?: Record<string,string> }} [options] 可选请求配置（例如路径密码 token）
 * @returns {Promise<string>} 预签名访问 URL（可能是直链或代理 URL）
 */
export async function getFileLink(path, expiresIn = null, forceDownload = false, options = {}) {
  const params = {
    path: path,
    force_download: forceDownload.toString(),
  };

  // 只有当expiresIn不为null时才添加expires_in参数
  if (expiresIn !== null) {
    params.expires_in = expiresIn.toString();
  }

  /** @type {{ params: Record<string,string>, headers?: Record<string,string> }} */
  const requestOptions = { params };
  if (options.headers) {
    requestOptions.headers = options.headers;
  }

  const resp = await get("/fs/file-link", requestOptions);
  if (!resp || resp.success === false) {
    throw new Error(resp?.message || "获取文件直链失败");
  }
  const url = resp?.data?.url;
  if (!url) {
    throw new Error(resp?.message || "获取文件直链失败");
  }
  return url;
}

/**
 * 校验目录路径密码
 * @param {string} path 目标路径
 * @param {string} password 明文密码
 * @returns {Promise<{ success: boolean; requiresPassword: boolean; verified: boolean; token?: string | null; path?: string }>}
 */
export async function verifyPathPassword(path, password) {
  const response = await post("/fs/meta/password/verify", { path, password });
  if (response && typeof response === "object" && "data" in response) {
    return /** @type {{ verified: boolean; requiresPassword: boolean; token?: string | null; path?: string }} */ (
      response.data
    );
  }
  return response;
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
export async function initMultipartUpload(path, fileName, fileSize, contentType, partSize = 5 * 1024 * 1024, extra = {}) {
  const partCount = Math.ceil(fileSize / partSize);

  const sha256 = extra?.sha256 || extra?.oid || null;

  return post(`/fs/multipart/init`, {
    path,
    fileName,
    fileSize,
    partSize,
    partCount,
    ...(contentType ? { contentType } : {}),
    ...(sha256 ? { sha256 } : {}),
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
 * 获取/刷新分片上传参数（统一入口）
 * @param {string} path 文件路径
 * @param {string} uploadId 现有的上传ID
 * @param {Array} partNumbers 需要签名的分片编号数组：
 * @returns {Promise<Object>} 分片上传参数响应对象
 */
export async function signMultipartParts(path, uploadId, partNumbers) {
  return post(`/fs/multipart/sign-parts`, { path, uploadId, partNumbers });
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
 * @param {string} [sha256] 文件 SHA-256（hex）。
 * @returns {Promise<Object>} 预签名URL响应对象
 */
export async function getPresignedUploadUrl(path, fileName, contentType, fileSize, sha256 = null) {
  return post(`/fs/presign`, {
    path,
    fileName,
    contentType,
    fileSize,
    sha256,
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
 * 批量复制文件或目录（统一任务模式）
 *
 * 所有复制操作统一创建任务，由后端 CopyTaskHandler 负责执行。
 * 复制策略（同存储/跨存储/S3优化）由后端内部决策。
 *
 * @param {Array<{sourcePath: string, targetPath: string}>} items 要复制的项目数组，每项包含源路径和目标路径
 * @param {Object} options 选项参数
 * @param {boolean} [options.skipExisting=true] 是否跳过已存在的文件
 * @returns {Promise<Object>} 批量复制结果响应对象 { success, data: { jobId, taskType, status, stats, createdAt } }
 */
export async function batchCopyItems(items, options = {}) {
  // 复制策略由后端 CopyTaskHandler 内部决策
  return createJob("copy", { items, options });
}

/**
 * 从文件系统创建分享链接（支持文件与文件夹）
 * @param {string} path 目标 FS 路径（文件或文件夹）
 * @param {Object} [options] 分享选项
 * @param {string} [options.password] 访问密码，空表示无密码
 * @param {number|string} [options.expires_in] 有效期（小时），0 表示永久
 * @param {number|string} [options.max_views] 最大下载次数，0 表示不限
 * @param {string} [options.remark] 备注
 * @param {string} [options.slug] 自定义 slug
 * @returns {Promise<Object>} 创建结果响应对象
 */
export async function createShareFromFileSystem(path, options = {}) {
  const body = { path };
  if (options.password) body.password = options.password;
  if (options.expires_in !== undefined && options.expires_in !== null) body.expires_in = options.expires_in;
  if (options.max_views !== undefined && options.max_views !== null) body.max_views = options.max_views;
  if (options.remark) body.remark = options.remark;
  if (options.slug) body.slug = options.slug;
  if (options.encryption_meta) body.encryption_meta = options.encryption_meta;
  return post(`/fs/create-share`, body);
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
        clearTimeout(cancelChecker);
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
        clearTimeout(cancelChecker);
      }
      reject(new Error("上传过程中发生网络错误"));
    };

    xhr.onabort = function () {
      if (cancelChecker) {
        clearTimeout(cancelChecker);
      }
      reject(new Error("上传已取消"));
    };

    // 定期检查取消状态
    let cancelChecker = null;
    if (onCancel) {
      const checkCancel = () => {
        if (onCancel()) {
          if (cancelChecker) {
            clearTimeout(cancelChecker);
            cancelChecker = null;
          }
          xhr.abort();
          return;
        }
        cancelChecker = setTimeout(checkCancel, 100);
      };
      cancelChecker = setTimeout(checkCancel, 100);
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




/******************************************************************************
 * 通用作业（Generic Jobs）API函数
 ******************************************************************************/

/**
 * 创建通用作业（支持多种任务类型的异步后台处理）
 * @param {string} taskType 任务类型（'copy', 'scheduled-sync', 'cleanup' 等）
 * @param {Object} payload 任务载荷（由具体任务类型决定）
 * @param {Object} options 选项参数
 * @param {boolean} [options.skipExisting=true] 是否跳过已存在的文件（适用于复制任务）
 * @param {number} [options.maxConcurrency=10] 最大并发数
 * @param {Object} [options.retryPolicy] 重试策略
 * @returns {Promise<Object>} 作业描述符 { jobId, taskType, status, stats, createdAt }
 */
export async function createJob(taskType, payload, options = {}) {
  const type = String(taskType || "").trim();
  if (!type) {
    throw new Error("taskType 不能为空");
  }

  if (type === "copy") {
    const items = Array.isArray(payload) ? payload : (payload?.items ?? payload);
    const payloadOptions = (!Array.isArray(payload) && payload && typeof payload === "object")
      ? (payload.options || {})
      : {};
    const mergedOptions = { ...payloadOptions, ...options };

    if (!Array.isArray(items) || items.length === 0) {
      throw new Error("copy 任务的 items 不能为空");
    }
    return post("/fs/jobs", {
      taskType: type,
      payload: {
        items,
        options: {
          skipExisting: mergedOptions.skipExisting !== false,
          maxConcurrency: mergedOptions.maxConcurrency || 10,
          retryPolicy: mergedOptions.retryPolicy,
        },
      },
    });
  }

  return post("/fs/jobs", {
    taskType: type,
    payload: payload || {},
  });
}

/**
 * 获取作业状态
 * @param {string} jobId 作业ID
 * @returns {Promise<Object>} 作业状态 { jobId, taskType, status, stats, createdAt, startedAt?, finishedAt?, errorMessage? }
 */
export async function getJobStatus(jobId) {
  return get(`/fs/jobs/${jobId}`);
}

/**
 * 取消作业
 * @param {string} jobId 作业ID
 * @returns {Promise<Object>} 取消结果
 */
export async function cancelJob(jobId) {
  return post(`/fs/jobs/${jobId}/cancel`);
}

/**
 * 列出作业
 * @param {Object} filter 过滤条件
 * @param {string} [filter.taskType] 任务类型（'copy', 'scheduled-sync' 等）
 * @param {string} [filter.status] 作业状态（pending/running/completed/partial/failed/cancelled）
 * @param {number} [filter.limit=20] 返回数量限制
 * @param {number} [filter.offset=0] 偏移量
 * @returns {Promise<Object>} 作业列表 { jobs, total, limit, offset }
 */
export async function listJobs(filter = {}) {
  const params = {
    limit: (filter.limit || 20).toString(),
    offset: (filter.offset || 0).toString(),
  };

  if (filter.taskType) {
    params.taskType = filter.taskType;
  }

  if (filter.status) {
    params.status = filter.status;
  }

  return get('/fs/jobs', { params });
}

/**
 * 获取当前用户可见的任务类型清单（用于任务管理 UI 做筛选/展示名）
 * @returns {Promise<Object>} { types: Array<{ taskType, i18nKey?, displayName?, category?, capabilities? }> }
 */
export async function listJobTypes() {
  return get("/fs/job-types");
}

/**
 * 删除作业
 * @param {string} jobId 作业ID
 * @returns {Promise<Object>} 删除结果
 */
export async function deleteJob(jobId) {
  return del(`/fs/jobs/${jobId}`);
}
