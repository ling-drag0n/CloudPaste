/**
 * 文件管理服务API
 * 统一管理所有文件相关的API调用，包括上传、下载、管理等
 */

import { get, post, put, del } from "../client";

/******************************************************************************
 * 文件上传相关API
 ******************************************************************************/

/**
 * 获取更准确的文件MIME类型，特别处理Markdown文件
 * @param {File} file - 文件对象
 * @returns {string} MIME类型
 */
function getAccurateMimeType(file) {
  // 如果是Markdown文件，强制设置正确的MIME类型
  if (file.name && /\.(md|markdown|mdown|mkd|mdwn|mdtxt|mdtext|rmd)$/i.test(file.name)) {
    return "text/markdown";
  }

  // 使用浏览器提供的类型，如果没有则使用通用二进制类型
  return file.type || "application/octet-stream";
}

/**
 * 上传文件（传统方式）
 * @param {File} file - 要上传的文件
 * @param {Object} options - 上传选项
 * @returns {Promise<Object>} 上传响应
 */
export async function uploadFile(file, options) {
  // 获取更准确的MIME类型
  const accurateMimeType = getAccurateMimeType(file);

  // 创建FormData对象
  const formData = new FormData();
  formData.append("file", file);

  // 添加文件MIME类型（确保后端能正确识别）
  formData.append("mimetype", accurateMimeType);

  // 添加其他选项
  if (options.s3_config_id) formData.append("s3_config_id", options.s3_config_id);
  if (options.slug) formData.append("slug", options.slug);
  if (options.path) formData.append("path", options.path);
  if (options.remark) formData.append("remark", options.remark);
  if (options.password) formData.append("password", options.password);
  if (options.expires_in) formData.append("expires_in", options.expires_in);
  if (options.max_views) formData.append("max_views", options.max_views);

  // 执行上传请求
  return await post("upload", formData, {
    headers: {
      // 不设置Content-Type，让浏览器自动设置multipart/form-data边界
      "Content-Type": undefined,
    },
  });
}

/**
 * 获取上传预签名URL
 * @param {Object} options - 上传选项
 * @param {string} options.s3_config_id - S3配置ID
 * @param {string} options.filename - 文件名
 * @param {string} options.mimetype - 文件的MIME类型
 * @param {string} [options.path] - 自定义存储路径
 * @param {string} [options.slug] - 自定义短链接
 * @param {number} [options.size] - 文件大小（字节）
 * @returns {Promise<Object>} 包含预签名URL的响应
 */
export async function getUploadPresignedUrl(options) {
  try {
    // 构建请求数据
    const data = {
      s3_config_id: options.s3_config_id,
      filename: options.filename,
      mimetype: options.mimetype,
      path: options.path,
      slug: options.slug,
      size: options.size,
      remark: options.remark,
      password: options.password,
      expires_in: options.expires_in,
      max_views: options.max_views,
      use_proxy: options.use_proxy,
    };

    return await post("s3/presign", data);
  } catch (error) {
    // 增强错误处理，特别是对409冲突状态码的处理
    if (error.message && error.message.includes("链接后缀已被占用")) {
      throw new Error("链接后缀已被占用，请尝试其他后缀");
    }

    // 将原始错误包装为更友好的错误消息
    throw new Error(`获取预签名URL失败: ${error.message}`);
  }
}

/**
 * 完成文件上传（更新文件元数据）
 * @param {Object} data - 上传完成后的元数据
 * @returns {Promise<Object>} 更新响应
 */
export async function completeFileUpload(data) {
  return await post("s3/commit", data);
}

/**
 * 直接上传文件到S3（前端直接上传）
 * @param {File} file - 要上传的文件
 * @param {Object} options - 上传选项
 * @param {Function} onProgress - 上传进度回调函数，参数为0-100的进度百分比、已加载字节数和总字节数
 * @param {Function} onXhrReady - 在XHR实例创建后的回调，用于支持取消上传
 * @param {Function} onFileIdReady - 在获取到文件ID后的回调，用于支持取消上传时清理文件记录
 * @returns {Promise<Object>} 上传响应
 */
export async function directUploadFile(file, options, onProgress, onXhrReady, onFileIdReady) {
  let fileId = null;
  try {
    // 获取更准确的MIME类型
    const accurateMimeType = getAccurateMimeType(file);

    // 步骤1: 获取预签名上传URL
    const presignedData = await getUploadPresignedUrl({
      s3_config_id: options.s3_config_id,
      filename: file.name,
      mimetype: accurateMimeType,
      path: options.path,
      slug: options.slug,
      size: file.size, // 传递文件大小供后端验证
      remark: options.remark,
      password: options.password,
      expires_in: options.expires_in,
      max_views: options.max_views,
      use_proxy: options.use_proxy,
    });

    if (!presignedData.success) {
      // 增强错误处理，处理服务器返回的特定错误消息
      if (presignedData.message) {
        // 判断是否是链接后缀冲突错误
        if (presignedData.message.includes("链接后缀已被占用")) {
          throw new Error("链接后缀已被占用，请尝试其他后缀");
        }
        // 判断是否是存储容量不足的错误
        else if (
          presignedData.message.includes("存储空间不足") ||
          presignedData.message.includes("insufficient storage") ||
          presignedData.message.includes("exceed") ||
          presignedData.message.includes("容量")
        ) {
          throw new Error(`存储空间不足: ${presignedData.message}`);
        }
        // 其他类型的错误消息
        throw new Error(presignedData.message);
      }
      throw new Error("获取预签名URL失败");
    }

    if (!presignedData.data) {
      throw new Error("获取预签名URL失败：服务器未返回有效数据");
    }

    const { upload_url, file_id, storage_path, s3_url, slug, provider_type, contentType } = presignedData.data;

    // 保存文件ID，用于错误处理时删除记录
    fileId = file_id;

    // 获取到文件ID后，调用回调函数
    if (typeof onFileIdReady === "function" && fileId) {
      onFileIdReady(fileId);
    }

    // 步骤2: 使用预签名URL直接上传文件到S3
    const uploadResult = await new Promise((resolve, reject) => {
      const xhr = new XMLHttpRequest();

      // 添加清理函数防止内存泄漏
      const cleanup = () => {
        if (xhr.upload) {
          xhr.upload.removeEventListener("progress", progressHandler);
        }
        xhr.removeEventListener("load", loadHandler);
        xhr.removeEventListener("error", errorHandler);
        xhr.removeEventListener("abort", abortHandler);
      };

      // 传递XHR实例给父组件，以支持取消上传
      if (typeof onXhrReady === "function") {
        onXhrReady(xhr);
      }

      // 定义事件处理器，便于清理
      const progressHandler = (event) => {
        if (event.lengthComputable) {
          const progress = Math.round((event.loaded / event.total) * 100);
          if (typeof onProgress === "function") {
            onProgress(progress, event.loaded, event.total);
          }
        }
      };

      const loadHandler = () => {
        cleanup();
        if (xhr.status >= 200 && xhr.status < 300) {
          // 获取ETag (S3返回的文件标识符)
          const etag = xhr.getResponseHeader("ETag");
          console.log("S3上传成功，返回的ETag:", etag); // 添加日志打印ETag
          resolve({
            success: true,
            etag: etag ? etag.replace(/"/g, "") : null, // 移除引号
          });
        } else {
          console.error("上传失败，状态码:", xhr.status, "响应:", xhr.responseText);
          reject(new Error(`上传失败: ${xhr.status} ${xhr.statusText}`));
        }
      };

      const errorHandler = (e) => {
        cleanup();
        console.error("上传错误:", e);
        reject(new Error("上传过程中发生网络错误"));
      };

      const abortHandler = () => {
        cleanup();
        reject(new Error("上传被取消"));
      };

      // 设置事件监听
      xhr.upload.addEventListener("progress", progressHandler);
      xhr.addEventListener("load", loadHandler);
      xhr.addEventListener("error", errorHandler);
      xhr.addEventListener("abort", abortHandler);

      // 配置请求
      xhr.open("PUT", upload_url, true);
      // 使用后端推断的正确MIME类型，而不是前端的 accurateMimeType
      xhr.setRequestHeader("Content-Type", contentType || accurateMimeType);

      // 根据提供商类型添加特定的请求头
      if (provider_type === "Backblaze B2") {
        // B2特定请求头
        xhr.setRequestHeader("X-Bz-Content-Sha1", "do_not_verify"); // 通知B2不验证内容SHA1
        // 尝试添加额外的请求头以帮助CORS预检请求通过
        xhr.setRequestHeader("X-Requested-With", "XMLHttpRequest");
        // xhr.setRequestHeader("Cache-Control", "no-cache");
        // xhr.setRequestHeader("X-Bz-File-Name", encodeURIComponent(file.name));
        // 日志调试
        console.log("上传到B2的URL:", upload_url);
      }

      // 发送文件数据
      xhr.send(file);
    });

    // 步骤3: 通知后端上传完成并更新文件信息
    const completeData = await completeFileUpload({
      file_id: fileId,
      s3_config_id: options.s3_config_id,
      storage_path,
      s3_url,
      filename: file.name,
      mimetype: contentType || accurateMimeType, // 使用后端推断的正确MIME类型
      size: file.size.toString(), // 确保size是字符串
      etag: uploadResult.etag,
      slug,
      remark: options.remark,
      password: options.password,
      expires_in: options.expires_in,
      max_views: options.max_views,
    });

    // 记录日志，用于调试
    console.log("文件上传完成，提交的元数据:", {
      file_id: fileId,
      size: file.size.toString(),
      etag: uploadResult.etag,
    });

    return completeData;
  } catch (error) {
    console.error("直接上传文件到S3失败:", error);

    // 如果已经创建了文件记录，但上传失败，则删除文件记录
    if (fileId) {
      console.log("上传失败，正在删除文件记录:", fileId);
      try {
        // 使用认证Store检查用户身份
        const { useAuthStore } = await import("@/stores/authStore.js");
        const authStore = useAuthStore();

        // 使用统一的批量删除API删除文件
        if (authStore.isAdmin || (authStore.authType === "apikey" && authStore.apiKey)) {
          await batchDeleteFiles([fileId]);
          console.log("已成功删除上传失败的文件记录（统一API）");
        } else {
          console.warn("未检测到有效的管理员令牌或API密钥，无法删除文件记录");
        }
      } catch (deleteError) {
        console.error("删除上传失败的文件记录错误:", deleteError);
        // 即使删除失败，我们仍然继续抛出原始错误
      }
    }

    throw error;
  }
}

/******************************************************************************
 * 统一文件管理API
 ******************************************************************************/

/**
 * 获取文件列表（统一接口，自动根据认证信息处理）
 * @param {number} limit - 每页条数
 * @param {number} offset - 偏移量
 * @param {Object} options - 额外查询选项（管理员可用）
 * @returns {Promise<Object>} 文件列表响应
 */
export async function getFiles(limit = 50, offset = 0, options = {}) {
  const params = { limit, offset, ...options };
  return await get("files", { params });
}

/**
 * 获取单个文件详情（统一接口，自动根据认证信息处理）
 * @param {string} id - 文件ID
 * @returns {Promise<Object>} 文件详情响应
 */
export async function getFile(id) {
  return await get(`files/${id}`);
}

/**
 * 更新文件元数据（统一接口，自动根据认证信息处理）
 * @param {string} id - 文件ID
 * @param {Object} metadata - 更新的文件元数据
 * @returns {Promise<Object>} 更新响应
 */
export async function updateFile(id, metadata) {
  return await put(`files/${id}`, metadata);
}

/**
 * 批量删除文件（统一接口，自动根据认证信息处理）
 * @param {Array<string>} ids - 文件ID数组
 * @param {string} deleteMode - 删除模式：'record_only' 仅删除记录，'both' 同时删除文件（默认）
 * @returns {Promise<Object>} 批量删除响应
 */
export async function batchDeleteFiles(ids, deleteMode = "both") {
  return await del(`files/batch-delete`, { ids, delete_mode: deleteMode });
}

/******************************************************************************
 * 公共文件访问API
 ******************************************************************************/

/**
 * 获取公开文件信息
 * @param {string} slug - 文件短链接
 * @returns {Promise<Object>} 文件信息响应
 */
export async function getPublicFile(slug) {
  return await get(`public/files/${slug}`);
}

/**
 * 验证文件密码
 * @param {string} slug - 文件短链接
 * @param {string} password - 文件密码
 * @returns {Promise<Object>} 验证响应
 */
export async function verifyFilePassword(slug, password) {
  return await post(`public/files/${slug}/verify`, { password });
}
