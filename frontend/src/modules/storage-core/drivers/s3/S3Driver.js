import { createCapabilities, STORAGE_STRATEGIES } from "../types.js";
import AwsS3 from "@uppy/aws-s3";
import { StorageAdapter } from "@/modules/storage-core/uppy/StorageAdapter.js";
import XHRUpload from "@uppy/xhr-upload";
import { getFullApiUrl, API_BASE_URL } from "@/api/config.js";
import { buildAuthHeadersForRequest } from "@/modules/security/index.js";
import { api } from "@/api";
import { createLogger } from "@/utils/logger.js";

const log = createLogger("S3Driver");

export class S3Driver {
  constructor(config = {}) {
    this.config = config;
    this.capabilities = createCapabilities({
      share: {
        backendStream: true,
        backendForm: true,
        presigned: true,
        url: true,
      },
      fs: {
        backendStream: true,
        backendForm: true,
        presignedSingle: true,
        multipart: true,
      },
    });

    this.share = {
      applyShareUploader: this.applyShareUploader.bind(this),
      applyUrlUploader: this.applyUrlUploader.bind(this),
      applyDirectShareUploader: this.applyDirectShareUploader.bind(this),
    };

    this.fs = {
      // 只读辅助：供断点续传插件查询进行中的上传和已上传分片
      listUploads: this.listUploads.bind(this),
      listParts: this.listParts.bind(this),
      applyFsUploader: this.applyFsUploader.bind(this),
    };
  }

  get storageConfigId() {
    return this.config?.id ?? null;
  }

  // FS --------------------------------------------------------------------
  /**
   * 在给定的 Uppy 实例上安装 FS 上传所需的 Uppy 插件
   * - presigned-single: 使用 AwsS3 + getUploadParameters（单请求 PUT 直传）
   * - presigned-multipart: 使用 AwsS3 多分片 hooks（create/sign/complete/listParts/abort）
   * @param {object} uppy 已创建的 Uppy 实例
   * @param {object} options { strategy, path }
   */
  applyFsUploader(uppy, { strategy, path } = {}) {
    if (!uppy) throw new Error("applyFsUploader 需要提供 Uppy 实例");

    // 后续如需在同一 Uppy 上切换策略，交由上层重新初始化 Uppy
    if (strategy === STORAGE_STRATEGIES.PRESIGNED_SINGLE || strategy === STORAGE_STRATEGIES.PRESIGNED_MULTIPART) {
      const isMultipart = strategy === STORAGE_STRATEGIES.PRESIGNED_MULTIPART;
      const MB = 1024 * 1024;
      // 分片大小/并发数由存储配置控制
      // - 默认分片大小 5MB
      // - 默认并发 3
      const partSizeMbRaw = Number(this.config?.multipart_part_size_mb);
      const partSizeMb = Number.isFinite(partSizeMbRaw) && partSizeMbRaw > 0 ? Math.floor(partSizeMbRaw) : 5;
      const partSizeBytes = Math.max(5 * MB, Math.min(partSizeMb * MB, 5 * 1024 * MB)); // 5MB ~ 5GB

      const concurrencyRaw = Number(this.config?.multipart_concurrency);
      const multipartConcurrency =
        Number.isFinite(concurrencyRaw) && concurrencyRaw > 0 ? Math.max(1, Math.min(Math.floor(concurrencyRaw), 10)) : 3;

      const adapter = new StorageAdapter(path || "/", uppy, {
        // per_part_url 场景：后端可能会调整 partSize（例如 S3 的 10k parts 上限），需要预初始化来拿到真实 chunkSize
        enableMultipartPreinit: isMultipart,
        ...(isMultipart ? { partSize: partSizeBytes } : {}),
      });

      // 多分片：提供完整 hooks；单请求：仅提供 getUploadParameters 并强制 shouldUseMultipart=false
      const awsS3Opts = {
        id: "AwsS3",
        limit: isMultipart ? multipartConcurrency : 3,
        shouldUseMultipart: () => isMultipart,
        getChunkSize: (data) => adapter.getChunkSizeForAwsS3(data),
      };

      if (isMultipart) {
        awsS3Opts.getUploadParameters = adapter.getUploadParameters.bind(adapter);
        awsS3Opts.createMultipartUpload = adapter.createMultipartUpload.bind(adapter);
        awsS3Opts.signPart = adapter.signPart.bind(adapter);
        // 为了兼容暂停/恢复期间的分片上传，显式提供 uploadPartBytes，避免走默认 HTTPCommunicationQueue
        awsS3Opts.uploadPartBytes = adapter.uploadPartBytes.bind(adapter);
        awsS3Opts.completeMultipartUpload = adapter.completeMultipartUpload.bind(adapter);
        awsS3Opts.abortMultipartUpload = adapter.abortMultipartUpload.bind(adapter);
        awsS3Opts.listParts = adapter.listParts.bind(adapter);
      } else {
        // 单文件上传:提供 getUploadParameters 和 uploadPartBytes
        // uploadPartBytes 使用 XMLHttpRequest 可以读取 ETag,避免 CORS 错误
        awsS3Opts.getUploadParameters = adapter.getUploadParameters.bind(adapter);
        awsS3Opts.uploadPartBytes = adapter.uploadSingleFile.bind(adapter);
      }

      uppy.use(AwsS3, awsS3Opts);
      return {
        adapter,
        mode: isMultipart ? STORAGE_STRATEGIES.PRESIGNED_MULTIPART : STORAGE_STRATEGIES.PRESIGNED_SINGLE,
      };
    }

    // S3 后端直传：通过后端网关 /fs/upload，支持流式与表单两种模式
    if (strategy === STORAGE_STRATEGIES.BACKEND_STREAM || strategy === STORAGE_STRATEGIES.BACKEND_FORM) {
      const headers = buildAuthHeadersForRequest({});

      // 统一注入 path / use_multipart 到所有文件的 meta
      try {
        uppy.setMeta({ path: path || "/", use_multipart: "false" });
      } catch {}

      if (strategy === STORAGE_STRATEGIES.BACKEND_FORM) {
        // 表单模式：沿用 multipart/form-data 行为
        uppy.use(XHRUpload, {
          id: "S3BackendForm",
          endpoint: getFullApiUrl("/fs/upload"),
          method: "POST",
          formData: true,
          fieldName: "file",
          limit: 3,
          allowedMetaFields: ["path", "use_multipart", "upload_id"],
          headers,
        });
      } else {
        // 流式模式：PUT + 原始 body，参数通过 query/header 传递
        uppy.use(XHRUpload, {
          id: "S3BackendStream",
          endpoint: (file) => {
            const meta = file.meta || {};
            const basePath = meta.path || path || "/";
            const uploadId = meta.upload_id;
            const params = new URLSearchParams();
            params.set("path", basePath);
            if (uploadId) {
              params.set("upload_id", uploadId);
            }
            return `${getFullApiUrl("/fs/upload")}?${params.toString()}`;
          },
          method: "PUT",
          formData: false,
          limit: 3,
          headers: (file) => {
            const meta = file.meta || {};
            const uploadOptions = {
              overwrite: !!meta.overwrite,
              originalFilename: !!meta.original_filename,
            };
            const rawName = meta.name || file.name || "upload-file";
            const encodedName = encodeURIComponent(rawName);
            return {
              ...headers,
              "x-fs-filename": encodedName,
              "x-fs-options": btoa(JSON.stringify(uploadOptions)),
            };
          },
        });
      }

      // 在上传前同步用户修改的文件名
      uppy.on("upload", () => {
        const files = uppy.getFiles();
        files.forEach((file) => {
          if (file.meta?.name && file.meta.name !== file.name) {
            uppy.setFileState(file.id, { name: file.meta.name });
          }
        });
      });

      return {
        adapter: null,
        mode: strategy,
      };
    }

    // 其它驱动若需要自定义实现，可在各自 Driver 层安装插件；此处默认仅处理 S3 系列策略
    return null;
  }

  /**
   * 在 Uppy 上安装用于“文件分享（本地）”的一致上传实现（一次性替换版）
   * - 统一走 AwsS3 单请求（getUploadParameters），暂停=取消、恢复=重传
   * - 在 'upload-success' 内部完成 share/commit（/share/presign + /share/commit）
   * @param {object} uppy Uppy 实例
   * @param {object} options { payload }
   */
  applyShareUploader(uppy, { payload, onShareRecord } = {}) {
    if (!uppy) throw new Error("applyShareUploader 需要提供 Uppy 实例");

    const basePayload = this.#withStorageConfig(payload || {});
    const pluginId = "AwsS3Share";

    // 避免重复安装
    if (uppy.getPlugin(pluginId)) return;

    // 创建 StorageAdapter 实例用于单文件上传(使用 XMLHttpRequest 避免 CORS)
    const adapter = new StorageAdapter("/", uppy);

    uppy.use(AwsS3, {
      id: pluginId,
      shouldUseMultipart: () => false,
      limit: 3,
      getUploadParameters: async (file) => {
        // 合并 per-file meta 覆盖（例如 slug）
        const meta = file?.meta || {};
        const fileName = typeof meta?.name === "string" && meta.name ? meta.name : file.name;
        const merged = {
          ...basePayload,
          slug: meta.slug ?? basePayload.slug,
          path: meta.path ?? basePayload.path,
        };

        const presign = await api.file.getUploadPresignedUrl({
          storage_config_id: merged.storage_config_id,
          filename: fileName,
          mimetype: file.type || "application/octet-stream",
          path: merged.path,
          size: file.size,
        });

        if (!presign?.success || !presign?.data) {
          // 服务端传输加密开启时，后端拒绝预签名直传（字节绕过后端无法加密）：
          // 自动回退为代理流式上传（/share/upload），对用户透明。
          if (presign?.code === "ENCRYPTION_REQUIRES_PROXY") {
            log.info("[ShareUploader] 服务端加密已开启，预签名被拒绝，回退为代理流式上传:", fileName);
            return await this.#proxyUploadShareFileFallback(uppy, file, { merged, meta, basePayload });
          }
          throw new Error(presign?.message || "获取预签名URL失败");
        }

        const data = presign.data;
        const uploadUrl = data.uploadUrl || data.upload_url;
        // 持久化 commit 所需元数据
        try {
          uppy.setFileMeta(file.id, {
            key: data.key,
            storage_config_id: data.storage_config_id || merged.storage_config_id,
            filename: typeof data?.filename === "string" && data.filename ? data.filename : fileName,
            path: merged.path,
            slug: merged.slug,
            password: basePayload.password || meta.password || null,
            expires_in: basePayload.expires_in,
            max_views: basePayload.max_views,
          });
        } catch {}

        return {
          method: "PUT",
          url: uploadUrl,
          headers: data.headers || {},
        };
      },
      // 使用 XMLHttpRequest 避免 CORS 问题
      uploadPartBytes: adapter.uploadSingleFile.bind(adapter),
    });

    // 在成功后执行 commit
    const onSuccess = async (file) => {
      const meta = file?.meta || {};
      // 预签名被拒后已通过代理通道完成上传并建档，无需再 commit
      if (meta.proxyUploaded) return;
      try {
        const commitRes = await api.file.completeFileUpload({
          key: meta.key,
          storage_config_id: meta.storage_config_id,
          filename:
            typeof meta?.filename === "string" && meta.filename
              ? meta.filename
              : typeof meta?.name === "string" && meta.name
                ? meta.name
                : file.name,
          size: file.size,
          etag: undefined, // ETag 可能因 CORS 不可用，后端兼容
          slug: meta.slug,
          remark: basePayload.remark,
          password: basePayload.password,
          expires_in: basePayload.expires_in,
          max_views: basePayload.max_views,
          use_proxy: basePayload.use_proxy,
          original_filename: basePayload.original_filename ?? false,
          path: meta.path,
        });
        // 暴露 fileId 以便上层需要时可读取
        if (commitRes?.data) {
          const shareRecord = commitRes.data;
          log.debug("[ShareUploader] commit result", shareRecord);
          try {
            uppy.setFileMeta(file.id, {
              fileId: shareRecord.id,
              shareRecord,
            });
          } catch {}
          try {
            uppy.emit("share-record", { file, shareRecord });
          } catch {}
          try {
            onShareRecord?.({ file, shareRecord });
          } catch {}
        }
      } catch (e) {
        // 发出错误事件以便 UI 感知
        try { uppy.emit("upload-error", file, e); } catch {}
      }
    };

    // 避免重复绑定
    uppy.on("upload-success", onSuccess);
  }

  /**
   * 预签名被拒（ENCRYPTION_REQUIRES_PROXY）时的回退：直接走 PUT /share/upload 代理流式上传
   * 复用 skipUpload 标记机制让 Uppy 将该文件标记为成功，share-record 已同步发出
   */
  async #proxyUploadShareFileFallback(uppy, file, { merged, meta, basePayload }) {
    const fileName = typeof meta?.name === "string" && meta.name ? meta.name : file.name;
    const options = {
      storage_config_id: merged.storage_config_id,
      path: merged.path,
      slug: merged.slug,
      remark: basePayload.remark,
      password: basePayload.password ?? meta.password ?? null,
      expires_in: basePayload.expires_in,
      max_views: basePayload.max_views,
      use_proxy: basePayload.use_proxy,
      original_filename: basePayload.original_filename ?? false,
    };

    const body = file?.data instanceof Blob ? file.data : null;
    if (!body) throw new Error("无法读取文件内容进行代理上传");

    const authHeaders = buildAuthHeadersForRequest({});
    const response = await new Promise((resolve, reject) => {
      try {
        const xhr = new XMLHttpRequest();
        xhr.open("PUT", getFullApiUrl("/share/upload"));
        xhr.setRequestHeader("Content-Type", file.type || "application/octet-stream");
        xhr.setRequestHeader("x-share-filename", encodeURIComponent(fileName));
        try {
          xhr.setRequestHeader("x-share-options", btoa(JSON.stringify(options)));
        } catch {}
        for (const [k, v] of Object.entries(authHeaders)) {
          try {
            xhr.setRequestHeader(k, v);
          } catch {}
        }
        xhr.onload = () => {
          try {
            resolve(JSON.parse(xhr.responseText));
          } catch {
            reject(new Error(`代理上传响应解析失败（HTTP ${xhr.status}）`));
          }
        };
        xhr.onerror = () => reject(new Error("代理上传网络错误"));
        xhr.send(body);
      } catch (e) {
        reject(e);
      }
    });

    if (!response?.success) {
      throw new Error(response?.message || "代理上传失败");
    }

    const shareRecord = response.data || {};
    try {
      uppy.setFileMeta(file.id, { proxyUploaded: true, shareRecord, fileId: shareRecord.id });
    } catch {}
    try {
      uppy.emit("share-record", { file, shareRecord });
    } catch {}

    // 复用 skipUpload 协议：跳过真实 PUT，让 Uppy 进入成功状态
    return {
      method: "PUT",
      url: `${API_BASE_URL}/__uppy_skip_upload__`,
      headers: { "x-cloudpaste-skip-upload": "1" },
    };
  }

  /**
   * Share 直传上传：通过 Uppy + XHRUpload 调用后端 /share/upload（ObjectStore 多存储通用）
   * S3 在这一模式下与 WebDAV 一致，由后端统一处理写入与建档。
   */
  applyDirectShareUploader(uppy, { payload, onShareRecord, shareMode } = {}) {
    if (!uppy) throw new Error("applyDirectShareUploader 需要提供 Uppy 实例");

    const basePayload = this.#withStorageConfig(payload || {});
    const mode = (shareMode || "stream").toLowerCase();

    const baseMeta = {
      storage_config_id: basePayload.storage_config_id,
      path: basePayload.path || "",
      slug: basePayload.slug || "",
      remark: basePayload.remark || "",
      password: basePayload.password || "",
      expires_in: basePayload.expires_in || "0",
      max_views: basePayload.max_views ?? 0,
      use_proxy: basePayload.use_proxy,
      original_filename: basePayload.original_filename,
    };

    const authHeaders = buildAuthHeadersForRequest({});
    try {
      uppy.setMeta(baseMeta);
    } catch {}

    if (mode === "stream") {
      // 流式分享：PUT /share/upload + 原始 body，参数通过头部传递
      uppy.use(XHRUpload, {
        id: "S3ShareUploadStream",
        endpoint: getFullApiUrl("/share/upload"),
        method: "PUT",
        formData: false,
        limit: 3,
        headers: (file) => {
          const meta = file.meta || {};
          const options = {
            storage_config_id: meta.storage_config_id,
            path: meta.path,
            slug: meta.slug,
            remark: meta.remark,
            password: meta.password,
            expires_in: meta.expires_in,
            max_views: meta.max_views,
            use_proxy: meta.use_proxy,
            original_filename: meta.original_filename,
            upload_id: meta.upload_id,
          };
          let encodedOptions = "";
          try {
            encodedOptions = btoa(JSON.stringify(options));
          } catch {
            encodedOptions = "";
          }
          const rawName = meta.name || file.name || "upload-file";
          const encodedName = encodeURIComponent(rawName);
          return {
            ...authHeaders,
            "x-share-filename": encodedName,
            ...(encodedOptions ? { "x-share-options": encodedOptions } : {}),
          };
        },
      });
    } else {
      // 表单分享：POST /share/upload multipart/form-data
      uppy.use(XHRUpload, {
        id: "S3ShareUploadDirect",
        endpoint: getFullApiUrl("/share/upload"),
        method: "POST",
        formData: true,
        fieldName: "file",
        limit: 3,
        allowedMetaFields: [
          "storage_config_id",
          "path",
          "slug",
          "remark",
          "password",
          "expires_in",
          "max_views",
          "use_proxy",
          "original_filename",
          "upload_id",
        ],
        headers: authHeaders,
      });
    }

    // 在上传前同步用户修改的文件名
    uppy.on("upload", () => {
      const files = uppy.getFiles();
      files.forEach((file) => {
        if (file.meta?.name && file.meta.name !== file.name) {
          uppy.setFileState(file.id, { name: file.meta.name });
        }
      });
    });

    const onSuccess = (file, response) => {
      try {
        const body = response && (response.body || response);
        if (!body || body.success !== true) return;
        const shareRecord = body.data;
        if (!shareRecord) return;

        try {
          uppy.setFileMeta(file.id, {
            ...(file.meta || {}),
            shareRecord,
            fileId: shareRecord.id,
          });
        } catch {}

        try {
          uppy.emit("share-record", { file, shareRecord });
        } catch {}

        try {
          onShareRecord?.({ file, shareRecord });
        } catch {}
      } catch {}
    };

    uppy.on("upload-success", onSuccess);
  }

  /**
   * URL 分享：使用通用 share/presign + AwsS3 单请求 + 在 'upload-success' 提交 share/commit
   */
  applyUrlUploader(uppy, { payload, onShareRecord } = {}) {
    if (!uppy) throw new Error("applyUrlUploader 需要提供 Uppy 实例");
    const basePayload = this.#withStorageConfig(payload || {});
    const pluginId = "AwsS3UrlShare";
    if (uppy.getPlugin(pluginId)) return;

    // 创建 StorageAdapter 实例用于单文件上传(使用 XMLHttpRequest 避免 CORS)
    const adapter = new StorageAdapter("/", uppy);

    uppy.use(AwsS3, {
      id: pluginId,
      shouldUseMultipart: () => false,
      limit: 3,
      getUploadParameters: async (file) => {
        const meta = file?.meta || {};
        const fileName = typeof meta?.name === "string" && meta.name ? meta.name : file.name;
        const merged = {
          ...basePayload,
          slug: meta.slug ?? basePayload.slug,
          path: meta.path ?? basePayload.path,
        };
        const presign = await api.file.getUploadPresignedUrl({
          storage_config_id: merged.storage_config_id,
          filename: fileName,
          mimetype: file.type || "application/octet-stream",
          path: merged.path,
          size: file.size,
        });
        if (!presign?.success || !presign?.data) throw new Error(presign?.message || "获取URL上传预签名失败");
        const presignData = presign.data;
        const uploadUrl = presignData.uploadUrl || presignData.upload_url;
        const resolvedKey = presignData.key;
        const resolvedStorageConfigId = presignData.storage_config_id || merged.storage_config_id;
        const resolvedFilename =
          typeof presignData?.filename === "string" && presignData.filename ? presignData.filename : fileName;
        if (!uploadUrl || !resolvedKey || !resolvedStorageConfigId) {
          throw new Error("URL上传预签名缺少必要的 key 或上传地址");
        }
        try {
          uppy.setFileMeta(file.id, {
            key: resolvedKey,
            storage_config_id: resolvedStorageConfigId,
            filename: resolvedFilename,
            path: merged.path,
            slug: merged.slug,
            remark: merged.remark,
            password: merged.password,
            expires_in: merged.expires_in,
            max_views: merged.max_views,
          });
        } catch {}
        return { method: "PUT", url: uploadUrl, headers: presignData.headers || {} };
      },
      // 使用 XMLHttpRequest 避免 CORS 问题
      uploadPartBytes: adapter.uploadSingleFile.bind(adapter),
    });

    const onSuccess = async (file) => {
      const meta = file?.meta || {};
      try {
        const commitRes = await api.file.completeFileUpload({
          key: meta.key,
          storage_config_id: meta.storage_config_id,
          filename:
            typeof meta?.filename === "string" && meta.filename
              ? meta.filename
              : typeof meta?.name === "string" && meta.name
                ? meta.name
                : file.name,
          size: file.size,
          etag: undefined,
          path: meta.path,
          slug: meta.slug,
          remark: meta.remark ?? basePayload.remark,
          password: meta.password ?? basePayload.password,
          expires_in: meta.expires_in ?? basePayload.expires_in,
          max_views: meta.max_views ?? basePayload.max_views,
        });
        if (commitRes?.data) {
          const shareRecord = commitRes.data;
          log.debug("[UrlShareUploader] commit result", shareRecord);
          try {
            uppy.setFileMeta(file.id, {
              fileId: shareRecord.id,
              shareRecord,
            });
          } catch {}
          try {
            uppy.emit("share-record", { file, shareRecord });
          } catch {}
          try {
            onShareRecord?.({ file, shareRecord });
          } catch {}
        }
      } catch (e) {
        try { uppy.emit('upload-error', file, e); } catch {}
      }
    };
    uppy.on('upload-success', onSuccess);
  }

  // Read-only helpers for resume plugin -------------------------------------
  async listUploads({ path } = {}) {
    // 后端接受目录或空字符串，返回该作用域下的未完成上传
    return api.fs.listMultipartUploads(path || "");
  }

  async listParts({ path, uploadId, fileName }) {
    // path 为文件完整路径，后端使用它定位资源；同时需要 uploadId 和 fileName
    return api.fs.listMultipartParts(path, uploadId, fileName);
  }

  // Helpers ---------------------------------------------------------------
  #withStorageConfig(payload) {
    if (payload?.storage_config_id) {
      return payload;
    }
    if (!this.storageConfigId) {
      throw new Error("缺少 storage_config_id，可在 payload 中指定或配置默认值");
    }
    return { ...payload, storage_config_id: this.storageConfigId };
  }
}

export const SUPPORTED_STRATEGIES = [
  STORAGE_STRATEGIES.BACKEND_STREAM,
  STORAGE_STRATEGIES.PRESIGNED_SINGLE,
  STORAGE_STRATEGIES.PRESIGNED_MULTIPART,
];
