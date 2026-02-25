/**
 * WebDAV 存储驱动
 * 默认支持 Reader/Writer/Proxy/Atomic 能力，不提供存储直链（DirectLink）
 */

import { BaseDriver } from "../../interfaces/capabilities/BaseDriver.js";
/**
 * 模块说明：
 * - 作用：WebDAV 驱动，负责目录/文件的读写、重命名/复制、搜索、代理 URL 生成等。
 * - 能力：声明 READER/WRITER/ATOMIC/PROXY/SEARCH，供 FS/features 按能力路由。
 * - 约定：路径规范化与错误映射封装在 _normalize/_buildDavPath/_wrapError 中，外层无需关心 webdav 客户端细节。
 */
import { CAPABILITIES } from "../../interfaces/capabilities/index.js";
import { ApiStatus, FILE_TYPES } from "../../../constants/index.js";
import { DriverError, NotFoundError, AppError } from "../../../http/errors.js";
import { decryptValue } from "../../../utils/crypto.js";
import { getMimeTypeFromFilename } from "../../../utils/fileUtils.js";
import { buildFullProxyUrl } from "../../../constants/proxy.js";
import { createClient } from "webdav";
import { Buffer } from "buffer";
import https from "https";
import { updateUploadProgress, completeUploadProgress } from "../../utils/UploadProgressTracker.js";
import { isNodeJSEnvironment } from "../../../utils/environmentUtils.js";
import { buildFileInfo } from "../../utils/FileInfoBuilder.js";
import { createHttpStreamDescriptor } from "../../streaming/StreamDescriptorUtils.js";

export class WebDavStorageDriver extends BaseDriver {
  constructor(config, encryptionSecret) {
    super(config);
    this.type = "WEBDAV";
    this.encryptionSecret = encryptionSecret;
    this.capabilities = [CAPABILITIES.READER, CAPABILITIES.WRITER, CAPABILITIES.ATOMIC, CAPABILITIES.PROXY];
    this.client = null;
    this.defaultFolder = config.default_folder || "";
    this.endpoint = config.endpoint_url || "";
    this.username = config.username || "";
    this.passwordEncrypted = config.password || "";
    this.urlProxy = config.url_proxy || null;
    this.tlsSkipVerify = config?.tls_insecure_skip_verify === 1;
    this.enableDiskUsage = config?.enable_disk_usage === 1;
  }

  /**
   * 规范化 WebDAV endpoint_url：
   * - 必须是合法 http(s) URL
   * - pathname 确保以 / 结尾，避免某些 URL join 规则把最后一段当成“文件名”导致拼接错误
   * @param {unknown} value
   * @returns {string}
   */
  _normalizeClientEndpointUrl(value) {
    const raw = value == null ? "" : String(value).trim();
    if (!raw) return "";
    let url;
    try {
      url = new URL(raw);
    } catch {
      throw new DriverError("WebDAV endpoint_url 不是合法的 URL", { status: ApiStatus.BAD_REQUEST, expose: true });
    }
    if (url.protocol !== "http:" && url.protocol !== "https:") {
      throw new DriverError("WebDAV endpoint_url 必须以 http:// 或 https:// 开头", { status: ApiStatus.BAD_REQUEST, expose: true });
    }
    if (!url.pathname.endsWith("/")) {
      url.pathname = `${url.pathname}/`;
    }
    return url.toString();
  }

  /**
   * 初始化 WebDAV 客户端
   */
  async initialize() {
    try {
      const password = await decryptValue(this.passwordEncrypted, this.encryptionSecret);
      if (!password) {
        throw new DriverError("WebDAV 凭据不可用", { status: ApiStatus.FORBIDDEN });
      }
      const endpointForClient = this._normalizeClientEndpointUrl(this.endpoint);
      if (!endpointForClient) {
        throw new DriverError("WebDAV 配置缺少 endpoint_url", { status: ApiStatus.BAD_REQUEST, expose: true });
      }

      const agent =
        endpointForClient.startsWith("https://") && this.tlsSkipVerify ? new https.Agent({ rejectUnauthorized: false }) : undefined;
      const clientOptions = agent ? { httpsAgent: agent } : {};
      const authHeader = this._basicAuthHeader(password);
      this.client = createClient(endpointForClient, {
        headers: {
          Authorization: authHeader,
        },
        ...clientOptions,
      });
      this.initialized = true;
      this.decryptedPassword = password;
      console.log(`WebDAV 驱动初始化完成: ${endpointForClient}`);
    } catch (error) {
      console.error("WebDAV 驱动初始化失败", error);
      throw this._wrapError(error, "WebDAV 驱动初始化失败", ApiStatus.INTERNAL_ERROR);
    }
  }

  /**
   * 目录列表
   */
  async listDirectory(subPath, ctx = {}) {
    this._ensureInitialized();
    const { mount, path, refresh = false, db } = ctx;
    const davPath = this._buildDavPath(subPath, true);
    try {
      const entries = await this.client.getDirectoryContents(davPath, { deep: false, glob: "*" });
      const basePath = path;
      const items = await Promise.all(
        entries.map(async (item) => {
          const isDirectory = item.type === "directory";
          const rawName = item.basename || this._basename(item.filename);
          const name = this._decodeComponent(rawName);
          const mountPath = this._joinMountPath(basePath, name, isDirectory);

          // 无 MIME 时根据文件名推断
          const rawMime = item.mime || null;
          let mimetype = null;
          if (isDirectory) {
            mimetype = "application/x-directory";
          } else if (rawMime && rawMime !== "httpd/unix-directory") {
            mimetype = rawMime;
          } else {
            // WebDAV 服务器未返回有效 MIME，根据文件名推断
            mimetype = getMimeTypeFromFilename(name);
          }

          let size = null;
          let modifiedDate = null;

          if (isDirectory) {
            // 目录：大小通常无法直接给出；modified 只有上游提供时才使用，否则为 null（显示 “-”）
            size = null;
            if (item.lastmod) {
              modifiedDate = new Date(item.lastmod);
            }
          } else {
            // 默认使用目录列表中的 size
            size = typeof item.size === "number" && Number.isFinite(item.size) && item.size >= 0 ? item.size : null;
            if (item.lastmod) {
              modifiedDate = new Date(item.lastmod);
            }

            // 部分 WebDAV 服务在目录列表中返回错误 size（如恒为 2），
            // 仅在明显异常时再发起一次 stat 精准获取大小，避免每个文件都额外请求。
            if (!Number.isFinite(size) || size <= 2) {
              try {
                const rel = subPath
                  ? `${subPath.replace(/^\\\\\+/, "")}/${rawName}`
                  : rawName;
                const fileDavPath = this._buildDavPath(rel, false);
                const stat = await this.client.stat(fileDavPath);
                if (stat && typeof stat.size === "number" && stat.size >= 0) {
                  size = stat.size;
                }
                if (stat?.lastmod) {
                  modifiedDate = new Date(stat.lastmod);
                }
              } catch {
                // stat 失败时保持原始 size，交由上层决定如何展示
              }
            }
          }

          const info = await buildFileInfo({
            fsPath: mountPath,
            name,
            isDirectory,
            size,
            modified: modifiedDate,
            mimetype,
            mount,
            storageType: mount?.storage_type,
            db,
          });

          return {
            ...info,
            isVirtual: false,
          };
        })
      );

      return {
        path,
        type: "directory",
        isRoot: subPath === "" || subPath === "/",
        isVirtual: false,
        mount_id: mount?.id,
        storage_type: mount?.storage_type,
        items,
      };
    } catch (error) {
      throw this._wrapError(error, "列出目录失败", this._statusFromError(error));
    }
  }

  /**
   * 文件信息
   */
  async getFileInfo(subPath, ctx = {}) {
    this._ensureInitialized();
    const { mount, path, db, request = null, userType = null, userId = null } = ctx;
    const davPath = this._buildDavPath(subPath, false);
    try {
      const stat = await this.client.stat(davPath);
      const isDirectory = stat.type === "directory";
      const name = this._decodeComponent(this._basename(path));
      const rawMime = stat.mime || null;
      // 处理 WebDAV 常见的错误 MIME 类型，并在无 MIME 时根据文件名推断
      let effectiveMime = null;
      if (isDirectory) {
        effectiveMime = "application/x-directory";
      } else if (rawMime && rawMime !== "httpd/unix-directory") {
        effectiveMime = rawMime;
      } else {
        // WebDAV 服务器未返回有效 MIME，根据文件名推断
        effectiveMime = getMimeTypeFromFilename(name);
      }

      const size =
        isDirectory ? null : typeof stat.size === "number" && Number.isFinite(stat.size) && stat.size >= 0 ? stat.size : null;
      const modifiedDate = stat.lastmod ? new Date(stat.lastmod) : null;

      const info = await buildFileInfo({
        fsPath: path,
        name,
        isDirectory,
        size,
        modified: modifiedDate,
        mimetype: effectiveMime,
        mount,
        storageType: mount?.storage_type,
        db,
      });

      return {
        ...info,
        etag: stat.etag || undefined,
      };
    } catch (error) {
      if (this._isNotFound(error)) {
        throw new NotFoundError("文件不存在");
      }
      throw this._wrapError(error, "获取文件信息失败", this._statusFromError(error));
    }
  }

  /**
   * 下载文件（返回 StorageStreamDescriptor）
   * @param {string} subPath - 挂载内子路径
   * @param {Object} ctx - 上下文（path/mount/subPath 等）
   * @returns {Promise<import('../../streaming/types.js').StorageStreamDescriptor>} 流描述对象
   */
  async downloadFile(subPath, ctx = {}) {
    this._ensureInitialized();
    const { path } = ctx;
    const davPath = this._buildDavPath(subPath, false);
    const url = this._buildRequestUrl(davPath);

    // 先获取文件元数据（HEAD 请求）
    let metadata;
    try {
      const headResp = await fetch(url, {
        method: "HEAD",
        headers: { Authorization: this._basicAuthHeader() },
      });
      if (!headResp.ok) {
        if (headResp.status === 404) {
          throw new NotFoundError("文件不存在");
        }
        throw this._wrapError(new Error(`HTTP ${headResp.status}`), "获取文件元数据失败", headResp.status);
      }
        metadata = {
          contentType: headResp.headers.get("content-type") || "application/octet-stream",
          contentLength: headResp.headers.get("content-length"),
          etag: headResp.headers.get("etag"),
          lastModified: headResp.headers.get("last-modified"),
        };
    } catch (error) {
      if (error instanceof AppError) {
        throw error;
      }
      throw this._wrapError(error, "获取文件元数据失败", this._statusFromError(error));
    }

    // 优先使用 HEAD 的 Content-Length；部分 WebDAV 服务不会返回该头，需降级为 stat
    let size = metadata.contentLength ? parseInt(metadata.contentLength, 10) : null;
    let contentType = metadata.contentType;
    let etag = metadata.etag || null;
    let lastModified = metadata.lastModified ? new Date(metadata.lastModified) : null;

    // 当 HEAD 未返回 Content-Length 或返回值明显异常时，尝试通过 WebDAV stat 精准获取文件大小
    if (size === null || !Number.isFinite(size) || size <= 0) {
      try {
        const stat = await this.client.stat(davPath);

        if (stat && typeof stat.size === "number" && stat.size >= 0) {
          size = stat.size;
        }

        // 仅在 HEAD 未提供对应元数据时，使用 stat 结果进行补全，避免覆盖上游更准确的 HTTP 头信息
        if (!contentType) {
          const rawMime = stat.mime || null;
          if (rawMime && rawMime !== "httpd/unix-directory") {
            contentType = rawMime;
          }
        }
        if (!etag && stat.etag) {
          etag = stat.etag;
        }
        if (!lastModified && stat.lastmod) {
          lastModified = new Date(stat.lastmod);
        }
      } catch (error) {
        // stat 404 统一视为文件不存在，其余错误仅记录，保持 HEAD 信息，由上层决定是否降级为 200
        if (this._isNotFound(error)) {
          throw new NotFoundError("文件不存在");
        }
      }
    }

    // 保存 url 和 auth 供闭包使用
    const fileUrl = url;
    const authHeader = this._basicAuthHeader();
    const wrapError = this._wrapError.bind(this);
    const statusFromError = this._statusFromError.bind(this);
    // 仅对 Range 请求显式关闭缓存
    const workerNoCacheOptionsForRange = typeof caches !== "undefined" ? { cf: { cacheEverything: false } } : {};

    const descriptor = createHttpStreamDescriptor({
      size,
      contentType,
      etag,
      lastModified,
      supportsRange: true,
      async fetchResponse(signal) {
        const resp = await fetch(fileUrl, {
          headers: { Authorization: authHeader },
          signal,
        });
        if (!resp.ok) {
          if (resp.status === 404) {
            throw new NotFoundError("文件不存在");
          }
          throw wrapError(new Error(`HTTP ${resp.status}`), "下载失败", resp.status);
        }
        return resp;
      },
      async fetchRangeResponse(signal, rangeHeader) {
        const resp = await fetch(fileUrl, {
          headers: {
            Authorization: authHeader,
            Range: rangeHeader,
            "Accept-Encoding": "identity",
            "Cache-Control": "no-store, no-transform",
            Pragma: "no-cache",
          },
          signal,
          cache: "no-store",
          ...workerNoCacheOptionsForRange,
        });
        if (!resp.ok) {
          if (resp.status === 404) {
            throw new NotFoundError("文件不存在");
          }
          throw wrapError(new Error(`HTTP ${resp.status}`), "下载失败", resp.status);
        }
        return resp;
      },
      async fetchHeadResponse(signal) {
        const resp = await fetch(fileUrl, {
          method: "HEAD",
          headers: { Authorization: authHeader },
          signal,
        });
        if (!resp.ok) {
          if (resp.status === 404) {
            throw new NotFoundError("文件不存在");
          }
          throw wrapError(new Error(`HTTP ${resp.status}`), "获取文件元数据失败", resp.status);
        }
        return resp;
      },
    });

    // WebDAV 场景（尤其是 Cloudflare -> Cloudflare）里，Range 经常被上游/平台忽略，
    // 软件切片会导致“看似 206 但一直加载/黑屏”。
    // 因此：WebDAV 若上游不支持 Range，就直接降级为 200 全量响应。
    descriptor.rangeFallbackPolicy = "full";

    return descriptor;
  }

  /**
   * 统一上传入口（文件 / 流）
   * - 外部统一调用此方法，内部根据数据类型选择流式或表单实现
   */
  async uploadFile(subPath, fileOrStream, ctx = {}) {
    this._ensureInitialized();
    const isWebStream = fileOrStream && typeof fileOrStream.getReader === "function";

    if (isWebStream) {
      // WebDAV 协议入口等场景：优先走真正的流式上传
      return await this.uploadStream(subPath, fileOrStream, ctx);
    }

    // 其它场景统一视为“表单/一次性上传”（读取到 Buffer 再写入）
    return await this.uploadForm(subPath, fileOrStream, ctx);
  }

  /**
   * 内部流式上传实现（主要用于 Web ReadableStream）
   */
  async uploadStream(subPath, stream, ctx = {}) {
    this._ensureInitialized();
    const { path } = ctx;
    const davPath = this._resolveTargetDavPath(subPath, path, stream, ctx);
    const url = this._buildRequestUrl(davPath);
    const headers = {};
    const contentType = ctx.contentType || null;
    if (contentType) {
      headers["Content-Type"] = contentType;
    }
    headers["Authorization"] = this._basicAuthHeader();

    // 为流式上传添加简单的进度统计（仅在 Web ReadableStream 环境下生效）
    /** @type {ReadableStream<any>} */
    let body = stream;
    try {
      const hasGetReader = stream && typeof stream.getReader === "function";
      if (hasGetReader) {
        const total = ctx.contentLength || ctx.fileSize || null;
        const reader = stream.getReader();
        let loaded = 0;
        let lastLogged = 0;
        const LOG_INTERVAL = 50 * 1024 * 1024; // 每 50MB 打印一次，避免刷屏

        const progressId = ctx.uploadId || davPath;

        body = new ReadableStream({
          async pull(controller) {
            const { done, value } = await reader.read();
              if (done) {
                controller.close();
                // 结束时再打印一次总进度
                if (loaded > 0 && total) {
                  const percentage = ((loaded / total) * 100).toFixed(1);
                  console.log(
                    `[StorageUpload] type=WEBDAV mode=STREAM event=completed loaded=${loaded} total=${total} percent=${percentage} path=${davPath}`
                  );
                }
                try {
                  completeUploadProgress(progressId);
                } catch {}
                return;
              }

            const chunkSize = value?.byteLength ?? value?.length ?? 0;
            loaded += chunkSize;

            const shouldLog =
              total != null
                ? loaded === total || loaded - lastLogged >= LOG_INTERVAL
                : loaded - lastLogged >= LOG_INTERVAL;

              if (shouldLog) {
                const percentage = total ? ((loaded / total) * 100).toFixed(1) : "未知";
                const totalLabel = total ?? "未知";
                console.log(
                  `[StorageUpload] type=WEBDAV mode=流式上传 status=进度 已传=${loaded} 总=${totalLabel} 进度=${percentage}% 路径=${davPath}`
                );
                lastLogged = loaded;
              }

            try {
              updateUploadProgress(progressId, {
                loaded,
                total,
                path: davPath,
                storageType: "WEBDAV",
              });
            } catch {}

            controller.enqueue(value);
          },
          cancel(reason) {
            try {
              reader.cancel(reason);
            } catch {
              // 取消失败时静默忽略
            }
          },
        });
      }
    } catch {
      // 若包装失败，退回原始 stream，避免影响主流程
      body = stream;
    }

    try {
      await this._ensureParentDirectories(davPath);
      /** @type {RequestInit} */
      const init = {
        method: "PUT",
        headers,
        body,
      };
      // Node.js 原生 fetch 在使用可读流作为 body 时必须显式设置 duplex
      if (isNodeJSEnvironment() && body != null) {
        // @ts-ignore
        init.duplex = "half";
      }
      const resp = await fetch(url, init);
      if (!resp.ok && resp.status !== 201 && resp.status !== 204) {
        throw this._wrapError(new Error(`HTTP ${resp.status}`), "上传文件失败", resp.status);
      }
      console.log(
        `[StorageUpload] type=WEBDAV mode=流式上传 status=成功 路径=${davPath}`
      );
      return { success: true, storagePath: davPath, message: "WEBDAV_STREAM_UPLOAD" };
    } catch (error) {
      throw this._wrapError(error, "上传文件失败", this._statusFromError(error));
    }
  }

  /**
   * 内部表单上传实现（一次性缓冲，适用于 Buffer / Uint8Array / ArrayBuffer / File / Blob / string 等）
   */
  async uploadForm(subPath, fileOrData, ctx = {}) {
    this._ensureInitialized();
    const { path } = ctx;
    const davPath = this._resolveTargetDavPath(subPath, path, fileOrData, ctx);
    const { body, length, contentType } = await this._normalizeBody(fileOrData, ctx);

    try {
      await this._ensureParentDirectories(davPath);
      await this.client.putFileContents(davPath, body, {
        overwrite: true,
        contentLength: length,
        contentType,
      });
      console.log(
        `[StorageUpload] type=WEBDAV mode=表单上传 status=成功 路径=${davPath} 大小=${length}`
      );

      return { success: true, storagePath: davPath, message: "WEBDAV_FORM_UPLOAD" };
    } catch (error) {
      throw this._wrapError(error, "上传文件失败", this._statusFromError(error));
    }
  }

  async createDirectory(subPath, ctx = {}) {
    this._ensureInitialized();
    const { path } = ctx;
    const davPath = this._buildDavPath(subPath, true);
    try {
      await this.client.createDirectory(davPath);
    } catch (error) {
      const status = this._statusFromError(error);
      // 部分 WebDAV 服务对已存在目录或根目录返回 405/501，视为目录已存在即可
      if (this._isConflict(error) || status === ApiStatus.NOT_IMPLEMENTED) {
        // 返回 FS 视图路径（契约）：path 字段必须等于输入参数
        return { success: true, path, alreadyExists: true };
      }
      throw this._wrapError(error, "创建目录失败", status);
    }
    // 返回 FS 视图路径（契约）：path 字段必须等于输入参数
    return { success: true, path };
  }

  async updateFile(subPath, content, ctx = {}) {
    this._ensureInitialized();
    const fsPath = ctx?.path;
    if (typeof fsPath !== "string" || !fsPath) {
      throw new DriverError("WebDAV 更新文件缺少 path 上下文（ctx.path）", {
        status: ApiStatus.INTERNAL_ERROR,
        expose: false,
        details: { subPath },
      });
    }

    const result = await this.uploadFile(subPath, content, ctx);
    return {
      success: !!result?.success,
      path: fsPath,
      message: result?.message,
    };
  }

  /**
   * 获取存储驱动统计信息
   * @returns {Promise<Object>} 统计信息
   */
  async getStats() {
    this._ensureInitialized();
    const base = {
      type: this.type,
      endpoint: this.endpoint,
      defaultFolder: this.defaultFolder || "/",
      capabilities: this.capabilities,
      initialized: this.initialized,
      timestamp: new Date().toISOString(),
    };

    if (!this.enableDiskUsage) {
      return {
        ...base,
        supported: false,
        message: "WebDAV 磁盘占用统计未启用（enable_disk_usage = false）",
      };
    }

    // WebDAV 配额（quota）读取：RFC 4331 定义 quota-used-bytes / quota-available-bytes
    // 注意：并非所有 WebDAV 服务都支持；因此失败时返回 supported=false，不抛异常
    try {
      const basePath = this._buildDavPath("", true);
      let quotaRes;
      try {
        quotaRes = this.client.getQuota.length >= 1 ? await this.client.getQuota(basePath) : await this.client.getQuota();
      } catch {
        quotaRes = await this.client.getQuota();
      }

      const quotaData = quotaRes && typeof quotaRes === "object" && "data" in quotaRes ? quotaRes.data : quotaRes;
      const used = quotaData && typeof quotaData === "object" && typeof quotaData.used === "number" ? quotaData.used : null;
      const available = quotaData && typeof quotaData === "object" && typeof quotaData.available === "number" ? quotaData.available : null;

      if (used == null && available == null) {
        return {
          ...base,
          supported: false,
          message: "WebDAV 服务器未提供配额信息（可能不支持 RFC 4331 quota 属性）",
        };
      }

      const totalBytes = used != null && available != null ? used + available : null;
      const usedBytes = used != null ? used : null;
      const remainingBytes = available != null ? available : null;

      let usagePercent = null;
      if (totalBytes && usedBytes != null && totalBytes > 0) {
        usagePercent = Math.min(100, Math.round((usedBytes / totalBytes) * 100));
      }

      return {
        ...base,
        supported: true,
        quota: {
          raw: quotaData || null,
          totalBytes,
          usedBytes,
          remainingBytes,
          usagePercent,
        },
      };
    } catch (error) {
      return {
        ...base,
        supported: false,
        message: error?.message || String(error),
      };
    }
  }

  async renameItem(oldSubPath, newSubPath, ctx = {}) {
    this._ensureInitialized();
    const { oldPath, newPath } = ctx;
    const isDir = typeof oldSubPath === "string" && oldSubPath.endsWith("/");
    const from = this._buildDavPath(oldSubPath, isDir);
    const to = this._buildDavPath(newSubPath, isDir);
    try {
      await this.client.moveFile(from, to, { overwrite: false });
      return { success: true, source: oldPath, target: newPath };
    } catch (error) {
      if (this._isNotSupported(error)) {
        throw new DriverError("WebDAV 不支持移动操作", { status: ApiStatus.NOT_IMPLEMENTED, expose: true });
      }
      throw this._wrapError(error, "重命名失败", this._statusFromError(error));
    }
  }

  async copyItem(sourceSubPath, targetSubPath, ctx = {}) {
    this._ensureInitialized();
    const { sourcePath, targetPath, skipExisting = false, _skipExistingChecked = false } = ctx;
    const isDir = typeof sourceSubPath === "string" && sourceSubPath.endsWith("/");
    const from = this._buildDavPath(sourceSubPath, isDir);
    const to = this._buildDavPath(targetSubPath, isDir);

    // skipExisting 检查：在复制前检查目标文件是否已存在
    // 如果入口层已检查（_skipExistingChecked=true），跳过重复检查
    if (skipExisting && !_skipExistingChecked) {
      try {
        const targetExists = await this.client.exists(to);
        if (targetExists) {
          return {
            status: "skipped",
            skipped: true,
            reason: "target_exists",
            source: sourcePath,
            target: targetPath,
            contentLength: 0,
          };
        }
      } catch (checkError) {
        // exists 检查失败时继续复制（降级处理）
        console.warn(`[WebDAV copyItem] skipExisting 检查失败 for ${to}:`, checkError?.message || checkError);
      }
    }

    try {
      const overwrite = !skipExisting;
      await this.client.copyFile(from, to, { overwrite });
      return { status: "success", source: sourcePath, target: targetPath };
    } catch (error) {
      if (this._isNotSupported(error)) {
        throw new DriverError("WebDAV 不支持复制操作", { status: ApiStatus.NOT_IMPLEMENTED, expose: true });
      }
      throw this._wrapError(error, "复制失败", this._statusFromError(error));
    }
  }

  async batchRemoveItems(subPaths, ctx = {}) {
    this._ensureInitialized();
    const results = [];
    const paths = Array.isArray(ctx?.paths) ? ctx.paths : null;
    for (let i = 0; i < (Array.isArray(subPaths) ? subPaths.length : 0); i++) {
      const sub = subPaths[i];
      const p = Array.isArray(paths) ? paths[i] : sub;
      const isDir = typeof sub === "string" && sub.endsWith("/");
      const davPath = this._buildDavPath(sub, isDir);
      try {
        await this.client.deleteFile(davPath);
        results.push({ path: p, success: true });
      } catch (error) {
        results.push({ path: p, success: false, error: error?.message || "删除失败" });
      }
    }
    return {
      success: results.filter((r) => r.success).length,
      failed: results.filter((r) => !r.success),
      results,
    };
  }

  async exists(subPath, ctx = {}) {
    this._ensureInitialized();
    const isDir = typeof subPath === "string" && subPath.endsWith("/");
    const davPath = this._buildDavPath(subPath, isDir);
    try {
      return await this.client.exists(davPath);
    } catch {
      return false;
    }
  }

  async stat(subPath, ctx = {}) {
    const isDir = typeof subPath === "string" && subPath.endsWith("/");
    return await this.client.stat(this._buildDavPath(subPath, isDir));
  }

  async generatePresignedUrl() {
    throw new DriverError("WebDAV 不支持预签名直链", { status: ApiStatus.NOT_IMPLEMENTED, expose: true });
  }

  /**
   * WebDAV 不提供存储直链能力（DirectLink），所有直链/代理决策由上层通过 url_proxy 或 native_proxy 处理。
   */
  async generateDownloadUrl(subPath, ctx = {}) {
    this._ensureInitialized();
    throw new DriverError("WebDAV 不支持存储直链 URL", {
      status: ApiStatus.NOT_IMPLEMENTED,
      expose: true,
    });
  }

  async generateProxyUrl(subPath, ctx = {}) {
    const { request, download = false, channel = "web" } = ctx;
    const fsPath = ctx?.path;

    // 驱动层仅负责根据路径构造基础代理URL，不再做签名与策略判断
    const proxyUrl = buildFullProxyUrl(request, fsPath, download);

    return {
      url: proxyUrl,
      type: "proxy",
      channel,
    };
  }

  /**
   * 上游 HTTP 能力：为 WebDAV 生成可由反向代理/Worker 直接访问的上游请求信息
   * - 返回值仅描述 data plane 访问方式，不做权限与签名判断
   * - headers 中只包含访问 WebDAV 必需的认证头，由外层按需附加 Range 等业务头
   * @param {string} path 挂载视图下的完整路径
   * @param {Object} [options]
   * @param {string} [options.subPath] 挂载内相对路径（优先使用）
   * @returns {Promise<{ url: string, headers: Record<string,string[]> }>}
   */
  async generateUpstreamRequest(path, options = {}) {
    this._ensureInitialized();

    const { subPath } = options;
    const relativePath = subPath || path;
    const davPath = this._buildDavPath(relativePath, false);
    const url = this._buildRequestUrl(davPath);

    /** @type {Record<string,string[]>} */
    const headers = {};
    const auth = this._basicAuthHeader();
    if (auth) {
      headers["Authorization"] = [auth];
    }

    return {
      url,
      headers,
    };
  }

  supportsProxyMode() {
    return true;
  }

  getProxyConfig() {
    return {
      enabled: this.supportsProxyMode(),
    };
  }

  /**
   * 构建 WebDAV 路径
   * - 仅基于挂载视图下的 subPath
   * - 远端根目录由 endpoint_url 本身的路径决定
   */
  _buildDavPath(subPath, ensureDir = false) {
    let raw = subPath || "";
    try {
      raw = decodeURI(raw);
    } catch {}
    const cleaned = this._normalize(raw);
    let full = cleaned;

    if (full && !full.startsWith("/")) {
      full = `/${full}`;
    }

    if (ensureDir) {
      if (!full) {
        full = "/";
      }
      if (!full.endsWith("/")) {
        full += "/";
      }
    }

    if (!full) {
      return "/";
    }

    return full;
  }

  _normalize(p) {
    const normalized = p.replace(/\\/g, "/").replace(/\/+/g, "/").replace(/^\/+/, "");
    const parts = normalized.split("/").filter(Boolean);
    for (const seg of parts) {
      if (seg === "..") {
        throw new DriverError("路径不允许包含 ..", { status: ApiStatus.FORBIDDEN, expose: true });
      }
    }
    return parts.join("/");
  }

  _decodeComponent(value) {
    if (!value) return value;
    try {
      return decodeURIComponent(value);
    } catch {
      try {
        return decodeURI(value);
      } catch {
        return value;
      }
    }
  }

  _joinMountPath(basePath, name, isDirectory) {
    const normalizedBase = basePath.endsWith("/") ? basePath : basePath + "/";
    return `${normalizedBase}${name}${isDirectory ? "/" : ""}`;
  }

  _buildMountPath(mount, subPath = "") {
    const mountRoot = mount?.mount_path || "/";
    const normalized = subPath.startsWith("/") ? subPath : `/${subPath}`;
    const compact = normalized.replace(/\/+/g, "/");
    return mountRoot.endsWith("/") ? `${mountRoot.replace(/\/+$/, "")}${compact}` : `${mountRoot}${compact}`;
  }

  _relativeTargetPath(targetPath, mount) {
    if (!targetPath) return targetPath;
    let relative = targetPath;
    if (mount?.mount_path && targetPath.startsWith(mount.mount_path)) {
      relative = targetPath.slice(mount.mount_path.length);
    }
    relative = relative.startsWith("/") ? relative.slice(1) : relative;
    return relative;
  }

  _basename(p) {
    const parts = (p || "").split("/").filter(Boolean);
    return parts.pop() || "";
  }

  _buildRequestUrl(davPath) {
    const base = this.endpoint.endsWith("/") ? this.endpoint.slice(0, -1) : this.endpoint;
    return `${base}${davPath}`;
  }

  _basicAuthHeader(passwordOverride = undefined) {
    const raw = `${this.username}:${passwordOverride ?? this.decryptedPassword ?? ""}`;
    const encoded = Buffer.from(raw, "utf8").toString("base64");
    return `Basic ${encoded}`;
  }

  _isNotFound(error) {
    const msg = error?.message || "";
    return msg.includes("404") || msg.includes("not found");
  }

  _isNotSupported(error) {
    const msg = error?.message?.toString?.() || "";
    return msg.includes("405") || msg.includes("501");
  }

  _isConflict(error) {
    const msg = error?.message?.toString?.() || "";
    return error?.statusCode === 409 || msg.includes("409");
  }

  _statusFromError(error) {
    const msg = error?.message || "";
    if (msg.includes("401") || msg.includes("403")) return ApiStatus.FORBIDDEN;
    if (msg.includes("404")) return ApiStatus.NOT_FOUND;
    if (msg.includes("405") || msg.includes("501")) return ApiStatus.NOT_IMPLEMENTED;
    return ApiStatus.INTERNAL_ERROR;
  }

  _wrapError(error, message, status = ApiStatus.INTERNAL_ERROR) {
    if (error instanceof DriverError || error instanceof AppError) return error;
    return new DriverError(message, { status, expose: status < 500, details: { cause: error?.message } });
  }

  async _normalizeBody(file, options = {}) {
    // 处理 Blob/File/ArrayBuffer/Uint8Array/Buffer/ReadableStream（Node 或 Web API）
    if (file === null || file === undefined) {
      throw new DriverError("上传体为空", { status: ApiStatus.BAD_REQUEST, expose: true });
    }

    // Buffer 或 Uint8Array
    if (typeof Buffer !== "undefined" && Buffer.isBuffer(file)) {
      return { body: file, length: file.length, contentType: options.contentType || null };
    }
    if (file instanceof Uint8Array) {
      return { body: file, length: file.byteLength, contentType: options.contentType || null };
    }

    // ArrayBuffer
    if (file instanceof ArrayBuffer) {
      const buf = Buffer.from(file);
      return { body: buf, length: buf.length, contentType: options.contentType || null };
    }

    // Web File/Blob
    if (typeof file.arrayBuffer === "function") {
      const buf = Buffer.from(await file.arrayBuffer());
      const length = file.size ?? buf.length;
      const type = options.contentType || file.type || null;
      return { body: buf, length, contentType: type };
    }

    // ReadableStream (Node 流或 Web API ReadableStream，例如 Cloudflare/Hono 请求体)
    const isNodeStream = file && (typeof file.pipe === "function" || file.readable);
    const isWebStream = file && typeof file.getReader === "function";
    if (isNodeStream || isWebStream) {
      // Node 流在 Node 环境下可以直接传递，由 webdav 客户端负责消费
      if (isNodeStream && !isWebStream) {
        const length = options.fileSize || options.contentLength || null;
        const type = options.contentType || null;
        return { body: file, length, contentType: type };
      }

      // Web API ReadableStream（Cloudflare/Hono 环境）：
      // 为避免 webdav 客户端与 WebStream 兼容性问题，这里统一读取为 Buffer 再上传，
      // 以确保上游存储端能够收到完整内容。
      try {
        const reader = file.getReader();
        /** @type {Uint8Array[]} */
        const chunks = [];
        let total = 0;
        // 按 Web Streams 标准逐块读取
        // eslint-disable-next-line no-constant-condition
        while (true) {
          const { done, value } = await reader.read();
          if (done) break;
          if (value && value.length) {
            chunks.push(value);
            total += value.length;
          }
        }
        const buf = Buffer.allocUnsafe(total);
        let offset = 0;
        for (const chunk of chunks) {
          buf.set(chunk, offset);
          offset += chunk.length;
        }
        const length = total;
        const type = options.contentType || null;
        return { body: buf, length, contentType: type };
      } catch (e) {
        throw new DriverError("读取上传流失败", {
          status: ApiStatus.INTERNAL_ERROR,
          expose: false,
          details: { cause: e?.message || String(e) },
        });
      }
    }

    // 字符串
    if (typeof file === "string") {
      const buf = Buffer.from(file);
      return { body: buf, length: buf.length, contentType: options.contentType || "text/plain" };
    }

    throw new DriverError("不支持的上传数据类型", { status: ApiStatus.BAD_REQUEST, expose: true });
  }

  async _ensureParentDirectories(davPath) {
    // davPath like /a/b/c.txt -> need ensure /a and /a/b
    const trimmed = davPath.endsWith("/") ? davPath.slice(0, -1) : davPath;
    const parts = trimmed.split("/").filter(Boolean);
    if (parts.length <= 1) return;
    const dirs = [];
    for (let i = 0; i < parts.length - 1; i++) {
      const prefix = "/" + parts.slice(0, i + 1).join("/") + "/";
      dirs.push(prefix);
    }
    for (const dir of dirs) {
      try {
        await this.client.createDirectory(dir);
      } catch (e) {
        // 如果目录已存在则忽略
        if (this._isConflict(e) || this._isNotFound(e) || this._isNotSupported(e)) {
          // _isNotFound 对某些服务返回 409/404 混用，_isNotSupported 对 405/501，均视为无害
          continue;
        }
        throw e;
      }
    }
  }

  /**
   * 解析目标路径：当传入目录时自动拼接文件名
   */
  _resolveTargetDavPath(subPath, path, file, options = {}) {
    const fileName =
      options.filename ||
      options.fileName ||
      file?.name ||
      path.split("/").filter(Boolean).pop() ||
      "unnamed_file";

    const normalizedSub = this._normalize(subPath || "");

    const isFilePath = this._isCompleteFilePath(normalizedSub, fileName);
    let joined = "";
    if (normalizedSub) joined = normalizedSub;

    if (!isFilePath) {
      joined = joined ? `${joined}/${fileName}` : fileName;
    }

    const withPrefix = joined.startsWith("/") ? joined : `/${joined}`;
    return withPrefix;
  }

  _isCompleteFilePath(relativePath, originalFileName) {
    if (!relativePath || !originalFileName) return false;
    const relLast = relativePath.split("/").filter(Boolean).pop();
    if (!relLast) return false;
    const rel = this._splitName(relLast);
    const ori = this._splitName(originalFileName);
    if (!rel.ext) {
      return rel.name === ori.name;
    }
    return rel.ext === ori.ext && rel.name === ori.name;
  }

  _splitName(name = "") {
    const idx = name.lastIndexOf(".");
    if (idx <= 0) {
      return { name, ext: "" };
    }
    return { name: name.slice(0, idx), ext: name.slice(idx) };
  }
}
