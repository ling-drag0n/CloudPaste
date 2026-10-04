/**
 * StorageStreaming - 统一的存储内容访问层
 *
 * - 作为所有内容访问路径的唯一入口（FS/WebDAV/Proxy/Share/Object/Preview）
 * - 调用驱动获取 StorageStreamDescriptor
 * - 处理 Range/条件请求
 * - 返回 RangeReader 供协议层构造 HTTP 响应
 * - 驱动层不再构造 HTTP Response
 * - Node/Worker 运行时差异在此层处理
 * - 统一的错误映射和日志
 */

import {
  parseRangeHeader,
  parseMultiRangeHeader,
  evaluateConditionalHeaders,
  buildResponseHeaders,
  mapDriverErrorToHttpStatus,
  shouldIgnoreRangeForIfRange,
} from "./utils.js";
import { STREAMING_CHANNELS } from "./types.js";
import { NotFoundError, DriverError } from "../../http/errors.js";
import { ApiStatus } from "../../constants/index.js";
import { smartWrapStreamWithByteSlice } from "./ByteSliceStream.js";
import {
  createDecryptingTransform,
  mapPlainRangeToCipherRange,
  parseEncryptionMeta,
} from "../../utils/fileEncryption.js";

// 视频“超大跳转”保护阈值（固定值，跟随 OpenList 的经验阈值）：
// - OpenList 在“上游不支持 Range，只能读掉 offset 再丢弃”的兜底里，对 offset > 100MB 会明确警告浪费带宽
//   参考：D:/github-project/OpenList/internal/net/util.go:314
// - 我们这里用于“视频大跳转”场景：当 Range start 超过该阈值时，会先探测上游是否真支持 Range
//   - 上游不支持：忽略 Range 返回 200（避免读掉大量字节导致流量爆炸）
//   - 上游支持：正常返回 206
const VIDEO_SOFTWARE_SLICE_MAX_START_BYTES = 100 * 1024 * 1024; // 100MB

const isLikelyVideoPath = (p) => {
  const s = (p || "").toLowerCase();
  return s.endsWith(".mp4") || s.endsWith(".m4v") || s.endsWith(".mov") || s.endsWith(".webm") || s.endsWith(".mkv") || s.endsWith(".avi");
};

const isVideoLikeRequest = (request, descriptor, path) => {
  const ct = descriptor?.contentType ? String(descriptor.contentType).toLowerCase() : "";
  if (ct.startsWith("video/")) return true;

  const dest = request?.headers?.get?.("sec-fetch-dest");
  if (dest && String(dest).toLowerCase() === "video") return true;

  const accept = request?.headers?.get?.("accept");
  if (accept && String(accept).toLowerCase().includes("video/")) return true;

  return isLikelyVideoPath(path);
};

/**
 * 将 Node.js Readable 转换为 Web ReadableStream（用于 Response body）
 * 目标：在 Node/Docker 场景下也能“边读边回”，避免整文件 Buffer.concat 导致大文件一直加载。
 *
 * @param {any} nodeStream
 * @param {() => Promise<void> | void} [onClose]
 * @returns {Promise<ReadableStream<Uint8Array>>}
 */
async function wrapNodeReadableToWebStream(nodeStream, onClose) {
  // 设计原则：
  // 1) 优先用 Node 原生 Readable.toWeb 做桥接（自带背压，不会把 1GB 视频“疯狂塞进内存队列”）。
  // 2) 不监听 "close" 事件；只把 "end" 当作正常结束，避免 Content-Length mismatch 导致浏览器“拖动不了进度条”。

  try {
    const { Readable } = await import("node:stream");
    if (Readable?.toWeb) {
      const webStream = Readable.toWeb(nodeStream);
      const reader = webStream.getReader();
      let cleaned = false;

      const cleanup = async () => {
        if (cleaned) return;
        cleaned = true;
        try {
          await onClose?.();
        } catch {
          // ignore
        }
      };

      return new ReadableStream({
        async pull(controller) {
          try {
            const { done, value } = await reader.read();
            if (done) {
              await cleanup();
              controller.close();
              return;
            }
            controller.enqueue(value);
          } catch (err) {
            await cleanup();
            controller.error(err);
          }
        },
        async cancel(reason) {
          try {
            await reader.cancel(reason);
          } catch {
            // ignore
          }
          try {
            nodeStream.destroy?.();
          } catch {
            // ignore
          }
          await cleanup();
        },
      });
    }
  } catch {
    // ignore (Worker 环境/兼容层可能没有 node:stream)
  }

  // 兜底：事件桥接（带简单背压）
  return new ReadableStream({
    start(controller) {
      let cleaned = false;

      const cleanup = async () => {
        if (cleaned) return;
        cleaned = true;
        try {
          nodeStream.off?.("data", onData);
          nodeStream.off?.("error", onError);
          nodeStream.off?.("end", onEnd);
        } catch {
          // ignore
        }
        try {
          await onClose?.();
        } catch {
          // ignore
        }
      };

      const onData = (chunk) => {
        // 兼容：某些 Node 流若设置了 encoding，chunk 可能是 string
        const data = typeof chunk === "string" ? Buffer.from(chunk) : chunk;
        // Buffer 本身就是 Uint8Array，直接 enqueue 即可（避免额外拷贝）
        controller.enqueue(data);
        // ReadableStream 的 desiredSize <= 0 表示消费者跟不上，暂停 Node 读取，避免内存无限增长
        if (controller.desiredSize !== null && controller.desiredSize <= 0) {
          try {
            nodeStream.pause?.();
          } catch {
            // ignore
          }
        }
      };

      const onError = (err) => {
        void cleanup().finally(() => {
          controller.error(err);
        });
      };

      const onEnd = () => {
        void cleanup().finally(() => {
          controller.close();
        });
      };

      nodeStream.on?.("data", onData);
      nodeStream.on?.("error", onError);
      nodeStream.on?.("end", onEnd);

      // 默认先暂停，等消费者 pull 再开始读
      try {
        nodeStream.pause?.();
      } catch {
        // ignore
      }
    },
    pull() {
      try {
        nodeStream.resume?.();
      } catch {
        // ignore
      }
    },
    async cancel() {
      try {
        nodeStream.destroy?.();
      } catch {
        // ignore
      }
      try {
        await onClose?.();
      } catch {
        // ignore
      }
    },
  });
}

/**
 * 将 Web ReadableStream 包装为“可感知 close”的流
 * 目标：当客户端读完/中断/报错时，确保调用 handle.close() 释放底层资源（例如远端连接、上游下载、文件句柄等）。
 *
 * @param {ReadableStream<Uint8Array>} webStream
 * @param {() => Promise<void> | void} [onClose]
 * @returns {ReadableStream<Uint8Array>}
 */
function wrapWebReadableStreamWithClose(webStream, onClose) {
  const reader = webStream.getReader();
  let cleaned = false;

  const cleanup = async () => {
    if (cleaned) return;
    cleaned = true;
    try {
      reader.releaseLock?.();
    } catch {
      // ignore
    }
    try {
      await onClose?.();
    } catch {
      // ignore
    }
  };

  return new ReadableStream({
    async pull(controller) {
      try {
        const { done, value } = await reader.read();
        if (done) {
          await cleanup();
          controller.close();
          return;
        }
        const data = value instanceof Uint8Array ? value : new Uint8Array(value);
        controller.enqueue(data);
      } catch (err) {
        await cleanup();
        controller.error(err);
      }
    },
    async cancel(reason) {
      try {
        await reader.cancel(reason);
      } catch {
        // ignore
      }
      await cleanup();
    },
  });
}

/**
 * @typedef {import('./types.js').StorageStreamDescriptor} StorageStreamDescriptor
 * @typedef {import('./types.js').RangeReader} RangeReader
 * @typedef {import('./types.js').RangeReaderOptions} RangeReaderOptions
 * @typedef {import('./types.js').StreamHandle} StreamHandle
 */

export class StorageStreaming {
  /**
   * @param {Object} options
   * @param {Object} options.mountManager - MountManager 实例
   * @param {Object} options.storageFactory - StorageFactory 类
   * @param {string} options.encryptionSecret - 加密密钥
   */
  constructor({ mountManager, storageFactory, encryptionSecret }) {
    this.mountManager = mountManager;
    this.storageFactory = storageFactory;
    this.encryptionSecret = encryptionSecret;
  }

  /**
   * 获取 RangeReader（主入口）
   * @param {RangeReaderOptions} options
   * @returns {Promise<RangeReader>}
   */
  async getRangeReader(options) {
    const { path, channel, mount, storageConfigId, request, userIdOrInfo, userType, ownerType, ownerId, db } = options;
    const rangeHeader = options?.rangeHeader ?? request?.headers?.get?.("range") ?? null;

    const logPrefix = `[StorageStreaming][${channel}]`;
    const ifRangeHeader = request?.headers?.get?.("if-range") ?? null;
    console.log(
      `${logPrefix} 开始处理: ${path}${rangeHeader ? ` | Range=${rangeHeader}` : ""}${ifRangeHeader ? ` | If-Range=${ifRangeHeader}` : ""}`,
    );

    try {
      // 1. 解析路径到驱动
      const { driver, resolvedMount, subPath } = await this._resolveDriver(options);

      // 2. 获取 StorageStreamDescriptor
      const downloadResult = await driver.downloadFile(subPath, {
        path,
        mount: resolvedMount,
        subPath,
        db,
        request,
        userIdOrInfo,
        userType,
        ownerType,
        ownerId,
      });

      // 验证返回结构
      /** @type {StorageStreamDescriptor} */
      const descriptor = this._adaptToDescriptor(downloadResult);

      // 3. 评估条件请求
      const { shouldReturn304, shouldReturn412 } = evaluateConditionalHeaders(request, descriptor.etag, descriptor.lastModified);

      if (shouldReturn304) {
        console.log(`${logPrefix} 返回 304 Not Modified`);
        return this._create304Reader(descriptor, channel);
      }

      if (shouldReturn412) {
        console.log(`${logPrefix} 返回 412 Precondition Failed`);
        return this._create412Reader(descriptor, channel);
      }

      // If-Range：不匹配则必须忽略 Range（返回 200 全量），避免客户端拼接错乱/花屏
      if (rangeHeader && shouldIgnoreRangeForIfRange(request, descriptor.etag, descriptor.lastModified)) {
        console.log(`${logPrefix} If-Range 不匹配，忽略 Range，返回 200 OK`);
        return this._create200Reader(descriptor, channel);
      }

      // 解析 Range 请求
      // Range 场景：若文件大小未知但描述符提供 probeSize，则优先探测 size 后再解析 Range
      if (rangeHeader && (descriptor.size === null || descriptor.size <= 0) && typeof descriptor.probeSize === "function") {
        try {
          // 不将探测失败视为致命错误，失败则回退为 200 全量响应
          await descriptor.probeSize({ signal: AbortSignal.timeout ? AbortSignal.timeout(8000) : undefined });
        } catch (e) {
          console.warn(`${logPrefix} Range size 探测失败，将回退为 200: ${e?.message || String(e)}`);
        }
      }

      // multi-range（多段 Range）：按 RFC 语义返回 multipart/byteranges（仅在 size 已知且驱动具备原生 getRange 时启用）
      if (rangeHeader && String(rangeHeader).includes(",")) {
        const parsed = parseMultiRangeHeader(rangeHeader, descriptor.size);
        if (!parsed || !parsed.isValid) {
          console.log(`${logPrefix} Multi-Range 格式无效，降级为 200 OK`);
          return this._create200Reader(descriptor, channel);
        }

        // size 未知：无法构造 multipart/byteranges（尤其 suffix-range），降级为 200
        if (descriptor.size === null || descriptor.size <= 0) {
          console.log(`${logPrefix} Multi-Range 但文件大小未知，降级为 200 OK`);
          return this._create200Reader(descriptor, channel);
        }

        // 无可满足范围：416
        if (parsed.hasNoOverlap) {
          console.log(`${logPrefix} Multi-Range 无可满足范围，返回 416`);
          return this._create416Reader(descriptor, channel);
        }

        // 规范/安全：若总请求字节数超过文件大小，视为可疑，忽略 Range
        let total = 0;
        for (const r of parsed.ranges) total += r.end - r.start + 1;
        if (total > descriptor.size) {
          console.log(`${logPrefix} Multi-Range 总字节数(${total})超过文件大小(${descriptor.size})，忽略 Range，返回 200`);
          return this._create200Reader(descriptor, channel);
        }

        // 单段其实不该走 multi-range，但为了兼容，回到单段 206
        if (parsed.ranges.length === 1) {
          const single = parsed.ranges[0];
          console.log(`${logPrefix} Multi-Range 解析后仅 1 段，回退为单段 206: ${single.start}-${single.end}`);
          return this._create206Reader(descriptor, { ...single, isSatisfiable: true }, channel);
        }

        // 关键限制：只有驱动具备 getRange（且上游真正返回 206）才启用 multipart，
        // 否则会变成“每段都从头读再丢掉”的超浪费方案，反而更糟糕。
        if (typeof descriptor.getRange !== "function") {
          console.log(`${logPrefix} Multi-Range 但驱动未实现 getRange，降级为 200（避免超浪费的软件多段切片）`);
          return this._create200Reader(descriptor, channel);
        }

        // 探测一次：确认上游真的支持 Range（避免“忽略 Range 返回 200”时我们却输出 multipart 头）
        try {
          const probe = await descriptor.getRange({ start: parsed.ranges[0].start, end: parsed.ranges[0].start });
          const ok = probe && probe.supportsRange !== false;
          await probe?.close?.();
          if (!ok) {
            console.log(`${logPrefix} Multi-Range 探测发现上游不支持 Range，降级为 200`);
            return this._create200Reader(descriptor, channel);
          }
        } catch (e) {
          console.log(`${logPrefix} Multi-Range 探测失败，降级为 200：${e?.message || String(e)}`);
          return this._create200Reader(descriptor, channel);
        }

        console.log(`${logPrefix} 返回 206 Multi-Range (multipart): ${parsed.ranges.map((r) => `${r.start}-${r.end}`).join(",")}`);
        return this._create206MultiRangeReader(descriptor, parsed.ranges, channel);
      }

      const range = parseRangeHeader(rangeHeader, descriptor.size);

      // 文件大小未知（例如 WebDAV HEAD 未返回 Content-Length）时，
      // 无法构造符合规范的 206/416（缺少 Content-Range），统一降级为 200 全量响应。
      if (range && range.unknownSize) {
        console.log(`${logPrefix} 文件大小未知，忽略 Range，返回 200 OK: ${range.start}-${range.end}`);
        return this._create200Reader(descriptor, channel);
      }

      if (range && !range.isValid) {
        console.log(`${logPrefix} Range 格式无效`);
        return this._create200Reader(descriptor, channel);
      }

      if (range && !range.isSatisfiable) {
        console.log(`${logPrefix} 返回 416 Range Not Satisfiable`);
        return this._create416Reader(descriptor, channel);
      }

      if (range && range.isSatisfiable) {
        // 视频“超大跳转”保护：
        // - 当用户拖动到很后面时，浏览器会发 Range: bytes=<很大>-...
        // - 如果上游不支持 Range，我们继续用软件切片会导致“先读掉巨量字节再丢弃”，流量与耗时非常夸张
        // - 因此：对视频场景的大 start，先探测上游是否真的支持 Range；不支持就直接忽略 Range 返回 200
        //   （这样至少不会把带宽打爆；但该上游本身也无法真正支持拖动）
        if (
          range.start > VIDEO_SOFTWARE_SLICE_MAX_START_BYTES &&
          isVideoLikeRequest(request, descriptor, path)
        ) {
          const startMb = Math.round((range.start / 1024 / 1024) * 10) / 10;
          const limitMb = Math.round((VIDEO_SOFTWARE_SLICE_MAX_START_BYTES / 1024 / 1024) * 10) / 10;
          console.warn(
            `${logPrefix} 检测到视频大跳转 Range(start=${range.start},约${startMb}MB，阈值=${limitMb}MB)，将先确认上游是否支持 Range`,
          );

          if (typeof descriptor.getRange !== "function") {
            console.warn(`${logPrefix} 驱动未实现 getRange，忽略 Range 返回 200（避免软件切片读掉大量字节）`);
            return this._create200Reader(descriptor, channel);
          }

          try {
            const probe = await descriptor.getRange({ start: range.start, end: range.start });
            const ok = probe?.supportsRange !== false;
            if (probe?.upstreamStatus !== undefined || probe?.upstreamContentRange) {
              console.warn(
                `${logPrefix} Range 探测结果: upstreamStatus=${probe?.upstreamStatus ?? "?"}, contentRange=${probe?.upstreamContentRange ?? ""}, supportsRange=${ok}`,
              );
            }
            await probe?.close?.();
            if (!ok) {
              console.warn(`${logPrefix} 上游不支持 Range（将返回 200），避免软件切片造成带宽爆炸`);
              return this._create200Reader(descriptor, channel);
            }
          } catch (e) {
            console.warn(`${logPrefix} 上游 Range 探测失败，将忽略 Range 返回 200（避免软件切片造成带宽爆炸）：${e?.message || String(e)}`);
            return this._create200Reader(descriptor, channel);
          }
        }

        console.log(`${logPrefix} 返回 206 Partial Content: ${range.start}-${range.end}`);
        return this._create206Reader(descriptor, range, channel);
      }

      // 5. 正常 200 响应
      console.log(`${logPrefix} 返回 200 OK`);
      return this._create200Reader(descriptor, channel);
    } catch (error) {
      console.error(`${logPrefix} 错误:`, error?.message || error);
      throw error;
    }
  }

  /**
   * 便捷方法：直接构造 HTTP Response
   * @param {RangeReaderOptions} options
   * @returns {Promise<Response>}
   */
  async createResponse(options) {
    try {
      // 加密文件专用通道（CPENC1 server 模式）：在本层按块解密后返回明文；
      // e2e 模式不在此解密（服务端无密钥），走常规机制原样返回密文，由前端解密。
      const encMeta = parseEncryptionMeta(options?.encryptionMeta || null);
      if (encMeta && encMeta.mode === "server") {
        return await this._createEncryptedResponse(options, encMeta);
      }

      const reader = await this.getRangeReader(options);
      let { status, headers } = reader;

      // 304/412 无响应体
      if (status === 304 || status === 412 || status === 416) {
        return new Response(null, { status, headers });
      }

      // HEAD：只返回 headers，不要触发读取/打开底层流（避免浪费带宽/IO）
      if (options?.request?.method === "HEAD") {
        return new Response(null, { status, headers });
      }

      const handle = await reader.getBody();
      if (!handle) {
        return new Response(null, { status, headers });
      }

      // 允许 Reader 在打开底层流之后，根据上游真实响应情况“降级为 200”（例如：上游/平台忽略 Range）。
      // 用于“关闭软切片”模式，让不支持 Range 的场景回退到标准 200 全量响应，避免 206 + 软件切片导致播放卡死等问题。
      if (typeof handle?.overrideStatus === "number") {
        status = handle.overrideStatus;
      }
      if (handle?.overrideHeaders) {
        headers = handle.overrideHeaders;
      }

      const { stream } = handle;

      // 兼容：当上游不支持 Range 时，用 ByteSliceStream 做“软件切片”。
      // 对软件切片场景移除 Content-Length，让客户端以流结束为准。
      if (status === 206 && handle?.softwareSlice) {
        try {
          headers.delete("Content-Length");
        } catch {
          // ignore
        }
      }

      // 对于 WebReadableStream（有 getReader 方法），直接作为 Response body 交给运行时处理
      if (stream && typeof stream.getReader === "function") {
        const body = wrapWebReadableStreamWithClose(stream, handle?.close);
        return new Response(body, { status, headers });
      }

      // 对于 Node Readable（本地存储等场景），必须转换为 Web ReadableStream 并保持“边读边回”。
      if (stream && (typeof stream.pipe === "function" || typeof stream.on === "function")) {
        const body = await wrapNodeReadableToWebStream(stream, handle?.close);
        return new Response(body, { status, headers });
      }

      // 兼容：某些 Node 流对象只有 asyncIterator 特征
      if (stream && typeof stream[Symbol.asyncIterator] === "function") {
        // 退回到“事件桥接”方式：将 asyncIterator 读出来再推送到 Web Stream
        const asyncIterable = stream;
        const body = new ReadableStream({
          async start(controller) {
            try {
              for await (const chunk of asyncIterable) {
                const data = chunk instanceof Uint8Array ? chunk : typeof chunk === "string" ? Buffer.from(chunk) : new Uint8Array(chunk);
                controller.enqueue(data);
              }
              controller.close();
            } catch (e) {
              controller.error(e);
            } finally {
              try {
                await handle?.close?.();
              } catch {
                // ignore
              }
            }
          },
          async cancel() {
            try {
              await handle?.close?.();
            } catch {
              // ignore
            }
          },
        });
        return new Response(body, { status, headers });
      }

      // 兜底：未知类型，直接交给 Response 处理
      return new Response(stream, { status, headers });
    } catch (error) {
      // 统一将驱动/流层错误映射为标准 HTTP 响应，便于前端获取 code/message
      const { status, message } = mapDriverErrorToHttpStatus(error);
      const code = error?.code || "STREAMING_ERROR";

      const body = JSON.stringify({
        success: false,
        code,
        message,
      });

      const headers = new Headers();
      headers.set("Content-Type", "application/json; charset=utf-8");

      return new Response(body, { status, headers });
    }
  }

  /**
   * 验证驱动返回的 StorageStreamDescriptor 结构
   * @param {any} result - 驱动返回的结果
   * @returns {import('./types.js').StorageStreamDescriptor}
   * @private
   */
  _adaptToDescriptor(result) {
    // 验证是否为有效的 StorageStreamDescriptor
    if (typeof result?.getStream === "function") {
      return result;
    }

    // 无法识别的结构，抛出错误
    throw new DriverError("驱动返回了无效的 StorageStreamDescriptor 结构，缺少 getStream 方法", {
      status: ApiStatus.INTERNAL_ERROR,
      code: "STREAMING_ERROR.INVALID_DOWNLOAD_RESULT",
    });
  }

  /**
   * 加密文件（CPENC1 server 模式）专用响应构造：
   * - 按明文坐标系解析 Range，映射到块对齐的密文区间，仅解密覆盖块（保留 206/视频拖动）
   * - 上游不支持 Range 时降级为 200 全量解密
   * - ETag 透传密文对象的 ETag（本层响应的强缓存语义有限，条件请求降级处理）
   * @param {RangeReaderOptions} options
   * @param {object} meta 已解析的加密元数据
   * @returns {Promise<Response>}
   * @private
   */
  async _createEncryptedResponse(options, meta) {
    const logPrefix = `[StorageStreaming][${options.channel}][enc]`;
    const request = options.request;
    const rangeHeader = options?.rangeHeader ?? request?.headers?.get?.("range") ?? null;

    // 1. 解析驱动与描述符（密文坐标系）
    const { driver, resolvedMount, subPath } = await this._resolveDriver(options);
    const descriptor = this._adaptToDescriptor(
      await driver.downloadFile(subPath, {
        path: options.path,
        mount: resolvedMount,
        subPath,
        db: options.db,
        request,
        userIdOrInfo: options.userIdOrInfo,
        userType: options.userType,
        ownerType: options.ownerType,
        ownerId: options.ownerId,
      }),
    );

    const plainSize = Number(meta.plainSize) || 0;
    const headers = new Headers();
    headers.set("Accept-Ranges", "bytes");
    if (descriptor.contentType) {
      headers.set("Content-Type", descriptor.contentType);
    }
    if (descriptor.etag) {
      headers.set("ETag", descriptor.etag);
    }

    // 2. 明文坐标系单段 Range 解析
    const parsePlainRange = (header) => {
      const m = /^bytes=(\d*)-(\d*)$/.exec(String(header || "").trim());
      if (!m || (m[1] === "" && m[2] === "")) return null;
      let start;
      let end;
      if (m[1] === "") {
        const suffix = Number(m[2]);
        if (!Number.isFinite(suffix) || suffix <= 0) return null;
        if (plainSize <= 0) return { satisfiable: false };
        start = Math.max(0, plainSize - suffix);
        end = plainSize - 1;
      } else {
        start = Number(m[1]);
        if (!Number.isFinite(start) || start >= plainSize) {
          return Number.isFinite(start) ? { satisfiable: false } : null;
        }
        end = m[2] === "" ? plainSize - 1 : Math.min(Number(m[2]), plainSize - 1);
        end = Math.max(start, Number.isFinite(end) ? end : plainSize - 1);
      }
      return { satisfiable: true, start, end };
    };

    // HEAD：只回头部，不触发底层流
    if (request?.method === "HEAD") {
      if (plainSize > 0) {
        headers.set("Content-Length", String(plainSize));
      }
      return new Response(null, { status: 200, headers });
    }

    const range = rangeHeader ? parsePlainRange(rangeHeader) : null;
    if (range && !range.satisfiable) {
      console.log(`${logPrefix} Range 超出明文范围，返回 416`);
      const h = new Headers(headers);
      h.set("Content-Range", `bytes */${plainSize}`);
      return new Response(null, { status: 416, headers: h });
    }

    const keyMaterial = { encryptionSecret: this.encryptionSecret };
    const closeSafe = async (handle) => {
      try {
        await handle?.close?.();
      } catch {
        // ignore
      }
    };

    /** 将密文流（Web 或 Node）接上解密层并挂 close 回调 */
    const buildBody = async (handle, transform) => {
      let webStream;
      if (handle.stream && (typeof handle.stream.pipe === "function" || typeof handle.stream.on === "function")) {
        webStream = await wrapNodeReadableToWebStream(handle.stream, handle?.close);
      } else {
        webStream = wrapWebReadableStreamWithClose(handle.stream, handle?.close);
      }
      const piped = webStream.pipeThrough(transform);
      return wrapWebReadableStreamWithClose(piped, async () => closeSafe(handle));
    };

    if (range) {
      const mapped = mapPlainRangeToCipherRange(meta, range.start, range.end);
      let handle = null;
      let transform;

      if (typeof descriptor.getRange === "function") {
        handle = await descriptor.getRange({ start: mapped.cipherStart, end: mapped.cipherEnd });
        transform = await createDecryptingTransform(meta, keyMaterial, {
          startChunkIndex: mapped.firstChunk,
          skipPlain: mapped.skipPlain,
          maxPlain: mapped.maxPlain,
          headerless: true,
        });
        console.log(
          `${logPrefix} 206 块映射: 明文 ${range.start}-${range.end} -> 密文块 ${mapped.firstChunk}-${mapped.lastChunk} (${mapped.cipherStart}-${mapped.cipherEnd})`,
        );
      } else {
        // 上游不支持 Range：降级 200 全量解密，避免“读掉 offset 再丢弃”的浪费
        console.log(`${logPrefix} 驱动不支持 getRange，忽略 Range 返回 200 全量解密`);
        handle = await descriptor.getStream();
        transform = await createDecryptingTransform(meta, keyMaterial, {});
      }

      const status = typeof descriptor.getRange === "function" ? 206 : 200;
      const body = await buildBody(handle, transform);
      if (status === 206) {
        headers.set("Content-Range", `bytes ${range.start}-${range.end}/${plainSize}`);
        headers.set("Content-Length", String(mapped.maxPlain));
      } else if (plainSize > 0) {
        headers.set("Content-Length", String(plainSize));
      }
      return new Response(body, { status, headers });
    }

    // 无 Range：200 全量解密
    const handle = await descriptor.getStream();
    const transform = await createDecryptingTransform(meta, keyMaterial, {});
    const body = await buildBody(handle, transform);
    if (plainSize > 0) {
      headers.set("Content-Length", String(plainSize));
    }
    console.log(`${logPrefix} 返回 200 OK（解密后 ${plainSize} 字节）`);
    return new Response(body, { status: 200, headers });
  }

  /**
   * 解析路径到驱动
   * @private
   */
  async _resolveDriver(options) {
    const { path, mount, storageConfigId, userIdOrInfo, userType, db, repositoryFactory } = options;

    // 如果提供了 storageConfigId，通过存储配置创建驱动（存储路径模式）
    if (storageConfigId && db) {
      // 获取存储配置
      const storageConfigRepo = repositoryFactory?.getStorageConfigRepository?.();
      let storageConfig = null;

      if (storageConfigRepo?.findByIdWithSecrets) {
        storageConfig = await storageConfigRepo.findByIdWithSecrets(storageConfigId);
      } else if (storageConfigRepo?.findById) {
        storageConfig = await storageConfigRepo.findById(storageConfigId);
      }

      if (!storageConfig) {
        throw new NotFoundError("存储配置不存在");
      }

      if (!storageConfig.storage_type) {
        throw new DriverError("存储配置缺少 storage_type", {
          status: ApiStatus.INTERNAL_ERROR,
          code: "STREAMING_ERROR.INVALID_CONFIG",
        });
      }

      // 使用 StorageFactory 创建驱动
      const { StorageFactory } = await import("../factory/StorageFactory.js");
      const driver = await StorageFactory.createDriver(storageConfig.storage_type, storageConfig, this.encryptionSecret);
      return { driver, resolvedMount: null, subPath: path };
    }

    // 否则通过 MountManager 解析（FS 路径模式）
    if (this.mountManager) {
      const { driver, mount: resolvedMount, subPath } = await this.mountManager.getDriverByPath(path, userIdOrInfo, userType);
      return { driver, resolvedMount, subPath };
    }

    throw new DriverError("无法解析存储路径：缺少 mountManager 或 storageConfigId", {
      status: ApiStatus.INTERNAL_ERROR,
      code: "STREAMING_ERROR.NO_RESOLVER",
    });
  }

  /**
   * 创建 200 OK RangeReader
   * @private
   */
  _create200Reader(descriptor, channel) {
    const headers = buildResponseHeaders(descriptor, null, channel);
    let streamHandle = null;
    let closed = false;

    return {
      status: 200,
      headers,
      async getBody() {
        if (closed) return null;
        streamHandle = await descriptor.getStream();
        return streamHandle;
      },
      async close() {
        if (closed) return;
        closed = true;
        if (streamHandle) {
          await streamHandle.close();
        }
      },
    };
  }

  /**
   * 创建 206 Partial Content RangeReader
   *
   * 核心逻辑：
   * 1. 优先使用驱动原生 getRange 方法
   * 2. 检测驱动是否真正返回了部分内容（supportsRange 标记）
   * 3. 如果驱动返回完整流（200 而非 206），使用 ByteSliceStream 进行软件切片
   * 4. 如果驱动不支持 getRange，直接使用 ByteSliceStream 包装完整流
   *
   * @private
   */
  _create206Reader(descriptor, range, channel) {
    let streamHandle = null;
    let closed = false;
    const { start, end } = range;

    // 这里 range 已由 parseRangeHeader 基于 descriptor.size 解析并校验，
    // start/end 均为有限整数且在文件范围内，可直接用于构造响应头与字节切片。
    const headers = buildResponseHeaders(descriptor, range, channel);

    return {
      status: 206,
      headers,
      async getBody() {
        if (closed) return null;

        // 优先使用驱动原生 Range 支持
        if (typeof descriptor.getRange === "function") {
          streamHandle = await descriptor.getRange(range);

          // 关键检测：驱动是否真正支持 Range 请求
          // 部分 WebDAV 服务器会忽略 Range 头，返回完整内容
          const supportsRange = streamHandle.supportsRange !== false;

          if (!supportsRange) {
            // 默认行为：用 ByteSliceStream 做软件切片（兼容不支持 Range 的上游）。
            // WebDAV 在 Cloudflare->Cloudflare 场景下，软件切片容易出现“黑屏一直加载”，因此可配置为直接降级 200。
            const fallbackPolicy = descriptor?.rangeFallbackPolicy || "software";

            if (fallbackPolicy === "full") {
              console.log(`[StorageStreaming] 检测到驱动不支持 Range，降级为 200 全量响应（忽略 Range）: ${start}-${end}`);
              if (streamHandle?.upstreamStatus !== undefined || streamHandle?.upstreamContentRange) {
                console.warn(
                  `[StorageStreaming] 上游 Range 证据: upstreamStatus=${streamHandle?.upstreamStatus ?? "?"}, contentRange=${streamHandle?.upstreamContentRange ?? ""}`,
                );
              }

              const fallbackHeaders = buildResponseHeaders(descriptor, null, channel);
              return {
                stream: streamHandle.stream,
                overrideStatus: 200,
                overrideHeaders: fallbackHeaders,
                async close() {
                  await streamHandle?.close?.();
                },
              };
            }

            console.log(`[StorageStreaming] 检测到驱动不支持 Range，使用 ByteSliceStream 切片: ${start}-${end}`);
            if (streamHandle?.upstreamStatus !== undefined || streamHandle?.upstreamContentRange) {
              console.warn(
                `[StorageStreaming] 上游 Range 证据: upstreamStatus=${streamHandle?.upstreamStatus ?? "?"}, contentRange=${streamHandle?.upstreamContentRange ?? ""}`,
              );
            }
            if (start > 0) {
              const mb = Math.round((start / 1024 / 1024) * 10) / 10;
              console.warn(`[StorageStreaming] 警告：该切片需要先读取并丢弃前 ${start} 字节（约 ${mb}MB），大跳转会很费流量且很慢`);
            }

            const originalStream = streamHandle.stream;
            const originalClose = streamHandle.close;
            const slicedStream = smartWrapStreamWithByteSlice(originalStream, start, end);

            return {
              stream: slicedStream,
              softwareSlice: true,
              async close() {
                if (originalClose) {
                  await originalClose();
                }
              },
            };
          }

          // 兼容：有些上游会返回 200 但带 Content-Range（我们在 StreamDescriptorUtils 里会判定 supportsRange=true）
          // 这里额外打一个提示日志，方便你排查“上游到底回了啥”。
          if (supportsRange && streamHandle?.upstreamStatus === 200 && streamHandle?.upstreamContentRange) {
            console.log(
              `[StorageStreaming] 上游 Range 兼容模式：status=200 但 Content-Range 存在（按 start 匹配视为 Range 生效）`,
            );
          }

          // 驱动原生支持 Range，直接返回
          return streamHandle;
        }

        // 驱动不支持 getRange 方法，降级使用 ByteSliceStream
        const fallbackPolicy = descriptor?.rangeFallbackPolicy || "software";
        if (fallbackPolicy === "full") {
          console.log(`[StorageStreaming] 驱动不支持 getRange 方法，降级为 200 全量响应（忽略 Range）: ${start}-${end}`);
          streamHandle = await descriptor.getStream();
          const fallbackHeaders = buildResponseHeaders(descriptor, null, channel);
          return {
            stream: streamHandle.stream,
            overrideStatus: 200,
            overrideHeaders: fallbackHeaders,
            async close() {
              await streamHandle?.close?.();
            },
          };
        }

        console.log(`[StorageStreaming] 驱动不支持 getRange 方法，使用 ByteSliceStream 切片: ${start}-${end}`);
        if (start > 0) {
          const mb = Math.round((start / 1024 / 1024) * 10) / 10;
          console.warn(`[StorageStreaming] 警告：该切片需要先读取并丢弃前 ${start} 字节（约 ${mb}MB），大跳转会很费流量且很慢`);
        }

        streamHandle = await descriptor.getStream();
        const originalStream = streamHandle.stream;
        const originalClose = streamHandle.close;
        const slicedStream = smartWrapStreamWithByteSlice(originalStream, start, end);

        return {
          stream: slicedStream,
          softwareSlice: true,
          async close() {
            if (originalClose) {
              await originalClose();
            }
          },
        };
      },
      async close() {
        if (closed) return;
        closed = true;
        if (streamHandle) {
          await streamHandle.close();
        }
      },
    };
  }

  /**
   * 创建 206 Multi-Range (multipart/byteranges) RangeReader
   * 注意：只在“驱动原生支持 getRange 且上游真正返回 206”时使用。
   *
   * @private
   */
  _create206MultiRangeReader(descriptor, ranges, channel) {
    let closed = false;
    let activeHandle = null;
    const boundary = `cp_${Math.random().toString(16).slice(2)}`;

    const headers = buildResponseHeaders(descriptor, null, channel);
    headers.set("Content-Type", `multipart/byteranges; boundary=${boundary}`);
    headers.delete("Content-Length");
    headers.delete("Content-Range");

    const encoder = new TextEncoder();

    const closeActive = async () => {
      try {
        await activeHandle?.close?.();
      } catch {
        // ignore
      } finally {
        activeHandle = null;
      }
    };

    return {
      status: 206,
      headers,
      async getBody() {
        if (closed) return null;
        if (typeof descriptor.getRange !== "function") {
          throw new DriverError("Multi-Range 需要驱动实现 getRange", {
            status: ApiStatus.INTERNAL_ERROR,
            code: "STREAMING_ERROR.MULTI_RANGE_NO_GETRANGE",
          });
        }

        const stream = new ReadableStream({
          async start(controller) {
            try {
              for (const r of ranges) {
                const contentType = descriptor.contentType || "application/octet-stream";
                const partHeader =
                  `--${boundary}\r\n` +
                  `Content-Type: ${contentType}\r\n` +
                  `Content-Range: bytes ${r.start}-${r.end}/${descriptor.size}\r\n` +
                  `\r\n`;
                controller.enqueue(encoder.encode(partHeader));

                activeHandle = await descriptor.getRange(r);
                const partStream = activeHandle?.stream;
                const webPartStream =
                  partStream && typeof partStream.getReader === "function"
                    ? partStream
                    : partStream && (typeof partStream.pipe === "function" || typeof partStream.on === "function")
                      ? await wrapNodeReadableToWebStream(partStream)
                      : partStream && typeof partStream[Symbol.asyncIterator] === "function"
                        ? new ReadableStream({
                            async start(inner) {
                              try {
                                for await (const chunk of partStream) {
                                  const data =
                                    chunk instanceof Uint8Array
                                      ? chunk
                                      : typeof chunk === "string"
                                        ? Buffer.from(chunk)
                                        : new Uint8Array(chunk);
                                  inner.enqueue(data);
                                }
                                inner.close();
                              } catch (e) {
                                inner.error(e);
                              }
                            },
                          })
                        : null;

                if (!webPartStream) {
                  throw new Error("Multi-Range part stream 类型不支持");
                }

                const reader = webPartStream.getReader();
                try {
                  while (true) {
                    const { done, value } = await reader.read();
                    if (done) break;
                    const data = value instanceof Uint8Array ? value : new Uint8Array(value);
                    controller.enqueue(data);
                  }
                } finally {
                  try {
                    reader.releaseLock?.();
                  } catch {
                    // ignore
                  }
                }

                await closeActive();
                controller.enqueue(encoder.encode("\r\n"));
              }

              controller.enqueue(encoder.encode(`--${boundary}--\r\n`));
              controller.close();
            } catch (e) {
              await closeActive();
              controller.error(e);
            }
          },
          async cancel() {
            await closeActive();
          },
        });

        return {
          stream,
          async close() {
            await closeActive();
          },
        };
      },
      async close() {
        if (closed) return;
        closed = true;
        await closeActive();
      },
    };
  }

  /**
   * 创建 304 Not Modified RangeReader
   * @private
   */
  _create304Reader(descriptor, channel) {
    // 304 也应携带缓存相关头（Cache-Control 等），否则客户端可能无法更新缓存策略
    const headers = buildResponseHeaders(descriptor, null, channel);
    // 304 不应返回实体相关头（例如 Content-Length/Content-Type）
    headers.delete("Content-Length");
    headers.delete("Content-Type");
    headers.delete("Content-Range");

    return {
      status: 304,
      headers,
      async getBody() {
        return null;
      },
      async close() {},
    };
  }

  /**
   * 创建 412 Precondition Failed RangeReader
   * @private
   */
  _create412Reader(descriptor, channel) {
    const headers = new Headers();
    if (descriptor.etag) headers.set("ETag", descriptor.etag);
    if (descriptor.lastModified) headers.set("Last-Modified", descriptor.lastModified.toUTCString());

    return {
      status: 412,
      headers,
      async getBody() {
        return null;
      },
      async close() {},
    };
  }

  /**
   * 创建 416 Range Not Satisfiable RangeReader
   * @private
   */
  _create416Reader(descriptor, channel) {
    const headers = new Headers();
    if (typeof descriptor.size === "number" && descriptor.size > 0) {
      headers.set("Content-Range", `bytes */${descriptor.size}`);
    }

    return {
      status: 416,
      headers,
      async getBody() {
        return null;
      },
      async close() {},
    };
  }
}

export { STREAMING_CHANNELS };
