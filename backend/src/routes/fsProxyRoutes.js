/**
 * 文件系统代理路由
 * 处理/p/*路径的文件访问请求
 * 专门用于web_proxy功能的文件代理访问
 * - Range/条件请求由 StorageStreaming 统一处理
 */

import { Hono } from "hono";
import crypto from "crypto";
import { AppError, AuthenticationError, AuthorizationError, DriverError } from "../http/errors.js";
import { ApiStatus } from "../constants/index.js";
import { MountManager } from "../storage/managers/MountManager.js";
import { findMountPointByPathForProxy } from "../storage/fs/utils/MountResolver.js";
import { PROXY_CONFIG, safeDecodeProxyPath } from "../constants/proxy.js";
import { ProxySignatureService } from "../services/ProxySignatureService.js";
import { getEncryptionSecret } from "../utils/environmentUtils.js";
import { getQueryBool } from "../utils/common.js";
import { CAPABILITIES } from "../storage/interfaces/capabilities/index.js";
import { StorageStreaming, STREAMING_CHANNELS } from "../storage/streaming/index.js";
import { isWebProxyEnabled } from "../security/helpers/proxyAccess.js";

// 签名代理路径不会走 RBAC，因此这里用结构化日志补充最少可观测性。
const emitProxyAudit = (c, details) => {
  const payload = {
    type: "proxy.audit",
    reqId: c.get?.("reqId") ?? null,
    path: details.path,
    decision: details.decision,
    reason: details.reason ?? null,
    signatureRequired: details.signatureRequired ?? false,
    signatureProvided: details.signatureProvided ?? false,
    mountId: details.mountId ?? null,
    timestamp: new Date().toISOString(),
  };

  console.log(JSON.stringify(payload));
};

const rewriteHlsM3u8ForSignature = (playlistText, options) => {
  const rawText = String(playlistText || "");
  const newline = rawText.includes("\r\n") ? "\r\n" : "\n";
  const lines = rawText.split(/\r?\n/);

  const decodeMulti = (value, maxTimes = 3) => {
    let out = String(value || "");
    for (let i = 0; i < maxTimes; i++) {
      try {
        const next = decodeURIComponent(out);
        if (next === out) break;
        out = next;
      } catch {
        break;
      }
    }
    return out;
  };

  const normalizeFsPath = (p) => {
    const raw = String(p || "").trim();
    if (!raw) return "/";
    const withLeading = raw.startsWith("/") ? raw : `/${raw}`;
    return withLeading.replace(/\/{2,}/g, "/");
  };

  const hasPathTraversal = (p) => String(p || "").split("/").includes("..");
  const hasExternalScheme = (u) => /^[a-zA-Z][a-zA-Z0-9+.-]*:\/\//.test(u) || String(u || "").startsWith("//");
  const hasSignParam = (u) => /(?:[?&])sign=/.test(String(u || ""));

  const playlistFsPath = normalizeFsPath(options.playlistFsPath || "/");
  const baseDir = (() => {
    const idx = playlistFsPath.lastIndexOf("/");
    return idx >= 0 ? playlistFsPath.slice(0, idx + 1) : "/";
  })();

  const expireTimestamp = options.expireTimestamp;
  const ts = options.ts;
  const secret = options.secret;

  const signForPath = (fsPath) => {
    const signData = `${fsPath}:${expireTimestamp}`;
    const hmac = crypto.createHmac("sha256", secret);
    hmac.update(signData);
    const hash = hmac.digest("base64");
    return `${hash}:${expireTimestamp}`;
  };

  const appendSignQuery = (uri, signature) => {
    if (!uri || hasSignParam(uri)) return uri;
    const raw = String(uri);
    const hashIndex = raw.indexOf("#");
    const beforeHash = hashIndex >= 0 ? raw.slice(0, hashIndex) : raw;
    const hash = hashIndex >= 0 ? raw.slice(hashIndex) : "";
    const qIndex = beforeHash.indexOf("?");
    const pathPart = qIndex >= 0 ? beforeHash.slice(0, qIndex) : beforeHash;
    const queryPart = qIndex >= 0 ? beforeHash.slice(qIndex + 1) : "";

    const extra =
      `${PROXY_CONFIG.SIGN_PARAM}=${encodeURIComponent(String(signature || ""))}` +
      (ts ? `&${PROXY_CONFIG.TIMESTAMP_PARAM}=${encodeURIComponent(String(ts))}` : "");

    return queryPart ? `${pathPart}?${queryPart}&${extra}${hash}` : `${pathPart}?${extra}${hash}`;
  };

  const resolveFsPath = (uriCore) => {
    const core = String(uriCore || "").trim();
    if (!core) return null;

    const noQh = core.split("#")[0].split("?")[0];

    // 支持 m3u8 里直接写 /api/p 或 /proxy/fs 的情况
    const apiPIdx = noQh.indexOf("/api/p/");
    if (apiPIdx >= 0) {
      return normalizeFsPath(decodeMulti(noQh.slice(apiPIdx + "/api/p".length)));
    }
    const proxyFsIdx = noQh.indexOf("/proxy/fs/");
    if (proxyFsIdx >= 0) {
      return normalizeFsPath(decodeMulti(noQh.slice(proxyFsIdx + "/proxy/fs".length)));
    }

    if (hasExternalScheme(noQh)) return null;

    const decodedPath = decodeMulti(noQh);
    const fsPath = decodedPath.startsWith("/") ? normalizeFsPath(decodedPath) : normalizeFsPath(`${baseDir}${decodedPath}`);
    if (hasPathTraversal(fsPath)) return null;
    return fsPath;
  };

  const rewriteSingleUri = (uri) => {
    const raw = String(uri || "");
    const leading = raw.match(/^\s*/)?.[0] || "";
    const trailing = raw.match(/\s*$/)?.[0] || "";
    const core = raw.trim();

    if (!core || core.startsWith("#") || hasSignParam(core)) return { uri: raw, changed: false };

    const fsPath = resolveFsPath(core);
    if (!fsPath) return { uri: raw, changed: false };

    const signed = appendSignQuery(core, signForPath(fsPath));
    return { uri: `${leading}${signed}${trailing}`, changed: signed !== core };
  };

  const rewriteTagLineUriAttributes = (line) => {
    const raw = String(line || "");
    const re = /\bURI\s*=\s*("[^"]*"|'[^']*'|[^,\s]+)/g;
    const matches = Array.from(raw.matchAll(re));
    if (!matches.length) return { line: raw, changed: false, signedCount: 0 };

    let out = "";
    let last = 0;
    let changed = false;
    let signedCount = 0;

    for (const m of matches) {
      const full = m[0];
      const token = m[1] || "";
      const start = m.index ?? 0;

      out += raw.slice(last, start);

      const quote = token.startsWith("\"") ? "\"" : token.startsWith("'") ? "'" : "";
      const inner = quote ? token.slice(1, -1) : token;
      const rewritten = rewriteSingleUri(inner);
      if (rewritten.changed) {
        changed = true;
        signedCount += 1;
      }
      out += `URI=${quote}${rewritten.uri.trim()}${quote}`;
      last = start + full.length;
    }

    out += raw.slice(last);
    return { line: out, changed, signedCount };
  };

  let changed = false;
  let signedCount = 0;
  const out = [];

  for (const line of lines) {
    const rawLine = String(line ?? "");
    if (!rawLine) {
      out.push(rawLine);
      continue;
    }

    if (rawLine.startsWith("#")) {
      const rewrittenTag = rewriteTagLineUriAttributes(rawLine);
      out.push(rewrittenTag.line);
      if (rewrittenTag.changed) changed = true;
      signedCount += rewrittenTag.signedCount;
      continue;
    }

    const rewritten = rewriteSingleUri(rawLine);
    out.push(rewritten.uri);
    if (rewritten.changed) {
      changed = true;
      signedCount += 1;
    }
  }

  return { text: out.join(newline), changed, signedCount };
};

const fsProxyRoutes = new Hono();

/**
 * 处理OPTIONS预检请求 - 代理路由
 */
fsProxyRoutes.options(`${PROXY_CONFIG.ROUTE_PREFIX}/*`, (c) => {
  // CORS头部将由全局CORS中间件自动处理
  return c.text("", 204); // No Content
});

/**
 * 处理文件代理访问
 * 路径格式：/p/mount/path/file.ext?download=true
 *
 */
fsProxyRoutes.get(`${PROXY_CONFIG.ROUTE_PREFIX}/*`, async (c) => {
  const run = async () => {
    const url = new URL(c.req.url);
    const fullPath = url.pathname;
    const rawPath = fullPath.replace(new RegExp(`^${PROXY_CONFIG.ROUTE_PREFIX}`), "") || "/";
    const path = safeDecodeProxyPath(rawPath);
    const download = getQueryBool(c, "download", false);
    const db = c.env.DB;
    const encryptionSecret = getEncryptionSecret(c);

    console.log(`[fsProxy] 代理访问: ${path}`);

    // 查找挂载点（已在MountResolver中验证web_proxy配置）
    const mountResult = await findMountPointByPathForProxy(db, path);

    if (mountResult.error) {
      console.warn(`代理访问失败 - 挂载点查找失败: ${mountResult.error.message}`);
      emitProxyAudit(c, {
        path,
        decision: "deny",
        reason: "mount_lookup_failed",
        signatureRequired: false,
        signatureProvided: Boolean(c.req.query(PROXY_CONFIG.SIGN_PARAM)),
      });
      const status = mountResult.error.status;
      const code = status === 401 ? "UNAUTHORIZED" : status === 403 ? "FORBIDDEN" : status === 404 ? "NOT_FOUND" : "PROXY_ERROR";
      throw new AppError(mountResult.error.message, { status, code, expose: true });
    }

    // /api/p is the public entry point for mounts explicitly configured for
    // web proxying. Without this check, any active mount could be streamed by
    // using the special proxy user type below, even when web_proxy is off.
    if (!isWebProxyEnabled(mountResult.mount)) {
      emitProxyAudit(c, {
        path,
        decision: "deny",
        reason: "web_proxy_disabled",
        signatureRequired: false,
        signatureProvided: Boolean(c.req.query(PROXY_CONFIG.SIGN_PARAM)),
        mountId: mountResult.mount?.id ?? null,
      });
      throw new AuthorizationError("该挂载未启用代理访问");
    }

    // 挂载点验证成功，mountResult包含mount和subPath信息

    // 检查是否需要签名验证
    const repositoryFactory = c.get("repos");
    const signatureService = new ProxySignatureService(db, encryptionSecret, repositoryFactory);
    const signatureNeed = await signatureService.needsSignature(mountResult.mount);

    const signature = c.req.query(PROXY_CONFIG.SIGN_PARAM) || null;
    const signatureProvided = !!signature;

    // 1) 若挂载要求签名：必须提供签名
    // 2) 若“主动提供了签名”：即使挂载不要求，也验证一下（避免误用/脏数据）

    if (signatureNeed.required && !signatureProvided) {
      console.warn(`代理访问失败 - 缺少签名: ${path} (${signatureNeed.reason})`);
      emitProxyAudit(c, {
        path,
        decision: "deny",
        reason: "missing_signature",
        signatureRequired: true,
        signatureProvided: false,
        mountId: mountResult.mount.id,
      });
      throw new AuthenticationError(`此文件需要签名访问 (${signatureNeed.description})`);
    }

    if (signatureProvided) {
      const verifyResult = signatureService.verifyStorageSignature(path, signature);
      if (!verifyResult.valid) {
        console.warn(`代理访问失败 - 签名验证失败: ${path} (${verifyResult.reason})`);
        emitProxyAudit(c, {
          path,
          decision: "deny",
          reason: "invalid_signature",
          signatureRequired: signatureNeed.required,
          signatureProvided: true,
          mountId: mountResult.mount.id,
        });
        throw new AuthenticationError(`签名验证失败: ${verifyResult.reason}`);
      }
      console.log(`[fsProxy] 签名验证成功: ${path}`);
    }

    // 创建 MountManager 并验证驱动能力
    const mountManager = new MountManager(db, encryptionSecret, repositoryFactory, { env: c.env });
    const driver = await mountManager.getDriver(mountResult.mount);
    if (!driver.hasCapability(CAPABILITIES.PROXY)) {
      throw new AppError("当前存储驱动不支持代理访问", { status: ApiStatus.NOT_IMPLEMENTED, code: "PROXY_NOT_SUPPORTED", expose: true });
    }

    // 获取文件名用于下载
    const fileName = path.split("/").filter(Boolean).pop() || "file";

    // 使用 StorageStreaming 层统一处理内容访问
    const streaming = new StorageStreaming({
      mountManager,
      storageFactory: null,
      encryptionSecret,
    });

    // 获取 Range 头
    const rangeHeader = c.req.header("Range") || null;

    // 通过 StorageStreaming 创建响应
    let response = await streaming.createResponse({
      path,
      channel: STREAMING_CHANNELS.PROXY,
      rangeHeader,
      request: c.req.raw,
      userIdOrInfo: PROXY_CONFIG.USER_TYPE,
      userType: PROXY_CONFIG.USER_TYPE,
      db,
    });

    // HLS 特殊处理：当启用签名访问时，m3u8 内的分片/子播放列表/key 等资源如果不带 sign，会导致后续请求 401。
    // 因此：在返回 m3u8 内容时，给所有可识别的 URI 追加 ?sign=...&ts=...
    // 仅在以下条件下重写：
    // - 当前响应为 200
    // - 非 Range 请求（Range 下重写会导致内容不完整）
    // - 非 download 模式（download 语义下无需保证播放器可播放）
    // - 当前路径看起来是 .m3u8
    if (
      signatureNeed.required &&
      !download &&
      !rangeHeader &&
      response &&
      response.status === 200 &&
      typeof path === "string" &&
      path.toLowerCase().endsWith(".m3u8")
    ) {
      try {
        const originalText = await response.clone().text();
        const requestTs = c.req.query(PROXY_CONFIG.TIMESTAMP_PARAM) || Date.now();
        const expireTimestampStr = String(signature || "").split(":")[1];
        const expireTimestamp = parseInt(expireTimestampStr, 10);
        if (Number.isNaN(expireTimestamp)) {
          throw new Error("malformed_signature");
        }

        const rewriteResult = rewriteHlsM3u8ForSignature(originalText, {
          playlistFsPath: path,
          expireTimestamp,
          ts: requestTs,
          secret: encryptionSecret,
        });

        if (rewriteResult.changed) {
          const headers = new Headers(response.headers);
          headers.delete("content-length");
          headers.delete("content-encoding");
          headers.delete("etag");
          headers.delete("last-modified");
          headers.set("cache-control", "no-store");
          if (!headers.get("content-type")) {
            headers.set("content-type", "application/vnd.apple.mpegurl; charset=utf-8");
          }

          response = new Response(rewriteResult.text, { status: response.status, headers });
          console.log(`[fsProxy][hls] 已重写 m3u8 URI: ${path} (+${rewriteResult.signedCount})`);
        }
      } catch (e) {
        console.warn("[fsProxy][hls] 重写 m3u8 失败，将返回原始内容：", e?.message || e);
      }
    }

    // 如果是下载模式，覆盖 Content-Disposition 头
    if (download) {
      const downloadDisposition = `attachment; filename="${encodeURIComponent(fileName)}"`;
      response.headers.set("Content-Disposition", downloadDisposition);
      c.header("Content-Disposition", downloadDisposition);
    }

    // 复制响应头到 Hono context（用于 CORS 中间件）
    for (const [key, value] of response.headers.entries()) {
      if (!["access-control-allow-origin", "access-control-allow-credentials", "access-control-expose-headers"].includes(key.toLowerCase())) {
        c.header(key, value);
      }
    }

    // 仅在非200状态码时记录详细信息
    if (response.status !== 200 && response.status !== 206) {
      console.log(`[fsProxy] 响应状态: ${response.status} -> ${path}`);
    }

    emitProxyAudit(c, {
      path,
      decision: "allow",
      reason: signatureNeed.required ? "signature_valid" : "signature_not_required",
      signatureRequired: signatureNeed.required,
      signatureProvided: signatureNeed.required ? Boolean(c.req.query(PROXY_CONFIG.SIGN_PARAM)) : false,
      mountId: mountResult.mount.id,
    });

    return response;
  };

  return run().catch((error) => {
    console.error("文件系统代理访问错误:", error);

    if (!(error instanceof AppError)) {
      const signatureParam = typeof c.req?.query === "function" ? c.req.query(PROXY_CONFIG.SIGN_PARAM) : null;
      emitProxyAudit(c, {
        path: c.req?.path ?? null,
        decision: "deny",
        reason: "internal_error",
        signatureRequired: false,
        signatureProvided: Boolean(signatureParam),
      });
      throw new DriverError("代理访问失败", { details: { cause: error?.message } });
    }

    throw error;
  });
});

export { fsProxyRoutes };
