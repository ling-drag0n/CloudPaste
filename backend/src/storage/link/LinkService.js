// LinkService: 统一封装分享视图与 FS 视图下的存储访问链接生成
// - 对外只暴露 StorageLink（见 LinkTypes）
// - 直链/代理能力由底层驱动与现有策略提供，这里只做归一与映射

import { StorageFactory } from "../factory/StorageFactory.js";
import { ObjectStore } from "../object/ObjectStore.js";
import { resolveStorageLinks } from "../object/ObjectLinkStrategy.js";
import { MountManager } from "../managers/MountManager.js";
import { FileSystem } from "../fs/FileSystem.js";
import { createDirectLink, createProxyLink } from "./LinkTypes.js";
import { findMountPointByPathForProxy } from "../fs/utils/MountResolver.js";
import { ProxySignatureService } from "../../services/ProxySignatureService.js";
import { WORKER_ENTRY, buildSignedProxyUrl, buildSignedWorkerUrl } from "../../constants/proxy.js";
import { UserType } from "../../constants/index.js";
import { AuthorizationError } from "../../http/errors.js";
import { getAccessibleMountsForUser } from "../../security/helpers/access.js";
import { isMountAccessible } from "../../security/helpers/proxyAccess.js";

/**
 * 将文件名转为 URL path segment 安全的形式（用于 /api/s/:slug/:filename）。
 * - 不能包含 / 或 \\（否则会被当作路径分隔）
 * - 空值时回退为 "file"
 * @param {string} filename
 * @returns {string}
 */
function toUrlSafeFilename(filename) {
  const raw = String(filename || "").trim();
  if (!raw) return "file";
  return raw.replace(/[\\/]+/g, "_");
}

/**
 * 构造分享内容的本地代理入口（带 filename 以兼容第三方预览器按 URL 后缀识别类型）
 * - 例如：/api/s/AdhhLX/Samba%20Dancing.fbx
 * - 下载语义：追加 ?down=true
 * @param {string} slug
 * @param {string} filename
 * @param {{ download?: boolean, password?: string }} [options]
 * @returns {string}
 */
function buildShareProxyPath(slug, filename, options = {}) {
  const safeName = toUrlSafeFilename(filename);
  const base = `/api/s/${encodeURIComponent(String(slug || ""))}/${encodeURIComponent(safeName)}`;
  const params = new URLSearchParams();
  if (options.download) {
    params.set("down", "true");
  }
  if (typeof options.password === "string" && options.password.length > 0) {
    params.set("password", options.password);
  }
  const query = params.toString();
  return query ? `${base}?${query}` : base;
}

/**
 * 规范化驱动上游返回的 headers，统一为 Record<string,string[]> 结构
 * @param {any} rawHeaders
 * @returns {Record<string,string[]>|undefined}
 */
function normalizeUpstreamHeaders(rawHeaders) {
  if (!rawHeaders || typeof rawHeaders !== "object") {
    return undefined;
  }
  const result = {};
  for (const [key, value] of Object.entries(rawHeaders)) {
    if (Array.isArray(value)) {
      result[key] = value.map((v) => String(v));
    } else if (value != null) {
      result[key] = [String(value)];
    }
  }
  return Object.keys(result).length > 0 ? result : undefined;
}


export class LinkService {
  /**
   * @param {D1Database} db
   * @param {string} encryptionSecret
   * @param {import('../..//utils/repositories.js').RepositoryFactory} repositoryFactory
   */
  constructor(db, encryptionSecret, repositoryFactory) {
    this.db = db;
    this.encryptionSecret = encryptionSecret;
    this.repositoryFactory = repositoryFactory;
  }

  // ===== 分享视图（slug → storage_config + storage_path） =====

  /**
   * 分享视图：生成“外部访问入口”链接（供浏览器/前端使用）
   * - 根据 use_proxy / 存储直链能力 / url_proxy 等配置，生成最终可对外暴露的 URL
   * @param {Object} file
   * @param {{ type?: string, id?: string } | null} userInfo
   * @param {Object} [options]
   * @returns {Promise<import("./LinkTypes.js").StorageLink>}
   */
  async getShareExternalLink(file, userInfo = null, options = {}) {
    return this.getLinkForShare(file, userInfo, { ...options, mode: "client" });
  }

  /**
   * 分享视图：生成“上游访问目标”链接（供反向代理/Proxy 调用 CloudPaste 时使用）
   * - 统一返回存储直链或 CloudPaste 自身的 /api/s/:slug 下载地址
   * @param {Object} file
   * @param {{ type?: string, id?: string } | null} userInfo
   * @param {Object} [options]
   * @returns {Promise<import("./LinkTypes.js").StorageLink>}
   */
  async getShareUpstreamLink(file, userInfo = null, options = {}) {
    return this.getLinkForShare(file, userInfo, { ...options, mode: "proxy" });
  }

  // 内部方法：根据分享文件记录生成 StorageLink，mode 仅用于区分“外部入口”和“上游目标”
  /**
   * @param {Object} file
   * @param {{ type?: string, id?: string } | null} userInfo
   * @param {{ mode?: "client" | "proxy", request?: Request, password?: string }} [options]
   * @returns {Promise<import('./LinkTypes.js').StorageLink>}
   */
  async getLinkForShare(file, userInfo = null, options = {}) {
    const mode = options.mode === "proxy" ? "proxy" : "client";
    const request = options.request || null;
    const forceDownload = !!options.forceDownload;

    // 本地代理开关（share 视图使用 use_proxy 与 FS 的 web_proxy 语义一致）
    const useProxyFlag = file?.use_proxy ?? 0;
    const slug = file?.slug || null;

    // 分享记录缺少存储信息时，只能走应用层代理链路
    if (!file || !file.storage_config_id || !file.storage_path || !file.storage_type || !slug) {
      if (mode === "proxy") {
        // 兜底：直接返回 /api/s 下载路径，交由上层处理
        const shareDownloadPath = buildShareProxyPath(slug || "", "file", {
          download: true,
          password: options.password,
        });
        let finalUrl = shareDownloadPath;
        if (request) {
          try {
            const base = new URL(request.url);
            const origin = `${base.protocol}//${base.host}`;
            finalUrl = new URL(shareDownloadPath, origin).toString();
          } catch (e) {
            console.warn("构建分享下载绝对 URL 失败，将返回相对路径：", e?.message || e);
          }
        }
        return createProxyLink(finalUrl);
      }
      // client 模式下没有存储信息和 slug 时，不提供任何 URL
      return createProxyLink("");
    }

    const objectStore = new ObjectStore(this.db, this.encryptionSecret, this.repositoryFactory);
    const storageConfig = await objectStore._getStorageConfig(file.storage_config_id);
    const driver = await StorageFactory.createDriver(storageConfig.storage_type, storageConfig, this.encryptionSecret);

    let previewDirectUrl = "";
    let downloadDirectUrl = "";

    try {
      const links = await resolveStorageLinks({
        driver,
        storageConfig,
        path: file.storage_path,
        request: null,
        forceDownload: true,
        userType: userInfo?.type || null,
        userId: userInfo?.id || null,
      });

      const preview = links?.preview || null;
      const download = links?.download || null;

      if (preview && (preview.type === "custom_host" || preview.type === "native_direct") && preview.url) {
        previewDirectUrl = preview.url;
      }
      if (download && (download.type === "custom_host" || download.type === "native_direct") && download.url) {
        downloadDirectUrl = download.url;
      }
    } catch (e) {
      console.warn("解析分享存储直链失败，将退回代理链路：", e?.message || e);
    }

    const urlProxy = storageConfig?.url_proxy || null;

    // mode=client：生成对外入口 URL
    if (mode === "client") {
      // 1) use_proxy = 1：本地 share 内容路由，忽略 url_proxy 与直链能力
      if (useProxyFlag) {
        const sharePath = buildShareProxyPath(slug, file?.filename || "file", { download: !!forceDownload });
        console.log(`[LinkService][share][local-proxy] 文件(${file.id || slug}) 使用本地 /api/s 内容路由: ${sharePath}`);
        return createProxyLink(sharePath);
      }

      // 2) use_proxy = 0 且配置了 url_proxy：使用 Worker / 反代入口
      if (urlProxy) {
        try {
          const entryPath = `${WORKER_ENTRY.SHARE_PREFIX}/${encodeURIComponent(slug)}${forceDownload ? "?down=true" : ""}`;
          const workerUrl = buildSignedWorkerUrl(urlProxy, entryPath, {});
          console.log(
            `[LinkService][share][url_proxy] 文件(${file.id || slug}) 使用 url_proxy=${urlProxy} 生成入口: ${workerUrl}`,
          );
          return createProxyLink(workerUrl);
        } catch (e) {
          console.warn("构建分享 Worker 入口链接失败，将退回为本地 share 内容路由：", e?.message || e);
          const fallbackPath = buildShareProxyPath(slug, file?.filename || "file", { download: !!forceDownload });
          return createProxyLink(fallbackPath);
        }
      }

      // 3) 无 url_proxy：有直链则使用直链，否则不提供 URL
      if (previewDirectUrl || (forceDownload && downloadDirectUrl)) {
        const directUrl = forceDownload ? (downloadDirectUrl || previewDirectUrl) : previewDirectUrl;
        if (directUrl) {
          console.log(
            `[LinkService][share][direct] 文件(${file.id || slug}) 使用存储直链作为${forceDownload ? "下载" : "预览"}入口: ${directUrl}`,
          );
          return createDirectLink(directUrl);
        }
      }

      // 没有直链能力且未配置 url_proxy 时，不再兜底本地 /api/s
      // 这会强制要求：要么开启 use_proxy，要么配置 url_proxy，否则分享视图不暴露任何外部入口
      return createProxyLink("");
    }

    // mode=proxy：为 /api/proxy/link(type=share) 等生成“上游访问目标”

    // 1) use_proxy = 1：统一走本地 share 内容路由（下载语义）
    if (useProxyFlag) {
      const shareDownloadPath = buildShareProxyPath(slug, file?.filename || "file", {
        download: true,
        password: options.password,
      });
      let finalUrl = shareDownloadPath;
      if (request) {
        try {
          const base = new URL(request.url);
          const origin = `${base.protocol}//${base.host}`;
          finalUrl = new URL(shareDownloadPath, origin).toString();
        } catch (e) {
          console.warn("构建分享下载绝对 URL 失败，将返回相对路径：", e?.message || e);
        }
      }
      console.log(
        `[LinkService][share][local-proxy] 文件(${file.id || slug}) 上游使用本地 /api/s 下载链路: ${finalUrl}`,
      );
      return createProxyLink(finalUrl);
    }

    // 2) use_proxy = 0 且驱动具备上游 HTTP 能力时，优先交给驱动返回 {url, headers}，由反代直连存储
    try {
      if (typeof driver.generateUpstreamRequest === "function") {
        const upstream = await driver.generateUpstreamRequest(file.storage_path, {
          subPath: file.storage_path,
          request,
          userType: userInfo?.type || null,
          userId: userInfo?.id || null,
        });
        if (upstream && upstream.url) {
          const headers = normalizeUpstreamHeaders(upstream.headers);
          console.log(
            `[LinkService][share][upstream-http] 文件(${file.id || slug}) 上游使用驱动 UPSTREAM_HTTP 能力: ${upstream.url}`,
          );
          return createProxyLink(upstream.url, { headers });
        }
      }
    } catch (e) {
      console.warn(
        "[LinkService][share][upstream-http] 构建上游请求失败，将回退直链/本地代理链路：",
        e?.message || e,
      );
    }

    // 3) use_proxy = 0：有直链能力时直接返回存储直链，否则退回本地 /api/s/:slug?down=true
    if (downloadDirectUrl) {
      console.log(
        `[LinkService][share][direct] 文件(${file.id || slug}) 上游使用存储直链下载: ${downloadDirectUrl}`,
      );
      return createDirectLink(downloadDirectUrl);
    }

    const shareDownloadPath = buildShareProxyPath(slug, file?.filename || "file", {
      download: true,
      password: options.password,
    });
    let finalUrl = shareDownloadPath;
    if (request) {
      try {
        const base = new URL(request.url);
        const origin = `${base.protocol}//${base.host}`;
        finalUrl = new URL(shareDownloadPath, origin).toString();
      } catch (e) {
        console.warn("构建分享下载绝对 URL 失败，将返回相对路径：", e?.message || e);
      }
    }
    console.log(
      `[LinkService][share][fallback] 文件(${file.id || slug}) 无直链能力，上游退回本地 /api/s 下载链路: ${finalUrl}`,
    );
    return createProxyLink(finalUrl);
  }

  // ===== FS 视图（挂载路径） =====

  /**
   * FS 视图：生成“外部访问入口”链接（供浏览器/前端使用）
   * - 根据 web_proxy/url_proxy/直链能力，生成最终可对外暴露的 URL
   * @param {string} path
   * @param {any} userIdOrInfo
   * @param {string} userType
   * @param {Object} [options]
   * @returns {Promise<import("./LinkTypes.js").StorageLink>}
   */
  async getFsExternalLink(path, userIdOrInfo, userType, options = {}) {
    return this.getLinkForFs(path, userIdOrInfo, userType, { ...options, mode: "client" });
  }

  /**
   * FS 视图：生成“上游访问目标”链接（供反向代理/Proxy 调用 CloudPaste 时使用）
   * - 统一返回存储直链或 CloudPaste 自身的 /api/p 代理地址
   * @param {string} path
   * @param {any} userIdOrInfo
   * @param {string} userType
   * @param {Object} [options]
   * @returns {Promise<import("./LinkTypes.js").StorageLink>}
   */
  async getFsUpstreamLink(path, userIdOrInfo, userType, options = {}) {
    return this.getLinkForFs(path, userIdOrInfo, userType, { ...options, mode: "proxy" });
  }

  // 内部方法：为 FS 路径生成 StorageLink，mode 仅用于区分“外部入口”和“上游目标”
  /**
   * @param {string} path
   * @param {any} userIdOrInfo
   * @param {string} userType
   * @param {{ mode?: "client" | "proxy", request?: Request }} [options]
   * @returns {Promise<import('./LinkTypes.js').StorageLink>}
   */
  async getLinkForFs(path, userIdOrInfo, userType, options = {}) {
    // mode 决定输出语义（仅在 LinkService 内部使用）：
    // - client: 返回给前端/浏览器使用的最终 URL（优先 url_proxy / 本地代理 / 直链）
    // - proxy:  返回给反向代理/Proxy 使用的上游 URL（S3 直链或本地 /api/p 签名代理）
    const mode = options.mode === "proxy" ? "proxy" : "client";

    // 解析挂载与存储配置，用于 web_proxy/url_proxy 决策
    let mountResult = null;
    let mount = null;
    let subPath = null;
    let storageConfig = null;
    let urlProxy = null;
    try {
      mountResult = await findMountPointByPathForProxy(this.db, path, this.repositoryFactory);
      if (!mountResult.error) {
        mount = mountResult.mount;
        subPath = mountResult.subPath || null;
        const storageConfigRepo = this.repositoryFactory?.getStorageConfigRepository?.();
        if (storageConfigRepo && mount.storage_config_id) {
          storageConfig = await storageConfigRepo.findById(mount.storage_config_id);
          urlProxy = storageConfig?.url_proxy || null;
        }
      }
    } catch (e) {
      console.warn("解析挂载或存储配置失败，将退回 FS 默认链接策略：", e?.message || e);
    }

    // The upstream-http branch below can return a presigned/direct URL without
    // going through FileSystem/MountManager. Re-run the API-key mount ACL here
    // so this optimization cannot bypass storage visibility or ACL filtering.
    if (mount && userType === UserType.API_KEY) {
      const accessibleMounts = await getAccessibleMountsForUser(
        this.db,
        userIdOrInfo,
        userType,
        this.repositoryFactory,
      );
      if (!isMountAccessible(mount, accessibleMounts)) {
        throw new AuthorizationError(`API密钥用户无权限访问挂载点: ${mount.name || mount.id}`);
      }
    }

    // 仅在挂载未开启 web_proxy 时才允许 url_proxy 生效：
    // - web_proxy = 1 → 强制由 CloudPaste 本地代理处理，不下放到外部反向代理
    // - allowUrlProxyOverride = true（例如 WebDAV use_proxy_url 场景）时忽略 web_proxy 限制
    const allowUrlProxyOverride = options.allowUrlProxyOverride === true;
    const hasUrlProxy = !!urlProxy && (!mount?.web_proxy || allowUrlProxyOverride);

    // mode=proxy 且驱动具备上游 HTTP 能力（例如 WebDAV）：优先交给驱动返回 {url, headers}，由反代直连存储
    if (mode === "proxy" && mount && mount.storage_config_id) {
      try {
        const objectStore = new ObjectStore(this.db, this.encryptionSecret, this.repositoryFactory);
        const upstreamStorageConfig = await objectStore._getStorageConfig(mount.storage_config_id);
        const driver = await StorageFactory.createDriver(
          upstreamStorageConfig.storage_type,
          upstreamStorageConfig,
          this.encryptionSecret,
        );

        if (typeof driver.generateUpstreamRequest === "function") {
          const upstream = await driver.generateUpstreamRequest(path, {
            subPath,
            request: options.request || null,
            userType,
            userId: userType === "ADMIN" ? userIdOrInfo : userIdOrInfo?.id || null,
            mount,
          });
          if (upstream && upstream.url) {
            const headers = normalizeUpstreamHeaders(upstream.headers);
            console.log(
              `[LinkService][fs][upstream-http] 路径(${path}) 上游使用驱动 UPSTREAM_HTTP 能力: ${upstream.url}`,
            );
            return createProxyLink(upstream.url, { headers });
          }
        }
      } catch (e) {
        console.warn(
          "[LinkService][fs][upstream-http] 构建上游请求失败，将回退 FS 默认链接策略：",
          e?.message || e,
        );
      }
    }

    const mountManager = new MountManager(this.db, this.encryptionSecret, this.repositoryFactory);
    const fileSystem = new FileSystem(mountManager);

    // 统一委托 FileSystem.generateFileLink（FsLinkStrategy）：
    // - 未配置 url_proxy 时：按 web_proxy + DirectLink 能力决策
    // - 配置了 url_proxy 时：强制走代理模式（forceProxy=true），再由 url_proxy 覆盖域名，实现“CloudPaste 负责签名，反代通吃流量”
    const linkResult = await fileSystem.generateFileLink(path, userIdOrInfo, userType, {
      ...options,
      userType,
      userId: userType === UserType.ADMIN ? userIdOrInfo : userIdOrInfo?.id,
      // client 模式下：配置了 url_proxy 时强制走代理路径（FsLinkStrategy 内部只会走 generateProxyUrl）
      // proxy 模式下：不受 url_proxy 影响，仅根据挂载策略与驱动能力决定是否代理
      forceProxy: options.forceProxy || (mode === "client" && hasUrlProxy),
    });

    const url = linkResult?.url || "";
    const type = linkResult?.type || null;

    // 直链：直接返回 direct link（S3 custom_host / native_direct）
    if (url && (type === "custom_host" || type === "native_direct")) {
      console.log(
        `[LinkService][fs][direct] 路径(${path}) 使用存储直链: ${url} (type=${type || "unknown"})`,
      );
      return createDirectLink(url);
    }

    // 代理链接：如挂载要求签名，则在此生成带签名的 /api/p 链接
    let finalUrl = url || "";
    let signatureForPath = null;
    try {
      const proxyMountResult =
        mountResult && !mountResult.error ? mountResult : await findMountPointByPathForProxy(this.db, path, this.repositoryFactory);
      if (!proxyMountResult.error) {
        const signatureService = new ProxySignatureService(this.db, this.encryptionSecret, this.repositoryFactory);
        const signatureNeed = await signatureService.needsSignature(proxyMountResult.mount);
        if (signatureNeed.required) {
          const signInfo = await signatureService.generateStorageSignature(path, proxyMountResult.mount, {
            expiresIn: options.expiresIn,
          });
          signatureForPath = signInfo.signature;

          finalUrl = buildSignedProxyUrl(options.request || null, path, {
            download: options.forceDownload || false,
            signature: signInfo.signature,
            requestTimestamp: signInfo.requestTimestamp,
            needsSignature: true,
          });
        }
      }
    } catch (e) {
      // 签名失败时不阻断流程，只记录一条警告日志并返回未签名的代理 URL
      console.warn("生成 FS 代理签名链接失败，将返回未签名链接：", e?.message || e);
    }

    // mode=proxy：返回给反向代理/Proxy 使用的“上游 URL”（仅关心直链 vs 本地 /api/p）
    if (mode === "proxy") {
      console.log(
        `[LinkService][fs][upstream] 路径(${path}) 上游使用本地 /api/p 代理链路: ${finalUrl || ""}`,
      );
      return createProxyLink(finalUrl || "");
    }

    // mode=client：
    // - 未配置 url_proxy：返回本地 /api/p 代理 URL
    // - 配置了 url_proxy：使用 url_proxy 作为 Worker / 反代入口域名，路径改为 /proxy/fs/<path>
    let proxiedUrl = finalUrl;
    if (hasUrlProxy) {
      try {
        const entryPath = `${WORKER_ENTRY.FS_PREFIX}${path}${options.forceDownload ? "?download=true" : ""}`;

        // - /proxy/fs 对外入口：sign 只覆盖 fsPath + expire（不包含 owner）
        // - Telegram 需要的 owner 上下文：由外部代理用 TOKEN 调 /api/proxy/link 后在“上游 URL”里解决，
        //   浏览器永远不需要知道 ot/oid。

        let workerSignature = null;
        try {
          const proxyMountResult =
            mountResult && !mountResult.error ? mountResult : await findMountPointByPathForProxy(this.db, path, this.repositoryFactory);
          if (!proxyMountResult.error) {
            const signatureService = new ProxySignatureService(this.db, this.encryptionSecret, this.repositoryFactory);
            const signatureNeed = await signatureService.needsSignature(proxyMountResult.mount);
            const forceWorkerSignature =
              proxyMountResult?.mount?.storage_type === StorageFactory.SUPPORTED_TYPES.TELEGRAM;
            if (signatureNeed.required || forceWorkerSignature) {
              const signInfo = await signatureService.generateStorageSignature(path, proxyMountResult.mount, {
                expiresIn: options.expiresIn,
                // 不传 ownerType/ownerId：保持外部代理兼容的签名格式（path:expire）
              });
              workerSignature = signInfo.signature;
            }
          }
        } catch (e) {
          console.warn("生成 FS Worker 入口签名失败，将返回未签名的入口链接：", e?.message || e);
          workerSignature = null;
        }

        // 外部入口：不携带 ot/oid，避免外部代理与缓存体系被迫升级
        proxiedUrl = buildSignedWorkerUrl(urlProxy, entryPath, {
          signature: workerSignature || undefined,
        });
        console.log(
          `[LinkService][fs][url_proxy] 路径(${path}) 使用 url_proxy=${urlProxy} 生成 Worker 入口: ${proxiedUrl}`,
        );
      } catch (e) {
        // 构建失败时保底使用原始 CloudPaste 代理链接，避免中断现有行为
        console.warn("构建 FS Worker 入口链接失败，将回退为原始代理链接：", e?.message || e);
        proxiedUrl = finalUrl;
      }
    }

    // 其余情况一律视为代理链路（包括本地 /api/p 代理 或 Worker 入口）
    console.log(
      `[LinkService][fs][proxy] 路径(${path}) 使用代理链接: ${proxiedUrl || ""}`,
    );
    return createProxyLink(proxiedUrl || "");
  }
}
