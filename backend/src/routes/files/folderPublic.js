/**
 * 文件夹分享公开路由
 *
 * - 公开只读访问 target_type === 'folder' 的分享记录
 * - 目录浏览统一走存储驱动 READER 能力（MountManager.getDriver + listDirectory）
 * - 单文件下载走后端代理（StorageStreaming，SHARE 通道）
 * - 下载次数语义：整个文件夹分享按“一次下载”计（而非文件夹内每个文件各计一次）
 *   同一浏览器会话内首次成功下载计 1 次，之后该会话内继续下载不再累加（以 HttpOnly Cookie 标识）
 *
 * 安全约定：
 * - path 参数为“分享根目录相对路径”，禁止绝对路径与 `..` 越权段
 * - 所有驱动 subPath 均基于 record.storage_path 拼接，解析后不会超出分享根目录
 */

import { ApiStatus } from "../../constants/index.js";
import { AppError, NotFoundError, AuthenticationError, ValidationError } from "../../http/errors.js";
import { jsonOk } from "../../utils/common.js";
import { getEncryptionSecret } from "../../utils/environmentUtils.js";
import { useRepositories } from "../../utils/repositories.js";
import { verifyPassword } from "../../utils/crypto.js";
import { generateShareAccessToken, verifyShareAccessToken } from "../../utils/shareAccessToken.js";
import { parseEncryptionMeta } from "../../utils/fileEncryption.js";
import { getFileBySlug } from "../../services/fileService.js";
import { checkAndDeleteExpiredFile } from "../../services/fileViewService.js";
import { MountManager } from "../../storage/managers/MountManager.js";
import { CAPABILITIES } from "../../storage/interfaces/capabilities/index.js";
import { StorageStreaming, STREAMING_CHANNELS } from "../../storage/streaming/index.js";
import { StorageFactory } from "../../storage/factory/StorageFactory.js";
import { getContentTypeAndDisposition } from "../../utils/fileUtils.js";

/**
 * 规范化“分享根目录相对路径”
 * - 统一分隔符为 `/`
 * - 拒绝绝对路径与包含 `..` 的越权路径
 * - 丢弃空段与 `.` 段
 * @param {unknown} input
 * @returns {string} 规范化后的相对路径（根目录返回空字符串）
 */
function normalizeRelPath(input) {
  if (input == null) return "";
  const raw = String(input).replace(/\\/g, "/").trim();
  if (!raw) return "";
  if (raw.startsWith("/")) {
    throw new ValidationError("非法路径");
  }

  const segments = raw.split("/");
  const cleaned = [];
  for (const segment of segments) {
    if (segment === "" || segment === ".") continue;
    if (segment === "..") {
      throw new ValidationError("非法路径");
    }
    cleaned.push(segment);
  }
  return cleaned.join("/");
}

/**
 * 拼接完整存储子路径（分享根目录 + 相对路径）
 * - 统一去除 storage_path 首尾斜杠（与 ShareRecordService 写入约定一致）
 * @param {string} storagePath - 分享记录的 storage_path（目录前缀）
 * @param {string} relPath - 规范化后的相对路径
 * @returns {string}
 */
function joinStoragePath(storagePath, relPath) {
  const base = String(storagePath || "")
    .replace(/\\/g, "/")
    .replace(/^\/+/, "")
    .replace(/\/+$/, "");
  if (!relPath) return base;
  return base ? `${base}/${relPath}` : relPath;
}

/**
 * 加载并校验文件夹分享记录
 * @returns {Promise<Object>}
 */
async function loadFolderShare(db, slug, encryptionSecret) {
  const record = await getFileBySlug(db, slug, encryptionSecret);
  if (!record) {
    throw new NotFoundError("分享不存在");
  }
  if (record.target_type !== "folder") {
    throw new NotFoundError("分享不存在");
  }
  return record;
}

/**
 * 文件夹分享“本次会话已计过下载次数”的 Cookie 名
 * - slug 可能包含非 ASCII/特殊字符，这里做一次稳定短哈希，保证 Cookie 名合法
 * @param {string} slug
 * @returns {string}
 */
function folderDownloadCookieName(slug) {
  const raw = String(slug || "");
  let hash = 0;
  for (let i = 0; i < raw.length; i += 1) {
    hash = (hash * 31 + raw.charCodeAt(i)) >>> 0;
  }
  return `cpfs_dl_${hash.toString(36)}`;
}

/**
 * 当前浏览器会话是否已经为该文件夹分享计过下载次数
 * @param {import("hono").Context} c
 * @param {string} slug
 * @returns {boolean}
 */
function hasSessionCounted(c, slug) {
  try {
    const cookieHeader = c.req.header("Cookie") || "";
    const name = folderDownloadCookieName(slug);
    const match = cookieHeader.match(new RegExp(`(?:^|;\\s*)${name}=([^;]*)`));
    return !!match && match[1] === "1";
  } catch {
    return false;
  }
}

/**
 * 构造“本次会话已计过下载次数”的 Set-Cookie 值
 * - 直接写入流式响应头，避免依赖框架对自定义响应的 Cookie 合并行为
 * @param {import("hono").Context} c
 * @param {string} slug
 * @returns {string}
 */
function buildSessionCookie(c, slug) {
  const isHttps = new URL(c.req.url).protocol === "https:";
  const name = folderDownloadCookieName(slug);
  const attrs = [`${name}=1`, "Path=/", `Max-Age=${60 * 60 * 12}`, "HttpOnly", "SameSite=Lax"];
  if (isHttps) attrs.push("Secure");
  return attrs.join("; ");
}

/**
 * 访问守卫：过期 / 达到下载上限时清理记录并返回 410
 * @param {Object} [options]
 * @param {boolean} [options.allowLimitReached] 允许“已达下载上限但本会话已计过次”的情况继续访问
 */
async function ensureAccessible(db, record, encryptionSecret, repositoryFactory, options = {}) {
  const { allowLimitReached = false } = options;

  // 有效期过期：始终拒绝并清理分享记录
  if (record.expires_at) {
    const expiry = new Date(record.expires_at);
    if (!Number.isNaN(expiry.getTime()) && Date.now() > expiry.getTime()) {
      await checkAndDeleteExpiredFile(db, record, encryptionSecret, repositoryFactory);
      throw new AppError("分享已过期或已达下载次数上限", {
        status: ApiStatus.GONE,
        code: "GONE",
        expose: true,
      });
    }
  }

  // 下载次数上限：文件夹分享以“整个分享”为一次下载
  const maxViews = Number(record.max_views) || 0;
  const views = Number(record.views) || 0;
  if (maxViews > 0 && views >= maxViews && !allowLimitReached) {
    await checkAndDeleteExpiredFile(db, record, encryptionSecret, repositoryFactory);
    throw new AppError("分享已过期或已达下载次数上限", {
      status: ApiStatus.GONE,
      code: "GONE",
      expose: true,
    });
  }
}

/**
 * 密码守卫（仅在分享设置了密码时生效）
 * 优先校验短时效访问令牌（?at=），兼容旧 ?password= 明文参数
 */
async function ensurePassword(record, plainPassword, request = null, encryptionSecret = null) {
  if (!record.password) return;

  if (request && encryptionSecret) {
    try {
      const atToken = new URL(request.url).searchParams.get("at");
      if (atToken && (await verifyShareAccessToken(encryptionSecret, atToken, { type: "folder", slug: record.slug }))) {
        return;
      }
    } catch {
      // URL 解析失败时回退到密码校验
    }
  }

  if (!plainPassword) {
    throw new AuthenticationError("需要密码访问");
  }
  const valid = await verifyPassword(plainPassword, record.password);
  if (!valid) {
    throw new AuthenticationError("密码不正确");
  }
}

/**
 * 将驱动返回的目录项归一化为稳定的对外契约结构
 * @param {Object} item - 驱动返回项
 * @param {string} relPath - 当前列的目录相对路径
 */
function normalizeItem(item, relPath) {
  const isDirectory = !!item?.isDirectory;
  const name = item?.name || "";
  const itemRelPath = relPath ? `${relPath}/${name}` : name;
  return {
    name,
    path: itemRelPath,
    isDirectory,
    size: isDirectory ? 0 : typeof item?.size === "number" && Number.isFinite(item.size) ? item.size : 0,
    modified_at: item?.modified || null,
  };
}

/**
 * 构造元信息负载（不含 items）
 */
function buildMetaPayload(record) {
  return {
    id: record.id,
    slug: record.slug,
    filename: record.filename,
    remark: record.remark,
    target_type: "folder",
    requires_password: !!record.password,
    expires_at: record.expires_at,
    max_views: record.max_views,
    views: record.views,
  };
}

/**
 * 列出分享根目录下某个相对目录的内容
 * @returns {Promise<{ path: string, items: Array<Object> }>}
 */
async function listFolder(db, record, encryptionSecret, repositoryFactory, relPath) {
  const pseudoMount = {
    id: record.id,
    storage_type: record.storage_type,
    storage_config_id: record.storage_config_id,
  };

  const mountManager = new MountManager(db, encryptionSecret, repositoryFactory);
  const driver = await mountManager.getDriver(pseudoMount);

  if (!driver.hasCapability(CAPABILITIES.READER)) {
    throw new AppError("当前存储不支持目录浏览", {
      status: 501,
      code: "NOT_IMPLEMENTED",
      expose: true,
    });
  }

  const fullSubPath = joinStoragePath(record.storage_path, relPath);
  // 驱动约定：目录 subPath 以 "/" 开头、以 "/" 结尾；根目录使用空字符串
  const driverSubPath = fullSubPath ? `/${fullSubPath}/` : "";
  const displayPath = relPath ? `/${relPath}` : "/";

  const result = await driver.listDirectory(driverSubPath, {
    path: displayPath,
    mount: pseudoMount,
    subPath: driverSubPath,
    db,
  });

  const items = Array.isArray(result?.items) ? result.items.map((item) => normalizeItem(item, relPath)) : [];
  return { path: relPath, items };
}

export const registerFolderPublicRoutes = (router) => {
  // 元信息（未设置密码时包含根目录列表）
  router.get("/api/share/folder/:slug", async (c) => {
    const db = c.env.DB;
    const slug = c.req.param("slug");
    const encryptionSecret = getEncryptionSecret(c);
    const repositoryFactory = useRepositories(c);

    const record = await loadFolderShare(db, slug, encryptionSecret);

    // 设置了密码：仅返回元信息，不暴露任何列表/URL
    if (record.password) {
      return jsonOk(c, { ...buildMetaPayload(record), path: "", items: null }, "需要密码访问");
    }

    await ensureAccessible(db, record, encryptionSecret, repositoryFactory, {
      allowLimitReached: hasSessionCounted(c, slug),
    });
    const listing = await listFolder(db, record, encryptionSecret, repositoryFactory, "");
    return jsonOk(c, { ...buildMetaPayload(record), path: listing.path, items: listing.items }, "获取分享成功");
  });

  // 校验密码并返回根目录列表
  router.post("/api/share/folder/verify/:slug", async (c) => {
    const db = c.env.DB;
    const slug = c.req.param("slug");
    const encryptionSecret = getEncryptionSecret(c);
    const repositoryFactory = useRepositories(c);

    const body = await c.req.json().catch(() => ({}));
    const plainPassword = body?.password;

    const record = await loadFolderShare(db, slug, encryptionSecret);
    await ensureAccessible(db, record, encryptionSecret, repositoryFactory, {
      allowLimitReached: hasSessionCounted(c, slug),
    });
    await ensurePassword(record, plainPassword);

    const listing = await listFolder(db, record, encryptionSecret, repositoryFactory, "");
    const payload = { ...buildMetaPayload(record), path: listing.path, items: listing.items };
    // 签发短时效访问令牌：后续 list/download URL 携带 ?at=，避免明文密码进 URL
    if (record.password) {
      payload.accessToken = await generateShareAccessToken(encryptionSecret, { type: "folder", slug });
    }
    return jsonOk(c, payload, "密码验证成功");
  });

  // 子目录列表
  router.get("/api/share/folder/:slug/list", async (c) => {
    const db = c.env.DB;
    const slug = c.req.param("slug");
    const encryptionSecret = getEncryptionSecret(c);
    const repositoryFactory = useRepositories(c);

    const record = await loadFolderShare(db, slug, encryptionSecret);
    await ensureAccessible(db, record, encryptionSecret, repositoryFactory, {
      allowLimitReached: hasSessionCounted(c, slug),
    });
    await ensurePassword(record, c.req.query("password"), c.req.raw, encryptionSecret);

    const relPath = normalizeRelPath(c.req.query("path"));
    const listing = await listFolder(db, record, encryptionSecret, repositoryFactory, relPath);
    return jsonOk(c, { ...buildMetaPayload(record), path: listing.path, items: listing.items }, "获取目录成功");
  });

  // 单文件下载（后端代理流式输出，成功后累计下载计数）
  router.get("/api/share/folder/:slug/download", async (c) => {
    const db = c.env.DB;
    const slug = c.req.param("slug");
    const encryptionSecret = getEncryptionSecret(c);
    const repositoryFactory = useRepositories(c);

    const record = await loadFolderShare(db, slug, encryptionSecret);
    // 同一浏览器会话内已计过次：达到上限也允许继续下载（整个文件夹只计一次）
    const sessionCounted = hasSessionCounted(c, slug);
    await ensureAccessible(db, record, encryptionSecret, repositoryFactory, {
      allowLimitReached: sessionCounted,
    });
    await ensurePassword(record, c.req.query("password"), c.req.raw, encryptionSecret);

    const relPath = normalizeRelPath(c.req.query("path"));
    if (!relPath) {
      throw new ValidationError("缺少文件路径");
    }

    const fullSubPath = joinStoragePath(record.storage_path, relPath);

    // 校验目标必须是文件（目录拒绝下载）
    const pseudoMount = {
      id: record.id,
      storage_type: record.storage_type,
      storage_config_id: record.storage_config_id,
    };
    const mountManager = new MountManager(db, encryptionSecret, repositoryFactory);
    const driver = await mountManager.getDriver(pseudoMount);
    const info = await driver.getFileInfo(`/${fullSubPath}`, {
      path: `/${relPath}`,
      mount: pseudoMount,
      subPath: `/${fullSubPath}`,
      db,
    });
    if (info?.isDirectory) {
      throw new ValidationError("不能下载目录");
    }

    // 反查分享记录：该文件若曾被"分享上传"加密（server 模式），下载时需在代理层解密；
    // 文件夹内普通文件（FS 上传）无记录，保持原样。
    let downloadEncryptionMeta = null;
    try {
      const fileRepo = repositoryFactory.getFileRepository();
      const shareRecord = await fileRepo
        .findByStoragePath(record.storage_config_id, fullSubPath, record.storage_type)
        .catch(() => null);
      const meta = parseEncryptionMeta(shareRecord?.encryption_meta || null);
      if (meta && meta.mode === "server") {
        downloadEncryptionMeta = meta;
      }
    } catch {
      // 反查失败不阻塞下载（未加密内容不受影响）
    }

    const streaming = new StorageStreaming({
      mountManager: null,
      storageFactory: StorageFactory,
      encryptionSecret,
    });

    const response = await streaming.createResponse({
      path: fullSubPath,
      channel: STREAMING_CHANNELS.SHARE,
      storageConfigId: record.storage_config_id,
      rangeHeader: c.req.header("Range") || null,
      request: c.req.raw,
      db,
      repositoryFactory,
      encryptionMeta: downloadEncryptionMeta,
    });

    const filename = relPath.split("/").filter(Boolean).pop() || record.filename || "download";
    // inline=1 时以内联方式返回（用于页面内预览）；默认强制下载
    const inlineParam = String(c.req.query("inline") || "").toLowerCase();
    const isInline = inlineParam === "1" || inlineParam === "true";
    const { contentType, contentDisposition } = getContentTypeAndDisposition(filename, null, {
      forceDownload: !isInline,
    });
    response.headers.set("Content-Type", contentType);
    response.headers.set("Content-Disposition", contentDisposition);

    // CORS 头（与分享代理链路保持一致）
    response.headers.set("Access-Control-Allow-Origin", "*");
    response.headers.set("Access-Control-Allow-Methods", "GET, HEAD, OPTIONS");
    response.headers.set("Access-Control-Allow-Headers", "Range, Content-Type");
    response.headers.set("Access-Control-Expose-Headers", "Content-Length, Content-Range, Accept-Ranges");

    // 成功交付后累计下载计数
    // - 仅在“实际下载”时计数；inline=1 为页面内预览，不消耗次数
    // - 同一浏览器会话内只计 1 次（整个文件夹分享按一次下载计），并写入 Cookie 以便后续下载放行
    if (!isInline && !sessionCounted && (response.status === 302 || (response.status >= 200 && response.status < 300))) {
      try {
        await repositoryFactory.getFileRepository().incrementDownloads(record.id);
        response.headers.set("Set-Cookie", buildSessionCookie(c, slug));
      } catch (e) {
        console.warn("递增下载计数失败（已忽略）:", e?.message || e);
      }
    }

    return response;
  });
};
