/**
 * 统一存储链接解析接口
 * POST /api/proxy/link
 *
 * - Origin（CloudPaste）只负责权限与“如何访问源”的决策
 * - 反向代理/Proxy 服务只调用此接口获取 { url, header }，然后直接向 url 发起请求
 * - FS / Share / WebDAV 最终都通过该接口解析，不在 Proxy 中分业务
 */

import { Hono } from "hono";
import { ApiStatus, UserType } from "../constants/index.js";
import { ValidationError, NotFoundError, AuthorizationError } from "../http/errors.js";
import { usePolicy } from "../security/policies/policies.js";
import { LinkService } from "../storage/link/LinkService.js";
import { FileService } from "../services/fileService.js";
import { getEncryptionSecret } from "../utils/environmentUtils.js";
import { getPrincipal } from "../security/middleware/securityContext.js";
import { isSharePasswordAccepted } from "../security/helpers/proxyAccess.js";

const proxyLinkRoutes = new Hono();

/**
 * 解析请求体
 * 约定：
 * - type = "fs"    → 使用 path 解析挂载视图（包含 WebDAV 在内）
 * - type = "share" → 使用 slug 解析分享视图
 * 目前不单独暴露 webdav 类型，WebDAV 在 FileSystem 层按挂载视图处理
 */
async function parseLinkBody(c) {
  let body = {};
  try {
    body = (await c.req.json()) || {};
  } catch {
    body = {};
  }

  const type = body.type || "fs";
  const path = body.path || null;
  const slug = body.slug || null;
  const password = typeof body.password === "string" ? body.password : null;

  if (!["fs", "share"].includes(type)) {
    throw new ValidationError("Invalid type, expected fs|share");
  }

  if (type === "fs" && !path) {
    throw new ValidationError("Missing path for fs type");
  }
  if (type === "share" && !slug) {
    throw new ValidationError("Missing slug for share type");
  }

  return { type, path, slug, password };
}

const parseProxyLinkBody = async (c, next) => {
  const body = await parseLinkBody(c);
  c.set("proxyLinkBody", body);
  await next();
};

const requireProxyFsRead = usePolicy("fs.read", {
  pathResolver: (c) => c.get("proxyLinkBody")?.path,
});

const authorizeProxyLink = async (c, next) => {
  const body = c.get("proxyLinkBody");

  // FS upstream resolution returns direct/presigned storage access and must
  // use the same authentication, permission and path checks as FS reads.
  if (body?.type === "fs") {
    return requireProxyFsRead(c, next);
  }

  // Share links remain usable by anonymous visitors, but password-protected
  // shares are checked below before an upstream URL is returned.
  return next();
};

/**
 * 统一响应格式
 * {
 *   code: 200,
 *   data: {
 *     url: string,
 *     header: Record<string, string[]>
 *   }
 * }
 */
function normalizeHeaders(rawHeaders) {
  if (!rawHeaders || typeof rawHeaders !== "object") {
    return {};
  }
  const result = {};
  for (const [key, value] of Object.entries(rawHeaders)) {
    if (Array.isArray(value)) {
      result[key] = value.map((v) => String(v));
    } else if (value != null) {
      result[key] = [String(value)];
    }
  }
  return result;
}

function buildLinkResponse({ url, header }) {
  return {
    code: 200,
    data: {
      url,
      header: header || {},
    },
  };
}

proxyLinkRoutes.post("/api/proxy/link", parseProxyLinkBody, authorizeProxyLink, async (c) => {
  const db = c.env.DB;
  const encryptionSecret = getEncryptionSecret(c);
  const repositoryFactory = c.get("repos");

  const { type, path, slug, password } = c.get("proxyLinkBody");

  const linkService = new LinkService(db, encryptionSecret, repositoryFactory);
  const fileService = new FileService(db, encryptionSecret, repositoryFactory);

  // 使用 principal，支持 ADMIN / API_KEY / 匿名三种身份。
  // FS 分支已由 requireProxyFsRead 强制认证；share 分支允许公开分享匿名解析。
  const principal = getPrincipal(c);
  let userType;
  let userIdOrInfo;

  if (principal && principal.isAdmin) {
    userType = UserType.ADMIN;
    userIdOrInfo = principal.id;
  } else if (principal && principal.type === "apiKey") {
    const apiKeyInfo = principal.attributes?.keyInfo ?? {
      id: principal.id,
      basicPath: principal.attributes?.basicPath ?? "/",
      permissions: principal.authorities,
    };
    userType = UserType.API_KEY;
    userIdOrInfo = apiKeyInfo;
  } else {
    userType = UserType.ANONYMOUS;
    userIdOrInfo = null;
  }

  switch (type) {
    case "fs": {
      const storageLink = await linkService.getFsUpstreamLink(path, userIdOrInfo, userType, {
        forceDownload: true,
        request: c.req.raw,
      });

      if (!storageLink || !storageLink.url) {
        throw new NotFoundError("FS path does not have a resolvable link");
      }

      return c.json(
        buildLinkResponse({
          url: storageLink.url,
          header: normalizeHeaders(storageLink.headers || {}),
        }),
        ApiStatus.OK,
      );
    }

    case "share": {
      const file = await fileService.getFileBySlug(slug);
      if (!(await isSharePasswordAccepted(file, password))) {
        throw new AuthorizationError("需要正确的文件密码");
      }

      const access = fileService.isFileAccessible(file);
      if (!access.accessible) {
        if (access.reason === "expired") {
          throw new AuthorizationError("File is expired");
        }
        throw new NotFoundError("File is not accessible");
      }

      const storageLink = await linkService.getShareUpstreamLink(
        file,
        // 分享视图只需要 user 类型与 id（目前仅用于后续扩展）
        { type: userType, id: userType === UserType.ADMIN ? userIdOrInfo : userIdOrInfo?.id ?? null },
        {
          request: c.req.raw,
          password,
        },
      );
      if (!storageLink || !storageLink.url) {
        throw new NotFoundError("Share file does not have a resolvable link");
      }

      return c.json(
        buildLinkResponse({
          url: storageLink.url,
          header: normalizeHeaders(storageLink.headers || {}),
        }),
        ApiStatus.OK,
      );
    }

    default:
      throw new ValidationError("Unsupported link type");
  }
});

export { proxyLinkRoutes };
