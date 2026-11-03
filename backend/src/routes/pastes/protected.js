import { AuthorizationError, ValidationError, NotFoundError } from "../../http/errors.js";
import { usePolicy } from "../../security/policies/policies.js";
import { resolvePrincipal } from "../../security/helpers/principal.js";
import { getEncryptionSecret } from "../../utils/environmentUtils.js";
import {
  getAllPastes,
  getUserPastes,
  getPasteById,
  batchDeletePastes,
  batchDeleteUserPastes,
  updatePaste,
  createPaste,
} from "../../services/pasteService.js";
import { ApiStatus, DbTables, UserType } from "../../constants/index.js";
import { Permission, PermissionChecker } from "../../constants/permissions.js";
import { getPagination, jsonOk, jsonCreated } from "../../utils/common.js";
import { useRepositories } from "../../utils/repositories.js";
import { ProxySignatureService } from "../../services/ProxySignatureService.js";
import { PASTE_URL_PROXY_TICKET_EXPIRES_IN_SECONDS, PASTE_URL_PROXY_TICKET_PATH } from "./urlProxyConfig.js";
import {
  batchUpdatePasteTags,
  createPasteTag,
  deletePasteTag,
  hydratePastesWithTags,
  listPasteTags,
  MAX_PASTE_TAGS,
  reorderPasteTags,
  setPasteTags,
  updatePasteTag,
  validatePasteTagIds,
} from "../../services/pasteTagService.js";

const getPrincipalContext = (c) => {
  const identity = resolvePrincipal(c, { allowedTypes: [UserType.ADMIN, UserType.API_KEY] });
  return {
    principal: identity.principal,
    userType: identity.type,
    userId: identity.userId,
    apiKeyInfo: identity.apiKeyInfo,
  };
};

const getTagFilters = (c) => {
  const tagIds = [...new Set(
    String(c.req.query("tag_ids") || "")
      .split(",")
      .map((id) => id.trim())
      .filter(Boolean),
  )];
  if (tagIds.length > MAX_PASTE_TAGS) {
    throw new ValidationError(`标签筛选数量不能超过 ${MAX_PASTE_TAGS} 个`);
  }
  const untaggedValue = String(c.req.query("untagged") || "").toLowerCase();
  return {
    tagIds,
    untagged: untaggedValue === "1" || untaggedValue === "true",
  };
};

export const registerPastesProtectedRoutes = (router) => {
  // SnapDOM useProxy 需要浏览器侧发起 <img src> 请求（无法携带 Authorization Header）。
  // 因此：通过 pastes.create 权限签发一个短期 ticket，供 /api/paste/url/proxy 验证后再代理上游内容。
  router.post("/api/paste/url/proxy-ticket", usePolicy("pastes.create"), async (c) => {
    const db = c.env.DB;
    const encryptionSecret = getEncryptionSecret(c);
    const repositoryFactory = useRepositories(c);

    const signatureService = new ProxySignatureService(db, encryptionSecret, repositoryFactory);
    const ticketInfo = await signatureService.generateStorageSignature(PASTE_URL_PROXY_TICKET_PATH, null, {
      expiresIn: PASTE_URL_PROXY_TICKET_EXPIRES_IN_SECONDS,
    });

    return jsonOk(
      c,
      {
        ticket: ticketInfo.signature,
        expiresIn: ticketInfo.expiresIn,
        expiresAt: ticketInfo.expiresAt,
      },
      "代理票据签发成功",
    );
  });

  router.post("/api/paste", usePolicy("pastes.create"), async (c) => {
    const db = c.env.DB;
    const body = await c.req.json();
    const { principal, userType, userId, apiKeyInfo } = getPrincipalContext(c);
    const authType = userType;
    const hasTagPayload = Object.prototype.hasOwnProperty.call(body || {}, "tag_ids");
    const isGuest = Boolean(apiKeyInfo?.isGuest) || String(apiKeyInfo?.role || "").toUpperCase() === "GUEST";
    const canManageTags =
      userType === UserType.ADMIN ||
      (userType === UserType.API_KEY &&
        !isGuest &&
        PermissionChecker.hasPermission(principal?.authorities ?? 0, Permission.TEXT_MANAGE));

    if (hasTagPayload && !canManageTags) {
      throw new AuthorizationError("缺少文本管理权限，无法设置标签");
    }

    const repositoryFactory = useRepositories(c);
    const tagIds = hasTagPayload ? await validatePasteTagIds(db, body.tag_ids, repositoryFactory) : [];
    const pasteData = { ...body };
    delete pasteData.tag_ids;

    const created_by = authType === UserType.ADMIN ? userId : authType === UserType.API_KEY ? `apikey:${userId}` : null;
    const paste = await createPaste(db, pasteData, created_by, repositoryFactory);
    const tags = tagIds.length ? await setPasteTags(db, paste.id, tagIds, authType === UserType.API_KEY ? created_by : null, repositoryFactory) : [];

    return jsonCreated(c, { ...paste, tags, authorizedBy: authType }, "文本创建成功");
  });

  router.get("/api/pastes", usePolicy("pastes.manage"), async (c) => {
    const db = c.env.DB;
    const { userType, userId, apiKeyInfo } = getPrincipalContext(c);

    if (userType === UserType.ADMIN) {
      const { limit, page, offset } = getPagination(c, { limit: 10, page: 1 });
      const search = c.req.query("search");
      const created_by = c.req.query("created_by");
      const result = await getAllPastes(db, page, limit, created_by, search, offset, useRepositories(c), getTagFilters(c));

      const results = Array.isArray(result.results) ? result.results : [];
      return jsonOk(
        c,
        {
          results,
          pagination: result.pagination,
        },
        "获取成功",
      );
    }

    // API Key 用户：只返回当前密钥创建的文本列表，并附带 key_info
    const { limit, offset } = getPagination(c, { limit: 30 });
    const search = c.req.query("search");
    const result = await getUserPastes(db, userId, limit, offset, search, useRepositories(c), getTagFilters(c));

    const results = Array.isArray(result.results) ? result.results : [];

    return jsonOk(
      c,
      {
        results,
        pagination: result.pagination,
        key_info: apiKeyInfo,
      },
      "获取成功",
    );
  });

  router.get("/api/pastes/:id", usePolicy("pastes.manage"), async (c) => {
    const db = c.env.DB;
    const { id } = c.req.param();
    const { userType, userId } = getPrincipalContext(c);

    let result;

    if (userType === UserType.ADMIN) {
      result = await getPasteById(db, id);
    } else {
      const repositoryFactory = useRepositories(c);
      const pasteRepository = repositoryFactory.getPasteRepository();

      const paste = await pasteRepository.findOne(DbTables.PASTES, {
        id,
        created_by: `apikey:${userId}`,
      });

      if (!paste) {
        throw new NotFoundError("文本不存在或无权访问");
      }

      paste.has_password = !!paste.password;

      let plainPassword = null;
      if (paste.has_password) {
        plainPassword = await pasteRepository.findPasswordByPasteId(paste.id);
      }

      result = {
        ...paste,
        plain_password: plainPassword,
      };
      [result] = await hydratePastesWithTags(db, [result], repositoryFactory);
    }

    return jsonOk(c, result, "获取成功");
  });

  router.delete("/api/pastes/batch-delete", usePolicy("pastes.manage"), async (c) => {
    const db = c.env.DB;
    const { userType, userId } = getPrincipalContext(c);

    const { ids } = await c.req.json();

    if (!ids || !Array.isArray(ids) || ids.length === 0) {
      throw new ValidationError("请提供有效的文本ID数组");
    }
    const deletedCount = userType === UserType.ADMIN ? await batchDeletePastes(db, ids, false) : await batchDeleteUserPastes(db, ids, userId);

    return jsonOk(c, undefined, `已删除 ${deletedCount} 个分享`);
  });

  router.get("/api/paste-tags", usePolicy("pastes.manage"), async (c) => {
    const db = c.env.DB;
    const { userType, userId } = getPrincipalContext(c);
    const createdBy = userType === UserType.API_KEY ? `apikey:${userId}` : null;
    const result = await listPasteTags(db, createdBy, useRepositories(c));
    return jsonOk(c, result, "获取标签成功");
  });

  router.post("/api/paste-tags", usePolicy("pastes.admin"), async (c) => {
    const db = c.env.DB;
    const { userId } = getPrincipalContext(c);
    const tag = await createPasteTag(db, await c.req.json(), userId, useRepositories(c));
    return jsonCreated(c, tag, "标签创建成功");
  });

  router.put("/api/paste-tags/reorder", usePolicy("pastes.admin"), async (c) => {
    const body = await c.req.json();
    await reorderPasteTags(c.env.DB, body?.ids, useRepositories(c));
    return jsonOk(c, undefined, "标签排序已更新");
  });

  router.put("/api/paste-tags/:id", usePolicy("pastes.admin"), async (c) => {
    const tag = await updatePasteTag(c.env.DB, c.req.param("id"), await c.req.json(), useRepositories(c));
    return jsonOk(c, tag, "标签更新成功");
  });

  router.delete("/api/paste-tags/:id", usePolicy("pastes.admin"), async (c) => {
    await deletePasteTag(c.env.DB, c.req.param("id"), useRepositories(c));
    return jsonOk(c, undefined, "标签已删除");
  });

  router.put("/api/pastes/:id/tags", usePolicy("pastes.manage"), async (c) => {
    const { userType, userId } = getPrincipalContext(c);
    const createdBy = userType === UserType.API_KEY ? `apikey:${userId}` : null;
    const body = await c.req.json();
    const tags = await setPasteTags(c.env.DB, c.req.param("id"), body?.tag_ids, createdBy, useRepositories(c));
    return jsonOk(c, { tags }, "文本标签已更新");
  });

  router.post("/api/pastes/batch-tags", usePolicy("pastes.manage"), async (c) => {
    const { userType, userId } = getPrincipalContext(c);
    const createdBy = userType === UserType.API_KEY ? `apikey:${userId}` : null;
    const result = await batchUpdatePasteTags(c.env.DB, await c.req.json(), createdBy, useRepositories(c));
    return jsonOk(c, result, "批量标签操作完成");
  });

  router.put("/api/pastes/:slug", usePolicy("pastes.manage"), async (c) => {
    const db = c.env.DB;
    const { slug } = c.req.param();
    const { userType, userId } = getPrincipalContext(c);
    const body = await c.req.json();

    const result = userType === UserType.ADMIN ? await updatePaste(db, slug, body) : await updatePaste(db, slug, body, `apikey:${userId}`);

    return jsonOk(c, result, "文本更新成功");
  });

  router.post("/api/pastes/clear-expired", usePolicy("pastes.admin"), async (c) => {
    const db = c.env.DB;

    const deletedCount = await batchDeletePastes(db, null, true);
    return jsonOk(c, undefined, `已清理 ${deletedCount} 个过期分享`);
  });
};
