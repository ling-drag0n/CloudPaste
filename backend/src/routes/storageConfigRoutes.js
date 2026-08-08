/**
 * 通用存储配置路由（现阶段对应 S3 配置的通用外观）
 */
import { Hono } from "hono";
import {
  getStorageConfigsByAdmin,
  getPublicStorageConfigs,
  getStorageConfigByIdForAdmin,
  getPublicStorageConfigById,
  createStorageConfig,
  updateStorageConfig,
  deleteStorageConfig,
  setDefaultStorageConfig,
  testStorageConnection,
} from "../services/storageConfigService.js";
import { UserType } from "../constants/index.js";
import { getPagination, jsonOk, jsonCreated } from "../utils/common.js";
import { getEncryptionSecret } from "../utils/environmentUtils.js";
import { usePolicy } from "../security/policies/policies.js";
import { resolvePrincipal } from "../security/helpers/principal.js";
import { useRepositories } from "../utils/repositories.js";
import { NotFoundError, AuthorizationError } from "../http/errors.js";

const storageConfigRoutes = new Hono();
const requireRead = usePolicy("storage.config.read");
const requireAdmin = usePolicy("admin.all");

// 获取存储配置列表（管理员或公开）
storageConfigRoutes.get("/api/storage", requireRead, async (c) => {
  const db = c.env.DB;
  const repositoryFactory = useRepositories(c);
  const identity = resolvePrincipal(c, { allowedTypes: [UserType.ADMIN, UserType.API_KEY] });
  const isAdmin = identity.isAdmin;
  const adminId = identity.userId;

  if (isAdmin) {
    const hasPageParam = c.req.query("page") !== undefined;
    const hasLimitParam = c.req.query("limit") !== undefined;

    if (hasPageParam || hasLimitParam) {
      const { limit, page } = getPagination(c, { limit: 10, page: 1 });
      const result = await getStorageConfigsByAdmin(db, adminId, { page, limit }, repositoryFactory);
      return jsonOk(c, { items: result.configs, total: result.total }, "获取存储配置列表成功");
    }

    const result = await getStorageConfigsByAdmin(db, adminId, {}, repositoryFactory);
    return jsonOk(c, { items: result.configs, total: result.total }, "获取存储配置列表成功");
  }

  // API 密钥用户：仅能看到“公开 + ACL 白名单”内的存储配置
  const configs = await getPublicStorageConfigs(db, repositoryFactory);

  const repoFactory = repositoryFactory;
  const aclRepo = repoFactory.getPrincipalStorageAclRepository
    ? repoFactory.getPrincipalStorageAclRepository()
    : null;

  let filteredConfigs = configs;
  if (aclRepo && identity.userId) {
    try {
      const allowedIds = await aclRepo.findConfigIdsBySubject("API_KEY", identity.userId);
      if (Array.isArray(allowedIds) && allowedIds.length > 0) {
        const allowedSet = new Set(allowedIds);
        filteredConfigs = configs.filter((cfg) => allowedSet.has(cfg.id));
      }
    } catch (error) {
      console.error("加载存储 ACL 失败，拒绝扩大存储配置列表范围：", error);
      throw new AuthorizationError("无法验证存储访问权限");
    }
  }

  return jsonOk(c, { items: filteredConfigs, total: filteredConfigs.length }, "获取存储配置列表成功");
});

// 获取单个存储配置详情
storageConfigRoutes.get("/api/storage/:id", requireRead, async (c) => {
  const db = c.env.DB;
  const { id } = c.req.param();
  const repositoryFactory = useRepositories(c);
  const identity = resolvePrincipal(c, { allowedTypes: [UserType.ADMIN, UserType.API_KEY] });
  const isAdmin = identity.isAdmin;
  const adminId = identity.userId;

  // reveal=plain|masked（仅管理员）
  const reveal = c.req.query("reveal"); // 'plain' | 'masked'
  let config;
  if (isAdmin) {
    if (reveal === "plain" || reveal === "masked") {
      const encryptionSecret = getEncryptionSecret(c);
      const { getStorageConfigByIdForAdminReveal } = await import("../services/storageConfigService.js");
      config = await getStorageConfigByIdForAdminReveal(db, id, adminId, encryptionSecret, reveal, repositoryFactory);
      // 简要审计日志（不打印明文）
      console.log(JSON.stringify({ type: "secrets.reveal", id, adminId, mode: reveal, timestamp: new Date().toISOString() }));
    } else {
      config = await getStorageConfigByIdForAdmin(db, id, adminId, repositoryFactory);
    }
  } else {
    // API 密钥用户：仅允许访问“公开 + ACL 白名单”内的存储配置
    const repoFactory = repositoryFactory;
    const aclRepo = repoFactory.getPrincipalStorageAclRepository
      ? repoFactory.getPrincipalStorageAclRepository()
      : null;

    const cfg = await getPublicStorageConfigById(db, id, repositoryFactory);

    if (aclRepo && identity.userId) {
      try {
        const allowedIds = await aclRepo.findConfigIdsBySubject("API_KEY", identity.userId);
        if (Array.isArray(allowedIds) && allowedIds.length > 0 && !allowedIds.includes(cfg.id)) {
          throw new NotFoundError("存储配置不存在");
        }
      } catch (error) {
        if (error instanceof NotFoundError) {
          throw error;
        }
        console.error("加载存储 ACL 失败，拒绝扩大存储访问范围：", error);
        throw new AuthorizationError("无法验证存储访问权限");
      }
    }

    config = cfg;
  }

  return jsonOk(c, config, "获取存储配置成功");
});

// 创建存储配置（管理员）
storageConfigRoutes.post("/api/storage", requireAdmin, async (c) => {
  const db = c.env.DB;
  const { userId: adminId } = resolvePrincipal(c, { allowedTypes: [UserType.ADMIN] });
  const encryptionSecret = getEncryptionSecret(c);
  const body = await c.req.json();
  const repositoryFactory = useRepositories(c);
  const config = await createStorageConfig(db, body, adminId, encryptionSecret, repositoryFactory);
  return jsonCreated(c, config, "存储配置创建成功");
});

// 更新存储配置（管理员）
storageConfigRoutes.put("/api/storage/:id", requireAdmin, async (c) => {
  const db = c.env.DB;
  const { userId: adminId } = resolvePrincipal(c, { allowedTypes: [UserType.ADMIN] });
  const { id } = c.req.param();
  const encryptionSecret = getEncryptionSecret(c);
  const repositoryFactory = useRepositories(c);

  const body = await c.req.json();
  await updateStorageConfig(db, id, body, adminId, encryptionSecret, repositoryFactory);

  const updated = await getStorageConfigByIdForAdmin(db, id, adminId, repositoryFactory);
  return jsonOk(c, updated, "存储配置已更新");
});

// 删除存储配置（管理员）
storageConfigRoutes.delete("/api/storage/:id", requireAdmin, async (c) => {
  const db = c.env.DB;
  const { userId: adminId } = resolvePrincipal(c, { allowedTypes: [UserType.ADMIN] });
  const { id } = c.req.param();
  const repositoryFactory = useRepositories(c);

  await deleteStorageConfig(db, id, adminId, repositoryFactory);
  return jsonOk(c, undefined, "存储配置删除成功");
});

// 设置默认存储配置（管理员）
storageConfigRoutes.put("/api/storage/:id/set-default", requireAdmin, async (c) => {
  const db = c.env.DB;
  const { userId: adminId } = resolvePrincipal(c, { allowedTypes: [UserType.ADMIN] });
  const { id } = c.req.param();
  const repositoryFactory = useRepositories(c);
  await setDefaultStorageConfig(db, id, adminId, repositoryFactory);
  return jsonOk(c, undefined, "默认存储配置设置成功");
});

// 测试存储配置连接（管理员）
storageConfigRoutes.post("/api/storage/:id/test", requireAdmin, async (c) => {
  const db = c.env.DB;
  const { userId: adminId } = resolvePrincipal(c, { allowedTypes: [UserType.ADMIN] });
  const { id } = c.req.param();
  const encryptionSecret = getEncryptionSecret(c);
  const requestOrigin = c.req.header("origin");
  const repositoryFactory = useRepositories(c);
  const testData = await testStorageConnection(db, id, adminId, encryptionSecret, requestOrigin, repositoryFactory);

  // 外层 success 只表示“请求是否被处理成功”
  // 测试通过/失败由 data.success 表示
  return jsonOk(c, testData, "OK");
});

export default storageConfigRoutes;
