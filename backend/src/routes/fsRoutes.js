import { Hono } from "hono";
import { ApiStatus, UserType } from "../constants/index.js";
import { AuthenticationError, AuthorizationError, ValidationError } from "../http/errors.js";
import { usePolicy } from "../security/policies/policies.js";
import { getStorageConfigByIdForAdmin, getPublicStorageConfigById } from "../services/storageConfigService.js";
import { getAccessibleMountsForUser } from "../security/helpers/access.js";
import { ensureRepositoryFactory } from "../utils/repositories.js";
import { registerBrowseRoutes } from "./fs/browse.js";
import { registerWriteRoutes } from "./fs/write.js";
import { registerMultipartRoutes } from "./fs/multipart.js";
import { registerOpsRoutes } from "./fs/ops.js";
import { registerSearchShareRoutes } from "./fs/search_share.js";
import { FsMetaService } from "../services/fsMetaService.js";
import { encryptValue, decryptValue } from "../utils/crypto.js";
import { getEncryptionSecret } from "../utils/environmentUtils.js";
import { createErrorResponse, jsonOk } from "../utils/common.js";

const fsRoutes = new Hono();

// 路径规范化
const normalizeFsPath = (path) => {
  if (!path || path === "/") {
    return "/";
  }
  const trimmed = path.replace(/\/+$/, "") || "/";
  return trimmed.startsWith("/") ? trimmed : `/${trimmed}`;
};

/**
 * 解析指定路径的有效 Meta 与密码域信息
 * @param {D1Database} db
 * @param {string} rawPath
 */
const resolveEffectiveMetaForPath = async (db, rawPath) => {
  const metaService = new FsMetaService(db);
  const normalizedPath = normalizeFsPath(rawPath || "/");
  const meta = await metaService.resolveMetaForPath(normalizedPath);
  const requiresPassword = Boolean(meta.password);
  const passwordOwnerPath = meta.password ? meta.passwordOwnerPath || normalizedPath : null;
  return { meta, normalizedPath, requiresPassword, passwordOwnerPath };
};

/**
 * 为指定路径和明文密码生成验证结果与 token
 * - 不负责身份判断，仅基于 fs_meta 的 password 字段工作
 * @param {D1Database} db
 * @param {string} rawPath
 * @param {string} plainPassword
 * @param {string} encryptionSecret
 */
const buildPathPasswordVerification = async (db, rawPath, plainPassword, encryptionSecret) => {
  const { meta, normalizedPath, requiresPassword, passwordOwnerPath } = await resolveEffectiveMetaForPath(db, rawPath);

  // 未配置密码：直接视为不需要路径密码
  if (!requiresPassword) {
    return {
      requiresPassword: false,
      verified: true,
      token: null,
      path: normalizedPath,
    };
  }

  if (!plainPassword || typeof plainPassword !== "string" || !plainPassword.trim()) {
    return {
      requiresPassword: true,
      verified: false,
      error: "EMPTY_PASSWORD",
      path: normalizedPath,
    };
  }

  if (plainPassword !== meta.password) {
    return {
      requiresPassword: true,
      verified: false,
      error: "INVALID_PASSWORD",
      path: normalizedPath,
    };
  }

  // 为后续中间件校验准备的 token 载荷：
  // - ownerPath: 密码所属路径（密码域）
  // - passwordVersion: 当前聚合后的密码值（当密码修改时会改变）
  // - verifiedAt: 签发时间（仅用于调试）
  const payload = JSON.stringify({
    ownerPath: passwordOwnerPath || normalizedPath,
    passwordVersion: meta.password,
    verifiedAt: new Date().toISOString(),
  });
  const token = await encryptValue(payload, encryptionSecret);

  return {
    requiresPassword: true,
    verified: true,
    token,
    path: passwordOwnerPath || normalizedPath,
  };
};

/**
 * 校验路径密码 token 是否对当前路径仍然有效
 * - 用于 FS 浏览路由的前置访问控制
 * @param {D1Database} db
 * @param {string} rawPath
 * @param {string|null} token
 * @param {string} encryptionSecret
 */
const verifyPathPasswordToken = async (db, rawPath, token, encryptionSecret) => {
  const { meta, normalizedPath, requiresPassword, passwordOwnerPath } = await resolveEffectiveMetaForPath(db, rawPath);

  // 当前路径未配置密码：无需 token，直接放行
  if (!requiresPassword) {
    return {
      requiresPassword: false,
      verified: true,
    };
  }

  if (!token || typeof token !== "string") {
    return {
      requiresPassword: true,
      verified: false,
      error: "MISSING_TOKEN",
    };
  }

  let payloadText;
  try {
    payloadText = await decryptValue(token, encryptionSecret);
  } catch {
    return {
      requiresPassword: true,
      verified: false,
      error: "INVALID_TOKEN",
    };
  }

  let payload;
  try {
    payload = JSON.parse(payloadText);
  } catch {
    return {
      requiresPassword: true,
      verified: false,
      error: "INVALID_TOKEN",
    };
  }

  if (!payload || typeof payload !== "object") {
    return {
      requiresPassword: true,
      verified: false,
      error: "INVALID_TOKEN",
    };
  }

  // 密码域 ownerPath 必须匹配当前路径的密码 owner
  if (!payload.ownerPath || normalizeFsPath(payload.ownerPath) !== normalizeFsPath(passwordOwnerPath || normalizedPath)) {
    return {
      requiresPassword: true,
      verified: false,
      error: "PATH_MISMATCH",
    };
  }

  // 当管理员修改了密码时，这里的 passwordVersion 将与当前 meta.password 不一致
  if (!payload.passwordVersion || payload.passwordVersion !== meta.password) {
    return {
      requiresPassword: true,
      verified: false,
      error: "PASSWORD_CHANGED",
    };
  }

  return {
    requiresPassword: true,
    verified: true,
  };
};

// 负责把 principal 映射为 legacy FS 服务层仍在使用的 userInfo 结构。
const unifiedFsAuthMiddleware = async (c, next) => {
  const principal = c.get("principal");

  if (!principal || principal.type === "anonymous") {
    throw new AuthenticationError("需要认证访问");
  }

  if (principal.isAdmin) {
    c.set("userInfo", {
      type: UserType.ADMIN,
      id: principal.id,
      hasFullAccess: true,
    });
  } else if (principal.type === UserType.API_KEY) {
    const apiKeyInfo = principal.attributes?.keyInfo ?? {
      id: principal.id,
      basicPath: principal.attributes?.basicPath ?? "/",
      permissions: principal.authorities,
    };

    c.set("userInfo", {
      type: UserType.API_KEY,
      info: apiKeyInfo,
      hasFullAccess: false,
    });
  } else {
    throw new AuthorizationError("不支持的身份类型");
  }

  await next();
};

const FS_BASE_PATH = "/api/fs";

const baseFsPolicy = usePolicy("fs.base");
fsRoutes.use(`${FS_BASE_PATH}/*`, baseFsPolicy, unifiedFsAuthMiddleware);

fsRoutes.use(`${FS_BASE_PATH}/list`, usePolicy("fs.list"));
fsRoutes.use(`${FS_BASE_PATH}/get`, usePolicy("fs.read"));
fsRoutes.use(`${FS_BASE_PATH}/download`, usePolicy("fs.read"));
fsRoutes.use(`${FS_BASE_PATH}/content`, usePolicy("fs.read"));
fsRoutes.use(`${FS_BASE_PATH}/file-link`, usePolicy("fs.share-link"));

// 目录路径密码校验接口
fsRoutes.post(`${FS_BASE_PATH}/meta/password/verify`, async (c) => {
  const db = c.env.DB;

  let body;
  try {
    body = await c.req.json();
  } catch {
    body = null;
  }

  const rawPath = body?.path ?? "/";
  const plainPassword = body?.password ?? "";

  if (!rawPath || typeof rawPath !== "string") {
    throw new ValidationError("请提供有效的路径");
  }

  const encryptionSecret = getEncryptionSecret(c);
  const result = await buildPathPasswordVerification(db, rawPath, plainPassword, encryptionSecret);

  // 未配置密码：直接返回“无需密码”的成功结果
  if (!result.requiresPassword) {
    return jsonOk(
      c,
      {
        verified: true,
        requiresPassword: false,
        token: null,
        path: result.path,
      },
      "该路径不需要密码",
    );
  }

  if (!result.verified) {
    const message = result.error === "EMPTY_PASSWORD" ? "密码不能为空" : "密码错误";
    return c.json(
      {
        ...createErrorResponse(ApiStatus.FORBIDDEN, message, "FS_PATH_PASSWORD_INVALID"),
        data: {
          path: result.path,
          requiresPassword: true,
        },
      },
      ApiStatus.FORBIDDEN,
    );
  }

  return jsonOk(
    c,
    {
      verified: true,
      requiresPassword: true,
      token: result.token,
      path: result.path,
    },
    "路径密码验证成功",
  );
});


const getServiceParams = (userInfo) => {
  if (userInfo.type === UserType.ADMIN) {
    return { userIdOrInfo: userInfo.id, userType: UserType.ADMIN };
  }
  return { userIdOrInfo: userInfo.info, userType: UserType.API_KEY };
};

// 统一命名：根据用户类型获取“存储配置”
const getStorageConfigByUserType = async (db, configId, userIdOrInfo, userType, encryptionSecret) => {
  if (userType === UserType.ADMIN) {
    return await getStorageConfigByIdForAdmin(db, configId, userIdOrInfo);
  }

  // 当前仅支持 API Key 用户访问公共存储配置
  if (userType === UserType.API_KEY) {
    const factory = ensureRepositoryFactory(db);
    const principalStorageAclRepository = factory.getPrincipalStorageAclRepository();

    // 解析主体信息用于存储 ACL：subjectType + subjectId
    const subjectType = "API_KEY";
    const subjectId = typeof userIdOrInfo === "string" ? userIdOrInfo : userIdOrInfo?.id ?? null;

    let allowed = true;

    if (principalStorageAclRepository && subjectId) {
      try {
        const allowedConfigIds = await principalStorageAclRepository.findConfigIdsBySubject(subjectType, subjectId);
        if (Array.isArray(allowedConfigIds) && allowedConfigIds.length > 0) {
          // 当存在显式 ACL 记录时，启用白名单模式
          allowed = allowedConfigIds.includes(configId);
        }
      } catch (error) {
        console.error("加载存储 ACL 失败，拒绝扩大存储访问范围：", error);
        throw new AuthorizationError("无法验证存储访问权限");
      }
    }

    if (!allowed) {
      return null;
    }

    return await getPublicStorageConfigById(db, configId);
  }

  // 其他用户类型目前不允许直接访问存储配置
  return null;
};

const sharedContext = {
  // FS 子路由只需要这三个 helper 即可完成鉴权相关操作。
  getAccessibleMounts: getAccessibleMountsForUser,
  getServiceParams,
  getStorageConfigByUserType,
  verifyPathPasswordToken,
};

registerBrowseRoutes(fsRoutes, sharedContext);
registerWriteRoutes(fsRoutes, sharedContext);
registerMultipartRoutes(fsRoutes, sharedContext);
registerOpsRoutes(fsRoutes, sharedContext);
registerSearchShareRoutes(fsRoutes, sharedContext);


export default fsRoutes;
