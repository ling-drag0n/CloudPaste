import { ValidationError } from "../../http/errors.js";
import { ApiStatus } from "../../constants/index.js";
import { jsonOk } from "../../utils/common.js";
import { MountManager } from "../../storage/managers/MountManager.js";
import { FileSystem } from "../../storage/fs/FileSystem.js";
import { useRepositories } from "../../utils/repositories.js";
import { FileShareService } from "../../services/fileShareService.js";
import { getEncryptionSecret } from "../../utils/environmentUtils.js";
import { usePolicy } from "../../security/policies/policies.js";

const parseJsonBody = async (c, next) => {
  const body = await c.req.json();
  c.set("jsonBody", body);
  await next();
};

const extractSearchParams = (queryParams) => {
  const query = queryParams.q || "";
  const scope = queryParams.scope || "global";
  const mountId = queryParams.mount_id || "";
  const path = queryParams.path || "";
  const pathToken = queryParams.path_token || "";
  const pathTokens = queryParams.path_tokens || "";
  const limit = parseInt(queryParams.limit) || 50;
  const cursor = queryParams.cursor || "";

  return {
    query,
    scope,
    mountId,
    path,
    pathToken,
    pathTokens,
    limit: Math.min(limit, 200),
    cursor,
  };
};

export const registerSearchShareRoutes = (router, helpers) => {
  const { getServiceParams, getAccessibleMounts = async () => null, verifyPathPasswordToken } = helpers;

  router.post(
    "/api/fs/create-share",
    parseJsonBody,
    usePolicy("fs.share.create", { pathResolver: (c) => c.get("jsonBody")?.path }),
    async (c) => {
      const db = c.env.DB;
      const encryptionSecret = getEncryptionSecret(c);
      const userInfo = c.get("userInfo");
      const { userIdOrInfo, userType } = getServiceParams(userInfo);

      const body = c.get("jsonBody");
      const { path, password, expires_in, max_views, remark, slug, encryption_meta } = body;

      if (!path) {
        throw new ValidationError("文件路径不能为空");
      }

      const repositoryFactory = useRepositories(c);
      const svc = new FileShareService(db, encryptionSecret, repositoryFactory);
      const result = await svc.createShareFromFileSystem(path, userIdOrInfo, userType, {
        password: password || null,
        expiresIn: Number(expires_in) || 0,
        maxViews: Number(max_views) || 0,
        remark: remark || undefined,
        slug: slug || null,
        encryptionMeta: encryption_meta || null,
      });

      return jsonOk(c, result, "分享创建成功");
    }
  );

  router.get("/api/fs/search", usePolicy("fs.search"), async (c) => {
    const db = c.env.DB;
    const encryptionSecret = getEncryptionSecret(c);
    const repositoryFactory = c.get("repos");
    const searchParams = extractSearchParams(c.req.query());
    const userInfo = c.get("userInfo");
    const { userIdOrInfo, userType } = getServiceParams(userInfo);

    if (!searchParams.query || searchParams.query.trim().length < 3) {
      throw new ValidationError("搜索查询至少需要3个字符");
    }

    const mountManager = new MountManager(db, encryptionSecret, repositoryFactory, { env: c.env });
    const fileSystem = new FileSystem(mountManager, c.env);
    const accessibleMounts = await getAccessibleMounts(db, userIdOrInfo, userType);
    const pathToken = c.req.header("x-fs-path-token") || searchParams.pathToken || null;
    const rawTokens = c.req.header("x-fs-path-tokens") || searchParams.pathTokens || "";
    const pathTokens = String(rawTokens || "")
      .split(",")
      .map((item) => item.trim())
      .filter((item) => item.length > 0);
    const result = await fileSystem.searchFiles(searchParams.query, searchParams, userIdOrInfo, userType, accessibleMounts, {
      pathToken,
      pathTokens,
      verifyPathPasswordToken,
      encryptionSecret,
    });

    return jsonOk(c, result, "搜索完成");
  });
};
