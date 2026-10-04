import { Hono } from "hono";
import { ValidationError } from "../http/errors.js";
import { ApiStatus, UserType } from "../constants/index.js";
import { usePolicy } from "../security/policies/policies.js";
import { resolvePrincipal } from "../security/helpers/principal.js";
import { getEncryptionSecret } from "../utils/environmentUtils.js";
import { FileShareService } from "../services/fileShareService.js";
import { LinkService } from "../storage/link/LinkService.js";
import { getFileBySlug, getPublicFileInfo } from "../services/fileService.js";
import { useRepositories } from "../utils/repositories.js";
import { getQueryBool, getQueryInt, jsonOk } from "../utils/common.js";

const requireFilesCreate = usePolicy("files.create");

const router = new Hono();


const parseFormData = async (c, next) => {
  const formData = await c.req.formData();
  c.set("formData", formData);
  await next();
};

// “直传即分享”（storage-first，S3 直传）
router.put("/api/upload-direct/:filename", requireFilesCreate, async (c) => {
  const db = c.env.DB;
  const encryptionSecret = getEncryptionSecret(c);
  const repositoryFactory = useRepositories(c);

  const { filename } = c.req.param();
  if (!filename) {
    throw new ValidationError("缺少 filename 参数");
  }

  const principalInfo = resolvePrincipal(c, { allowedTypes: [UserType.ADMIN, UserType.API_KEY] });
  const { type: userType, userId, apiKeyInfo } = principalInfo;
  const userIdOrInfo = userType === UserType.ADMIN ? userId : apiKeyInfo;

  const bodyStream = c.req.raw?.body;
  if (!bodyStream) {
    throw new ValidationError("请求体为空");
  }

  const declaredLengthHeader = c.req.header("content-length");
  const declaredLength = declaredLengthHeader ? parseInt(declaredLengthHeader, 10) : 0;

  const storageConfigId = c.req.query("storage_config_id") || null;
  const uploadId = c.req.query("upload_id") || null;

  // E2E 加密元数据（可选，URL 编码的 JSON）
  let encryptionMeta = null;
  const encryptionMetaRaw = c.req.query("encryption_meta");
  if (encryptionMetaRaw) {
    try {
      encryptionMeta = JSON.parse(encryptionMetaRaw);
    } catch {
      encryptionMeta = null;
    }
  }

  const shareParams = {
    storage_config_id: storageConfigId,
    path: c.req.query("path") || null,
    slug: c.req.query("slug") || null,
    remark: c.req.query("remark") || "",
    password: c.req.query("password") || null,
    expiresIn: getQueryInt(c, "expires_in", 0),
    maxViews: getQueryInt(c, "max_views", 0),
    override: getQueryBool(c, "override", false),
    useProxy: c.req.query("use_proxy") != null ? getQueryBool(c, "use_proxy", true) : undefined,
    originalFilename: getQueryBool(c, "original_filename", false),
    contentType: c.req.header("content-type") || undefined,
    request: c.req.raw,
    encryptionMeta,
  };

  const shareService = new FileShareService(db, encryptionSecret, repositoryFactory);

  const result = await shareService.uploadDirectToStorageAndShare(
    filename,
    bodyStream,
    declaredLength,
    userIdOrInfo,
    userType,
    { ...shareParams, uploadId: uploadId || null }
  );

  // 对齐 /api/share/get/:slug：统一返回公开文件信息（含 previewUrl/downloadUrl/linkType/previewSelection），不再返回分享页 URL
  // upload-direct 属于受信任调用场景：
  // - 即使设置了密码，这里也视为“已通过校验”，始终返回 preview/download 入口，方便调用方直接使用
  // - 对于代理模式（share 内容路由），自动在预览/下载 URL 上附加 password 查询参数
  try {
    const file = await getFileBySlug(db, result.slug, encryptionSecret);
    const hasPassword = !!file.password;
    const linkService = new LinkService(db, encryptionSecret, repositoryFactory);
    const link = await linkService.getShareExternalLink(file, null);
    const requestUrl = new URL(c.req.url);
    const publicInfo = await getPublicFileInfo(
      db,
      file,
      false,
      link,
      encryptionSecret,
      { baseOrigin: requestUrl.origin },
    );

    // 如果是代理模式且本次上传提供了密码，为预览/下载 URL 附加 password 查询参数
    let previewUrl = publicInfo.previewUrl || null;
    let downloadUrl = publicInfo.downloadUrl || null;
    const linkType = publicInfo.linkType || null;
    const password = shareParams.password || null;
    if (password && (linkType === "proxy" || file.use_proxy)) {
      if (previewUrl && !previewUrl.includes("password=")) {
        const separator = previewUrl.includes("?") ? "&" : "?";
        previewUrl = `${previewUrl}${separator}password=${encodeURIComponent(password)}`;
      }
      if (downloadUrl && !downloadUrl.includes("password=")) {
        const separator = downloadUrl.includes("?") ? "&" : "?";
        downloadUrl = `${downloadUrl}${separator}password=${encodeURIComponent(password)}`;
      }
    }

    const response = {
      ...publicInfo,
      previewUrl,
      downloadUrl,
      requires_password: hasPassword,
    };

    return jsonOk(c, response, "文件上传成功");
  } catch (error) {
    // 兜底：生成公开信息失败时，返回基础分享记录但移除 url 字段，避免泄露分享页 URL
    console.warn("upload-direct: 生成公开文件信息失败，将返回基础分享记录：", error);
    const { url, ...rest } = result || {};
    return jsonOk(c, rest, "文件上传成功");
  }
});

// 流式分享上传：通过 PUT /api/share/upload 使用原始 body 直传
router.put("/api/share/upload", requireFilesCreate, async (c) => {
  const db = c.env.DB;
  const encryptionSecret = getEncryptionSecret(c);
  const repositoryFactory = useRepositories(c);

  const principalInfo = resolvePrincipal(c, { allowedTypes: [UserType.ADMIN, UserType.API_KEY] });
  const { type: userType, userId, apiKeyInfo } = principalInfo;
  const userIdOrInfo = userType === UserType.ADMIN ? userId : apiKeyInfo;

  const bodyStream = c.req.raw?.body;
  if (!bodyStream) {
    throw new ValidationError("请求体为空");
  }

  const declaredLengthHeader = c.req.header("content-length");
  const declaredLength = declaredLengthHeader ? parseInt(declaredLengthHeader, 10) : 0;

  const filenameHeaderRaw = c.req.header("x-share-filename");
  if (!filenameHeaderRaw) {
    throw new ValidationError("缺少 x-share-filename 头部");
  }

  let filenameHeader = filenameHeaderRaw;
  try {
    filenameHeader = decodeURIComponent(filenameHeaderRaw);
  } catch {
    // 解码失败时回退到原始值，避免影响兼容性
    filenameHeader = filenameHeaderRaw;
  }

  let options = {};
  const optionsHeader = c.req.header("x-share-options");
  if (optionsHeader) {
    try {
      const decoded = Buffer.from(optionsHeader, "base64").toString("utf8");
      options = JSON.parse(decoded) || {};
    } catch {
      options = {};
    }
  }

  const storageConfigId = options.storage_config_id || null;
  const uploadId = options.upload_id || null;

  const shareParams = {
    storage_config_id: storageConfigId,
    path: options.path || null,
    slug: options.slug || null,
    remark: options.remark || "",
    password: options.password || null,
    expiresIn: Number(options.expires_in || 0),
    maxViews: Number(options.max_views || 0),
    override: false,
    useProxy: options.use_proxy !== undefined ? !!options.use_proxy : undefined,
    originalFilename: !!options.original_filename,
    contentType: c.req.header("content-type") || undefined,
    request: c.req.raw,
    uploadId: uploadId || null,
    encryptionMeta: options.encryption_meta || null,
  };

  const shareService = new FileShareService(db, encryptionSecret, repositoryFactory);

  const result = await shareService.uploadDirectToStorageAndShare(
    filenameHeader,
    bodyStream,
    declaredLength,
    userIdOrInfo,
    userType,
    shareParams
  );

  return jsonOk(c, result, "文件上传成功");
});

// 通用分享上传：通过 ObjectStore + File，多存储通用
router.post("/api/share/upload", requireFilesCreate, parseFormData, async (c) => {
  const db = c.env.DB;
  const encryptionSecret = getEncryptionSecret(c);
  const repositoryFactory = useRepositories(c);

  const formData = c.get("formData");
  const file = formData.get("file");
  if (!file) {
    throw new ValidationError("缺少文件参数");
  }

  const storageConfigId = formData.get("storage_config_id") || null;
  const path = formData.get("path") || null;
  const slug = formData.get("slug") || null;
  const remark = formData.get("remark") || "";
  const password = formData.get("password") || null;
  const expiresIn = Number(formData.get("expires_in") || 0);
  const maxViews = Number(formData.get("max_views") || 0);
  const useProxyRaw = formData.get("use_proxy");
  const originalFilenameRaw = formData.get("original_filename");
  const uploadId = formData.get("upload_id") || null;

  const principalInfo = resolvePrincipal(c, { allowedTypes: [UserType.ADMIN, UserType.API_KEY] });
  const { type: userType, userId, apiKeyInfo } = principalInfo;
  const userIdOrInfo = userType === UserType.ADMIN ? userId : apiKeyInfo;

  const shareService = new FileShareService(db, encryptionSecret, repositoryFactory);

  const shareParams = {
    storage_config_id: storageConfigId,
    path: path || null,
    slug: slug || null,
    remark,
    password,
    expiresIn,
    maxViews,
    override: false,
    useProxy:useProxyRaw === "true"? true: useProxyRaw === "false"? false: undefined,
    originalFilename: originalFilenameRaw === "true",
    contentType: file.type || undefined,
    request: c.req.raw,
    uploadId: uploadId || null,
    encryptionMeta: (() => {
      const raw = formData.get("encryption_meta");
      if (!raw) return null;
      try {
        return JSON.parse(raw);
      } catch {
        return null;
      }
    })(),
  };

  const result = await shareService.uploadFileViaObjectStoreAndShare(
    file,
    userIdOrInfo,
    userType,
    shareParams
  );

  return jsonOk(c, result, "文件上传成功");
});

// 预签名上传（上传即分享）的初始化
router.post("/api/share/presign", requireFilesCreate, async (c) => {
  const db = c.env.DB;
  const encryptionSecret = getEncryptionSecret(c);
  const repositoryFactory = useRepositories(c);
  const body = await c.req.json();

  const principalInfo = resolvePrincipal(c, { allowedTypes: [UserType.ADMIN, UserType.API_KEY] });
  const { type: userType, userId, apiKeyInfo } = principalInfo;
  const userIdOrInfo = userType === UserType.ADMIN ? userId : apiKeyInfo;

  const { filename, fileSize, contentType, path, storage_config_id, sha256, e2e } = body || {};
  if (!filename) {
    throw new ValidationError("缺少 filename");
  }

  const shareService = new FileShareService(db, encryptionSecret, repositoryFactory);

  const presign = await shareService.createPresignedShareUpload({
    filename,
    fileSize,
    contentType,
    path: path || null,
    storage_config_id: storage_config_id || null,
    sha256: sha256 || null,
    e2e: e2e === true,
    userIdOrInfo,
    userType,
  });

  return jsonOk(c, presign, "生成预签名成功");
});

// 预签名提交（创建分享记录）
router.post("/api/share/commit", requireFilesCreate, async (c) => {
  const db = c.env.DB;
  const encryptionSecret = getEncryptionSecret(c);
  const repositoryFactory = useRepositories(c);
  const body = await c.req.json();

  const principalInfo = resolvePrincipal(c, { allowedTypes: [UserType.ADMIN, UserType.API_KEY] });
  const { type: userType, userId, apiKeyInfo } = principalInfo;
  const userIdOrInfo = userType === UserType.ADMIN ? userId : apiKeyInfo;

  const { key, storage_config_id, filename, size, etag, sha256, slug, remark, password, expires_in, max_views, use_proxy, original_filename, encryption_meta } =
    body || {};
  if (!filename) {
    throw new ValidationError("缺少 filename");
  }

  // 新协议：必须 key + storage_config_id
  const finalKey = key || null;
  const finalStorageConfigId = storage_config_id || null;
  if (!finalKey || !finalStorageConfigId) {
    throw new ValidationError("缺少 key 或 storage_config_id");
  }

  const shareService = new FileShareService(db, encryptionSecret, repositoryFactory);

  const result = await shareService.commitPresignedShareUpload({
    key: finalKey,
    storage_config_id: finalStorageConfigId,
    filename,
    size,
    etag,
    sha256: sha256 || null,
    slug: slug || null,
    remark: remark || "",
    password: password || null,
    expiresIn: Number(expires_in) || 0,
    maxViews: Number(max_views) || 0,
    useProxy: use_proxy !== undefined ? !!use_proxy : undefined,
    originalFilename: !!original_filename,
    encryptionMeta: encryption_meta || null,
    userIdOrInfo,
    userType,
    request: c.req.raw,
  });

  return jsonOk(c, result, "预签名上传提交成功");
});

// =============== URL 信息/代理（并入分享上传模块） ===============
const parseJsonBody = async (c, next) => {
  const body = await c.req.json();
  c.set("jsonBody", body);
  await next();
};

router.post("/api/share/url/info", requireFilesCreate, parseJsonBody, async (c) => {
  const db = c.env.DB;
  const body = c.get("jsonBody") || {};

  if (!body.url) {
    throw new ValidationError("缺少URL参数");
  }

  const encryptionSecret = getEncryptionSecret(c);
  const { FileShareService } = await import("../services/fileShareService.js");
  const shareService = new FileShareService(db, encryptionSecret);
  const metadata = await shareService.validateUrlMetadata(body.url);
  return jsonOk(c, metadata, "URL验证成功");
});

router.get("/api/share/url/proxy", requireFilesCreate, async (c) => {
  const db = c.env.DB;
  const url = c.req.query("url");
  if (!url) {
    throw new ValidationError("缺少URL参数");
  }

  const encryptionSecret = getEncryptionSecret(c);
  const { FileShareService } = await import("../services/fileShareService.js");
  const shareService = new FileShareService(db, encryptionSecret);
  return await shareService.proxyUrlContent(url);
});

// URL → 预签名：根据URL元信息生成上传预签名
export default router;
