import { ApiStatus, UserType } from "../constants/index.js";
import { ValidationError, NotFoundError, DriverError, AuthorizationError } from "../http/errors.js";
import { ShareRecordService } from "./share/ShareRecordService.js";
import { StorageQuotaGuard } from "../storage/usage/StorageQuotaGuard.js";

export class FileShareService {
  constructor(db, encryptionSecret, repositoryFactory = null) {
    this.db = db;
    this.encryptionSecret = encryptionSecret;
    this.repositoryFactory = repositoryFactory;
    this.quota = new StorageQuotaGuard(db, encryptionSecret, repositoryFactory);
    this.records = new ShareRecordService(db, encryptionSecret, repositoryFactory);
  }

  async _assertSystemMaxUploadSize(fileSizeBytes) {
    const size = Number(fileSizeBytes) || 0;
    if (size <= 0) return;

    const systemRepo = this.repositoryFactory?.getSystemRepository?.();
    if (!systemRepo) return;

    const DEFAULT_MAX_UPLOAD_SIZE_MB = 100;
    const setting = await systemRepo.getSettingMetadata("max_upload_size").catch(() => null);
    const limitMb = setting?.value ? parseInt(setting.value, 10) : DEFAULT_MAX_UPLOAD_SIZE_MB;
    const limitBytes = Number.isFinite(limitMb) ? limitMb * 1024 * 1024 : DEFAULT_MAX_UPLOAD_SIZE_MB * 1024 * 1024;
    if (limitBytes > 0 && size > limitBytes) {
      throw new ValidationError(`文件大小超出系统限制（最大 ${limitMb} MB）`);
    }
  }

  async _assertShareStorageQuotaByKey({ storageConfig, storageKey, incomingBytes, context }) {
    const cfg = storageConfig;
    const key = String(storageKey || "");
    const size = Number(incomingBytes) || 0;
    if (!cfg?.id || !key || size <= 0) {
      return;
    }

    let oldBytes = null;
    try {
      const fileRepo = this.repositoryFactory.getFileRepository();
      if (cfg.storage_type) {
        const existing = await fileRepo.findByStoragePath(cfg.id, key, cfg.storage_type).catch(() => null);
        if (existing && typeof existing.size === "number" && existing.size >= 0) {
          oldBytes = existing.size;
        }
      }
    } catch {
      oldBytes = null;
    }

    await this.quota.assertCanConsume({
      storageConfigId: cfg.id,
      incomingBytes: size,
      oldBytes,
      context: context || "share",
    });
  }

  _getTelegramShareMaxDirectUploadBytes(storageConfig) {
    const mode = String(storageConfig?.bot_api_mode || "official").trim().toLowerCase();
    if (mode === "self_hosted") {
      // 自建 Bot API Server：按你的后端能力决定限制，这里不强行限制
      return Infinity;
    }
    // 官方 Bot API：本项目约定 share 直传只支持小文件，避免“能传不能下/不能预览”的坑
    return 20 * 1024 * 1024;
  }

  _assertTelegramShareDirectUploadAllowed(storageConfig, fileSizeBytes) {
    const size = Number(fileSizeBytes) || 0;
    if (size <= 0) return;
    const maxBytes = this._getTelegramShareMaxDirectUploadBytes(storageConfig);
    if (Number.isFinite(maxBytes) && size > maxBytes) {
      throw new ValidationError(
        "Telegram 分享上传：未勾选“自建 Bot API Server”时仅支持 ≤ 20MB；更大的文件请用“挂载浏览器”的分片上传，或勾选自建。",
      );
    }
  }

  _resolveAclSubject(userType, userIdOrInfo) {
    if (userType !== UserType.API_KEY) {
      return { subjectType: null, subjectId: null };
    }

    const subjectId =
      typeof userIdOrInfo === "string"
        ? userIdOrInfo
        : userIdOrInfo?.id ?? null;

    if (!subjectId) {
      return { subjectType: null, subjectId: null };
    }

    return { subjectType: "API_KEY", subjectId };
  }

  async resolveStorageConfig({
    storageConfigId = null,
    userType = UserType.ADMIN,
    withSecrets = false,
    subjectType = null,
    subjectId = null,
  } = {}) {
    const repo = this.repositoryFactory.getStorageConfigRepository();
    if (!repo) {
      return null;
    }
    const requirePublic = userType === UserType.API_KEY;
    const findByIdFn =
      withSecrets && typeof repo.findByIdWithSecrets === "function"
        ? repo.findByIdWithSecrets.bind(repo)
        : repo.findById.bind(repo);

    let allowedConfigIdsSet = null;

    if (
      requirePublic &&
      subjectType &&
      subjectId &&
      this.repositoryFactory?.getPrincipalStorageAclRepository
    ) {
      try {
        const aclRepo = this.repositoryFactory.getPrincipalStorageAclRepository();
        const allowedIds = await aclRepo.findConfigIdsBySubject(subjectType, subjectId);
        if (Array.isArray(allowedIds) && allowedIds.length > 0) {
          allowedConfigIdsSet = new Set(allowedIds);
        }
      } catch (error) {
        console.error("加载存储 ACL 失败，拒绝扩大存储配置选择范围：", error);
        throw new AuthorizationError("无法验证存储访问权限");
      }
    }

    const isAllowed = (cfg) => {
      if (!cfg) return false;
      if (requirePublic && cfg.is_public !== 1) return false;
      if (allowedConfigIdsSet && !allowedConfigIdsSet.has(cfg.id)) return false;
      return true;
    };

    // 显式指定 ID 时优先使用该配置
    if (storageConfigId) {
      let existing = null;
      if (requirePublic && typeof repo.findPublicById === "function") {
        existing = await repo.findPublicById(storageConfigId).catch(() => null);
      } else {
        existing = await findByIdFn(storageConfigId).catch(() => null);
      }

      if (isAllowed(existing)) {
        return existing;
      }
    }

    // 其次尝试默认配置
    let config = await repo.findDefault({ requirePublic, withSecrets }).catch(() => null);
    if (isAllowed(config)) {
      return config;
    }

    // 最后尝试公开/全部列表中的第一个允许项
    const fallbackList = requirePublic ? await repo.findPublic().catch(() => []) : await repo.findAll().catch(() => []);
    if (!Array.isArray(fallbackList) || fallbackList.length === 0) {
      return null;
    }

    if (!allowedConfigIdsSet) {
      return fallbackList[0];
    }

    const firstAllowed = fallbackList.find((cfg) => isAllowed(cfg));
    return firstAllowed || null;
  }

  // 预签名初始化
  async createPresignedShareUpload({
    filename,
    fileSize,
    contentType,
    path = null,
    storage_config_id = null,
    sha256 = null,
    userIdOrInfo,
    userType,
  }) {
    if (!filename) throw new ValidationError("缺少 filename");
    await this._assertSystemMaxUploadSize(fileSize);

    const { ObjectStore } = await import("../storage/object/ObjectStore.js");
    const { subjectType, subjectId } = this._resolveAclSubject(userType, userIdOrInfo);
    const cfg = await this.resolveStorageConfig({
      storageConfigId: storage_config_id,
      userType,
      subjectType,
      subjectId,
    });
    if (!cfg) throw new ValidationError("未找到可用的存储配置，无法生成预签名URL");

    const store = new ObjectStore(this.db, this.encryptionSecret, this.repositoryFactory);
    const presign = await store.presignUpload({
      storage_config_id: cfg.id,
      directory: path,
      filename,
      fileSize,
      contentType,
      sha256,
    });
    if (!cfg.storage_type) throw new ValidationError("存储配置缺少 storage_type");
    // 配额校验：考虑覆盖同路径时排除旧记录体积
    await this._assertShareStorageQuotaByKey({
      storageConfig: cfg,
      storageKey: presign.key,
      incomingBytes: Number(fileSize) || 0,
      context: "share-presign",
    });
    return {
      uploadUrl: presign.uploadUrl,
      contentType: presign.contentType,
      expiresIn: presign.expiresIn,
      key: presign.key,
      filename,
      provider_type: presign.provider_type,
      storage_config_id: presign.storage_config_id,
      headers: presign.headers || undefined,
      sha256: presign.sha256 || (sha256 ? String(sha256) : undefined),
      skipUpload: presign.skipUpload === true ? true : undefined,
    };
  }

  // 预签名提交
  async commitPresignedShareUpload({
    key,
    storage_config_id,
    filename,
    size,
    etag,
    sha256 = null,
    slug,
    remark,
    password,
    expiresIn,
    maxViews,
    useProxy,
    originalFilename,
    userIdOrInfo,
    userType,
    request,
  }) {
    await this._assertSystemMaxUploadSize(Number(size));
    // 必须包含 key + storage_config_id
    const finalKey = key;
    const finalStorageConfigId = storage_config_id;
    if (!finalKey || !finalStorageConfigId) {
      throw new ValidationError("缺少 key 或 storage_config_id");
    }

    const { subjectType, subjectId } = this._resolveAclSubject(userType, userIdOrInfo);
    const storageConfig = await this.resolveStorageConfig({
      storageConfigId: finalStorageConfigId,
      userType,
      subjectType,
      subjectId,
    });
    if (!storageConfig) {
      throw new NotFoundError("存储配置不存在");
    }

    await this._assertShareStorageQuotaByKey({
      storageConfig,
      storageKey: finalKey,
      incomingBytes: Number(size) || 0,
      context: "share-presign-commit",
    });

    const { ObjectStore } = await import("../storage/object/ObjectStore.js");
    const store = new ObjectStore(this.db, this.encryptionSecret, this.repositoryFactory);
    const { getEffectiveMimeType } = await import("../utils/fileUtils.js");
    const mimeType = getEffectiveMimeType(undefined, filename);
    const commit = await store.commitUpload({
      storage_config_id: storageConfig.id,
      key: finalKey,
      filename,
      size,
      etag,
      sha256,
      contentType: mimeType,
    });

    // 根据命名策略决定是否覆盖更新记录
    const { shouldUseRandomSuffix } = await import("../utils/common.js");
    const useRandom = await shouldUseRandomSuffix(this.db).catch(() => false);
    return await this.records.createShareRecord({
      mount: null,
      fsPath: null,
      storageSubPath: commit.key,
      filename,
      size: Number(size) || 0,
      remark: remark || "",
      userIdOrInfo,
      userType,
      slug: slug || null,
      override: false,
      password: password || null,
      expiresInHours: Number(expiresIn) || 0,
      maxViews: Number(maxViews) || 0,
      useProxy: useProxy !== undefined ? !!useProxy : undefined,
      mimeType,
      request,
      uploadResult: commit.uploadResult,
      originalFilenameUsed: !!originalFilename,
      storageConfig,
      updateIfExists: !useRandom,
    });
  }

  // 直传
  async uploadDirectToStorageAndShare(filename, bodyStream, fileSize, userIdOrInfo, userType, options = {}) {
    if (!filename) throw new ValidationError("缺少文件名参数");
    const normalizedSize = Number(fileSize) || 0;
    if (!bodyStream || normalizedSize <= 0) throw new ValidationError("上传内容为空");
    await this._assertSystemMaxUploadSize(normalizedSize);
    const { ObjectStore } = await import("../storage/object/ObjectStore.js");
    const storedFilename = filename;
    const { getEffectiveMimeType } = await import("../utils/fileUtils.js");
    const mimeType = getEffectiveMimeType(options.contentType, storedFilename);

    // 选择 storage_config：显式优先，否则选默认（API Key 需公开 + ACL 白名单）
    const { subjectType, subjectId } = this._resolveAclSubject(userType, userIdOrInfo);
    const cfg = await this.resolveStorageConfig({
      storageConfigId: options.storage_config_id,
      userType,
      subjectType,
      subjectId,
    });
    if (!cfg) throw new ValidationError("未找到可用的存储配置，无法上传");

    // Telegram（Bot API）share 直传：只支持小文件（不走断点续传）
    if (String(cfg.storage_type) === "TELEGRAM") {
      this._assertTelegramShareDirectUploadAllowed(cfg, normalizedSize);
    }

    const store = new ObjectStore(this.db, this.encryptionSecret, this.repositoryFactory);
    // 预先计算最终Key用于配额校验（考虑命名策略与冲突重命名）
    const plannedKey = await store.getPlannedKey(cfg.id, options.path || null, storedFilename);
    if (!cfg.storage_type) throw new ValidationError("存储配置缺少 storage_type");
    await this._assertShareStorageQuotaByKey({
      storageConfig: cfg,
      storageKey: plannedKey,
      incomingBytes: normalizedSize,
      context: "share-upload-direct",
    });
    const result = await store.uploadDirect({
      storage_config_id: cfg.id,
      directory: options.path || null,
      filename: storedFilename,
      bodyStream,
      size: normalizedSize,
      contentType: mimeType,
      uploadId: options.uploadId || null,
      userIdOrInfo,
      userType,
    });

    const { shouldUseRandomSuffix } = await import("../utils/common.js");
    const useRandom = await shouldUseRandomSuffix(this.db).catch(() => false);
    return await this.records.createShareRecord({
      mount: null,
      fsPath: null,
      storageSubPath: result.storagePath,
      filename,
      size: normalizedSize,
      remark: options.remark || "",
      userIdOrInfo,
      userType,
      slug: options.slug || null,
      override: Boolean(options.override),
      password: options.password || null,
      expiresInHours: options.expiresIn || 0,
      maxViews: options.maxViews || 0,
      useProxy: options.useProxy,
      mimeType,
      request: options.request || null,
      uploadResult: { storagePath: result.storagePath, publicUrl: result.publicUrl, etag: result.etag },
      originalFilenameUsed: Boolean(options.originalFilename),
      storageConfig: cfg,
      updateIfExists: !useRandom,
    });
  }

  // 通过 ObjectStore + File/Blob 上传并创建分享记录（多存储通用）
  async uploadFileViaObjectStoreAndShare(file, userIdOrInfo, userType, options = {}) {
    if (!file) throw new ValidationError("缺少文件参数");
    const normalizedSize = Number(file.size ?? options.size ?? 0) || 0;
    if (normalizedSize <= 0) {
      throw new ValidationError("上传内容为空");
    }
    await this._assertSystemMaxUploadSize(normalizedSize);

    const { ObjectStore } = await import("../storage/object/ObjectStore.js");
    const { getEffectiveMimeType } = await import("../utils/fileUtils.js");
    const storedFilename = file.name || options.filename || "upload.bin";
    const mimeType = getEffectiveMimeType(options.contentType || file.type || null, storedFilename);

    // 选择 storage_config：显式优先，否则选默认（API Key 需公开 + ACL 白名单）
    const { subjectType, subjectId } = this._resolveAclSubject(userType, userIdOrInfo);
    const cfg = await this.resolveStorageConfig({
      storageConfigId: options.storage_config_id,
      userType,
      subjectType,
      subjectId,
    });
    if (!cfg) throw new ValidationError("未找到可用的存储配置，无法上传");

    // Telegram（Bot API）share 表单上传：只支持小文件（不走断点续传）
    if (String(cfg.storage_type) === "TELEGRAM") {
      this._assertTelegramShareDirectUploadAllowed(cfg, normalizedSize);
    }

    const store = new ObjectStore(this.db, this.encryptionSecret, this.repositoryFactory);

    // 预先计算最终Key用于配额校验（考虑命名策略与冲突重命名）
    const plannedKey = await store.getPlannedKey(cfg.id, options.path || null, storedFilename);
    if (!cfg.storage_type) throw new ValidationError("存储配置缺少 storage_type");
    await this._assertShareStorageQuotaByKey({
      storageConfig: cfg,
      storageKey: plannedKey,
      incomingBytes: normalizedSize,
      context: "share-upload-file",
    });

    const uploadResult = await store.uploadFileForShare({
      storage_config_id: cfg.id,
      directory: options.path || null,
      filename: storedFilename,
      file,
      size: normalizedSize,
      contentType: mimeType,
      uploadId: options.uploadId || null,
      userIdOrInfo,
      userType,
    });

    const { shouldUseRandomSuffix } = await import("../utils/common.js");
    const useRandom = await shouldUseRandomSuffix(this.db).catch(() => false);

    return await this.records.createShareRecord({
      mount: null,
      fsPath: null,
      storageSubPath: uploadResult.storagePath,
      filename: storedFilename,
      size: normalizedSize,
      remark: options.remark || "",
      userIdOrInfo,
      userType,
      slug: options.slug || null,
      override: Boolean(options.override),
      password: options.password || null,
      expiresInHours: options.expiresIn || 0,
      maxViews: options.maxViews || 0,
      useProxy: options.useProxy,
      mimeType,
      request: options.request || null,
      uploadResult: {
        storagePath: uploadResult.storagePath,
        publicUrl: uploadResult.publicUrl,
        etag: uploadResult.etag,
      },
      originalFilenameUsed: Boolean(options.originalFilename),
      storageConfig: cfg,
      updateIfExists: !useRandom,
    });
  }


  // 从 FS 创建分享
  async createShareFromFileSystem(fsPath, userIdOrInfo, userType, options = {}) {
    const { MountManager } = await import("../storage/managers/MountManager.js");
    const { FileSystem } = await import("../storage/fs/FileSystem.js");
    const mountManager = new MountManager(this.db, this.encryptionSecret, this.repositoryFactory);
    const fileSystem = new FileSystem(mountManager);
    const fileInfo = await fileSystem.getFileInfo(fsPath, userIdOrInfo, userType);
    if (!fileInfo || fileInfo.isDirectory) {
      throw new ValidationError("只能为文件创建分享");
    }
    const { mount, subPath } = await mountManager.getDriverByPath(fsPath, userIdOrInfo, userType);
    return await this.records.createShareRecord({
      mount,
      fsPath,
      storageSubPath: subPath || "",
      filename: fileInfo.name,
      size: fileInfo.size || 0,
      remark: options.remark || `来自文件系统: ${fsPath}`,
      userIdOrInfo,
      userType,
      slug: options.slug || null,
      override: Boolean(options.override),
      password: options.password || null,
      expiresInHours: options.expiresIn || 0,
      maxViews: options.maxViews || 0,
      useProxy: options.useProxy,
      mimeType: fileInfo.mimetype || fileInfo.mimeType || undefined,
      request: options.request || null,
      uploadResult: null,
      originalFilenameUsed: true,
    });
  }

  // URL工具
  async validateUrlMetadata(url) {
    let metadata = null;
    try {
      const parsedUrl = new URL(url);
      if (parsedUrl.protocol !== "http:" && parsedUrl.protocol !== "https:") {
        throw new ValidationError("仅支持 HTTP/HTTPS URL");
      }
      let response; let method = "HEAD"; let corsSupported = false;
      try {
        response = await fetch(url, { method: "HEAD" });
        corsSupported = response.ok;
        if (!response.ok) {
          throw new DriverError("HEAD 请求失败", { details: { status: response.status } });
        }
      } catch {
        response = await fetch(url, { method: "GET" });
        method = "GET";
        corsSupported = response.ok;
      }
      if (!response.ok) {
        throw new DriverError("远程服务返回错误状态", {
          details: { status: response.status },
        });
      }
      const contentType = response.headers.get("content-type") || "application/octet-stream";
      const contentLength = response.headers.get("content-length");
      const lastModified = response.headers.get("last-modified");
      let filename = "";
      const segments = parsedUrl.pathname.split("/").filter(Boolean);
      if (segments.length > 0) { const last = segments[segments.length-1]; if (last.includes(".") && !last.endsWith(".")) { try { filename = decodeURIComponent(last);} catch { filename = last; } } }
      if (!filename) { const cd = response.headers.get("content-disposition"); const m = cd && cd.match(/filename[^;=\n]*=((['"]).*?\2|[^;\n]*)/i); if (m && m[1]) filename = m[1].replace(/['"]/g, ""); }
      if (!filename) filename = "download";
      metadata = { url, filename, contentType, size: contentLength ? parseInt(contentLength, 10) : null, lastModified, method, corsSupported };
      return metadata;
    } catch (error) {
      if (error instanceof TypeError && error.message.includes("Invalid URL")) {
        throw new ValidationError("无效的 URL 格式");
      }
      if (error instanceof DriverError) {
        throw error;
      }
      // 其它网络/上游错误归类为 DriverError
      throw new DriverError("获取 URL 元信息失败", {
        details: { cause: error?.message },
      });
    }
  }
  async proxyUrlContent(url) {
    const parsedUrl = new URL(url);
    if (parsedUrl.protocol !== "http:" && parsedUrl.protocol !== "https:") {
      throw new ValidationError("仅支持 HTTP/HTTPS URL");
    }
    return await fetch(url);
  }
}

export default FileShareService;
