import { ValidationError } from "../../http/errors.js";
import { ApiStatus, UserType } from "../../constants/index.js";
import { generateFileId, generateUniqueFileSlug } from "../../utils/common.js";
import { getSettingMetadata } from "../systemService.js";
import { hashPassword } from "../../utils/crypto.js";
import { getEffectiveMimeType } from "../../utils/fileUtils.js";

export class ShareRecordService {
  constructor(db, encryptionSecret, repositoryFactory) {
    this.db = db;
    this.encryptionSecret = encryptionSecret;
    this.repositoryFactory = repositoryFactory;
  }

  resolveCreatedBy(userIdOrInfo, userType) {
    if (userType === UserType.ADMIN || userType === "admin") {
      return userIdOrInfo;
    }
    if (userType === UserType.API_KEY || userType === "apiKey") {
      if (typeof userIdOrInfo === "object" && userIdOrInfo?.id) return `apikey:${userIdOrInfo.id}`;
      return `apikey:${userIdOrInfo}`;
    }
    return "anonymous";
  }

  async createShareRecord({
    mount,
    fsPath,
    targetType = "file",
    storageSubPath = "",
    filename,
    size,
    remark = "",
    userIdOrInfo,
    userType,
    slug,
    override = false,
    password = null,
    expiresInHours = 0,
    maxViews = 0,
    useProxy = undefined,
    mimeType,
    request = null,
    uploadResult = null,
    originalFilenameUsed = true,
    storageConfig = null,
    updateIfExists = false,
    encryptionMeta = null,
  }) {
    if (!mount?.storage_config_id && !storageConfig) {
      throw new ValidationError("缺少挂载或存储配置，无法创建分享");
    }

    const createdBy = this.resolveCreatedBy(userIdOrInfo, userType);
    const finalTargetType = targetType === "folder" ? "folder" : "file";

    const finalSlug = await generateUniqueFileSlug(this.db, slug, override, {
      userIdOrInfo,
      userType,
      encryptionSecret: this.encryptionSecret,
      repositoryFactory: this.repositoryFactory,
      db: this.db,
    });

    const fileRepository = this.repositoryFactory.getFileRepository();

    const fileId = generateFileId();
    const now = new Date().toISOString();
    const expiresAt = expiresInHours > 0 ? new Date(Date.now() + expiresInHours * 3600000).toISOString() : null;
    const maxViewsValue = maxViews > 0 ? maxViews : null;
    // use_proxy 默认值：优先使用显式传入，其次根据全局设置 default_use_proxy 决定
    let useProxyFlag;
    if (useProxy === true) {
      useProxyFlag = 1;
    } else if (useProxy === false) {
      useProxyFlag = 0;
    } else {
      // 未显式传入时，根据系统设置 default_use_proxy 决定
      try {
        const setting = await getSettingMetadata(this.db, "default_use_proxy", this.repositoryFactory);
        const defaultUseProxy = setting ? setting.value === "true" : false;
        useProxyFlag = defaultUseProxy ? 1 : 0;
      } catch (error) {
        console.warn("读取 default_use_proxy 设置失败，使用默认直链模式:", error);
        useProxyFlag = 0;
      }
    }
    const passwordHash = password ? await hashPassword(password) : null;
    const normalizedMimeType = mimeType ?? getEffectiveMimeType(undefined, filename) ?? "application/octet-stream";
    // 加密元数据：对象则序列化，null 表示未加密（覆盖上传时显式清空旧值）
    const encryptionMetaJson =
      encryptionMeta && typeof encryptionMeta === "object" ? JSON.stringify(encryptionMeta) : null;

    // 存储路径语义：
    // - FS 挂载创建分享：storageSubPath 来自 MountManager，可能带前导 "/"，这里统一去掉
    // - storage-first（ObjectStore/share upload）：优先使用 uploadResult.storagePath（应为对象 key）
    const relativePath = (storageSubPath || "").replace(/^\/+/u, "");
    const storagePath = mount?.storage_config_id
      ? relativePath
      : (uploadResult?.storagePath || relativePath || fsPath || filename);
    const storageType = mount?.storage_type || storageConfig?.storage_type;
    if (!storageType) {
      throw new ValidationError("存储配置缺少 storage_type");
    }
    const storageConfigId = mount?.storage_config_id || storageConfig?.id;

    let finalFileId = fileId;
    let createdAt = now;
    let views = 0;

    if (updateIfExists && storageConfigId && storagePath) {
      const existing = await fileRepository.findByStoragePath(storageConfigId, storagePath, storageType).catch(() => null);
      if (existing) {
        // 更新已有记录（覆盖模式）：不改变 slug/created_by/created_at
        await fileRepository.updateFile(existing.id, {
          filename,
          size,
          mimetype: normalizedMimeType,
          target_type: finalTargetType,
          etag: uploadResult?.etag || null,
          remark,
          expires_at: expiresAt,
          max_views: maxViewsValue,
          use_proxy: useProxyFlag,
          encryption_meta: encryptionMetaJson,
          updated_at: now,
        });

        if (password) {
          await fileRepository.upsertFilePasswordRecord(existing.id, password);
        }

        finalFileId = existing.id;
        createdAt = existing.created_at || now;
        views = existing.views || 0;

        const fileForUrl = {
          id: existing.id,
          slug: existing.slug,
          filename,
          mimetype: normalizedMimeType,
          size,
          remark,
          created_at: createdAt,
          storage_config_id: storageConfigId,
          storage_type: storageType,
          storage_path: storagePath,
          file_path: fsPath,
          target_type: finalTargetType,
          use_proxy: useProxyFlag,
          public_url: uploadResult?.publicUrl || null,
          etag: uploadResult?.etag || null,
          max_views: maxViewsValue,
          expires_at: expiresAt,
          views,
          created_by: existing.created_by,
        };

        fileForUrl.password_plain = password || null;
        return {
          id: existing.id,
          slug: existing.slug,
          filename,
          mimetype: normalizedMimeType,
          size,
          remark,
          created_at: createdAt,
          requires_password: Boolean(password || existing.password),
          views: views,
          max_views: maxViewsValue,
          expires_at: expiresAt,
          url: finalTargetType === "folder" ? `/folder/${existing.slug}` : `/file/${existing.slug}`,
          use_proxy: useProxyFlag,
          encryption_mode: encryptionMeta ? encryptionMeta.mode : null,
          created_by: existing.created_by,
          used_original_filename: originalFilenameUsed,
          storage_path: storagePath,
          storage_type: storageType,
          target_type: finalTargetType,
        };
      }
    }

    // 新建记录（默认或不存在时）
    await fileRepository.createFile({
      id: fileId,
      slug: finalSlug,
      filename,
      storage_config_id: storageConfigId,
      storage_type: storageType,
      storage_path: storagePath,
      file_path: fsPath,
      mimetype: normalizedMimeType,
      target_type: finalTargetType,
      size,
      etag: uploadResult?.etag || null,
      remark,
      password: passwordHash,
      expires_at: expiresAt,
      max_views: maxViewsValue,
      use_proxy: useProxyFlag,
      encryption_meta: encryptionMetaJson,
      created_by: createdBy,
      created_at: now,
      updated_at: now,
    });

    if (password) {
      await fileRepository.upsertFilePasswordRecord(fileId, password);
    }

    const fileForUrl = {
      id: fileId,
      slug: finalSlug,
      filename,
      mimetype: normalizedMimeType,
      size,
      remark,
      created_at: now,
      storage_config_id: storageConfigId,
      storage_type: storageType,
      storage_path: storagePath,
      file_path: fsPath,
      target_type: finalTargetType,
      use_proxy: useProxyFlag,
      public_url: uploadResult?.publicUrl || null,
      etag: uploadResult?.etag || null,
      max_views: maxViewsValue,
      expires_at: expiresAt,
      views: 0,
      created_by: createdBy,
      password_plain: password || null,
    };

    const response = {
      id: fileId,
      slug: finalSlug,
      filename,
      mimetype: normalizedMimeType,
      size,
      remark,
      created_at: now,
      requires_password: Boolean(password),
      views: 0,
      max_views: maxViewsValue,
      expires_at: expiresAt,
      // 分享页 URL，前端通过该地址进入 fileshare 视图
      url: finalTargetType === "folder" ? `/folder/${finalSlug}` : `/file/${finalSlug}`,
      use_proxy: useProxyFlag,
      encryption_mode: encryptionMeta ? encryptionMeta.mode : null,
      created_by: createdBy,
      used_original_filename: originalFilenameUsed,
      storage_path: storagePath,
      storage_type: storageType,
      target_type: finalTargetType,
    };

    return response;
  }
}
