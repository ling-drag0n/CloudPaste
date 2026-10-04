import { ApiStatus } from "../../constants/index.js";
import { AppError, NotFoundError, AuthenticationError, ValidationError } from "../../http/errors.js";
import { jsonOk } from "../../utils/common.js";
import { guardShareFile, getFileBySlug, getPublicFileInfo } from "../../services/fileService.js";
import { verifyPassword } from "../../utils/crypto.js";
import { generateShareAccessToken } from "../../utils/shareAccessToken.js";
import { useRepositories } from "../../utils/repositories.js";
import { getEncryptionSecret } from "../../utils/environmentUtils.js";
import { LinkService } from "../../storage/link/LinkService.js";

/**
 * 文件夹分享的最小元信息负载
 * - 文件夹没有单一文件 URL，避免调用 LinkService/getPublicFileInfo 生成无效直链
 * @param {Object} file
 */
const buildFolderMetaPayload = (file) => ({
  id: file.id,
  slug: file.slug,
  filename: file.filename,
  remark: file.remark,
  target_type: "folder",
  requires_password: !!file.password,
  expires_at: file.expires_at,
  max_views: file.max_views,
  views: file.views,
});

export const registerFilesPublicRoutes = (router) => {
  const getShareFileInfoHandler = async (c) => {
    const db = c.env.DB;
    const { slug } = c.req.param();
    const encryptionSecret = getEncryptionSecret(c);
    const requestUrl = new URL(c.req.url);

    const file = await getFileBySlug(db, slug);

    // 文件夹分享：短路返回最小元信息负载
    if (file.target_type === "folder") {
      return jsonOk(c, buildFolderMetaPayload(file), "获取分享成功");
    }

    const requiresPassword = !!file.password;

    if (!requiresPassword) {
      const { file: guardedFile, isExpired } = await guardShareFile(db, slug, encryptionSecret, { incrementViews: false });

      if (isExpired) {
        const repositoryFactory = useRepositories(c);
        const fileRepository = repositoryFactory.getFileRepository();
        const fileStillExists = await fileRepository.findById(guardedFile.id).catch(() => null);
        if (fileStillExists) {
          console.log(`文件(${guardedFile.id})达到最大访问次数但未被删除，再次尝试删除...`);
          const { checkAndDeleteExpiredFile } = await import("../../services/fileViewService.js");
          await checkAndDeleteExpiredFile(db, guardedFile, encryptionSecret, repositoryFactory);
        }
        throw new AppError("文件已达到最大查看次数", { status: ApiStatus.GONE, code: "GONE", expose: true });
      }

      const repositoryFactory = useRepositories(c);
      const linkService = new LinkService(db, encryptionSecret, repositoryFactory);
      const link = await linkService.getShareExternalLink(guardedFile, null);
      const publicInfo = await getPublicFileInfo(db, guardedFile, requiresPassword, link, encryptionSecret, {
        baseOrigin: requestUrl.origin,
      });

      return jsonOk(c, publicInfo, "获取文件成功");
    }

    const repositoryFactory = useRepositories(c);
    const linkService = new LinkService(db, encryptionSecret, repositoryFactory);
    const link = await linkService.getShareExternalLink(file, null);
    const publicInfo = await getPublicFileInfo(db, file, true, link, encryptionSecret, {
      baseOrigin: requestUrl.origin,
    });
    return jsonOk(c, publicInfo, "获取文件成功");
  };

  const verifyShareFilePasswordHandler = async (c) => {
    const db = c.env.DB;
    const { slug } = c.req.param();
    const body = await c.req.json();
    const encryptionSecret = getEncryptionSecret(c);
    const requestUrl = new URL(c.req.url);

    if (!body.password) {
      throw new ValidationError("密码是必需的");
    }

    const file = await getFileBySlug(db, slug);

    // 文件夹分享：先校验密码，再短路返回最小元信息负载
    if (file.target_type === "folder") {
      if (file.password) {
        const passwordValid = await verifyPassword(body.password, file.password);
        if (!passwordValid) {
          throw new AuthenticationError("密码不正确");
        }
      }
      const meta = buildFolderMetaPayload(file);
      // 签发短时效访问令牌：后续 list/download URL 携带 ?at=，避免明文密码进 URL
      meta.accessToken = await generateShareAccessToken(encryptionSecret, { type: "folder", slug });
      return jsonOk(c, meta, "密码验证成功");
    }

    if (!file.password) {
      const repositoryFactory = useRepositories(c);
      const linkService = new LinkService(db, encryptionSecret, repositoryFactory);
      const link = await linkService.getShareExternalLink(file, null);
      const publicInfo = await getPublicFileInfo(db, file, false, link, encryptionSecret, {
        baseOrigin: requestUrl.origin,
      });
      return jsonOk(c, publicInfo, "此文件不需要密码");
    }

    const passwordValid = await verifyPassword(body.password, file.password);
    if (!passwordValid) {
      throw new AuthenticationError("密码不正确");
    }

    const { file: guardedFile, isExpired } = await guardShareFile(db, slug, encryptionSecret, { incrementViews: false });

    if (isExpired) {
      throw new AppError("文件已达到最大查看次数", { status: ApiStatus.GONE, code: "GONE", expose: true });
    }

    const repositoryFactory = useRepositories(c);
    const linkService = new LinkService(db, encryptionSecret, repositoryFactory);
    const link = await linkService.getShareExternalLink(guardedFile, null);
    let fileWithPassword = guardedFile;

    if (fileWithPassword.password) {
      const fileRepository = repositoryFactory.getFileRepository();
      const passwordInfo = await fileRepository.getFilePassword(guardedFile.id);
      if (passwordInfo && passwordInfo.plain_password) {
        fileWithPassword = {
          ...fileWithPassword,
          plain_password: passwordInfo.plain_password,
        };
      }
    }

    const publicInfo = await getPublicFileInfo(db, fileWithPassword, false, link, encryptionSecret, {
      baseOrigin: requestUrl.origin,
    });
    // 签发短时效访问令牌：下载/预览 URL 携带 ?at=，避免明文密码进 URL（旧 ?password= 仍兼容）
    publicInfo.accessToken = await generateShareAccessToken(encryptionSecret, { type: "file", slug });

    return jsonOk(c, publicInfo, "密码验证成功");
  };

  // Share 控制面（JSON）
  router.get("/api/share/get/:slug", getShareFileInfoHandler);

  // Share 密码验证
  router.post("/api/share/verify/:slug", verifyShareFilePasswordHandler);
};
