/**
 * 文件查看路由
 * 处理文件分享的查看、下载、预览功能
 */
import { Hono } from "hono";
import { getEncryptionSecret } from "../utils/environmentUtils.js";
import { handleFileDownload, checkAndDeleteExpiredFile } from "../services/fileViewService.js";
import { getFileBySlug, isFileAccessible } from "../services/fileService.js";
import { useRepositories } from "../utils/repositories.js";
import { ApiStatus } from "../constants/index.js";
import { AppError, AuthorizationError, NotFoundError } from "../http/errors.js";
import { verifyPassword } from "../utils/crypto.js";
import { verifyShareAccessToken } from "../utils/shareAccessToken.js";

const app = new Hono();

// ==========================================
// 路由处理器
// ==========================================

/**
 * Share 内容交付统一入口
 * - 负责密码/过期/可访问性校验
 * - 最终委托 FileViewService 进行直链/代理决策与数据流输出
 */
async function handleShareDelivery(c, { forceDownload, forceProxy }) {
  const slug = c.req.param("slug");
  const db = c.env.DB;
  const encryptionSecret = getEncryptionSecret(c);
  const repositoryFactory = useRepositories(c);

  // 统一从文件记录出发做密码与过期校验，避免直链路径绕过密码保护
  const file = await getFileBySlug(db, slug, encryptionSecret);
  if (!file) {
    throw new NotFoundError("文件不存在");
  }

  const url = new URL(c.req.url);
  const passwordParam = url.searchParams.get("password");

  if (file.password) {
    // 优先校验短时效访问令牌（?at=），兼容旧 ?password= 明文参数
    let authorized = false;
    const atToken = url.searchParams.get("at");
    if (atToken) {
      authorized = await verifyShareAccessToken(encryptionSecret, atToken, { type: "file", slug });
    }
    if (!authorized) {
      if (!passwordParam) {
        throw new AuthorizationError("需要密码访问此文件");
      }
      authorized = await verifyPassword(passwordParam, file.password);
    }
    if (!authorized) {
      throw new AuthorizationError("密码不正确");
    }
  }

  const accessCheck = await isFileAccessible(db, file, encryptionSecret);
  if (!accessCheck.accessible) {
    if (accessCheck.reason === "expired") {
      await checkAndDeleteExpiredFile(db, file, encryptionSecret, repositoryFactory);
      throw new AppError("文件已过期", { status: ApiStatus.GONE, code: "GONE", expose: true });
    }
    throw new AuthorizationError("文件不可访问");
  }

  const response = await handleFileDownload(slug, db, encryptionSecret, c.req.raw, forceDownload, repositoryFactory, {
    forceProxy: !!forceProxy,
  });

  // 仅在“真实下载”且交付成功时递增下载计数；
  // 仅打开/预览分享页（forceDownload=false）不会计数。
  if (forceDownload && (response.status === 302 || (response.status >= 200 && response.status < 300))) {
    try {
      const fileRepo = repositoryFactory.getFileRepository();
      await fileRepo.incrementDownloads(file.id);
    } catch (e) {
      console.warn("递增下载计数失败（已忽略）:", e?.message || e);
    }
  }

  return response;
}

// Share 本地代理入口 /api/s/:slug（等价 FS 的 /api/p）
// - 永远同源本地流式 200，不做直链/Worker 决策
// - down=true/1 表示下载语义，否则为预览语义
app.get("/api/s/:slug", async (c) => {
  const url = new URL(c.req.url);
  const down = url.searchParams.get("down");
  const legacyMode = url.searchParams.get("mode");
  const forceDownload =
    (down && down !== "0" && down !== "false") ||
    legacyMode === "attachment" ||
    legacyMode === "download";
  return handleShareDelivery(c, { forceDownload, forceProxy: true });
});

// Share 本地代理入口
// - 例如：/api/s/:slug/:filename
// - filename 仅用于展示/识别类型，不参与文件定位
app.get("/api/s/:slug/:filename", async (c) => {
  const url = new URL(c.req.url);
  const down = url.searchParams.get("down");
  const legacyMode = url.searchParams.get("mode");
  const forceDownload =
    (down && down !== "0" && down !== "false") ||
    legacyMode === "attachment" ||
    legacyMode === "download";
  return handleShareDelivery(c, { forceDownload, forceProxy: true });
});

// Share 文本/编码检测专用同源内容口（共用）
app.get("/api/share/content/:slug", async (c) => {
  return handleShareDelivery(c, { forceDownload: false, forceProxy: true });
});

// Share 文本/编码检测专用同源内容口
app.get("/api/share/content/:slug/:filename", async (c) => {
  return handleShareDelivery(c, { forceDownload: false, forceProxy: true });
});

export default app;
