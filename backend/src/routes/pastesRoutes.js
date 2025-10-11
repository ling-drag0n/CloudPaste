import { Hono } from "hono";
import { authGateway } from "../middlewares/authGatewayMiddleware.js";
import {
  getAllPastes,
  getUserPastes,
  getPasteById,
  batchDeletePastes,
  batchDeleteUserPastes,
  updatePaste,
  createPaste,
  getPasteBySlug,
  verifyPastePassword,
  incrementAndCheckPasteViews,
  isPasteAccessible,
} from "../services/pasteService.js";
import { HTTPException } from "hono/http-exception";
import { ApiStatus, DbTables } from "../constants/index.js";
import { createErrorResponse } from "../utils/common.js";
import { RepositoryFactory } from "../repositories/index.js";

const app = new Hono();

// ==================== 公共访问接口（无需认证） ====================

// 创建新的文本分享
app.post("/api/paste", authGateway.requireText(), async (c) => {
  const db = c.env.DB;
  const body = await c.req.json();

  try {
    // 获取认证信息
    const userId = authGateway.utils.getUserId(c);
    const authType = authGateway.utils.getAuthType(c);

    // 创建者信息
    const created_by = authType === "admin" ? userId : authType === "apikey" ? `apikey:${userId}` : null;

    // 创建文本分享
    const paste = await createPaste(db, body, created_by);

    // 返回创建结果
    return c.json({
      ...paste,
      authorizedBy: authType, // 添加授权方式信息，方便调试
    });
  } catch (error) {
    // 处理特定错误
    if (error.message.includes("链接后缀已被占用")) {
      throw new HTTPException(ApiStatus.CONFLICT, { message: error.message });
    }
    // 处理其他错误
    throw new HTTPException(ApiStatus.INTERNAL_ERROR, { message: error.message || "创建分享失败" });
  }
});

// 获取文本分享（公共访问）
app.get("/api/paste/:slug", async (c) => {
  const db = c.env.DB;
  const slug = c.req.param("slug");

  try {
    // 获取文本分享
    const paste = await getPasteBySlug(db, slug);

    // 检查是否需要密码
    if (paste.has_password) {
      return c.json({
        slug: paste.slug,
        hasPassword: true,
        remark: paste.remark,
        expires_at: paste.expires_at,
        max_views: paste.max_views,
        views: paste.views,
        created_at: paste.created_at,
        created_by: paste.created_by,
        requiresPassword: true,
      });
    }

    // 检查是否可访问
    if (!isPasteAccessible(paste)) {
      throw new HTTPException(ApiStatus.GONE, { message: "文本分享已过期或超过最大查看次数" });
    }

    // 原子化增加查看次数并检查状态
    const result = await incrementAndCheckPasteViews(db, paste.id, paste.max_views);

    // 如果是最后一次正常访问，返回内容（即使文本已被删除）
    if (result.isLastNormalAccess) {
      return c.json({
        slug: paste.slug,
        content: paste.content,
        remark: paste.remark,
        expires_at: paste.expires_at,
        max_views: paste.max_views,
        views: result.paste.views, // 使用数据库中的真实views
        created_at: paste.created_at,
        created_by: paste.created_by,
        hasPassword: false,
        isLastView: true, // 标识这是最后一次查看
      });
    }

    // 如果文本已被删除且不是最后一次正常访问，返回错误
    if (result.isDeleted) {
      throw new HTTPException(ApiStatus.GONE, { message: "文本分享已达到最大查看次数" });
    }

    // 返回文本内容（使用准确的views数据）
    return c.json({
      slug: paste.slug,
      content: paste.content,
      remark: paste.remark,
      expires_at: paste.expires_at,
      max_views: paste.max_views,
      views: result.paste.views, // 使用数据库中的真实views
      created_at: paste.created_at,
      created_by: paste.created_by,
      hasPassword: false,
      isLastView: result.isLastView, // 标识这是否是最后一次查看
    });
  } catch (error) {
    console.error("获取文本分享失败:", error);
    throw error;
  }
});

// 使用密码获取文本分享
app.post("/api/paste/:slug", async (c) => {
  const db = c.env.DB;
  const slug = c.req.param("slug");
  const { password } = await c.req.json();

  if (!password) {
    throw new HTTPException(ApiStatus.BAD_REQUEST, { message: "请提供密码" });
  }

  try {
    // 先验证密码（不增加views）
    const paste = await verifyPastePassword(db, slug, password, false);

    // 原子化增加查看次数并检查状态
    const result = await incrementAndCheckPasteViews(db, paste.id, paste.max_views);

    // 如果是最后一次正常访问，返回内容（即使文本已被删除）
    if (result.isLastNormalAccess) {
      return c.json({
        slug: paste.slug,
        content: paste.content,
        remark: paste.remark,
        hasPassword: true,
        plain_password: paste.plain_password,
        expires_at: paste.expires_at,
        max_views: paste.max_views,
        views: result.paste.views, // 使用数据库中的真实views
        created_at: paste.created_at,
        updated_at: paste.updated_at,
        created_by: paste.created_by,
        isLastView: true, // 标识这是最后一次查看
      });
    }

    // 如果文本已被删除且不是最后一次正常访问，返回错误
    if (result.isDeleted) {
      throw new HTTPException(ApiStatus.GONE, { message: "文本分享已达到最大查看次数" });
    }

    // 返回文本内容（使用准确的views数据）
    return c.json({
      slug: paste.slug,
      content: paste.content,
      remark: paste.remark,
      hasPassword: true,
      plain_password: paste.plain_password,
      expires_at: paste.expires_at,
      max_views: paste.max_views,
      views: result.paste.views, // 使用数据库中的真实views
      created_at: paste.created_at,
      updated_at: paste.updated_at,
      created_by: paste.created_by,
      isLastView: result.isLastView, // 标识这是否是最后一次查看
    });
  } catch (error) {
    console.error("验证文本密码失败:", error);
    throw error;
  }
});

// 获取原始文本内容
app.get("/api/raw/:slug", async (c) => {
  const db = c.env.DB;
  const slug = c.req.param("slug");
  const password = c.req.query("password"); // 从查询参数中获取密码

  try {
    // 获取文本分享
    const paste = await getPasteBySlug(db, slug);

    // 如果需要密码且未提供或密码不正确
    if (paste.has_password) {
      if (!password) {
        throw new HTTPException(ApiStatus.UNAUTHORIZED, { message: "需要密码才能访问此内容" });
      }

      // 验证密码
      try {
        await verifyPastePassword(db, slug, password, false); // 不增加查看次数
      } catch (error) {
        throw new HTTPException(ApiStatus.UNAUTHORIZED, { message: "密码错误" });
      }

      // 对于有密码的文本，raw接口需要增加查看次数（因为这可能是首次内容访问）
      const result = await incrementAndCheckPasteViews(db, paste.id, paste.max_views);

      // 如果是最后一次正常访问，继续返回内容（即使文本已被删除）
      if (result.isLastNormalAccess) {
        // 继续执行，返回原始内容
      } else if (result.isDeleted) {
        // 如果文本已被删除且不是最后一次正常访问，返回错误
        throw new HTTPException(ApiStatus.GONE, { message: "文本分享已达到最大查看次数" });
      }
    } else {
      // 对于无密码的文本，raw接口不增加查看次数
      // 但需要检查是否仍然可访问
      if (!isPasteAccessible(paste)) {
        throw new HTTPException(ApiStatus.GONE, { message: "文本分享已过期或超过最大查看次数" });
      }
    }

    // 设置响应头并返回原始文本内容
    c.header("Content-Type", "text/plain; charset=utf-8");
    c.header("Content-Disposition", `inline; filename="${slug}.txt"`);

    return c.text(paste.content);
  } catch (error) {
    console.error("获取原始文本内容失败:", error);

    // 根据错误类型返回适当的错误状态和信息
    if (error instanceof HTTPException) {
      c.header("Content-Type", "text/plain; charset=utf-8");
      return c.text(error.message, error.status);
    }

    c.header("Content-Type", "text/plain; charset=utf-8");
    return c.text("获取内容失败", ApiStatus.INTERNAL_ERROR);
  }
});

// ==================== 管理接口（需要认证） ====================
// 获取文本列表（统一认证）
app.get("/api/pastes", authGateway.requireText(), async (c) => {
  const db = c.env.DB;
  const userType = authGateway.utils.getAuthType(c);
  const userId = authGateway.utils.getUserId(c);
  const apiKeyInfo = authGateway.utils.getApiKeyInfo(c);

  try {
    let result;

    if (userType === "admin") {
      // 管理员：获取查询参数，支持page和offset两种分页方式
      const limit = parseInt(c.req.query("limit") || "10");
      const page = parseInt(c.req.query("page") || "1");
      const offset = parseInt(c.req.query("offset") || (page - 1) * limit);
      const search = c.req.query("search");
      const created_by = c.req.query("created_by");

      // 使用管理员服务获取文本列表，传递offset参数
      result = await getAllPastes(db, page, limit, created_by, search, offset);
    } else {
      // API密钥用户：获取查询参数
      const limit = parseInt(c.req.query("limit") || "30");
      const offset = parseInt(c.req.query("offset") || "0");
      const search = c.req.query("search");

      // 构建查询选项
      // 使用用户服务获取文本列表
      result = await getUserPastes(db, userId, limit, offset, search);
    }

    const response = {
      code: ApiStatus.SUCCESS,
      message: "获取成功",
      data: result.results || result,
      success: true,
    };

    // 添加分页信息
    if (result.pagination) {
      response.pagination = result.pagination;
    }

    // API密钥用户需要返回密钥信息
    if (userType === "apikey") {
      response.key_info = apiKeyInfo;
    }

    return c.json(response);
  } catch (error) {
    console.error("获取文本列表错误:", error);
    return c.json(createErrorResponse(ApiStatus.INTERNAL_ERROR, error.message || "获取文本列表失败"), ApiStatus.INTERNAL_ERROR);
  }
});

// 获取单个文本详情（统一认证）
app.get("/api/pastes/:id", authGateway.requireText(), async (c) => {
  const db = c.env.DB;
  const { id } = c.req.param();
  const userType = authGateway.utils.getAuthType(c);
  const userId = authGateway.utils.getUserId(c);

  try {
    let result;

    if (userType === "admin") {
      // 管理员：可以获取任何文本的详情
      result = await getPasteById(db, id);
    } else {
      // API密钥用户：只能获取自己文本的详情
      const { RepositoryFactory } = await import("../repositories/index.js");
      const { DbTables } = await import("../constants/index.js");

      const repositoryFactory = new RepositoryFactory(db);
      const pasteRepository = repositoryFactory.getPasteRepository();

      const paste = await pasteRepository.findOne(DbTables.PASTES, {
        id: id,
        created_by: `apikey:${userId}`,
      });

      if (!paste) {
        return c.json(createErrorResponse(ApiStatus.NOT_FOUND, "文本不存在或无权访问"), ApiStatus.NOT_FOUND);
      }

      // 确保has_password是布尔类型
      paste.has_password = !!paste.password;

      // 如果文本有密码，查询明文密码
      let plainPassword = null;
      if (paste.has_password) {
        plainPassword = await pasteRepository.findPasswordByPasteId(paste.id);
      }

      result = {
        ...paste,
        plain_password: plainPassword,
      };
    }

    return c.json({
      code: ApiStatus.SUCCESS,
      message: "获取成功",
      data: result,
      success: true,
    });
  } catch (error) {
    console.error("获取文本详情错误:", error);
    return c.json(createErrorResponse(ApiStatus.INTERNAL_ERROR, error.message || "获取文本详情失败"), ApiStatus.INTERNAL_ERROR);
  }
});

// 批量删除文本（统一认证）
app.delete("/api/pastes/batch-delete", authGateway.requireText(), async (c) => {
  const db = c.env.DB;
  const userType = authGateway.utils.getAuthType(c);
  const userId = authGateway.utils.getUserId(c);

  try {
    const { ids } = await c.req.json();
    let deletedCount;

    if (userType === "admin") {
      // 管理员：可以删除任何文本
      deletedCount = await batchDeletePastes(db, ids, false);
    } else {
      // API密钥用户：只能删除自己的文本
      deletedCount = await batchDeleteUserPastes(db, ids, userId);
    }

    return c.json({
      code: ApiStatus.SUCCESS,
      message: `已删除 ${deletedCount} 个分享`,
      success: true,
    });
  } catch (error) {
    console.error("批量删除文本错误:", error);
    return c.json(createErrorResponse(ApiStatus.INTERNAL_ERROR, error.message || "批量删除文本失败"), ApiStatus.INTERNAL_ERROR);
  }
});

// 更新文本（统一认证）
app.put("/api/pastes/:slug", authGateway.requireText(), async (c) => {
  const db = c.env.DB;
  const { slug } = c.req.param();
  const userType = authGateway.utils.getAuthType(c);
  const userId = authGateway.utils.getUserId(c);
  const body = await c.req.json();

  try {
    let result;

    if (userType === "admin") {
      // 管理员：可以更新任何文本
      result = await updatePaste(db, slug, body);
    } else {
      // API密钥用户：只能更新自己的文本
      result = await updatePaste(db, slug, body, `apikey:${userId}`);
    }

    return c.json({
      code: ApiStatus.SUCCESS,
      message: "文本更新成功",
      data: {
        id: result.id,
        slug: result.slug,
      },
      success: true,
    });
  } catch (error) {
    console.error("更新文本错误:", error);
    return c.json(createErrorResponse(ApiStatus.INTERNAL_ERROR, error.message || "更新文本失败"), ApiStatus.INTERNAL_ERROR);
  }
});

// 管理员专用：清理过期文本
app.post("/api/pastes/clear-expired", authGateway.requireAdmin(), async (c) => {
  const db = c.env.DB;

  try {
    const deletedCount = await batchDeletePastes(db, null, true);

    return c.json({
      code: ApiStatus.SUCCESS,
      message: `已清理 ${deletedCount} 个过期分享`,
      success: true,
    });
  } catch (error) {
    console.error("清理过期文本错误:", error);
    return c.json(createErrorResponse(ApiStatus.INTERNAL_ERROR, error.message || "清理过期文本失败"), ApiStatus.INTERNAL_ERROR);
  }
});

export default app;
