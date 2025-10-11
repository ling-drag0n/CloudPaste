/**
 * 统一挂载点路由
 */
import { Hono } from "hono";
import { authGateway } from "../middlewares/authGatewayMiddleware.js";
import { createMount, updateMount, deleteMount, getAllMounts } from "../services/storageMountService.js";
import { ApiStatus } from "../constants/index.js";
import { createErrorResponse } from "../utils/common.js";
import { HTTPException } from "hono/http-exception";
import { Permission } from "../constants/permissions.js";

const mountRoutes = new Hono();

/**
 * 统一的API错误处理函数
 * 消除原来两个文件中的重复代码
 * @param {Context} c - Hono上下文
 * @param {Error} error - 捕获的错误
 * @param {string} defaultMessage - 默认错误消息
 * @returns {Response} JSON错误响应
 */
const handleApiError = (c, error, defaultMessage) => {
  // 记录错误，但避免冗余日志
  console.error(`API错误: ${error.message || defaultMessage}`);

  // 如果是HTTPException，使用其状态码
  if (error instanceof HTTPException) {
    return c.json(createErrorResponse(error.status, error.message), error.status);
  }

  // 其他错误视为内部服务器错误
  return c.json(createErrorResponse(ApiStatus.INTERNAL_ERROR, error.message || defaultMessage), ApiStatus.INTERNAL_ERROR);
};

/**
 * 获取挂载点列表
 * 统一入口，根据用户权限返回不同数据：
 * - 管理员：返回所有挂载点（包括禁用的）
 * - API密钥用户：返回有权限的活跃挂载点
 */
mountRoutes.get("/api/mount/list", authGateway({
  requireAuth: true,
  customCheck: (authResult) => {
    // 管理员或有挂载查看权限的API密钥用户
    return authResult.isAdmin() || authResult.hasPermission(Permission.MOUNT_VIEW);
  }
}), async (c) => {
  const db = c.env.DB;
  const authResult = c.get("authResult");

  try {
    if (authResult.isAdmin()) {
      // 管理员：获取所有挂载点（包括禁用的，用于管理界面）
      const mounts = await getAllMounts(db, true);
      
      return c.json({
        code: ApiStatus.SUCCESS,
        message: "获取挂载点列表成功",
        data: mounts,
        success: true,
      });
    } else {
      // API密钥用户：根据基本路径获取可访问的挂载点
      const apiKeyInfo = authGateway.utils.getApiKeyInfo(c);
      const mounts = await authGateway.utils.getAccessibleMounts(db, apiKeyInfo, "apiKey");
      
      return c.json({
        code: ApiStatus.SUCCESS,
        message: "获取挂载点列表成功",
        data: mounts,
        success: true,
      });
    }
  } catch (error) {
    return handleApiError(c, error, "获取挂载点列表失败");
  }
});

/**
 * 创建挂载点（仅管理员）
 */
mountRoutes.post("/api/mount/create", authGateway.requireAdmin(), async (c) => {
  const db = c.env.DB;
  const adminId = authGateway.utils.getUserId(c);

  try {
    const body = await c.req.json();
    const mount = await createMount(db, body, adminId);

    // 返回创建成功响应
    return c.json({
      code: ApiStatus.CREATED,
      message: "挂载点创建成功",
      data: mount,
      success: true,
    });
  } catch (error) {
    return handleApiError(c, error, "创建挂载点失败");
  }
});

/**
 * 更新挂载点（仅管理员）
 */
mountRoutes.put("/api/mount/:id", authGateway.requireAdmin(), async (c) => {
  const db = c.env.DB;
  const adminId = authGateway.utils.getUserId(c);
  const { id } = c.req.param();

  try {
    const body = await c.req.json();
    await updateMount(db, id, body, adminId, true);

    return c.json({
      code: ApiStatus.SUCCESS,
      message: "挂载点已更新",
      success: true,
    });
  } catch (error) {
    return handleApiError(c, error, "更新挂载点失败");
  }
});

/**
 * 删除挂载点（仅管理员）
 */
mountRoutes.delete("/api/mount/:id", authGateway.requireAdmin(), async (c) => {
  const db = c.env.DB;
  const adminId = authGateway.utils.getUserId(c);
  const { id } = c.req.param();

  try {
    await deleteMount(db, id, adminId, true);

    return c.json({
      code: ApiStatus.SUCCESS,
      message: "挂载点删除成功",
      success: true,
    });
  } catch (error) {
    return handleApiError(c, error, "删除挂载点失败");
  }
});

export default mountRoutes;
