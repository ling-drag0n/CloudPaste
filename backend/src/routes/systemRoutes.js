import { Hono } from "hono";
import { authGateway } from "../middlewares/authGatewayMiddleware.js";
import {
  getMaxUploadSize,
  getDashboardStats,
  getSettingsByGroup,
  getAllSettingsByGroups,
  getGroupsInfo,
  updateGroupSettings,
  getSettingMetadata,
} from "../services/systemService.js";
import { ApiStatus } from "../constants/index.js";
import { createErrorResponse } from "../utils/common.js";

const systemRoutes = new Hono();

// 获取最大上传文件大小限制（公共API）
systemRoutes.get("/api/system/max-upload-size", async (c) => {
  const db = c.env.DB;

  try {
    // 获取最大上传大小设置
    const size = await getMaxUploadSize(db);

    return c.json({
      code: ApiStatus.SUCCESS,
      message: "获取最大上传大小成功",
      data: { max_upload_size: size },
      success: true,
    });
  } catch (error) {
    console.error("获取最大上传大小错误:", error);
    // 获取默认值
    const defaultSize = await getMaxUploadSize(db);
    return c.json({
      code: ApiStatus.SUCCESS,
      message: "获取最大上传大小成功（使用默认值）",
      data: { max_upload_size: defaultSize },
      success: true,
    });
  }
});

// 仪表盘统计数据API
systemRoutes.get("/api/admin/dashboard/stats", authGateway.requireAdmin(), async (c) => {
  try {
    const db = c.env.DB;
    const adminId = authGateway.utils.getUserId(c);

    const stats = await getDashboardStats(db, adminId);

    return c.json({
      code: ApiStatus.SUCCESS,
      message: "获取仪表盘统计数据成功",
      data: stats,
      success: true,
    });
  } catch (error) {
    console.error("获取仪表盘统计数据失败:", error);
    return c.json(createErrorResponse(ApiStatus.INTERNAL_ERROR, "获取仪表盘统计数据失败: " + error.message), ApiStatus.INTERNAL_ERROR);
  }
});

// 获取系统版本信息（公共API）
systemRoutes.get("/api/version", async (c) => {
  // 判断运行环境和数据存储
  const runtimeEnv = process.env.RUNTIME_ENV || "unknown";
  const isDocker = runtimeEnv === "docker";

  // 统一的默认版本配置
  const DEFAULT_VERSION = "0.8.1";
  const DEFAULT_NAME = "cloudpaste-api";

  let version = DEFAULT_VERSION;
  let name = DEFAULT_NAME;

  // 根据环境获取版本信息
  if (isDocker) {
    // Docker环境：尝试读取package.json
    try {
      const fs = await import("fs");
      const path = await import("path");
      const packagePath = path.resolve("./package.json");
      const packageContent = fs.readFileSync(packagePath, "utf8");
      const packageJson = JSON.parse(packageContent);

      version = packageJson.version || DEFAULT_VERSION;
      name = packageJson.name || DEFAULT_NAME;
    } catch (error) {
      console.warn("Docker环境读取package.json失败，使用默认值:", error.message);
      // 保持默认值
    }
  } else {
    // Workers环境：使用环境变量或默认值
    version = process.env.APP_VERSION || DEFAULT_VERSION;
    name = process.env.APP_NAME || DEFAULT_NAME;
  }

  const versionInfo = {
    version,
    name,
    environment: isDocker ? "Docker" : "Cloudflare Workers",
    storage: isDocker ? "SQLite" : "Cloudflare D1",
    nodeVersion: process.version || "unknown",
    uptime: Math.round(process.uptime()),
  };

  return c.json({
    code: ApiStatus.SUCCESS,
    message: "获取版本信息成功",
    data: versionInfo,
    success: true,
  });
});

// ==================== 新增：分组设置管理API接口 ====================

// 按分组获取设置项（公开访问，无需认证）
systemRoutes.get("/api/admin/settings", async (c) => {
  const db = c.env.DB;

  try {
    const groupId = c.req.query("group");
    const includeMetadata = c.req.query("metadata") !== "false";

    if (groupId) {
      // 按分组查询
      const groupIdNum = parseInt(groupId);
      if (isNaN(groupIdNum)) {
        return c.json(createErrorResponse(ApiStatus.BAD_REQUEST, "分组ID必须是数字"), ApiStatus.BAD_REQUEST);
      }

      const settings = await getSettingsByGroup(db, groupIdNum, includeMetadata);
      return c.json({
        code: ApiStatus.SUCCESS,
        message: "获取分组设置成功",
        data: settings,
        success: true,
      });
    } else {
      // 获取所有分组的设置
      const includeSystemGroup = c.req.query("includeSystem") === "true";
      const groupedSettings = await getAllSettingsByGroups(db, includeSystemGroup);

      return c.json({
        code: ApiStatus.SUCCESS,
        message: "获取所有分组设置成功",
        data: groupedSettings,
        success: true,
      });
    }
  } catch (error) {
    console.error("获取设置错误:", error);
    return c.json(createErrorResponse(ApiStatus.INTERNAL_ERROR, "获取设置失败: " + error.message), ApiStatus.INTERNAL_ERROR);
  }
});

// 获取分组列表和统计信息
systemRoutes.get("/api/admin/settings/groups", authGateway.requireAdmin(), async (c) => {
  const db = c.env.DB;

  try {
    const groupsInfo = await getGroupsInfo(db);

    return c.json({
      code: ApiStatus.SUCCESS,
      message: "获取分组信息成功",
      data: { groups: groupsInfo },
      success: true,
    });
  } catch (error) {
    console.error("获取分组信息错误:", error);
    return c.json(createErrorResponse(ApiStatus.INTERNAL_ERROR, "获取分组信息失败: " + error.message), ApiStatus.INTERNAL_ERROR);
  }
});

// 获取设置项元数据
systemRoutes.get("/api/admin/settings/metadata", authGateway.requireAdmin(), async (c) => {
  const db = c.env.DB;

  try {
    const key = c.req.query("key");
    if (!key) {
      return c.json(createErrorResponse(ApiStatus.BAD_REQUEST, "缺少设置键名参数"), ApiStatus.BAD_REQUEST);
    }

    const metadata = await getSettingMetadata(db, key);
    if (!metadata) {
      return c.json(createErrorResponse(ApiStatus.NOT_FOUND, "设置项不存在"), ApiStatus.NOT_FOUND);
    }

    return c.json({
      code: ApiStatus.SUCCESS,
      message: "获取设置元数据成功",
      data: metadata,
      success: true,
    });
  } catch (error) {
    console.error("获取设置元数据错误:", error);
    return c.json(createErrorResponse(ApiStatus.INTERNAL_ERROR, "获取设置元数据失败: " + error.message), ApiStatus.INTERNAL_ERROR);
  }
});

// 按分组批量更新设置
systemRoutes.put("/api/admin/settings/group/:groupId", authGateway.requireAdmin(), async (c) => {
  const db = c.env.DB;

  try {
    const groupId = parseInt(c.req.param("groupId"));
    if (isNaN(groupId)) {
      return c.json(createErrorResponse(ApiStatus.BAD_REQUEST, "分组ID必须是数字"), ApiStatus.BAD_REQUEST);
    }

    const body = await c.req.json();
    if (!body || typeof body !== "object") {
      return c.json(createErrorResponse(ApiStatus.BAD_REQUEST, "请求参数无效"), ApiStatus.BAD_REQUEST);
    }

    const validateType = c.req.query("validate") !== "false";
    const result = await updateGroupSettings(db, groupId, body, { validateType });

    return c.json({
      code: result.success ? ApiStatus.SUCCESS : ApiStatus.ACCEPTED,
      message: result.message,
      data: result,
      success: result.success,
    });
  } catch (error) {
    console.error("批量更新分组设置错误:", error);
    return c.json(createErrorResponse(ApiStatus.INTERNAL_ERROR, "批量更新分组设置失败: " + error.message), ApiStatus.INTERNAL_ERROR);
  }
});

export default systemRoutes;
