import app from "./src/index.js";
import { ApiStatus } from "./src/constants/index.js";
import { checkAndInitDatabase } from "./src/utils/database.js";

// 记录数据库是否已初始化的内存标识
let isDbInitialized = false;
// 记录数据库初始化是否正在进行中
let isInitializing = false;
// 存储初始化Promise，用于等待初始化完成
let initializationPromise = null;

// 导出Cloudflare Workers请求处理函数
export default {
  async fetch(request, env, ctx) {
    try {
      // 创建一个新的环境对象，将D1数据库连接和加密密钥添加到环境中
      if (!env.ENCRYPTION_SECRET) {
        throw new Error("ENCRYPTION_SECRET 未配置，请在Cloudflare绑定中设置安全密钥");
      }

      const bindings = {
        ...env,
        DB: env.DB,
        ENCRYPTION_SECRET: env.ENCRYPTION_SECRET,
      };

      // 检查并初始化数据库（确保只初始化一次）
      if (!isDbInitialized) {
        if (!isInitializing) {
          // 开始初始化流程
          isInitializing = true;
          console.log("首次请求，检查数据库状态...");
          
          initializationPromise = checkAndInitDatabase(env.DB)
            .then(() => {
              console.log("数据库初始化成功");
              isDbInitialized = true;
              isInitializing = false;
            })
            .catch((error) => {
              console.error("数据库初始化失败:", error);
              isInitializing = false;
              initializationPromise = null; // 重置Promise以便下次请求重试
              // 重置标记以便下次请求重试
              throw error;
            });
        }
        
        // 等待初始化完成（包括并发请求）
        if (initializationPromise) {
          await initializationPromise;
        }
      }

      // 检查是否是直接文件下载或特殊API请求
      const url = new URL(request.url);
      const pathParts = url.pathname.split("/");

      if (pathParts.length >= 4 && pathParts[1] === "api" && pathParts[2] === "raw") {
        // 将请求转发到API应用，它会路由到userPasteRoutes中的/api/raw/:slug处理器
        return app.fetch(request, bindings, ctx);
      }

      // 处理其他API请求
      return app.fetch(request, bindings, ctx);
    } catch (error) {
      console.error("处理请求时发生错误:", error);

      // 兼容前端期望的错误格式
      return new Response(
          JSON.stringify({
            code: ApiStatus.INTERNAL_ERROR,
            message: "服务器内部错误",
            error: error.message,
            success: false,
            data: null,
          }),
          {
            status: ApiStatus.INTERNAL_ERROR,
            headers: { "Content-Type": "application/json" },
          }
      );
    }
  },
};
