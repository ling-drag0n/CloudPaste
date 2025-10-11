import { defineConfig, loadEnv } from "vite";
import vue from "@vitejs/plugin-vue";
import { VitePWA } from "vite-plugin-pwa";
import { fileURLToPath, URL } from "node:url";

// https://vitejs.dev/config/
export default defineConfig(({ command, mode }) => {
  // 加载环境变量
  const env = loadEnv(mode, process.cwd(), "");

  // 统一版本管理
  const APP_VERSION = "0.8.1";
  const isDev = command === "serve";

  // 打印环境变量，帮助调试
  console.log("Vite环境变量:", {
    VITE_BACKEND_URL: env.VITE_BACKEND_URL || "未设置",
    VITE_APP_ENV: env.VITE_APP_ENV || "未设置",
    APP_VERSION: APP_VERSION,
    MODE: mode,
    COMMAND: command,
  });

  return {
    define: {
      // 注入版本号到应用中
      __APP_VERSION__: JSON.stringify(APP_VERSION),
      // 注入环境变量到应用中
      __APP_ENV__: JSON.stringify(env.VITE_APP_ENV || "production"),
      __BACKEND_URL__: JSON.stringify(env.VITE_BACKEND_URL || ""),
    },
    plugins: [
      vue(),
      VitePWA({
        registerType: "autoUpdate",
        injectRegister: "auto", //自动注入更新检测代码
        devOptions: {
          enabled: true, // 开发环境启用PWA
        },
        workbox: {
          globPatterns: ["**/*.{js,css,html,ico,png,svg,woff,woff2,ttf}"],
          maximumFileSizeToCacheInBytes: 5 * 1024 * 1024,
          skipWaiting: true,
          clientsClaim: true,
          cleanupOutdatedCaches: true,
          navigateFallback: "index.html",
          navigateFallbackAllowlist: [/^\/$/, /^\/upload$/, /^\/admin/, /^\/paste\/.+/, /^\/file\/.+/, /^\/mount-explorer/],

          // 集成自定义Service Worker代码以支持Background Sync API
          importScripts: ["/sw-background-sync.js"],

          // 基于主流PWA最佳实践的正确缓存策略
          runtimeCaching: [
            // 应用静态资源 - StaleWhileRevalidate
            {
              urlPattern: ({ request }) => request.destination === "style" || request.destination === "script" || request.destination === "worker",
              handler: "StaleWhileRevalidate",
              options: {
                cacheName: "app-static-resources",
                expiration: {
                  maxEntries: 1000,
                  maxAgeSeconds: 7 * 24 * 60 * 60, // 7天（依赖Vite版本控制）
                },
                cacheableResponse: {
                  statuses: [0, 200],
                },
              },
            },

            // 字体文件 - CacheFirst（字体很少变化，可长期缓存）
            {
              urlPattern: ({ request }) => request.destination === "font",
              handler: "CacheFirst",
              options: {
                cacheName: "fonts",
                expiration: {
                  maxEntries: 50,
                  maxAgeSeconds: 30 * 24 * 60 * 60, // 30天（字体变化频率低）
                },
                cacheableResponse: {
                  statuses: [0, 200],
                },
              },
            },

            // 第三方CDN资源 - StaleWhileRevalidate
            {
              urlPattern: ({ url }) =>
                  url.origin !== self.location.origin &&
                  (url.hostname.includes("cdn") ||
                      url.hostname.includes("googleapis") ||
                      url.hostname.includes("gstatic") ||
                      url.hostname.includes("jsdelivr") ||
                      url.hostname.includes("unpkg") ||
                      url.hostname.includes("elemecdn") ||
                      url.hostname.includes("bootcdn") ||
                      url.hostname.includes("staticfile")),
              handler: "StaleWhileRevalidate",
              options: {
                cacheName: "external-cdn-resources",
                expiration: {
                  maxEntries: 100,
                  maxAgeSeconds: 30 * 24 * 60 * 60, // 30天
                },
                cacheableResponse: {
                  statuses: [0, 200],
                },
                plugins: [
                  {
                    cacheWillUpdate: async ({ response }) => {
                      return response && response.status === 200;
                    },
                    handlerDidError: async ({ request, error }) => {
                      console.warn(`CDN资源处理失败: ${request.url}`, error);
                      return null; // 优雅降级
                    },
                  },
                ],
              },
            },

            // 图廊图片 - StaleWhileRevalidate（图片适合后台更新）
            {
              urlPattern: ({ request, url }) =>
                  request.destination === "image" && (url.pathname.includes("/api/") || url.searchParams.has("X-Amz-Algorithm") || url.hostname !== self.location.hostname),
              handler: "StaleWhileRevalidate",
              options: {
                cacheName: "gallery-images",
                expiration: {
                  maxEntries: 300,
                  maxAgeSeconds: 7 * 24 * 60 * 60, // 7天
                },
                cacheableResponse: {
                  statuses: [0, 200],
                },
              },
            },

            // 用户媒体文件 - NetworkFirst（大文件适度缓存）
            {
              urlPattern: ({ request, url }) =>
                  (request.destination === "video" || request.destination === "audio" || /\.(mp4|webm|ogg|mp3|wav|flac|aac)$/i.test(url.pathname)) &&
                  (url.pathname.includes("/api/") || url.searchParams.has("X-Amz-Algorithm") || url.hostname !== self.location.hostname),
              handler: "NetworkFirst",
              options: {
                cacheName: "user-media",
                expiration: {
                  maxEntries: 30,
                  maxAgeSeconds: 2 * 60 * 60, // 2小时（媒体文件较大，适度缓存）
                },
                networkTimeoutSeconds: 15,
                cacheableResponse: {
                  statuses: [0, 200, 206], // 支持范围请求
                },
                rangeRequests: true,
              },
            },

            // 用户文档文件 - NetworkFirst（文档快速更新）
            {
              urlPattern: ({ url }) =>
                  /\.(pdf|doc|docx|xls|xlsx|ppt|pptx|txt|md)$/i.test(url.pathname) &&
                  (url.pathname.includes("/api/") || url.searchParams.has("X-Amz-Algorithm") || url.hostname !== self.location.hostname),
              handler: "NetworkFirst",
              options: {
                cacheName: "user-documents",
                expiration: {
                  maxEntries: 50,
                  maxAgeSeconds: 2 * 60 * 60,
                },
                networkTimeoutSeconds: 10,
                cacheableResponse: {
                  statuses: [0, 200],
                },
              },
            },

            // 应用内置图片 - CacheFirst（应用资源稳定）
            {
              urlPattern: ({ request, url }) => request.destination === "image" && url.origin === self.location.origin && !url.pathname.includes("/api/"),
              handler: "CacheFirst",
              options: {
                cacheName: "app-images",
                expiration: {
                  maxEntries: 100,
                  maxAgeSeconds: 30 * 24 * 60 * 60, // 30天（应用图片稳定）
                },
                cacheableResponse: {
                  statuses: [0, 200],
                },
              },
            },

            // 系统API缓存 - NetworkFirst
            {
              urlPattern: /^.*\/api\/(system\/max-upload-size|health|version).*$/,
              handler: "NetworkFirst",
              options: {
                cacheName: "system-api",
                expiration: {
                  maxEntries: 10,
                  maxAgeSeconds: 30 * 60, // 30分钟
                },
                networkTimeoutSeconds: 3,
                cacheableResponse: {
                  statuses: [0, 200],
                },
              },
            },

            // 文件系统动态操作API - NetworkOnly（不缓存，确保实时性）
            {
              urlPattern: /^.*\/api\/fs\/(get|list|upload|batch-remove|batch-copy|mkdir|multipart|download|update|create-share).*$/,
              handler: "NetworkOnly",
              options: {
                cacheName: "fs-dynamic-operations",
              },
            },

            // 文件系统预签名URL - NetworkOnly（每次生成新URL和fileId）
            {
              urlPattern: /^.*\/api\/fs\/(presign|file-link).*$/,
              handler: "NetworkOnly",
              options: {
                cacheName: "fs-presign-operations",
              },
            },

            // 文本分享API - NetworkOnly（涉及访问计数，必须实时）
            {
              urlPattern: /^.*\/api\/(pastes|paste|raw)\/.*$/,
              handler: "NetworkOnly",
              options: {
                cacheName: "pastes-realtime",
              },
            },

            // 搜索API - NetworkOnly（后端已有缓存，前端不应再缓存）
            {
              urlPattern: /^.*\/api\/fs\/search.*$/,
              handler: "NetworkOnly",
              options: {
                cacheName: "search-realtime",
              },
            },

            // 上传相关API - NetworkOnly（操作性API，不应缓存）
            {
              urlPattern: /^.*\/api\/(upload|url)\/.*$/,
              handler: "NetworkOnly",
              options: {
                cacheName: "upload-operations",
              },
            },

            // 公共API缓存 - StaleWhileRevalidate（公共内容后台更新）
            {
              urlPattern: /^.*\/api\/public\/.*$/,
              handler: "StaleWhileRevalidate",
              options: {
                cacheName: "public-api",
                expiration: {
                  maxEntries: 50,
                  maxAgeSeconds: 60 * 60, // 1小时（公共内容相对稳定）
                },
                cacheableResponse: {
                  statuses: [0, 200],
                },
              },
            },

            // WebDAV API - NetworkOnly（实时性要求高）
            {
              urlPattern: /^.*\/dav\/.*$/,
              handler: "NetworkOnly",
              options: {
                cacheName: "webdav-realtime",
              },
            },

            // S3预签名URL - NetworkOnly（避免URL过期问题）
            {
              urlPattern: ({ url }) => url.searchParams.has("X-Amz-Algorithm") || url.searchParams.has("Signature") || url.pathname.includes("/presigned/"),
              handler: "NetworkOnly",
              options: {
                cacheName: "presigned-urls-realtime",
              },
            },

            // 管理员配置读取API - 短期缓存（仅GET请求）
            {
              urlPattern: ({ request, url }) => request.method === "GET" && /^.*\/api\/(mount\/list|admin\/api-keys|admin\/system-settings).*$/.test(url.href),
              handler: "NetworkFirst",
              options: {
                cacheName: "admin-config-read",
                expiration: {
                  maxEntries: 20,
                  maxAgeSeconds: 5 * 60, // 5分钟（配置读取短期缓存）
                },
                networkTimeoutSeconds: 3,
                cacheableResponse: {
                  statuses: [0, 200],
                },
              },
            },

            // 管理员配置写入API - NetworkOnly（POST/PUT/DELETE操作）
            {
              urlPattern: ({ request, url }) =>
                  ["POST", "PUT", "DELETE"].includes(request.method) &&
                  /^.*\/api\/(mount\/(create|[^\/]+)|admin\/api-keys|admin\/system-settings|admin\/login|admin\/change-password|admin\/cache).*$/.test(url.href),
              handler: "NetworkOnly",
              options: {
                cacheName: "admin-config-write",
              },
            },

            // 页面导航缓存 - NetworkFirst（页面短期缓存）
            {
              urlPattern: ({ request }) => request.mode === "navigate",
              handler: "NetworkFirst",
              options: {
                cacheName: "pages",
                expiration: {
                  maxEntries: 20,
                  maxAgeSeconds: 2 * 60 * 60, // 2小时
                },
                networkTimeoutSeconds: 3,
                cacheableResponse: {
                  statuses: [0, 200],
                },
              },
            },

            // 通用API回退缓存 - NetworkFirst（其他API短期缓存）
            {
              urlPattern: /^.*\/api\/.*$/,
              handler: "NetworkFirst",
              options: {
                cacheName: "api-fallback",
                expiration: {
                  maxEntries: 30,
                  maxAgeSeconds: 10 * 60, // 10分钟
                },
                networkTimeoutSeconds: 5,
                cacheableResponse: {
                  statuses: [0, 200],
                },
              },
            },
          ],
        },
        includeAssets: ["favicon.ico", "apple-touch-icon.png", "robots.txt", "dist/**/*"],
        manifest: {
          name: "CloudPaste",
          short_name: "CloudPaste",
          description: "安全分享您的内容，支持 Markdown 编辑和文件上传",
          theme_color: "#0ea5e9",
          background_color: "#ffffff",
          display: "standalone",
          orientation: "portrait-primary", // 与manifest.json保持一致
          scope: "/",
          start_url: "/",
          lang: "zh-CN", // 添加语言设置
          categories: ["productivity", "utilities"], // 添加应用分类
          icons: [
            {
              src: "icons/icons-32.png",
              sizes: "32x32",
              type: "image/png",
            },
            {
              src: "icons/icon-96.png",
              sizes: "96x96",
              type: "image/png",
            },
            {
              src: "icons/icon-192.png",
              sizes: "192x192",
              type: "image/png",
            },
            {
              src: "icons/icon-512.png",
              sizes: "512x512",
              type: "image/png",
            },
            {
              src: "icons/icon-512-maskable.png",
              sizes: "512x512",
              type: "image/png",
              purpose: "maskable",
            },
          ],
          shortcuts: [
            {
              name: "文件上传",
              short_name: "上传",
              description: "快速上传文件",
              url: "/upload",
              icons: [
                {
                  src: "icons/shortcut-upload-96.png",
                  sizes: "96x96",
                },
              ],
            },
          ],
        },
      }),
    ],
    resolve: {
      alias: {
        "@": fileURLToPath(new URL("./src", import.meta.url)),
      },
    },
    server: {
      port: 3000,
      open: true,
      // 设置代理 - 仅在本地开发模式下使用
      proxy: {
        // 当 VITE_BACKEND_URL 为本地地址时，将请求代理到本地worker
        "/api": {
          target: env.VITE_BACKEND_URL || "http://localhost:8787",
          changeOrigin: true,
          secure: false,
          // 打印代理日志
          configure: (proxy, _options) => {
            proxy.on("error", (err, _req, _res) => {
              console.log("代理错误", err);
            });
            proxy.on("proxyReq", (_proxyReq, req, _res) => {
              console.log("代理请求:", req.method, req.url);
            });
            proxy.on("proxyRes", (proxyRes, req, _res) => {
              console.log("代理响应:", req.method, req.url, proxyRes.statusCode);
            });
          },
        },
      },
    },
    optimizeDeps: {
      include: ["vue-i18n", "chart.js", "qrcode", "mime-db"],
      // 移除vditor排除配置，因为现在从本地dist目录加载
    },
    build: {
      minify: "terser",
      terserOptions: {
        compress: {
          drop_console: true,
        },
      },
      chunkSizeWarningLimit: 1000,
      rollupOptions: {
        output: {
          manualChunks: {
            // 将大型库分离到单独的 chunk
            "vendor-vue": ["vue", "vue-router", "vue-i18n"],
            "vendor-charts": ["chart.js", "vue-chartjs"],
            "vendor-utils": ["axios", "qrcode", "file-saver", "docx", "html-to-image"],
          },
        },
      },
    },
  };
});
