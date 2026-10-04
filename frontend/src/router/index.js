/**
 * Vue Router 配置
 */

import { createRouter, createWebHistory } from "vue-router";
import OfflineFallback from "../components/OfflineFallback.vue";
import { showPageUnavailableToast } from "../pwa/offlineToast.js";
import { useAuthStore } from "@/stores/authStore.js";
import { useSiteConfigStore } from "@/stores/siteConfigStore.js";
import NProgress from "nprogress";
import "nprogress/nprogress.css";
import { createLogger } from "@/utils/logger.js";

const log = createLogger("Router");

const isProbablyOffline = () => {
  return typeof navigator !== "undefined" && navigator.onLine === false;
};

// 懒加载组件 - 添加离线错误处理
const createOfflineAwareImport = (importFn, componentName = "页面") => {
  return () =>
    importFn().catch((error) => {
      log.error("组件加载失败:", error);

      NProgress.done();

      // 如果是离线状态且加载失败，显示离线回退页面和Toast提示
      if (isProbablyOffline()) {
        log.debug("[离线模式] 组件未缓存，显示离线回退页面");

        // 显示Toast提示
        setTimeout(() => {
          showPageUnavailableToast(componentName);
        }, 100);

        return OfflineFallback;
      }

      // 在线状态下的加载失败，重新抛出错误
      throw error;
    });
};

const HomeView = createOfflineAwareImport(() => import("../modules/paste/editor/MarkdownEditorView.vue"), "首页");
const PasteView = createOfflineAwareImport(() => import("../modules/paste/public/PasteView.vue"), "文本分享页面");
const FileView = createOfflineAwareImport(() => import("../modules/fileshare/public/FileView.vue"), "文件预览页面");
const FolderShareView = createOfflineAwareImport(() => import("../modules/fileshare/public/FolderShareView.vue"), "文件夹分享页面");
const MountExplorerView = createOfflineAwareImport(() => import("../modules/fs/MountExplorerView.vue"), "挂载浏览器");

// 路由配置 - 完全对应原有的页面逻辑
const routes = [
  {
    path: "/",
    name: "Home",
    component: HomeView,
    meta: {
      title: "CloudPaste - 在线剪贴板",
      originalPage: "home",
    },
  },
  {
    // 兼容旧链接：独立“文件上传”页面已并入“挂载浏览”
    path: "/upload",
    redirect: { name: "MountExplorer" },
  },
  // 管理员登录页面
  {
    path: "/admin/login",
    name: "AdminLogin",
    component: createOfflineAwareImport(() => import("../modules/admin/views/AdminLoginView.vue"), "管理员登录"),
    meta: {
      title: "登录 - CloudPaste",
      originalPage: "admin-login",
    },
  },
  // 管理面板
  {
    path: "/admin",
    component: createOfflineAwareImport(() => import("../modules/admin/views/AdminLayout.vue"), "管理面板布局"),
    meta: {
      title: "管理面板 - CloudPaste",
      originalPage: "admin",
      requiresAuth: true,
    },
    children: [
      {
        path: "dashboard",
        name: "AdminDashboard",
        component: createOfflineAwareImport(() => import("../modules/admin/views/DashboardView.vue"), "仪表板"),
        meta: {
          title: "仪表板 - CloudPaste",
          adminOnly: true, // 只有管理员可访问
        },
      },
      {
        path: "text-management",
        name: "AdminTextManagement",
        component: createOfflineAwareImport(() => import("../modules/paste/admin/TextManagementView.vue"), "文本管理"),
        meta: {
          title: "文本管理 - CloudPaste",
          requiredPermissions: ["text"], // 需要文本权限
        },
      },
      {
        path: "file-management",
        name: "AdminFileManagement",
        component: createOfflineAwareImport(() => import("../modules/fileshare/admin/FileManagementView.vue"), "文件管理"),
        meta: {
          title: "文件管理 - CloudPaste",
          requiredPermissions: ["file"], // 需要文件权限
        },
      },
      {
        path: "share-management",
        name: "AdminShareManagement",
        component: createOfflineAwareImport(() => import("../modules/fileshare/admin/ShareManagementView.vue"), "分享链接管理"),
        meta: {
          title: "分享链接管理 - CloudPaste",
          requiredPermissions: ["file"], // 需要文件权限
        },
      },
      {
        path: "key-management",
        name: "AdminKeyManagement",
        component: createOfflineAwareImport(() => import("../modules/admin/views/KeyManagementView.vue"), "密钥管理"),
        meta: {
          title: "密钥管理 - CloudPaste",
          adminOnly: true, // 只有管理员可访问
        },
      },
      {
        path: "mount-management",
        name: "AdminMountManagement",
        component: createOfflineAwareImport(() => import("../modules/admin/views/MountManagementView.vue"), "挂载管理"),
        meta: {
          title: "挂载管理 - CloudPaste",
          requiredPermissions: ["mount"], // 需要挂载权限
        },
      },
      {
        path: "fs-meta-management",
        name: "AdminFsMetaManagement",
        component: createOfflineAwareImport(() => import("../modules/admin/views/FsMetaManagementView.vue"), "元信息管理"),
        meta: {
          title: "元信息管理 - CloudPaste",
          adminOnly: true, // 只有管理员可访问
        },
      },
      {
        path: "storage",
        name: "AdminStorage",
        component: createOfflineAwareImport(() => import("../modules/admin/views/StorageConfigView.vue"), "存储管理"),
        meta: {
          title: "存储管理 - CloudPaste",
          adminOnly: true, // 只有管理员可访问
        },
      },
      {
        path: "storage-config",
        redirect: { name: "AdminStorage" },
        meta: {
          adminOnly: true,
        },
      },
      {
        path: "scheduled-jobs",
        name: "AdminScheduledJobs",
        component: createOfflineAwareImport(() => import("../modules/admin/views/ScheduledJobsView.vue"), "定时任务管理"),
        meta: {
          title: "定时任务管理 - CloudPaste",
          adminOnly: true, // 只有管理员可访问
        },
      },
      {
        path: "scheduled-jobs/create",
        name: "AdminScheduledJobCreate",
        component: createOfflineAwareImport(() => import("../modules/admin/views/ScheduledJobFormView.vue"), "创建定时任务"),
        meta: {
          title: "创建定时任务 - CloudPaste",
          adminOnly: true,
        },
      },
      {
        path: "scheduled-jobs/:id/edit",
        name: "AdminScheduledJobEdit",
        component: createOfflineAwareImport(() => import("../modules/admin/views/ScheduledJobFormView.vue"), "编辑定时任务"),
        meta: {
          title: "编辑定时任务 - CloudPaste",
          adminOnly: true,
        },
      },
      {
        path: "account",
        name: "AdminAccountManagement",
        component: createOfflineAwareImport(() => import("../modules/admin/views/AccountManagementView.vue"), "账号管理"),
        meta: {
          title: "账号管理 - CloudPaste",
          requiresAuth: true, // 管理员和API密钥用户都可访问
        },
      },
      {
        path: "backup",
        name: "AdminBackup",
        component: createOfflineAwareImport(() => import("../modules/admin/views/BackupView.vue"), "数据备份"),
        meta: {
          title: "数据备份 - CloudPaste",
          adminOnly: true, // 只有管理员可访问
        },
      },
      {
        path: "tasks",
        name: "AdminTasks",
        component: createOfflineAwareImport(() => import("../modules/admin/views/AdminTasksView.vue"), "任务管理"),
        meta: {
          title: "任务管理 - CloudPaste",
          requiredPermissions: ["mount"],
        },
      },
      {
        path: "fs-index",
        name: "AdminFsIndexManagement",
        component: createOfflineAwareImport(() => import("../modules/admin/views/FsIndexManagement.vue"), "文件系统索引管理"),
        meta: {
          title: "索引管理 - CloudPaste",
          adminOnly: true, // 只有管理员可访问
        },
      },
      {
        path: "settings",
        children: [
          {
            path: "global",
            name: "AdminGlobalSettings",
            component: createOfflineAwareImport(() => import("../modules/admin/views/settings/GlobalSettingsView.vue"), "全局设置"),
            meta: {
              title: "全局设置 - CloudPaste",
              adminOnly: true, // 只有管理员可访问
            },
          },

          {
            path: "webdav",
            name: "AdminWebDAVSettings",
            component: createOfflineAwareImport(() => import("../modules/admin/views/settings/WebDAVSettingsView.vue"), "WebDAV设置"),
            meta: {
              title: "WebDAV设置 - CloudPaste",
              adminOnly: true, // 只有管理员可访问
            },
          },
          {
            path: "preview",
            name: "AdminPreviewSettings",
            component: createOfflineAwareImport(() => import("../modules/admin/views/settings/PreviewSettingsView.vue"), "预览设置"),
            meta: {
              title: "预览设置 - CloudPaste",
              adminOnly: true, // 只有管理员可访问
            },
          },
          {
            path: "site",
            name: "AdminSiteSettings",
            component: createOfflineAwareImport(() => import("../modules/admin/views/settings/SiteSettingsView.vue"), "站点设置"),
            meta: {
              title: "站点设置 - CloudPaste",
              adminOnly: true, // 只有管理员可访问
            },
          },
        ],
      },
    ],
  },
  {
    path: "/paste/:slug",
    name: "PasteView",
    component: PasteView,
    props: true,
    meta: {
      title: "查看分享 - CloudPaste",
      originalPage: "paste-view",
    },
  },
  {
    path: "/file/:slug",
    name: "FileView",
    component: FileView,
    props: true,
    meta: {
      title: "文件预览 - CloudPaste",
      originalPage: "file-view",
    },
  },
  {
    path: "/folder/:slug",
    name: "FolderShareView",
    component: FolderShareView,
    props: true,
    meta: {
      title: "文件夹分享 - CloudPaste",
      originalPage: "folder-share",
    },
  },
  {
    path: "/mount-explorer",
    name: "MountExplorer",
    component: MountExplorerView,
    props: (route) => ({
      darkMode: route.meta.darkMode || false,
    }),
    meta: {
      title: "挂载浏览 - CloudPaste",
      originalPage: "mount-explorer",
    },
  },
  {
    path: "/mount-explorer/:pathMatch(.*)+",
    name: "MountExplorerPath",
    component: MountExplorerView,
    props: (route) => ({
      darkMode: route.meta.darkMode || false,
    }),
    meta: {
      title: "挂载浏览 - CloudPaste",
      originalPage: "mount-explorer",
    },
  },
  {
    path: "/:pathMatch(.*)*",
    name: "NotFound",
    redirect: "/",
    meta: {
      title: "页面未找到 - CloudPaste",
    },
  },
];

// 创建路由实例
const router = createRouter({
  history: createWebHistory(),
  routes,
  scrollBehavior(to, from, savedPosition) {
    // MountExplorer 的滚动由页面自身管理（用于“预览 ↔ 列表”保持原位/恢复）
    const isMountExplorer = (r) => r?.name === "MountExplorer" || r?.name === "MountExplorerPath";
    if (isMountExplorer(to) && isMountExplorer(from)) {
      // 返回 false：表示不做任何自动滚动处理，保持当前滚动
      return false;
    }

    // 浏览器前进/后退：优先使用浏览器提供的滚动位置
    if (savedPosition) return savedPosition;

    // 其它页面：保持默认“进新页回到顶部”的行为
    return { top: 0 };
  },
});

const normalizeFsPathForMountExplorer = (path) => {
  const raw = typeof path === "string" && path ? path : "/";
  const withLeading = raw.startsWith("/") ? raw : `/${raw}`;
  const collapsed = withLeading.replace(/\/{2,}/g, "/");
  if (collapsed === "/") return "/";
  return collapsed.replace(/\/+$/, "");
};

const buildMountExplorerRoutePath = (fsPath) => {
  const normalized = normalizeFsPathForMountExplorer(fsPath);
  if (normalized === "/") return "/mount-explorer";
  const segments = normalized.replace(/^\/+/, "").split("/").filter(Boolean);
  return `/mount-explorer/${segments.map((seg) => encodeURIComponent(seg)).join("/")}`;
};

// 配置NProgress - 遵循官方默认值
NProgress.configure({
  showSpinner: false, // 隐藏旋转器
});

// 权限检查工具函数
const hasRoutePermission = (route, authStore) => {
  // 检查是否只有管理员可访问
  if (route.meta?.adminOnly) {
    return authStore.isAdmin;
  }

  // 检查是否需要特定权限
  if (route.meta?.requiredPermissions) {
    return route.meta.requiredPermissions.some((permission) => {
      switch (permission) {
        case "text":
          return authStore.hasTextManagePermission;
        case "file":
          return authStore.hasFileManagePermission;
        case "mount":
          return authStore.hasMountPermission;
        default:
          return false;
      }
    });
  }

  // 默认允许访问
  return true;
};

// 前台入口开关
const isPublicEntryDisabled = (pageKey, siteConfigStore) => {
  if (!siteConfigStore?.isInitialized) return false;
  if (pageKey === "home") return siteConfigStore.siteHomeEditorEnabled === false;
  if (pageKey === "mount-explorer") return siteConfigStore.siteMountExplorerEnabled === false;
  return false;
};

// 禁用页面时的默认落点：优先跳到仍然开启的公开页面；全关则跳管理入口
const getFallbackRoute = (authStore, siteConfigStore) => {
  if (!siteConfigStore?.isInitialized) {
    return { name: "Home" };
  }

  if (siteConfigStore.siteHomeEditorEnabled) return { name: "Home" };
  if (siteConfigStore.siteMountExplorerEnabled) return { name: "MountExplorer" };

  // 公开入口都关了：让用户进管理入口（未登录会自动去登录页）
  if (authStore?.isAuthenticated) return { path: "/admin" };
  return { name: "AdminLogin" };
};

const dispatchPublicEntryDisabledMessage = () => {
  try {
    window.dispatchEvent(
      new CustomEvent("global-message", {
        detail: {
          type: "warning",
          content: "该页面已被管理员关闭，已为你跳转到其它页面。（This page is disabled and you have been redirected.）",
          duration: 2500,
        },
      })
    );
  } catch {}
};

// 获取用户默认路由
const getDefaultRouteForUser = (authStore) => {
  if (authStore.isAdmin) {
    return "AdminDashboard";
  }

  // API密钥用户：根据权限确定默认页面
  if (authStore.authType === "apikey") {
    // 按优先级检查权限
    if (authStore.hasTextManagePermission) {
      return "AdminTextManagement";
    }

    if (authStore.hasFileManagePermission) {
      return "AdminFileManagement";
    }

    if (authStore.hasMountPermission) {
      return "AdminMountManagement";
    }

    // 没有任何功能权限，默认到账户管理页面
    return "AdminAccountManagement";
  }

  // 其他情况
  return null;
};

// 路由守卫 - 使用认证Store进行主动权限验证
router.beforeEach(async (to, from, next) => {
  // 启动进度条
  NProgress.start();

  try {
    const authStore = useAuthStore();
    const siteConfigStore = useSiteConfigStore();

    // 确保初始化完成，避免刷新时的认证闪烁
    if (!authStore.initialized) {
      await authStore.initialize();
    }

    // 确保站点配置已初始化：用于“前台入口开关（隐藏入口 + 禁止访问路由）”
    if (!siteConfigStore.isInitialized && typeof siteConfigStore.initialize === "function") {
      await siteConfigStore.initialize();
    }

    // 自动游客会话：在首次未认证状态下尝试一次基于 Guest API Key 的登录
    // 统一在任意路由入口触发一次（包括 /admin 相关路由），由 maybeAutoGuestLogin 自身通过 guestAutoTried 控制只尝试一次
    if (!authStore.isAuthenticated && authStore.authType === "none") {
      if (typeof authStore.maybeAutoGuestLogin === "function") {
        await authStore.maybeAutoGuestLogin();
      }
    }

    // 如果需要认证且认证状态需要重新验证，则进行验证
    if (to.meta.requiresAuth && authStore.needsRevalidation) {
      log.debug("路由守卫：需要重新验证认证状态");
      await authStore.validateAuth();
    }

    // ===== 公开页面入口开关 =====
    const pageKey = to.meta?.originalPage;
    if (typeof pageKey === "string" && isPublicEntryDisabled(pageKey, siteConfigStore)) {
      log.debug("路由守卫：公开页面入口已关闭，执行跳转", { pageKey, to: to.fullPath });
      dispatchPublicEntryDisabledMessage();
      NProgress.done();
      next(getFallbackRoute(authStore, siteConfigStore));
      return;
    }

    // 登录页面访问控制
    if (to.name === "AdminLogin") {
      if (authStore.isAuthenticated) {
        log.debug("路由守卫：已认证用户访问登录页面，重定向到合适的管理页面");
        const defaultRoute = getDefaultRouteForUser(authStore);
        NProgress.done();
        if (defaultRoute) {
          next({ name: defaultRoute });
        } else {
          next({ name: "Home" });
        }
        return;
      }
      // 未认证用户可以访问登录页面
      next();
      return;
    }

    // 管理页面权限检查 - 检查是否是admin路由或其子路由
    if (to.meta.requiresAuth && (to.path.startsWith("/admin") || to.matched.some((record) => record.path.startsWith("/admin")))) {
      if (!authStore.isAuthenticated) {
        log.debug("路由守卫：用户未认证，重定向到登录页面");
        NProgress.done();
        next({ name: "AdminLogin", query: { redirect: to.fullPath } });
        return;
      }

      // 检查是否有管理权限（管理员或任何已认证的API密钥用户）
      // API密钥用户需要能够访问管理面板进行登出操作，具体权限在页面级别控制
      const hasManagementAccess = authStore.isAdmin || authStore.authType === "apikey";

      if (!hasManagementAccess) {
        log.debug("路由守卫：用户无管理权限，重定向到首页");
        NProgress.done();
        next({ name: "Home" });
        return;
      }

      // 处理 /admin 根路径访问，进行智能重定向
      if (to.path === "/admin") {
        log.debug("路由守卫：访问 /admin 根路径，进行重定向");
        log.debug("路由守卫：用户权限详情", {
          authType: authStore.authType,
          isAdmin: authStore.isAdmin,
          hasTextSharePermission: authStore.hasTextSharePermission,
          hasFileSharePermission: authStore.hasFileSharePermission,
          hasMountPermission: authStore.hasMountPermission,
          apiKeyPermissions: authStore.apiKeyPermissions,
        });
        const defaultRoute = getDefaultRouteForUser(authStore);
        NProgress.done();
        if (defaultRoute) {
          log.debug("路由守卫：重定向到默认页面", defaultRoute);
          next({ name: defaultRoute });
        } else {
          log.debug("路由守卫：用户无任何管理权限，重定向到首页");
          next({ name: "Home" });
        }
        return;
      }

      // 页面级权限检查和智能重定向
      if (to.name && to.name.startsWith("Admin")) {
        // 检查用户是否有访问目标页面的权限
        if (!hasRoutePermission(to, authStore)) {
          log.debug("路由守卫：用户无权限访问目标页面，进行智能重定向", {
            targetRoute: to.name,
            userPermissions: {
              isAdmin: authStore.isAdmin,
              hasTextManagePermission: authStore.hasTextManagePermission,
              hasFileManagePermission: authStore.hasFileManagePermission,
              hasMountPermission: authStore.hasMountPermission,
            },
          });

          // 获取用户默认路由
          const defaultRoute = getDefaultRouteForUser(authStore);
          NProgress.done();
          if (defaultRoute) {
            log.debug("路由守卫：重定向到默认页面", defaultRoute);
            next({ name: defaultRoute });
          } else {
            log.debug("路由守卫：用户无任何管理权限，重定向到首页");
            next({ name: "Home" });
          }
          return;
        }

        // 特殊处理：如果访问 /admin 根路径，重定向到合适的页面
        if (to.name === "AdminDashboard" && !authStore.isAdmin) {
          log.debug("路由守卫：API密钥用户访问仪表板，重定向到默认页面");
          const defaultRoute = getDefaultRouteForUser(authStore);
          if (defaultRoute && defaultRoute !== "AdminDashboard") {
            NProgress.done();
            next({ name: defaultRoute });
            return;
          }
        }
      }

      log.debug("路由守卫：管理权限验证通过", {
        isAdmin: authStore.isAdmin,
        authType: authStore.authType,
        isGuest: authStore.isGuest,
        hasTextManagePermission: authStore.hasTextManagePermission,
        hasFileManagePermission: authStore.hasFileManagePermission,
        hasMountPermission: authStore.hasMountPermission,
      });
    }

    // 挂载浏览器页面权限检查
    if (to.name === "MountExplorer" || to.name === "MountExplorerPath") {
      // 移除自动重定向逻辑，让组件自己处理权限显示
      // 这样用户可以看到友好的"无权限"提示而不是突然被重定向

      log.debug("路由守卫：挂载页面访问", {
        isAuthenticated: authStore.isAuthenticated,
        hasMountPermission: authStore.hasMountPermission,
        authType: authStore.authType,
      });

      // 只对有挂载权限的API密钥用户进行路径权限检查
      const isKeyLikeUser = authStore.isKeyUser;
      if (isKeyLikeUser && authStore.hasMountPermission && to.params.pathMatch) {
        const requestedPath = "/" + (Array.isArray(to.params.pathMatch) ? to.params.pathMatch.join("/") : to.params.pathMatch);
        if (!authStore.hasPathPermission(requestedPath)) {
          log.debug("路由守卫：用户无此路径权限，重定向到基本路径");
          const basePath = authStore.userInfo.basicPath || "/";
          const redirectPath = buildMountExplorerRoutePath(basePath);
          NProgress.done();
          next({ path: redirectPath });
          return;
        }
      }
    }

    next();
  } catch (error) {
    log.error("路由守卫错误:", error);
    NProgress.done();
    // 发生错误时允许继续，避免阻塞路由
    next();
  }
});

// 路由错误处理
router.onError((error) => {
  log.error("路由错误:", error);

  NProgress.done();

  // 如果是离线状态下的组件加载失败，不需要额外处理
  // 因为 createOfflineAwareImport 已经处理了
  if (pwaState.isOffline) {
    log.debug("[离线模式] 路由错误已由离线回退机制处理");
    return;
  }

  // 在线状态下的其他错误，可以添加额外的错误处理逻辑
  log.error("在线状态下的路由错误:", error);
});

// 路由后置守卫 - 处理页面标题和调试信息
router.afterEach(async (to, from) => {
  // 完成进度条
  NProgress.done();
  // 动态设置页面标题，支持国际化和站点配置
  let title = "CloudPaste";
  let siteTitle = "CloudPaste";

  try {
    // 动态导入站点配置Store
    const { useSiteConfigStore } = await import("../stores/siteConfigStore.js");
    const siteConfigStore = useSiteConfigStore();

    // 获取站点标题（如果store已初始化）
    if (siteConfigStore.isInitialized) {
      siteTitle = siteConfigStore.siteTitle || "CloudPaste";
    }

    // 动态导入 i18n 实例
    const { default: i18n } = await import("../i18n/index.js");
    const { t } = i18n.global;

    // 根据路由名称设置对应的国际化标题
    switch (to.name) {
      case "Home":
        title = `${siteTitle} - ${t("pageTitle.homeSubtitle")}`;
        break;
      case "AdminDashboard":
        title = `${t("pageTitle.adminModules.dashboard")} - ${siteTitle}`;
        break;
      case "AdminTextManagement":
        title = `${t("pageTitle.adminModules.textManagement")} - ${siteTitle}`;
        break;
      case "AdminFileManagement":
        title = `${t("pageTitle.adminModules.fileManagement")} - ${siteTitle}`;
        break;
      case "AdminShareManagement":
        title = `${t("pageTitle.adminModules.shareManagement")} - ${siteTitle}`;
        break;
      case "AdminStorage":
        title = `${t("pageTitle.adminModules.storageConfig")} - ${siteTitle}`;
        break;
      case "AdminMountManagement":
        title = `${t("pageTitle.adminModules.mountManagement")} - ${siteTitle}`;
        break;
      case "AdminFsMetaManagement":
        title = `${t("pageTitle.adminModules.fsMetaManagement")} - ${siteTitle}`;
        break;
      case "AdminKeyManagement":
        title = `${t("pageTitle.adminModules.keyManagement")} - ${siteTitle}`;
        break;
      case "AdminGlobalSettings":
        title = `${t("pageTitle.adminModules.globalSettings")} - ${siteTitle}`;
        break;
      case "AdminPreviewSettings":
        title = `${t("pageTitle.adminModules.previewSettings")} - ${siteTitle}`;
        break;
      case "AdminAccountSettings":
        title = `${t("pageTitle.adminModules.accountSettings")} - ${siteTitle}`;
        break;
      case "AdminAccountManagement":
        title = `${t("pageTitle.adminModules.accountSettings")} - ${siteTitle}`;
        break;
      case "AdminWebDAVSettings":
        title = `${t("pageTitle.adminModules.webdavSettings")} - ${siteTitle}`;
        break;
      case "AdminSiteSettings":
        title = `${t("pageTitle.adminModules.siteSettings")} - ${siteTitle}`;
        break;
      case "AdminBackup":
        title = `${t("pageTitle.adminModules.backup")} - ${siteTitle}`;
        break;
      case "AdminTasks":
        title = `${t("pageTitle.adminModules.tasks")} - ${siteTitle}`;
        break;
      case "PasteView":
        title = `${t("pageTitle.pasteViewSubtitle")} - ${siteTitle}`;
        break;
      case "FileView":
        title = `${t("pageTitle.fileViewSubtitle")} - ${siteTitle}`;
        break;
      case "FolderShareView":
        title = `${t("fileView.folder.title")} - ${siteTitle}`;
        break;
      case "MountExplorer":
      case "MountExplorerPath":
        title = `${t("pageTitle.mountExplorerSubtitle")} - ${siteTitle}`;
        break;
      case "NotFound":
        title = `${t("pageTitle.notFoundSubtitle")} - ${siteTitle}`;
        break;
      default:
        title = to.meta?.title || siteTitle;
    }
  } catch (error) {
    log.warn("无法加载国际化标题或站点配置，使用默认标题:", error);
    title = to.meta?.title || "CloudPaste";
  }

  document.title = title;

  const fromPage = from.meta?.originalPage || "unknown";
  const toPage = to.meta?.originalPage || "unknown";
  log.debug(`页面从 ${fromPage} 切换到 ${toPage}`);

  try {
    const authStore = useAuthStore();
    log.debug(`页面切换后权限状态: 认证类型=${authStore.authType}, 已认证=${authStore.isAuthenticated}, 管理员=${authStore.isAdmin}`);
  } catch (error) {
    log.warn("无法获取认证状态:", error);
  }

  // 保持原有的路径日志
  log.debug("路径变化检测:", to.path);
});

// 导出路由实例
export default router;

// 导出工具函数
export const routerUtils = {
  /**
   * 导航到指定页面
   * @param {string} page - 页面名称 ('home', 'upload', 'admin', 'paste-view', 'file-view', 'mount-explorer')
   * @param {object} options - 可选参数 (如 slug, path, module)
   */
  navigateTo(page, options = {}) {
    const routeMap = {
      home: { name: "Home" },
      // 兼容旧调用：upload 已并入 mount-explorer
      upload: { name: "MountExplorer" },
      admin: { name: "Admin" },
      "paste-view": {
        name: "PasteView",
        params: { slug: options.slug || options.pasteSlug },
      },
      "file-view": {
        name: "FileView",
        params: { slug: options.slug || options.fileSlug },
      },
      "folder-view": {
        name: "FolderShareView",
        params: { slug: options.slug || options.folderSlug },
      },
      "mount-explorer": { name: "MountExplorer" },
    };

    const route = routeMap[page];
    if (route) {
      // 特殊处理 admin 的模块参数
      if (page === "admin") {
        if (options.module && options.module !== "dashboard") {
          // 处理嵌套路由，如 settings/global
          if (options.module.includes("/")) {
            router.push(`/admin/${options.module}`);
          } else {
            router.push(`/admin/${options.module}`);
          }
        } else {
          router.push("/admin");
        }
        return;
      }

      // 特殊处理 mount-explorer 的路径参数
      if (page === "mount-explorer") {
        const routePath = buildMountExplorerRoutePath(options.path || "/");
        router.push({ path: routePath });
        return;
      }

      router.push(route);
    } else {
      log.warn(`未知页面: ${page}`);
      router.push({ name: "Home" });
    }
  },

  /**
   * 获取当前页面名称
   */
  getCurrentPage() {
    return router.currentRoute.value.meta?.originalPage || "home";
  },

  /**
   * 检查是否为指定页面
   */
  isCurrentPage(page) {
    return this.getCurrentPage() === page;
  },

  /**
   * 获取当前路由的 slug 参数
   */
  getCurrentSlug() {
    return router.currentRoute.value.params.slug || null;
  },
};
