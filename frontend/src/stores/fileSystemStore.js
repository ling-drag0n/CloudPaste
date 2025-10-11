/**
 * 文件系统 Pinia Store
 * 管理文件系统的全局状态和业务逻辑
 */

import { defineStore } from "pinia";
import { ref, computed } from "vue";
import { useRouter } from "vue-router";
import { api } from "@/api";
import { useAuthStore } from "./authStore.js";

export const useFileSystemStore = defineStore("fileSystem", () => {
  const router = useRouter();

  // ===== 状态管理 =====
  const currentPath = ref("/");
  const directoryData = ref(null);
  const loading = ref(false);
  const error = ref(null);
  const isInitialized = ref(false);

  // 请求去重状态
  const currentLoadingPath = ref(null);

  // 初始化状态控制
  const initializationState = ref({
    isInitializing: false,
    lastProcessedRoute: null,
  });

  // ===== 计算属性 =====
  const authStore = useAuthStore();

  /**
   * 检查当前路径是否有权限访问
   */
  const hasPermissionForCurrentPath = computed(() => {
    return authStore.hasPathPermission(currentPath.value);
  });

  /**
   * 是否应该加载目录（有权限且已初始化）
   */
  const shouldLoadDirectory = computed(() => {
    return hasPermissionForCurrentPath.value && isInitialized.value;
  });

  /**
   * 目录项目列表
   */
  const directoryItems = computed(() => {
    return directoryData.value?.items || [];
  });

  /**
   * 是否为虚拟目录
   */
  const isVirtualDirectory = computed(() => {
    return directoryData.value?.isVirtual || false;
  });

  // ===== Actions =====

  /**
   * 加载目录内容
   * @param {string} path - 目录路径
   * @param {boolean} force - 是否强制刷新
   */
  const loadDirectory = async (path, force = false) => {
    const normalizedPath = path || "/";

    // 防止重复请求
    if (!force && currentLoadingPath.value === normalizedPath) {
      console.log(`目录 ${normalizedPath} 正在加载中，跳过重复请求`);
      return;
    }

    // 权限检查
    if (!authStore.hasPathPermission(normalizedPath)) {
      error.value = `没有权限访问路径: ${normalizedPath}`;
      return;
    }

    try {
      loading.value = true;
      currentLoadingPath.value = normalizedPath;
      error.value = null;

      // 使用统一API函数
      const getDirectoryList = api.fs.getDirectoryList;

      // 调用API获取目录内容，传递refresh选项
      const response = await getDirectoryList(normalizedPath, { refresh: force });

      if (response.success) {
        directoryData.value = response.data;
        currentPath.value = normalizedPath;
      } else {
        throw new Error(response.message || "获取目录列表失败");
      }
    } catch (err) {
      console.error("加载目录失败:", err);
      error.value = err.message || "加载目录失败";
      directoryData.value = null;
    } finally {
      loading.value = false;
      currentLoadingPath.value = null;
    }
  };

  /**
   * 刷新当前目录
   * 主动刷新
   */
  const refreshDirectory = async () => {
    if (currentPath.value) {
      await loadDirectory(currentPath.value, true);
    }
  };

  /**
   * 导航到指定路径
   * @param {string} path - 目标路径
   */
  const navigateTo = async (path) => {
    const normalizedPath = path || "/";

    // 关闭预览模式（如果有）
    const route = router.currentRoute.value;
    if (route.query.preview) {
      // 清除预览参数
      updateUrl(normalizedPath);
    } else {
      // 更新URL
      updateUrl(normalizedPath);
    }
  };

  /**
   * 更新浏览器URL
   * @param {string} path - 路径
   * @param {string} previewFile - 预览文件名（可选）
   */
  const updateUrl = (path, previewFile = null) => {
    const normalizedPath = path || "/";
    const query = {};

    if (previewFile) {
      query.preview = previewFile;
    }

    let routePath = "/mount-explorer";
    if (normalizedPath !== "/") {
      const pathSegments = normalizedPath.split("/").filter(Boolean);
      if (pathSegments.length > 0) {
        routePath = `/mount-explorer/${pathSegments.join("/")}`;
      }
    }

    router.push({
      path: routePath,
      query,
    });
  };

  /**
   * 从路由获取路径
   */
  const getPathFromRoute = () => {
    const route = router.currentRoute.value;

    if (route.params.pathMatch) {
      const pathArray = Array.isArray(route.params.pathMatch) ? route.params.pathMatch : [route.params.pathMatch];
      const urlPath = "/" + pathArray.join("/");
      return urlPath.endsWith("/") ? urlPath : urlPath + "/";
    }
    return "/";
  };

  /**
   * 判断当前路由意图：是目录浏览还是文件预览
   */
  const getRouteIntent = () => {
    const route = router.currentRoute.value;

    // 如果有preview参数，说明主要意图是文件预览
    if (route.query.preview) {
      return {
        type: "file_preview",
        directoryPath: getPathFromRoute(),
        fileName: route.query.preview,
      };
    }

    // 否则是目录浏览
    return {
      type: "directory_browse",
      directoryPath: getPathFromRoute(),
    };
  };

  /**
   * 初始化路径（包含权限检查）
   */
  const initializePath = () => {
    const urlPath = getPathFromRoute();

    if (!authStore.isAdmin && authStore.apiKeyInfo) {
      const basicPath = authStore.apiKeyInfo.basic_path || "/";
      const normalizedBasicPath = basicPath === "/" ? "/" : basicPath.replace(/\/+$/, "");
      const normalizedUrlPath = urlPath.replace(/\/+$/, "") || "/";

      if (normalizedBasicPath !== "/" && normalizedUrlPath !== normalizedBasicPath && !normalizedUrlPath.startsWith(normalizedBasicPath + "/")) {
        console.log("URL路径超出权限范围，重定向到基本路径:", basicPath);
        // 先设置正确的路径，再进行重定向
        currentPath.value = basicPath;
        // 使用updateUrl而不是navigateTo，避免循环调用
        updateUrl(basicPath);
        return false; // 返回false表示需要重定向，不应继续执行后续逻辑
      }
    }

    currentPath.value = urlPath;
    return true; // 返回true表示路径正常，可以继续执行
  };

  /**
   * 从路由初始化文件系统
   */
  const initializeFromRoute = async () => {
    // 防止重复初始化
    const route = router.currentRoute.value;
    const routeKey = `${route.path}?${new URLSearchParams(route.query).toString()}`;

    // 只防止正在初始化中的重复调用，不防止路由变化后的重新初始化
    if (initializationState.value.isInitializing) {
      return;
    }

    initializationState.value.isInitializing = true;
    initializationState.value.lastProcessedRoute = routeKey;

    try {
      // 等待认证状态就绪
      if (!authStore.isAuthenticated) {
        console.log("等待认证状态就绪...");
        initializationState.value.isInitializing = false; // 重置状态，避免阻塞后续调用
        return;
      }

      // 1. 首先初始化路径
      const shouldContinue = initializePath();

      // 如果路径初始化返回false（表示需要重定向），则不继续执行后续逻辑
      if (!shouldContinue) {
        initializationState.value.isInitializing = false; // 重置状态
        return;
      }

      // 标记为已初始化
      isInitialized.value = true;

      // 2. 智能判断路由意图：只有在目录浏览时才加载目录
      const intent = getRouteIntent();

      if (intent.type === "directory_browse") {
        await loadDirectory(currentPath.value);
      } else if (intent.type === "file_preview") {
        // 如果目录未加载或路径不匹配，才加载目录
        if (!directoryData.value || currentPath.value !== intent.directoryPath) {
          await loadDirectory(intent.directoryPath);
        }
      }
    } catch (err) {
      console.error("文件系统初始化失败:", err);
      error.value = err.message || "初始化失败";
    } finally {
      initializationState.value.isInitializing = false;
    }
  };

  /**
   * 重置状态
   */
  const resetState = () => {
    currentPath.value = "/";
    directoryData.value = null;
    loading.value = false;
    error.value = null;
    isInitialized.value = false;
    currentLoadingPath.value = null;
    initializationState.value = {
      isInitializing: false,
      lastProcessedRoute: null,
    };
  };

  return {
    // 状态
    currentPath,
    directoryData,
    loading,
    error,
    isInitialized,

    // 计算属性
    hasPermissionForCurrentPath,
    shouldLoadDirectory,
    directoryItems,
    isVirtualDirectory,

    // Actions
    loadDirectory,
    refreshDirectory,
    navigateTo,
    updateUrl,
    initializeFromRoute,
    resetState,
  };
});
