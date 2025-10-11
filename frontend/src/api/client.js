/**
 * API请求客户端
 * 提供统一的请求方法和错误处理
 */

import { getFullApiUrl } from "./config";
import { ApiStatus } from "./ApiStatus"; // 导入API状态码常量

/**
 * 获取离线操作类型
 * @param {string} endpoint - API端点
 * @param {string} method - HTTP方法
 * @returns {Object|null} 操作类型信息或null（如果不支持离线）
 */
function getOfflineOperationType(endpoint, method) {
  // 文本分享操作
  if (endpoint.includes("/paste") && method === "POST") {
    return { type: "createPaste", description: "离线创建文本分享已加入队列" };
  }

  // 统一文本分享操作
  if (endpoint.includes("/pastes/")) {
    if (method === "PUT") return { type: "updatePaste", description: "离线更新文本分享已加入队列" };
  }

  if (endpoint.includes("/pastes/batch-delete") && method === "DELETE") {
    return { type: "batchDeletePastes", description: "离线批量删除文本分享已加入队列" };
  }

  if (endpoint.includes("/pastes/clear-expired") && method === "POST") {
    return { type: "clearExpiredPastes", description: "离线清理过期文本分享已加入队列" };
  }

  // 系统管理操作
  if (endpoint.includes("/admin/settings/group/") && method === "PUT") {
    return { type: "updateGroupSettings", description: "离线分组设置更新已加入队列" };
  }

  if (endpoint.includes("/admin/cache/clear") && method === "POST") {
    return { type: "clearCache", description: "离线缓存清理已加入队列" };
  }

  // 文件密码验证
  if (endpoint.includes("/public/files/") && endpoint.includes("/verify") && method === "POST") {
    return { type: "verifyFilePassword", description: "离线文件密码验证已加入队列" };
  }

  // 不支持的操作类型
  return null;
}

/**
 * 检查是否为密码相关的请求
 * @param {string} endpoint - API端点
 * @param {Object} options - 请求选项
 * @returns {Object} 密码请求类型检查结果
 */
function checkPasswordRelatedRequest(endpoint, options) {
  // 判断是否是密码验证请求（文本或文件分享的密码验证）
  const isTextPasswordVerify = endpoint.match(/^(\/)?paste\/[a-zA-Z0-9_-]+$/i) && options.method === "POST";
  const isFilePasswordVerify = endpoint.match(/^(\/)?public\/files\/[a-zA-Z0-9_-]+\/verify$/i) && options.method === "POST";
  const hasPasswordInBody = options.body && (typeof options.body === "string" ? options.body.includes("password") : options.body.password);

  // 检查是否是修改密码请求
  const isChangePasswordRequest = endpoint.includes("/admin/change-password") && options.method === "POST";

  const isPasswordVerify = (isTextPasswordVerify || isFilePasswordVerify) && hasPasswordInBody;

  return {
    isPasswordVerify,
    isChangePasswordRequest,
    isTextPasswordVerify,
    isFilePasswordVerify,
    hasPasswordInBody,
  };
}

/**
 * 添加认证令牌到请求头
 * @param {Object} headers - 原始请求头
 * @returns {Promise<Object>} 添加了令牌的请求头
 */
async function addAuthToken(headers) {
  // 如果请求头中已有Authorization，优先使用传入的值
  if (headers.Authorization) {
    console.log("使用传入的Authorization头:", headers.Authorization);
    return headers;
  }

  try {
    // 尝试从认证Store获取认证信息
    // 注意：这里需要动态导入，因为可能存在循环依赖
    const { useAuthStore } = await import("@/stores/authStore.js");
    const authStore = useAuthStore();

    // 检查管理员认证
    if (authStore.authType === "admin" && authStore.adminToken) {
      console.log("从认证Store获取admin_token，长度:", authStore.adminToken.length);
      return {
        ...headers,
        Authorization: `Bearer ${authStore.adminToken}`,
      };
    }

    // 检查API密钥认证（即使isAuthenticated还未设置为true）
    if (authStore.authType === "apikey" && authStore.apiKey) {
      console.log("从认证Store获取API密钥，长度:", authStore.apiKey.length);
      return {
        ...headers,
        Authorization: `ApiKey ${authStore.apiKey}`,
      };
    }
  } catch (error) {
    console.error("无法从认证Store获取认证信息:", error);
  }

  console.log("未找到认证凭据，请求将不包含Authorization头");
  return headers;
}

/**
 * 通用API请求方法
 * @param {string} endpoint - API端点路径
 * @param {Object} options - 请求选项
 * @returns {Promise<any>} 请求响应数据
 */
export async function fetchApi(endpoint, options = {}) {
  // 规范化查询参数处理
  let finalEndpoint = endpoint;
  if (options.params && Object.keys(options.params).length > 0) {
    const searchParams = new URLSearchParams();

    Object.entries(options.params).forEach(([key, value]) => {
      // 跳过undefined值（符合标准）
      if (value === undefined) {
        return;
      }

      // 处理数组参数（符合标准）
      if (Array.isArray(value)) {
        value.forEach((v) => {
          if (v !== undefined) {
            searchParams.append(key, String(v));
          }
        });
      } else if (value !== null) {
        // 单值参数使用set（避免重复）
        searchParams.set(key, String(value));
      }
    });

    const queryString = searchParams.toString();
    if (queryString) {
      finalEndpoint = endpoint.includes("?") ? `${endpoint}&${queryString}` : `${endpoint}?${queryString}`;
    }
  }

  const url = getFullApiUrl(finalEndpoint);

  // 详细的调试日志
  const debugInfo = {
    url,
    method: options.method || "GET",
    headers: { ...(options.headers || {}) },
    body: options.body,
    timestamp: new Date().toISOString(),
  };

  console.log(`🚀 API请求: ${debugInfo.method} ${debugInfo.url}`, debugInfo);

  // 🎯 PWA网络状态检测 - 符合最佳实践
  if (!navigator.onLine) {
    console.warn(`🔌 离线状态，API请求可能失败: ${url}`);
    // Service Worker Cache API会处理HTTP缓存，这里处理离线操作队列
    if (options.method && options.method !== "GET") {
      await handleOfflineOperation(endpoint, options);
    }
  }

  // 检查请求体是否为FormData类型
  const isFormData = options.body instanceof FormData;

  // 默认请求选项
  const defaultOptions = {
    headers: {
      // 如果是FormData，不设置默认的Content-Type，让浏览器自动处理
      ...(isFormData ? {} : { "Content-Type": "application/json" }),
    },
  };

  // 合并默认选项和用户传入的选项，并添加认证令牌
  const requestOptions = {
    ...defaultOptions,
    ...options,
    headers: await addAuthToken({
      ...defaultOptions.headers,
      ...options.headers,
    }),
  };

  // 如果请求体是对象类型但不是FormData，则自动序列化为JSON
  if (requestOptions.body && typeof requestOptions.body === "object" && !isFormData) {
    requestOptions.body = JSON.stringify(requestOptions.body);
  }

  try {
    const startTime = Date.now();

    // 添加默认超时处理（30秒）
    const timeoutMs = requestOptions.timeout || 30000;
    let signal = requestOptions.signal;

    // 如果没有提供signal，使用AbortSignal.timeout()（现代浏览器）
    if (!signal) {
      if (typeof AbortSignal.timeout === "function") {
        // 使用官方推荐的AbortSignal.timeout()
        signal = AbortSignal.timeout(timeoutMs);
      } else {
        // 降级到传统方式（兼容旧浏览器）
        const controller = new AbortController();
        signal = controller.signal;
        setTimeout(() => controller.abort(), timeoutMs);
      }
    }

    const response = await fetch(url, {
      ...requestOptions,
      signal,
    });
    const endTime = Date.now();
    const timeTaken = endTime - startTime;

    console.log(`⏱️ API响应耗时: ${timeTaken}ms, 状态: ${response.status}`, {
      url,
      status: response.status,
      statusText: response.statusText,
      headers: Object.fromEntries([...response.headers.entries()]),
    });

    // 首先解析响应内容
    let responseData;
    const contentType = response.headers.get("content-type");

    // 检查是否需要返回blob响应
    if (options.responseType === "blob") {
      responseData = await response.blob();
      console.log(`📦 API响应Blob(${url}): ${responseData.size} 字节, 类型: ${responseData.type}`);
    } else if (contentType && contentType.includes("application/json")) {
      responseData = await response.json();
      console.log(`📦 API响应数据(${url}):`, responseData);
    } else {
      responseData = await response.text();
      console.log(`📝 API响应文本(${url}): ${responseData.substring(0, 100)}${responseData.length > 100 ? "..." : ""}`);
    }

    // 如果响应不成功，抛出错误
    if (!response.ok) {
      // 对于blob响应的错误，需要重新解析为JSON获取错误信息
      if (options.responseType === "blob" && responseData instanceof Blob) {
        try {
          const errorText = await responseData.text();
          const errorData = JSON.parse(errorText);
          responseData = errorData;
        } catch (e) {
          // 如果无法解析为JSON，使用默认错误信息
          responseData = { message: `HTTP错误 ${response.status}` };
        }
      }

      // 特殊处理401未授权错误
      if (response.status === ApiStatus.UNAUTHORIZED) {
        console.error(`🚫 授权失败(${url}):`, responseData);

        // 检查特殊的密码验证请求类型
        const isPasswordRelatedRequest = checkPasswordRelatedRequest(endpoint, options);
        const { isPasswordVerify, isChangePasswordRequest } = isPasswordRelatedRequest;

        // 如果是密码验证请求，直接返回错误，不清除令牌
        if (isPasswordVerify) {
          console.log(`密码验证失败，不清除认证令牌。端点: ${endpoint}`);

          // 确保返回后端提供的具体错误信息
          const errorMessage = responseData && responseData.message ? responseData.message : "密码错误";

          throw new Error(errorMessage);
        }

        // 如果是修改密码请求，可能是当前密码验证失败
        if (isChangePasswordRequest) {
          // 返回具体的错误信息，通常是"当前密码错误"
          const errorMessage = responseData && responseData.message ? responseData.message : "验证失败";

          throw new Error(errorMessage);
        }

        // 判断使用的是哪种认证方式
        const authHeader = requestOptions.headers.Authorization || "";

        // 使用认证Store处理认证失败
        try {
          const { useAuthStore } = await import("@/stores/authStore.js");
          const authStore = useAuthStore();

          // 管理员令牌过期
          if (authHeader.startsWith("Bearer ")) {
            console.log("管理员令牌验证失败，执行登出");
            await authStore.logout();
            throw new Error("管理员会话已过期，请重新登录");
          }
          // API密钥处理
          else if (authHeader.startsWith("ApiKey ")) {
            // 检查是否是权限不足问题（而非API密钥无效）
            const isPermissionIssue =
              responseData &&
              responseData.message &&
              (responseData.message.includes("未授权访问") ||
                responseData.message.includes("无权访问") ||
                responseData.message.includes("需要管理员权限或有效的API密钥") ||
                responseData.message.includes("权限不足") ||
                responseData.message.includes("没有权限"));

            if (isPermissionIssue) {
              // 权限不足，仅抛出错误，但不清除API密钥
              console.log("API密钥权限不足，不执行登出");
              throw new Error(responseData.message || "访问被拒绝，您可能无权执行此操作");
            } else {
              // 其他情况（如密钥真的无效）时，执行登出
              console.log("API密钥验证失败，执行登出");
              await authStore.logout();
              throw new Error("API密钥无效或已过期");
            }
          } else {
            throw new Error("未授权访问，请登录后重试");
          }
        } catch (storeError) {
          console.error("无法使用认证Store处理认证失败:", storeError);
          throw new Error("认证失败，请重新登录");
        }
      }

      // 对409状态码做特殊处理（链接后缀冲突或其他冲突）
      if (response.status === ApiStatus.CONFLICT) {
        console.error(`❌ 资源冲突错误(${url}):`, responseData);
        // 使用后端返回的具体错误信息，无论是字符串形式还是对象形式
        if (typeof responseData === "string") {
          throw new Error(responseData);
        } else if (responseData && typeof responseData === "object" && responseData.message) {
          throw new Error(responseData.message);
        } else {
          throw new Error("链接后缀已被占用，请尝试其他后缀");
        }
      }

      // 处理新的后端错误格式 (code, message)
      if (responseData && typeof responseData === "object") {
        console.error(`❌ API错误(${url}):`, responseData);
        throw new Error(responseData.message || `HTTP错误 ${response.status}: ${response.statusText}`);
      }

      console.error(`❌ HTTP错误(${url}): ${response.status}`, responseData);
      throw new Error(`HTTP错误 ${response.status}: ${response.statusText}`);
    }

    // 处理新的后端统一响应格式 (code, message, data)
    if (responseData && typeof responseData === "object") {
      // 如果响应包含code字段
      if ("code" in responseData) {
        // 成功响应，code应该是200、201(创建成功)或202(部分成功)
        if (responseData.code !== ApiStatus.SUCCESS && responseData.code !== ApiStatus.CREATED && responseData.code !== ApiStatus.ACCEPTED) {
          console.error(`❌ API业务错误(${url}):`, responseData);
          throw new Error(responseData.message || "请求失败");
        }

        // 如果成功，返回完整的responseData
        return responseData;
      }

      // 如果响应不包含code字段，直接返回整个响应
      return responseData;
    }

    // 成功响应后存储重要业务数据到IndexedDB
    await handleSuccessfulResponse(endpoint, options, responseData);

    // 如果响应不符合统一格式，则直接返回
    return responseData;
  } catch (error) {
    // 处理不同类型的错误
    if (error.name === "AbortError") {
      console.warn(`⏹️ API请求被取消(${url}):`, error.message);
      throw new Error("请求被取消或超时");
    } else if (error.name === "TimeoutError") {
      console.error(`⏰ API请求超时(${url}):`, error.message);
      throw new Error("请求超时，服务器响应时间过长");
    } else if (error.name === "TypeError" && error.message.includes("fetch")) {
      console.error(`🌐 网络错误(${url}):`, error.message);
      throw new Error("网络连接失败，请检查网络设置");
    } else {
      console.error(`❌ API请求失败(${url}):`, error);
      throw error;
    }
  }
}

// 离线操作锁
let offlineOperationLock = false;

// 处理离线操作（PWA
async function handleOfflineOperation(endpoint, options) {
  if (offlineOperationLock) {
    console.log("[PWA] 离线操作正在处理中，跳过重复操作");
    return;
  }

  console.log(`[PWA] 处理离线操作: ${options.method} ${endpoint}`);
  try {
    offlineOperationLock = true;

    const { pwaUtils } = await import("../pwa/pwaManager.js");
    if (!pwaUtils || !pwaUtils.storage) {
      console.warn("[PWA] pwaUtils或storage不可用");
      return;
    }

    // 获取当前认证信息
    let authToken = null;
    let authType = null;

    try {
      const { useAuthStore } = await import("@/stores/authStore.js");
      const authStore = useAuthStore();

      if (authStore.authType === "admin" && authStore.adminToken) {
        authToken = authStore.adminToken;
        authType = "admin";
        console.log(`[PWA] 获取管理员认证信息，token长度: ${authToken.length}`);
      } else if (authStore.authType === "apikey" && authStore.apiKey) {
        authToken = authStore.apiKey;
        authType = "apikey";
        console.log(`[PWA] 获取API密钥认证信息，token长度: ${authToken.length}`);
      }
    } catch (error) {
      console.error("[PWA] 获取认证信息失败:", error);
    }

    const operation = {
      endpoint,
      method: options.method,
      data: options.body,
      authToken, // 保存认证token
      authType, // 保存认证类型
      timestamp: new Date().toISOString(),
      status: "pending",
    };

    // 根据端点和方法确定操作类型
    const operationType = getOfflineOperationType(endpoint, options.method);
    if (!operationType) {
      console.log(`[PWA] 跳过离线操作（不适合离线处理）: ${options.method} ${endpoint}`);
      return;
    }

    operation.type = operationType.type;
    await pwaUtils.storage.addToOfflineQueue(operation);
    console.log(`[PWA] ${operationType.description}`);

    // 尝试注册Background Sync以确保可靠同步
    if (pwaUtils.isBackgroundSyncSupported()) {
      try {
        await pwaUtils.registerBackgroundSync("sync-offline-queue");
        console.log("[PWA] Background Sync 已注册，操作将在网络恢复时自动同步");
      } catch (error) {
        console.warn("[PWA] Background Sync 注册失败:", error);
      }
    }
  } catch (error) {
    console.warn("[PWA] 离线操作处理失败:", error);
  } finally {
    // 确保锁被释放
    offlineOperationLock = false;
  }
}

// 处理成功响应的业务数据存储（PWA离线）
async function handleSuccessfulResponse(endpoint, options, responseData) {
  try {
    const { pwaUtils } = await import("../pwa/pwaManager.js");
    if (!pwaUtils || !pwaUtils.storage) return;

    const method = options.method || "GET";

    // 只存储用户创建的重要业务数据，不存储所有API响应
    if (method === "POST" && endpoint.includes("/paste") && responseData.data) {
      // 存储新创建的文本分享
      await pwaUtils.storage.savePaste(responseData.data);
      console.log(`[PWA] 已存储文本分享: ${responseData.data.slug}`);
    } else if (method === "POST" && endpoint.includes("/upload") && responseData.data) {
      // 存储上传的文件信息
      await pwaUtils.storage.saveFile(responseData.data);
      console.log(`[PWA] 已存储文件信息: ${responseData.data.filename || responseData.data.slug}`);
    } else if (method === "POST" && endpoint.includes("/admin/settings")) {
      // 存储重要设置更新
      const settingKey = `admin_setting_${Date.now()}`;
      await pwaUtils.storage.saveSetting(settingKey, responseData);
      console.log(`[PWA] 已存储管理员设置: ${settingKey}`);
    }
  } catch (error) {
    console.warn("[PWA] 业务数据存储失败:", error);
  }
}

/**
 * GET请求方法
 */
export function get(endpoint, options = {}) {
  return fetchApi(endpoint, { ...options, method: "GET" });
}

/**
 * 发送POST请求
 * @param {string} endpoint - API端点
 * @param {Object|ArrayBuffer|Blob} data - 请求数据
 * @param {Object} options - 可选配置
 * @returns {Promise<Object>} 响应数据
 */
export async function post(endpoint, data, options = {}) {
  try {
    // 检查是否需要发送原始二进制数据（用于分片上传）
    if (options.rawBody && (data instanceof ArrayBuffer || data instanceof Blob)) {
      const url = getFullApiUrl(endpoint);

      // 获取认证头
      const authHeaders = await addAuthToken({});
      const headers = {
        ...authHeaders,
        ...options.headers,
      };

      // 提取分片信息（如果存在）
      let partInfo = "";
      const partNumberMatch = endpoint.match(/partNumber=(\d+)/);
      const isLastPartMatch = endpoint.match(/isLastPart=(true|false)/);

      if (partNumberMatch) {
        const partNumber = partNumberMatch[1];
        const isLastPart = isLastPartMatch ? isLastPartMatch[1] === "true" : false;
        partInfo = `，分片: ${partNumber}${isLastPart ? " (最后分片)" : ""}`;
      }

      console.log(`发送二进制数据到 ${url}${partInfo}，大小: ${data instanceof Blob ? data.size : data.byteLength} 字节`);

      // 添加对 XHR 对象的处理，以支持取消功能
      const xhr = new XMLHttpRequest();

      // 如果提供了 XHR 创建回调，调用它以支持取消操作
      if (options.onXhrCreated && typeof options.onXhrCreated === "function") {
        options.onXhrCreated(xhr);
      }

      // 返回一个基于 XHR 的 Promise
      return new Promise((resolve, reject) => {
        xhr.open("POST", url, true);

        // 设置请求头
        Object.keys(headers).forEach((key) => {
          xhr.setRequestHeader(key, headers[key]);
        });

        // 设置超时
        if (options.timeout) {
          xhr.timeout = options.timeout;
        }

        // 设置响应类型为 JSON
        xhr.responseType = "json";

        // 监听上传进度
        if (options.onUploadProgress && typeof options.onUploadProgress === "function") {
          xhr.upload.onprogress = (event) => {
            if (event.lengthComputable) {
              options.onUploadProgress(Math.round((event.loaded / event.total) * 100));
            }
          };
        }

        // 监听请求完成
        xhr.onload = function () {
          if (xhr.status >= 200 && xhr.status < 300) {
            let responseData;

            // 尝试解析响应
            try {
              if (xhr.response) {
                responseData = xhr.response;
              } else if (xhr.responseType === "" || xhr.responseType === "text") {
                // 如果响应类型为文本，尝试解析为 JSON
                try {
                  responseData = JSON.parse(xhr.responseText);
                } catch (e) {
                  responseData = xhr.responseText;
                }
              } else {
                responseData = xhr.response;
              }

              console.log(`✅ 二进制上传请求成功 ${url}${partInfo}`);
              resolve(responseData);
            } catch (e) {
              console.error(`解析响应错误: ${e.message}`);
              reject(new Error(`解析响应错误: ${e.message}`));
            }
          } else {
            let errorMsg;
            try {
              if (xhr.responseType === "" || xhr.responseType === "text") {
                try {
                  const errorObj = JSON.parse(xhr.responseText);
                  errorMsg = errorObj.message || `HTTP错误 ${xhr.status}`;
                } catch (e) {
                  errorMsg = xhr.responseText || `HTTP错误 ${xhr.status}`;
                }
              } else if (xhr.response && xhr.response.message) {
                errorMsg = xhr.response.message;
              } else {
                errorMsg = `HTTP错误 ${xhr.status}`;
              }
            } catch (e) {
              errorMsg = `HTTP错误 ${xhr.status}`;
            }

            console.error(`❌ 二进制上传请求失败 ${url}${partInfo}: ${errorMsg}`);
            reject(new Error(errorMsg));
          }
        };

        // 监听网络错误
        xhr.onerror = function () {
          console.error(`❌ 网络错误: ${url}${partInfo}`);
          reject(new Error("网络错误，请检查连接"));
        };

        // 超时时间
        xhr.timeout = options.timeout || 300000; // 默认5分钟超时

        // 监听超时
        xhr.ontimeout = function () {
          console.error(`❌ 请求超时: ${url}${partInfo}`);
          reject(new Error("请求超时，服务器响应时间过长"));
        };

        // 监听中止
        xhr.onabort = function () {
          console.log(`⏹️ 请求已被中止: ${url}${partInfo}`);
          reject(new Error("请求已被用户取消"));
        };

        // 发送请求
        xhr.send(data);
      });
    }

    // 常规JSON数据或FormData
    return await fetchApi(endpoint, {
      ...options,
      method: "POST",
      body: data,
    });
  } catch (error) {
    console.error(`POST ${endpoint} 请求错误:`, error);
    throw error;
  }
}

/**
 * PUT请求方法
 */
export function put(endpoint, data, options = {}) {
  return fetchApi(endpoint, { ...options, method: "PUT", body: data });
}

/**
 * DELETE请求方法
 */
export function del(endpoint, data, options = {}) {
  return fetchApi(endpoint, { ...options, method: "DELETE", body: data });
}
