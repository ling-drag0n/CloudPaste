/**
 * API请求客户端
 * 提供统一的请求方法和错误处理
 */

import { getFullApiUrl } from "./config";
import { ApiStatus } from "./ApiStatus"; // 导入API状态码常量
import { logoutViaBridge, buildAuthHeaders } from "@/modules/security/index.js";
import { enqueueOfflineOperation } from "@/modules/pwa-offline/index.js";
import { createLogger } from "@/utils/logger.js";
import { useOnline } from "@vueuse/core";

const isOnline = useOnline();

const apiLog = createLogger("API");

// - 优先使用后端返回的 message
// - 附带请求ID（X-Request-Id）方便排查
function extractRequestIdFromResponse(response) {
  try {
    return response?.headers?.get("X-Request-Id") || response?.headers?.get("x-request-id") || null;
  } catch {
    return null;
  }
}

function isGenericServerErrorMessage(message) {
  const text = String(message || "").trim();
  if (!text) return true;
  return (
    text === "服务器内部错误" ||
    text === "内部错误" ||
    text === "请求失败" ||
    text === "未知错误" ||
    text.includes("服务器内部错误") ||
    text.includes("内部错误")
  );
}

function appendRequestIdIfNeeded(message, requestId) {
  const text = String(message || "").trim();
  const rid = String(requestId || "").trim();
  if (!rid) return text;
  // 只在提示过于泛化时追加 requestId，避免正常业务错误变得很长
  if (!isGenericServerErrorMessage(text)) return text;
  return `${text}（请求ID:${rid}）`;
}

function appendDebugInfoIfNeeded(message, { requestId, debugMessage } = {}) {
  const text = String(message || "").trim();
  if (!isGenericServerErrorMessage(text)) return text;

  const parts = [];
  const rid = String(requestId || "").trim();
  if (debugMessage) {
    const reason = String(debugMessage).replace(/\s+/g, " ").trim();
    if (reason) {
      const clipped = reason.length > 160 ? `${reason.slice(0, 159)}…` : reason;
      parts.push(`原因:${clipped}`);
    }
  }
  if (rid) parts.push(`请求ID:${rid}`);
  if (!parts.length) return text;
  return `${text}（${parts.join("，")}）`;
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
  const isFolderPasswordVerify = endpoint.match(/^(\/)?share\/folder\/verify\/[a-zA-Z0-9_-]+$/i) && options.method === "POST";
  const isFsMetaPasswordVerify = endpoint.includes("/fs/meta/password/verify") && options.method === "POST";
  const hasPasswordInBody = options.body && (typeof options.body === "string" ? options.body.includes("password") : options.body.password);

  // 检查是否是修改密码请求
  const isChangePasswordRequest = endpoint.includes("/admin/change-password") && options.method === "POST";

  const isPasswordVerify = (isTextPasswordVerify || isFilePasswordVerify || isFolderPasswordVerify || isFsMetaPasswordVerify) && hasPasswordInBody;

  return {
    isPasswordVerify,
    isChangePasswordRequest,
    isTextPasswordVerify,
    isFilePasswordVerify,
    isFolderPasswordVerify,
    isFsMetaPasswordVerify,
    hasPasswordInBody,
  };
}

/**
 * 添加认证令牌到请求头
 * @param {Object} headers - 原始请求头
 * @returns {Promise<Object>} 添加了令牌的请求头
 */
async function addAuthToken(headers) {
  const merged = buildAuthHeaders(headers);

  if (headers.Authorization) {
    apiLog.debug("请求已携带 Authorization（来自调用方）");
  } else if (merged.Authorization) {
    apiLog.debug("请求已携带 Authorization（来自 authBridge）");
  } else {
    apiLog.debug("请求未携带 Authorization");
  }

  return merged;
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

  apiLog.debug(`API请求: ${debugInfo.method} ${debugInfo.url}`, {
    hasBody: !!debugInfo.body,
    headerKeys: Object.keys(debugInfo.headers || {}),
  });

  // PWA网络状态检测 - 符合最佳实践
  if (!isOnline.value) {
    apiLog.warn("离线状态，API请求可能失败:", url);
    if (options.method && options.method !== "GET") {
      await enqueueOfflineOperation(endpoint, options);
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

    apiLog.debug(`API响应耗时: ${timeTaken}ms, 状态: ${response.status}`, {
      url,
      status: response.status,
      statusText: response.statusText,
    });

    // 304 Not Modified：成熟项目常用的条件请求语义（If-None-Match）
    // - 304 无响应体，不应尝试解析 JSON
    // - 交由上层用本地缓存数据兜底
    if (response.status === 304) {
      const etag = response.headers.get("etag") || response.headers.get("ETag") || null;
      apiLog.debug(`API响应: 304 Not Modified`, { url, etag });
      return {
        success: true,
        notModified: true,
        status: 304,
        etag,
        data: null,
      };
    }

    // 首先解析响应内容
    let responseData;
    const contentType = response.headers.get("content-type");
    const requestId = extractRequestIdFromResponse(response);

    // 检查是否需要返回blob响应
    if (options.responseType === "blob") {
      responseData = await response.blob();
      apiLog.debug(`API响应Blob: ${responseData.size} 字节`, { url, type: responseData.type });
    } else if (contentType && contentType.includes("application/json")) {
      responseData = await response.json();
      apiLog.debug(`API响应JSON`, {
        url,
        kind: Array.isArray(responseData) ? "array" : typeof responseData,
        keys: responseData && typeof responseData === "object" && !Array.isArray(responseData) ? Object.keys(responseData).slice(0, 20) : undefined,
        length: Array.isArray(responseData) ? responseData.length : undefined,
      });
    } else {
      responseData = await response.text();
      apiLog.debug(`API响应文本: ${responseData.substring(0, 100)}${responseData.length > 100 ? "..." : ""}`);
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
        apiLog.error(`🚫 授权失败(${url}):`, responseData);

        // 检查特殊的密码验证请求类型
        const isPasswordRelatedRequest = checkPasswordRelatedRequest(endpoint, options);
        const { isPasswordVerify, isChangePasswordRequest } = isPasswordRelatedRequest;

        // 如果是密码验证请求，直接返回错误，不清除令牌
        if (isPasswordVerify) {
          apiLog.debug(`密码验证失败：不清除认证令牌（端点: ${endpoint}）`);

          // 确保返回后端提供的具体错误信息
          const errorMessage = responseData && responseData.message ? responseData.message : "密码错误";

          const error = new Error(errorMessage);
          error.__logged = true;
          throw error;
        }

        // 如果是修改密码请求，可能是当前密码验证失败
        if (isChangePasswordRequest) {
          // 返回具体的错误信息，通常是"当前密码错误"
          const errorMessage = responseData && responseData.message ? responseData.message : "验证失败";

          const error = new Error(errorMessage);
          error.__logged = true;
          throw error;
        }

        // 判断使用的是哪种认证方式
        const authHeader = requestOptions.headers.Authorization || "";
        const errorCode =
          responseData && typeof responseData === "object" && typeof responseData.code === "string"
            ? responseData.code
            : "";

        const isAuthErrorCode =
          errorCode === "UNAUTHORIZED" ||
          errorCode === "AUTH_ERROR" ||
          errorCode === "AUTHENTICATION_ERROR" ||
          errorCode === "AUTH_INVALID" ||
          errorCode === "AUTH_EXPIRED";

        // 管理员令牌过期
        if (authHeader.startsWith("Bearer ")) {
          // 仅在明确的认证错误场景下才执行登出：
          // - 后端返回的 code 表明是认证问题
          // - 或请求命中了 /admin 登录态相关接口
          const isAdminAuthEndpoint = endpoint.startsWith("/admin") || endpoint.includes("/admin/");

          if (isAuthErrorCode || isAdminAuthEndpoint) {
            apiLog.debug("管理员令牌验证失败：执行登出");
            await logoutViaBridge();
            const error = new Error("管理员会话已过期，请重新登录");
            error.__logged = true;
            throw error;
          }

          // 对于非认证类 401（例如存储驱动或业务错误），保留会话，仅抛出业务错误
          const errorMessage =
            responseData && typeof responseData === "object" && responseData.message
              ? responseData.message
              : "请求未授权，但当前管理员会话仍保持，请检查配置或稍后重试";

          const error = new Error(errorMessage);
          error.__logged = true;
          throw error;
        }

        // API密钥处理
        if (authHeader.startsWith("ApiKey ")) {
          const isPermissionIssue =
            responseData &&
            responseData.message &&
            (responseData.message.includes("未授权访问") ||
              responseData.message.includes("无权访问") ||
              responseData.message.includes("需要管理员权限或有效的API密钥") ||
              responseData.message.includes("权限不足") ||
              responseData.message.includes("没有权限"));

          if (isPermissionIssue) {
            apiLog.debug("API密钥权限不足：不执行登出");
            const error = new Error(responseData.message || "访问被拒绝，您可能无权执行此操作");
            error.__logged = true;
            throw error;
          }

          apiLog.debug("API密钥验证失败：执行登出");
          await logoutViaBridge();
          const apiKeyError = new Error("API密钥无效或已过期");
          apiKeyError.__logged = true;
          throw apiKeyError;
        }

        const unauthorizedError = new Error("未授权访问，请登录后重试");
        unauthorizedError.__logged = true;
        throw unauthorizedError;
      }

      // 对409状态码做特殊处理（链接后缀冲突或其他冲突）
      if (response.status === ApiStatus.CONFLICT) {
        apiLog.error(`❌ 资源冲突错误(${url}):`, responseData);
        // 使用后端返回的具体错误信息，无论是字符串形式还是对象形式
        if (typeof responseData === "string") {
          const error = new Error(responseData);
          error.__logged = true;
          throw error;
        } else if (responseData && typeof responseData === "object" && responseData.message) {
          const error = new Error(responseData.message);
          error.__logged = true;
          throw error;
        } else {
          const error = new Error("链接后缀已被占用，请尝试其他后缀");
          error.__logged = true;
          throw error;
        }
      }

      // 处理新的后端错误格式 (code, message)
      if (responseData && typeof responseData === "object") {
        apiLog.error(`❌ API错误(${url}):`, responseData);
        const baseMessage = responseData.message || `HTTP错误 ${response.status}: ${response.statusText}`;
        const payloadRequestId =
          typeof responseData.requestId === "string" && responseData.requestId.trim()
            ? responseData.requestId.trim()
            : requestId;
        const debugMessage = typeof responseData.debugMessage === "string" ? responseData.debugMessage : null;
        const error = new Error(appendDebugInfoIfNeeded(baseMessage, { requestId: payloadRequestId, debugMessage }));
        error.__logged = true;
        if (responseData.code) {
          error.code = responseData.code;
        }
        if (Object.prototype.hasOwnProperty.call(responseData, "data")) {
          error.data = responseData.data;
        }
        if (payloadRequestId) {
          error.requestId = payloadRequestId;
        }
        if (debugMessage) {
          error.debugMessage = debugMessage;
        }
        throw error;
      }

      apiLog.error(`❌ HTTP错误(${url}): ${response.status}`, responseData);
      const error = new Error(appendRequestIdIfNeeded(`HTTP错误 ${response.status}: ${response.statusText}`, requestId));
      error.__logged = true;
      if (requestId) {
        error.requestId = requestId;
      }
      throw error;
    }

    // 处理新的后端统一响应格式 (code, message, data)
    if (responseData && typeof responseData === "object") {
      // success 布尔判断
      if ("success" in responseData) {
        if (responseData.success !== true) {
          apiLog.error(`❌ API业务错误(${url}):`, responseData);
          const baseMessage = responseData.message || "请求失败";
          const payloadRequestId =
            typeof responseData.requestId === "string" && responseData.requestId.trim()
              ? responseData.requestId.trim()
              : requestId;
          const debugMessage = typeof responseData.debugMessage === "string" ? responseData.debugMessage : null;
          const error = new Error(appendDebugInfoIfNeeded(baseMessage, { requestId: payloadRequestId, debugMessage }));
          error.__logged = true;
          if (responseData.code) {
            error.code = responseData.code;
          }
          if (payloadRequestId) {
            error.requestId = payloadRequestId;
          }
          if (debugMessage) {
            error.debugMessage = debugMessage;
          }
          throw error;
        }
        return responseData;
      }

      // 兼容旧接口：HTTP 200 但 payload 的 code 表示失败（例如 code=500）
      if (
        typeof responseData.code === "number" &&
        responseData.code >= 400 &&
        responseData.message &&
        responseData.success !== true
      ) {
        const payloadRequestId =
          typeof responseData.requestId === "string" && responseData.requestId.trim()
            ? responseData.requestId.trim()
            : requestId;
        const debugMessage = typeof responseData.debugMessage === "string" ? responseData.debugMessage : null;
        const error = new Error(appendDebugInfoIfNeeded(responseData.message, { requestId: payloadRequestId, debugMessage }));
        error.__logged = true;
        error.code = responseData.code;
        if (payloadRequestId) {
          error.requestId = payloadRequestId;
        }
        if (debugMessage) {
          error.debugMessage = debugMessage;
        }
        throw error;
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
      // 请求被主动取消时，静默处理，不抛出错误
      apiLog.debug(`API请求被取消(${url})`);
      // 创建一个特殊的 AbortError 对象，让调用方可以识别
      const abortError = new Error("请求已取消");
      abortError.name = "AbortError";
      abortError.__aborted = true;
      abortError.__logged = true;
      throw abortError;
    } else if (error.name === "TimeoutError") {
      apiLog.error(`⏰ API请求超时(${url}):`, error.message);
      throw new Error("请求超时，服务器响应时间过长");
    } else if (error.name === "TypeError" && error.message.includes("fetch")) {
      apiLog.error(`🌐 网络错误(${url}):`, error.message);
      throw new Error("网络连接失败，请检查网络设置");
    } else {
      // 避免对已经在上层记录过的业务错误重复打印日志
      if (!error.__logged) {
        apiLog.error(`❌ API请求失败(${url}):`, error);
      }
      // 兜底：保证抛出去的一定是 Error，避免上层拿不到 error.message 而只能显示“未知错误”
      if (error instanceof Error) {
        throw error;
      }
      throw new Error(String(error ?? "未知错误"));
    }
  }
}

// 离线操作锁
let offlineOperationLock = false;

// 处理离线操作（PWA
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
      apiLog.debug(`[PWA] 已存储文本分享: ${responseData.data.slug}`);
    } else if (method === "POST" && endpoint.includes("/upload") && responseData.data) {
      // 存储上传的文件信息
      await pwaUtils.storage.saveFile(responseData.data);
      apiLog.debug(`[PWA] 已存储文件信息: ${responseData.data.filename || responseData.data.slug}`);
    } else if (method === "POST" && endpoint.includes("/admin/settings")) {
      // 存储重要设置更新
      const settingKey = `admin_setting_${Date.now()}`;
      await pwaUtils.storage.saveSetting(settingKey, responseData);
      apiLog.debug(`[PWA] 已存储管理员设置: ${settingKey}`);
    }
  } catch (error) {
    apiLog.warn("[PWA] 业务数据存储失败:", error);
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

      apiLog.debug(`发送二进制数据到 ${url}${partInfo}`, { size: data instanceof Blob ? data.size : data.byteLength });

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

              apiLog.debug(`二进制上传请求成功 ${url}${partInfo}`);
              resolve(responseData);
            } catch (e) {
              apiLog.error(`解析响应错误: ${e.message}`);
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

            apiLog.error(`❌ 二进制上传请求失败 ${url}${partInfo}: ${errorMsg}`);
            reject(new Error(errorMsg));
          }
        };

        // 监听网络错误
        xhr.onerror = function () {
          apiLog.error(`❌ 网络错误: ${url}${partInfo}`);
          reject(new Error("网络错误，请检查连接"));
        };

        // 超时时间
        xhr.timeout = options.timeout || 300000; // 默认5分钟超时

        // 监听超时
        xhr.ontimeout = function () {
          apiLog.error(`❌ 请求超时: ${url}${partInfo}`);
          reject(new Error("请求超时，服务器响应时间过长"));
        };

        // 监听中止
        xhr.onabort = function () {
          apiLog.debug(`请求已被中止: ${url}${partInfo}`);
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
    apiLog.error(`POST ${endpoint} 请求错误:`, error);
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
