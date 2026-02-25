import { ValidationError } from "../../../http/errors.js";
import { decryptValue } from "../../../utils/crypto.js";
import { createClient } from "webdav";
import { Buffer } from "buffer";
import https from "https";

function buildUtf8BasicAuthHeader(username, password) {
  const raw = `${username ?? ""}:${password ?? ""}`;
  const encoded = Buffer.from(raw, "utf8").toString("base64");
  return `Basic ${encoded}`;
}

function normalizeEndpointUrlForClient(value) {
  const raw = value == null ? "" : String(value).trim();
  if (!raw) return "";
  let url;
  try {
    url = new URL(raw);
  } catch {
    throw new ValidationError("endpoint_url 不是合法的 URL");
  }
  if (url.protocol !== "http:" && url.protocol !== "https:") {
    throw new ValidationError("endpoint_url 格式无效，必须以 http:// 或 https:// 开头");
  }
  if (!url.pathname.endsWith("/")) {
    url.pathname = `${url.pathname}/`;
  }
  return url.toString();
}

const normalize = (p) => {
  const cleaned = (p || "").toString().replace(/\\/g, "/").replace(/\/+/g, "/").replace(/^\/+/, "");
  const parts = cleaned.split("/").filter(Boolean);
  for (const seg of parts) {
    if (seg === "..") {
      throw new ValidationError("default_folder 不允许包含 ..");
    }
  }
  return parts.join("/");
};

const buildPath = (base, sub = "") => {
  const prefix = normalize(base);
  const rel = normalize(sub);
  let combined = prefix;
  if (rel) {
    combined = combined ? `${combined}/${rel}` : rel;
  }
  return combined ? `/${combined}` : "/";
};

export async function webDavTestConnection(config, encryptionSecret) {
  const endpoint = normalizeEndpointUrlForClient(config.endpoint_url);
  if (!endpoint) {
    throw new ValidationError("缺少 endpoint_url");
  }
  const username = config.username;
  const encryptedPassword = config.password;
  const defaultFolder = config.default_folder || "";
  if (!username || !encryptedPassword) {
    throw new ValidationError("缺少 WebDAV 用户名或密码");
  }

  const password = await decryptValue(encryptedPassword, encryptionSecret);
  if (!password) {
    throw new ValidationError("无法解密 WebDAV 密码");
  }

  const agent =
    endpoint.startsWith("https://") && config?.tls_insecure_skip_verify === 1
      ? new https.Agent({ rejectUnauthorized: false })
      : undefined;
  const clientOptions = agent ? { httpsAgent: agent } : {};
  const authHeader = buildUtf8BasicAuthHeader(username, password);
  const client = createClient(endpoint, {
    headers: {
      Authorization: authHeader,
    },
    ...clientOptions,
  });

  const state = {
    read: { success: false, error: null, prefix: null, objectCount: 0, firstObjects: [] },
    write: { success: false, error: null, testFile: null, uploadTime: 0, cleaned: false, cleanupError: null },
    info: {
      endpoint_url: endpoint,
      defaultFolder,
      tlsSkipVerify: config?.tls_insecure_skip_verify === 1,
      urlProxy: config.url_proxy || null,
      davCompliance: null,
      quota: null,
      davError: null,
      quotaError: null,
    },
  };

  const basePath = buildPath(defaultFolder);

  // 协议能力测试：DAV 合规信息
  try {
    // 使用默认目录路径探测 DAV 能力（大部分服务对任意路径返回一致的 DAV 头）
    const dav = await client.getDAVCompliance(basePath);
    state.info.davCompliance = dav || null;
  } catch (error) {
    state.info.davError = error?.message || String(error);
  }

  // 配额信息测试（部分服务可能不支持）
  if (config?.enable_disk_usage === 1) {
    try {
      // RFC 4331 的 quota 一般是对“collection（目录）”返回；这里优先对 defaultFolder 对应的 basePath 取
      // webdav 库不同版本的 getQuota 签名可能不同：有的支持传 path，有的不支持；所以做一次兼容尝试
      let quotaRes;
      try {
        quotaRes = client.getQuota.length >= 1 ? await client.getQuota(basePath) : await client.getQuota();
      } catch {
        quotaRes = await client.getQuota();
      }
      const quotaData = quotaRes && typeof quotaRes === "object" && "data" in quotaRes ? quotaRes.data : quotaRes;
      if (!quotaData || typeof quotaData !== "object") {
        state.info.quota = null;
        state.info.quotaError = "WebDAV 服务器未返回配额信息（可能不支持 RFC 4331 quota 属性）";
      } else {
        const used = typeof quotaData.used === "number" ? quotaData.used : null;
        const available = typeof quotaData.available === "number" ? quotaData.available : null;
        if (used == null && available == null) {
          state.info.quota = null;
          state.info.quotaError = "WebDAV 服务器未提供 quota-used-bytes / quota-available-bytes（可能未实现配额）";
        } else {
          state.info.quota = { used, available };
        }
      }
    } catch (error) {
      state.info.quotaError = error?.message || String(error);
    }
  } else {
    state.info.quota = null;
    state.info.quotaError = "磁盘占用统计未启用（enable_disk_usage = false）";
  }

  // 读测试：列根目录
  try {
    const dirRes = await client.getDirectoryContents(basePath, { deep: false, glob: "*" });
    const entries =
      Array.isArray(dirRes) && dirRes
        ? dirRes
        : dirRes && typeof dirRes === "object" && "data" in dirRes
        ? dirRes.data
        : [];

    state.read.success = true;
    state.read.prefix = basePath;
    state.read.objectCount = entries.length;

    if (entries.length > 0) {
      state.read.firstObjects = entries.slice(0, 3).map((item) => {
        const name = item.basename || item.filename || "";
        const size = typeof item.size === "number" ? item.size : 0;
        const lastModified = item.lastmod ? new Date(item.lastmod).toISOString() : null;
        return {
          key: name,
          size,
          lastModified,
        };
      });
    }
  } catch (error) {
    state.read.success = false;
    state.read.error = error?.message || String(error);
  }

  // 写测试：上传/删除临时小文件
  const testFile = `${basePath.endsWith("/") ? basePath : basePath + "/"}__webdav_test_${Date.now()}.txt`;
  state.write.testFile = testFile;
  try {
    const startTime = Date.now();
    await client.putFileContents(testFile, "cloudpaste webdav connectivity test", { overwrite: true });
    state.write.success = true;
    state.write.uploadTime = Date.now() - startTime;
    // 清理
    try {
      await client.deleteFile(testFile);
      state.write.cleaned = true;
    } catch (cleanupError) {
      state.write.cleaned = false;
      state.write.cleanupError = cleanupError?.message || String(cleanupError);
    }
  } catch (error) {
    state.write.success = false;
    state.write.error = error?.message || String(error);
  }

  // 汇总整体状态与消息（与 S3Tester 结构对齐）
  const basicConnectSuccess = state.read.success;
  const writeSuccess = state.write.success;

  const overallSuccess = basicConnectSuccess && writeSuccess;
  let message = "WebDAV 配置测试";

  if (basicConnectSuccess) {
    if (writeSuccess) {
      message += "成功 (读写权限均可用)";
    } else {
      message += "部分成功 (仅读权限可用)";
    }
  } else {
    message += "失败 (读取权限不可用)";
  }

  const checks = [
    {
      key: "read",
      label: "读权限",
      success: basicConnectSuccess === true,
      ...(state.read.error ? { error: state.read.error } : {}),
      items: [
        { key: "prefix", label: "目录前缀", value: state.read.prefix || basePath },
        { key: "objectCount", label: "对象数量", value: state.read.objectCount },
        ...(Array.isArray(state.read.firstObjects) && state.read.firstObjects.length
          ? [{ key: "sample", label: "示例对象", value: state.read.firstObjects }]
          : []),
      ],
    },
    {
      key: "write",
      label: "写权限",
      success: writeSuccess === true,
      ...(state.write.error ? { error: state.write.error } : {}),
      items: [
        { key: "testFile", label: "测试文件", value: state.write.testFile },
        { key: "uploadTime", label: "上传耗时(ms)", value: state.write.uploadTime },
        { key: "cleaned", label: "已清理", value: state.write.cleaned === true },
        ...(state.write.cleanupError ? [{ key: "cleanupError", label: "清理错误", value: state.write.cleanupError }] : []),
      ],
    },
  ];

  return { success: overallSuccess, message, result: { info: state.info, checks } };
}
