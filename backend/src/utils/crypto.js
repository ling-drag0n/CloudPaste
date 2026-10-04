/**
 * 加密相关工具函数
 */

import { sha256 } from "hono/utils/crypto";
import { ValidationError } from "../http/errors.js";
import { timingSafeEqualStrings } from "./fileEncryption.js";
// 导入Node.js的crypto模块以解决ESM环境中的引用错误
import crypto from "crypto";
// 为Node.js环境提供Web Crypto API的兼容层
import { webcrypto } from "crypto";
// 如果环境中没有全局crypto对象，将webcrypto赋值给全局
if (typeof globalThis.crypto === "undefined") {
  globalThis.crypto = webcrypto;
}

/**
 * base64 编码工具（兼容 Node / 浏览器 / Workers）
 */
const base64EncodeBytes = (bytes) => {
  // Node 环境：优先用 Buffer，稳定且性能好
  if (typeof Buffer !== "undefined") {
    return Buffer.from(bytes).toString("base64");
  }

  // Web/Worker 环境：把 bytes 转成二进制字符串，再 btoa
  let binary = "";
  const chunkSize = 0x8000;
  for (let i = 0; i < bytes.length; i += chunkSize) {
    const chunk = bytes.subarray(i, i + chunkSize);
    binary += String.fromCharCode(...chunk);
  }
  return btoa(binary);
};

const base64DecodeToBytes = (base64) => {
  if (typeof Buffer !== "undefined") {
    return new Uint8Array(Buffer.from(String(base64), "base64"));
  }

  const bin = atob(String(base64));
  const out = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i += 1) {
    out[i] = bin.charCodeAt(i);
  }
  return out;
};

const base64EncodeUtf8 = (text) => {
  const encoder = new TextEncoder();
  return base64EncodeBytes(encoder.encode(String(text)));
};

const base64DecodeUtf8 = (base64) => {
  const decoder = new TextDecoder("utf-8");
  const bytes = base64DecodeToBytes(base64);
  return decoder.decode(bytes);
};

/**
 * PBKDF2 迭代次数（可通过环境变量 PBKDF2_ITERATIONS 调整；Workers 免费版 CPU 受限时可调低）
 */
function getPbkdf2Iterations() {
  const raw = typeof process !== "undefined" && process.env ? process.env.PBKDF2_ITERATIONS : null;
  const parsed = Number(raw);
  if (!Number.isFinite(parsed)) return 100000;
  return Math.min(600000, Math.max(10000, Math.floor(parsed)));
}

function base64UrlEncode(bytes) {
  let binary = "";
  const step = 0x8000;
  for (let i = 0; i < bytes.length; i += step) {
    binary += String.fromCharCode(...bytes.subarray(i, i + step));
  }
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

function base64UrlDecode(b64url) {
  const normalized = String(b64url).replace(/-/g, "+").replace(/_/g, "/");
  const padded = normalized + "=".repeat((4 - (normalized.length % 4)) % 4);
  const bin = atob(padded);
  const out = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i += 1) {
    out[i] = bin.charCodeAt(i);
  }
  return out;
}

async function derivePbkdf2Hash(password, saltBytes, iterations) {
  const encoder = new TextEncoder();
  const baseKey = await crypto.subtle.importKey("raw", encoder.encode(password), "PBKDF2", false, ["deriveBits"]);
  const bits = await crypto.subtle.deriveBits(
    { name: "PBKDF2", hash: "SHA-256", salt: saltBytes, iterations },
    baseKey,
    256,
  );
  return new Uint8Array(bits);
}

/**
 * 生成密码哈希（PBKDF2-SHA256，随机盐）
 * 格式：pbkdf2$<iterations>$<saltB64url>$<hashB64url>
 * 旧格式（64位 SHA-256 hex / 历史明文）仍可被 verifyPassword 验证
 * @param {string} password - 原始密码
 * @returns {Promise<string>} 密码哈希
 */
export async function hashPassword(password) {
  const iterations = getPbkdf2Iterations();
  const salt = crypto.getRandomValues(new Uint8Array(16));
  const hash = await derivePbkdf2Hash(String(password), salt, iterations);
  return `pbkdf2$${iterations}$${base64UrlEncode(salt)}$${base64UrlEncode(hash)}`;
}

/**
 * 验证密码（按存储格式自动分发：pbkdf2$ / 64位SHA-256 / 历史明文）
 * @param {string} plainPassword - 原始密码
 * @param {string} hashedPassword - 哈希后的密码
 * @returns {Promise<boolean>} 验证结果
 */
export async function verifyPassword(plainPassword, hashedPassword) {
  if (!hashedPassword || typeof hashedPassword !== "string") {
    return false;
  }

  // PBKDF2 新格式
  if (hashedPassword.startsWith("pbkdf2$")) {
    const parts = hashedPassword.split("$");
    if (parts.length !== 4) return false;
    const iterations = Number(parts[1]);
    if (!Number.isFinite(iterations) || iterations < 1) return false;
    try {
      const salt = base64UrlDecode(parts[2]);
      const expected = base64UrlDecode(parts[3]);
      const actual = await derivePbkdf2Hash(String(plainPassword), salt, iterations);
      // 逐字节常量时间比较
      let diff = actual.length ^ expected.length;
      const len = Math.min(actual.length, expected.length);
      for (let i = 0; i < len; i += 1) {
        diff |= actual[i] ^ expected[i];
      }
      return diff === 0;
    } catch {
      return false;
    }
  }

  // 旧格式：SHA-256 hex（无盐）
  if (hashedPassword.length === 64) {
    const hashedInput = await sha256(plainPassword);
    return timingSafeEqualStrings(hashedInput, hashedPassword);
  }

  // 历史明文兜底
  return timingSafeEqualStrings(plainPassword, hashedPassword);
}

/**
 * 派生配置加密密钥（AES-256-GCM）
 */
async function deriveConfigKey(secret) {
  const encoder = new TextEncoder();
  const baseKey = await crypto.subtle.importKey("raw", encoder.encode(String(secret)), "HKDF", false, ["deriveBits"]);
  const bits = await crypto.subtle.deriveBits(
    {
      name: "HKDF",
      hash: "SHA-256",
      salt: encoder.encode("cloudpaste-config-encryption"),
      info: encoder.encode("config-enc-v1"),
    },
    baseKey,
    256,
  );
  return crypto.subtle.importKey("raw", bits, { name: "AES-GCM" }, false, ["encrypt", "decrypt"]);
}

/**
 * 加密敏感配置（AES-256-GCM）
 * 新格式：enc1:<ivB64url>:<ctB64url>；旧格式 encrypted:<sig>:<payload> 仍可解密
 * @param {string} value - 需要加密的值
 * @param {string} secret - 加密密钥
 * @returns {Promise<string>} 加密后的值
 */
export async function encryptValue(value, secret) {
  const encoder = new TextEncoder();
  const iv = crypto.getRandomValues(new Uint8Array(12));
  const key = await deriveConfigKey(secret);
  const cipher = new Uint8Array(await crypto.subtle.encrypt({ name: "AES-GCM", iv }, key, encoder.encode(String(value))));
  return `enc1:${base64UrlEncode(iv)}:${base64UrlEncode(cipher)}`;
}

/**
 * 解密敏感配置
 * 兼容三种格式：enc1:（AES-GCM）、encrypted:（旧版 base64 伪加密）、明文直返
 * @param {string} encryptedValue - 加密后的值
 * @param {string} secret - 加密密钥
 * @returns {Promise<string>} 解密后的值
 */
export async function decryptValue(encryptedValue, secret) {
  // 检查是否为加密值
  if (encryptedValue === undefined || encryptedValue === null) {
    // 容错：未提供值时按原样返回，避免空值触发运行时错误
    return encryptedValue;
  }
  if (typeof encryptedValue !== "string") {
    // 非字符串直接返回（保持向后兼容，不在此抛错）
    return encryptedValue;
  }

  // 新格式：AES-256-GCM
  if (encryptedValue.startsWith("enc1:")) {
    const parts = encryptedValue.split(":");
    if (parts.length !== 3) {
      throw new ValidationError("无效的加密格式");
    }
    try {
      const key = await deriveConfigKey(secret);
      const iv = base64UrlDecode(parts[1]);
      const cipher = base64UrlDecode(parts[2]);
      const plain = await crypto.subtle.decrypt({ name: "AES-GCM", iv }, key, cipher);
      return new TextDecoder().decode(plain);
    } catch (error) {
      throw new ValidationError("解密失败: " + (error?.message || "密钥不匹配"));
    }
  }

  // 旧格式：base64 伪加密（仅完整性标签，无保密性），保持可读
  if (encryptedValue.startsWith("encrypted:")) {
    const parts = encryptedValue.split(":");
    if (parts.length !== 3) {
      throw new ValidationError("无效的加密格式");
    }
    try {
      return base64DecodeUtf8(parts[2]);
    } catch (error) {
      throw new ValidationError("解密失败: " + error.message);
    }
  }

  return encryptedValue; // 未加密的值直接返回
}

/**
 * 对密钥进行掩码展示
 * @param {string|null|undefined} secret
 * @param {number} visibleTail 显示尾部多少位
 * @returns {string|null|undefined}
 */
export function maskSecret(secret, visibleTail = 4) {
  if (!secret || typeof secret !== "string") return secret;
  if (secret.length <= visibleTail) return "*".repeat(Math.max(0, secret.length));
  return "*".repeat(secret.length - visibleTail) + secret.slice(-visibleTail);
}

/**
 * 如是加密格式则解密，否则直返原值（兼容历史明文）
 * @param {string|null|undefined} value
 * @param {string} encryptionSecret
 * @returns {Promise<string|null|undefined>}
 */
export async function decryptIfNeeded(value, encryptionSecret) {
  if (value === null || value === undefined) return value;
  if (typeof value !== "string") return value;
  return await decryptValue(value, encryptionSecret);
}

/**
 * 生成前端可控的密钥展示对象
 * @param {object} cfg 原始配置对象（包含密钥字段）
 * @param {string} encryptionSecret
 * @param {{mode:'none'|'masked'|'plain'}} options
 * @returns {Promise<object>} 带密钥字段处理后的对象
 */
export async function buildSecretView(cfg, encryptionSecret, options = { mode: "none" }) {
  const mode = options.mode || "none";
  const result = { ...cfg };

  // 统一的 secret 字段集合
  // - S3: access_key_id / secret_access_key
  // - WebDAV: password
  // - OneDrive/GoogleDrive: client_secret / refresh_token
  // - Telegram: bot_token
  // - GitHub: token
  // - HuggingFace: hf_token
  const SECRET_FIELDS = [
    "access_key_id",
    "secret_access_key",
    "password",
    "client_secret",
    "refresh_token",
    "bot_token",
    "token",
    "hf_token",
  ];

  if (mode === "none") {
    for (const key of SECRET_FIELDS) {
      delete result[key];
    }
    return result;
  }
  if (mode === "masked") {
    for (const key of SECRET_FIELDS) {
      if (!Object.prototype.hasOwnProperty.call(cfg, key)) continue;
      result[key] = maskSecret(await decryptIfNeeded(cfg[key], encryptionSecret));
    }
    return result;
  }
  if (mode === "plain") {
    for (const key of SECRET_FIELDS) {
      if (!Object.prototype.hasOwnProperty.call(cfg, key)) continue;
      result[key] = await decryptIfNeeded(cfg[key], encryptionSecret);
    }
    return result;
  }
  return result;
}
