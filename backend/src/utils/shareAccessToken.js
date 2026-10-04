/**
 * 分享访问令牌（?at=）
 *
 * 背景：受密码保护的分享此前把明文密码拼在下载 URL 的 query 中，会泄露到
 * 浏览器历史 / 服务器与反代日志。现在密码验证接口成功后签发短时效 HMAC 令牌，
 * 下载 URL 只携带令牌；旧 ?password= 参数保持兼容。
 *
 * 令牌格式：<expireAtMs>.<base64url(HMAC-SHA256(secret, "shareat:<type>:<slug>:<expireAtMs>"))>
 */

const DEFAULT_TTL_SECONDS = 24 * 60 * 60;
const encoder = new TextEncoder();

function b64urlFromBuffer(buffer) {
  const bytes = new Uint8Array(buffer);
  let binary = "";
  const step = 0x8000;
  for (let i = 0; i < bytes.length; i += step) {
    binary += String.fromCharCode(...bytes.subarray(i, i + step));
  }
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

async function hmac(secret, message) {
  const key = await crypto.subtle.importKey("raw", encoder.encode(String(secret)), { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
  return crypto.subtle.sign("HMAC", key, encoder.encode(message));
}

function timingSafeEqualStrings(a, b) {
  const sa = String(a ?? "");
  const sb = String(b ?? "");
  const maxLen = Math.max(sa.length, sb.length);
  let diff = sa.length ^ sb.length;
  for (let i = 0; i < maxLen; i += 1) {
    diff |= (sa.charCodeAt(i) || 0) ^ (sb.charCodeAt(i) || 0);
  }
  return diff === 0;
}

/**
 * 签发分享访问令牌
 * @param {string} encryptionSecret
 * @param {{type: "file"|"folder", slug: string, ttlSeconds?: number}} options
 * @returns {Promise<string>}
 */
export async function generateShareAccessToken(encryptionSecret, { type, slug, ttlSeconds = DEFAULT_TTL_SECONDS }) {
  const ttl = Number(ttlSeconds) > 0 ? Math.min(Number(ttlSeconds), 7 * 24 * 60 * 60) : DEFAULT_TTL_SECONDS;
  const expireAtMs = Date.now() + Math.floor(ttl * 1000);
  const payload = `shareat:${type}:${slug}:${expireAtMs}`;
  const sig = b64urlFromBuffer(await hmac(encryptionSecret, payload));
  return `${expireAtMs}.${sig}`;
}

/**
 * 校验分享访问令牌（常量时间比较 + 过期检查）
 * @param {string} encryptionSecret
 * @param {string} token
 * @param {{type: "file"|"folder", slug: string}} options
 * @returns {Promise<boolean>}
 */
export async function verifyShareAccessToken(encryptionSecret, token, { type, slug }) {
  if (!token || typeof token !== "string") return false;
  const dotIndex = token.indexOf(".");
  if (dotIndex <= 0) return false;
  const expireRaw = token.slice(0, dotIndex);
  const sig = token.slice(dotIndex + 1);
  const expireAtMs = Number(expireRaw);
  if (!Number.isFinite(expireAtMs) || expireAtMs <= 0) return false;
  if (Date.now() > expireAtMs) return false;
  const payload = `shareat:${type}:${slug}:${expireRaw}`;
  const expected = b64urlFromBuffer(await hmac(encryptionSecret, payload));
  return timingSafeEqualStrings(expected, sig);
}
