/**
 * 端到端加密（E2E）前端模块 —— CPENC1 分块 AES-256-GCM
 *
 * 与后端 backend/src/utils/fileEncryption.js 共用同一格式 spec：
 *   Header: 8B magic "CPENC1\0\0" + 4B LE chunkSize + 8B 随机 noncePrefix
 *   Chunk:  4B LE 密文长度 + 密文（明文块 + 16B GCM tag）
 *   块 nonce = noncePrefix(8B) || 4B BE 块序号；AAD = "CPENC1:" + 块序号
 *
 * 密钥模型：每文件随机 32B DEK，只存在于分享链接 fragment（#e2ek=），服务器零密钥。
 */

export const DEFAULT_CHUNK_SIZE = 4 * 1024 * 1024;
const MIN_CHUNK_SIZE = 64 * 1024;
const MAX_CHUNK_SIZE = 64 * 1024 * 1024;
const MAGIC_BYTES = new Uint8Array([0x43, 0x50, 0x45, 0x4e, 0x43, 0x31, 0x00, 0x00]); // "CPENC1\0\0"
const HEADER_SIZE = 20;
const LEN_PREFIX_SIZE = 4;
const TAG_SIZE = 16;

const encoder = new TextEncoder();

// ------------------------- base64url -------------------------

export function bytesToB64url(bytes) {
  let binary = "";
  const step = 0x8000;
  for (let i = 0; i < bytes.length; i += step) {
    binary += String.fromCharCode(...bytes.subarray(i, i + step));
  }
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

export function b64urlToBytes(b64url) {
  const normalized = String(b64url).replace(/-/g, "+").replace(/_/g, "/");
  const padded = normalized + "=".repeat((4 - (normalized.length % 4)) % 4);
  const bin = atob(padded);
  const out = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i += 1) {
    out[i] = bin.charCodeAt(i);
  }
  return out;
}

// ------------------------- 密钥 -------------------------

/** 生成随机 32B DEK，返回 base64url（用于放进链接 fragment） */
export function generateE2EKeyB64() {
  return bytesToB64url(crypto.getRandomValues(new Uint8Array(32)));
}

async function importKey(rawBytes) {
  return crypto.subtle.importKey("raw", rawBytes, { name: "AES-GCM" }, false, ["encrypt", "decrypt"]);
}

function chunkNonce(noncePrefix, index) {
  const nonce = new Uint8Array(12);
  nonce.set(noncePrefix.subarray(0, 8), 0);
  nonce[8] = (index >>> 24) & 0xff;
  nonce[9] = (index >>> 16) & 0xff;
  nonce[10] = (index >>> 8) & 0xff;
  nonce[11] = index & 0xff;
  return nonce;
}

function chunkAad(index) {
  return encoder.encode("CPENC1:" + index);
}

// ------------------------- meta -------------------------

function normalizeChunkSize(chunkSize) {
  const size = Number(chunkSize) || DEFAULT_CHUNK_SIZE;
  if (size < MIN_CHUNK_SIZE || size > MAX_CHUNK_SIZE || (size & (size - 1)) !== 0) {
    return DEFAULT_CHUNK_SIZE;
  }
  return size;
}

/**
 * 校验并规范化 e2e meta（后端/前端通用结构）
 */
export function validateE2EMeta(meta) {
  if (!meta || typeof meta !== "object") throw new Error("无效的端到端加密元数据");
  if (meta.mode !== "e2e" || meta.algo !== "A256GCM") throw new Error("不支持的端到端加密格式");
  const noncePrefix = b64urlToBytes(meta.noncePrefix);
  if (noncePrefix.length !== 8) throw new Error("无效的端到端加密 nonce");
  meta.chunkSize = normalizeChunkSize(meta.chunkSize);
  meta.plainSize = Number(meta.plainSize) || 0;
  meta.cipherSize = Number(meta.cipherSize) || 0;
  return meta;
}

/** 第 i 块的明文长度 */
function plainLengthOfChunk(meta, i) {
  const cs = meta.chunkSize;
  const fullChunks = Math.floor(meta.plainSize / cs);
  const hasTail = meta.plainSize % cs > 0;
  if (i < fullChunks) return cs;
  if (i === fullChunks && hasTail) return meta.plainSize - fullChunks * cs;
  if (meta.plainSize === 0 && i === 0) return 0;
  return cs;
}

/** 第 i 块的密文起始偏移 */
function cipherOffsetOfChunk(meta, i) {
  const cs = meta.chunkSize;
  const fullChunks = Math.floor(meta.plainSize / cs);
  if (meta.plainSize === 0) return HEADER_SIZE;
  if (i < fullChunks) return HEADER_SIZE + i * (LEN_PREFIX_SIZE + cs + TAG_SIZE);
  return HEADER_SIZE + fullChunks * (LEN_PREFIX_SIZE + cs + TAG_SIZE);
}

/**
 * 明文区间 -> 密文块区间映射（与后端实现一致）
 */
export function mapPlainRangeToCipherRange(meta, start, end) {
  const cs = meta.chunkSize;
  const plainSize = meta.plainSize;
  if (plainSize <= 0) {
    return { firstChunk: 0, lastChunk: 0, cipherStart: HEADER_SIZE, cipherEnd: HEADER_SIZE + LEN_PREFIX_SIZE + TAG_SIZE - 1, skipPlain: 0, maxPlain: 0 };
  }
  const s = Math.max(0, Math.min(Number(start) || 0, plainSize - 1));
  const e = Math.max(s, Math.min(Number(end) || 0, plainSize - 1));
  const firstChunk = Math.floor(s / cs);
  const lastChunk = Math.floor(e / cs);
  const cipherStart = cipherOffsetOfChunk(meta, firstChunk);
  const cipherEnd = cipherOffsetOfChunk(meta, lastChunk) + LEN_PREFIX_SIZE + plainLengthOfChunk(meta, lastChunk) + TAG_SIZE - 1;
  return { firstChunk, lastChunk, cipherStart, cipherEnd, skipPlain: s - firstChunk * cs, maxPlain: e - s + 1 };
}

// ------------------------- 加密 -------------------------

/**
 * 加密 Blob/File（内存型，建议 ≤200MB）
 * @param {Blob|File} blob
 * @param {{chunkSize?: number, onProgress?: (percent:number)=>void}} [options]
 * @returns {Promise<{cipherBlob: Blob, meta: object, keyB64: string}>}
 */
export async function encryptBlobForE2E(blob, options = {}) {
  if (!blob) throw new Error("缺少待加密内容");
  const chunkSize = normalizeChunkSize(options.chunkSize);
  const rawKey = crypto.getRandomValues(new Uint8Array(32));
  const noncePrefix = crypto.getRandomValues(new Uint8Array(8));
  const key = await importKey(rawKey);

  const plainSize = blob.size;
  const parts = [];
  const header = new Uint8Array(HEADER_SIZE);
  header.set(MAGIC_BYTES, 0);
  new DataView(header.buffer).setUint32(8, chunkSize, true);
  header.set(noncePrefix, 12);
  parts.push(header);
  let cipherSize = HEADER_SIZE;

  let index = 0;
  let offset = 0;
  while (offset < plainSize) {
    const end = Math.min(offset + chunkSize, plainSize);
    const plain = new Uint8Array(await blob.slice(offset, end).arrayBuffer());
    const cipher = new Uint8Array(
      await crypto.subtle.encrypt({ name: "AES-GCM", iv: chunkNonce(noncePrefix, index), additionalData: chunkAad(index) }, key, plain),
    );
    const record = new Uint8Array(LEN_PREFIX_SIZE + cipher.length);
    new DataView(record.buffer).setUint32(0, cipher.length, true);
    record.set(cipher, LEN_PREFIX_SIZE);
    parts.push(record);
    cipherSize += record.length;
    index += 1;
    offset = end;
    if (typeof options.onProgress === "function") {
      try {
        options.onProgress(Math.round((offset / plainSize) * 100));
      } catch {}
    }
  }
  // 空文件：单个 0 明文块
  if (plainSize === 0) {
    const cipher = new Uint8Array(
      await crypto.subtle.encrypt({ name: "AES-GCM", iv: chunkNonce(noncePrefix, 0), additionalData: chunkAad(0) }, key, new Uint8Array(0)),
    );
    const record = new Uint8Array(LEN_PREFIX_SIZE + cipher.length);
    new DataView(record.buffer).setUint32(0, cipher.length, true);
    record.set(cipher, LEN_PREFIX_SIZE);
    parts.push(record);
    cipherSize += record.length;
  }

  const meta = {
    v: 1,
    mode: "e2e",
    algo: "A256GCM",
    keyId: "v1",
    kid: "",
    chunkSize,
    noncePrefix: bytesToB64url(noncePrefix),
    plainSize,
    cipherSize,
  };

  return {
    cipherBlob: new Blob(parts, { type: "application/octet-stream" }),
    meta,
    keyB64: bytesToB64url(rawKey),
  };
}

// ------------------------- 解密 -------------------------

/**
 * 解密完整密文 Blob
 * @param {object} meta e2e 元数据
 * @param {string|Uint8Array} keyB64 密钥（base64url 或原始字节）
 * @param {Blob} cipherBlob
 * @param {{onProgress?: (percent:number)=>void}} [options]
 * @returns {Promise<Blob>} 明文 Blob
 */
export async function decryptBlobForE2E(meta, keyB64, cipherBlob, options = {}) {
  validateE2EMeta(meta);
  const key = await importKey(typeof keyB64 === "string" ? b64urlToBytes(keyB64) : keyB64);
  const cs = meta.chunkSize;
  const plainSize = meta.plainSize;
  const fullChunks = Math.floor(plainSize / cs);
  const hasTail = plainSize % cs > 0;
  const totalChunks = plainSize === 0 ? 1 : fullChunks + (hasTail ? 1 : 0);

  const out = [];
  let produced = 0;
  for (let i = 0; i < totalChunks; i += 1) {
    const recordOffset = cipherOffsetOfChunk(meta, i);
    const plainLen = plainLengthOfChunk(meta, i);
    const cipherLen = LEN_PREFIX_SIZE + plainLen + TAG_SIZE;
    const record = new Uint8Array(await cipherBlob.slice(recordOffset, recordOffset + cipherLen).arrayBuffer());
    const cipher = record.subarray(LEN_PREFIX_SIZE);
    const plain = new Uint8Array(
      await crypto.subtle.decrypt({ name: "AES-GCM", iv: chunkNonce(b64urlToBytes(meta.noncePrefix), i), additionalData: chunkAad(i) }, key, cipher),
    );
    out.push(plain);
    produced += plain.length;
    if (typeof options.onProgress === "function") {
      try {
        options.onProgress(Math.round(((i + 1) / totalChunks) * 100));
      } catch {}
    }
  }
  void produced;
  return new Blob(out, { type: "application/octet-stream" });
}

/**
 * 解密密文字节区间（Range 拉取用）
 * @param {object} meta
 * @param {string|Uint8Array} keyB64
 * @param {ArrayBuffer|Uint8Array} cipherSlice 覆盖 [mapped.cipherStart, mapped.cipherEnd] 的密文
 * @param {{startChunkIndex:number, skipPlain:number, maxPlain:number}} range mapPlainRangeToCipherRange 的结果
 * @returns {Promise<Uint8Array>} 明文片段
 */
export async function decryptRangeForE2E(meta, keyB64, cipherSlice, range) {
  validateE2EMeta(meta);
  const key = await importKey(typeof keyB64 === "string" ? b64urlToBytes(keyB64) : keyB64);
  const noncePrefix = b64urlToBytes(meta.noncePrefix);
  const bytes = cipherSlice instanceof Uint8Array ? cipherSlice : new Uint8Array(cipherSlice);
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);

  const out = [];
  let emitted = 0;
  let index = range.startChunkIndex;
  let offset = 0;
  let toSkip = range.skipPlain;

  while (emitted < range.maxPlain) {
    if (offset + LEN_PREFIX_SIZE > bytes.length) break;
    const cipherLen = view.getUint32(bytes.byteOffset + offset, true);
    if (cipherLen < TAG_SIZE || cipherLen > meta.chunkSize + TAG_SIZE) throw new Error("加密数据块长度异常");
    if (offset + LEN_PREFIX_SIZE + cipherLen > bytes.length) throw new Error("加密数据不完整（块被截断）");
    const cipher = bytes.subarray(offset + LEN_PREFIX_SIZE, offset + LEN_PREFIX_SIZE + cipherLen);
    const plain = new Uint8Array(
      await crypto.subtle.decrypt({ name: "AES-GCM", iv: chunkNonce(noncePrefix, index), additionalData: chunkAad(index) }, key, cipher),
    );
    offset += LEN_PREFIX_SIZE + cipherLen;
    index += 1;

    let chunk = plain;
    if (toSkip > 0) {
      if (chunk.length <= toSkip) {
        toSkip -= chunk.length;
        continue;
      }
      chunk = chunk.subarray(toSkip);
      toSkip = 0;
    }
    if (emitted + chunk.length > range.maxPlain) {
      chunk = chunk.subarray(0, range.maxPlain - emitted);
    }
    out.push(chunk);
    emitted += chunk.length;
  }
  if (emitted !== range.maxPlain) {
    throw new Error("加密数据不完整（明文长度不足）");
  }

  const result = new Uint8Array(emitted);
  let filled = 0;
  for (const chunk of out) {
    result.set(chunk, filled);
    filled += chunk.length;
  }
  return result;
}

// ------------------------- 链接 fragment -------------------------

const FRAGMENT_KEY_PREFIX = "e2ek=";

/**
 * 从 URL fragment 读取 E2E 密钥（fragment 不会发送到服务器）
 */
export function readE2EKeyFromLocation(href = typeof window !== "undefined" ? window.location.href : "") {
  if (!href) return null;
  try {
    const hashIndex = href.indexOf("#");
    if (hashIndex < 0) return null;
    const fragment = href.slice(hashIndex + 1);
    const params = new URLSearchParams(fragment);
    const key = params.get("e2ek");
    return key ? key : null;
  } catch {
    return null;
  }
}

/**
 * 把 E2E 密钥附加到分享链接（fragment 形式，#e2ek=...）
 */
export function appendE2EKeyToUrl(url, keyB64) {
  if (!url || !keyB64) return url || "";
  try {
    const clean = url.split("#")[0];
    return `${clean}#${FRAGMENT_KEY_PREFIX}${keyB64}`;
  } catch {
    return url;
  }
}
