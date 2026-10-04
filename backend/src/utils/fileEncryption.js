/**
 * 文件传输加密核心（分块 AES-256-GCM）
 *
 * 格式（CPENC1，前端 E2E 模块共用同一 spec）：
 *   Header: 8B magic "CPENC1\0\0" + 4B LE chunkSize + 8B 随机 noncePrefix
 *   Chunk:  4B LE 密文长度 + 密文（明文块 + 16B GCM tag）
 *   块 nonce = noncePrefix(8B) || 4B BE 块序号（从 0 开始）
 *   AAD     = "CPENC1:" + 块序号（防块重排/截断）
 *
 * 密钥模型：
 *   - 每文件随机 32B DEK（数据加密密钥）
 *   - server 模式：DEK 用 KEK（HKDF-SHA256 派生自 ENCRYPTION_SECRET）AES-GCM 包裹后随 meta 落库，
 *     更换 ENCRYPTION_SECRET 只需重新 wrap，无需重加密文件
 *   - e2e 模式：DEK 仅存在于分享链接 fragment，服务端零密钥
 *
 * 仅依赖 Web Crypto API（Workers / Node 18+ 均可用），不依赖 Node crypto。
 */

import { ValidationError } from "../http/errors.js";

const MAGIC_BYTES = new Uint8Array([0x43, 0x50, 0x45, 0x4e, 0x43, 0x31, 0x00, 0x00]); // "CPENC1\0\0"
const HEADER_SIZE = 8 + 4 + 8;
const LEN_PREFIX_SIZE = 4;
const TAG_SIZE = 16;
export const DEFAULT_CHUNK_SIZE = 4 * 1024 * 1024;
export const MAX_CHUNK_SIZE = 64 * 1024 * 1024;
const MIN_CHUNK_SIZE = 64 * 1024;

const ALGO = "A256GCM";
const META_VERSION = 1;
const KEY_ID = "v1";
const AAD_PREFIX = "CPENC1:";
const KEK_INFO = "cloudpaste-file-kek-v1";

const encoder = new TextEncoder();

// ------------------------- base64url（无填充） -------------------------

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

function isBytes(value) {
  return value instanceof Uint8Array || (typeof Buffer !== "undefined" && Buffer.isBuffer(value));
}

function asBytes(value) {
  return isBytes(value) ? value : new Uint8Array(value);
}

// ------------------------- 密钥派生与包裹 -------------------------

async function deriveKek(encryptionSecret) {
  const ikm = encoder.encode(String(encryptionSecret));
  const baseKey = await crypto.subtle.importKey("raw", ikm, "HKDF", false, ["deriveBits"]);
  const bits = await crypto.subtle.deriveBits(
    {
      name: "HKDF",
      hash: "SHA-256",
      salt: encoder.encode("cloudpaste-file-encryption"),
      info: encoder.encode(KEK_INFO),
    },
    baseKey,
    256,
  );
  return crypto.subtle.importKey("raw", bits, { name: "AES-GCM" }, false, ["encrypt", "decrypt"]);
}

async function importAesGcmKey(rawBytes) {
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
  return encoder.encode(AAD_PREFIX + index);
}

/**
 * 生成随机 DEK 并用 KEK 包裹
 * wrappedKey 布局：8B 随机 IV 前缀 + 密文（DEK 32B + 16B tag），nonce = IV前缀 || 0x00000000
 * @returns {Promise<{rawDek: Uint8Array, wrappedKey: Uint8Array}>}
 */
export async function generateWrappedDek(fileId, encryptionSecret) {
  const rawDek = crypto.getRandomValues(new Uint8Array(32));
  const kek = await deriveKek(encryptionSecret);
  const ivPrefix = crypto.getRandomValues(new Uint8Array(8));
  const cipher = new Uint8Array(
    await crypto.subtle.encrypt(
      { name: "AES-GCM", iv: chunkNonce(ivPrefix, 0), additionalData: encoder.encode(AAD_PREFIX + "key:" + fileId) },
      kek,
      rawDek,
    ),
  );
  const wrappedKey = new Uint8Array(8 + cipher.length);
  wrappedKey.set(ivPrefix, 0);
  wrappedKey.set(cipher, 8);
  return { rawDek, wrappedKey };
}

/**
 * 解包 DEK
 */
export async function unwrapDek(fileId, encryptionSecret, wrappedKey) {
  const wrapped = asBytes(wrappedKey);
  if (wrapped.length <= 8) {
    throw new ValidationError("无效的密钥包裹数据");
  }
  const kek = await deriveKek(encryptionSecret);
  const raw = await crypto.subtle.decrypt(
    {
      name: "AES-GCM",
      iv: chunkNonce(wrapped.subarray(0, 8), 0),
      additionalData: encoder.encode(AAD_PREFIX + "key:" + fileId),
    },
    kek,
    wrapped.subarray(8),
  );
  return new Uint8Array(raw);
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
 * 构造新的服务端加密会话（生成 DEK / noncePrefix / meta）
 * @param {string} keyScopeId 密钥绑定 ID（写入 meta.kid，解密时用于 AAD 校验）
 * @returns {Promise<{rawDek: Uint8Array, meta: object}>}
 */
export async function createServerEncryptionMeta(keyScopeId, encryptionSecret, options = {}) {
  const chunkSize = normalizeChunkSize(options.chunkSize);
  const { rawDek, wrappedKey } = await generateWrappedDek(keyScopeId, encryptionSecret);
  const noncePrefix = crypto.getRandomValues(new Uint8Array(8));
  return {
    rawDek,
    meta: {
      v: META_VERSION,
      mode: "server",
      algo: ALGO,
      keyId: KEY_ID,
      kid: String(keyScopeId || ""),
      chunkSize,
      noncePrefix: bytesToB64url(noncePrefix),
      wrappedKey: bytesToB64url(wrappedKey),
      plainSize: 0,
      cipherSize: 0,
    },
  };
}

/**
 * 校验并规范化 meta（原地修补 chunkSize/plainSize/cipherSize 后返回）
 */
export function validateEncryptionMeta(meta) {
  if (!meta || typeof meta !== "object") {
    throw new ValidationError("无效的加密元数据");
  }
  if (meta.algo !== ALGO || (meta.v ?? META_VERSION) !== META_VERSION) {
    throw new ValidationError("不支持的加密格式版本");
  }
  if (meta.mode !== "server" && meta.mode !== "e2e") {
    throw new ValidationError("不支持的加密模式");
  }
  let noncePrefix;
  try {
    noncePrefix = b64urlToBytes(meta.noncePrefix);
  } catch {
    throw new ValidationError("无效的加密 nonce");
  }
  if (noncePrefix.length !== 8) {
    throw new ValidationError("无效的加密 nonce");
  }
  if (meta.mode === "server") {
    let wrapped;
    try {
      wrapped = b64urlToBytes(meta.wrappedKey);
    } catch {
      throw new ValidationError("缺少加密密钥材料");
    }
    if (wrapped.length <= 8) {
      throw new ValidationError("缺少加密密钥材料");
    }
  }
  meta.chunkSize = normalizeChunkSize(meta.chunkSize);
  meta.plainSize = Number(meta.plainSize) || 0;
  meta.cipherSize = Number(meta.cipherSize) || 0;
  return meta;
}

/** 解析存储中的 meta JSON（null/空 均返回 null） */
export function parseEncryptionMeta(raw) {
  if (!raw) return null;
  if (typeof raw === "object") return raw;
  try {
    const parsed = JSON.parse(raw);
    return parsed && typeof parsed === "object" ? parsed : null;
  } catch {
    return null;
  }
}

/** 计算明文 plainSize 对应的密文总长度 */
export function computeCipherSize(plainSize, chunkSize = DEFAULT_CHUNK_SIZE) {
  const cs = normalizeChunkSize(chunkSize);
  const size = Number(plainSize) || 0;
  if (size <= 0) return HEADER_SIZE + LEN_PREFIX_SIZE + TAG_SIZE; // 空文件：单个 0 明文块
  const fullChunks = Math.floor(size / cs);
  const remainder = size % cs;
  let total = HEADER_SIZE + fullChunks * (LEN_PREFIX_SIZE + cs + TAG_SIZE);
  if (remainder > 0) total += LEN_PREFIX_SIZE + remainder + TAG_SIZE;
  return total;
}

/** 第 i 块的明文长度 */
export function plainLengthOfChunk(meta, i) {
  const cs = meta.chunkSize;
  const fullChunks = Math.floor(meta.plainSize / cs);
  const hasTail = meta.plainSize % cs > 0;
  if (i < fullChunks) return cs;
  if (i === fullChunks && hasTail) return meta.plainSize - fullChunks * cs;
  // plainSize 为 0 时只有第 0 块（0 明文块）
  if (meta.plainSize === 0 && i === 0) return 0;
  return cs; // 最后一块恰为整块
}

/** 第 i 块的密文起始偏移 */
export function cipherOffsetOfChunk(meta, i) {
  const cs = meta.chunkSize;
  const fullChunks = Math.floor(meta.plainSize / cs);
  if (meta.plainSize === 0) return HEADER_SIZE;
  if (i < fullChunks) return HEADER_SIZE + i * (LEN_PREFIX_SIZE + cs + TAG_SIZE);
  return HEADER_SIZE + fullChunks * (LEN_PREFIX_SIZE + cs + TAG_SIZE);
}

// ------------------------- 加密 -------------------------

class ChunkSealer {
  constructor(key, chunkSize, noncePrefix) {
    this.key = key;
    this.chunkSize = chunkSize;
    this.noncePrefix = noncePrefix;
    this.index = 0;
    this.buffer = new Uint8Array(chunkSize);
    this.buffered = 0;
    this.cipherSize = HEADER_SIZE;
    this.plainSize = 0;
  }

  async sealChunk(plaintext) {
    const nonce = chunkNonce(this.noncePrefix, this.index);
    const cipher = new Uint8Array(
      await crypto.subtle.encrypt({ name: "AES-GCM", iv: nonce, additionalData: chunkAad(this.index) }, this.key, plaintext),
    );
    const record = new Uint8Array(LEN_PREFIX_SIZE + cipher.length);
    new DataView(record.buffer).setUint32(0, cipher.length, true);
    record.set(cipher, LEN_PREFIX_SIZE);
    this.index += 1;
    this.cipherSize += record.length;
    return record;
  }

  /** 缓冲任意长度输入，产出完整块的密文记录 */
  async push(chunk) {
    const records = [];
    const bytes = asBytes(chunk);
    this.plainSize += bytes.length;
    let offset = 0;
    while (offset < bytes.length) {
      const take = Math.min(this.chunkSize - this.buffered, bytes.length - offset);
      this.buffer.set(bytes.subarray(offset, offset + take), this.buffered);
      this.buffered += take;
      offset += take;
      if (this.buffered === this.chunkSize) {
        records.push(await this.sealChunk(this.buffer));
        this.buffered = 0;
        this.buffer = new Uint8Array(this.chunkSize);
      }
    }
    return records;
  }

  /** 结束：封口剩余缓冲；空文件产生单个 0 明文块，整块对齐时不再追加空块 */
  async finish() {
    if (this.buffered > 0 || this.index === 0) {
      const record = await this.sealChunk(this.buffer.subarray(0, this.buffered));
      this.buffered = 0;
      return record;
    }
    return null;
  }
}

/**
 * 创建服务端加密转换流
 * @param {string} keyScopeId 密钥绑定 ID（存入 meta.kid）
 * @param {string} encryptionSecret
 * @param {{chunkSize?: number, rawDek?: Uint8Array, meta?: object}} options
 *   传入 rawDek + meta 时直接复用（E2E 前端测试/密钥注入场景），否则自动生成
 * @returns {Promise<{transform: TransformStream, meta: object, getCipherSize: () => number}>}
 */
export async function createEncryptingTransform(keyScopeId, encryptionSecret, options = {}) {
  let rawDek;
  let meta;
  if (options.rawDek && options.meta) {
    rawDek = options.rawDek;
    meta = options.meta;
  } else {
    ({ rawDek, meta } = await createServerEncryptionMeta(keyScopeId, encryptionSecret, options));
  }
  const key = await importAesGcmKey(rawDek);
  const sealer = new ChunkSealer(key, meta.chunkSize, b64urlToBytes(meta.noncePrefix));
  const noncePrefix = b64urlToBytes(meta.noncePrefix);

  const buildHeader = () => {
    const header = new Uint8Array(HEADER_SIZE);
    header.set(MAGIC_BYTES, 0);
    new DataView(header.buffer).setUint32(8, meta.chunkSize, true);
    header.set(noncePrefix, 12);
    return header;
  };

  const transform = new TransformStream({
    start(controller) {
      controller.enqueue(buildHeader());
    },
    async transform(chunk, controller) {
      const records = await sealer.push(chunk);
      for (const record of records) controller.enqueue(record);
    },
    async flush(controller) {
      const record = await sealer.finish();
      if (record) controller.enqueue(record);
    },
  });

  return {
    transform,
    meta,
    getCipherSize: () => sealer.cipherSize,
  };
}

// ------------------------- 解密 -------------------------

/**
 * 缓冲式解密状态机：喂入任意长度密文字节，产出明文
 * 支持：从第 startChunkIndex 块开始、跳过块内前 skipPlain 字节、最多输出 maxPlain 字节
 */
class DecryptStateMachine {
  constructor(key, meta, { startChunkIndex = 0, skipPlain = 0, maxPlain = Infinity, headerless = false } = {}) {
    this.key = key;
    this.meta = meta;
    this.noncePrefix = b64urlToBytes(meta.noncePrefix);
    this.nextChunkIndex = startChunkIndex;
    this.toSkip = skipPlain;
    this.maxPlain = maxPlain;
    this.emitted = 0;
    this.headerDone = headerless; // Range 块切片不含文件头
    this.exhausted = false;
    this.chunks = [];
    this.length = 0;
  }

  _append(bytes) {
    const copy = new Uint8Array(asBytes(bytes));
    this.chunks.push(copy);
    this.length += copy.length;
  }

  _consume(n) {
    if (n > this.length) return null;
    const out = new Uint8Array(n);
    let filled = 0;
    while (filled < n) {
      const head = this.chunks[0];
      const take = Math.min(n - filled, head.length);
      out.set(head.subarray(0, take), filled);
      filled += take;
      if (take === head.length) {
        this.chunks.shift();
      } else {
        this.chunks[0] = head.subarray(take);
      }
      this.length -= take;
    }
    return out;
  }

  /** 喂入密文，返回可产出的明文块数组 */
  async feed(bytes) {
    if (this.exhausted) return [];
    this._append(bytes);
    const output = [];

    if (!this.headerDone) {
      if (this.length < HEADER_SIZE) return output;
      const header = this._consume(HEADER_SIZE);
      if (!header.subarray(0, 8).every((b, i) => b === MAGIC_BYTES[i])) {
        throw new ValidationError("加密数据格式错误（magic 不匹配）");
      }
      const headerChunkSize = new DataView(header.buffer).getUint32(8, true);
      if (headerChunkSize !== this.meta.chunkSize) {
        throw new ValidationError("加密数据块大小与元数据不一致");
      }
      this.headerDone = true;
    }

    for (;;) {
      if (this.emitted >= this.maxPlain) {
        this.exhausted = true;
        break;
      }
      const expectedPlain = this.meta.plainSize > 0 ? this.meta.plainSize : null;
      if (expectedPlain !== null) {
        const producedBefore = this.nextChunkIndex * this.meta.chunkSize;
        const remainingPlain = Math.max(0, expectedPlain - producedBefore);
        if (remainingPlain === 0) {
          this.exhausted = true;
          break;
        }
      }
      if (this.length < LEN_PREFIX_SIZE) break;
      // 预读长度前缀后放回，等待整块密文到齐再消费
      const peek = this._consume(LEN_PREFIX_SIZE);
      const cipherLen = new DataView(peek.buffer, peek.byteOffset, 4).getUint32(0, true);
      this.chunks.unshift(peek);
      this.length += LEN_PREFIX_SIZE;
      if (cipherLen < TAG_SIZE || cipherLen > this.meta.chunkSize + TAG_SIZE) {
        throw new ValidationError("加密数据块长度异常");
      }
      if (this.length < LEN_PREFIX_SIZE + cipherLen) break;

      this._consume(LEN_PREFIX_SIZE);
      const cipher = this._consume(cipherLen);
      if (!cipher) break;
      const plain = new Uint8Array(
        await crypto.subtle.decrypt(
          { name: "AES-GCM", iv: chunkNonce(this.noncePrefix, this.nextChunkIndex), additionalData: chunkAad(this.nextChunkIndex) },
          this.key,
          cipher,
        ),
      );
      this.nextChunkIndex += 1;

      let out = plain;
      if (this.toSkip > 0) {
        if (out.length <= this.toSkip) {
          this.toSkip -= out.length;
          continue;
        }
        out = out.subarray(this.toSkip);
        this.toSkip = 0;
      }
      if (out.length > 0) {
        if (this.emitted + out.length > this.maxPlain) {
          out = out.subarray(0, this.maxPlain - this.emitted);
        }
        output.push(out);
        this.emitted += out.length;
        if (this.emitted >= this.maxPlain) {
          this.exhausted = true;
          break;
        }
      }
    }
    return output;
  }

  /** 流结束校验 */
  finish() {
    if (this.exhausted) return;
    if (!this.headerDone && (this.meta.plainSize > 0 || this.length > 0)) {
      throw new ValidationError("加密数据不完整（缺少头部）");
    }
    if (this.meta.plainSize > 0) {
      const producedBefore = this.nextChunkIndex * this.meta.chunkSize;
      if (producedBefore < this.meta.plainSize) {
        throw new ValidationError("加密数据不完整（流提前结束）");
      }
    }
  }
}

/**
 * 创建解密转换流
 * @param {object} meta files.encryption_meta（对象或 JSON 字符串）
 * @param {Uint8Array|string|{encryptionSecret:string, fileId?:string}} keyMaterial
 *   e2e 模式传原始 DEK（Uint8Array 或 base64url 字符串）；
 *   server 模式传 {encryptionSecret}（解包作用域取 meta.kid，兼容显式传 fileId 覆盖）
 * @param {{startChunkIndex?: number, skipPlain?: number, maxPlain?: number, headerless?: boolean}} range
 *   headerless=true 用于块对齐的密文切片（Range 下载），切片直接从块记录开始
 */
export async function createDecryptingTransform(metaInput, keyMaterial, range = {}) {
  const meta = validateEncryptionMeta(typeof metaInput === "string" ? parseEncryptionMeta(metaInput) : metaInput);
  let key;
  if (meta.mode === "server") {
    if (!keyMaterial || typeof keyMaterial !== "object" || !keyMaterial.encryptionSecret) {
      throw new ValidationError("服务端解密需要 encryptionSecret");
    }
    const scopeId = keyMaterial.fileId || meta.kid;
    if (!scopeId) {
      throw new ValidationError("缺少密钥绑定 ID（kid）");
    }
    const rawDek = await unwrapDek(scopeId, keyMaterial.encryptionSecret, b64urlToBytes(meta.wrappedKey));
    key = await importAesGcmKey(rawDek);
  } else {
    const raw = isBytes(keyMaterial) ? keyMaterial : b64urlToBytes(String(keyMaterial));
    if (raw.length !== 32) {
      throw new ValidationError("无效的端到端加密密钥");
    }
    key = await importAesGcmKey(raw);
  }

  const state = new DecryptStateMachine(key, meta, {
    startChunkIndex: range.startChunkIndex ?? 0,
    skipPlain: range.skipPlain ?? 0,
    maxPlain: range.maxPlain ?? Infinity,
    headerless: range.headerless ?? false,
  });

  return new TransformStream({
    async transform(chunk, controller) {
      const outputs = await state.feed(chunk);
      for (const out of outputs) controller.enqueue(out);
    },
    flush() {
      state.finish();
    },
  });
}

/**
 * 用明文字节区间映射到密文字节区间（块对齐）
 * @param {object} meta
 * @param {number} start 明文起始（含）
 * @param {number} end 明文结束（含）
 * @returns {{firstChunk:number, lastChunk:number, cipherStart:number, cipherEnd:number,
 *            skipPlain:number, maxPlain:number}}
 */
export function mapPlainRangeToCipherRange(metaInput, start, end) {
  const meta = validateEncryptionMeta(typeof metaInput === "string" ? parseEncryptionMeta(metaInput) : metaInput);
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

  return {
    firstChunk,
    lastChunk,
    cipherStart,
    cipherEnd,
    skipPlain: s - firstChunk * cs,
    maxPlain: e - s + 1,
  };
}

// ------------------------- 访问令牌 / 常量时间比较 -------------------------

/**
 * 常量时间字符串比较（长度不同时仍完整遍历，避免时序泄露）
 */
export function timingSafeEqualStrings(a, b) {
  const sa = String(a ?? "");
  const sb = String(b ?? "");
  const maxLen = Math.max(sa.length, sb.length);
  let diff = sa.length ^ sb.length;
  for (let i = 0; i < maxLen; i += 1) {
    diff |= (sa.charCodeAt(i) || 0) ^ (sb.charCodeAt(i) || 0);
  }
  return diff === 0;
}
