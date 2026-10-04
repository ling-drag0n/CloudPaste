/**
 * fileEncryption 单元测试：分块 AES-256-GCM 加解密、Range 映射、密钥包裹、篡改检测
 * 运行：cd backend && npm test
 */

import { test } from "node:test";
import assert from "node:assert/strict";
import {
  createEncryptingTransform,
  createDecryptingTransform,
  createServerEncryptionMeta,
  mapPlainRangeToCipherRange,
  computeCipherSize,
  generateWrappedDek,
  unwrapDek,
  parseEncryptionMeta,
  timingSafeEqualStrings,
} from "../src/utils/fileEncryption.js";

const SECRET = "test-encryption-secret";
const SMALL_CHUNK = 64 * 1024; // 测试用小块（64KB，格式允许的最小块）
const FILE_ID = "file-test-1";

function makeBytes(size, seed = 0x9e) {
  const bytes = new Uint8Array(size);
  for (let i = 0; i < size; i += 1) {
    bytes[i] = (seed + i * 31 + (i >> 8) * 7) & 0xff;
  }
  return bytes;
}

/** 用加密变换把明文一次性加密为完整密文（测试辅助） */
async function encryptToCipher(fileId, plaintext, options = {}) {
  const { transform, meta, getCipherSize } = await createEncryptingTransform(fileId, SECRET, {
    chunkSize: options.chunkSize || SMALL_CHUNK,
    rawDek: options.rawDek,
    meta: options.meta,
  });
  const writer = transform.writable.getWriter();
  const reader = transform.readable.getReader();
  const chunks = [];
  const readAll = (async () => {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      chunks.push(value);
    }
  })();
  if (options.feedSize) {
    for (let i = 0; i < plaintext.length; i += options.feedSize) {
      await writer.write(plaintext.subarray(i, Math.min(i + options.feedSize, plaintext.length)));
    }
  } else {
    await writer.write(plaintext);
  }
  await writer.close();
  await readAll;
  const total = chunks.reduce((n, c) => n + c.length, 0);
  const cipher = new Uint8Array(total);
  let offset = 0;
  for (const c of chunks) {
    cipher.set(c, offset);
    offset += c.length;
  }
  meta.plainSize = plaintext.length;
  meta.cipherSize = getCipherSize();
  return { cipher, meta };
}

/** 用解密变换完整解密（测试辅助） */
async function decryptAll(meta, cipher, keyMaterial, range = {}) {
  const transform = await createDecryptingTransform(meta, keyMaterial, range);
  const reader = transform.readable.getReader();
  const writer = transform.writable.getWriter();
  const out = [];
  const collect = (async () => {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) return;
      out.push(value);
    }
  })();
  try {
    if (range.feedSize) {
      for (let i = 0; i < cipher.length; i += range.feedSize) {
        await writer.write(cipher.subarray(i, Math.min(i + range.feedSize, cipher.length)));
      }
    } else {
      await writer.write(cipher);
    }
    await writer.close();
  } catch (e) {
    // 解密失败时读取侧会以同一错误 reject，吞掉避免 unhandledRejection
    await collect.catch(() => {});
    throw e;
  }
  await collect;
  const total = out.reduce((n, c) => n + c.length, 0);
  const plain = new Uint8Array(total);
  let offset = 0;
  for (const c of out) {
    plain.set(c, offset);
    offset += c.length;
  }
  return plain;
}

function decryptServer(meta, cipher, fileId = FILE_ID, range = {}) {
  return decryptAll(meta, cipher, { fileId, encryptionSecret: SECRET }, range);
}

test("roundtrip：各种大小的明文（空、小块、块边界、跨块）", async () => {
  const sizes = [0, 1, 1023, SMALL_CHUNK - 1, SMALL_CHUNK, SMALL_CHUNK + 1, SMALL_CHUNK * 2 + 123];
  for (const size of sizes) {
    const fileId = `file-rt-${size}`;
    const plaintext = makeBytes(size);
    const { cipher, meta } = await encryptToCipher(fileId, plaintext);
    assert.equal(meta.plainSize, size);
    assert.equal(
      cipher.length,
      computeCipherSize(size, meta.chunkSize),
      `size=${size} 密文长度应匹配预计算值`,
    );
    const decrypted = await decryptServer(meta, cipher, fileId);
    assert.equal(decrypted.length, size, `size=${size} 解密长度`);
    assert.ok(decrypted.every((b, i) => b === plaintext[i]), `size=${size} 解密内容一致`);
  }
});

test("roundtrip：7 字节乱序喂入（模拟任意网络分片边界）", async () => {
  const plaintext = makeBytes(SMALL_CHUNK * 2 + 999);
  const { cipher, meta } = await encryptToCipher(FILE_ID, plaintext, { feedSize: 7 });
  const plain = await decryptServer(meta, cipher, FILE_ID, { feedSize: 7 });
  assert.equal(plain.length, plaintext.length);
  assert.ok(plain.every((b, i) => b === plaintext[i]));
});

test("密文被篡改时解密必须失败", async () => {
  const plaintext = makeBytes(SMALL_CHUNK + 5);
  const { cipher, meta } = await encryptToCipher(FILE_ID, plaintext);
  const tampered = cipher.slice();
  tampered[30] ^= 0xff; // header 20B + 长度 4B + 6B，必落在第一块密文内
  await assert.rejects(() => decryptServer(meta, tampered));
});

test("块重排（AAD 绑定块序号）解密必须失败", async () => {
  const plaintext = makeBytes(SMALL_CHUNK * 2 + 10);
  const { cipher, meta } = await encryptToCipher(FILE_ID, plaintext);
  const header = 20;
  const len1 = new DataView(cipher.buffer, header).getUint32(0, true);
  const start2 = header + 4 + len1;
  const len2 = new DataView(cipher.buffer, start2).getUint32(0, true);
  assert.equal(len1, len2, "两个整块密文长度应相同");
  const swapped = cipher.slice();
  swapped.set(cipher.subarray(header + 4, start2), start2 + 4);
  swapped.set(cipher.subarray(start2 + 4, start2 + 4 + len2), header + 4);
  await assert.rejects(() => decryptServer(meta, swapped));
});

test("Range 映射：块对齐、跨块、尾部短块、块内偏移", async () => {
  const size = SMALL_CHUNK * 3 + 500;
  const plaintext = makeBytes(size);
  const { cipher, meta } = await encryptToCipher(FILE_ID, plaintext);

  const ranges = [
    [0, 100],
    [100, SMALL_CHUNK + 5],
    [SMALL_CHUNK, SMALL_CHUNK * 2 - 1],
    [SMALL_CHUNK * 3, size - 1],
    [size - 1, size - 1],
  ];
  for (const [start, end] of ranges) {
    const mapped = mapPlainRangeToCipherRange(meta, start, end);
    assert.equal(mapped.maxPlain, end - start + 1, `range ${start}-${end} maxPlain`);
    assert.equal(mapped.skipPlain, start - mapped.firstChunk * meta.chunkSize, `range ${start}-${end} skipPlain`);
    const cipherSlice = cipher.subarray(mapped.cipherStart, mapped.cipherEnd + 1);
    const plain = await decryptServer(meta, cipherSlice, FILE_ID, {
      startChunkIndex: mapped.firstChunk,
      skipPlain: mapped.skipPlain,
      maxPlain: mapped.maxPlain,
      headerless: true,
    });
    assert.equal(plain.length, end - start + 1, `range ${start}-${end} 解密长度`);
    assert.ok(plain.every((b, i) => b === plaintext[start + i]), `range ${start}-${end} 内容一致`);
  }
});

test("wrapped DEK：包裹/解包往返，错误密钥或错误文件 ID 失败", async () => {
  const { rawDek, wrappedKey } = await generateWrappedDek(FILE_ID, SECRET);
  const unwrapped = await unwrapDek(FILE_ID, SECRET, wrappedKey);
  assert.deepEqual(Array.from(unwrapped), Array.from(rawDek));
  await assert.rejects(() => unwrapDek(FILE_ID, "wrong-secret", wrappedKey));
  await assert.rejects(() => unwrapDek("other-file", SECRET, wrappedKey));
});

test("e2e 模式：注入 rawDek 加密，无需服务端密钥即可解密", async () => {
  const { rawDek, meta } = await createServerEncryptionMeta(FILE_ID, SECRET, { chunkSize: SMALL_CHUNK });
  meta.mode = "e2e";
  delete meta.wrappedKey;

  const plaintext = makeBytes(SMALL_CHUNK + 42);
  const { cipher } = await encryptToCipher(FILE_ID, plaintext, { rawDek, meta });
  const plain = await decryptAll(meta, cipher, rawDek);
  assert.equal(plain.length, plaintext.length);
  assert.ok(plain.every((b, i) => b === plaintext[i]));
});

test("e2e 模式：缺少密钥或密钥长度错误时报错", async () => {
  const { meta } = await createServerEncryptionMeta(FILE_ID, SECRET, { chunkSize: SMALL_CHUNK });
  meta.mode = "e2e";
  delete meta.wrappedKey;
  await assert.rejects(() => createDecryptingTransform(meta, "short-key"));
  await assert.rejects(() => createDecryptingTransform(meta, null));
});

test("parseEncryptionMeta：JSON 字符串与空值", () => {
  assert.equal(parseEncryptionMeta(null), null);
  assert.equal(parseEncryptionMeta(""), null);
  const meta = { v: 1, mode: "server", algo: "A256GCM" };
  assert.deepEqual(parseEncryptionMeta(JSON.stringify(meta)), meta);
  assert.deepEqual(parseEncryptionMeta(meta), meta);
  assert.equal(parseEncryptionMeta("not-json"), null);
});

test("timingSafeEqualStrings：等值/不等值/长度差异", () => {
  assert.equal(timingSafeEqualStrings("abc", "abc"), true);
  assert.equal(timingSafeEqualStrings("abc", "abd"), false);
  assert.equal(timingSafeEqualStrings("abc", "abcd"), false);
  assert.equal(timingSafeEqualStrings("", ""), true);
  assert.equal(timingSafeEqualStrings(null, ""), true);
  assert.equal(timingSafeEqualStrings("a".repeat(1000), "a".repeat(999) + "b"), false);
});
