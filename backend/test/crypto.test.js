/**
 * crypto 工具单元测试：PBKDF2 密码哈希、旧格式兼容、AES-GCM 配置加密
 */

import { test } from "node:test";
import assert from "node:assert/strict";
import { sha256 } from "hono/utils/crypto";
import {
  hashPassword,
  verifyPassword,
  encryptValue,
  decryptValue,
} from "../src/utils/crypto.js";

const SECRET = "test-secret";

test("hashPassword：PBKDF2 新格式，验证往返", async () => {
  const hash = await hashPassword("s3cret-密码");
  assert.ok(hash.startsWith("pbkdf2$"), "新密码应为 pbkdf2 格式");
  assert.equal(await verifyPassword("s3cret-密码", hash), true);
  assert.equal(await verifyPassword("wrong", hash), false);
  // 两次哈希盐不同
  const hash2 = await hashPassword("s3cret-密码");
  assert.notEqual(hash, hash2);
});

test("verifyPassword：兼容旧版 64 位 SHA-256 hex（常量时间比较）", async () => {
  const legacyHash = await sha256("legacy-password");
  assert.equal(legacyHash.length, 64);
  assert.equal(await verifyPassword("legacy-password", legacyHash), true);
  assert.equal(await verifyPassword("wrong", legacyHash), false);
});

test("verifyPassword：历史明文兜底与异常输入", async () => {
  assert.equal(await verifyPassword("plain", "plain"), true);
  assert.equal(await verifyPassword("plain", "other"), false);
  assert.equal(await verifyPassword("x", null), false);
  assert.equal(await verifyPassword("x", ""), false);
  assert.equal(await verifyPassword("x", "pbkdf2$bad$format"), false);
});

test("encryptValue/decryptValue：AES-GCM 往返，密文不含明文", async () => {
  const plain = "my-s3cret-access-key-密钥";
  const enc = await encryptValue(plain, SECRET);
  assert.ok(enc.startsWith("enc1:"), "新密文应为 enc1 格式");
  assert.ok(!enc.includes("my-s3cret"), "密文不得包含明文片段");
  assert.equal(await decryptValue(enc, SECRET), plain);
  await assert.rejects(() => decryptValue(enc, "wrong-secret"), /解密失败/);
  // 随机 IV：两次加密结果不同
  const enc2 = await encryptValue(plain, SECRET);
  assert.notEqual(enc, enc2);
});

test("decryptValue：兼容旧版 encrypted: 伪加密格式", async () => {
  const legacy = `encrypted:${btoa("sig")}:${btoa(unescape(encodeURIComponent("legacy-value")))}`;
  assert.equal(await decryptValue(legacy, SECRET), "legacy-value");
  // 明文直返
  assert.equal(await decryptValue("plain-value", SECRET), "plain-value");
  assert.equal(await decryptValue(null, SECRET), null);
});
