/**
 * E2E 文件访问助手：取回密文并在浏览器内解密
 *
 * - 密钥从分享链接 fragment（#e2ek=）读取，不会发送到服务器
 * - 加密元数据来自后端公开 JSON（encryption_meta，仅含分块参数，无密钥材料）
 * - v1 采用整文件取回解密（建议 ≤200MB），媒体播放用 Blob URL 可完整拖动
 */

import { useGlobalMessage } from "@/composables/core/useGlobalMessage.js";
import { createLogger } from "@/utils/logger.js";
import {
  validateE2EMeta,
  decryptBlobForE2E,
  readE2EKeyFromLocation,
} from "./e2eCrypto.js";

const log = createLogger("E2EFileAccess");

/** 文件是否为端到端加密分享 */
export function isE2EFile(file) {
  return file?.encryption_mode === "e2e" || file?.encryption_meta?.mode === "e2e";
}

/**
 * 取回并解密 E2E 文件
 * @param {object} file 分享文件信息（需含 encryption_meta 与 downloadUrl）
 * @param {{buildDownloadUrl: (file: any) => string, onProgress?: (percent:number)=>void, signal?: AbortSignal}} options
 * @returns {Promise<Blob>} 明文 Blob
 */
export async function fetchAndDecryptE2EFile(file, options = {}) {
  if (!isE2EFile(file)) {
    throw new Error("该文件不是端到端加密分享");
  }
  const meta = file.encryption_meta;
  if (!meta) {
    throw new Error("缺少端到端加密元数据");
  }
  validateE2EMeta(meta);

  const key = readE2EKeyFromLocation();
  if (!key) {
    throw new Error("链接中缺少端到端加密密钥（#e2ek=），请使用包含密钥的完整分享链接打开");
  }

  const url = options.buildDownloadUrl(file);
  if (!url) {
    throw new Error("缺少下载地址");
  }

  const response = await fetch(url, { signal: options.signal, credentials: "same-origin" });
  if (!response.ok) {
    throw new Error(`取回密文失败（HTTP ${response.status}）`);
  }

  // 带进度的流式读取
  const cipherBlob = await readBodyWithProgress(response, meta.cipherSize, options.onProgress);
  return await decryptBlobForE2E(meta, key, cipherBlob, { onProgress: options.onProgress });
}

async function readBodyWithProgress(response, expectedSize, onProgress) {
  if (!response.body || typeof response.body.getReader !== "function") {
    return await response.blob();
  }
  const reader = response.body.getReader();
  const chunks = [];
  let received = 0;
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    chunks.push(value);
    received += value.length;
    if (typeof onProgress === "function" && expectedSize > 0) {
      try {
        onProgress(Math.min(100, Math.round((received / expectedSize) * 100)));
      } catch {}
    }
  }
  return new Blob(chunks, { type: "application/octet-stream" });
}

/**
 * 触发浏览器保存明文文件
 */
export async function downloadE2EFile(file, buildDownloadUrl) {
  const blob = await fetchAndDecryptE2EFile(file, { buildDownloadUrl });
  const fileName = file?.filename || "download";
  const url = URL.createObjectURL(blob);
  try {
    const link = document.createElement("a");
    link.href = url;
    link.download = fileName;
    document.body.appendChild(link);
    link.click();
    setTimeout(() => {
      try {
        document.body.removeChild(link);
      } catch {}
    }, 100);
  } finally {
    setTimeout(() => URL.revokeObjectURL(url), 60_000);
  }
  return blob;
}

/**
 * 解密并在新标签页预览（Blob URL 由浏览器原生渲染：图片/PDF/视频/文本均可）
 */
export async function previewE2EFile(file, buildDownloadUrl) {
  const { showError } = useGlobalMessage();
  try {
    const blob = await fetchAndDecryptE2EFile(file, { buildDownloadUrl });
    const url = URL.createObjectURL(blob);
    window.open(url, "_blank");
    // 预览标签页加载后延迟释放
    setTimeout(() => URL.revokeObjectURL(url), 5 * 60_000);
    return true;
  } catch (error) {
    log.error("端到端加密预览失败:", error);
    showError(error?.message || "端到端加密预览失败");
    return false;
  }
}
