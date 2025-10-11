/**
 * 增强的MIME类型检测工具
 * 文件扩展名优先，魔术字节补充，适配url解析mime的
 */

import { fileTypeFromBuffer } from "file-type";
import { getEffectiveMimeType } from "./fileUtils.js";

/**
 * 从URL获取文件内容并检测MIME类型
 * @param {string} url - 文件URL
 * @param {string} realFilename - 真实文件名（从数据库获取）
 * @param {Object} options - 选项
 * @returns {Promise<Object>} 检测结果
 */
export async function detectMimeTypeFromUrl(url, realFilename = null, options = {}) {
  const { maxBytes = 4096, timeout = 10000, useFileType = true, abortSignal = null } = options;

  const result = {
    url,
    realFilename,
    detectedMimeType: null,
    filenameMimeType: null,
    finalMimeType: null,
    detectionMethod: null,
    confidence: 0,
    error: null,
  };

  try {
    // 优先使用真实文件名推断MIME类型
    if (realFilename) {
      result.filenameMimeType = getEffectiveMimeType(null, realFilename);
      result.finalMimeType = result.filenameMimeType;
      result.detectionMethod = "filename";
      result.confidence = 0.9;
    } else {
      // 回退到从URL路径提取文件名
      const urlObj = new URL(url);
      const pathname = urlObj.pathname;
      const filename = pathname.split("/").pop() || "";
      if (filename) {
        result.filenameMimeType = getEffectiveMimeType(null, filename);
        result.finalMimeType = result.filenameMimeType;
        result.detectionMethod = "url-filename";
        result.confidence = 0.7;
      }
    }

    // 如果文件名检测失败或需要验证，使用file-type库
    if (useFileType && (!result.finalMimeType || result.finalMimeType === "application/octet-stream")) {
      try {
        const response = await fetch(url, {
          method: "GET",
          headers: {
            Range: `bytes=0-${maxBytes - 1}`,
            "User-Agent": "CloudPaste-Backend/1.0",
          },
          signal: abortSignal || AbortSignal.timeout(timeout),
        });

        if (response.ok || response.status === 206) {
          const arrayBuffer = await response.arrayBuffer();
          const fileTypeResult = await fileTypeFromBuffer(arrayBuffer);

          if (fileTypeResult) {
            result.detectedMimeType = fileTypeResult.mime;
            result.finalMimeType = fileTypeResult.mime;
            result.detectionMethod = "file-type";
            result.confidence = 0.85;
          }
        }
      } catch (fetchError) {
        // file-type检测失败，保持文件名检测结果
      }
    }

    // 如果没有任何检测结果，使用默认类型
    if (!result.finalMimeType) {
      result.finalMimeType = "application/octet-stream";
      result.detectionMethod = "default";
      result.confidence = 0.1;
    }
  } catch (error) {
    result.error = error.message;
    result.finalMimeType = "application/octet-stream";
    result.detectionMethod = "error";
    result.confidence = 0.1;
  }

  return result;
}

/**
 * 增强的URL元数据获取函数
 * @param {string} url - 文件URL
 * @param {string} realFilename - 真实文件名（可选）
 * @param {Object} options - 选项
 * @returns {Promise<Object>} 元数据对象
 */
export async function getEnhancedUrlMetadata(url, realFilename = null, options = {}) {
  const { timeout = 10000, enableMimeDetection = true, abortSignal = null } = options;

  const metadata = {
    url,
    filename: "",
    contentType: "application/octet-stream",
    enhancedContentType: null,
    size: null,
    lastModified: null,
    corsSupported: false,
    detectionMethod: [],
    detectionConfidence: 0,
    fileTypeLibraryUsed: false,
    error: null,
  };

  try {
    // 设置文件名
    if (realFilename) {
      metadata.filename = realFilename;
    } else {
      const urlObj = new URL(url);
      const pathname = urlObj.pathname;
      metadata.filename = pathname.split("/").pop() || `file_${Date.now()}`;
    }

    // 获取基本元数据（HEAD请求）
    let headSuccess = false;
    try {
      const headResponse = await fetch(url, {
        method: "HEAD",
        headers: {
          "User-Agent": "CloudPaste-Backend/1.0",
        },
        signal: abortSignal || AbortSignal.timeout(timeout),
      });

      if (headResponse.ok) {
        metadata.contentType = headResponse.headers.get("content-type") || "application/octet-stream";
        const contentLength = headResponse.headers.get("content-length");
        metadata.size = contentLength ? parseInt(contentLength) : null;
        metadata.lastModified = headResponse.headers.get("last-modified") || null;
        metadata.corsSupported = true;
        headSuccess = true;
      }
    } catch (headError) {
      // HEAD请求失败，继续尝试其他方法
    }

    // 如果HEAD请求失败或没有获取到文件大小，尝试Range请求
    if (!headSuccess || metadata.size === null) {
      try {
        const rangeResponse = await fetch(url, {
          method: "GET",
          headers: {
            Range: "bytes=0-0", // 只请求第一个字节
            "User-Agent": "CloudPaste-Backend/1.0",
          },
          signal: abortSignal || AbortSignal.timeout(timeout),
        });

        if (rangeResponse.status === 206) {
          // 支持Range请求，从Content-Range头获取总大小
          const contentRange = rangeResponse.headers.get("content-range");
          if (contentRange) {
            // Content-Range: bytes 0-0/1234567
            const match = contentRange.match(/bytes \d+-\d+\/(\d+)/);
            if (match) {
              metadata.size = parseInt(match[1]);
            }
          }
        } else if (rangeResponse.ok) {
          // 不支持Range请求，但请求成功，尝试从Content-Length获取
          const contentLength = rangeResponse.headers.get("content-length");
          if (contentLength) {
            metadata.size = parseInt(contentLength);
          }

          // 如果还没有获取到Content-Type，从这个响应中获取
          if (!metadata.contentType || metadata.contentType === "application/octet-stream") {
            metadata.contentType = rangeResponse.headers.get("content-type") || "application/octet-stream";
          }
        }
      } catch (rangeError) {
        // Range请求也失败，尝试最后的GET请求（只获取前几KB）
        try {
          const getResponse = await fetch(url, {
            method: "GET",
            headers: {
              Range: "bytes=0-4095", // 获取前4KB
              "User-Agent": "CloudPaste-Backend/1.0",
            },
            signal: abortSignal || AbortSignal.timeout(timeout),
          });

          if (getResponse.ok || getResponse.status === 206) {
            // 从Content-Length或Content-Range获取大小信息
            if (getResponse.status === 206) {
              const contentRange = getResponse.headers.get("content-range");
              if (contentRange) {
                const match = contentRange.match(/bytes \d+-\d+\/(\d+)/);
                if (match) {
                  metadata.size = parseInt(match[1]);
                }
              }
            } else {
              const contentLength = getResponse.headers.get("content-length");
              if (contentLength) {
                metadata.size = parseInt(contentLength);
              }
            }

            // 获取Content-Type
            if (!metadata.contentType || metadata.contentType === "application/octet-stream") {
              metadata.contentType = getResponse.headers.get("content-type") || "application/octet-stream";
            }
          }
        } catch (getError) {
          // 所有方法都失败了，保持默认值
        }
      }
    }

    // MIME类型检测
    if (enableMimeDetection) {
      const mimeResult = await detectMimeTypeFromUrl(url, metadata.filename, options);

      if (mimeResult.finalMimeType && mimeResult.finalMimeType !== "application/octet-stream") {
        metadata.enhancedContentType = mimeResult.finalMimeType;
        metadata.detectionMethod = [mimeResult.detectionMethod];
        metadata.detectionConfidence = mimeResult.confidence;
        metadata.fileTypeLibraryUsed = mimeResult.detectionMethod?.includes("file-type");
      }
    }

    // 如果没有检测结果，使用文件名推断
    if (!metadata.enhancedContentType) {
      metadata.enhancedContentType = getEffectiveMimeType(null, metadata.filename);
      metadata.detectionMethod = ["filename-fallback"];
      metadata.detectionConfidence = 0.6;
    }
  } catch (error) {
    metadata.error = error.message;
  }

  return metadata;
}
