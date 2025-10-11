/**
 * 环境检测和配置工具
 * 用于根据部署环境（Cloudflare Worker vs Docker/Server）提供最优配置
 */

/**
 * 检测当前是否运行在Cloudflare Worker环境
 * @returns {boolean} 是否为Worker环境
 */
export function isCloudflareWorkerEnvironment() {
  // Wrangler 开发环境检测
  // 检测 Wrangler 特征：有 caches 和 Response，但仍有 process（wrangler dev --local）
  if (typeof caches !== "undefined" && typeof Response !== "undefined" && typeof window === "undefined" && typeof process !== "undefined") {
    // 进一步检测是否为 Wrangler 环境
    const hasWorkerFeatures = typeof navigator !== "undefined" && typeof globalThis !== "undefined";
    if (hasWorkerFeatures) {
      return true;
    }
  }

  // 标准 Cloudflare Workers 检测
  return typeof caches !== "undefined" && typeof Response !== "undefined" && typeof process === "undefined" && typeof window === "undefined";
}

/**
 * 获取环境自适应的上传配置
 * @returns {Object} 上传配置对象
 */
export function getEnvironmentOptimizedUploadConfig() {
  const isWorker = isCloudflareWorkerEnvironment();

  return isWorker
    ? {
        partSize: 6 * 1024 * 1024, // 6MB - Worker环境内存限制
        queueSize: 1, // 1并发 - 避免CPU时间超限
        environment: "Cloudflare Worker",
        maxConcurrency: 1, // 最大并发数
        bufferSize: 6 * 1024 * 1024, // 缓冲区大小
      }
    : {
        partSize: 8 * 1024 * 1024, // 8MB - Docker环境更大分片
        queueSize: 4, // 4并发
        environment: "Docker/Server",
        maxConcurrency: 4, // 最大并发数
        bufferSize: 32 * 1024 * 1024, // 缓冲区大小
      };
}

/**
 * 获取环境名称
 * @returns {string} 环境名称
 */
export function getEnvironmentName() {
  return isCloudflareWorkerEnvironment() ? "Cloudflare Worker" : "Docker/Server";
}

/**
 * 获取推荐的分片大小
 * @returns {number} 分片大小（字节）
 */
export function getRecommendedPartSize() {
  return isCloudflareWorkerEnvironment() ? 6 * 1024 * 1024 : 8 * 1024 * 1024;
}

/**
 * 获取推荐的并发数
 * @returns {number} 并发数
 */
export function getRecommendedConcurrency() {
  return isCloudflareWorkerEnvironment() ? 1 : 4;
}

/**
 * 检测当前是否运行在Node.js环境
 * @returns {boolean} 是否为Node.js环境
 */
export function isNodeJSEnvironment() {
  return typeof process !== "undefined" && process.versions && process.versions.node;
}

/**
 * 在Node.js环境中将Web API ReadableStream转换为Uint8Array
 * 解决AWS SDK v3在Node.js中对ReadableStream的兼容性问题
 *
 * 参考：
 * - AWS SDK Issue: https://github.com/aws/aws-sdk-js-v3/issues/6428
 * - WHATWG Streams Issue: https://github.com/whatwg/streams/issues/1019
 *
 * @param {ReadableStream|any} stream - 输入流
 * @returns {Promise<Uint8Array|any>} 转换后的Uint8Array或原始stream
 */
export async function convertStreamForAWSCompatibility(stream) {
  // 只在Node.js环境中处理Web API ReadableStream
  if (!isNodeJSEnvironment()) {
    return stream; // Cloudflare Workers环境直接返回
  }

  // 检测是否为Web API ReadableStream
  if (stream && typeof stream.getReader === "function" && typeof stream.pipeTo === "function") {
    console.log("Node.js环境检测到Web API ReadableStream，使用Response.arrayBuffer()转换");

    try {
      // 使用Response包装ReadableStream，然后调用arrayBuffer()
      const response = new Response(stream);
      const arrayBuffer = await response.arrayBuffer();
      const result = new Uint8Array(arrayBuffer);

      console.log(`Web API ReadableStream转换完成，总大小: ${(result.length / 1024 / 1024).toFixed(1)}MB`);
      return result;
    } catch (error) {
      console.error("ReadableStream转换失败，回退到手动读取:", error.message);

      // 回退到手动读取方法
      const reader = stream.getReader();
      const chunks = [];
      let totalLength = 0;

      try {
        while (true) {
          const { done, value } = await reader.read();
          if (done) break;

          chunks.push(value);
          totalLength += value.length;
        }

        // 合并所有chunks为单个Uint8Array
        const result = new Uint8Array(totalLength);
        let offset = 0;
        for (const chunk of chunks) {
          result.set(chunk, offset);
          offset += chunk.length;
        }

        console.log(`手动ReadableStream转换完成，总大小: ${(totalLength / 1024 / 1024).toFixed(1)}MB`);
        return result;
      } finally {
        reader.releaseLock();
      }
    }
  }

  // 不是Web API ReadableStream，直接返回
  return stream;
}
