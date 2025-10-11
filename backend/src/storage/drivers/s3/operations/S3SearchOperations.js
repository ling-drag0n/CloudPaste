/**
 * S3搜索操作模块
 * 负责S3存储的搜索相关操作
 * 从searchService.js迁移而来，符合存储驱动架构
 */

import { HTTPException } from "hono/http-exception";
import { ApiStatus } from "../../../../constants/index.js";
import { listS3Directory } from "../../../../utils/s3Utils.js";
import { normalizeS3SubPath } from "../utils/S3PathUtils.js";
import { getEffectiveMimeType } from "../../../../utils/fileUtils.js";
import { GetFileType, getFileTypeName } from "../../../../utils/fileTypeDetector.js";
import { handleFsError } from "../../../fs/utils/ErrorHandler.js";
import { updateMountLastUsed } from "../../../fs/utils/MountResolver.js";

export class S3SearchOperations {
  /**
   * 构造函数
   * @param {S3Client} s3Client - S3客户端
   * @param {Object} config - S3配置
   * @param {string} encryptionSecret - 加密密钥
   */
  constructor(s3Client, config, encryptionSecret) {
    this.s3Client = s3Client;
    this.config = config;
    this.encryptionSecret = encryptionSecret;
  }

  /**
   * 在S3挂载点中搜索文件
   * @param {string} query - 搜索查询
   * @param {Object} options - 搜索选项
   * @param {Object} options.mount - 挂载点对象
   * @param {string} options.searchPath - 搜索路径范围
   * @param {number} options.maxResults - 最大结果数量
   * @param {D1Database} options.db - 数据库实例
   * @returns {Promise<Array>} 搜索结果数组
   */
  async searchInMount(query, options = {}) {
    return handleFsError(
      async () => {
        const { mount, searchPath, maxResults = 1000, db } = options;

        if (!mount) {
          throw new HTTPException(ApiStatus.BAD_REQUEST, { message: "挂载点信息不能为空" });
        }

        // 更新挂载点最后使用时间
        if (db && mount.id) {
          await updateMountLastUsed(db, mount.id);
        }

        // 确定搜索前缀
        let searchPrefix = "";
        if (searchPath) {
          // 计算相对于挂载点的子路径
          const mountPath = mount.mount_path.replace(/\/+$/, "") || "/";
          const normalizedSearchPath = searchPath.replace(/\/+$/, "") || "/";

          if (normalizedSearchPath.startsWith(mountPath)) {
            const subPath = normalizedSearchPath.substring(mountPath.length) || "/";
            searchPrefix = normalizeS3SubPath(subPath, true);
          }
        }

        // 构建完整的S3前缀
        const rootPrefix = this.config.root_prefix ? (this.config.root_prefix.endsWith("/") ? this.config.root_prefix : this.config.root_prefix + "/") : "";
        let fullPrefix = rootPrefix;
        if (searchPrefix && searchPrefix !== "/") {
          fullPrefix += searchPrefix;
        }

        // 递归搜索S3对象
        const results = await this.recursiveS3Search(query, fullPrefix, mount, undefined, maxResults, db);

        return results;
      },
      "S3挂载点搜索",
      "S3挂载点搜索失败"
    );
  }

  /**
   * 递归搜索S3对象
   * @param {string} query - 搜索查询
   * @param {string} prefix - 搜索前缀
   * @param {Object} mount - 挂载点对象
   * @param {string} continuationToken - 分页令牌
   * @param {number} maxResults - 最大结果数量
   * @param {D1Database} db - 数据库实例
   * @returns {Promise<Array>} 搜索结果数组
   */
  async recursiveS3Search(query, prefix, mount, continuationToken = undefined, maxResults = 1000, db = null) {
    const results = [];

    try {
      // 使用空字符串作为delimiter来获取所有对象（包括子目录中的文件）
      const listResponse = await listS3Directory(this.s3Client, this.config.bucket_name, prefix, "", continuationToken);

      if (listResponse.Contents) {
        for (const item of listResponse.Contents) {
          // 检查是否已达到最大结果数量
          if (results.length >= maxResults) {
            break;
          }

          // 检查文件名是否匹配搜索查询
          if (this.matchesSearchQuery(item.Key, query)) {
            // 构建结果对象
            const result = await this.buildSearchResult(item, mount, prefix, db);
            results.push(result);
          }
        }
      }

      // 如果有更多结果且未达到最大数量限制，继续递归搜索
      if (listResponse.IsTruncated && listResponse.NextContinuationToken && results.length < maxResults) {
        const remainingResults = maxResults - results.length;
        const moreResults = await this.recursiveS3Search(query, prefix, mount, listResponse.NextContinuationToken, remainingResults, db);
        results.push(...moreResults);
      }

      return results;
    } catch (error) {
      console.error(`S3搜索失败 - Bucket: ${this.config.bucket_name}, Prefix: ${prefix}:`, error);
      return results; // 返回已找到的结果，不抛出错误
    }
  }

  /**
   * 检查文件名是否匹配搜索查询
   * @param {string} key - S3对象键
   * @param {string} query - 搜索查询
   * @returns {boolean} 是否匹配
   */
  matchesSearchQuery(key, query) {
    const fileName = key.split("/").pop() || "";
    const normalizedQuery = query.toLowerCase();
    const normalizedFileName = fileName.toLowerCase();

    // 支持模糊匹配
    return normalizedFileName.includes(normalizedQuery);
  }

  /**
   * 构建搜索结果对象
   * @param {Object} item - S3对象
   * @param {Object} mount - 挂载点对象
   * @param {string} prefix - 搜索前缀
   * @param {D1Database} db - 数据库实例
   * @returns {Promise<Object>} 搜索结果对象
   */
  async buildSearchResult(item, mount, prefix, db) {
    const fileName = item.Key.split("/").pop() || "";

    // 计算相对于S3根前缀的路径
    let relativePath = item.Key;
    if (prefix && prefix !== "" && item.Key.startsWith(prefix)) {
      relativePath = item.Key.substring(prefix.length);
    }

    // 确保相对路径以/开头
    if (relativePath && !relativePath.startsWith("/")) {
      relativePath = "/" + relativePath;
    }

    // 构建完整的挂载路径
    const mountPath = mount.mount_path.replace(/\/+$/, "") || "/";
    const fullPath = mountPath + (relativePath || "/" + fileName);

    // 获取文件类型信息
    const fileType = await GetFileType(fileName, db);
    const fileTypeName = await getFileTypeName(fileName, db);

    return {
      name: fileName,
      path: fullPath.replace(/\/+/g, "/"), // 规范化路径
      size: item.Size,
      modified: item.LastModified,
      isDirectory: false,
      mimetype: getEffectiveMimeType(null, fileName),
      type: fileType,
      typeName: fileTypeName,
      mount_id: mount.id,
      mount_name: mount.name,
      storage_type: mount.storage_type,
      s3_key: item.Key,
    };
  }

  /**
   * 排序搜索结果
   * @param {Array} results - 搜索结果数组
   * @param {string} query - 搜索查询
   * @returns {Array} 排序后的结果数组
   */
  static sortSearchResults(results, query) {
    return results.sort((a, b) => {
      // 优先级1: 文件名完全匹配
      const aExactMatch = a.name.toLowerCase() === query.toLowerCase();
      const bExactMatch = b.name.toLowerCase() === query.toLowerCase();
      if (aExactMatch && !bExactMatch) return -1;
      if (!aExactMatch && bExactMatch) return 1;

      // 优先级2: 文件名开头匹配
      const aStartsMatch = a.name.toLowerCase().startsWith(query.toLowerCase());
      const bStartsMatch = b.name.toLowerCase().startsWith(query.toLowerCase());
      if (aStartsMatch && !bStartsMatch) return -1;
      if (!aStartsMatch && bStartsMatch) return 1;

      // 优先级3: 修改时间（最新的在前）
      return new Date(b.modified) - new Date(a.modified);
    });
  }
}
