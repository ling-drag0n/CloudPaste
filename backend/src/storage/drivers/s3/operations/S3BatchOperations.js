/**
 * S3批量操作模块
 * 负责批量操作：批量删除、批量复制、批量移动等
 */

import { HTTPException } from "hono/http-exception";
import { ApiStatus } from "../../../../constants/index.js";
import { S3Client, DeleteObjectCommand, CopyObjectCommand, ListObjectsV2Command, HeadObjectCommand, PutObjectCommand } from "@aws-sdk/client-s3";
import { normalizeS3SubPath } from "../utils/S3PathUtils.js";
import { updateMountLastUsed } from "../../../fs/utils/MountResolver.js";
import { clearDirectoryCache } from "../../../../cache/index.js";
import { generatePresignedUrl, generatePresignedPutUrl, createS3Client, getDirectoryPresignedUrls } from "../../../../utils/s3Utils.js";
import { getMimeTypeFromFilename } from "../../../../utils/fileUtils.js";
import { findMountPointByPath } from "../../../fs/utils/MountResolver.js";
import { updateParentDirectoriesModifiedTime } from "../utils/S3DirectoryUtils.js";
import { handleFsError } from "../../../fs/utils/ErrorHandler.js";
import { normalizePath } from "../../../fs/utils/PathResolver.js";
import { shouldUseRandomSuffix, generateShortId } from "../../../../utils/common.js";

export class S3BatchOperations {
  /**
   * 构造函数
   * @param {S3Client} s3Client - S3客户端
   * @param {Object} config - S3配置
   * @param {string} encryptionSecret - 加密密钥
   * @param {D1Database} db - 数据库实例（用于读取系统设置）
   */
  constructor(s3Client, config, encryptionSecret, db = null) {
    this.s3Client = s3Client;
    this.config = config;
    this.encryptionSecret = encryptionSecret;
    this.db = db;
  }

  /**
   * 递归删除S3目录
   * @param {S3Client} s3Client - S3客户端实例
   * @param {string} bucketName - 存储桶名称
   * @param {string} prefix - 目录前缀
   * @param {string} storageConfigId - 存储配置ID
   * @returns {Promise<void>}
   */
  async deleteDirectoryRecursive(s3Client, bucketName, prefix, storageConfigId) {
    let continuationToken = undefined;

    try {
      do {
        const listParams = {
          Bucket: bucketName,
          Prefix: prefix,
          MaxKeys: 1000,
          ContinuationToken: continuationToken,
        };

        const listCommand = new ListObjectsV2Command(listParams);
        const response = await s3Client.send(listCommand);

        if (response.Contents && response.Contents.length > 0) {
          // 批量删除对象
          const deletePromises = response.Contents.map(async (item) => {
            const deleteParams = {
              Bucket: bucketName,
              Key: item.Key,
            };

            const deleteCommand = new DeleteObjectCommand(deleteParams);
            await s3Client.send(deleteCommand);

            // 文件删除完成，无需数据库操作
          });

          await Promise.all(deletePromises);
        }

        continuationToken = response.NextContinuationToken;
      } while (continuationToken);

      console.log(`成功删除目录: ${prefix}`);
    } catch (error) {
      console.error(`删除目录失败: ${error.message}`);
      throw error;
    }
  }

  /**
   * 批量删除文件或目录
   * @param {Array<string>} paths - 需要删除的路径数组
   * @param {Object} options - 选项参数
   * @returns {Promise<Object>} 删除结果
   */
  async batchRemoveItems(paths, options = {}) {
    const { db, findMountPointByPath, userIdOrInfo, userType } = options;

    // 结果统计
    const result = {
      success: 0,
      failed: [],
    };

    // 逐个删除文件
    for (let path of paths) {
      try {
        // 规范化路径
        path = normalizePath(path, path.endsWith("/"));

        // 查找挂载点
        const mountResult = await findMountPointByPath(db, path, userIdOrInfo, userType);

        if (mountResult.error) {
          result.failed.push({
            path: path,
            error: mountResult.error.message,
          });
          continue;
        }

        const { mount: itemMount, subPath } = mountResult;

        // 获取S3配置
        const s3Config = await db.prepare("SELECT * FROM s3_configs WHERE id = ?").bind(itemMount.storage_config_id).first();
        if (!s3Config) {
          result.failed.push({
            path: path,
            error: "存储配置不存在",
          });
          continue;
        }

        // 判断是目录还是文件
        const isDirectory = path.endsWith("/");

        // 规范化S3子路径
        const s3SubPath = normalizeS3SubPath(subPath, isDirectory);

        if (isDirectory) {
          // 对于目录，需要递归删除所有内容
          await this.deleteDirectoryRecursive(this.s3Client, s3Config.bucket_name, s3SubPath, itemMount.storage_config_id);
        } else {
          // 对于文件，直接删除
          const deleteParams = {
            Bucket: s3Config.bucket_name,
            Key: s3SubPath,
          };

          try {
            const deleteCommand = new DeleteObjectCommand(deleteParams);
            await this.s3Client.send(deleteCommand);
          } catch (error) {
            if (error.$metadata && error.$metadata.httpStatusCode === 404) {
              result.failed.push({
                path: path,
                error: "文件不存在",
              });
              continue;
            }
            throw error;
          }
        }

        // 更新父目录的修改时间
        const rootPrefix = s3Config.root_prefix ? (s3Config.root_prefix.endsWith("/") ? s3Config.root_prefix : s3Config.root_prefix + "/") : "";
        await updateParentDirectoriesModifiedTime(this.s3Client, s3Config.bucket_name, s3SubPath, rootPrefix, true);

        // 文件删除完成，无需数据库操作

        // 更新挂载点的最后使用时间
        await updateMountLastUsed(db, itemMount.id);

        result.success++;
      } catch (error) {
        console.error(`删除路径 ${path} 失败:`, error);
        result.failed.push({
          path: path,
          error: error.message || "删除失败",
        });
      }
    }

    return result;
  }

  /**
   * 复制单个文件或目录
   * @param {string} sourcePath - 源路径
   * @param {string} targetPath - 目标路径
   * @param {Object} options - 选项参数
   * @returns {Promise<Object>} 复制结果
   */
  async copyItem(sourcePath, targetPath, options = {}) {
    const { db, findMountPointByPath, userIdOrInfo, userType } = options;

    return handleFsError(
        async () => {
          // 规范化路径
          sourcePath = normalizePath(sourcePath, sourcePath.endsWith("/"));
          targetPath = normalizePath(targetPath, targetPath.endsWith("/"));

          // 检查路径类型 (都是文件或都是目录)
          const sourceIsDirectory = sourcePath.endsWith("/");
          let targetIsDirectory = targetPath.endsWith("/");

          // 如果源是目录但目标不是目录格式，自动添加斜杠
          if (sourceIsDirectory && !targetIsDirectory) {
            targetPath = targetPath + "/";
            targetIsDirectory = true;
          }

          // 对于文件复制，确保目标路径也是文件路径格式
          if (!sourceIsDirectory && targetIsDirectory) {
            throw new HTTPException(ApiStatus.BAD_REQUEST, { message: "复制文件时，目标路径不能是目录格式" });
          }

          // 查找源路径挂载点
          const sourceMountResult = await findMountPointByPath(db, sourcePath, userIdOrInfo, userType);
          if (sourceMountResult.error) {
            throw new HTTPException(sourceMountResult.error.status, { message: sourceMountResult.error.message });
          }

          // 查找目标路径挂载点
          const targetMountResult = await findMountPointByPath(db, targetPath, userIdOrInfo, userType);
          if (targetMountResult.error) {
            throw new HTTPException(targetMountResult.error.status, { message: targetMountResult.error.message });
          }

          const { mount: sourceMount, subPath: sourceSubPath } = sourceMountResult;
          const { mount: targetMount, subPath: targetSubPath } = targetMountResult;

          // 检查是否为跨存储复制
          if (sourceMount.storage_config_id !== targetMount.storage_config_id) {
            // 跨存储复制，返回预签名URL信息
            return await this.handleCrossStorageCopy(db, sourcePath, targetPath, userIdOrInfo, userType);
          }

          // 同存储复制
          return await this._handleSameStorageCopy(db, sourcePath, targetPath, sourceMount, targetMount, sourceSubPath, targetSubPath);
        },
        "复制项目",
        "复制项目失败"
    );
  }

  /**
   * 批量复制文件或目录
   * @param {Array<Object>} items - 要复制的项目数组
   * @param {Object} options - 选项参数
   * @returns {Promise<Object>} 复制结果
   */
  async batchCopyItems(items, options = {}) {
    const { db, findMountPointByPath, userIdOrInfo, userType } = options;

    // 结果统计
    const result = {
      success: 0,
      failed: [],
      details: [],
      crossStorageResults: [],
      hasCrossStorageOperations: false,
    };

    // 逐个处理每个复制项
    for (const item of items) {
      try {
        // 检查路径是否为空或无效
        if (!item.sourcePath || !item.targetPath) {
          const errorMessage = "源路径或目标路径不能为空";
          console.error(errorMessage, item);
          result.failed.push({
            sourcePath: item.sourcePath || "未指定",
            targetPath: item.targetPath || "未指定",
            error: errorMessage,
          });
          continue;
        }

        // 检查并修正路径格式：如果源路径是目录（以"/"结尾），确保目标路径也是目录格式
        let { sourcePath, targetPath } = item;
        const sourceIsDirectory = sourcePath.endsWith("/");

        // 如果源是目录但目标不是目录格式，自动添加斜杠
        if (sourceIsDirectory && !targetPath.endsWith("/")) {
          targetPath = targetPath + "/";
          console.log(`自动修正目录路径格式: ${item.sourcePath} -> ${targetPath}`);
        }

        // 查找源路径挂载点
        const sourceMountResult = await findMountPointByPath(db, sourcePath, userIdOrInfo, userType);
        if (sourceMountResult.error) {
          result.failed.push({
            sourcePath,
            targetPath,
            error: sourceMountResult.error.message,
          });
          continue;
        }

        // 查找目标路径挂载点
        const targetMountResult = await findMountPointByPath(db, targetPath, userIdOrInfo, userType);
        if (targetMountResult.error) {
          result.failed.push({
            sourcePath,
            targetPath,
            error: targetMountResult.error.message,
          });
          continue;
        }

        const { mount: sourceMount, subPath: sourceSubPath } = sourceMountResult;
        const { mount: targetMount, subPath: targetSubPath } = targetMountResult;

        // 检查是否为跨存储复制
        if (sourceMount.storage_config_id !== targetMount.storage_config_id) {
          // 跨存储复制，生成预签名URL
          const crossStorageResult = await this._handleCrossStorageCopy(db, sourcePath, targetPath, userIdOrInfo, userType);

          result.crossStorageResults.push(crossStorageResult);
          result.hasCrossStorageOperations = true;
          continue;
        }

        // 同存储复制
        const copyResult = await this._handleSameStorageCopy(db, sourcePath, targetPath, sourceMount, targetMount, sourceSubPath, targetSubPath);

        // 根据复制结果更新统计
        if (copyResult.status === "skipped" || copyResult.skipped === true) {
          result.skipped++;
        } else {
          result.success++;
        }
        result.details.push(copyResult);
      } catch (error) {
        console.error(`复制失败:`, error);
        result.failed.push({
          sourcePath: item.sourcePath,
          targetPath: item.targetPath,
          error: error.message || "复制失败",
        });
      }
    }

    return result;
  }

  /**
   * 处理同存储复制
   * @private
   */
  async _handleSameStorageCopy(db, sourcePath, targetPath, sourceMount, targetMount, sourceSubPath, targetSubPath) {
    // 获取源和目标的S3配置
    const sourceS3Config = await db.prepare("SELECT * FROM s3_configs WHERE id = ?").bind(sourceMount.storage_config_id).first();
    const targetS3Config = await db.prepare("SELECT * FROM s3_configs WHERE id = ?").bind(targetMount.storage_config_id).first();

    if (!sourceS3Config || !targetS3Config) {
      throw new Error("S3配置不存在");
    }

    const isDirectory = sourcePath.endsWith("/");
    const s3SourcePath = normalizeS3SubPath(sourceSubPath, isDirectory);
    const s3TargetPath = normalizeS3SubPath(targetSubPath, isDirectory);

    // 检查源路径是否存在
    try {
      const sourceExists = await this._checkS3ObjectExists(sourceS3Config.bucket_name, s3SourcePath);
      if (!sourceExists) {
        // 如果是目录，尝试列出目录内容确认存在性
        if (isDirectory) {
          const listResponse = await this._listS3Directory(sourceS3Config.bucket_name, s3SourcePath);

          // 如果没有内容，说明目录不存在或为空
          if (!listResponse.Contents || listResponse.Contents.length === 0) {
            throw new HTTPException(ApiStatus.NOT_FOUND, { message: "源路径不存在或为空目录" });
          }
        } else {
          throw new HTTPException(ApiStatus.NOT_FOUND, { message: "源文件不存在" });
        }
      }
    } catch (error) {
      if (error instanceof HTTPException) {
        throw error;
      }
      throw new HTTPException(ApiStatus.INTERNAL_ERROR, { message: "检查源路径存在性失败: " + error.message });
    }

    if (isDirectory) {
      // 目录复制
      return await this._copyDirectory(sourceS3Config, s3SourcePath, s3TargetPath, sourcePath, targetPath, db);
    } else {
      // 文件复制
      return await this._copyFile(sourceS3Config, s3SourcePath, s3TargetPath, sourcePath, targetPath, db);
    }
  }

  /**
   * 复制单个文件
   * @private
   */
  async _copyFile(s3Config, s3SourcePath, s3TargetPath, sourcePath, targetPath, db = null) {
    // 检查目标文件是否已存在，如果存在则跳过
    if (await this._checkItemExists(s3Config.bucket_name, s3TargetPath)) {
      console.log(`[S3BatchOps] 同存储复制目标文件已存在，跳过: ${sourcePath} -> ${targetPath}`);
      return {
        source: sourcePath,
        target: targetPath,
        status: "skipped",
        skipped: true,
        message: "文件已存在，跳过复制",
      };
    }

    // 检查目标父目录是否存在（对于文件复制）
    if (s3TargetPath.includes("/")) {
      // 对于文件，获取其所在目录
      const parentPath = s3TargetPath.substring(0, s3TargetPath.lastIndexOf("/") + 1);

      // 添加验证：确保parentPath不为空
      if (parentPath && parentPath.trim() !== "") {
        const parentExists = await this._checkDirectoryExists(s3Config.bucket_name, parentPath);

        if (!parentExists) {
          // 自动创建父目录而不是抛出错误
          console.log(`复制操作: 正在创建目标父目录 "${parentPath}"`);

          try {
            // 创建一个空对象作为目录标记
            const createDirParams = {
              Bucket: s3Config.bucket_name,
              Key: parentPath,
              Body: Buffer.from("", "utf-8"),
              ContentType: "application/x-directory", // 目录内容类型
            };

            const createDirCommand = new PutObjectCommand(createDirParams);
            await this.s3Client.send(createDirCommand);
          } catch (dirError) {
            console.error(`复制操作: 创建目标父目录 "${parentPath}" 失败:`, dirError);
            // 如果创建目录失败，才抛出错误
            throw new HTTPException(ApiStatus.CONFLICT, { message: `无法创建目标父目录: ${dirError.message}` });
          }
        }
      }
    }

    // 执行复制
    const copyParams = {
      Bucket: s3Config.bucket_name,
      CopySource: encodeURIComponent(s3Config.bucket_name + "/" + s3SourcePath),
      Key: s3TargetPath,
    };

    const copyCommand = new CopyObjectCommand(copyParams);
    await this.s3Client.send(copyCommand);

    // 更新父目录的修改时间
    const rootPrefix = s3Config.root_prefix ? (s3Config.root_prefix.endsWith("/") ? s3Config.root_prefix : s3Config.root_prefix + "/") : "";
    await updateParentDirectoriesModifiedTime(this.s3Client, s3Config.bucket_name, s3TargetPath, rootPrefix);

    return {
      source: sourcePath,
      target: targetPath,
      status: "success",
      message: "文件复制成功",
    };
  }

  /**
   * 递归复制S3目录
   * @param {S3Client} s3Client - S3客户端实例
   * @param {string} bucketName - 存储桶名称
   * @param {string} sourcePrefix - 源目录前缀
   * @param {string} targetPrefix - 目标目录前缀
   * @param {boolean} skipExisting - 是否跳过已存在的文件
   * @returns {Promise<Object>} 复制结果
   */
  async copyDirectoryRecursive(s3Client, bucketName, sourcePrefix, targetPrefix, skipExisting = true) {
    let continuationToken = undefined;
    const result = {
      success: 0,
      skipped: 0,
      failed: 0,
    };

    try {
      do {
        const listParams = {
          Bucket: bucketName,
          Prefix: sourcePrefix,
          MaxKeys: 1000,
          ContinuationToken: continuationToken,
        };

        const listCommand = new ListObjectsV2Command(listParams);
        const response = await s3Client.send(listCommand);

        if (response.Contents && response.Contents.length > 0) {
          for (const item of response.Contents) {
            try {
              const sourceKey = item.Key;
              const relativePath = sourceKey.substring(sourcePrefix.length);
              const targetKey = targetPrefix + relativePath;

              // 检查目标文件是否已存在
              if (skipExisting) {
                try {
                  const listParams = {
                    Bucket: bucketName,
                    Prefix: targetKey,
                    MaxKeys: 1,
                  };
                  const listCommand = new ListObjectsV2Command(listParams);
                  const listResponse = await s3Client.send(listCommand);

                  // 检查是否找到精确匹配的对象
                  const exactMatch = listResponse.Contents?.find((item) => item.Key === targetKey);
                  if (exactMatch) {
                    // 文件已存在，跳过
                    result.skipped++;
                    console.log(`[S3BatchOps] 文件已存在，跳过复制: ${sourceKey} -> ${targetKey}`);
                    continue;
                  }
                } catch (error) {
                  // ListObjects失败，继续复制
                }
              }

              // 执行复制
              const copyParams = {
                Bucket: bucketName,
                CopySource: encodeURIComponent(bucketName + "/" + sourceKey),
                Key: targetKey,
              };

              const copyCommand = new CopyObjectCommand(copyParams);
              await s3Client.send(copyCommand);

              result.success++;
            } catch (error) {
              console.error(`复制文件失败 ${item.Key}:`, error);
              result.failed++;
            }
          }
        }

        continuationToken = response.NextContinuationToken;
      } while (continuationToken);

      return result;
    } catch (error) {
      console.error(`复制目录失败: ${error.message}`);
      throw error;
    }
  }

  /**
   * 复制目录
   * @private
   */
  async _copyDirectory(s3Config, s3SourcePath, s3TargetPath, sourcePath, targetPath, db = null) {
    // 确保源路径和目标路径都以斜杠结尾（标准化目录路径）
    const normalizedS3SourcePath = s3SourcePath.endsWith("/") ? s3SourcePath : s3SourcePath + "/";
    let normalizedS3TargetPath = s3TargetPath.endsWith("/") ? s3TargetPath : s3TargetPath + "/";

    // 如果源路径和目标路径相同（考虑到自动添加斜杠的情况）
    if (normalizedS3SourcePath === normalizedS3TargetPath) {
      // 提取目标目录的父目录和名称
      const targetPathParts = normalizedS3TargetPath.split("/").filter((part) => part.length > 0);
      const targetDirName = targetPathParts.pop(); // 获取目标目录名称
      const targetParentDir = targetPathParts.length > 0 ? "/" + targetPathParts.join("/") + "/" : "/";

      // 使用 _parseFileName 函数正确处理已有的数字后缀
      const { baseName } = this._parseFileName(targetDirName);

      // 在父目录下创建重命名的目录
      let counter = 1;
      let newDirName = `${baseName}(${counter})`;
      let finalS3TargetPath = `${targetParentDir}${newDirName}/`;

      // 检查重命名后的目录是否存在，如果存在则继续递增计数器
      while (await this._checkDirectoryExists(s3Config.bucket_name, finalS3TargetPath)) {
        counter++;
        newDirName = `${baseName}(${counter})`;
        finalS3TargetPath = `${targetParentDir}${newDirName}/`;
      }

      // 更新逻辑路径
      const targetPathWithoutTrailingSlash = targetPath.endsWith("/") ? targetPath.slice(0, -1) : targetPath;
      const targetPathParts2 = targetPathWithoutTrailingSlash.split("/");
      targetPathParts2.pop(); // 移除原始目录名
      const targetParentPath = targetPathParts2.join("/");
      const finalTargetPath = `${targetParentPath}/${baseName}(${counter})/`;

      // 使用递归复制目录函数
      const result = await this.copyDirectoryRecursive(this.s3Client, s3Config.bucket_name, normalizedS3SourcePath, finalS3TargetPath, false);

      return {
        source: sourcePath,
        target: finalTargetPath,
        status: "success",
        message: `目录已重命名为 ${finalTargetPath.split("/").slice(-2, -1)[0]} 并复制成功`,
        renamed: true,
        originalTarget: targetPath,
        details: result,
      };
    } else {
      // 正常的目录复制（目标路径与源路径不同）
      // 实现目录自动重命名逻辑
      let finalS3TargetPath = normalizedS3TargetPath;
      let finalTargetPath = targetPath.endsWith("/") ? targetPath : targetPath + "/";
      let wasRenamed = false;

      // 提取目标目录的父目录和名称（与相同路径逻辑保持一致）
      const targetPathParts = normalizedS3TargetPath.split("/").filter((part) => part.length > 0);
      const targetDirName = targetPathParts.pop(); // 获取目标目录名称
      const targetParentDir = targetPathParts.length > 0 ? "/" + targetPathParts.join("/") + "/" : "/";

      // 使用 _parseFileName 函数正确处理已有的数字后缀
      const { baseName } = this._parseFileName(targetDirName);

      // 检查目标目录是否已存在，如果存在则自动重命名
      let counter = 1;

      // 首先检查原始目标路径是否存在
      if (await this._checkDirectoryExists(s3Config.bucket_name, normalizedS3TargetPath)) {
        // 原始目标存在，需要重命名
        let newDirName = `${baseName}(${counter})`;
        finalS3TargetPath = `${targetParentDir}${newDirName}/`;

        // 检查重命名后的目录是否存在，如果存在则继续递增计数器
        while (await this._checkDirectoryExists(s3Config.bucket_name, finalS3TargetPath)) {
          counter++;
          newDirName = `${baseName}(${counter})`;
          finalS3TargetPath = `${targetParentDir}${newDirName}/`;
        }
        wasRenamed = true;
      } else {
        // 原始目标不存在，直接使用原始路径
        finalS3TargetPath = normalizedS3TargetPath;
      }

      // 更新逻辑路径
      const targetPathWithoutTrailingSlash = targetPath.endsWith("/") ? targetPath.slice(0, -1) : targetPath;
      const targetPathParts2 = targetPathWithoutTrailingSlash.split("/");
      targetPathParts2.pop(); // 移除原始目录名
      const targetParentPath = targetPathParts2.join("/");
      finalTargetPath = `${targetParentPath}/${baseName}(${counter})/`;

      // 使用递归复制目录函数
      const result = await this.copyDirectoryRecursive(this.s3Client, s3Config.bucket_name, normalizedS3SourcePath, finalS3TargetPath, false);

      return {
        source: sourcePath,
        target: finalTargetPath,
        status: "success",
        message: wasRenamed ? `目录已重命名为 ${finalTargetPath.split("/").slice(-2, -1)[0]} 并复制成功` : "目录复制成功",
        renamed: wasRenamed,
        originalTarget: targetPath,
        details: result,
      };
    }
  }

  /**
   * 处理跨存储复制
   * @param {D1Database} db - 数据库实例
   * @param {string} sourcePath - 源路径
   * @param {string} targetPath - 目标路径
   * @param {string|Object} userIdOrInfo - 用户ID或API密钥信息
   * @param {string} userType - 用户类型
   * @returns {Promise<Object>} 跨存储复制结果
   */
  async handleCrossStorageCopy(db, sourcePath, targetPath, userIdOrInfo, userType) {
    return handleFsError(
        async () => {
          // 规范化路径
          sourcePath = normalizePath(sourcePath, sourcePath.endsWith("/"));
          targetPath = normalizePath(targetPath, targetPath.endsWith("/"));

          // 检查路径类型 (都是文件或都是目录)
          const sourceIsDirectory = sourcePath.endsWith("/");
          let targetIsDirectory = targetPath.endsWith("/");

          // 如果源是目录但目标不是目录格式，自动添加斜杠
          if (sourceIsDirectory && !targetIsDirectory) {
            targetPath = targetPath + "/";
            targetIsDirectory = true;
          }

          // 对于文件复制，确保目标路径也是文件路径格式
          if (!sourceIsDirectory && targetIsDirectory) {
            throw new HTTPException(ApiStatus.BAD_REQUEST, { message: "复制文件时，目标路径不能是目录格式" });
          }

          // 查找源路径挂载点
          const sourceMountResult = await findMountPointByPath(db, sourcePath, userIdOrInfo, userType);
          if (sourceMountResult.error) {
            throw new HTTPException(sourceMountResult.error.status, { message: sourceMountResult.error.message });
          }

          // 查找目标路径挂载点
          const targetMountResult = await findMountPointByPath(db, targetPath, userIdOrInfo, userType);
          if (targetMountResult.error) {
            throw new HTTPException(targetMountResult.error.status, { message: targetMountResult.error.message });
          }

          const { mount: sourceMount, subPath: sourceSubPath } = sourceMountResult;
          const { mount: targetMount, subPath: targetSubPath } = targetMountResult;

          // 获取源和目标S3配置
          const sourceS3Config = await db.prepare("SELECT * FROM s3_configs WHERE id = ?").bind(sourceMount.storage_config_id).first();
          const targetS3Config = await db.prepare("SELECT * FROM s3_configs WHERE id = ?").bind(targetMount.storage_config_id).first();

          if (!sourceS3Config || !targetS3Config) {
            throw new HTTPException(ApiStatus.NOT_FOUND, { message: "存储配置不存在" });
          }

          // 创建源S3客户端
          const sourceS3Client = await createS3Client(sourceS3Config, this.encryptionSecret);

          // 判断是否为目录
          const isDirectory = sourcePath.endsWith("/");

          // 规范化S3子路径
          const s3SourcePath = normalizeS3SubPath(sourceSubPath, isDirectory);
          const s3TargetPath = normalizeS3SubPath(targetSubPath, isDirectory);

          // 检查源路径是否存在
          try {
            const sourceExists = await this._checkS3ObjectExists(sourceS3Config.bucket_name, s3SourcePath);
            if (!sourceExists) {
              // 如果是目录，尝试列出目录内容确认存在性
              if (isDirectory) {
                const listResponse = await this._listS3Directory(sourceS3Config.bucket_name, s3SourcePath);

                // 如果没有内容，说明目录不存在或为空
                if (!listResponse.Contents || listResponse.Contents.length === 0) {
                  throw new HTTPException(ApiStatus.NOT_FOUND, { message: "源路径不存在或为空目录" });
                }
              } else {
                throw new HTTPException(ApiStatus.NOT_FOUND, { message: "源文件不存在" });
              }
            }
          } catch (error) {
            if (error instanceof HTTPException) {
              throw error;
            }
            throw new HTTPException(ApiStatus.INTERNAL_ERROR, { message: "检查源路径存在性失败: " + error.message });
          }

          if (isDirectory) {
            // 实现跨存储目录复制的自动重命名逻辑
            let finalS3TargetPath = s3TargetPath;
            let finalTargetPath = targetPath;
            let wasRenamed = false;

            // 创建目标存储的S3客户端用于检查文件存在性
            const targetS3Client = await createS3Client(targetS3Config, this.encryptionSecret);

            // 提取目标目录的父目录和名称（与同存储复制保持一致的逻辑）
            const targetPathParts = s3TargetPath.split("/").filter((part) => part.length > 0);
            const targetDirName = targetPathParts.pop(); // 获取目标目录名称
            const targetParentDir = targetPathParts.length > 0 ? "/" + targetPathParts.join("/") + "/" : "/";

            // 使用 _parseFileName 函数正确处理已有的数字后缀
            const { baseName } = this._parseFileName(targetDirName);

            // 检查目标目录是否已存在，如果存在则自动重命名
            let counter = 1;

            // 首先检查原始目标路径是否存在
            if (await this._checkDirectoryExistsWithClient(targetS3Client, targetS3Config.bucket_name, s3TargetPath)) {
              // 原始目标存在，需要重命名
              let newDirName = `${baseName}(${counter})`;
              finalS3TargetPath = `${targetParentDir}${newDirName}/`;

              // 检查重命名后的目录是否存在，如果存在则继续递增计数器
              while (await this._checkDirectoryExistsWithClient(targetS3Client, targetS3Config.bucket_name, finalS3TargetPath)) {
                counter++;
                newDirName = `${baseName}(${counter})`;
                finalS3TargetPath = `${targetParentDir}${newDirName}/`;
              }
              wasRenamed = true;
            } else {
              // 原始目标不存在，直接使用原始路径
              finalS3TargetPath = s3TargetPath;
            }

            // 更新逻辑路径
            const targetPathWithoutTrailingSlash = targetPath.endsWith("/") ? targetPath.slice(0, -1) : targetPath;
            const targetPathParts2 = targetPathWithoutTrailingSlash.split("/");
            targetPathParts2.pop(); // 移除原始目录名
            const targetParentPath = targetPathParts2.join("/");
            finalTargetPath = `${targetParentPath}/${baseName}(${counter})/`;

            // 目录跨存储复制，获取目录中所有文件的预签名URL（使用重命名后的路径）
            const items = await getDirectoryPresignedUrls(sourceS3Client, sourceS3Config, targetS3Config, s3SourcePath, finalS3TargetPath, this.encryptionSecret);

            return {
              crossStorage: true,
              isDirectory: true,
              source: sourcePath,
              target: finalTargetPath,
              status: "success",
              sourceMount: sourceMount.id,
              targetMount: targetMount.id,
              items,
              renamed: wasRenamed,
              originalTarget: targetPath,
              message: wasRenamed
                  ? `目录将重命名为 ${finalTargetPath.split("/").slice(-2, -1)[0]} 并进行跨存储复制，共 ${items.length} 个文件`
                  : `跨存储目录复制请求已生成，共 ${items.length} 个文件`,
            };
          } else {
            // 文件跨存储复制，生成预签名URL

            // 实现跨存储复制的自动重命名逻辑
            let finalS3TargetPath = s3TargetPath;
            let finalTargetPath = targetPath;
            let wasRenamed = false;

            // 创建目标存储的S3客户端用于检查文件存在性
            const targetS3Client = await createS3Client(targetS3Config, this.encryptionSecret);

            // 检查目标文件是否已存在（支持skipExisting逻辑）
            try {
              const targetExists = await this._checkItemExistsWithClient(targetS3Client, targetS3Config.bucket_name, s3TargetPath);
              if (targetExists) {
                console.log(`[S3BatchOps] 跨存储复制目标文件已存在，跳过: ${sourcePath} -> ${targetPath}`);
                return {
                  crossStorage: true,
                  isDirectory: false,
                  source: sourcePath,
                  target: targetPath,
                  status: "skipped",
                  skipped: true,
                  sourceMount: sourceMount.id,
                  targetMount: targetMount.id,
                  message: "目标文件已存在，跳过跨存储复制",
                };
              }
            } catch (error) {
              console.warn(`[S3BatchOps] 检查目标文件存在性失败，继续复制: ${error.message}`);
              // 检查失败时继续复制，避免因为权限问题阻止正常操作
            }

            // 根据系统设置决定冲突处理策略
            const database = db || this.db;
            let useRandomSuffix = false;

            if (database) {
              try {
                useRandomSuffix = await shouldUseRandomSuffix(database);
              } catch (error) {
                console.warn("获取文件命名策略失败，使用默认覆盖模式:", error);
                useRandomSuffix = false;
              }
            }

            if (useRandomSuffix) {
              // 随机后缀模式：检查冲突，如果存在则添加随机后缀
              if (await this._checkItemExistsWithClient(targetS3Client, targetS3Config.bucket_name, finalS3TargetPath)) {
                const { baseName: s3BaseName, extension: s3Ext, directory: s3Dir } = this._parseFileName(s3TargetPath);
                const { baseName: logicalBaseName, extension: logicalExt, directory: logicalDir } = this._parseFileName(targetPath);

                const shortId = generateShortId();
                finalS3TargetPath = `${s3Dir}${s3BaseName}-${shortId}${s3Ext}`;
                finalTargetPath = `${logicalDir}${logicalBaseName}-${shortId}${logicalExt}`;
                wasRenamed = true;
              }
            }
            // 覆盖模式：不检查冲突，直接使用原始路径覆盖

            // 生成源文件的下载预签名URL
            const expiresIn = 3600; // 1小时
            const downloadUrl = await generatePresignedUrl(sourceS3Config, s3SourcePath, this.encryptionSecret, expiresIn, false);

            // 生成目标文件的上传预签名URL（使用重命名后的路径）
            const fileName = sourcePath.split("/").filter(Boolean).pop() || "file";
            const contentType = getMimeTypeFromFilename(fileName);

            const uploadUrl = await generatePresignedPutUrl(targetS3Config, finalS3TargetPath, contentType, this.encryptionSecret, expiresIn);

            return {
              crossStorage: true,
              isDirectory: false,
              source: sourcePath,
              target: finalTargetPath,
              status: "success",
              sourceMount: sourceMount.id,
              targetMount: targetMount.id,
              sourceS3Path: s3SourcePath,
              targetS3Path: finalS3TargetPath,
              fileName,
              contentType,
              downloadUrl,
              uploadUrl,
              renamed: wasRenamed,
              originalTarget: targetPath,
              message: wasRenamed ? `文件将重命名为 ${finalTargetPath.split("/").pop()} 并进行跨存储复制` : "已生成跨存储文件复制的预签名URL",
            };
          }
        },
        "跨存储复制",
        "跨存储复制请求处理失败"
    );
  }

  /**
   * 处理跨存储复制（私有方法）
   * @private
   */
  async _handleCrossStorageCopy(db, sourcePath, targetPath, userIdOrInfo, userType) {
    return await this.handleCrossStorageCopy(db, sourcePath, targetPath, userIdOrInfo, userType);
  }

  /**
   * 单个项目重命名（文件或目录）
   * @param {string} oldPath - 旧路径
   * @param {string} newPath - 新路径
   * @param {Object} options - 选项参数
   * @returns {Promise<Object>} 重命名结果
   */
  async renameItem(oldPath, newPath, options = {}) {
    const { db, findMountPointByPath, userIdOrInfo, userType } = options;

    return handleFsError(
        async () => {
          // 规范化路径
          oldPath = normalizePath(oldPath, oldPath.endsWith("/"));
          newPath = normalizePath(newPath, newPath.endsWith("/"));

          // 检查路径类型必须匹配
          const oldIsDirectory = oldPath.endsWith("/");
          const newIsDirectory = newPath.endsWith("/");

          if (oldIsDirectory !== newIsDirectory) {
            throw new HTTPException(ApiStatus.BAD_REQUEST, { message: "源路径和目标路径类型必须一致（文件或目录）" });
          }

          // 查找挂载点
          const mountResult = await findMountPointByPath(db, oldPath, userIdOrInfo, userType);
          if (mountResult.error) {
            throw new HTTPException(mountResult.error.status, { message: mountResult.error.message });
          }

          const { mount, subPath: oldSubPath } = mountResult;

          // 检查新路径是否在同一挂载点
          const newMountResult = await findMountPointByPath(db, newPath, userIdOrInfo, userType);
          if (newMountResult.error || newMountResult.mount.id !== mount.id) {
            throw new HTTPException(ApiStatus.BAD_REQUEST, { message: "重命名操作必须在同一挂载点内进行" });
          }

          const { subPath: newSubPath } = newMountResult;
          const s3Config = await db.prepare("SELECT * FROM s3_configs WHERE id = ?").bind(mount.storage_config_id).first();

          if (!s3Config) {
            throw new HTTPException(ApiStatus.NOT_FOUND, { message: "存储配置不存在" });
          }

          const oldS3SubPath = normalizeS3SubPath(oldSubPath, oldIsDirectory);
          const newS3SubPath = normalizeS3SubPath(newSubPath, newIsDirectory);

          // 处理root_prefix
          const rootPrefix = s3Config.root_prefix ? (s3Config.root_prefix.endsWith("/") ? s3Config.root_prefix : s3Config.root_prefix + "/") : "";
          const fullOldS3Path = rootPrefix + oldS3SubPath;
          const fullNewS3Path = rootPrefix + newS3SubPath;

          // 检查源文件/目录是否存在
          const sourceExists = oldIsDirectory
              ? await this._checkDirectoryExists(s3Config.bucket_name, fullOldS3Path)
              : await this._checkItemExists(s3Config.bucket_name, fullOldS3Path);

          if (!sourceExists) {
            throw new HTTPException(ApiStatus.NOT_FOUND, { message: "源文件或目录不存在" });
          }

          // 检查目标是否已存在
          const targetExists = newIsDirectory
              ? await this._checkDirectoryExists(s3Config.bucket_name, fullNewS3Path)
              : await this._checkItemExists(s3Config.bucket_name, fullNewS3Path);

          if (targetExists) {
            throw new HTTPException(ApiStatus.CONFLICT, { message: "目标路径已存在" });
          }

          if (oldIsDirectory) {
            // 重命名目录：复制所有内容到新位置，然后删除原目录
            await this.copyDirectoryRecursive(this.s3Client, s3Config.bucket_name, fullOldS3Path, fullNewS3Path, false);
            await this.deleteDirectoryRecursive(this.s3Client, s3Config.bucket_name, fullOldS3Path, mount.storage_config_id);
          } else {
            // 重命名文件：复制到新位置，然后删除原文件
            const copyParams = {
              Bucket: s3Config.bucket_name,
              CopySource: encodeURIComponent(s3Config.bucket_name + "/" + fullOldS3Path),
              Key: fullNewS3Path,
              MetadataDirective: "COPY",
            };

            const copyCommand = new CopyObjectCommand(copyParams);
            await this.s3Client.send(copyCommand);

            // 删除原文件
            const deleteParams = {
              Bucket: s3Config.bucket_name,
              Key: fullOldS3Path,
            };

            const deleteCommand = new DeleteObjectCommand(deleteParams);
            await this.s3Client.send(deleteCommand);

            // 文件移动完成，无需数据库操作
          }

          // 更新父目录的修改时间
          await updateParentDirectoriesModifiedTime(this.s3Client, s3Config.bucket_name, fullOldS3Path, rootPrefix);

          // 更新挂载点的最后使用时间
          await updateMountLastUsed(db, mount.id);

          // 清除缓存
          await clearDirectoryCache({ mountId: mount.id });

          return {
            success: true,
            oldPath,
            newPath,
            message: oldIsDirectory ? "目录重命名成功" : "文件重命名成功",
          };
        },
        "重命名文件或目录",
        "重命名失败"
    );
  }

  /**
   * 检查S3对象是否存在
   * @private
   * @param {string} bucketName - 存储桶名称
   * @param {string} key - 对象键
   * @returns {Promise<boolean>} 是否存在
   */
  async _checkS3ObjectExists(bucketName, key) {
    try {
      const listParams = {
        Bucket: bucketName,
        Prefix: key,
        MaxKeys: 1,
      };

      const listCommand = new ListObjectsV2Command(listParams);
      const listResponse = await this.s3Client.send(listCommand);

      // 检查是否找到精确匹配的对象
      const exactMatch = listResponse.Contents?.find((item) => item.Key === key);
      return !!exactMatch;
    } catch (error) {
      return false;
    }
  }

  /**
   * 列出S3目录内容
   * @private
   * @param {string} bucketName - 存储桶名称
   * @param {string} prefix - 目录前缀
   * @returns {Promise<Object>} 列表响应
   */
  async _listS3Directory(bucketName, prefix) {
    const listParams = {
      Bucket: bucketName,
      Prefix: prefix,
      MaxKeys: 1, // 只需要检查是否有内容，不需要全部列出
    };

    const listCommand = new ListObjectsV2Command(listParams);
    return await this.s3Client.send(listCommand);
  }

  /**
   * 检查文件是否存在
   * @private
   * @param {string} bucketName - 存储桶名称
   * @param {string} key - 文件路径
   * @returns {Promise<boolean>} 是否存在
   */
  async _checkItemExists(bucketName, key) {
    try {
      const listParams = {
        Bucket: bucketName,
        Prefix: key,
        MaxKeys: 1,
      };

      const listCommand = new ListObjectsV2Command(listParams);
      const listResponse = await this.s3Client.send(listCommand);

      // 检查是否找到精确匹配的对象
      const exactMatch = listResponse.Contents?.find((item) => item.Key === key);
      return !!exactMatch;
    } catch (error) {
      return false;
    }
  }

  /**
   * 检查目录是否存在
   * @private
   * @param {string} bucketName - 存储桶名称
   * @param {string} dirPath - 目录路径
   * @returns {Promise<boolean>} 是否存在
   */
  async _checkDirectoryExists(bucketName, dirPath) {
    try {
      // 首先尝试检查目录标记对象是否存在
      const headParams = {
        Bucket: bucketName,
        Key: dirPath,
      };
      const headCommand = new HeadObjectCommand(headParams);
      await this.s3Client.send(headCommand);
      return true;
    } catch (error) {
      if (error.$metadata && error.$metadata.httpStatusCode === 404) {
        // 目录标记对象不存在，检查是否有以此路径为前缀的对象
        try {
          const listParams = {
            Bucket: bucketName,
            Prefix: dirPath,
            MaxKeys: 1,
          };
          const listCommand = new ListObjectsV2Command(listParams);
          const listResponse = await this.s3Client.send(listCommand);

          // 如果有任何对象以此路径为前缀，则认为目录存在
          return listResponse.Contents && listResponse.Contents.length > 0;
        } catch (listError) {
          return false;
        }
      }
      throw error;
    }
  }

  /**
   * 解析文件名，提取基础名称、扩展名和目录路径
   * @private
   * @param {string} filePath - 文件路径
   * @returns {Object} 包含 baseName, extension, directory 的对象
   */
  _parseFileName(filePath) {
    // 处理空路径的边界情况
    if (!filePath || filePath.trim() === "") {
      return { baseName: "", extension: "", directory: "" };
    }

    const pathParts = filePath.split("/");
    const fileName = pathParts.pop() || "";
    const directory = pathParts.length > 0 ? pathParts.join("/") + "/" : "";

    // 处理空文件名的边界情况
    if (!fileName) {
      return { baseName: "", extension: "", directory };
    }

    const lastDotIndex = fileName.lastIndexOf(".");
    let baseName, extension;

    // 修复只有扩展名的文件处理（如 ".txt"）
    if (lastDotIndex > 0) {
      // 正常情况：文件名.扩展名
      baseName = fileName.substring(0, lastDotIndex);
      extension = fileName.substring(lastDotIndex);
    } else if (lastDotIndex === 0) {
      // 只有扩展名的情况：.txt
      baseName = "";
      extension = fileName;
    } else {
      // 没有扩展名的情况
      baseName = fileName;
      extension = "";
    }

    // 移除已有的数字后缀 (如果存在) - 支持多层嵌套的数字后缀
    // 例如：folder(1)(1) → folder, document(2)(3) → document
    // 添加循环保护，防止无限循环
    let loopCount = 0;
    const maxLoops = 10; // 最多处理10层嵌套

    while (loopCount < maxLoops && baseName) {
      const numberMatch = baseName.match(/^(.+)\((\d+)\)$/);
      if (numberMatch && numberMatch[1]) {
        baseName = numberMatch[1];
        loopCount++;
      } else {
        break;
      }
    }

    // 确保 baseName 不为空（为空时使用默认值）
    if (!baseName && !extension) {
      baseName = "unnamed";
    }

    return { baseName, extension, directory };
  }

  /**
   * 使用指定的S3客户端检查文件是否存在
   * @private
   * @param {S3Client} s3Client - S3客户端实例
   * @param {string} bucketName - 存储桶名称
   * @param {string} key - 文件路径
   * @returns {Promise<boolean>} 是否存在
   */
  async _checkItemExistsWithClient(s3Client, bucketName, key) {
    try {
      const listParams = {
        Bucket: bucketName,
        Prefix: key,
        MaxKeys: 1,
      };

      const listCommand = new ListObjectsV2Command(listParams);
      const listResponse = await s3Client.send(listCommand);

      // 检查是否找到精确匹配的对象
      const exactMatch = listResponse.Contents?.find((item) => item.Key === key);
      return !!exactMatch;
    } catch (error) {
      return false;
    }
  }

  /**
   * 使用指定的S3客户端检查目录是否存在
   * @private
   * @param {S3Client} s3Client - S3客户端实例
   * @param {string} bucketName - 存储桶名称
   * @param {string} dirPath - 目录路径
   * @returns {Promise<boolean>} 是否存在
   */
  async _checkDirectoryExistsWithClient(s3Client, bucketName, dirPath) {
    try {
      // 首先尝试检查目录标记对象是否存在
      const headParams = {
        Bucket: bucketName,
        Key: dirPath,
      };
      const headCommand = new HeadObjectCommand(headParams);
      await s3Client.send(headCommand);
      return true;
    } catch (error) {
      if (error.$metadata && error.$metadata.httpStatusCode === 404) {
        // 目录标记对象不存在，检查是否有以此路径为前缀的对象
        try {
          const listParams = {
            Bucket: bucketName,
            Prefix: dirPath,
            MaxKeys: 1,
          };
          const listCommand = new ListObjectsV2Command(listParams);
          const result = await s3Client.send(listCommand);

          // 如果有任何对象以此路径为前缀，则认为目录存在
          return result.Contents && result.Contents.length > 0;
        } catch (listError) {
          console.error(`检查目录存在性时出错: ${listError.message}`);
          return false;
        }
      }
      throw error;
    }
  }
}
