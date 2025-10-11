/**
 * S3后端分片上传操作模块
 * 负责后端分片上传相关操作，主要用于WebDAV等需要直接处理分片数据的场景
 */

import {
  CreateMultipartUploadCommand,
  UploadPartCommand,
  CompleteMultipartUploadCommand,
  AbortMultipartUploadCommand,
  ListPartsCommand,
  HeadObjectCommand,
} from "@aws-sdk/client-s3";
import { handleFsError } from "../../../fs/utils/ErrorHandler.js";
import { getMimeTypeFromFilename } from "../../../../utils/fileUtils.js";
import { buildS3Url } from "../../../../utils/s3Utils.js";
import { updateParentDirectoriesModifiedTime, checkDirectoryExists } from "../utils/S3DirectoryUtils.js";
import { HTTPException } from "hono/http-exception";
import { ApiStatus } from "../../../../constants/index.js";
import { getRecommendedPartSize } from "../../../../utils/environmentUtils.js";

export class S3BackendMultipartOperations {
  /**
   * 构造函数
   * @param {S3Client} s3Client - S3客户端实例
   * @param {Object} config - S3配置对象
   * @param {string} encryptionSecret - 加密密钥
   */
  constructor(s3Client, config, encryptionSecret) {
    this.s3Client = s3Client;
    this.config = config;
    this.encryptionSecret = encryptionSecret;
  }

  /**
   * 初始化后端分片上传
   * @param {string} s3SubPath - 已规范化的S3子路径
   * @param {Object} options - 选项参数
   * @param {string} options.contentType - 文件MIME类型
   * @param {number} options.fileSize - 文件大小（可选）
   * @param {string} options.filename - 文件名（可选）
   * @returns {Promise<Object>} 包含uploadId和其他必要信息的上传会话
   */
  async initializeBackendMultipartUpload(s3SubPath, options = {}) {
    const { contentType, fileSize, filename } = options;

    return handleFsError(
      async () => {
        // 检查父目录是否存在，如果不存在则自动创建
        if (s3SubPath.includes("/")) {
          const parentPath = s3SubPath.substring(0, s3SubPath.lastIndexOf("/") + 1);
          const parentExists = await checkDirectoryExists(this.s3Client, this.config.bucket_name, parentPath);

          if (!parentExists) {
            // 自动创建父目录而不是抛出错误
            console.log(`后端分片上传: 父目录 ${parentPath} 不存在，正在自动创建...`);

            try {
              // 创建一个空对象作为目录标记
              const { PutObjectCommand } = await import("@aws-sdk/client-s3");
              const createDirParams = {
                Bucket: this.config.bucket_name,
                Key: parentPath,
                Body: Buffer.from("", "utf-8"),
                ContentType: "application/x-directory", // 目录内容类型
              };

              const createDirCommand = new PutObjectCommand(createDirParams);
              await this.s3Client.send(createDirCommand);
              console.log(`后端分片上传: 已成功创建父目录 ${parentPath}`);
            } catch (dirError) {
              console.error(`后端分片上传: 创建父目录 ${parentPath} 失败:`, dirError);
              // 即使创建目录失败，我们也尝试继续上传文件
              // 某些S3实现可能不需要显式目录对象
            }
          }
        }

        // 统一从文件名推断MIME类型，不依赖前端传来的contentType
        const fileName = filename || s3SubPath.split("/").filter(Boolean).pop();
        const finalContentType = getMimeTypeFromFilename(fileName);
        console.log(`后端分片上传初始化：从文件名[${fileName}]推断MIME类型: ${finalContentType}`);

        // 创建分片上传
        const createCommand = new CreateMultipartUploadCommand({
          Bucket: this.config.bucket_name,
          Key: s3SubPath,
          ContentType: finalContentType,
        });

        const createResponse = await this.s3Client.send(createCommand);

        // 返回必要的信息用于后续上传
        return {
          uploadId: createResponse.UploadId,
          bucket: this.config.bucket_name,
          key: s3SubPath,
          storage_type: "S3",
          recommendedPartSize: getRecommendedPartSize(),
        };
      },
      "初始化后端分片上传",
      "初始化后端分片上传失败"
    );
  }

  /**
   * 上传单个分片
   * @param {string} s3SubPath - 已规范化的S3子路径
   * @param {Object} options - 选项参数
   * @param {string} options.uploadId - 上传ID
   * @param {number} options.partNumber - 分片编号（从1开始）
   * @param {ArrayBuffer} options.partData - 分片数据
   * @returns {Promise<Object>} 包含ETag的响应对象
   */
  async uploadBackendPart(s3SubPath, options = {}) {
    const { uploadId, partNumber, partData } = options;

    return handleFsError(
      async () => {
        // 上传分片
        const uploadCommand = new UploadPartCommand({
          Bucket: this.config.bucket_name,
          Key: s3SubPath,
          UploadId: uploadId,
          PartNumber: partNumber,
          Body: partData,
        });

        const uploadResponse = await this.s3Client.send(uploadCommand);

        // 返回必要的信息用于后续完成上传
        return {
          partNumber: partNumber,
          etag: uploadResponse.ETag,
        };
      },
      "上传后端分片",
      "上传后端分片失败"
    );
  }

  /**
   * 完成后端分片上传
   * @param {string} s3SubPath - 已规范化的S3子路径
   * @param {Object} options - 选项参数
   * @param {string} options.uploadId - 上传ID
   * @param {Array<{partNumber: number, etag: string}>} options.parts - 已上传分片的信息
   * @param {string} options.contentType - 文件MIME类型（可选）
   * @param {number} options.fileSize - 文件大小（可选）
   * @returns {Promise<Object>} 完成上传的响应
   */
  async completeBackendMultipartUpload(s3SubPath, options = {}) {
    const { uploadId, parts, contentType = "application/octet-stream", fileSize = 0 } = options;

    return handleFsError(
      async () => {
        // 确保parts按照partNumber排序
        const sortedParts = [...parts].sort((a, b) => a.partNumber - b.partNumber);

        let completeResponse;
        try {
          // 完成分片上传
          const completeCommand = new CompleteMultipartUploadCommand({
            Bucket: this.config.bucket_name,
            Key: s3SubPath,
            UploadId: uploadId,
            MultipartUpload: {
              Parts: sortedParts.map((part) => ({
                PartNumber: part.partNumber,
                ETag: part.etag,
              })),
            },
          });

          completeResponse = await this.s3Client.send(completeCommand);
        } catch (error) {
          // 特殊处理"NoSuchUpload"错误 - 可能上传已经完成
          if (error.name === "NoSuchUpload" || (error.message && error.message.includes("The specified multipart upload does not exist"))) {
            console.log(`检测到NoSuchUpload错误，检查文件是否已存在: ${s3SubPath}`);
            try {
              // 尝试检查对象是否已存在（上传可能已成功）
              const headCommand = new HeadObjectCommand({
                Bucket: this.config.bucket_name,
                Key: s3SubPath,
              });

              const headResponse = await this.s3Client.send(headCommand);
              if (headResponse) {
                console.log(`文件已存在，视为上传成功: ${s3SubPath}`);
                // 创建一个类似于成功完成的响应
                completeResponse = {
                  ETag: headResponse.ETag,
                  Location: buildS3Url(this.config, s3SubPath),
                };
              } else {
                // 如果检查对象也失败，重新抛出原始错误
                throw error;
              }
            } catch (headError) {
              console.error(`检查对象是否存在失败: ${headError.message}`);
              // 重新抛出原始错误
              throw error;
            }
          } else {
            // 其他类型错误，继续抛出
            throw error;
          }
        }

        // 更新父目录的修改时间
        const rootPrefix = this.config.root_prefix ? (this.config.root_prefix.endsWith("/") ? this.config.root_prefix : this.config.root_prefix + "/") : "";
        await updateParentDirectoriesModifiedTime(this.s3Client, this.config.bucket_name, s3SubPath, rootPrefix);

        // 构建S3直接访问URL
        const s3Url = buildS3Url(this.config, s3SubPath);

        return {
          success: true,
          etag: completeResponse.ETag,
          location: completeResponse.Location,
          s3Url: s3Url,
        };
      },
      "完成后端分片上传",
      "完成后端分片上传失败"
    );
  }

  /**
   * 中止后端分片上传并彻底清理所有已上传分片
   * @param {string} s3SubPath - 已规范化的S3子路径
   * @param {Object} options - 选项参数
   * @param {string} options.uploadId - 上传ID
   * @returns {Promise<Object>} 中止上传的响应
   */
  async abortBackendMultipartUpload(s3SubPath, options = {}) {
    const { uploadId } = options;
    const MAX_RETRY = 3; // 最大重试次数

    try {
      console.log(`中止后端分片上传: Bucket=${this.config.bucket_name}, Key=${s3SubPath}`);

      // 记录当前分片状态，用于验证是否成功清理
      let currentParts = [];
      try {
        const listPartsCommand = new ListPartsCommand({
          Bucket: this.config.bucket_name,
          Key: s3SubPath,
          UploadId: uploadId,
        });
        const listPartsResponse = await this.s3Client.send(listPartsCommand);
        currentParts = listPartsResponse.Parts || [];
      } catch (listError) {
        console.warn(`无法获取已上传分片列表: ${listError.message}`);
      }

      // 执行中止操作并重试
      let success = false;
      let lastError = null;

      for (let attempt = 1; attempt <= MAX_RETRY; attempt++) {
        try {
          const abortCommand = new AbortMultipartUploadCommand({
            Bucket: this.config.bucket_name,
            Key: s3SubPath,
            UploadId: uploadId,
          });

          await this.s3Client.send(abortCommand);
          success = true;
          break;
        } catch (retryError) {
          lastError = retryError;
          console.error(`中止后端分片上传失败 (尝试 ${attempt}/${MAX_RETRY}): ${retryError.message}`);

          // 如果不是最后一次尝试，等待一段时间再重试
          if (attempt < MAX_RETRY) {
            const delayMs = 500 * Math.pow(2, attempt - 1); // 指数退避策略
            await new Promise((resolve) => setTimeout(resolve, delayMs));
          }
        }
      }

      // 验证是否成功清理
      if (success && currentParts.length > 0) {
        try {
          const verifyCommand = new ListPartsCommand({
            Bucket: this.config.bucket_name,
            Key: s3SubPath,
            UploadId: uploadId,
          });
          await this.s3Client.send(verifyCommand);
          // 如果没有抛出错误，说明分片上传仍然存在
          console.warn(`警告: 中止后端分片上传后资源未完全清理`);
          success = false;
        } catch (verifyError) {
          // 如果抛出 NoSuchUpload 错误，说明已成功清理
          if (verifyError.name === "NoSuchUpload") {
            console.log(`后端分片上传资源已完全清理`);
            success = true;
          } else {
            console.warn(`验证清理结果时出错: ${verifyError.message}`);
          }
        }
      }

      if (!success && lastError) {
        throw lastError;
      }

      return {
        success: true,
        message: "已彻底清理后端分片上传资源",
      };
    } catch (error) {
      console.error(`中止后端分片上传失败: ${error.message}`);

      // 如果我们已经有了S3客户端和必要信息，但前面的尝试都失败，进行最后一次尝试
      if (this.s3Client && this.config.bucket_name && s3SubPath && uploadId) {
        try {
          console.log(`进行紧急清理尝试`);
          const finalAbortCommand = new AbortMultipartUploadCommand({
            Bucket: this.config.bucket_name,
            Key: s3SubPath,
            UploadId: uploadId,
          });

          await this.s3Client.send(finalAbortCommand);
          return {
            success: true,
            message: "已中止后端分片上传 (紧急恢复执行成功)",
          };
        } catch (finalError) {
          console.error(`紧急清理尝试失败: ${finalError.message}`);
        }
      }

      // 如果已经是HTTPException，直接抛出
      if (error instanceof HTTPException) {
        throw error;
      }
      // 其他错误转换为内部服务器错误
      throw new HTTPException(ApiStatus.INTERNAL_ERROR, { message: error.message || "中止后端分片上传失败，无法清理资源" });
    }
  }
}
