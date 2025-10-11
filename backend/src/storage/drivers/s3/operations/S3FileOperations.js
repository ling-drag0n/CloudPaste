/**
 * S3文件操作模块
 * 负责单个文件的基础操作：获取信息、下载、上传、删除等
 */

import { HTTPException } from "hono/http-exception";
import { ApiStatus } from "../../../../constants/index.js";
import { generatePresignedUrl, createS3Client } from "../../../../utils/s3Utils.js";
import { S3Client, GetObjectCommand, PutObjectCommand, DeleteObjectCommand, ListObjectsV2Command, CopyObjectCommand } from "@aws-sdk/client-s3";
import { getMimeTypeFromFilename } from "../../../../utils/fileUtils.js";
import { handleFsError } from "../../../fs/utils/ErrorHandler.js";
import { updateParentDirectoriesModifiedTime } from "../utils/S3DirectoryUtils.js";
import { CAPABILITIES } from "../../../interfaces/capabilities/index.js";
import { GetFileType, getFileTypeName } from "../../../../utils/fileTypeDetector.js";
import { FILE_TYPES, FILE_TYPE_NAMES } from "../../../../constants/index.js";

export class S3FileOperations {
  /**
   * 构造函数
   * @param {S3Client} s3Client - S3客户端
   * @param {Object} config - S3配置
   * @param {string} encryptionSecret - 加密密钥
   * @param {Object} driver - 存储驱动实例（用于代理能力）
   */
  constructor(s3Client, config, encryptionSecret, driver = null) {
    this.s3Client = s3Client;
    this.config = config;
    this.encryptionSecret = encryptionSecret;
    this.driver = driver;
  }

  /**
   * 从S3获取文件内容
   * @param {Object} s3Config - S3配置对象
   * @param {string} s3SubPath - S3子路径
   * @param {string} fileName - 文件名
   * @param {boolean} forceDownload - 是否强制下载
   * @param {string} encryptionSecret - 加密密钥
   * @param {Request} request - 请求对象，用于获取Range头
   * @returns {Promise<Response>} 文件内容响应
   */
  async getFileFromS3(s3Config, s3SubPath, fileName, forceDownload = false, encryptionSecret, request = null) {
    try {
      const s3Client = await createS3Client(s3Config, encryptionSecret);

      // 构建GetObject参数
      const getParams = {
        Bucket: s3Config.bucket_name,
        Key: s3SubPath.startsWith("/") ? s3SubPath.slice(1) : s3SubPath,
      };

      // 处理Range请求（用于视频流等）
      if (request) {
        const rangeHeader = request.headers.get("range");
        if (rangeHeader) {
          getParams.Range = rangeHeader;
          console.log(`[S3FileOperations] Range请求: ${rangeHeader} -> ${s3SubPath}`);
        }
      }

      // 使用AWS SDK v3的GetObjectCommand
      const getCommand = new GetObjectCommand(getParams);
      const response = await s3Client.send(getCommand);

      // 获取内容类型
      const contentType = response.ContentType || getMimeTypeFromFilename(fileName);

      // 构建响应头
      const headers = new Headers();
      headers.set("Content-Type", contentType);
      headers.set("Content-Length", response.ContentLength?.toString() || "0");

      // 检查是否为Range请求响应
      const contentRange = response.ContentRange;
      const isRangeResponse = !!contentRange;

      // 设置缓存控制 - Range请求使用较短的缓存时间
      if (isRangeResponse) {
        headers.set("Cache-Control", "public, max-age=3600"); // Range请求1小时缓存
      }

      // 处理下载
      if (forceDownload) {
        headers.set("Content-Disposition", `attachment; filename="${encodeURIComponent(fileName)}"`);
      } else {
        // 对于某些文件类型，设置为inline显示
        if (contentType.startsWith("image/") || contentType.startsWith("video/") || contentType === "application/pdf") {
          headers.set("Content-Disposition", `inline; filename="${encodeURIComponent(fileName)}"`);
        }
      }

      // 处理Range响应 - 确保传递所有Range相关头部
      if (contentRange) {
        headers.set("Content-Range", contentRange);
        console.log(`[S3FileOperations] Range响应: ${contentRange}`);
      }

      // 始终设置Accept-Ranges以支持Range请求
      headers.set("Accept-Ranges", "bytes");

      // 设置ETag
      if (response.ETag) {
        headers.set("ETag", response.ETag);
      }

      // 设置Last-Modified
      if (response.LastModified) {
        headers.set("Last-Modified", response.LastModified.toUTCString());
      }

      // 返回Response，AWS SDK v3成功响应状态码为200或206
      const statusCode = isRangeResponse ? 206 : 200;
      return new Response(response.Body, {
        status: statusCode,
        headers,
      });
    } catch (error) {
      console.error("从S3获取文件失败:", error);

      // 如果是HTTPException，直接抛出
      if (error instanceof HTTPException) {
        throw error;
      }

      // 处理网络错误或其他错误
      throw new HTTPException(ApiStatus.INTERNAL_ERROR, {
        message: `获取文件失败: ${error.message}`,
      });
    }
  }

  /**
   * 获取文件信息
   * @param {string} s3SubPath - S3子路径
   * @param {Object} options - 选项参数
   * @returns {Promise<Object>} 文件信息
   */
  async getFileInfo(s3SubPath, options = {}) {
    const { mount, path, userType, userId, request, db } = options;

    return handleFsError(
      async () => {
        // 使用 ListObjectsV2Command 获取文件信息
        console.log(`getFileInfo - 使用 ListObjects 查询文件: ${s3SubPath}`);

        const listParams = {
          Bucket: this.config.bucket_name,
          Prefix: s3SubPath,
          MaxKeys: 1,
        };

        try {
          const listCommand = new ListObjectsV2Command(listParams);
          const listResponse = await this.s3Client.send(listCommand);

          // 检查是否找到精确匹配的文件
          const exactMatch = listResponse.Contents?.find((item) => item.Key === s3SubPath);

          if (!exactMatch) {
            throw new HTTPException(ApiStatus.NOT_FOUND, { message: "文件不存在" });
          }

          // 构建文件信息对象
          const fileName = path.split("/").filter(Boolean).pop() || "/";

          // 检查是否为目录：基于Key是否以'/'结尾判断
          const isDirectory = exactMatch.Key.endsWith("/");

          const fileType = isDirectory ? FILE_TYPES.FOLDER : await GetFileType(fileName, db);
          const fileTypeName = isDirectory ? FILE_TYPE_NAMES[FILE_TYPES.FOLDER] : await getFileTypeName(fileName, db);
          const detectedMimeType = isDirectory ? "application/x-directory" : getMimeTypeFromFilename(fileName);

          const result = {
            path: path,
            name: fileName,
            isDirectory: isDirectory,
            size: isDirectory ? 0 : exactMatch.Size || 0, // 目录大小为0
            modified: exactMatch.LastModified ? exactMatch.LastModified.toISOString() : new Date().toISOString(),
            mimetype: detectedMimeType,
            etag: exactMatch.ETag ? exactMatch.ETag.replace(/"/g, "") : undefined,
            mount_id: mount.id,
            storage_type: mount.storage_type,
            type: fileType, // 整数类型常量 (0-6)
            typeName: fileTypeName, // 类型名称（用于调试）
          };

          // 生成预签名URL（如果需要）
          if (userType && userId) {
            try {
              const cacheOptions = {
                userType,
                userId,
                enableCache: mount.cache_ttl > 0,
              };

              // 根据挂载点配置决定URL类型
              if (!!mount.web_proxy && this.driver?.hasCapability?.(CAPABILITIES.PROXY)) {
                // 代理模式：使用驱动的代理能力生成URL
                try {
                  const previewProxy = await this.driver.generateProxyUrl(path, { mount, request, download: false, db });
                  const downloadProxy = await this.driver.generateProxyUrl(path, { mount, request, download: true, db });

                  result.preview_url = previewProxy.url;
                  result.download_url = downloadProxy.url;
                  console.log(`为文件[${result.name}]生成代理URL: ✓预览 ✓下载`);
                } catch (error) {
                  console.warn(`代理URL生成失败，回退到预签名URL:`, error);
                  // 回退到预签名URL
                  const previewUrl = await generatePresignedUrl(this.config, s3SubPath, this.encryptionSecret, null, false, null, cacheOptions);
                  result.preview_url = previewUrl;

                  const downloadUrl = await generatePresignedUrl(this.config, s3SubPath, this.encryptionSecret, null, true, null, cacheOptions);
                  result.download_url = downloadUrl;
                }
              } else {
                // 直链模式：返回S3预签名URL
                const previewUrl = await generatePresignedUrl(this.config, s3SubPath, this.encryptionSecret, null, false, null, cacheOptions);
                result.preview_url = previewUrl;

                const downloadUrl = await generatePresignedUrl(this.config, s3SubPath, this.encryptionSecret, null, true, null, cacheOptions);
                result.download_url = downloadUrl;
                console.log(`为文件[${result.name}]生成预签名URL: ✓预览 ✓下载`);
              }
            } catch (urlError) {
              console.warn(`生成URL失败: ${urlError.message}`);
            }
          }

          console.log(`getFileInfo - ListObjects 成功获取文件信息: ${result.name}`);
          return result;
        } catch (listError) {
          // 如果 ListObjects 失败，fallback 到 GET 方法
          console.log(`getFileInfo - ListObjects 失败，fallback 到 GET 方法: ${listError.message}`);

          try {
            const getParams = {
              Bucket: this.config.bucket_name,
              Key: s3SubPath,
              Range: "bytes=0-0", // 只获取第一个字节来检查文件存在性
            };

            const getCommand = new GetObjectCommand(getParams);
            const getResponse = await this.s3Client.send(getCommand);

            const fileName = path.split("/").filter(Boolean).pop() || "/";

            // 检查是否为目录：基于ContentType判断
            const isDirectory = getResponse.ContentType === "application/x-directory";

            const fileType = isDirectory ? FILE_TYPES.FOLDER : await GetFileType(fileName, db);
            const fileTypeName = isDirectory ? FILE_TYPE_NAMES[FILE_TYPES.FOLDER] : await getFileTypeName(fileName, db);

            const result = {
              path: path,
              name: fileName,
              isDirectory: isDirectory,
              size: isDirectory ? 0 : getResponse.ContentLength || 0, // 目录大小为0
              modified: getResponse.LastModified ? getResponse.LastModified.toISOString() : new Date().toISOString(),
              mimetype: getResponse.ContentType || "application/octet-stream", // 统一使用mimetype字段名
              etag: getResponse.ETag ? getResponse.ETag.replace(/"/g, "") : undefined,
              mount_id: mount.id,
              storage_type: mount.storage_type,
              type: fileType, // 整数类型常量 (0-6)
              typeName: fileTypeName, // 类型名称（用于调试）
            };

            // 生成预签名URL（如果需要）
            if (userType && userId) {
              try {
                const cacheOptions = {
                  userType,
                  userId,
                  enableCache: mount.cache_ttl > 0,
                };

                // 根据挂载点配置决定URL类型（兼容数据库的0/1和布尔值）
                if (!!mount.web_proxy && this.driver?.hasCapability?.(CAPABILITIES.PROXY)) {
                  // 代理模式：使用驱动的代理能力生成URL
                  try {
                    const previewProxy = await this.driver.generateProxyUrl(path, { mount, request, download: false, db });
                    const downloadProxy = await this.driver.generateProxyUrl(path, { mount, request, download: true, db });

                    result.preview_url = previewProxy.url;
                    result.download_url = downloadProxy.url;
                    console.log(`为文件[${result.name}]生成代理URL(GET): ✓预览 ✓下载`);
                  } catch (error) {
                    console.warn(`代理URL生成失败，回退到预签名URL:`, error);
                    // 回退到预签名URL
                    const previewUrl = await generatePresignedUrl(this.config, s3SubPath, this.encryptionSecret, null, false, null, cacheOptions);
                    result.preview_url = previewUrl;

                    const downloadUrl = await generatePresignedUrl(this.config, s3SubPath, this.encryptionSecret, null, true, null, cacheOptions);
                    result.download_url = downloadUrl;
                  }
                } else {
                  // 直链模式：返回S3预签名URL（保持现有逻辑）
                  const previewUrl = await generatePresignedUrl(this.config, s3SubPath, this.encryptionSecret, null, false, null, cacheOptions);
                  result.preview_url = previewUrl;

                  const downloadUrl = await generatePresignedUrl(this.config, s3SubPath, this.encryptionSecret, null, true, null, cacheOptions);
                  result.download_url = downloadUrl;
                  console.log(`为文件[${result.name}]生成预签名URL(GET): ✓预览 ✓下载`);
                }
              } catch (urlError) {
                console.warn(`生成URL失败(GET): ${urlError.message}`);
              }
            }

            console.log(`getFileInfo(GET) - 文件[${result.name}], S3 ContentType[${getResponse.ContentType}]`);
            return result;
          } catch (getError) {
            // 检查是否是NotFound错误，转换为HTTPException
            if (getError.$metadata?.httpStatusCode === 404 || getError.name === "NotFound") {
              throw new HTTPException(ApiStatus.NOT_FOUND, { message: "文件不存在" });
            }

            throw getError;
          }
        }
      },
      "获取文件信息",
      "获取文件信息失败"
    );
  }

  /**
   * 下载文件
   * @param {string} s3SubPath - S3子路径
   * @param {string} fileName - 文件名
   * @param {Request} request - 请求对象
   * @returns {Promise<Response>} 文件响应
   */
  async downloadFile(s3SubPath, fileName, request = null) {
    return handleFsError(
      async () => {
        // 使用现有的getFileFromS3函数
        return await this.getFileFromS3(this.config, s3SubPath, fileName, false, this.encryptionSecret, request);
      },
      "下载文件",
      "下载文件失败"
    );
  }

  /**
   * 生成文件预签名URL
   * @param {string} s3SubPath - S3子路径
   * @param {Object} options - 选项参数
   * @returns {Promise<Object>} 预签名URL信息
   */
  async generatePresignedUrl(s3SubPath, options = {}) {
    const { expiresIn = 604800, forceDownload = false, userType, userId, mount } = options;

    return handleFsError(
      async () => {
        const cacheOptions = {
          userType,
          userId,
          enableCache: mount?.cache_ttl > 0,
        };

        const presignedUrl = await generatePresignedUrl(this.config, s3SubPath, this.encryptionSecret, expiresIn, forceDownload, null, cacheOptions);

        // 提取文件名
        const fileName = s3SubPath.split("/").filter(Boolean).pop() || "file";

        return {
          success: true,
          presignedUrl: presignedUrl,
          name: fileName,
          expiresIn: expiresIn,
          expiresAt: new Date(Date.now() + expiresIn * 1000).toISOString(),
          forceDownload: forceDownload,
        };
      },
      "获取文件预签名URL",
      "获取文件预签名URL失败"
    );
  }

  /**
   * 检查文件是否存在
   * @param {string} s3SubPath - S3子路径
   * @returns {Promise<boolean>} 是否存在
   */
  async exists(s3SubPath) {
    try {
      const listParams = {
        Bucket: this.config.bucket_name,
        Prefix: s3SubPath,
        MaxKeys: 1,
      };

      const listCommand = new ListObjectsV2Command(listParams);
      const listResponse = await this.s3Client.send(listCommand);

      // 检查是否找到精确匹配的文件
      const exactMatch = listResponse.Contents?.find((item) => item.Key === s3SubPath);
      return !!exactMatch;
    } catch (error) {
      return false;
    }
  }

  /**
   * 更新文件内容
   * @param {string} s3SubPath - S3子路径
   * @param {string|ArrayBuffer} content - 新内容
   * @param {Object} options - 选项参数
   * @returns {Promise<Object>} 更新结果
   */
  async updateFile(s3SubPath, content, options = {}) {
    const { fileName } = options;

    return handleFsError(
      async () => {
        const MAX_FILE_SIZE = 10 * 1024 * 1024; // 10MB

        // 检查内容大小
        if (typeof content === "string" && content.length > MAX_FILE_SIZE) {
          throw new HTTPException(ApiStatus.BAD_REQUEST, { message: "文件内容过大，超过最大限制(10MB)" });
        } else if (content instanceof ArrayBuffer && content.byteLength > MAX_FILE_SIZE) {
          throw new HTTPException(ApiStatus.BAD_REQUEST, { message: "文件内容过大，超过最大限制(10MB)" });
        }

        // 推断MIME类型
        const contentType = getMimeTypeFromFilename(fileName || s3SubPath);

        // 首先检查文件是否存在，获取原始元数据
        let originalMetadata = null;
        try {
          const listParams = {
            Bucket: this.config.bucket_name,
            Prefix: s3SubPath,
            MaxKeys: 1,
          };
          const listCommand = new ListObjectsV2Command(listParams);
          const listResponse = await this.s3Client.send(listCommand);

          // 检查是否找到精确匹配的文件
          const exactMatch = listResponse.Contents?.find((item) => item.Key === s3SubPath);
          if (exactMatch) {
            originalMetadata = {
              LastModified: exactMatch.LastModified,
              ETag: exactMatch.ETag,
              Size: exactMatch.Size,
            };
          }
        } catch (error) {
          console.warn(`获取原始文件元数据失败: ${error.message}`);
          // 错误表示无法获取元数据，这是正常的（创建新文件）
        }

        const putParams = {
          Bucket: this.config.bucket_name,
          Key: s3SubPath,
          Body: content,
          ContentType: contentType,
        };

        console.log(`准备更新S3对象: ${s3SubPath}, 内容类型: ${contentType}`);
        const putCommand = new PutObjectCommand(putParams);
        const result = await this.s3Client.send(putCommand);

        // 更新父目录的修改时间
        await updateParentDirectoriesModifiedTime(this.s3Client, this.config.bucket_name, s3SubPath);

        return {
          success: true,
          path: s3SubPath,
          etag: result.ETag ? result.ETag.replace(/"/g, "") : undefined,
          mimetype: contentType,
          message: "文件更新成功",
          isNewFile: !originalMetadata,
        };
      },
      "更新文件",
      "更新文件失败"
    );
  }

  /**
   * 重命名文件
   * @param {string} oldS3SubPath - 原S3子路径
   * @param {string} newS3SubPath - 新S3子路径
   * @param {Object} options - 选项参数
   * @returns {Promise<Object>} 重命名结果
   */
  async renameFile(oldS3SubPath, newS3SubPath, options = {}) {
    return handleFsError(
      async () => {
        // 检查源文件是否存在
        const sourceExists = await this.exists(oldS3SubPath);
        if (!sourceExists) {
          throw new HTTPException(ApiStatus.NOT_FOUND, { message: "源文件不存在" });
        }

        // 检查目标文件是否已存在
        const targetExists = await this.exists(newS3SubPath);
        if (targetExists) {
          throw new HTTPException(ApiStatus.CONFLICT, { message: "目标文件已存在" });
        }

        // 复制文件到新位置
        const copyParams = {
          Bucket: this.config.bucket_name,
          CopySource: encodeURIComponent(this.config.bucket_name + "/" + oldS3SubPath),
          Key: newS3SubPath,
        };

        const copyCommand = new CopyObjectCommand(copyParams);
        await this.s3Client.send(copyCommand);

        // 删除原文件
        const deleteParams = {
          Bucket: this.config.bucket_name,
          Key: oldS3SubPath,
        };

        const deleteCommand = new DeleteObjectCommand(deleteParams);
        await this.s3Client.send(deleteCommand);

        return {
          success: true,
          oldPath: oldS3SubPath,
          newPath: newS3SubPath,
          message: "文件重命名成功",
        };
      },
      "重命名文件",
      "重命名文件失败"
    );
  }

  /**
   * 复制单个文件
   * @param {string} sourceS3SubPath - 源S3子路径
   * @param {string} targetS3SubPath - 目标S3子路径
   * @param {Object} options - 选项参数
   * @returns {Promise<Object>} 复制结果
   */
  async copyFile(sourceS3SubPath, targetS3SubPath, options = {}) {
    const { skipExisting = true } = options;

    try {
      // 检查源文件是否存在
      const sourceExists = await this.exists(sourceS3SubPath);
      if (!sourceExists) {
        throw new HTTPException(ApiStatus.NOT_FOUND, { message: "源文件不存在" });
      }

      // 检查目标文件是否已存在
      if (skipExisting) {
        const targetExists = await this.exists(targetS3SubPath);
        if (targetExists) {
          // 文件已存在，跳过
          return {
            success: true,
            skipped: true,
            source: sourceS3SubPath,
            target: targetS3SubPath,
            message: "文件已存在，跳过复制",
          };
        }
      }

      // 执行复制
      const copyParams = {
        Bucket: this.config.bucket_name,
        CopySource: encodeURIComponent(this.config.bucket_name + "/" + sourceS3SubPath),
        Key: targetS3SubPath,
        MetadataDirective: "COPY", // 保持原有元数据
      };

      const copyCommand = new CopyObjectCommand(copyParams);
      await this.s3Client.send(copyCommand);

      // 更新父目录的修改时间
      await updateParentDirectoriesModifiedTime(this.s3Client, this.config.bucket_name, targetS3SubPath);

      return {
        success: true,
        skipped: false,
        source: sourceS3SubPath,
        target: targetS3SubPath,
        message: "文件复制成功",
      };
    } catch (error) {
      console.error("复制文件失败:", error);

      if (error.$metadata?.httpStatusCode === 404) {
        throw new HTTPException(ApiStatus.NOT_FOUND, { message: "源文件不存在" });
      }

      throw new HTTPException(ApiStatus.INTERNAL_ERROR, {
        message: `复制文件失败: ${error.message}`,
      });
    }
  }
}
