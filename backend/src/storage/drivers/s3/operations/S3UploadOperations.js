/**
 * S3上传操作模块
 * 负责文件上传相关操作：直接上传、分片上传（前端分片）、预签名上传等
 */

import { HTTPException } from "hono/http-exception";
import { ApiStatus } from "../../../../constants/index.js";
import { generatePresignedPutUrl, buildS3Url } from "../../../../utils/s3Utils.js";
import { S3Client, PutObjectCommand, ListMultipartUploadsCommand, ListPartsCommand, UploadPartCommand } from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";
import { updateMountLastUsed } from "../../../fs/utils/MountResolver.js";
import { getMimeTypeFromFilename } from "../../../../utils/fileUtils.js";

import { clearDirectoryCache } from "../../../../cache/index.js";
import { handleFsError } from "../../../fs/utils/ErrorHandler.js";
import { updateParentDirectoriesModifiedTime } from "../utils/S3DirectoryUtils.js";
import { getEnvironmentOptimizedUploadConfig, convertStreamForAWSCompatibility } from "../../../../utils/environmentUtils.js";

export class S3UploadOperations {
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
   * 待废弃，使用uploadStream代替
   * 直接上传文件
   * @param {string} s3SubPath - S3子路径
   * @param {File} file - 文件对象
   * @param {Object} options - 选项参数
   * @returns {Promise<Object>} 上传结果
   */
  async uploadFile(s3SubPath, file, options = {}) {
    const { mount, db } = options;

    return handleFsError(
      async () => {
        // 统一从文件名推断MIME类型，不依赖file.type
        const contentType = getMimeTypeFromFilename(file.name);
        console.log(`直接上传：从文件名[${file.name}]推断MIME类型: ${contentType}`);

        // 构建最终的S3路径
        const fileName = file.name;
        let finalS3Path;

        // 智能检查s3SubPath是否已经包含完整的文件路径
        const { isCompleteFilePath } = await import("../utils/S3PathUtils.js");
        if (s3SubPath && isCompleteFilePath(s3SubPath, fileName)) {
          // s3SubPath已经包含完整的文件路径，直接使用
          finalS3Path = s3SubPath;
        } else {
          // s3SubPath是目录路径，需要拼接文件名
          if (s3SubPath && !s3SubPath.endsWith("/")) {
            finalS3Path = s3SubPath + "/" + fileName;
          } else {
            finalS3Path = s3SubPath + fileName;
          }
        }

        console.log(`构建S3路径: subPath=[${s3SubPath}], fileName=[${fileName}], finalPath=[${finalS3Path}]`);

        // 读取文件内容
        const fileContent = await file.arrayBuffer();

        // 准备上传参数
        const putParams = {
          Bucket: this.config.bucket_name,
          Key: finalS3Path,
          Body: fileContent,
          ContentType: contentType,
        };

        // 执行上传
        const putCommand = new PutObjectCommand(putParams);
        const result = await this.s3Client.send(putCommand);

        // 构建S3 URL
        const { buildS3Url } = await import("../../../../utils/s3Utils.js");
        const s3Url = buildS3Url(this.config, finalS3Path);

        // 更新父目录的修改时间
        const { updateParentDirectoriesModifiedTime } = await import("../utils/S3DirectoryUtils.js");
        await updateParentDirectoriesModifiedTime(this.s3Client, this.config.bucket_name, finalS3Path);

        // 更新最后使用时间
        if (db && mount.id) {
          await updateMountLastUsed(db, mount.id);
        }

        // 清除缓存
        if (mount) {
          await clearDirectoryCache({ mountId: mount.id });
        }

        return {
          success: true,
          fileName: fileName,
          size: file.size,
          contentType: contentType,
          s3Path: finalS3Path,
          s3Url: s3Url,
          etag: result.ETag ? result.ETag.replace(/"/g, "") : null,
          message: "文件上传成功",
        };
      },
      "上传文件",
      "上传文件失败"
    );
  }

  /**
   * 上传流式数据
   * @param {string} s3SubPath - S3子路径
   * @param {ReadableStream} stream - 数据流
   * @param {Object} options - 选项参数
   * @returns {Promise<Object>} 上传结果
   */
  async uploadStream(s3SubPath, stream, options = {}) {
    const { mount, db, filename, contentType, contentLength, useMultipart = false } = options;

    return handleFsError(
      async () => {
        // 构建最终的S3路径
        let finalS3Path;
        // 智能检查s3SubPath是否已经包含完整的文件路径
        const { isCompleteFilePath } = await import("../utils/S3PathUtils.js");
        if (s3SubPath && isCompleteFilePath(s3SubPath, filename)) {
          finalS3Path = s3SubPath;
        } else if (s3SubPath && !s3SubPath.endsWith("/")) {
          finalS3Path = s3SubPath + "/" + filename;
        } else {
          finalS3Path = s3SubPath + filename;
        }

        let result;
        let etag;

        if (useMultipart) {
          // 使用AWS SDK Upload类进行流式分片上传
          const { Upload } = await import("@aws-sdk/lib-storage");

          // 获取环境自适应的上传配置
          const uploadConfig = getEnvironmentOptimizedUploadConfig();

          console.log(`S3流式分片上传 - 环境: ${uploadConfig.environment}, 分片: ${uploadConfig.partSize / 1024 / 1024}MB, 并发: ${uploadConfig.queueSize}`);

          const upload = new Upload({
            client: this.s3Client,
            params: {
              Bucket: this.config.bucket_name,
              Key: finalS3Path,
              Body: stream,
              ContentType: contentType,
            },
            queueSize: uploadConfig.queueSize,
            partSize: uploadConfig.partSize,
            leavePartsOnError: false, // 出错时自动清理分片
          });

          // 监听上传进度
          let lastProgressLog = 0;
          upload.on("httpUploadProgress", (progress) => {
            const { loaded = 0, total = contentLength } = progress;

            // 每50MB记录一次进度，与成功代码完全相同
            const REDUCED_LOG_INTERVAL = 50 * 1024 * 1024; // 50MB
            if (loaded - lastProgressLog >= REDUCED_LOG_INTERVAL || loaded === total) {
              const progressMB = (loaded / (1024 * 1024)).toFixed(2);
              const totalMB = total > 0 ? (total / (1024 * 1024)).toFixed(2) : "未知";
              const percentage = total > 0 ? ((loaded / total) * 100).toFixed(1) : "未知";
              console.log(`WebDAV PUT - 流式上传进度: ${progressMB}MB / ${totalMB}MB (${percentage}%)`);
              lastProgressLog = loaded;
            }
          });

          // 执行上传
          const startTime = Date.now();
          result = await upload.done();
          const duration = Date.now() - startTime;
          const speedMBps = contentLength > 0 ? (contentLength / 1024 / 1024 / (duration / 1000)).toFixed(2) : "未知";

          console.log(`WebDAV PUT - 流式上传完成，用时: ${duration}ms，平均速度: ${speedMBps}MB/s`);

          etag = result.ETag ? result.ETag.replace(/"/g, "") : undefined;
        } else {
          // 使用PutObjectCommand进行直接流式上传
          console.log(`WebDAV PUT [${filename}]: 开始直接上传 (${(contentLength / 1024 / 1024).toFixed(1)}MB)`);

          // 处理AWS SDK v3在Node.js环境中的ReadableStream兼容性问题
          const compatibleStream = await convertStreamForAWSCompatibility(stream);

          const putCommand = new PutObjectCommand({
            Bucket: this.config.bucket_name,
            Key: finalS3Path,
            Body: compatibleStream,
            ContentType: contentType,
            ContentLength: contentLength > 0 ? contentLength : undefined,
          });

          result = await this.s3Client.send(putCommand);

          etag = result.ETag ? result.ETag.replace(/"/g, "") : undefined;

          console.log(`WebDAV PUT [${filename}]: 直接上传完成 100%`);
        }

        // 更新父目录的修改时间
        await updateParentDirectoriesModifiedTime(this.s3Client, this.config.bucket_name, finalS3Path);

        // 更新最后使用时间
        if (db && mount.id) {
          await updateMountLastUsed(db, mount.id);
        }

        // 清除缓存
        if (mount) {
          await clearDirectoryCache({ mountId: mount.id });
        }

        // 构建S3 URL
        const s3Url = buildS3Url(this.config, finalS3Path);

        return {
          success: true,
          message: useMultipart ? "流式分片上传成功" : "流式直接上传成功",
          s3Path: finalS3Path,
          s3Url: s3Url,
          etag: etag,
          contentType: contentType,
        };
      },
      "流式上传",
      "流式上传失败"
    );
  }

  /**
   * 生成预签名上传URL
   * @param {string} s3SubPath - S3子路径
   * @param {Object} options - 选项参数
   * @returns {Promise<Object>} 预签名上传URL信息
   */
  async generatePresignedUploadUrl(s3SubPath, options = {}) {
    const { fileName, fileSize, expiresIn = 3600 } = options;

    return handleFsError(
      async () => {
        // 推断MIME类型
        const contentType = getMimeTypeFromFilename(fileName);

        const presignedUrl = await generatePresignedPutUrl(this.config, s3SubPath, contentType, this.encryptionSecret, expiresIn);

        // 生成S3直接访问URL
        const s3Url = buildS3Url(this.config, s3SubPath);

        return {
          success: true,
          uploadUrl: presignedUrl,
          s3Url: s3Url,
          contentType: contentType,
          expiresIn: expiresIn,
          s3Path: s3SubPath,
          fileName: fileName,
          fileSize: fileSize,
        };
      },
      "生成预签名上传URL",
      "生成预签名上传URL失败"
    );
  }

  /**
   * 处理上传完成后的操作
   * @param {string} s3SubPath - S3子路径
   * @param {Object} options - 选项参数
   * @returns {Promise<Object>} 处理结果
   */
  async handleUploadComplete(s3SubPath, options = {}) {
    const { mount, db, fileName, fileSize, contentType, etag } = options;

    try {
      // 更新父目录的修改时间
      const rootPrefix = this.config.root_prefix ? (this.config.root_prefix.endsWith("/") ? this.config.root_prefix : this.config.root_prefix + "/") : "";
      await updateParentDirectoriesModifiedTime(this.s3Client, this.config.bucket_name, s3SubPath, rootPrefix);

      // 更新最后使用时间
      if (db && mount.id) {
        await updateMountLastUsed(db, mount.id);
      }

      // 清除缓存
      if (mount) {
        await clearDirectoryCache({ mountId: mount.id });
      }

      // 构建S3 URL
      const s3Url = buildS3Url(this.config, s3SubPath);

      return {
        success: true,
        message: "上传完成处理成功",
        fileName: fileName,
        size: fileSize,
        contentType: contentType,
        s3Path: s3SubPath,
        s3Url: s3Url,
        etag: etag ? etag.replace(/"/g, "") : null,
      };
    } catch (error) {
      console.error("处理上传完成失败:", error);
      throw new HTTPException(ApiStatus.INTERNAL_ERROR, {
        message: `处理上传完成失败: ${error.message}`,
      });
    }
  }

  /**
   * 取消上传操作
   * @param {string} s3SubPath - S3子路径
   * @param {Object} options - 选项参数
   * @returns {Promise<Object>} 取消结果
   */
  async cancelUpload(s3SubPath, options = {}) {
    const { uploadId } = options;

    try {
      if (uploadId) {
        // 取消分片上传
        const { AbortMultipartUploadCommand } = await import("@aws-sdk/client-s3");
        const abortParams = {
          Bucket: this.config.bucket_name,
          Key: s3SubPath,
          UploadId: uploadId,
        };

        const abortCommand = new AbortMultipartUploadCommand(abortParams);
        await this.s3Client.send(abortCommand);
      }

      return {
        success: true,
        message: "上传已取消",
      };
    } catch (error) {
      console.error("取消上传失败:", error);
      throw new HTTPException(ApiStatus.INTERNAL_ERROR, {
        message: `取消上传失败: ${error.message}`,
      });
    }
  }

  /**
   * 初始化前端分片上传（生成预签名URL列表）
   * @param {string} s3SubPath - S3子路径
   * @param {Object} options - 选项参数
   * @returns {Promise<Object>} 初始化结果
   */
  async initializeFrontendMultipartUpload(s3SubPath, options = {}) {
    const { fileName, fileSize, partSize = 5 * 1024 * 1024, partCount, mount, db } = options;

    return handleFsError(
      async () => {
        // 推断MIME类型
        const contentType = getMimeTypeFromFilename(fileName);
        console.log(`初始化前端分片上传：从文件名[${fileName}]推断MIME类型: ${contentType}`);

        // 构建最终的S3路径
        let finalS3Path;

        // 智能检查s3SubPath是否已经包含完整的文件路径
        const { isCompleteFilePath } = await import("../utils/S3PathUtils.js");
        if (s3SubPath && isCompleteFilePath(s3SubPath, fileName)) {
          // s3SubPath已经是完整的文件路径，直接使用
          finalS3Path = s3SubPath;
        } else if (s3SubPath && !s3SubPath.endsWith("/")) {
          // s3SubPath是目录路径，需要拼接文件名
          finalS3Path = s3SubPath + "/" + fileName;
        } else {
          // s3SubPath为空或以斜杠结尾，直接拼接文件名
          finalS3Path = s3SubPath + fileName;
        }

        // 创建分片上传
        const { CreateMultipartUploadCommand } = await import("@aws-sdk/client-s3");
        const createCommand = new CreateMultipartUploadCommand({
          Bucket: this.config.bucket_name,
          Key: finalS3Path,
          ContentType: contentType,
        });

        const createResponse = await this.s3Client.send(createCommand);
        const uploadId = createResponse.UploadId;

        // 计算分片数量
        const calculatedPartCount = partCount || Math.ceil(fileSize / partSize);

        // 生成预签名URL列表
        const presignedUrls = [];
        const { UploadPartCommand } = await import("@aws-sdk/client-s3");

        for (let partNumber = 1; partNumber <= calculatedPartCount; partNumber++) {
          const uploadPartCommand = new UploadPartCommand({
            Bucket: this.config.bucket_name,
            Key: finalS3Path,
            UploadId: uploadId,
            PartNumber: partNumber,
          });

          // 生成预签名URL
          const presignedUrl = await getSignedUrl(this.s3Client, uploadPartCommand, {
            expiresIn: this.config.signature_expires_in || 3600,
          });

          presignedUrls.push({
            partNumber: partNumber,
            url: presignedUrl,
          });
        }

        // 更新最后使用时间
        if (db && mount.id) {
          await updateMountLastUsed(db, mount.id);
        }

        return {
          uploadId: uploadId,
          bucket: this.config.bucket_name,
          key: finalS3Path,
          partSize: partSize,
          partCount: calculatedPartCount,
          presignedUrls: presignedUrls,
          mount_id: mount ? mount.id : null,
          path: mount ? mount.mount_path + s3SubPath : s3SubPath,
          storage_type: mount ? mount.storage_type : "S3",
        };
      },
      "初始化前端分片上传",
      "初始化前端分片上传失败"
    );
  }

  /**
   * 完成前端分片上传
   * @param {string} s3SubPath - S3子路径
   * @param {Object} options - 选项参数
   * @returns {Promise<Object>} 完成结果
   */
  async completeFrontendMultipartUpload(s3SubPath, options = {}) {
    const { uploadId, parts, fileName, fileSize, mount, db } = options;

    return handleFsError(
      async () => {
        // 构建最终的S3路径
        let finalS3Path;

        // 智能检查s3SubPath是否已经包含完整的文件路径
        const { isCompleteFilePath } = await import("../utils/S3PathUtils.js");
        if (s3SubPath && isCompleteFilePath(s3SubPath, fileName)) {
          // s3SubPath已经是完整的文件路径，直接使用
          finalS3Path = s3SubPath;
        } else if (s3SubPath && !s3SubPath.endsWith("/")) {
          // s3SubPath是目录路径，需要拼接文件名
          finalS3Path = s3SubPath + "/" + fileName;
        } else {
          // s3SubPath为空或以斜杠结尾，直接拼接文件名
          finalS3Path = s3SubPath + fileName;
        }

        // 验证parts格式
        const validatedParts = parts.map((part) => {
          if (!part.PartNumber || !part.ETag) {
            throw new Error(`分片数据不完整: PartNumber=${part.PartNumber}, ETag=${part.ETag}`);
          }

          return {
            PartNumber: part.PartNumber,
            ETag: part.ETag,
          };
        });

        // 确保parts按照PartNumber排序
        const sortedParts = [...validatedParts].sort((a, b) => a.PartNumber - b.PartNumber);

        // 完成分片上传
        const { CompleteMultipartUploadCommand } = await import("@aws-sdk/client-s3");
        const completeCommand = new CompleteMultipartUploadCommand({
          Bucket: this.config.bucket_name,
          Key: finalS3Path,
          UploadId: uploadId,
          MultipartUpload: {
            Parts: sortedParts,
          },
        });

        const completeResponse = await this.s3Client.send(completeCommand);

        // 更新最后使用时间
        if (db && mount.id) {
          await updateMountLastUsed(db, mount.id);
        }

        // 清除缓存
        if (mount) {
          await clearDirectoryCache({ mountId: mount.id });
        }

        // 推断MIME类型
        const contentType = getMimeTypeFromFilename(fileName);

        // 构建S3 URL
        const s3Url = buildS3Url(this.config, finalS3Path);

        // 文件上传完成，无需数据库操作

        return {
          success: true,
          fileName: fileName,
          size: fileSize,
          contentType: contentType,
          s3Path: finalS3Path,
          s3Url: s3Url,
          etag: completeResponse.ETag ? completeResponse.ETag.replace(/"/g, "") : null,
          location: completeResponse.Location,
          message: "前端分片上传完成",
        };
      },
      "完成前端分片上传",
      "完成前端分片上传失败"
    );
  }

  /**
   * 中止前端分片上传
   * @param {string} s3SubPath - S3子路径
   * @param {Object} options - 选项参数
   * @returns {Promise<Object>} 中止结果
   */
  async abortFrontendMultipartUpload(s3SubPath, options = {}) {
    const { uploadId, fileName, mount, db } = options;

    return handleFsError(
      async () => {
        // 构建最终的S3路径（包含文件名）
        let finalS3Path;
        // 智能检查s3SubPath是否已经包含完整的文件路径
        const { isCompleteFilePath } = await import("../utils/S3PathUtils.js");
        if (s3SubPath && isCompleteFilePath(s3SubPath, fileName)) {
          finalS3Path = s3SubPath;
        } else if (s3SubPath && !s3SubPath.endsWith("/")) {
          finalS3Path = s3SubPath + "/" + fileName;
        } else {
          finalS3Path = s3SubPath + fileName;
        }

        console.log(`中止前端分片上传: Bucket=${this.config.bucket_name}, Key=${finalS3Path}, UploadId=${uploadId}`);

        // 中止分片上传
        const { AbortMultipartUploadCommand } = await import("@aws-sdk/client-s3");
        const abortCommand = new AbortMultipartUploadCommand({
          Bucket: this.config.bucket_name,
          Key: finalS3Path,
          UploadId: uploadId,
        });

        await this.s3Client.send(abortCommand);

        // 更新最后使用时间
        if (db && mount.id) {
          await updateMountLastUsed(db, mount.id);
        }

        return {
          success: true,
          message: "前端分片上传已中止",
        };
      },
      "中止前端分片上传",
      "中止前端分片上传失败"
    );
  }

  /**
   * 列出进行中的分片上传
   * @param {string} s3SubPath - S3子路径（可选，用于过滤特定文件的上传）
   * @param {Object} options - 选项参数
   * @returns {Promise<Object>} 进行中的上传列表
   */
  async listMultipartUploads(s3SubPath = "", options = {}) {
    const { maxUploads = 1000, keyMarker, uploadIdMarker } = options;

    return handleFsError(
      async () => {
        const listCommand = new ListMultipartUploadsCommand({
          Bucket: this.config.bucket_name,
          Prefix: s3SubPath,
          MaxUploads: maxUploads,
          KeyMarker: keyMarker,
          UploadIdMarker: uploadIdMarker,
        });

        const response = await this.s3Client.send(listCommand);

        // 格式化响应数据
        const uploads = (response.Uploads || []).map((upload) => ({
          key: upload.Key,
          uploadId: upload.UploadId,
          initiated: upload.Initiated,
          storageClass: upload.StorageClass,
          owner: upload.Owner,
        }));

        return {
          success: true,
          uploads: uploads,
          bucket: response.Bucket,
          keyMarker: response.KeyMarker,
          uploadIdMarker: response.UploadIdMarker,
          nextKeyMarker: response.NextKeyMarker,
          nextUploadIdMarker: response.NextUploadIdMarker,
          maxUploads: response.MaxUploads,
          isTruncated: response.IsTruncated,
          prefix: response.Prefix,
        };
      },
      "列出进行中的分片上传",
      "列出进行中的分片上传失败"
    );
  }

  /**
   * 列出已上传的分片
   * @param {string} s3SubPath - S3子路径
   * @param {string} uploadId - 上传ID
   * @param {Object} options - 选项参数
   * @returns {Promise<Object>} 已上传的分片列表
   */
  async listMultipartParts(s3SubPath, uploadId, options = {}) {
    const { maxParts = 1000, partNumberMarker } = options;

    try {
      const listCommand = new ListPartsCommand({
        Bucket: this.config.bucket_name,
        Key: s3SubPath,
        UploadId: uploadId,
        MaxParts: maxParts,
        PartNumberMarker: partNumberMarker,
      });

      const response = await this.s3Client.send(listCommand);

      // 格式化响应数据
      const parts = (response.Parts || []).map((part) => ({
        partNumber: part.PartNumber,
        lastModified: part.LastModified,
        etag: part.ETag,
        size: part.Size,
      }));

      return {
        success: true,
        parts: parts,
        bucket: response.Bucket,
        key: response.Key,
        uploadId: response.UploadId,
        partNumberMarker: response.PartNumberMarker,
        nextPartNumberMarker: response.NextPartNumberMarker,
        maxParts: response.MaxParts,
        isTruncated: response.IsTruncated,
        storageClass: response.StorageClass,
        owner: response.Owner,
      };
    } catch (error) {
      // 特殊处理NoSuchUpload错误 - 这是S3生命周期策略清理导致的正常业务场景
      if (error.name === "NoSuchUpload" || (error.message && error.message.includes("The specified multipart upload does not exist"))) {
        console.log(`[S3UploadOperations] NoSuchUpload - uploadId已被S3生命周期策略清理: ${uploadId}`);

        // 返回空分片列表，这是正常的业务场景，不应该报错
        return {
          success: true,
          parts: [],
          bucket: this.config.bucket_name,
          key: s3SubPath,
          uploadId: uploadId,
          partNumberMarker: partNumberMarker,
          nextPartNumberMarker: null,
          maxParts: maxParts,
          isTruncated: false,
          storageClass: null,
          owner: null,
          uploadNotFound: true,
          message: "多部分上传已被S3生命周期策略清理",
        };
      }

      // 其他错误继续抛出
      throw new HTTPException(ApiStatus.INTERNAL_ERROR, {
        message: error.message || "列出已上传的分片失败",
      });
    }
  }

  /**
   * 为现有上传刷新预签名URL
   * @param {string} s3SubPath - S3子路径
   * @param {string} uploadId - 现有的上传ID
   * @param {Array} partNumbers - 需要刷新URL的分片编号数组
   * @param {Object} options - 选项参数
   * @returns {Promise<Object>} 刷新的预签名URL列表
   */
  async refreshMultipartUrls(s3SubPath, uploadId, partNumbers, options = {}) {
    const { expiresIn = 3600 } = options; // 默认1小时过期

    return handleFsError(
      async () => {
        const presignedUrls = [];

        // 为每个分片生成预签名URL
        for (const partNumber of partNumbers) {
          const command = new UploadPartCommand({
            Bucket: this.config.bucket_name,
            Key: s3SubPath,
            UploadId: uploadId,
            PartNumber: partNumber,
          });

          const presignedUrl = await getSignedUrl(this.s3Client, command, {
            expiresIn: expiresIn,
          });

          presignedUrls.push({
            partNumber: partNumber,
            url: presignedUrl,
          });
        }

        return {
          success: true,
          uploadId: uploadId,
          presignedUrls: presignedUrls,
          expiresIn: expiresIn,
        };
      },
      "刷新分片上传预签名URL",
      "刷新分片上传预签名URL失败"
    );
  }
}
