/**
 * 文件系统统一抽象层
 * 同时服务于网页端API和WebDAV协议
 * 内部根据存储能力选择最优实现
 */

import { HTTPException } from "hono/http-exception";
import { ApiStatus } from "../../constants/index.js";
import { CAPABILITIES } from "../interfaces/capabilities/index.js";
import { findMountPointByPath } from "./utils/MountResolver.js";

export class FileSystem {
  /**
   * 构造函数
   * @param {MountManager} mountManager - 挂载管理器实例
   */
  constructor(mountManager) {
    this.mountManager = mountManager;
  }

  /**
   * 列出目录内容
   * @param {string} path - 目录路径
   * @param {string|Object} userIdOrInfo - 用户ID或API密钥信息
   * @param {string} userType - 用户类型
   * @param {Object} options - 选项参数
   * @param {boolean} options.refresh - 是否强制刷新，跳过缓存
   * @returns {Promise<Object>} 目录内容
   */
  async listDirectory(path, userIdOrInfo, userType, options = {}) {
    const { driver, mount, subPath } = await this.mountManager.getDriverByPath(path, userIdOrInfo, userType);

    // 检查驱动是否支持读取能力
    if (!driver.hasCapability(CAPABILITIES.READER)) {
      throw new HTTPException(ApiStatus.NOT_IMPLEMENTED, {
        message: `存储驱动 ${driver.getType()} 不支持读取操作`,
      });
    }

    // 调用驱动的listDirectory方法，传递选项参数
    return await driver.listDirectory(path, {
      mount,
      subPath,
      db: this.mountManager.db,
      ...options,
    });
  }

  /**
   * 获取文件信息
   * @param {string} path - 文件路径
   * @param {string|Object} userIdOrInfo - 用户ID或API密钥信息
   * @param {string} userType - 用户类型
   * @param {Request} request - 请求对象（用于构建完整URL）
   * @returns {Promise<Object>} 文件信息
   */
  async getFileInfo(path, userIdOrInfo, userType, request = null) {
    const { driver, mount, subPath } = await this.mountManager.getDriverByPath(path, userIdOrInfo, userType);

    if (!driver.hasCapability(CAPABILITIES.READER)) {
      throw new HTTPException(ApiStatus.NOT_IMPLEMENTED, {
        message: `存储驱动 ${driver.getType()} 不支持读取操作`,
      });
    }

    return await driver.getFileInfo(path, {
      mount,
      subPath,
      db: this.mountManager.db,
      userType,
      userId: userIdOrInfo,
      request,
    });
  }

  /**
   * 下载文件
   * @param {string} path - 文件路径
   * @param {string} fileName - 文件名
   * @param {Request} request - 请求对象
   * @param {string|Object} userIdOrInfo - 用户ID或API密钥信息
   * @param {string} userType - 用户类型
   * @returns {Promise<Response>} 文件响应
   */
  async downloadFile(path, fileName, request, userIdOrInfo, userType) {
    const { driver, mount, subPath } = await this.mountManager.getDriverByPath(path, userIdOrInfo, userType);

    if (!driver.hasCapability(CAPABILITIES.READER)) {
      throw new HTTPException(ApiStatus.NOT_IMPLEMENTED, {
        message: `存储驱动 ${driver.getType()} 不支持读取操作`,
      });
    }

    return await driver.downloadFile(path, {
      mount,
      subPath,
      db: this.mountManager.db,
      request,
      userIdOrInfo,
      userType,
    });
  }

  /**
   * 上传文件
   * @param {string} path - 目标路径
   * @param {File} file - 文件对象
   * @param {string|Object} userIdOrInfo - 用户ID或API密钥信息
   * @param {string} userType - 用户类型
   * @param {Object} options - 选项参数
   * @returns {Promise<Object>} 上传结果
   */
  async uploadFile(path, file, userIdOrInfo, userType, options = {}) {
    const { driver, mount, subPath } = await this.mountManager.getDriverByPath(path, userIdOrInfo, userType);

    if (!driver.hasCapability(CAPABILITIES.WRITER)) {
      throw new HTTPException(ApiStatus.NOT_IMPLEMENTED, {
        message: `存储驱动 ${driver.getType()} 不支持写入操作`,
      });
    }

    return await driver.uploadFile(path, file, {
      mount,
      subPath,
      db: this.mountManager.db,
      userIdOrInfo,
      userType,
      ...options,
    });
  }

  /**
   * 上传流式数据
   * @param {string} path - 目标路径
   * @param {ReadableStream} stream - 数据流
   * @param {string|Object} userIdOrInfo - 用户ID或API密钥信息
   * @param {string} userType - 用户类型
   * @param {Object} options - 选项参数
   * @param {string} options.filename - 文件名
   * @param {string} options.contentType - 内容类型
   * @param {number} options.contentLength - 内容长度
   * @param {boolean} options.useMultipart - 是否使用分片上传
   * @returns {Promise<Object>} 上传结果
   */
  async uploadStream(path, stream, userIdOrInfo, userType, options = {}) {
    const { driver, mount, subPath } = await this.mountManager.getDriverByPath(path, userIdOrInfo, userType);

    if (!driver.hasCapability(CAPABILITIES.WRITER)) {
      throw new HTTPException(ApiStatus.NOT_IMPLEMENTED, {
        message: `存储驱动 ${driver.getType()} 不支持写入操作`,
      });
    }

    // 检查驱动是否支持流式上传
    if (!driver.uploadStream) {
      throw new HTTPException(ApiStatus.NOT_IMPLEMENTED, {
        message: `存储驱动 ${driver.getType()} 不支持流式上传`,
      });
    }

    return await driver.uploadStream(path, stream, {
      mount,
      subPath,
      db: this.mountManager.db,
      userIdOrInfo,
      userType,
      ...options,
    });
  }

  /**
   * 创建目录
   * @param {string} path - 目录路径
   * @param {string|Object} userIdOrInfo - 用户ID或API密钥信息
   * @param {string} userType - 用户类型
   * @param {Object} options - 选项参数
   * @returns {Promise<Object>} 创建结果
   */
  async createDirectory(path, userIdOrInfo, userType) {
    const { driver, mount, subPath } = await this.mountManager.getDriverByPath(path, userIdOrInfo, userType);

    if (!driver.hasCapability(CAPABILITIES.WRITER)) {
      throw new HTTPException(ApiStatus.NOT_IMPLEMENTED, {
        message: `存储驱动 ${driver.getType()} 不支持写入操作`,
      });
    }

    return await driver.createDirectory(path, {
      mount,
      subPath,
      db: this.mountManager.db,
    });
  }

  /**
   * 重命名文件或目录
   * @param {string} oldPath - 原路径
   * @param {string} newPath - 新路径
   * @param {string|Object} userIdOrInfo - 用户ID或API密钥信息
   * @param {string} userType - 用户类型
   * @returns {Promise<Object>} 重命名结果
   */
  async renameItem(oldPath, newPath, userIdOrInfo, userType) {
    const { driver, mount, subPath } = await this.mountManager.getDriverByPath(oldPath, userIdOrInfo, userType);

    if (!driver.hasCapability(CAPABILITIES.ATOMIC)) {
      throw new HTTPException(ApiStatus.NOT_IMPLEMENTED, {
        message: `存储驱动 ${driver.getType()} 不支持原子操作`,
      });
    }

    return await driver.renameItem(oldPath, newPath, {
      mount,
      subPath,
      db: this.mountManager.db,
      userIdOrInfo,
      userType,
    });
  }

  /**
   * 复制文件或目录
   * @param {string} sourcePath - 源路径
   * @param {string} targetPath - 目标路径
   * @param {string|Object} userIdOrInfo - 用户ID或API密钥信息
   * @param {string} userType - 用户类型
   * @param {Object} options - 选项参数
   * @returns {Promise<Object>} 复制结果
   */
  async copyItem(sourcePath, targetPath, userIdOrInfo, userType, options = {}) {
    const { driver, mount, subPath } = await this.mountManager.getDriverByPath(sourcePath, userIdOrInfo, userType);

    if (!driver.hasCapability(CAPABILITIES.ATOMIC)) {
      throw new HTTPException(ApiStatus.NOT_IMPLEMENTED, {
        message: `存储驱动 ${driver.getType()} 不支持原子操作`,
      });
    }

    return await driver.copyItem(sourcePath, targetPath, {
      mount,
      subPath,
      db: this.mountManager.db,
      userIdOrInfo,
      userType,
      findMountPointByPath,
      encryptionSecret: this.mountManager.encryptionSecret,
      ...options,
    });
  }

  /**
   * 批量删除文件和目录
   * @param {Array<string>} paths - 路径数组
   * @param {string|Object} userIdOrInfo - 用户ID或API密钥信息
   * @param {string} userType - 用户类型
   * @returns {Promise<Object>} 批量删除结果
   */
  async batchRemoveItems(paths, userIdOrInfo, userType) {
    if (!paths || paths.length === 0) {
      return { success: 0, failed: [] };
    }

    // 获取第一个路径的驱动来执行批量操作
    const { driver, mount, subPath } = await this.mountManager.getDriverByPath(paths[0], userIdOrInfo, userType);

    if (!driver.hasCapability(CAPABILITIES.WRITER)) {
      throw new HTTPException(ApiStatus.NOT_IMPLEMENTED, {
        message: `存储驱动 ${driver.getType()} 不支持写入操作`,
      });
    }

    // 导入findMountPointByPath函数

    return await driver.batchRemoveItems(paths, {
      mount,
      subPath,
      db: this.mountManager.db,
      userIdOrInfo,
      userType,
      findMountPointByPath,
    });
  }

  /**
   * 批量复制文件和目录
   * @param {Array<Object>} items - 复制项数组，每项包含sourcePath和targetPath
   * @param {string|Object} userIdOrInfo - 用户ID或API密钥信息
   * @param {string} userType - 用户类型
   * @returns {Promise<Object>} 批量复制结果
   */
  async batchCopyItems(items, userIdOrInfo, userType) {
    // 结果统计
    const result = {
      success: 0,
      skipped: 0,
      failed: [],
      details: [],
      crossStorageResults: [], // 用于存储跨存储复制的预签名URL和元数据
    };

    if (!items || items.length === 0) {
      return result;
    }

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

        // 使用skipExisting选项，如果item中有则使用，否则使用默认值
        const skipExisting = item.skipExisting !== undefined ? item.skipExisting : true;

        const copyResult = await this.copyItem(sourcePath, targetPath, userIdOrInfo, userType, { skipExisting });

        // 检查是否为跨存储复制结果
        if (copyResult.crossStorage) {
          // 将跨存储复制结果添加到专门的数组中
          result.crossStorageResults.push(copyResult);
          continue; // 跳过后续处理，继续下一个项目
        }

        // 根据复制结果更新统计
        // 检查复制结果的状态 - 兼容不同的返回格式
        const isSuccess = copyResult.status === "success" || copyResult.success === true;
        const isSkipped = copyResult.skipped === true || copyResult.status === "skipped";

        if (isSuccess || isSkipped) {
          if (isSkipped) {
            result.skipped++;
            console.log(`文件已存在，跳过复制: ${item.sourcePath} -> ${item.targetPath}`);
          } else {
            result.success++;
            console.log(`文件复制成功: ${item.sourcePath} -> ${item.targetPath}`);
          }

          // 如果是目录复制，并且有详细统计，则合并统计数据
          if (copyResult.stats) {
            result.success += copyResult.stats.success || 0;
            result.skipped += copyResult.stats.skipped || 0;

            // 合并失败记录
            if (copyResult.stats.failed > 0 && copyResult.details) {
              copyResult.details.forEach((detail) => {
                if (detail.status === "failed") {
                  result.failed.push({
                    sourcePath: detail.source,
                    targetPath: detail.target,
                    error: detail.error,
                  });
                  console.error(`复制子项失败: ${detail.source} -> ${detail.target}, 错误: ${detail.error}`);
                }
              });
            }

            // 添加详细记录
            if (copyResult.details) {
              result.details = result.details.concat(copyResult.details);
            }
          } else if (copyResult.details && typeof copyResult.details === "object") {
            // 处理S3BatchOperations.js风格的返回格式
            // copyResult.details 包含 {success: number, skipped: number, failed: number}
            const details = copyResult.details;
            if (details.success !== undefined) {
              result.success += details.success;
              console.log(`目录复制统计 - 成功: ${details.success}, 跳过: ${details.skipped}, 失败: ${details.failed}`);
            }
            if (details.skipped !== undefined) {
              result.skipped += details.skipped;
            }
            // 注意：S3BatchOperations的failed是数字，不是数组
            if (details.failed && details.failed > 0) {
              console.warn(`目录复制中有 ${details.failed} 个文件失败`);
            }
          }
        } else {
          // 如果不是成功或跳过，则认为是失败
          const errorMessage = copyResult.message || copyResult.error || "复制失败";
          console.error(`复制失败: ${item.sourcePath} -> ${item.targetPath}, 错误: ${errorMessage}`);
          result.failed.push({
            sourcePath: item.sourcePath,
            targetPath: item.targetPath,
            error: errorMessage,
          });
        }
      } catch (error) {
        // 记录失败信息
        const errorMessage = error instanceof HTTPException ? error.message : error.message || "未知错误";
        console.error(`复制失败: ${item.sourcePath} -> ${item.targetPath}, 错误: ${errorMessage}`, error);

        result.failed.push({
          sourcePath: item.sourcePath,
          targetPath: item.targetPath,
          error: errorMessage,
        });
      }
    }

    // 如果有跨存储复制结果，添加标志到结果中
    if (result.crossStorageResults.length > 0) {
      result.hasCrossStorageOperations = true;
    }

    return result;
  }

  /**
   * 生成预签名URL
   * @param {string} path - 文件路径
   * @param {string|Object} userIdOrInfo - 用户ID或API密钥信息
   * @param {string} userType - 用户类型
   * @param {Object} options - 选项参数
   * @returns {Promise<Object>} 预签名URL信息
   */
  async generatePresignedUrl(path, userIdOrInfo, userType, options = {}) {
    const { driver, mount, subPath } = await this.mountManager.getDriverByPath(path, userIdOrInfo, userType);

    if (!driver.hasCapability(CAPABILITIES.PRESIGNED)) {
      throw new HTTPException(ApiStatus.NOT_IMPLEMENTED, {
        message: `存储驱动 ${driver.getType()} 不支持预签名URL`,
      });
    }

    return await driver.generatePresignedUrl(path, {
      mount,
      subPath,
      db: this.mountManager.db,
      userIdOrInfo,
      userType,
      ...options,
    });
  }

  /**
   * 初始化前端分片上传（生成预签名URL列表）
   * @param {string} path - 完整路径
   * @param {string} fileName - 文件名
   * @param {number} fileSize - 文件大小
   * @param {string|Object} userIdOrInfo - 用户ID或API密钥信息
   * @param {string} userType - 用户类型
   * @param {number} partSize - 分片大小，默认5MB
   * @param {number} partCount - 分片数量
   * @returns {Promise<Object>} 初始化结果
   */
  async initializeFrontendMultipartUpload(path, fileName, fileSize, userIdOrInfo, userType, partSize = 5 * 1024 * 1024, partCount) {
    const { driver, mount, subPath } = await this.mountManager.getDriverByPath(path, userIdOrInfo, userType);

    if (!driver.hasCapability(CAPABILITIES.MULTIPART)) {
      throw new HTTPException(ApiStatus.NOT_IMPLEMENTED, {
        message: `存储驱动 ${driver.getType()} 不支持分片上传`,
      });
    }

    return await driver.initializeFrontendMultipartUpload(subPath, {
      fileName,
      fileSize,
      partSize,
      partCount,
      mount,
      db: this.mountManager.db,
      userIdOrInfo,
      userType,
    });
  }

  /**
   * 完成前端分片上传
   * @param {string} path - 完整路径
   * @param {string} uploadId - 上传ID
   * @param {Array} parts - 分片信息
   * @param {string} fileName - 文件名
   * @param {number} fileSize - 文件大小
   * @param {string|Object} userIdOrInfo - 用户ID或API密钥信息
   * @param {string} userType - 用户类型
   * @returns {Promise<Object>} 完成结果
   */
  async completeFrontendMultipartUpload(path, uploadId, parts, fileName, fileSize, userIdOrInfo, userType) {
    const { driver, mount, subPath } = await this.mountManager.getDriverByPath(path, userIdOrInfo, userType);

    if (!driver.hasCapability(CAPABILITIES.MULTIPART)) {
      throw new HTTPException(ApiStatus.NOT_IMPLEMENTED, {
        message: `存储驱动 ${driver.getType()} 不支持分片上传`,
      });
    }

    return await driver.completeFrontendMultipartUpload(subPath, {
      uploadId,
      parts,
      fileName,
      fileSize,
      mount,
      db: this.mountManager.db,
      userIdOrInfo,
      userType,
    });
  }

  /**
   * 中止前端分片上传
   * @param {string} path - 完整路径
   * @param {string} uploadId - 上传ID
   * @param {string} fileName - 文件名
   * @param {string|Object} userIdOrInfo - 用户ID或API密钥信息
   * @param {string} userType - 用户类型
   * @returns {Promise<Object>} 中止结果
   */
  async abortFrontendMultipartUpload(path, uploadId, fileName, userIdOrInfo, userType) {
    const { driver, mount, subPath } = await this.mountManager.getDriverByPath(path, userIdOrInfo, userType);

    if (!driver.hasCapability(CAPABILITIES.MULTIPART)) {
      throw new HTTPException(ApiStatus.NOT_IMPLEMENTED, {
        message: `存储驱动 ${driver.getType()} 不支持分片上传`,
      });
    }

    return await driver.abortFrontendMultipartUpload(subPath, {
      uploadId,
      fileName,
      mount,
      db: this.mountManager.db,
      userIdOrInfo,
      userType,
    });
  }

  /**
   * 列出进行中的分片上传
   * @param {string} path - 目标路径（可选，用于过滤特定文件的上传）
   * @param {string|Object} userIdOrInfo - 用户ID或API密钥信息
   * @param {string} userType - 用户类型
   * @param {Object} options - 选项参数
   * @returns {Promise<Object>} 进行中的上传列表
   */
  async listMultipartUploads(path = "", userIdOrInfo, userType, options = {}) {
    const { driver, mount, subPath } = await this.mountManager.getDriverByPath(path || "/", userIdOrInfo, userType);

    if (!driver.hasCapability(CAPABILITIES.MULTIPART)) {
      throw new HTTPException(ApiStatus.NOT_IMPLEMENTED, {
        message: `存储驱动 ${driver.getType()} 不支持分片上传`,
      });
    }

    return await driver.listMultipartUploads(subPath, {
      mount,
      db: this.mountManager.db,
      ...options,
    });
  }

  /**
   * 列出已上传的分片
   * @param {string} path - 目标路径
   * @param {string} uploadId - 上传ID
   * @param {string} fileName - 文件名
   * @param {string|Object} userIdOrInfo - 用户ID或API密钥信息
   * @param {string} userType - 用户类型
   * @param {Object} options - 选项参数
   * @returns {Promise<Object>} 已上传的分片列表
   */
  async listMultipartParts(path, uploadId, fileName, userIdOrInfo, userType, options = {}) {
    const { driver, mount, subPath } = await this.mountManager.getDriverByPath(path, userIdOrInfo, userType);

    if (!driver.hasCapability(CAPABILITIES.MULTIPART)) {
      throw new HTTPException(ApiStatus.NOT_IMPLEMENTED, {
        message: `存储驱动 ${driver.getType()} 不支持分片上传`,
      });
    }

    return await driver.listMultipartParts(subPath, uploadId, {
      mount,
      db: this.mountManager.db,
      fileName,
      ...options,
    });
  }

  /**
   * 为现有上传刷新预签名URL
   * @param {string} path - 目标路径
   * @param {string} uploadId - 现有的上传ID
   * @param {Array} partNumbers - 需要刷新URL的分片编号数组
   * @param {string|Object} userIdOrInfo - 用户ID或API密钥信息
   * @param {string} userType - 用户类型
   * @param {Object} options - 选项参数
   * @returns {Promise<Object>} 刷新的预签名URL列表
   */
  async refreshMultipartUrls(path, uploadId, partNumbers, userIdOrInfo, userType, options = {}) {
    const { driver, mount, subPath } = await this.mountManager.getDriverByPath(path, userIdOrInfo, userType);

    if (!driver.hasCapability(CAPABILITIES.MULTIPART)) {
      throw new HTTPException(ApiStatus.NOT_IMPLEMENTED, {
        message: `存储驱动 ${driver.getType()} 不支持分片上传`,
      });
    }

    return await driver.refreshMultipartUrls(subPath, uploadId, partNumbers, {
      mount,
      db: this.mountManager.db,
      ...options,
    });
  }

  // /**
  //  * 初始化后端分片上传 - 已废弃，项目使用前端分片上传
  //  * @deprecated 使用前端分片上传 initializeFrontendMultipartUpload 替代
  //  * @param {string} path - 目标路径
  //  * @param {string|Object} userIdOrInfo - 用户ID或API密钥信息
  //  * @param {string} userType - 用户类型
  //  * @param {string} contentType - 内容类型
  //  * @param {number} fileSize - 文件大小
  //  * @param {string} filename - 文件名
  //  * @returns {Promise<Object>} 初始化结果
  //  */
  // async initializeBackendMultipartUpload(path, userIdOrInfo, userType, contentType, fileSize, filename) {
  //   const { driver, mount, subPath } = await this.mountManager.getDriverByPath(path, userIdOrInfo, userType);

  //   if (!driver.hasCapability(CAPABILITIES.MULTIPART)) {
  //     throw new HTTPException(ApiStatus.NOT_IMPLEMENTED, {
  //       message: `存储驱动 ${driver.getType()} 不支持分片上传`,
  //     });
  //   }

  //   return await driver.initializeBackendMultipartUpload(path, {
  //     mount,
  //     subPath,
  //     db: this.mountManager.db,
  //     contentType,
  //     fileSize,
  //     filename,
  //   });
  // }

  // /**
  //  * 上传后端分片 - 已废弃，项目使用前端分片上传
  //  * @deprecated 使用前端分片上传替代
  //  * @param {string} path - 目标路径
  //  * @param {string|Object} userIdOrInfo - 用户ID或API密钥信息
  //  * @param {string} userType - 用户类型
  //  * @param {string} uploadId - 上传ID
  //  * @param {number} partNumber - 分片编号
  //  * @param {ArrayBuffer} partData - 分片数据
  //  * @param {string} s3Key - S3键（可选）
  //  * @returns {Promise<Object>} 上传结果
  //  */
  // async uploadBackendPart(path, userIdOrInfo, userType, uploadId, partNumber, partData, s3Key = null) {
  //   const { driver, mount, subPath } = await this.mountManager.getDriverByPath(path, userIdOrInfo, userType);

  //   if (!driver.hasCapability(CAPABILITIES.MULTIPART)) {
  //     throw new HTTPException(ApiStatus.NOT_IMPLEMENTED, {
  //       message: `存储驱动 ${driver.getType()} 不支持分片上传`,
  //     });
  //   }

  //   return await driver.uploadBackendPart(path, {
  //     mount,
  //     subPath,
  //     db: this.mountManager.db,
  //     uploadId,
  //     partNumber,
  //     partData,
  //     s3Key,
  //   });
  // }

  // /**
  //  * 完成后端分片上传 - 已废弃，项目使用前端分片上传
  //  * @deprecated 使用前端分片上传 completeFrontendMultipartUpload 替代
  //  * @param {string} path - 目标路径
  //  * @param {string|Object} userIdOrInfo - 用户ID或API密钥信息
  //  * @param {string} userType - 用户类型
  //  * @param {string} uploadId - 上传ID
  //  * @param {Array} parts - 分片信息
  //  * @param {string} contentType - 内容类型
  //  * @param {number} fileSize - 文件大小
  //  * @param {string} s3Key - S3键（可选）
  //  * @returns {Promise<Object>} 完成结果
  //  */
  // async completeBackendMultipartUpload(path, userIdOrInfo, userType, uploadId, parts, contentType, fileSize, s3Key = null) {
  //   const { driver, mount, subPath } = await this.mountManager.getDriverByPath(path, userIdOrInfo, userType);

  //   if (!driver.hasCapability(CAPABILITIES.MULTIPART)) {
  //     throw new HTTPException(ApiStatus.NOT_IMPLEMENTED, {
  //       message: `存储驱动 ${driver.getType()} 不支持分片上传`,
  //     });
  //   }

  //   return await driver.completeBackendMultipartUpload(path, {
  //     mount,
  //     subPath,
  //     db: this.mountManager.db,
  //     uploadId,
  //     parts,
  //     contentType,
  //     fileSize,
  //     userIdOrInfo,
  //     userType,
  //     s3Key,
  //   });
  // }

  /**
   * 中止后端分片上传
   * @param {string} path - 目标路径
   * @param {string|Object} userIdOrInfo - 用户ID或API密钥信息
   * @param {string} userType - 用户类型
   * @param {string} uploadId - 上传ID
   * @param {string} s3Key - S3键（可选）
   * @returns {Promise<Object>} 中止结果
   */
  async abortBackendMultipartUpload(path, userIdOrInfo, userType, uploadId, s3Key = null) {
    const { driver, mount, subPath } = await this.mountManager.getDriverByPath(path, userIdOrInfo, userType);

    if (!driver.hasCapability(CAPABILITIES.MULTIPART)) {
      throw new HTTPException(ApiStatus.NOT_IMPLEMENTED, {
        message: `存储驱动 ${driver.getType()} 不支持分片上传`,
      });
    }

    return await driver.abortBackendMultipartUpload(path, {
      mount,
      subPath,
      db: this.mountManager.db,
      uploadId,
      s3Key,
    });
  }

  /**
   * 检查文件或目录是否存在
   * @param {string} path - 文件或目录路径
   * @param {string|Object} userIdOrInfo - 用户ID或API密钥信息
   * @param {string} userType - 用户类型
   * @returns {Promise<boolean>} 是否存在
   */
  async exists(path, userIdOrInfo, userType) {
    const { driver, mount, subPath } = await this.mountManager.getDriverByPath(path, userIdOrInfo, userType);

    return await driver.exists(path, {
      mount,
      subPath,
      db: this.mountManager.db,
      userIdOrInfo,
      userType,
    });
  }

  /**
   * 更新文件内容
   * @param {string} path - 文件路径
   * @param {string} content - 新内容
   * @param {string|Object} userIdOrInfo - 用户ID或API密钥信息
   * @param {string} userType - 用户类型
   * @returns {Promise<Object>} 更新结果
   */
  async updateFile(path, content, userIdOrInfo, userType) {
    const { driver, mount, subPath } = await this.mountManager.getDriverByPath(path, userIdOrInfo, userType);

    if (!driver.hasCapability(CAPABILITIES.WRITER)) {
      throw new HTTPException(ApiStatus.NOT_IMPLEMENTED, {
        message: `存储驱动 ${driver.getType()} 不支持写入操作`,
      });
    }

    return await driver.updateFile(path, content, {
      mount,
      subPath,
      db: this.mountManager.db,
      userIdOrInfo,
      userType,
    });
  }

  /**
   * 跨存储复制文件
   * @param {string} sourcePath - 源路径
   * @param {string} targetPath - 目标路径
   * @param {string|Object} userIdOrInfo - 用户ID或API密钥信息
   * @param {string} userType - 用户类型
   * @returns {Promise<Object>} 跨存储复制结果
   */
  async handleCrossStorageCopy(sourcePath, targetPath, userIdOrInfo, userType) {
    const { driver } = await this.mountManager.getDriverBByPath(sourcePath, userIdOrInfo, userType);

    if (!driver.hasCapability(CAPABILITIES.ATOMIC)) {
      throw new HTTPException(ApiStatus.NOT_IMPLEMENTED, {
        message: `存储驱动 ${driver.getType()} 不支持原子操作`,
      });
    }

    return await driver.handleCrossStorageCopy(sourcePath, targetPath, {
      db: this.mountManager.db,
      userIdOrInfo,
      userType,
    });
  }

  /**
   * 搜索文件
   * @param {string} query - 搜索查询
   * @param {Object} searchParams - 搜索参数
   * @param {string} searchParams.scope - 搜索范围 ('global', 'mount', 'directory')
   * @param {string} searchParams.mountId - 挂载点ID（当scope为'mount'时）
   * @param {string} searchParams.path - 搜索路径（当scope为'directory'时）
   * @param {number} searchParams.limit - 结果限制数量，默认50
   * @param {number} searchParams.offset - 结果偏移量，默认0
   * @param {string|Object} userIdOrInfo - 用户ID或API密钥信息
   * @param {string} userType - 用户类型
   * @returns {Promise<Object>} 搜索结果
   */
  async searchFiles(query, searchParams, userIdOrInfo, userType) {
    const { scope = "global", mountId, path, limit = 50, offset = 0 } = searchParams;

    // 参数验证
    if (!query || query.trim().length < 2) {
      throw new HTTPException(ApiStatus.BAD_REQUEST, { message: "搜索查询至少需要2个字符" });
    }

    // 验证搜索范围
    if (!["global", "mount", "directory"].includes(scope)) {
      throw new HTTPException(ApiStatus.BAD_REQUEST, { message: "无效的搜索范围" });
    }

    // 验证分页参数
    if (limit < 1 || limit > 200) {
      throw new HTTPException(ApiStatus.BAD_REQUEST, { message: "limit参数必须在1-200之间" });
    }

    if (offset < 0) {
      throw new HTTPException(ApiStatus.BAD_REQUEST, { message: "offset参数不能为负数" });
    }

    // 检查搜索缓存
    const { searchCacheManager } = await import("../../cache/SearchCache.js");
    const cachedResult = searchCacheManager.get(query, searchParams, userType, userIdOrInfo);
    if (cachedResult) {
      console.log(`搜索缓存命中 - 查询: ${query}, 用户类型: ${userType}`);
      return cachedResult;
    }

    // 获取可访问的挂载点 - 权限检查在路由层完成
    let accessibleMounts;
    try {
      const { RepositoryFactory } = await import("../../repositories/index.js");
      const repositoryFactory = new RepositoryFactory(this.mountManager.db);
      const mountRepository = repositoryFactory.getMountRepository();
      accessibleMounts = await mountRepository.findAll(false); // false = 只获取活跃的挂载点
    } catch (error) {
      throw new HTTPException(ApiStatus.UNAUTHORIZED, { message: "未授权访问" });
    }

    if (!accessibleMounts || accessibleMounts.length === 0) {
      return {
        results: [],
        total: 0,
        hasMore: false,
        searchParams: searchParams,
      };
    }

    // 根据搜索范围过滤挂载点
    let targetMounts = accessibleMounts;
    if ((scope === "mount" || scope === "directory") && mountId) {
      targetMounts = accessibleMounts.filter((mount) => mount.id === mountId);
      if (targetMounts.length === 0) {
        throw new HTTPException(ApiStatus.FORBIDDEN, { message: "没有权限访问指定的挂载点" });
      }
    }

    // 并行搜索各个挂载点
    const searchPromises = targetMounts.map(async (mount) => {
      try {
        const driver = await this.mountManager.getDriver(mount);

        // 检查驱动是否支持搜索（通过ReaderCapable）
        if (!driver.hasCapability(CAPABILITIES.READER)) {
          return [];
        }

        return await driver.search(query, {
          mount,
          searchPath: scope === "directory" ? path : null,
          maxResults: 1000,
          db: this.mountManager.db,
        });
      } catch (error) {
        console.warn(`挂载点 ${mount.id} 搜索失败:`, error);
        return [];
      }
    });

    const mountResults = await Promise.allSettled(searchPromises);

    // 聚合搜索结果
    const allResults = [];
    for (const result of mountResults) {
      if (result.status === "fulfilled" && result.value) {
        allResults.push(...result.value);
      }
    }

    // 排序和分页
    const { S3SearchOperations } = await import("../drivers/s3/operations/S3SearchOperations.js");
    const sortedResults = S3SearchOperations.sortSearchResults(allResults, query);
    const total = sortedResults.length;
    const paginatedResults = sortedResults.slice(offset, offset + limit);

    const searchResult = {
      results: paginatedResults,
      total: total,
      hasMore: offset + limit < total,
      searchParams: searchParams,
      mountsSearched: targetMounts.length,
    };

    // 缓存搜索结果（仅当结果不为空时缓存）
    if (total > 0) {
      searchCacheManager.set(query, searchParams, userType, userIdOrInfo, searchResult, 300); // 5分钟缓存
      console.log(`搜索结果已缓存 - 查询: ${query}, 结果数: ${total}, 用户类型: ${userType}`);
    }

    return searchResult;
  }

  /**
   * 获取存储统计信息
   * @param {string} path - 路径（可选，用于特定挂载点的统计）
   * @param {string|Object} userIdOrInfo - 用户ID或API密钥信息
   * @param {string} userType - 用户类型
   * @returns {Promise<Object>} 统计信息
   */
  async getStats(path, userIdOrInfo, userType) {
    if (path) {
      const { driver } = await this.mountManager.getDriverByPath(path, userIdOrInfo, userType);
      return await driver.getStats();
    } else {
      // 返回整个文件系统的统计信息
      return {
        type: "FileSystem",
        mountManager: this.mountManager.constructor.name,
        timestamp: new Date().toISOString(),
      };
    }
  }

  /**
   * 清理资源
   * @returns {Promise<void>}
   */
  async cleanup() {
    // 清理挂载管理器的资源
    if (this.mountManager && typeof this.mountManager.cleanup === "function") {
      await this.mountManager.cleanup();
    }
  }
}
