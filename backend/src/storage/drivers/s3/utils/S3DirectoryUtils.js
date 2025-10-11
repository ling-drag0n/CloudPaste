/**
 * S3目录操作工具
 * 提供S3特定的目录操作功能，如父目录时间更新、目录存在检查等
 */

import { HeadObjectCommand, ListObjectsV2Command, PutObjectCommand, DeleteObjectCommand } from "@aws-sdk/client-s3";
import { createS3Client } from "../../../../utils/s3Utils.js";

/**
 * 统一的父目录时间更新工具函数
 * 根据S3配置和文件路径自动创建S3客户端并更新父目录时间
 * @param {Object} s3Config - S3配置对象
 * @param {string} filePath - 文件或目录路径
 * @param {string} encryptionSecret - 加密密钥
 * @returns {Promise<void>}
 */
export async function updateParentDirectoriesModifiedTimeHelper(s3Config, filePath, encryptionSecret) {
  try {
    const s3Client = await createS3Client(s3Config, encryptionSecret);
    const rootPrefix = s3Config.root_prefix ? (s3Config.root_prefix.endsWith("/") ? s3Config.root_prefix : s3Config.root_prefix + "/") : "";
    await updateParentDirectoriesModifiedTime(s3Client, s3Config.bucket_name, filePath, rootPrefix);
  } catch (error) {
    console.warn(`更新父目录修改时间失败:`, error);
  }
}

/**
 * 更新目录及其所有父目录的修改时间
 * @param {S3Client} s3Client - S3客户端实例
 * @param {string} bucketName - 存储桶名称
 * @param {string} filePath - 文件或目录路径
 * @param {string} rootPrefix - 根前缀
 * @param {boolean} skipMissingDirectories - 是否跳过不存在的目录（用于删除操作）
 */
export async function updateParentDirectoriesModifiedTime(s3Client, bucketName, filePath, rootPrefix = "", skipMissingDirectories = false) {
  try {
    // 获取文件所在的目录路径
    let currentPath = filePath;

    // 如果是文件，获取其父目录
    if (!filePath.endsWith("/")) {
      const lastSlashIndex = filePath.lastIndexOf("/");
      if (lastSlashIndex > 0) {
        currentPath = filePath.substring(0, lastSlashIndex + 1);
      } else {
        // 文件在根目录，无需更新父目录
        return;
      }
    }

    const updatedPaths = new Set();

    // 逐级向上更新父目录
    while (currentPath && currentPath !== "/" && currentPath !== rootPrefix) {
      // 避免重复更新同一路径
      if (updatedPaths.has(currentPath)) {
        break;
      }

      try {
        // 构建完整的S3键
        const s3Key = rootPrefix + currentPath;

        // 检查目录是否存在（通过查找目录标记文件）
        const headCommand = new HeadObjectCommand({
          Bucket: bucketName,
          Key: s3Key,
        });

        let directoryExists = false;
        try {
          await s3Client.send(headCommand);
          directoryExists = true;
        } catch (error) {
          if (error.$metadata?.httpStatusCode !== 404) {
            throw error;
          }
        }

        // 如果目录不存在且不跳过缺失目录，则创建目录标记
        if (!directoryExists && !skipMissingDirectories) {
          const putParams = {
            Bucket: bucketName,
            Key: s3Key,
            Body: "",
            ContentType: "application/x-directory",
            Metadata: {
              "last-modified": new Date().toISOString(),
            },
          };

          const putCommand = new PutObjectCommand(putParams);
          await s3Client.send(putCommand);

          updatedPaths.add(currentPath);
          console.log(`已更新目录修改时间: ${currentPath}`);
        } else if (directoryExists) {
          // 目录存在，更新其修改时间
          const putParams = {
            Bucket: bucketName,
            Key: s3Key,
            Body: "",
            ContentType: "application/x-directory",
            Metadata: {
              "last-modified": new Date().toISOString(),
            },
          };

          const putCommand = new PutObjectCommand(putParams);
          await s3Client.send(putCommand);

          updatedPaths.add(currentPath);
          console.log(`已更新目录修改时间: ${currentPath}`);
        }
      } catch (error) {
        if (skipMissingDirectories && error.$metadata?.httpStatusCode === 404) {
          console.log(`跳过不存在的目录: ${currentPath}`);
        } else {
          console.warn(`更新目录修改时间失败 ${currentPath}:`, error);
        }
      }

      // 移动到上一级目录
      const lastSlashIndex = currentPath.lastIndexOf("/", currentPath.length - 2);
      if (lastSlashIndex > 0) {
        currentPath = currentPath.substring(0, lastSlashIndex + 1);
      } else {
        break;
      }
    }
  } catch (error) {
    console.warn(`更新父目录修改时间失败:`, error);
  }
}

/**
 * 检查S3目录是否存在
 * @param {S3Client} s3Client - S3客户端实例
 * @param {string} bucketName - 存储桶名称
 * @param {string} directoryPath - 目录路径
 * @param {string} rootPrefix - 根前缀
 * @returns {Promise<boolean>} 目录是否存在
 */
export async function checkS3DirectoryExists(s3Client, bucketName, directoryPath, rootPrefix = "") {
  try {
    // 确保目录路径以斜杠结尾
    const normalizedPath = directoryPath.endsWith("/") ? directoryPath : directoryPath + "/";
    const s3Key = rootPrefix + normalizedPath;

    const headCommand = new HeadObjectCommand({
      Bucket: bucketName,
      Key: s3Key,
    });

    await s3Client.send(headCommand);
    return true;
  } catch (error) {
    if (error.$metadata?.httpStatusCode === 404) {
      return false;
    }
    throw error;
  }
}

/**
 * 创建S3目录标记
 * @param {S3Client} s3Client - S3客户端实例
 * @param {string} bucketName - 存储桶名称
 * @param {string} directoryPath - 目录路径
 * @param {string} rootPrefix - 根前缀
 * @returns {Promise<void>}
 */
export async function createS3DirectoryMarker(s3Client, bucketName, directoryPath, rootPrefix = "") {
  try {
    // 确保目录路径以斜杠结尾
    const normalizedPath = directoryPath.endsWith("/") ? directoryPath : directoryPath + "/";
    const s3Key = rootPrefix + normalizedPath;

    const putParams = {
      Bucket: bucketName,
      Key: s3Key,
      Body: Buffer.from("", "utf-8"),
      ContentType: "application/x-directory",
      Metadata: {
        "last-modified": new Date().toISOString(),
        created: new Date().toISOString(),
      },
    };

    const putCommand = new PutObjectCommand(putParams);
    await s3Client.send(putCommand);

    console.log(`已创建目录标记: ${directoryPath}`);
  } catch (error) {
    console.error(`创建目录标记失败 ${directoryPath}:`, error);
    throw error;
  }
}

/**
 * 删除S3目录标记
 * @param {S3Client} s3Client - S3客户端实例
 * @param {string} bucketName - 存储桶名称
 * @param {string} directoryPath - 目录路径
 * @param {string} rootPrefix - 根前缀
 * @returns {Promise<void>}
 */
export async function deleteS3DirectoryMarker(s3Client, bucketName, directoryPath, rootPrefix = "") {
  try {
    // 确保目录路径以斜杠结尾
    const normalizedPath = directoryPath.endsWith("/") ? directoryPath : directoryPath + "/";
    const s3Key = rootPrefix + normalizedPath;

    const deleteCommand = new DeleteObjectCommand({
      Bucket: bucketName,
      Key: s3Key,
    });

    await s3Client.send(deleteCommand);
    console.log(`已删除目录标记: ${directoryPath}`);
  } catch (error) {
    console.warn(`删除目录标记失败 ${directoryPath}:`, error);
    // 不抛出错误，因为目录标记可能不存在
  }
}

/**
 * 检查S3目录是否存在
 * 从webdavUtils.js迁移而来，提供S3目录存在性检查
 * @param {S3Client} s3Client - S3客户端
 * @param {string} bucketName - 存储桶名称
 * @param {string} dirPath - 目录路径
 * @returns {Promise<boolean>} 目录是否存在
 */
export async function checkDirectoryExists(s3Client, bucketName, dirPath) {
  // 确保目录路径以斜杠结尾
  const normalizedPath = dirPath.endsWith("/") ? dirPath : dirPath + "/";

  try {
    // 首先尝试作为显式目录对象检查
    try {
      const headParams = {
        Bucket: bucketName,
        Key: normalizedPath,
      };

      const headCommand = new HeadObjectCommand(headParams);
      await s3Client.send(headCommand);
      return true; // 如果存在显式目录对象，直接返回true
    } catch (headError) {
      // 显式目录对象不存在，继续检查隐式目录
      if (headError.$metadata && headError.$metadata.httpStatusCode === 404) {
        // 尝试列出以该路径为前缀的对象
        const listParams = {
          Bucket: bucketName,
          Prefix: normalizedPath,
          MaxKeys: 1, // 只需要一个对象即可确认目录存在
        };

        const listCommand = new ListObjectsV2Command(listParams);
        const listResponse = await s3Client.send(listCommand);

        // 如果有对象以该路径为前缀，则认为目录存在
        return listResponse.Contents && listResponse.Contents.length > 0;
      } else {
        // 其他错误则抛出
        throw headError;
      }
    }
  } catch (error) {
    // 如果是最终的404错误，表示目录不存在
    if (error.$metadata && error.$metadata.httpStatusCode === 404) {
      return false;
    }
    // 其他错误则抛出
    throw error;
  }
}
