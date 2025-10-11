/**
 * 文件系统错误处理工具
 * 提供统一的错误处理和包装机制
 */

import { HTTPException } from "hono/http-exception";
import { ApiStatus } from "../../../constants/index.js";

/**
 * 通用错误处理包装函数
 * 用于统一处理文件系统操作中的错误，简化代码重复
 * @param {Function} fn - 要执行的异步函数
 * @param {string} operationName - 操作名称，用于错误日志
 * @param {string} defaultErrorMessage - 默认错误消息
 * @returns {Promise<any>} - 函数执行结果
 * @throws {HTTPException} - 统一处理后的HTTP异常
 */
export async function handleFsError(fn, operationName, defaultErrorMessage) {
  try {
    return await fn();
  } catch (error) {
    console.error(`${operationName}错误:`, error);

    // 如果已经是HTTPException，直接抛出
    if (error instanceof HTTPException) {
      console.log(`[ErrorHandler] 错误已经是HTTPException，直接抛出 - Status: ${error.status}, Message: ${error.message}`);
      throw error;
    }

    // 添加详细的错误转换日志
    console.error(`[ErrorHandler] 转换原始错误为HTTPException:`);
    console.error(`[ErrorHandler] 原始错误类型: ${error.constructor.name}`);
    console.error(`[ErrorHandler] 原始错误消息: ${error.message}`);
    console.error(`[ErrorHandler] 使用消息: ${error.message || defaultErrorMessage}`);

    // 其他错误转换为内部服务器错误
    throw new HTTPException(ApiStatus.INTERNAL_ERROR, {
      message: error.message || defaultErrorMessage,
    });
  }
}
