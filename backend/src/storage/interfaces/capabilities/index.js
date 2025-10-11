/**
 * 存储驱动能力接口统一导出
 * 基于alist设计理念的模块化能力接口系统
 */

// 基础驱动接口
export { BaseDriver } from "./BaseDriver.js";

// 能力接口
export { ReaderCapable, isReaderCapable, READER_CAPABILITY } from "./ReaderCapable.js";
export { WriterCapable, isWriterCapable, WRITER_CAPABILITY } from "./WriterCapable.js";
export { PresignedCapable, isPresignedCapable, PRESIGNED_CAPABILITY } from "./PresignedCapable.js";
export { MultipartCapable, isMultipartCapable, MULTIPART_CAPABILITY } from "./MultipartCapable.js";
export { AtomicCapable, isAtomicCapable, ATOMIC_CAPABILITY } from "./AtomicCapable.js";
export { ProxyCapable, isProxyCapable, PROXY_CAPABILITY } from "./ProxyCapable.js";

// 导入检查函数用于内部使用
import { isReaderCapable } from "./ReaderCapable.js";
import { isWriterCapable } from "./WriterCapable.js";
import { isPresignedCapable } from "./PresignedCapable.js";
import { isMultipartCapable } from "./MultipartCapable.js";
import { isAtomicCapable } from "./AtomicCapable.js";
import { isProxyCapable } from "./ProxyCapable.js";

/**
 * 所有可用的能力标识符
 */
export const CAPABILITIES = {
  READER: "ReaderCapable",
  WRITER: "WriterCapable",
  PRESIGNED: "PresignedCapable",
  MULTIPART: "MultipartCapable",
  ATOMIC: "AtomicCapable",
  PROXY: "ProxyCapable",
};

/**
 * 能力检查函数映射
 */
export const CAPABILITY_CHECKERS = {
  [CAPABILITIES.READER]: isReaderCapable,
  [CAPABILITIES.WRITER]: isWriterCapable,
  [CAPABILITIES.PRESIGNED]: isPresignedCapable,
  [CAPABILITIES.MULTIPART]: isMultipartCapable,
  [CAPABILITIES.ATOMIC]: isAtomicCapable,
  [CAPABILITIES.PROXY]: isProxyCapable,
};

/**
 * 检查对象是否支持指定能力
 * @param {Object} obj - 要检查的对象
 * @param {string} capability - 能力名称
 * @returns {boolean} 是否支持该能力
 */
export function hasCapability(obj, capability) {
  const checker = CAPABILITY_CHECKERS[capability];
  if (!checker) {
    throw new Error(`未知的能力类型: ${capability}`);
  }
  return checker(obj);
}

/**
 * 获取对象支持的所有能力
 * @param {Object} obj - 要检查的对象
 * @returns {Array<string>} 支持的能力列表
 */
export function getObjectCapabilities(obj) {
  const capabilities = [];

  for (const [capability, checker] of Object.entries(CAPABILITY_CHECKERS)) {
    if (checker(obj)) {
      capabilities.push(capability);
    }
  }

  return capabilities;
}

/**
 * 验证对象是否实现了所需的能力
 * @param {Object} obj - 要验证的对象
 * @param {Array<string>} requiredCapabilities - 所需的能力列表
 * @returns {Object} 验证结果
 */
export function validateCapabilities(obj, requiredCapabilities) {
  const supportedCapabilities = getObjectCapabilities(obj);
  const missingCapabilities = requiredCapabilities.filter((capability) => !supportedCapabilities.includes(capability));

  return {
    isValid: missingCapabilities.length === 0,
    supportedCapabilities,
    missingCapabilities,
    requiredCapabilities,
  };
}

/**
 * 能力接口的混入工具
 * 用于将多个能力接口混入到一个类中
 * @param {Function} BaseClass - 基础类
 * @param {...Function} capabilities - 能力接口类
 * @returns {Function} 混入后的类
 */
export function mixinCapabilities(BaseClass, ...capabilities) {
  class MixedClass extends BaseClass {}

  // 混入所有能力接口的方法
  for (const Capability of capabilities) {
    const proto = Capability.prototype;
    const propertyNames = Object.getOwnPropertyNames(proto);

    for (const name of propertyNames) {
      if (name !== "constructor") {
        const descriptor = Object.getOwnPropertyDescriptor(proto, name);
        if (descriptor) {
          Object.defineProperty(MixedClass.prototype, name, descriptor);
        }
      }
    }
  }

  return MixedClass;
}
