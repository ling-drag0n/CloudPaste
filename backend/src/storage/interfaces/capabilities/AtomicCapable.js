/**
 * 原子操作能力接口
 * 定义存储驱动的原子操作能力
 * 支持文件和目录的重命名、移动、复制等原子操作
 * 确保操作的原子性和一致性
 */

export class AtomicCapable {
  /**
   * 重命名文件或目录
   * @param {string} sourcePath - 源路径
   * @param {string} targetPath - 目标路径
   * @param {Object} options - 选项参数
   * @param {boolean} options.overwrite - 是否覆盖目标文件，默认false
   * @param {boolean} options.createParentDirs - 是否创建父目录，默认true
   * @returns {Promise<Object>} 重命名结果
   */
  async rename(sourcePath, targetPath, options = {}) {
    throw new Error("rename方法必须在实现AtomicCapable的类中实现");
  }

  /**
   * 移动文件或目录
   * @param {string} sourcePath - 源路径
   * @param {string} targetPath - 目标路径
   * @param {Object} options - 选项参数
   * @param {boolean} options.overwrite - 是否覆盖目标文件，默认false
   * @param {boolean} options.createParentDirs - 是否创建父目录，默认true
   * @returns {Promise<Object>} 移动结果
   */
  async move(sourcePath, targetPath, options = {}) {
    // 默认实现：移动等同于重命名
    return await this.rename(sourcePath, targetPath, options);
  }

  /**
   * 复制文件或目录
   * @param {string} sourcePath - 源路径
   * @param {string} targetPath - 目标路径
   * @param {Object} options - 选项参数
   * @param {boolean} options.overwrite - 是否覆盖目标文件，默认false
   * @param {boolean} options.recursive - 是否递归复制目录，默认true
   * @param {boolean} options.preserveMetadata - 是否保留元数据，默认true
   * @returns {Promise<Object>} 复制结果
   */
  async copy(sourcePath, targetPath, options = {}) {
    throw new Error("copy方法必须在实现AtomicCapable的类中实现");
  }

  /**
   * 批量重命名文件和目录
   * @param {Array<Object>} operations - 重命名操作数组
   * @param {Object} options - 选项参数
   * @param {boolean} options.continueOnError - 遇到错误时是否继续
   * @param {boolean} options.atomic - 是否作为原子操作执行
   * @returns {Promise<Object>} 批量重命名结果
   */
  async batchRename(operations, options = {}) {
    // 默认实现：逐个重命名
    const results = [];
    const errors = [];

    for (const operation of operations) {
      try {
        const result = await this.rename(operation.sourcePath, operation.targetPath, {
          ...options,
          ...operation.options,
        });
        results.push({
          sourcePath: operation.sourcePath,
          targetPath: operation.targetPath,
          success: true,
          result,
        });
      } catch (error) {
        errors.push({
          sourcePath: operation.sourcePath,
          targetPath: operation.targetPath,
          success: false,
          error: error.message,
        });
        if (!options.continueOnError) {
          throw error;
        }
      }
    }

    return {
      success: errors.length === 0,
      results,
      errors,
      total: operations.length,
      succeeded: results.length,
      failed: errors.length,
    };
  }

  /**
   * 批量复制文件和目录
   * @param {Array<Object>} operations - 复制操作数组
   * @param {Object} options - 选项参数
   * @param {boolean} options.continueOnError - 遇到错误时是否继续
   * @param {boolean} options.atomic - 是否作为原子操作执行
   * @returns {Promise<Object>} 批量复制结果
   */
  async batchCopy(operations, options = {}) {
    // 默认实现：逐个复制
    const results = [];
    const errors = [];

    for (const operation of operations) {
      try {
        const result = await this.copy(operation.sourcePath, operation.targetPath, {
          ...options,
          ...operation.options,
        });
        results.push({
          sourcePath: operation.sourcePath,
          targetPath: operation.targetPath,
          success: true,
          result,
        });
      } catch (error) {
        errors.push({
          sourcePath: operation.sourcePath,
          targetPath: operation.targetPath,
          success: false,
          error: error.message,
        });
        if (!options.continueOnError) {
          throw error;
        }
      }
    }

    return {
      success: errors.length === 0,
      results,
      errors,
      total: operations.length,
      succeeded: results.length,
      failed: errors.length,
    };
  }

  /**
   * 检查路径冲突
   * @param {string} sourcePath - 源路径
   * @param {string} targetPath - 目标路径
   * @param {Object} options - 选项参数
   * @returns {Promise<Object>} 冲突检查结果
   */
  async checkPathConflict(sourcePath, targetPath, options = {}) {
    // 默认实现：基本的路径冲突检查
    if (sourcePath === targetPath) {
      return {
        hasConflict: true,
        type: "same_path",
        message: "源路径和目标路径相同",
      };
    }

    // 检查是否是父子路径关系
    if (targetPath.startsWith(sourcePath + "/")) {
      return {
        hasConflict: true,
        type: "parent_child",
        message: "不能将目录移动到其子目录中",
      };
    }

    return {
      hasConflict: false,
      type: "none",
      message: "无路径冲突",
    };
  }
}

/**
 * 检查对象是否实现了AtomicCapable接口
 * @param {Object} obj - 要检查的对象
 * @returns {boolean} 是否实现了AtomicCapable接口
 */
export function isAtomicCapable(obj) {
  return (
    obj &&
    typeof obj.rename === "function" &&
    typeof obj.copy === "function"
  );
}

/**
 * AtomicCapable能力的标识符
 */
export const ATOMIC_CAPABILITY = "AtomicCapable";
