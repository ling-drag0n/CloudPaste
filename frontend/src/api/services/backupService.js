import { get, post } from "../client.js";

/**
 * 备份服务 - 处理数据备份与还原相关的API调用
 */
export class BackupService {
  /**
   * 获取备份模块信息
   * @returns {Promise<Object>} 模块信息
   */
  static async getModules() {
    return await get("/admin/backup/modules");
  }

  /**
   * 创建备份
   * @param {Object} options - 备份选项
   * @param {string} options.backup_type - 备份类型 ('full' | 'modules')
   * @param {Array} options.selected_modules - 选中的模块（当backup_type为'modules'时）
   * @returns {Promise<Blob>} 备份文件
   */
  static async createBackup(options) {
    // 使用项目标准的post方法，但需要特殊处理blob响应
    return post("/admin/backup/create", options, { responseType: "blob" });
  }

  /**
   * 还原备份
   * @param {File} file - 备份文件
   * @param {string} mode - 还原模式 ('overwrite' | 'merge')
   * @returns {Promise<Object>} 还原结果
   */
  static async restoreBackup(file, mode = "overwrite") {
    const formData = new FormData();
    formData.append("backup_file", file);
    formData.append("mode", mode);

    // 使用项目标准的post方法发送FormData
    return post("/admin/backup/restore", formData);
  }

  /**
   * 下载备份文件
   * @param {Blob} blob - 备份数据
   * @param {string} filename - 文件名
   */
  static downloadBackup(blob, filename) {
    const url = window.URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = filename || "backup.json";
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    window.URL.revokeObjectURL(url);
  }

  /**
   * 解析备份文件预览信息
   * @param {File} file - 备份文件
   * @returns {Promise<Object>} 备份预览信息
   */
  static async parseBackupPreview(file) {
    return new Promise((resolve, reject) => {
      const reader = new FileReader();

      reader.onload = (e) => {
        try {
          const backupData = JSON.parse(e.target.result);
          if (backupData.metadata) {
            resolve(backupData.metadata);
          } else {
            reject(new Error("备份文件格式错误：缺少元数据"));
          }
        } catch (error) {
          reject(new Error("备份文件格式错误：无效的JSON格式"));
        }
      };

      reader.onerror = () => {
        reject(new Error("文件读取失败"));
      };

      reader.readAsText(file);
    });
  }
}

export default BackupService;
