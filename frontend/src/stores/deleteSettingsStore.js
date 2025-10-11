/**
 * 全局删除设置状态管理
 * 管理删除模式的全局状态，支持持久化存储
 */

import { defineStore } from "pinia";
import { ref } from "vue";

export const useDeleteSettingsStore = defineStore("deleteSettings", () => {
  // 删除模式状态：false = 同时删除文件和记录，true = 仅删除记录
  const deleteRecordOnly = ref(false);

  // 从localStorage加载设置
  const loadSettings = () => {
    try {
      const saved = localStorage.getItem("cloudpaste_delete_settings");
      if (saved) {
        const settings = JSON.parse(saved);
        deleteRecordOnly.value = settings.deleteRecordOnly || false;
      }
    } catch (error) {
      console.warn("加载删除设置失败:", error);
    }
  };

  // 保存设置到localStorage
  const saveSettings = () => {
    try {
      const settings = {
        deleteRecordOnly: deleteRecordOnly.value,
      };
      localStorage.setItem("cloudpaste_delete_settings", JSON.stringify(settings));
    } catch (error) {
      console.warn("保存删除设置失败:", error);
    }
  };

  // 切换删除模式
  const toggleDeleteMode = () => {
    deleteRecordOnly.value = !deleteRecordOnly.value;
    saveSettings();
  };

  // 获取删除模式字符串（用于API调用）
  const getDeleteMode = () => {
    return deleteRecordOnly.value ? "record_only" : "both";
  };

  // 初始化时加载设置
  loadSettings();

  return {
    deleteRecordOnly,
    toggleDeleteMode,
    getDeleteMode,
    loadSettings,
    saveSettings,
  };
});
