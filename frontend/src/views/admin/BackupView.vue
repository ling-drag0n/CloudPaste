<template>
  <div class="backup-management">
    <div class="mb-6">
      <h2 class="text-2xl font-bold" :class="darkMode ? 'text-white' : 'text-gray-900'">{{ $t("admin.backup.title") }}</h2>
      <p class="mt-2 text-sm" :class="darkMode ? 'text-gray-400' : 'text-gray-600'">{{ $t("admin.backup.subtitle") }}</p>
    </div>

    <!-- 操作面板：双卡片网格布局 -->
    <div class="grid grid-cols-1 md:grid-cols-2 gap-6 mb-6">
      <!-- 备份操作卡片 -->
      <div class="p-6 rounded-lg border shadow hover:shadow-md transition-shadow" :class="darkMode ? 'bg-gray-800 border-gray-700' : 'bg-white border-gray-200'">
        <h3 class="text-lg font-semibold mb-4" :class="darkMode ? 'text-white' : 'text-gray-900'">{{ $t("admin.backup.backupOperations.title") }}</h3>

        <!-- 备份类型选择 -->
        <div class="backup-type-selection mb-4">
          <h4 class="text-sm font-medium mb-3" :class="darkMode ? 'text-white' : 'text-gray-900'">{{ $t("admin.backup.backupOperations.backupType") }}</h4>
          <div class="space-y-3">
            <label class="flex items-start">
              <input type="radio" v-model="backupType" value="full" class="mt-1 mr-3" />
              <div>
                <div class="font-medium" :class="darkMode ? 'text-white' : 'text-gray-900'">{{ $t("admin.backup.backupOperations.fullBackup.title") }}</div>
                <div class="text-sm" :class="darkMode ? 'text-gray-400' : 'text-gray-600'">{{ $t("admin.backup.backupOperations.fullBackup.description") }}</div>
              </div>
            </label>
            <label class="flex items-start">
              <input type="radio" v-model="backupType" value="modules" class="mt-1 mr-3" />
              <div>
                <div class="font-medium" :class="darkMode ? 'text-white' : 'text-gray-900'">{{ $t("admin.backup.backupOperations.moduleBackup.title") }}</div>
                <div class="text-sm" :class="darkMode ? 'text-gray-400' : 'text-gray-600'">{{ $t("admin.backup.backupOperations.moduleBackup.description") }}</div>
              </div>
            </label>
          </div>
        </div>

        <!-- 模块选择（仅在选择模块备份时显示） -->
        <div v-if="backupType === 'modules'" class="module-selection mb-4">
          <div class="flex items-center justify-between mb-3">
            <h4 class="text-sm font-medium" :class="darkMode ? 'text-white' : 'text-gray-900'">{{ $t("admin.backup.backupOperations.selectModules") }}</h4>
            <span v-if="selectedModules.length > 0" class="text-xs px-2 py-1 rounded" :class="darkMode ? 'bg-blue-900/20 text-blue-300' : 'bg-blue-50 text-blue-700'">
              {{ $t("admin.backup.backupOperations.selectedModules", { count: selectedModules.length }) }}
            </span>
          </div>

          <div class="max-h-64 overflow-y-auto space-y-2 custom-scrollbar">
            <label
              v-for="(module, key) in availableModules"
              :key="key"
              class="flex items-center justify-between p-3 rounded border cursor-pointer transition-all"
              :class="[
                selectedModules.includes(key)
                  ? darkMode
                    ? 'border-blue-600 bg-blue-900/20'
                    : 'border-blue-500 bg-blue-50'
                  : darkMode
                  ? 'border-gray-600 hover:bg-gray-700'
                  : 'border-gray-300 hover:bg-gray-50',
              ]"
            >
              <div class="flex items-center">
                <input type="checkbox" v-model="selectedModules" :value="key" class="mr-3" />
                <div>
                  <div class="font-medium" :class="darkMode ? 'text-white' : 'text-gray-900'">{{ module.name }}</div>
                  <div class="text-sm mt-1" :class="darkMode ? 'text-gray-400' : 'text-gray-600'">{{ module.description }}</div>
                </div>
              </div>
              <span class="text-xs px-2 py-1 rounded" :class="darkMode ? 'bg-gray-700 text-gray-300' : 'bg-gray-100 text-gray-600'">{{
                $t("admin.backup.logs.recordsCount", { count: module.record_count || 0 })
              }}</span>
            </label>
          </div>
        </div>

        <!-- 备份按钮 -->
        <div class="backup-actions">
          <button
            @click="createBackup"
            :disabled="isCreating || (backupType === 'modules' && selectedModules.length === 0)"
            class="w-full px-6 py-2 bg-blue-600 hover:bg-blue-700 disabled:bg-gray-400 text-white font-medium rounded-md transition-colors"
          >
            {{ isCreating ? $t("admin.backup.backupOperations.creating") : $t("admin.backup.backupOperations.createBackup") }}
          </button>
        </div>
      </div>

      <!-- 恢复操作卡片 -->
      <div class="p-6 rounded-lg border shadow hover:shadow-md transition-shadow" :class="darkMode ? 'bg-gray-800 border-gray-700' : 'bg-white border-gray-200'">
        <h3 class="text-lg font-semibold mb-4" :class="darkMode ? 'text-white' : 'text-gray-900'">{{ $t("admin.backup.restoreOperations.title") }}</h3>

        <div class="restore-controls space-y-4 mb-4">
          <input type="file" @change="handleFileSelect" accept=".json" ref="fileInput" class="hidden" />

          <button @click="$refs.fileInput.click()" class="w-full px-4 py-2 bg-gray-600 hover:bg-gray-700 text-white font-medium rounded-md transition-colors">
            {{ $t("admin.backup.restoreOperations.selectFile") }}
          </button>

          <div v-if="selectedFile" class="space-y-4">
            <div class="p-3 rounded border" :class="darkMode ? 'border-gray-600 bg-gray-700' : 'border-gray-300 bg-gray-50'">
              <span class="text-sm" :class="darkMode ? 'text-gray-300' : 'text-gray-700'"> {{ selectedFile.name }} ({{ formatFileSize(selectedFile.size) }}) </span>
            </div>

            <div class="flex items-center space-x-4">
              <span class="text-sm font-medium" :class="darkMode ? 'text-white' : 'text-gray-900'">{{ $t("admin.backup.restoreOperations.restoreMode") }}</span>
              <label class="flex items-center">
                <input type="radio" v-model="restoreMode" value="overwrite" class="mr-1" />
                <span class="text-sm">{{ $t("admin.backup.restoreOperations.overwriteMode") }}</span>
              </label>
              <label class="flex items-center">
                <input type="radio" v-model="restoreMode" value="merge" class="mr-1" />
                <span class="text-sm">{{ $t("admin.backup.restoreOperations.mergeMode") }}</span>
              </label>
            </div>

            <button
              @click="restoreBackup"
              :disabled="isRestoring"
              class="w-full px-6 py-2 bg-orange-600 hover:bg-orange-700 disabled:bg-gray-400 text-white font-medium rounded-md transition-colors"
            >
              {{ isRestoring ? $t("admin.backup.restoreOperations.restoring") : $t("admin.backup.restoreOperations.executeRestore") }}
            </button>
          </div>
        </div>
      </div>
    </div>

    <!-- 操作日志 -->
    <div class="logs-panel p-6 rounded-lg border" :class="darkMode ? 'bg-gray-800 border-gray-700' : 'bg-white border-gray-200'">
      <div class="flex items-center justify-between mb-4">
        <h3 class="text-lg font-semibold" :class="darkMode ? 'text-white' : 'text-gray-900'">{{ $t("admin.backup.operationLogs") }}</h3>
        <button
          @click="clearLogs"
          class="text-sm px-3 py-1 rounded border"
          :class="darkMode ? 'border-gray-600 text-gray-400 hover:bg-gray-700' : 'border-gray-300 text-gray-600 hover:bg-gray-50'"
        >
          {{ $t("admin.backup.logs.clearLogs") }}
        </button>
      </div>

      <div class="logs-container h-64 overflow-y-auto border rounded" :class="darkMode ? 'border-gray-600 bg-gray-900' : 'border-gray-300 bg-gray-50'">
        <div v-if="logs.length === 0" class="flex items-center justify-center h-full text-sm" :class="darkMode ? 'text-gray-500' : 'text-gray-500'">暂无操作日志</div>
        <div v-else class="p-3 space-y-1">
          <div v-for="(log, index) in logs" :key="index" class="flex items-start text-sm font-mono">
            <span class="mr-2 font-medium" :class="getLogTypeColor(log.type)"> [{{ log.type }}] </span>
            <span class="mr-2 text-xs" :class="darkMode ? 'text-gray-500' : 'text-gray-500'">
              {{ formatLogTime(log.timestamp) }}
            </span>
            <span :class="darkMode ? 'text-gray-300' : 'text-gray-700'">
              {{ log.message }}
            </span>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script>
import { ref, computed, onMounted } from "vue";
import { useI18n } from "vue-i18n";
import { useAdminBase } from "@/composables/admin-management/useAdminBase.js";
import BackupService from "@/api/services/backupService.js";

export default {
  name: "BackupView",
  props: {
    darkMode: {
      type: Boolean,
      required: true,
    },
  },
  setup(props) {
    const { t } = useI18n();
    const { showSuccess, showError } = useAdminBase();

    // 统一的通知方法
    const showNotification = (message, type) => {
      if (type === "success") {
        showSuccess(message);
      } else if (type === "error") {
        showError(message);
      }
    };

    // 响应式数据
    const backupType = ref("full");
    const selectedModules = ref([]);
    const selectedFile = ref(null);
    const backupPreview = ref(null);
    const restoreMode = ref("overwrite");

    // 状态
    const isCreating = ref(false);
    const isRestoring = ref(false);
    const logs = ref([]);

    // 可用模块（从API获取）
    const availableModules = ref({});

    // 日志管理
    const addLog = (message, type = "INFO") => {
      logs.value.push({
        timestamp: new Date(),
        type,
        message,
      });
    };

    const clearLogs = () => {
      logs.value = [];
    };

    // 工具方法
    const formatFileSize = (bytes) => {
      if (bytes === 0) return "0 Bytes";
      const k = 1024;
      const sizes = ["Bytes", "KB", "MB", "GB"];
      const i = Math.floor(Math.log(bytes) / Math.log(k));
      return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + " " + sizes[i];
    };

    const formatLogTime = (timestamp) => {
      return timestamp.toLocaleTimeString();
    };

    const getLogTypeColor = (type) => {
      switch (type) {
        case "SUCCESS":
          return "text-green-600";
        case "ERROR":
          return "text-red-600";
        case "INFO":
        default:
          return "text-blue-600";
      }
    };

    // 计算属性
    const getSelectedTablesCount = computed(() => {
      const tables = new Set();
      selectedModules.value.forEach((moduleKey) => {
        const module = availableModules.value[moduleKey];
        if (module && module.tables) {
          module.tables.forEach((table) => {
            tables.add(table);
          });
        }
      });
      return tables.size;
    });

    // 生成本地时区的详细时间戳
    const generateLocalTimestamp = () => {
      const now = new Date();
      const year = now.getFullYear();
      const month = String(now.getMonth() + 1).padStart(2, "0");
      const day = String(now.getDate()).padStart(2, "0");
      const hour = String(now.getHours()).padStart(2, "0");
      const minute = String(now.getMinutes()).padStart(2, "0");
      const second = String(now.getSeconds()).padStart(2, "0");
      return `${year}-${month}-${day}-${hour}-${minute}-${second}`;
    };

    // 生成模块备份文件名
    const generateModuleBackupFilename = () => {
      // 模块缩写映射
      const moduleAbbreviations = {
        text_management: "txt",
        file_management: "file",
        mount_management: "mount",
        storage_config: "s3cfg",
        key_management: "key",
        account_management: "admin",
        system_settings: "sys",
      };

      // 生成缩写组合（去重并排序，用下划线分隔）
      const abbreviations = [...new Set(selectedModules.value)]
        .map((moduleKey) => moduleAbbreviations[moduleKey] || moduleKey)
        .sort() // 排序确保一致性
        .join("_");

      const timestamp = generateLocalTimestamp();
      return `cloudpaste-${abbreviations}-${timestamp}.json`;
    };

    // 加载模块信息
    const loadModuleInfo = async () => {
      try {
        addLog(t("admin.backup.logs.gettingModuleInfo"), "INFO");
        const response = await BackupService.getModules();

        // 为每个模块添加国际化的名称和描述
        const modules = response.data.modules;
        for (const [key, module] of Object.entries(modules)) {
          module.name = t(`admin.backup.modules.${key}.name`);
          module.description = t(`admin.backup.modules.${key}.description`);
        }

        availableModules.value = modules;
        addLog(t("admin.backup.logs.moduleInfoSuccess"), "SUCCESS");
      } catch (error) {
        console.error("获取模块信息失败:", error);
        addLog(t("admin.backup.errors.getModuleInfoFailed", { error: error.message }), "ERROR");
        showNotification(t("admin.backup.errors.getModuleInfoFailed", { error: error.message }), "error");
      }
    };

    // 统一的备份方法
    const createBackup = async () => {
      if (backupType.value === "full") {
        await createFullBackup();
      } else if (backupType.value === "modules") {
        await createModuleBackup();
      }
    };

    // 创建完整备份
    const createFullBackup = async () => {
      isCreating.value = true;
      try {
        addLog(t("admin.backup.logs.startFullBackup"), "INFO");

        const blob = await BackupService.createBackup({
          backup_type: "full",
        });

        // 解析备份内容以显示详细统计
        const backupText = await blob.text();
        const backupData = JSON.parse(backupText);

        // 显示每个表的导出统计
        if (backupData.metadata && backupData.metadata.tables) {
          for (const [tableName, recordCount] of Object.entries(backupData.metadata.tables)) {
            addLog(t("admin.backup.logs.tableExported", { table: tableName, count: recordCount }), recordCount > 0 ? "SUCCESS" : "INFO");
          }
        }

        addLog(t("admin.backup.logs.backupComplete", { count: backupData.metadata.total_records }), "SUCCESS");

        // 下载文件
        const url = window.URL.createObjectURL(blob);
        const link = document.createElement("a");
        link.href = url;

        // 生成详细时间戳的文件名（本地时区）
        const timestamp = generateLocalTimestamp();
        link.download = `cloudpaste-full-${timestamp}.json`;

        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
        window.URL.revokeObjectURL(url);

        addLog(t("admin.backup.logs.downloadStarted"), "SUCCESS");

        showNotification(t("admin.backup.success.backupCreated"), "success");
      } catch (error) {
        console.error("创建备份失败:", error);
        addLog(t("admin.backup.errors.backupFailed", { error: error.message }), "ERROR");
        showNotification(t("admin.backup.errors.backupFailed", { error: error.message }), "error");
      } finally {
        isCreating.value = false;
      }
    };

    // 创建模块备份
    const createModuleBackup = async () => {
      if (selectedModules.value.length === 0) {
        addLog(t("admin.backup.errors.selectAtLeastOneModule"), "ERROR");
        return;
      }

      isCreating.value = true;
      try {
        addLog(t("admin.backup.logs.startModuleBackup", { count: selectedModules.value.length }), "INFO");

        const blob = await BackupService.createBackup({
          backup_type: "modules",
          selected_modules: selectedModules.value,
        });

        // 解析备份内容以显示详细统计
        const backupText = await blob.text();
        const backupData = JSON.parse(backupText);

        // 显示每个表的导出统计
        if (backupData.metadata && backupData.metadata.tables) {
          for (const [tableName, recordCount] of Object.entries(backupData.metadata.tables)) {
            addLog(t("admin.backup.logs.tableExported", { table: tableName, count: recordCount }), recordCount > 0 ? "SUCCESS" : "INFO");
          }
        }

        addLog(t("admin.backup.logs.moduleBackupComplete", { count: backupData.metadata.total_records }), "SUCCESS");

        // 下载文件
        const url = window.URL.createObjectURL(blob);
        const link = document.createElement("a");
        link.href = url;
        link.download = generateModuleBackupFilename();
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
        window.URL.revokeObjectURL(url);

        addLog(t("admin.backup.logs.downloadStarted"), "SUCCESS");
        showNotification(t("admin.backup.success.backupCreated"), "success");
      } catch (error) {
        console.error("创建备份失败:", error);
        addLog(t("admin.backup.errors.backupFailed", { error: error.message }), "ERROR");
        showNotification(t("admin.backup.errors.backupFailed", { error: error.message }), "error");
      } finally {
        isCreating.value = false;
      }
    };

    // 文件选择处理
    const handleFileSelect = (event) => {
      const file = event.target.files[0];
      if (file) {
        selectedFile.value = file;
        addLog(`${t("admin.backup.restoreOperations.selectFile")}: ${file.name}`, "INFO");

        // 尝试预览备份内容
        const reader = new FileReader();
        reader.onload = (e) => {
          try {
            const backup = JSON.parse(e.target.result);
            backupPreview.value = backup;
            addLog(t("admin.backup.success.backupCreated"), "SUCCESS");
          } catch (error) {
            addLog(t("admin.backup.errors.invalidBackupFile"), "ERROR");
            backupPreview.value = null;
          }
        };
        reader.readAsText(file);
      }
    };

    // 执行恢复
    const restoreBackup = async () => {
      if (!selectedFile.value) {
        addLog(t("admin.backup.errors.fileSelectRequired"), "ERROR");
        return;
      }

      isRestoring.value = true;
      try {
        addLog(t("admin.backup.logs.startRestore", { mode: t(`admin.backup.restoreOperations.${restoreMode.value}Mode`) }), "INFO");

        const response = await BackupService.restoreBackup(selectedFile.value, restoreMode.value);

        // 显示详细的表级别统计信息
        if (response.data && response.data.results) {
          for (const [tableName, stats] of Object.entries(response.data.results)) {
            if (stats.expected > 0) {
              if (restoreMode.value === "overwrite") {
                // 覆盖模式：显示成功/失败
                addLog(
                  t("admin.backup.logs.tableRestoredOverwrite", {
                    table: tableName,
                    success: stats.success,
                    expected: stats.expected,
                  }),
                  stats.failed > 0 ? "WARNING" : "SUCCESS"
                );
              } else {
                // 合并模式：显示新增/忽略/失败
                addLog(
                  t("admin.backup.logs.tableRestored", {
                    table: tableName,
                    added: stats.success,
                    ignored: stats.ignored,
                    expected: stats.expected,
                  }),
                  stats.failed > 0 ? "WARNING" : "SUCCESS"
                );
              }
            }
          }
        }

        // 根据模式显示不同的总结信息
        if (restoreMode.value === "overwrite") {
          addLog(t("admin.backup.logs.restoreCompleteOverwrite", { count: response.data.total_records }), "SUCCESS");
        } else {
          // 合并模式：计算新增和忽略的记录数
          const totalSuccess = Object.values(response.data.results).reduce((sum, r) => sum + r.success, 0);
          const totalIgnored = Object.values(response.data.results).reduce((sum, r) => sum + r.ignored, 0);

          addLog(t("admin.backup.logs.restoreComplete", { added: totalSuccess, ignored: totalIgnored }), "SUCCESS");
        }
        showNotification(t("admin.backup.success.dataRestored"), "success");
      } catch (error) {
        console.error("恢复失败:", error);
        addLog(t("admin.backup.errors.restoreFailed", { error: error.message }), "ERROR");
        showNotification(t("admin.backup.errors.restoreFailed", { error: error.message }), "error");
      } finally {
        isRestoring.value = false;
      }
    };

    // 组件挂载时加载模块信息
    onMounted(() => {
      loadModuleInfo();
    });

    return {
      // 响应式数据
      backupType,
      selectedModules,
      selectedFile,
      backupPreview,
      restoreMode,
      isCreating,
      isRestoring,
      logs,
      availableModules,

      // 计算属性
      getSelectedTablesCount,
      generateLocalTimestamp,
      generateModuleBackupFilename,

      // 方法
      createBackup,
      createFullBackup,
      createModuleBackup,
      handleFileSelect,
      restoreBackup,
      clearLogs,
      formatFileSize,
      formatLogTime,
      getLogTypeColor,
      showNotification,
    };
  },
};
</script>
