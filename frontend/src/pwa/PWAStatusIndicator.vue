<!--
  PWA 状态指示器组件
  显示完整的PWA状态信息
-->
<template>
  <div class="pwa-status-indicator">
    <!-- 主状态指示器 -->
    <div v-if="showIndicator" :class="indicatorClasses" @click="toggleDetails" :title="statusText">
      <div class="flex items-center space-x-2">
        <!-- 状态图标 -->
        <component :is="statusIcon" class="w-4 h-4" />

        <!-- 状态文本 -->
        <span v-if="showText" class="text-sm font-medium">
          {{ statusText }}
        </span>

        <!-- 更新进度 -->
        <div v-if="pwaState.isUpdating" class="animate-spin">
          <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path
              stroke-linecap="round"
              stroke-linejoin="round"
              stroke-width="2"
              d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"
            />
          </svg>
        </div>
      </div>
    </div>

    <!-- 详细状态面板 -->
    <Transition name="slide-down">
      <div v-if="showDetails" class="pwa-details-panel">
        <div class="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg shadow-lg p-4 mt-2">
          <h3 class="text-lg font-semibold text-gray-900 dark:text-white mb-3">
            {{ t("pwa.status.title") }}
          </h3>

          <!-- 状态网格 -->
          <div class="grid grid-cols-2 gap-3 text-sm">
            <!-- 网络状态 -->
            <div class="flex justify-between">
              <span class="text-gray-600 dark:text-gray-400">{{ t("pwa.labels.networkStatus") }}:</span>
              <span :class="pwaState.isOffline ? 'text-red-500' : 'text-green-500'">
                {{ pwaState.isOffline ? t("pwa.network.offline") : t("pwa.network.online") }}
              </span>
            </div>

            <!-- 安装状态 -->
            <div class="flex justify-between">
              <span class="text-gray-600 dark:text-gray-400">{{ t("pwa.labels.installStatus") }}:</span>
              <span :class="pwaState.isInstalled ? 'text-green-500' : 'text-gray-500'">
                {{ pwaState.isInstalled ? t("pwa.install.installed") : t("pwa.install.notInstalled") }}
              </span>
            </div>

            <!-- Service Worker 状态 -->
            <div class="flex justify-between">
              <span class="text-gray-600 dark:text-gray-400">{{ t("pwa.labels.serviceWorker") }}:</span>
              <span :class="swStateColor">
                {{ swStateText }}
              </span>
            </div>

            <!-- 更新状态 -->
            <div class="flex justify-between">
              <span class="text-gray-600 dark:text-gray-400">{{ t("pwa.labels.updateStatus") }}:</span>
              <span :class="pwaState.isUpdateAvailable ? 'text-orange-500' : 'text-green-500'">
                {{ pwaState.isUpdateAvailable ? t("pwa.update.available") : t("pwa.update.latest") }}
              </span>
            </div>

            <!-- 通知权限 -->
            <div class="flex justify-between">
              <span class="text-gray-600 dark:text-gray-400">{{ t("pwa.labels.notificationPermission") }}:</span>
              <span :class="notificationPermissionColor">
                {{ notificationPermissionText }}
              </span>
            </div>

            <!-- 后台同步 -->
            <div class="flex justify-between">
              <span class="text-gray-600 dark:text-gray-400">{{ t("pwa.labels.backgroundSync") }}:</span>
              <span :class="pwaState.backgroundSyncSupported ? 'text-green-500' : 'text-gray-500'">
                {{ pwaState.backgroundSyncSupported ? t("pwa.backgroundSync.supported") : t("pwa.backgroundSync.notSupported") }}
              </span>
            </div>

            <!-- 版本信息 -->
            <div class="flex justify-between col-span-2">
              <span class="text-gray-600 dark:text-gray-400">{{ t("pwa.labels.appVersion") }}:</span>
              <span class="text-gray-900 dark:text-white font-mono">
                {{ pwaState.version }}
              </span>
            </div>
          </div>

          <!-- 操作按钮 -->
          <div class="flex space-x-2 mt-4">
            <!-- 安装按钮 -->
            <button
              v-if="pwaState.isInstallable"
              @click="installApp"
              :disabled="installing"
              class="px-3 py-1.5 bg-blue-500 text-white text-sm rounded hover:bg-blue-600 disabled:opacity-50"
            >
              {{ installing ? t("pwa.install.installing") : t("pwa.actions.install") }}
            </button>

            <!-- 更新按钮 -->
            <button
              v-if="pwaState.isUpdateAvailable"
              @click="updateApp"
              :disabled="pwaState.isUpdating"
              class="px-3 py-1.5 bg-green-500 text-white text-sm rounded hover:bg-green-600 disabled:opacity-50"
            >
              {{ pwaState.isUpdating ? t("pwa.update.updating") : t("pwa.update.updateApp") }}
            </button>

            <!-- 通知权限按钮 -->
            <button
              v-if="pwaState.notificationPermission === 'default'"
              @click="requestNotification"
              :disabled="requestingNotification"
              class="px-3 py-1.5 bg-purple-500 text-white text-sm rounded hover:bg-purple-600 disabled:opacity-50"
            >
              {{ requestingNotification ? t("pwa.notification.requesting") : t("pwa.notification.enable") }}
            </button>

            <!-- 检查更新按钮 -->
            <button @click="checkUpdate" :disabled="checking" class="px-3 py-1.5 bg-gray-500 text-white text-sm rounded hover:bg-gray-600 disabled:opacity-50">
              {{ checking ? t("pwa.update.checking") : t("pwa.update.checkUpdate") }}
            </button>
          </div>
        </div>
      </div>
    </Transition>
  </div>
</template>

<script setup>
import { ref, computed, onMounted, onUnmounted } from "vue";
import { useI18n } from "vue-i18n";
import { pwaState, pwaUtils } from "./pwaManager.js";

// 国际化支持
const { t } = useI18n();

// Props
const props = defineProps({
  position: {
    type: String,
    default: "bottom-right", // 'top-left', 'top-right', 'bottom-left', 'bottom-right'
    validator: (value) => ["top-left", "top-right", "bottom-left", "bottom-right"].includes(value),
  },
  showText: {
    type: Boolean,
    default: false,
  },
  autoHide: {
    type: Boolean,
    default: true,
  },
  darkMode: {
    type: Boolean,
    default: false,
  },
});

// 响应式状态
const showDetails = ref(false);
const installing = ref(false);
const checking = ref(false);
const requestingNotification = ref(false);

// 计算属性
const showIndicator = computed(() => {
  if (!props.autoHide) return true;

  // 有重要状态时显示
  return pwaState.isOffline || pwaState.isUpdateAvailable || pwaState.isUpdating || pwaState.isInstallable;
});

const indicatorClasses = computed(() => {
  const base = "fixed z-50 px-3 py-2 rounded-full shadow-lg cursor-pointer transition-all duration-300 hover:scale-105";

  // 位置类
  const positions = {
    "top-left": "top-4 left-4",
    "top-right": "top-4 right-4",
    "bottom-left": "bottom-4 left-4",
    "bottom-right": "bottom-4 right-4",
  };

  // 状态颜色
  let colorClass = "";
  if (pwaState.isOffline) {
    colorClass = "bg-red-500 text-white";
  } else if (pwaState.isUpdateAvailable) {
    colorClass = "bg-orange-500 text-white";
  } else if (pwaState.isUpdating) {
    colorClass = "bg-blue-500 text-white";
  } else if (pwaState.isInstallable) {
    colorClass = "bg-green-500 text-white";
  } else {
    colorClass = "bg-gray-500 text-white";
  }

  return `${base} ${positions[props.position]} ${colorClass}`;
});

const statusText = computed(() => {
  if (pwaState.isOffline) return t("pwa.status.offline");
  if (pwaState.isUpdateAvailable) return t("pwa.status.updateAvailable");
  if (pwaState.isUpdating) return t("pwa.status.updating");
  if (pwaState.isInstallable) return t("pwa.status.installable");
  return t("pwa.status.ready");
});

const statusIcon = computed(() => {
  // 返回对应的图标组件名或SVG
  if (pwaState.isOffline) return "OfflineIcon";
  if (pwaState.isUpdateAvailable) return "UpdateIcon";
  if (pwaState.isUpdating) return "LoadingIcon";
  if (pwaState.isInstallable) return "InstallIcon";
  return "CheckIcon";
});

const swStateText = computed(() => {
  const stateKey = `pwa.serviceWorker.${pwaState.swState}`;
  return t(stateKey, t("pwa.serviceWorker.unknown"));
});

const swStateColor = computed(() => {
  const colors = {
    unknown: "text-gray-500",
    installing: "text-blue-500",
    waiting: "text-orange-500",
    active: "text-green-500",
    redundant: "text-red-500",
  };
  return colors[pwaState.swState] || "text-gray-500";
});

// 通知权限相关计算属性
const notificationPermissionText = computed(() => {
  const permissionKey = `pwa.notification.${pwaState.notificationPermission}`;
  return t(permissionKey, t("common.unknown"));
});

const notificationPermissionColor = computed(() => {
  const colors = {
    default: "text-gray-500",
    granted: "text-green-500",
    denied: "text-red-500",
  };
  return colors[pwaState.notificationPermission] || "text-gray-500";
});

// 方法
const toggleDetails = () => {
  showDetails.value = !showDetails.value;
};

const installApp = async () => {
  installing.value = true;
  try {
    await pwaUtils.install();
  } finally {
    installing.value = false;
  }
};

const updateApp = async () => {
  try {
    const success = await pwaUtils.update();
    if (success) {
      // 等待一下然后刷新
      setTimeout(() => {
        pwaUtils.reloadApp();
      }, 1000);
    }
  } catch (error) {
    console.error("更新失败:", error);
  }
};

const checkUpdate = async () => {
  checking.value = true;
  try {
    await pwaUtils.checkForUpdate();
  } finally {
    checking.value = false;
  }
};

// 请求通知权限
const requestNotification = async () => {
  requestingNotification.value = true;
  try {
    await pwaUtils.requestNotificationPermission();
  } catch (error) {
    console.error("请求通知权限失败:", error);
  } finally {
    requestingNotification.value = false;
  }
};

// 生命周期
onMounted(() => {
  // 监听PWA更新事件
  window.addEventListener("pwa-update-available", () => {
    console.log("[PWA] 收到更新通知");
  });
});

onUnmounted(() => {
  // 清理事件监听器
  window.removeEventListener("pwa-update-available", () => {});
});
</script>

<style scoped>
.slide-down-enter-active,
.slide-down-leave-active {
  transition: all 0.3s ease-out;
}

.slide-down-enter-from {
  opacity: 0;
  transform: translateY(-10px);
}

.slide-down-leave-to {
  opacity: 0;
  transform: translateY(-10px);
}

.pwa-details-panel {
  position: fixed;
  z-index: 60;
  max-width: 320px;
}

/* 根据位置调整详情面板位置 */
.pwa-status-indicator:has(.top-left) .pwa-details-panel {
  top: 60px;
  left: 16px;
}

.pwa-status-indicator:has(.top-right) .pwa-details-panel {
  top: 60px;
  right: 16px;
}

.pwa-status-indicator:has(.bottom-left) .pwa-details-panel {
  bottom: 60px;
  left: 16px;
}

.pwa-status-indicator:has(.bottom-right) .pwa-details-panel {
  bottom: 60px;
  right: 16px;
}
</style>
