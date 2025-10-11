/**
 * 🎯 Background Sync API 集成 - 符合主流PWA最佳实践
 *
 * 此文件实现标准的Background Sync API功能，包括：
 * 1. 离线操作队列的可靠同步
 * 2. 重试机制和错误处理
 * 3. 同步状态管理和反馈
 * 4. 与现有PWA架构的无缝集成
 */

// 🎯 Background Sync 配置
const SYNC_CONFIG = {
  // 同步标签
  TAGS: {
    OFFLINE_QUEUE: "sync-offline-queue",
    DATA_SYNC: "sync-data",
    RETRY_FAILED: "sync-retry-failed",
  },

  // 重试配置
  RETRY: {
    MAX_ATTEMPTS: 3,
    INITIAL_DELAY: 1000, // 1秒
    BACKOFF_MULTIPLIER: 2, // 指数退避
    MAX_DELAY: 30000, // 最大30秒
  },

  // IndexedDB配置
  DB: {
    NAME: "CloudPasteOfflineDB",
    VERSION: 685, // 与pwaManager.js保持一致
    STORES: {
      OFFLINE_QUEUE: "offlineQueue",
      SYNC_STATUS: "syncStatus",
    },
  },
};

// IndexedDB操作工具类
class SyncStorageManager {
  constructor() {
    this.db = null;
  }

  async init() {
    return new Promise((resolve, reject) => {
      const request = indexedDB.open(SYNC_CONFIG.DB.NAME, SYNC_CONFIG.DB.VERSION);

      request.onerror = () => reject(request.error);
      request.onsuccess = () => {
        this.db = request.result;
        resolve(this.db);
      };

      // 数据库升级时确保存储结构存在
      request.onupgradeneeded = (event) => {
        const db = event.target.result;

        // 确保离线队列存储存在
        if (!db.objectStoreNames.contains(SYNC_CONFIG.DB.STORES.OFFLINE_QUEUE)) {
          const queueStore = db.createObjectStore(SYNC_CONFIG.DB.STORES.OFFLINE_QUEUE, {
            keyPath: "id",
            autoIncrement: true,
          });
          queueStore.createIndex("timestamp", "timestamp", { unique: false });
          queueStore.createIndex("type", "type", { unique: false });
          queueStore.createIndex("status", "status", { unique: false });
        }

        // 创建同步状态存储
        if (!db.objectStoreNames.contains(SYNC_CONFIG.DB.STORES.SYNC_STATUS)) {
          const statusStore = db.createObjectStore(SYNC_CONFIG.DB.STORES.SYNC_STATUS, {
            keyPath: "id",
          });
          statusStore.createIndex("lastSync", "lastSync", { unique: false });
        }
      };
    });
  }

  async getOfflineQueue() {
    if (!this.db) await this.init();

    const transaction = this.db.transaction([SYNC_CONFIG.DB.STORES.OFFLINE_QUEUE], "readonly");
    const store = transaction.objectStore(SYNC_CONFIG.DB.STORES.OFFLINE_QUEUE);

    return new Promise((resolve, reject) => {
      const request = store.getAll();
      request.onsuccess = () => resolve(request.result || []);
      request.onerror = () => reject(request.error);
    });
  }

  async removeFromQueue(id) {
    if (!this.db) await this.init();

    const transaction = this.db.transaction([SYNC_CONFIG.DB.STORES.OFFLINE_QUEUE], "readwrite");
    const store = transaction.objectStore(SYNC_CONFIG.DB.STORES.OFFLINE_QUEUE);

    return new Promise((resolve, reject) => {
      const request = store.delete(id);
      request.onsuccess = () => resolve();
      request.onerror = () => reject(request.error);
    });
  }

  async updateQueueItem(id, updates) {
    if (!this.db) await this.init();

    const transaction = this.db.transaction([SYNC_CONFIG.DB.STORES.OFFLINE_QUEUE], "readwrite");
    const store = transaction.objectStore(SYNC_CONFIG.DB.STORES.OFFLINE_QUEUE);

    return new Promise((resolve, reject) => {
      const getRequest = store.get(id);
      getRequest.onsuccess = () => {
        const item = getRequest.result;
        if (item) {
          Object.assign(item, updates);
          const putRequest = store.put(item);
          putRequest.onsuccess = () => resolve(item);
          putRequest.onerror = () => reject(putRequest.error);
        } else {
          reject(new Error("Queue item not found"));
        }
      };
      getRequest.onerror = () => reject(getRequest.error);
    });
  }

  async updateSyncStatus(status) {
    if (!this.db) await this.init();

    const transaction = this.db.transaction([SYNC_CONFIG.DB.STORES.SYNC_STATUS], "readwrite");
    const store = transaction.objectStore(SYNC_CONFIG.DB.STORES.SYNC_STATUS);

    const statusRecord = {
      id: "global",
      lastSync: new Date().toISOString(),
      ...status,
    };

    return new Promise((resolve, reject) => {
      const request = store.put(statusRecord);
      request.onsuccess = () => resolve(statusRecord);
      request.onerror = () => reject(request.error);
    });
  }
}

// 创建存储管理器实例
const syncStorage = new SyncStorageManager();

// Background Sync 事件处理器
self.addEventListener("sync", async (event) => {
  console.log(`[Background Sync] 收到同步事件: ${event.tag}`);

  switch (event.tag) {
    case SYNC_CONFIG.TAGS.OFFLINE_QUEUE:
      event.waitUntil(syncOfflineQueue());
      break;

    case SYNC_CONFIG.TAGS.DATA_SYNC:
      event.waitUntil(syncApplicationData());
      break;

    case SYNC_CONFIG.TAGS.RETRY_FAILED:
      event.waitUntil(retryFailedOperations());
      break;

    default:
      console.warn(`[Background Sync] 未知的同步标签: ${event.tag}`);
  }
});

// 同步离线操作队列
async function syncOfflineQueue() {
  try {
    console.log("[Background Sync] 开始同步离线操作队列");

    const queue = await syncStorage.getOfflineQueue();
    const pendingItems = queue.filter((item) => item.status === "pending");

    if (pendingItems.length === 0) {
      console.log("[Background Sync] 没有待同步的离线操作");
      return;
    }

    console.log(`[Background Sync] 发现 ${pendingItems.length} 个待同步操作`);

    let successCount = 0;
    let failureCount = 0;

    for (const item of pendingItems) {
      try {
        await processQueueItem(item);
        await syncStorage.removeFromQueue(item.id);
        successCount++;
        console.log(`[Background Sync] 操作同步成功: ${item.type} (ID: ${item.id})`);
      } catch (error) {
        failureCount++;
        console.error(`[Background Sync] 操作同步失败: ${item.type} (ID: ${item.id})`, error);

        // 更新重试计数
        const retryCount = (item.retryCount || 0) + 1;
        if (retryCount < SYNC_CONFIG.RETRY.MAX_ATTEMPTS) {
          await syncStorage.updateQueueItem(item.id, {
            status: "retry",
            retryCount,
            lastError: error.message,
            nextRetry: new Date(Date.now() + calculateRetryDelay(retryCount)).toISOString(),
          });
        } else {
          await syncStorage.updateQueueItem(item.id, {
            status: "failed",
            retryCount,
            lastError: error.message,
            failedAt: new Date().toISOString(),
          });
        }
      }
    }

    // 更新同步状态
    try {
      await syncStorage.updateSyncStatus({
        type: "offline-queue",
        successCount,
        failureCount,
        totalProcessed: pendingItems.length,
      });
      console.log(`[Background Sync] 同步状态已更新`);
    } catch (error) {
      console.error(`[Background Sync] 更新同步状态失败:`, error);
      // 不阻止后续通知流程
    }

    console.log(`[Background Sync] 离线队列同步完成: 成功 ${successCount}, 失败 ${failureCount}`);

    // 第1层：Service Worker → PWA Manager 通信
    // 同步完成后通知所有客户端页面
    console.log(`[Background Sync] 准备通知客户端: successCount=${successCount}, failureCount=${failureCount}`);

    if (successCount > 0 || failureCount > 0) {
      const syncedOperations = pendingItems.slice(0, successCount).map((item) => ({
        type: item.type,
        id: item.id,
        timestamp: item.timestamp,
      }));

      console.log(`[Background Sync] 发送通知给客户端`, { successCount, failureCount, syncedOperations });

      await notifyClientsOfSyncCompletion({
        type: "offline-queue",
        successCount,
        failureCount,
        totalProcessed: pendingItems.length,
        syncedOperations,
        timestamp: new Date().toISOString(),
      });
    } else {
      console.log(`[Background Sync] 跳过通知：没有成功或失败的操作`);
    }

    // 如果有失败的操作，注册重试同步
    if (failureCount > 0) {
      await registerRetrySync();
    }
  } catch (error) {
    console.error("[Background Sync] 同步离线队列失败:", error);
    throw error;
  }
}

// 处理单个队列项目
async function processQueueItem(item) {
  const { endpoint, method, data, type, authToken, authType } = item;

  // 构建请求URL
  const baseUrl = self.location.origin;
  const url = endpoint.startsWith("/") ? `${baseUrl}/api${endpoint}` : `${baseUrl}/api/${endpoint}`;

  // 构建请求选项
  const options = {
    method: method || "POST",
    headers: {
      "Content-Type": "application/json",
    },
  };

  // 添加认证信息
  if (authToken && authType) {
    if (authType === "admin") {
      options.headers.Authorization = `Bearer ${authToken}`;
    } else if (authType === "apikey") {
      options.headers.Authorization = `ApiKey ${authToken}`;
    }
    console.log(`[Background Sync] 添加认证信息: ${authType} (token长度: ${authToken.length})`);
  } else {
    console.warn(`[Background Sync] 缺少认证信息: authToken=${!!authToken}, authType=${authType}`);
  }

  // 添加请求体
  if (data && (method === "POST" || method === "PUT" || method === "PATCH")) {
    options.body = typeof data === "string" ? data : JSON.stringify(data);
  }

  console.log(`[Background Sync] 发送请求: ${method} ${url}`, { headers: options.headers, hasBody: !!options.body });

  // 发送请求
  const response = await fetch(url, options);

  if (!response.ok) {
    const errorText = await response.text();
    console.error(`[Background Sync] API请求失败: ${response.status} ${response.statusText}`, errorText);
    throw new Error(`HTTP ${response.status}: ${response.statusText} - ${errorText}`);
  }

  const result = await response.json();
  console.log(`[Background Sync] API请求成功:`, result);
  return result;
}

// 计算重试延迟（指数退避）
function calculateRetryDelay(retryCount) {
  const delay = SYNC_CONFIG.RETRY.INITIAL_DELAY * Math.pow(SYNC_CONFIG.RETRY.BACKOFF_MULTIPLIER, retryCount - 1);
  return Math.min(delay, SYNC_CONFIG.RETRY.MAX_DELAY);
}

// 注册重试同步
async function registerRetrySync() {
  try {
    await self.registration.sync.register(SYNC_CONFIG.TAGS.RETRY_FAILED);
    console.log("[Background Sync] 重试同步已注册");
  } catch (error) {
    console.error("[Background Sync] 注册重试同步失败:", error);
  }
}

// 通知客户端同步完成 - 分层事件通信架构的第1层
async function notifyClientsOfSyncCompletion(syncResult) {
  try {
    // 获取所有活跃的客户端页面
    const clients = await self.clients.matchAll({
      includeUncontrolled: true,
      type: "window",
    });

    if (clients.length === 0) {
      console.log("[Background Sync] 没有活跃的客户端页面需要通知");
      return;
    }

    // 构建标准化的通知消息
    const message = {
      type: "PWA_SYNC_COMPLETED",
      payload: {
        syncType: syncResult.type,
        successCount: syncResult.successCount,
        failureCount: syncResult.failureCount,
        totalProcessed: syncResult.totalProcessed,
        syncedOperations: syncResult.syncedOperations || [],
        timestamp: syncResult.timestamp,
        source: "background-sync",
      },
    };

    // 向所有客户端发送通知
    const notificationPromises = clients.map((client) => {
      try {
        client.postMessage(message);
        return Promise.resolve();
      } catch (error) {
        console.error("[Background Sync] 向客户端发送消息失败:", error);
        return Promise.reject(error);
      }
    });

    await Promise.allSettled(notificationPromises);
    console.log(`[Background Sync] 已通知 ${clients.length} 个客户端页面同步完成`, syncResult);
  } catch (error) {
    console.error("[Background Sync] 通知客户端失败:", error);
  }
}

// 重试失败的操作
async function retryFailedOperations() {
  try {
    console.log("[Background Sync] 开始重试失败的操作");

    const queue = await syncStorage.getOfflineQueue();
    const retryItems = queue.filter((item) => item.status === "retry" && new Date(item.nextRetry) <= new Date());

    if (retryItems.length === 0) {
      console.log("[Background Sync] 没有需要重试的操作");
      return;
    }

    console.log(`[Background Sync] 发现 ${retryItems.length} 个需要重试的操作`);

    // 将重试项目状态重置为pending，让主同步流程处理
    for (const item of retryItems) {
      await syncStorage.updateQueueItem(item.id, {
        status: "pending",
      });
    }

    // 触发主同步流程
    await syncOfflineQueue();
  } catch (error) {
    console.error("[Background Sync] 重试失败操作出错:", error);
    throw error;
  }
}

// 同步应用数据（可扩展）
async function syncApplicationData() {
  try {
    console.log("[Background Sync] 开始同步应用数据");

    // 这里可以添加其他应用数据的同步逻辑
    // 例如：用户设置、缓存更新等

    await syncStorage.updateSyncStatus({
      type: "application-data",
      status: "completed",
    });

    console.log("[Background Sync] 应用数据同步完成");
  } catch (error) {
    console.error("[Background Sync] 同步应用数据失败:", error);
    throw error;
  }
}

// 消息处理 - 与主应用通信
self.addEventListener("message", async (event) => {
  const { type, data } = event.data || {};

  switch (type) {
    case "REGISTER_BACKGROUND_SYNC":
      try {
        await self.registration.sync.register(data.tag || SYNC_CONFIG.TAGS.OFFLINE_QUEUE);
        event.ports[0]?.postMessage({ success: true });
      } catch (error) {
        event.ports[0]?.postMessage({ success: false, error: error.message });
      }
      break;

    case "GET_SYNC_STATUS":
      try {
        const queue = await syncStorage.getOfflineQueue();
        const pendingCount = queue.filter((item) => item.status === "pending").length;
        const failedCount = queue.filter((item) => item.status === "failed").length;

        event.ports[0]?.postMessage({
          success: true,
          data: { pendingCount, failedCount, totalCount: queue.length },
        });
      } catch (error) {
        event.ports[0]?.postMessage({ success: false, error: error.message });
      }
      break;

    default:
      console.warn(`[Background Sync] 未知的消息类型: ${type}`);
  }
});

console.log("[Background Sync] Service Worker 脚本已加载");
