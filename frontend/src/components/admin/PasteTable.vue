<template>
  <AdminTable
    :data="pastes"
    :columns="pasteColumns"
    :column-classes="pasteColumnClasses"
    :manual-sorting="false"
    :selectable="true"
    :selected-items="selectedPastes"
    row-id-field="id"
    empty-text="暂无文本分享数据"
    @selection-change="handleSelectionChange"
  >
    <template #mobile="{ data }">
      <!-- 移动端卡片组件 - 小于中等设备显示 -->
      <div v-for="paste in data" :key="paste.id" class="bg-white dark:bg-gray-800 border-b dark:border-gray-700 last:border-b-0">
        <!-- 卡片头部 - 复选框、链接和操作按钮 -->
        <div class="p-4">
          <div class="flex justify-between items-start mb-2">
            <!-- 左侧：复选框、链接和复制按钮 -->
            <div class="flex items-center">
              <input
                type="checkbox"
                :checked="selectedPastes.includes(paste.id)"
                @click="handleMobileSelect(paste.id)"
                class="h-4 w-4 text-primary-600 focus:ring-primary-500 border-gray-300 rounded cursor-pointer mr-2"
              />
              <span
                class="cursor-pointer font-medium text-base hover:underline"
                @click="emit('view', paste.slug)"
                :class="isExpired(paste) ? 'text-red-600 dark:text-red-400' : 'text-primary-600 dark:text-primary-400'"
              >
                {{ paste.slug }}
              </span>
              <button @click="emit('copy-link', paste.slug)" class="ml-2 text-gray-400 hover:text-gray-600 dark:hover:text-gray-200 relative" title="复制链接">
                <svg xmlns="http://www.w3.org/2000/svg" class="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path
                    stroke-linecap="round"
                    stroke-linejoin="round"
                    stroke-width="2"
                    d="M8 16H6a2 2 0 01-2-2V6a2 2 0 012-2h8a2 2 0 012 2v2m-6 12h8a2 2 0 002-2v-8a2 2 0 00-2-2h-8a2 2 0 00-2 2v8a2 2 0 002 2z"
                  />
                </svg>
                <!-- 复制成功提示 -->
                <span v-if="copiedTexts[paste.id]" class="absolute -top-8 left-1/2 transform -translate-x-1/2 px-2 py-1 text-xs text-white bg-green-500 rounded whitespace-nowrap">
                  已复制
                </span>
              </button>
              <!-- 二维码按钮 -->
              <button @click="emit('show-qrcode', paste.slug)" class="ml-2 text-gray-400 hover:text-gray-600 dark:hover:text-gray-200" title="显示二维码">
                <svg xmlns="http://www.w3.org/2000/svg" class="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path
                    stroke-linecap="round"
                    stroke-linejoin="round"
                    stroke-width="2"
                    d="M12 4v1m6 11h2m-6 0h-2v4m0-11v3m0 0h.01M12 12h4.01M16 20h4M4 12h4m12 0h.01M5 8h2a1 1 0 001-1V5a1 1 0 00-1-1H5a1 1 0 00-1 1v2a1 1 0 001 1zm12 0h2a1 1 0 001-1V5a1 1 0 00-1-1h-2a1 1 0 00-1 1v2a1 1 0 001 1zM5 20h2a1 1 0 001-1v-2a1 1 0 00-1-1H5a1 1 0 00-1 1v2a1 1 0 001 1z"
                  />
                </svg>
              </button>
            </div>

            <!-- 右侧：操作按钮组 -->
            <div class="flex space-x-2">
              <!-- 预览按钮 -->
              <button class="text-primary-600 hover:text-primary-900 dark:text-primary-400 dark:hover:text-primary-300 p-1" @click="emit('preview', paste)">
                <svg xmlns="http://www.w3.org/2000/svg" class="h-5 w-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 12a3 3 0 11-6 0 3 3 0 016 0z" />
                  <path
                    stroke-linecap="round"
                    stroke-linejoin="round"
                    stroke-width="2"
                    d="M2.458 12C3.732 7.943 7.523 5 12 5c4.478 0 8.268 2.943 9.542 7-1.274 4.057-5.064 7-9.542 7-4.477 0-8.268-2.943-9.542-7z"
                  />
                </svg>
              </button>

              <!-- 编辑按钮 -->
              <button class="text-indigo-600 hover:text-indigo-900 dark:text-indigo-400 dark:hover:text-indigo-300 p-1" @click="emit('edit', paste)">
                <svg xmlns="http://www.w3.org/2000/svg" class="h-5 w-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path
                    stroke-linecap="round"
                    stroke-linejoin="round"
                    stroke-width="2"
                    d="M11 5H6a2 2 0 00-2 2v11a2 2 0 002 2h11a2 2 0 002-2v-5m-1.414-9.414a2 2 0 112.828 2.828L11.828 15H9v-2.828l8.586-8.586z"
                  />
                </svg>
              </button>

              <!-- 复制原始文本直链按钮 -->
              <button class="text-purple-600 hover:text-purple-900 dark:text-purple-400 dark:hover:text-purple-300 p-1 relative" @click="emit('copy-raw-link', paste)">
                <svg xmlns="http://www.w3.org/2000/svg" class="h-5 w-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13.828 10.172a4 4 0 00-5.656 0l-4 4a4 4 0 105.656 5.656l1.102-1.101" />
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10.172 13.828a4 4 0 015.656 0l4 4a4 4 0 01-5.656 5.656l-1.102-1.101" />
                </svg>
                <!-- 原始文本直链复制成功提示 -->
                <span v-if="copiedRawTexts[paste.id]" class="absolute -top-8 right-0 px-2 py-1 text-xs text-white bg-green-500 rounded whitespace-nowrap">已复制直链</span>
              </button>

              <!-- 删除按钮 -->
              <button class="text-red-600 hover:text-red-900 dark:text-red-400 dark:hover:text-red-300 p-1" @click="emit('delete', paste.id)">
                <svg xmlns="http://www.w3.org/2000/svg" class="h-5 w-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path
                    stroke-linecap="round"
                    stroke-linejoin="round"
                    stroke-width="2"
                    d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16"
                  />
                </svg>
              </button>
            </div>
          </div>

          <!-- 卡片内容 - 详细信息 -->
          <div class="mt-3 space-y-2 text-sm">
            <!-- 备注信息 -->
            <div v-if="paste.remark" class="flex">
              <span class="text-gray-500 dark:text-gray-400 w-16 flex-shrink-0">备注:</span>
              <span :class="darkMode ? 'text-gray-300' : 'text-gray-700'">{{ truncateText(paste.remark, 30) }}</span>
            </div>

            <!-- 过期时间 -->
            <div v-if="paste.expires_at" class="flex">
              <span class="text-gray-500 dark:text-gray-400 w-16 flex-shrink-0">过期:</span>
              <span :class="getExpiryClass(paste.expires_at)">{{ formatExpiry(paste.expires_at) }}</span>
            </div>

            <!-- 查看次数 -->
            <div class="flex">
              <span class="text-gray-500 dark:text-gray-400 w-16 flex-shrink-0">查看:</span>
              <span :class="darkMode ? 'text-gray-300' : 'text-gray-700'">{{ paste.view_count || 0 }}{{ paste.max_views ? `/${paste.max_views}` : "" }} 次</span>
            </div>

            <!-- 创建时间 -->
            <div class="flex">
              <span class="text-gray-500 dark:text-gray-400 w-16 flex-shrink-0">创建:</span>
              <span :class="darkMode ? 'text-gray-300' : 'text-gray-700'">{{ formatDate(paste.created_at) }}</span>
            </div>
          </div>
        </div>
      </div>
    </template>
  </AdminTable>
</template>

<script setup>
import { computed, h } from "vue";
import AdminTable from "@/components/common/AdminTable.vue";

// 导入统一的时间处理工具
import { formatDateTime, formatExpiry as formatExpiryUtil, isExpired as isExpiredUtil } from "@/utils/timeUtils.js";

const props = defineProps({
  darkMode: {
    type: Boolean,
    required: true,
  },
  pastes: {
    type: Array,
    required: true,
  },
  loading: {
    type: Boolean,
    default: false,
  },
  selectedPastes: {
    type: Array,
    required: true,
  },
  copiedTexts: {
    type: Object,
    default: () => ({}),
  },
  copiedRawTexts: {
    type: Object,
    default: () => ({}),
  },
});

const emit = defineEmits(["toggle-select-all", "toggle-select-item", "view", "copy-link", "copy-raw-link", "preview", "edit", "delete", "show-qrcode"]);

// 工具函数
const truncateText = (text, length = 10) => {
  if (!text) return "无";
  return text.length <= length ? text : `${text.substring(0, length)}...`;
};

const formatDate = (dateString) => {
  return formatDateTime(dateString);
};

const formatExpiry = (dateString) => {
  return formatExpiryUtil(dateString);
};

const isExpired = (paste) => {
  if (!paste.expires_at) return false;
  return isExpiredUtil(paste.expires_at);
};

const getExpiryClass = (dateString) => {
  if (!dateString) return "";
  const now = new Date();
  const expiryDate = new Date(dateString);
  if (expiryDate < now) {
    return "text-red-600 dark:text-red-400";
  } else {
    const timeDiff = expiryDate.getTime() - now.getTime();
    const daysDiff = timeDiff / (1000 * 3600 * 24);
    if (daysDiff <= 1) {
      return "text-orange-600 dark:text-orange-400";
    } else if (daysDiff <= 7) {
      return "text-yellow-600 dark:text-yellow-400";
    } else {
      return "text-green-600 dark:text-green-400";
    }
  }
};

// 移动端选择处理
const handleMobileSelect = (pasteId) => {
  emit("toggle-select-item", pasteId);
};

// 选择变化处理
const handleSelectionChange = (event) => {
  if (event.type === "toggle-all") {
    emit("toggle-select-all");
  } else if (event.type === "toggle-item") {
    emit("toggle-select-item", event.id);
  }
};

// 创建columns配置
const pasteColumns = computed(() => [
  {
    key: "slug",
    type: "accessor",
    header: "链接",
    sortable: true,
    render: (_, paste) => {
      if (!paste) return h("span", "无数据");
      return h("div", { class: "flex items-center space-x-2" }, [
        h(
          "span",
          {
            class: ["cursor-pointer hover:underline truncate max-w-[120px]", isExpired(paste) ? "text-red-600 dark:text-red-400" : "text-primary-600 dark:text-primary-400"],
            title: paste.slug,
            onClick: () => emit("view", paste.slug),
          },
          paste.slug
        ),
        // 复制链接按钮
        h(
          "button",
          {
            class: "text-gray-400 hover:text-gray-600 dark:hover:text-gray-200 p-1 hover:bg-gray-100 dark:hover:bg-gray-700 rounded-full relative",
            title: "复制链接",
            onClick: () => emit("copy-link", paste.slug),
          },
          [
            h(
              "svg",
              {
                xmlns: "http://www.w3.org/2000/svg",
                class: "h-5 w-5",
                fill: "none",
                viewBox: "0 0 24 24",
                stroke: "currentColor",
              },
              [
                h("path", {
                  "stroke-linecap": "round",
                  "stroke-linejoin": "round",
                  "stroke-width": "2",
                  d: "M8 16H6a2 2 0 01-2-2V6a2 2 0 012-2h8a2 2 0 012 2v2m-6 12h8a2 2 0 002-2v-8a2 2 0 00-2-2h-8a2 2 0 00-2 2v8a2 2 0 002 2z",
                }),
              ]
            ),
            // 复制成功提示
            props.copiedTexts[paste.id]
              ? h(
                  "span",
                  {
                    class: "absolute -top-8 left-1/2 transform -translate-x-1/2 px-2 py-1 text-xs text-white bg-green-500 rounded whitespace-nowrap",
                  },
                  "已复制"
                )
              : null,
          ]
        ),
        // 二维码按钮
        h(
          "button",
          {
            class: "text-gray-400 hover:text-gray-600 dark:hover:text-gray-200 p-1 hover:bg-gray-100 dark:hover:bg-gray-700 rounded-full",
            title: "显示二维码",
            onClick: () => emit("show-qrcode", paste.slug),
          },
          [
            h(
              "svg",
              {
                xmlns: "http://www.w3.org/2000/svg",
                class: "h-5 w-5",
                fill: "none",
                viewBox: "0 0 24 24",
                stroke: "currentColor",
              },
              [
                h("path", {
                  "stroke-linecap": "round",
                  "stroke-linejoin": "round",
                  "stroke-width": "2",
                  d: "M12 4v1m6 11h2m-6 0h-2v4m0-11v3m0 0h.01M12 12h4.01M16 20h4M4 12h4m12 0h.01M5 8h2a1 1 0 001-1V5a1 1 0 00-1-1H5a1 1 0 00-1 1v2a1 1 0 001 1zm12 0h2a1 1 0 001-1V5a1 1 0 00-1-1h-2a1 1 0 00-1 1v2a1 1 0 001 1zM5 20h2a1 1 0 001-1v-2a1 1 0 00-1-1H5a1 1 0 00-1 1v2a1 1 0 001 1z",
                }),
              ]
            ),
          ]
        ),
      ]);
    },
  },
  {
    key: "remark",
    type: "accessor",
    header: "备注",
    sortable: true,
    render: (_, paste) => {
      if (!paste) return h("span", "无数据");
      return h(
        "span",
        {
          class: [
            "truncate max-w-[120px] inline-block px-2 py-0.5 rounded",
            paste.remark ? "bg-blue-50 dark:bg-blue-900 text-blue-700 dark:text-blue-300" : props.darkMode ? "text-gray-300" : "text-gray-500",
            isExpired(paste) ? "text-red-600 dark:text-red-400" : "",
          ],
          title: paste.remark || "无",
        },
        truncateText(paste.remark)
      );
    },
  },
  {
    key: "created_at",
    type: "accessor",
    header: "创建时间",
    sortable: true,
    render: (_, paste) => {
      if (!paste) return h("span", "无数据");
      return h(
        "span",
        {
          class: [props.darkMode ? "text-gray-300" : "text-gray-500", isExpired(paste) ? "text-red-600 dark:text-red-400" : ""],
        },
        formatDate(paste.created_at)
      );
    },
  },
  {
    key: "expires_at",
    type: "accessor",
    header: "过期时间",
    sortable: true,
    // 自定义排序函数：按过期时间排序，NULL值排在最后
    sortingFn: (rowA, rowB) => {
      const expiresA = rowA.original.expires_at;
      const expiresB = rowB.original.expires_at;

      // 处理 NULL 值：永不过期的排在最后
      if (!expiresA && !expiresB) return 0;
      if (!expiresA) return 1;
      if (!expiresB) return -1;

      // 比较日期
      const dateA = new Date(expiresA);
      const dateB = new Date(expiresB);
      return dateA.getTime() - dateB.getTime();
    },
    render: (_, paste) => {
      if (!paste) return h("span", "无数据");
      return h(
        "span",
        {
          class: [props.darkMode ? "text-gray-300" : "text-gray-500", isExpired(paste) ? "text-red-600 dark:text-red-400" : ""],
        },
        paste.expires_at ? formatExpiry(paste.expires_at) : "永不过期"
      );
    },
  },
  {
    key: "password",
    type: "display",
    header: "密码",
    sortable: false,
    render: (paste) => {
      if (!paste) return h("span", "无数据");
      return h("div", { class: "flex items-center" }, [
        paste.has_password
          ? h(
              "span",
              {
                class: [
                  "inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium",
                  isExpired(paste) ? "bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-300" : "bg-yellow-100 text-yellow-800 dark:bg-yellow-800 dark:text-yellow-100",
                ],
              },
              [
                h(
                  "svg",
                  {
                    xmlns: "http://www.w3.org/2000/svg",
                    class: "h-3 w-3 mr-1",
                    fill: "none",
                    viewBox: "0 0 24 24",
                    stroke: "currentColor",
                  },
                  [
                    h("path", {
                      "stroke-linecap": "round",
                      "stroke-linejoin": "round",
                      "stroke-width": "2",
                      d: "M12 15v2m-6 4h12a2 2 0 002-2v-6a2 2 0 00-2-2H6a2 2 0 00-2 2v6a2 2 0 002 2zm10-10V7a4 4 0 00-8 0v4h8z",
                    }),
                  ]
                ),
                "已加密",
              ]
            )
          : h(
              "span",
              {
                class: [
                  "inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium",
                  isExpired(paste) ? "bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-300" : "bg-green-100 text-green-800 dark:bg-green-800 dark:text-green-100",
                ],
              },
              [
                h(
                  "svg",
                  {
                    xmlns: "http://www.w3.org/2000/svg",
                    class: "h-3 w-3 mr-1",
                    fill: "none",
                    viewBox: "0 0 24 24",
                    stroke: "currentColor",
                  },
                  [
                    h("path", {
                      "stroke-linecap": "round",
                      "stroke-linejoin": "round",
                      "stroke-width": "2",
                      d: "M8 11V7a4 4 0 118 0m-4 8v2m-6 4h12a2 2 0 002-2v-6a2 2 0 00-2-2H6a2 2 0 00-2 2v6a2 2 0 002 2z",
                    }),
                  ]
                ),
                "公开",
              ]
            ),
      ]);
    },
  },
  {
    key: "remaining_views",
    type: "display",
    header: "剩余次数",
    sortable: false,
    render: (paste) => {
      if (!paste) return h("span", "无数据");
      const currentViews = paste.view_count || 0;
      const maxViews = paste.max_views;

      let remainingText, remainingClass;
      if (!maxViews) {
        remainingText = "无限制";
        remainingClass = props.darkMode ? "text-gray-300" : "text-gray-500";
      } else {
        const remaining = maxViews - currentViews;
        if (remaining <= 0) {
          remainingText = "已用完";
          remainingClass = "text-red-500";
        } else if (remaining < 3) {
          remainingText = `${remaining} 次`;
          remainingClass = "text-yellow-500";
        } else {
          remainingText = `${remaining} 次`;
          remainingClass = "text-green-600 dark:text-green-400";
        }
      }

      return h(
        "span",
        {
          class: [props.darkMode ? "text-gray-300" : "text-gray-500", isExpired(paste) ? "text-red-600 dark:text-red-400" : remainingClass],
        },
        remainingText
      );
    },
  },
  {
    key: "created_by",
    type: "accessor",
    header: "创建者",
    sortable: true,
    render: (_, paste) => {
      if (!paste) return h("span", "无数据");
      return h("div", { class: "flex items-center justify-center" }, [
        paste.created_by && paste.created_by.startsWith("apikey:")
          ? h(
              "span",
              {
                class: "px-2 py-0.5 text-xs rounded bg-blue-100 text-blue-800 dark:bg-blue-800 dark:text-blue-100 inline-block text-center",
              },
              paste.key_name ? `密钥：${paste.key_name}` : `密钥：${paste.created_by.substring(7, 12)}...`
            )
          : paste.created_by === "admin" || (paste.created_by && /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(paste.created_by))
          ? h(
              "span",
              {
                class: "px-2 py-0.5 text-xs rounded bg-green-100 text-green-800 dark:bg-green-800 dark:text-green-100 inline-block text-center",
              },
              "管理员"
            )
          : h(
              "span",
              {
                class: "px-2 py-0.5 text-xs rounded bg-gray-100 text-gray-800 dark:bg-gray-700 dark:text-gray-300 inline-block text-center",
              },
              paste.created_by || "未知来源"
            ),
      ]);
    },
  },
  {
    key: "actions",
    type: "display",
    header: "操作",
    sortable: false,
    render: (paste) => {
      if (!paste) return h("span", "无数据");
      return h("div", { class: "flex justify-end space-x-2" }, [
        // 预览按钮
        h(
          "button",
          {
            class: "text-primary-600 hover:text-primary-900 dark:text-primary-400 dark:hover:text-primary-300 p-1.5 rounded-full hover:bg-gray-100 dark:hover:bg-gray-700",
            title: "预览",
            onClick: () => emit("preview", paste),
          },
          [
            h(
              "svg",
              {
                xmlns: "http://www.w3.org/2000/svg",
                class: "h-5 w-5",
                fill: "none",
                viewBox: "0 0 24 24",
                stroke: "currentColor",
              },
              [
                h("path", { "stroke-linecap": "round", "stroke-linejoin": "round", "stroke-width": "2", d: "M15 12a3 3 0 11-6 0 3 3 0 016 0z" }),
                h("path", {
                  "stroke-linecap": "round",
                  "stroke-linejoin": "round",
                  "stroke-width": "2",
                  d: "M2.458 12C3.732 7.943 7.523 5 12 5c4.478 0 8.268 2.943 9.542 7-1.274 4.057-5.064 7-9.542 7-4.477 0-8.268-2.943-9.542-7z",
                }),
              ]
            ),
          ]
        ),
        // 编辑按钮
        h(
          "button",
          {
            class: "text-indigo-600 hover:text-indigo-900 dark:text-indigo-400 dark:hover:text-indigo-300 p-1.5 rounded-full hover:bg-gray-100 dark:hover:bg-gray-700",
            title: "编辑",
            onClick: () => emit("edit", paste),
          },
          [
            h(
              "svg",
              {
                xmlns: "http://www.w3.org/2000/svg",
                class: "h-5 w-5",
                fill: "none",
                viewBox: "0 0 24 24",
                stroke: "currentColor",
              },
              [
                h("path", {
                  "stroke-linecap": "round",
                  "stroke-linejoin": "round",
                  "stroke-width": "2",
                  d: "M11 5H6a2 2 0 00-2 2v11a2 2 0 002 2h11a2 2 0 002-2v-5m-1.414-9.414a2 2 0 112.828 2.828L11.828 15H9v-2.828l8.586-8.586z",
                }),
              ]
            ),
          ]
        ),
        // 复制原始链接按钮
        h(
          "button",
          {
            class: "text-purple-600 hover:text-purple-900 dark:text-purple-400 dark:hover:text-purple-300 p-1.5 rounded-full hover:bg-gray-100 dark:hover:bg-gray-700 relative",
            title: "复制原始链接",
            onClick: () => emit("copy-raw-link", paste),
          },
          [
            h(
              "svg",
              {
                xmlns: "http://www.w3.org/2000/svg",
                class: "h-5 w-5",
                fill: "none",
                viewBox: "0 0 24 24",
                stroke: "currentColor",
              },
              [
                h("path", {
                  "stroke-linecap": "round",
                  "stroke-linejoin": "round",
                  "stroke-width": "2",
                  d: "M13.828 10.172a4 4 0 00-5.656 0l-4 4a4 4 0 105.656 5.656l1.102-1.101",
                }),
                h("path", {
                  "stroke-linecap": "round",
                  "stroke-linejoin": "round",
                  "stroke-width": "2",
                  d: "M10.172 13.828a4 4 0 015.656 0l4 4a4 4 0 01-5.656 5.656l-1.102-1.101",
                }),
              ]
            ),
            // 复制成功提示
            props.copiedRawTexts[paste.id]
              ? h(
                  "span",
                  {
                    class: "absolute -top-8 right-0 px-2 py-1 text-xs text-white bg-green-500 rounded whitespace-nowrap",
                  },
                  "已复制直链"
                )
              : null,
          ]
        ),
        // 删除按钮
        h(
          "button",
          {
            class: "text-red-600 hover:text-red-900 dark:text-red-400 dark:hover:text-red-300 p-1.5 rounded-full hover:bg-gray-100 dark:hover:bg-gray-700",
            title: "删除",
            onClick: () => emit("delete", paste.id),
          },
          [
            h(
              "svg",
              {
                xmlns: "http://www.w3.org/2000/svg",
                class: "h-5 w-5",
                fill: "none",
                viewBox: "0 0 24 24",
                stroke: "currentColor",
              },
              [
                h("path", {
                  "stroke-linecap": "round",
                  "stroke-linejoin": "round",
                  "stroke-width": "2",
                  d: "M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16",
                }),
              ]
            ),
          ]
        ),
      ]);
    },
  },
]);

// 列样式配置
const pasteColumnClasses = computed(() => ({
  slug: "",
  remark: "hidden sm:table-cell",
  created_at: "hidden md:table-cell",
  expires_at: "hidden lg:table-cell",
  password: "hidden sm:table-cell",
  remaining_views: "",
  created_by: "hidden lg:table-cell",
  actions: "",
}));
</script>
