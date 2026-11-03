<template>
  <AdminTable
    :data="pastes"
    :columns="pasteColumns"
    :column-classes="pasteColumnClasses"
    :manual-sorting="false"
    :selectable="true"
    :selected-items="selectedPastes"
    row-id-field="id"
    :empty-text="$t('admin.paste.table.noData')"
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
                {{ paste.title || paste.remark || paste.slug }}
              </span>
              <button @click="emit('copy-link', paste.slug)" class="ml-2 text-gray-400 hover:text-gray-600 dark:hover:text-gray-200 relative" title="复制链接">
                <IconCopy class="h-4 w-4" />
                <!-- 复制成功提示 -->
                <span v-if="copiedTexts[paste.id]" class="absolute -top-8 left-1/2 transform -translate-x-1/2 px-2 py-1 text-xs text-white bg-green-500 rounded whitespace-nowrap">
                  已复制
                </span>
              </button>
              <!-- 二维码按钮 -->
              <button @click="emit('show-qrcode', paste.slug)" class="ml-2 text-gray-400 hover:text-gray-600 dark:hover:text-gray-200" title="显示二维码">
                <IconQrCode class="h-4 w-4" />
              </button>
            </div>

            <!-- 右侧：操作按钮组 -->
            <div class="flex space-x-2">
              <!-- 预览按钮 -->
              <button class="text-primary-600 hover:text-primary-900 dark:text-primary-400 dark:hover:text-primary-300 p-1" @click="emit('preview', paste)">
                <IconEye class="h-5 w-5" />
              </button>

              <!-- 编辑按钮 -->
              <button class="text-indigo-600 hover:text-indigo-900 dark:text-indigo-400 dark:hover:text-indigo-300 p-1" @click="emit('edit', paste)">
                <IconRename class="h-5 w-5" />
              </button>

              <!-- 复制原始文本直链按钮 -->
              <button class="text-purple-600 hover:text-purple-900 dark:text-purple-400 dark:hover:text-purple-300 p-1 relative" @click="emit('copy-raw-link', paste.slug)">
                <IconLink class="h-5 w-5" />
                <!-- 原始文本直链复制成功提示 -->
                <span v-if="copiedRawTexts[paste.id]" class="absolute -top-8 right-0 px-2 py-1 text-xs text-white bg-green-500 rounded whitespace-nowrap">已复制直链</span>
              </button>

              <!-- 删除按钮 -->
              <button class="text-red-600 hover:text-red-900 dark:text-red-400 dark:hover:text-red-300 p-1" @click="emit('delete', paste.id)">
                <IconDelete class="h-5 w-5" />
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

            <div v-if="paste.tags?.length" class="flex">
              <span class="text-gray-500 dark:text-gray-400 w-16 flex-shrink-0">标签:</span>
              <PasteTagPills :tags="paste.tags" :max="3" size="xs" />
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

            <!-- 可见性 -->
            <div class="flex">
              <span class="text-gray-500 dark:text-gray-400 w-16 flex-shrink-0">可见性:</span>
              <span
                v-if="paste.is_public === false"
                class="inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium bg-gray-100 text-gray-700 dark:bg-gray-700 dark:text-gray-200"
              >
                <IconEyeOff class="h-3 w-3 mr-0.5" />
                仅内部
              </span>
              <span
                v-else
                class="inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium bg-green-100 text-green-700 dark:bg-green-800 dark:text-green-100"
              >
                <IconGlobeAlt class="h-3 w-3 mr-0.5" />
                公开
              </span>
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
import PasteTagPills from "./PasteTagPills.vue";
import { IconCopy, IconDelete, IconEye, IconEyeOff, IconGlobeAlt, IconLink, IconQrCode, IconRename } from "@/components/icons";

// 导入统一的时间处理工具
import { formatDateTime, formatExpiry as formatExpiryUtil, formatRelativeTime, parseUTCDate, isExpired as isExpiredUtil } from "@/utils/timeUtils.js";
// 导入创建者徽章统一逻辑
import { creatorBadgeUtils } from "@/composables/admin-management/useCreatorBadge.js";

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

const emit = defineEmits(["toggle-select-all", "toggle-select-item", "view", "copy-link", "copy-raw-link", "preview", "edit", "delete", "show-qrcode", "toggle-visibility"]);

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

// 加密徽章样式（参考文件管理）
const passwordBadgeClass = computed(
  () =>
    `inline-flex items-center gap-1 rounded-full border px-1.5 py-0.5 text-[11px] font-semibold uppercase tracking-wide ${
      props.darkMode ? "bg-amber-500/15 text-amber-100 border-amber-400/30" : "bg-amber-50 text-amber-700 border-amber-200"
    }`
);

const passwordIconClass = computed(() => (props.darkMode ? "text-amber-200" : "text-amber-600"));

// 渲染加密徽章
const renderPasswordBadge = () =>
  h(
    "span",
    {
      class: `${passwordBadgeClass.value} ml-2`,
      title: "密码保护",
    },
    [
      h(
        "svg",
        {
          xmlns: "http://www.w3.org/2000/svg",
          class: `h-3 w-3 ${passwordIconClass.value}`,
          fill: "none",
          viewBox: "0 0 24 24",
          stroke: "currentColor",
        },
        [
          h("path", {
            "stroke-linecap": "round",
            "stroke-linejoin": "round",
            "stroke-width": "2",
            d: "M7 11V7a5 5 0 0110 0v4",
          }),
          h("rect", {
            "stroke-linecap": "round",
            "stroke-linejoin": "round",
            "stroke-width": "2",
            x: "6",
            y: "11",
            width: "12",
            height: "9",
            rx: "2",
          }),
        ]
      ),
      h(
        "span",
        {
          class: "leading-none",
        },
        "加密"
      ),
    ]
  );

// 创建columns配置
const pasteColumns = computed(() => [
  {
    key: "slug",
    type: "accessor",
    header: "标题",
    sortable: true,
    render: (_, paste) => {
      if (!paste) return h("span", "无数据");
      const displayTitle = paste.title || paste.slug || paste.remark;
      return h("div", { class: "flex max-w-[220px] flex-wrap items-center gap-1.5" }, [
        // 标题和加密徽章容器
        h("div", { class: "flex items-center" }, [
          h(
            "span",
            {
              class: ["cursor-pointer hover:underline truncate max-w-[120px]", isExpired(paste) ? "text-red-600 dark:text-red-400" : "text-primary-600 dark:text-primary-400"],
              title: displayTitle,
              onClick: () => emit("view", paste.slug),
            },
            displayTitle
          ),
          // 加密徽章（仅在有密码时显示）
          paste.has_password ? renderPasswordBadge() : null,
        ]),
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
        paste.tags?.length ? h(PasteTagPills, { tags: paste.tags, max: 2, size: "xs" }) : null,
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

      if (!paste.expires_at) {
        return h("span", {
          class: props.darkMode ? "text-gray-300" : "text-gray-500"
        }, "永不过期");
      }

      // 直接使用现有工具函数
      const date = formatDateTime(paste.expires_at);
      const relative = formatRelativeTime(paste.expires_at);

      // 计算颜色（组件内逻辑，不污染工具类）
      const expiryDate = parseUTCDate(paste.expires_at);
      const now = new Date();
      let colorClass;

      if (expiryDate < now) {
        colorClass = "text-red-600 dark:text-red-400";
      } else {
        const daysDiff = (expiryDate.getTime() - now.getTime()) / (1000 * 3600 * 24);
        if (daysDiff <= 1) {
          colorClass = "text-orange-600 dark:text-orange-400";
        } else if (daysDiff <= 7) {
          colorClass = "text-yellow-600 dark:text-yellow-400";
        } else {
          colorClass = "text-green-600 dark:text-green-400";
        }
      }

      return h("div", { class: "flex flex-col" }, [
        // 第一行：日期
        h("span", {
          class: props.darkMode ? "text-gray-300" : "text-gray-700"
        }, date),
        // 第二行：相对时间（小字+颜色）
        relative ? h("span", {
          class: ["text-xs", colorClass]
        }, relative) : null
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
        } else if (remaining < 10) {
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
      // 使用统一的创建者徽章工具
      const badgeClass = creatorBadgeUtils.getBadgeClass(paste.created_by, paste.key_name);
      const creatorType = creatorBadgeUtils.getCreatorType(paste.created_by, paste.key_name);
      let text = "未知来源";
      if (creatorType === "admin") {
        text = "管理员";
      } else if (creatorType === "apikey") {
        text = paste.key_name ? `密钥：${paste.key_name}` : `密钥：${paste.created_by?.substring(7, 12) || ""}...`;
      } else if (paste.created_by) {
        text = paste.created_by;
      }
      return h("div", { class: "flex items-center justify-center" }, [
        h(
          "span",
          {
            class: `px-2 py-0.5 text-xs rounded inline-block text-center ${badgeClass}`,
          },
          text
        ),
      ]);
    },
  },
  {
    key: "is_public",
    type: "display",
    header: "可见性",
    sortable: true,
    render: (paste) => {
      if (!paste) return h("span", "无数据");

      return h("div", { class: "flex items-center justify-center space-x-2" }, [
        // 滑块按钮
        h("button", {
          type: "button",
          onClick: (e) => {
            e.stopPropagation();
            emit("toggle-visibility", paste);
          },
          class: [
            "relative inline-flex h-6 w-11 items-center rounded-full transition-colors focus:outline-none focus:ring-2 focus:ring-primary-500 focus:ring-offset-2 cursor-pointer",
            paste.is_public
              ? "bg-primary-600"
              : (props.darkMode ? "bg-gray-700" : "bg-gray-200")
          ]
        }, [
          // 滑块圆点
          h("span", {
            class: [
              "inline-block h-4 w-4 transform rounded-full bg-white transition-transform",
              paste.is_public ? "translate-x-6" : "translate-x-1"
            ]
          })
        ]),
        // 状态文字
        h("span", {
          class: ["text-xs", props.darkMode ? "text-gray-300" : "text-gray-700"]
        }, paste.is_public ? "公开" : "私密")
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
            onClick: () => emit("copy-raw-link", paste.slug),
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
  remark: "hidden sm:table-cell text-center",
  created_at: "hidden md:table-cell text-center",
  expires_at: "hidden lg:table-cell text-center",
  is_public: "hidden sm:table-cell text-center",
  remaining_views: "text-center",
  created_by: "hidden lg:table-cell text-center",
  actions: "text-center",
}));
</script>
