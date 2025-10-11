<template>
  <AdminTable
    :data="files"
    :columns="fileColumns"
    :column-classes="fileColumnClasses"
    :manual-sorting="false"
    :selectable="true"
    :selected-items="selectedFiles"
    row-id-field="id"
    empty-text="暂无文件数据"
    :loading="loading"
    @selection-change="handleSelectionChange"
  >
    <template #mobile="{ data }">
      <!-- 移动端卡片组件 - 小于中等设备显示 -->
      <div v-for="file in data" :key="file.id" class="bg-white dark:bg-gray-800 border-b dark:border-gray-700 last:border-b-0">
        <!-- 卡片头部 - 复选框和文件名 -->
        <div class="p-3 border-b border-gray-200 dark:border-gray-700">
          <div class="flex items-start">
            <div class="flex-shrink-0 mr-3 mt-1">
              <input
                type="checkbox"
                :checked="selectedFiles.includes(file.id)"
                @click="handleMobileSelect(file.id)"
                class="h-4 w-4 text-primary-600 focus:ring-primary-500 border-gray-300 rounded cursor-pointer"
              />
            </div>
            <div class="flex-1">
              <div class="flex items-center">
                <!-- 文件图标 -->
                <div class="flex-shrink-0 mr-2 w-5 h-5">
                  <span v-html="getFileIconClassLocal(file)"></span>
                </div>
                <!-- 文件名 -->
                <div class="font-medium" :class="darkMode ? 'text-white' : 'text-gray-900'" :title="file.filename">{{ truncateFilename(file.filename) }}</div>
                <span v-if="file.has_password" class="ml-2" :title="'密码保护'">
                  <svg
                    xmlns="http://www.w3.org/2000/svg"
                    class="h-4 w-4"
                    :class="darkMode ? 'text-yellow-400' : 'text-yellow-600'"
                    fill="none"
                    viewBox="0 0 24 24"
                    stroke="currentColor"
                  >
                    <path
                      stroke-linecap="round"
                      stroke-linejoin="round"
                      stroke-width="2"
                      d="M12 15v2m-6 4h12a2 2 0 002-2v-6a2 2 0 00-2-2H6a2 2 0 00-2 2v6a2 2 0 002 2zm10-10V7a4 4 0 00-8 0v4h8z"
                    />
                  </svg>
                </span>
              </div>
              <div class="text-xs mt-1 truncate" :class="darkMode ? 'text-gray-400' : 'text-gray-500'" :title="file.slug ? `/${file.slug}` : '无短链接'">
                {{ file.slug ? `/${file.slug}` : "无短链接" }}
              </div>
              <div v-if="file.remark" class="text-xs mt-1 italic truncate max-w-xs overflow-hidden" :class="darkMode ? 'text-blue-400' : 'text-blue-600'" :title="file.remark">
                {{ file.remark }}
              </div>
            </div>
          </div>
        </div>

        <!-- 文件详细信息 -->
        <div class="p-4 grid grid-cols-2 gap-4 text-sm bg-gray-50 dark:bg-gray-700/50">
          <!-- 文件大小 -->
          <div>
            <div class="text-xs font-medium uppercase" :class="darkMode ? 'text-gray-500' : 'text-gray-500'">大小</div>
            <div :class="darkMode ? 'text-gray-300' : 'text-gray-700'">{{ formatFileSize(file.size) }}</div>
          </div>

          <!-- MIME类型 -->
          <div>
            <div class="text-xs font-medium uppercase" :class="darkMode ? 'text-gray-500' : 'text-gray-500'">类型</div>
            <div>
              <span
                class="px-2 py-0.5 text-xs rounded inline-block max-w-full truncate"
                :class="getMimeTypeClass(file)"
                :title="getSimpleMimeType(file.mimetype, file.filename, file)"
              >
                {{ getSimpleMimeType(file.mimetype, file.filename, file) }}
              </span>
            </div>
          </div>

          <!-- 剩余次数 -->
          <div>
            <div class="text-xs font-medium uppercase" :class="darkMode ? 'text-gray-500' : 'text-gray-500'">剩余次数</div>
            <div :class="getRemainingViewsClass(file)">
              {{ getRemainingViews(file) === "无限制" ? "无限制" : getRemainingViews(file) === "已用完" ? "已用完" : `${getRemainingViews(file)} 次` }}
            </div>
            <div v-if="file.views && file.max_views" class="text-xs" :class="darkMode ? 'text-gray-400' : 'text-gray-500'">已用: {{ file.views || 0 }}/{{ file.max_views }}</div>
            <div v-if="file.expires_at" class="text-xs" :class="expiresClass(file.expires_at)">
              {{ formatExpiry(file.expires_at) }}
            </div>
          </div>

          <!-- 存储配置 -->
          <div>
            <div class="text-xs font-medium uppercase" :class="darkMode ? 'text-gray-500' : 'text-gray-500'">存储配置</div>
            <div :class="darkMode ? 'text-gray-300' : 'text-gray-700'">{{ file.storage_config_name || "默认存储" }}</div>
            <div class="text-xs" :class="darkMode ? 'text-gray-400' : 'text-gray-500'">
              {{ file.storage_provider_type || "未知" }}
            </div>
          </div>

          <!-- 创建者 -->
          <div>
            <div class="text-xs font-medium uppercase" :class="darkMode ? 'text-gray-500' : 'text-gray-500'">创建者</div>
            <div :class="darkMode ? 'text-gray-300' : 'text-gray-700'" class="flex flex-col">
              <span
                v-if="file.created_by && file.created_by.startsWith('apikey:')"
                class="px-2 py-1 text-xs rounded bg-blue-100 text-blue-800 dark:bg-blue-800 dark:text-blue-100 inline-block mt-1 w-fit"
              >
                {{ file.key_name ? `密钥：${file.key_name}` : `密钥：${file.created_by.substring(7, 12)}...` }}
              </span>
              <span v-else-if="file.created_by" class="px-2 py-1 text-xs rounded bg-green-100 text-green-800 dark:bg-green-800 dark:text-green-100 inline-block mt-1 w-fit">
                管理员
              </span>
              <span v-else class="px-2 py-1 text-xs rounded bg-gray-100 text-gray-800 dark:bg-gray-700 dark:text-gray-300 inline-block mt-1 w-fit"> 未知来源 </span>
            </div>
          </div>

          <!-- 创建时间 -->
          <div class="col-span-2">
            <div class="text-xs font-medium uppercase" :class="darkMode ? 'text-gray-500' : 'text-gray-500'">创建时间</div>
            <div :class="darkMode ? 'text-gray-300' : 'text-gray-700'">{{ formatDate(file.created_at) }}</div>
          </div>
        </div>

        <!-- 操作按钮 -->
        <div class="p-4 border-t border-gray-200 dark:border-gray-700 flex justify-end space-x-2">
          <button @click="$emit('preview', file)" class="p-2 rounded-md" :class="darkMode ? 'bg-gray-700 text-blue-400' : 'bg-gray-100 text-blue-600'">
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
          <button @click="$emit('edit', file)" class="p-2 rounded-md" :class="darkMode ? 'bg-gray-700 text-green-400' : 'bg-gray-100 text-green-600'">
            <svg xmlns="http://www.w3.org/2000/svg" class="h-5 w-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path
                stroke-linecap="round"
                stroke-linejoin="round"
                stroke-width="2"
                d="M11 5H6a2 2 0 00-2 2v11a2 2 0 002 2h11a2 2 0 002-2v-5m-1.414-9.414a2 2 0 112.828 2.828L11.828 15H9v-2.828l8.586-8.586z"
              />
            </svg>
          </button>
          <button @click="$emit('generate-qr', file)" class="p-2 rounded-md" :class="darkMode ? 'bg-gray-700 text-indigo-400' : 'bg-gray-100 text-indigo-600'">
            <svg xmlns="http://www.w3.org/2000/svg" class="h-5 w-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path
                stroke-linecap="round"
                stroke-linejoin="round"
                stroke-width="2"
                d="M12 4v1m6 11h2m-6 0h-2v4m0-11v3m0 0h.01M12 12h4.01M16 20h4M4 12h4m12 0h.01M5 8h2a1 1 0 001-1V5a1 1 0 00-1-1H5a1 1 0 00-1 1v2a1 1 0 001 1zm12 0h2a1 1 0 001-1V5a1 1 0 00-1-1h-2a1 1 0 00-1 1v2a1 1 0 001 1zM5 20h2a1 1 0 001-1v-2a1 1 0 00-1-1H5a1 1 0 00-1 1v2a1 1 0 001 1z"
              />
            </svg>
          </button>
          <button @click="openFileLink(file)" class="p-2 rounded-md" :class="darkMode ? 'bg-gray-700 text-amber-400' : 'bg-gray-100 text-amber-600'">
            <svg xmlns="http://www.w3.org/2000/svg" class="h-5 w-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10 6H6a2 2 0 00-2 2v10a2 2 0 002 2h10a2 2 0 002-2v-4M14 4h6m0 0v6m0-6L10 14" />
            </svg>
          </button>
          <button @click="emit('copy-link', file)" class="p-2 rounded-md relative" :class="darkMode ? 'bg-gray-700 text-cyan-400' : 'bg-gray-100 text-cyan-600'">
            <svg xmlns="http://www.w3.org/2000/svg" class="h-5 w-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path
                stroke-linecap="round"
                stroke-linejoin="round"
                stroke-width="2"
                d="M8 16H6a2 2 0 01-2-2V6a2 2 0 012-2h8a2 2 0 012 2v2m-6 12h8a2 2 0 002-2v-8a2 2 0 00-2-2h-8a2 2 0 00-2 2v8a2 2 0 002 2z"
              />
            </svg>
            <!-- 移动端复制成功提示 -->
            <span v-if="props.copiedFiles[file.id]" class="absolute -top-8 right-0 px-2 py-1 text-xs text-white bg-green-500 rounded whitespace-nowrap"> 已复制 </span>
          </button>
          <!-- 移动端复制永久直链按钮 -->
          <button @click="emit('copy-permanent-link', file)" class="p-2 rounded-md relative" :class="darkMode ? 'bg-gray-700 text-purple-400' : 'bg-gray-100 text-purple-600'">
            <svg xmlns="http://www.w3.org/2000/svg" class="h-5 w-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13.828 10.172a4 4 0 00-5.656 0l-4 4a4 4 0 105.656 5.656l1.102-1.101" />
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10.172 13.828a4 4 0 0 1 5.656 0l4 4a4 4 0 01-5.656 5.656l-1.102-1.101" />
            </svg>
            <!-- 移动端永久链接复制成功提示 -->
            <span v-if="props.copiedPermanentFiles[file.id]" class="absolute -top-8 right-0 px-2 py-1 text-xs text-white bg-green-500 rounded whitespace-nowrap"> 已复制直链 </span>
          </button>
          <button @click="$emit('delete', file)" class="p-2 rounded-md" :class="darkMode ? 'bg-gray-700 text-red-400' : 'bg-gray-100 text-red-600'">
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
    </template>
  </AdminTable>
</template>

<script setup>
import { defineProps, defineEmits, computed, h } from "vue";
import AdminTable from "@/components/common/AdminTable.vue";
import { getDisplayName } from "@/utils/fileTypes.js";

// 导入统一的工具函数
import { getRemainingViews as getRemainingViewsUtil, getRemainingViewsClass as getRemainingViewsClassUtil, formatFileSize } from "@/utils/fileUtils.js";
import { getFileIcon } from "@/utils/fileTypeIcons.js";
import { formatDateTime, formatExpiry as formatExpiryUtil, parseUTCDate } from "@/utils/timeUtils.js";

const props = defineProps({
  files: {
    type: Array,
    required: true,
  },
  darkMode: {
    type: Boolean,
    default: false,
  },
  selectedFiles: {
    type: Array,
    default: () => [],
  },
  userType: {
    type: String,
    default: "admin",
  },
  copiedFiles: {
    type: Object,
    default: () => ({}),
  },
  copiedPermanentFiles: {
    type: Object,
    default: () => ({}),
  },

  loading: {
    type: Boolean,
    default: false,
  },
});

const isAdmin = computed(() => props.userType === "admin");
const emit = defineEmits(["toggle-select", "toggle-select-all", "preview", "edit", "delete", "generate-qr", "copy-link", "copy-permanent-link"]);

// 定义表格列（简化配置）
const fileColumns = computed(() => [
  // 文件名列
  {
    key: "filename",
    type: "accessor",
    header: "文件名",
    sortable: true,
    render: (_, file) => {
      return h("div", { class: "flex flex-col" }, [
        h("div", { class: "flex items-center" }, [
          h("div", {
            class: "flex-shrink-0 mr-2 w-5 h-5",
            innerHTML: getFileIconClassLocal(file),
          }),
          h(
            "span",
            {
              class: "font-medium truncate max-w-64",
              title: file.filename,
            },
            truncateFilename(file.filename)
          ),
          file.has_password &&
            h(
              "span",
              {
                class: `ml-2 h-4 w-4 ${props.darkMode ? "text-yellow-400" : "text-yellow-600"}`,
                title: "密码保护",
              },
              "🔒"
            ),
        ]),
        h(
          "span",
          {
            class: `text-xs mt-1 truncate max-w-64 ${props.darkMode ? "text-gray-400" : "text-gray-500"}`,
            title: file.slug ? `/${file.slug}` : "无短链接", // 鼠标悬停显示完整短链接
          },
          file.slug ? `/${file.slug}` : "无短链接"
        ),
        file.remark &&
          h(
            "span",
            {
              class: `text-xs mt-1 italic truncate max-w-64 ${props.darkMode ? "text-blue-400" : "text-blue-600"}`,
              title: file.remark, // 鼠标悬停显示完整备注
            },
            file.remark
          ),
      ]);
    },
  },

  // MIME类型列
  {
    key: "mimetype",
    type: "accessor",
    header: "MIME类型",
    sortable: true,
    render: (_, file) => {
      const mimeTypeText = getSimpleMimeType(file.mimetype, file.filename, file);
      return h(
        "span",
        {
          class: `px-2 py-1 text-xs rounded ${getMimeTypeClass(file)} inline-block max-w-32 truncate`,
          title: mimeTypeText, // 鼠标悬停显示完整MIME类型
        },
        mimeTypeText
      );
    },
  },

  // 文件大小列
  {
    key: "size",
    type: "accessor",
    header: "大小",
    sortable: true,
    render: (value) => formatFileSize(value),
  },

  // 剩余次数列
  {
    key: "remaining_views",
    type: "display",
    header: "剩余次数",
    sortable: false,
    render: (file) => {
      const remaining = getRemainingViews(file);

      // 确定显示文本
      let displayText;
      if (remaining === "无限制") {
        displayText = "无限制";
      } else if (remaining === "已用完") {
        displayText = "已用完";
      } else {
        displayText = `${remaining} 次`;
      }

      const children = [h("span", { class: getRemainingViewsClass(file) }, displayText)];

      // 只有当文件有访问次数限制时才显示已用次数
      if (file.views && file.max_views) {
        children.push(
          h(
            "span",
            {
              class: `text-xs mt-1 ${props.darkMode ? "text-gray-400" : "text-gray-500"}`,
            },
            `已用: ${file.views || 0}/${file.max_views}`
          )
        );
      }

      // 只有当文件有过期时间时才显示过期时间
      if (file.expires_at) {
        children.push(
          h(
            "span",
            {
              class: `text-xs mt-1 ${expiresClass(file.expires_at)}`,
            },
            formatExpiry(file.expires_at)
          )
        );
      }

      return h("div", { class: "flex flex-col" }, children);
    },
  },

  // 存储配置列
  {
    key: "storage_config",
    type: "display",
    header: "存储配置",
    sortable: false,
    render: (file) => {
      return h("div", { class: "flex flex-col" }, [
        h("span", {}, file.storage_config_name || "默认存储"),
        h(
          "span",
          {
            class: `text-xs mt-1 ${props.darkMode ? "text-gray-400" : "text-gray-500"}`,
          },
          file.storage_provider_type || "未知"
        ),
      ]);
    },
  },

  // 创建者列
  {
    key: "created_by",
    type: "display",
    header: "创建者",
    sortable: false,
    render: (file) => {
      let badgeClass, text;

      if (file.created_by && file.created_by.startsWith("apikey:")) {
        badgeClass = "px-2 py-1 text-xs rounded bg-blue-100 text-blue-800 dark:bg-blue-800 dark:text-blue-100";
        text = file.key_name ? `密钥：${file.key_name}` : `密钥：${file.created_by.substring(7, 12)}...`;
      } else if (file.created_by) {
        badgeClass = "px-2 py-1 text-xs rounded bg-green-100 text-green-800 dark:bg-green-800 dark:text-green-100";
        text = "管理员";
      } else {
        badgeClass = "px-2 py-1 text-xs rounded bg-gray-100 text-gray-800 dark:bg-gray-700 dark:text-gray-300";
        text = "未知来源";
      }

      return h("span", { class: `${badgeClass} inline-block text-center w-fit` }, text);
    },
  },

  // 创建时间列
  {
    key: "created_at",
    type: "accessor",
    header: "创建时间",
    sortable: true,
    render: (value) => formatDate(value),
  },

  // 操作列
  {
    key: "actions",
    type: "display",
    header: "操作",
    sortable: false,
    render: (file) => {
      const actions = [
        {
          title: "预览",
          event: () => emit("preview", file),
          color: "text-blue-600 hover:text-blue-900 dark:text-blue-400",
          svg: h(
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
        },
        {
          title: "编辑",
          event: () => emit("edit", file),
          color: "text-green-600 hover:text-green-900 dark:text-green-400",
          svg: h(
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
        },
        {
          title: "二维码",
          event: () => emit("generate-qr", file),
          color: "text-indigo-600 hover:text-indigo-900 dark:text-indigo-400",
          svg: h(
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
        },
        {
          title: "跳转",
          event: () => openFileLink(file),
          color: "text-amber-600 hover:text-amber-900 dark:text-amber-400",
          svg: h(
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
                d: "M10 6H6a2 2 0 00-2 2v10a2 2 0 002 2h10a2 2 0 002-2v-4M14 4h6m0 0v6m0-6L10 14",
              }),
            ]
          ),
        },
        {
          title: "复制链接",
          event: () => emit("copy-link", file),
          color: "text-cyan-600 hover:text-cyan-900 dark:text-cyan-400",
          svg: h(
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
        },
        {
          title: "复制直链",
          event: () => emit("copy-permanent-link", file),
          color: "text-purple-600 hover:text-purple-900 dark:text-purple-400",
          svg: h(
            "svg",
            {
              xmlns: "http://www.w3.org/2000/svg",
              class: "h-5 w-5",
              fill: "none",
              viewBox: "0 0 24 24",
              stroke: "currentColor",
            },
            [
              h("path", { "stroke-linecap": "round", "stroke-linejoin": "round", "stroke-width": "2", d: "M13.828 10.172a4 4 0 00-5.656 0l-4 4a4 4 0 105.656 5.656l1.102-1.101" }),
              h("path", { "stroke-linecap": "round", "stroke-linejoin": "round", "stroke-width": "2", d: "M10.172 13.828a4 4 0 015.656 0l4 4a4 4 0 01-5.656 5.656l-1.102-1.101" }),
            ]
          ),
        },
      ];

      if (isAdmin.value) {
        actions.push({
          title: "删除",
          event: () => emit("delete", file),
          color: "text-red-600 hover:text-red-900 dark:text-red-400",
          svg: h(
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
        });
      }

      return h(
        "div",
        { class: "flex space-x-2" },
        actions.map((action) =>
          h(
            "button",
            {
              onClick: action.event,
              class: `${action.color} relative`,
              title: action.title,
            },
            [
              h("span", { class: "sr-only" }, action.title),
              action.svg,
              // 添加复制反馈提示
              action.title === "复制链接" && props.copiedFiles[file.id]
                ? h(
                    "span",
                    {
                      class: "absolute -top-8 left-1/2 transform -translate-x-1/2 px-2 py-1 text-xs text-white bg-green-500 rounded whitespace-nowrap",
                    },
                    "已复制"
                  )
                : null,
              action.title === "复制直链" && props.copiedPermanentFiles[file.id]
                ? h(
                    "span",
                    {
                      class: "absolute -top-8 left-1/2 transform -translate-x-1/2 px-2 py-1 text-xs text-white bg-green-500 rounded whitespace-nowrap",
                    },
                    "已复制直链"
                  )
                : null,
            ]
          )
        )
      );
    },
  },
]);

// 列样式配置
const fileColumnClasses = {
  select: "w-10",
  mimetype: "hidden md:table-cell",
  size: "hidden sm:table-cell",
  remaining_views: "hidden xl:table-cell",
  storage_config: "hidden lg:table-cell",
  created_by: "hidden lg:table-cell",
  created_at: "hidden sm:table-cell",
};

// 处理选择变化
const handleSelectionChange = (event) => {
  if (event.type === "toggle-all") {
    emit("toggle-select-all");
  } else if (event.type === "toggle-item") {
    emit("toggle-select", event.id);
  }
};

// 处理移动端选择
const handleMobileSelect = (fileId) => {
  emit("toggle-select", fileId);
};

// 工具函数 - 从原FileTable.vue复制
const getRemainingViews = (file) => {
  return getRemainingViewsUtil(file);
};

const getRemainingViewsClass = (file) => {
  return getRemainingViewsClassUtil(file, props.darkMode);
};

const getSimpleMimeType = (mimeType, filename, file) => {
  // 优先显示 mimeType，再显示后端的 typeName
  if (mimeType && mimeType !== "application/octet-stream") {
    return mimeType;
  }

  // 如果 mimeType 无效，使用后端返回的 typeName
  if (file?.typeName && file.typeName !== "unknown") {
    return file.typeName;
  }

  // 最后回退：显示文件名（去掉扩展名）
  return filename && typeof filename === "string" ? getDisplayName(filename) : "未知文件";
};

const getMimeTypeClass = (file) => {
  const type = file.type || 0;
  switch (type) {
    case 1: // VIDEO
      return props.darkMode ? "bg-purple-900/50 text-purple-300" : "bg-purple-100 text-purple-800";
    case 2: // ARCHIVE
      return props.darkMode ? "bg-orange-900/50 text-orange-300" : "bg-orange-100 text-orange-800";
    case 3: // AUDIO
      return props.darkMode ? "bg-green-900/50 text-green-300" : "bg-green-100 text-green-800";
    case 4: // TEXT
      return props.darkMode ? "bg-yellow-900/50 text-yellow-300" : "bg-yellow-100 text-yellow-800";
    case 5: // IMAGE
      return props.darkMode ? "bg-pink-900/50 text-pink-300" : "bg-pink-100 text-pink-800";
    case 6: // OFFICE
      return props.darkMode ? "bg-red-900/50 text-red-300" : "bg-red-100 text-red-800";
    default: // UNKNOWN
      return props.darkMode ? "bg-gray-700 text-gray-300" : "bg-gray-100 text-gray-700";
  }
};

const getFileIconClassLocal = (file) => {
  const fileItem = {
    name: file.filename,
    filename: file.filename,
    isDirectory: false,
    type: file.type,
  };
  return getFileIcon(fileItem, props.darkMode);
};

const formatDate = (dateString) => {
  if (!dateString) return "未知";
  return formatDateTime(dateString);
};

const expiresClass = (expiresAt) => {
  if (!expiresAt) return props.darkMode ? "text-gray-400" : "text-gray-500";

  const expiryDate = parseUTCDate(expiresAt);
  if (!expiryDate) return props.darkMode ? "text-gray-400" : "text-gray-500";

  const now = new Date();

  // 已过期
  if (expiryDate < now) {
    return props.darkMode ? "text-red-400" : "text-red-600";
  }

  // 即将过期（24小时内）
  const nearExpiry = new Date(now.getTime() + 24 * 60 * 60 * 1000);
  if (expiryDate < nearExpiry) {
    return props.darkMode ? "text-yellow-400" : "text-yellow-600";
  }

  return props.darkMode ? "text-green-400" : "text-green-600";
};

const formatExpiry = (expiresAt) => {
  if (!expiresAt) return "";
  return formatExpiryUtil(expiresAt);
};

const truncateFilename = (filename) => {
  if (!filename || typeof filename !== "string") return "";
  if (filename.length <= 20) return filename;
  return filename.substring(0, 20) + "...";
};

const openFileLink = (file) => {
  if (!file || !file.slug) {
    alert("该文件没有有效的分享链接");
    return;
  }

  const baseUrl = window.location.origin;
  const fileUrl = `${baseUrl}/file/${file.slug}`;
  window.open(fileUrl, "_blank");
};
</script>
