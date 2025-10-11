<template>
  <div class="generic-preview text-center py-6 w-full self-center flex flex-col items-center justify-center" style="min-height: 200px">
    <svg xmlns="http://www.w3.org/2000/svg" class="h-16 w-16 mx-auto mb-3" :class="iconClass" fill="none" viewBox="0 0 24 24" stroke="currentColor">
      <path
        stroke-linecap="round"
        stroke-linejoin="round"
        stroke-width="1.5"
        d="M7 21h10a2 2 0 002-2V9.414a1 1 0 00-.293-.707l-5.414-5.414A1 1 0 0012.586 3H7a2 2 0 00-2 2v14a2 2 0 002 2z"
      />
    </svg>
    <p class="text-gray-600 dark:text-gray-300 font-medium">{{ fileTypeDescription }}</p>
    <p class="text-gray-500 dark:text-gray-400 text-sm mt-2">{{ actionSuggestion }}</p>

    <!-- 文件类型详细信息 -->
    <div v-if="showDetails" class="mt-4 p-3 bg-gray-100 dark:bg-gray-700 rounded-lg text-left max-w-md">
      <h4 class="text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">{{ t("fileView.preview.generic.fileInfo") }}</h4>
      <div class="space-y-1 text-xs text-gray-600 dark:text-gray-400">
        <div v-if="filename">
          <span class="font-medium">{{ t("fileView.preview.generic.filename") }}:</span> {{ filename }}
        </div>
        <div v-if="mimetype">
          <span class="font-medium">{{ t("fileView.preview.generic.mimeType") }}:</span> {{ mimetype }}
        </div>
        <div v-if="fileExtension">
          <span class="font-medium">{{ t("fileView.preview.generic.fileExtension") }}:</span> {{ fileExtension }}
        </div>
        <div v-if="suggestedApps.length > 0">
          <span class="font-medium">{{ t("fileView.preview.generic.suggestedApps") }}:</span> {{ suggestedApps.join(", ") }}
        </div>
      </div>
    </div>

    <!-- 显示/隐藏详细信息按钮 -->
    <button @click="showDetails = !showDetails" class="mt-3 text-xs text-blue-600 dark:text-blue-400 hover:underline">
      {{ showDetails ? t("fileView.preview.generic.hideDetails") : t("fileView.preview.generic.showDetails") }}
    </button>
  </div>
</template>

<script setup>
import { ref, computed } from "vue";
import { useI18n } from "vue-i18n";

const { t } = useI18n();

const props = defineProps({
  iconClass: {
    type: String,
    default: "text-gray-400",
  },
  filename: {
    type: String,
    default: "",
  },
  mimetype: {
    type: String,
    default: "",
  },
});

const showDetails = ref(false);

// 获取文件扩展名
const fileExtension = computed(() => {
  if (!props.filename) return "";
  const parts = props.filename.split(".");
  return parts.length > 1 ? `.${parts.pop().toLowerCase()}` : "";
});

// 根据文件类型生成描述
const fileTypeDescription = computed(() => {
  const ext = fileExtension.value.toLowerCase();
  const mime = props.mimetype.toLowerCase();

  // 常见文件类型描述
  const typeDescriptions = {
    // 压缩文件
    ".zip": "压缩文件",
    ".rar": "RAR压缩文件",
    ".7z": "7-Zip压缩文件",
    ".tar": "TAR归档文件",
    ".gz": "Gzip压缩文件",

    // 可执行文件
    ".exe": "Windows可执行文件",
    ".msi": "Windows安装包",
    ".dmg": "macOS磁盘映像",
    ".deb": "Debian安装包",
    ".rpm": "RPM安装包",

    // 数据库文件
    ".db": "数据库文件",
    ".sqlite": "SQLite数据库",
    ".sql": "SQL脚本文件",

    // 字体文件
    ".ttf": "TrueType字体",
    ".otf": "OpenType字体",
    ".woff": "Web字体",

    // 其他
    ".iso": "光盘映像文件",
    ".bin": "二进制文件",
    ".log": "日志文件",
  };

  if (typeDescriptions[ext]) {
    return typeDescriptions[ext];
  }

  // 根据MIME类型判断
  if (mime.startsWith("application/")) {
    return t("fileView.preview.generic.applicationFile");
  } else if (mime.startsWith("font/")) {
    return t("fileView.preview.generic.fontFile");
  } else if (mime.startsWith("model/")) {
    return t("fileView.preview.generic.modelFile");
  }

  return t("fileView.preview.generic.unsupportedType");
});

// 操作建议
const actionSuggestion = computed(() => {
  const ext = fileExtension.value.toLowerCase();

  if ([".zip", ".rar", ".7z", ".tar", ".gz"].includes(ext)) {
    return t("fileView.preview.generic.downloadAndExtract");
  } else if ([".exe", ".msi", ".dmg", ".deb", ".rpm"].includes(ext)) {
    return t("fileView.preview.generic.downloadAndInstall");
  } else if ([".db", ".sqlite"].includes(ext)) {
    return t("fileView.preview.generic.downloadAndOpenWithDb");
  } else if ([".ttf", ".otf", ".woff"].includes(ext)) {
    return t("fileView.preview.generic.downloadAndInstallFont");
  } else if (ext === ".iso") {
    return t("fileView.preview.generic.downloadAndMount");
  }

  return t("fileView.preview.generic.downloadAndOpenWith");
});

// 建议的应用程序
const suggestedApps = computed(() => {
  const ext = fileExtension.value.toLowerCase();

  const appSuggestions = {
    ".zip": ["WinRAR", "7-Zip", "WinZip"],
    ".rar": ["WinRAR", "7-Zip"],
    ".7z": ["7-Zip", "WinRAR"],
    ".exe": ["Windows系统"],
    ".msi": ["Windows安装程序"],
    ".dmg": ["macOS系统"],
    ".db": ["DB Browser for SQLite", "DBeaver"],
    ".sqlite": ["DB Browser for SQLite", "SQLite Expert"],
    ".sql": ["MySQL Workbench", "phpMyAdmin", "DBeaver"],
    ".ttf": ["字体管理器", "系统字体安装"],
    ".otf": ["字体管理器", "系统字体安装"],
    ".iso": ["虚拟光驱软件", "刻录软件"],
    ".log": ["文本编辑器", "Notepad++", "VS Code"],
  };

  return appSuggestions[ext] || [];
});
</script>
