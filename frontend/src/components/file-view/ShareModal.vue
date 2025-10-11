<template>
  <div v-if="visible" class="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black bg-opacity-50" @click="handleClickOutside">
    <div class="relative rounded-lg max-w-md w-full shadow-xl" :class="darkMode ? 'bg-gray-800' : 'bg-white'">
      <!-- 标题栏 -->
      <div class="flex items-center justify-between p-6 border-b" :class="darkMode ? 'border-gray-700' : 'border-gray-200'">
        <h3 class="text-lg font-medium" :class="darkMode ? 'text-white' : 'text-gray-900'">
          {{ t("fileView.actions.share") }}
        </h3>
        <button @click="closeModal" class="transition-colors" :class="darkMode ? 'text-gray-400 hover:text-gray-300' : 'text-gray-400 hover:text-gray-500'">
          <svg class="h-6 w-6" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
          </svg>
        </button>
      </div>

      <!-- 分享内容 -->
      <div class="p-6 space-y-6">
        <!-- 文件信息 -->
        <div class="flex items-center space-x-3 p-3 rounded-lg" :class="darkMode ? 'bg-gray-700/50' : 'bg-gray-50'">
          <div class="flex-shrink-0">
            <svg class="h-8 w-8 text-blue-500" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path
                stroke-linecap="round"
                stroke-linejoin="round"
                stroke-width="2"
                d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z"
              />
            </svg>
          </div>
          <div class="flex-1 min-w-0">
            <p class="text-sm font-medium truncate" :class="darkMode ? 'text-gray-100' : 'text-gray-900'">
              {{ fileInfo.filename }}
            </p>
            <p class="text-xs" :class="darkMode ? 'text-gray-400' : 'text-gray-500'">
              {{ formatFileSize(fileInfo.size) }}
            </p>
          </div>
        </div>

        <!-- 快速操作 -->
        <div class="grid grid-cols-2 gap-3">
          <button
            @click="copyLink"
            class="flex items-center justify-center px-4 py-3 rounded-lg transition-colors focus:outline-none focus:ring-2 focus:ring-offset-2"
            :class="[
              copySuccess
                ? darkMode
                  ? 'bg-green-600 text-white'
                  : 'bg-green-600 text-white'
                : darkMode
                ? 'bg-blue-600 hover:bg-blue-700 text-white focus:ring-blue-500 focus:ring-offset-gray-800'
                : 'bg-blue-600 hover:bg-blue-700 text-white focus:ring-blue-500 focus:ring-offset-white',
            ]"
          >
            <svg class="h-5 w-5 mr-2" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path
                stroke-linecap="round"
                stroke-linejoin="round"
                stroke-width="2"
                d="M8 5H6a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2v-1M8 5a2 2 0 002 2h2a2 2 0 002-2M8 5a2 2 0 012-2h2a2 2 0 012 2m0 0h2a2 2 0 012 2v3m2 4H10m0 0l3-3m-3 3l3 3"
              />
            </svg>
            <span class="text-sm font-medium">{{ copySuccess ? t("fileView.actions.copied") : t("fileView.actions.copyLink") }}</span>
          </button>

          <button
            @click="nativeShare"
            class="flex items-center justify-center px-4 py-3 rounded-lg transition-colors focus:outline-none focus:ring-2 focus:ring-offset-2"
            :class="[
              darkMode
                ? 'bg-purple-600 hover:bg-purple-700 text-white focus:ring-purple-500 focus:ring-offset-gray-800'
                : 'bg-purple-600 hover:bg-purple-700 text-white focus:ring-purple-500 focus:ring-offset-white',
            ]"
          >
            <svg class="h-5 w-5 mr-2" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path
                stroke-linecap="round"
                stroke-linejoin="round"
                stroke-width="2"
                d="M8.684 13.342C8.886 12.938 9 12.482 9 12c0-.482-.114-.938-.316-1.342m0 2.684a3 3 0 110-2.684m0 2.684l6.632 3.316m-6.632-6l6.632-3.316m0 0a3 3 0 105.367-2.684 3 3 0 00-5.367 2.684zm0 9.316a3 3 0 105.368 2.684 3 3 0 00-5.368-2.684z"
              />
            </svg>
            <span class="text-sm font-medium">{{ t("fileView.actions.nativeShare") }}</span>
          </button>
        </div>

        <!-- 社交平台分享 -->
        <div>
          <h4 class="text-sm font-medium mb-3" :class="darkMode ? 'text-gray-100' : 'text-gray-700'">
            {{ t("fileView.actions.shareToSocial") }}
          </h4>
          <div class="grid grid-cols-5 gap-3">
            <button
              v-for="platform in socialPlatforms"
              :key="platform.key"
              @click="shareToSocial(platform)"
              class="flex flex-col items-center p-3 rounded-lg transition-colors hover:bg-opacity-80 text-white"
              :class="platform.bgClass"
              :title="platform.name"
            >
              <div class="h-6 w-6 mb-1" v-html="platform.icon"></div>
              <span class="text-xs font-medium">{{ platform.name }}</span>
            </button>
          </div>
        </div>

        <!-- 二维码 -->
        <div>
          <button
            @click="toggleQRCode"
            class="flex items-center justify-between w-full p-3 rounded-lg transition-colors"
            :class="darkMode ? 'bg-gray-700 hover:bg-gray-600' : 'bg-gray-100 hover:bg-gray-200'"
          >
            <div class="flex items-center">
              <div class="h-6 w-6 mr-3 text-green-500">
                <svg fill="currentColor" viewBox="0 0 24 24">
                  <path
                    d="M3 11v8h8v-8H3zm2 6V13h4v4H5zm6-16v8h8V1h-8zm2 6V3h4v4h-4zM1 1v8h8V1H1zm2 6V3h4v4H3zm8 14h2v-2h-2v2zm0-4h2v-2h-2v2zm2 0h2v-2h-2v2zm0-4h2V9h-2v2zm2 0h2V9h-2v2zm-4 8h2v-2h-2v2zm2-4h2v-2h-2v2zm0-4h2V9h-2v2z"
                  />
                </svg>
              </div>
              <span class="text-sm font-medium" :class="darkMode ? 'text-gray-100' : 'text-gray-900'">
                {{ t("fileView.actions.qrCode") }}
              </span>
            </div>
            <svg
              class="h-5 w-5 transition-transform"
              :class="[showQRCode ? 'rotate-180' : '', darkMode ? 'text-gray-400' : 'text-gray-500']"
              fill="none"
              viewBox="0 0 24 24"
              stroke="currentColor"
            >
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7" />
            </svg>
          </button>

          <!-- 二维码区域 -->
          <div v-if="showQRCode" class="mt-3 p-4 rounded-lg text-center" :class="darkMode ? 'bg-gray-700/50' : 'bg-gray-50'">
            <div v-if="qrCodeDataURL" class="inline-block p-3 bg-white rounded-lg">
              <img :src="qrCodeDataURL" alt="QR Code" class="w-32 h-32" />
            </div>
            <div v-else-if="qrCodeError" class="w-32 h-32 mx-auto rounded-lg flex items-center justify-center" :class="darkMode ? 'bg-red-900/20' : 'bg-red-50'">
              <span class="text-sm text-red-500">{{ t("fileView.actions.qrCodeError") }}</span>
            </div>
            <div v-else class="w-32 h-32 mx-auto rounded-lg flex items-center justify-center" :class="darkMode ? 'bg-gray-600' : 'bg-gray-200'">
              <span class="text-sm" :class="darkMode ? 'text-gray-400' : 'text-gray-500'">{{ t("fileView.actions.generating") }}</span>
            </div>
            <p v-if="!qrCodeError" class="text-xs mt-2" :class="darkMode ? 'text-gray-400' : 'text-gray-500'">
              {{ t("fileView.actions.scanToShare") }}
            </p>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script>
import { ref, watch, nextTick, onUnmounted } from "vue";
import { useI18n } from "vue-i18n";
import { copyToClipboard } from "@/utils/clipboard";
import { formatFileSize } from "@/utils/fileUtils";
import QRCode from "qrcode";

// 社交平台配置常量
const SOCIAL_PLATFORMS = [
  {
    key: "weibo",
    name: "Weibo",
    bgClass: "bg-red-500 hover:bg-red-600",
    icon: '<svg fill="currentColor" viewBox="0 0 24 24"><path d="M15.518 3.06c8.834-.854 7.395 7.732 7.394 7.731-.625 1.439-1.673.309-1.673.309.596-7.519-5.692-6.329-5.692-6.329-.898-1.067-.029-1.711-.029-1.711zm4.131 6.985c-.661 1.01-1.377.126-1.376.126.205-3.179-2.396-2.598-2.396-2.598-.719-.765-.091-1.346-.091-1.346 4.882-.551 3.863 3.818 3.863 3.818zM5.317 7.519s4.615-3.86 6.443-1.328c0 0 .662 1.08-.111 2.797.003-.003 3.723-1.96 5.408.159 0 0 .848 1.095-.191 2.649 0 0 2.918-.099 2.918 2.715 0 2.811-4.104 6.44-9.315 6.44-5.214 0-8.026-2.092-8.596-3.102 0 0-3.475-4.495 3.444-10.33zm10.448 7.792s.232-4.411-5.71-4.207c-6.652.231-6.579 4.654-6.579 4.654.021.39.097 3.713 5.842 3.713 5.98 0 6.447-4.16 6.447-4.16zm-9.882.86s-.059-3.632 3.804-3.561c3.412.06 3.206 3.165 3.206 3.165s-.026 2.979-3.684 2.979c-3.288 0-3.326-2.583-3.326-2.583zm2.528 1.037c.672 0 1.212-.447 1.212-.998 0-.551-.543-.998-1.212-.998-.672 0-1.215.447-1.215.998 0 .551.546.998 1.215.998z"/></svg>',
    url: "https://service.weibo.com/share/share.php?url={url}&title={title}",
  },
  {
    key: "qq",
    name: "QQ",
    bgClass: "bg-blue-500 hover:bg-blue-600",
    icon: '<svg fill="currentColor" viewBox="0 0 24 24"><path d="M12.003 2c-2.265 0-6.29 1.364-6.29 7.325v1.195S3.55 14.96 3.55 17.474c0 .665.17 1.025.281 1.025.114 0 .902-.484 1.748-2.072 0 0-.18 2.197 1.904 3.967 0 0-1.77.495-1.77 1.182 0 .686 4.078.43 6.29 0 2.239.425 6.287.687 6.287 0 0-.688-1.768-1.182-1.768-1.182 2.085-1.77 1.905-3.967 1.905-3.967.845 1.588 1.634 2.072 1.746 2.072.111 0 .283-.36.283-1.025 0-2.514-2.166-6.954-2.166-6.954V9.325C18.29 3.364 14.268 2 12.003 2z"/></svg>',
    url: "https://connect.qq.com/widget/shareqq/index.html?url={url}&title={title}&desc={text}&summary={title}",
  },
  {
    key: "twitter",
    name: "X",
    bgClass: "bg-black hover:bg-gray-800",
    icon: '<svg fill="currentColor" viewBox="0 0 24 24"><path d="M18.244 2.25h3.308l-7.227 8.26 8.502 11.24H16.17l-5.214-6.817L4.99 21.75H1.68l7.73-8.835L1.254 2.25H8.08l4.713 6.231zm-1.161 17.52h1.833L7.084 4.126H5.117z"/></svg>',
    url: "https://twitter.com/intent/tweet?url={url}&text={text}",
  },
  {
    key: "telegram",
    name: "Telegram",
    bgClass: "bg-blue-400 hover:bg-blue-500",
    icon: '<svg fill="currentColor" viewBox="0 0 24 24"><path d="M11.944 0A12 12 0 0 0 0 12a12 12 0 0 0 12 12 12 12 0 0 0 12-12A12 12 0 0 0 12 0a12 12 0 0 0-.056 0zm4.962 7.224c.1-.002.321.023.465.14a.506.506 0 0 1 .171.325c.016.093.036.306.02.472-.18 1.898-.962 6.502-1.36 8.627-.168.9-.499 1.201-.82 1.23-.696.065-1.225-.46-1.9-.902-1.056-.693-1.653-1.124-2.678-1.8-1.185-.78-.417-1.21.258-1.91.177-.184 3.247-2.977 3.307-3.23.007-.032.014-.15-.056-.212s-.174-.041-.249-.024c-.106.024-1.793 1.14-5.061 3.345-.48.33-.913.49-1.302.48-.428-.008-1.252-.241-1.865-.44-.752-.245-1.349-.374-1.297-.789.027-.216.325-.437.893-.663 3.498-1.524 5.83-2.529 6.998-3.014 3.332-1.386 4.025-1.627 4.476-1.635z"/></svg>',
    url: "https://t.me/share/url?url={url}&text={text}",
  },
  {
    key: "facebook",
    name: "Facebook",
    bgClass: "bg-blue-600 hover:bg-blue-700",
    icon: '<svg fill="currentColor" viewBox="0 0 24 24"><path d="M24 12.073c0-6.627-5.373-12-12-12s-12 5.373-12 12c0 5.99 4.388 10.954 10.125 11.854v-8.385H7.078v-3.47h3.047V9.43c0-3.007 1.792-4.669 4.533-4.669 1.312 0 2.686.235 2.686.235v2.953H15.83c-1.491 0-1.956.925-1.956 1.874v2.25h3.328l-.532 3.47h-2.796v8.385C19.612 23.027 24 18.062 24 12.073z"/></svg>',
    url: "https://www.facebook.com/sharer/sharer.php?u={url}",
  },
];

export default {
  name: "ShareModal",
  props: {
    visible: {
      type: Boolean,
      default: false,
    },
    fileInfo: {
      type: Object,
      required: true,
    },
    darkMode: {
      type: Boolean,
      default: false,
    },
  },
  emits: ["close"],
  setup(props, { emit }) {
    const { t } = useI18n();

    const showQRCode = ref(false);
    const qrCodeDataURL = ref("");
    const copySuccess = ref(false);
    const qrCodeError = ref(false);

    // 使用外部社交平台配置常量
    const socialPlatforms = ref(SOCIAL_PLATFORMS);

    // 获取分享文本
    const getShareText = () => {
      return t("fileView.actions.shareFileText", { filename: props.fileInfo.filename });
    };

    // 复制链接
    const copyLink = async () => {
      try {
        const success = await copyToClipboard(window.location.href);
        if (success) {
          copySuccess.value = true;
          // 2秒后重置状态
          setTimeout(() => {
            copySuccess.value = false;
          }, 2000);
          console.log("Link copied successfully");
        }
      } catch (error) {
        console.error("Failed to copy link:", error);
      }
    };

    // 原生分享
    const nativeShare = async () => {
      if (navigator.share) {
        try {
          await navigator.share({
            title: props.fileInfo.filename,
            text: getShareText(),
            url: window.location.href,
          });
        } catch (error) {
          if (error.name !== "AbortError") {
            console.error("Native share failed:", error);
            // 回退到复制链接
            await copyLink();
          }
        }
      } else {
        // 不支持原生分享，回退到复制链接
        await copyLink();
      }
    };

    // 分享到社交平台
    const shareToSocial = (platform) => {
      // 构建分享URL，替换占位符
      const shareUrl = platform.url
        .replace("{url}", encodeURIComponent(window.location.href))
        .replace("{title}", encodeURIComponent(props.fileInfo.filename))
        .replace("{text}", encodeURIComponent(getShareText()));

      window.open(shareUrl, "_blank", "width=600,height=400");
    };

    // 切换二维码显示
    const toggleQRCode = async () => {
      showQRCode.value = !showQRCode.value;

      if (showQRCode.value && !qrCodeDataURL.value && !qrCodeError.value) {
        try {
          qrCodeDataURL.value = await QRCode.toDataURL(window.location.href, {
            width: 128,
            margin: 1,
            color: {
              dark: "#000000",
              light: "#FFFFFF",
            },
          });
          qrCodeError.value = false;
        } catch (error) {
          console.error("Failed to generate QR code:", error);
          qrCodeError.value = true;
        }
      }
    };

    // 关闭模态框
    const closeModal = () => {
      emit("close");
    };

    // 监听ESC键关闭
    const handleKeydown = (event) => {
      if (event.key === "Escape" && props.visible) {
        closeModal();
      }
    };

    // 点击外部关闭 - 更健壮的检测方式
    const handleClickOutside = (event) => {
      // 检查点击的是否是背景遮罩层（最外层div）
      if (event.target === event.currentTarget) {
        closeModal();
      }
    };

    // 监听键盘和点击事件
    watch(
      () => props.visible,
      (newVal) => {
        if (newVal) {
          // 立即添加键盘事件监听器
          document.addEventListener("keydown", handleKeydown);
          // 使用nextTick确保DOM更新后再添加点击事件监听器
          nextTick(() => {
            document.addEventListener("click", handleClickOutside);
          });
        } else {
          // 移除事件监听器并重置状态
          document.removeEventListener("keydown", handleKeydown);
          document.removeEventListener("click", handleClickOutside);
          // 重置二维码相关状态
          showQRCode.value = false;
          qrCodeDataURL.value = "";
          qrCodeError.value = false;
        }
      }
    );

    // 组件卸载时清理事件监听器
    onUnmounted(() => {
      document.removeEventListener("keydown", handleKeydown);
      document.removeEventListener("click", handleClickOutside);
    });

    return {
      t,
      showQRCode,
      qrCodeDataURL,
      qrCodeError,
      copySuccess,
      socialPlatforms,
      formatFileSize,
      copyLink,
      nativeShare,
      shareToSocial,
      toggleQRCode,
      closeModal,
    };
  },
};
</script>
