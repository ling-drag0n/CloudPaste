<template>
  <div v-if="shareLink" class="mt-3 p-3 rounded-md share-link-box" :class="darkMode ? 'bg-gray-800/50' : 'bg-gray-50'">
    <div class="flex items-center">
      <span class="text-sm mr-2" :class="darkMode ? 'text-gray-400' : 'text-gray-500'">{{ $t("markdown.shareLink") }}</span>
      <a :href="shareLink" target="_blank" class="link-text text-sm flex-grow" :class="darkMode ? 'text-blue-400 hover:text-blue-300' : 'text-blue-600 hover:text-blue-500'">
        {{ shareLink }}
      </a>

      <!-- 复制图标 -->
      <button
        @click="copyShareLink"
        class="ml-2 p-1 rounded-md transition-colors"
        :class="darkMode ? 'hover:bg-gray-700 text-gray-400 hover:text-gray-200' : 'hover:bg-gray-200 text-gray-500 hover:text-gray-700'"
        :title="$t('markdown.copyLink')"
      >
        <svg xmlns="http://www.w3.org/2000/svg" class="h-5 w-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
          <path
            stroke-linecap="round"
            stroke-linejoin="round"
            stroke-width="2"
            d="M8 5H6a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2v-1M8 5a2 2 0 002 2h2a2 2 0 002-2M8 5a2 2 0 012-2h2a2 2 0 012 2m0 0h2a2 2 0 012 2v3m2 4H10m0 0l3-3m-3 3l3 3"
          />
        </svg>
      </button>

      <!-- 二维码图标 -->
      <button
        @click="showQRCode"
        class="ml-2 p-1 rounded-md transition-colors"
        :class="darkMode ? 'hover:bg-gray-700 text-gray-400 hover:text-gray-200' : 'hover:bg-gray-200 text-gray-500 hover:text-gray-700'"
        :title="$t('markdown.showQRCode')"
      >
        <svg xmlns="http://www.w3.org/2000/svg" class="h-5 w-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
          <path
            stroke-linecap="round"
            stroke-linejoin="round"
            stroke-width="2"
            d="M12 4v1m6 11h2m-6 0h-2v4m0-11v3m0 0h.01M12 12h4.01M16 20h4M4 12h4m12 0h.01M5 8h2a1 1 0 001-1V5a1 1 0 00-1-1H5a1 1 0 00-1 1v2a1 1 0 001 1zm12 0h2a1 1 0 001-1V5a1 1 0 00-1-1h-2a1 1 0 00-1 1v2a1 1 0 001 1zM5 20h2a1 1 0 001-1v-2a1 1 0 00-1-1H5a1 1 0 00-1 1v2a1 1 0 001 1z"
          />
        </svg>
      </button>

      <!-- 复制原始文本直链按钮 -->
      <button
        @click="copyRawTextLink"
        class="ml-2 p-1 rounded-md transition-colors"
        :class="darkMode ? 'hover:bg-gray-700 text-gray-400 hover:text-gray-200' : 'hover:bg-gray-200 text-gray-500 hover:text-gray-700'"
        :title="$t('markdown.copyRawLink')"
      >
        <svg xmlns="http://www.w3.org/2000/svg" class="h-5 w-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13.828 10.172a4 4 0 00-5.656 0l-4 4a4 4 0 105.656 5.656l1.102-1.101" />
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10.172 13.828a4 4 0 015.656 0l4 4a4 4 0 01-5.656 5.656l-1.102-1.101" />
        </svg>
      </button>

      <span class="ml-2 text-xs" :class="darkMode ? 'text-gray-500' : 'text-gray-400'">{{ $t("markdown.linkExpireIn", { seconds: countdown }) }}</span>
    </div>
  </div>
</template>

<script setup>
import { ref, onUnmounted } from "vue";
import { useI18n } from "vue-i18n";
import { copyToClipboard as clipboardCopy } from "@/utils/clipboard";
import { api } from "@/api";

const { t } = useI18n();

// Props
const props = defineProps({
  darkMode: {
    type: Boolean,
    default: false,
  },
  shareLink: {
    type: String,
    default: "",
  },
  sharePassword: {
    type: String,
    default: "",
  },
});

// Emits
const emit = defineEmits(["show-qr-code", "status-message"]);

// 倒计时
const countdown = ref(15);
let countdownTimer = null;

// 开始倒计时
const startCountdown = () => {
  if (countdownTimer) {
    clearInterval(countdownTimer);
  }

  countdown.value = 15;

  countdownTimer = setInterval(() => {
    countdown.value--;

    if (countdown.value <= 0) {
      clearInterval(countdownTimer);
      emit("countdown-end");
    }
  }, 1000);
};

// 复制分享链接
const copyShareLink = async () => {
  if (!props.shareLink) return;

  try {
    const success = await clipboardCopy(props.shareLink);

    if (success) {
      emit("status-message", t("markdown.linkCopied"));
    } else {
      throw new Error(t("markdown.copyFailed"));
    }
  } catch (err) {
    console.error("复制失败:", err);
    emit("status-message", t("markdown.copyFailed"));
  }
};

// 复制原始文本直链
const copyRawTextLink = async () => {
  if (!props.shareLink) return;

  const slug = props.shareLink.split("/").pop();
  const rawLink = api.paste.getRawPasteUrl(slug, props.sharePassword || null);

  try {
    const success = await clipboardCopy(rawLink);

    if (success) {
      emit("status-message", t("markdown.rawLinkCopied"));
    } else {
      throw new Error(t("markdown.copyFailed"));
    }
  } catch (err) {
    console.error("复制失败:", err);
    emit("status-message", t("markdown.copyFailed"));
  }
};

// 显示二维码
const showQRCode = () => {
  emit("show-qr-code", props.shareLink);
};

// 组件卸载时清理定时器
onUnmounted(() => {
  if (countdownTimer) {
    clearInterval(countdownTimer);
    countdownTimer = null;
  }
});

// 暴露方法
defineExpose({
  startCountdown,
});
</script>

<style scoped>
.share-link-box {
  animation: fadeIn 0.3s ease-out;
  border: 1px solid v-bind('props.darkMode ? "rgba(75, 85, 99, 0.3)" : "rgba(229, 231, 235, 0.8)"');
}

.link-text {
  text-decoration: none;
  word-break: break-all;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.link-text:hover {
  text-decoration: underline;
}

@keyframes fadeIn {
  from {
    opacity: 0;
  }
  to {
    opacity: 1;
  }
}

/* 移动端优化 */
@media (max-width: 640px) {
  .share-link-box {
    max-width: 100%;
    overflow-x: hidden;
  }
}
</style>
