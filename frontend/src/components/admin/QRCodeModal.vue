<template>
  <div class="fixed inset-0 z-[60] overflow-auto bg-black bg-opacity-50 flex items-center justify-center p-2 sm:p-4 pt-20 sm:pt-4">
    <div class="relative bg-white dark:bg-gray-800 rounded-lg max-w-xs sm:max-w-md w-full mx-auto shadow-xl overflow-hidden max-h-[95vh] sm:max-h-[85vh]">
      <!-- 标题栏 -->
      <div class="px-4 sm:px-6 py-3 sm:py-4 border-b flex justify-between items-center" :class="darkMode ? 'border-gray-700' : 'border-gray-200'">
        <h3 class="text-base sm:text-lg font-medium" :class="darkMode ? 'text-white' : 'text-gray-900'">文件分享二维码</h3>
        <button @click="$emit('close')" class="text-gray-400 hover:text-gray-500">
          <svg class="h-6 w-6" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
          </svg>
        </button>
      </div>

      <!-- 二维码内容 -->
      <div class="px-4 sm:px-6 py-3 sm:py-4 flex flex-col items-center overflow-y-auto" style="max-height: calc(95vh - 140px)">
        <!-- 二维码 -->
        <div class="mb-6 p-4 bg-white rounded-lg">
          <img :src="qrCodeUrl" :alt="`QR Code for ${fileSlug}`" class="max-w-full h-auto" />
        </div>

        <!-- 下载按钮 -->
        <div class="w-full flex space-x-3 mb-2">
          <button @click="downloadQRCode" class="flex-1 px-4 py-2 bg-blue-600 hover:bg-blue-700 text-white rounded-md text-sm font-medium transition-colors">
            <span class="flex items-center justify-center">
              <svg xmlns="http://www.w3.org/2000/svg" class="h-4 w-4 mr-1.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-4l-4 4m0 0l-4-4m4 4V4" />
              </svg>
              下载二维码
            </span>
          </button>
        </div>

        <button
          @click="$emit('close')"
          class="w-full px-4 py-2 rounded-md text-sm font-medium transition-colors mt-2"
          :class="darkMode ? 'bg-gray-700 hover:bg-gray-600 text-white' : 'bg-gray-200 hover:bg-gray-300 text-gray-700'"
        >
          关闭
        </button>
      </div>
    </div>
  </div>
</template>

<script setup>
import { defineProps, defineEmits, computed } from "vue";

const props = defineProps({
  qrCodeUrl: {
    type: String,
    required: true,
  },
  fileSlug: {
    type: String,
    required: true,
  },
  darkMode: {
    type: Boolean,
    default: false,
  },
});

defineEmits(["close"]);

// 计算完整的文件URL（保留这个计算属性，虽然不再显示在界面上，但可能在其他地方会用到）
const fileUrl = computed(() => {
  const baseUrl = window.location.origin;
  return `${baseUrl}/file/${props.fileSlug}`;
});

/**
 * 下载二维码图片
 */
const downloadQRCode = () => {
  // 创建一个临时的a标签用于下载
  const link = document.createElement("a");
  link.href = props.qrCodeUrl;
  link.download = `cloudpaste-file-${props.fileSlug}.png`;
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
};
</script>
