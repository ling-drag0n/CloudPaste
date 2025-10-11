<template>
  <div class="video-preview-container">
    <!-- 视频预览 -->
    <div class="video-preview">
      <VideoPlayer
        ref="videoPlayerRef"
        v-if="previewUrl && videoData"
        :video="videoData"
        :dark-mode="darkMode"
        :autoplay="false"
        :volume="0.7"
        :muted="false"
        :loop="false"
        :custom-controls="[]"
        @play="handlePlay"
        @pause="handlePause"
        @error="handleError"
        @canplay="handleCanPlay"
        @ended="handleVideoEnded"
        @timeupdate="handleTimeUpdate"
        @fullscreen="handleFullscreen"
        @fullscreenExit="handleFullscreenExit"
        @ready="handlePlayerReady"
      />
      <div v-else class="loading-indicator text-center py-8">
        <div class="animate-spin rounded-full h-10 w-10 border-b-2 mx-auto" :class="darkMode ? 'border-primary-500' : 'border-primary-600'"></div>
        <p class="mt-2 text-sm" :class="darkMode ? 'text-gray-400' : 'text-gray-600'">正在加载视频...</p>
      </div>
    </div>
  </div>
</template>

<script setup>
import { computed, ref, onMounted, onBeforeUnmount, watch } from "vue";
import VideoPlayer from "../../common/VideoPlayer.vue";

// Props 定义
const props = defineProps({
  previewUrl: {
    type: String,
    required: true,
  },
  mimetype: {
    type: String,
    required: true,
  },
  filename: {
    type: String,
    default: "",
  },
  darkMode: {
    type: Boolean,
    default: false,
  },
});

// Emits 定义
const emit = defineEmits(["load", "error", "play", "pause", "fullscreen", "fullscreenExit"]);

// 响应式数据
const videoPlayerRef = ref(null);
const isPlaying = ref(false);
const originalTitle = ref("");
const currentTime = ref(0);
const duration = ref(0);

// 当前视频数据（响应式）
const currentVideoData = ref(null);

// 为了兼容性，保留 videoData 计算属性
const videoData = computed(() => currentVideoData.value);

// 更新页面标题
const updatePageTitle = (playing = false, fileName = null) => {
  // 使用传入的文件名，如果没有则使用默认值
  const title = fileName || "视频预览";

  document.title = playing ? `🎬 ${title}` : `${title}`;
};

// 恢复原始页面标题
const restoreOriginalTitle = () => {
  if (originalTitle.value) {
    document.title = originalTitle.value;
  }
};

// 生成默认海报
const generateDefaultPoster = (fileName) => {
  // 创建一个简单的默认海报
  const canvas = document.createElement("canvas");
  canvas.width = 320;
  canvas.height = 180;
  const ctx = canvas.getContext("2d");

  // 设置背景
  ctx.fillStyle = "#1f2937";
  ctx.fillRect(0, 0, canvas.width, canvas.height);

  // 设置文字
  ctx.fillStyle = "#ffffff";
  ctx.font = "16px Arial";
  ctx.textAlign = "center";
  ctx.fillText("🎬", canvas.width / 2, canvas.height / 2 - 10);
  ctx.font = "12px Arial";
  ctx.fillText(fileName || "视频文件", canvas.width / 2, canvas.height / 2 + 15);

  return canvas.toDataURL();
};

// 事件处理函数
const handlePlay = (data) => {
  isPlaying.value = true;
  const videoName = data?.video?.name || props.filename;
  updatePageTitle(true, videoName);
  emit("play", data);
};

const handlePause = (data) => {
  isPlaying.value = false;
  const videoName = data?.video?.name || props.filename;
  updatePageTitle(false, videoName);
  emit("pause", data);
};

const handleError = (error) => {
  // 忽略Service Worker相关的误报错误
  if (error?.target?.src?.includes(window.location.origin) && currentVideoData.value?.url) {
    console.log("🎬 忽略Service Worker相关的误报错误，视频实际可以正常播放");
    return;
  }

  isPlaying.value = false;
  console.error("视频播放错误:", error);
  emit("error", error);
};

const handleCanPlay = () => {
  emit("load");
};

const handleTimeUpdate = (data) => {
  currentTime.value = data.currentTime;
  duration.value = data.duration;
};

// 处理视频播放结束
const handleVideoEnded = () => {
  console.log("视频播放结束");
  isPlaying.value = false;
  updatePageTitle(false, props.filename);
};

// 处理全屏事件
const handleFullscreen = () => {
  console.log("进入全屏模式");
  emit("fullscreen");
};

const handleFullscreenExit = () => {
  console.log("退出全屏模式");
  emit("fullscreenExit");
};

// 处理播放器准备就绪
const handlePlayerReady = (player) => {
  console.log("🎬 视频播放器准备就绪:", player);
};

// 初始化当前视频数据
const initializeCurrentVideo = async () => {
  if (!props.previewUrl) {
    console.log("❌ 无法初始化当前视频：previewUrl为空");
    return;
  }

  console.log("🎬 开始初始化当前视频:", props.filename);

  // 构建视频数据对象
  currentVideoData.value = {
    name: props.filename || "视频文件",
    title: props.filename || "视频预览",
    url: props.previewUrl,
    poster: generateDefaultPoster(props.filename),
    contentType: props.mimetype,
    mimetype: props.mimetype,
  };

  console.log("🎬 视频数据初始化完成:", currentVideoData.value);
};

// 监听 previewUrl 变化，当准备好时初始化当前视频
watch(
  () => props.previewUrl,
  async (newPreviewUrl) => {
    // 当previewUrl存在时，初始化视频数据
    if (newPreviewUrl) {
      console.log("🎬 检测到 previewUrl 变化，开始重新初始化当前视频:", newPreviewUrl);
      await initializeCurrentVideo();
    }
  },
  { immediate: true } // 立即执行，确保首次加载时也会触发
);

// 快捷键处理
const handleKeydown = (event) => {
  // 如果用户正在输入框中输入，不处理快捷键
  if (event.target.tagName === "INPUT" || event.target.tagName === "TEXTAREA") {
    return;
  }

  const player = videoPlayerRef.value?.getInstance();
  if (!player) return;

  switch (event.code) {
    case "Space":
      event.preventDefault();
      player.toggle(); // 播放/暂停
      break;
    case "ArrowLeft":
      event.preventDefault();
      player.seek = Math.max(0, player.currentTime - 10); // 后退10秒
      break;
    case "ArrowRight":
      event.preventDefault();
      player.seek = Math.min(player.duration, player.currentTime + 10); // 前进10秒
      break;
    case "ArrowUp":
      event.preventDefault();
      player.volume = Math.min(1, player.volume + 0.1); // 音量+10%
      break;
    case "ArrowDown":
      event.preventDefault();
      player.volume = Math.max(0, player.volume - 0.1); // 音量-10%
      break;
    case "KeyF":
      event.preventDefault();
      player.fullscreen = !player.fullscreen; // 切换全屏
      break;
  }
};

// 生命周期钩子
onMounted(() => {
  // 保存原始页面标题
  originalTitle.value = document.title;

  // 添加键盘事件监听
  document.addEventListener("keydown", handleKeydown);

  // 不需要在这里初始化视频，watch 会处理
});

onBeforeUnmount(() => {
  // 恢复原始页面标题
  restoreOriginalTitle();

  // 移除键盘事件监听
  document.removeEventListener("keydown", handleKeydown);

  console.log("🧹 视频预览组件已卸载");
});
</script>

<style scoped>
.video-preview-container {
  width: 100%;
}

.video-preview {
  min-height: 200px;
  display: flex;
  flex-direction: column;
  justify-content: center;
  position: relative;
  padding: 1rem;
}

.loading-indicator {
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
}

/* 移动端优化 */
@media (max-width: 768px) {
  .video-preview {
    padding: 0.75rem !important;
    min-height: 150px;
  }
}
</style>
