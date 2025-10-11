<template>
  <div class="video-preview-container">
    <!-- 视频预览 -->
    <div class="video-preview p-4">
      <VideoPlayer
        ref="videoPlayerRef"
        v-if="videoUrl && videoData"
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
        <p class="mt-2 text-sm" :class="darkMode ? 'text-gray-400' : 'text-gray-600'">{{ $t("mount.videoPreview.loadingVideo") }}</p>
      </div>
    </div>
  </div>
</template>

<script setup>
import { computed, ref, onMounted, onBeforeUnmount, watch } from "vue";
import { useI18n } from "vue-i18n";
import VideoPlayer from "../../common/VideoPlayer.vue";
import api from "@/api/index.js";
import { FileType } from "@/utils/fileTypes.js";

const { t } = useI18n();

// Props 定义
const props = defineProps({
  // 文件信息
  file: {
    type: Object,
    required: true,
  },
  // 视频URL
  videoUrl: {
    type: String,
    default: null,
  },
  // 是否为深色模式
  darkMode: {
    type: Boolean,
    default: false,
  },
  // 是否为管理员
  isAdmin: {
    type: Boolean,
    default: false,
  },
  // 当前目录路径
  currentPath: {
    type: String,
    default: "",
  },
  // 目录项目列表
  directoryItems: {
    type: Array,
    default: () => [],
  },
});

// Emits 定义
const emit = defineEmits(["play", "pause", "error", "canplay", "loaded", "fullscreen", "fullscreenExit"]);

// 响应式数据
const videoPlayerRef = ref(null);
const isPlaying = ref(false);
const originalTitle = ref("");
const currentTime = ref(0);
const duration = ref(0);

// 当前视频数据（响应式）
const currentVideoData = ref(null);

// HLS相关状态
const isHLSVideo = ref(false);
const hlsSegmentUrls = ref(new Map()); // 存储 .ts 文件名到预签名URL的映射
const isLoadingHLSSegments = ref(false);

// 为了兼容性，保留 videoData 计算属性
const videoData = computed(() => currentVideoData.value);

// 更新页面标题
const updatePageTitle = (playing = false, fileName = null) => {
  // 使用传入的文件名，如果没有则使用默认值
  const title = fileName || t("mount.videoPreview.videoPlayer");

  document.title = playing ? `🎬 ${title}` : `${title}`;
};

// 恢复原始页面标题
const restoreOriginalTitle = () => {
  if (originalTitle.value) {
    document.title = originalTitle.value;
  }
};

// 检测是否为HLS视频文件
const checkIfHLSVideo = (file) => {
  if (!file || !file.name) return false;
  return file.name.toLowerCase().endsWith(".m3u8");
};

// 获取同目录下的HLS分片文件
const loadHLSSegments = async () => {
  if (!props.currentPath || isLoadingHLSSegments.value) {
    return;
  }

  try {
    isLoadingHLSSegments.value = true;
    let directoryItems = [];

    // 优先使用传入的目录数据，避免重复API调用
    if (props.directoryItems && props.directoryItems.length > 0) {
      console.log("✅ 使用已有的目录数据，避免重复API调用");
      directoryItems = props.directoryItems;
    } else {
      const response = await api.fs.getDirectoryList(props.currentPath);

      if (response.success && response.data?.items) {
        directoryItems = response.data.items;
      } else {
        console.error("❌ 获取目录列表失败");
        return;
      }
    }

    // 过滤出 HLS .ts 分片文件 - 使用FileType.VIDEO进行精确过滤
    const tsFileList = directoryItems.filter((item) => {
      if (item.isDirectory) return false;

      // 首先检查是否为视频文件类型
      if (item.type !== FileType.VIDEO) return false;

      // 然后检查是否为 HLS .ts 分片文件
      const fileName = item.name?.toLowerCase() || "";
      return fileName.endsWith(".ts") || fileName.endsWith(".m2ts");
    });
    
    console.log("🎬 过滤后的TS分片文件:", tsFileList);

    if (tsFileList.length > 0) {
      console.log(`🎬 找到 ${tsFileList.length} 个TS分片文件，开始生成预签名URL...`);
      await generateTsPresignedUrls(tsFileList);
    }
  } catch (error) {
    console.error("❌ 加载HLS分片文件失败:", error);
  } finally {
    isLoadingHLSSegments.value = false;
  }
};

// 为TS分片文件生成预签名URL
const generateTsPresignedUrls = async (tsFileList) => {
  const urlMap = new Map();

  for (const tsFile of tsFileList) {
    console.log(`🎬 处理TS分片文件: ${tsFile.name}`);
    try {
      const presignedUrl = await generateS3PresignedUrl(tsFile);
      if (presignedUrl) {
        urlMap.set(tsFile.name, presignedUrl);
      }
    } catch (error) {
      console.error(`❌ 生成TS分片文件预签名URL失败: ${tsFile.name}`, error);
    }
  }

  hlsSegmentUrls.value = urlMap;
};

// 生成S3预签名URL
const generateS3PresignedUrl = async (file) => {
  try {
    const getFileLink = api.fs.getFileLink;
    // 使用S3配置的默认签名时间
    const response = await getFileLink(file.path, null, false);

    if (response?.success && response.data?.presignedUrl) {
      return response.data.presignedUrl;
    }
  } catch (error) {
    console.error(`获取文件预签名URL失败: ${file.name}`, error);
  }
  return null;
};

// 事件处理函数
const handlePlay = (data) => {
  isPlaying.value = true;
  const videoName = data?.video?.name;
  updatePageTitle(true, videoName);
  emit("play", data);
};

const handlePause = (data) => {
  isPlaying.value = false;
  const videoName = data?.video?.name;
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
  emit("canplay");
  emit("loaded");
};

const handleTimeUpdate = (data) => {
  currentTime.value = data.currentTime;
  duration.value = data.duration;
};

// 处理视频播放结束
const handleVideoEnded = () => {
  isPlaying.value = false;
  updatePageTitle(false, props.file?.name);
};

// 处理全屏事件
const handleFullscreen = () => {
  emit("fullscreen");
};

const handleFullscreenExit = () => {
  emit("fullscreenExit");
};

// 处理播放器准备就绪
const handlePlayerReady = (player) => {
  console.log("🎬 视频播放器准备就绪:", player);
};

// 初始化当前视频数据
const initializeCurrentVideo = async () => {
  if (!props.file) {
    return;
  }

  // 检测是否为HLS视频
  isHLSVideo.value = checkIfHLSVideo(props.file);

  if (isHLSVideo.value) {
    await loadHLSSegments();
  }

  // 使用S3预签名URL或传入的视频URL
  if (props.videoUrl) {
    currentVideoData.value = {
      name: props.file.name || "unknown",
      title: props.file.name || "unknown",
      url: props.videoUrl,
      poster: generateDefaultPoster(props.file.name),
      contentType: props.file.contentType,
      originalFile: props.file,
      isHLS: isHLSVideo.value,
      hlsSegmentUrls: hlsSegmentUrls.value,
    };
    return;
  }

  // 降级方案：理论上不应该到达这里，因为videoUrl应该总是存在
  console.warn("⚠️ videoUrl为空，这表明上游有问题");
  currentVideoData.value = {
    name: props.file.name || "unknown",
    title: props.file.name || "unknown",
    url: null,
    poster: generateDefaultPoster(props.file.name),
    contentType: props.file.contentType,
    originalFile: props.file,
    isHLS: isHLSVideo.value,
    hlsSegmentUrls: hlsSegmentUrls.value,
  };
};

// 生成默认封面
const generateDefaultPoster = (name) => {
  const firstChar = (name || "V")[0].toUpperCase();
  const canvas = document.createElement("canvas");
  canvas.width = 320;
  canvas.height = 180;
  const ctx = canvas.getContext("2d");

  // 背景色
  ctx.fillStyle = props.darkMode ? "#374151" : "#6b7280";
  ctx.fillRect(0, 0, 320, 180);

  // 播放按钮背景
  ctx.fillStyle = props.darkMode ? "#60a5fa" : "#3b82f6";
  ctx.beginPath();
  ctx.arc(160, 90, 30, 0, 2 * Math.PI);
  ctx.fill();

  // 播放按钮三角形
  ctx.fillStyle = "#ffffff";
  ctx.beginPath();
  ctx.moveTo(150, 75);
  ctx.lineTo(150, 105);
  ctx.lineTo(175, 90);
  ctx.closePath();
  ctx.fill();

  // 文件名
  ctx.fillStyle = "#ffffff";
  ctx.font = "bold 16px Arial";
  ctx.textAlign = "center";
  ctx.textBaseline = "middle";
  ctx.fillText(firstChar, 160, 140);

  return canvas.toDataURL();
};

// 监听 videoUrl 变化，当准备好时初始化当前视频
watch(
  () => props.videoUrl,
  async (newVideoUrl) => {
    // 当videoUrl存在且文件信息存在时，初始化视频数据
    if (newVideoUrl && props.file) {
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
@import "@/styles/pages/mount-explorer/video-preview.css";
</style>
