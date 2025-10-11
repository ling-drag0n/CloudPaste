<template>
  <div class="audio-player-container" :class="{ 'dark-theme': darkMode }">
    <div ref="aplayerContainer" class="aplayer-container"></div>
  </div>
</template>

<script setup>
import { ref, onMounted, onBeforeUnmount, watch, nextTick } from "vue";
import APlayer from "aplayer";
import "aplayer/dist/APlayer.min.css";

// Props 定义
const props = defineProps({
  // 音频文件列表
  audioList: {
    type: Array,
    default: () => [],
  },
  // 当前播放的音频信息
  currentAudio: {
    type: Object,
    default: () => null,
  },
  // 是否为深色模式
  darkMode: {
    type: Boolean,
    default: false,
  },
  // 是否自动播放
  autoplay: {
    type: Boolean,
    default: false,
  },
  // 是否显示播放列表
  showPlaylist: {
    type: Boolean,
    default: true,
  },
  // 播放器主题色
  theme: {
    type: String,
    default: "#3b82f6",
  },
  // 播放器模式：'normal', 'mini', 'fixed'
  mode: {
    type: String,
    default: "normal",
  },
  // 是否循环播放
  loop: {
    type: String,
    default: "all", // 'all', 'one', 'none'
  },
  // 播放顺序
  order: {
    type: String,
    default: "list", // 'list', 'random'
  },
  // 播放列表是否折叠
  listFolded: {
    type: Boolean,
    default: true,
  },
  // 播放列表最大高度
  listMaxHeight: {
    type: String,
    default: "250px",
  },
  // 音量
  volume: {
    type: Number,
    default: 0.7,
  },
  // 是否显示歌词
  showLrc: {
    type: Boolean,
    default: false,
  },
});

// Emits 定义
const emit = defineEmits(["play", "pause", "ended", "timeupdate", "loadstart", "canplay", "error", "listswitch", "listadd", "listremove", "listclear", "noticeshow", "noticehide"]);

// 响应式数据
const aplayerContainer = ref(null);
const aplayerInstance = ref(null);

// 计算主题色
const getThemeColor = () => {
  if (props.darkMode) {
    return "#60a5fa"; // 深色模式下使用较亮的蓝色
  }
  return props.theme;
};

// 初始化 APlayer
const initAPlayer = () => {
  if (!aplayerContainer.value) return;

  // 销毁现有实例
  if (aplayerInstance.value) {
    aplayerInstance.value.destroy();
    aplayerInstance.value = null;
  }

  // 准备音频数据
  const audioData = prepareAudioData();
  if (audioData.length === 0) return;

  // APlayer 配置
  const options = {
    container: aplayerContainer.value,
    audio: audioData,
    autoplay: props.autoplay,
    theme: getThemeColor(),
    loop: props.loop,
    order: props.order,
    preload: "metadata",
    volume: props.volume,
    mutex: true,
    listFolded: props.listFolded,
    listMaxHeight: props.listMaxHeight,
    lrcType: props.showLrc ? 3 : 0,
    storageName: "cloudpaste-aplayer",
  };

  // 根据模式调整配置
  if (props.mode === "mini") {
    options.mini = true;
  } else if (props.mode === "fixed") {
    options.fixed = {
      type: "fixed",
      position: "bottom",
    };
  }

  try {
    // 创建 APlayer 实例
    aplayerInstance.value = new APlayer(options);

    // 绑定事件监听器
    bindEvents();

    // 应用主题样式
    applyThemeStyles();
  } catch (error) {
    console.error("APlayer 初始化失败:", error);
    emit("error", error);
  }
};

// 准备音频数据
const prepareAudioData = () => {
  const audioData = [];

  // 如果有当前音频，优先添加
  if (props.currentAudio) {
    audioData.push(formatAudioItem(props.currentAudio));
  }

  // 添加播放列表中的其他音频
  if (props.audioList && props.audioList.length > 0) {
    props.audioList.forEach((audio) => {
      // 避免重复添加当前音频
      if (!props.currentAudio || audio.url !== props.currentAudio.url) {
        audioData.push(formatAudioItem(audio));
      }
    });
  }

  return audioData;
};

// 格式化音频项
const formatAudioItem = (audio) => {
  return {
    name: audio.name || audio.title || "未知音频",
    artist: audio.artist || "未知艺术家",
    url: audio.url,
    cover: audio.cover || audio.poster || generateDefaultCover(audio.name),
    lrc: audio.lrc || audio.lyrics,
    theme: getThemeColor(),
  };
};

// 生成默认封面
const generateDefaultCover = (name) => {
  // 使用文件名首字母生成简单的默认封面
  const firstChar = (name || "M")[0].toUpperCase();
  const canvas = document.createElement("canvas");
  canvas.width = 100;
  canvas.height = 100;
  const ctx = canvas.getContext("2d");

  // 背景色
  ctx.fillStyle = getThemeColor();
  ctx.fillRect(0, 0, 100, 100);

  // 文字
  ctx.fillStyle = "#ffffff";
  ctx.font = "bold 40px Arial";
  ctx.textAlign = "center";
  ctx.textBaseline = "middle";
  ctx.fillText(firstChar, 50, 50);

  return canvas.toDataURL();
};

// 绑定事件监听器
const bindEvents = () => {
  if (!aplayerInstance.value) return;

  const ap = aplayerInstance.value;

  ap.on("play", () => {
    emit("play", {
      audio: ap.list.audios[ap.list.index],
      index: ap.list.index,
    });
  });

  ap.on("pause", () => {
    emit("pause", {
      audio: ap.list.audios[ap.list.index],
      index: ap.list.index,
    });
  });

  ap.on("ended", () => {
    emit("ended", {
      audio: ap.list.audios[ap.list.index],
      index: ap.list.index,
    });
  });

  ap.on("timeupdate", () => {
    emit("timeupdate", {
      currentTime: ap.audio.currentTime,
      duration: ap.audio.duration,
      percentage: (ap.audio.currentTime / ap.audio.duration) * 100,
    });
  });

  ap.on("loadstart", () => {
    emit("loadstart");
  });

  ap.on("canplay", () => {
    emit("canplay");
  });

  ap.on("error", (error) => {
    // 检查是否是Service Worker相关的误报错误
    if (error?.target?.src?.includes(window.location.origin) && ap?.list?.audios?.[ap.list.index]?.url?.startsWith("https://")) {
      console.log("🎵 忽略Service Worker相关的误报错误，音频实际可以正常播放");
      return; // 忽略Service Worker误报错误
    }

    console.error("APlayer 播放错误:", error);
    emit("error", error);
  });

  ap.on("listswitch", (index) => {
    // 确保索引有效且音频数据存在
    const audio = ap.list && ap.list.audios && ap.list.audios[index] ? ap.list.audios[index] : null;

    emit("listswitch", {
      audio: audio,
      index: index,
    });
  });
};

// 应用主题样式
const applyThemeStyles = () => {
  if (!aplayerContainer.value) return;

  nextTick(() => {
    const aplayerElement = aplayerContainer.value.querySelector(".aplayer");
    if (!aplayerElement) return;

    // 更新主题色
    const themeColor = getThemeColor();
    aplayerElement.style.setProperty("--aplayer-theme", themeColor);

    // 应用暗色主题类
    if (props.darkMode) {
      aplayerContainer.value.classList.add("dark-theme");
    } else {
      aplayerContainer.value.classList.remove("dark-theme");
    }
  });
};

// 公开的方法
const play = () => {
  if (aplayerInstance.value) {
    aplayerInstance.value.play();
  }
};

const pause = () => {
  if (aplayerInstance.value) {
    aplayerInstance.value.pause();
  }
};

const toggle = () => {
  if (aplayerInstance.value) {
    aplayerInstance.value.toggle();
  }
};

const seek = (time) => {
  if (aplayerInstance.value) {
    aplayerInstance.value.seek(time);
  }
};

const setVolume = (volume) => {
  if (aplayerInstance.value) {
    aplayerInstance.value.volume(volume);
  }
};

const switchAudio = (index) => {
  if (aplayerInstance.value && aplayerInstance.value.list) {
    aplayerInstance.value.list.switch(index);
  }
};

const addAudio = (audio) => {
  if (aplayerInstance.value && aplayerInstance.value.list) {
    aplayerInstance.value.list.add(formatAudioItem(audio));
  }
};

const removeAudio = (index) => {
  if (aplayerInstance.value && aplayerInstance.value.list) {
    aplayerInstance.value.list.remove(index);
  }
};

// 暴露方法给父组件
defineExpose({
  play,
  pause,
  toggle,
  seek,
  setVolume,
  switchAudio,
  addAudio,
  removeAudio,
  getInstance: () => aplayerInstance.value,
});

// 监听属性变化
watch(
  () => props.darkMode,
  () => {
    applyThemeStyles();
  },
  { immediate: false }
);

watch(
  () => props.theme,
  () => {
    if (aplayerInstance.value) {
      aplayerInstance.value.theme(getThemeColor());
    }
    applyThemeStyles();
  }
);

watch(
  () => [props.currentAudio, props.audioList, props.loop, props.order],
  () => {
    initAPlayer();
  },
  { deep: true }
);

watch(
  () => props.volume,
  (newVolume) => {
    setVolume(newVolume);
  }
);

// 生命周期
onMounted(() => {
  nextTick(() => {
    initAPlayer();
  });
});

onBeforeUnmount(() => {
  if (aplayerInstance.value) {
    aplayerInstance.value.destroy();
    aplayerInstance.value = null;
  }
});
</script>

<style scoped>
.audio-player-container {
  width: 100%;
  position: relative;
}

.aplayer-container {
  width: 100%;
}
</style>

<style>
/* APlayer 深色模式样式 */
.dark-theme .aplayer {
  background: #1f2937 !important;
  border: 1px solid #374151 !important;
}

.dark-theme .aplayer .aplayer-info {
  background: #1f2937 !important;
  border-bottom: 1px solid #374151 !important;
}

.dark-theme .aplayer .aplayer-info .aplayer-music .aplayer-title {
  color: #f9fafb !important;
}

.dark-theme .aplayer .aplayer-info .aplayer-music .aplayer-author {
  color: #d1d5db !important;
}

.dark-theme .aplayer .aplayer-controller {
  background: #1f2937 !important;
}

.dark-theme .aplayer .aplayer-controller .aplayer-time {
  color: #d1d5db !important;
}

.dark-theme .aplayer .aplayer-controller .aplayer-time .aplayer-icon {
  fill: #d1d5db !important;
}

.dark-theme .aplayer .aplayer-controller .aplayer-icon {
  fill: #d1d5db !important;
}

.dark-theme .aplayer .aplayer-controller .aplayer-icon:hover {
  fill: #f9fafb !important;
}

.aplayer-dark .aplayer-list {
  background: #1f2937 !important;
  border: 1px solid #374151 !important;
}

.aplayer-dark .aplayer-list ol li {
  color: #d1d5db !important;
  border-bottom: 1px solid #374151 !important;
}

.aplayer-dark .aplayer-list ol li:hover {
  background: #374151 !important;
}

/* 暗色模式下选中项文字改为黑色，适配白色背景 */
.dark-theme .aplayer-list ol li.aplayer-list-light .aplayer-list-title {
  color: #1f2937 !important;
}

.dark-theme .aplayer-list ol li.aplayer-list-light .aplayer-list-author {
  color: #4b5563 !important;
}

/* 普通列表项保持白色文字 */
.dark-theme .aplayer-list ol li .aplayer-list-title {
  color: #f9fafb !important;
}

.dark-theme .aplayer-list ol li .aplayer-list-author {
  color: #9ca3af !important;
}

/* 响应式设计 */
@media (max-width: 768px) {
  .aplayer-container {
    font-size: 14px;
  }

  .aplayer .aplayer-info .aplayer-music .aplayer-title {
    font-size: 14px !important;
  }

  .aplayer .aplayer-info .aplayer-music .aplayer-author {
    font-size: 12px !important;
  }
}

/* 自定义主题色变量 */
.aplayer {
  --aplayer-theme: #3b82f6;
}

.aplayer .aplayer-controller .aplayer-bar-wrap .aplayer-bar .aplayer-played {
  background: var(--aplayer-theme) !important;
}

.aplayer .aplayer-controller .aplayer-bar-wrap .aplayer-bar .aplayer-played .aplayer-thumb {
  background: var(--aplayer-theme) !important;
}

.aplayer .aplayer-controller .aplayer-volume-wrap .aplayer-volume-bar .aplayer-volume {
  background: var(--aplayer-theme) !important;
}
</style>
