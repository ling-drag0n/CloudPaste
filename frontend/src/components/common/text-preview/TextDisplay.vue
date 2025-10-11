<template>
  <div class="text-display" :class="{ 'text-display-dark': darkMode }">
    <pre class="text-content" :class="textClass">{{ content }}</pre>
  </div>
</template>

<script setup>
import { computed } from "vue";

// Props
const props = defineProps({
  content: {
    type: String,
    required: true,
  },
  darkMode: {
    type: Boolean,
    default: false,
  },
});

// 计算属性
const textClass = computed(() => ({
  "text-content-dark": props.darkMode,
  "text-content-light": !props.darkMode,
}));
</script>

<style scoped>
.text-display {
  width: 100%;
  flex: 1;
  overflow: auto;
}

.text-content {
  padding: 1rem;
  font-size: 0.875rem;
  font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
  white-space: pre-wrap;
  word-break: break-words;
  color: #1f2937;
  background-color: transparent;
  margin: 0;
  min-height: 100%;
  line-height: 1.5;
  width: 100%;
  box-sizing: border-box;
}

.text-content-dark {
  color: #e5e7eb;
}

.text-content-light {
  color: #1f2937;
}

/* 滚动条样式 */
.text-display::-webkit-scrollbar {
  width: 8px;
  height: 8px;
}

.text-display::-webkit-scrollbar-track {
  background-color: #f3f4f6;
}

.text-display-dark::-webkit-scrollbar-track {
  background-color: #374151;
}

.text-display::-webkit-scrollbar-thumb {
  background-color: #d1d5db;
  border-radius: 4px;
}

.text-display-dark::-webkit-scrollbar-thumb {
  background-color: #6b7280;
}

.text-display::-webkit-scrollbar-thumb:hover {
  background-color: #9ca3af;
}

.text-display-dark::-webkit-scrollbar-thumb:hover {
  background-color: #9ca3af;
}
</style>
