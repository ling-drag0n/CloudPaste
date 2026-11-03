<script setup>
import { ref, watch } from "vue";
import { IconClose, IconTag } from "@/components/icons";
import PasteTagPicker from "./PasteTagPicker.vue";

const props = defineProps({
  show: { type: Boolean, default: false },
  mode: { type: String, default: "add" },
  tags: { type: Array, default: () => [] },
  selectedCount: { type: Number, default: 0 },
  loading: { type: Boolean, default: false },
});

const emit = defineEmits(["close", "apply"]);
const selected = ref([]);

watch(() => props.show, (show) => {
  if (show) selected.value = [];
});
</script>

<template>
  <div v-if="show" class="fixed inset-0 z-[70] overflow-y-auto" role="dialog" aria-modal="true" :aria-labelledby="'batch-tag-title'">
    <div class="flex min-h-screen items-center justify-center p-4">
      <div class="fixed inset-0 bg-gray-900/60" @click="$emit('close')"></div>
      <div class="relative w-full max-w-md overflow-hidden rounded-xl bg-white shadow-2xl dark:bg-gray-800">
        <header class="flex items-center justify-between border-b border-gray-200 px-4 py-3 dark:border-gray-700">
          <div class="flex items-center gap-2">
            <span class="rounded-full bg-primary-50 p-2 text-primary-600 dark:bg-primary-900/30 dark:text-primary-300"><IconTag class="h-5 w-5" /></span>
            <div>
              <h3 id="batch-tag-title" class="font-medium text-gray-900 dark:text-white">{{ mode === 'add' ? '批量添加标签' : '批量移除标签' }}</h3>
              <p class="text-xs text-gray-500 dark:text-gray-400">将作用于已选择的 {{ selectedCount }} 条文本</p>
            </div>
          </div>
          <button type="button" aria-label="关闭批量标签" class="rounded-md p-1 text-gray-400 hover:bg-gray-100 dark:hover:bg-gray-700" @click="$emit('close')"><IconClose class="h-5 w-5" /></button>
        </header>
        <div class="max-h-[55vh] overflow-y-auto p-4">
          <PasteTagPicker v-model="selected" :tags="tags" />
        </div>
        <footer class="flex justify-end gap-2 border-t border-gray-200 bg-gray-50 px-4 py-3 dark:border-gray-700 dark:bg-gray-700/50">
          <button type="button" class="rounded-md border border-gray-300 px-3 py-2 text-sm text-gray-700 dark:border-gray-600 dark:text-gray-200" @click="$emit('close')">取消</button>
          <button
            type="button"
            class="rounded-md bg-primary-600 px-3 py-2 text-sm font-medium text-white hover:bg-primary-700 disabled:cursor-not-allowed disabled:opacity-50"
            :disabled="loading || selected.length === 0"
            @click="$emit('apply', selected)"
          >
            {{ loading ? '处理中...' : mode === 'add' ? '添加标签' : '移除标签' }}
          </button>
        </footer>
      </div>
    </div>
  </div>
</template>
