<script setup>
import { computed, reactive, ref, watch } from "vue";
import { IconArrowUp, IconCheck, IconChevronDown, IconClose, IconDelete, IconPlus, IconRename } from "@/components/icons";

const TAG_COLORS = ["#EF4444", "#F97316", "#FACC15", "#22C55E", "#14B8A6", "#06B6D4", "#3B82F6", "#6366F1", "#A855F7", "#EC4899"];
const DARK_CHECK_COLORS = new Set(["#FACC15", "#22C55E", "#14B8A6", "#06B6D4"]);
const MAX_TAGS = 10;

const props = defineProps({
  show: { type: Boolean, default: false },
  tags: { type: Array, default: () => [] },
  loading: { type: Boolean, default: false },
});

const emit = defineEmits(["close", "create", "update", "delete", "reorder"]);
const createForm = reactive({ name: "", color: "#3B82F6" });
const editingId = ref(null);
const editForm = reactive({ name: "", color: "#3B82F6" });
const atTagLimit = computed(() => props.tags.length >= MAX_TAGS);

const checkColorClass = (color) => DARK_CHECK_COLORS.has(color) ? "text-gray-900" : "text-white";

watch(() => props.show, (show) => {
  if (!show) editingId.value = null;
});

const createTag = () => {
  const name = createForm.name.trim();
  if (!name || atTagLimit.value) return;
  emit("create", { name, color: createForm.color });
  createForm.name = "";
};

const startEdit = (tag) => {
  editingId.value = tag.id;
  editForm.name = tag.name;
  editForm.color = tag.color;
};

const saveEdit = (id) => {
  const name = editForm.name.trim();
  if (!name) return;
  emit("update", id, { name, color: editForm.color });
  editingId.value = null;
};

const move = (index, direction) => {
  const next = props.tags.map((tag) => tag.id);
  const target = index + direction;
  if (target < 0 || target >= next.length) return;
  [next[index], next[target]] = [next[target], next[index]];
  emit("reorder", next);
};
</script>

<template>
  <div v-if="show" class="fixed inset-0 z-[70] overflow-y-auto" role="dialog" aria-modal="true" aria-labelledby="tag-manager-title">
    <div class="flex min-h-screen items-center justify-center p-4">
      <div class="fixed inset-0 bg-gray-900/60" @click="$emit('close')"></div>
      <div class="relative w-full max-w-lg overflow-hidden rounded-xl bg-white shadow-2xl dark:bg-gray-800">
        <header class="flex items-center justify-between border-b border-gray-200 px-4 py-3 dark:border-gray-700">
          <div>
            <h3 id="tag-manager-title" class="font-medium text-gray-900 dark:text-white">管理标签</h3>
            <p class="text-xs text-gray-500 dark:text-gray-400">标签是全局的；删除标签不会删除文本。</p>
          </div>
          <button type="button" aria-label="关闭标签管理" class="rounded-md p-1 text-gray-400 hover:bg-gray-100 dark:hover:bg-gray-700" @click="$emit('close')"><IconClose class="h-5 w-5" /></button>
        </header>

        <div class="max-h-[70vh] overflow-y-auto p-4">
          <form class="rounded-lg border border-gray-200 bg-gray-50 p-3 dark:border-gray-700 dark:bg-gray-900/30" @submit.prevent="createTag">
            <div class="mb-2 flex items-center justify-between gap-2 text-xs">
              <label class="font-semibold text-gray-600 dark:text-gray-300">新建标签</label>
              <span class="text-gray-500 dark:text-gray-400">{{ tags.length }}/{{ MAX_TAGS }}</span>
            </div>
            <div class="flex gap-2">
              <input v-model="createForm.name" maxlength="32" class="min-w-0 flex-1 rounded-md border border-gray-300 bg-white px-3 py-2 text-sm text-gray-900 focus:border-primary-500 focus:ring-primary-500 dark:border-gray-600 dark:bg-gray-700 dark:text-white" placeholder="例如：工作、代码、收藏" />
              <button type="submit" class="inline-flex items-center gap-1 rounded-md bg-primary-600 px-3 py-2 text-sm font-medium text-white hover:bg-primary-700 disabled:cursor-not-allowed disabled:opacity-50" :disabled="loading || atTagLimit || !createForm.name.trim()"><IconPlus class="h-4 w-4" />添加</button>
            </div>
            <div class="mt-3 flex flex-wrap gap-2" aria-label="选择标签颜色">
              <button
                v-for="color in TAG_COLORS"
                :key="color"
                type="button"
                class="flex h-7 w-7 items-center justify-center rounded-full border border-black/10 shadow-sm transition-transform hover:scale-105 focus:outline-none focus-visible:ring-2 focus-visible:ring-primary-500 focus-visible:ring-offset-2 dark:border-white/20 dark:focus-visible:ring-offset-gray-800"
                :class="createForm.color === color ? 'scale-110' : ''"
                :style="{ backgroundColor: color }"
                :aria-label="color"
                :aria-pressed="createForm.color === color"
                @click="createForm.color = color"
                @pointerup="$event.currentTarget.blur()"
              >
                <IconCheck v-if="createForm.color === color" class="h-4 w-4 drop-shadow-sm" :class="checkColorClass(color)" />
              </button>
            </div>
          </form>

          <div class="mt-4 space-y-2">
            <div v-if="tags.length === 0" class="rounded-lg border border-dashed border-gray-300 py-8 text-center text-sm text-gray-500 dark:border-gray-600 dark:text-gray-400">创建第一个标签，开始整理文本。</div>
            <div v-for="(tag, index) in tags" :key="tag.id" class="rounded-lg border border-gray-200 p-3 dark:border-gray-700">
              <template v-if="editingId === tag.id">
                <div class="flex gap-2">
                  <input v-model="editForm.name" maxlength="32" class="min-w-0 flex-1 rounded-md border border-gray-300 bg-white px-3 py-2 text-sm dark:border-gray-600 dark:bg-gray-700 dark:text-white" @keydown.enter.prevent="saveEdit(tag.id)" />
                  <button type="button" class="rounded-md bg-primary-600 px-3 py-2 text-xs font-medium text-white" @click="saveEdit(tag.id)">保存</button>
                  <button type="button" class="rounded-md px-2 py-2 text-xs text-gray-500" @click="editingId = null">取消</button>
                </div>
                <div class="mt-2 flex flex-wrap gap-2" aria-label="选择标签颜色">
                  <button
                    v-for="color in TAG_COLORS"
                    :key="color"
                    type="button"
                    class="flex h-6 w-6 items-center justify-center rounded-full border border-black/10 shadow-sm transition-transform hover:scale-105 focus:outline-none focus-visible:ring-2 focus-visible:ring-primary-500 focus-visible:ring-offset-2 dark:border-white/20 dark:focus-visible:ring-offset-gray-800"
                    :class="editForm.color === color ? 'scale-110' : ''"
                    :style="{ backgroundColor: color }"
                    :aria-label="color"
                    :aria-pressed="editForm.color === color"
                    @click="editForm.color = color"
                    @pointerup="$event.currentTarget.blur()"
                  >
                    <IconCheck v-if="editForm.color === color" class="h-3.5 w-3.5 drop-shadow-sm" :class="checkColorClass(color)" />
                  </button>
                </div>
              </template>
              <template v-else>
                <div class="flex items-center gap-3">
                  <span class="h-3 w-3 flex-none rounded-full ring-1 ring-inset ring-gray-400/50" :style="{ backgroundColor: tag.color }"></span>
                  <div class="min-w-0 flex-1">
                    <p class="truncate text-sm font-medium text-gray-900 dark:text-white">{{ tag.name }}</p>
                    <p class="text-xs text-gray-500 dark:text-gray-400">{{ tag.usage_count || 0 }} 条文本</p>
                  </div>
                  <div class="flex items-center gap-1">
                    <button type="button" class="rounded p-1.5 text-gray-400 hover:bg-gray-100 disabled:opacity-30 dark:hover:bg-gray-700" :disabled="index === 0 || loading" title="上移" @click="move(index, -1)"><IconArrowUp class="h-4 w-4" /></button>
                    <button type="button" class="rounded p-1.5 text-gray-400 hover:bg-gray-100 disabled:opacity-30 dark:hover:bg-gray-700" :disabled="index === tags.length - 1 || loading" title="下移" @click="move(index, 1)"><IconChevronDown class="h-4 w-4" /></button>
                    <button type="button" class="rounded p-1.5 text-indigo-500 hover:bg-indigo-50 dark:hover:bg-indigo-900/20" title="编辑" @click="startEdit(tag)"><IconRename class="h-4 w-4" /></button>
                    <button type="button" class="rounded p-1.5 text-red-500 hover:bg-red-50 dark:hover:bg-red-900/20" title="删除" @click="$emit('delete', tag)"><IconDelete class="h-4 w-4" /></button>
                  </div>
                </div>
              </template>
            </div>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>
