<script setup>
import { IconAdjustments, IconMinus, IconPlus, IconTag } from "@/components/icons";

defineProps({
  tags: { type: Array, default: () => [] },
  selectedTagIds: { type: Array, default: () => [] },
  untagged: { type: Boolean, default: false },
  selectedCount: { type: Number, default: 0 },
  canManage: { type: Boolean, default: false },
  loading: { type: Boolean, default: false },
});

defineEmits(["clear", "toggle-tag", "toggle-untagged", "batch-add", "batch-remove", "manage"]);

const selectedTagStyle = (tag, selectedTagIds) => {
  if (!selectedTagIds.includes(tag.id)) return null;
  return { borderColor: tag.color === "#FFFFFF" ? "#9CA3AF" : tag.color };
};
</script>

<template>
  <section class="rounded-lg border border-gray-200 bg-white/80 p-2 shadow-sm dark:border-gray-700 dark:bg-gray-800/80" aria-label="标签筛选">
    <div class="flex items-start gap-2">
      <div class="flex min-w-0 flex-1 flex-wrap items-center gap-1.5">
        <span class="mr-1 inline-flex flex-none items-center gap-1 text-xs font-semibold text-gray-500 dark:text-gray-400">
          <IconTag class="h-4 w-4" />
          标签
        </span>
        <button
          type="button"
          class="inline-flex flex-none items-center rounded-full border px-2.5 py-1.5 text-xs font-medium transition focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-gray-400"
          :class="selectedTagIds.length === 0 && !untagged
            ? 'border-gray-300 bg-gray-100 text-gray-700 dark:border-gray-500 dark:bg-gray-700 dark:text-gray-100'
            : 'border-gray-200 bg-white text-gray-600 hover:border-gray-300 dark:border-gray-600 dark:bg-gray-700 dark:text-gray-300'"
          @pointerup="$event.currentTarget.blur()"
          @click="$emit('clear')"
        >
          全部
        </button>
        <button
          v-for="tag in tags"
          :key="tag.id"
          type="button"
          class="inline-flex max-w-full flex-none items-center gap-1.5 rounded-full border px-2.5 py-1.5 text-xs font-medium transition focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-offset-1"
          :class="selectedTagIds.includes(tag.id)
            ? 'border-gray-400 bg-gray-100 text-gray-900 shadow-sm dark:border-gray-500 dark:bg-gray-700 dark:text-white'
            : 'border-gray-200 bg-white text-gray-600 hover:-translate-y-px hover:shadow-sm dark:border-gray-600 dark:bg-gray-800 dark:text-gray-300'"
          :style="selectedTagStyle(tag, selectedTagIds)"
          :aria-pressed="selectedTagIds.includes(tag.id)"
          @pointerup="$event.currentTarget.blur()"
          @click="$emit('toggle-tag', tag.id)"
        >
          <span class="h-2.5 w-2.5 flex-none rounded-full ring-1 ring-inset ring-gray-400/50" :style="{ backgroundColor: tag.color }" aria-hidden="true"></span>
          <span class="truncate">{{ tag.name }}</span>
        </button>
        <button
          type="button"
          class="inline-flex flex-none items-center gap-1.5 rounded-full border px-2.5 py-1.5 text-xs font-medium transition focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-primary-500"
          :class="untagged
            ? 'border-gray-500 bg-gray-100 text-gray-900 ring-1 ring-gray-300 dark:bg-gray-700 dark:text-white'
            : 'border-gray-200 bg-white text-gray-600 hover:border-gray-300 dark:border-gray-600 dark:bg-gray-800 dark:text-gray-300'"
          :aria-pressed="untagged"
          @pointerup="$event.currentTarget.blur()"
          @click="$emit('toggle-untagged')"
        >
          <span class="h-2.5 w-2.5 rounded-full border border-dashed border-gray-400" aria-hidden="true"></span>
          无
        </button>
      </div>
      <div class="inline-flex flex-none items-center gap-1.5">
        <div class="inline-flex h-7 items-stretch overflow-hidden rounded-md border border-gray-200 bg-white dark:border-gray-600 dark:bg-gray-800" aria-label="批量标签操作">
          <button
            type="button"
            class="inline-flex w-7 items-center justify-center text-gray-500 transition hover:bg-primary-50 hover:text-primary-700 focus:z-10 focus:outline-none focus:ring-2 focus:ring-inset focus:ring-primary-500 disabled:cursor-not-allowed disabled:opacity-35 dark:text-gray-300 dark:hover:bg-primary-900/30 dark:hover:text-primary-200"
            :disabled="selectedCount === 0 || tags.length === 0 || loading"
            aria-label="给已选文本添加标签"
            title="添加标签"
            @click="$emit('batch-add')"
          >
            <IconPlus class="h-4 w-4" />
          </button>
          <button
            type="button"
            class="inline-flex w-7 items-center justify-center border-l border-gray-200 text-gray-500 transition hover:bg-gray-100 hover:text-gray-900 focus:z-10 focus:outline-none focus:ring-2 focus:ring-inset focus:ring-primary-500 disabled:cursor-not-allowed disabled:opacity-35 dark:border-gray-600 dark:text-gray-300 dark:hover:bg-gray-700 dark:hover:text-white"
            :disabled="selectedCount === 0 || tags.length === 0 || loading"
            aria-label="从已选文本移除标签"
            title="移除标签"
            @click="$emit('batch-remove')"
          >
            <IconMinus class="h-4 w-4" />
          </button>
        </div>
        <button
          v-if="canManage"
          type="button"
          aria-label="管理标签"
          class="inline-flex flex-none items-center gap-1 rounded-md px-2 py-1.5 text-xs font-medium text-gray-600 hover:bg-gray-100 focus:outline-none focus:ring-2 focus:ring-primary-500 dark:text-gray-300 dark:hover:bg-gray-700 sm:px-2.5"
          :disabled="loading"
          @click="$emit('manage')"
        >
          <IconAdjustments class="h-4 w-4" />
          <span class="hidden sm:inline">管理标签</span>
        </button>
      </div>
    </div>
  </section>
</template>
