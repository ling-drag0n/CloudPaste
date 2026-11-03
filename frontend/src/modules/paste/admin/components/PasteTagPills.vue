<script setup>
import { computed } from "vue";

const props = defineProps({
  tags: { type: Array, default: () => [] },
  max: { type: Number, default: 3 },
  clickable: { type: Boolean, default: false },
  size: { type: String, default: "sm" },
});

const emit = defineEmits(["tag-click"]);

const visibleTags = computed(() => props.tags.slice(0, props.max));
const hiddenCount = computed(() => Math.max(0, props.tags.length - props.max));

const tagStyle = (color) => ({
  "--tag-color": color || "#6B7280",
  "--tag-bg": `${color || "#6B7280"}18`,
  "--tag-border": `${color || "#6B7280"}45`,
});
</script>

<template>
  <div v-if="tags.length" class="flex min-w-0 flex-wrap items-center gap-1" aria-label="文本标签">
    <component
      :is="clickable ? 'button' : 'span'"
      v-for="tag in visibleTags"
      :key="tag.id"
      :type="clickable ? 'button' : undefined"
      class="inline-flex max-w-[9rem] items-center gap-1 rounded-full border font-medium leading-none text-gray-700 dark:text-gray-200"
      :class="[
        size === 'xs' ? 'px-1.5 py-1 text-[10px]' : 'px-2 py-1 text-[11px]',
        clickable ? 'transition hover:-translate-y-px hover:shadow-sm focus:outline-none focus:ring-2 focus:ring-offset-1' : '',
      ]"
      :style="tagStyle(tag.color)"
      :title="tag.name"
      @click="clickable && emit('tag-click', tag)"
    >
      <span class="h-2 w-2 flex-none rounded-full bg-[var(--tag-color)] ring-1 ring-inset ring-gray-400/50" aria-hidden="true"></span>
      <span class="truncate">{{ tag.name }}</span>
    </component>
    <span
      v-if="hiddenCount"
      class="rounded-full border border-gray-200 bg-gray-50 px-1.5 py-1 text-[10px] font-medium leading-none text-gray-500 dark:border-gray-600 dark:bg-gray-700 dark:text-gray-300"
      :title="tags.slice(max).map((tag) => tag.name).join('、')"
    >
      +{{ hiddenCount }}
    </span>
  </div>
</template>

<style scoped>
span[style],
button[style] {
  background-color: var(--tag-bg);
  border-color: var(--tag-border);
}

button[style]:focus-visible {
  --tw-ring-color: var(--tag-color);
}
</style>
