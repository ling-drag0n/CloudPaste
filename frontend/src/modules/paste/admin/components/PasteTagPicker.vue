<script setup>
import { computed, ref } from "vue";
import { onClickOutside } from "@vueuse/core";
import { IconCheck, IconChevronDown } from "@/components/icons";

const props = defineProps({
  tags: { type: Array, default: () => [] },
  modelValue: { type: Array, default: () => [] },
  emptyText: { type: String, default: "还没有标签，请先在标签管理中创建。" },
  variant: { type: String, default: "pills" },
  placeholder: { type: String, default: "请选择标签（可选）" },
});

const emit = defineEmits(["update:modelValue"]);
const rootRef = ref(null);
const open = ref(false);
const selectedTags = computed(() => props.tags.filter((tag) => props.modelValue.includes(tag.id)));
const selectionText = computed(() => selectedTags.value.length ? selectedTags.value.map((tag) => tag.name).join("、") : props.placeholder);

const selectedTagStyle = (tag) => {
  if (!props.modelValue.includes(tag.id)) return null;
  return { borderColor: tag.color === "#FFFFFF" ? "#9CA3AF" : tag.color };
};

onClickOutside(rootRef, () => {
  open.value = false;
});

const toggle = (id) => {
  const next = props.modelValue.includes(id)
    ? props.modelValue.filter((tagId) => tagId !== id)
    : [...props.modelValue, id];
  emit("update:modelValue", next);
};
</script>

<template>
  <div ref="rootRef">
    <div v-if="tags.length && variant === 'select'">
      <button
        type="button"
        class="flex h-[2.375rem] w-full items-center justify-between gap-3 rounded-md border border-gray-300 bg-white px-3 py-2 text-left text-sm leading-5 text-gray-900 transition focus:border-primary-500 focus:outline-none focus:ring-1 focus:ring-primary-500 dark:border-gray-700 dark:bg-gray-800 dark:text-gray-100 dark:focus:border-primary-600 dark:focus:ring-primary-600"
        :aria-expanded="open"
        aria-haspopup="listbox"
        @click="open = !open"
        @keydown.esc="open = false"
      >
        <span
          v-if="selectedTags.length"
          class="flex min-w-0 flex-1 items-center gap-2 overflow-hidden"
          :title="selectionText"
        >
          <span
            v-for="(tag, index) in selectedTags"
            :key="tag.id"
            class="inline-flex flex-none items-center gap-1"
            :class="index > 0 ? 'border-l border-gray-300 pl-2 dark:border-gray-600' : ''"
          >
            <span class="h-2.5 w-2.5 rounded-full ring-1 ring-inset ring-gray-400/50" :style="{ backgroundColor: tag.color }" aria-hidden="true"></span>
            <span>{{ tag.name }}</span>
          </span>
        </span>
        <span v-else class="min-w-0 flex-1 truncate text-gray-500 dark:text-gray-400">{{ placeholder }}</span>
        <IconChevronDown class="h-4 w-4 flex-none text-gray-500 transition-transform" :class="open ? 'rotate-180' : ''" />
      </button>

      <div
        v-if="open"
        class="mt-1 max-h-56 overflow-y-auto rounded-md border border-gray-200 bg-white p-1 shadow-lg dark:border-gray-700 dark:bg-gray-800"
        role="listbox"
        aria-multiselectable="true"
      >
        <button
          v-for="tag in tags"
          :key="tag.id"
          type="button"
          role="option"
          class="flex w-full items-center gap-2 rounded px-2.5 py-2 text-left text-sm transition-colors focus:outline-none focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-primary-500"
          :class="modelValue.includes(tag.id)
            ? 'bg-primary-50 text-primary-700 dark:bg-primary-900/30 dark:text-primary-200'
            : 'text-gray-700 hover:bg-gray-100 dark:text-gray-200 dark:hover:bg-gray-700'"
          :aria-selected="modelValue.includes(tag.id)"
          @click="toggle(tag.id)"
        >
          <span class="h-2.5 w-2.5 flex-none rounded-full ring-1 ring-inset ring-gray-400/50" :style="{ backgroundColor: tag.color }" aria-hidden="true"></span>
          <span class="min-w-0 flex-1 truncate">{{ tag.name }}</span>
          <IconCheck v-if="modelValue.includes(tag.id)" class="h-4 w-4 flex-none" />
        </button>
      </div>
    </div>

    <div v-else-if="tags.length" class="flex flex-wrap gap-2">
      <button
        v-for="tag in tags"
        :key="tag.id"
        type="button"
        class="inline-flex max-w-full items-center gap-1.5 rounded-full border px-2.5 py-1.5 text-xs font-medium transition focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-offset-1"
        :class="modelValue.includes(tag.id)
          ? 'border-gray-400 bg-gray-100 text-gray-900 shadow-sm dark:border-gray-500 dark:bg-gray-700 dark:text-white'
          : 'border-gray-200 bg-white text-gray-600 hover:-translate-y-px hover:shadow-sm dark:border-gray-600 dark:bg-gray-800 dark:text-gray-300'"
        :style="selectedTagStyle(tag)"
        :aria-pressed="modelValue.includes(tag.id)"
        @pointerup="$event.currentTarget.blur()"
        @click="toggle(tag.id)"
      >
        <span class="h-2.5 w-2.5 flex-none rounded-full ring-1 ring-inset ring-gray-400/50" :style="{ backgroundColor: tag.color }" aria-hidden="true"></span>
        <span class="truncate">{{ tag.name }}</span>
      </button>
    </div>
    <p v-else class="rounded-md border border-dashed border-gray-300 px-3 py-4 text-center text-xs text-gray-500 dark:border-gray-600 dark:text-gray-400">
      {{ emptyText }}
    </p>
  </div>
</template>
