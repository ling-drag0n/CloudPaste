<template>
  <div class="audio-preview rounded-lg p-4 mb-2 bg-gray-100 dark:bg-gray-700 w-full">
    <audio controls class="w-full" @loadeddata="handleLoad" @error="handleError">
      <source :src="previewUrl" :type="mimetype" />
      {{ t("fileView.preview.audio.notSupported") }}
    </audio>
  </div>
</template>

<script setup>
import { useI18n } from "vue-i18n";

const { t } = useI18n();

defineProps({
  previewUrl: {
    type: String,
    required: true,
  },
  mimetype: {
    type: String,
    required: true,
  },
});

const emit = defineEmits(["load", "error"]);

const handleLoad = () => {
  emit("load");
};

const handleError = () => {
  emit("error");
};
</script>
