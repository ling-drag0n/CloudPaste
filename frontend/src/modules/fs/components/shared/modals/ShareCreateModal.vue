<template>
  <div v-if="modelValue" class="fixed inset-0 z-[60] overflow-auto bg-black bg-opacity-50 flex items-center justify-center p-2 sm:p-4 pt-20 sm:pt-4">
    <div
      class="relative w-full max-w-sm sm:max-w-lg rounded-lg shadow-xl flex flex-col max-h-[90vh]"
      :class="darkMode ? 'bg-gray-800' : 'bg-white'"
    >
      <!-- 标题栏 -->
      <div class="p-4 flex justify-between items-center border-b" :class="darkMode ? 'border-gray-700' : 'border-gray-200'">
        <h3 class="text-lg font-semibold" :class="darkMode ? 'text-gray-100' : 'text-gray-900'">{{ t("mount.shareCreate.title") }}</h3>
        <button
          @click="closeModal"
          class="p-1 rounded-full transition-colors"
          :class="darkMode ? 'hover:bg-gray-700 text-gray-400 hover:text-gray-200' : 'hover:bg-gray-200 text-gray-500 hover:text-gray-700'"
        >
          <IconClose size="lg" aria-hidden="true" />
        </button>
      </div>

      <!-- 内容区 -->
      <div class="flex-1 p-4 overflow-y-auto">
        <!-- 分享对象（只读） -->
        <div class="mb-4 flex items-center gap-2 p-3 rounded-md" :class="darkMode ? 'bg-gray-700/50' : 'bg-gray-50'">
          <component :is="isDirectory ? IconFolder : IconDocument" size="md" class="flex-shrink-0" :class="darkMode ? 'text-gray-300' : 'text-gray-500'" aria-hidden="true" />
          <div class="flex-1 min-w-0">
            <div class="text-sm font-medium truncate" :class="darkMode ? 'text-gray-100' : 'text-gray-900'">
              {{ targetName || targetPath }}
            </div>
            <div class="text-xs" :class="darkMode ? 'text-gray-400' : 'text-gray-500'">{{ targetTypeLabel }}</div>
          </div>
        </div>

        <!-- 结果展示 -->
        <template v-if="result">
          <ShareLinkBox
            :dark-mode="darkMode"
            :label="t('mount.shareCreate.resultLabel')"
            :share-link="shareLink"
            :copy-tooltip="t('mount.shareCreate.copyLink')"
            :copy-success-text="t('mount.shareCreate.linkCopied')"
            :copy-failure-text="t('mount.shareCreate.copyFailed')"
            :show-qr-button="true"
            :qr-tooltip="t('mount.shareCreate.showQRCode')"
            @show-qr-code="showQRCodeModal = true"
            @status-message="handleStatusMessage"
          />

          <div class="mt-3 space-y-1 text-xs" :class="darkMode ? 'text-gray-400' : 'text-gray-500'">
            <div v-if="result.requires_password">
              {{ t("mount.shareCreate.passwordLabel") }}: {{ formData.password || t("mount.shareCreate.passwordSet") }}
            </div>
            <div>
              {{ t("mount.shareCreate.expiresAtLabel") }}:
              {{ result.expires_at ? formatDateTime(result.expires_at) : t("mount.shareCreate.expiresNever") }}
            </div>
            <div>
              {{ t("mount.shareCreate.maxViewsLabel") }}:
              {{ Number(result.max_views) > 0 ? result.max_views : t("mount.shareCreate.unlimited") }}
            </div>
          </div>
        </template>

        <!-- 表单 -->
        <template v-else>
          <div v-if="errorMessage" class="mb-3 p-3 rounded text-sm" :class="darkMode ? 'bg-red-900/20 text-red-300' : 'bg-red-50 text-red-600'">
            {{ errorMessage }}
          </div>

          <!-- 备注 -->
          <div class="mb-3">
            <label class="block text-sm font-medium mb-1" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">{{ t("mount.shareCreate.remark") }}</label>
            <textarea
              v-model="formData.remark"
              rows="2"
              class="w-full px-3 py-2 border rounded-md text-sm focus:ring-blue-500 focus:border-blue-500"
              :class="darkMode ? 'bg-gray-700 border-gray-600 text-gray-100' : 'bg-white border-gray-300 text-gray-900'"
              :placeholder="t('mount.shareCreate.remarkPlaceholder')"
            ></textarea>
          </div>

          <!-- 访问密码 -->
          <div class="mb-3">
            <label class="block text-sm font-medium mb-1" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">{{ t("mount.shareCreate.password") }}</label>
            <input
              type="text"
              v-model="formData.password"
              class="w-full px-3 py-2 border rounded-md text-sm focus:ring-blue-500 focus:border-blue-500"
              :class="darkMode ? 'bg-gray-700 border-gray-600 text-gray-100' : 'bg-white border-gray-300 text-gray-900'"
              :placeholder="t('mount.shareCreate.passwordPlaceholder')"
            />
          </div>

          <!-- 有效期 -->
          <div class="mb-3">
            <label class="block text-sm font-medium mb-1" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">{{ t("mount.shareCreate.expiry") }}</label>
            <select
              v-model="formData.expires_in"
              class="w-full px-3 py-2 border rounded-md text-sm focus:ring-blue-500 focus:border-blue-500"
              :class="darkMode ? 'bg-gray-700 border-gray-600 text-gray-100' : 'bg-white border-gray-300 text-gray-900'"
            >
              <option v-for="opt in expiryOptions" :key="opt.value" :value="opt.value">{{ opt.label }}</option>
            </select>
          </div>

          <!-- 下载次数上限 -->
          <div class="mb-3">
            <label class="block text-sm font-medium mb-1" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">{{ t("mount.shareCreate.maxViews") }}</label>
            <input
              type="number"
              min="0"
              :value="formData.max_views"
              @input="handleMaxViewsInput($event.target.value)"
              class="w-full px-3 py-2 border rounded-md text-sm focus:ring-blue-500 focus:border-blue-500"
              :class="darkMode ? 'bg-gray-700 border-gray-600 text-gray-100' : 'bg-white border-gray-300 text-gray-900'"
              :placeholder="t('mount.shareCreate.maxViewsPlaceholder')"
            />
          </div>

          <!-- 自定义 slug -->
          <div class="mb-1">
            <label class="block text-sm font-medium mb-1" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">{{ t("mount.shareCreate.slug") }}</label>
            <input
              type="text"
              :value="formData.slug"
              @input="handleSlugInput($event.target.value)"
              class="w-full px-3 py-2 border rounded-md text-sm focus:ring-blue-500 focus:border-blue-500"
              :class="darkMode ? 'bg-gray-700 border-gray-600 text-gray-100' : 'bg-white border-gray-300 text-gray-900'"
              :placeholder="t('mount.shareCreate.slugPlaceholder')"
            />
            <p v-if="slugError" class="mt-1 text-xs text-red-500">{{ slugError }}</p>
          </div>
        </template>
      </div>

      <!-- 底部操作栏 -->
      <div class="p-4 border-t flex justify-end gap-2" :class="darkMode ? 'border-gray-700' : 'border-gray-200'">
        <button
          type="button"
          @click="closeModal"
          :disabled="isSubmitting"
          class="px-4 py-2 rounded-md text-sm font-medium transition-colors disabled:opacity-50"
          :class="darkMode ? 'bg-gray-700 hover:bg-gray-600 text-gray-100' : 'bg-gray-200 hover:bg-gray-300 text-gray-700'"
        >
          {{ result ? t("mount.shareCreate.close") : t("mount.shareCreate.cancel") }}
        </button>
        <button
          v-if="!result"
          type="button"
          @click="submit"
          :disabled="isSubmitting || !targetPath"
          class="px-4 py-2 rounded-md text-sm font-medium transition-colors flex items-center gap-2 disabled:opacity-50 bg-blue-600 hover:bg-blue-700 text-white"
        >
          <IconRefresh v-if="isSubmitting" size="sm" class="animate-spin" aria-hidden="true" />
          {{ isSubmitting ? t("mount.shareCreate.submitting") : t("mount.shareCreate.submit") }}
        </button>
      </div>
    </div>

    <!-- 二维码弹窗 -->
    <QRCodeModal :visible="showQRCodeModal" :share-link="shareLink" @close="showQRCodeModal = false" @status-message="handleStatusMessage" />
  </div>
</template>

<script setup>
import { computed, ref, watch } from "vue";
import { useI18n } from "vue-i18n";
import { IconClose, IconDocument, IconFolder, IconRefresh } from "@/components/icons";
import ShareLinkBox from "@/components/common/ShareLinkBox.vue";
import QRCodeModal from "@/modules/paste/editor/components/QRCodeModal.vue";
import { createShareFromFileSystem } from "@/api/services/fsService.js";
import { useShareSettingsForm } from "@/composables/upload/useShareSettingsForm.js";
import { useGlobalMessage } from "@/composables/core/useGlobalMessage.js";
import { formatDateTime } from "@/utils/timeUtils.js";
import { createLogger } from "@/utils/logger.js";

const props = defineProps({
  modelValue: { type: Boolean, default: false },
  darkMode: { type: Boolean, default: false },
  // 目标 FS 路径（文件或文件夹）
  targetPath: { type: String, default: "" },
  targetName: { type: String, default: "" },
  isDirectory: { type: Boolean, default: false },
});

const emit = defineEmits(["update:modelValue", "created", "close"]);

const { t } = useI18n();
const log = createLogger("ShareCreateModal");
const { showSuccess, showError } = useGlobalMessage();

const { formData, slugError, validateSlug, handleSlugInput, handleMaxViewsInput, resetShareSettings } = useShareSettingsForm();

const isSubmitting = ref(false);
const errorMessage = ref("");
const result = ref(null);
const shareLink = ref("");
const showQRCodeModal = ref(false);

const expiryOptions = computed(() => [
  { value: "1", label: t("mount.shareCreate.expiryOptions.hour1") },
  { value: "24", label: t("mount.shareCreate.expiryOptions.day1") },
  { value: "168", label: t("mount.shareCreate.expiryOptions.day7") },
  { value: "720", label: t("mount.shareCreate.expiryOptions.day30") },
  { value: "0", label: t("mount.shareCreate.expiryOptions.never") },
]);

const targetTypeLabel = computed(() =>
  props.isDirectory ? t("mount.shareCreate.folderBadge") : t("mount.shareCreate.fileBadge")
);

const resetModalState = () => {
  resetShareSettings();
  isSubmitting.value = false;
  errorMessage.value = "";
  result.value = null;
  shareLink.value = "";
  showQRCodeModal.value = false;
};

watch(
  () => props.modelValue,
  (open) => {
    if (open) resetModalState();
  }
);

const closeModal = () => {
  if (isSubmitting.value) return;
  emit("update:modelValue", false);
  emit("close");
};

const handleStatusMessage = (payload) => {
  if (!payload?.message) return;
  if (payload.type === "error") {
    showError(payload.message);
  } else {
    showSuccess(payload.message);
  }
};

const submit = async () => {
  if (!props.targetPath || isSubmitting.value) return;
  if (!validateSlug()) return;

  isSubmitting.value = true;
  errorMessage.value = "";

  try {
    const resp = await createShareFromFileSystem(props.targetPath, {
      password: formData.password || "",
      expires_in: String(formData.expires_in ?? "0"),
      max_views: Math.max(0, Number(formData.max_views) || 0),
      remark: formData.remark || "",
      slug: formData.slug || "",
    });

    if (!resp || resp.success === false) {
      throw new Error(resp?.message || t("mount.shareCreate.failed"));
    }

    result.value = resp.data || {};
    shareLink.value = result.value.url ? `${window.location.origin}${result.value.url}` : "";
    emit("created", result.value);
  } catch (error) {
    log.error("创建分享链接失败:", error);
    errorMessage.value = error?.message || t("mount.shareCreate.failed");
    showError(errorMessage.value);
  } finally {
    isSubmitting.value = false;
  }
};
</script>

<style scoped>
/* 组件特有样式 */
</style>
