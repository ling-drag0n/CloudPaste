<template>
  <div class="editor-form mt-4 border-t pt-3 w-full overflow-hidden" :class="darkMode ? 'border-gray-700' : 'border-gray-200'">
    <div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-5 gap-4">
      <div class="form-group">
        <label class="form-label" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">{{ $t("markdown.form.remark") }}</label>
        <input
          type="text"
          class="form-input"
          :class="getInputClasses()"
          :placeholder="$t('markdown.form.remarkPlaceholder')"
          v-model="formData.remark"
          :disabled="!hasPermission"
        />
      </div>

      <div class="form-group">
        <label class="form-label" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">{{ $t("markdown.form.customLink") }}</label>
        <input
          type="text"
          class="form-input"
          :class="[getInputClasses(), slugError ? (darkMode ? 'border-red-500' : 'border-red-600') : '']"
          :placeholder="$t('markdown.form.customLinkPlaceholder')"
          v-model="formData.customLink"
          :disabled="!hasPermission"
          @input="validateCustomLink"
        />
        <p v-if="slugError" class="mt-1 text-sm" :class="darkMode ? 'text-red-400' : 'text-red-600'">{{ slugError }}</p>
        <p v-else class="mt-1 text-xs text-gray-500 dark:text-gray-400">{{ $t("markdown.validation.slugInvalid") }}</p>
      </div>

      <div class="form-group">
        <label class="form-label" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">{{ $t("markdown.form.password") }}</label>
        <input
          type="text"
          class="form-input"
          :class="getInputClasses()"
          :placeholder="$t('markdown.form.passwordPlaceholder')"
          v-model="formData.password"
          :disabled="!hasPermission"
        />
      </div>

      <div class="form-group">
        <label class="form-label" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">{{ $t("markdown.form.expiryTime") }}</label>
        <select class="form-input" :class="getInputClasses()" v-model="formData.expiry_time" :disabled="!hasPermission">
          <option value="1">1 {{ $t("markdown.form.expiryHour") }}</option>
          <option value="24">1 {{ $t("markdown.form.expiryDay") }}</option>
          <option value="168">7 {{ $t("markdown.form.expiryDay") }}</option>
          <option value="720">30 {{ $t("markdown.form.expiryDay") }}</option>
          <option value="0">{{ $t("markdown.form.expiryNever") }}</option>
        </select>
      </div>

      <div class="form-group">
        <label class="form-label" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">{{ $t("markdown.form.maxViews") }}</label>
        <input
          type="number"
          min="0"
          step="1"
          pattern="\d*"
          class="form-input"
          :class="getInputClasses()"
          :placeholder="$t('markdown.form.maxViewsPlaceholder')"
          v-model.number="formData.max_views"
          @input="validateMaxViews"
          :disabled="!hasPermission"
        />
      </div>
    </div>

    <div class="submit-section mt-4 flex flex-row items-center gap-4">
      <button class="btn-primary" @click="handleSubmit" :disabled="isSubmitting || !hasPermission">
        {{ isSubmitting ? $t("markdown.form.creating") : $t("markdown.form.createShare") }}
      </button>

      <div class="saving-status ml-auto text-sm" v-if="savingStatus">
        <span :class="[isErrorMessage(savingStatus) ? (darkMode ? 'text-red-400' : 'text-red-600') : darkMode ? 'text-gray-300' : 'text-gray-600']">{{ savingStatus }}</span>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, reactive } from "vue";
import { useI18n } from "vue-i18n";

const { t } = useI18n();

// Props
const props = defineProps({
  darkMode: {
    type: Boolean,
    default: false,
  },
  hasPermission: {
    type: Boolean,
    default: false,
  },
  isSubmitting: {
    type: Boolean,
    default: false,
  },
  savingStatus: {
    type: String,
    default: "",
  },
});

// Emits
const emit = defineEmits(["submit", "form-change"]);

// 表单数据
const formData = reactive({
  remark: "",
  customLink: "",
  password: "",
  expiry_time: "0",
  max_views: 0,
});

// 验证错误
const slugError = ref("");

// 验证自定义链接后缀格式
const validateCustomLink = () => {
  slugError.value = "";

  if (!formData.customLink) {
    emit("form-change", { ...formData, isValid: true });
    return true;
  }

  const slugRegex = /^[a-zA-Z0-9._-]+$/;
  if (!slugRegex.test(formData.customLink)) {
    slugError.value = t("markdown.validation.slugInvalid");
    emit("form-change", { ...formData, isValid: false });
    return false;
  }

  emit("form-change", { ...formData, isValid: true });
  return true;
};

// 验证最大查看次数
const validateMaxViews = (event) => {
  const value = event.target.value;

  if (value < 0) {
    formData.max_views = 0;
    return;
  }

  if (value.toString().includes(".")) {
    formData.max_views = parseInt(value);
  }

  if (isNaN(value) || value === "") {
    formData.max_views = 0;
  } else {
    formData.max_views = parseInt(value);
  }

  emit("form-change", { ...formData, isValid: !slugError.value });
};

// 获取输入框样式
const getInputClasses = () => {
  return props.darkMode
    ? "bg-gray-800 border-gray-700 text-gray-100 focus:ring-primary-600 focus:border-primary-600"
    : "bg-white border-gray-300 text-gray-900 focus:ring-primary-500 focus:border-primary-500";
};

// 判断是否为错误消息
const isErrorMessage = (message) => {
  const errorKeywords = [
    t("markdown.messages.createFailed"),
    t("markdown.messages.linkOccupied"),
    t("common.noPermission"),
    t("markdown.messages.contentTooLarge"),
    t("markdown.messages.unknownError"),
    t("markdown.copyFailed"),
    "失败",
    "错误",
    "不能", // 保留中文关键词作为后备
  ];
  return errorKeywords.some((keyword) => message.includes(keyword));
};

// 处理表单提交
const handleSubmit = () => {
  if (!validateCustomLink()) {
    return;
  }

  emit("submit", { ...formData });
};

// 重置表单
const resetForm = () => {
  formData.remark = "";
  formData.customLink = "";
  formData.password = "";
  formData.expiry_time = "0";
  formData.max_views = 0;
  slugError.value = "";
};

// 获取表单数据
const getFormData = () => {
  return { ...formData };
};

// 设置表单数据
const setFormData = (data) => {
  Object.assign(formData, data);
};

// 暴露方法
defineExpose({
  resetForm,
  getFormData,
  setFormData,
  validateCustomLink,
});
</script>

<style scoped>
.form-input {
  width: 100%;
  max-width: 100%;
  padding: 0.5rem;
  border-width: 1px;
  border-radius: 0.375rem;
}

.form-label {
  display: block;
  margin-bottom: 0.5rem;
  font-size: 0.875rem;
}

.form-group {
  margin-bottom: 0.75rem;
}

.btn-primary {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  padding: 0.5rem 1rem;
  font-weight: 500;
  border-radius: 0.375rem;
  background-color: v-bind('props.darkMode ? "#3b82f6" : "#2563eb"');
  color: white;
  transition: background-color 0.2s;
}

.btn-primary:hover {
  background-color: v-bind('props.darkMode ? "#2563eb" : "#1d4ed8"');
}

.btn-primary:disabled {
  opacity: 0.7;
  cursor: not-allowed;
}

/* 移动端优化 */
@media (max-width: 640px) {
  .form-input,
  .form-label {
    width: 100%;
    max-width: 100%;
  }

  .form-group {
    margin-bottom: 0.75rem;
  }
}
</style>
