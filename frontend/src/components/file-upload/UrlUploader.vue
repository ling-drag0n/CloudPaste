<template>
  <div class="url-uploader">
    <!-- URL输入区域 -->
    <div
      class="url-input-zone mb-5 border-2 border-dashed rounded-lg transition-all duration-300 overflow-hidden"
      :class="darkMode ? 'border-gray-600 bg-gray-800/30' : 'border-gray-300 bg-gray-50'"
    >
      <!-- 上部图标和说明 -->
      <div class="flex flex-col items-center justify-center py-4 px-4">
        <div class="icon-container mb-2 bg-opacity-10 p-2 rounded-full" :class="darkMode ? 'bg-blue-600' : 'bg-blue-100'">
          <svg
            xmlns="http://www.w3.org/2000/svg"
            class="h-8 w-8 transition-colors duration-300"
            :class="darkMode ? 'text-blue-400' : 'text-blue-600'"
            fill="none"
            viewBox="0 0 24 24"
            stroke="currentColor"
          >
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.5" d="M13.828 10.172a4 4 0 00-5.656 0l-4 4a4 4 0 105.656 5.656l1.102-1.101" />
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.5" d="M10.172 13.828a4 4 0 015.656 0l4 4a4 4 0 01-5.656 5.656l-1.102-1.101" />
          </svg>
        </div>
        <div class="text-center mb-3">
          <p class="text-base font-medium transition-colors duration-300" :class="darkMode ? 'text-gray-200' : 'text-gray-800'">
            {{ t("file.enterUrl") }}
          </p>
          <p class="text-xs mt-0.5 transition-colors duration-300" :class="darkMode ? 'text-gray-400' : 'text-gray-500'">
            {{ t("file.supportedUrlTypes") }}
          </p>
        </div>
      </div>

      <!-- 输入框区域 - 使用渐变边框效果 -->
      <div class="px-4 pb-6">
        <div class="w-full max-w-md mx-auto relative">
          <div
            class="input-wrapper relative rounded-lg overflow-hidden shadow-sm"
            :class="[darkMode ? 'shadow-gray-800' : 'shadow-gray-200', urlInput ? (darkMode ? 'ring-1 ring-blue-600/50' : 'ring-1 ring-blue-500/50') : '']"
          >
            <input
              type="url"
              v-model="urlInput"
              class="form-input w-full pr-[110px] rounded-md shadow-sm focus:ring-2 focus:ring-offset-1 focus:border-transparent h-12 px-4"
              :class="[
                darkMode
                  ? 'bg-gray-700 border-gray-600 text-white focus:ring-blue-600 focus:ring-offset-gray-800'
                  : 'bg-white border-gray-300 text-gray-900 focus:ring-blue-500 focus:ring-offset-white',
              ]"
              :placeholder="t('file.urlPlaceholder')"
              :disabled="isAnalyzing || isUploading"
            />
            <button
              type="button"
              @click="analyzeUrl"
              class="absolute right-0 top-0 h-full px-4 font-medium focus:outline-none transition-colors flex items-center justify-center min-w-[100px] rounded-r-md"
              :class="[
                isAnalyzing
                  ? darkMode
                    ? 'bg-gray-700 text-gray-400 cursor-not-allowed'
                    : 'bg-gray-200 text-gray-500 cursor-not-allowed'
                  : darkMode
                  ? 'bg-blue-600 hover:bg-blue-700 text-white'
                  : 'bg-blue-600 hover:bg-blue-700 text-white',
                !urlInput ? (darkMode ? 'opacity-60' : 'opacity-60') : '',
              ]"
              :disabled="!urlInput || isAnalyzing || isUploading"
            >
              <svg v-if="isAnalyzing" class="animate-spin -ml-1 mr-2 h-4 w-4" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
                <path
                  class="opacity-75"
                  fill="currentColor"
                  d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"
                ></path>
              </svg>
              {{ isAnalyzing ? t("file.analyzing") : t("file.analyze") }}
            </button>
          </div>
          <p v-if="urlError" class="mt-2 text-sm text-red-600 dark:text-red-400 flex items-center">
            <svg xmlns="http://www.w3.org/2000/svg" class="h-4 w-4 mr-1 flex-shrink-0" viewBox="0 0 20 20" fill="currentColor">
              <path fill-rule="evenodd" d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-7 4a1 1 0 11-2 0 1 1 0 012 0zm-1-9a1 1 0 00-1 1v4a1 1 0 102 0V6a1 1 0 00-1-1z" clip-rule="evenodd" />
            </svg>
            {{ urlError }}
          </p>
        </div>
      </div>
    </div>

    <!-- 已解析文件信息预览 -->
    <div v-if="fileInfo" class="url-file-preview mb-5 animate-fadeIn">
      <div class="files-header flex justify-between items-center mb-2">
        <h3 class="text-sm font-medium flex items-center" :class="darkMode ? 'text-gray-200' : 'text-gray-700'">
          <svg
            xmlns="http://www.w3.org/2000/svg"
            class="h-4 w-4 mr-1.5"
            :class="darkMode ? 'text-blue-400' : 'text-blue-600'"
            fill="none"
            viewBox="0 0 24 24"
            stroke="currentColor"
          >
            <path
              stroke-linecap="round"
              stroke-linejoin="round"
              stroke-width="2"
              d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z"
            />
          </svg>
          {{ t("file.urlFileInfo") }}
        </h3>
        <button
          type="button"
          @click="clearFileInfo"
          class="text-xs px-2 py-1 rounded transition-colors flex items-center"
          :class="darkMode ? 'bg-gray-700 hover:bg-gray-600 text-gray-300' : 'bg-gray-100 hover:bg-gray-200 text-gray-600'"
          :disabled="isUploading"
        >
          <svg xmlns="http://www.w3.org/2000/svg" class="h-3 w-3 mr-1" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
          </svg>
          {{ t("file.clear") }}
        </button>
      </div>

      <div class="file-item p-3 rounded-lg border transition-colors" :class="darkMode ? 'bg-gray-800/60 border-gray-700' : 'bg-gray-50 border-gray-200'">
        <div class="flex items-center">
          <div class="file-icon mr-3 p-1.5 rounded-lg" :class="darkMode ? 'bg-gray-700' : 'bg-white'">
            <div class="h-8 w-8" v-html="getFileIconClassLocal(fileInfo.filename)"></div>
          </div>
          <div class="file-info w-0 flex-grow mr-3">
            <div class="font-medium text-base truncate" :class="darkMode ? 'text-white' : 'text-gray-900'">
              {{ displayFilename }}
            </div>
            <div class="flex justify-between mt-0.5">
              <span class="text-xs font-medium px-1.5 py-0.5 rounded-full" :class="darkMode ? 'bg-gray-700 text-gray-300' : 'bg-gray-200 text-gray-700'">
                {{ displayFileSize }}
              </span>
              <span
                v-if="displayMimeType"
                class="text-xs px-1.5 py-0.5 rounded-full max-w-32 truncate inline-block"
                :class="darkMode ? 'bg-blue-900/30 text-blue-300' : 'bg-blue-100 text-blue-700'"
                :title="displayMimeType"
              >
                {{ displayMimeType }}
              </span>
            </div>
          </div>
        </div>

        <!-- 自定义文件名 -->
        <div class="mt-3 pt-2 border-t" :class="darkMode ? 'border-gray-700' : 'border-gray-200'">
          <label class="text-xs font-medium mb-1 block" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">
            {{ t("file.customFileName") }}
          </label>
          <input
            type="text"
            v-model="customFilename"
            class="form-input w-full rounded-md shadow-sm focus:ring-2 focus:ring-offset-1 focus:border-transparent py-2"
            :class="[
              darkMode
                ? 'bg-gray-700 border-gray-600 text-white focus:ring-blue-600 focus:ring-offset-gray-800'
                : 'bg-white border-gray-300 text-gray-900 focus:ring-blue-500 focus:ring-offset-white',
            ]"
            :placeholder="displayFilename || t('file.customFilename')"
            :disabled="isUploading"
          />
        </div>
      </div>
    </div>

    <!-- 上传选项表单 -->
    <div class="upload-form">
      <form @submit.prevent="submitUpload">
        <!-- S3配置选择 -->
        <div class="mb-6">
          <h3 class="text-lg font-medium mb-4" :class="darkMode ? 'text-gray-200' : 'text-gray-700'">{{ t("file.storage") }}</h3>
          <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div class="form-group flex flex-col">
              <label class="form-label text-sm font-medium mb-1.5" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">{{ t("file.storage") }}</label>
              <div class="relative">
                <select
                  v-model="formData.s3_config_id"
                  class="form-input w-full rounded-md shadow-sm focus:ring-2 focus:ring-offset-1 focus:border-transparent appearance-none"
                  :class="[
                    darkMode
                      ? 'bg-gray-700 border-gray-600 text-white focus:ring-blue-600 focus:ring-offset-gray-800'
                      : 'bg-white border-gray-300 text-gray-900 focus:ring-blue-500 focus:ring-offset-white',
                  ]"
                  :disabled="!s3Configs.length || loading || isUploading"
                  required
                >
                  <option value="" disabled selected>{{ s3Configs.length ? t("file.selectStorage") : t("file.noStorage") }}</option>
                  <option v-for="config in s3Configs" :key="config.id" :value="config.id">{{ config.name }} ({{ config.provider_type }})</option>
                </select>
                <div class="absolute inset-y-0 right-0 flex items-center pr-3 pointer-events-none">
                  <svg
                    xmlns="http://www.w3.org/2000/svg"
                    class="h-5 w-5"
                    :class="darkMode ? 'text-gray-400' : 'text-gray-500'"
                    fill="none"
                    viewBox="0 0 24 24"
                    stroke="currentColor"
                  >
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7" />
                  </svg>
                </div>
              </div>
            </div>

            <!-- 存储路径 -->
            <div class="form-group flex flex-col">
              <label class="form-label text-sm font-medium mb-1.5" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">{{ t("file.path") }}</label>
              <input
                type="text"
                v-model="formData.path"
                class="form-input w-full rounded-md shadow-sm focus:ring-2 focus:ring-offset-1 focus:border-transparent"
                :class="[
                  darkMode
                    ? 'bg-gray-700 border-gray-600 text-white focus:ring-blue-600 focus:ring-offset-gray-800'
                    : 'bg-white border-gray-300 text-gray-900 focus:ring-blue-500 focus:ring-offset-white',
                ]"
                :placeholder="t('file.pathPlaceholder')"
                :disabled="isUploading"
              />
            </div>
          </div>
        </div>

        <!-- 分享设置表单 -->
        <div class="mt-6 border-t pt-4 w-full overflow-hidden" :class="darkMode ? 'border-gray-700' : 'border-gray-200'">
          <h3 class="text-lg font-medium mb-4" :class="darkMode ? 'text-gray-200' : 'text-gray-700'">{{ t("file.shareSettings") }}</h3>

          <!-- 使用与FileUploader相同的表单布局 -->
          <div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-5 gap-4">
            <!-- 备注 -->
            <div class="form-group flex flex-col">
              <label class="form-label text-sm font-medium mb-1.5" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">{{ t("file.remark") }}</label>
              <input
                type="text"
                v-model="formData.remark"
                class="form-input w-full rounded-md shadow-sm focus:ring-2 focus:ring-offset-1 focus:border-transparent"
                :class="[
                  darkMode
                    ? 'bg-gray-700 border-gray-600 text-white focus:ring-blue-600 focus:ring-offset-gray-800'
                    : 'bg-white border-gray-300 text-gray-900 focus:ring-blue-500 focus:ring-offset-white',
                ]"
                :placeholder="t('file.remarkPlaceholder')"
                :disabled="isUploading"
              />
            </div>

            <!-- 自定义链接 -->
            <div class="form-group flex flex-col">
              <label class="form-label text-sm font-medium mb-1.5" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">{{ t("file.customLink") }}</label>
              <input
                type="text"
                v-model="formData.slug"
                class="form-input w-full rounded-md shadow-sm focus:ring-2 focus:ring-offset-1 focus:border-transparent"
                :class="[
                  darkMode
                    ? 'bg-gray-700 border-gray-600 text-white focus:ring-blue-600 focus:ring-offset-gray-800'
                    : 'bg-white border-gray-300 text-gray-900 focus:ring-blue-500 focus:ring-offset-white',
                  slugError ? (darkMode ? 'border-red-500' : 'border-red-600') : '',
                ]"
                :placeholder="t('file.customLinkPlaceholder')"
                :disabled="isUploading"
                @input="validateCustomLink"
              />
              <p v-if="slugError" class="mt-1 text-sm text-red-600 dark:text-red-400">{{ slugError }}</p>
              <p v-else class="mt-1 text-xs text-gray-500 dark:text-gray-400">{{ t("file.onlyAllowedChars") }}</p>
            </div>

            <!-- 密码保护 -->
            <div class="form-group flex flex-col">
              <label class="form-label text-sm font-medium mb-1.5" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">{{ t("file.passwordProtection") }}</label>
              <input
                type="text"
                v-model="formData.password"
                class="form-input w-full rounded-md shadow-sm focus:ring-2 focus:ring-offset-1 focus:border-transparent"
                :class="[
                  darkMode
                    ? 'bg-gray-700 border-gray-600 text-white focus:ring-blue-600 focus:ring-offset-gray-800'
                    : 'bg-white border-gray-300 text-gray-900 focus:ring-blue-500 focus:ring-offset-white',
                ]"
                :placeholder="t('file.passwordPlaceholder')"
                :disabled="isUploading"
              />
            </div>

            <!-- 过期时间 -->
            <div class="form-group flex flex-col">
              <label class="form-label text-sm font-medium mb-1.5" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">{{ t("file.expireTime") }}</label>
              <select
                v-model="formData.expires_in"
                class="form-input w-full rounded-md shadow-sm focus:ring-2 focus:ring-offset-1 focus:border-transparent"
                :class="[
                  darkMode
                    ? 'bg-gray-700 border-gray-600 text-white focus:ring-blue-600 focus:ring-offset-gray-800'
                    : 'bg-white border-gray-300 text-gray-900 focus:ring-blue-500 focus:ring-offset-white',
                ]"
                :disabled="isUploading"
              >
                <option value="1">{{ t("file.expireOptions.hour1") }}</option>
                <option value="24">{{ t("file.expireOptions.day1") }}</option>
                <option value="168">{{ t("file.expireOptions.day7") }}</option>
                <option value="720">{{ t("file.expireOptions.day30") }}</option>
                <option value="0">{{ t("file.expireOptions.never") }}</option>
              </select>
            </div>

            <!-- 最大查看次数 -->
            <div class="form-group flex flex-col">
              <label class="form-label text-sm font-medium mb-1.5" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">{{ t("file.maxViews") }}</label>
              <input
                type="number"
                v-model.number="formData.max_views"
                min="0"
                step="1"
                pattern="\d*"
                class="form-input w-full rounded-md shadow-sm focus:ring-2 focus:ring-offset-1 focus:border-transparent"
                :class="[
                  darkMode
                    ? 'bg-gray-700 border-gray-600 text-white focus:ring-blue-600 focus:ring-offset-gray-800'
                    : 'bg-white border-gray-300 text-gray-900 focus:ring-blue-500 focus:ring-offset-white',
                ]"
                :placeholder="t('file.maxViewsPlaceholder')"
                :disabled="isUploading"
                @input="validateMaxViews"
              />
            </div>
          </div>

          <!-- 上传方式选择 -->
          <div class="mt-4">
            <label class="text-base font-medium mb-2 block" :class="darkMode ? 'text-gray-200' : 'text-gray-700'">{{ t("file.uploadMethod") }}</label>
            <div class="flex space-x-4">
              <label class="inline-flex items-center">
                <input type="radio" v-model="uploadMethod" value="presigned" class="form-radio" :disabled="isUploading" />
                <span class="ml-2" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">{{ t("file.presignedUpload") }}</span>
              </label>
              <label class="inline-flex items-center">
                <input type="radio" v-model="uploadMethod" value="multipart" class="form-radio" :disabled="isUploading" />
                <span class="ml-2" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">{{ t("file.multipartUpload") }}</span>
              </label>
            </div>
            <p class="text-xs mt-1" :class="darkMode ? 'text-gray-400' : 'text-gray-500'">
              {{ uploadMethod === "presigned" ? t("file.presignedUploadDesc") : t("file.multipartUploadDesc") }}
            </p>
          </div>

          <!-- 上传进度 -->
          <div v-if="uploadProgress > 0 && isUploading" class="mt-4">
            <div class="flex justify-between items-center mb-1">
              <div class="flex items-center">
                <span class="text-sm mr-2" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">{{ t("file.uploadProgress") }}</span>
                <span
                  class="text-xs px-2 py-0.5 rounded mr-2"
                  :class="[
                    currentStage === 'cancelled'
                      ? darkMode
                        ? 'bg-red-900/30 text-red-200'
                        : 'bg-red-100 text-red-700'
                      : darkMode
                      ? 'bg-blue-900/30 text-blue-200'
                      : 'bg-blue-100 text-blue-700',
                  ]"
                >
                  <template v-if="currentStage === 'starting'">
                    {{ t("file.starting") }}
                  </template>
                  <template v-else-if="currentStage === 'downloading'">
                    {{ t("file.downloading") }}
                  </template>
                  <template v-else-if="currentStage === 'downloading_proxy'">
                    {{ t("file.downloadingProxy") }}
                  </template>
                  <template v-else-if="currentStage === 'preparing'">
                    {{ t("file.preparing") }}
                  </template>
                  <template v-else-if="currentStage === 'initializing'">
                    {{ t("file.initializing") }}
                  </template>
                  <template v-else-if="currentStage === 'uploading'">
                    {{ t("file.uploading") }}
                  </template>
                  <template v-else-if="currentStage === 'finalizing'">
                    {{ t("file.finalizing") }}
                  </template>
                  <template v-else-if="currentStage === 'completed'">
                    {{ t("file.completed") }}
                  </template>
                  <template v-else-if="currentStage === 'cancelled'">
                    {{ t("file.cancelled") }}
                  </template>
                </span>
                <span v-if="uploadSpeed" class="text-xs px-2 py-0.5 rounded" :class="darkMode ? 'bg-gray-700 text-gray-300' : 'bg-gray-200 text-gray-700'">
                  {{ uploadSpeed }}
                </span>
              </div>
              <span class="text-sm font-medium" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">{{ uploadProgress }}%</span>
            </div>
            <div class="w-full bg-gray-200 rounded-full h-2.5 dark:bg-gray-700 overflow-hidden">
              <div
                class="h-2.5 rounded-full transition-all duration-200 ease-out relative overflow-hidden"
                :class="[currentStage === 'cancelled' ? 'bg-red-600' : uploadProgress >= 95 ? 'bg-green-600' : 'bg-blue-600']"
                :style="{ width: `${uploadProgress}%` }"
              >
                <div class="progress-stripes absolute inset-0 w-full h-full" :class="uploadProgress < 100 ? 'animate-progress-stripes' : ''"></div>
              </div>
            </div>
          </div>

          <!-- 表单按钮 -->
          <div class="submit-section mt-6 flex flex-row items-center gap-3">
            <button
              type="submit"
              :disabled="!fileInfo || !formData.s3_config_id || isUploading || loading"
              class="btn-primary px-4 py-2 rounded-md font-medium focus:outline-none focus:ring-2 focus:ring-offset-2 transition-colors flex items-center justify-center min-w-[120px]"
              :class="[
                !fileInfo || !formData.s3_config_id || isUploading || loading
                  ? darkMode
                    ? 'bg-gray-700 text-gray-400 cursor-not-allowed'
                    : 'bg-gray-200 text-gray-500 cursor-not-allowed'
                  : darkMode
                  ? 'bg-blue-600 hover:bg-blue-700 text-white focus:ring-blue-600 focus:ring-offset-gray-800'
                  : 'bg-blue-600 hover:bg-blue-700 text-white focus:ring-blue-500 focus:ring-offset-white',
              ]"
            >
              <svg v-if="isUploading" class="animate-spin -ml-1 mr-2 h-4 w-4" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
                <path
                  class="opacity-75"
                  fill="currentColor"
                  d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"
                ></path>
              </svg>
              {{ isUploading ? t("file.loading") : t("file.upload") }}
            </button>

            <!-- 将取消按钮放在上传按钮右侧 -->
            <button
              v-if="isUploading"
              type="button"
              @click="cancelUpload"
              class="px-4 py-2 rounded-md font-medium focus:outline-none focus:ring-2 focus:ring-offset-2 transition-colors flex items-center justify-center border"
              :class="
                darkMode
                  ? 'bg-gray-700 border-gray-600 text-gray-300 hover:bg-red-900/60 hover:text-red-200 hover:border-red-800 focus:ring-gray-500 focus:ring-offset-gray-800'
                  : 'bg-gray-100 border-gray-300 text-gray-700 hover:bg-red-50 hover:text-red-600 hover:border-red-300 focus:ring-gray-300 focus:ring-offset-white'
              "
            >
              <svg xmlns="http://www.w3.org/2000/svg" class="h-4 w-4 mr-1" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
              </svg>
              {{ t("file.cancel") }}
            </button>
          </div>
        </div>
      </form>
    </div>
  </div>
</template>

<script setup>
import { ref, reactive, defineProps, defineEmits, watch, computed } from "vue";
import { useI18n } from "vue-i18n";
import { api } from "@/api";
// 导入文件类型工具
import { getFileIcon } from "../../utils/fileTypeIcons";
import { formatFileSize as formatFileSizeUtil } from "@/utils/fileTypes.js";
import { validateUrlInfo } from "../../api/services/urlUploadService.js";

const { t } = useI18n(); // 初始化i18n

const props = defineProps({
  darkMode: {
    type: Boolean,
    default: false,
  },
  s3Configs: {
    type: Array,
    default: () => [],
  },
  loading: {
    type: Boolean,
    default: false,
  },
  isAdmin: {
    type: Boolean,
    default: false,
  },
});

const emit = defineEmits(["upload-success", "upload-error", "refresh-files"]);

// URL输入和状态
const urlInput = ref("");
const urlError = ref("");
const isAnalyzing = ref(false);
const fileInfo = ref(null);
const customFilename = ref("");

// 上传状态
const isUploading = ref(false);
const uploadProgress = ref(0);
const uploadSpeed = ref("");
const activeXhr = ref(null);
const lastLoaded = ref(0);
const lastTime = ref(0);
const uploadMethod = ref("presigned"); // presigned或multipart
const slugError = ref("");
const fileId = ref(null);
const uploadId = ref(null); // 添加 uploadId 状态变量
const isCancelled = ref(false); // 取消上传标志
const multipartUploader = ref(null); // 分片上传器实例
const currentStage = ref("starting"); // 添加 currentStage 状态变量

// 表单数据
const formData = reactive({
  s3_config_id: "",
  slug: "",
  path: "",
  remark: "",
  password: "",
  expires_in: "0", // 默认永不过期
  max_views: 0, // 默认无限制
});

// 计算属性：显示友好的文件大小
const displayFileSize = computed(() => {
  // 如果有明确的大小信息，使用它
  if (fileInfo.value && fileInfo.value.size !== null && fileInfo.value.size !== undefined && fileInfo.value.size > 0) {
    return formatFileSize(fileInfo.value.size);
  }

  // 如果没有大小信息，显示"未知大小"
  return t("file.unknownSize");
});

// 计算属性：显示友好的MIME类型
const displayMimeType = computed(() => {
  if (!fileInfo.value) return null;

  // 优先显示 contentType，再显示后端的 typeName
  if (fileInfo.value.contentType && fileInfo.value.contentType !== "application/octet-stream") {
    console.log("✅ 使用 contentType:", fileInfo.value.contentType);
    return fileInfo.value.contentType;
  }

  // 如果 contentType 无效，使用后端返回的 typeName
  if (fileInfo.value.typeName && fileInfo.value.typeName !== "unknown") {
    console.log("✅ 使用 typeName:", fileInfo.value.typeName);
    return fileInfo.value.typeName;
  }

  // 最后回退：不显示类型信息
  return null;
});

// 计算属性：解码并显示文件名
const displayFilename = computed(() => {
  if (!fileInfo.value || !fileInfo.value.filename) {
    return "Unknown File";
  }

  try {
    // 尝试URL解码文件名
    return decodeURIComponent(fileInfo.value.filename);
  } catch (e) {
    console.warn("解码文件名失败:", e);
    return fileInfo.value.filename; // 如果解码失败，返回原始文件名
  }
});

// 监听s3Configs变化，自动选择默认配置
watch(
  () => props.s3Configs,
  (configs) => {
    if (configs && configs.length > 0) {
      // 查找默认配置
      const defaultConfig = configs.find((config) => config.is_default);
      if (defaultConfig) {
        // 使用默认配置的ID
        formData.s3_config_id = defaultConfig.id;
      } else if (!formData.s3_config_id && configs.length > 0) {
        // 如果没有默认配置且当前未选择配置，则选择第一个
        formData.s3_config_id = configs[0].id;
      }
    }
  },
  { immediate: true } // 页面加载时立即执行
);

/**
 * 获取与文件类型匹配的SVG图标
 * @param {string} filename - 文件名
 * @returns {string} SVG图标HTML字符串
 */
const getFileIconClassLocal = (filename) => {
  if (!filename) return getDefaultFileIcon();
  const mockFileItem = {
    name: filename,
    isDirectory: false,
    type: fileInfo.value?.type || 0,
  };

  return getFileIcon(mockFileItem, props.darkMode);
};

/**
 * 获取默认文件图标
 * @returns {string} 默认文件图标SVG字符串
 */
const getDefaultFileIcon = () => {
  return `<svg xmlns="http://www.w3.org/2000/svg" class="h-full w-full" viewBox="0 0 24 24" fill="none">
    <path d="M14 2H6C5.46957 2 4.96086 2.21071 4.58579 2.58579C4.21071 2.96086 4 3.46957 4 4V20C4 20.5304 4.21071 21.0391 4.58579 21.4142C4.96086 21.7893 5.46957 22 6 22H18C18.5304 22 19.0391 21.7893 19.4142 21.4142C19.7893 21.0391 20 20.5304 20 20V8L14 2Z"
      stroke="${props.darkMode ? "#93c5fd" : "#3b82f6"}" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" fill="${
    props.darkMode ? "#93c5fd" : "#3b82f6"
  }" fill-opacity="${props.darkMode ? "0.1" : "0.1"}"/>
    <path d="M14 2V8H20" stroke="${props.darkMode ? "#93c5fd" : "#3b82f6"}" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>
  </svg>`;
};

/**
 * 解析URL获取文件信息
 */
const analyzeUrl = async () => {
  if (!urlInput.value || isAnalyzing.value || isUploading.value) return;

  // 清除之前的错误
  urlError.value = "";
  fileInfo.value = null;
  isAnalyzing.value = true;

  try {
    // 验证URL格式
    if (!isValidUrl(urlInput.value)) {
      urlError.value = t("file.messages.invalidUrl");
      isAnalyzing.value = false;
      return;
    }

    console.log("开始URL验证和增强检测:", urlInput.value);

    // 使用后端API进行URL验证和增强MIME检测
    const response = await validateUrlInfo(urlInput.value);

    if (response.success && response.data) {
      const metadata = response.data;

      // 构建兼容的文件信息对象
      const data = {
        url: metadata.url,
        filename: metadata.filename,
        contentType: metadata.enhancedContentType || metadata.contentType,
        size: metadata.size,
        lastModified: metadata.lastModified,
        corsSupported: metadata.corsSupported,
        mimetype: metadata.enhancedContentType || metadata.contentType,
        detectionMethod: metadata.detectionMethod,
        detectionConfidence: metadata.detectionConfidence,
        fileTypeLibraryUsed: metadata.fileTypeLibraryUsed,
        type: metadata.type,
        typeName: metadata.typeName,
      };

      fileInfo.value = data;

      // 显示检测信息
      if (metadata.fileTypeLibraryUsed) {
        console.log(`✅ 后端file-type检测成功: ${metadata.contentType} (置信度: ${metadata.detectionConfidence})`);
      } else {
        console.log(`📡 传统检测: ${metadata.contentType}`);
      }
    } else {
      throw new Error(response.message || "URL验证失败");
    }

    // 设置自定义文件名
    if (fileInfo.value.filename) {
      try {
        customFilename.value = decodeURIComponent(fileInfo.value.filename);
      } catch (e) {
        console.warn("解码文件名失败:", e);
        customFilename.value = fileInfo.value.filename || "";
      }
    } else {
      customFilename.value = "";
    }
  } catch (error) {
    console.error("URL验证失败:", error);
    urlError.value = error.message || t("file.messages.urlAnalysisFailed");
  } finally {
    isAnalyzing.value = false;
  }
};

/**
 * 清除文件信息
 */
const clearFileInfo = () => {
  if (isUploading.value) return;
  fileInfo.value = null;
  customFilename.value = "";
};

/**
 * 验证URL格式是否有效
 * @param {string} url - 要验证的URL
 * @returns {boolean} URL是否有效
 */
const isValidUrl = (url) => {
  try {
    const parsedUrl = new URL(url);
    return parsedUrl.protocol === "http:" || parsedUrl.protocol === "https:";
  } catch (e) {
    return false;
  }
};

/**
 * 格式化文件大小
 * @param {number} bytes - 文件字节数
 * @returns {string} 格式化后的文件大小
 */
const formatFileSize = (bytes) => {
  return formatFileSizeUtil(bytes);
};

/**
 * 格式化上传速度
 * @param {number} bytesPerSecond - 每秒字节数
 * @returns {string} 格式化后的上传速度
 */
const formatSpeed = (bytesPerSecond) => {
  if (bytesPerSecond < 1024) {
    return `${Math.round(bytesPerSecond)} B/s`;
  } else if (bytesPerSecond < 1024 * 1024) {
    return `${Math.round((bytesPerSecond / 1024) * 10) / 10} KB/s`;
  } else {
    return `${Math.round((bytesPerSecond / (1024 * 1024)) * 10) / 10} MB/s`;
  }
};

/**
 * 清理文件的所有上传相关引用和状态
 * @param {Object} fileItem - 文件项对象
 */
const cleanupFileUploadReferences = (fileItem) => {
  // 清理所有可能的上传引用
  if (fileItem.multipartUploader) {
    try {
      fileItem.multipartUploader.abort();
    } catch (error) {
      console.warn("清理multipartUploader时出错:", error);
    }
    fileItem.multipartUploader = null;
  }

  if (fileItem.xhr) {
    try {
      fileItem.xhr.abort();
    } catch (error) {
      console.warn("清理xhr时出错:", error);
    }
    fileItem.xhr = null;
  }

  // 清理其他上传相关状态
  fileItem.uploadStartTime = null;
  fileItem.lastProgressTime = null;
};

/**
 * 验证自定义链接
 * @returns {boolean} 验证是否通过
 */
const validateCustomLink = () => {
  slugError.value = "";

  // 如果为空则不验证（使用随机生成的slug）
  if (!formData.slug) {
    return true;
  }

  // 验证格式：只允许字母、数字、连字符、下划线、点号
  const slugRegex = /^[a-zA-Z0-9._-]+$/;
  if (!slugRegex.test(formData.slug)) {
    slugError.value = t("file.messages.slugInvalid");
    return false;
  }

  return true;
};

/**
 * 验证并处理可打开次数的输入
 * 确保输入的是有效的非负整数
 * @param {Event} event - 输入事件对象
 */
const validateMaxViews = (event) => {
  // 获取输入的值
  const value = event.target.value;

  // 如果是负数，则设置为0
  if (value < 0) {
    formData.max_views = 0;
    return;
  }

  // 如果包含小数点，截取整数部分
  if (value.toString().includes(".")) {
    formData.max_views = parseInt(value);
  }

  // 确保值为有效数字
  if (isNaN(value) || value === "") {
    formData.max_views = 0;
  } else {
    // 确保是整数
    formData.max_views = parseInt(value);
  }
};

/**
 * 提交URL上传
 * 根据选择的上传方式（预签名直传或分片上传）执行相应的上传逻辑
 */
const submitUpload = async () => {
  if (!fileInfo.value || !formData.s3_config_id || isUploading.value) return;

  // 验证可打开次数，确保是非负整数
  if (formData.max_views < 0) {
    emit("upload-error", new Error(t("file.messages.negativeMaxViews")));
    return;
  }

  // 验证自定义链接格式
  if (formData.slug && !validateCustomLink()) {
    emit("upload-error", new Error(slugError.value));
    return;
  }

  // 处理默认备注格式
  // 如果用户没有输入备注，则设置默认格式为"[url直链]URL地址"
  if (!formData.remark) {
    // 截取URL，如果太长则截断
    const maxUrlLength = 100;
    const shortUrl = urlInput.value.length > maxUrlLength ? urlInput.value.substring(0, maxUrlLength) + "..." : urlInput.value;

    formData.remark = `[${t("file.urlUpload.urlUpload")}]${shortUrl}`;
  }

  isUploading.value = true;
  uploadProgress.value = 0;
  uploadSpeed.value = "";

  // 重置上传速度计算相关变量
  lastLoaded.value = 0;
  lastTime.value = Date.now();

  try {
    // 根据上传方式选择不同的上传策略
    if (uploadMethod.value === "presigned") {
      await presignedDirectUpload();
    } else {
      await chunkedMultipartUpload();
    }

    // 上传成功，通知父组件
    emit("upload-success", {
      message: t("file.urlUploadSuccess"),
      url: urlInput.value,
      fileInfo: fileInfo.value,
    });

    // 刷新文件列表
    emit("refresh-files");

    // 重置表单
    resetForm();
  } catch (error) {
    console.error("URL上传失败:", error);
    emit("upload-error", error);
  } finally {
    isUploading.value = false;
  }
};

/**
 * 预签名直传方式
 * 先从URL获取内容，然后使用预签名URL上传到S3
 */
const presignedDirectUpload = async () => {
  try {
    // 重置取消标志
    isCancelled.value = false;

    // 设置初始阶段和进度
    currentStage.value = "starting";
    uploadProgress.value = 5;

    // 1. 获取预签名URL
    currentStage.value = "initializing";

    const presignedResponse = await api.urlUpload.getUrlUploadPresignedUrl({
      url: urlInput.value,
      s3_config_id: formData.s3_config_id,
      filename: customFilename.value || fileInfo.value.filename,
      slug: formData.slug,
      remark: formData.remark,
      path: formData.path,
      password: formData.password,
      expires_in: Number(formData.expires_in),
      max_views: formData.max_views,
      metadata: {
        source_url: urlInput.value,
      },
    });

    if (!presignedResponse.success || !presignedResponse.data) {
      throw new Error(t("file.messages.getPresignedUrlFailed"));
    }

    // 保存文件ID
    fileId.value = presignedResponse.data.file_id;

    // 如果已经取消，则中止上传
    if (isCancelled.value) {
      throw new Error(t("file.messages.uploadCancelled"));
    }

    uploadProgress.value = 10;

    // 2. 使用预签名URL上传文件
    currentStage.value = "uploading";

    const uploadResult = await api.urlUpload.uploadFromUrlToS3({
      url: urlInput.value,
      uploadUrl: presignedResponse.data.upload_url,
      onProgress: (progress, loaded, _total, phase) => {
        // 如果已取消，不再更新进度
        if (isCancelled.value) return;

        uploadProgress.value = progress;

        // 根据phase更新阶段
        if (phase === "downloading") {
          currentStage.value = "downloading";
        } else if (phase === "uploading") {
          currentStage.value = "uploading";
        }

        // 更新上传速度
        const now = Date.now();
        const timeElapsed = (now - lastTime.value) / 1000; // 转换为秒

        if (timeElapsed > 0.5) {
          // 每0.5秒更新一次速度
          const loadedChange = loaded - lastLoaded.value; // 这段时间内上传的字节数
          const speed = loadedChange / timeElapsed; // 字节/秒

          uploadSpeed.value = formatSpeed(speed);

          // 更新上次加载值和时间
          lastLoaded.value = loaded;
          lastTime.value = now;
        }
      },
      // 传递xhr引用的设置函数，以便能够取消请求
      setXhr: (xhr) => {
        activeXhr.value = xhr;
      },
    });

    // 如果已经取消，则中止上传
    if (isCancelled.value) {
      throw new Error(t("file.messages.uploadCancelled"));
    }

    // 3. 提交完成信息
    currentStage.value = "finalizing";

    await api.urlUpload.commitUrlUpload({
      file_id: fileId.value,
      etag: uploadResult.etag,
      size: uploadResult.size,
      remark: formData.remark,
      password: formData.password,
      expires_in: Number(formData.expires_in),
      max_views: formData.max_views,
      slug: formData.slug,
      path: formData.path,
    });

    // 完成
    currentStage.value = "completed";
    uploadProgress.value = 100;
    uploadSpeed.value = "";

    return true;
  } catch (error) {
    console.error("客户端URL上传失败:", error);

    // 如果已经获取了文件ID，尝试取消并清理文件记录
    if (fileId.value) {
      try {
        await api.urlUpload.cancelUrlUpload(fileId.value);
        console.log(`已清理未完成的上传记录: ${fileId.value}`);
      } catch (cancelError) {
        console.error("清理未完成的上传记录失败:", cancelError);
      }
      // 重置fileId
      fileId.value = null;
    }

    throw error;
  }
};

/**
 * 分片上传方式
 * 先从URL获取内容，然后使用分片上传到S3
 */
const chunkedMultipartUpload = async () => {
  try {
    // 重置取消标志
    isCancelled.value = false;

    // 设置初始阶段和进度
    currentStage.value = "starting";
    uploadProgress.value = 5;

    // 步骤1: 下载文件内容 (5% -> 40%)
    currentStage.value = "downloading";

    // 重置上传速度计算相关变量
    lastLoaded.value = 0;
    lastTime.value = Date.now();

    const blob = await api.urlUpload.fetchUrlContent({
      url: urlInput.value,
      onProgress: (progress, loaded, _total, _phase, phaseType) => {
        if (isCancelled.value) return;

        // 下载阶段占总进度的35%（从5%到40%）
        const downloadProgress = 5 + Math.round((progress / 49) * 35);
        uploadProgress.value = downloadProgress;

        // 如果是使用代理的方式，更新阶段状态
        if (phaseType === "proxyDownload") {
          currentStage.value = "downloading_proxy";
        }

        // 计算下载速度
        const now = Date.now();
        const timeElapsed = (now - lastTime.value) / 1000; // 转换为秒

        if (timeElapsed > 0.5) {
          // 每0.5秒更新一次速度
          const loadedChange = loaded - lastLoaded.value;
          const speed = loadedChange / timeElapsed; // 字节/秒
          uploadSpeed.value = formatSpeed(speed);

          // 更新上次加载值和时间
          lastLoaded.value = loaded;
          lastTime.value = now;
        }
      },
      setXhr: (xhr) => {
        activeXhr.value = xhr; // 保存xhr引用，以便能够取消下载
      },
    });

    if (isCancelled.value) {
      throw new Error(t("file.messages.uploadCancelled"));
    }

    uploadProgress.value = 40;

    // 步骤2: 准备上传配置
    const uploadConfig = {
      s3_config_id: formData.s3_config_id,
      filename: customFilename.value || fileInfo.value.filename,
      slug: formData.slug,
      remark: formData.remark,
      password: formData.password,
      expires_in: Number(formData.expires_in),
      max_views: formData.max_views,
      path: formData.path,
    };

    // 步骤3: 执行分片上传 (40% -> 100%)
    currentStage.value = "uploading";

    const result = await api.urlUpload.performUrlMultipartUpload(urlInput.value, blob, uploadConfig, {
      onProgress: (progress, _uploadedBytes, _totalBytes, speed) => {
        // 上传占总进度的60%，40%~100%之间
        const adjustedProgress = 40 + Math.round(progress * 0.6);
        uploadProgress.value = Math.min(adjustedProgress, 100);

        // 更新上传速度
        if (speed > 0) {
          uploadSpeed.value = formatSpeed(speed);
        }
      },
      onCancel: () => isCancelled.value,
      onXhrCreated: (uploaderRef) => {
        // 保存上传器引用，用于取消操作
        multipartUploader.value = uploaderRef;
      },
      onError: (error, partNumber) => {
        console.error(`分片 ${partNumber || "未知"} 上传失败:`, error);
      },
    });

    if (!result.success) {
      throw new Error(result.message || "分片上传失败");
    }

    // 完成
    currentStage.value = "completed";
    uploadProgress.value = 100;
    uploadSpeed.value = "";

    return true;
  } catch (error) {
    console.error("分片上传失败:", error);
    throw error;
  }
};

/**
 * 取消上传
 */
const cancelUpload = async () => {
  if (!isUploading.value) return;

  // 设置取消标志
  isCancelled.value = true;

  // 设置取消状态
  currentStage.value = "cancelled";

  // 使用统一的清理函数清理所有上传引用
  const tempFileItem = {
    multipartUploader: multipartUploader.value,
    xhr: activeXhr.value,
  };
  cleanupFileUploadReferences(tempFileItem);

  // 更新引用
  multipartUploader.value = null;
  activeXhr.value = null;

  // 如果已经有文件ID和uploadId，尝试通知服务器取消上传
  if (fileId.value && uploadId.value) {
    try {
      if (uploadMethod.value === "multipart") {
        await api.urlUpload.abortMultipartUpload(fileId.value, uploadId.value);
      }
      // 预签名直传方式不需要额外的取消操作，文件会在提交阶段自动被覆盖
    } catch (error) {
      console.error("取消上传失败:", error);
    }
  }
  // 如果只有文件ID但没有uploadId（预签名直传方式），尝试清理文件记录
  else if (fileId.value && !uploadId.value) {
    try {
      await api.urlUpload.cancelUrlUpload(fileId.value);
      console.log(`已清理未完成的上传记录: ${fileId.value}`);
    } catch (cancelError) {
      console.error("清理未完成的上传记录失败:", cancelError);
    }
  }

  isUploading.value = false;
  uploadProgress.value = 0;
  uploadSpeed.value = "";
  multipartUploader.value = null;
  uploadId.value = null; // 重置 uploadId
  fileId.value = null; // 重置文件ID

  emit("upload-error", new Error(t("file.uploadCancelled")));
};

/**
 * 重置表单
 */
const resetForm = () => {
  urlInput.value = "";
  fileInfo.value = null;
  customFilename.value = "";
  uploadProgress.value = 0;
  uploadSpeed.value = "";
  fileId.value = null;
  uploadId.value = null; // 重置 uploadId
  urlError.value = "";
  isCancelled.value = false;
  currentStage.value = ""; // 重置上传阶段

  // 使用统一的清理函数清理所有上传引用
  const tempFileItem = {
    multipartUploader: multipartUploader.value,
    xhr: activeXhr.value,
  };
  cleanupFileUploadReferences(tempFileItem);

  multipartUploader.value = null;
  activeXhr.value = null;

  // 保留S3配置ID，重置其他表单字段
  const s3ConfigId = formData.s3_config_id;
  Object.assign(formData, {
    s3_config_id: s3ConfigId,
    slug: "",
    path: "",
    remark: "",
    password: "",
    expires_in: "0",
    max_views: 0,
  });
};
</script>

<style scoped>
.upload-form {
  width: 100%;
}

.progress-stripes {
  background-image: linear-gradient(
    45deg,
    rgba(255, 255, 255, 0.15) 25%,
    transparent 25%,
    transparent 50%,
    rgba(255, 255, 255, 0.15) 50%,
    rgba(255, 255, 255, 0.15) 75%,
    transparent 75%,
    transparent
  );
  background-size: 1rem 1rem;
}

.animate-progress-stripes {
  animation: progress-stripes 1s linear infinite;
}

@keyframes progress-stripes {
  from {
    background-position: 1rem 0;
  }
  to {
    background-position: 0 0;
  }
}

.form-input:disabled,
.btn-primary:disabled {
  cursor: not-allowed;
  opacity: 0.7;
}

.input-wrapper {
  position: relative;
  z-index: 1;
  transition: all 0.3s ease;
}

.input-wrapper:hover {
  transform: translateY(-1px);
}

.input-wrapper:focus-within {
  transform: translateY(-1px);
}

.url-file-preview {
  position: relative;
}

@keyframes fadeIn {
  from {
    opacity: 0;
    transform: translateY(10px);
  }
  to {
    opacity: 1;
    transform: translateY(0);
  }
}

.animate-fadeIn {
  animation: fadeIn 0.3s ease-out forwards;
}

/* 在暗模式下增强视觉对比度 */
.dark .url-input-zone {
  box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06);
}

.dark .file-item {
  box-shadow: 0 1px 3px 0 rgba(0, 0, 0, 0.1), 0 1px 2px 0 rgba(0, 0, 0, 0.06);
}
</style>
