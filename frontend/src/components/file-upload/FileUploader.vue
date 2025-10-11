<template>
  <div class="file-uploader">
    <!-- 上传类型选项卡 -->
    <div class="upload-tabs mb-4">
      <div class="flex border-b" :class="darkMode ? 'border-gray-700' : 'border-gray-200'">
        <button
          @click="activeTab = 'file'"
          class="py-2 px-4 font-medium text-sm focus:outline-none transition-colors duration-200"
          :class="[
            activeTab === 'file'
              ? darkMode
                ? 'text-blue-400 border-b-2 border-blue-400'
                : 'text-blue-600 border-b-2 border-blue-600'
              : darkMode
              ? 'text-gray-400 hover:text-gray-300'
              : 'text-gray-500 hover:text-gray-700',
          ]"
        >
          <div class="flex items-center">
            <svg xmlns="http://www.w3.org/2000/svg" class="h-4 w-4 mr-1.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path
                stroke-linecap="round"
                stroke-linejoin="round"
                stroke-width="2"
                d="M9 13h6m-3-3v6m5 5H7a2 2 0 01-2-2V5a2 2 0 012-2h7.586a1 1 0 01.707.293l4.414 4.414a1 1 0 01.293.707V19a2 2 0 01-2 2z"
              />
            </svg>
            {{ t("file.uploadTabs.fileUpload") }}
          </div>
        </button>
        <button
          @click="activeTab = 'url'"
          class="py-2 px-4 font-medium text-sm focus:outline-none transition-colors duration-200"
          :class="[
            activeTab === 'url'
              ? darkMode
                ? 'text-blue-400 border-b-2 border-blue-400'
                : 'text-blue-600 border-b-2 border-blue-600'
              : darkMode
              ? 'text-gray-400 hover:text-gray-300'
              : 'text-gray-500 hover:text-gray-700',
          ]"
        >
          <div class="flex items-center">
            <svg xmlns="http://www.w3.org/2000/svg" class="h-4 w-4 mr-1.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13.828 10.172a4 4 0 00-5.656 0l-4 4a4 4 0 105.656 5.656l1.102-1.101" />
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10.172 13.828a4 4 0 015.656 0l4 4a4 4 0 01-5.656 5.656l-1.102-1.101" />
            </svg>
            {{ t("file.uploadTabs.urlUpload") }}
          </div>
        </button>
      </div>
    </div>

    <!-- 文件上传界面 -->
    <div v-if="activeTab === 'file'">
      <!-- 文件拖放区域 -->
      <div
        class="drop-zone mb-4 border-2 border-dashed rounded-lg flex flex-col items-center justify-center py-6 px-4 cursor-pointer transition-all duration-300"
        :class="[
          darkMode ? 'border-gray-600 hover:border-gray-500 bg-gray-800/30' : 'border-gray-300 hover:border-gray-400 bg-gray-50',
          isDragging ? (darkMode ? 'border-blue-500 bg-blue-500/10 pulsing-border' : 'border-blue-500 bg-blue-50 pulsing-border') : '',
        ]"
        @dragenter.prevent="onDragOver"
        @dragover.prevent="onDragOver"
        @dragleave.prevent="onDragLeave"
        @drop.prevent="onDrop"
        @click="triggerFileInput"
      >
        <div class="icon-container mb-3">
          <svg
            xmlns="http://www.w3.org/2000/svg"
            class="h-12 w-12 transition-colors duration-300"
            :class="[darkMode ? 'text-gray-400' : 'text-gray-500', isDragging ? (darkMode ? 'text-blue-400' : 'text-blue-500') : '']"
            fill="none"
            viewBox="0 0 24 24"
            stroke="currentColor"
          >
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.5" d="M7 16a4 4 0 01-.88-7.903A5 5 0 1115.9 6L16 6a5 5 0 011 9.9M15 13l-3-3m0 0l-3 3m3-3v12" />
          </svg>
        </div>
        <div class="text-center">
          <p
            class="text-base font-medium transition-colors duration-300"
            :class="[darkMode ? 'text-gray-300' : 'text-gray-700', isDragging ? (darkMode ? 'text-blue-300' : 'text-blue-700') : '']"
          >
            {{ isDragging ? t("file.drag") : t("file.select") }}
          </p>
          <p
            class="text-sm mt-1 transition-colors duration-300"
            :class="[darkMode ? 'text-gray-400' : 'text-gray-500', isDragging ? (darkMode ? 'text-blue-300/80' : 'text-blue-600/80') : '']"
          >
            {{ t("file.maxSizeExceeded", { size: formatMaxFileSize() }) }}
          </p>
          <p
            class="text-sm mt-1 transition-colors duration-300"
            :class="[darkMode ? 'text-gray-400' : 'text-gray-500', isDragging ? (darkMode ? 'text-blue-300/80' : 'text-blue-600/80') : '']"
          >
            <span class="px-1.5 py-0.5 rounded text-xs" :class="darkMode ? 'bg-gray-700 text-blue-300' : 'bg-gray-200 text-blue-600'">
              {{ t("file.multipleFilesSupported") }}
            </span>
          </p>
        </div>
        <input ref="fileInput" type="file" class="hidden" multiple @change="onFileSelected" />
      </div>

      <!-- 已选文件预览 -->
      <div v-if="selectedFiles.length > 0" class="selected-files mb-6">
        <div class="files-header flex justify-between items-center mb-3">
          <h3 class="text-base font-medium" :class="darkMode ? 'text-gray-200' : 'text-gray-700'">
            {{ t("file.selectedFiles", { count: selectedFiles.length }) }}
          </h3>
          <button
            type="button"
            @click="clearAllFiles"
            class="text-sm px-2 py-1 rounded transition-colors flex items-center"
            :class="darkMode ? 'bg-gray-700 hover:bg-gray-600 text-gray-300' : 'bg-gray-100 hover:bg-gray-200 text-gray-600'"
          >
            <svg xmlns="http://www.w3.org/2000/svg" class="h-4 w-4 mr-1" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path
                stroke-linecap="round"
                stroke-linejoin="round"
                stroke-width="2"
                d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16"
              />
            </svg>
            {{ t("file.clearAll") }}
          </button>
        </div>
        <div class="files-list max-h-60 overflow-y-auto">
          <div
            v-for="(file, index) in selectedFiles"
            :key="index"
            class="selected-file mb-3 flex items-center p-3 rounded-md"
            :class="[
              darkMode ? 'bg-gray-700/50' : 'bg-gray-100',
              fileItems[index]?.status === 'error' ? (darkMode ? 'border-l-4 border-red-500' : 'border-l-4 border-red-500') : '',
              fileItems[index]?.status === 'success' ? (darkMode ? 'border-l-4 border-green-500' : 'border-l-4 border-green-500') : '',
              fileItems[index]?.status === 'uploading' ? (darkMode ? 'border-l-4 border-blue-500' : 'border-l-4 border-blue-500') : '',
            ]"
          >
            <div class="file-icon mr-3">
              <svg xmlns="http://www.w3.org/2000/svg" class="h-8 w-8" :class="darkMode ? 'text-gray-300' : 'text-gray-600'" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path
                  stroke-linecap="round"
                  stroke-linejoin="round"
                  stroke-width="2"
                  d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z"
                />
              </svg>
            </div>
            <div class="file-info flex-grow mr-3">
              <div class="font-medium truncate" :class="darkMode ? 'text-white' : 'text-gray-900'">
                {{ file.name }}
              </div>
              <div class="flex justify-between">
                <span class="text-sm" :class="darkMode ? 'text-gray-400' : 'text-gray-500'">
                  {{ formatFileSize(file.size) }}
                </span>

                <!-- 文件状态显示 -->
                <span
                  v-if="fileItems[index]"
                  class="text-xs ml-2 px-2 py-0.5 rounded-full"
                  :class="{
                    'bg-yellow-100 text-yellow-800 dark:bg-yellow-900/30 dark:text-yellow-300': fileItems[index].status === 'pending',
                    'bg-blue-100 text-blue-800 dark:bg-blue-900/30 dark:text-blue-300': fileItems[index].status === 'uploading',
                    'bg-green-100 text-green-800 dark:bg-green-900/30 dark:text-green-300': fileItems[index].status === 'success',
                    'bg-red-100 text-red-800 dark:bg-red-900/30 dark:text-red-300': fileItems[index].status === 'error',
                  }"
                >
                  {{
                    fileItems[index].status === "pending"
                      ? t("file.pending")
                      : fileItems[index].status === "uploading"
                      ? `${fileItems[index].progress}%`
                      : fileItems[index].status === "success"
                      ? t("file.success")
                      : fileItems[index].status === "error"
                      ? t("file.error")
                      : ""
                  }}
                </span>
              </div>

              <!-- 单个文件进度条 -->
              <div v-if="fileItems[index]?.status === 'uploading'" class="w-full bg-gray-200 rounded-full h-1.5 mt-1 dark:bg-gray-700">
                <div
                  class="h-1.5 rounded-full transition-all duration-200"
                  :class="fileItems[index].progress >= 95 ? 'bg-green-500' : 'bg-blue-500'"
                  :style="{ width: `${fileItems[index].progress}%` }"
                ></div>
              </div>
            </div>
            <!-- 取消上传按钮，仅在上传状态显示 -->
            <button
              v-if="fileItems[index]?.status === 'uploading'"
              type="button"
              @click="cancelSingleUpload(index)"
              class="p-1 rounded-full hover:bg-opacity-20 transition-colors mr-1"
              :class="darkMode ? 'hover:bg-red-900/60 text-gray-400 hover:text-red-300' : 'hover:bg-red-100 text-gray-500 hover:text-red-500'"
              :title="t('file.cancelUpload')"
            >
              <svg xmlns="http://www.w3.org/2000/svg" class="h-5 w-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 19V5M5 12l7-7 7 7" />
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 4L20 20" stroke="red" />
              </svg>
            </button>
            <!-- 重试按钮，仅在错误状态显示 -->
            <button
              v-if="fileItems[index]?.status === 'error'"
              type="button"
              @click="retryUpload(index)"
              class="p-1 rounded-full hover:bg-opacity-20 transition-colors mr-1"
              :class="darkMode ? 'hover:bg-gray-600 text-gray-400' : 'hover:bg-gray-200 text-gray-500'"
              :title="t('file.retry')"
            >
              <svg xmlns="http://www.w3.org/2000/svg" class="h-5 w-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path
                  stroke-linecap="round"
                  stroke-linejoin="round"
                  stroke-width="2"
                  d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"
                />
              </svg>
            </button>
            <button
              type="button"
              @click="clearSelectedFile(index)"
              class="p-1 rounded-full hover:bg-opacity-20 transition-colors"
              :class="darkMode ? 'hover:bg-gray-600 text-gray-400' : 'hover:bg-gray-200 text-gray-500'"
              :title="t('file.clearSelected')"
              :disabled="fileItems[index]?.status === 'uploading'"
            >
              <svg xmlns="http://www.w3.org/2000/svg" class="h-5 w-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
              </svg>
            </button>
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

            <!-- 更改这里的布局，使用更灵活的网格系统，每个栏位使用相同的模式 -->
            <div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-5 gap-4">
              <!-- 每个表单组都使用一致的结构，增强布局的一致性 -->

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

            <!-- 上传进度 -->
            <div v-if="totalProgress > 0 && isUploading" class="mt-4">
              <div class="flex justify-between items-center mb-1">
                <div class="flex items-center">
                  <span class="text-sm mr-2" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">{{ t("file.uploadProgress") }}</span>
                  <span v-if="uploadSpeed" class="text-xs px-2 py-0.5 rounded" :class="darkMode ? 'bg-blue-900/30 text-blue-200' : 'bg-blue-100 text-blue-700'">
                    {{ uploadSpeed }}
                  </span>
                </div>
                <span class="text-sm font-medium" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">{{ totalProgress }}%</span>
              </div>
              <div class="w-full bg-gray-200 rounded-full h-2.5 dark:bg-gray-700 overflow-hidden">
                <div
                  class="h-2.5 rounded-full transition-all duration-200 ease-out relative overflow-hidden"
                  :class="totalProgress >= 95 ? 'bg-green-600' : 'bg-blue-600'"
                  :style="{ width: `${totalProgress}%` }"
                >
                  <div class="progress-stripes absolute inset-0 w-full h-full" :class="totalProgress < 100 ? 'animate-progress-stripes' : ''"></div>
                </div>
              </div>
            </div>

            <!-- 表单按钮 -->
            <div class="submit-section mt-6 flex flex-row items-center gap-3">
              <button
                type="submit"
                :disabled="selectedFiles.length === 0 || !formData.s3_config_id || isUploading || loading"
                class="btn-primary px-4 py-2 rounded-md font-medium focus:outline-none focus:ring-2 focus:ring-offset-2 transition-colors flex items-center justify-center min-w-[120px]"
                :class="[
                  selectedFiles.length === 0 || !formData.s3_config_id || isUploading || loading
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

    <!-- URL上传界面 -->
    <div v-else>
      <UrlUploader
        :darkMode="darkMode"
        :s3Configs="s3Configs"
        :loading="loading"
        :isAdmin="isAdmin"
        @upload-success="$emit('upload-success', $event)"
        @upload-error="$emit('upload-error', $event)"
        @refresh-files="$emit('refresh-files')"
      />
    </div>
  </div>
</template>

<script setup>
import { ref, reactive, defineProps, defineEmits, getCurrentInstance, onMounted, onUnmounted, watch } from "vue";
import { api } from "@/api";
import { API_BASE_URL } from "../../api/config"; // 导入API_BASE_URL
import { useI18n } from "vue-i18n"; // 导入i18n
import UrlUploader from "./UrlUploader.vue"; // 导入URL上传组件

const { t } = useI18n(); // 初始化i18n

// 添加选项卡状态
const activeTab = ref("file"); // 默认选择文件上传

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

// 最大文件大小限制(MB)
const maxFileSizeMB = ref(100); // 默认值

// 拖拽状态
const isDragging = ref(false);
const fileInput = ref(null);
const selectedFiles = ref([]);
const fileItems = ref([]);

// 上传状态
const isUploading = ref(false);
const uploadProgress = ref(0);
const uploadSpeed = ref("");
const message = ref(null);
const activeXhr = ref(null);
const lastLoaded = ref(0);
const lastTime = ref(0);
const slugError = ref(""); // 添加slug错误状态
const currentUploadIndex = ref(-1);
const totalProgress = ref(0);

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

// 当前上传文件的ID
const currentFileId = ref(null);

// 组件挂载时获取最大上传大小并添加粘贴事件监听
onMounted(async () => {
  try {
    const size = await api.file.getMaxUploadSize();
    maxFileSizeMB.value = size;
  } catch (error) {
    console.error("获取最大上传大小失败:", error);
    // 失败时保持默认值
  }

  // 添加粘贴事件监听
  document.addEventListener("paste", handlePaste);
});

// 组件卸载时移除粘贴事件监听
onUnmounted(() => {
  document.removeEventListener("paste", handlePaste);
});

// 处理粘贴事件
const handlePaste = (event) => {
  if (event.clipboardData && event.clipboardData.files.length > 0) {
    event.preventDefault();

    // 处理所有粘贴的文件
    Array.from(event.clipboardData.files).forEach((file) => {
      // 验证文件大小是否超过限制
      if (file.size > maxFileSizeMB.value * 1024 * 1024) {
        message.value = {
          type: "error",
          content: t("file.maxSizeExceeded", { size: maxFileSizeMB.value }),
        };
        // 触发错误事件，让父组件处理消息显示
        emit("upload-error", new Error(t("file.maxSizeExceeded", { size: formatMaxFileSize() })));
        return;
      }

      // 检查文件是否已经在列表中（基于名称和大小）
      const isFileAlreadyAdded = selectedFiles.value.some((existingFile) => existingFile.name === file.name && existingFile.size === file.size);

      if (!isFileAlreadyAdded) {
        selectedFiles.value.push(file);

        // 初始化文件项状态
        fileItems.value.push({
          file,
          progress: 0,
          status: "pending", // pending, uploading, success, error
          message: "",
          fileId: null,
          xhr: null,
        });
      }
    });
  }
};

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

// 拖拽处理函数
const onDragOver = () => {
  isDragging.value = true;
};

const onDragLeave = (event) => {
  // 只有当鼠标离开拖拽区域而不是其内部元素时才重置
  const rect = event.currentTarget.getBoundingClientRect();
  const x = event.clientX;
  const y = event.clientY;

  // 判断鼠标是否真的离开了整个区域
  if (x <= rect.left || x >= rect.right || y <= rect.top || y >= rect.bottom) {
    isDragging.value = false;
  }
};

const onDrop = (event) => {
  isDragging.value = false;
  if (event.dataTransfer.files.length > 0) {
    // 处理所有拖放的文件
    Array.from(event.dataTransfer.files).forEach((file) => {
      // 验证文件大小是否超过限制
      if (file.size > maxFileSizeMB.value * 1024 * 1024) {
        message.value = {
          type: "error",
          content: t("file.maxSizeExceeded", { size: maxFileSizeMB.value }),
        };
        // 触发错误事件，让父组件处理消息显示
        emit("upload-error", new Error(t("file.maxSizeExceeded", { size: formatMaxFileSize() })));
        return;
      }

      // 检查文件是否已经在列表中（基于名称和大小）
      const isFileAlreadyAdded = selectedFiles.value.some((existingFile) => existingFile.name === file.name && existingFile.size === file.size);

      if (!isFileAlreadyAdded) {
        selectedFiles.value.push(file);

        // 初始化文件项状态
        fileItems.value.push({
          file,
          progress: 0,
          status: "pending", // pending, uploading, success, error
          message: "",
          fileId: null,
          xhr: null,
        });
      }
    });
  }
};

// 点击选择文件
const triggerFileInput = () => {
  fileInput.value.click();
};

const onFileSelected = (event) => {
  if (event.target.files.length > 0) {
    // 处理所有选择的文件
    Array.from(event.target.files).forEach((file) => {
      // 验证文件大小是否超过限制
      if (file.size > maxFileSizeMB.value * 1024 * 1024) {
        message.value = {
          type: "error",
          content: t("file.maxSizeExceeded", { size: maxFileSizeMB.value }),
        };
        // 触发错误事件，让父组件处理消息显示
        emit("upload-error", new Error(t("file.maxSizeExceeded", { size: formatMaxFileSize() })));
        return;
      }

      // 检查文件是否已经在列表中（基于名称和大小）
      const isFileAlreadyAdded = selectedFiles.value.some((existingFile) => existingFile.name === file.name && existingFile.size === file.size);

      if (!isFileAlreadyAdded) {
        selectedFiles.value.push(file);

        // 初始化文件项状态
        fileItems.value.push({
          file,
          progress: 0,
          status: "pending", // pending, uploading, success, error
          message: "",
          fileId: null,
          xhr: null,
        });
      }
    });
  }
};

// 清除已选文件
const clearSelectedFile = (index) => {
  // 检查文件是否正在上传
  if (fileItems.value[index]?.status === "uploading") {
    return; // 不允许删除正在上传的文件
  }

  // 从数组中移除文件
  selectedFiles.value.splice(index, 1);
  fileItems.value.splice(index, 1);

  // 重置文件输入框，以便重新选择同一文件
  if (fileInput.value && selectedFiles.value.length === 0) {
    fileInput.value.value = "";
  }
};

// 清除所有文件
const clearAllFiles = () => {
  // 过滤出非上传中的文件索引
  const indicesToRemove = fileItems.value
    .map((item, index) => (item.status !== "uploading" ? index : -1))
    .filter((index) => index !== -1)
    .sort((a, b) => b - a); // 倒序排列以便从后向前删除

  // 从后向前删除文件
  for (const index of indicesToRemove) {
    selectedFiles.value.splice(index, 1);
    fileItems.value.splice(index, 1);
  }

  // 重置文件输入框，以便重新选择同一文件
  if (fileInput.value && selectedFiles.value.length === 0) {
    fileInput.value.value = "";
  }
};

// 格式化最大文件大小
const formatMaxFileSize = () => {
  // 将MB转换为字节
  const sizeInBytes = maxFileSizeMB.value * 1024 * 1024;
  return formatFileSize(sizeInBytes);
};

// 导入统一的工具函数
import { formatFileSize } from "@/utils/fileUtils.js";

// 取消上传
const cancelUpload = () => {
  if (isUploading.value) {
    // 取消所有待上传和正在上传的文件
    for (let i = 0; i < fileItems.value.length; i++) {
      const fileItem = fileItems.value[i];

      // 只处理"正在上传"或"等待上传"的文件
      if (fileItem.status === "uploading" || fileItem.status === "pending") {
        // 如果有活动的XHR请求，取消它
        if (fileItem.xhr) {
          fileItem.xhr.abort();
        }

        // 更新文件状态为取消
        fileItem.status = "error";
        fileItem.message = t("file.messages.uploadCancelled");

        // 如果已获取了文件ID，则删除相应的文件记录
        if (fileItem.fileId) {
          // 使用统一的批量删除API
          api.file
            .batchDeleteFiles([fileItem.fileId])
            .then(() => {
              console.log("已成功删除被取消的文件记录", fileItem.fileId);
            })
            .catch((error) => {
              console.error("删除被取消的文件记录失败:", error);
            });
        }
      }
    }

    // 重置上传状态
    isUploading.value = false;
    uploadProgress.value = 0;
    totalProgress.value = 0;
    uploadSpeed.value = "";
    currentUploadIndex.value = -1;

    message.value = {
      type: "error",
      content: t("file.messages.uploadCancelled"),
    };

    // 触发错误事件，让父组件处理消息显示
    emit("upload-error", new Error(t("file.messages.uploadCancelled")));
  }
};

// 验证自定义链接后缀格式
const validateCustomLink = () => {
  // 清除现有错误
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

// 上传文件
const submitUpload = async () => {
  if (selectedFiles.value.length === 0 || !formData.s3_config_id || isUploading.value) return;

  // 检查是否有文件可以上传（排除已上传成功或正在上传的文件）
  const filesToUpload = fileItems.value.filter((item) => item.status !== "success" && item.status !== "uploading");

  if (filesToUpload.length === 0) {
    message.value = {
      type: "warning",
      content: t("file.messages.noFilesSelected"),
    };
    emit("upload-error", new Error(t("file.messages.noFilesSelected")));
    return;
  }

  // 验证文件大小是否超过限制
  for (let i = 0; i < selectedFiles.value.length; i++) {
    const file = selectedFiles.value[i];
    // 跳过已上传的文件
    if (fileItems.value[i].status === "success") continue;

    if (file.size > maxFileSizeMB.value * 1024 * 1024) {
      message.value = {
        type: "error",
        content: t("file.messages.fileTooLarge"),
      };
      // 更新文件状态
      fileItems.value[i].status = "error";
      fileItems.value[i].message = t("file.messages.fileTooLarge");

      // 触发错误事件，让父组件处理消息显示
      emit("upload-error", new Error(t("file.messages.fileTooLarge")));
      return;
    }
  }

  // 验证可打开次数，确保是非负整数
  if (formData.max_views < 0) {
    // 仍然设置message，但不在本组件中显示，而是通过事件传递给父组件
    message.value = {
      type: "error",
      content: t("file.messages.negativeMaxViews"),
    };

    // 触发错误事件，让父组件处理消息显示
    emit("upload-error", new Error(t("file.messages.negativeMaxViews")));
    return;
  }

  // 验证自定义链接格式
  if (formData.slug && !validateCustomLink()) {
    message.value = { type: "error", text: slugError.value };
    return;
  }

  isUploading.value = true;
  uploadProgress.value = 0;
  totalProgress.value = 0;
  uploadSpeed.value = "";
  message.value = null;
  currentUploadIndex.value = -1;

  // 重置上传速度计算相关变量
  lastLoaded.value = 0;
  lastTime.value = Date.now();

  const uploadResults = [];
  const errors = [];

  // 顺序上传每个文件
  for (let i = 0; i < selectedFiles.value.length; i++) {
    // 检查上传是否被取消，如果已取消则立即退出循环
    if (!isUploading.value) {
      console.log("上传已被取消，停止继续上传");
      break;
    }

    // 跳过已上传成功的文件
    if (fileItems.value[i].status === "success") {
      continue;
    }

    currentUploadIndex.value = i;
    const file = selectedFiles.value[i];
    const fileItem = fileItems.value[i];

    // 更新文件状态为上传中
    fileItem.status = "uploading";
    fileItem.progress = 0;

    try {
      // 如果有自定义slug，则根据文件索引添加后缀，避免冲突
      const fileSlug = formData.slug ? (selectedFiles.value.length > 1 ? `${formData.slug}-${i + 1}` : formData.slug) : "";

      // 使用直接上传到S3的方法
      const response = await api.file.directUploadFile(
        file,
        {
          s3_config_id: formData.s3_config_id,
          slug: fileSlug,
          path: formData.path || "",
          remark: formData.remark || "",
          password: formData.password || "",
          expires_in: formData.expires_in || "0",
          max_views: formData.max_views !== undefined ? Number(formData.max_views) : 0,
        },
        // 进度回调函数
        (progress, loaded, total) => {
          // 更新当前文件进度
          fileItem.progress = progress;

          // 更新整体进度
          const totalSize = selectedFiles.value.reduce((sum, f) => sum + f.size, 0);
          const completedSize = selectedFiles.value.filter((_, idx) => idx < i || fileItems.value[idx].status === "success").reduce((sum, f) => sum + f.size, 0);

          const currentProgress = (loaded / total) * file.size;
          totalProgress.value = Math.round(((completedSize + currentProgress) / totalSize) * 100);

          // 计算上传速度
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
        // 获取XHR实例的回调
        (xhr) => {
          activeXhr.value = xhr;
          fileItem.xhr = xhr;
        },
        // 获取文件ID的回调
        (fileId) => {
          currentFileId.value = fileId;
          fileItem.fileId = fileId;
        }
      );

      // 上传成功，更新文件状态
      fileItem.status = "success";
      fileItem.progress = 100;

      uploadResults.push(response);

      // 再次检查上传是否被取消，如果已取消则立即退出循环
      if (!isUploading.value) {
        console.log("上传过程中被取消，停止继续上传其他文件");
        break;
      }
    } catch (error) {
      console.error(`上传文件 ${file.name} 失败:`, error);
      // 检查是否是链接后缀冲突错误
      const errorMessage = error.message || t("common.unknownError");
      const isSlugConflict =
        (errorMessage.includes("slug") &&
          (errorMessage.includes("already exists") || errorMessage.includes("already taken") || errorMessage.includes("duplicate") || errorMessage.includes("conflict"))) ||
        errorMessage.includes("链接后缀已被占用") ||
        errorMessage.includes("已存在");

      // 检查是否是存储空间不足错误
      const isInsufficientStorage =
        errorMessage.includes("存储空间不足") ||
        errorMessage.includes("insufficient storage") ||
        errorMessage.includes("超过剩余空间") ||
        errorMessage.includes("exceeds") ||
        (errorMessage.includes("storage") && (errorMessage.includes("limit") || errorMessage.includes("full") || errorMessage.includes("quota")));

      // 检查是否是权限错误
      const isPermissionError =
        errorMessage.includes("没有权限使用此存储配置") ||
        errorMessage.includes("没有权限") ||
        errorMessage.includes("权限不足") ||
        errorMessage.includes("permission denied") ||
        errorMessage.includes("forbidden") ||
        errorMessage.includes("unauthorized");

      // 更新文件状态为错误
      fileItem.status = "error";
      // 如果是链接后缀冲突，提供更具体的错误消息
      if (isSlugConflict) {
        fileItem.message = t("file.messages.slugConflict");
      } else if (isInsufficientStorage) {
        // 处理存储空间不足的错误消息
        fileItem.message = processInsufficientStorageError(errorMessage);
      } else if (isPermissionError) {
        // 处理权限错误
        fileItem.message = t("file.messages.permissionError");
      } else {
        fileItem.message = errorMessage;
      }

      errors.push({
        fileName: file.name,
        error: error,
        isSlugConflict: isSlugConflict,
        isInsufficientStorage: isInsufficientStorage,
        isPermissionError: isPermissionError,
      });

      // 文件上传失败后也检查上传是否被取消
      if (!isUploading.value) {
        console.log("上传过程中被取消，停止继续上传其他文件");
        break;
      }
    }
  }

  // 处理整体上传结果
  currentUploadIndex.value = -1;

  if (errors.length > 0) {
    // 检查是否所有错误都是链接后缀冲突
    const allSlugConflicts = errors.every((err) => err.isSlugConflict);
    // 检查是否有链接后缀冲突
    const hasSlugConflicts = errors.some((err) => err.isSlugConflict);
    // 检查是否所有错误都是存储空间不足
    const allInsufficientStorage = errors.every((err) => err.isInsufficientStorage);
    // 检查是否所有错误都是权限错误
    const allPermissionErrors = errors.every((err) => err.isPermissionError);
    // 获取第一个存储空间不足错误（如果有的话）
    const firstStorageError = errors.find((err) => err.isInsufficientStorage);

    // 有错误发生
    if (errors.length === selectedFiles.value.length) {
      // 所有文件都上传失败
      if (allSlugConflicts) {
        // 如果所有失败都是因为链接后缀冲突
        message.value = {
          type: "error",
          content: t("file.allSlugConflicts"),
        };
        emit("upload-error", new Error(t("file.allSlugConflicts")));
      } else if (allInsufficientStorage && firstStorageError) {
        // 如果所有失败都是因为存储空间不足，显示第一个错误的具体信息
        message.value = {
          type: "error",
          content: processInsufficientStorageError(firstStorageError.error.message),
        };
        emit("upload-error", new Error(processInsufficientStorageError(firstStorageError.error.message)));
      } else if (allPermissionErrors) {
        // 如果所有失败都是因为权限错误
        message.value = {
          type: "error",
          content: t("file.allPermissionErrors"),
        };
        emit("upload-error", new Error(t("file.allPermissionErrors")));
      } else {
        message.value = {
          type: "error",
          content: t("file.allUploadsFailed"),
        };
        emit("upload-error", new Error(t("file.allUploadsFailed")));
      }
    } else {
      // 部分文件上传失败
      if (hasSlugConflicts) {
        // 如果有链接后缀冲突错误
        const slugConflictCount = errors.filter((err) => err.isSlugConflict).length;
        message.value = {
          type: "warning",
          content: slugConflictCount === errors.length ? t("file.someSlugConflicts", { count: slugConflictCount }) : t("file.someUploadsFailed", { count: errors.length }),
        };
        emit("upload-error", new Error(message.value.content));
      } else {
        message.value = {
          type: "warning",
          content: t("file.someUploadsFailed", { count: errors.length }),
        };
        emit("upload-error", new Error(t("file.someUploadsFailed", { count: errors.length })));
      }
    }
  } else if (uploadResults.length > 0) {
    // 至少有一个文件上传成功
    message.value = {
      type: "success",
      content: uploadResults.length > 1 ? t("file.multipleUploadsSuccessful", { count: uploadResults.length }) : t("file.uploadSuccessful"),
    };

    // 成功事件
    emit("upload-success", uploadResults);

    // 触发刷新文件列表事件
    emit("refresh-files");

    // 重置表单数据
    formData.slug = "";
    formData.remark = "";
    formData.password = "";
  }

  // 清除已上传成功的文件
  const successfulIndices = fileItems.value
    .map((item, index) => (item.status === "success" ? index : -1))
    .filter((index) => index !== -1)
    .sort((a, b) => b - a); // 倒序排列以便从后向前删除

  // 是否全部文件都上传成功
  const allSuccess = successfulIndices.length === selectedFiles.value.length;

  // 从后向前删除已上传成功的文件
  for (const index of successfulIndices) {
    selectedFiles.value.splice(index, 1);
    fileItems.value.splice(index, 1);
  }

  // 全部上传成功后，重置文件输入框
  if (allSuccess && fileInput.value) {
    fileInput.value.value = "";
  }

  isUploading.value = false;
};

// 格式化上传速度
const formatSpeed = (bytesPerSecond) => {
  if (bytesPerSecond < 1024) {
    return `${bytesPerSecond.toFixed(0)} B/s`;
  } else if (bytesPerSecond < 1024 * 1024) {
    return `${(bytesPerSecond / 1024).toFixed(1)} KB/s`;
  } else {
    return `${(bytesPerSecond / (1024 * 1024)).toFixed(1)} MB/s`;
  }
};

// 处理存储空间不足的错误消息
const processInsufficientStorageError = (errorMessage) => {
  // 检查是否是存储空间不足的错误
  const isInsufficientStorage =
    errorMessage.includes("存储空间不足") || errorMessage.includes("insufficient storage") || errorMessage.includes("超过剩余空间") || errorMessage.includes("exceeds");

  if (!isInsufficientStorage) {
    return errorMessage;
  }

  try {
    // 使用正则表达式提取文件大小、剩余空间和总容量限制
    // 匹配中文和英文格式的错误消息
    const fileSizeMatch = errorMessage.match(/文件大小\((.*?)\)|file size\((.*?)\)/i);
    const remainingSpaceMatch = errorMessage.match(/剩余空间\((.*?)\)|remaining space\((.*?)\)/i);
    const totalCapacityMatch = errorMessage.match(/总容量限制为(.*?)。|capacity limit is(.*?)\./i);

    const fileSize = fileSizeMatch ? fileSizeMatch[1] || fileSizeMatch[2] : "";
    const remainingSpace = remainingSpaceMatch ? remainingSpaceMatch[1] || remainingSpaceMatch[2] : "";
    const totalCapacity = totalCapacityMatch ? totalCapacityMatch[1] || totalCapacityMatch[2] : "";

    if (fileSize && remainingSpace && totalCapacity) {
      // 使用国际化文本构建错误消息
      return t("file.insufficientStorageDetailed", {
        fileSize: fileSize,
        remainingSpace: remainingSpace,
        totalCapacity: totalCapacity,
      });
    }
  } catch (e) {
    console.error("Error processing insufficient storage message:", e);
  }

  // 如果提取失败，返回原始错误消息
  return errorMessage;
};

// 重试上传单个文件
const retryUpload = async (index) => {
  if (!selectedFiles.value[index] || !formData.s3_config_id || isUploading.value) return;

  const file = selectedFiles.value[index];
  const fileItem = fileItems.value[index];

  // 重置文件状态为待上传
  fileItem.status = "pending";
  fileItem.progress = 0;
  fileItem.message = "";

  isUploading.value = true;
  currentUploadIndex.value = index;

  // 重置上传速度计算相关变量
  lastLoaded.value = 0;
  lastTime.value = Date.now();

  try {
    // 如果有自定义slug，则保持与之前相同的命名逻辑
    const fileSlug = formData.slug ? (selectedFiles.value.length > 1 ? `${formData.slug}-${index + 1}` : formData.slug) : "";

    // 更新文件状态为上传中
    fileItem.status = "uploading";

    // 使用现有的上传函数进行上传
    const response = await api.file.directUploadFile(
      file,
      {
        s3_config_id: formData.s3_config_id,
        slug: fileSlug,
        path: formData.path || "",
        remark: formData.remark || "",
        password: formData.password || "",
        expires_in: formData.expires_in || "0",
        max_views: formData.max_views !== undefined ? Number(formData.max_views) : 0,
      },
      // 进度回调函数
      (progress, loaded, total) => {
        // 更新当前文件进度
        fileItem.progress = progress;

        // 计算上传速度
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
      // 获取XHR实例的回调
      (xhr) => {
        activeXhr.value = xhr;
        fileItem.xhr = xhr;
      },
      // 获取文件ID的回调
      (fileId) => {
        currentFileId.value = fileId;
        fileItem.fileId = fileId;
      }
    );

    // 上传成功，更新文件状态
    fileItem.status = "success";
    fileItem.progress = 100;

    // 显示成功消息
    message.value = {
      type: "success",
      content: t("file.retrySuccessful"),
    };

    // 触发刷新文件列表事件
    emit("refresh-files");

    // 延迟从列表中移除成功上传的文件
    setTimeout(() => {
      const index = selectedFiles.value.findIndex((f) => f === file);
      if (index !== -1) {
        selectedFiles.value.splice(index, 1);
        fileItems.value.splice(index, 1);
      }
    }, 2000);
  } catch (error) {
    console.error(`重试上传文件 ${file.name} 失败:`, error);

    // 检查是否是链接后缀冲突错误
    const errorMessage = error.message || t("file.messages.unknownError");
    const isSlugConflict =
      (errorMessage.includes("slug") &&
        (errorMessage.includes("already exists") || errorMessage.includes("already taken") || errorMessage.includes("duplicate") || errorMessage.includes("conflict"))) ||
      errorMessage.includes("链接后缀已被占用") ||
      errorMessage.includes("已存在");

    // 检查是否是存储空间不足错误
    const isInsufficientStorage =
      errorMessage.includes("存储空间不足") ||
      errorMessage.includes("insufficient storage") ||
      errorMessage.includes("超过剩余空间") ||
      errorMessage.includes("exceeds") ||
      (errorMessage.includes("storage") && (errorMessage.includes("limit") || errorMessage.includes("full") || errorMessage.includes("quota")));

    // 检查是否是权限错误
    const isPermissionError =
      errorMessage.includes("没有权限使用此存储配置") ||
      errorMessage.includes("没有权限") ||
      errorMessage.includes("权限不足") ||
      errorMessage.includes("permission denied") ||
      errorMessage.includes("forbidden") ||
      errorMessage.includes("unauthorized");

    // 更新文件状态为错误
    fileItem.status = "error";

    // 设置具体的错误消息
    if (isSlugConflict) {
      fileItem.message = t("file.messages.slugConflict");
    } else if (isInsufficientStorage) {
      // 处理存储空间不足的错误消息
      fileItem.message = processInsufficientStorageError(errorMessage);
    } else if (isPermissionError) {
      // 处理权限错误
      fileItem.message = t("file.messages.permissionError");
    } else {
      fileItem.message = errorMessage;
    }

    // 触发错误事件
    emit("upload-error", error);
  } finally {
    // 重试逻辑更简单，总是设置isUploading.value为false
    // 因为retryUpload通常是单独操作，而不是批量上传的一部分
    isUploading.value = false;
    currentUploadIndex.value = -1;
  }
};

// 取消单个文件上传
const cancelSingleUpload = (index) => {
  if (index < 0 || index >= fileItems.value.length) return;

  const fileItem = fileItems.value[index];

  // 只处理"正在上传"或"等待上传"的文件
  if (fileItem.status !== "uploading" && fileItem.status !== "pending") return;

  // 如果有活动的XHR请求，取消它
  if (fileItem.xhr) {
    fileItem.xhr.abort();
  }

  // 更新文件状态为取消
  fileItem.status = "error";
  fileItem.message = t("file.messages.uploadCancelled");

  // 如果已获取了文件ID，则删除相应的文件记录
  if (fileItem.fileId) {
    // 使用统一的批量删除API
    api.file
      .batchDeleteFiles([fileItem.fileId])
      .then(() => {
        console.log("已成功删除被取消的文件记录", fileItem.fileId);
      })
      .catch((error) => {
        console.error("删除被取消的文件记录失败:", error);
      });
  }

  // 如果这是当前正在上传的文件，且没有其他文件正在上传，则重置上传状态
  if (currentUploadIndex.value === index) {
    // 检查是否还有其他正在上传的文件
    const stillUploading = fileItems.value.some((item, idx) => idx !== index && item.status === "uploading");

    if (!stillUploading) {
      // 不设置isUploading.value为false，以确保上传循环继续处理队列中的后续文件
      // 但重置其他状态，以确保UI显示正确
      uploadProgress.value = 0;
      totalProgress.value = 0;
      uploadSpeed.value = "";

      // 不重置currentUploadIndex，让submitUpload函数的循环继续处理
      // currentUploadIndex.value = -1;
    }
  }

  // 显示提示消息
  message.value = {
    type: "warning",
    content: t("file.singleFileCancelMessage"),
  };
};
</script>

<style scoped>
/* 脉动边框动画 */
@keyframes pulseBorder {
  0% {
    border-color: rgba(59, 130, 246, 0.5); /* 淡蓝色 */
  }
  50% {
    border-color: rgba(59, 130, 246, 1); /* 全蓝色 */
  }
  100% {
    border-color: rgba(59, 130, 246, 0.5); /* 淡蓝色 */
  }
}

/* 进度条条纹动画 */
@keyframes progressStripes {
  0% {
    background-position: 0 0;
  }
  100% {
    background-position: 30px 0;
  }
}

.pulsing-border {
  animation: pulseBorder 1.5s ease-in-out infinite;
}

/* 提升动画性能 */
.drop-zone {
  will-change: border-color, background-color, transform;
  transform: translateZ(0);
}

/* 拖动元素进入时的缩放效果 */
.drop-zone.pulsing-border {
  transform: scale(1.01);
  transition: transform 0.3s ease;
}

/* 进度条条纹样式 */
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
  background-size: 30px 30px;
}

/* 进度条条纹动画 */
.animate-progress-stripes {
  animation: progressStripes 1s linear infinite;
}
</style>
