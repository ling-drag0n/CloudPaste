<template>
  <div class="file-preview-container">
    <!-- 文件预览区域 -->
    <div class="file-preview mb-6 p-4 rounded-lg" :class="darkMode ? 'bg-gray-800/50' : 'bg-white'">
      <!-- 文件标题和操作按钮 -->
      <div class="mb-4">
        <h3 class="text-lg font-medium mb-3" :class="darkMode ? 'text-gray-200' : 'text-gray-700'" :title="file.name">
          {{ file.name }}
        </h3>
        <div class="flex flex-wrap gap-2">
          <!-- 下载按钮 -->
          <button
            @click="handleDownload"
            class="inline-flex items-center px-3 py-1.5 rounded-md transition-colors text-sm font-medium"
            :class="darkMode ? 'bg-primary-600 hover:bg-primary-700 text-white' : 'bg-primary-500 hover:bg-primary-600 text-white'"
          >
            <svg class="w-4 h-4 mr-1.5" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-4l-4 4m0 0l-4-4m4 4V4" />
            </svg>
            <span>{{ t("mount.filePreview.downloadFile") }}</span>
          </button>

          <!-- S3直链预览按钮 -->
          <button
            @click="handleS3DirectPreview"
            class="inline-flex items-center px-3 py-1.5 rounded-md transition-colors text-sm font-medium"
            :class="darkMode ? 'bg-green-600 hover:bg-green-700 text-white' : 'bg-green-500 hover:bg-green-600 text-white'"
            :disabled="isGeneratingPreview"
          >
            <svg v-if="!isGeneratingPreview" class="w-4 h-4 mr-1.5" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 12a3 3 0 11-6 0 3 3 0 016 0z" />
              <path
                stroke-linecap="round"
                stroke-linejoin="round"
                stroke-width="2"
                d="M2.458 12C3.732 7.943 7.523 5 12 5c4.478 0 8.268 2.943 9.542 7-1.274 4.057-5.064 7-9.542 7-4.477 0-8.268-2.943-9.542-7z"
              />
            </svg>
            <svg v-else class="animate-spin w-4 h-4 mr-1.5" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
              <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
              <path
                class="opacity-75"
                fill="currentColor"
                d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"
              ></path>
            </svg>
            <span>{{ isGeneratingPreview ? t("mount.filePreview.generating") : t("mount.filePreview.directPreview") }}</span>
          </button>

          <!-- 生成分享链接按钮 -->
          <button
            @click="handleCreateShare"
            class="inline-flex items-center px-3 py-1.5 rounded-md transition-colors text-sm font-medium"
            :class="darkMode ? 'bg-blue-600 hover:bg-blue-700 text-white' : 'bg-blue-500 hover:bg-blue-600 text-white'"
            :disabled="isCreatingShare"
          >
            <svg v-if="!isCreatingShare" class="w-4 h-4 mr-1.5" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path
                stroke-linecap="round"
                stroke-linejoin="round"
                stroke-width="2"
                d="M8.684 13.342C8.886 12.938 9 12.482 9 12c0-.482-.114-.938-.316-1.342m0 2.684a3 3 0 110-2.684m0 2.684l6.632 3.316m-6.632-6l6.632-3.316m0 0a3 3 0 105.367-2.684 3 3 0 00-5.367 2.684zm0 9.316a3 3 0 105.367 2.684 3 3 0 00-5.367-2.684z"
              />
            </svg>
            <svg v-else class="animate-spin w-4 h-4 mr-1.5" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
              <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
              <path
                class="opacity-75"
                fill="currentColor"
                d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"
              ></path>
            </svg>
            <span>{{ isCreatingShare ? t("mount.filePreview.creatingShare") : t("mount.filePreview.createShare") }}</span>
          </button>
        </div>
      </div>

      <!-- 文件信息 -->
      <div class="file-info grid grid-cols-1 md:grid-cols-2 gap-4 mb-4 p-3 rounded-lg bg-opacity-50" :class="darkMode ? 'bg-gray-700/50' : 'bg-gray-100'">
        <div class="file-info-item flex items-center">
          <span class="font-medium mr-2" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">{{ t("mount.filePreview.fileSize") }}</span>
          <span :class="darkMode ? 'text-gray-400' : 'text-gray-600'">{{ formatFileSize(file.size) }}</span>
        </div>
        <div class="file-info-item flex items-center">
          <span class="font-medium mr-2" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">{{ t("mount.filePreview.modifiedTime") }}</span>
          <span :class="darkMode ? 'text-gray-400' : 'text-gray-600'">{{ formatDate(file.modified) }}</span>
        </div>
        <div class="file-info-item flex items-center">
          <span class="font-medium mr-2" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">{{ t("mount.filePreview.fileType") }}</span>
          <span :class="darkMode ? 'text-gray-400' : 'text-gray-600'">{{ file.mimetype || t("mount.filePreview.unknown") }}</span>
        </div>
      </div>

      <!-- 文本预览工具栏 -->
      <div v-if="isText" class="text-preview-toolbar p-3 mb-4 rounded-lg bg-opacity-50" :class="darkMode ? 'bg-gray-700/50' : 'bg-gray-100'">
        <!-- 响应式布局：桌面端横向，移动端纵向 -->
        <div class="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-3">
          <!-- 左侧控制区：模式切换器和编码选择器 -->
          <div class="toolbar-left flex flex-wrap items-center gap-3">
            <div class="mode-switcher">
              <select
                v-model="textPreviewMode"
                class="mode-select px-3 py-1 text-sm border rounded"
                :class="darkMode ? 'bg-gray-600 border-gray-500 text-gray-200' : 'bg-white border-gray-300 text-gray-700'"
              >
                <option v-for="mode in availablePreviewModes" :key="mode.value" :value="mode.value">
                  {{ mode.label }}
                </option>
              </select>
            </div>

            <div class="encoding-selector">
              <select
                v-model="textEncoding"
                class="encoding-select px-3 py-1 text-sm border rounded"
                :class="darkMode ? 'bg-gray-600 border-gray-500 text-gray-200' : 'bg-white border-gray-300 text-gray-700'"
              >
                <option v-for="encoding in availableEncodings" :key="encoding.value" :value="encoding.value" :title="encoding.description">
                  {{ encoding.label }}
                </option>
              </select>
            </div>
          </div>

          <!-- 右侧按钮区：保存和全屏按钮 -->
          <div class="toolbar-right flex flex-wrap items-center gap-2">
            <!-- 右键菜单提示图标 -->
            <div
              v-if="textPreviewMode === 'edit'"
              class="context-menu-hint flex items-center justify-center w-8 h-8 rounded-full transition-all duration-200 cursor-help hover:scale-110"
              :class="darkMode ? 'hover:bg-gray-700/50' : 'hover:bg-gray-100/50'"
              :title="$t('mount.filePreview.rightClickHint')"
            >
              <svg width="20" height="20" viewBox="0 0 24 24" fill="#f59e0b">
                <path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm1 15h-2v-2h2v2zm0-4h-2V7h2v6z" />
              </svg>
            </div>

            <!-- 保存按钮 - 仅在编辑模式下显示 -->
            <button
              v-if="textPreviewMode === 'edit'"
              @click="handleSaveFile"
              :disabled="isSaving"
              class="save-btn flex items-center px-3 py-1 text-sm border rounded transition-colors"
              :class="[
                darkMode ? 'bg-blue-600 hover:bg-blue-700 border-blue-500 text-white' : 'bg-blue-500 hover:bg-blue-600 border-blue-400 text-white',
                isSaving ? 'opacity-50 cursor-not-allowed' : '',
              ]"
              :title="$t('mount.filePreview.saveFileShortcut')"
            >
              <!-- Loading图标 -->
              <svg v-if="isSaving" class="w-4 h-4 mr-1 animate-spin" fill="none" viewBox="0 0 24 24">
                <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
                <path
                  class="opacity-75"
                  fill="currentColor"
                  d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"
                ></path>
              </svg>
              <!-- 保存图标 -->
              <svg v-else class="w-4 h-4 mr-1" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path
                  stroke-linecap="round"
                  stroke-linejoin="round"
                  stroke-width="2"
                  d="M8 7H5a2 2 0 00-2 2v9a2 2 0 002 2h14a2 2 0 002-2V9a2 2 0 00-2-2h-3m-1 4l-3-3m0 0l-3 3m3-3v12"
                />
              </svg>
              {{ isSaving ? $t("mount.filePreview.saving") : $t("mount.filePreview.save") }}
            </button>

            <!-- 全屏按钮 -->
            <button
              @click="toggleFullscreen"
              class="fullscreen-btn flex items-center px-3 py-1 text-sm border rounded transition-colors"
              :class="darkMode ? 'bg-gray-600 hover:bg-gray-700 border-gray-500 text-gray-200' : 'bg-white hover:bg-gray-50 border-gray-300 text-gray-700'"
              :title="isContentFullscreen ? '退出全屏' : '全屏显示'"
            >
              <svg v-if="!isContentFullscreen" class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path
                  stroke-linecap="round"
                  stroke-linejoin="round"
                  stroke-width="2"
                  d="M4 8V4m0 0h4M4 4l5 5m11-1V4m0 0h-4m4 0l-5 5M4 16v4m0 0h4m-4 0l5-5m11 5l-5-5m5 5v-4m0 4h-4"
                />
              </svg>
              <svg v-else class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path
                  stroke-linecap="round"
                  stroke-linejoin="round"
                  stroke-width="2"
                  d="M9 9V4.5M9 9H4.5M9 9L3.5 3.5M15 9h4.5M15 9V4.5M15 9l5.5-5.5M9 15v4.5M9 15H4.5M9 15l-5.5 5.5M15 15h4.5M15 15v4.5m0 0l5.5 5.5"
                />
              </svg>
            </button>
          </div>
        </div>
      </div>

      <!-- 预览内容 -->
      <div
        ref="previewContentRef"
        class="preview-content border rounded-lg overflow-hidden transition-all duration-300 flex flex-col"
        :class="[darkMode ? 'border-gray-700' : 'border-gray-200']"
        style="max-height: 600px; min-height: 400px"
      >
        <!-- 全屏模式下的工具栏 -->
        <div v-if="isContentFullscreen && isText" class="fullscreen-toolbar p-3 border-b" :class="darkMode ? 'bg-gray-800 border-gray-700' : 'bg-white border-gray-200'">
          <!-- 响应式布局：桌面端横向，移动端纵向 -->
          <div class="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-3">
            <!-- 左侧：文件信息 -->
            <div class="toolbar-left flex flex-wrap items-center gap-3">
              <h3 class="text-lg font-medium" :class="darkMode ? 'text-gray-200' : 'text-gray-800'">{{ file.name }}</h3>
              <span class="text-sm" :class="darkMode ? 'text-gray-400' : 'text-gray-600'">{{
                textPreviewMode === "edit" ? $t("mount.filePreview.editMode") : $t("mount.filePreview.previewMode")
              }}</span>
            </div>

            <!-- 右侧：控制按钮 -->
            <div class="toolbar-right flex flex-wrap items-center gap-2">
              <!-- 模式切换 -->
              <select
                v-model="textPreviewMode"
                class="mode-select px-2 py-1 text-sm border rounded"
                :class="darkMode ? 'bg-gray-700 border-gray-600 text-gray-200' : 'bg-white border-gray-300 text-gray-700'"
              >
                <option v-for="mode in availablePreviewModes" :key="mode.value" :value="mode.value">
                  {{ mode.label }}
                </option>
              </select>

              <!-- 编码选择 -->
              <select
                v-model="textEncoding"
                class="encoding-select px-2 py-1 text-sm border rounded"
                :class="darkMode ? 'bg-gray-700 border-gray-600 text-gray-200' : 'bg-white border-gray-300 text-gray-700'"
              >
                <option v-for="encoding in availableEncodings" :key="encoding.value" :value="encoding.value" :title="encoding.description">
                  {{ encoding.label }}
                </option>
              </select>

              <!-- 保存按钮 -->
              <button
                v-if="textPreviewMode === 'edit'"
                @click="handleSaveFile"
                :disabled="isSaving"
                class="save-btn flex items-center px-2 py-1 text-sm border rounded transition-colors"
                :class="[
                  darkMode ? 'bg-blue-600 hover:bg-blue-700 border-blue-500 text-white' : 'bg-blue-500 hover:bg-blue-600 border-blue-400 text-white',
                  isSaving ? 'opacity-50 cursor-not-allowed' : '',
                ]"
                :title="$t('mount.filePreview.saveFileShortcut')"
              >
                <!-- Loading图标 -->
                <svg v-if="isSaving" class="w-4 h-4 mr-1 animate-spin" fill="none" viewBox="0 0 24 24">
                  <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
                  <path
                    class="opacity-75"
                    fill="currentColor"
                    d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"
                  ></path>
                </svg>
                <!-- 保存图标 -->
                <svg v-else class="w-4 h-4 mr-1" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path
                    stroke-linecap="round"
                    stroke-linejoin="round"
                    stroke-width="2"
                    d="M8 7H5a2 2 0 00-2 2v9a2 2 0 002 2h14a2 2 0 002-2V9a2 2 0 00-2-2h-3m-1 4l-3-3m0 0l-3 3m3-3v12"
                  />
                </svg>
                {{ isSaving ? $t("mount.filePreview.saving") : $t("mount.filePreview.save") }}
              </button>

              <!-- 退出全屏按钮 -->
              <button
                @click="toggleFullscreen"
                class="exit-fullscreen-btn flex items-center px-2 py-1 text-sm border rounded transition-colors"
                :class="darkMode ? 'bg-gray-600 hover:bg-gray-700 border-gray-500 text-gray-200' : 'bg-white hover:bg-gray-50 border-gray-300 text-gray-700'"
                title="退出全屏"
              >
                <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path
                    stroke-linecap="round"
                    stroke-linejoin="round"
                    stroke-width="2"
                    d="M9 9V4.5M9 9H4.5M9 9L3.5 3.5M15 9h4.5M15 9V4.5M15 9l5.5-5.5M9 15v4.5M9 15H4.5M9 15l-5.5 5.5M15 15h4.5M15 15v4.5m0 0l5.5 5.5"
                  />
                </svg>
              </button>
            </div>
          </div>
        </div>
        <!-- 加载指示器 -->
        <div v-if="isLoading" class="flex-1 flex items-center justify-center">
          <div class="flex flex-col items-center justify-center p-12">
            <div class="animate-spin rounded-full h-12 w-12 border-b-2" :class="darkMode ? 'border-primary-500' : 'border-primary-600'"></div>
          </div>
        </div>

        <!-- 图片预览 -->
        <div v-else-if="isImage" class="flex-1 flex justify-center items-center p-4">
          <img
            v-if="authenticatedPreviewUrl"
            :src="authenticatedPreviewUrl"
            :alt="file.name"
            class="max-w-full max-h-[600px] object-contain"
            @load="handleContentLoaded"
            @error="handleContentError"
          />
          <div v-else class="loading-indicator text-center py-8">
            <div class="animate-spin rounded-full h-10 w-10 border-b-2 mx-auto" :class="darkMode ? 'border-primary-500' : 'border-primary-600'"></div>
          </div>
        </div>

        <!-- 视频预览 -->
        <div v-else-if="isVideo">
          <VideoPreview
            :file="file"
            :video-url="authenticatedPreviewUrl"
            :dark-mode="darkMode"
            :is-admin="isAdmin"
            :current-path="getCurrentDirectoryPath()"
            :directory-items="directoryItems"
            @loaded="handleContentLoaded"
          />
        </div>

        <!-- 音频预览 -->
        <div v-else-if="isAudio" class="flex-1 flex items-center justify-center">
          <AudioPreview
            :file="file"
            :audio-url="authenticatedPreviewUrl"
            :dark-mode="darkMode"
            :is-admin="isAdmin"
            :current-path="getCurrentDirectoryPath()"
            :directory-items="directoryItems"
            @play="handleAudioPlay"
            @pause="handleAudioPause"
            @error="handleAudioError"
            @loaded="handleContentLoaded"
          />
        </div>

        <!-- PDF预览 -->
        <div v-else-if="isPdf" class="pdf-preview h-[600px]">
          <iframe
            v-if="authenticatedPreviewUrl"
            :src="authenticatedPreviewUrl"
            frameborder="0"
            class="w-full h-full"
            @load="handleContentLoaded"
            @error="handleContentError"
          ></iframe>
          <div v-else class="loading-indicator text-center py-8">
            <div class="animate-spin rounded-full h-10 w-10 border-b-2 mx-auto" :class="darkMode ? 'border-primary-500' : 'border-primary-600'"></div>
          </div>
        </div>

        <!-- Office文件预览 -->
        <div v-else-if="isOffice" ref="officePreviewRef" class="office-preview h-[900px] w-full">
          <!-- Office预览头部控制栏 -->
          <div class="sticky top-0 z-20 flex items-center justify-between p-2 bg-gray-100 dark:bg-gray-700 border-b border-gray-200 dark:border-gray-600">
            <span class="text-sm font-medium text-gray-700 dark:text-gray-300">
              {{ isWordDoc ? t("mount.filePreview.wordPreview") : isExcel ? t("mount.filePreview.excelPreview") : t("mount.filePreview.powerpointPreview") }}
            </span>
            <div class="flex items-center space-x-2">
              <button
                @click="toggleOfficeFullscreen"
                class="text-xs px-2 py-1 rounded flex items-center bg-gray-200 dark:bg-gray-600 text-gray-700 dark:text-gray-300 hover:bg-gray-300 dark:hover:bg-gray-500 transition-colors"
              >
                <svg v-if="!isOfficeFullscreen" class="w-3.5 h-3.5 mr-1" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path
                    stroke-linecap="round"
                    stroke-linejoin="round"
                    stroke-width="2"
                    d="M4 8V4m0 0h4M4 4l5 5m11-1V4m0 0h-4m4 0l-5 5M4 16v4m0 0h4m-4 0l5-5m11 5v-4m0 4h-4m4 0l-5-5"
                  />
                </svg>
                <svg v-else class="w-3.5 h-3.5 mr-1" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
                </svg>
                <span>{{ isOfficeFullscreen ? t("mount.filePreview.exitFullscreen") : t("mount.filePreview.fullscreen") }}</span>
              </button>
              <button
                @click="toggleOfficePreviewService"
                class="text-xs px-2 py-1 rounded bg-blue-100 dark:bg-blue-700 text-blue-700 dark:text-blue-100 hover:bg-blue-200 dark:hover:bg-blue-600 transition-colors"
              >
                {{ useGoogleDocsPreview ? t("mount.filePreview.useMicrosoftPreview") : t("mount.filePreview.useGooglePreview") }}
              </button>
            </div>
          </div>

          <!-- 加载状态 -->
          <div v-if="officePreviewLoading" class="flex flex-col items-center justify-center h-full">
            <div class="animate-spin rounded-full h-12 w-12 border-b-2 mb-4" :class="darkMode ? 'border-primary-500' : 'border-primary-600'"></div>
            <p class="text-sm" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">{{ t("mount.filePreview.loadingPreview") }}</p>
          </div>

          <!-- 错误状态 -->
          <div v-else-if="officePreviewError" class="flex flex-col items-center justify-center h-full p-4">
            <svg
              class="w-16 h-16 mb-4"
              :class="darkMode ? 'text-red-400' : 'text-red-500'"
              xmlns="http://www.w3.org/2000/svg"
              fill="none"
              viewBox="0 0 24 24"
              stroke="currentColor"
            >
              <path
                stroke-linecap="round"
                stroke-linejoin="round"
                stroke-width="1.5"
                d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z"
              />
            </svg>
            <p class="text-center mb-4" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">{{ officePreviewError }}</p>
            <button
              @click="updateOfficePreviewUrls"
              class="px-3 py-1.5 text-sm font-medium rounded-md transition-colors"
              :class="darkMode ? 'bg-primary-600 hover:bg-primary-700 text-white' : 'bg-primary-500 hover:bg-primary-600 text-white'"
            >
              {{ t("mount.filePreview.retry") }}
            </button>
          </div>

          <!-- 预览内容 -->
          <div v-else-if="currentOfficePreviewUrl" class="w-full h-full">
            <iframe
              :src="currentOfficePreviewUrl"
              frameborder="0"
              class="w-full h-full"
              @load="handleOfficePreviewLoaded"
              @error="handleOfficePreviewError"
              sandbox="allow-scripts allow-same-origin allow-forms"
              referrerpolicy="no-referrer"
            ></iframe>
          </div>
        </div>

        <!-- Markdown预览 - 使用TextRenderer统一处理 -->
        <div v-else-if="isMarkdown" :class="isContentFullscreen ? 'fullscreen-text-container' : ''">
          <TextPreview
            ref="textPreviewRef"
            :file="file"
            :text-url="authenticatedPreviewUrl"
            :dark-mode="darkMode"
            :is-admin="isAdmin"
            :current-path="getCurrentDirectoryPath()"
            :directory-items="directoryItems"
            :initial-mode="textPreviewMode"
            :initial-encoding="textEncoding"
            :max-height="dynamicMaxHeight"
            @load="handleContentLoaded"
            @error="handleContentError"
            @mode-change="handleModeChange"
            @encoding-change="handleEncodingChange"
          />
        </div>

        <!-- 文本预览 -->
        <div v-else-if="isText" :class="isContentFullscreen ? 'fullscreen-text-container' : ''">
          <TextPreview
            ref="textPreviewRef"
            :file="file"
            :text-url="authenticatedPreviewUrl"
            :dark-mode="darkMode"
            :is-admin="isAdmin"
            :current-path="getCurrentDirectoryPath()"
            :directory-items="directoryItems"
            :initial-mode="textPreviewMode"
            :initial-encoding="textEncoding"
            :max-height="dynamicMaxHeight"
            @load="handleContentLoaded"
            @error="handleContentError"
            @mode-change="handleModeChange"
            @encoding-change="handleEncodingChange"
          />
        </div>

        <!-- 其他文件类型或错误状态 -->
        <div v-else-if="loadError" class="flex-1 flex items-center justify-center">
          <div class="generic-preview text-center py-12">
            <svg
              xmlns="http://www.w3.org/2000/svg"
              class="h-20 w-20 mx-auto mb-4"
              :class="darkMode ? 'text-red-400' : 'text-red-500'"
              fill="none"
              viewBox="0 0 24 24"
              stroke="currentColor"
            >
              <path
                stroke-linecap="round"
                stroke-linejoin="round"
                stroke-width="1.5"
                d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z"
              />
            </svg>
            <p class="text-lg font-medium mb-2" :class="darkMode ? 'text-red-300' : 'text-red-700'">{{ t("mount.filePreview.previewError") }}</p>
            <p class="text-sm" :class="darkMode ? 'text-gray-400' : 'text-gray-500'">{{ t("mount.filePreview.retryLoad") }}</p>
          </div>
        </div>

        <!-- 压缩文件预览 -->
        <div v-else-if="isArchive" class="flex-1">
          <ArchivePreview
            :file="file"
            :dark-mode="darkMode"
            :authenticated-preview-url="authenticatedPreviewUrl"
            @download="handleDownload"
            @loaded="handleContentLoaded"
            @error="handleContentError"
          />
        </div>

        <!-- 不支持预览的文件类型 -->
        <div v-else class="flex-1 flex items-center justify-center">
          <div class="generic-preview text-center py-12">
            <svg
              xmlns="http://www.w3.org/2000/svg"
              class="h-20 w-20 mx-auto mb-4"
              :class="darkMode ? 'text-gray-500' : 'text-gray-400'"
              fill="none"
              viewBox="0 0 24 24"
              stroke="currentColor"
            >
              <path
                stroke-linecap="round"
                stroke-linejoin="round"
                stroke-width="1.5"
                d="M7 21h10a2 2 0 002-2V9.414a1 1 0 00-.293-.707l-5.414-5.414A1 1 0 0012.586 3H7a2 2 0 00-2 2v14a2 2 0 002 2z"
              />
            </svg>
            <p class="text-lg font-medium mb-2" :class="darkMode ? 'text-gray-300' : 'text-gray-700'">{{ t("mount.filePreview.cannotPreview") }}</p>
            <p class="text-sm" :class="darkMode ? 'text-gray-400' : 'text-gray-500'">{{ t("mount.filePreview.downloadToView") }}</p>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { computed, ref, watch, onMounted, onBeforeUnmount } from "vue";
import { useI18n } from "vue-i18n";
import { usePreviewRenderers, useFilePreviewExtensions, useFileSave } from "../../../composables/index.js";
import { useAuthStore } from "@/stores/authStore.js";
import { getPreviewModeFromFilename, PREVIEW_MODES, SUPPORTED_ENCODINGS } from "@/utils/textUtils.js";
import { isArchiveFile } from "@/utils/fileTypes.js";
import AudioPreview from "./AudioPreview.vue";
import VideoPreview from "./VideoPreview.vue";
import TextPreview from "./TextPreview.vue";
import ArchivePreview from "./ArchivePreview.vue";

const { t } = useI18n();

// Props 定义
const props = defineProps({
  file: {
    type: Object,
    required: true,
  },
  darkMode: {
    type: Boolean,
    default: false,
  },
  isLoading: {
    type: Boolean,
    default: false,
  },
  // 认证相关props
  isAdmin: {
    type: Boolean,
    default: false,
  },
  apiKeyInfo: {
    type: Object,
    default: null,
  },
  hasFilePermission: {
    type: Boolean,
    default: false,
  },
  // 目录项目列表（用于音频播放列表等功能）
  directoryItems: {
    type: Array,
    default: () => [],
  },
});

// Emit 事件定义
const emit = defineEmits(["download", "loaded", "error", "updated", "switch-audio", "show-message"]);

// 使用认证Store
const authStore = useAuthStore();

// 创建认证信息对象
const authInfo = computed(() => ({
  isAdmin: props.isAdmin ?? authStore.isAdmin,
  apiKeyInfo: props.apiKeyInfo ?? authStore.apiKeyInfo,
  hasFilePermission: props.hasFilePermission ?? authStore.hasPathPermission(props.file?.path || ""),
  get isAuthenticated() {
    return this.isAdmin || !!this.apiKeyInfo;
  },
  get authType() {
    return this.isAdmin ? "admin" : this.apiKeyInfo ? "apikey" : "none";
  },
}));

// 使用文件预览渲染器
const renderers = usePreviewRenderers(
  computed(() => props.file),
  emit,
  computed(() => props.darkMode)
);

// 使用文件预览扩展功能
const extensions = useFilePreviewExtensions(
  computed(() => props.file),
  authInfo,
  renderers.officePreviewLoading,
  renderers.officePreviewError,
  renderers.officePreviewTimedOut,
  renderers.previewUrl,
  renderers.handleFullscreenChange,
  renderers.handleKeyDown,
  emit,
  renderers.authenticatedPreviewUrl,
  renderers.previewTimeoutId,
  renderers.microsoftOfficePreviewUrl,
  renderers.googleDocsPreviewUrl
);

// 解构保留的预览功能
const {
  // 模板中使用的状态
  authenticatedPreviewUrl,
  officePreviewLoading,
  officePreviewError,
  useGoogleDocsPreview,
  isOfficeFullscreen,

  // 模板中使用的计算属性
  isImage,
  isVideo,
  isAudio,
  isPdf,
  isOffice,
  isText,
  isWordDoc,
  isExcel,
  currentOfficePreviewUrl,

  // 模板中使用的DOM引用
  officePreviewRef,

  // 模板中使用的方法
  formatFileSize,
  formatDate,
  toggleOfficePreviewService,
  toggleOfficeFullscreen,
  updateOfficePreviewUrls,
  handleContentLoaded,
  handleContentError,

  // 渲染器中的方法
  handleOfficePreviewLoaded,
  handleOfficePreviewError,
  handleAudioPlay,
  handleAudioPause,
  handleAudioError,
} = renderers;

// 从扩展功能中解构保留的功能
const { isGeneratingPreview, handleDownload, handleS3DirectPreview, getCurrentDirectoryPath, isCreatingShare, handleCreateShare } = extensions;

// 智能初始模式计算属性
const smartInitialMode = computed(() => {
  if (!props.file?.name) return "text";
  return getPreviewModeFromFilename(props.file.name);
});

// 文本预览状态管理
const textPreviewMode = ref("text");
const textEncoding = ref("utf-8");
const textPreviewRef = ref(null);
const userHasManuallyChanged = ref(false);

// 使用文件保存composable
const { isSaving, saveFile } = useFileSave();

// 编辑权限控制
const canEdit = computed(() => {
  // 管理员总是可以编辑
  if (authStore.isAdmin) {
    return true;
  }

  // API密钥用户需要有上传权限和路径权限
  return authStore.hasMountUploadPermission && authStore.hasPathPermission(props.file?.path || "");
});

// 可用的预览模式选项
const availablePreviewModes = computed(() => {
  const modes = [
    { value: PREVIEW_MODES.TEXT, label: "Text" },
    { value: PREVIEW_MODES.CODE, label: "Code" },
    { value: PREVIEW_MODES.MARKDOWN, label: "Markdown" },
    { value: PREVIEW_MODES.HTML, label: "HTML" },
  ];

  // 根据权限添加编辑模式
  if (canEdit.value) {
    modes.push({ value: PREVIEW_MODES.EDIT, label: "Edit" });
  }

  return modes;
});

// 可用的编码选项
const availableEncodings = computed(() => {
  // 返回所有支持的编码
  return SUPPORTED_ENCODINGS;
});

// 内容区域全屏状态管理
const isContentFullscreen = ref(false);
const previewContentRef = ref(null);

// 动态计算文本预览的最大高度
const dynamicMaxHeight = computed(() => {
  if (isContentFullscreen.value) {
    // 全屏模式下：100vh减去工具栏高度(60px)
    return "calc(100vh - 60px)";
  } else {
    // 普通模式下：保持原有的600px限制
    return 600;
  }
});

// 简洁的全屏切换实现
const toggleFullscreen = () => {
  if (!document.fullscreenElement) {
    // 进入全屏
    previewContentRef.value?.requestFullscreen();
  } else {
    // 退出全屏
    document.exitFullscreen();
  }
};

// 保存文件功能
const handleSaveFile = async () => {
  if (!textPreviewRef.value || !textPreviewRef.value.getValue) {
    console.error("无法获取编辑器内容");
    emit("show-message", {
      type: "error",
      message: t("mount.filePreview.cannotGetEditorContent"),
    });
    return;
  }

  // 获取编辑器最新内容
  const content = textPreviewRef.value.getValue();

  // 使用composable保存文件
  const result = await saveFile(props.file.path, props.file.name, content, getCurrentDirectoryPath());

  if (result.success) {
    // 显示成功消息
    emit("show-message", {
      type: "success",
      message: result.message,
    });

    // 触发文件更新事件，通知父组件刷新
    emit("updated", {
      file: props.file,
      action: "save",
      result: result.data,
    });
  } else {
    // 显示错误消息
    emit("show-message", {
      type: "error",
      message: result.message,
    });
  }
};

// 添加缺失的计算属性
const isMarkdown = computed(() => {
  // 检查文件扩展名是否为markdown
  const filename = props.file?.name || "";
  const ext = filename.split(".").pop()?.toLowerCase();
  return ["md", "markdown", "mdown", "mkd"].includes(ext);
});

// 压缩文件检测
const isArchive = computed(() => {
  return props.file?.name && isArchiveFile(props.file.name);
});

// 监听模式变化
watch(textPreviewMode, (newMode) => {
  console.log("模式切换到:", newMode);
  // 通过ref调用TextPreview组件的方法
  if (textPreviewRef.value) {
    textPreviewRef.value.switchMode(newMode);
  }
});

// 监听编码变化
watch(textEncoding, (newEncoding) => {
  console.log("编码切换到:", newEncoding);
  // 通过ref调用TextPreview组件的方法
  if (textPreviewRef.value) {
    textPreviewRef.value.switchEncoding(newEncoding);
  }
});

// 文本预览事件处理（保留用于其他地方调用）
const handleModeChange = (newMode) => {
  textPreviewMode.value = newMode;
  userHasManuallyChanged.value = true; // 标记用户已手动切换
};

const handleEncodingChange = (newEncoding) => {
  textEncoding.value = newEncoding;
};

// 监听文件变化，智能设置初始预览模式
watch(
  () => props.file,
  (newFile, oldFile) => {
    if (newFile) {
      // 如果是新文件或文件名发生变化，重置用户手动切换标记并设置智能模式
      if (!oldFile || (oldFile && newFile.name !== oldFile.name)) {
        userHasManuallyChanged.value = false;
        textPreviewMode.value = smartInitialMode.value;
        console.log(`文件变化，智能设置预览模式: ${newFile.name} → ${smartInitialMode.value}`);
      }
    }
  },
  { immediate: true }
);

// 组件生命周期
onMounted(() => {
  console.log("FilePreview组件已挂载");

  // 监听全屏变化
  document.addEventListener("fullscreenchange", () => {
    isContentFullscreen.value = !!document.fullscreenElement;
  });
});

onBeforeUnmount(() => {
  // 退出全屏状态（如果处于全屏中）
  if (document.fullscreenElement) {
    document.exitFullscreen().catch(console.error);
  }
});
</script>

<style scoped>
/* 代码高亮和Markdown预览样式 */
.preview-content pre {
  margin: 0;
  padding: 1rem;
  border-radius: 0.5rem;
  overflow: auto;
}

.preview-content code {
  font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
  font-size: 0.875rem;
  line-height: 1.5;
}

/* 全屏模式样式 */
:deep(:fullscreen) {
  background-color: white;
  padding: 0;
  overflow: auto;
  border: none !important;
  border-radius: 0 !important;
}

:deep(.dark :fullscreen) {
  background-color: #1f2937;
}

:deep(:fullscreen iframe) {
  height: calc(100vh - 60px);
  width: 100%;
  border: none;
}

/* 确保全屏模式下的控制栏固定在顶部 */
:deep(:fullscreen .sticky) {
  position: sticky;
  top: 0;
  z-index: 20;
  width: 100%;
}

/* 全屏模式下的工具栏样式 */
:deep(:fullscreen .fullscreen-toolbar) {
  position: sticky;
  top: 0;
  z-index: 21;
  background-color: inherit;
}

/* 全屏模式下的文本容器 */
:deep(:fullscreen .fullscreen-text-container) {
  height: calc(100vh - 60px);
  display: flex;
  flex-direction: column;
}

/* 全屏模式下工具栏的暗色主题适配 */
:deep(:fullscreen .fullscreen-toolbar.bg-gray-800) {
  background-color: #1f2937 !important;
  border-color: #374151 !important;
}

:deep(:fullscreen .fullscreen-toolbar.bg-white) {
  background-color: #ffffff !important;
  border-color: #e5e7eb !important;
}

/* 全屏按钮悬停效果增强 */
button:hover svg {
  transform: scale(1.05);
  transition: transform 0.2s ease;
}

/* Markdown预览样式 */
.markdown-preview {
  line-height: 1.6;
}

/* Vditor相关样式 */
:deep(.vditor-reset) {
  font-size: 1rem;
  line-height: 1.7;
  padding: 0.5rem;
  transition: all 0.3s ease;
  color: v-bind('props.darkMode ? "#d4d4d4" : "#374151"');
  background-color: transparent !important;
}

/* 确保暗色模式下的特定样式 */
:deep(.vditor-reset--dark) {
  color: #d4d4d4 !important;
  background-color: transparent !important;
}

/* 确保亮色模式下的特定样式 */
:deep(.vditor-reset--light) {
  color: #374151 !important;
  background-color: transparent !important;
}

/* 标题样式 */
:deep(.vditor-reset h1, .vditor-reset h2) {
  border-bottom: 1px solid v-bind('props.darkMode ? "#30363d" : "#e5e7eb"');
  padding-bottom: 0.3em;
  margin-top: 1.8em;
  margin-bottom: 1em;
}

:deep(.vditor-reset h1) {
  font-size: 2em;
}

:deep(.vditor-reset h2) {
  font-size: 1.6em;
}

:deep(.vditor-reset h3) {
  font-size: 1.4em;
  margin-top: 1.5em;
  margin-bottom: 0.75em;
}

/* 段落样式 */
:deep(.vditor-reset p) {
  margin-top: 0.75em;
  margin-bottom: 0.75em;
}

/* 行内代码样式 */
:deep(.vditor-reset code:not(.hljs)) {
  background-color: v-bind('props.darkMode ? "#252526" : "#f3f4f6"');
  color: v-bind('props.darkMode ? "#ce9178" : "#ef4444"');
  padding: 0.2em 0.4em;
  border-radius: 3px;
  font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
}

/* 引用块样式 */
:deep(.vditor-reset blockquote) {
  border-left: 4px solid v-bind('props.darkMode ? "#4b5563" : "#e5e7eb"');
  padding: 0.5em 1em;
  margin-left: 0;
  margin-right: 0;
  margin-top: 1em;
  margin-bottom: 1em;
  color: v-bind('props.darkMode ? "#9ca3af" : "#6b7280"');
  background-color: v-bind('props.darkMode ? "#1a1a1a" : "#f9fafb"');
  border-radius: 0.25rem;
}

/* 链接样式 */
:deep(.vditor-reset a) {
  color: v-bind('props.darkMode ? "#3b82f6" : "#2563eb"');
  text-decoration: none;
}

:deep(.vditor-reset a:hover) {
  text-decoration: underline;
}

/* 表格样式 */
:deep(.vditor-reset table) {
  border-collapse: collapse;
  margin: 1.25em 0;
  width: 100%;
}

:deep(.vditor-reset table th, .vditor-reset table td) {
  border: 1px solid v-bind('props.darkMode ? "#30363d" : "#e5e7eb"');
  padding: 0.6em 1em;
}

:deep(.vditor-reset table th) {
  background-color: v-bind('props.darkMode ? "#252526" : "#f3f4f6"');
  font-weight: 600;
  color: v-bind('props.darkMode ? "#e2e8f0" : "#374151"');
}

:deep(.vditor-reset table td) {
  background-color: v-bind('props.darkMode ? "#1e1e1e" : "#ffffff"');
}

:deep(.vditor-reset table tr:nth-child(even) td) {
  background-color: v-bind('props.darkMode ? "#252526" : "#f9fafb"');
}

/* 列表样式 */
:deep(.vditor-reset ul, .vditor-reset ol) {
  padding-left: 2em;
  margin: 1em 0;
}

/* 图片样式 */
:deep(.vditor-reset img) {
  max-width: 100%;
  margin: 1.25em 0;
  border-radius: 0.25rem;
}

/* 针对暗色模式的自定义样式 */
:deep(.hljs) {
  background: transparent !important;
}

/* 代码块在暗色模式下的样式 */
:deep(.vditor-reset--dark pre) {
  background-color: #1e1e1e !important;
  border: 1px solid #333 !important;
}

:deep(.vditor-reset--dark code.hljs) {
  background-color: #1e1e1e !important;
  color: #d4d4d4 !important;
}

/* 代码块在亮色模式下的样式 */
:deep(.vditor-reset--light pre) {
  background-color: #f6f8fa !important;
  border: 1px solid #e5e7eb !important;
}

:deep(.vditor-reset--light code.hljs) {
  background-color: #f6f8fa !important;
  color: #24292e !important;
}

/* 响应式调整 */
@media (max-width: 640px) {
  :deep(.vditor-reset) {
    font-size: 15px;
    padding: 0.25rem;
  }
}

/* 移动端文件预览容器优化 */
@media (max-width: 768px) {
  .file-preview-container {
    margin: 0 -1rem;
    padding: 0 0.5rem;
  }

  .file-preview {
    margin-bottom: 1rem !important;
    padding: 0.75rem !important;
    border-radius: 0.5rem !important;
  }

  /* 文件信息网格在移动端单列显示 */
  .file-info {
    grid-template-columns: 1fr !important;
    gap: 0.5rem !important;
    padding: 0.75rem !important;
  }

  /* 按钮组在移动端更紧凑 */
  .file-preview .flex.flex-wrap {
    gap: 0.5rem !important;
  }

  /* 预览内容区域优化 */
  .preview-content {
    border-radius: 0.5rem !important;
  }

  /* PDF预览高度调整 */
  .pdf-preview {
    height: 60vh !important;
  }

  /* Office预览高度调整 */
  .office-preview {
    height: 65vh !important;
  }

  /* Markdown预览高度调整 */
  .markdown-preview {
    max-height: 60vh !important;
    padding: 0.75rem !important;
  }
}

/* 全局确保代码高亮在暗色模式下可见  */
:deep(.hljs-comment) {
  color: #6a9955 !important;
}
:deep(.hljs-keyword) {
  color: #569cd6 !important;
}
:deep(.hljs-attribute) {
  color: #9cdcfe !important;
}
:deep(.hljs-literal) {
  color: #569cd6 !important;
}
:deep(.hljs-symbol) {
  color: #b5cea8 !important;
}
:deep(.hljs-name) {
  color: #569cd6 !important;
}
:deep(.hljs-tag) {
  color: #569cd6 !important;
}
:deep(.hljs-string) {
  color: #ce9178 !important;
}
:deep(.hljs-number) {
  color: #b5cea8 !important;
}
:deep(.hljs-title) {
  color: #dcdcaa !important;
}
:deep(.hljs-built_in) {
  color: #4ec9b0 !important;
}
:deep(.hljs-class) {
  color: #4ec9b0 !important;
}
:deep(.hljs-variable) {
  color: #9cdcfe !important;
}
:deep(.hljs-params) {
  color: #9cdcfe !important;
}
:deep(.hljs-meta) {
  color: #db8942 !important;
}

/* 上面是旧的 */

/* 全屏模式样式 */
.preview-content-fullscreen {
  background-color: var(--bg-color);
}

.fullscreen-toolbar {
  flex-shrink: 0;
}

.fullscreen-text-container {
  height: calc(100vh - 60px); /* 减去工具栏高度 */
  display: flex;
  flex-direction: column;
}

.fullscreen-text-container :deep(.text-preview-wrapper) {
  height: 100%;
  flex: 1;
}

.fullscreen-text-container :deep(.editor-container) {
  height: 100%;
  min-height: unset;
}

/* 确保Monaco编辑器在全屏模式下正确显示 */
.preview-content-fullscreen :deep(.monaco-editor) {
  height: 100% !important;
}

/* 暗色主题变量 */
:root {
  --bg-color: #ffffff;
}

.dark {
  --bg-color: #1f2937;
}
</style>
