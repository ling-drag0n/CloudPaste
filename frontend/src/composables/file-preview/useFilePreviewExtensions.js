/**
 * 文件预览扩展功能 Composable
 * 专注交互功能（编辑、保存、下载等）
 */

import { ref, onMounted, onUnmounted } from "vue";
import { useI18n } from "vue-i18n";
import { createLogger } from "@/utils/logger.js";

export function useFilePreviewExtensions(
  file,
  authInfo,
  officePreviewLoading,
  officePreviewError,
  officePreviewTimedOut,
  previewUrl,
  handleFullscreenChange,
  handleKeyDown,
  emit,
  authenticatedPreviewUrl,
  previewTimeoutId
) {
  const { t } = useI18n();
  const log = createLogger("PreviewExtensions");

  // ===== Office预览处理 =====

  /**
   * Office预览加载完成处理
   */
  const handleOfficePreviewLoaded = () => {
    officePreviewLoading.value = false;
    officePreviewError.value = "";
    officePreviewTimedOut.value = false;
    log.debug("Office预览加载完成");
  };

  /**
   * Office预览加载错误处理
   */
  const handleOfficePreviewError = (error) => {
    log.error("Office预览加载错误:", error);

    // 清除加载状态
    officePreviewLoading.value = false;
    officePreviewTimedOut.value = false;

    // 设置错误信息
    if (error && error.message) {
      officePreviewError.value = error.message;
    } else {
      officePreviewError.value = t("mount.filePreview.previewError");
    }

    log.debug("Office预览错误处理完成");
  };

  // ===== 编辑模式处理已移除 =====

  // ===== 音频播放器事件处理 =====

  /**
   * 音频播放事件处理
   */
  const handleAudioPlay = (data) => {
    log.debug("音频开始播放:", data);
    // 可以在这里添加播放统计或其他逻辑
  };

  /**
   * 音频暂停事件处理
   */
  const handleAudioPause = (data) => {
    log.debug("音频暂停播放:", data);
    // 可以在这里添加暂停统计或其他逻辑
  };

  /**
   * 音频错误事件处理
   */
  const handleAudioError = (error) => {
    // 忽略Service Worker相关的误报错误（基于当前预览URL）
    if (error?.target?.src?.includes(window.location.origin) && previewUrl.value?.startsWith("https://")) {
      log.debug("忽略Service Worker相关的误报错误，音频实际可以正常播放");
      return;
    }

    log.error("音频播放错误:", error);
  };

  // ===== 其他功能 =====

  // S3直链预览状态
  const isGeneratingPreview = ref(false);

  /**
   * 处理下载按钮点击
   */
  const handleDownload = () => {
    emit("download", file.value);
  };

  /**
   * 处理S3直链预览
   */
  const handleS3DirectPreview = async () => {
    if (isGeneratingPreview.value) return;

    try {
      isGeneratingPreview.value = true;
      log.debug("开始生成直链/代理预览...");

      const baseUrl = previewUrl.value;
      if (!baseUrl) {
        throw new Error("当前文件缺少可用的预览URL");
      }

      log.debug("直链/代理预览使用原始URL:", baseUrl);
      window.open(baseUrl, "_blank");
      log.debug("预览成功");
      return;
    } catch (error) {
      log.error("S3直链预览失败:", error);
      emit("show-message", {
        type: "error",
        message: t("mount.filePreview.s3PreviewError", { message: error.message }),
      });
    } finally {
      isGeneratingPreview.value = false;
    }
  };

  /**
   * 获取当前目录路径
   */
  const getCurrentDirectoryPath = () => {
    if (!file.value?.path) return "";

    // 从文件路径中提取目录路径
    const filePath = file.value.path;
    const lastSlashIndex = filePath.lastIndexOf("/");

    if (lastSlashIndex === -1) {
      return "/"; // 根目录
    }

    return filePath.substring(0, lastSlashIndex + 1);
  };

  // ===== 分享功能 =====

  const isCreatingShare = ref(false);

  /**
   * 处理创建分享链接：打开分享创建弹窗（由父组件统一处理）
   */
  const handleCreateShare = () => {
    if (!file.value || !file.value.path) {
      return;
    }
    emit("create-share", file.value);
  };

  // ===== 生命周期管理 =====

  /**
   * 组件挂载时的初始化
   */
  const initializeExtensions = () => {
    log.debug("文件预览扩展功能初始化完成");
  };

  /**
   * 组件卸载时的清理
   */
  const cleanupExtensions = () => {
    // 清理URL资源
    if (authenticatedPreviewUrl && authenticatedPreviewUrl.value) {
      URL.revokeObjectURL(authenticatedPreviewUrl.value);
      authenticatedPreviewUrl.value = null;
    }

    // 清除计时器
    if (previewTimeoutId && previewTimeoutId.value) {
      clearTimeout(previewTimeoutId.value);
      previewTimeoutId.value = null;
    }

    log.debug("文件预览扩展功能清理完成");
  };

  // 生命周期钩子
  onMounted(initializeExtensions);
  onUnmounted(cleanupExtensions);

  return {
    // Office预览处理
    handleOfficePreviewLoaded,
    handleOfficePreviewError,

    // 音频处理
    handleAudioPlay,
    handleAudioPause,
    handleAudioError,

    // 其他功能
    isGeneratingPreview,
    handleDownload,
    handleS3DirectPreview,
    getCurrentDirectoryPath,

    // 分享功能
    isCreatingShare,
    handleCreateShare,

    // 生命周期
    initializeExtensions,
    cleanupExtensions,
  };
}
