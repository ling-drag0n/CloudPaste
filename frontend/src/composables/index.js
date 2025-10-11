/**
 * Composables 统一聚合导出
 */

// ===== 核心系统 =====
import { useGlobalMessage } from "./core/useGlobalMessage.js";

// ===== 文件预览功能 =====
import { usePreviewRenderers } from "./file-preview/usePreviewRenderers.js";
import { useFilePreviewExtensions } from "./file-preview/useFilePreviewExtensions.js";
import { useFilePreview } from "./file-preview/useFilePreview.js";

// ===== 压缩文件功能 =====
import { useArchivePreview } from "./archive/useArchivePreview.js";

// ===== 文件系统功能 =====
import { useFileOperations } from "./file-system/useFileOperations.js";
import { useDirectorySort } from "./file-system/useDirectorySort.js";
import { useFileBasket } from "./file-system/useFileBasket.js";
import { useFileSave } from "./file-system/useFileSave.js";

// ===== UI交互功能 =====
import { useSelection } from "./ui-interaction/useSelection.js";
import { useUIState } from "./ui-interaction/useUIState.js";
import { useGalleryView } from "./ui-interaction/useGalleryView.js";
import { usePhotoSwipe } from "./ui-interaction/usePhotoSwipe.js";

// 重新导出所有功能
export {
  useGlobalMessage,
  usePreviewRenderers,
  useFilePreviewExtensions,
  useFilePreview,
  useArchivePreview,
  useFileOperations,
  useDirectorySort,
  useFileBasket,
  useFileSave,
  useSelection,
  useUIState,
  useGalleryView,
  usePhotoSwipe,
};

// ===== 便捷的聚合导出 =====

/**
 * 文件预览完整功能聚合
 * 导出文件预览相关的所有功能
 */
export const FilePreviewComposables = {
  usePreviewRenderers,
  useFilePreviewExtensions,
  useFilePreview,
  useArchivePreview,
};

/**
 * 文件系统完整功能聚合
 * 导出文件系统相关的所有功能
 */
export const FileSystemComposables = {
  useFileOperations,
  useDirectorySort,
  useFileBasket,
  useFileSave,
};

/**
 * UI交互完整功能聚合
 * 导出UI交互相关的所有功能
 */
export const UIComposables = {
  useSelection,
  useUIState,
  useGalleryView,
  usePhotoSwipe,
};

// ===== 常量定义 =====

/**
 * 文件操作类型
 */
export const FILE_ACTIONS = {
  VIEW: "view",
  EDIT: "edit",
  DELETE: "delete",
  DOWNLOAD: "download",
  COPY: "copy",
  MOVE: "move",
  RENAME: "rename",
  SHARE: "share",
};

/**
 * 预览类型
 */
export const PREVIEW_TYPES = {
  IMAGE: "image",
  VIDEO: "video",
  AUDIO: "audio",
  PDF: "pdf",
  OFFICE: "office",
  TEXT: "text",
  CODE: "code",
  MARKDOWN: "markdown",
  HTML: "html",
  UNKNOWN: "unknown",
};

/**
 * UI状态类型
 */
export const UI_STATE_TYPES = {
  VIEW_MODES: {
    LIST: "list",
    GRID: "grid",
    DETAIL: "detail",
  },
  SELECTION_MODES: {
    SINGLE: "single",
    MULTIPLE: "multiple",
    NONE: "none",
  },
  LOADING_STATES: {
    IDLE: "idle",
    LOADING: "loading",
    SUCCESS: "success",
    ERROR: "error",
  },
};

/**
 * 排序字段和顺序
 */
export const SORT_CONFIG = {
  FIELDS: {
    NAME: "name",
    SIZE: "size",
    MODIFIED: "modified",
  },
  ORDERS: {
    DEFAULT: "default",
    ASC: "asc",
    DESC: "desc",
  },
};

// ===== 默认导出 =====
export default {
  // 单独的组合函数
  usePreviewRenderers,
  useFilePreviewExtensions,
  useFilePreview,
  useFileOperations,
  useDirectorySort,
  useFileBasket,
  useSelection,
  useUIState,
  useGalleryView,
  usePhotoSwipe,

  // 聚合对象
  FilePreviewComposables,
  FileSystemComposables,
  UIComposables,

  // 常量
  FILE_ACTIONS,
  PREVIEW_TYPES,
  UI_STATE_TYPES,
  SORT_CONFIG,
};
