/**
 * 缓存模块统一导出
 * 提供所有缓存相关类和实例的统一入口
 */
// ==================== 基础缓存类 ====================
export { BaseCache, createCache } from "./BaseCache.js";

// ==================== 目录缓存 ====================
export { DirectoryCacheManager, directoryCacheManager, clearDirectoryCache } from "./DirectoryCache.js";

// ==================== S3 URL缓存 ====================
export { S3UrlCacheManager, s3UrlCacheManager, clearS3UrlCache } from "./S3UrlCache.js";

// ==================== 搜索缓存 ====================
export { SearchCacheManager, searchCacheManager, clearSearchCache } from "./SearchCache.js";

// ==================== 预览设置缓存 ====================
export { PreviewSettingsCache, previewSettingsCache } from "./PreviewSettingsCache.js";

// ==================== 兼容导出 ====================
// 兼容的默认导出
export { default as previewSettingsCacheDefault } from "./PreviewSettingsCache.js";
