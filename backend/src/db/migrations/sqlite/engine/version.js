// SQLite/D1 schema 版本号（逻辑版本）
// 说明：
// - `app-v01..app-vNN` 的 NN 上限来自这里
// - 这里的版本号用于“迁移编排”，不与具体数据库方言强绑定

export const APP_SCHEMA_VERSION = 35;

// 兼容命名：历史代码中使用 DB_SCHEMA_VERSION
export const DB_SCHEMA_VERSION = APP_SCHEMA_VERSION;
