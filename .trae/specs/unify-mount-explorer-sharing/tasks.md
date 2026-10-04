# Tasks

- [x] Task 1: 数据层：为分享记录增加 `target_type` 并支撑文件夹分享
  - [x] SubTask 1.1: 在 [schema.js](file:///workspace/backend/src/db/migrations/sqlite/engine/schema.js) 的 `files` 建表语句新增 `target_type TEXT NOT NULL DEFAULT 'file'`；同步更新 [schema.sql](file:///workspace/backend/schema.sql) 及 MySQL/Postgres 方言建表（`backend/src/db/dialects/*`、`db/providers/*` 如涉及）
  - [x] SubTask 1.2: 在 [migrations.js](file:///workspace/backend/src/db/migrations/sqlite/engine/migrations.js) 新增 `case 35`（调用 `addTableField(db, DbTables.FILES, "target_type", "target_type TEXT NOT NULL DEFAULT 'file'")`），并把 [version.js](file:///workspace/backend/src/db/migrations/sqlite/engine/version.js) 的 `APP_SCHEMA_VERSION` 从 34 提升到 35
  - [x] SubTask 1.3: 在 [FileRepository.js](file:///workspace/backend/src/repositories/FileRepository.js) 的 `createFile`/`updateFile`/查询中透传与返回 `target_type`，并新增/复用原子递增下载计数方法（`incrementViews` 复用或新增 `incrementDownloads`）
  - [x] SubTask 1.4: 本地验证：新建库与旧库升级后 `files` 表均包含 `target_type`，既有查询不报错

- [x] Task 2: 后端分享创建支持文件夹与可选限制
  - [x] SubTask 2.1: 扩展 [search_share.js](file:///workspace/backend/src/routes/fs/search_share.js) 的 `POST /api/fs/create-share`，接收 `{ path, password, expires_in, max_views, remark, slug }` 并透传（保留 `fs.share.create` 策略与 pathResolver）
  - [x] SubTask 2.2: 改造 [fileShareService.js](file:///workspace/backend/src/services/fileShareService.js) 的 `createShareFromFileSystem`：移除对目录的拒绝，按 `FileSystem.getFileInfo` 自动判定 `target_type`（文件夹写入目录前缀路径、`size=0`、`mimetype='inode/directory'`），并把 password/expires_in/max_views/remark/slug 传给 [ShareRecordService.js](file:///workspace/backend/src/services/share/ShareRecordService.js) 的 `createShareRecord`
  - [x] SubTask 2.3: 在 `ShareRecordService.createShareRecord` 写入 `target_type`
  - [x] SubTask 2.4: 本地验证：对文件与文件夹分别调用创建接口，返回记录含正确 `target_type`、slug 与限制字段

- [x] Task 3: 后端访问校验与下载计数改造
  - [x] SubTask 3.1: 在 [fileService.js](file:///workspace/backend/src/services/fileService.js) 统一 `validateFileAccess`：过期与下载上限判定改为 `views >= max_views`（上限>0 时）；新增/扩展 `guardShareFolder`（返回目录分享记录并校验过期/上限）
  - [x] SubTask 3.2: 将 `/api/share/get/:slug`（[files/public.js](file:///workspace/backend/src/routes/files/public.js)）与 `/api/share/verify/:slug` 改为**不递增**计数（仅校验），打开页面不消耗次数
  - [x] SubTask 3.3: 在下载入口 [fileViewRoutes.js](file:///workspace/backend/src/routes/fileViewRoutes.js) 的 `handleShareDelivery`（`/api/s/:slug` 及 `?down=true` 等下载语义）成功后原子递增下载计数并复检上限，超限返回 410 并清理记录
  - [x] SubTask 3.4: 在 [fileService.js](file:///workspace/backend/src/services/fileService.js) 的 `getPublicFileInfo` 中：当 `max_views>0` 时强制 `downloadUrl` 走后端代理 `/api/s/:slug?down=true`（避免直链绕过计数）
  - [x] SubTask 3.5: 本地验证：无密码分享下载 N 次后第 N+1 次返回 410；仅打开页面不计数

- [x] Task 4: 后端文件夹分享公开接口
  - [x] SubTask 4.1: 新增文件夹分享公开路由（建议新建 `backend/src/routes/files/folderPublic.js` 并注册）：`GET /api/share/folder/:slug`（元信息，未设密码时含根目录列表）、`POST /api/share/folder/verify/:slug`（校验密码并返回根列表）、`GET /api/share/folder/:slug/list?path=<rel>&password=`（子目录列表）、`GET /api/share/folder/:slug/download?path=<rel>&password=`（流式下载单个文件）
  - [x] SubTask 4.2: 通过 `storage_config_id` + `MountManager`/`FileSystem` 解析目录并 `listDirectory`；对 `path` 做规范化与越权校验（禁止 `..`/绝对路径，解析后必须位于分享根目录内）
  - [x] SubTask 4.3: 下载接口强制后端代理并复用计数逻辑（成功下载 +1，超限 410）
  - [x] SubTask 4.4: 本地验证：列目录、进入子目录、越权 path 被拒、下载计数累计

- [x] Task 5: 前台合并入口（移除上传页）
  - [x] SubTask 5.1: [router/index.js](file:///workspace/frontend/src/router/index.js) 移除 `/upload` 页面路由，改为重定向到 `/mount-explorer`；更新 `isPublicEntryDisabled`/`getFallbackRoute`/`getDefaultRouteForUser`/标题 switch/`routerUtils.routeMap` 中的 Upload 处理
  - [x] SubTask 5.2: [App.vue](file:///workspace/frontend/src/App.vue) 移除「文件上传」导航入口（含移动端菜单），保留首页/挂载浏览/管理
  - [x] SubTask 5.3: 将 `siteUploadPageEnabled` 的消费点改为控制挂载浏览上传能力；挂载浏览中据此隐藏上传入口
  - [x] SubTask 5.4: 移除 [UploadView.vue](file:///workspace/frontend/src/modules/upload/public/UploadView.vue) 及其无引用专用组件，保留被复用的通用组件
  - [x] SubTask 5.5: 验证：访问 `/upload` 跳转到挂载浏览；导航不再出现上传入口；无残留引用报错

- [x] Task 6: 挂载浏览分享创建 UI
  - [x] SubTask 6.1: 新增分享创建弹窗组件（如 `frontend/src/modules/fs/components/shared/modals/ShareCreateModal.vue`）：目标（文件/文件夹）展示、备注、密码、有效期、下载次数、自定义 slug（复用 [useShareSettingsForm.js](file:///workspace/frontend/src/composables/upload/useShareSettingsForm.js) 思路与校验），提交后展示结果链接（复用 [ShareLinkBox.vue](file:///workspace/frontend/src/components/common/ShareLinkBox.vue) 与二维码弹窗）
  - [x] SubTask 6.2: 扩展 [fsService.js](file:///workspace/frontend/src/api/services/fsService.js) 的 `createShareFromFileSystem(path)` 为 `createShareFromFileSystem(path, options)`，携带限制参数
  - [x] SubTask 6.3: 在 [useContextMenu.js](file:///workspace/frontend/src/composables/useContextMenu.js)、[FloatingActionBar.vue](file:///workspace/frontend/src/modules/fs/components/shared/FloatingActionBar.vue)、[FileItem.vue](file:///workspace/frontend/src/modules/fs/components/directory/FileItem.vue) 增加「生成分享链接」动作（文件与文件夹均可用）
  - [x] SubTask 6.4: 改造 [FilePreview.vue](file:///workspace/frontend/src/modules/fs/components/preview/FilePreview.vue) 的分享按钮：从「直接复制无限制链接」改为打开分享创建弹窗
  - [x] SubTask 6.5: 在 [MountExplorerView.vue](file:///workspace/frontend/src/modules/fs/MountExplorerView.vue) 接线弹窗状态与事件
  - [x] SubTask 6.6: 验证：文件与文件夹均可生成链接，限制字段写入后端

- [x] Task 7: 上传即生成分享链接（迁移到挂载浏览）
  - [x] SubTask 7.1: 在 [UppyUploadModal.vue](file:///workspace/frontend/src/modules/fs/components/shared/modals/UppyUploadModal.vue) 增加「上传并生成分享链接」开关与选项区（备注/密码/有效期/下载次数）
  - [x] SubTask 7.2: 上传成功后按上传得到的文件路径调用 `createShareFromFileSystem`，结果面板展示链接；单个失败不影响其他文件
  - [x] SubTask 7.3: 验证：开启后上传 N 个文件生成 N 条分享；关闭后行为与现状一致

- [x] Task 8: 公开文件夹分享页
  - [x] SubTask 8.1: 新增路由 `/folder/:slug` → `frontend/src/modules/fileshare/public/FolderShareView.vue`（[router/index.js](file:///workspace/frontend/src/router/index.js)）
  - [x] SubTask 8.2: 在 [fileshareService.js](file:///workspace/frontend/src/modules/fileshare/fileshareService.js) / `api/services/fileGateway.js` 增加文件夹分享 API 封装（meta/verify/list/download）
  - [x] SubTask 8.3: 实现只读目录浏览：密码门（参考 [FileViewPassword.vue](file:///workspace/frontend/src/modules/fileshare/public/components/FileViewPassword.vue)）、面包屑、条目列表（文件夹可进入、文件可预览/下载）、展示剩余下载次数与过期时间、受限时的错误态（过期/次数用尽）
  - [x] SubTask 8.4: [FileView.vue](file:///workspace/frontend/src/modules/fileshare/public/FileView.vue) 加载到 `target_type==='folder'` 时跳转 `/folder/:slug`
  - [x] SubTask 8.5: 验证：无密码/有密码/过期/次数用尽四种场景均表现正确

- [x] Task 9: 管理端文件管理适配
  - [x] SubTask 9.1: [FileManagementView.vue](file:///workspace/frontend/src/modules/fileshare/admin/FileManagementView.vue) / [FileTable.vue](file:///workspace/frontend/src/modules/fileshare/admin/components/FileTable.vue) 增加「类型（文件/文件夹）」列，文案「最大访问次数」改为「最大下载次数」
  - [x] SubTask 9.2: 确保 [FileEditModal.vue](file:///workspace/frontend/src/components/file/FileEditModal.vue) 可编辑文件夹分享的密码/有效期/下载次数
  - [x] SubTask 9.3: 验证：文件夹分享出现在列表且可编辑限制

- [x] Task 10: 国际化与收尾
  - [x] SubTask 10.1: 补充 `zh-CN`/`en-US` 文案（分享创建弹窗、文件夹分享页、下载次数相关标签、上传并分享）
  - [x] SubTask 10.2: 全量自检：跑前后端构建/静态检查，回归现有文件分享与挂载浏览核心路径，确认无新增 bug

- [x] Task 11: 修正密码校验的状态码（对齐 spec）
  - [x] SubTask 11.1: [files/public.js](file:///workspace/backend/src/routes/files/public.js) 两处 `AuthorizationError("密码不正确")` 改为 `AuthenticationError`（401），未提供密码与密码错误均返回 401
  - [x] SubTask 11.2: [folderPublic.js](file:///workspace/backend/src/routes/files/folderPublic.js) 的 `ensurePassword` 中 `"需要密码访问"` / `"密码不正确"` 改为 `AuthenticationError`（401）
  - [x] SubTask 11.3: 验证：错误密码返回 401，正确密码成功，前端 FileViewPassword 归类为「密码错误」

# Task Dependencies
- Task 11 依赖 Task 3、Task 4
- Task 2 依赖 Task 1
- Task 3 依赖 Task 1
- Task 4 依赖 Task 1、Task 3
- Task 6 依赖 Task 2
- Task 7 依赖 Task 2、Task 6
- Task 8 依赖 Task 4
- Task 9 依赖 Task 1、Task 2
- Task 10 依赖 Task 5~9
- Task 5 与 Task 1~4 可并行
