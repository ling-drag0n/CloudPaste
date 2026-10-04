# 文件上传与挂载浏览合并 + 挂载浏览分享链接 Spec

## Why
当前前台把「文件上传」（`/upload`，上传即分享）与「挂载浏览」（`/mount-explorer`，浏览挂载盘）拆成两个页面，能力割裂且入口重复；同时挂载浏览里对文件生成的分享链接**不能设置密码/有效期/下载次数**，也**完全不支持文件夹分享**。

需要：合并两者并保留挂载浏览，在挂载浏览内统一支持「生成文件分享链接」和「生成文件夹分享链接」，链接支持**密码验证、有效期（时间）、下载次数限制**，任一限制到达即失效。

## What Changes
- 移除独立「文件上传」页面与导航入口；`/upload` 重定向到 `/mount-explorer`。**BREAKING**（前台入口变化）
- 挂载浏览新增统一的「生成分享链接」能力：文件、文件夹均可创建分享，支持备注、自定义 slug、密码、有效期（小时）、下载次数上限。
- 迁移「上传即生成分享链接」流程：挂载浏览的上传弹窗可选开启分享，上传完成后按设定生成分享链接并展示（密码/有效期/下载次数）。
- 后端支持文件夹分享（`files.target_type = 'file' | 'folder'`），新增公开只读文件夹分享页 `/folder/:slug`（列目录、进入子目录、预览/下载单个文件）。
- 计数语义调整：分享计数仅在**实际下载文件**时 +1（文件分享与文件夹分享统一累计）；打开分享页不再消耗次数；达到上限或超期即失效。**BREAKING**（现有「最大访问次数」语义改为「最大下载次数」）
- 管理端「文件管理」展示分享类型（文件/文件夹）与「最大下载次数」，并可编辑限制。

## Impact
- Affected specs: 前台入口与导航、挂载浏览、文件分享、公开分享页、文件管理（admin）、数据库 schema 与迁移
- Affected code（关键文件）:
  - 前台路由/导航：[router/index.js](file:///workspace/frontend/src/router/index.js)、[App.vue](file:///workspace/frontend/src/App.vue)
  - 上传页（移除）：[UploadView.vue](file:///workspace/frontend/src/modules/upload/public/UploadView.vue) 及其专用组件
  - 挂载浏览：[MountExplorerView.vue](file:///workspace/frontend/src/modules/fs/MountExplorerView.vue)、[useContextMenu.js](file:///workspace/frontend/src/composables/useContextMenu.js)、[FloatingActionBar.vue](file:///workspace/frontend/src/modules/fs/components/shared/FloatingActionBar.vue)、[FileItem.vue](file:///workspace/frontend/src/modules/fs/components/directory/FileItem.vue)、[FilePreview.vue](file:///workspace/frontend/src/modules/fs/components/preview/FilePreview.vue)、[UppyUploadModal.vue](file:///workspace/frontend/src/modules/fs/components/shared/modals/UppyUploadModal.vue)
  - 分享页：[FileView.vue](file:///workspace/frontend/src/modules/fileshare/public/FileView.vue)、[fileshareService.js](file:///workspace/frontend/src/modules/fileshare/fileshareService.js)、[fsService.js](file:///workspace/frontend/src/api/services/fsService.js)
  - 管理端：[FileManagementView.vue](file:///workspace/frontend/src/modules/fileshare/admin/FileManagementView.vue)、[FileEditModal.vue](file:///workspace/frontend/src/components/file/FileEditModal.vue)
  - 后端：[search_share.js](file:///workspace/backend/src/routes/fs/search_share.js)、[fileShareService.js](file:///workspace/backend/src/services/fileShareService.js)、[ShareRecordService.js](file:///workspace/backend/src/services/share/ShareRecordService.js)、[fileService.js](file:///workspace/backend/src/services/fileService.js)、[files/public.js](file:///workspace/backend/src/routes/files/public.js)、[fileViewRoutes.js](file:///workspace/backend/src/routes/fileViewRoutes.js)、[FileRepository.js](file:///workspace/backend/src/repositories/FileRepository.js)
  - 数据层：[schema.js](file:///workspace/backend/src/db/migrations/sqlite/engine/schema.js)、[migrations.js](file:///workspace/backend/src/db/migrations/sqlite/engine/migrations.js)、[version.js](file:///workspace/backend/src/db/migrations/sqlite/engine/version.js)、[schema.sql](file:///workspace/backend/schema.sql)
  - i18n：`frontend/src/i18n/locales/{zh-CN,en-US}/**`

## ADDED Requirements

### Requirement: 挂载浏览内生成分享链接（文件与文件夹）
系统 SHALL 在挂载浏览中提供统一的分享创建入口，对**文件**与**文件夹**均可生成分享链接，并支持以下可选项：备注（remark）、自定义 slug、访问密码、有效期（小时，0 表示永久）、下载次数上限（0 表示不限）。

#### Scenario: 为文件生成带限制的分享链接
- **WHEN** 用户在挂载浏览对某个文件触发「生成分享链接」，填写密码 "abc123"、有效期 24 小时、下载次数上限 5 并提交
- **THEN** 后端创建分享记录（`target_type='file'`，写入 `password` 哈希、`expires_at`、`max_views=5`），返回分享链接（`/file/:slug`）
- **AND** 前端展示该链接、可复制、可显示二维码，并展示已设置的密码/有效期/下载次数

#### Scenario: 为文件夹生成分享链接
- **WHEN** 用户对某个文件夹触发「生成分享链接」并提交选项
- **THEN** 后端创建分享记录（`target_type='folder'`，`storage_path` 指向该目录前缀，`filename` 为目录名），返回分享链接（`/folder/:slug`）

#### Scenario: 不填写任何限制
- **WHEN** 用户不填密码、有效期与下载次数
- **THEN** 生成永久、无密码、不限下载次数的分享链接

### Requirement: 文件夹分享的公开只读访问页
系统 SHALL 提供公开只读的文件夹分享页 `/folder/:slug`：访客可查看该目录及其子目录内容，并对其中单个文件进行预览与下载；不能进行任何写操作。

#### Scenario: 打开无密码的文件夹分享
- **WHEN** 访客访问 `/folder/:slug` 且该分享无密码、未过期、未达下载上限
- **THEN** 返回目录元信息（名称、剩余下载次数、过期时间）与根目录条目列表

#### Scenario: 进入子目录
- **WHEN** 访客点击列表中的子文件夹
- **THEN** 展示该子目录内容，且路径被限制在分享根目录之内

#### Scenario: 越权路径访问
- **WHEN** 请求的 `path` 通过 `..`、绝对路径或编码绕出分享根目录
- **THEN** 后端拒绝请求（400/403），不返回根目录以外的任何内容

#### Scenario: 单文件预览与下载
- **WHEN** 访客在文件夹分享页点击某文件的预览或下载
- **THEN** 通过后端代理返回文件内容（预览 `inline`、下载 `attachment`）

### Requirement: 分享链接的密码验证
系统 SHALL 在访问受密码保护的分享链接前要求验证密码，验证通过后才返回内容；文件分享与文件夹分享行为一致。

#### Scenario: 未提供密码
- **WHEN** 访客访问受密码保护的 `/file/:slug` 或 `/folder/:slug` 且未提供密码
- **THEN** 返回 `requires_password=true`，且响应中不包含任何可直接访问的下载/预览 URL

#### Scenario: 密码错误
- **WHEN** 访客提交错误密码
- **THEN** 返回 401 且不返回内容

#### Scenario: 密码正确
- **WHEN** 访客提交正确密码
- **THEN** 返回目录/文件信息；受计数限制时下载 URL 走后端代理并携带密码参数

### Requirement: 有效期限制
系统 SHALL 依据分享记录的 `expires_at` 判定有效性；当前时间超过 `expires_at` 时，分享链接失效。

#### Scenario: 分享已过期
- **WHEN** 访客访问 `expires_at` 已过去的分享链接
- **THEN** 返回 410（GONE），前端展示「链接已过期」

### Requirement: 下载次数限制与到达限制即失效
系统 SHALL 在**每次实际下载文件**时将该分享的下载计数 `+1`；当计数达到上限（`max_views>0` 且 `views>=max_views`）时分享失效。文件夹分享累计其内所有被下载文件的次数。

#### Scenario: 文件分享下载计数
- **WHEN** 分享设置下载次数上限为 3，访客成功下载该文件 3 次
- **THEN** 第 4 次访问/下载返回 410（GONE）

#### Scenario: 打开分享页不消耗次数
- **WHEN** 访客仅打开分享页或预览但不下载
- **THEN** 下载计数不增加

#### Scenario: 文件夹分享累计计数
- **WHEN** 文件夹分享上限为 5，访客分别下载了其中 5 个文件
- **THEN** 该文件夹分享整体失效，后续任意访问返回 410

#### Scenario: 达到上限后记录被清理
- **WHEN** 分享计数达到上限
- **THEN** 分享记录被删除（引用挂载盘的分享仅删除记录，不删除网盘中的真实文件/文件夹）

### Requirement: 计数不可被直链绕过
系统 SHALL 在分享设置了下载次数上限时，强制其下载入口经由后端代理（本地内容路由），使计数无法通过存储直链被绕过。

#### Scenario: 设置了下载上限的分享
- **WHEN** 分享设置了 `max_views>0`
- **THEN** 其 `downloadUrl` 指向后端代理入口（`/api/s/:slug?down=true` 或文件夹分享下载入口），而非存储直链

### Requirement: 上传即生成分享链接（迁移至挂载浏览）
系统 SHALL 在挂载浏览的上传弹窗中提供「上传并生成分享链接」选项，允许在上传前设置备注/密码/有效期/下载次数，上传完成后为每个上传成功的文件生成对应分享链接。

#### Scenario: 开启上传并分享
- **WHEN** 用户开启该选项并完成上传
- **THEN** 上传完成后自动为每个新文件创建分享记录，并在结果面板展示分享链接（可复制/二维码）

#### Scenario: 上传但分享失败
- **WHEN** 文件上传成功但创建分享记录失败
- **THEN** 上传结果保留，按文件提示分享失败原因，不阻塞其他文件

### Requirement: 管理端展示与管理文件夹分享
系统 SHALL 在管理端「文件管理」中展示分享类型（文件/文件夹），并将「最大访问次数」更新为「最大下载次数」；支持编辑分享的密码、有效期与下载次数上限。

#### Scenario: 列表展示类型
- **WHEN** 管理员打开文件管理列表
- **THEN** 每条分享展示类型标识（文件/文件夹）与剩余/最大下载次数

#### Scenario: 编辑限制
- **WHEN** 管理员编辑某条分享的密码/有效期/下载次数上限并保存
- **THEN** 更新生效，后续访问按新限制校验

## MODIFIED Requirements

### Requirement: 前台入口与导航
独立「文件上传」页面被移除，前台导航仅保留「首页 / 挂载浏览 / 管理」。`/upload` 访问重定向到 `/mount-explorer`。原「上传页面开关」（`siteUploadPageEnabled`）语义调整为：控制**挂载浏览中的上传能力**是否可用。

#### Scenario: 访问已移除入口
- **WHEN** 用户访问 `/upload`
- **THEN** 重定向到 `/mount-explorer`

#### Scenario: 上传能力被关闭
- **WHEN** 管理员关闭上传能力（`siteUploadPageEnabled=false`）
- **THEN** 挂载浏览中不显示上传入口（上传按钮/上传弹窗不可用），浏览功能正常

### Requirement: 现有文件分享的计数语义
原基于「页面打开即计数」的 `views/max_views` 语义改为「实际下载计数/下载上限」。数据库列名保持不变（`files.views`、`files.max_views`），但行为与展示统一为「下载次数」。既有分享记录沿用同一计数，不做数据回填。

#### Scenario: 旧分享记录
- **WHEN** 访问一个已存在的分享记录
- **THEN** 打开页面不再增加计数；仅实际下载时增加

### Requirement: `/file/:slug` 对文件夹分享的处理
当 `/file/:slug` 对应的分享记录为文件夹（`target_type='folder'`）时，系统 SHALL 重定向到 `/folder/:slug`。

#### Scenario: 访问文件夹分享的旧入口
- **WHEN** 访客访问指向文件夹分享的 `/file/:slug`
- **THEN** 前端自动跳转到 `/folder/:slug` 并正常渲染

## REMOVED Requirements

### Requirement: 独立文件上传页（上传即分享专用页）
**Reason**: 与挂载浏览能力重复，按需求合并保留挂载浏览。
**Migration**: 上传能力并入挂载浏览；「上传即分享」流程迁移为挂载浏览上传弹窗中的可选项；`/upload` 重定向到 `/mount-explorer`。相关专用组件（如上传页专用文件列表/最近上传）在无引用后移除。
