# AGENTS.md

本文件为 AI 编码代理提供本仓库的工作指南。

## 项目概述

CloudPaste 是一个 Serverless 文件管理与 Markdown 分享工具，基于 Cloudflare Workers + D1 数据库，支持多种存储聚合（S3 兼容、WebDAV、OneDrive、Google Drive、Telegram、HuggingFace 等）、30+ 文件格式在线预览与 WebDAV 挂载。当前版本：1.9.1。

仓库为前后端分离的单仓库（monorepo）：

- `backend/` — Hono API，双运行模式：Cloudflare Workers（D1 数据库）与 Node/Docker（better-sqlite3），共用同一套业务代码
- `frontend/` — Vue 3 + Vite SPA，含 PWA 支持
- `docker/` — Docker 与 Docker Compose 部署配置
- `Api-doc.md` / `Api-s3_direct.md` — API 接口文档

## 常用命令

### 后端（在 `backend/` 目录下）

```bash
npm install                 # 安装依赖
npm run dev                 # wrangler dev 本地开发（需先初始化 D1）
npm run dev:scheduled       # 本地开发并启用 scheduled 触发器测试
npm run docker-dev          # Node 模式运行（better-sqlite3，数据存 ./data）
npm test                    # node --test（串行，--test-concurrency=1）
npm run deploy              # wrangler publish 部署到 Cloudflare
```

首次本地开发需初始化数据库：

```bash
wrangler d1 create cloudpaste-db
wrangler d1 execute cloudpaste-db --file=./schema.sql
```

### 前端（在 `frontend/` 目录下）

```bash
npm install                 # 安装依赖
npm run dev                 # Vite 开发服务器
npm run build               # 生产构建（构建时启用 PWA）
npm run lint                # ESLint 检查 src 下的 .js/.vue 文件
```

前端通过 `.env.development` 配置 `VITE_BACKEND_URL` 指向后端开发服务器（默认 http://localhost:8787）。

## 架构与分层约定

### 后端分层（严格自上而下）

```
routes/ → services/ → repositories/
```

- `src/routes/` — HTTP 路由层。领域子目录：`fs/`（挂载文件系统）、`files/`（文件分享）、`pastes/`（文本分享）；另有 `adminRoutes.js`、`apiKeyRoutes.js`、`mountRoutes.js`、`webdavRoutes.js`、`systemRoutes.js` 等
- `src/services/` — 领域服务，业务逻辑所在，不要把业务逻辑写在路由里
- `src/repositories/` — 数据访问层，通过 `src/db/dialects/`（sqlite/mysql/postgres）适配不同数据库
- `src/security/` — 认证与授权（JWT + API Key），含 `policies/` 与中间件；新增受保护接口必须走统一的安全层
- `src/storage/` — 存储抽象层。`drivers/` 下每个存储源一个目录（s3、webdav、onedrive、googledrive、telegram、discord、github、huggingface-datasets、local、mirror 等），含 `template/` 驱动模板与 `tester/` 连通性测试；新增存储驱动请参照 template
- `src/constants/` — 常量（ApiStatus、Permission、DbTables、UserType 等），响应状态码等应引用常量而非硬编码
- `src/http/` — 统一错误类型与响应封装，路由应使用统一的响应格式
- `src/scheduled/` — 定时任务（cron 每 5 分钟触发）
- `src/adapters/SQLiteAdapter.js` + `unified-entry.js` — Node/Docker 模式下用 better-sqlite3 模拟 D1 接口，使同一份代码跑在 Workers 和 Node 上

改动后端时注意：任何涉及数据库结构的变更需同步更新 `backend/schema.sql` 与 `src/db/migrations/`（迁移由 `runner.js` 管理），并保持三种 dialect 兼容。

### 前端分层（ESLint 强制约束）

```
modules/（领域层）← api/（HTTP 客户端，无业务语义）
components/（通用组件，禁止依赖 modules/*）
```

- `src/modules/` — 领域模块，按业务拆分：`paste/`、`fileshare/`、`fs/`、`upload/`、`storage-core/`、`security/`、`pwa-offline/`、`admin/`
- `src/api/` — HTTP 客户端与 API service，不含领域语义
- `src/components/` — 可复用通用组件，**禁止 import `modules/*`**（ESLint `import/no-restricted-paths` 强制）；领域组件封装在 `modules/<domain>` 内
- `src/composables/`、`src/stores/`（Pinia）、`src/router/`（所有页面路由入口）、`src/i18n/`、`src/utils/`、`src/workers/`（Web Worker）
- `vite.config.js` 中有针对 `foliate-js` PDF 的 stub 插件（项目用自己的 PDF 预览），不要删除

新页面/新功能请放入对应 `modules/<domain>`，不要把领域逻辑堆进 `components/` 或 `api/`。

## 环境与配置

- `backend/wrangler.toml` — Workers 配置：D1 绑定 `DB`、Workflows 绑定 `JOB_WORKFLOW`、cron 触发器；`wrangler.spa.toml` 用于前后端一体化部署（Worker 同时托管前端静态资源）
- 环境变量：`ENCRYPTION_SECRET`（敏感配置加密密钥）、`ADMIN_TOKEN_EXPIRY_DAYS`、`DEBUG_DRIVER_CACHE` 等
- `docker-compose.yml` — Docker 部署入口，本地存储需设置 `DATA_DIR`
- 版本号统一维护：`backend/package.json` 与 `frontend/vite.config.js` 的 `APP_VERSION` 需保持一致

## 代码风格

- 全仓库使用 ES Module（`"type": "module"`），不使用 TypeScript（仅类型定义辅助）
- 前端组件为 Vue 3 `<script setup>` + Tailwind CSS；已关闭 `vue/multi-word-component-names` 规则
- 提交前运行 `npm run lint`（前端）确保通过
- 用户界面文案需走 i18n（`src/i18n/`），不要硬编码中文/英文文案

## 参考文档

- [README_CN.md](./README_CN.md) — 部署教程与完整功能说明
- [Api-doc.md](./Api-doc.md) — API 总览
- [Api-s3_direct.md](./Api-s3_direct.md) — 服务器文件直传接口
- 在线文档：https://doc.cloudpaste.qzz.io
