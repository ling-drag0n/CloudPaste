# CloudPaste 📋

<div align="center">
    <p>
    <a href="README_CN.md">中文</a> | <a href="README.md">English</a> |
    <a href="https://www.readme-i18n.com/ling-drag0n/CloudPaste?lang=es">Español</a> |
    <a href="https://www.readme-i18n.com/ling-drag0n/CloudPaste?lang=fr">français</a> |
    <a href="https://www.readme-i18n.com/ling-drag0n/CloudPaste?lang=ja">日本語</a>
    </p>
    <img width="100" height="100" src="https://img.icons8.com/dusk/100/paste.png" alt="paste"/>
    <h3>🌩️ Serverless 文件管理与 Markdown 分享工具，支持多种存储聚合、30+文件格式在线预览 与 WebDAV挂载</h3>
</div>

<div align="center">
    <a href="https://deepwiki.com/ling-drag0n/CloudPaste"><img src="https://deepwiki.com/badge.svg" alt="Ask DeepWiki"></a>
    <a href="./LICENSE"><img src="https://img.shields.io/badge/license-Apache%202.0-blue.svg" alt="License"></a>
    <a href="https://github.com/ling-drag0n/CloudPaste/stargazers"><img src="https://img.shields.io/github/stars/ling-drag0n/CloudPaste.svg" alt="GitHub Stars"></a>
    <a href="https://www.cloudflare.com/"><img src="https://img.shields.io/badge/Powered%20by-Cloudflare-F38020?logo=cloudflare" alt="Powered by Cloudflare"></a>
    <a href="https://hub.docker.com/r/dragon730/cloudpaste-backend"><img src="https://img.shields.io/docker/pulls/dragon730/cloudpaste-backend.svg" alt="Docker Pulls"></a>
</div>

<p align="center">
  <a href="#-展示">📸 展示</a> •
  <a href="#-特点">✨ 特点</a> •
  <a href="#-部署教程">🚀 部署教程</a> •
  <a href="#-技术栈">🔧 技术栈</a> •
  <a href="#-开发">💻 开发</a> •
  <a href="#-许可证">📄 许可证</a>
</p>

## 📸 部分展示

<table align="center">
  <tr>
    <td><img src="./images/image-1.png" width="400"/></td>
    <td><img src="./images/image-2.png" width="400"/></td>
  </tr>
  <tr>
    <td><img src="./images/image-3.png" width="400"/></td>
    <td><img src="./images/image-4.png" width="400"/></td>
  </tr>
  <tr>
    <td><img src="./images/image-5.png" width="400"/></td>
    <td><img src="./images/image-en1.png" width="400"/></td>
  </tr>
  <tr>
    <td><img src="./images/image-mount1.png" width="400"/></td>
    <td><img src="./images/image-mount2.png" width="400"/></td>
  </tr>
</table>

## ✨ 核心特性

### Cloudflare 原生架构

- **边缘计算**：基于 Cloudflare Workers 、WorkFlow 和 D1 数据库，全球 300+ 节点就近响应
- **零运维**：无需管理服务器，自动扩缩容，按请求计费
- **一体化部署**：前后端打包在同一个 Worker，省心省力
- **分离部署**：可选前后端分离，灵活适配个人需求

### 多存储支持

- **S3 兼容**：Cloudflare R2、Backblaze B2、AWS S3、阿里云 OSS、腾讯云 COS、MinIO 等
- **网盘集成**：WebDAV、OneDrive、Google Drive、Telegram、Discord Bot、HuggingFace Database、GitHub API/Releases（只读）等等
- **本地存储**：Docker 部署支持本地文件系统
- **智能上传**：前端预签名直传 + 流式上传 +分片断点续传，进度实时显示，最大限度摆脱cf限制
- **文件预览**：支持30+种格式直接预览（图片、视频、音频、PDF、Office、代码、电子书等），其余可通过外部IFrame嵌入[KKFileview](https://github.com/kekingcn/kkFileView)预览
- **定时任务**：支持定时清理上传会话、存储同步、搜索索引重建等自动化任务
- **统一管理**：可视化配置多存储，灵活切换默认存储源

### 强大的 Markdown 编辑器

- **Vditor 集成**：支持 GitHub 风格 Markdown、数学公式、流程图、思维导图
- **实时预览**：所见即所得编辑体验
- **多格式导出**：PDF、HTML、PNG、Word 一键导出
- **安全分享**：密码保护、过期时间、访问次数限制
- **Raw 直链**：类似 GitHub Raw，适合配置文件托管

### WebDAV 协议支持

- **标准协议**：支持任意 WebDAV 客户端挂载为网络驱动器
- **完整操作**：目录创建、文件上传、删除、重命名、移动
- **权限控制**：API 密钥授权，细粒度访问控制
- **缓存优化**：可配置 TTL，减少上游请求

### 灵活的权限管理

- **API 密钥**：创建多权限只读/读写密钥，绑定特定存储路径
- **时效控制**：自定义有效期，自动失效与手动撤销
- **JWT 认证**：安全的管理员认证系统
- **PWA 支持**：可安装为桌面应用，离线使用

### 多种部署方式

- **自动部署**：GitHub Actions 一键部署，支持自动触发
- **手动部署**：Wrangler CLI 部署，灵活可控
- **Docker 部署**：前后端镜像 + Docker Compose 一键启动
- **多平台**：支持 Cloudflare、Vercel、ClawCloud、HuggingFace 等

## 🔐 文件传输加密

CloudPaste 提供双模式文件内容加密，兼顾安全性与性能：

### 服务端透明加密（管理开关，默认关闭）

- 管理后台 → 全局设置 → 安全设置 → 「服务端文件传输加密」
- 开启后新上传的分享文件以 **AES-256-GCM（CPENC1 分块格式）** 加密后写入存储，下载时代理解密，前端与现有链接基本兼容
- 性能：加解密使用平台原生 WebCrypto（硬件加速），仅覆盖新上传文件，开关关闭时零开销
- 注意：开启后加密文件强制走代理访问（禁用 302 直链），S3 预签名直传与断点续传自动回退为代理上传；Workers 免费版 CPU 限额较低，大文件加密建议付费版或 Docker 部署

### 端到端加密（E2E，分享创建时勾选）

- 「上传即分享」弹窗勾选 **端到端加密** 后，文件在浏览器内加密才上传，服务器、存储桶、数据库全程只见密文
- 密钥仅存在于分享链接 fragment（`#e2ek=...`），不会发送到服务器；**泄露完整链接即泄露密钥**，可叠加访问密码形成双重保护
- 查看者打开链接后由浏览器解密预览/下载；无密钥片段的链接无法获得明文
- v1 为内存型加密，单文件建议 ≤200MB

### 其他安全加固（本版本附带）

- 分享密码不再出现在 URL 中：密码验证后签发短时效 HMAC 访问令牌（`?at=`），旧 `?password=` 链接保持兼容
- 密码哈希升级为 PBKDF2-SHA256（加盐，迭代次数可通过环境变量 `PBKDF2_ITERATIONS` 调整，默认 100000），兼容旧数据并自动升级
- 存储配置密钥加密升级为真 AES-256-GCM（旧 `encrypted:` 格式仍可解密）
- 代理签名比较改为常量时间比较；新增「强制 HTTPS」开关（HTTP→HTTPS 308 跳转 + HSTS，Docker 自建部署建议开启）

> 重要：ENCRYPTION_SECRET 是加密体系的主密钥，请妥善保管并定期备份；更换后已加密文件需用旧密钥恢复。已加密的分享请连同完整链接（含 #e2ek=）一并保存。

## 🚀 部署教程

### 前期准备

在开始部署前，请确保您已准备以下内容：

- [ ] [Cloudflare](https://dash.cloudflare.com) 账号（必需）
- [ ] 如使用 R2：开通 **Cloudflare R2** 服务并创建存储桶（需绑定支付方式）
- [ ] 如使用 Vercel：注册 [Vercel](https://vercel.com) 账号
- [ ] 其他 S3 存储服务的配置信息：
    - `S3_ACCESS_KEY_ID`
    - `S3_SECRET_ACCESS_KEY`
    - `S3_BUCKET_NAME`
    - `S3_ENDPOINT`

**以下教程可能过时 具体参考： [Cloudpaste 在线部署文档](https://doc.cloudpaste.qzz.io)**

<details>
<summary><b>👉 查看完整部署教程</b></summary>

### 📑 目录

- [Action 自动部署](#Action自动部署)
    - [部署架构选择](#部署架构选择)
    - [配置 GitHub 仓库](#配置-GitHub-仓库)
    - [一体化部署教程（推荐）](#一体化部署教程推荐)
    - [前后端分离部署教程](#前后端分离部署教程)
- [手动部署](#手动部署)
    - [一体化手动部署（推荐）](#一体化手动部署推荐)
    - [前后端分离手动部署](#前后端分离手动部署)
- [ClawCloud 部署 CloudPaste 教程](#ClawCloud部署CloudPaste教程)

---

## Action 自动部署

使用 GitHub Actions 可以实现代码推送后自动部署应用。CloudPaste 提供两种部署架构供您选择。

### 部署架构选择

#### 🔄 一体化部署（推荐）

**前后端部署在同一个 Cloudflare Worker 上**

✨ **优势：**
- **前后端同源** - 无跨域问题，配置更简单
- **成本更低** - 导航请求不计费，相比分离部署节省 60%+ 成本
- **部署更简单** - 一次部署完成前后端，无需管理多个服务
- **性能更好** - 前后端在同一 Worker，响应速度更快

#### 🔀 前后端分离部署

**后端部署到 Cloudflare Workers，前端部署到 Cloudflare Pages**

✨ **优势：**
- **灵活管理** - 前后端独立部署，互不影响
- **团队协作** - 前后端可由不同团队维护
- **扩展性强** - 前端可轻松切换到其他平台（如 Vercel）

---

### 配置 GitHub 仓库

#### 1️⃣ Fork 或克隆仓库

访问并 Fork 仓库：[https://github.com/ling-drag0n/CloudPaste](https://github.com/ling-drag0n/CloudPaste)

#### 2️⃣ 配置 GitHub Secrets

进入您的 GitHub 仓库设置：**Settings** → **Secrets and variables** → **Actions** → **New repository secret**

添加以下 Secrets：

| Secret 名称             | 必需 | 用途                                                  |
| ----------------------- | ---- | ----------------------------------------------------- |
| `CLOUDFLARE_API_TOKEN`  | ✅   | Cloudflare API 令牌（需要 Workers、D1 和 Pages 权限） |
| `CLOUDFLARE_ACCOUNT_ID` | ✅   | Cloudflare 账户 ID                                    |
| `ENCRYPTION_SECRET`     | ❌   | 用于加密敏感数据的密钥（如不提供，将自动生成）        |
| `ACTIONS_VAR_TOKEN`     | ✅   | 用于部署控制面板的 GitHub Token（使用控制面板时需要，如不使用这不需要） |

#### 3️⃣ 获取 Cloudflare API 令牌

**获取 API Token：**

1. 访问 [Cloudflare API Tokens](https://dash.cloudflare.com/profile/api-tokens)
2. 点击 **Create Token**
3. 选择 **Edit Cloudflare Workers** 模板
4. **添加额外权限**：
    - Account → **D1** → **Edit**
    - Account → **Cloudflare Pages** → **Edit** (如使用分离部署)
5. 点击 **Continue to summary** → **Create Token**
6. **复制 Token** 并保存到 GitHub Secrets

![D1 Permission](./images/D1.png)

**获取 Account ID：**

1. 访问 [Cloudflare Dashboard](https://dash.cloudflare.com)
2. 在右侧栏找到 **Account ID**
3. 点击复制并保存到 GitHub Secrets

#### 4️⃣ （可选）配置部署控制面板

如果您想使用可视化控制面板管理自动部署开关，需要额外配置：

**创建 GitHub Personal Access Token：**

1. 访问 [GitHub Token 设置](https://github.com/settings/tokens)
2. 点击 **Generate new token** → **Generate new token (classic)**
3. 设置 Token 名称（如 `CloudPaste Deployment Control`）
4. 选择权限：
    - ✅ **repo** (完整仓库访问权限)
    - ✅ **workflow** (工作流权限)
5. 点击 **Generate token**
6. 复制 Token 并保存为 Secret `ACTIONS_VAR_TOKEN`

**使用控制面板：**

1. 进入仓库 **Actions** 标签页
2. 在左侧工作流列表中，点击 **🎛️ 部署控制面板**
3. 点击右侧 **Run workflow** → **Run workflow**
4. 在弹出界面中选择要开启/关闭的部署方式
5. 点击 **Run workflow** 应用配置
6. 控制面板会在写入开关状态后，自动触发对应的部署工作流一次（是否真正部署由当前开关状态决定）

---

### 🔄 一体化部署教程（推荐）

#### 部署步骤

1️⃣ **配置完成 GitHub Secrets**（参考上方配置章节）

2️⃣ **触发部署工作流**

方式一：手动触发（首次部署推荐）

- 进入仓库 **Actions** 标签页
- 点击左侧 **Deploy SPA CF Workers[一体化部署]**
- 点击右侧 **Run workflow** → 选择 `main` 分支 → **Run workflow**

方式二：自动触发

- 使用部署控制面板开启 **SPA 一体化自动部署**
- 之后每次推送 `frontend/` 或 `backend/` 目录的代码到 `main` 分支时自动部署

> 提示：在 Actions 页面手动运行 **Deploy SPA CF Workers[一体化部署]** 工作流时，会强制部署一次，不受自动部署开关影响；自动部署行为（push 或控制面板触发）始终由 `SPA_DEPLOY` 开关控制。

3️⃣ **等待部署完成**

部署过程约 3-5 分钟，工作流会自动完成以下步骤：

- ✅ 构建前端静态资源
- ✅ 安装后端依赖
- ✅ 创建/验证 D1 数据库
- ✅ 初始化数据库表结构
- ✅ 设置加密密钥
- ✅ 部署到 Cloudflare Workers

#### 部署完成


**访问您的应用：** `https://cloudpaste-spa.your-account.workers.dev`

**后续配置：**

1. 首次访问会自动初始化数据库
2. 使用默认管理员账户登录：
    - 用户名：`admin`
    - 密码：`admin123`
3. **⚠️ 重要：立即修改默认管理员密码！**
4. 在管理员面板中配置您的 S3/WEBDAV 兼容存储服务
5. （可选）在 Cloudflare Dashboard 中绑定自定义域名

---

### 🔀 前后端分离部署教程

如果您选择前后端分离部署，请按以下步骤操作：

#### 后端部署

1️⃣ **配置完成 GitHub Secrets**（参考上方配置章节）

2️⃣ **触发后端部署**

方式一：手动触发

- 进入仓库 **Actions** 标签页
- 点击左侧 **Deploy Backend CF Workers[Worker后端分离部署]**
- 点击 **Run workflow** → **Run workflow**

方式二：自动触发

- 使用部署控制面板开启 **后端分离自动部署**
- 推送 `backend/` 目录代码时自动部署

3️⃣ **等待部署完成**

工作流会自动完成：

- ✅ 创建/验证 D1 数据库
- ✅ 初始化数据库表结构
- ✅ 设置加密密钥
- ✅ 部署 Worker 到 Cloudflare

4️⃣ **记录后端地址**

部署成功后记下您的后端 Worker URL：
`https://cloudpaste-backend.your-account.workers.dev`

**<span style="color:red">⚠️ 重要：记住您的后端域名，前端部署时需要使用！</span>**

#### 前端部署

##### Cloudflare Pages

1️⃣ **触发前端部署**

方式一：手动触发

- 进入仓库 **Actions** 标签页
- 点击左侧 **Deploy Frontend CF Pages[Pages前端分离部署]**
- 点击 **Run workflow** → **Run workflow**

方式二：自动触发

- 使用部署控制面板开启 **前端分离自动部署**
- 推送 `frontend/` 目录代码时自动部署

> 提示：在 Actions 页面手动运行「后端」「前端」部署工作流时，同样会强制部署一次，不受自动部署开关影响；自动部署行为由 `BACKEND_DEPLOY` / `FRONTEND_DEPLOY` 开关控制。

2️⃣ **配置环境变量**

**必须步骤：前端部署完成后，需要手动配置后端地址！**

1. 登录 [Cloudflare Dashboard](https://dash.cloudflare.com/)
2. 导航到 **Pages** → **cloudpaste-frontend**
3. 点击 **Settings** → **Environment variables**
4. 添加环境变量：
    - **名称**：`VITE_BACKEND_URL`
    - **值**：您的后端 Worker URL（如 `https://cloudpaste-backend.your-account.workers.dev`）
    - **注意**：末尾不带 `/`，建议使用自定义域名

**<span style="color:red">⚠️ 必须填写完整的后端域名，格式：https://xxxx.com</span>**

3️⃣ **重新部署前端**

**重要：配置环境变量后，必须再次运行前端工作流！**

- 返回 GitHub Actions
- 再次手动触发 **Deploy Frontend CF Pages** 工作流
- 这样才能加载后端域名配置

![Frontend Redeploy](./images/test-1.png)

4️⃣ **访问应用**

前端部署地址：`https://cloudpaste-frontend.pages.dev`

**<span style="color:red">⚠️ 务必严格按照步骤操作，否则会出现后端域名加载失败！</span>**

##### Vercel（备选方案）

Vercel 部署步骤：

1. Fork 后在 Vercel 中导入 GitHub 项目
2. 配置部署参数：

```
Framework Preset（框架预设）: Vite
Build Command（构建命令）: npm run build
Output Directory（输出目录）: dist
Install Command（安装命令）: npm install
```

3. 配置环境变量：
    - 名称：`VITE_BACKEND_URL`
    - 值：您的后端 Worker URL
4. 点击 **Deploy** 按钮进行部署

**☝️ Cloudflare Pages 和 Vercel 二选一即可**

**<span style="color:red">⚠️ 安全提示：请在系统初始化后立即修改默认管理员密码（用户名: admin, 密码: admin123）。</span>**

---

## 手动部署

CloudPaste 支持两种手动部署方式：一体化部署（推荐）和前后端分离部署。

### 🔄 一体化手动部署（推荐）

一体化部署将前后端部署到同一个 Cloudflare Worker，配置更简单，成本更低。

#### 步骤 1：克隆仓库

```bash
git clone https://github.com/ling-drag0n/CloudPaste.git
cd CloudPaste
```

#### 步骤 2：构建前端

```bash
cd frontend
npm install
npm run build
# 或者使用 pnpm（同样支持）
# pnpm install
# pnpm run build
cd ..
```

**验证构建产物：** 确保 `frontend/dist` 目录存在且包含 `index.html`

#### 步骤 3：配置后端

```bash
cd backend
npm install
# 或者使用 pnpm（同样支持）
# pnpm install
npx wrangler login
```

#### 步骤 4：创建 D1 数据库

```bash
npx wrangler d1 create cloudpaste-db
```

记下输出的 `database_id`（例如：`xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`）

#### 步骤 5：初始化数据库

```bash
npx wrangler d1 execute cloudpaste-db --file=./schema.sql
```

#### 步骤 6：配置 wrangler.spa.toml

编辑 `backend/wrangler.spa.toml` 文件，修改数据库 ID：

```toml
[[d1_databases]]
binding = "DB"
database_name = "cloudpaste-db"
database_id = "您的数据库ID"  # 替换为步骤4获取的ID
```

#### 步骤 7：部署到 Cloudflare Workers

```bash
npx wrangler deploy --config wrangler.spa.toml
```

部署成功后，会显示您的应用 URL：

```
Published cloudpaste-spa (X.XX sec)
  https://cloudpaste-spa.your-account.workers.dev
```

#### 部署完成！

**访问您的应用：** 打开上述 URL 即可使用 CloudPaste

**后续配置：**
1. 首次访问会自动初始化数据库
2. 使用默认管理员账户登录（用户名：`admin`，密码：`admin123`）
3. **⚠️ 立即修改默认管理员密码！**
4. 在管理员面板中配置 S3 兼容存储服务
5. （可选）在 Cloudflare Dashboard 中绑定自定义域名

**<span style="color:red">⚠️ 安全提示：请在系统初始化后立即修改默认管理员密码。</span>**

---

### 🔀 前后端分离手动部署

如果您需要前后端独立部署和管理，可以选择分离部署方式。

#### 后端手动部署

1. 克隆仓库

```bash
git clone https://github.com/ling-drag0n/CloudPaste.git
cd CloudPaste/backend
```

2. 安装依赖

   ```bash
   npm install
   ```

3. 登录 Cloudflare

   ```bash
   npx wrangler login
   ```

4. 创建 D1 数据库

   ```bash
   npx wrangler d1 create cloudpaste-db
   ```

   记下输出的数据库 ID。

5. 修改 wrangler.toml 配置

   ```toml
   [[d1_databases]]
   binding = "DB"
   database_name = "cloudpaste-db"
   database_id = "您的数据库ID"
   ```

6. 部署 Worker

   ```bash
   npx wrangler deploy
   ```

   记下输出的 URL，这是您的后端 API 地址。

7. 初始化数据库（自动）
   访问您的 Worker URL 触发初始化：

   ```
   https://cloudpaste-backend.your-username.workers.dev
   ```

**<span style="color:red">⚠️ 重要：记住您的后端域名，前端部署时需要使用！</span>**

#### 前端手动部署

#### Cloudflare Pages

1. 准备前端代码

   ```bash
   cd CloudPaste/frontend
   npm install
   ```

2. 配置环境变量
   创建或修改 `.env.production` 文件：

   ```
   VITE_BACKEND_URL=https://cloudpaste-backend.your-username.workers.dev
   VITE_APP_ENV=production
   VITE_ENABLE_DEVTOOLS=false
   ```

3. 构建前端项目

   ```bash
   npm run build
   ```

   [构建时需注意！！](https://github.com/ling-drag0n/CloudPaste/issues/6#issuecomment-2818746354)

4. 部署到 Cloudflare Pages

   **方法一**：通过 Wrangler CLI

   ```bash
   npx wrangler pages deploy dist --project-name=cloudpaste-frontend
   ```

   **方法二**：通过 Cloudflare Dashboard

    1. 登录 [Cloudflare Dashboard](https://dash.cloudflare.com/)
    2. 选择 "Pages"
    3. 点击 "Create a project" → "Direct Upload"
    4. 上传 `dist` 目录内的文件
    5. 设置项目名称（如 "cloudpaste-frontend"）
    6. 点击 "Save and Deploy"

#### Vercel

1. 准备前端代码

   ```bash
   cd CloudPaste/frontend
   npm install
   ```

2. 安装并登录 Vercel CLI

   ```bash
   npm install -g vercel
   vercel login
   ```

3. 配置环境变量，与 Cloudflare Pages 相同
4. 构建并部署

   ```bash
   vercel --prod
   ```

   根据提示配置项目。

---

## ClawCloud 部署 CloudPaste 教程

#### 每月 10G 免费流量，只适合轻度使用

###### Step 1:

注册链接：[Claw Cloud](https://ap-northeast-1.run.claw.cloud/signin) （不带#AFF）
不需要信用卡，只要 GitHub 注册日期大于 180 天，每个月都送 5 美金额度。

###### Step 2:

注册后，在首页点击 APP Launchpad 进入，然后点击右上角的 create app

![image.png](https://s2.loli.net/2025/04/21/soj5eWMhxTg1VFt.png)

###### Step 3:

先是部署后端，如图所示（仅供参考）：
![image.png](https://s2.loli.net/2025/04/21/AHrMnuVyNhK6eUk.png)

后端的数据存储就是这里：
![image.png](https://s2.loli.net/2025/04/21/ANaoU5Y6cxPOVfw.png)

###### Step 4:

然后是前端，如图所示（仅供参考）：
![image.png](https://s2.loli.net/2025/04/21/kaT5Qu8ctovFdUp.png)

##### 部署完成即可使用，可根据需要自定义域名

</details>

<details>
<summary><b>👉 Docker部署教程</b></summary>

### 📑 目录

- [Docker 命令行部署](#Docker命令行部署:)
    - [后端 Docker 部署](#后端Docker部署)
    - [前端 Docker 部署](#前端Docker部署)
- [Docker Compose 一键部署](#Docker-Compose一键部署:)

---

## Docker 命令行部署:

### 后端 Docker 部署

CloudPaste 后端支持通过官方 Docker 镜像快速部署。

1. 创建数据存储目录

   ```bash
   mkdir -p sql_data
   ```

2. 运行后端容器

   ```bash
   docker run -d --name cloudpaste-backend \
     -p 8787:8787 \
     -v $(pwd)/sql_data:/data \
     -e ENCRYPTION_SECRET=您的加密密钥 \
     -e NODE_ENV=production \
     dragon730/cloudpaste-backend:latest
   ```

   记下部署的 URL（如 `http://your-server-ip:8787`），后续前端部署需要用到。

**<span style="color:red">⚠️ 安全提示：请务必自定义 ENCRYPTION_SECRET 并保存好，此密钥用于加密敏感数据。</span>**

### 前端 Docker 部署

前端使用 Nginx 提供服务，并在启动时配置后端 API 地址。

```bash
docker run -d --name cloudpaste-frontend \
  -p 80:80 \
  -e BACKEND_URL=http://your-server-ip:8787 \
  dragon730/cloudpaste-frontend:latest
```

**<span style="color:red">⚠️ 注意：BACKEND_URL 必须包含完整 URL（包括协议 http:// 或 https://）</span>**
**<span style="color:red">⚠️ 安全提示：请在系统初始化后立即修改默认管理员密码（用户名: admin, 密码: admin123）。</span>**

### Docker 镜像更新

当项目发布新版本时，您可以按以下步骤更新 Docker 部署：

1. 拉取最新镜像

   ```bash
   docker pull dragon730/cloudpaste-backend:latest
   docker pull dragon730/cloudpaste-frontend:latest
   ```

2. 停止并移除旧容器

   ```bash
   docker stop cloudpaste-backend cloudpaste-frontend
   docker rm cloudpaste-backend cloudpaste-frontend
   ```

3. 使用上述相同的运行命令启动新容器（保留数据目录和配置）

## Docker-Compose 一键部署:

使用 Docker Compose 可以一键部署前后端服务，是最简单推荐的方式。

1. 创建 `docker-compose.yml` 文件

```yaml
version: "3.8"

services:
  frontend:
    image: dragon730/cloudpaste-frontend:latest
    environment:
      - BACKEND_URL=https://xxx.com # 填写后端服务地址
    ports:
      - "8080:80" #"127.0.0.1:8080:80"
    depends_on:
      - backend # 依赖backend服务
    networks:
      - cloudpaste-network
    restart: unless-stopped

  backend:
    image: dragon730/cloudpaste-backend:latest
    environment:
      - NODE_ENV=production
      - PORT=8787
      - ENCRYPTION_SECRET=自定义密钥 # 请修改为您自己的安全密钥
      - TASK_WORKER_POOL_SIZE=2 # 任务工作池大小
    volumes:
      - ./sql_data:/data # 数据持久化
    ports:
      - "8787:8787" #"127.0.0.1:8787:8787"
    networks:
      - cloudpaste-network
    restart: unless-stopped

networks:
  cloudpaste-network:
    driver: bridge
```

2. 启动服务

```bash
docker-compose up -d
```

**<span style="color:red">⚠️ 安全提示：请在系统初始化后立即修改默认管理员密码（用户名: admin, 密码: admin123）。</span>**

3. 访问服务

前端: `http://your-server-ip:80`
后端: `http://your-server-ip:8787`

### Docker Compose 更新

当需要更新到新版本时：

1. 拉取最新镜像

   ```bash
   docker-compose pull
   ```

2. 使用新镜像重新创建容器（保留数据卷）

   ```bash
   docker-compose up -d --force-recreate
   ```

**<span style="color:orange">💡 提示：如果遇到配置变更，可能需要备份数据后修改 docker-compose.yml 文件</span>**

### Nginx 反代示例（仅供参考）

```nginx
server {
    listen 443 ssl;
    server_name paste.yourdomain.com;  # 替换为您的域名

    # SSL 证书配置
    ssl_certificate     /path/to/cert.pem;  # 替换为证书路径
    ssl_certificate_key /path/to/key.pem;   # 替换为密钥路径

    # 前端代理配置
    location / {
        proxy_pass http://localhost:80;  # Docker前端服务地址
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }

    # 后端API代理配置
    location /api {
        proxy_pass http://localhost:8787;  # Docker后端服务地址
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        client_max_body_size 0;

        # WebSocket支持 (如果需要)
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
    }

    # WebDav 配置
    location /dav/ {
        proxy_pass http://localhost:8787/dav/;  # 指向您的后端服务

        # WebDAV 必要头信息
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;

        # WebDAV 方法支持
        proxy_pass_request_headers on;

        # 支持所有WebDAV方法
        proxy_method $request_method;

        # 必要的头信息处理
        proxy_set_header Destination $http_destination;
        proxy_set_header Overwrite $http_overwrite;

        # 处理大文件
        client_max_body_size 0;
        client_body_buffer_size 128k;

        # WebDAV 上传/下载优化
        proxy_buffering off;           # 关闭代理缓冲，减少延迟
        proxy_request_buffering off;   # 关闭请求缓冲，让数据流式传输
        proxy_max_temp_file_size 0;    # 不生成临时文件
        send_timeout 3600;             # 上传超时时间
        proxy_read_timeout 3600;       # 读取超时
        proxy_send_timeout 3600;       # 发送超时
        proxy_connect_timeout 300;     # 连接超时
    }
}
```

**<span style="color:red">⚠️ 安全提示：建议配置 HTTPS 和反向代理（如 Nginx）以提升安全性。</span>**

</details>

<details>
<summary><b>👉 S3相关跨域配置教程</b></summary>

## R2 API 相关获取及跨域配置

1. 登录 Cloudflare Dashboard
2. 点击 R2 存储，创建一个存储桶。
3. 创建 API 令牌
   ![R2api](./images/R2/R2-api.png)
   ![R2rw](./images/R2/R2-rw.png)

4. 创建后把全部数据都保存好，后续要用
5. 配置跨域规则，点击对应存储桶，点击设置，编辑 CORS 策略，如下所示：

```json
[
  {
    "AllowedOrigins": ["http://localhost:3000", "https://根据自己的前端域名来替代"],
    "AllowedMethods": ["GET", "PUT", "POST", "DELETE", "HEAD"],
    "AllowedHeaders": ["*"],
    "ExposeHeaders": ["ETag"],
    "MaxAgeSeconds": 3600
  }
]
```

## B2 API 相关获取及跨域配置

1. 若没有 B2 账号，可以先[注册](https://www.backblaze.com/sign-up/cloud-storage?referrer=getstarted)一个，然后创建一个存储桶。
   ![B2账号注册](./images/B2/B2-1.png)
2. 点击侧边栏的 Application Key，点击 Create Key，然后如图所示。
   ![B2key](./images/B2/B2-2.png)
3. 配置 B2 的跨域，B2 跨域配置比较麻烦，需注意
   ![B2cors](./images/B2/B2-3.png)
4. 可以先尝试一下 1 或 2，去到上传页面看看是否能上传，F12 打开控制台若显示跨域错误，则使用 3。要一劳永逸就直接使用 3。

   ![B21](./images/B2/B2-4.png)

关于 3 的配置由于面板无法配置，只能手动配置，需[下载 B2 CLI](https://www.backblaze.com/docs/cloud-storage-command-line-tools)对应工具。具体可以参考："https://docs.cloudreve.org/zh/usage/storage/b2 " 。

下载后，在对应下载目录 cmd，在命令行输入以下命令：

```txt
b2-windows.exe account authorize   //进行账号登录，根据提示填入之前的 keyID 和 applicationKey
b2-windows.exe bucket get <bucketName> //你可以执行获取bucket信息，<bucketName>换成桶名字
```

windows 配置，采用“.\b2-windows.exe xxx”，
所以在对应 cli 的 exe 文件夹中 cmd 输入，python 的 cli 也同理：

```cmd
b2-windows.exe bucket update <bucketName> allPrivate --cors-rules "[{\"corsRuleName\":\"CloudPaste\",\"allowedOrigins\":[\"*\"],\"allowedHeaders\":[\"*\"],\"allowedOperations\":[\"b2_upload_file\",\"b2_download_file_by_name\",\"b2_download_file_by_id\",\"s3_head\",\"s3_get\",\"s3_put\",\"s3_post\",\"s3_delete\"],\"exposeHeaders\":[\"Etag\",\"content-length\",\"content-type\",\"x-bz-content-sha1\"],\"maxAgeSeconds\":3600}]"
```

其中<bucketName>换成你的存储桶名字，关于允许跨域的域名 allowedOrigins 可以根据个人配置，这里是允许所有。

5. 已完成跨域配置

## MinIO API 相关获取及跨域配置

1. **部署 MinIO 服务器**

   使用以下 Docker Compose 配置（参考）快速部署 MinIO 服务：

   ```yaml
   version: "3"

   services:
     minio:
       image: minio/minio:RELEASE.2025-02-18T16-25-55Z
       container_name: minio-server
       command: server /data --console-address :9001 --address :9000
       environment:
         - MINIO_ROOT_USER=minioadmin # 设置管理员用户名
         - MINIO_ROOT_PASSWORD=minioadmin # 设置管理员密码
         - MINIO_BROWSER=on
         - MINIO_SERVER_URL=https://minio.example.com # S3 API 访问地址
         - MINIO_BROWSER_REDIRECT_URL=https://console.example.com # 控制台访问地址
       ports:
         - "9000:9000" # S3 API 端口
         - "9001:9001" # 控制台端口
       volumes:
         - ./data:/data
         - ./certs:/root/.minio/certs # 如需配置SSL证书
       restart: always
   ```

   运行 `docker-compose up -d` 启动服务。

2. **配置反向代理（参考）**

   为确保 MinIO 服务正常工作，特别是文件预览功能，需要正确配置反向代理。以下是 OpenResty/Nginx 的推荐配置：

   **MinIO S3 API 反向代理 (minio.example.com)**:

   ```nginx
   location / {
       proxy_pass http://127.0.0.1:9000;
       proxy_set_header Host $host;
       proxy_set_header X-Real-IP $remote_addr;
       proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
       proxy_set_header X-Forwarded-Proto $scheme;

       # HTTP 连接优化
       proxy_http_version 1.1;
       proxy_set_header Connection "";  # 启用HTTP/1.1的keepalive

       # 关键配置：解决403错误和预览问题
       proxy_cache off;
       proxy_buffering off;
       proxy_request_buffering off;

       # 无文件大小限制
       client_max_body_size 0;
   }
   ```

   **MinIO 控制台反向代理 (console.example.com)**:

   ```nginx
   location / {
       proxy_pass http://127.0.0.1:9001;
       proxy_set_header Host $host;
       proxy_set_header X-Real-IP $remote_addr;
       proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
       proxy_set_header X-Forwarded-Proto $scheme;

       # WebSocket 支持
       proxy_http_version 1.1;
       proxy_set_header Upgrade $http_upgrade;
       proxy_set_header Connection "upgrade";

       # 关键配置
       proxy_cache off;
       proxy_buffering off;

       # 无文件大小限制
       client_max_body_size 0;
   }
   ```

3. **访问控制台创建存储桶和创建访问密钥**

   如有详细配置需求，可参考官方文档：https://min.io/docs/minio/container/index.html

   CN: https://min-io.cn/docs/minio/container/index.html

   ![minio-1](./images/minio-1.png)

4. **相关配置（可选）**

   允许的源包含您的前端域名
   ![minio-2](./images/minio-2.png)

5. **在 CloudPaste 中配置 MinIO**

    - 登录 CloudPaste 管理界面
    - 进入 "S3 存储配置" → "添加存储配置"
    - 选择 "其他兼容 S3 服务" 作为提供商类型
    - 填入以下信息：
        - 名称：自定义名称
        - 端点 URL：您的 MinIO 服务地址（如 `https://minio.example.com`）
        - 存储桶名称：之前创建的存储桶名称
        - 访问密钥 ID：您的 Access Key
        - 访问密钥：您的 Secret Key
        - 区域：可留空
        - 路径风格访问：必须启用！！！！
    - 点击 "测试连接" 确认配置正确
    - 保存配置

6. **注意与故障排查**

    - **注意事项**：如使用 Cloudfare 开启 cdn 可能需要加上 proxy_set_header Accept-Encoding "identity"，同时存在缓存问题，最好仅用 DNS 解析
    - **403 错误**：确保反向代理配置中包含 `proxy_cache off` 和 `proxy_buffering off`
    - **预览问题**：确保 MinIO 服务器正确配置了 `MINIO_SERVER_URL` 和 `MINIO_BROWSER_REDIRECT_URL`
    - **上传失败**：检查 CORS 配置是否正确，确保允许的源包含您的前端域名
    - **控制台无法访问**：检查 WebSocket 配置是否正确，特别是 `Connection "upgrade"` 设置

## 更多 S3 相关配置待续......

</details>

<details>
<summary><b>👉 WebDAV配置详细指南</b></summary>

## WebDAV 配置与使用详解

CloudPaste 提供简易的 WebDAV 协议支持，允许您将存储空间挂载为网络驱动器，便于直接通过文件管理器访问和管理文件。

### WebDAV 服务基本信息

- **WebDAV 基础 URL**: `https://你的后端域名/dav`
- **支持的认证方式**:
    - Basic 认证（用户名+密码）
- **支持的权限类型**:
    - 管理员账户 - 拥有完整操作权限
    - API 密钥 - 按需启用

### 权限配置

#### 1. 管理员账户访问

使用管理员账户和密码直接访问 WebDAV 服务：

- **用户名**: 管理员用户名
- **密码**: 管理员密码

#### 2. API 密钥访问（推荐）

为更安全的访问方式，建议创建专用 API 密钥：

1. 登录管理界面
2. 导航至"API 密钥管理"
3. 创建新 API 密钥，**确保启用"挂载权限"**
4. 使用方式：
    - **用户名**: API 密钥值
    - **密码**: 与用户名相同的 API 密钥值

### NGINX 反向代理配置

如果使用 NGINX 作为反向代理，需要添加特定的 WebDAV 配置以确保所有 WebDAV 方法正常工作：

```nginx
# WebDAV 配置
location /dav {
    proxy_pass http://localhost:8787;  # 指向您的后端服务

    # WebDAV 必要头信息
    proxy_set_header Host $host;
    proxy_set_header X-Real-IP $remote_addr;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;

    # WebDAV 方法支持
    proxy_pass_request_headers on;

    # 支持所有WebDAV方法
    proxy_method $request_method;

    # 必要的头信息处理
    proxy_set_header Destination $http_destination;
    proxy_set_header Overwrite $http_overwrite;

    # 处理大文件
    client_max_body_size 0;

    # 超时设置
    proxy_connect_timeout 3600s;
    proxy_send_timeout 3600s;
    proxy_read_timeout 3600s;
}
```

### 常见问题解决

1. **连接问题**:

    - 确认 WebDAV URL 格式正确
    - 验证认证凭据是否有效
    - 检查 API 密钥是否具有挂载权限

2. **权限错误**:

    - 确认账户具有所需的权限
    - 管理员账户应有完整权限
    - API 密钥需特别启用挂载权限

3. **⚠️⚠️ Webdav 上传问题**:

    - Worker 部署的 webdav 上传大小可能受限于 CF 的 CDN 限制 100MB 左右，导致报错 413
    - 对于 Docker 部署，只需注意 nginx 代理配置，上传模式任意。

</details>

## 🔧 技术栈

### 前端

- **框架**: Vue.js 3 + Vite
- **样式**: TailwindCSS
- **编辑器**: Vditor
- **国际化**: Vue-i18n
- **图表**: Chart.js + Vue-chartjs

### 后端

- **运行时**: Cloudflare Workers
- **框架**: Hono
- **数据库**: Cloudflare D1 (SQLite)
- **存储**: 多 S3 兼容服务 (支持 R2, B2, AWS S3)
- **认证**: JWT 令牌 + API 密钥

## 💻 开发

### API 文档

[API 文档](Api-doc.md)

[服务器 文件直传 API 文档](Api-s3_direct.md) - 服务器 文件直传接口详细说明

### 本地开发设置

1. **克隆项目仓库**

   ```bash
   git clone https://github.com/ling-drag0n/cloudpaste.git
   cd cloudpaste
   ```

2. **后端设置**

   ```bash
   cd backend
   npm install
   # 初始化 D1 数据库
   wrangler d1 create cloudpaste-db
   wrangler d1 execute cloudpaste-db --file=./schema.sql
   ```

3. **前端设置**

   ```bash
   cd frontend
   npm install
   ```

4. **配置环境变量**

    - 在 `backend` 目录下，创建 `wrangler.toml` 文件设置开发环境变量
    - 在 `frontend` 目录下，配置 `.env.development` 文件设置前端环境变量

5. **启动开发服务器**

   ```bash
   # 后端
   cd backend
   npm run dev

   # 前端 (另一个终端)
   cd frontend
   npm run dev
   ```

### 项目结构

```
CloudPaste/
├── frontend/                         # 前端 Vite + Vue 3 SPA
│   ├── src/
│   │   ├── api/                      # HTTP 客户端与 API service（无领域语义）
│   │   ├── modules/                  # 领域模块层（按业务拆分）
│   │   │   ├── paste/                # 文本分享（编辑器 / 公共查看 / 管理）
│   │   │   ├── fileshare/            # 文件分享（公共页 / 管理）
│   │   │   ├── fs/                   # 挂载文件系统浏览器（MountExplorer）
│   │   │   ├── upload/               # 上传控制器与上传视图
│   │   │   ├── storage-core/         # 存储驱动与 Uppy 集成（底层抽象）
│   │   │   ├── security/             # 前端认证桥接 / 请求头构造
│   │   │   ├── pwa-offline/          # PWA 离线与队列
│   │   │   └── admin/                # 管理后台（仪表盘 / 设置 / 密钥管理等）
│   │   ├── components/               # 可复用通用组件（对模块无依赖）
│   │   ├── composables/              # 共享组合式 API（file-system / preview 等）
│   │   ├── stores/                   # Pinia Store（auth / fileSystem / siteConfig 等）
│   │   ├── router/                   # Vue Router 配置（所有页面路由入口）
│   │   ├── pwa/                      # PWA 状态与安装提示
│   │   ├── utils/                    # 通用工具函数（clipboard / time / icons 等）
│   │   ├── styles/                   # 全局样式与 Tailwind 配置入口
│   │   └── assets/                   # 静态资源
│   ├── eslint.config.cjs             # 前端 ESLint 配置（含 import 限制）
│   ├── vite.config.js                # Vite 构建配置
│   └── package.json
├── backend/                          # 后端（Cloudflare Workers / Docker 双运行模式）
│   ├── src/
│   │   ├── routes/                   # HTTP 路由层（fs / files / pastes / admin / system 等）
│   │   │   ├── fs/                   # 挂载文件系统 API（list / read / write / search / share）
│   │   │   ├── files/                # 文件分享 API（公开 / 管理）
│   │   │   ├── pastes/               # 文本分享 API（公开 / 管理）
│   │   │   ├── adminRoutes.js        # 管理端通用路由
│   │   │   ├── apiKeyRoutes.js       # API 密钥管理路由
│   │   │   ├── mountRoutes.js        # 挂载点配置路由
│   │   │   ├── systemRoutes.js       # 系统设置与仪表盘统计
│   │   │   └── fsRoutes.js           # 统一 FS 入口聚合
│   │   ├── services/                 # 领域服务（pastes / files / system / apiKey 等）
│   │   ├── security/                 # 认证 + 授权层（AuthService / securityContext / authorize / policies）
│   │   ├── webdav/                   # WebDAV 协议实现与路径处理
│   │   ├── storage/                  # 存储抽象（S3 驱动、挂载管理、文件系统操作）
│   │   ├── repositories/             # 数据访问层（D1 + SQLite Repository）
│   │   ├── cache/                    # 缓存与失效（主要用于 FS）
│   │   ├── constants/                # 常量定义（ApiStatus / Permission / DbTables / UserType 等）
│   │   ├── http/                     # 统一错误类型与响应封装
│   │   └── utils/                    # 通用工具（common / crypto / environment 等）
│   ├── schema.sql                    # D1 / SQLite 数据库初始化脚本
│   ├── wrangler.toml                 # Cloudflare Workers / D1 配置
│   └── package.json
├── docker/                           # Docker 与 Compose 部署配置
├── images/                           # README 中使用的截图资源
├── Api-doc.md                        # API 总览文档
├── Api-s3_direct.md                  # S3 直传相关 API 文档
└── README.md                         # 当前项目说明文档
```

### 自定义 Docker 构建

如果您希望自定义 Docker 镜像或进行开发调试，可以按照以下步骤手动构建：

1. **构建后端镜像**

   ```bash
   # 在项目根目录执行
   docker build -t cloudpaste-backend:custom -f docker/backend/Dockerfile .

   # 运行自定义构建的镜像
   docker run -d --name cloudpaste-backend \
     -p 8787:8787 \
     -v $(pwd)/sql_data:/data \
     -e ENCRYPTION_SECRET=开发测试密钥 \
     cloudpaste-backend:custom
   ```

2. **构建前端镜像**

   ```bash
   # 在项目根目录执行
   docker build -t cloudpaste-frontend:custom -f docker/frontend/Dockerfile .

   # 运行自定义构建的镜像
   docker run -d --name cloudpaste-frontend \
     -p 80:80 \
     -e BACKEND_URL=http://localhost:8787 \
     cloudpaste-frontend:custom
   ```

3. **开发环境 Docker Compose**

   创建 `docker-compose.dev.yml` 文件：

   ```yaml
   version: "3.8"

   services:
     frontend:
       build:
         context: .
         dockerfile: docker/frontend/Dockerfile
       environment:
         - BACKEND_URL=http://backend:8787
       ports:
         - "80:80"
       depends_on:
         - backend

     backend:
       build:
         context: .
         dockerfile: docker/backend/Dockerfile
       environment:
         - NODE_ENV=development
         - RUNTIME_ENV=docker
         - PORT=8787
         - ENCRYPTION_SECRET=dev_secret_key
       volumes:
         - ./sql_data:/data
       ports:
         - "8787:8787"
   ```

   启动开发环境：

   ```bash
   docker-compose -f docker-compose.yml up --build
   ```

## 📄 许可证

Apache License 2.0

本项目使用 Apache License 2.0 许可证 - 详情请参阅 [LICENSE](LICENSE) 文件。

## ❤️ 贡献

- **赞助**：项目维护不易，喜欢本项目的话，可以作者大大一点小小的鼓励哦，您的每一份支持都是我前进的动力\~

  ![image.png](./images/PayQrcode.png)

  <a href="https://afdian.com/a/drag0n"><img width="200" src="https://pic1.afdiancdn.com/static/img/welcome/button-sponsorme.png" alt=""></a>

    - **赞助者**：非常感谢以下赞助者对本项目的支持！！

      [![赞助者](https://afdian.730888.xyz/image)](https://afdian.com/a/drag0n)

- **Contributors**：感谢以下贡献者对本项目的无私贡献！

  [![Contributors](https://contrib.rocks/image?repo=ling-drag0n/CloudPaste)](https://github.com/ling-drag0n/CloudPaste/graphs/contributors)

## Star History

[![Star History Chart](https://api.star-history.com/svg?repos=ling-drag0n/CloudPaste&type=Date)](https://star-history.com/#ling-drag0n/CloudPaste&Date)

**如果觉得项目不错希望您能给个免费的 star✨✨，非常感谢！**
