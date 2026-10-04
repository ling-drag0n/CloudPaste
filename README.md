# CloudPaste 📋

<div align="center">
    <p>
    <a href="README_CN.md">中文</a> | <a href="README.md">English</a> |
    <a href="https://www.readme-i18n.com/ling-drag0n/CloudPaste?lang=es">Español</a> |
    <a href="https://www.readme-i18n.com/ling-drag0n/CloudPaste?lang=fr">français</a> |
    <a href="https://www.readme-i18n.com/ling-drag0n/CloudPaste?lang=ja">日本語</a>
    </p>
    <img width="100" height="100" src="https://img.icons8.com/dusk/100/paste.png" alt="paste"/>
    <h3>🌩️ Serverless file management and Markdown sharing tool, supports multiple storage aggregation, online preview of 30+ file formats, and WebDAV mounting</h3>
</div>

<div align="center">
    <a href="https://deepwiki.com/ling-drag0n/CloudPaste"><img src="https://deepwiki.com/badge.svg" alt="Ask DeepWiki"></a>
    <a href="./LICENSE"><img src="https://img.shields.io/badge/license-Apache%202.0-blue.svg" alt="License"></a>
    <a href="https://github.com/ling-drag0n/CloudPaste/stargazers"><img src="https://img.shields.io/github/stars/ling-drag0n/CloudPaste.svg" alt="GitHub Stars"></a>
    <a href="https://www.cloudflare.com/"><img src="https://img.shields.io/badge/Powered%20by-Cloudflare-F38020?logo=cloudflare" alt="Powered by Cloudflare"></a>
    <a href="https://hub.docker.com/r/dragon730/cloudpaste-backend"><img src="https://img.shields.io/docker/pulls/dragon730/cloudpaste-backend.svg" alt="Docker Pulls"></a>
</div>

<p align="center">
  <a href="#-showcase">📸 Showcase</a> •
  <a href="#-features">✨ Features</a> •
  <a href="#-deployment-guide">🚀 Deployment Guide</a> •
  <a href="#-tech-stack">🔧 Tech Stack</a> •
  <a href="#-development">💻 Development</a> •
  <a href="#-license">📄 License</a>
</p>

## 📸 Showcase

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

## ✨ Core Features

### Cloudflare Native Architecture

- **Edge Computing**: Based on Cloudflare Workers, WorkFlow, and D1 database, with 300+ global nodes for nearby responses
- **Zero Maintenance**: No server management required, automatic scaling, pay-per-request billing
- **Integrated Deployment**: Frontend and backend packaged in the same Worker, saving time and effort
- **Separate Deployment**: Optional frontend-backend separation, flexible to adapt to personal needs

### Multi-Storage Support

- **S3 Compatible**: Cloudflare R2, Backblaze B2, AWS S3, Alibaba Cloud OSS, Tencent Cloud COS, MinIO, etc.
- **Cloud Storage Integration**: WebDAV, OneDrive, Google Drive, Telegram, Discord Bot, HuggingFace Database, GitHub API/Releases (read-only), etc.
- **Local Storage**: Docker deployment supports local file system
- **Smart Upload**: Frontend pre-signed direct upload + streaming upload + chunked resumable upload, with real-time progress display, minimizing CF limitations
- **File Preview**: Direct preview support for 30+ formats (images, videos, audio, PDF, Office, code, e-books, etc.), others can be previewed through external IFrame embedding [KKFileview](https://github.com/kekingcn/kkFileView)
- **Scheduled Tasks**: Support for automated tasks like cleaning upload sessions, storage synchronization, search index rebuilding, etc.
- **Unified Management**: Visual configuration for multiple storage, flexible switching of default storage sources

### Powerful Markdown Editor

- **Vditor Integration**: Support for GitHub-style Markdown, math formulas, flowcharts, mind maps
- **Real-time Preview**: WYSIWYG editing experience
- **Multi-format Export**: One-click export to PDF, HTML, PNG, Word
- **Secure Sharing**: Password protection, expiration time, access count limits
- **Raw Direct Links**: Similar to GitHub Raw, suitable for configuration file hosting

### WebDAV Protocol Support

- **Standard Protocol**: Supports mounting as a network drive with any WebDAV client
- **Complete Operations**: Directory creation, file upload, deletion, renaming, moving
- **Permission Control**: API key authorization, fine-grained access control
- **Cache Optimization**: Configurable TTL, reducing upstream requests

### Flexible Permission Management

- **API Keys**: Create multi-permission read/write keys, bound to specific storage paths
- **Time Control**: Custom validity period, automatic expiration and manual revocation
- **JWT Authentication**: Secure administrator authentication system
- **PWA Support**: Can be installed as a desktop application, usable offline

### Multiple Deployment Methods

- **Automatic Deployment**: One-click deployment via GitHub Actions, supports automatic triggering
- **Manual Deployment**: Wrangler CLI deployment, flexible and controllable
- **Docker Deployment**: Frontend and backend images + Docker Compose one-click startup
- **Multi-platform**: Supports Cloudflare, Vercel, ClawCloud, HuggingFace,etc.

## 🔐 File Transfer Encryption

CloudPaste offers dual-mode file content encryption with performance in mind:

### Server-side transparent encryption (admin toggle, off by default)

- Admin panel → Global Settings → Security → "Server-side file transfer encryption"
- When enabled, newly uploaded shared files are encrypted with **AES-256-GCM (CPENC1 chunked format)** before being written to storage and decrypted transparently on download; the frontend and existing links remain compatible
- Performance: uses the platform-native WebCrypto (hardware accelerated); only affects new uploads and costs nothing while the toggle is off
- Note: encrypted files are always served via proxy (302 direct links disabled); S3 presigned direct upload and resumable upload fall back to proxy upload automatically. Workers free plan has a low CPU limit — use the paid plan or Docker deployment for large files

### End-to-end encryption (E2E, opt-in at share creation)

- Check **End-to-end encryption** in the upload-and-share modal: files are encrypted in the browser before upload, so the server, storage bucket and database only ever hold ciphertext
- The key lives only in the share link fragment (`#e2ek=...`) and is never sent to the server; **anyone with the full link can decrypt** — combine with an access password for two-layer protection
- Viewers decrypt in their browser for preview/download; a link without the key fragment cannot reveal plaintext
- v1 uses in-memory encryption: keep single files under ~200MB

### Additional hardening in this release

- Share passwords no longer appear in URLs: verification now issues a short-lived HMAC access token (`?at=`); legacy `?password=` links keep working
- Password hashing upgraded to PBKDF2-SHA256 (salted; iterations configurable via `PBKDF2_ITERATIONS`, default 100000) with automatic upgrade of legacy records
- Storage credential encryption upgraded to real AES-256-GCM (legacy `encrypted:` values remain readable)
- Proxy signature verification now uses constant-time comparison; new "Force HTTPS" toggle (308 redirect + HSTS, recommended for Docker/self-hosted deployments)

> Important: `ENCRYPTION_SECRET` is the master key of the encryption system — keep it safe and back it up. Rotating it makes existing encrypted files recoverable only with the old key. Keep the full share link (including `#e2ek=`) for E2E shares.

## 🚀 Deployment Guide

### Prerequisites

Before starting deployment, please ensure you have prepared the following:

- [ ] [Cloudflare](https://dash.cloudflare.com) account (required)
- [ ] If using R2: Activate **Cloudflare R2** service and create a bucket (requires payment method)
- [ ] If using Vercel: Register for a [Vercel](https://vercel.com) account
- [ ] Configuration information for other S3 storage services:
    - `S3_ACCESS_KEY_ID`
    - `S3_SECRET_ACCESS_KEY`
    - `S3_BUCKET_NAME`
    - `S3_ENDPOINT`

**The following tutorial may be outdated. For specific details, refer to: [Cloudpaste Online Deployment Documentation](https://doc.cloudpaste.qzz.io)**

<details>
<summary><b>👉 View Complete Deployment Guide</b></summary>

### 📑 Table of Contents

- [Action Automated Deployment](#Action-Automated-Deployment)
    - [Deployment Architecture Selection](#Deployment-Architecture-Selection)
    - [Configure GitHub Repository](#Configure-GitHub-Repository)
    - [Unified Deployment Tutorial (Recommended)](#Unified-Deployment-Tutorial-Recommended)
    - [Separated Deployment Tutorial](#Separated-Deployment-Tutorial)
- [Manual Deployment](#Manual-Deployment)
    - [Unified Manual Deployment (Recommended)](#Unified-Manual-Deployment-Recommended)
    - [Separated Manual Deployment](#Separated-Manual-Deployment)
- [ClawCloud CloudPaste Deployment Tutorial](#ClawCloud-CloudPaste-Deployment-Tutorial)

---

## Action Automated Deployment

Using GitHub Actions enables automatic deployment of your application after code is pushed. CloudPaste offers two deployment architectures for you to choose from.

### Deployment Architecture Selection

#### 🔄 Unified Deployment (Recommended)

**Frontend and backend deployed on the same Cloudflare Worker**

✨ **Advantages:**
- **Same Origin** - No CORS issues, simpler configuration
- **Lower Cost** - Navigation requests are free, saving 60%+ costs compared to separated deployment
- **Simpler Deployment** - Deploy frontend and backend in one go, no need to manage multiple services
- **Better Performance** - Frontend and backend on the same Worker, faster response time

#### 🔀 Separated Deployment

**Backend deployed to Cloudflare Workers, frontend deployed to Cloudflare Pages**

✨ **Advantages:**
-  **Flexible Management** - Independent deployment, no mutual interference
- **Team Collaboration** - Frontend and backend can be maintained by different teams
- **Scalability** - Frontend can easily switch to other platforms (e.g., Vercel)

---

### Configure GitHub Repository

#### 1️⃣ Fork or Clone Repository

Visit and Fork the repository: [https://github.com/ling-drag0n/CloudPaste](https://github.com/ling-drag0n/CloudPaste)

#### 2️⃣ Configure GitHub Secrets

Go to your GitHub repository settings: **Settings** → **Secrets and variables** → **Actions** → **New repository secret**

Add the following Secrets:

| Secret Name             | Required | Purpose                                                                               |
| ----------------------- | -------- | ------------------------------------------------------------------------------------- |
| `CLOUDFLARE_API_TOKEN`  | ✅       | Cloudflare API token (requires Workers, D1, and Pages permissions)                                      |
| `CLOUDFLARE_ACCOUNT_ID` | ✅       | Cloudflare account ID                                                                                   |
| `ENCRYPTION_SECRET`     | ❌       | Key for encrypting sensitive data (will be auto-generated if not provided)                              |
| `ACTIONS_VAR_TOKEN`     | ✅       | GitHub Token for deployment control panel (required only when using the control panel, otherwise skip) |

#### 3️⃣ Obtain Cloudflare API Token

**Get API Token:**

1. Visit [Cloudflare API Tokens](https://dash.cloudflare.com/profile/api-tokens)
2. Click **Create Token**
3. Select **Edit Cloudflare Workers** template
4. **Add additional permissions**:
    - Account → **D1** → **Edit**
    - Account → **Cloudflare Pages** → **Edit** (if using separated deployment)
5. Click **Continue to summary** → **Create Token**
6. **Copy the Token** and save it to GitHub Secrets

![D1 Permission](./images/D1.png)

**Get Account ID:**

1. Visit [Cloudflare Dashboard](https://dash.cloudflare.com)
2. Find **Account ID** in the right sidebar
3. Click to copy and save to GitHub Secrets

#### 4️⃣ (Optional) Configure Deployment Control Panel

If you want to use the visual control panel to manage auto-deployment switches, you need additional configuration:

**Create GitHub Personal Access Token:**

1. Visit [GitHub Token Settings](https://github.com/settings/tokens)
2. Click **Generate new token** → **Generate new token (classic)**
3. Set Token name (e.g., `CloudPaste Deployment Control`)
4. Select permissions:
    - ✅ **repo** (Full repository access)
    - ✅ **workflow** (Workflow permissions)
5. Click **Generate token**
6. Copy the Token and save as Secret `ACTIONS_VAR_TOKEN`

**Using the Control Panel:**

1. Go to repository **Actions** tab
2. In the left workflow list, click **🎛️ Deployment Control Panel**
3. Click **Run workflow** → **Run workflow** on the right
4. In the popup, select the deployment method to enable/disable
5. Click **Run workflow** to apply configuration
6. After updating the switch state, the control panel will automatically trigger the corresponding deployment workflow once (whether it actually deploys is decided by the current switch state)

---

### 🔄 Unified Deployment Tutorial (Recommended)

#### Deployment Steps

1️⃣ **Configure GitHub Secrets** (refer to the configuration section above)

2️⃣ **Trigger Deployment Workflow**

Method 1: Manual Trigger (recommended for first deployment)

- Go to repository **Actions** tab
- Click **Deploy SPA CF Workers[一体化部署]** on the left
- Click **Run workflow** on the right → select `main` branch → **Run workflow**

Method 2: Auto Trigger

- Use the deployment control panel to enable **SPA Unified Auto Deploy**
- After that, deployment will be triggered automatically when pushing code to `frontend/` or `backend/` directory to `main` branch

> Note: When you manually run **Deploy SPA CF Workers[一体化部署]** from the Actions page, it will **always deploy once regardless of the auto-deploy switch**. Automatic behavior (push or control panel triggered) is still controlled by the `SPA_DEPLOY` switch.

3️⃣ **Wait for Deployment to Complete**

The deployment process takes about 3-5 minutes. The workflow will automatically complete the following steps:

- ✅ Build frontend static assets
- ✅ Install backend dependencies
- ✅ Create/verify D1 database
- ✅ Initialize database schema
- ✅ Set encryption secret
- ✅ Deploy to Cloudflare Workers

4️⃣ **Get Deployment URL**

After successful deployment, you will see output similar to this in the Actions log:

```
Published cloudpaste-spa (X.XX sec)
  https://cloudpaste-spa.your-account.workers.dev
```

Your CloudPaste has been successfully deployed! Visit the URL above to use it.

#### Deployment Complete

**Visit your application:** `https://cloudpaste-spa.your-account.workers.dev`

**Post-deployment Configuration:**

1. The database will be automatically initialized on first visit
2. Log in with the default admin account:
    - Username: `admin`
    - Password: `admin123`
3. **⚠️ Important: Change the default admin password immediately!**
4. Configure your S3-compatible storage service in the admin panel
5. (Optional) Bind a custom domain in Cloudflare Dashboard

**Advantages Recap:**
- ✅ Same origin for frontend and backend, no CORS issues
- ✅ Navigation requests are free, reducing costs by 60%+
- ✅ Deploy in one go, simple management

---

### 🔀 Separated Deployment Tutorial

If you choose separated deployment, follow these steps:

#### Backend Deployment

1️⃣ **Configure GitHub Secrets** (refer to the configuration section above)

2️⃣ **Trigger Backend Deployment**

Method 1: Manual Trigger

- Go to repository **Actions** tab
- Click **Deploy Backend CF Workers[Worker后端分离部署]** on the left
- Click **Run workflow** → **Run workflow**

Method 2: Auto Trigger

- Use the deployment control panel to enable **Backend Separated Auto Deploy**
- Deployment will be triggered automatically when pushing `backend/` directory code

3️⃣ **Wait for Deployment to Complete**

The workflow will automatically complete:

- ✅ Create/verify D1 database
- ✅ Initialize database schema
- ✅ Set encryption secret
- ✅ Deploy Worker to Cloudflare

4️⃣ **Record Backend URL**

After successful deployment, note down your backend Worker URL:
`https://cloudpaste-backend.your-account.workers.dev`

**<span style="color:red">⚠️ Important: Remember your backend domain, you'll need it for frontend deployment!</span>**

#### Frontend Deployment

##### Cloudflare Pages

1️⃣ **Trigger Frontend Deployment**

Method 1: Manual Trigger

- Go to repository **Actions** tab
- Click **Deploy Frontend CF Pages[Pages前端分离部署]** on the left
- Click **Run workflow** → **Run workflow**

Method 2: Auto Trigger

- Use the deployment control panel to enable **Frontend Separated Auto Deploy**
- Deployment will be triggered automatically when pushing `frontend/` directory code

> Note: When you manually run the **Backend** or **Frontend** deployment workflows from the Actions page, they will **always deploy once regardless of the auto-deploy switch**. Automatic behavior is controlled by the `BACKEND_DEPLOY` / `FRONTEND_DEPLOY` switches.

2️⃣ **Configure Environment Variables**

**Required step: After frontend deployment, you must manually configure the backend address!**

1. Log in to [Cloudflare Dashboard](https://dash.cloudflare.com/)
2. Navigate to **Pages** → **cloudpaste-frontend**
3. Click **Settings** → **Environment variables**
4. Add environment variable:
    - **Name**: `VITE_BACKEND_URL`
    - **Value**: Your backend Worker URL (e.g., `https://cloudpaste-backend.your-account.workers.dev`)
    - **Note**: No trailing `/`, custom domain recommended

**<span style="color:red">⚠️ Must fill in the complete backend domain, format: https://xxxx.com</span>**

3️⃣ **Redeploy Frontend**

**Important: After configuring environment variables, you must run the frontend workflow again!**

- Return to GitHub Actions
- Manually trigger **Deploy Frontend CF Pages** workflow again
- This is necessary to load the backend domain configuration

![Frontend Redeploy](./images/test-1.png)

4️⃣ **Access Application**

Frontend deployment URL: `https://cloudpaste-frontend.pages.dev`

**<span style="color:red">⚠️ Please strictly follow the steps, otherwise backend domain loading will fail!</span>**

##### Vercel (Alternative)

Vercel deployment steps:

1. Import GitHub project in Vercel after forking
2. Configure deployment parameters:

```
Framework Preset: Vite
Build Command: npm run build
Output Directory: dist
Install Command: npm install
```

3. Configure environment variables:
    - Name: `VITE_BACKEND_URL`
    - Value: Your backend Worker URL
4. Click **Deploy** button to deploy

**☝️ Choose either Cloudflare Pages or Vercel**

**<span style="color:red">⚠️ Security Notice: Please change the default admin password immediately after system initialization (username: admin, password: admin123).</span>**

---

## Manual Deployment

CloudPaste supports two manual deployment methods: unified deployment (recommended) and separated deployment.

### 🔄 Unified Manual Deployment (Recommended)

Unified deployment deploys both frontend and backend to the same Cloudflare Worker, offering simpler configuration and lower costs.

#### Step 1: Clone Repository

```bash
git clone https://github.com/ling-drag0n/CloudPaste.git
cd CloudPaste
```

#### Step 2: Build Frontend

```bash
cd frontend
npm install
npm run build
cd ..
```

**Verify build output:** Ensure `frontend/dist` directory exists and contains `index.html`

#### Step 3: Configure Backend

```bash
cd backend
npm install
npx wrangler login
```

#### Step 4: Create D1 Database

```bash
npx wrangler d1 create cloudpaste-db
```

Note the `database_id` from the output (e.g., `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`)

#### Step 5: Initialize Database

```bash
npx wrangler d1 execute cloudpaste-db --file=./schema.sql
```

#### Step 6: Configure wrangler.spa.toml

Edit `backend/wrangler.spa.toml` file and modify the database ID:

```toml
[[d1_databases]]
binding = "DB"
database_name = "cloudpaste-db"
database_id = "YOUR_DATABASE_ID"  # Replace with ID from Step 4
```

#### Step 7: Deploy to Cloudflare Workers

```bash
npx wrangler deploy --config wrangler.spa.toml
```

After successful deployment, you'll see your application URL:

```
Published cloudpaste-spa (X.XX sec)
  https://cloudpaste-spa.your-account.workers.dev
```

#### Deployment Complete!

**Visit your application:** Open the URL above to use CloudPaste

**Post-deployment Configuration:**
1. The database will be automatically initialized on first visit
2. Log in with the default admin account (username: `admin`, password: `admin123`)
3. **⚠️ Change the default admin password immediately!**
4. Configure S3-compatible storage service in the admin panel
5. (Optional) Bind a custom domain in Cloudflare Dashboard

**<span style="color:red">⚠️ Security Notice: Please change the default admin password immediately after system initialization.</span>**

---

### 🔀 Separated Manual Deployment

If you need to deploy and manage frontend and backend independently, you can choose the separated deployment method.

#### Backend Manual Deployment

1. Clone the repository

```bash
git clone https://github.com/ling-drag0n/CloudPaste.git
cd CloudPaste/backend
```

2. Install dependencies

   ```bash
   npm install
   ```

3. Log in to Cloudflare

   ```bash
   npx wrangler login
   ```

4. Create D1 database

   ```bash
   npx wrangler d1 create cloudpaste-db
   ```

   Note the database ID from the output.

5. Modify wrangler.toml configuration

   ```toml
   [[d1_databases]]
   binding = "DB"
   database_name = "cloudpaste-db"
   database_id = "YOUR_DATABASE_ID"
   ```

6. Deploy Worker

   ```bash
   npx wrangler deploy
   ```

   Note the URL from the output; this is your backend API address.

7. Initialize database (automatic)
   Visit your Worker URL to trigger initialization:

   ```
   https://cloudpaste-backend.your-username.workers.dev
   ```

**<span style="color:red">⚠️ Important: Remember your backend domain, you'll need it for frontend deployment!</span>**

#### Frontend Manual Deployment

#### Cloudflare Pages

1. Prepare frontend code

   ```bash
   cd CloudPaste/frontend
   npm install
   ```

2. Configure environment variables
   Create or modify the `.env.production` file:

   ```
   VITE_BACKEND_URL=https://cloudpaste-backend.your-username.workers.dev
   VITE_APP_ENV=production
   VITE_ENABLE_DEVTOOLS=false
   ```

3. Build frontend project

   ```bash
   npm run build
   ```

   [Be careful when building! !](https://github.com/ling-drag0n/CloudPaste/issues/6#issuecomment-2818746354)

4. Deploy to Cloudflare Pages

   **Method 1**: Via Wrangler CLI

   ```bash
   npx wrangler pages deploy dist --project-name=cloudpaste-frontend
   ```

   **Method 2**: Via Cloudflare Dashboard

    1. Log in to [Cloudflare Dashboard](https://dash.cloudflare.com/)
    2. Select "Pages"
    3. Click "Create a project" → "Direct Upload"
    4. Upload files from the `dist` directory
    5. Set project name (e.g., "cloudpaste-frontend")
    6. Click "Save and Deploy"

#### Vercel

1. Prepare frontend code

   ```bash
   cd CloudPaste/frontend
   npm install
   ```

2. Install and log in to Vercel CLI

   ```bash
   npm install -g vercel
   vercel login
   ```

3. Configure environment variables, same as for Cloudflare Pages
4. Build and deploy

   ```bash
   vercel --prod
   ```

   Follow the prompts to configure the project.

---

## ClawCloud CloudPaste Deployment Tutorial

#### 10GB free traffic per month, suitable for light usage only

###### Step 1:

Registration link: [Claw Cloud](https://ap-northeast-1.run.claw.cloud/signin) (no #AFF)
No credit card required, as long as your GitHub registration date is more than 180 days, you get $5 credit every month.

###### Step 2:

After registration, click APP Launchpad on the homepage, then click create app in the upper right corner

![image.png](https://s2.loli.net/2025/04/21/soj5eWMhxTg1VFt.png)

###### Step 3:

First deploy the backend, as shown in the figure (for reference only):
![image.png](https://s2.loli.net/2025/04/21/AHrMnuVyNhK6eUk.png)

Backend data storage is here:
![image.png](https://s2.loli.net/2025/04/21/ANaoU5Y6cxPOVfw.png)

###### Step 4:

Then the frontend, as shown in the figure (for reference only):
![image.png](https://s2.loli.net/2025/04/21/kaT5Qu8ctovFdUp.png)

##### Deployment is complete and ready to use, custom domain names can be configured as needed

</details>

<details>
<summary><b>👉 Docker Deployment Guide</b></summary>

### 📑 Table of Contents

- [Docker Command Line Deployment](#Docker-Command-Line-Deployment)
    - [Backend Docker Deployment](#Backend-Docker-Deployment)
    - [Frontend Docker Deployment](#Frontend-Docker-Deployment)
- [Docker Compose One-Click Deployment](#Docker-Compose-One-Click-Deployment)

---

## Docker Command Line Deployment

### Backend Docker Deployment

CloudPaste backend can be quickly deployed using the official Docker image.

1. Create data storage directory

   ```bash
   mkdir -p sql_data
   ```

2. Run the backend container

   ```bash
   docker run -d --name cloudpaste-backend \
     -p 8787:8787 \
     -v $(pwd)/sql_data:/data \
     -e ENCRYPTION_SECRET=your-encryption-key \
     -e NODE_ENV=production \
     dragon730/cloudpaste-backend:latest
   ```

   Note the deployment URL (e.g., `http://your-server-ip:8787`), which will be needed for the frontend deployment.

**<span style="color:red">⚠️ Security tip: Be sure to customize ENCRYPTION_SECRET and keep it safe, as this key is used to encrypt sensitive data.</span>**

### Frontend Docker Deployment

The frontend uses Nginx to serve and configures the backend API address at startup.

```bash
docker run -d --name cloudpaste-frontend \
  -p 80:80 \
  -e BACKEND_URL=http://your-server-ip:8787 \
  dragon730/cloudpaste-frontend:latest
```

**<span style="color:red">⚠️ Note: BACKEND_URL must include the complete URL (including protocol http:// or https://)</span>**
**<span style="color:red">⚠️ Security reminder: Please change the default administrator password immediately after system initialization (Username: admin, Password: admin123).</span>**

### Docker Image Update

When a new version of the project is released, you can update your Docker deployment following these steps:

1. Pull the latest images

   ```bash
   docker pull dragon730/cloudpaste-backend:latest
   docker pull dragon730/cloudpaste-frontend:latest
   ```

2. Stop and remove old containers

   ```bash
   docker stop cloudpaste-backend cloudpaste-frontend
   docker rm cloudpaste-backend cloudpaste-frontend
   ```

3. Start new containers using the same run commands as above (preserving data directory and configuration)

## Docker Compose One-Click Deployment

Using Docker Compose allows you to deploy both frontend and backend services with one click, which is the simplest recommended method.

1. Create a `docker-compose.yml` file

```yaml
version: "3.8"

services:
  frontend:
    image: dragon730/cloudpaste-frontend:latest
    environment:
      - BACKEND_URL=https://xxx.com # Fill in the backend service address
    ports:
      - "8080:80" #"127.0.0.1:8080:80"
    depends_on:
      - backend # Depends on backend service
    networks:
      - cloudpaste-network
    restart: unless-stopped

  backend:
    image: dragon730/cloudpaste-backend:latest
    environment:
      - NODE_ENV=production
      - PORT=8787
      - ENCRYPTION_SECRET=custom-key # Please modify this to your own security key
      - TASK_WORKER_POOL_SIZE=2
    volumes:
      - ./sql_data:/data # Data persistence
    ports:
      - "8787:8787" #"127.0.0.1:8787:8787"
    networks:
      - cloudpaste-network
    restart: unless-stopped

networks:
  cloudpaste-network:
    driver: bridge
```

2. Start the services

```bash
docker-compose up -d
```

**<span style="color:red">⚠️ Security reminder: Please change the default administrator password immediately after system initialization (Username: admin, Password: admin123).</span>**

3. Access the services

Frontend: `http://your-server-ip:80`
Backend: `http://your-server-ip:8787`

### Docker Compose Update

When you need to update to a new version:

1. Pull the latest images

   ```bash
   docker-compose pull
   ```

2. Recreate containers using new images (preserving data volumes)

   ```bash
   docker-compose up -d --force-recreate
   ```

**<span style="color:orange">💡 Tip: If there are configuration changes, you may need to backup data and modify the docker-compose.yml file</span>**

### Nginx Reverse Proxy Example

```nginx
server {
    listen 443 ssl;
    server_name paste.yourdomain.com;  # Replace with your domain name

    # SSL certificate configuration
    ssl_certificate     /path/to/cert.pem;  # Replace with certificate path
    ssl_certificate_key /path/to/key.pem;   # Replace with key path

    # Frontend proxy configuration
    location / {
        proxy_pass http://localhost:80;  # Docker frontend service address
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }

    # Backend API proxy configuration
    location /api {
        proxy_pass http://localhost:8787;  # Docker backend service address
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        client_max_body_size 0;

        # WebSocket support (if needed)
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
    }

    # WebDAV Configuration
    location /dav {
        proxy_pass http://localhost:8787/dav;  # Points to your backend service

        # WebDAV necessary headers
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;

        # WebDAV method support
        proxy_pass_request_headers on;

        # Support all WebDAV methods
        proxy_method $request_method;

        # Necessary header processing
        proxy_set_header Destination $http_destination;
        proxy_set_header Overwrite $http_overwrite;

        # Handle large files
        client_max_body_size 0;

        # Timeout settings
        proxy_connect_timeout 3600s;
        proxy_send_timeout 3600s;
        proxy_read_timeout 3600s;
    }
}
```

**<span style="color:red">⚠️ Security tip: It is recommended to configure HTTPS and a reverse proxy (such as Nginx) to enhance security.</span>**

</details>

<details>
<summary><b>👉 S3 Cross-Origin Configuration Guide</b></summary>

## R2 API Retrieval and Cross-Origin Configuration

1. Log in to Cloudflare Dashboard
2. Click R2 Storage and create a bucket.
3. Create API token
   ![R2api](./images/R2/R2-api.png)
   ![R2rw](./images/R2/R2-rw.png)

4. Save all data after creation; you'll need it later
5. Configure cross-origin rules: click the corresponding bucket, click Settings, edit CORS policy as shown below:

```json
[
  {
    "AllowedOrigins": ["http://localhost:3000", "https://replace-with-your-frontend-domain"],
    "AllowedMethods": ["GET", "PUT", "POST", "DELETE", "HEAD"],
    "AllowedHeaders": ["*"],
    "ExposeHeaders": ["ETag"],
    "MaxAgeSeconds": 3600
  }
]
```

## B2 API Retrieval and Cross-Origin Configuration

1. If you don't have a B2 account, [register](https://www.backblaze.com/sign-up/cloud-storage?referrer=getstarted) one first, then create a bucket.
   ![B2账号注册](./images/B2/B2-1.png)
2. Click Application Key in the sidebar, click Create Key, and follow the illustration.
   ![B2key](./images/B2/B2-2.png)
3. Configure B2 cross-origin; B2 cross-origin configuration is more complex, take note
   ![B2cors](./images/B2/B2-3.png)
4. You can try options 1 or 2 first, go to the upload page and see if you can upload. If F12 console shows cross-origin errors, use option 3. For a permanent solution, use option 3 directly.

   ![B21](./images/B2/B2-4.png)

Regarding option 3 configuration, since the panel cannot configure it, you need to configure manually by [downloading B2 CLI](https://www.backblaze.com/docs/cloud-storage-command-line-tools) tool. For more details, refer to: "https://docs.cloudreve.org/zh/usage/storage/b2".

After downloading, in the corresponding download directory CMD, enter the following commands:

```txt
b2-windows.exe account authorize   //Log in to your account, following prompts to enter your keyID and applicationKey
b2-windows.exe bucket get <bucketName> //You can execute to get bucket information, replace <bucketName> with your bucket name
```

Windows configuration, Use ".\b2-windows.exe xxx",
Python CLI would be similar:

```cmd
b2-windows.exe bucket update <bucketName> allPrivate --cors-rules "[{\"corsRuleName\":\"CloudPaste\",\"allowedOrigins\":[\"*\"],\"allowedHeaders\":[\"*\"],\"allowedOperations\":[\"b2_upload_file\",\"b2_download_file_by_name\",\"b2_download_file_by_id\",\"s3_head\",\"s3_get\",\"s3_put\",\"s3_post\",\"s3_delete\"],\"exposeHeaders\":[\"Etag\",\"content-length\",\"content-type\",\"x-bz-content-sha1\"],\"maxAgeSeconds\":3600}]"
```

Replace <bucketName> with your bucket name. For allowedOrigins in the cross-origin allowance, you can configure based on your needs; here it allows all.

5. Cross-origin configuration complete

## MinIO API Access and Cross-Origin Configuration

1. **Deploy MinIO Server**

   Use the following Docker Compose configuration (reference) to quickly deploy MinIO:

   ```yaml
   version: "3"

   services:
     minio:
       image: minio/minio:RELEASE.2025-02-18T16-25-55Z
       container_name: minio-server
       command: server /data --console-address :9001 --address :9000
       environment:
         - MINIO_ROOT_USER=minioadmin # Admin username
         - MINIO_ROOT_PASSWORD=minioadmin # Admin password
         - MINIO_BROWSER=on
         - MINIO_SERVER_URL=https://minio.example.com # S3 API access URL
         - MINIO_BROWSER_REDIRECT_URL=https://console.example.com # Console access URL
       ports:
         - "9000:9000" # S3 API port
         - "9001:9001" # Console port
       volumes:
         - ./data:/data
         - ./certs:/root/.minio/certs # SSL certificates (if needed)
       restart: always
   ```

   Run `docker-compose up -d` to start the service.

2. **Configure Reverse Proxy (Reference)**

   To ensure MinIO functions correctly, especially file previews, configure reverse proxy properly. Recommended OpenResty/Nginx settings:

   **MinIO S3 API Reverse Proxy (minio.example.com)**:

   ```nginx
   location / {
       proxy_pass http://127.0.0.1:9000;
       proxy_set_header Host $host;
       proxy_set_header X-Real-IP $remote_addr;
       proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
       proxy_set_header X-Forwarded-Proto $scheme;

       # HTTP optimization
       proxy_http_version 1.1;
       proxy_set_header Connection "";  # Enable HTTP/1.1 keepalive

       # Critical: Resolve 403 errors & preview issues
       proxy_cache off;
       proxy_buffering off;
       proxy_request_buffering off;

       # No file size limit
       client_max_body_size 0;
   }
   ```

   **MinIO Console Reverse Proxy (console.example.com)**:

   ```nginx
   location / {
       proxy_pass http://127.0.0.1:9001;
       proxy_set_header Host $host;
       proxy_set_header X-Real-IP $remote_addr;
       proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
       proxy_set_header X-Forwarded-Proto $scheme;

       # WebSocket support
       proxy_http_version 1.1;
       proxy_set_header Upgrade $http_upgrade;
       proxy_set_header Connection "upgrade";

       # Critical settings
       proxy_cache off;
       proxy_buffering off;

       # No file size limit
       client_max_body_size 0;
   }
   ```

3. **Access Console to Create Buckets & Access Keys**

   For detailed configuration, refer to official docs:  
   https://min.io/docs/minio/container/index.html  
   CN: https://min-io.cn/docs/minio/container/index.html

   ![minio-1](./images/minio-1.png)

4. **Additional Configuration (Optional)**

   Allowed origins must include your frontend domain.  
   ![minio-2](./images/minio-2.png)

5. **Configure MinIO in CloudPaste**

    - Log in to CloudPaste admin panel
    - Go to "S3 Storage Settings" → "Add Storage Configuration"
    - Select "Other S3-compatible service" as provider
    - Enter details:
        - Name: Custom name
        - Endpoint URL: MinIO service URL (e.g., `https://minio.example.com`)
        - Bucket Name: Pre-created bucket
        - Access Key ID: Your Access Key
        - Secret Key: Your Secret Key
        - Region: Leave empty
        - Path-Style Access: MUST ENABLE!
    - Click "Test Connection" to verify
    - Save settings

6. **Troubleshooting**

    - **Note**: If using Cloudflare's CDN, you may need to add `proxy_set_header Accept-Encoding "identity"`, and there are caching issues to consider. It is recommended to use only DNS resolution.
    - **403 Error**: Ensure reverse proxy includes `proxy_cache off` & `proxy_buffering off`
    - **Preview Issues**: Verify `MINIO_SERVER_URL` & `MINIO_BROWSER_REDIRECT_URL` are correctly set
    - **Upload Failures**: Check CORS settings; allowed origins must include frontend domain
    - **Console Unreachable**: Verify WebSocket config, especially `Connection "upgrade"`

## More S3-related configurations to come......

</details>

<details>
<summary><b>👉 WebDAV Configuration Guide</b></summary>

## WebDAV Configuration and Usage Guide

CloudPaste provides simple WebDAV protocol support, allowing you to mount storage spaces as network drives for convenient access and management of files directly through file managers.

### WebDAV Service Basic Information

- **WebDAV Base URL**: `https://your-backend-domain/dav`
- **Supported Authentication Methods**:
    - Basic Authentication (username+password)
- **Supported Permission Types**:
    - Administrator accounts - Full operation permissions
    - API keys - Requires enabled mount permission (mount_permission)

### Permission Configuration

#### 1. Administrator Account Access

Use administrator account and password to directly access the WebDAV service:

- **Username**: Administrator username
- **Password**: Administrator password

#### 2. API Key Access (Recommended)

For a more secure access method, it is recommended to create a dedicated API key:

1. Log in to the management interface
2. Navigate to "API Key Management"
3. Create a new API key, **ensure "Mount Permission" is enabled**
4. Usage method:
    - **Username**: API key value
    - **Password**: The same API key value as the username

### NGINX Reverse Proxy Configuration

If using NGINX as a reverse proxy, specific WebDAV configuration needs to be added to ensure all WebDAV methods work properly:

```nginx
# WebDAV Configuration
location /dav {
    proxy_pass http://localhost:8787;  # Points to your backend service

    # WebDAV necessary headers
    proxy_set_header Host $host;
    proxy_set_header X-Real-IP $remote_addr;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;

    # WebDAV method support
    proxy_pass_request_headers on;

    # Support all WebDAV methods
    proxy_method $request_method;

    # Necessary header processing
    proxy_set_header Destination $http_destination;
    proxy_set_header Overwrite $http_overwrite;

    # Handle large files
    client_max_body_size 0;

    # Timeout settings
    proxy_connect_timeout 3600s;
    proxy_send_timeout 3600s;
    proxy_read_timeout 3600s;
}
```

### Common Issues and Solutions

1. **Connection Problems**:

    - Confirm the WebDAV URL format is correct
    - Verify that authentication credentials are valid
    - Check if the API key has mount permission

2. **Permission Errors**:

    - Confirm the account has the required permissions
    - Administrator accounts should have full permissions
    - API keys need to have mount permission specifically enabled

3. **⚠️⚠️ WebDAV Upload Issues**:

    - The upload size for webdav deployed by Workers may be limited by CF's CDN restrictions to around 100MB, resulting in a 413 error.
    - For Docker deployments, just pay attention to the nginx proxy configuration, any upload mode is acceptable

</details>

## 🔧 Tech Stack

### Frontend

- **Framework**: Vue.js 3 + Vite
- **Styling**: TailwindCSS
- **Editor**: Vditor
- **Internationalization**: Vue-i18n
- **Charts**: Chart.js + Vue-chartjs

### Backend

- **Runtime**: Cloudflare Workers
- **Framework**: Hono
- **Database**: Cloudflare D1 (SQLite)
- **Storage**: Multiple S3-compatible services (supports R2, B2, AWS S3)
- **Authentication**: JWT tokens + API keys

## 💻 Development

### API Documentation

[API Documentation](Api-doc.md)

[Server Direct File Upload API Documentation](Api-s3_direct.md) - Detailed description of the server direct file upload interface

### Local Development Setup

1. **Clone project repository**

   ```bash
   git clone https://github.com/ling-drag0n/cloudpaste.git
   cd cloudpaste
   ```

2. **Backend setup**

   ```bash
   cd backend
   npm install
   # Initialize D1 database
   wrangler d1 create cloudpaste-db
   wrangler d1 execute cloudpaste-db --file=./schema.sql
   ```

3. **Frontend setup**

   ```bash
   cd frontend
   npm install
   ```

4. **Configure environment variables**

    - In the `backend` directory, create a `wrangler.toml` file to set development environment variables
    - In the `frontend` directory, configure the `.env.development` file to set frontend environment variables

5. **Start development servers**

   ```bash
   # Backend
   cd backend
   npm run dev

   # Frontend (in another terminal)
   cd frontend
   npm run dev
   ```

### Project Structure

```
CloudPaste/
├── frontend/                         # Frontend Vite + Vue 3 SPA
│   ├── src/
│   │   ├── api/                      # HTTP client & API services (no domain semantics)
│   │   ├── modules/                  # Domain modules layer (by business area)
│   │   │   ├── paste/                # Text sharing (editor / public view / admin)
│   │   │   ├── fileshare/            # File sharing (public page / admin)
│   │   │   ├── fs/                   # Mounted file system explorer (MountExplorer)
│   │   │   ├── upload/               # Upload controller & upload views
│   │   │   ├── storage-core/         # Storage drivers & Uppy wiring (low-level abstraction)
│   │   │   ├── security/             # Frontend auth bridge & Authorization header helpers
│   │   │   ├── pwa-offline/          # PWA offline queue & state
│   │   │   └── admin/                # Admin panel (dashboard / settings / key management, etc.)
│   │   ├── components/               # Reusable, cross-module UI components (no module imports)
│   │   ├── composables/              # Shared composition APIs (file-system / preview / upload, etc.)
│   │   ├── stores/                   # Pinia stores (auth / fileSystem / siteConfig, etc.)
│   │   ├── router/                   # Vue Router configuration (single entry for all views)
│   │   ├── pwa/                      # PWA state & installation prompts
│   │   ├── utils/                    # Utilities (clipboard / time / file icons, etc.)
│   │   ├── styles/                   # Global styles & Tailwind config entry
│   │   └── assets/                   # Static assets
│   ├── eslint.config.cjs             # Frontend ESLint config (including import boundaries)
│   ├── vite.config.js                # Vite build configuration
│   └── package.json
├── backend/                          # Backend (Cloudflare Workers / Docker runtime)
│   ├── src/
│   │   ├── routes/                   # HTTP routing layer (fs / files / pastes / admin / system, etc.)
│   │   │   ├── fs/                   # Mount FS APIs (list / read / write / search / share)
│   │   │   ├── files/                # File sharing APIs (public / protected)
│   │   │   ├── pastes/               # Text sharing APIs (public / protected)
│   │   │   ├── adminRoutes.js        # Generic admin routes
│   │   │   ├── apiKeyRoutes.js       # API key management routes
│   │   │   ├── mountRoutes.js        # Mount configuration routes
│   │   │   ├── systemRoutes.js       # System settings & dashboard stats
│   │   │   └── fsRoutes.js           # Unified FS entry aggregation
│   │   ├── services/                 # Domain services (pastes / files / system / apiKey, etc.)
│   │   ├── security/                 # Auth + authorization (AuthService / securityContext / authorize / policies)
│   │   ├── webdav/                   # WebDAV implementation & path handling
│   │   ├── storage/                  # Storage abstraction (S3 drivers, mount manager, file system ops)
│   │   ├── repositories/             # Data access layer (D1 + SQLite repositories)
│   │   ├── cache/                    # Cache & invalidation (mainly FS)
│   │   ├── constants/                # Constants (ApiStatus / Permission / DbTables / UserType, etc.)
│   │   ├── http/                     # Unified error types & response helpers
│   │   └── utils/                    # Utilities (common / crypto / environment, etc.)
│   ├── schema.sql                    # D1 / SQLite schema bootstrap
│   ├── wrangler.toml                 # Cloudflare Workers / D1 configuration
│   └── package.json
├── docs/                             # Architecture & design docs
│   ├── frontend-architecture-implementation.md    # Frontend layering & modules/* design
│   ├── frontend-architecture-optimization-plan.md # Frontend optimization plan (Phase 2/3)
│   ├── auth-permissions-design.md                # Auth & permissions system design
│   └── backend-error-handling-refactor.md        # Backend error handling refactor design
├── docker/                           # Docker & Compose deployment configs
├── images/                           # Screenshots used in README
├── Api-doc.md                        # API overview
├── Api-s3_direct.md                  # S3 direct upload API docs
└── README.md                         # Main project README
```

### Custom Docker Build

If you want to customize Docker images or debug during development, you can follow these steps to build manually:

1. **Build backend image**

   ```bash
   # Execute in the project root directory
   docker build -t cloudpaste-backend:custom -f docker/backend/Dockerfile .

   # Run the custom built image
   docker run -d --name cloudpaste-backend \
     -p 8787:8787 \
     -v $(pwd)/sql_data:/data \
     -e ENCRYPTION_SECRET=development-test-key \
     cloudpaste-backend:custom
   ```

2. **Build frontend image**

   ```bash
   # Execute in the project root directory
   docker build -t cloudpaste-frontend:custom -f docker/frontend/Dockerfile .

   # Run the custom built image
   docker run -d --name cloudpaste-frontend \
     -p 80:80 \
     -e BACKEND_URL=http://localhost:8787 \
     cloudpaste-frontend:custom
   ```

3. **Development environment Docker Compose**

   Create a `docker-compose.dev.yml` file:

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

   Start the development environment:

   ```bash
   docker-compose -f docker-compose.yml up --build
   ```

## 📄 License

Apache License 2.0

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## ❤️ Contribution
- **Sponsorship**: Maintaining the project is not easy. If you like this project, you can give the author a little encouragement. Every bit of your support is the motivation for me to move forward~

  ![image.png](./images/PayQrcode.png)

  <a href="https://afdian.com/a/drag0n"><img width="200" src="https://pic1.afdiancdn.com/static/img/welcome/button-sponsorme.png" alt=""></a>

    - **Sponsors**: A huge thank you to the following sponsors for their support of this project!!

      [![Sponsors](https://afdian.730888.xyz/image)](https://afdian.com/a/drag0n)

- **Contributors**: Thanks to the following contributors for their selfless contributions to this project!

  [![Contributors](https://contrib.rocks/image?repo=ling-drag0n/CloudPaste)](https://github.com/ling-drag0n/CloudPaste/graphs/contributors)

**If you think the project is good I hope you can give a free star✨✨, Thank you very much!**
