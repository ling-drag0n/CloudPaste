# CloudPaste 后端服务 API 文档

## API 文档

所有 API 返回格式统一为：

```json
{
  "code": 200, // HTTP状态码
  "message": "success", // 消息
  "data": {}, // 数据内容
  "success": true // 操作是否成功
}
```

## 认证方式

### 1. 管理员认证

使用 Bearer Token 认证，需要在请求头中添加：

```
Authorization: Bearer <admin_token>
```

管理员令牌通过 `/api/admin/login` 接口获取。

### 2. API 密钥认证

使用 ApiKey 认证，需要在请求头中添加：

```
Authorization: ApiKey <api_key>
```

API 密钥由管理员在后台创建，使用位标志权限系统，支持以下权限类型：

**权限位标志值：**

- **1 (TEXT)**: 文本分享权限 - 允许创建、查看、修改和删除文本分享
- **2 (FILE_SHARE)**: 文件分享权限 - 允许创建和管理文件分享链接
- **256 (MOUNT_VIEW)**: 挂载页查看权限 - 允许浏览挂载页内容
- **512 (MOUNT_UPLOAD)**: 上传权限 - 允许上传文件和创建目录
- **1024 (MOUNT_COPY)**: 复制权限 - 允许复制文件和目录
- **2048 (MOUNT_RENAME)**: 重命名权限 - 允许重命名文件和目录
- **4096 (MOUNT_DELETE)**: 删除权限 - 允许删除文件和目录
- **65536 (WEBDAV_READ)**: WebDAV 读取权限 - 允许通过 WebDAV 读取文件（GET、PROPFIND 等）
- **131072 (WEBDAV_MANAGE)**: WebDAV 管理权限 - 允许通过 WebDAV 管理文件（PUT、DELETE、MKCOL 等）

**权限组合示例：**

- `259` = TEXT + FILE_SHARE + MOUNT_VIEW (1+2+256) - 基础查看权限
- `771` = 基础权限 + MOUNT_UPLOAD (1+2+256+512) - 包含上传权限
- `7939` = 除 WebDAV 外的所有权限 (1+2+256+512+1024+2048+4096) - 挂载页完整权限
- `198915` = 所有权限 (1+2+256+512+1024+2048+4096+65536+131072) - 完整权限

**路径限制：**

- **basic_path**: 路径权限限制 - 限制 API 密钥用户只能访问指定路径及其子路径

### 3. WebDAV 认证

WebDAV 支持两种认证方式：

#### Basic Auth（推荐）

```
Authorization: Basic <base64(api_key:api_key)>
```

#### Bearer Token

```
Authorization: Bearer <api_key>
```

### 4. 自定义授权头

部分 API 还支持自定义授权头：

```
X-Custom-Auth-Key: <api_key>
```

### 认证错误响应

认证失败时返回统一的错误格式：

```json
{
  "code": 401,
  "message": "需要认证访问",
  "success": false
}
```

权限不足时返回：

```json
{
  "code": 403,
  "message": "权限不足",
  "success": false
}
```

### 公共 API

#### 基础 API

- `GET /api/health`

  - 描述：API 健康检查端点，用于监控服务状态
  - 参数：无
  - 响应：
    ```json
    {
      "status": "ok",
      "timestamp": "2023-05-01T12:00:00Z"
    }
    ```

- `GET /api/version`
  - 描述：获取系统版本信息
  - 参数：无
  - 响应：包含版本号、应用名称、运行环境、存储类型、Node.js 版本和运行时间的系统信息
  - 响应示例：
    ```json
    {
      "code": 200,
      "message": "获取版本信息成功",
      "data": {
        "version": "0.8.1",
        "name": "cloudpaste-api",
        "environment": "Docker",
        "storage": "SQLite",
        "nodeVersion": "v18.17.0",
        "uptime": 3600
      },
      "success": true
    }
    ```

#### 系统设置 API

**注意：系统设置 API 已重构为分组管理架构，支持更灵活的设置管理**

- `GET /api/admin/settings`

  - 描述：获取系统设置（支持按分组查询或获取所有分组）
  - 授权：无需授权（公开访问）
  - 查询参数：
    - `group` - 分组 ID（可选）：1=全局设置，3=WebDAV 设置
    - `metadata` - 是否包含元数据（可选，默认 true）
    - `includeSystem` - 是否包含系统内部分组（可选，默认 false，仅在不指定 group 时有效）
  - 响应：
    - **按分组查询时**：
      ```json
      {
        "code": 200,
        "message": "获取分组设置成功",
        "data": [
          {
            "key": "max_upload_size",
            "value": "100",
            "description": "最大上传文件大小限制（MB）",
            "type": "number",
            "group_id": 1,
            "options": null,
            "sort_order": 1
          }
        ],
        "success": true
      }
      ```
    - **获取所有分组时**：
      ```json
      {
        "code": 200,
        "message": "获取所有分组设置成功",
        "data": {
          "1": {
            "groupName": "全局设置",
            "settings": [...]
          },
          "3": {
            "groupName": "WebDAV设置",
            "settings": [...]
          }
        },
        "success": true
      }
      ```

- `GET /api/admin/settings/groups`

  - 描述：获取分组列表和统计信息
  - 授权：需要管理员令牌
  - 响应：分组信息列表
    ```json
    {
      "code": 200,
      "message": "获取分组信息成功",
      "data": {
        "groups": [
          {
            "id": 1,
            "name": "全局设置",
            "description": "系统全局配置项",
            "settingCount": 5
          },
          {
            "id": 3,
            "name": "WebDAV设置",
            "description": "WebDAV协议相关配置",
            "settingCount": 2
          }
        ]
      },
      "success": true
    }
    ```

- `GET /api/admin/settings/metadata`

  - 描述：获取设置项元数据
  - 授权：需要管理员令牌
  - 查询参数：
    - `key` - 设置键名（必填）
  - 响应：设置项的详细元数据
    ```json
    {
      "code": 200,
      "message": "获取设置元数据成功",
      "data": {
        "key": "max_upload_size",
        "description": "最大上传文件大小限制（MB）",
        "type": "number",
        "group_id": 1,
        "options": null,
        "sort_order": 1,
        "flags": 0
      },
      "success": true
    }
    ```

- `PUT /api/admin/settings/group/:groupId`
  - 描述：按分组批量更新设置
  - 授权：需要管理员令牌
  - 参数：groupId - 分组 ID（1=全局设置，3=WebDAV 设置）
  - 查询参数：
    - `validate` - 是否进行类型验证（可选，默认 true）
  - 请求体：设置键值对
    ```json
    {
      "max_upload_size": 200,
      "default_paste_expiry": 14,
      "default_file_expiry": 30
    }
    ```
  - 响应：批量更新结果
    ```json
    {
      "code": 200,
      "message": "批量更新设置成功，共更新3项",
      "data": {
        "success": true,
        "updated": 3,
        "failed": 0,
        "results": [
          {
            "key": "max_upload_size",
            "success": true,
            "oldValue": "100",
            "newValue": "200"
          }
        ]
      },
      "success": true
    }
    ```

### 文本分享 API

#### 创建和访问文本分享

- `POST /api/paste`

  - 描述：创建新的文本分享
  - 授权：需要管理员令牌或有文本权限的 API 密钥
  - 请求体：
    ```json
    {
      "content": "要分享的文本内容", // 必填
      "remark": "备注信息", // 可选
      "expires_at": "2023-12-31T23:59:59Z", // 可选，过期时间
      "max_views": 100, // 可选，最大查看次数
      "password": "访问密码", // 可选
      "slug": "custom-slug" // 可选，自定义短链接
    }
    ```
  - 响应：创建的文本分享信息，包含访问链接
    ```json
    {
      "code": 200,
      "message": "文本分享创建成功",
      "data": {
        "id": "123",
        "slug": "abc123",
        "url": "https://example.com/p/abc123",
        "expires_at": "2023-12-31T23:59:59Z",
        "max_views": 100,
        "views": 0,
        "has_password": true
      },
      "success": true
    }
    ```

- `GET /api/paste/:slug`

  - 描述：获取文本分享内容
  - 参数：slug - 文本短链接
  - 响应：文本分享内容，如果需要密码则返回密码提示

- `POST /api/paste/:slug`

  - 描述：使用密码获取受保护的文本分享
  - 参数：slug - 文本短链接
  - 请求体：
    ```json
    {
      "password": "访问密码" // 必填
    }
    ```
  - 响应：验证成功后返回文本分享内容

- `GET /api/raw/:slug`

  - 描述：获取文本分享的原始内容（纯文本格式）
  - 参数：slug - 文本短链接
  - 查询参数：
    - `password` - 如果文本受密码保护，需提供密码
  - 响应：纯文本格式的内容，Content-Type 为 text/plain

#### 统一文本管理接口

- `GET /api/pastes`

  - 描述：获取文本分享列表（统一接口，支持管理员和 API 密钥用户）
  - 授权：需要管理员令牌或有文本权限的 API 密钥
  - 查询参数：
    - **管理员用户**：
      - `page` - 页码，默认为 1
      - `limit` - 每页数量，默认为 10
      - `created_by` - 可选，按创建者筛选
    - **API 密钥用户**：
      - `limit` - 每页数量，默认为 30
      - `offset` - 偏移量，默认为 0
  - 响应：文本分享列表和分页信息，API 密钥用户只能看到自己创建的文本

- `GET /api/pastes/:id`

  - 描述：获取单个文本详情（统一接口）
  - 授权：需要管理员令牌或有文本权限的 API 密钥
  - 参数：id - 文本 ID
  - 响应：文本分享详细信息，API 密钥用户只能访问自己创建的文本

- `DELETE /api/pastes/batch-delete`

  - 描述：批量删除文本（统一接口）
  - 授权：需要管理员令牌或有文本权限的 API 密钥
  - 请求体：
    ```json
    {
      "ids": ["文本ID1", "文本ID2", "文本ID3"] // 必填，要删除的文本ID数组
    }
    ```
  - 响应：批量删除结果，API 密钥用户只能删除自己创建的文本

- `PUT /api/pastes/:slug`
  - 描述：更新文本信息（统一接口）
  - 授权：需要管理员令牌或有文本权限的 API 密钥
  - 参数：slug - 文本短链接
  - 请求体：可包含 remark, expires_at, max_views, password 等字段
  - 响应：更新后的文本信息，API 密钥用户只能更新自己创建的文本

#### 管理员专用接口

- `POST /api/pastes/clear-expired`

  - 描述：清理过期文本（管理员专用）
  - 授权：需要管理员令牌
  - 响应：清理结果
    ```json
    {
      "code": 200,
      "message": "已清理 5 个过期分享",
      "success": true
    }
    ```

### 文件分享 API

#### 文件上传和下载

- `POST /api/s3/presign`

  - 描述：获取 S3 预签名上传 URL
  - 授权：需要管理员令牌或有文件权限的 API 密钥
  - 请求体：
    ```json
    {
      "s3_config_id": "S3配置ID", // 必填
      "filename": "文件名.jpg", // 必填
      "size": 1024, // 可选，文件大小（字节）
      "mimetype": "image/jpeg", // 可选，MIME类型
      "path": "custom/path/", // 可选，自定义路径
      "slug": "custom-slug", // 可选，自定义短链接
      "override": true, // 可选，是否覆盖已存在文件
      "remark": "文件备注", // 可选，文件备注信息
      "password": "访问密码", // 可选，文件访问密码
      "expires_in": 168, // 可选，过期时间（小时）
      "max_views": 100, // 可选，最大查看次数
      "use_proxy": false // 可选，是否使用代理访问
    }
    ```
  - 响应：包含上传 URL 和文件信息

- `POST /api/s3/commit`

  - 描述：文件上传完成后的提交确认
  - 授权：需要管理员令牌或有文件权限的 API 密钥
  - 请求体：
    ```json
    {
      "file_id": "文件ID", // 必填
      "etag": "文件ETag", // 可选，S3返回的ETag（某些S3兼容服务可能无法提供）
      "size": 1024 // 可选，文件实际大小（字节）
    }
    ```
  - 响应：文件提交结果
  - **重要说明**：业务参数（remark、password、expires_in、max_views 等）应该在 presign 阶段传递，commit 阶段只负责确认上传完成

- `GET /api/file-download/:slug`

  - 描述：直接下载文件（强制下载）
  - 参数：slug - 文件短链接
  - 查询参数：
    - `password` - 如果文件受密码保护，需提供密码
  - 响应：文件内容（下载），包含 Content-Disposition: attachment 头

- `GET /api/file-view/:slug`

  - 描述：预览文件（浏览器内查看）
  - 参数：slug - 文件短链接
  - 查询参数：
    - `password` - 如果文件受密码保护，需提供密码
  - 响应：文件内容（预览），包含 Content-Disposition: inline 头

- `GET /api/office-preview/:slug`
  - 描述：获取 Office 文件的预览 URL
  - 参数：slug - 文件短链接
  - 查询参数：
    - `password` - 如果文件受密码保护，需提供密码
  - 响应：JSON 格式的预签名 URL，用于 Microsoft Office 在线查看服务
  - 注意：此 API 不直接返回文件内容，而是返回用于重定向到 Microsoft Office 在线预览服务的 URL

#### 公共文件查询和验证

- `GET /api/public/files/:slug`

  - 描述：获取文件公开信息
  - 参数：slug - 文件短链接
  - 响应：包含文件基本信息（不含下载链接）

- `POST /api/public/files/:slug/verify`
  - 描述：验证文件访问密码
  - 参数：slug - 文件短链接
  - 请求体：
    ```json
    {
      "password": "文件密码"
    }
    ```
  - 响应：验证成功后返回带下载链接的文件信息

#### 统一文件管理接口

- `GET /api/files`

  - 描述：获取文件列表（统一接口，支持管理员和 API 密钥用户）
  - 授权：需要管理员令牌或有文件权限的 API 密钥
  - 查询参数：
    - **管理员用户**：
      - `limit` - 每页数量，默认为 30
      - `offset` - 偏移量，默认为 0
      - `created_by` - 可选，按创建者筛选
    - **API 密钥用户**：
      - `limit` - 每页数量，默认为 30
      - `offset` - 偏移量，默认为 0
  - 响应：文件列表和分页信息，API 密钥用户只能看到自己上传的文件

- `GET /api/files/:id`

  - 描述：获取单个文件详情（统一接口）
  - 授权：需要管理员令牌或有文件权限的 API 密钥
  - 参数：id - 文件 ID
  - 响应：文件详细信息和下载链接，API 密钥用户只能访问自己上传的文件

- `PUT /api/files/:id`

  - 描述：更新文件信息（统一接口）
  - 授权：需要管理员令牌或有文件权限的 API 密钥
  - 参数：id - 文件 ID
  - 请求体：可包含 remark, slug, expires_at, max_views, password, use_proxy 等字段
  - 响应：更新后的文件信息，API 密钥用户只能更新自己上传的文件

- `DELETE /api/files/batch-delete`

  - 描述：批量删除文件（统一接口）
  - 授权：需要管理员令牌或有文件权限的 API 密钥
  - 请求体：
    ```json
    {
      "ids": ["文件ID1", "文件ID2", "文件ID3"], // 必填，要删除的文件ID数组
      "delete_mode": "both" // 可选，删除模式：record_only（仅删除记录）或 both（删除记录和文件，默认）
    }
    ```
  - 响应：批量删除结果，包含成功和失败的统计信息，API 密钥用户只能删除自己上传的文件

    ```json
    {
      "code": 200,
      "message": "批量删除完成，成功: 2，失败: 1",
      "data": {
        "success": 2,
        "failed": [
          {
            "id": "file-id-3",
            "error": "文件不存在或无权限删除"
          }
        ]
      },
      "success": true
    }
    ```

### S3 存储配置 API

- `GET /api/s3-configs`

  - 描述：获取所有公开的 S3 配置列表
  - 参数：无
  - 响应：S3 配置列表

- `GET /api/s3-configs/:id`

  - 描述：获取单个 S3 配置详情（公开配置）
  - 参数：id - 配置 ID
  - 响应：S3 配置详情

- `POST /api/s3-configs`

  - 描述：创建新的 S3 配置
  - 授权：需要管理员令牌
  - 请求体：
    ```json
    {
      "name": "配置名称", // 必填
      "provider_type": "Cloudflare R2", // 必填，提供商类型
      "endpoint_url": "https://xxxx.r2.cloudflarestorage.com", // 必填，端点URL
      "bucket_name": "my-bucket", // 必填，存储桶名称
      "access_key_id": "ACCESS_KEY", // 必填
      "secret_access_key": "SECRET_KEY", // 必填
      "region": "auto", // 可选，区域
      "path_style": false, // 可选，路径样式寻址，默认false
      "default_folder": "uploads/", // 可选，默认上传文件夹
      "is_public": true, // 可选，是否公开，默认false
      "total_storage_bytes": 10737418240 // 可选，存储容量限制（字节）
    }
    ```
  - 响应：新创建的 S3 配置（不包含敏感字段）

- `PUT /api/s3-configs/:id`

  - 描述：更新 S3 配置
  - 授权：需要管理员令牌
  - 参数：id - 配置 ID
  - 请求体：与 POST 请求类似，所有字段均为可选
  - 响应：更新后的 S3 配置

- `DELETE /api/s3-configs/:id`

  - 描述：删除 S3 配置
  - 授权：需要管理员令牌
  - 参数：id - 配置 ID
  - 响应：删除结果

- `PUT /api/s3-configs/:id/set-default`

  - 描述：设置默认 S3 配置
  - 授权：需要管理员令牌
  - 参数：id - 配置 ID
  - 响应：设置结果

- `POST /api/s3-configs/:id/test`
  - 描述：测试 S3 配置连接有效性
  - 授权：需要管理员令牌
  - 参数：id - 配置 ID
  - 响应：测试结果，包含连接状态和详细信息

### 管理员 API

- `POST /api/admin/login`

  - 描述：管理员登录
  - 请求体：
    ```json
    {
      "username": "管理员用户名",
      "password": "管理员密码"
    }
    ```
  - 响应：登录令牌和管理员信息
    ```json
    {
      "code": 200,
      "message": "登录成功",
      "data": {
        "username": "admin",
        "token": "abc123def456...",
        "expiresAt": "2025-01-28T10:30:45.123Z"
      }
    }
    ```

- `POST /api/admin/logout`

  - 描述：管理员登出
  - 授权：需要管理员令牌
  - 响应：登出结果

- `POST /api/admin/change-password`

  - 描述：管理员修改密码
  - 授权：需要管理员令牌
  - 请求体：
    ```json
    {
      "currentPassword": "当前密码",
      "newPassword": "新密码",
      "newUsername": "新用户名" // 可选
    }
    ```
  - 响应：修改结果

- `GET /api/test/admin-token`

  - 描述：测试管理员令牌有效性
  - 授权：需要管理员令牌
  - 响应：令牌有效状态
    ```json
    {
      "code": 200,
      "message": "令牌有效",
      "success": true
    }
    ```

- `GET /api/test/api-key`

  - 描述：测试 API 密钥有效性
  - 授权：需要有效的 API 密钥
  - 响应：API 密钥验证状态和权限信息（详见下方完整示例）

- `GET /api/admin/dashboard/stats`
  - 描述：获取管理员仪表盘统计数据
  - 授权：需要管理员令牌
  - 响应：系统统计数据，包含文本和文件使用情况、用户活跃度和系统性能指标
  - 响应示例：
    ```json
    {
      "code": 200,
      "message": "获取仪表盘统计数据成功",
      "data": {
        "pastes": {
          "total": 1250,
          "today": 45,
          "thisWeek": 320,
          "thisMonth": 1100
        },
        "files": {
          "total": 850,
          "today": 25,
          "thisWeek": 180,
          "thisMonth": 650,
          "totalSize": "2.5GB"
        },
        "apiKeys": {
          "total": 15,
          "active": 12
        },
        "storage": {
          "configs": 3,
          "mounts": 5
        }
      },
      "success": true
    }
    ```

### API 密钥管理 API

- `GET /api/admin/api-keys`

  - 描述：获取所有 API 密钥列表
  - 授权：需要管理员令牌
  - 响应：API 密钥列表，包含每个密钥的权限和使用情况

- `POST /api/admin/api-keys`

  - 描述：创建新的 API 密钥
  - 授权：需要管理员令牌
  - 请求体：
    ```json
    {
      "name": "密钥名称", // 必填
      "permissions": 7, // 必填，位标志权限值（数字）
      "role": "GENERAL", // 可选，用户角色：GUEST/GENERAL/ADMIN，默认GENERAL
      "basic_path": "/", // 可选，基本路径权限，默认为根路径"/"
      "is_guest": false, // 可选，是否为访客（免密访问），默认false
      "expires_at": "2023-12-31T23:59:59Z", // 可选，过期时间
      "custom_key": "custom-api-key-123" // 可选，自定义密钥值（仅限字母、数字、横杠和下划线）
    }
    ```
  - 权限位标志说明：
    - `1` (TEXT) - 文本分享权限
    - `2` (FILE_SHARE) - 文件分享权限
    - `256` (MOUNT_VIEW) - 挂载页查看权限
    - `512` (MOUNT_UPLOAD) - 上传权限
    - `1024` (MOUNT_COPY) - 复制权限
    - `2048` (MOUNT_RENAME) - 重命名权限
    - `4096` (MOUNT_DELETE) - 删除权限
    - `65536` (WEBDAV_READ) - WebDAV 读取权限
    - `131072` (WEBDAV_MANAGE) - WebDAV 管理权限
  - 示例权限组合：
    - `259` = TEXT + FILE_SHARE + MOUNT_VIEW (1+2+256)
    - `198915` = 所有权限 (1+2+256+512+1024+2048+4096+65536+131072)
  - 响应：新创建的 API 密钥信息，包含完整的密钥值（仅在创建时返回）

- `PUT /api/admin/api-keys/:id`

  - 描述：更新 API 密钥
  - 授权：需要管理员令牌
  - 参数：id - 密钥 ID
  - 请求体：
    ```json
    {
      "name": "新密钥名称", // 可选
      "permissions": 15, // 可选，位标志权限值（数字）
      "role": "GENERAL", // 可选，用户角色：GUEST/GENERAL/ADMIN
      "basic_path": "/restricted/path/", // 可选，基本路径权限
      "is_guest": false, // 可选，是否为访客（免密访问）
      "expires_at": "2023-12-31T23:59:59Z" // 可选，过期时间
    }
    ```
  - 响应：更新后的密钥信息

- `DELETE /api/admin/api-keys/:id`

  - 描述：删除 API 密钥
  - 授权：需要管理员令牌
  - 参数：id - 密钥 ID
  - 响应：删除结果

- `GET /api/test/api-key`
  - 描述：测试 API 密钥有效性
  - 授权：需要有效的 API 密钥
  - 响应：密钥有效状态和权限信息
    ```json
    {
      "code": 200,
      "message": "API密钥验证成功",
      "data": {
        "name": "密钥名称",
        "basic_path": "/",
        "permissions": {
          "text": true,
          "file_share": true,
          "mount_view": true,
          "mount_upload": false,
          "mount_copy": false,
          "mount_rename": false,
          "mount_delete": false,
          "webdav_read": false,
          "webdav_manage": false
        },
        "key_info": {
          "id": "密钥ID",
          "name": "密钥名称",
          "basic_path": "/",
          "permissions": 259,
          "role": "GENERAL",
          "is_guest": false
        }
      },
      "success": true
    }
    ```

### 系统设置 API

- `GET /api/system/max-upload-size`

  - 描述：获取系统允许的最大上传文件大小（公共 API，无需认证）
  - 授权：无需授权
  - 响应：包含最大上传大小的对象
    ```json
    {
      "code": 200,
      "message": "获取最大上传大小成功",
      "data": {
        "max_upload_size": 100
      },
      "success": true
    }
    ```

### 缓存管理 API

#### 管理员缓存管理

- `GET /api/admin/cache/stats`

  - 描述：获取系统监控信息，包括缓存统计和系统内存信息
  - 授权：需要管理员令牌
  - 响应：系统监控信息，包括缓存统计和系统信息
    ```json
    {
      "code": 200,
      "message": "获取系统监控信息成功",
      "data": {
        "cache": {
          "directory": {
            "totalEntries": 150,
            "hitRate": 0.85,
            "missRate": 0.15
          },
          "s3Url": {
            "totalEntries": 50,
            "hitRate": 0.9,
            "missRate": 0.1
          },
          "search": {
            "totalEntries": 25,
            "hitRate": 0.75,
            "missRate": 0.25,
            "cacheSize": "2.5MB",
            "maxAge": 300
          }
        },
        "system": {
          "memory": {
            "used": 128,
            "free": 512,
            "total": 640
          },
          "uptime": 3600
        },
        "timestamp": "2023-05-01T12:00:00Z"
      },
      "success": true
    }
    ```

- `POST /api/admin/cache/clear`

  - 描述：清理系统缓存（支持目录缓存、S3URL 缓存和搜索缓存）
  - 授权：需要管理员令牌
  - 请求体：
    ```json
    {
      "mountId": "挂载点ID", // 可选，清理特定挂载点的缓存
      "s3ConfigId": "S3配置ID" // 可选，清理特定S3配置相关的缓存
      // 注意：如果两个参数都不提供，将清理所有缓存（目录、S3URL、搜索）
    }
    ```
  - 响应：清理结果
    ```json
    {
      "code": 200,
      "message": "缓存清理成功，共清理 75 项",
      "data": {
        "clearedCount": 75,
        "timestamp": "2023-05-01T12:00:00Z"
      },
      "success": true
    }
    ```

#### API 密钥用户缓存管理

- `POST /api/user/cache/clear`
  - 描述：API 密钥用户清理缓存（仅限其权限范围内的缓存）
  - 授权：需要有挂载权限的 API 密钥
  - 请求体：格式同管理员版本，但会自动限制在用户的 basic_path 权限范围内
    ```json
    {
      "mountId": "挂载点ID", // 可选，仅能清理用户有权限访问的挂载点
      "cacheType": "all" // 可选，缓存类型：all（默认）、directory、search
    }
    ```
  - 响应：清理结果（仅包含用户权限范围内的缓存清理统计）
  - 注意：API 密钥用户只能清理其 basic_path 权限范围内的缓存

### 挂载管理 API

#### 统一挂载点管理接口

- `GET /api/mount/list`

  - 描述：获取挂载点列表（统一接口，支持管理员和 API 密钥用户）
  - 授权：需要管理员令牌或有挂载权限的 API 密钥
  - 参数：无
  - 响应：挂载点列表和详细信息
  - 权限说明：
    - **管理员用户**：返回所有挂载点（包括禁用的），用于管理界面
    - **API 密钥用户**：只返回 basic_path 权限范围内的活跃挂载点

- `POST /api/mount/create`

  - 描述：创建新的挂载点（仅管理员）
  - 授权：需要管理员令牌
  - 请求体：
    ```json
    {
      "name": "挂载点名称", // 必填
      "storage_type": "S3", // 必填，存储类型：S3、WebDAV等
      "storage_config_id": "S3配置ID", // 当storage_type=S3时必填
      "mount_path": "/mount-path", // 必填，挂载路径
      "remark": "挂载点备注", // 可选
      "is_active": true, // 可选，是否启用，默认true
      "sort_order": 0, // 可选，排序顺序，默认0
      "cache_ttl": 300, // 可选，缓存TTL（秒），默认300
      "web_proxy": false, // 可选，是否启用Web代理，默认false
      "webdav_policy": "302_redirect", // 可选，WebDAV策略，默认302_redirect
      "enable_sign": false, // 可选，是否启用签名，默认false
      "sign_expires": null // 可选，签名过期时间
    }
    ```
  - 响应：新创建的挂载点信息

- `PUT /api/mount/:id`

  - 描述：更新挂载点信息（仅管理员）
  - 授权：需要管理员令牌
  - 参数：id - 挂载点 ID
  - 请求体：包含需要更新的字段，格式同创建
  - 响应：更新结果

- `DELETE /api/mount/:id`
  - 描述：删除挂载点（仅管理员）
  - 授权：需要管理员令牌
  - 参数：id - 挂载点 ID
  - 响应：删除结果

**注意**：

- 挂载点的创建、更新和删除操作仅限管理员执行
- API 密钥用户只能通过 `/api/mount/list` 查看其 basic_path 权限范围内的挂载点
- 统一接口根据用户权限自动返回相应的数据范围，无需区分不同的 API 端点

### 文件系统 API

文件系统 API 统一为 `/api/fs/*` 路径，支持管理员和 API 密钥用户认证。系统会根据认证信息自动处理权限和访问范围。

#### 统一文件系统操作

- `GET /api/fs/list`

  - 描述：列出目录内容
  - 授权：需要管理员令牌或有文件权限的 API 密钥
  - 查询参数：
    - `path` - 要列出内容的目录路径，默认为根目录("/")
  - 响应：目录内容列表，包含文件和子目录信息
  - 权限：API 密钥用户只能访问其 basic_path 权限范围内的目录

- `GET /api/fs/get`

  - 描述：获取文件信息
  - 授权：需要管理员令牌或有文件权限的 API 密钥
  - 查询参数：
    - `path` - 文件路径
  - 响应：文件详细信息
  - 权限：API 密钥用户只能访问其 basic_path 权限范围内的文件

- `GET /api/fs/download`

  - 描述：下载文件（强制下载）
  - 授权：需要管理员令牌或有文件权限的 API 密钥
  - 查询参数：
    - `path` - 文件路径
  - 响应：文件内容（下载），包含 Content-Disposition: attachment 头
  - 权限：API 密钥用户只能下载其 basic_path 权限范围内的文件

- `POST /api/fs/mkdir`

  - 描述：创建目录
  - 授权：需要管理员令牌或有文件权限的 API 密钥
  - 请求体：
    ```json
    {
      "path": "要创建的目录路径" // 必填
    }
    ```
  - 响应：创建结果
  - 权限：API 密钥用户只能在其 basic_path 权限范围内创建目录

- `POST /api/fs/upload`

  - 描述：上传文件
  - 授权：需要管理员令牌或有文件权限的 API 密钥
  - 请求体：FormData 格式
    - `file` - 文件内容（必填）
    - `path` - 上传目标路径，包含文件名（必填）
    - `use_multipart` - 是否使用分片上传，true/false（可选）
  - 响应：上传结果
    ```json
    {
      "code": 200,
      "message": "文件上传成功",
      "data": {
        "path": "/uploads/example.jpg",
        "size": 1024000,
        "etag": "abc123def456",
        "contentType": "image/jpeg"
      },
      "success": true
    }
    ```
  - 权限：API 密钥用户只能在其 basic_path 权限范围内上传文件

- `POST /api/fs/rename`

  - 描述：重命名文件或目录
  - 授权：需要管理员令牌或有文件权限的 API 密钥
  - 请求体：
    ```json
    {
      "oldPath": "原路径", // 必填
      "newPath": "新路径" // 必填
    }
    ```
  - 响应：重命名结果
  - 权限：API 密钥用户只能重命名其 basic_path 权限范围内的文件或目录

- `DELETE /api/fs/batch-remove`

  - 描述：批量删除文件或目录
  - 授权：需要管理员令牌或有文件权限的 API 密钥
  - 请求体：
    ```json
    {
      "paths": ["路径1", "路径2", "..."] // 必填，要删除项目的路径数组
    }
    ```
  - 响应：批量删除结果
  - 权限：API 密钥用户只能删除其 basic_path 权限范围内的文件或目录

- `GET /api/fs/file-link`

  - 描述：获取文件直链(预签名 URL)，可用于直接访问文件，无需再次身份验证
  - 授权：需要管理员令牌或有文件权限的 API 密钥
  - 查询参数：
    - `path` - 文件路径（必填）
    - `expires_in` - 链接有效期（秒），默认为 604800（7 天）
    - `force_download` - 是否强制下载，true 或 false（默认 false）
  - 响应：包含预签名 URL 的对象，可直接访问或分享
  - 权限：API 密钥用户只能获取其 basic_path 权限范围内文件的直链

- `POST /api/fs/update`

  - 描述：更新文件内容或创建新文件
  - 授权：需要管理员令牌或有文件权限的 API 密钥
  - 请求体：
    ```json
    {
      "path": "文件路径", // 必填，包含文件名
      "content": "文件内容" // 必填，文件的新内容
    }
    ```
  - 响应：更新结果，包含文件路径、ETag、内容类型和是否为新创建的文件
  - 权限：API 密钥用户只能更新其 basic_path 权限范围内的文件

- `POST /api/fs/presign`

  - 描述：获取预签名上传 URL，用于直接上传文件到存储系统
  - 授权：需要管理员令牌或有文件权限的 API 密钥
  - 请求体：
    ```json
    {
      "path": "上传目标路径", // 必填，目标目录路径
      "fileName": "文件名.jpg", // 必填，文件名
      "contentType": "文件MIME类型", // 可选，默认为application/octet-stream
      "fileSize": 1024000 // 可选，文件大小（字节）
    }
    ```
  - 响应：包含预签名 URL 和上传配置的对象
  - 权限：API 密钥用户只能在其 basic_path 权限范围内获取预签名 URL

- `POST /api/fs/presign/commit`

  - 描述：提交预签名上传，确认文件上传完成并更新元数据
  - 授权：需要管理员令牌或有文件权限的 API 密钥
  - 请求体：
    ```json
    {
      "targetPath": "上传目标路径", // 必填，完整的文件路径
      "mountId": "挂载点ID", // 必填，挂载点ID
      "fileSize": 1024000 // 可选，文件大小（字节）
    }
    ```
  - 响应：文件上传完成状态和文件信息
  - 权限：API 密钥用户只能在其 basic_path 权限范围内提交预签名上传

- `POST /api/fs/batch-copy`

  - 描述：批量复制文件或目录，支持自动重命名避免覆盖
  - 授权：需要管理员令牌或有文件权限的 API 密钥
  - 请求体：
    ```json
    {
      "items": [
        // 必填，要复制的项目数组
        {
          "sourcePath": "源路径1", // 必填，源文件或目录路径
          "targetPath": "目标路径1" // 必填，目标文件或目录路径
        },
        {
          "sourcePath": "源路径2",
          "targetPath": "目标路径2"
        }
      ],
      "skipExisting": false // 可选，是否跳过已存在的文件，默认为false（使用自动重命名）
    }
    ```
  - 响应：批量复制结果，包含成功、跳过和失败的项目数量
  - 特殊功能：
    - **自动重命名**：当目标文件/目录已存在时，自动重命名为 `file(1).txt`、`folder(1)/` 等格式
    - **跨存储复制**：支持不同存储类型之间的复制，响应中会包含`requiresClientSideCopy`标志和`crossStorageResults`数组
  - 权限：API 密钥用户只能在其 basic_path 权限范围内进行复制操作

- `POST /api/fs/batch-copy-commit`

  - 描述：提交批量跨存储复制完成信息
  - 授权：需要管理员令牌或有文件权限的 API 密钥
  - 请求体：
    ```json
    {
      "targetMountId": "目标挂载点ID", // 必填
      "files": [
        // 必填，已复制文件列表
        {
          "targetPath": "目标路径1", // 必填
          "s3Path": "S3存储路径1", // 必填
          "contentType": "文件MIME类型", // 可选
          "fileSize": 1024000, // 可选，文件大小（字节）
          "etag": "文件ETag" // 可选
        },
        {
          "targetPath": "目标路径2",
          "s3Path": "S3存储路径2"
        }
      ]
    }
    ```
  - 响应：提交结果，包含成功和失败的文件数量

- `GET /api/fs/search`

  - 描述：搜索文件和目录
  - 授权：需要管理员令牌或有文件权限的 API 密钥
  - 查询参数：
    - `query` - 搜索关键词（必填，至少 2 个字符）
    - `scope` - 搜索范围（可选，默认为"global"）
      - `global` - 全局搜索，搜索所有可访问的挂载点
      - `mount` - 单个挂载点搜索，需要配合 mount_id 参数
      - `directory` - 目录搜索，搜索指定路径及其子目录，需要配合 path 参数
    - `mount_id` - 挂载点 ID（当 scope 为"mount"时必填）
    - `path` - 搜索路径（当 scope 为"directory"时必填）
    - `limit` - 结果数量限制（可选，默认 50，最大 200）
    - `offset` - 结果偏移量（可选，默认 0）
  - 响应：搜索结果
    ```json
    {
      "code": 200,
      "message": "搜索完成",
      "data": {
        "results": [
          {
            "name": "文件名.txt",
            "path": "/path/to/file.txt",
            "size": 1024,
            "lastModified": "2023-01-01T00:00:00.000Z",
            "isDirectory": false,
            "mimeType": "text/plain",
            "mountId": "mount-123",
            "mountName": "我的存储",
            "relativePath": "folder/file.txt"
          }
        ],
        "total": 25,
        "hasMore": false,
        "searchParams": {
          "query": "搜索关键词",
          "scope": "global",
          "limit": 50,
          "offset": 0
        },
        "mountsSearched": 3
      },
      "success": true
    }
    ```
  - 权限：API 密钥用户只能搜索其 basic_path 权限范围内的文件和目录

#### 分片上传 API

**重要说明**：分片上传 API 已统一为 `/api/fs/multipart/*` 路径，支持管理员和 API 密钥用户认证。

- `POST /api/fs/multipart/init`

  - 描述：初始化分片上传
  - 授权：需要管理员令牌或有文件权限的 API 密钥
  - 请求体：
    ```json
    {
      "path": "上传目标路径", // 必填，包含文件名
      "contentType": "文件MIME类型", // 可选，默认为application/octet-stream
      "filename": "文件名.jpg" // 可选，如果path中未包含
    }
    ```
  - 响应：初始化信息，包含 uploadId 和其他元数据
  - 权限：API 密钥用户只能在其 basic_path 权限范围内初始化分片上传

- `POST /api/fs/multipart/part`

  - 描述：上传文件分片
  - 授权：需要管理员令牌或有文件权限的 API 密钥
  - 查询参数：
    - `path` - 上传目标路径（必填）
    - `uploadId` - 分片上传 ID（必填，来自 init 响应）
    - `partNumber` - 分片编号（必填，从 1 开始）
    - `isLastPart` - 是否为最后一个分片（可选，布尔值）
    - `key` - S3 存储键值（可选，来自 init 响应）
  - 请求体：分片内容（二进制）
  - 响应：分片上传结果，包含 ETag 等信息
  - 权限：API 密钥用户只能在其 basic_path 权限范围内上传分片

- `POST /api/fs/multipart/complete`

  - 描述：完成分片上传
  - 授权：需要管理员令牌或有文件权限的 API 密钥
  - 请求体：
    ```json
    {
      "path": "上传目标路径", // 必填
      "uploadId": "分片上传ID", // 必填，来自init响应
      "parts": [
        // 必填，分片信息数组
        {
          "PartNumber": 1,
          "ETag": "分片1的ETag"
        },
        {
          "PartNumber": 2,
          "ETag": "分片2的ETag"
        }
      ],
      "key": "S3存储键值" // 可选，来自init响应
    }
    ```
  - 响应：上传完成结果
  - 权限：API 密钥用户只能在其 basic_path 权限范围内完成分片上传

- `POST /api/fs/multipart/abort`

  - 描述：中止分片上传
  - 授权：需要管理员令牌或有文件权限的 API 密钥
  - 请求体：
    ```json
    {
      "path": "上传目标路径", // 必填
      "uploadId": "分片上传ID", // 必填，来自init响应
      "key": "S3存储键值" // 可选，来自init响应
    }
    ```
  - 响应：中止结果
  - 权限：API 密钥用户只能在其 basic_path 权限范围内中止分片上传

### WebDAV 接口

#### WebDAV 访问

- `WebDAV端点: /dav`

  - 描述：WebDAV 协议接入点，提供标准 WebDAV 协议访问
  - 支持的 WebDAV 方法：
    - `GET` - 获取文件内容
    - `PUT` - 上传文件
    - `DELETE` - 删除文件
    - `PROPFIND` - 获取文件/目录属性
    - `PROPPATCH` - 修改属性
    - `MKCOL` - 创建目录
    - `COPY` - 复制文件/目录
    - `MOVE` - 移动文件/目录
    - `LOCK` - 锁定资源，防止其他客户端修改
    - `UNLOCK` - 解锁之前锁定的资源
  - 授权：基本 HTTP 认证（Basic Auth）或 Bearer 令牌认证
    - Basic Auth: 使用 API 密钥（用户名和密码相同设置为 API 密钥值）或管理员凭据
    - Bearer Auth: 使用 API 密钥值或管理员令牌
  - 权限要求：
    - 管理员账户：自动拥有所有操作权限
    - API 密钥：需要具有挂载权限（mount_permission）

### URL 上传 API

#### URL 验证与元信息

- `POST /api/url/info`

  - 描述：验证 URL 并获取文件元信息
  - 授权：无需授权
  - 请求体：
    ```json
    {
      "url": "https://example.com/image.jpg" // 必填，要验证的URL
    }
    ```
  - 响应：包含 URL 文件的元信息，如文件名、大小、MIME 类型等

- `GET /api/url/proxy`

  - 描述：代理 URL 内容，用于不支持 CORS 的资源
  - 授权：无需授权
  - 查询参数：
    - `url` - 要代理的 URL（必填）
  - 响应：原始 URL 的内容流（适用于前端无法直接访问的资源）

#### URL 上传准备与提交

- `POST /api/url/presign`

  - 描述：为 URL 上传准备预签名 URL 和文件记录
  - 授权：需要管理员令牌或有文件权限的 API 密钥
  - 请求体：
    ```json
    {
      "url": "https://example.com/image.jpg", // 必填，源文件URL
      "s3_config_id": "S3配置ID", // 必填，上传目标S3配置
      "metadata": {
        // 可选，自定义元数据
        "filename": "自定义文件名.jpg",
        "contentType": "image/jpeg"
      },
      "filename": "自定义文件名.jpg", // 可选，覆盖元数据中的文件名
      "slug": "custom-slug", // 可选，自定义短链接
      "remark": "文件备注", // 可选，文件说明
      "path": "custom/path/" // 可选，自定义存储路径
    }
    ```
  - 响应：包含上传信息和预签名 URL 的对象

- `POST /api/url/commit`

  - 描述：URL 上传完成后的提交确认
  - 授权：需要管理员令牌或有文件权限的 API 密钥
  - 请求体：
    ```json
    {
      "file_id": "文件ID", // 必填
      "etag": "文件ETag", // 必填，S3返回的ETag
      "size": 1024000, // 可选，文件大小（字节）
      "remark": "文件备注", // 可选
      "password": "访问密码", // 可选
      "expires_in": 168, // 可选，过期时间（小时）
      "max_views": 10, // 可选，最大查看次数
      "slug": "custom-slug" // 可选，自定义短链接
    }
    ```
  - 响应：文件提交结果和访问信息

#### URL 分片上传

- `POST /api/url/multipart/init`

  - 描述：初始化 URL 分片上传流程
  - 授权：需要管理员令牌或有文件权限的 API 密钥
  - 请求体：
    ```json
    {
      "url": "https://example.com/largefile.zip", // 必填，源文件URL
      "s3_config_id": "S3配置ID", // 必填，上传目标S3配置
      "metadata": {
        // 可选，自定义元数据
        "filename": "自定义文件名.zip",
        "contentType": "application/zip"
      },
      "filename": "自定义文件名.zip", // 可选，覆盖元数据中的文件名
      "slug": "custom-slug", // 可选，自定义短链接
      "remark": "文件备注", // 可选
      "password": "访问密码", // 可选
      "expires_in": 168, // 可选，过期时间（小时）
      "max_views": 10, // 可选，最大查看次数
      "part_size": 5242880, // 可选，分片大小（字节）
      "total_size": 104857600, // 可选，总文件大小（字节）
      "part_count": 20, // 可选，分片数量
      "path": "custom/path/" // 可选，自定义存储路径
    }
    ```
  - 响应：初始化结果，包含 uploadId 和分片上传配置

- `POST /api/url/multipart/complete`

  - 描述：完成 URL 分片上传流程
  - 授权：需要管理员令牌或有文件权限的 API 密钥
  - 请求体：
    ```json
    {
      "file_id": "文件ID", // 必填
      "upload_id": "上传ID", // 必填，来自init响应
      "parts": [
        // 必填，分片信息数组
        {
          "PartNumber": 1,
          "ETag": "分片1的ETag"
        },
        {
          "PartNumber": 2,
          "ETag": "分片2的ETag"
        }
      ]
    }
    ```
  - 响应：完成结果和文件访问信息

- `POST /api/url/multipart/abort`

  - 描述：终止 URL 分片上传流程
  - 授权：需要管理员令牌或有文件权限的 API 密钥
  - 请求体：
    ```json
    {
      "file_id": "文件ID", // 必填
      "upload_id": "上传ID" // 必填，来自init响应
    }
    ```
  - 响应：终止结果

- `POST /api/url/cancel`

  - 描述：取消 URL 上传并删除文件记录
  - 授权：需要管理员令牌或有文件权限的 API 密钥
  - 请求体：
    ```json
    {
      "file_id": "文件ID" // 必填，要取消上传的文件ID
    }
    ```
  - 响应：取消结果和清理状态

## API 使用说明

### 错误处理

所有 API 在出错时返回统一的错误格式：

```json
{
  "code": 400,
  "message": "错误描述",
  "success": false
}
```

常见 HTTP 状态码：

- `200` - 成功
- `201` - 创建成功
- `400` - 请求参数错误
- `401` - 未授权
- `403` - 权限不足
- `404` - 资源不存在
- `409` - 资源冲突
- `410` - 资源已过期
- `500` - 服务器内部错误

### 分页参数

支持分页的 API 通常使用以下参数：

- `limit` - 每页数量，默认 30
- `offset` - 偏移量，默认 0
- `page` - 页码（部分 API 使用）

### 文件上传限制

- 最大文件大小由系统设置决定，可通过 `/api/system/max-upload-size`（公共 API）或 `/api/admin/settings?group=1`（管理员 API）查询
- 大文件建议使用分片上传或预签名 URL 上传
- API 密钥用户受 basic_path 路径限制

### 缓存机制

- 系统使用目录缓存提高性能
- 管理员和 API 密钥用户都可以手动清理缓存
- 文件操作会自动清理相关缓存

#### 搜索缓存

- 搜索结果会被缓存 5 分钟，提高重复搜索的响应速度
- 缓存键基于搜索参数、用户类型和用户信息生成
- 文件操作（上传、删除、重命名等）会自动清理相关的搜索缓存
- 搜索缓存支持按挂载点和用户维度进行清理
- 管理员可以通过 `/api/admin/cache/stats` 查看搜索缓存统计信息
