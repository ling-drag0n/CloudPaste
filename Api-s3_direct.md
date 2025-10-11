# CloudPaste 文件服务器直传 API 接口文档

## 接口概述

`/api/upload-direct`接口提供了一种简单高效的方式，可以直接上传文件到 S3 兼容的存储服务（如 AWS S3、Backblaze B2、Cloudflare R2 等）。该接口支持各种认证方式，并提供了丰富的参数控制文件存储和访问方式。

## 请求方法

- **HTTP 方法**: `PUT` 

## URL 格式

https://{域名}/api/upload-direct/{filename}

## 路径参数

| 参数名   | 类型   | 必填 | 描述                                                                     |
| -------- | ------ | ---- | ------------------------------------------------------------------------ |
| filename | string | 是   | 要上传的文件名，包括扩展名。此名称用于确定文件类型和生成安全的存储路径。 |

## 查询参数

以下查询参数可以附加到请求 URL 中:

| 参数名            | 类型   | 必填 | 默认值   | 描述                                                                                        |
| ----------------- | ------ | ---- | -------- | ------------------------------------------------------------------------------------------- |
| slug              | string | 否   | 自动生成 | 自定义短链接，用于访问文件。只能包含字母、数字、下划线和横杠。                              |
| path              | string | 否   | 空       | 自定义路径，文件将存储在这个路径下。如果提供，会自动添加斜杠作为目录路径。                  |
| s3_config_id      | string | 否   | 默认配置 | 指定 S3 配置 ID。若不提供，系统将选择默认配置。API 密钥用户只能使用公开配置。               |
| expires_in        | number | 否   | 0        | 文件过期时间（小时）。0 表示永不过期。                                                      |
| max_views         | number | 否   | 0        | 文件最大查看次数。0 表示无限制。                                                            |
| remark            | string | 否   | 空       | 文件备注信息。                                                                              |
| password          | string | 否   | 空       | 访问密码，设置后需要密码才能访问文件。                                                      |
| use_proxy         | string | 否   | "1"      | 是否使用代理访问。"1"表示使用代理，"0"表示直接访问 S3。                                     |
| override          | string | 否   | "false"  | 是否覆盖同名 slug 的已存在文件。"true"表示覆盖，"false"表示不覆盖。只能覆盖自己创建的文件。 |
| original_filename | string | 否   | "false"  | 是否使用原始文件名存储。"true"表示使用原始文件名，"false"表示添加随机前缀。                 |

## 请求头

| 头名称            | 必填 | 描述                                                                 |
| ----------------- | ---- | -------------------------------------------------------------------- |
| Content-Type      | 否   | 文件的 MIME 类型。如果未提供或为通用类型，系统会根据文件扩展名推断。 |
| Authorization     | 否\* | 授权头，支持 Bearer 令牌或 ApiKey 认证。                             |
| X-Custom-Auth-Key | 否\* | 自定义授权密钥，可用于 API 密钥认证。                                |

\* 必须提供至少一种授权方式。

## 请求体

请求体应包含原始文件内容。

## 返回数据

上传成功后，服务器将返回 JSON 格式的响应:

```json
{
  "code": 200,
  "message": "文件上传成功",
  "data": {
    "id": "file_xxxxx", // 文件ID
    "slug": "example", // 文件短链接
    "filename": "example.jpg", // 原始文件名
    "mimetype": "image/jpeg", // MIME类型
    "size": 12345, // 文件大小(字节)
    "remark": "示例文件", // 备注信息
    "created_at": "2023-01-01T12:00:00.000Z", // 创建时间(ISO格式)
    "requires_password": false, // 是否需要密码
    "views": 0, // 当前查看次数
    "max_views": null, // 最大查看次数限制
    "expires_at": null, // 过期时间

    // 主要访问URL（根据use_proxy参数决定使用代理还是直接URL）
    "previewUrl": "https://...", // 预览URL
    "downloadUrl": "https://...", // 下载URL

    // 直接S3访问URL (预签名，不包含密码参数)
    "s3_direct_preview_url": "https://...", // S3直接预览URL
    "s3_direct_download_url": "https://...", // S3直接下载URL

    // 代理访问URL (通过服务器，包含密码参数)
    "proxy_preview_url": "https://...", // 代理预览URL
    "proxy_download_url": "https://...", // 代理下载URL

    // 其他信息
    "use_proxy": 1, // 是否使用代理 (1=代理, 0=直接)
    "created_by": "admin:1", // 创建者信息 (admin:ID 或 apikey:ID)
    "used_original_filename": true // 是否使用了原始文件名
  },
  "success": true
}
```

> **注意**: 当上传时设置了`password`参数，返回的**代理访问 URL**（proxy_preview_url 和 proxy_download_url）将自动包含密码参数，使用户能够直接通过这些链接访问受密码保护的文件，而无需再次输入密码。S3 直接访问 URL 不会包含密码参数，因为它们是预签名 URL，添加额外参数会使其失效。

## 授权方式

API 支持以下几种授权方式:

### 1. Bearer 令牌授权 (管理员)

```
Authorization: Bearer {admin_token}
```

管理员令牌可以通过登录管理后台获取。

### 2. API 密钥授权

```
Authorization: ApiKey {api_key}
```

API 密钥可以在管理后台创建。

### 3. 自定义授权头

```
X-Custom-Auth-Key: {api_key}
```

与 API 密钥授权相同，但使用自定义头部。

## 错误处理

以下是可能的错误响应示例:

- **400 Bad Request**: 请求参数错误
- **401 Unauthorized**: 未授权
- **403 Forbidden**: 没有权限
- **409 Conflict**: 资源冲突，例如 slug 已存在
- **413 Payload Too Large**: 文件过大
- **500 Internal Server Error**: 服务器内部错误

错误响应格式:

```json
{
  "code": 400,
  "message": "错误信息",
  "success": false
}
```

## 调用示例

### 基本上传示例 (使用 curl)

```bash
curl -X PUT "https://your-domain.com/api/upload-direct/example.txt" \
  -H "Authorization: Bearer YOUR_ADMIN_TOKEN" \
  -H "Content-Type: text/plain" \
  --data-binary "这是一个测试文件内容"
```

### 带自定义 slug 和密码保护的上传

```bash
curl -X PUT "https://your-domain.com/api/upload-direct/confidential.pdf?slug=secret-doc&password=your-password&expires_in=24" \
  -H "Authorization: ApiKey YOUR_API_KEY" \
  -H "Content-Type: application/pdf" \
  --data-binary @/path/to/local/file.pdf
```

### 使用 API 密钥上传图片并设置查看次数限制

```bash
curl -X PUT "https://your-domain.com/api/upload-direct/image.jpg?max_views=5&remark=一次性查看图片" \
  -H "X-Custom-Auth-Key: YOUR_API_KEY" \
  -H "Content-Type: image/jpeg" \
  --data-binary @/path/to/image.jpg
```

### 使用原始文件名上传文件

```bash
curl -X PUT "https://your-domain.com/api/upload-direct/important-document.docx?original_filename=true" \
  -H "Authorization: ApiKey YOUR_API_KEY" \
  -H "Content-Type: application/vnd.openxmlformats-officedocument.wordprocessingml.document" \
  --data-binary @/path/to/important-document.docx
```

## 注意事项

1. **预签名 URL**: 返回的直接访问 URL 是预签名的，有效期根据 S3 配置的`signature_expires_in`设置，默认为 1 小时。
2. **MIME 类型**: 如果未提供 Content-Type 或提供了通用类型(application/octet-stream)，系统会根据文件扩展名推断 MIME 类型。
3. **S3 服务商兼容性**: 系统针对不同 S3 服务商(如 AWS S3、Backblaze B2、Cloudflare R2)进行了特殊处理，确保上传过程的兼容性。
4. **文件覆盖**: 使用`override=true`参数可以覆盖已存在的同名 slug 文件。只能覆盖自己创建的文件，覆盖过程会删除旧文件并上传新文件，保持相同的访问链接。
5. **原始文件名**: 使用`original_filename=true`参数时，系统将使用原始文件名存储文件，不添加随机前缀。
6. **存储空间限制**: 系统会检查 S3 配置的存储空间限制，如果上传后会超出限制则拒绝上传。
7. **权限控制**: API 密钥用户只能使用公开的 S3 配置，管理员可以使用自己创建的所有配置。
8. **路径处理**: 如果提供了自定义路径，系统会自动添加斜杠确保作为目录路径处理。
