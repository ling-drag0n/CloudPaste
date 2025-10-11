# 第一阶段：构建sqlite3模块
FROM node:18-alpine AS builder

WORKDIR /app

# 设置Alpine镜像源以加快软件包安装
# 使用多个镜像源以支持全球不同地区（注释掉不需要的）
RUN echo "https://mirrors.aliyun.com/alpine/latest-stable/main/" > /etc/apk/repositories && \
    echo "https://mirrors.aliyun.com/alpine/latest-stable/community/" >> /etc/apk/repositories
# 或使用 USTC 镜像 (中国)
#RUN echo "https://mirrors.ustc.edu.cn/alpine/latest-stable/main/" > /etc/apk/repositories && \
#    echo "https://mirrors.ustc.edu.cn/alpine/latest-stable/community/" >> /etc/apk/repositories
# 或使用官方镜像 (全球)
#RUN echo "https://dl-cdn.alpinelinux.org/alpine/latest-stable/main/" > /etc/apk/repositories && \
#    echo "https://dl-cdn.alpinelinux.org/alpine/latest-stable/community/" >> /etc/apk/repositories

# 加速npm安装
RUN npm config set registry https://registry.npmmirror.com/

# 复制package.json和package-lock.json
COPY backend/package*.json ./

# 预先安装sqlite3相关依赖(最常变化的模块)，利用Docker缓存层
RUN npm install sqlite3 --verbose

# 安装编译依赖，优化缓存，使用--no-cache-dir减少npm缓存
RUN apk add --no-cache --virtual .build-deps python3 make g++ sqlite-dev && \
    npm install --verbose --include=optional --no-cache-dir && \
    apk del .build-deps

# 第二阶段：创建最终镜像
FROM node:18-alpine

WORKDIR /app

# 设置Alpine镜像源，与构建阶段保持一致
RUN echo "https://mirrors.aliyun.com/alpine/latest-stable/main/" > /etc/apk/repositories && \
    echo "https://mirrors.aliyun.com/alpine/latest-stable/community/" >> /etc/apk/repositories

# 只安装运行时所需的sqlite库
RUN apk add --no-cache sqlite-libs

# 复制第一阶段构建好的node_modules
COPY --from=builder /app/node_modules ./node_modules

# 复制应用代码
COPY backend/ ./

# 创建数据目录
RUN mkdir -p /data

# 设置环境变量
# ERROR: 0,  WARN: 1,  INFO: 2,  DEBUG: 3,
ENV NODE_ENV=production
ENV LOG_LEVEL=3
ENV RUNTIME_ENV=docker
ENV DATA_DIR=/data
ENV PORT=8787
# 注意：这个密钥应该在运行时通过环境变量传入，这里只是设置默认值
ENV ENCRYPTION_SECRET=PLEASE_CHANGE_THIS_IN_PRODUCTION

# 暴露端口
EXPOSE 8787

# 启动命令
CMD ["node", "--expose-gc", "server.js"]