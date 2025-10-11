/**
 * S3分片上传器
 * 专注于分片上传的核心逻辑，基于S3实际状态进行断点续传
 */
export class S3MultipartUploader {
  constructor(options = {}) {
    // 基础配置
    this.maxConcurrentUploads = options.maxConcurrentUploads || 3;
    this.maxRetries = options.maxRetries || 3;
    this.retryDelay = options.retryDelay || 1000; // 重试延迟（毫秒）

    // 回调函数
    this.onProgress = options.onProgress || (() => {});
    this.onError = options.onError || (() => {});

    // 上传状态
    this.state = "idle"; // idle, uploading, completed, failed, aborted
    this.isAborted = false;

    // 分片管理
    this.chunks = []; // 分片信息数组
    this.completedChunks = new Set(); // 已完成的分片编号
    this.activeXhrs = new Map(); // 活动的XHR请求

    // 核心上传信息
    this.file = null;
    this.uploadId = null;
    this.presignedUrls = [];
    this.partSize = 5 * 1024 * 1024; // 5MB

    // 进度跟踪
    this.totalBytes = 0;
    this.uploadedBytes = 0;
    this.startTime = null;
  }

  /**
   * 设置文件内容
   * @param {File|Blob} content - 要上传的文件或Blob
   * @param {number} partSize - 分片大小
   */
  setContent(content, partSize = 5 * 1024 * 1024) {
    this.file = content;
    this.partSize = partSize;
    this.totalBytes = content.size;

    // 创建分片信息
    this._createChunks();
  }

  /**
   * 创建分片信息数组
   * @private
   */
  _createChunks() {
    this.chunks = [];
    const totalChunks = Math.ceil(this.totalBytes / this.partSize);

    for (let i = 0; i < totalChunks; i++) {
      const start = i * this.partSize;
      const end = Math.min(start + this.partSize, this.totalBytes);
      const partNumber = i + 1;

      this.chunks.push({
        partNumber,
        start,
        end,
        size: end - start,
        status: "pending", // pending, uploading, completed, failed
        retryCount: 0,
        etag: null,
        uploadedBytes: 0,
      });
    }
  }

  /**
   * 从S3 ListParts结果恢复分片状态
   * @param {Array} s3Parts - S3 ListParts API返回的分片信息
   * @returns {boolean} 是否成功恢复状态
   */
  restoreFromS3ListParts(s3Parts) {
    if (!Array.isArray(s3Parts) || s3Parts.length === 0) {
      console.log("没有S3分片信息需要恢复");
      return false;
    }

    console.log(`开始从S3恢复${s3Parts.length}个分片的状态`);

    let restoredCount = 0;

    // 遍历S3分片信息，更新对应的chunks状态
    s3Parts.forEach((s3Part) => {
      const partNumber = s3Part.PartNumber;
      const etag = s3Part.ETag;
      const size = s3Part.Size;

      // 查找对应的chunk（partNumber从1开始，数组索引从0开始）
      const chunkIndex = partNumber - 1;

      if (chunkIndex >= 0 && chunkIndex < this.chunks.length) {
        const chunk = this.chunks[chunkIndex];

        // 更新chunk状态
        chunk.status = "completed";
        chunk.etag = etag;
        chunk.uploadedBytes = size;
        chunk.retryCount = 0;

        // 添加到已完成分片集合
        this.completedChunks.add(partNumber);

        console.log(`恢复分片 ${partNumber}: ETag=${etag}, Size=${size}`);
        restoredCount++;
      } else {
        console.warn(`S3分片 ${partNumber} 超出了chunks范围，跳过恢复`);
      }
    });

    // 重新计算上传进度
    this._updateProgress();

    console.log(`S3状态恢复完成: 成功恢复${restoredCount}个分片，总进度${this._calculateProgress().toFixed(1)}%`);

    return restoredCount > 0;
  }

  /**
   * 计算当前进度百分比
   * @returns {number} 进度百分比 (0-100)
   * @private
   */
  _calculateProgress() {
    const completedChunks = this.chunks.filter((chunk) => chunk.status === "completed");
    const totalChunks = this.chunks.length;
    return totalChunks > 0 ? (completedChunks.length / totalChunks) * 100 : 0;
  }

  /**
   * 设置上传信息
   * @param {string} uploadId - 上传ID
   * @param {Array} presignedUrls - 预签名URL列表
   * @param {string} identifier - 业务标识符（可选）
   */
  setUploadInfo(uploadId, presignedUrls, identifier = null) {
    this.uploadId = uploadId;
    this.presignedUrls = presignedUrls;
    this.identifier = identifier;

    // 映射预签名URL到分片
    this._mapPresignedUrls();
  }

  /**
   * 映射预签名URL到分片
   * @private
   */
  _mapPresignedUrls() {
    this.chunks.forEach((chunk) => {
      const urlInfo = this.presignedUrls.find((url) => url.partNumber === chunk.partNumber);
      if (urlInfo) {
        chunk.presignedUrl = urlInfo.url;
      }
    });
  }

  /**
   * 上传所有分片
   * @returns {Promise<Array>} 已完成的分片信息
   */
  async uploadAllParts() {
    if (this.isAborted) {
      throw new Error("上传已中止");
    }

    this._setState("uploading");
    this.startTime = Date.now();

    try {
      // 过滤出需要上传的分片（跳过已完成的）
      const pendingChunks = this.chunks.filter((chunk) => chunk.status !== "completed" && chunk.presignedUrl);

      if (pendingChunks.length === 0) {
        // 所有分片都已完成
        this._setState("completed");
        return this._getCompletedParts();
      }

      // 并发上传分片
      await this._uploadPartsWithConcurrency(pendingChunks);

      // 检查是否所有分片都已完成
      const allCompleted = this.chunks.every((chunk) => chunk.status === "completed");
      if (allCompleted) {
        this._setState("completed");
        return this._getCompletedParts();
      } else {
        throw new Error("部分分片上传失败");
      }
    } catch (error) {
      if (this.isAborted) {
        this._setState("aborted");
      } else {
        this._setState("failed");
      }
      throw error;
    }
  }

  /**
   * 中止上传
   */
  abort() {
    this.isAborted = true;
    this._setState("aborted");

    // 中止所有活动的XHR请求
    for (const [partNumber, xhr] of this.activeXhrs) {
      try {
        xhr.abort();
      } catch (error) {
        console.warn(`中止分片 ${partNumber} 请求失败:`, error);
      }
    }
    this.activeXhrs.clear();
  }

  /**
   * 重置上传器状态
   */
  reset() {
    // 先中止所有活动请求
    this.abort();

    // 重置所有状态
    this.chunks = [];
    this.completedChunks.clear();
    this.uploadedBytes = 0;
    this.startTime = null;
    this.isAborted = false;
    this._setState("idle");
  }

  /**
   * 获取已完成的分片信息
   * @returns {Array} 分片信息数组
   * @private
   */
  _getCompletedParts() {
    return this.chunks
      .filter((chunk) => chunk.status === "completed" && chunk.etag)
      .map((chunk) => ({
        PartNumber: chunk.partNumber, 
        ETag: chunk.etag,
      }))
      .sort((a, b) => a.PartNumber - b.PartNumber);
  }

  /**
   * 更改状态
   * @param {string} newState - 新状态
   * @private
   */
  _setState(newState) {
    this.state = newState;
  }

  /**
   * 更新进度
   * @private
   */
  _updateProgress() {
    this.uploadedBytes = this.chunks.filter((chunk) => chunk.status === "completed").reduce((total, chunk) => total + chunk.uploadedBytes, 0);

    const progress = this.totalBytes > 0 ? (this.uploadedBytes / this.totalBytes) * 100 : 0;
    this.onProgress(progress, this.uploadedBytes, this.totalBytes);
  }

  /**
   * 并发上传分片
   * @param {Array} chunks - 要上传的分片数组
   * @private
   */
  async _uploadPartsWithConcurrency(chunks) {
    const semaphore = new Array(this.maxConcurrentUploads).fill(null);
    let chunkIndex = 0;

    const uploadNext = async () => {
      if (chunkIndex >= chunks.length || this.isAborted) {
        return;
      }

      const chunk = chunks[chunkIndex++];
      try {
        await this._uploadChunkWithRetry(chunk);
      } catch (error) {
        console.error(`分片 ${chunk.partNumber} 上传失败:`, error);
        throw error;
      }

      // 继续上传下一个分片
      await uploadNext();
    };

    // 启动并发上传
    const promises = semaphore.map(() => uploadNext());
    await Promise.all(promises);
  }

  /**
   * 带重试的分片上传
   * @param {Object} chunk - 分片信息
   * @private
   */
  async _uploadChunkWithRetry(chunk) {
    for (let attempt = 1; attempt <= this.maxRetries; attempt++) {
      if (this.isAborted) {
        throw new Error("上传已中止");
      }

      try {
        await this._uploadChunk(chunk);
        return; // 成功上传，退出重试循环
      } catch (error) {
        console.warn(`分片 ${chunk.partNumber} 第 ${attempt} 次上传失败:`, error);

        if (attempt === this.maxRetries) {
          // 最后一次重试失败
          chunk.status = "failed";
          this.onError(error, chunk.partNumber);
          throw error;
        } else {
          // 等待后重试
          await new Promise((resolve) => setTimeout(resolve, this.retryDelay * attempt));
        }
      }
    }
  }

  /**
   * 上传单个分片
   * @param {Object} chunk - 分片信息
   * @private
   */
  async _uploadChunk(chunk) {
    return new Promise((resolve, reject) => {
      const blob = this.file.slice(chunk.start, chunk.end);
      const xhr = new XMLHttpRequest();

      // 保存XHR引用以便取消
      this.activeXhrs.set(chunk.partNumber, xhr);

      xhr.open("PUT", chunk.presignedUrl);
      xhr.setRequestHeader("Content-Type", "application/octet-stream");
      xhr.timeout = 300000; // 5分钟超时

      xhr.onload = () => {
        this.activeXhrs.delete(chunk.partNumber);

        if (xhr.status >= 200 && xhr.status < 300) {
          const etag = xhr.getResponseHeader("ETag");
          if (etag) {
            chunk.status = "completed";
            chunk.etag = etag;
            chunk.uploadedBytes = chunk.size;
            this.completedChunks.add(chunk.partNumber);
            this._updateProgress();
            resolve();
          } else {
            reject(new Error("未获取到ETag"));
          }
        } else {
          reject(new Error(`HTTP ${xhr.status}: ${xhr.statusText}`));
        }
      };

      xhr.onerror = () => {
        this.activeXhrs.delete(chunk.partNumber);
        reject(new Error("网络错误"));
      };

      xhr.ontimeout = () => {
        this.activeXhrs.delete(chunk.partNumber);
        reject(new Error("上传超时"));
      };

      chunk.status = "uploading";
      xhr.send(blob);
    });
  }
}
