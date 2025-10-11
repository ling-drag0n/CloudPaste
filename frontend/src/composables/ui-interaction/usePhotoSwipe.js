/**
 * PhotoSwipe图片预览组合式函数
 * 基于PhotoSwipe v5
 */

import { ref, nextTick } from "vue";
import PhotoSwipeLightbox from "photoswipe/lightbox";
import "photoswipe/style.css";
import "@/styles/photoswipe-custom.css";

export function usePhotoSwipe() {
  // PhotoSwipe实例
  const lightbox = ref(null);
  const isInitialized = ref(false);

  /**
   * 初始化PhotoSwipe
   *
   */
  const initPhotoSwipe = () => {
    if (isInitialized.value) return;

    try {
      lightbox.value = new PhotoSwipeLightbox({
        // 动态模式不需要gallery和children选择器
        // 我们使用loadAndOpen方法直接传递数据

        // 动态导入PhotoSwipe核心模块
        pswpModule: () => import("photoswipe"),

        // 基础配置
        showHideAnimationType: "zoom",

        // 移动端优化
        pinchToClose: true,
        closeOnVerticalDrag: true,

        // 界面配置
        padding: { top: 20, bottom: 40, left: 100, right: 100 },

        // 缩放配置 - 允许任意缩放大小
        initialZoomLevel: (zoomLevelObject) => {
          // 打开时显示适合视口的65%大小，这样用户可以缩小到更小或放大到更大
          return zoomLevelObject.fit * 0.65;
        },
        secondaryZoomLevel: "fit", // 点击缩放按钮时回到适合视口大小
        maxZoomLevel: (zoomLevelObject) => {
          // 最大可以放大到8倍fit大小
          return zoomLevelObject.fit * 8;
        },

        // 键盘导航
        arrowKeys: true,

        // 鼠标滚轮缩放
        wheelToZoom: true,

        // 背景点击关闭
        bgOpacity: 0.8,

        // 动画配置
        showAnimationDuration: 333,
        hideAnimationDuration: 333,
      });

      // 监听PhotoSwipe事件
      setupPhotoSwipeEvents();

      // 初始化
      lightbox.value.init();
      isInitialized.value = true;

      console.log("✅ PhotoSwipe初始化成功");
    } catch (error) {
      console.error("❌ PhotoSwipe初始化失败:", error);
    }
  };

  /**
   * 设置PhotoSwipe事件监听器
   */
  const setupPhotoSwipeEvents = () => {
    if (!lightbox.value) return;

    // 监听打开事件
    lightbox.value.on("beforeOpen", () => {
      console.log("🔍 PhotoSwipe正在打开");
    });

    // 监听关闭事件
    lightbox.value.on("close", () => {
      console.log("🔍 PhotoSwipe已关闭");
    });

    // 监听图片加载错误
    lightbox.value.on("contentLoadError", (e) => {
      console.error("🔍 PhotoSwipe图片加载失败", e);
    });

    // 监听索引变化
    lightbox.value.on("change", () => {
      const pswp = lightbox.value.pswp;
      if (pswp) {
        console.log(`🔍 PhotoSwipe切换到第${pswp.currIndex + 1}张图片`);
      }
    });

    // ✅ 注册自定义UI元素（官方推荐方式）
    lightbox.value.on("uiRegister", () => {
      registerCustomUIElements();
    });
  };

  /**
   * 注册自定义UI元素
   * 使用PhotoSwipe官方API，保持原生风格
   */
  const registerCustomUIElements = () => {
    const pswp = lightbox.value.pswp;
    if (!pswp) {
      console.warn("⚠️ PhotoSwipe实例不可用，无法注册自定义UI元素");
      return;
    }

    try {
      // 注册旋转按钮
      registerRotateButton(pswp);

      // 注册翻转按钮
      registerFlipButton(pswp);

      // 注册图片信息显示
      registerImageInfo(pswp);

      console.log("✅ PhotoSwipe自定义UI元素注册成功");
    } catch (error) {
      console.error("❌ PhotoSwipe自定义UI元素注册失败:", error);
    }
  };

  /**
   * 打开PhotoSwipe预览
   * @param {Array} images - 图片数组
   * @param {number} startIndex - 起始索引
   * @param {Map} imageStates - 图片状态管理Map（可选）
   * @param {Function} loadImageUrl - 图片URL加载函数（可选）
   */
  const openPhotoSwipe = async (images, startIndex = 0, imageStates = null, loadImageUrl = null) => {
    if (!images || images.length === 0) {
      console.warn("⚠️ PhotoSwipe: 没有图片可预览");
      return;
    }

    // 确保PhotoSwipe已初始化
    if (!isInitialized.value) {
      initPhotoSwipe();
      // 等待初始化完成
      await nextTick();
    }

    try {
      // 转换图片数据为PhotoSwipe格式
      const photoSwipeItems = await convertImagesToPhotoSwipeFormat(images, imageStates, loadImageUrl);

      if (photoSwipeItems.length === 0) {
        console.warn("⚠️ PhotoSwipe: 没有有效的图片数据");
        return;
      }

      // 验证起始索引
      const validStartIndex = Math.max(0, Math.min(startIndex, photoSwipeItems.length - 1));

      console.log(`🔍 PhotoSwipe打开预览: ${photoSwipeItems.length}张图片, 起始索引: ${validStartIndex}`);

      // 使用PhotoSwipe的动态模式打开
      lightbox.value.loadAndOpen(validStartIndex, photoSwipeItems);
    } catch (error) {
      console.error("❌ PhotoSwipe打开失败:", error);
    }
  };

  /**
   * 将图片数据转换为PhotoSwipe格式
   * 按照官方文档要求的数据结构
   * @param {Array} images - 原始图片数组
   * @param {Map} imageStates - 图片状态管理Map（可选）
   * @param {Function} loadImageUrl - 图片URL加载函数（可选）
   * @returns {Array} PhotoSwipe格式的图片数组
   */
  const convertImagesToPhotoSwipeFormat = async (images, imageStates = null, loadImageUrl = null) => {
    const photoSwipeItems = [];

    for (const image of images) {
      try {
        // 获取图片URL和尺寸信息
        const imageData = await getImageDataForPhotoSwipe(image, imageStates, loadImageUrl);

        if (imageData) {
          photoSwipeItems.push(imageData);
        }
      } catch (error) {
        console.warn(`⚠️ 跳过无效图片: ${image.name}`, error);
      }
    }

    return photoSwipeItems;
  };

  /**
   * 获取单张图片的PhotoSwipe数据
   * @param {Object} image - 图片对象
   * @param {Map} imageStates - 图片状态管理Map（可选）
   * @param {Function} loadImageUrl - 图片URL加载函数（可选）
   * @returns {Object|null} PhotoSwipe格式的图片数据
   */
  const getImageDataForPhotoSwipe = async (image, imageStates = null, loadImageUrl = null) => {
    try {
      // 获取图片URL（使用现有的状态管理）
      let imageUrl = getImageUrl(image, imageStates);

      // 如果没有URL且提供了加载函数，尝试加载
      if (!imageUrl && loadImageUrl && imageStates) {
        console.log(`🔄 PhotoSwipe: 为图片 ${image.name} 加载URL`);
        await loadImageUrl(image);
        // 重新获取URL
        imageUrl = getImageUrl(image, imageStates);
      }

      if (!imageUrl) {
        console.warn(`⚠️ 图片URL为空: ${image.name}`);
        return null;
      }

      // 获取图片尺寸
      const dimensions = await getImageDimensions(image, imageUrl, imageStates);

      // 构建PhotoSwipe数据格式
      const photoSwipeItem = {
        src: imageUrl,
        width: dimensions.width,
        height: dimensions.height,
        alt: image.name,
        // 可选：添加标题
        title: image.name,
        // 可选：添加原始图片对象引用
        originalImage: image,
      };

      return photoSwipeItem;
    } catch (error) {
      console.error(`❌ 获取图片数据失败: ${image.name}`, error);
      return null;
    }
  };

  /**
   * 获取图片URL
   * 复用现有的图片状态管理逻辑 - 只从状态管理中获取，不使用图片对象中的URL
   * @param {Object} image - 图片对象
   * @param {Map} imageStates - 图片状态管理Map（可选）
   * @returns {string|null} 图片URL
   */
  const getImageUrl = (image, imageStates = null) => {
    // 只从状态管理中获取URL，确保懒加载生效
    if (imageStates) {
      const imageState = imageStates.get(image.path);
      if (imageState?.status === "loaded" && imageState.url) {
        return imageState.url;
      }
    }
    return null;
  };

  /**
   * 获取图片尺寸
   * PhotoSwipe要求预定义图片尺寸
   * @param {Object} image - 图片对象
   * @param {string} imageUrl - 图片URL
   * @param {Map} imageStates - 图片状态管理Map（可选）
   * @returns {Object} 包含width和height的对象
   */
  const getImageDimensions = async (image, imageUrl, imageStates = null) => {
    // 如果有状态管理，优先从状态中获取尺寸
    if (imageStates) {
      const imageState = imageStates.get(image.path);
      if (imageState?.naturalWidth && imageState?.naturalHeight) {
        return {
          width: imageState.naturalWidth,
          height: imageState.naturalHeight,
        };
      }
    }

    // 如果图片对象中已有尺寸信息，直接使用
    if (image.naturalWidth && image.naturalHeight) {
      return {
        width: image.naturalWidth,
        height: image.naturalHeight,
      };
    }

    // 如果图片状态中有尺寸信息，使用它
    if (image.aspectRatio && image.naturalWidth) {
      return {
        width: image.naturalWidth,
        height: image.naturalHeight,
      };
    }

    // 尝试从图片元素获取尺寸
    try {
      const dimensions = await loadImageDimensions(imageUrl);

      // 如果获取到的尺寸有效，保存到状态管理中
      if (imageStates && dimensions.width > 0 && dimensions.height > 0) {
        const currentState = imageStates.get(image.path);
        if (currentState) {
          imageStates.set(image.path, {
            ...currentState,
            naturalWidth: dimensions.width,
            naturalHeight: dimensions.height,
          });
        }
      }

      return dimensions;
    } catch (error) {
      console.warn(`⚠️ 无法获取图片尺寸: ${image.name}, 使用默认尺寸`);
      // 返回默认尺寸
      return {
        width: 1200,
        height: 800,
      };
    }
  };

  /**
   * 异步加载图片并获取尺寸
   * @param {string} imageUrl - 图片URL
   * @returns {Promise<Object>} 包含width和height的Promise
   */
  const loadImageDimensions = (imageUrl) => {
    return new Promise((resolve, reject) => {
      const img = new Image();

      img.onload = () => {
        resolve({
          width: img.naturalWidth,
          height: img.naturalHeight,
        });
      };

      img.onerror = () => {
        reject(new Error("图片加载失败"));
      };

      // 设置超时
      setTimeout(() => {
        reject(new Error("图片加载超时"));
      }, 5000);

      img.src = imageUrl;
    });
  };

  /**
   * 销毁PhotoSwipe实例
   */
  const destroyPhotoSwipe = () => {
    if (lightbox.value) {
      lightbox.value.destroy();
      lightbox.value = null;
      isInitialized.value = false;
      console.log("🔍 PhotoSwipe已销毁");
    }
  };

  /**
   * 注册旋转按钮
   * 使用PhotoSwipe官方API，保持原生风格
   */
  const registerRotateButton = (pswp) => {
    // 存储每张图片的旋转角度
    const imageRotations = new Map();

    pswp.ui.registerElement({
      name: "rotate-button",
      title: "旋转",
      ariaLabel: "旋转图片",
      order: 7, // 在缩放按钮(order: 10)之前
      isButton: true,
      // ✅ 还原到最开始的简单旋转样式
      html: {
        isCustomSVG: true,
        inner:
          '<path d="M12 6v3l4-4-4-4v3c-4.42 0-8 3.58-8 8 0 1.57.46 3.03 1.24 4.26L6.7 14.8c-.45-.83-.7-1.79-.7-2.8 0-3.31 2.69-6 6-6zm6.76 1.74L17.3 9.2c.44.84.7 1.79.7 2.8 0 3.31-2.69 6-6 6v-3l-4 4 4 4v-3c4.42 0 8-3.58 8-8 0-1.57-.46-3.03-1.24-4.26z" id="pswp__icn-rotate"/>',
        outlineID: "pswp__icn-rotate",
      },
      onClick: (_, __, pswp) => {
        try {
          const currentSlide = pswp.currSlide;
          if (!currentSlide || !currentSlide.content) {
            console.warn("⚠️ 旋转按钮: 当前幻灯片不可用");
            return;
          }

          // 获取当前图片的唯一标识
          const imageKey = currentSlide.data.src;
          if (!imageKey) {
            console.warn("⚠️ 旋转按钮: 图片URL不可用");
            return;
          }

          const currentRotation = imageRotations.get(imageKey) || 0;
          const newRotation = (currentRotation + 90) % 360;

          // 更新旋转角度
          imageRotations.set(imageKey, newRotation);

          // 应用CSS变换到图片元素，保持翻转状态
          const imageElement = currentSlide.content.element;
          if (imageElement) {
            // 获取当前的transform值，保持翻转状态
            const currentTransform = imageElement.style.transform || "";
            const scaleMatch = currentTransform.match(/scaleY\([^)]*\)/);
            const scaleTransform = scaleMatch ? scaleMatch[0] : "";

            // 组合变换：翻转 + 旋转
            const rotateTransform = `rotate(${newRotation}deg)`;
            const combinedTransform = [scaleTransform, rotateTransform].filter(Boolean).join(" ");

            imageElement.style.transform = combinedTransform;
            imageElement.style.transition = "transform 0.3s ease";

            console.log(`🔄 图片旋转到 ${newRotation}度`);
          } else {
            console.warn("⚠️ 旋转按钮: 图片元素不可用");
          }
        } catch (error) {
          console.error("❌ 旋转按钮操作失败:", error);
        }
      },
    });
  };

  /**
   * 注册翻转按钮
   * 使用PhotoSwipe官方API，保持原生风格
   */
  const registerFlipButton = (pswp) => {
    // 存储每张图片的翻转状态
    const imageFlips = new Map();

    pswp.ui.registerElement({
      name: "flip-button",
      title: "翻转",
      ariaLabel: "上下翻转图片",
      order: 7.5, // 在旋转按钮之后，下载按钮之前
      isButton: true,
      html: {
        isCustomSVG: true,
        inner: '<path d="M16 4l4 4h-3v8h-2V8h-3l4-4zm0 24l-4-4h3v-8h2v8h3l-4 4zM8 14h2v4H8v-4zm14 0h2v4h-2v-4z" id="pswp__icn-flip"/>',
        outlineID: "pswp__icn-flip",
      },
      onClick: (_, __, pswp) => {
        try {
          const currentSlide = pswp.currSlide;
          if (!currentSlide || !currentSlide.content) {
            console.warn("⚠️ 翻转按钮: 当前幻灯片不可用");
            return;
          }

          // 获取当前图片的唯一标识
          const imageKey = currentSlide.data.src;
          if (!imageKey) {
            console.warn("⚠️ 翻转按钮: 图片URL不可用");
            return;
          }

          const currentFlip = imageFlips.get(imageKey) || false;
          const newFlip = !currentFlip;

          // 更新翻转状态
          imageFlips.set(imageKey, newFlip);

          // 应用CSS变换到图片元素
          const imageElement = currentSlide.content.element;
          if (imageElement) {
            // 获取当前的transform值，保持旋转状态
            const currentTransform = imageElement.style.transform || "";
            const rotateMatch = currentTransform.match(/rotate\([^)]*\)/);
            const rotateTransform = rotateMatch ? rotateMatch[0] : "";

            // 组合变换：翻转 + 旋转
            const flipTransform = newFlip ? "scaleY(-1)" : "";
            const combinedTransform = [flipTransform, rotateTransform].filter(Boolean).join(" ");

            imageElement.style.transform = combinedTransform;
            imageElement.style.transition = "transform 0.3s ease";

            console.log(`🔄 图片${newFlip ? "已翻转" : "取消翻转"}`);
          } else {
            console.warn("⚠️ 翻转按钮: 图片元素不可用");
          }
        } catch (error) {
          console.error("❌ 翻转按钮操作失败:", error);
        }
      },
    });
  };

  /**
   * 注册图片信息显示
   * 使用PhotoSwipe官方API，保持原生风格
   */
  const registerImageInfo = (pswp) => {
    pswp.ui.registerElement({
      name: "image-info",
      className: "pswp__image-info",
      appendTo: "wrapper", // 添加到wrapper而不是toolbar
      onInit: (el, pswp) => {
        try {
          // 创建信息容器
          el.innerHTML = `
            <div class="pswp__image-info-content">
              <div class="pswp__image-name"></div>
              <div class="pswp__image-details"></div>
            </div>
          `;

          const nameEl = el.querySelector(".pswp__image-name");
          const detailsEl = el.querySelector(".pswp__image-details");

          if (!nameEl || !detailsEl) {
            console.error("❌ 图片信息显示: 无法找到信息元素");
            return;
          }

          // 更新图片信息
          const updateImageInfo = () => {
            try {
              const currentSlide = pswp.currSlide;
              if (currentSlide && currentSlide.data) {
                const image = currentSlide.data.originalImage;
                const name = image?.name || "Unknown";
                const width = currentSlide.data.width || "Unknown";
                const height = currentSlide.data.height || "Unknown";
                const size = image?.size ? formatFileSize(image.size) : "";

                nameEl.textContent = name;
                detailsEl.textContent = `${width} × ${height}${size ? ` • ${size}` : ""}`;
              } else {
                nameEl.textContent = "Unknown";
                detailsEl.textContent = "";
              }
            } catch (error) {
              console.error("❌ 更新图片信息失败:", error);
            }
          };

          // 监听图片切换
          pswp.on("change", updateImageInfo);

          // 初始更新
          updateImageInfo();

          console.log("✅ 图片信息显示初始化成功");
        } catch (error) {
          console.error("❌ 图片信息显示初始化失败:", error);
        }
      },
    });
  };

  /**
   * 格式化文件大小
   * 工具函数，用于显示文件大小
   */
  const formatFileSize = (bytes) => {
    if (!bytes) return "";

    const sizes = ["B", "KB", "MB", "GB"];
    const i = Math.floor(Math.log(bytes) / Math.log(1024));
    return Math.round((bytes / Math.pow(1024, i)) * 100) / 100 + " " + sizes[i];
  };

  return {
    // 状态
    isInitialized,

    // 方法
    initPhotoSwipe,
    openPhotoSwipe,
    destroyPhotoSwipe,

    // 工具方法
    convertImagesToPhotoSwipeFormat,
    getImageDataForPhotoSwipe,
  };
}
