/**
 * 上下文菜单指令
 * 统一处理移动端长按和桌面端右键菜单
 */

/**
 * 检测设备类型（缓存结果避免重复计算）
 * @returns {boolean} 是否为移动设备
 */
const isMobileDevice = (() => {
  let cached = null;
  return () => {
    if (cached === null) {
      cached = /Android|iPhone|iPad|iPod|BlackBerry|IEMobile|Opera Mini/i.test(navigator.userAgent) || "ontouchstart" in window || navigator.maxTouchPoints > 0;
    }
    return cached;
  };
})();

/**
 * 上下文菜单指令实现
 */
export const contextMenuDirective = {
  mounted(el, binding) {
    // 存储定时器和状态
    let longPressTimer = null;
    let isLongPress = false;
    let startX = 0;
    let startY = 0;
    const isMobile = isMobileDevice();

    // 长按阈值配置
    const LONG_PRESS_DURATION = 500; // 500ms触发长按
    const MOVE_THRESHOLD = 10; // 移动超过10px取消长按

    /**
     * 开始长按检测（移动端）
     * @param {TouchEvent} e 触摸事件
     */
    const handleTouchStart = (e) => {
      if (!isMobile) return;

      // 安全检查：确保touches存在且不为空
      if (!e.touches || e.touches.length === 0) {
        console.warn("⚠️ TouchStart事件缺少touches数据");
        return;
      }

      // 记录起始位置
      const touch = e.touches[0];
      startX = touch.clientX;
      startY = touch.clientY;
      isLongPress = false;

      // 清除之前的定时器
      if (longPressTimer) {
        clearTimeout(longPressTimer);
        longPressTimer = null;
      }

      // 设置长按定时器
      longPressTimer = setTimeout(() => {
        // 双重检查：确保定时器没有被清除
        if (longPressTimer) {
          isLongPress = true;
          // 触发上下文菜单
          showContextMenu(startX, startY);
        }
      }, LONG_PRESS_DURATION);
    };

    /**
     * 处理触摸移动（移动端）
     * @param {TouchEvent} e 触摸事件
     */
    const handleTouchMove = (e) => {
      if (!isMobile || !longPressTimer) return;

      // 安全检查：确保touches存在且不为空
      if (!e.touches || e.touches.length === 0) {
        // 如果touches为空，取消长按
        clearTimeout(longPressTimer);
        longPressTimer = null;
        return;
      }

      const touch = e.touches[0];
      const deltaX = Math.abs(touch.clientX - startX);
      const deltaY = Math.abs(touch.clientY - startY);

      // 如果移动距离超过阈值，取消长按
      if (deltaX > MOVE_THRESHOLD || deltaY > MOVE_THRESHOLD) {
        clearTimeout(longPressTimer);
        longPressTimer = null;
      }
    };

    /**
     * 结束触摸（移动端）
     */
    const handleTouchEnd = () => {
      if (!isMobile) return;

      // 清除长按定时器
      if (longPressTimer) {
        clearTimeout(longPressTimer);
        longPressTimer = null;
      }
    };

    /**
     * 处理右键菜单（桌面端）
     * @param {MouseEvent} e 鼠标事件
     */
    const handleContextMenu = (e) => {
      if (isMobile) return;

      // 阻止默认右键菜单
      e.preventDefault();
      e.stopPropagation();

      // 显示自定义上下文菜单
      showContextMenu(e.clientX, e.clientY);
    };

    /**
     * 处理点击事件
     * @param {MouseEvent|TouchEvent} e 事件对象
     */
    const handleClick = (e) => {
      // 如果是长按触发的，阻止点击事件但不阻止冒泡（避免与PhotoSwipe冲突）
      if (isLongPress) {
        e.preventDefault();
        // 注意：不调用stopPropagation()，避免与PhotoSwipe等组件冲突
        isLongPress = false;
        return;
      }

      // 正常点击逻辑由父组件处理
    };

    /**
     * 显示上下文菜单（带异常处理）
     * @param {number} x X坐标
     * @param {number} y Y坐标
     */
    const showContextMenu = (x, y) => {
      try {
        // 调用绑定的处理函数
        if (typeof binding.value === "function") {
          binding.value({ x, y, isMobile });
        } else if (binding.value && typeof binding.value.handler === "function") {
          binding.value.handler({ x, y, isMobile });
        } else {
          console.warn("⚠️ 上下文菜单指令：未找到有效的处理函数");
        }
      } catch (error) {
        console.error("❌ 上下文菜单处理函数执行失败:", error);
      }
    };

    // 绑定事件监听器
    if (isMobile) {
      // 移动端：长按触发
      el.addEventListener("touchstart", handleTouchStart, { passive: false });
      el.addEventListener("touchmove", handleTouchMove, { passive: false });
      el.addEventListener("touchend", handleTouchEnd, { passive: false });
      el.addEventListener("touchcancel", handleTouchEnd, { passive: false });
    } else {
      // 桌面端：右键触发
      el.addEventListener("contextmenu", handleContextMenu);
    }

    // 通用点击事件处理
    el.addEventListener("click", handleClick);

    // 存储清理函数到元素上，供unmounted时使用
    el._contextMenuCleanup = () => {
      if (longPressTimer) {
        clearTimeout(longPressTimer);
      }

      if (isMobile) {
        el.removeEventListener("touchstart", handleTouchStart);
        el.removeEventListener("touchmove", handleTouchMove);
        el.removeEventListener("touchend", handleTouchEnd);
        el.removeEventListener("touchcancel", handleTouchEnd);
      } else {
        el.removeEventListener("contextmenu", handleContextMenu);
      }

      el.removeEventListener("click", handleClick);
    };
  },

  unmounted(el) {
    // 清理事件监听器
    if (el._contextMenuCleanup) {
      el._contextMenuCleanup();
      delete el._contextMenuCleanup;
    }
  },
};

/**
 * 默认导出指令对象
 */
export default contextMenuDirective;
