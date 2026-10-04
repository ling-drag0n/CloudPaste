<template>
  <!-- Speed Dial 浮动工具栏 - Glassmorphism 风格 -->
  <Teleport to="body">
    <div
      class="fixed z-50 right-4 bottom-4 group"
      @mouseenter="handleMouseEnter"
      @mouseleave="handleMouseLeave"
    >
      <!-- Speed Dial 菜单项 -->
      <Transition name="speed-dial">
        <div
          v-show="isExpanded"
          class="flex flex-col-reverse items-center mb-2.5 space-y-reverse space-y-1.5"
        >
          <!-- 设置按钮 -->
          <SpeedDialItem
            :delay="0"
            :tooltip="t('mount.toolbar.settings')"
            :dark-mode="darkMode"
            :expanded="isExpanded"
            @click="handleSettings"
          >
            <IconSettings size="sm" class="w-[18px] h-[18px]" />
          </SpeedDialItem>

          <!-- 切换勾选框按钮 -->
          <SpeedDialItem
            :delay="25"
            :tooltip="showCheckboxes ? t('mount.toolbar.hideCheckboxes') : t('mount.toolbar.showCheckboxes')"
            :dark-mode="darkMode"
            :expanded="isExpanded"
            :active="showCheckboxes"
            @click="handleToggleCheckboxes"
          >
            <IconCheckbox size="sm" class="w-[18px] h-[18px]" />
          </SpeedDialItem>

          <!-- 文件篮按钮 - 打开文件篮列表 -->
          <SpeedDialItem
            :delay="50"
            :tooltip="t('fileBasket.title')"
            :dark-mode="darkMode"
            :expanded="isExpanded"
            @click="handleOpenBasket"
          >
            <IconShoppingCart size="sm" class="w-[18px] h-[18px]" />
          </SpeedDialItem>

          <!-- 任务列表按钮 -->
          <SpeedDialItem
            :delay="75"
            :tooltip="t('mount.toolbar.tasks')"
            :dark-mode="darkMode"
            :expanded="isExpanded"
            @click="handleOpenTasks"
          >
            <IconTaskList size="sm" class="w-[18px] h-[18px]" />
          </SpeedDialItem>

          <!-- 上传按钮 -->
          <SpeedDialItem
            v-if="canWrite && canUpload"
            :delay="100"
            :tooltip="t('mount.toolbar.upload')"
            :dark-mode="darkMode"
            :expanded="isExpanded"
            @click="handleUpload"
          >
            <IconUpload size="sm" class="w-[18px] h-[18px]" />
          </SpeedDialItem>

          <!-- 新建文件夹按钮 -->
          <SpeedDialItem
            v-if="canWrite"
            :delay="125"
            :tooltip="t('mount.toolbar.newFolder')"
            :dark-mode="darkMode"
            :expanded="isExpanded"
            @click="handleNewFolder"
          >
            <IconFolderPlus size="sm" class="w-[18px] h-[18px]" />
          </SpeedDialItem>

          <!-- 刷新按钮 -->
          <SpeedDialItem
            :delay="150"
            :tooltip="t('mount.toolbar.refresh')"
            :dark-mode="darkMode"
            :expanded="isExpanded"
            @click="handleRefresh"
          >
            <IconRefresh size="sm" class="w-[18px] h-[18px]" />
          </SpeedDialItem>
        </div>
      </Transition>

      <!-- 主触发按钮 - 简洁优雅设计 -->
      <button
        type="button"
        @click="toggleExpand"
        class="speed-dial-trigger flex items-center justify-center w-10 h-10 rounded-full transition-all duration-200 focus:outline-none"
        :class="triggerButtonClass"
        :aria-expanded="isExpanded"
        :aria-label="t('mount.toolbar.more')"
      >
        <!-- 三点菜单图标 / 关闭图标 -->
        <component 
          :is="isExpanded ? IconClose : IconMenu"
          size="md"
          class="transition-transform duration-200"
          :class="{ 'rotate-180': isExpanded }"
        />
      </button>
    </div>
  </Teleport>
</template>

<script setup>
import { ref, computed, onMounted, onBeforeUnmount, h } from 'vue'
import { useLocalStorage } from '@vueuse/core'
import { useI18n } from 'vue-i18n'
import {
  IconSettings,
  IconCheckbox,
  IconShoppingCart,
  IconTaskList,
  IconUpload,
  IconFolderPlus,
  IconRefresh,
  IconMenu,
  IconClose
} from '@/components/icons'

// Speed Dial 子项组件 - 简洁风格，与主按钮一致大小
const SpeedDialItem = (props, { slots, emit }) => {
  // 外层容器作为 group，让 tooltip 可以响应 hover
  const wrapperClass = 'relative group/item'
  
  // 按钮尺寸：40px (w-10 h-10)，与主按钮一致
  const baseClass = 'speed-dial-item flex items-center justify-center w-10 h-10 rounded-full transition-all cursor-pointer'
  
  // 简洁风格：白色/深色背景 + 细边框
  const colorClass = props.darkMode
    ? props.active
      ? 'bg-primary-600 text-white hover:bg-primary-500'
      : 'bg-gray-700 text-gray-200 hover:bg-gray-600'
    : props.active
      ? 'bg-primary-500 text-white hover:bg-primary-600'
      : 'bg-white text-gray-600 border border-gray-200 hover:bg-gray-50 hover:text-gray-800'
  
  // 阴影
  const shadowClass = 'shadow-lg'
  
  // Stagger 动画延迟
  const style = {
    transitionDelay: props.expanded ? `${props.delay}ms` : '0ms',
    opacity: props.expanded ? 1 : 0,
    transform: props.expanded ? 'scale(1) translateY(0)' : 'scale(0.6) translateY(8px)',
  }

  return h('div', { class: wrapperClass, style }, [
    h('button', {
      type: 'button',
      class: `${baseClass} ${colorClass} ${shadowClass}`,
      onClick: () => emit('click'),
      'aria-label': props.tooltip,
    }, slots.default?.()),
    // Tooltip - 现在是 group/item 的子元素，可以正确响应 hover
    h('div', {
      class: `absolute right-full mr-2.5 top-1/2 -translate-y-1/2 px-2 py-1 text-xs font-medium rounded whitespace-nowrap pointer-events-none transition-all duration-150 ${
        props.darkMode 
          ? 'bg-gray-800 text-gray-100' 
          : 'bg-gray-800 text-white'
      } opacity-0 group-hover/item:opacity-100 shadow-md`,
    }, props.tooltip)
  ])
}
SpeedDialItem.props = ['delay', 'tooltip', 'darkMode', 'expanded', 'active']
SpeedDialItem.emits = ['click']

const props = defineProps({
  darkMode: {
    type: Boolean,
    default: false
  },
  canWrite: {
    type: Boolean,
    default: true
  },
  canUpload: {
    type: Boolean,
    default: true
  },
  showCheckboxes: {
    type: Boolean,
    default: false
  }
})

const emit = defineEmits([
  'refresh',
  'new-folder',
  'upload',
  'toggle-checkboxes',
  'open-basket',
  'open-tasks',
  'settings'
])

const { t } = useI18n()

// 展开/收起状态
const isExpanded = useLocalStorage('floating-toolbar-expanded', false)
let hoverTimeout = null

// 保留生命周期钩子，方便未来扩展
onMounted(() => {})

// 组件卸载时清理 timeout
onBeforeUnmount(() => {
  if (hoverTimeout) {
    clearTimeout(hoverTimeout)
    hoverTimeout = null
  }
})

// 触发按钮样式 - 简洁中性风格，无蓝色
const triggerButtonClass = computed(() => {
  return props.darkMode
    ? 'bg-gray-700 text-gray-200 shadow-lg hover:bg-gray-600'
    : 'bg-white text-gray-600 shadow-lg border border-gray-200 hover:bg-gray-50'
})

// 切换展开/收起
function toggleExpand() {
  isExpanded.value = !isExpanded.value
}

// Hover 触发（可选）
function handleMouseEnter() {
  if (hoverTimeout) clearTimeout(hoverTimeout)
  // 可以启用 hover 展开，取消下面注释
  // hoverTimeout = setTimeout(() => { isExpanded.value = true }, 100)
}

function handleMouseLeave() {
  if (hoverTimeout) clearTimeout(hoverTimeout)
  // 可以启用 hover 收起，取消下面注释
  // hoverTimeout = setTimeout(() => { isExpanded.value = false }, 300)
}

// 事件处理
function handleRefresh() {
  emit('refresh')
}

function handleNewFolder() {
  emit('new-folder')
}

function handleUpload() {
  emit('upload')
}

function handleToggleCheckboxes() {
  emit('toggle-checkboxes')
}

function handleOpenBasket() {
  emit('open-basket')
}

function handleOpenTasks() {
  emit('open-tasks')
}

function handleSettings() {
  emit('settings')
}
</script>

<style scoped>
/* 主触发按钮动画 - 更细腻的交互 */
.speed-dial-trigger {
  transition: 
    transform 180ms cubic-bezier(0.34, 1.56, 0.64, 1),
    background 180ms ease,
    box-shadow 180ms ease,
    border-color 180ms ease;
}

.speed-dial-trigger:hover {
  transform: scale(1.06);
}

.speed-dial-trigger:active {
  transform: scale(0.94);
}

/* Speed Dial 菜单整体动画 */
.speed-dial-enter-active,
.speed-dial-leave-active {
  transition: opacity 180ms cubic-bezier(0.4, 0, 0.2, 1);
}

.speed-dial-enter-from,
.speed-dial-leave-to {
  opacity: 0;
}

/* 子项按钮动画 - 弹性效果 */
.speed-dial-item {
  transition: 
    opacity 180ms cubic-bezier(0.4, 0, 0.2, 1),
    transform 220ms cubic-bezier(0.34, 1.56, 0.64, 1),
    background-color 150ms ease,
    border-color 150ms ease,
    color 150ms ease,
    box-shadow 150ms ease;
}

.speed-dial-item:hover {
  transform: scale(1.1) !important;
}

.speed-dial-item:active {
  transform: scale(0.92) !important;
}
</style>
