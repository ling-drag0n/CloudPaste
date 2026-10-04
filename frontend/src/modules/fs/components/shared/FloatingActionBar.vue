<template>
  <!-- 悬浮操作栏 - 选中文件时在底部显示 -->
  <Teleport to="body">
    <Transition name="slide-up">
      <div
        v-if="selectedCount > 0"
        class="fixed bottom-6 left-1/2 -translate-x-1/2 z-50 flex items-center gap-1 px-3 py-2 rounded-token-xl shadow-token-3"
        :class="darkMode 
          ? 'bg-gray-800/95 backdrop-blur-md border border-gray-700' 
          : 'bg-white/95 backdrop-blur-md border border-gray-200'"
      >
        <!-- 选中数量徽章 -->
        <span 
          class="flex items-center justify-center min-w-[24px] h-6 px-2 rounded-token-full text-xs font-medium"
          :class="darkMode ? 'bg-primary-500/20 text-primary-400' : 'bg-primary-100 text-primary-600'"
        >
          {{ selectedCount }}
        </span>

        <div class="w-px h-5 mx-1" :class="darkMode ? 'bg-gray-600' : 'bg-gray-300'" />

        <!-- 下载按钮 -->
        <ActionButton
          :title="t('mount.fileItem.download')"
          :dark-mode="darkMode"
          @click="handleDownload"
        >
          <IconDownload size="sm" />
        </ActionButton>

        <!-- 复制链接按钮 -->
        <ActionButton
          :title="t('mount.fileItem.getLink')"
          :dark-mode="darkMode"
          :disabled="selectedCount > 1"
          @click="handleCopyLink"
        >
          <IconLink size="sm" />
        </ActionButton>

        <!-- 生成分享链接按钮（单选，文件或文件夹均可） -->
        <ActionButton
          :title="t('mount.contextMenu.createShare')"
          :dark-mode="darkMode"
          :disabled="selectedCount > 1"
          @click="handleShare"
        >
          <IconShare size="sm" />
        </ActionButton>

        <!-- 复制按钮 -->
        <ActionButton
          :title="t('mount.fileItem.copy')"
          :dark-mode="darkMode"
          @click="handleCopy"
        >
          <IconCopy size="sm" />
        </ActionButton>

        <!-- 添加到文件篮按钮 - 购物车图标 -->
        <ActionButton
          :title="t('fileBasket.actions.addToBasket')"
          :dark-mode="darkMode"
          @click="handleAddToBasket"
        >
          <IconShoppingCart size="sm" />
        </ActionButton>

        <!-- 重命名按钮（仅单选时可用） -->
        <ActionButton
          :title="t('mount.fileItem.rename')"
          :dark-mode="darkMode"
          :disabled="selectedCount > 1"
          @click="handleRename"
        >
          <IconRename size="sm" />
        </ActionButton>

        <div class="w-px h-5 mx-1" :class="darkMode ? 'bg-gray-600' : 'bg-gray-300'" />

        <!-- 删除按钮 -->
        <ActionButton
          :title="t('mount.fileItem.delete')"
          :dark-mode="darkMode"
          danger
          @click="handleDelete"
        >
          <IconDelete size="sm" />
        </ActionButton>

        <div class="w-px h-5 mx-1" :class="darkMode ? 'bg-gray-600' : 'bg-gray-300'" />

        <!-- 清除选择按钮 -->
        <ActionButton
          :title="t('breadcrumb.exitSelection')"
          :dark-mode="darkMode"
          @click="handleClearSelection"
        >
          <IconClose size="sm" />
        </ActionButton>
      </div>
    </Transition>
  </Teleport>
</template>

<script setup>
import { h } from 'vue'
import { useI18n } from 'vue-i18n'
import {
  IconDownload,
  IconLink,
  IconCopy,
  IconShoppingCart,
  IconRename,
  IconDelete,
  IconClose,
  IconShare
} from '@/components/icons'

// 操作按钮子组件
const ActionButton = (props, { slots, emit }) => {
  const baseClass = 'p-2 rounded-token-md transition-all duration-token-fast'
  const enabledClass = props.darkMode
    ? 'hover:bg-gray-700 text-gray-300 hover:text-gray-100'
    : 'hover:bg-gray-100 text-gray-600 hover:text-gray-900'
  const disabledClass = 'opacity-40 cursor-not-allowed'
  const dangerClass = props.darkMode
    ? 'hover:bg-red-500/20 text-red-400 hover:text-red-300'
    : 'hover:bg-red-50 text-red-500 hover:text-red-600'

  return h('button', {
    type: 'button',
    title: props.title,
    disabled: props.disabled,
    class: `${baseClass} ${props.disabled ? disabledClass : props.danger ? dangerClass : enabledClass}`,
    onClick: () => !props.disabled && emit('click')
  }, slots.default?.())
}
ActionButton.props = ['title', 'darkMode', 'disabled', 'danger']
ActionButton.emits = ['click']

const props = defineProps({
  selectedCount: {
    type: Number,
    default: 0
  },
  darkMode: {
    type: Boolean,
    default: false
  }
})

const emit = defineEmits([
  'download',
  'copy-link',
  'share',
  'copy',
  'add-to-basket',
  'rename',
  'delete',
  'clear-selection'
])

const { t } = useI18n()

// 事件处理
function handleDownload() {
  emit('download')
}

function handleCopyLink() {
  if (props.selectedCount === 1) {
    emit('copy-link')
  }
}

function handleShare() {
  if (props.selectedCount === 1) {
    emit('share')
  }
}

function handleCopy() {
  emit('copy')
}

function handleAddToBasket() {
  emit('add-to-basket')
}

function handleRename() {
  if (props.selectedCount === 1) {
    emit('rename')
  }
}

function handleDelete() {
  emit('delete')
}

function handleClearSelection() {
  emit('clear-selection')
}
</script>

<style scoped>
/* 滑入滑出动画 */
.slide-up-enter-active,
.slide-up-leave-active {
  transition: all 250ms cubic-bezier(0.4, 0, 0.2, 1);
}

.slide-up-enter-from,
.slide-up-leave-to {
  opacity: 0;
  transform: translateX(-50%) translateY(100px);
}
</style>
