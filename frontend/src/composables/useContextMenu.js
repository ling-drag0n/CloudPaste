/**
 * 右键菜单 Composable
 * 
 * 封装 @imengyu/vue3-context-menu，提供文件浏览器右键菜单功能
 */
import { ref, h } from 'vue'
import ContextMenu from '@imengyu/vue3-context-menu'
import '@imengyu/vue3-context-menu/lib/vue3-context-menu.css'
import {
  IconDownload,
  IconLink,
  IconCopy,
  IconShoppingCart,
  IconCheckbox,
  IconRename,
  IconDelete,
  IconShare
} from '@/components/icons'

// 图标渲染函数
const icons = {
  download: () => h(IconDownload, { size: 'sm' }),
  link: () => h(IconLink, { size: 'sm' }),
  share: () => h(IconShare, { size: 'sm' }),
  copy: () => h(IconCopy, { size: 'sm' }),
  basket: () => h(IconShoppingCart, { size: 'sm' }),
  checkbox: () => h(IconCheckbox, { size: 'sm' }),
  edit: () => h(IconRename, { size: 'sm' }),
  delete: () => h(IconDelete, { size: 'sm', class: 'text-red-500' }),
}

/**
 * 右键菜单 Composable
 * @param {Object} options - 配置选项
 * @param {Function} options.onDownload - 下载回调
 * @param {Function} options.onGetLink - 获取链接回调
 * @param {Function} options.onShare - 生成分享链接回调（文件与文件夹均支持）
 * @param {Function} options.onRename - 重命名回调
 * @param {Function} options.onDelete - 删除回调
 * @param {Function} options.onCopy - 复制回调
 * @param {Function} options.onAddToBasket - 添加到文件篮回调
 * @param {Function} options.onToggleCheckboxes - 切换勾选框显示回调
 * @param {Function} options.t - i18n 翻译函数
 * @returns {Object} 右键菜单方法
 */
export function useContextMenu(options = {}) {
  const { onDownload, onGetLink, onShare, onRename, onDelete, onCopy, onAddToBasket, onToggleCheckboxes, t } = options
  
  // 当前选中的项目
  const contextItem = ref(null)

  /**
   * 生成单个文件的菜单项
   * @param {Object} item - 文件项
   * @returns {Array} 菜单项数组
   */
  function generateFileMenuItems(item) {
    const items = []
    
    // 下载（仅文件）
    if (!item.isDirectory) {
      items.push({
        label: t?.('mount.fileItem.download') || '下载',
        icon: icons.download,
        onClick: () => onDownload?.(item),
      })
      
      // 获取链接（仅文件）
      items.push({
        label: t?.('mount.fileItem.getLink') || '获取链接',
        icon: icons.link,
        onClick: () => onGetLink?.(item),
        divided: 'down',
      })
    }
    
    // 复制
    items.push({
      label: t?.('mount.fileItem.copy') || '复制',
      icon: icons.copy,
      onClick: () => onCopy?.(item),
    })

    // 生成分享链接（文件与文件夹均支持）
    items.push({
      label: t?.('mount.contextMenu.createShare') || '生成分享链接',
      icon: icons.share,
      onClick: () => onShare?.(item),
    })
    
    // 添加到文件篮
    items.push({
      label: t?.('mount.contextMenu.addToBasket') || '添加到文件篮',
      icon: icons.basket,
      onClick: () => onAddToBasket?.(item),
      divided: 'down',
    })
    
    // 重命名（仅文件）
    if (!item.isDirectory) {
      items.push({
        label: t?.('mount.fileItem.rename') || '重命名',
        icon: icons.edit,
        onClick: () => onRename?.(item),
      })
    }
    
    // 删除
    items.push({
      label: t?.('mount.fileItem.delete') || '删除',
      icon: icons.delete,
      customClass: 'context-menu-danger',
      onClick: () => onDelete?.(item),
    })
    
    return items
  }


  /**
   * 生成多选时的菜单项
   * @param {Array} selectedItems - 选中的项目数组
   * @returns {Array} 菜单项数组
   */
  function generateBatchMenuItems(selectedItems) {
    const hasFiles = selectedItems.some(item => !item.isDirectory)
    const items = []
    
    // 批量下载（仅当有文件时）
    if (hasFiles) {
      items.push({
        label: t?.('mount.contextMenu.batchDownload') || '批量下载',
        icon: icons.download,
        onClick: () => selectedItems.filter(i => !i.isDirectory).forEach(i => onDownload?.(i)),
      })
    }
    
    // 批量复制
    items.push({
      label: t?.('mount.contextMenu.batchCopy') || '批量复制',
      icon: icons.copy,
      onClick: () => onCopy?.(selectedItems),
    })
    
    // 批量添加到文件篮
    items.push({
      label: t?.('mount.contextMenu.batchAddToBasket') || '批量添加到文件篮',
      icon: icons.basket,
      onClick: () => onAddToBasket?.(selectedItems),
      divided: 'down', // 在此项下方添加分隔线
    })
    
    // 批量删除
    items.push({
      label: t?.('mount.contextMenu.batchDelete') || '批量删除',
      icon: icons.delete,
      customClass: 'context-menu-danger',
      onClick: () => onDelete?.(selectedItems),
    })
    
    return items
  }

  /**
   * 显示右键菜单
   * @param {MouseEvent} event - 鼠标事件
   * @param {Object} item - 当前项目
   * @param {Array} selectedItems - 选中的项目数组
   * @param {boolean} darkMode - 暗色模式
   * @param {boolean} showCheckboxes - 当前勾选框显示状态
   */
  function showContextMenu(event, item, selectedItems = [], darkMode = false, showCheckboxes = false) {
    event.preventDefault()
    contextItem.value = item
    
    // 判断是否为多选模式
    const isMultiSelect = selectedItems.length > 1
    
    // 生成菜单项
    const menuItems = isMultiSelect 
      ? generateBatchMenuItems(selectedItems)
      : generateFileMenuItems(item)
    
    // 添加勾选框切换选项（在菜单顶部，带分隔线）
    if (onToggleCheckboxes) {
      menuItems.unshift({
        label: showCheckboxes 
          ? (t?.('mount.contextMenu.hideCheckboxes') || '隐藏勾选框')
          : (t?.('mount.contextMenu.showCheckboxes') || '显示勾选框'),
        icon: icons.checkbox,
        onClick: () => onToggleCheckboxes?.(),
        divided: 'down', // 在此项下方添加分隔线
      })
    }
    
    // 显示菜单
    ContextMenu.showContextMenu({
      x: event.clientX,
      y: event.clientY,
      items: menuItems,
      theme: darkMode ? 'mac dark' : 'mac',
      zIndex: 1000,
    })
  }

  /**
   * 隐藏右键菜单
   */
  function hideContextMenu() {
    ContextMenu.closeContextMenu()
    contextItem.value = null
  }

  return {
    contextItem,
    showContextMenu,
    hideContextMenu,
    generateFileMenuItems,
    generateBatchMenuItems,
  }
}

export default useContextMenu
