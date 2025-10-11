/**
 * 编码检测 Composable
 * 提供编码检测和管理功能
 */

import { ref, computed } from 'vue';
import { 
  detectEncodingFromUrl, 
  smartDetectEncoding, 
  SUPPORTED_ENCODINGS,
  isEncodingSupported,
  getEncodingInfo,
  normalizeEncoding
} from '@/utils/encodingDetector.js';

export function useEncodingDetection() {
  // 状态
  const selectedEncoding = ref('utf-8');
  const detectionResult = ref(null);
  const isDetecting = ref(false);
  const detectionError = ref(null);
  
  // 计算属性
  const hasDetectionResult = computed(() => !!detectionResult.value);
  
  const detectionConfidence = computed(() => {
    return detectionResult.value?.confidence || 0;
  });
  
  const isDetectionReliable = computed(() => {
    return detectionConfidence.value >= 70;
  });
  
  const availableEncodings = computed(() => {
    return SUPPORTED_ENCODINGS;
  });
  
  const selectedEncodingInfo = computed(() => {
    return getEncodingInfo(selectedEncoding.value);
  });
  
  /**
   * 检测文件编码
   * @param {string} url - 文件URL
   * @param {string} filename - 文件名（可选）
   * @param {Object} options - 检测选项
   * @returns {Promise<Object>} 检测结果
   */
  const detectEncoding = async (url, filename = '', options = {}) => {
    if (!url) {
      throw new Error('URL不能为空');
    }
    
    try {
      isDetecting.value = true;
      detectionError.value = null;
      
      // 使用智能检测
      const result = await smartDetectEncoding(url, filename, options);
      
      detectionResult.value = result;
      
      if (result.success) {
        // 自动选择检测到的编码
        selectEncoding(result.encoding);
      } else {
        detectionError.value = result.error;
      }
      
      return result;
      
    } catch (error) {
      console.error('编码检测失败:', error);
      detectionError.value = error.message;
      
      return {
        success: false,
        encoding: 'utf-8',
        confidence: 0,
        allResults: [],
        error: error.message
      };
    } finally {
      isDetecting.value = false;
    }
  };
  
  /**
   * 选择编码
   * @param {string} encoding - 编码名称
   */
  const selectEncoding = (encoding) => {
    if (!encoding) return;
    
    const normalized = normalizeEncoding(encoding);
    
    if (isEncodingSupported(normalized)) {
      selectedEncoding.value = normalized;
    } else {
      console.warn(`不支持的编码: ${encoding}，使用默认编码 utf-8`);
      selectedEncoding.value = 'utf-8';
    }
  };
  
  /**
   * 重置检测状态
   */
  const resetDetection = () => {
    selectedEncoding.value = 'utf-8';
    detectionResult.value = null;
    isDetecting.value = false;
    detectionError.value = null;
  };
  
  /**
   * 获取编码建议
   * 基于检测结果提供编码选择建议
   * @returns {Array} 编码建议列表
   */
  const getEncodingSuggestions = () => {
    if (!detectionResult.value || !detectionResult.value.allResults) {
      return [
        { encoding: 'utf-8', confidence: 100, reason: '默认编码' }
      ];
    }
    
    const suggestions = detectionResult.value.allResults
      .filter(result => result.confidence > 20) // 过滤低置信度结果
      .slice(0, 5) // 最多5个建议
      .map(result => ({
        encoding: result.encoding,
        confidence: result.confidence,
        reason: getConfidenceReason(result.confidence),
        language: result.language
      }));
    
    // 确保UTF-8总是在列表中
    if (!suggestions.find(s => s.encoding === 'utf-8')) {
      suggestions.push({
        encoding: 'utf-8',
        confidence: 50,
        reason: '通用编码',
        language: null
      });
    }
    
    return suggestions.sort((a, b) => b.confidence - a.confidence);
  };
  
  /**
   * 获取置信度描述
   * @param {number} confidence - 置信度
   * @returns {string} 描述
   */
  const getConfidenceReason = (confidence) => {
    if (confidence >= 90) return '非常可能';
    if (confidence >= 70) return '很可能';
    if (confidence >= 50) return '可能';
    if (confidence >= 30) return '不太确定';
    return '不确定';
  };
  
  /**
   * 验证编码选择
   * @param {string} encoding - 编码名称
   * @returns {Object} 验证结果
   */
  const validateEncoding = (encoding) => {
    const normalized = normalizeEncoding(encoding);
    const isSupported = isEncodingSupported(normalized);
    const info = getEncodingInfo(normalized);
    
    return {
      isValid: isSupported,
      normalized,
      info,
      message: isSupported ? '编码有效' : `不支持的编码: ${encoding}`
    };
  };
  
  return {
    // 状态
    selectedEncoding,
    detectionResult,
    isDetecting,
    detectionError,
    
    // 计算属性
    hasDetectionResult,
    detectionConfidence,
    isDetectionReliable,
    availableEncodings,
    selectedEncodingInfo,
    
    // 方法
    detectEncoding,
    selectEncoding,
    resetDetection,
    getEncodingSuggestions,
    validateEncoding
  };
}
