/**
 * Uppy 插件管理器
 * 负责管理所有媒体插件的状态、配置和生命周期
 */

import Webcam from "@uppy/webcam";
import ScreenCapture from "@uppy/screen-capture";
import Audio from "@uppy/audio";
import ImageEditor from "@uppy/image-editor";
import Url from "@uppy/url";

/**
 * 插件管理器类
 */
export class UppyPluginManager {
  constructor(uppyInstance, locale = "zh-CN") {
    this.uppy = uppyInstance;
    this.locale = locale;

    // 插件状态
    this.pluginStates = this.loadPluginStates();

    // 插件定义
    this.pluginDefinitions = this.createPluginDefinitions();
  }

  /**
   * 从localStorage加载插件状态
   */
  loadPluginStates() {
    const saved = localStorage.getItem("uppy-plugin-states");
    if (saved) {
      try {
        return JSON.parse(saved);
      } catch (e) {
        console.warn("Failed to parse saved plugin states:", e);
      }
    }
    // 默认状态：摄像头开启，其他关闭
    return {
      webcam: true,
      screen: false,
      audio: false,
    };
  }

  /**
   * 保存插件状态到localStorage
   */
  savePluginStates() {
    localStorage.setItem("uppy-plugin-states", JSON.stringify(this.pluginStates));
  }

  /**
   * 获取插件国际化文本
   */
  getPluginTexts() {
    const isZh = this.locale === "zh-CN";
    return {
      webcam: {
        label: isZh ? "摄像头" : "Webcam",
        description: isZh ? "拍照和录制视频" : "Take photos and record videos",
        strings: {
          pluginNameCamera: isZh ? "摄像头" : "Camera",
          smile: isZh ? "微笑！" : "Smile!",
          takePicture: isZh ? "拍照" : "Take a picture",
          startRecording: isZh ? "开始录制" : "Begin recording",
          stopRecording: isZh ? "停止录制" : "Stop recording",
          allowAccessTitle: isZh ? "请允许访问您的摄像头" : "Please allow access to your camera",
          allowAccessDescription: isZh
            ? "为了拍照或录制视频，请允许此网站访问摄像头。"
            : "In order to take pictures or record video with your camera, please allow camera access for this site.",
        },
      },
      screen: {
        label: isZh ? "屏幕录制" : "Screen Capture",
        description: isZh ? "录制屏幕或应用窗口" : "Record screen or application window",
        strings: {
          startCapturing: isZh ? "开始屏幕录制" : "Begin screen capture",
          stopCapturing: isZh ? "停止屏幕录制" : "Stop screen capture",
          submitRecordedFile: isZh ? "提交录制文件" : "Submit recorded file",
          streamActive: isZh ? "录制中" : "Recording active",
          streamPassive: isZh ? "录制暂停" : "Recording stopped",
          micDisabled: isZh ? "麦克风被禁用" : "Microphone disabled",
          recording: isZh ? "录制中" : "Recording",
        },
      },
      audio: {
        label: isZh ? "音频录制" : "Audio Recording",
        description: isZh ? "录制音频文件" : "Record audio files",
        strings: {
          pluginNameAudio: isZh ? "音频" : "Audio",
          startAudioRecording: isZh ? "开始录音" : "Begin audio recording",
          stopAudioRecording: isZh ? "停止录音" : "Stop audio recording",
          allowAudioAccessTitle: isZh ? "请允许访问您的麦克风" : "Please allow access to your microphone",
          allowAudioAccessDescription: isZh
            ? "为了录制音频，请允许此网站访问麦克风。"
            : "In order to record audio with your microphone, please allow microphone access for this site.",
          noAudioTitle: isZh ? "麦克风不可用" : "Microphone not available",
          noAudioDescription: isZh ? "为了录制音频，请连接麦克风或其他音频输入设备" : "To record audio, please connect a microphone or other audio input device",
          recordingStoppedMaxSize: isZh ? "录制已停止，因为文件大小即将超出限制" : "Recording stopped because file size is about to exceed the limit",
          recordingLength: isZh ? "录制时长 %{recording_length}" : "Recording length %{recording_length}",
          submitRecordedFile: isZh ? "提交录制文件" : "Submit recorded file",
          discardRecordedFile: isZh ? "丢弃录制文件" : "Discard recorded file",
        },
      },
      imageEditor: {
        label: isZh ? "图片编辑" : "Image Editor",
        description: isZh ? "编辑和优化图片" : "Edit and optimize images",
        strings: {
          edit: isZh ? "编辑" : "Edit",
          save: isZh ? "保存" : "Save",
          cancel: isZh ? "取消" : "Cancel",
          revert: isZh ? "还原" : "Revert",
          crop: isZh ? "裁剪" : "Crop",
          rotate: isZh ? "旋转" : "Rotate",
          flip: isZh ? "翻转" : "Flip",
          brightness: isZh ? "亮度" : "Brightness",
          contrast: isZh ? "对比度" : "Contrast",
          saturation: isZh ? "饱和度" : "Saturation",
        },
      },
    };
  }

  /**
   * 创建插件定义
   */
  createPluginDefinitions() {
    const texts = this.getPluginTexts();

    return [
      {
        key: "webcam",
        label: texts.webcam.label,
        description: texts.webcam.description,
        enabled: this.pluginStates.webcam,
        plugin: Webcam,
        config: {
          modes: ["video-audio", "video-only", "picture"],
          mirror: true,
          showRecordingLength: true,
          locale: {
            strings: texts.webcam.strings,
          },
        },
      },
      {
        key: "screen",
        label: texts.screen.label,
        description: texts.screen.description,
        enabled: this.pluginStates.screen,
        plugin: ScreenCapture,
        config: {
          displayMediaConstraints: {
            video: {
              width: { min: 640, ideal: 1920, max: 1920 },
              height: { min: 480, ideal: 1080, max: 1080 },
            },
            audio: true,
          },
          locale: {
            strings: texts.screen.strings,
          },
        },
      },
      {
        key: "audio",
        label: texts.audio.label,
        description: texts.audio.description,
        enabled: this.pluginStates.audio,
        plugin: Audio,
        config: {
          showRecordingLength: true,
          locale: {
            strings: texts.audio.strings,
          },
        },
      },
      // URL导入功能需要Companion服务器，暂时禁用
      // {
      //   key: "url",
      //   label: "URL导入",
      //   description: "从链接导入文件",
      //   enabled: false,
      //   plugin: Url,
      //   config: {
      //     companionUrl: "https://api2.transloadit.com/companion",
      //     companionAllowedHosts: ["https://api2.transloadit.com"],
      //   },
      // },
    ];
  }

  /**
   * 获取插件列表（用于UI显示）
   */
  getPluginList() {
    return this.pluginDefinitions.map((plugin) => ({
      key: plugin.key,
      label: plugin.label,
      description: plugin.description,
      enabled: plugin.enabled,
    }));
  }

  /**
   * 切换插件状态
   */
  togglePlugin(pluginKey) {
    const plugin = this.pluginDefinitions.find((p) => p.key === pluginKey);
    if (plugin) {
      plugin.enabled = !plugin.enabled;
      this.pluginStates[pluginKey] = plugin.enabled;
      this.savePluginStates();
      return plugin.enabled;
    }
    return false;
  }

  /**
   * 添加所有启用的插件到Uppy实例
   */
  addPluginsToUppy() {
    if (!this.uppy) return;

    // 默认添加图片编辑插件
    const texts = this.getPluginTexts();
    this.uppy.use(ImageEditor, {
      quality: 0.8,
      locale: {
        strings: texts.imageEditor.strings,
      },
    });

    // 添加用户选择的插件
    this.pluginDefinitions.forEach((plugin) => {
      if (plugin.enabled) {
        this.uppy.use(plugin.plugin, plugin.config);
      }
    });
  }

  /**
   * 更新语言设置
   */
  updateLocale(newLocale) {
    this.locale = newLocale;
    this.pluginDefinitions = this.createPluginDefinitions();
  }

  /**
   * 获取启用的插件数量
   */
  getEnabledPluginsCount() {
    return this.pluginDefinitions.filter((p) => p.enabled).length;
  }
}

/**
 * 创建插件管理器实例
 */
export function createUppyPluginManager(uppyInstance, locale = "zh-CN") {
  return new UppyPluginManager(uppyInstance, locale);
}
