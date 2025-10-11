import { createI18n } from "vue-i18n";
import zhCN from "./locales/zh-CN/index.js";
import enUS from "./locales/en-US/index.js";

// 获取浏览器语言设置
const getBrowserLanguage = () => {
  const browserLang = navigator.language || navigator.userLanguage;
  if (browserLang.startsWith("zh")) {
    return "zh-CN";
  }
  return "en-US";
};

// 获取保存的语言设置，如果没有则使用浏览器语言
const getSavedLanguage = () => {
  const savedLang = localStorage.getItem("language");
  return savedLang || getBrowserLanguage();
};

// 保存语言设置到本地存储
export const saveLanguagePreference = (lang) => {
  localStorage.setItem("language", lang);
};

// 创建i18n实例
const i18n = createI18n({
  legacy: false, // 使用组合式API
  locale: getSavedLanguage(),
  fallbackLocale: "zh-CN", // 回退语言
  messages: {
    "zh-CN": zhCN,
    "en-US": enUS,
  },
  // 确保正确处理日期和数字等格式化
  globalInjection: true,
  // 使用HTML
  warnHtmlMessage: false,
  // 确保语言改变时静默警告
  silentTranslationWarn: true,
  silentFallbackWarn: true,
});

// 调试辅助函数 - 仅在开发环境需要时使用
export const debugI18n = () => {
  if (import.meta.env.DEV) {
    console.log("当前i18n配置:", {
      当前语言: i18n.global.locale.value,
      回退语言: i18n.global.fallbackLocale.value,
      可用语言: Object.keys(i18n.global.messages.value),
      模块化结构: "已启用",
    });
  }
};

export default i18n;
