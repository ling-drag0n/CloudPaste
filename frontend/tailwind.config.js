/** @type {import('tailwindcss').Config} */
export default {
  content: ["./index.html", "./src/**/*.{vue,js,ts,jsx,tsx}"],
  theme: {
    screens: {
      xs: "360px",
      sm: "640px",
      md: "768px",
      lg: "1024px",
      xl: "1280px",
      "2xl": "1536px",
    },
    extend: {
      colors: {
        primary: {
          50: "#f0f9ff",
          100: "#e0f2fe",
          200: "#bae6fd",
          300: "#7dd3fc",
          400: "#38bdf8",
          500: "#3b82f6",
          600: "#0284c7",
          700: "#0369a1",
          800: "#075985",
          900: "#0c4a6e",
          950: "#082f49",
        },
        secondary: {
          50: "#f5f3ff",
          100: "#ede9fe",
          200: "#ddd6fe",
          300: "#c4b5fd",
          400: "#a78bfa",
          500: "#8b5cf6",
          600: "#7c3aed",
          700: "#6d28d9",
          800: "#5b21b6",
          900: "#4c1d95",
          950: "#2e1065",
        },
        // 支持自定义CSS变量的颜色系统
        "custom-bg": {
          50: "var(--custom-bg-light, rgb(249 250 251))",
          900: "var(--custom-bg-dark, rgb(17 24 39))",
        },
        "custom-surface": {
          DEFAULT: "var(--custom-surface-light, rgb(255 255 255))",
          dark: "var(--custom-surface-dark, rgb(31 41 55))",
        },
        "custom-text": {
          DEFAULT: "var(--custom-text-light, rgb(17 24 39))",
          dark: "var(--custom-text-dark, rgb(243 244 246))",
        },
      },

      gridTemplateColumns: {
        "file-list": "2fr 0.8fr 1fr 0.8fr 0.8fr 1.5fr 0.8fr",
        "search-result": "2fr 1fr 1fr 1.5fr auto",
        "search-result-simple": "2fr 1fr auto",
      },
    },
  },
  plugins: [],
  darkMode: "class",
};
