<template>
  <div class="offline-fallback">
    <div class="container">
      <div class="content">
        <!-- 离线图标 -->
        <div class="icon">
          <svg class="w-16 h-16 text-yellow-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path
              stroke-linecap="round"
              stroke-linejoin="round"
              stroke-width="2"
              d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-2.5L13.732 4c-.77-.833-1.964-.833-2.732 0L4.082 16.5c-.77.833.192 2.5 1.732 2.5z"
            />
          </svg>
        </div>

        <!-- 标题 -->
        <h1 :class="['title', darkMode ? 'text-white' : 'text-gray-900']">页面暂时无法访问</h1>

        <!-- 描述 -->
        <p :class="['description', darkMode ? 'text-gray-300' : 'text-gray-600']">您当前处于离线状态，此页面尚未缓存。请检查网络连接后重试，或访问其他已缓存的页面。</p>

        <!-- 操作按钮 -->
        <div class="actions">
          <button @click="goHome" :class="['btn-primary', darkMode ? 'bg-blue-600 hover:bg-blue-700' : 'bg-blue-500 hover:bg-blue-600']">返回首页</button>

          <button
            @click="goBack"
            :class="[
              'btn-secondary',
              darkMode ? 'text-gray-300 hover:text-white border-gray-600 hover:border-gray-500' : 'text-gray-600 hover:text-gray-800 border-gray-300 hover:border-gray-400',
            ]"
          >
            返回上页
          </button>

          <button
            @click="retry"
            :class="[
              'btn-secondary',
              darkMode ? 'text-gray-300 hover:text-white border-gray-600 hover:border-gray-500' : 'text-gray-600 hover:text-gray-800 border-gray-300 hover:border-gray-400',
            ]"
          >
            重新尝试
          </button>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { computed } from "vue";
import { useRouter } from "vue-router";

const props = defineProps({
  darkMode: {
    type: Boolean,
    default: false,
  },
  targetRoute: {
    type: String,
    default: "",
  },
});

const router = useRouter();

// 返回首页
const goHome = () => {
  router.push("/");
};

// 返回上一页
const goBack = () => {
  if (window.history.length > 1) {
    router.go(-1);
  } else {
    goHome();
  }
};

// 重新尝试访问目标页面
const retry = () => {
  if (props.targetRoute) {
    router.push(props.targetRoute);
  } else {
    window.location.reload();
  }
};
</script>

<style scoped>
.offline-fallback {
  min-height: 100vh;
  display: flex;
  align-items: center;
  justify-content: center;
  padding: 1rem;
}

.container {
  max-width: 28rem;
  width: 100%;
}

.content {
  text-align: center;
}

.icon {
  margin-bottom: 1.5rem;
  display: flex;
  justify-content: center;
}

.title {
  font-size: 1.5rem;
  font-weight: 600;
  margin-bottom: 1rem;
}

.description {
  font-size: 1rem;
  line-height: 1.5;
  margin-bottom: 2rem;
}

.actions {
  display: flex;
  flex-direction: column;
  gap: 0.75rem;
}

.btn-primary,
.btn-secondary {
  padding: 0.75rem 1.5rem;
  border-radius: 0.5rem;
  font-weight: 500;
  transition: all 0.2s;
  cursor: pointer;
  border: 1px solid transparent;
}

.btn-primary {
  color: white;
  border: none;
}

.btn-secondary {
  background: transparent;
  border: 1px solid;
}

@media (min-width: 640px) {
  .actions {
    flex-direction: row;
    justify-content: center;
  }
}
</style>
