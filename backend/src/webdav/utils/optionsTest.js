/**
 * WebDAV OPTIONS方法测试
 * 测试重构后的OPTIONS方法功能
 */

import { handleOptions } from "../methods/options.js";

/**
 * 模拟Hono上下文
 */
function createMockContext(headers = {}, url = "http://localhost:3000/dav/test.txt") {
  return {
    req: {
      header: (name) => headers[name] || null,
      url: url,
    },
    env: {
      ENCRYPTION_SECRET: "test-secret-key-for-testing-only",
    },
  };
}

/**
 * 模拟数据库
 */
const mockDb = {
  prepare: () => ({
    bind: () => ({
      first: () => null,
      all: () => [],
    }),
  }),
};

/**
 * 测试CORS预检请求
 */
export async function testCorsPreflightRequest() {
  console.log("\n=== 测试CORS预检请求 ===");

  try {
    const corsHeaders = {
      Origin: "http://localhost:3000",
      "Access-Control-Request-Method": "PUT",
      "Access-Control-Request-Headers": "Content-Type",
    };

    const context = createMockContext(corsHeaders);
    const response = await handleOptions(context, "/dav/test.txt", "user123", "user", mockDb);

    console.log("✓ CORS预检请求状态码:", response.status);
    console.log("✓ CORS预检请求头:");

    const responseHeaders = {};
    response.headers.forEach((value, key) => {
      responseHeaders[key] = value;
    });

    console.log("  - Access-Control-Allow-Origin:", responseHeaders["access-control-allow-origin"]);
    console.log("  - Access-Control-Allow-Methods:", responseHeaders["access-control-allow-methods"]);
    console.log("  - Access-Control-Max-Age:", responseHeaders["access-control-max-age"]);

    const success = response.status === 204 && responseHeaders["access-control-allow-origin"] === "*" && responseHeaders["access-control-allow-methods"].includes("PUT");

    console.log("✓ CORS预检测试:", success ? "通过" : "失败");
    return success;
  } catch (error) {
    console.error("✗ CORS预检测试失败:", error);
    return false;
  }
}

/**
 * 测试WebDAV OPTIONS请求
 */
export async function testWebDAVOptionsRequest() {
  console.log("\n=== 测试WebDAV OPTIONS请求 ===");

  try {
    const webdavHeaders = {
      "User-Agent": "Microsoft-WebDAV-MiniRedir/10.0.19041",
      Authorization: "Bearer test-token",
    };

    const context = createMockContext(webdavHeaders);
    const response = await handleOptions(context, "/dav/test.txt", "admin", "admin", mockDb);

    console.log("✓ WebDAV OPTIONS状态码:", response.status);

    const responseHeaders = {};
    response.headers.forEach((value, key) => {
      responseHeaders[key] = value;
    });

    console.log("✓ WebDAV OPTIONS响应头:");
    console.log("  - DAV:", responseHeaders["dav"]);
    console.log("  - Allow:", responseHeaders["allow"]);
    console.log("  - Server:", responseHeaders["server"]);
    console.log("  - MS-Author-Via:", responseHeaders["ms-author-via"]);

    const success = response.status === 200 && responseHeaders["dav"] && responseHeaders["allow"] && responseHeaders["allow"].includes("LOCK");

    console.log("✓ WebDAV OPTIONS测试:", success ? "通过" : "失败");
    return success;
  } catch (error) {
    console.error("✗ WebDAV OPTIONS测试失败:", error);
    return false;
  }
}

/**
 * 测试不同客户端的兼容性
 */
export async function testClientCompatibility() {
  console.log("\n=== 测试客户端兼容性 ===");

  const clients = [
    {
      name: "Windows资源管理器",
      userAgent: "Microsoft-WebDAV-MiniRedir/10.0.19041",
      expectedHeaders: ["ms-author-via"],
    },
    {
      name: "Mac Finder",
      userAgent: "WebDAVFS/3.0.0 (03008000) Darwin/20.6.0 (x86_64)",
      expectedHeaders: ["x-dav-powered-by"],
    },
    {
      name: "Office应用",
      userAgent: "Microsoft Office/16.0 (Windows NT 10.0; Microsoft Outlook 16.0.13901; Pro)",
      expectedHeaders: ["ms-author-via"],
    },
    {
      name: "通用WebDAV客户端",
      userAgent: "WebDAV-Client/1.0",
      expectedHeaders: ["dav", "allow"],
    },
  ];

  let allPassed = true;

  for (const client of clients) {
    try {
      const context = createMockContext({ "User-Agent": client.userAgent });
      const response = await handleOptions(context, "/dav", "admin", "admin", mockDb);

      const responseHeaders = {};
      response.headers.forEach((value, key) => {
        responseHeaders[key.toLowerCase()] = value;
      });

      const hasExpectedHeaders = client.expectedHeaders.every((header) => responseHeaders[header.toLowerCase()]);

      console.log(`✓ ${client.name}:`, hasExpectedHeaders ? "兼容" : "不兼容");

      if (!hasExpectedHeaders) {
        console.log(`  缺少头: ${client.expectedHeaders.filter((h) => !responseHeaders[h.toLowerCase()])}`);
        allPassed = false;
      }
    } catch (error) {
      console.error(`✗ ${client.name}测试失败:`, error.message);
      allPassed = false;
    }
  }

  console.log("✓ 客户端兼容性测试:", allPassed ? "全部通过" : "有失败");
  return allPassed;
}

/**
 * 测试功能检测
 */
export async function testCapabilityDetection() {
  console.log("\n=== 测试功能检测 ===");

  try {
    // 测试管理员用户（应该有完整权限）
    const adminContext = createMockContext({}, "http://localhost:3000/dav");
    const adminResponse = await handleOptions(adminContext, "/dav", "admin", "admin", mockDb);

    const adminHeaders = {};
    adminResponse.headers.forEach((value, key) => {
      adminHeaders[key.toLowerCase()] = value;
    });

    const adminMethods = adminHeaders["allow"] || "";
    console.log("✓ 管理员支持的方法:", adminMethods);

    // 测试普通用户（可能权限受限）
    const userContext = createMockContext({}, "http://localhost:3000/dav/restricted");
    const userResponse = await handleOptions(userContext, "/dav/restricted", "user123", "user", mockDb);

    const userHeaders = {};
    userResponse.headers.forEach((value, key) => {
      userHeaders[key.toLowerCase()] = value;
    });

    const userMethods = userHeaders["allow"] || "";
    console.log("✓ 普通用户支持的方法:", userMethods);

    const hasLockSupport = adminMethods.includes("LOCK") && adminMethods.includes("UNLOCK");
    const hasPropfindSupport = adminMethods.includes("PROPFIND");
    const hasBasicMethods = adminMethods.includes("GET") && adminMethods.includes("PUT");

    console.log("✓ 锁定支持:", hasLockSupport ? "是" : "否");
    console.log("✓ PROPFIND支持:", hasPropfindSupport ? "是" : "否");
    console.log("✓ 基本方法支持:", hasBasicMethods ? "是" : "否");

    const success = hasLockSupport && hasPropfindSupport && hasBasicMethods;
    console.log("✓ 功能检测测试:", success ? "通过" : "失败");
    return success;
  } catch (error) {
    console.error("✗ 功能检测测试失败:", error);
    return false;
  }
}

/**
 * 运行所有OPTIONS测试
 */
export async function runAllOptionsTests() {
  console.log("🔧 开始WebDAV OPTIONS方法测试");

  const results = {
    cors: await testCorsPreflightRequest(),
    webdav: await testWebDAVOptionsRequest(),
    compatibility: await testClientCompatibility(),
    capability: await testCapabilityDetection(),
  };

  const allPassed = Object.values(results).every((result) => result === true);

  console.log("\n📊 OPTIONS测试结果汇总:");
  console.log("- CORS预检:", results.cors ? "✓ 通过" : "✗ 失败");
  console.log("- WebDAV OPTIONS:", results.webdav ? "✓ 通过" : "✗ 失败");
  console.log("- 客户端兼容性:", results.compatibility ? "✓ 通过" : "✗ 失败");
  console.log("- 功能检测:", results.capability ? "✓ 通过" : "✗ 失败");
  console.log("\n🎯 总体结果:", allPassed ? "✅ 全部通过" : "❌ 存在失败");

  return allPassed;
}

// 如果直接运行此文件，执行测试
if (import.meta.url === `file://${process.argv[1]}`) {
  runAllOptionsTests().then((result) => {
    process.exit(result ? 0 : 1);
  });
}
