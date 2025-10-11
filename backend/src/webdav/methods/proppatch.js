/**
 * 处理 WebDAV PROPPATCH 请求
 * "允许所有属性"的简单响应，客户端兼容性
 * @param {Object} c - Hono上下文
 * @param {string} path - 请求路径
 * @param {string|Object} userId - 用户ID或信息
 * @param {string} userType - 用户类型
 * @param {D1Database} db - 数据库实例
 * @returns {Response} HTTP响应
 */
export async function handleProppatch(c, path, userId, userType, db) {
  try {
    console.log(`WebDAV PROPPATCH 请求 - 路径: ${path} (兼容性实现)`);

    // 获取请求体
    const requestBody = await c.req.text();
    console.log(`PROPPATCH 请求体:`, requestBody);

    // 解析请求体中的属性
    let properties = [];
    try {
      // 简单的XML解析，提取所有要设置的属性
      const setMatches = requestBody.match(/<D:set[^>]*>(.*?)<\/D:set>/gs);
      if (setMatches) {
        for (const setMatch of setMatches) {
          const propMatches = setMatch.match(/<D:prop[^>]*>(.*?)<\/D:prop>/gs);
          if (propMatches) {
            for (const propMatch of propMatches) {
              // 提取属性元素
              const propertyMatches = propMatch.match(/<([^>\/\s]+)[^>]*>/g);
              if (propertyMatches) {
                for (const propertyMatch of propertyMatches) {
                  if (!propertyMatch.includes("D:prop")) {
                    const propertyName = propertyMatch.match(/<([^>\s\/]+)/)[1];
                    properties.push(propertyName);
                  }
                }
              }
            }
          }
        }
      }
    } catch (parseError) {
      console.warn(`PROPPATCH XML解析警告:`, parseError.message);
    }

    console.log(`PROPPATCH 提取的属性:`, properties);

    // 构建成功的207 Multi-Status响应
    const href = `/dav${path}`;

    let responseXml;
    if (properties.length > 0) {
      // 为每个属性构建成功的propstat
      const propstatElements = properties
        .map((prop) => {
          return `      <D:propstat>
        <D:prop><${prop}/></D:prop>
        <D:status>HTTP/1.1 200 OK</D:status>
      </D:propstat>`;
        })
        .join("\n");

      responseXml = `<?xml version="1.0" encoding="utf-8"?>
<D:multistatus xmlns:D="DAV:">
  <D:response>
    <D:href>${href}</D:href>
${propstatElements}
  </D:response>
</D:multistatus>`;
    } else {
      // 没有属性时的简单成功响应
      responseXml = `<?xml version="1.0" encoding="utf-8"?>
<D:multistatus xmlns:D="DAV:">
  <D:response>
    <D:href>${href}</D:href>
    <D:propstat>
      <D:prop/>
      <D:status>HTTP/1.1 200 OK</D:status>
    </D:propstat>
  </D:response>
</D:multistatus>`;
    }
    return new Response(responseXml, {
      status: 207, // Multi-Status
      headers: {
        "Content-Type": "application/xml; charset=utf-8",
        "Content-Length": responseXml.length.toString(),
      },
    });
  } catch (error) {
    console.error("PROPPATCH 错误:", error);
    return new Response("Internal Server Error", {
      status: 500,
      headers: {
        "Content-Type": "text/plain; charset=utf-8",
      },
    });
  }
}
