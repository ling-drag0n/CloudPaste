import assert from "node:assert/strict";
import test from "node:test";
import { Hono } from "hono";

import { Permission } from "../src/constants/permissions.js";
import { hashPassword } from "../src/utils/crypto.js";
import { isMountAccessible, isWebProxyEnabled, isSharePasswordAccepted } from "../src/security/helpers/proxyAccess.js";
import { usePolicy } from "../src/security/policies/policies.js";
import { getAccessibleMountsByBasicPath } from "../src/services/apiKeyService.js";

const requireProxyFsRead = usePolicy("fs.read", {
  pathResolver: (c) => c.get("proxyLinkBody")?.path,
});

const requestFsLink = async ({ principal }) => {
  const app = new Hono();
  app.use("*", async (c, next) => {
    c.set("principal", principal);
    c.set("authService", {
      checkBasicPathPermission(base, target) {
        const normalizedBase = base === "/" ? "/" : String(base).replace(/\/+$/, "");
        const normalizedTarget = String(target || "/").replace(/\/+$/, "") || "/";
        return normalizedBase === "/" || normalizedTarget === normalizedBase || normalizedTarget.startsWith(`${normalizedBase}/`);
      },
    });
    await next();
  });
  app.post(
    "/api/proxy/link",
    async (c, next) => {
      c.set("proxyLinkBody", await c.req.json());
      await next();
    },
    requireProxyFsRead,
    (c) => c.json({ ok: true }),
  );
  app.onError((error, c) => c.json({ code: error.code }, error.status || 500));

  return app.request("http://localhost/api/proxy/link", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ type: "fs", path: "/private/file.txt" }),
  });
};

test("anonymous callers cannot resolve FS upstream links", async () => {
  const response = await requestFsLink({
    principal: { type: "anonymous", id: null, authorities: 0, attributes: {}, isAdmin: false },
  });

  assert.equal(response.status, 401);
});

test("API keys without MOUNT_VIEW cannot resolve FS upstream links", async () => {
  const response = await requestFsLink({
    principal: {
      type: "apiKey",
      id: "key-1",
      authorities: 0,
      attributes: { basicPath: "/private" },
      isAdmin: false,
    },
  });

  assert.equal(response.status, 403);
});

test("API keys cannot resolve FS links outside their basic path", async () => {
  const response = await requestFsLink({
    principal: {
      type: "apiKey",
      id: "key-1",
      authorities: Permission.MOUNT_VIEW,
      attributes: { basicPath: "/allowed" },
      isAdmin: false,
    },
  });

  assert.equal(response.status, 403);
});

test("local proxy access requires an explicitly enabled web_proxy mount", () => {
  assert.equal(isWebProxyEnabled({ web_proxy: true }), true);
  assert.equal(isWebProxyEnabled({ web_proxy: 1 }), true);
  assert.equal(isWebProxyEnabled({ web_proxy: false }), false);
  assert.equal(isWebProxyEnabled({ web_proxy: 0 }), false);
  assert.equal(isWebProxyEnabled({}), false);
});

test("upstream link optimization honors the resolved mount ACL", () => {
  const privateMount = { id: "private-mount" };
  assert.equal(isMountAccessible(privateMount, [{ id: "public-mount" }]), false);
  assert.equal(isMountAccessible(privateMount, [{ id: "private-mount" }]), true);
  assert.equal(isMountAccessible(privateMount, null), false);
});

test("password-protected shares require the correct password", async () => {
  const passwordHash = await hashPassword("correct horse battery staple");
  const file = { password: passwordHash };

  assert.equal(await isSharePasswordAccepted(file, null), false);
  assert.equal(await isSharePasswordAccepted(file, "wrong"), false);
  assert.equal(await isSharePasswordAccepted(file, "correct horse battery staple"), true);
  assert.equal(await isSharePasswordAccepted({ password: null }, null), true);
});

test("ACL lookup failures fail closed instead of expanding mount access", async () => {
  const repositoryFactory = {
    getMountRepository: () => ({
      findMany: async () => [{ id: "mount-1", mount_path: "/public", storage_config_id: "cfg-1" }],
    }),
    getStorageConfigRepository: () => ({
      findById: async () => ({ id: "cfg-1", is_public: 1 }),
    }),
    getPrincipalStorageAclRepository: () => ({
      findConfigIdsBySubject: async () => {
        throw new Error("acl unavailable");
      },
    }),
  };

  await assert.rejects(
    getAccessibleMountsByBasicPath({}, "/", "API_KEY", "key-1", repositoryFactory),
    { code: "FORBIDDEN", status: 403 },
  );
});
