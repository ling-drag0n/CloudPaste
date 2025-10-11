import { ApiStatus } from "../constants/index.js";
import { generateRandomString, createErrorResponse } from "../utils/common.js";
import { hashPassword, verifyPassword } from "../utils/crypto.js";
import { HTTPException } from "hono/http-exception";
import { RepositoryFactory } from "../repositories/index.js";

/**
 * 验证管理员令牌
 * @param {D1Database} db - D1数据库实例
 * @param {string} token - JWT令牌
 * @returns {Promise<string|null>} 管理员ID或null
 */
export async function validateAdminToken(db, token) {
  console.log("验证管理员令牌:", token.substring(0, 5) + "..." + token.substring(token.length - 5));

  try {
    // 使用 AdminRepository 验证令牌
    const repositoryFactory = new RepositoryFactory(db);
    const adminRepository = repositoryFactory.getAdminRepository();

    const adminId = await adminRepository.validateToken(token);

    if (adminId) {
      console.log("令牌验证成功，管理员ID:", adminId);
    } else {
      console.log("令牌验证失败");
    }

    return adminId;
  } catch (error) {
    console.error("验证令牌时发生错误:", error);
    return null;
  }
}

/**
 * 管理员登录
 * @param {D1Database} db - D1数据库实例
 * @param {string} username - 用户名
 * @param {string} password - 密码
 * @returns {Promise<Object>} 登录结果，包含token和过期时间
 */
export async function login(db, username, password) {
  // 参数验证
  if (!username || !password) {
    throw new HTTPException(ApiStatus.BAD_REQUEST, { message: "用户名和密码不能为空" });
  }

  // 使用 AdminRepository 查询管理员
  const repositoryFactory = new RepositoryFactory(db);
  const adminRepository = repositoryFactory.getAdminRepository();

  const admin = await adminRepository.findByUsername(username);

  if (!admin) {
    throw new HTTPException(ApiStatus.UNAUTHORIZED, { message: "用户名或密码错误" });
  }

  // 验证密码
  const isValid = await verifyPassword(password, admin.password);
  if (!isValid) {
    throw new HTTPException(ApiStatus.UNAUTHORIZED, { message: "用户名或密码错误" });
  }

  // ===== token管理：限制并发session数量 =====
  const MAX_TOKENS_PER_USER = 5; // 每个用户最多5个token

  // 获取该用户的所有token（包括过期的）
  const existingTokens = await adminRepository.getAllTokensForAdmin(admin.id);

  // 如果超过限制，删除最旧的token，保留最新的5个
  if (existingTokens.length >= MAX_TOKENS_PER_USER) {
    // 保留最新的4个token，为新token留出空间
    const tokensToKeep = existingTokens.slice(0, MAX_TOKENS_PER_USER - 1);
    const tokensToDelete = existingTokens.slice(MAX_TOKENS_PER_USER - 1);

    for (const tokenToDelete of tokensToDelete) {
      await adminRepository.deleteToken(tokenToDelete.token);
    }

    console.log(`已清理 ${tokensToDelete.length} 个旧token（包括过期token），保留最新的 ${tokensToKeep.length} 个，当前用户: ${admin.username}`);
  }
  // ===== token管理结束 =====

  // 更新最后登录时间
  await adminRepository.updateLastLogin(admin.id);

  // 生成并存储令牌
  const token = generateRandomString(32);
  const expiresAt = new Date();
  expiresAt.setDate(expiresAt.getDate() + 1); // 1天过期

  // 使用 AdminRepository 创建令牌
  await adminRepository.createToken(admin.id, token, expiresAt);

  // 返回认证信息
  return {
    username: admin.username,
    token,
    expiresAt: expiresAt.toISOString(),
  };
}

/**
 * 管理员登出
 * @param {D1Database} db - D1数据库实例
 * @param {string} token - 认证令牌
 * @returns {Promise<void>}
 */
export async function logout(db, token) {
  // 使用 AdminRepository 删除令牌
  const repositoryFactory = new RepositoryFactory(db);
  const adminRepository = repositoryFactory.getAdminRepository();

  await adminRepository.deleteToken(token);
}

/**
 * 更改管理员密码或用户名
 * @param {D1Database} db - D1数据库实例
 * @param {string} adminId - 管理员ID
 * @param {string} currentPassword - 当前密码
 * @param {string} newPassword - 新密码，可选
 * @param {string} newUsername - 新用户名，可选
 * @returns {Promise<void>}
 */
export async function changePassword(db, adminId, currentPassword, newPassword, newUsername) {
  // 使用 AdminRepository
  const repositoryFactory = new RepositoryFactory(db);
  const adminRepository = repositoryFactory.getAdminRepository();

  // 验证当前密码
  const admin = await adminRepository.findById(adminId);
  if (!admin) {
    throw new HTTPException(ApiStatus.NOT_FOUND, { message: "管理员不存在" });
  }

  if (!(await verifyPassword(currentPassword, admin.password))) {
    throw new HTTPException(ApiStatus.UNAUTHORIZED, { message: "当前密码错误" });
  }

  // 检查新密码是否与当前密码相同
  if (newPassword && (await verifyPassword(newPassword, admin.password))) {
    throw new HTTPException(ApiStatus.BAD_REQUEST, { message: "新密码不能与当前密码相同" });
  }

  // 如果提供了新用户名，先检查用户名是否已存在
  if (newUsername && newUsername.trim() !== "") {
    const usernameExists = await adminRepository.existsByUsername(newUsername, adminId);

    if (usernameExists) {
      throw new HTTPException(ApiStatus.CONFLICT, { message: "用户名已存在" });
    }

    // 更新用户名和密码
    const updateData = { username: newUsername };
    if (newPassword) {
      updateData.password = await hashPassword(newPassword);
    }

    await adminRepository.updateAdmin(adminId, updateData);
  } else if (newPassword) {
    // 仅更新密码
    const newPasswordHash = await hashPassword(newPassword);
    await adminRepository.updateAdmin(adminId, { password: newPasswordHash });
  } else {
    throw new HTTPException(ApiStatus.BAD_REQUEST, { message: "未提供新密码或新用户名" });
  }

  // 成功修改后，删除该管理员的所有认证令牌，强制重新登录
  await adminRepository.deleteTokensByAdminId(adminId);
}

/**
 * 测试管理员令牌是否有效
 * @param {D1Database} db - D1数据库实例
 * @param {string} token - JWT令牌
 * @returns {Promise<boolean>} 令牌是否有效
 */
export async function testAdminToken(db, token) {
  const adminId = await validateAdminToken(db, token);
  return adminId !== null;
}
