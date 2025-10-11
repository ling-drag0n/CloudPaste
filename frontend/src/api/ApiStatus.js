/**
 * API状态码常量
 * 与后端保持一致
 */
export const ApiStatus = {
  SUCCESS: 200,
  CREATED: 201,
  ACCEPTED: 202, // 部分成功，用于批量操作中有部分失败的情况
  BAD_REQUEST: 400,
  UNAUTHORIZED: 401,
  FORBIDDEN: 403,
  NOT_FOUND: 404,
  CONFLICT: 409,
  GONE: 410,
  INTERNAL_ERROR: 500,
};
