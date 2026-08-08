import { verifyPassword } from "../../utils/crypto.js";

export const isWebProxyEnabled = (mount) => mount?.web_proxy === 1 || mount?.web_proxy === true;

export const isMountAccessible = (mount, accessibleMounts) => {
  const mountId = mount?.id;
  return Boolean(mountId) && Array.isArray(accessibleMounts) && accessibleMounts.some((candidate) => candidate?.id === mountId);
};

export const isSharePasswordAccepted = async (file, password) => {
  if (!file?.password) {
    return true;
  }
  return Boolean(password) && verifyPassword(password, file.password);
};
