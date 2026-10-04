import { Hono } from "hono";
import { registerFilesPublicRoutes } from "./files/public.js";
import { registerFilesProtectedRoutes } from "./files/protected.js";
import { registerFolderPublicRoutes } from "./files/folderPublic.js";

const app = new Hono();

registerFilesPublicRoutes(app);
registerFilesProtectedRoutes(app);
registerFolderPublicRoutes(app);

export default app;
