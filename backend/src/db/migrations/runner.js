/**
 * 轻量迁移运行器（Node + Workers 通用）
 *
 */

const MIGRATIONS_TABLE = "schema_migrations";

/**
 * 确保迁移记录表存在（SQLite/D1 语法）
 * @param {any} db
 */
export async function ensureSchemaMigrationsTable(db) {
  await db
    .prepare(
      `
      CREATE TABLE IF NOT EXISTS ${MIGRATIONS_TABLE} (
        id TEXT PRIMARY KEY,
        applied_at TEXT NOT NULL
      )
    `
    )
    .run();
}

/**
 * 读取已应用迁移 ID 集合
 * @param {any} db
 * @returns {Promise<Set<string>>}
 */
export async function getAppliedMigrationIds(db) {
  const resp = await db.prepare(`SELECT id FROM ${MIGRATIONS_TABLE}`).all();
  const rows = resp?.results || [];
  return new Set(rows.map((r) => r.id).filter(Boolean));
}

async function hasMigrationId(db, id) {
  try {
    const row = await db.prepare(`SELECT 1 AS ok FROM ${MIGRATIONS_TABLE} WHERE id = ?`).bind(id).first();
    return !!row;
  } catch {
    return false;
  }
}

function nowIso() {
  return new Date().toISOString();
}

function assertUniqueMigrationIds(migrations) {
  const seen = new Set();
  for (const m of migrations) {
    if (!m?.id) {
      throw new Error("migration: 缺少 id");
    }
    if (seen.has(m.id)) {
      throw new Error(`migration: 重复的 id: ${m.id}`);
    }
    seen.add(m.id);
  }
}

/**
 * 应用迁移列表
 * @param {{ db:any, dialect:any, env:any, providerName:string }} runtime
 * @param {Array<{ id:string, up:(ctx:any)=>Promise<void|boolean> }>} migrations
 */
export async function applyMigrations(runtime, migrations) {
  const { db, dialect } = runtime;
  const list = Array.isArray(migrations) ? migrations : [];

  assertUniqueMigrationIds(list);
  await ensureSchemaMigrationsTable(db);

  const insertSql =
    typeof dialect?.buildInsertIgnoreSql === "function"
      ? dialect.buildInsertIgnoreSql({
          table: MIGRATIONS_TABLE,
          columns: ["id", "applied_at"],
        })
      : `INSERT OR IGNORE INTO ${MIGRATIONS_TABLE} (id, applied_at) VALUES (?, ?)`;

  // 一次性读取已应用集合（1 次查询），在内存中比对。
  // 原实现逐条 SELECT，35 条迁移 = 35 次跨区往返；Worker 冷启动时会随
  // 「边缘→主库」延迟线性放大，导致每个新 isolate 的首个请求卡数秒。
  const applied = await getAppliedMigrationIds(db);

  for (const migration of list) {
    if (applied.has(migration.id)) {
      continue;
    }

    const result = await migration.up(runtime);
    const status = result === false ? "skipped" : "applied";
    console.log(`[db:migrations迁移表] ${status}: ${migration.id}`);

    // 记录迁移已应用（并发场景下允许重复写入被忽略）
    await db.prepare(insertSql).bind(migration.id, nowIso()).run();
    applied.add(migration.id);

    // up() 可能批量写入 schema_migrations（如 adopt/baseline），
    // 所以真正执行过迁移后重新同步一次，保持与原实现相同的正确性语义。
    // 注意：迁移已完成的库根本不会进这里 —— 热路径仍然只有 2 次查询。
    const refreshed = await getAppliedMigrationIds(db);
    for (const id of refreshed) applied.add(id);
  }
}

export default {
  applyMigrations,
  ensureSchemaMigrationsTable,
  getAppliedMigrationIds,
};
