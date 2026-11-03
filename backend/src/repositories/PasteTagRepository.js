import { BaseRepository } from "./BaseRepository.js";
import { DbTables } from "../constants/index.js";

const chunk = (items, size) => {
  const chunks = [];
  for (let index = 0; index < items.length; index += size) {
    chunks.push(items.slice(index, index + size));
  }
  return chunks;
};

export class PasteTagRepository extends BaseRepository {
  async listWithUsageCount(createdBy = null) {
    const countExpression = createdBy
      ? "COUNT(DISTINCT CASE WHEN p.created_by = ? THEN a.paste_id END)"
      : "COUNT(DISTINCT a.paste_id)";
    const params = createdBy ? [createdBy] : [];
    const result = await this.query(
      `SELECT t.id, t.name, t.color, t.sort_order, t.created_by, t.created_at, t.updated_at,
              ${countExpression} AS usage_count
       FROM ${DbTables.PASTE_TAGS} t
       LEFT JOIN ${DbTables.PASTE_TAG_ASSIGNMENTS} a ON a.tag_id = t.id
       LEFT JOIN ${DbTables.PASTES} p ON p.id = a.paste_id
       GROUP BY t.id, t.name, t.color, t.sort_order, t.created_by, t.created_at, t.updated_at
       ORDER BY t.sort_order ASC, t.name COLLATE NOCASE ASC`,
      params,
    );
    return result.results || [];
  }

  async countUntagged(createdBy = null) {
    let sql = `SELECT COUNT(*) AS count
               FROM ${DbTables.PASTES} p
               WHERE NOT EXISTS (
                 SELECT 1 FROM ${DbTables.PASTE_TAG_ASSIGNMENTS} a WHERE a.paste_id = p.id
               )`;
    const params = [];
    if (createdBy) {
      sql += " AND p.created_by = ?";
      params.push(createdBy);
    }
    const result = await this.queryFirst(sql, params);
    return Number(result?.count || 0);
  }

  async countPastes(createdBy = null) {
    let sql = `SELECT COUNT(*) AS count FROM ${DbTables.PASTES}`;
    const params = [];
    if (createdBy) {
      sql += " WHERE created_by = ?";
      params.push(createdBy);
    }
    const result = await this.queryFirst(sql, params);
    return Number(result?.count || 0);
  }

  async findById(id) {
    return await super.findById(DbTables.PASTE_TAGS, id);
  }

  async findByName(name, excludeId = null) {
    let sql = `SELECT * FROM ${DbTables.PASTE_TAGS} WHERE name = ? COLLATE NOCASE`;
    const params = [name];
    if (excludeId) {
      sql += " AND id != ?";
      params.push(excludeId);
    }
    return await this.queryFirst(sql, params);
  }

  async createTag(tag) {
    return await this.create(DbTables.PASTE_TAGS, tag);
  }

  async updateTag(id, updates) {
    return await this.update(DbTables.PASTE_TAGS, id, {
      ...updates,
      updated_at: new Date().toISOString(),
    });
  }

  async deleteTag(id) {
    return await this.delete(DbTables.PASTE_TAGS, id);
  }

  async reorderTags(ids) {
    for (const entries of chunk(ids.map((id, index) => ({ id, index })), 80)) {
      const statements = entries.map(({ id, index }) =>
        this.db
          .prepare(`UPDATE ${DbTables.PASTE_TAGS} SET sort_order = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?`)
          .bind(index, id),
      );
      await this.db.batch(statements);
    }
  }

  async findExistingTagIds(ids) {
    if (!ids.length) return [];
    const placeholders = ids.map(() => "?").join(",");
    const result = await this.query(`SELECT id FROM ${DbTables.PASTE_TAGS} WHERE id IN (${placeholders})`, ids);
    return (result.results || []).map((row) => row.id);
  }

  async findAccessiblePasteIds(ids, createdBy = null) {
    if (!ids.length) return [];
    const placeholders = ids.map(() => "?").join(",");
    let sql = `SELECT id FROM ${DbTables.PASTES} WHERE id IN (${placeholders})`;
    const params = [...ids];
    if (createdBy) {
      sql += " AND created_by = ?";
      params.push(createdBy);
    }
    const result = await this.query(sql, params);
    return (result.results || []).map((row) => row.id);
  }

  async getTagsForPasteIds(pasteIds) {
    const uniqueIds = [...new Set(pasteIds.filter(Boolean))];
    if (!uniqueIds.length) return new Map();

    const map = new Map(uniqueIds.map((id) => [id, []]));
    for (const ids of chunk(uniqueIds, 80)) {
      const placeholders = ids.map(() => "?").join(",");
      const result = await this.query(
        `SELECT a.paste_id, t.id, t.name, t.color, t.sort_order
         FROM ${DbTables.PASTE_TAG_ASSIGNMENTS} a
         INNER JOIN ${DbTables.PASTE_TAGS} t ON t.id = a.tag_id
         WHERE a.paste_id IN (${placeholders})
         ORDER BY t.sort_order ASC, t.name COLLATE NOCASE ASC`,
        ids,
      );
      for (const row of result.results || []) {
        map.get(row.paste_id)?.push({
          id: row.id,
          name: row.name,
          color: row.color,
          sort_order: row.sort_order,
        });
      }
    }
    return map;
  }

  async replacePasteTags(pasteId, tagIds) {
    const uniqueTagIds = [...new Set(tagIds)];
    const statements = [
      this.db.prepare(`DELETE FROM ${DbTables.PASTE_TAG_ASSIGNMENTS} WHERE paste_id = ?`).bind(pasteId),
    ];

    if (uniqueTagIds.length > 0) {
      const values = uniqueTagIds.map(() => "(?, ?, CURRENT_TIMESTAMP)").join(", ");
      statements.push(
        this.db
          .prepare(`INSERT INTO ${DbTables.PASTE_TAG_ASSIGNMENTS} (paste_id, tag_id, created_at) VALUES ${values}`)
          .bind(...uniqueTagIds.flatMap((tagId) => [pasteId, tagId])),
      );
    }

    await this.db.batch(statements);
  }

  async addTagsToPastes(pasteIds, tagIds) {
    const pairs = pasteIds.flatMap((pasteId) => tagIds.map((tagId) => [pasteId, tagId]));
    const sql = this._buildInsertIgnoreSql(DbTables.PASTE_TAG_ASSIGNMENTS, ["paste_id", "tag_id", "created_at"]);
    for (const pairChunk of chunk(pairs, 80)) {
      const timestamp = new Date().toISOString();
      const statements = pairChunk.map(([pasteId, tagId]) => this.db.prepare(sql).bind(pasteId, tagId, timestamp));
      await this.db.batch(statements);
    }
  }

  async removeTagsFromPastes(pasteIds, tagIds) {
    for (const pasteIdChunk of chunk(pasteIds, 70)) {
      const pastePlaceholders = pasteIdChunk.map(() => "?").join(",");
      const tagPlaceholders = tagIds.map(() => "?").join(",");
      await this.execute(
        `DELETE FROM ${DbTables.PASTE_TAG_ASSIGNMENTS}
         WHERE paste_id IN (${pastePlaceholders}) AND tag_id IN (${tagPlaceholders})`,
        [...pasteIdChunk, ...tagIds],
      );
    }
  }
}
