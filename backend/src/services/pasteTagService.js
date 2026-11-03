import { AuthorizationError, ConflictError, NotFoundError, ValidationError } from "../http/errors.js";
import { ensureRepositoryFactory } from "../utils/repositories.js";

export const PASTE_TAG_COLORS = [
  "#EF4444",
  "#F97316",
  "#FACC15",
  "#EAB308",
  "#22C55E",
  "#14B8A6",
  "#06B6D4",
  "#3B82F6",
  "#6366F1",
  "#A855F7",
  "#EC4899",
];

export const MAX_PASTE_TAGS = 10;

const normalizeName = (value) => String(value ?? "").trim().replace(/\s+/g, " ");

const validateName = (value) => {
  const name = normalizeName(value);
  if (!name) throw new ValidationError("标签名称不能为空");
  if (name.length > 32) throw new ValidationError("标签名称不能超过 32 个字符");
  return name;
};

const validateColor = (value) => {
  const color = String(value ?? "").toUpperCase();
  if (!PASTE_TAG_COLORS.includes(color)) {
    throw new ValidationError("请选择有效的标签颜色");
  }
  return color;
};

const normalizeIds = (value, { field = "ID", max = 100, allowEmpty = true } = {}) => {
  if (!Array.isArray(value)) throw new ValidationError(`${field}列表格式无效`);
  const ids = [...new Set(value.map((id) => String(id || "").trim()).filter(Boolean))];
  if (!allowEmpty && ids.length === 0) throw new ValidationError(`请至少选择一个${field}`);
  if (ids.length > max) throw new ValidationError(`${field}数量不能超过 ${max} 个`);
  return ids;
};

const assertTagIdsExist = async (repository, tagIds) => {
  if (!tagIds.length) return;
  const existing = await repository.findExistingTagIds(tagIds);
  if (existing.length !== tagIds.length) {
    throw new ValidationError("包含不存在的标签，请刷新后重试");
  }
};

export async function validatePasteTagIds(db, rawTagIds, repositoryFactory = null) {
  const tagIds = normalizeIds(rawTagIds, { field: "标签", max: MAX_PASTE_TAGS, allowEmpty: true });
  const factory = ensureRepositoryFactory(db, repositoryFactory);
  await assertTagIdsExist(factory.getPasteTagRepository(), tagIds);
  return tagIds;
}

const assertPastesAccessible = async (repository, pasteIds, createdBy = null) => {
  const existing = await repository.findAccessiblePasteIds(pasteIds, createdBy);
  if (existing.length !== pasteIds.length) {
    throw new AuthorizationError("部分文本不存在或无权操作");
  }
};

export async function listPasteTags(db, createdBy = null, repositoryFactory = null) {
  const factory = ensureRepositoryFactory(db, repositoryFactory);
  const repository = factory.getPasteTagRepository();
  const [tags, untaggedCount, totalCount] = await Promise.all([
    repository.listWithUsageCount(createdBy),
    repository.countUntagged(createdBy),
    repository.countPastes(createdBy),
  ]);
  return {
    tags: tags.map((tag) => ({ ...tag, usage_count: Number(tag.usage_count || 0) })),
    untagged_count: untaggedCount,
    total_count: totalCount,
  };
}

export async function createPasteTag(db, payload, createdBy, repositoryFactory = null) {
  const factory = ensureRepositoryFactory(db, repositoryFactory);
  const repository = factory.getPasteTagRepository();
  const name = validateName(payload?.name);
  const color = validateColor(payload?.color);

  if (await repository.findByName(name)) {
    throw new ConflictError("已存在同名标签");
  }

  const existing = await repository.listWithUsageCount();
  if (existing.length >= MAX_PASTE_TAGS) {
    throw new ValidationError(`标签数量不能超过 ${MAX_PASTE_TAGS} 个`);
  }
  const tag = {
    id: crypto.randomUUID(),
    name,
    color,
    sort_order: existing.length,
    created_by: createdBy || null,
    created_at: new Date().toISOString(),
    updated_at: new Date().toISOString(),
  };
  await repository.createTag(tag);
  return { ...tag, usage_count: 0 };
}

export async function updatePasteTag(db, id, payload, repositoryFactory = null) {
  const factory = ensureRepositoryFactory(db, repositoryFactory);
  const repository = factory.getPasteTagRepository();
  const current = await repository.findById(id);
  if (!current) throw new NotFoundError("标签不存在");

  const updates = {};
  if (Object.prototype.hasOwnProperty.call(payload || {}, "name")) {
    updates.name = validateName(payload.name);
    if (await repository.findByName(updates.name, id)) {
      throw new ConflictError("已存在同名标签");
    }
  }
  if (Object.prototype.hasOwnProperty.call(payload || {}, "color")) {
    updates.color = validateColor(payload.color);
  }
  if (Object.keys(updates).length === 0) throw new ValidationError("没有可更新的标签字段");

  await repository.updateTag(id, updates);
  return { ...current, ...updates, updated_at: new Date().toISOString() };
}

export async function deletePasteTag(db, id, repositoryFactory = null) {
  const factory = ensureRepositoryFactory(db, repositoryFactory);
  const repository = factory.getPasteTagRepository();
  if (!(await repository.findById(id))) throw new NotFoundError("标签不存在");
  await repository.deleteTag(id);
}

export async function reorderPasteTags(db, rawIds, repositoryFactory = null) {
  const ids = normalizeIds(rawIds, { field: "标签", max: MAX_PASTE_TAGS, allowEmpty: true });
  const factory = ensureRepositoryFactory(db, repositoryFactory);
  const repository = factory.getPasteTagRepository();
  await assertTagIdsExist(repository, ids);
  await repository.reorderTags(ids);
}

export async function hydratePastesWithTags(db, pastes, repositoryFactory = null) {
  if (!Array.isArray(pastes) || pastes.length === 0) return pastes || [];
  const factory = ensureRepositoryFactory(db, repositoryFactory);
  const repository = factory.getPasteTagRepository();
  const tagMap = await repository.getTagsForPasteIds(pastes.map((paste) => paste.id));
  return pastes.map((paste) => ({ ...paste, tags: tagMap.get(paste.id) || [] }));
}

export async function setPasteTags(db, pasteId, rawTagIds, createdBy = null, repositoryFactory = null) {
  const tagIds = normalizeIds(rawTagIds, { field: "标签", max: MAX_PASTE_TAGS, allowEmpty: true });
  const factory = ensureRepositoryFactory(db, repositoryFactory);
  const repository = factory.getPasteTagRepository();
  await assertPastesAccessible(repository, [pasteId], createdBy);
  await assertTagIdsExist(repository, tagIds);
  await repository.replacePasteTags(pasteId, tagIds);
  const tagMap = await repository.getTagsForPasteIds([pasteId]);
  return tagMap.get(pasteId) || [];
}

export async function batchUpdatePasteTags(db, payload, createdBy = null, repositoryFactory = null) {
  const pasteIds = normalizeIds(payload?.ids, { field: "文本", max: 100, allowEmpty: false });
  const tagIds = normalizeIds(payload?.tag_ids, { field: "标签", max: MAX_PASTE_TAGS, allowEmpty: false });
  const action = payload?.action;
  if (action !== "add" && action !== "remove") {
    throw new ValidationError("批量标签操作仅支持 add 或 remove");
  }

  const factory = ensureRepositoryFactory(db, repositoryFactory);
  const repository = factory.getPasteTagRepository();
  await assertPastesAccessible(repository, pasteIds, createdBy);
  await assertTagIdsExist(repository, tagIds);

  if (action === "add") {
    await repository.addTagsToPastes(pasteIds, tagIds);
  } else {
    await repository.removeTagsFromPastes(pasteIds, tagIds);
  }

  return { affected_count: pasteIds.length };
}
