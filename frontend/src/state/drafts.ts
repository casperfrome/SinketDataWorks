import type { StudioObject } from "../types";

export type Drafts = Record<string, StudioObject>;

/** A save acknowledges only the exact draft submitted; later edits remain dirty. */
export function settleSavedDraft(
  current: StudioObject | undefined,
  submitted: StudioObject,
  saved: StudioObject,
): StudioObject | undefined {
  if (!current || current === submitted) return undefined;
  return { ...current, version: saved.version, updatedAt: saved.updatedAt };
}

/** Apply server metadata without replacing unsaved code, graph, or notebook cells. */
export function mergeMetadataDraft(
  current: StudioObject | undefined,
  saved: StudioObject,
  changedFields: string[],
): StudioObject | undefined {
  if (!current) return undefined;
  const metadata = Object.fromEntries(
    changedFields
      .filter((field) => Object.hasOwn(saved, field))
      .map((field) => [field, saved[field as keyof StudioObject]]),
  );
  return {
    ...current,
    ...metadata,
    version: saved.version,
    updatedAt: saved.updatedAt,
  };
}

/** The refreshed object list accounts for both direct and recursive deletions. */
export function pruneDeletedDrafts(
  drafts: Drafts,
  aliveIds: ReadonlySet<string>,
): Drafts {
  const entries = Object.entries(drafts).filter(([id]) => aliveIds.has(id));
  return entries.length === Object.keys(drafts).length
    ? drafts
    : Object.fromEntries(entries);
}
