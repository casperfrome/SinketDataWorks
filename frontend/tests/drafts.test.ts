import assert from "node:assert/strict";
import test from "node:test";
import type { StudioObject } from "../src/types.ts";
import {
  mergeMetadataDraft,
  pruneDeletedDrafts,
  settleSavedDraft,
} from "../src/state/drafts.ts";

const object = (overrides: Partial<StudioObject> = {}): StudioObject => ({
  id: "sql-node",
  workspaceId: "local",
  parentId: "folder",
  kind: "NODE",
  nodeType: "MaxCompute SQL",
  name: "orders",
  description: "订单开发",
  content: "SELECT 1;",
  config: { run: { parameters: { bizdate: "20260927" } } },
  tags: [],
  favorite: false,
  deleted: false,
  version: 1,
  owner: "local_admin",
  updatedAt: "2026-09-27T10:00:00Z",
  ...overrides,
});

test("a save response preserves typing and configuration edits made while the request was pending", () => {
  const submitted = object({ content: "SELECT order_id FROM orders;" });
  const current = {
    ...submitted,
    content: "SELECT order_id, amount FROM orders;",
    config: {
      ...submitted.config,
      run: { parameters: { bizdate: "20260928" } },
    },
  };
  const saved = { ...submitted, version: 2, updatedAt: "2026-09-27T10:01:00Z" };
  const settled = settleSavedDraft(current, submitted, saved)!;
  assert.equal(settled.content, current.content);
  assert.equal(settled.config, current.config);
  assert.equal(settled.version, 2);
  assert.equal(settled.updatedAt, saved.updatedAt);
  assert.equal(current.version, 1);
});

test("only the exact submitted draft becomes clean; an independent later draft remains dirty", () => {
  const submitted = object();
  const saved = { ...submitted, version: 2 };
  assert.equal(settleSavedDraft(submitted, submitted, saved), undefined);
  assert.notEqual(
    settleSavedDraft({ ...submitted }, submitted, saved),
    undefined,
  );
});

test("a repeated save response cannot recreate a cleared draft", () => {
  const submitted = object();
  const saved = { ...submitted, version: 2 };
  const clean = settleSavedDraft(submitted, submitted, saved);
  assert.equal(settleSavedDraft(clean, submitted, saved), undefined);
  assert.equal(settleSavedDraft(undefined, submitted, saved), undefined);
});

test("rename, move, favorite and owner changes do not swallow unsaved content or configuration", () => {
  const current = object({
    content: "-- 尚未保存\nSELECT * FROM orders;",
    config: { graph: { nodes: [{ id: "new-node" }], edges: [] } },
    description: "尚未保存的描述",
  });
  const saved = object({
    name: "daily_orders",
    parentId: "new-folder",
    favorite: true,
    owner: "analyst",
    version: 3,
    updatedAt: "2026-09-27T10:03:00Z",
  });
  const merged = mergeMetadataDraft(current, saved, [
    "name",
    "parentId",
    "favorite",
    "owner",
  ])!;
  assert.equal(merged.name, "daily_orders");
  assert.equal(merged.parentId, "new-folder");
  assert.equal(merged.favorite, true);
  assert.equal(merged.owner, "analyst");
  assert.equal(merged.content, current.content);
  assert.equal(merged.config, current.config);
  assert.equal(merged.description, current.description);
  assert.equal(merged.version, 3);
  assert.equal(merged.updatedAt, saved.updatedAt);
  assert.equal(current.name, "orders");
});

test("metadata responses do not create a draft for a clean file", () => {
  assert.equal(
    mergeMetadataDraft(undefined, object({ favorite: true }), ["favorite"]),
    undefined,
  );
});

test("deleting a parent folder clears all missing descendant drafts and preserves unrelated edits", () => {
  const drafts = {
    parent: object({ id: "parent", kind: "FOLDER" }),
    child: object({
      id: "child",
      parentId: "parent",
      content: "-- child edit",
    }),
    grandchild: object({
      id: "grandchild",
      parentId: "child",
      content: "-- descendant edit",
    }),
    unrelated: object({
      id: "unrelated",
      parentId: null,
      content: "-- keep me",
    }),
  };
  const remaining = pruneDeletedDrafts(
    drafts,
    new Set(["unrelated", "other-clean-object"]),
  );
  assert.deepEqual(Object.keys(remaining), ["unrelated"]);
  assert.equal(remaining.unrelated, drafts.unrelated);
  assert.equal(Object.keys(drafts).length, 4);
  assert.equal(
    pruneDeletedDrafts(remaining, new Set(["unrelated"])),
    remaining,
  );
});
