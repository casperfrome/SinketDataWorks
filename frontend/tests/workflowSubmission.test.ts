import test from "node:test";
import assert from "node:assert/strict";
import type { StudioObject } from "../src/types.ts";
import { captureWorkflowSubmission, saveWorkflowSubmission, runLabel, sourceLabel } from "../src/state/workflows.ts";
import { settleSavedDraft } from "../src/state/drafts.ts";

const node = (id: string): StudioObject => ({ id, workspaceId: "w", parentId: null, kind: "NODE", nodeType: "MySQL", name: id, content: "SELECT 1", description: "", config: { run: { provider: "MYSQL" } }, tags: [], favorite: false, deleted: false, version: 1, owner: "local", updatedAt: "now" });
const workflow = (ids: string[]): StudioObject => ({ ...node("workflow"), kind: "WORKFLOW", config: { run: { provider: "WORKFLOW" }, graph: { nodes: ids.map((objectId, i) => ({ id: `n${i}`, objectId, label: objectId })), edges: [] } } });

test("captures related drafts once per object and preserves edits made during saving", async () => {
  const n = node("n"), w = workflow(["n", "n"]), unrelated = node("unrelated");
  const submitted = { ...n, content: "SELECT 'clicked'" };
  const drafts: Record<string, StudioObject> = { n: submitted, unrelated };
  const captured = captureWorkflowSubmission(w.id, [w, n, unrelated], drafts);
  const later = { ...submitted, content: "SELECT 'later'" }; drafts.n = later;
  const savedIds: string[] = [];
  const result = await saveWorkflowSubmission(captured, async (object, dirty) => {
    if (!dirty) return object;
    savedIds.push(object.id); assert.equal(object.content, "SELECT 'clicked'");
    const saved = { ...object, version: 2 };
    drafts.n = settleSavedDraft(drafts.n, object, saved)!;
    return saved;
  });
  assert.deepEqual(savedIds, ["n"]); assert.deepEqual(result.expectedNodeVersions, { n: 2 });
  assert.equal(drafts.n.content, "SELECT 'later'"); assert.equal(drafts.n.version, 2); assert.equal(drafts.unrelated, unrelated);
});

test("failed partial save prevents submission and retains unrelated state", async () => {
  const a = node("a"), b = node("b"), w = workflow(["a", "b"]);
  const captured = captureWorkflowSubmission(w.id, [w, a, b], { a, b });
  const saved: string[] = []; let submitted = false;
  await assert.rejects(async () => { await saveWorkflowSubmission(captured, async object => { saved.push(object.id); return object.id === "a" ? undefined : object; }); submitted = true; }, /未创建发布或运行/);
  assert.deepEqual(saved, ["workflow", "a"]); assert.equal(submitted, false);
});

test("preflight rejects missing, unsupported, deleted and cross-space references before saving", () => {
  const a = node("a"), w = workflow(["a"]);
  assert.throws(() => captureWorkflowSubmission(w.id, [w], {}), /未绑定/);
  for (const invalid of [{ ...a, deleted: true }, { ...a, workspaceId: "other" }, { ...a, config: {} }, { ...a, nodeType: "Python" }]) {
    assert.throws(() => captureWorkflowSubmission(w.id, [w, invalid], {}));
  }
  assert.throws(() => captureWorkflowSubmission(w.id, [{ ...w, config: { graph: w.config.graph } }, a], {}), /执行方式/);
});

test("release and development labels are distinct from provider labels", () => {
  assert.equal(runLabel({ provider: "WORKFLOW", simulation: false } as any), "真实工作流");
  assert.equal(sourceLabel({ executionSource: "RELEASE", releaseNo: 3 } as any), "已发布版本 R3");
  assert.equal(sourceLabel({ executionSource: "DEVELOPMENT" } as any), "开发调试");
});
