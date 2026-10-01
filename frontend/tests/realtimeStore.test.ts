import assert from "node:assert/strict";
import test from "node:test";
import { createBinding, createJob, createRelease, createTask } from "../src/realtime/model.ts";
import { emptyState, getKafkaPassword, getRealtimeState, kafkaReferences, setKafkaPassword, storageKey, updateRealtimeState } from "../src/realtime/store.ts";
import type { KafkaDatasource } from "../src/realtime/types.ts";

const stored = new Map<string, string>();
let failWrites = false;
Object.defineProperty(globalThis, "localStorage", { configurable: true, value: { getItem: (key: string) => stored.get(key) || null, setItem: (key: string, value: string) => { if (failWrites) throw new Error("QuotaExceededError"); stored.set(key, value); } } });

test("workspace storage and in-memory drafts remain isolated", () => {
  const first = createTask("store-a", "workspace A");
  const second = createTask("store-b", "workspace B");
  assert.equal(updateRealtimeState("store-a", state => ({ ...state, tasks: [first], drafts: { [first.id]: { ...first, sql: "unsaved A" } } })), true);
  assert.equal(updateRealtimeState("store-b", state => ({ ...state, tasks: [second] })), true);
  assert.equal(getRealtimeState("store-a").drafts[first.id].sql, "unsaved A");
  assert.equal(getRealtimeState("store-b").tasks[0].name, "workspace B");
  assert.equal(getRealtimeState("store-b").drafts[first.id], undefined);
  assert.notEqual(storageKey("a/b"), storageKey("a%2Fb"));
});

test("failed browser writes keep latest edits in memory and a later retry can save", () => {
  const task = createTask("store-quota", "订单草稿");
  updateRealtimeState("store-quota", state => ({ ...state, tasks: [task] }));
  const before = stored.get(storageKey("store-quota"));
  failWrites = true;
  try {
    assert.equal(updateRealtimeState("store-quota", state => ({ ...state, drafts: { [task.id]: { ...task, sql: "SELECT latest_edit;" } } })), false);
    assert.equal(getRealtimeState("store-quota").drafts[task.id].sql, "SELECT latest_edit;");
    assert.equal(stored.get(storageKey("store-quota")), before);
  } finally { failWrites = false; }
  assert.equal(updateRealtimeState("store-quota", state => state), true);
  assert.ok(stored.get(storageKey("store-quota"))!.includes("latest_edit"));
});

test("corrupt JSON is retained rather than silently replaced by an empty configuration", () => {
  const key = storageKey("store-corrupt");
  stored.set(key, "{corrupt-original");
  assert.deepEqual(getRealtimeState("store-corrupt"), emptyState());
  const task = createTask("store-corrupt", "safe memory draft");
  assert.equal(updateRealtimeState("store-corrupt", state => ({ ...state, tasks: [task] })), false);
  assert.equal(getRealtimeState("store-corrupt").tasks[0].name, "safe memory draft");
  assert.equal(stored.get(key), "{corrupt-original");
});

test("malformed nested records and cross-workspace records are rejected before rendering", () => {
  const malformed = { ...emptyState(), tasks: [{ id: "bad-record" }] };
  const wrongWorkspace = { ...emptyState(), tasks: [createTask("some-other-workspace", "foreign")] };
  stored.set(storageKey("store-malformed"), JSON.stringify(malformed));
  stored.set(storageKey("store-wrong-workspace"), JSON.stringify(wrongWorkspace));
  assert.deepEqual(getRealtimeState("store-malformed"), emptyState());
  assert.deepEqual(getRealtimeState("store-wrong-workspace"), emptyState());
});

test("valid stored tasks are restored including unsaved drafts and open-tab state", () => {
  const task = createTask("store-restore", "restored task");
  const state = { ...emptyState(), tasks: [task], drafts: { [task.id]: { ...task, sql: "uncommitted SQL" } }, openTabs: [task.id], activeId: task.id };
  stored.set(storageKey("store-restore"), JSON.stringify(state));
  assert.deepEqual(getRealtimeState("store-restore"), state);
});

test("Kafka credentials remain session-only and accidental password properties are discarded", () => {
  const source: KafkaDatasource = { id: "kafka-session", workspaceId: "store-password", type: "KAFKA", name: "Kafka", bootstrapServers: "broker:9092", securityProtocol: "SASL_SSL", saslMechanism: "PLAIN", username: "reader" };
  setKafkaPassword(source.id, "never-persist-this-secret");
  assert.equal(getKafkaPassword(source.id), "never-persist-this-secret");
  updateRealtimeState("store-password", state => ({ ...state, kafkaSources: [{ ...source, password: "also-do-not-persist" } as KafkaDatasource] }));
  const raw = stored.get(storageKey("store-password"))!;
  assert.doesNotMatch(raw, /never-persist|also-do-not-persist|"password"/);
  assert.equal("password" in getRealtimeState("store-password").kafkaSources[0], false);
  setKafkaPassword(source.id, "");
  assert.equal(getKafkaPassword(source.id), "");
});

test("references include saved tasks, unsaved bindings and immutable release snapshots", () => {
  const task = createTask("store-references", "saved task");
  task.bindings = [{ ...createBinding("SOURCE"), datasourceId: "kafka-referenced" }];
  const release = createRelease(task, []);
  task.bindings = [];
  const draft = { ...task, name: "draft task", bindings: [{ ...createBinding("SOURCE"), datasourceId: "kafka-referenced" }] };
  updateRealtimeState("store-references", state => ({ ...state, tasks: [task], drafts: { [task.id]: draft }, releases: [release] }));
  assert.deepEqual(kafkaReferences("store-references", "kafka-referenced"), ["draft task", "saved task · 发布版本 v1"]);
  assert.deepEqual(kafkaReferences("store-references", "unreferenced"), []);
});

test("failed atomic save preserves its dirty draft and retry acknowledges it once", () => {
  const workspace = "store-atomic-save";
  const task = createTask(workspace, "save task");
  const draft = { ...task, sql: "SELECT latest_draft;" };
  updateRealtimeState(workspace, state => ({ ...state, tasks: [task], drafts: { [task.id]: draft } }));
  const save = () => updateRealtimeState(workspace, state => {
    // Deliberately mutate the isolated commit input, exercising protection beyond pure updaters.
    state.tasks = state.tasks.map(item => item.id === task.id ? state.drafts[task.id] : item);
    delete state.drafts[task.id];
    return state;
  }, { requirePersist: true });
  failWrites = true;
  try {
    assert.equal(save(), false);
    assert.equal(getRealtimeState(workspace).drafts[task.id].sql, draft.sql);
    assert.equal(getRealtimeState(workspace).tasks[0].sql, task.sql);
  } finally { failWrites = false; }
  assert.equal(save(), true);
  assert.equal(getRealtimeState(workspace).drafts[task.id], undefined);
  assert.equal(getRealtimeState(workspace).tasks.length, 1);
  assert.equal(getRealtimeState(workspace).tasks[0].sql, draft.sql);
});

test("failed atomic publication does not clear a draft or allocate a duplicate version on retry", () => {
  const workspace = "store-atomic-release";
  const task = createTask(workspace, "publish task");
  const draft = { ...task, sql: "INSERT INTO sink SELECT * FROM source;" };
  updateRealtimeState(workspace, state => ({ ...state, tasks: [task], drafts: { [task.id]: draft } }));
  const publish = () => updateRealtimeState(workspace, state => {
    const snapshot = state.drafts[task.id];
    const release = createRelease(snapshot, state.releases);
    const { [task.id]: _acknowledged, ...drafts } = state.drafts;
    return { ...state, drafts, releases: [...state.releases, release], tasks: state.tasks.map(item => item.id === task.id ? snapshot : item) };
  }, { requirePersist: true });
  failWrites = true;
  try {
    assert.equal(publish(), false);
    assert.equal(publish(), false);
    assert.equal(getRealtimeState(workspace).releases.length, 0);
    assert.equal(getRealtimeState(workspace).drafts[task.id].sql, draft.sql);
  } finally { failWrites = false; }
  assert.equal(publish(), true);
  const saved = getRealtimeState(workspace);
  assert.equal(saved.releases.length, 1);
  assert.equal(saved.releases[0].releaseNo, 1);
  assert.equal(saved.releases[0].snapshot.sql, draft.sql);
  assert.equal(saved.drafts[task.id], undefined);
});

test("failed atomic start creates no job and retry starts precisely one recorded job", () => {
  const workspace = "store-atomic-start";
  const task = createTask(workspace, "start task");
  const release = createRelease(task, []);
  const draft = { ...task, sql: "-- independent unsaved edit" };
  updateRealtimeState(workspace, state => ({ ...state, tasks: [task], drafts: { [task.id]: draft }, releases: [release] }));
  const start = () => updateRealtimeState(workspace, state => {
    const job = createJob(release);
    return { ...state, jobs: [...state.jobs, job], selectedJobId: job.id };
  }, { requirePersist: true });
  failWrites = true;
  try {
    assert.equal(start(), false);
    assert.equal(start(), false);
    assert.equal(getRealtimeState(workspace).jobs.length, 0);
    assert.equal(getRealtimeState(workspace).selectedJobId, "");
    assert.equal(getRealtimeState(workspace).drafts[task.id].sql, draft.sql);
  } finally { failWrites = false; }
  assert.equal(start(), true);
  assert.equal(getRealtimeState(workspace).jobs.length, 1);
  assert.equal(getRealtimeState(workspace).jobs[0].releaseId, release.id);
  assert.equal(getRealtimeState(workspace).drafts[task.id].sql, draft.sql);
});

test("atomic commit cannot overwrite damaged storage or discard its existing memory draft", () => {
  const workspace = "store-corrupt-atomic";
  const key = storageKey(workspace);
  stored.set(key, "{damaged-state");
  const task = createTask(workspace, "retained draft");
  updateRealtimeState(workspace, state => ({ ...state, drafts: { [task.id]: task } }));
  const before = getRealtimeState(workspace);
  assert.equal(updateRealtimeState(workspace, () => emptyState(), { requirePersist: true }), false);
  assert.equal(getRealtimeState(workspace), before);
  assert.equal(getRealtimeState(workspace).drafts[task.id].name, task.name);
  assert.equal(stored.get(key), "{damaged-state");
});
