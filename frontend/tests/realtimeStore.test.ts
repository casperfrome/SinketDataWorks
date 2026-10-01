import assert from "node:assert/strict";
import test from "node:test";
import { createTask, createRelease, createBinding } from "../src/realtime/model.ts";
import { acknowledgeTask, emptyState, getRealtimeState, legacyImportId, legacyStorageKey, localState, mergeRemoteState, parseLegacyState, refreshRealtimeState, settleSavedDraft, storageKey, updateRealtimeState } from "../src/realtime/store.ts";

const stored = new Map<string,string>();
let failWrites=false;
Object.defineProperty(globalThis,"localStorage",{configurable:true,value:{getItem:(key:string)=>stored.get(key)||null,setItem:(key:string,value:string)=>{if(failWrites)throw new Error("QuotaExceededError");stored.set(key,value);}}});
const originalFetch=globalThis.fetch;
test.afterEach(()=>{globalThis.fetch=originalFetch;failWrites=false;});
const response=(value:unknown,status=200)=>new Response(JSON.stringify(value),{status,headers:{"Content-Type":"application/json"}});

test("browser persistence contains drafts and UI, never authoritative tasks, releases, jobs or Kafka credentials",()=>{
  const task={...createTask("boundary","task"),revision:1};
  const state={...emptyState(),tasks:[task],releases:[createRelease(task,[])],drafts:{[task.id]:task},openTabs:[task.id],activeId:task.id};
  const saved=localState(state);
  assert.equal(saved.schemaVersion,2);assert.equal(saved.drafts[task.id].name,"task");
  for(const key of ["tasks","releases","jobs","kafkaSources"])assert.equal(Object.hasOwn(saved,key),false);
});
test("an async save only acknowledges its submitted draft and preserves newer editor changes with updated revision",()=>{
  const submitted={...createTask("ack","task"),revision:3,sql:"SELECT 1"},saved={...submitted,revision:4,updatedAt:"server-time"};
  assert.equal(settleSavedDraft(structuredClone(submitted),submitted,saved),undefined);
  const newer={...submitted,sql:"SELECT 2"};
  assert.deepEqual(settleSavedDraft(newer,submitted,saved),{...newer,revision:4,updatedAt:"server-time"});
  updateRealtimeState("ack",state=>({...state,tasks:[submitted],drafts:{[submitted.id]:newer}}));
  acknowledgeTask("ack",submitted,saved);
  assert.equal(getRealtimeState("ack").drafts[submitted.id].sql,"SELECT 2");
  assert.equal(getRealtimeState("ack").tasks[0].revision,4);
});
test("workspace drafts remain isolated and failed local writes retain memory edits",()=>{
  const a=createTask("isolate-a","A"),b=createTask("isolate-b","B");
  updateRealtimeState("isolate-a",state=>({...state,drafts:{[a.id]:a}}));
  updateRealtimeState("isolate-b",state=>({...state,drafts:{[b.id]:b}}));
  failWrites=true;
  assert.equal(updateRealtimeState("isolate-a",state=>({...state,drafts:{[a.id]:{...a,sql:"SELECT 8"}}})),false);
  assert.equal(getRealtimeState("isolate-a").drafts[a.id].sql,"SELECT 8");
  assert.equal(getRealtimeState("isolate-b").drafts[b.id].name,"B");
});
test("legacy imports use a stable content identifier and reject malformed or cross-workspace records",()=>{
  const task=createTask("legacy","task"),state={...emptyState(),tasks:[task]};
  const raw=JSON.stringify(state);
  assert.equal(legacyImportId("legacy",raw),legacyImportId("legacy",raw));
  assert.notEqual(legacyImportId("legacy",raw),legacyImportId("legacy",raw+" "));
  assert.equal(parseLegacyState(raw,"legacy").tasks[0].id,task.id);
  assert.throws(()=>parseLegacyState(raw,"other"),/跨工作空间/);
  assert.throws(()=>parseLegacyState('{"schemaVersion":3}',"legacy"),/结构或版本/);
});
test("legacy migration keeps its v1 backup, imports once, restores drafts and never displays mock runs as real jobs",async()=>{
  const wid="migrate",task=createTask(wid,"legacy task"),draft={...task,sql:"SELECT 42"};
  const legacy={...emptyState(),tasks:[task],drafts:{[task.id]:draft},openTabs:[task.id],activeId:task.id,jobs:[{id:"mock-job",taskId:task.id,status:"RUNNING"}]};
  const raw=JSON.stringify(legacy);stored.set(legacyStorageKey(wid),raw);
  let imports=0;
  globalThis.fetch=async(url,options)=>{if(String(url).endsWith("/import")){imports++;assert.equal(JSON.parse(String(options?.body)).importId,legacyImportId(wid,raw));return response({});}return response({...emptyState(),tasks:[{...task,revision:1}],drafts:{}});};
  await refreshRealtimeState(wid);await refreshRealtimeState(wid);
  assert.equal(imports,1);assert.equal(stored.get(legacyStorageKey(wid)),raw);
  assert.equal(getRealtimeState(wid).jobs.length,0);assert.equal(getRealtimeState(wid).drafts[task.id].sql,"SELECT 42");
  assert.equal(getRealtimeState(wid).drafts[task.id].revision,1);
  assert.equal(Object.hasOwn(JSON.parse(stored.get(storageKey(wid))!),"jobs"),false);
});
test("failed migration records no receipt, preserves the backup and retries with the same import ID",async()=>{
  const wid="retry-import",task=createTask(wid,"task"),raw=JSON.stringify({...emptyState(),tasks:[task]});stored.set(legacyStorageKey(wid),raw);
  const ids:string[]=[];let failed=true;
  globalThis.fetch=async(url,options)=>{if(String(url).endsWith("/import")){ids.push(JSON.parse(String(options?.body)).importId);return failed?response({code:"UNAVAILABLE",message:"server down"},503):response({});}return response({...emptyState(),tasks:[{...task,revision:1}]});};
  await assert.rejects(refreshRealtimeState(wid),/server down/);assert.equal(stored.has(storageKey(wid)+":import"),false);
  failed=false;await refreshRealtimeState(wid);assert.equal(ids[0],ids[1]);assert.equal(stored.get(legacyStorageKey(wid)),raw);
});
test("migration keeps remapped Kafka references in imported drafts and concurrent local edits",async()=>{
  const wid="import-remap",binding={...createBinding("SOURCE","KAFKA"),datasourceId:"old-kafka"},task={...createTask(wid,"task"),bindings:[binding]},draft={...task,sql:"-- 数据源引用：old-kafka\nSELECT 42"};
  stored.set(legacyStorageKey(wid),JSON.stringify({...emptyState(),tasks:[task],drafts:{[task.id]:draft}}));
  const remappedTask={...task,revision:1,bindings:[{...binding,datasourceId:"server-kafka"}]};
  const remappedDraft={...draft,revision:1,bindings:remappedTask.bindings,sql:"-- 数据源引用：server-kafka\nSELECT 42"};
  globalThis.fetch=async(url)=>{if(String(url).endsWith("/import")){updateRealtimeState(wid,state=>({...state,drafts:{[task.id]:{...draft,sql:"-- 数据源引用：old-kafka\nSELECT 43"}}}));return response({idMap:{"old-kafka":"server-kafka"},drafts:{[task.id]:remappedDraft}});}return response({...emptyState(),tasks:[remappedTask],drafts:{[task.id]:remappedDraft}});};
  await refreshRealtimeState(wid);
  assert.equal(getRealtimeState(wid).drafts[task.id].bindings[0].datasourceId,"server-kafka");
  assert.equal(getRealtimeState(wid).drafts[task.id].sql,"-- 数据源引用：server-kafka\nSELECT 43");
});
test("server refresh merges local edits made while the request is pending and restores server drafts",async()=>{
  const wid="refresh-race",task={...createTask(wid,"task"),revision:1},other={...createTask(wid,"other"),revision:1};
  let resolveFetch!:(value:Response)=>void;
  globalThis.fetch=()=>new Promise(resolve=>{resolveFetch=resolve;});
  const pending=refreshRealtimeState(wid);
  updateRealtimeState(wid,state=>({...state,drafts:{[task.id]:{...task,sql:"SELECT local"}}}));
  resolveFetch(response({...emptyState(),tasks:[task,other],drafts:{[task.id]:{...task,sql:"SELECT remote"},[other.id]:{...other,sql:"SELECT shared"}}}));
  await pending;
  assert.equal(getRealtimeState(wid).drafts[task.id].sql,"SELECT local");assert.equal(getRealtimeState(wid).drafts[other.id].sql,"SELECT shared");
});
test("damaged v2 storage is retained even when server state loads successfully",async()=>{
  const wid="corrupt-v2",raw="{damaged";stored.set(storageKey(wid),raw);
  globalThis.fetch=async()=>response({...emptyState(),tasks:[{...createTask(wid,"server task"),revision:1}]});
  await refreshRealtimeState(wid);
  assert.equal(getRealtimeState(wid).tasks.length,1);assert.equal(stored.get(storageKey(wid)),raw);
});
test("a stale refresh cannot regress a completed save, resurrect its acknowledged draft or drop a just-created task",()=>{
  const task={...createTask("race-save","task"),revision:1},draft={...task,sql:"SELECT saved"};
  const requested={...emptyState(),tasks:[task],drafts:{[task.id]:draft}};
  const created={...createTask("race-save","created during refresh"),revision:1};
  const current={...requested,tasks:[{...draft,revision:2},created],drafts:{}};
  const merged=mergeRemoteState(current,requested,requested);
  assert.equal(merged.tasks.find(item=>item.id===task.id)?.revision,2);assert.equal(merged.drafts[task.id],undefined);assert.ok(merged.tasks.some(item=>item.id===created.id));
});
