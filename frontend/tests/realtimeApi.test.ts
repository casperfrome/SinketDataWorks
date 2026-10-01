import assert from "node:assert/strict";
import test from "node:test";
import { api, ApiError } from "../src/api.ts";
import { createTask } from "../src/realtime/model.ts";

const original=globalThis.fetch;test.afterEach(()=>{globalThis.fetch=original;});
const ok=(value:unknown,status=200)=>new Response(JSON.stringify(value),{status});
test("SQL validation and publication follow accepted operations and return their server results",async()=>{
  const calls:{url:string;body:any}[]=[];
  const task={...createTask("space /中文","task"),revision:5};
  globalThis.fetch=async(url,options)=>{const address=String(url);calls.push({url:address,body:options?.body?JSON.parse(String(options.body)):undefined});return address.includes("/operations/")?ok({id:"op",status:"SUCCESS",result:address.includes("workspaceId=")?{valid:true,errors:[]}:null}):ok({operationId:"op"},202);};
  assert.equal((await api.realtimeValidate(task.workspaceId,task)).valid,true);
  assert.equal(calls[0].body.task.id,task.id);assert.equal(calls[1].url,"/api/v1/realtime/operations/op?workspaceId=space+%2F%E4%B8%AD%E6%96%87");
  globalThis.fetch=async(url,options)=>String(url).includes("/operations/")?ok({id:"pub",status:"SUCCESS",result:{id:"release",snapshot:{...task,revision:6}}}):ok({operationId:"pub"},202);
  assert.equal((await api.realtimePublish(task.workspaceId,task,"note")).snapshot.revision,6);
});
test("operation failures preserve meaningful server codes",async()=>{
  globalThis.fetch=async(url)=>String(url).includes("/operations/")?ok({id:"op",status:"FAILED",errorCode:"DDL_CONFLICT",message:"managed table changed"}):ok({operationId:"op"},202);
  await assert.rejects(api.realtimePlan("workspace",createTask("workspace","task")),(error:unknown)=>error instanceof ApiError&&error.code==="DDL_CONFLICT"&&error.message==="managed table changed");
});
test("job controls send stable request IDs, upgrade target and optional recovery savepoint",async()=>{
  const calls:{url:string;body:any}[]=[];
  globalThis.fetch=async(url,options)=>{calls.push({url:String(url),body:JSON.parse(String(options?.body))});return ok({operationId:"op",jobId:"job"},202);};
  await api.realtimeStart("w","r1","start-key","savepoint-id");await api.realtimeControl("w","job","upgrade","upgrade-key","r2");await api.realtimeRollback("w","upgrade-op","rollback-key");
  assert.deepEqual(calls[0].body,{workspaceId:"w",releaseId:"r1",requestId:"start-key",savepointId:"savepoint-id"});
  assert.equal(calls[1].url,"/api/v1/realtime/jobs/job/upgrade");assert.equal(calls[1].body.targetReleaseId,"r2");assert.equal(calls[2].body.requestId,"rollback-key");
  await api.realtimeControl("w","job","restart","restart-stateful");await api.realtimeControl("w","job","restart","restart-fresh",undefined,true);
  assert.equal(Object.hasOwn(calls[3].body,"allowFreshStart"),false);assert.equal(calls[4].body.allowFreshStart,true);
});
test("preview carries the complete task context and retrieves saved result pages",async()=>{
  const calls:{url:string;body:any}[]=[];globalThis.fetch=async(url,options)=>{calls.push({url:String(url),body:options?.body?JSON.parse(String(options.body)):undefined});return ok({operationId:"op",previewId:"preview",rows:[{kind:"UPDATE_AFTER",fields:[7]}]});};
  const task=createTask("w","task");await api.realtimePreview("w",task,"SELECT * FROM source");await api.realtimePreviewResult("w","preview",1);await api.realtimeCancelPreview("w","preview");
  assert.equal(calls[0].body.task.id,task.id);assert.equal(calls[0].body.query,"SELECT * FROM source");assert.equal(calls[1].url,"/api/v1/realtime/previews/preview?workspaceId=w&token=1");assert.equal(calls[2].url,"/api/v1/realtime/previews/preview/cancel");
});
test("retrying publication after a lost response reuses its idempotency key",async()=>{
  const task={...createTask("retry-publish","task"),revision:1},ids:string[]=[];let lost=true;
  globalThis.fetch=async(url,options)=>{
    if(String(url).includes("/operations/"))return ok({id:"op",status:"SUCCESS",result:{id:"release",snapshot:task}});
    ids.push(JSON.parse(String(options?.body)).requestId);if(lost)throw new Error("lost response");return ok({operationId:"op"},202);
  };
  await assert.rejects(api.realtimePublish(task.workspaceId,task,"same note"));lost=false;
  await api.realtimePublish(task.workspaceId,task,"same note");assert.equal(ids[0],ids[1]);
});
