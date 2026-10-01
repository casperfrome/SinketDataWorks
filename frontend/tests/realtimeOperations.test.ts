import assert from "node:assert/strict";
import test from "node:test";
import { jobElapsedSeconds, latestRealtimeJob } from "../src/realtime/operationsModel.ts";
import type { RealtimeJob } from "../src/realtime/types.ts";
const job=(input:Partial<RealtimeJob>):RealtimeJob=>({id:"job",taskId:"task",releaseId:"release",status:"RUNNING",createdAt:"2026-10-01T08:00:00Z",startedAt:"2026-10-01T08:00:00Z",logs:[],savepoints:[],checkpointBase:0,...input});
test("operations select the latest attempt regardless of server ordering",()=>{
 const old=job({id:"old",status:"FINISHED"}),latest=job({id:"new",createdAt:"2026-10-01T08:01:00Z"});
 assert.equal(latestRealtimeJob([latest,old],"task")?.id,"new");assert.equal(latestRealtimeJob([old,latest],"task")?.id,"new");assert.equal(latestRealtimeJob([old],"other"),undefined);
});
test("terminal duration is fixed and unavailable metrics are not synthesized",()=>{
 const now=Date.parse("2026-10-01T08:05:00Z");
 assert.equal(jobElapsedSeconds(job({status:"FINISHED",durationMs:1200}),now),1);
 assert.equal(jobElapsedSeconds(job({status:"FINISHED"}),now),undefined);
 assert.equal(jobElapsedSeconds(job({status:"STOPPED",stoppedAt:"2026-10-01T08:00:12Z"}),now),12);
 assert.equal(jobElapsedSeconds(job({connectionMessage:"unreachable",metrics:{sampledAt:"2026-10-01T08:00:10Z"}}),now),10);
 assert.equal(jobElapsedSeconds(job({connectionMessage:"unreachable"}),now),undefined);
});
