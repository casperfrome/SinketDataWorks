import { test } from "node:test";
import assert from "node:assert/strict";
import { cycleCron, cronFields, defaultSchedule, yesterday } from "../src/state/schedules.ts";
const base={cycle:"DAY",interval:1,time:"02:30",weekdays:["MON","FRI"],monthdays:[1,15,31]};
test("schedule defaults are paused, daily Shanghai, previous business day",()=>{
  assert.equal(defaultSchedule.enabled,false);assert.equal(defaultSchedule.cron,"0 0 2 * * *");assert.equal(defaultSchedule.timezone,"Asia/Shanghai");assert.equal(defaultSchedule.businessDateOffset,-1);assert.equal(defaultSchedule.retries,0);
});
test("form cycles produce the six-field server contract",()=>{
  assert.equal(cycleCron(base),"0 30 2 * * *");assert.equal(cycleCron({...base,cycle:"MINUTE",interval:5}),"0 */5 * * * *");assert.equal(cycleCron({...base,cycle:"HOUR",interval:2}),"0 30 */2 * * *");assert.equal(cycleCron({...base,cycle:"WEEK"}),"0 30 2 * * MON,FRI");assert.equal(cycleCron({...base,cycle:"MONTH"}),"0 30 2 1,15,31 * *");
});
test("rejects empty selections, invalid intervals and times",()=>{
  for(const patch of [{cycle:"WEEK",weekdays:[]},{cycle:"MONTH",monthdays:[]},{cycle:"MONTH",monthdays:[32]},{cycle:"MINUTE",interval:0},{cycle:"HOUR",interval:24},{time:"25:00"},{time:""}])assert.throws(()=>cycleCron({...base,...patch}));
});
test("reopening a saved form retains its schedule instead of resetting defaults",()=>{
  for(const fields of [base,{...base,cycle:"MINUTE",interval:7},{...base,cycle:"HOUR",interval:3},{...base,cycle:"WEEK"},{...base,cycle:"MONTH"}])assert.equal(cycleCron(cronFields(cycleCron(fields))),cycleCron(fields));
  assert.equal(cronFields("0 0 2 L * *").cycle,"CUSTOM");assert.equal(cronFields("0 * * * * *").cycle,"MINUTE");
});
test("business-date default is a valid yesterday in Shanghai",()=>{
  const date=yesterday();assert.match(date,/^\d{4}-\d{2}-\d{2}$/);const delta=Date.now()-Date.parse(`${date}T00:00:00+08:00`);assert.ok(delta>=86400000&&delta<172800000);
});
