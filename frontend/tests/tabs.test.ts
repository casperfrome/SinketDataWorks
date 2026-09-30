import assert from "node:assert/strict";
import test from "node:test";
import {tabsToClose,afterClosingTabs,restoreEditorTabs} from "../src/state/tabs.ts";
const tabs=["a","b","c","d"],dirty=new Set(["b"]);
test("close operations are anchored to the clicked tab, including inactive tabs",()=>{
  assert.deepEqual(tabsToClose(tabs,"b","others",dirty),["a","c","d"]);
  assert.deepEqual(tabsToClose(tabs,"b","left",dirty),["a"]);
  assert.deepEqual(tabsToClose(tabs,"b","right",dirty),["c","d"]);
  assert.deepEqual(tabsToClose(tabs,"b","current",dirty),["b"]);
  assert.deepEqual(tabsToClose(tabs,"b","all",dirty),tabs);
  assert.deepEqual(tabsToClose(tabs,"b","saved",dirty),["a","c","d"]);
});
test("boundary menus have no targets and stale anchors are safe",()=>{
  assert.deepEqual(tabsToClose(tabs,"a","left",dirty),[]);
  assert.deepEqual(tabsToClose(tabs,"d","right",dirty),[]);
  assert.deepEqual(tabsToClose(tabs,"missing","others",dirty),[]);
});
test("keep active tab or select nearest surviving right, then left",()=>{
  assert.equal(afterClosingTabs(tabs,"b",["c","d"]).activeId,"b");
  assert.equal(afterClosingTabs(tabs,"b",["a","b"]).activeId,"c");
  assert.equal(afterClosingTabs(tabs,"d",["c","d"]).activeId,"b");
  assert.deepEqual(afterClosingTabs(tabs,"b",tabs),{tabs:[],activeId:""});
  assert.deepEqual(tabs,["a","b","c","d"]);
});
test("refresh preserves all-closed state, order and a valid selection",()=>{
  const ids=new Set(tabs);
  assert.deepEqual(restoreEditorTabs({openTabs:[],activeId:""},ids,"a"),{tabs:[],activeId:""});
  assert.deepEqual(restoreEditorTabs({openTabs:["c","b"],activeId:"b"},ids,"a"),{tabs:["c","b"],activeId:"b"});
  assert.deepEqual(restoreEditorTabs({openTabs:["c","b"],activeId:"a"},ids,"a"),{tabs:["c","b"],activeId:"c"});
});
test("first visit gets a default, deleted tabs are pruned and URL opens explicitly",()=>{
  const ids=new Set(tabs);
  assert.deepEqual(restoreEditorTabs({},ids,"a"),{tabs:["a"],activeId:"a"});
  assert.deepEqual(restoreEditorTabs({openTabs:["missing","b","b"]},ids,"a"),{tabs:["b"],activeId:"b"});
  assert.deepEqual(restoreEditorTabs({openTabs:[],activeId:""},ids,"a","d"),{tabs:["d"],activeId:"d"});
});
