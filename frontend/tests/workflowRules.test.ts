import assert from "node:assert/strict";
import test from "node:test";
import {
  connectionProblem,
  removeGraphElements,
} from "../src/data/workflowRules.ts";
import type { WorkflowGraph } from "../src/types.ts";

function graph(ids: string[], edges: [string, string][]): WorkflowGraph {
  return {
    nodes: ids.map((id, index) => ({
      id,
      label: id,
      nodeType: "MaxCompute SQL",
      x: index * 100,
      y: 0,
    })),
    edges: edges.map(([source, target], index) => ({
      id: `e${index}`,
      source,
      target,
    })),
  };
}

test("allows converging dependencies and an independent branch", () => {
  const workflow = graph(
    ["extractA", "extractB", "clean", "report", "audit"],
    [
      ["extractA", "clean"],
      ["extractB", "clean"],
      ["clean", "report"],
    ],
  );
  assert.equal(connectionProblem(workflow, "extractA", "report"), null);
  assert.equal(connectionProblem(workflow, "report", "audit"), null);
});

test("rejects a downstream dependency that closes a multi-step cycle", () => {
  const workflow = graph(
    ["source", "transform", "aggregate", "report"],
    [
      ["source", "transform"],
      ["transform", "aggregate"],
      ["aggregate", "report"],
    ],
  );
  assert.match(
    connectionProblem(workflow, "report", "source") ?? "",
    /循环依赖/,
  );
  assert.match(
    connectionProblem(workflow, "aggregate", "transform") ?? "",
    /循环依赖/,
  );
});

test("rejects self links, duplicate dependencies, and missing targets", () => {
  const workflow = graph(["source", "target"], [["source", "target"]]);
  assert.match(connectionProblem(workflow, "source", "source") ?? "", /自身/);
  assert.match(connectionProblem(workflow, "source", "target") ?? "", /已存在/);
  assert.match(
    connectionProblem(workflow, "source", "deleted") ?? "",
    /不存在/,
  );
});

test("handles a long dependency chain without overflowing the call stack", () => {
  const ids = Array.from({ length: 12000 }, (_, index) => String(index));
  const workflow = graph(
    ids,
    ids.slice(1).map((id, index) => [ids[index], id]),
  );
  assert.match(connectionProblem(workflow, "11999", "0") ?? "", /循环依赖/);
});

test("deleting a node removes incoming and outgoing edges while preserving unrelated work", () => {
  const workflow = graph(
    ["source", "transform", "report", "audit"],
    [
      ["source", "transform"],
      ["transform", "report"],
      ["source", "audit"],
    ],
  );
  const result = removeGraphElements(workflow, ["transform"]);
  assert.deepEqual(
    result.nodes.map((node) => node.id),
    ["source", "report", "audit"],
  );
  assert.deepEqual(
    result.edges.map((edge) => [edge.source, edge.target]),
    [["source", "audit"]],
  );
  assert.equal(workflow.nodes.length, 4);
  assert.equal(workflow.edges.length, 3);
});

test("consecutive node-removal and edge-removal notifications cannot restore a deleted node", () => {
  const workflow = graph(
    ["source", "transform", "report"],
    [
      ["source", "transform"],
      ["transform", "report"],
    ],
  );
  const afterNodes = removeGraphElements(workflow, ["transform"]);
  const afterEdges = removeGraphElements(afterNodes, [], ["e0", "e1"]);
  assert.deepEqual(afterEdges, afterNodes);
});
