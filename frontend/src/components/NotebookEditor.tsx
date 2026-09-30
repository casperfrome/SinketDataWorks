import { useState } from "react";
import Editor from "@monaco-editor/react";
import { Button, Empty, Input, Select, Space, Tag, Tooltip } from "antd";
import {
  ArrowDownOutlined,
  ArrowUpOutlined,
  DeleteOutlined,
  FileMarkdownOutlined,
  PlusOutlined,
  CaretRightOutlined,
  ClearOutlined,
} from "@ant-design/icons";
import type { StudioObject } from "../types";

interface Cell {
  id: string;
  type: "code" | "markdown";
  source: string;
  output?: string;
  executionCount?: number;
}
interface Props {
  object: StudioObject;
  onChange: (patch: Partial<StudioObject>) => void;
  theme: "dark" | "light";
  fontSize: number;
}
export default function NotebookEditor({
  object,
  onChange,
  fontSize,
  theme,
}: Props) {
  const cells: Cell[] = object.config.cells ?? [];
  const [active, setActive] = useState<string>();
  const update = (next: Cell[]) => {
    if (JSON.stringify(next) !== JSON.stringify(cells))
      onChange({ config: { ...object.config, cells: next } });
  };
  const patch = (id: string, value: Partial<Cell>) =>
    update(cells.map((c) => (c.id === id ? { ...c, ...value } : c)));
  const add = (type: Cell["type"], after?: number) => {
    const next = [...cells],
      cell = { id: crypto.randomUUID(), type, source: "" };
    next.splice(after === undefined ? next.length : after + 1, 0, cell);
    update(next);
    setActive(cell.id);
  };
  const simulate = (cell: Cell) => ({
    ...cell,
    executionCount: (cell.executionCount ?? 0) + 1,
    output: `[本地模拟] 单元格执行完成\n代码共 ${cell.source.split("\n").length} 行。此结果用于演示，不会执行 Python 或访问外部服务。\n${new Date().toLocaleString("zh-CN")}`,
  });
  const move = (index: number, delta: number) => {
    const next = [...cells];
    [next[index], next[index + delta]] = [next[index + delta], next[index]];
    update(next);
  };
  return (
    <div className="notebook-editor">
      <div className="notebook-toolbar">
        <Space size={8}>
          <Button
            size="small"
            icon={<PlusOutlined />}
            onClick={() => add("code")}
          >
            代码单元格
          </Button>
          <Button
            size="small"
            icon={<FileMarkdownOutlined />}
            onClick={() => add("markdown")}
          >
            Markdown
          </Button>
          <Button
            size="small"
            icon={<CaretRightOutlined />}
            onClick={() =>
              update(cells.map((c) => (c.type === "code" ? simulate(c) : c)))
            }
          >
            运行全部
          </Button>
          <Button
            size="small"
            icon={<ClearOutlined />}
            onClick={() =>
              update(
                cells.map((c) => ({
                  ...c,
                  output: undefined,
                  executionCount: undefined,
                })),
              )
            }
          >
            清空输出
          </Button>
        </Space>
        <Space>
          <span className="muted">Python 3</span>
          <Tag color="blue">本地模拟</Tag>
        </Space>
      </div>
      <div className="notebook-cells">
        {cells.length === 0 ? (
          <Empty description="添加一个代码单元格，开始探索数据">
            <Button
              type="primary"
              icon={<PlusOutlined />}
              onClick={() => add("code")}
            >
              添加代码单元格
            </Button>
          </Empty>
        ) : (
          cells.map((cell, index) => (
            <div
              className={`notebook-cell ${active === cell.id ? "active" : ""}`}
              key={cell.id}
              onClick={() => setActive(cell.id)}
            >
              <div className="cell-gutter">
                <span>[{cell.executionCount ?? " "}]</span>
                {cell.type === "code" && (
                  <Tooltip title="本地模拟运行此单元格">
                    <Button
                      type="text"
                      size="small"
                      aria-label={`运行单元格 ${index + 1}`}
                      icon={<CaretRightOutlined />}
                      onClick={() => patch(cell.id, simulate(cell))}
                    />
                  </Tooltip>
                )}
              </div>
              <div className="cell-main">
                <div className="cell-tools">
                  <Select
                    size="small"
                    value={cell.type}
                    options={[
                      { value: "code", label: "Python 代码" },
                      { value: "markdown", label: "Markdown" },
                    ]}
                    onChange={(type) =>
                      patch(cell.id, {
                        type,
                        output: undefined,
                        executionCount: undefined,
                      })
                    }
                  />
                  <span className="cell-index">单元格 {index + 1}</span>
                  <Space size={1}>
                    <Tooltip title="上移">
                      <Button
                        size="small"
                        type="text"
                        disabled={index === 0}
                        icon={<ArrowUpOutlined />}
                        onClick={() => move(index, -1)}
                      />
                    </Tooltip>
                    <Tooltip title="下移">
                      <Button
                        size="small"
                        type="text"
                        disabled={index === cells.length - 1}
                        icon={<ArrowDownOutlined />}
                        onClick={() => move(index, 1)}
                      />
                    </Tooltip>
                    <Tooltip title="插入单元格">
                      <Button
                        size="small"
                        type="text"
                        icon={<PlusOutlined />}
                        onClick={() => add("code", index)}
                      />
                    </Tooltip>
                    <Tooltip title="删除单元格">
                      <Button
                        size="small"
                        type="text"
                        icon={<DeleteOutlined />}
                        onClick={() =>
                          update(cells.filter((c) => c.id !== cell.id))
                        }
                      />
                    </Tooltip>
                  </Space>
                </div>
                {cell.type === "markdown" ? (
                  <Input.TextArea
                    className="notebook-source"
                    autoSize={{ minRows: 2, maxRows: 30 }}
                    style={{ fontSize }}
                    value={cell.source}
                    placeholder={"# 标题\n在这里编写 Markdown 文本"}
                    onChange={(e) => patch(cell.id, { source: e.target.value })}
                  />
                ) : (
                  <div
                    className="notebook-code-cell"
                    onKeyDownCapture={(e) => {
                      if (e.key === "Enter" && e.shiftKey) {
                        e.preventDefault();
                        e.stopPropagation();
                        patch(cell.id, simulate(cell));
                      }
                    }}
                  >
                    <Editor
                      path={`${object.id}/cells/${cell.id}.py`}
                      height={Math.max(
                        104,
                        Math.min(
                          510,
                          (cell.source.split("\n").length + 2) *
                            Math.round(fontSize * 1.6),
                        ),
                      )}
                      language="python"
                      theme={theme === "dark" ? "dataworks-dark" : "vs"}
                      value={cell.source}
                      onChange={(value) => {
                        if ((value ?? "") !== cell.source)
                          patch(cell.id, { source: value ?? "" });
                      }}
                      options={{
                        fontSize,
                        fontFamily: 'Consolas, "Cascadia Code", monospace',
                        minimap: { enabled: false },
                        scrollBeyondLastLine: false,
                        automaticLayout: true,
                        lineNumbers: "on",
                        glyphMargin: false,
                        folding: false,
                        wordWrap: "on",
                        padding: { top: 12, bottom: 12 },
                        overviewRulerLanes: 0,
                        scrollbar: { vertical: "auto", horizontal: "auto" },
                      }}
                    />
                  </div>
                )}
                {cell.type === "markdown" && cell.source && (
                  <div className="markdown-preview">
                    {cell.source.split("\n").map((line, i) =>
                      /^#{1,3} /.test(line) ? (
                        <strong
                          key={i}
                          style={{
                            fontSize:
                              20 - (line.match(/^#+/)?.[0].length ?? 1) * 2,
                          }}
                        >
                          {line.replace(/^#+\s/, "")}
                        </strong>
                      ) : (
                        <p key={i}>{line || "\u00a0"}</p>
                      ),
                    )}
                  </div>
                )}
                {cell.output && (
                  <div className="cell-output">
                    <Tag color="blue">本地模拟</Tag>
                    <pre>{cell.output}</pre>
                  </div>
                )}
              </div>
            </div>
          ))
        )}
        {cells.length > 0 && (
          <Button
            className="add-cell-bottom"
            type="dashed"
            block
            icon={<PlusOutlined />}
            onClick={() => add("code")}
          >
            添加单元格
          </Button>
        )}
      </div>
      <div className="notebook-status">
        <span>{cells.length} 个单元格</span>
        <span>Shift + Enter 运行当前单元格（本地模拟）</span>
      </div>
    </div>
  );
}
