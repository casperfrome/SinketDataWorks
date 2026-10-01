import { useEffect, useState } from "react";
import { App, Button, Input, Modal, Space, Table, Tag } from "antd";
import { RefreshCw, Trash2 } from "lucide-react";
import { api } from "../api";
import type { StudioObject } from "../types";
import "../panels.css";

export default function RecycleView({ workspaceId, onRestored }: { workspaceId: string; onRestored: () => Promise<void> }) {
  const { message } = App.useApp();
  const [items, setItems] = useState<StudioObject[]>([]);
  const [loading, setLoading] = useState(false), [error, setError] = useState("");
  const [revision, setRevision] = useState(0), [search, setSearch] = useState("");
  const [target, setTarget] = useState<StudioObject | null>(null), [name, setName] = useState("");
  const [restoring, setRestoring] = useState(false);
  useEffect(() => {
    let alive = true; setLoading(true); setError(""); setItems([]); setTarget(null);
    void api.objects(workspaceId, true).then(data => { if (alive) setItems(data); }).catch(e => { if (alive) setError(e.message); }).finally(() => { if (alive) setLoading(false); });
    return () => { alive = false; };
  }, [workspaceId, revision]);
  const restore = async () => {
    if (!target || !name.trim()) return;
    setRestoring(true);
    try { await api.restore(target.id, name.trim()); await onRestored(); setTarget(null); setRevision(n => n + 1); message.success("已恢复到离线数据开发"); }
    catch (e) { message.error((e as Error).message); }
    finally { setRestoring(false); }
  };
  return <section className="management-view recycle-view">
    <div className="management-page-header"><div className="management-title"><Trash2 size={18} /><h2>回收站</h2><Tag>{items.length}</Tag></div><Space><Input.Search allowClear placeholder="搜索已删除文件" value={search} onChange={e => setSearch(e.target.value)} /><Button icon={<RefreshCw size={14} />} onClick={() => setRevision(n => n + 1)}>刷新</Button></Space></div>
    {error && <p className="parameter-error" role="alert">{error}</p>}
    <Table size="small" loading={loading} rowKey="id" pagination={{ pageSize: 15 }} dataSource={items.filter(item => item.name.toLowerCase().includes(search.toLowerCase()))} columns={[
      { title: "名称", dataIndex: "name" }, { title: "类型", dataIndex: "nodeType" },
      { title: "原目录", render: (_, item) => items.find(parent => parent.id === item.parentId)?.name || (item.parentId ? "原目录" : "项目根目录") },
      { title: "删除时间", dataIndex: "updatedAt", render: value => new Date(value).toLocaleString("zh-CN", { hour12: false }) },
      { title: "操作", render: (_, item) => <Button type="link" disabled={items.some(parent => parent.id === item.parentId)} title={items.some(parent => parent.id === item.parentId) ? "请先恢复父目录" : undefined} onClick={() => { setTarget(item); setName(item.name); }}>恢复</Button> },
    ]} />
    <Modal title="恢复文件或目录" open={!!target} confirmLoading={restoring} okText="恢复" onOk={() => void restore()} onCancel={() => setTarget(null)} okButtonProps={{ disabled: !name.trim() }}>
      <p>目录恢复时会一并恢复本次删除的下属文件。若名称冲突，可修改后恢复。</p><Input aria-label="恢复名称" value={name} onChange={e => setName(e.target.value)} maxLength={128} />
    </Modal>
  </section>;
}
