import { useEffect, useState } from "react";
import { Button, Select } from "antd";
import type { DataSource, StudioObject } from "../types";
import { api } from "../api";

export default function DatasourceBinding({ object, onChange, onManage }: { object: StudioObject; onChange: (patch: Partial<StudioObject>) => void; onManage: () => void }) {
  const [sources, setSources] = useState<DataSource[]>([]), [error, setError] = useState("");
  const [revision, setRevision] = useState(0);
  useEffect(() => {
    let alive = true; setError("");
    void api.datasources(object.workspaceId).then(data => { if (alive) setSources(data); }).catch(e => { if (alive) setError(e.message); });
    return () => { alive = false; };
  }, [object.workspaceId, revision]);
  return <div className="datasource-binding"><span>数据源</span><Select size="small" aria-label="节点数据源" showSearch optionFilterProp="label" value={object.config.run?.dataSourceId || undefined} placeholder={error || "选择 MySQL 连接"} options={sources.map(source => ({ value: source.id, label: `${source.name} · ${source.database}` }))} onOpenChange={open => { if (open) setRevision(n => n + 1); }} onChange={dataSourceId => onChange({ config: { ...object.config, run: { ...object.config.run, provider: "MYSQL", dataSourceId } } })} /><Button size="small" type="link" onClick={onManage}>管理数据源</Button></div>;
}
