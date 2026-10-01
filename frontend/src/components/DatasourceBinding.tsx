import { useEffect, useState } from "react";
import { Button, Select } from "antd";
import type { DataSource, StudioObject } from "../types";
import { api } from "../api";
import { sqlProvider } from "../state/workflows";
import { schemaLabel } from "../state/sync";

export default function DatasourceBinding({ object, onChange, onManage }: { object: StudioObject; onChange: (patch: Partial<StudioObject>) => void; onManage: () => void }) {
  const [sources, setSources] = useState<DataSource[]>([]), [error, setError] = useState("");
  const [revision, setRevision] = useState(0);
  useEffect(() => {
    let alive = true; setError("");
    void api.datasources(object.workspaceId).then(data => { if (alive) setSources(data.filter((s): s is DataSource => s.type !== "KAFKA")); }).catch(e => { if (alive) setError(e.message); });
    return () => { alive = false; };
  }, [object.workspaceId, revision]);
  const provider = sqlProvider(object.nodeType);
  return <div className="datasource-binding"><span>数据库 Schema</span><Select size="small" aria-label="节点数据源" showSearch optionFilterProp="label" value={object.config.run?.dataSourceId || undefined} placeholder={error || `选择 ${object.nodeType} Schema`} options={sources.filter(source => (source.type || "MYSQL") === provider).map(source => ({ value: source.id, label: schemaLabel(source) }))} onOpenChange={open => { if (open) setRevision(n => n + 1); }} onChange={dataSourceId => onChange({ config: { ...object.config, run: { ...object.config.run, provider, dataSourceId, executionMode: provider === "DORIS" ? "QUERY" : object.config.run?.executionMode || "QUERY", timeoutSeconds: object.config.run?.timeoutSeconds || 30 } } })} /><Button size="small" type="link" onClick={onManage}>管理数据源</Button></div>;
}
