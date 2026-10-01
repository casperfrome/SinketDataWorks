import { useRef, useState } from "react";
import { App, Button, Form, Input, Modal, Table, Tag } from "antd";
import { ArrowLeft, LayoutPanelLeft, Plus } from "lucide-react";
import { api } from "../api";
import type { Workspace, WorkspaceInput } from "../types";

interface Props {
  workspaces: Workspace[];
  currentId: string;
  onClose: () => void;
  onCreated: (workspace: Workspace) => void;
  onEnter: (id: string) => void;
}

export default function WorkspaceManager({ workspaces, currentId, onClose, onCreated, onEnter }: Props) {
  const { message } = App.useApp();
  const [creating, setCreating] = useState(false);
  const [saving, setSaving] = useState(false);
  const savingRef = useRef(false);
  const [search, setSearch] = useState("");
  const [createdId, setCreatedId] = useState("");
  const [form] = Form.useForm<WorkspaceInput>();
  const visible = workspaces.filter(w => `${w.name} ${w.code}`.toLowerCase().includes(search.toLowerCase()));
  const create = async (input: WorkspaceInput) => {
    if (savingRef.current) return;
    savingRef.current = true;
    setSaving(true);
    try {
      const workspace = await api.createWorkspace(input);
      onCreated(workspace);
      setCreatedId(workspace.id);
      setCreating(false);
      setSearch("");
      form.resetFields();
      message.success("工作空间已创建，可进入离线或实时数据开发添加任务和数据源");
    } catch (error) {
      message.error(error instanceof Error ? error.message : "创建工作空间失败");
    } finally {
      savingRef.current = false;
      setSaving(false);
    }
  };
  return (
    <Modal title={creating ? "创建工作空间" : "工作空间管理"} open onCancel={() => { if (!saving) onClose(); }}
      width={820} closable={!saving} maskClosable={!saving} keyboard={!saving}
      footer={creating ? <>
        <Button disabled={saving} onClick={() => setCreating(false)}>返回列表</Button>
        <Button type="primary" loading={saving} onClick={() => form.submit()}>创建工作空间</Button>
      </> : <Button onClick={onClose}>关闭</Button>}>
      {creating ? <div className="workspace-create">
        <Button type="text" icon={<ArrowLeft size={14} />} disabled={saving} onClick={() => setCreating(false)}>工作空间列表</Button>
        <p className="workspace-description">新空间从空白开始，独立管理开发文件、数据源与运行记录。</p>
        <Form form={form} layout="vertical" onFinish={create} disabled={saving} requiredMark={false}>
          <Form.Item name="code" label="工作空间名称" extra="唯一标识，创建后不可修改。3–64 位小写字母、数字或下划线，以字母开头。"
            rules={[{ required: true, message: "请输入工作空间名称" }, { pattern: /^[a-z][a-z0-9_]{2,63}$/, message: "请输入符合规则的工作空间名称" }]}>
            <Input placeholder="例如 finance_analysis" maxLength={64} autoFocus />
          </Form.Item>
          <Form.Item name="name" label="显示名" extra="用于空间列表和开发工作台，可使用中文。"
            rules={[{ required: true, whitespace: true, message: "请输入显示名" }, { max: 100, message: "显示名不能超过 100 个字符" }, { pattern: /^[^\u0000-\u001f\u007f-\u009f]+$/, message: "显示名不能含控制字符" }]}>
            <Input placeholder="例如 财务分析" maxLength={100} />
          </Form.Item>
          <Form.Item label="运行环境"><div className="workspace-environment"><Tag>本地</Tag><span>使用当前部署环境，创建后可在空间内配置数据源。</span></div></Form.Item>
        </Form>
      </div> : <>
        <p className="workspace-description">按项目组织数据开发。需要开展独立项目时，再创建工作空间。</p>
        <div className="workspace-manager-toolbar">
          <Input.Search aria-label="搜索工作空间" placeholder="搜索显示名或工作空间名称" allowClear value={search} onChange={event => setSearch(event.target.value)} />
          <Button type="primary" icon={<Plus size={14} />} onClick={() => setCreating(true)}>创建工作空间</Button>
        </div>
        <Table<Workspace> rowKey="id" size="small" dataSource={visible} pagination={visible.length > 8 ? { pageSize: 8 } : false}
          scroll={{ x: 600 }} rowClassName={workspace => workspace.id === createdId ? "workspace-created-row" : ""}
          locale={{ emptyText: "未找到工作空间，试试其他名称" }} columns={[
            { title: "工作空间", dataIndex: "name", render: (_, workspace) => <div className="workspace-list-name"><LayoutPanelLeft size={16} /><div><strong>{workspace.name}</strong><small>{workspace.code}</small></div></div> },
            { title: "类型", width: 110, render: (_, workspace) => workspace.type === "DEFAULT" ? <Tag>默认空间</Tag> : <Tag color="blue">自建空间</Tag> },
            { title: "环境", dataIndex: "region", width: 80 },
            { title: "操作", width: 150, render: (_, workspace) => workspace.id === currentId ? <Tag color="green">当前空间</Tag> : <Button type="link" onClick={() => onEnter(workspace.id)}>进入离线数据开发</Button> },
          ]} />
      </>}
    </Modal>
  );
}
