import "../run-parameters.css";

export interface RunParametersProps {
  parameters?: Record<string, string>;
  title?: string;
}

export default function RunParameters({ parameters, title = "本次运行参数" }: RunParametersProps) {
  const rows = Object.entries(parameters || {});
  if (!rows.length) return null;
  return <section className="run-actual-parameters" aria-label={title}>
    <h4>{title}</h4>
    <dl>{rows.map(([name, value]) => <div key={name}><dt><code>{name}</code></dt><dd><code>{value}</code></dd></div>)}</dl>
  </section>;
}
