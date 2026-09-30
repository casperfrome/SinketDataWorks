import { DatePicker } from "antd";
import dayjs from "dayjs";
import type { CSSProperties } from "react";

export default function BusinessDateInput({ value, onChange, label, min, size, style }: { value: string; onChange: (value: string) => void; label: string; min?: string; size?: "small" | "middle"; style?: CSSProperties }) {
  return <DatePicker aria-label={label} value={value ? dayjs(value) : null} onChange={date => onChange(date?.format("YYYY-MM-DD") || "")} format="YYYY-MM-DD" placeholder="选择日期" size={size} style={style} disabledDate={date => !!min && date.format("YYYY-MM-DD") < min} />;
}
