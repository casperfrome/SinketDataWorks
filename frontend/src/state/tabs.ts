export type TabCloseAction = "current" | "others" | "left" | "right" | "all" | "saved";
export const tabCloseLabels: Record<TabCloseAction,string> = {current:"关闭当前标签页",others:"关闭其他标签页",left:"关闭左侧标签页",right:"关闭右侧标签页",all:"关闭全部标签页",saved:"关闭已保存的标签页"};
export function tabsToClose(tabs: readonly string[], anchor: string, action: TabCloseAction, dirty: ReadonlySet<string>): string[] {
  const index=tabs.indexOf(anchor);
  if(action==="all")return [...tabs];
  if(action==="saved")return tabs.filter(id=>!dirty.has(id));
  if(index<0)return [];
  if(action==="current")return [anchor];
  if(action==="others")return tabs.filter(id=>id!==anchor);
  return action==="left"?tabs.slice(0,index):tabs.slice(index+1);
}
export function afterClosingTabs(tabs: readonly string[], active: string, closing: readonly string[]) {
  const removed=new Set(closing), remaining=tabs.filter(id=>!removed.has(id));
  if(!removed.has(active)&&remaining.includes(active))return {tabs:remaining,activeId:active};
  const index=tabs.indexOf(active);
  return {tabs:remaining,activeId:tabs.slice(index+1).find(id=>!removed.has(id))||tabs.slice(0,Math.max(index,0)).reverse().find(id=>!removed.has(id))||remaining[0]||""};
}
export function restoreEditorTabs(saved: {openTabs?: unknown;activeId?: unknown}, available: ReadonlySet<string>, fallback: string, requested?: string|null) {
  const tabs=Array.isArray(saved.openTabs)?[...new Set(saved.openTabs.filter((id):id is string=>typeof id==="string"&&available.has(id)))]:[];
  if(requested&&available.has(requested))return {tabs:tabs.includes(requested)?tabs:[...tabs,requested],activeId:requested};
  if(Array.isArray(saved.openTabs)){
    const active=typeof saved.activeId==="string"&&tabs.includes(saved.activeId)?saved.activeId:tabs[0]||"";
    return {tabs,activeId:active};
  }
  return {tabs:fallback?[fallback]:[],activeId:fallback};
}
