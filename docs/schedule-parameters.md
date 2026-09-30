# 调度参数与工作台精简

## 页面与升级

主导航只保留数据开发、数据源、回收站。右侧仅有调度配置和版本；数据源改在编辑器顶部选择。旧运行结果、发布快照、版本与文件恢复能力继续可用。

Flyway V6 是一次性迁移：全部空间的现有开发对象移入回收站，相关计划暂停，待执行触发取消，打开标签清空。版本、文件内容、工作空间和连接保留。已删除对象仍留在回收站。后续新建对象不会再次清理，服务不再生成示例文件。恢复目录会一起恢复该目录删除批次中的后代；已有同名项时可改名恢复。恢复文件不会自动重新启用调度。

## 配置与保存

调度参数初始为空；支持新增、清空值、删除。加载代码中的 `${参数名}` 会去重、保留已有赋值和未引用的配置，来源显示代码解析；新建行显示手动添加。

表达式模式以空格分隔 `name=value`。例如 `bizdate=$[yyyymmdd-1] region=杭州`。名称须以字母或下划线开头，仅包含字母、数字、下划线，最多 64 字符；不能重复。参数值不能缺失或含空白、等号。错误输入保留在草稿中，修正前不可保存。SQL 和参数共用未保存保护。

参数存储于 `config.schedule.parameters`，每行 `{name,value,source}`，`source` 为 `CODE` 或 `MANUAL`。保存生成文件版本，恢复版本同时恢复参数。发布快照固定参数；开发运行读取保存版本，单节点调试可使用独立记忆的常量覆盖，已发布运行读取快照。工作流参数是子节点默认值，同名子节点配置优先。

修改后先保存并重新发布，再在调度面板顶部选择新执行版本并应用。业务日期偏移在调度时间分组，默认 -1，时区默认 Asia/Shanghai。

## 单节点运行与带参运行

节点工具栏提供“运行”（F8）和“带参运行”（Shift+F8）。普通运行优先使用该节点记住的调试值，再使用调度参数的实际默认值；只有代码或同步配置引用的参数缺值时才打开补填弹窗。无参数节点直接运行。带参运行每次打开弹窗，预填当前实际值，显示“调试参数”“调度默认”或“待填写”；无参数时也可确认运行。

手填值作为常量独立保存在 `dw_node_debug_parameters`，刷新后仍可复用，不修改调度配置、不增加节点版本，也不进入发布包。支持中文、空格、等号，每项 1–4096 字符；输入 `$bizdate` 等内容会作为字面字符串使用。未被手填覆盖的调度表达式在每次提交时重新计算，弹窗的默认值只是预览。“恢复调度默认值”移除调试覆盖；确认运行被接受后生效，取消弹窗保持原值。

运行前保存点击时的节点草稿，保存过程中继续编辑的内容仍保留为下一份草稿。取消弹窗不保存代码或创建运行；参数、保存、版本或同步预检失败不更新调试记忆。运行被接受后即使执行失败，已确认的调试值仍保留。运行详情显示本次固定的实际参数。

“加载代码中的参数”产生的空值调度行及错误表达式仍属于非法调度草稿，须修正后才能保存和运行。弹窗补填适用于代码中尚未配置的参数，不会自动修改或删除调度行。

工作流开发调试、已发布版本、自动调度及原参数重跑不读取独立调试记忆。节点移入回收站后保留调试值，恢复后可复用；复制节点不会复制调试值。V9 仅新增参数表，不回填历史运行，也不修改现有节点和计划。

## 支持的值

| 写法 | 含义 |
|---|---|
| `杭州`、`123` | 常量 |
| `$bizdate` | 业务日期，yyyymmdd |
| `$cyctime` | 计划时间，yyyymmddhh24miss |
| `${yyyymmdd}`、`${yyyymmdd-1}` | 业务日期格式化、日偏移 |
| `${yyyymm-1}`、`${yyyy+1}` | 业务月份或年份偏移 |
| `$[yyyymmdd-1]` | 计划日期前一天 |
| `$[yyyymmddhh24miss-1/24]` | 计划时间前一小时 |
| `$[yyyymmddhh24miss-15/24/60]` | 计划时间前 15 分钟 |
| `$[add_months(yyyymmdd,-1)]` | 计划日期前一月，月末自动对齐 |
| `partition_${yyyymmdd}_01` | 常量拼接 |

年月日支持 yyyy、yy、mm、dd；计划时间另支持 hh24、hh12/hh、mi、ss。支持数字乘法偏移，例如 `${yyyymmdd-7*2}`；不执行任意代码，不支持的表达式明确报错。业务日期表达式不支持时分秒，计划月份/年份移动使用 add_months。

例如计划时间为北京时间 `2026-09-29 02:00`、日期偏移 -1，则 `$[yyyymmdd-1]` 和 `${yyyymmdd}` 都为 `20260928`，`${yyyymmdd-1}` 为 `20260927`。

## 时间、预览与执行

调度参数预览可选业务日期及 1–20 个实例，默认 5 个；展示业务日期、计划时间和实际值，仅计算参数，不执行 SQL。所有日期运算使用调度时区。

普通手动运行固定提交时刻。指定业务日期重算使用该业务日期减去日期偏移后、Cron 当日第一个计划时点；该日无计划时明确报错。预览与执行共用后端规则。计划时间和业务日期创建时固定，排队、延迟、自动重试与原参数重跑不会改为实际执行时刻。

MySQL SQL 值位置支持 `${参数名}`，包括 `'prefix_${参数名}'`。SQL 编译为 JDBC 参数绑定，参数值不会拼接成 SQL；不支持动态表名和列名。只读与数据库范围校验保持生效。

旧 `:bizdate`、`:source_cutoff`、`:build_id`、`:upstream_别名_build_id` 保留原类型及行为，新参数单独保存在运行的 `scheduleParameters`，不会覆盖内部 `parameters`。其他引擎可保存和预览参数，仍然模拟执行。

## 接口

- `POST /api/v1/schedule-parameters/extract`：`{code}`，返回去重参数名。
- `POST /api/v1/schedule-parameters/preview`：调度时间字段、`businessDate`、`count`、`parameters`、`code`，返回实例数组。每项包含 `businessDate`、UTC `scheduledAt`、`timezone` 和 `values`。
- `POST /api/v1/runs/parameters/prepare`：`{object: StudioObject}`，基于捕获的当前草稿提取参数并返回 `parameters`、`debugParameters`、`missingParameters`。参数项包含 `name`、可选实际 `value`、可选 `defaultValue` 和 `source=DEBUG|SCHEDULE|MISSING`。接口只准备参数，不保存或执行节点。
- `POST /api/v1/runs`：单节点可携带 `debugParameters` 字符串映射；省略时复用记忆，显式映射完全替换记忆，`{}` 清除覆盖并使用调度默认值。只接受当前引用的参数，缺值返回 `MISSING_DEBUG_PARAMETERS`。发布和工作流运行不读取独立调试覆盖。

## 官方参考

- [配置和使用调度参数](https://help.aliyun.com/zh/dataworks/user-guide/configure-and-use-scheduling-parameters)
- [调度参数最佳实践](https://help.aliyun.com/zh/dataworks/user-guide/best-practices-of-configuring-scheduling-parameters)
- [支持的表达式格式](https://help.aliyun.com/zh/dataworks/user-guide/supported-formats-of-scheduling-parameters)
