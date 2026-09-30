# 调度参数与工作台精简

## 页面与升级

主导航只保留数据开发、数据源、回收站。右侧仅有调度配置和版本；数据源改在编辑器顶部选择。旧运行结果、发布快照、版本与文件恢复能力继续可用。

Flyway V6 是一次性迁移：全部空间的现有开发对象移入回收站，相关计划暂停，待执行触发取消，打开标签清空。版本、文件内容、工作空间和连接保留。已删除对象仍留在回收站。后续新建对象不会再次清理，服务不再生成示例文件。恢复目录会一起恢复该目录删除批次中的后代；已有同名项时可改名恢复。恢复文件不会自动重新启用调度。

## 配置与保存

调度参数初始为空；支持新增、清空值、删除。加载代码中的 `${参数名}` 会去重、保留已有赋值和未引用的配置，来源显示代码解析；新建行显示手动添加。

表达式模式以空格分隔 `name=value`。例如 `bizdate=$[yyyymmdd-1] region=杭州`。名称须以字母或下划线开头，仅包含字母、数字、下划线，最多 64 字符；不能重复。参数值不能缺失或含空白、等号。错误输入保留在草稿中，修正前不可保存。SQL 和参数共用未保存保护。

参数存储于 `config.schedule.parameters`，每行 `{name,value,source}`，`source` 为 `CODE` 或 `MANUAL`。保存生成文件版本，恢复版本同时恢复参数。发布快照固定参数；开发运行读取保存版本，已发布运行读取快照。工作流参数是子节点默认值，同名子节点配置优先。

修改后先保存并重新发布，再在调度面板顶部选择新执行版本并应用。业务日期偏移在调度时间分组，默认 -1，时区默认 Asia/Shanghai。

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

## 官方参考

- [配置和使用调度参数](https://help.aliyun.com/zh/dataworks/user-guide/configure-and-use-scheduling-parameters)
- [调度参数最佳实践](https://help.aliyun.com/zh/dataworks/user-guide/best-practices-of-configuring-scheduling-parameters)
- [支持的表达式格式](https://help.aliyun.com/zh/dataworks/user-guide/supported-formats-of-scheduling-parameters)
