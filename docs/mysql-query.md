# MySQL SQL 读写与多语句执行

新建 MySQL 节点绑定数据源后可执行查询、增删改和表结构操作，一次运行支持多条 SQL。开发运行、任务发布、调度和工作流使用同一执行路径；其余节点继续模拟。见 [工作流说明](workflow-execution.md) 和 [参数说明](schedule-parameters.md)。

## 本地配置与使用

以下示例使用默认元数据库 `127.0.0.1:3306/fake_dataworks_260927`，另以 `127.0.0.1:3307/studio_demo` 为业务数据库。使用自己的 MySQL 实例时，请调整连接地址和账号。后端禁止把元数据库作为业务数据源。

首次初始化业务数据时，在本地终端设置 `DEMO_ROOT_PASSWORD`，然后从项目根目录执行 `scripts/init-query-demo.ps1 -ContainerId 'your-business-mysql-container'`，或先设置 `DEMO_MYSQL_CONTAINER`。以下启动命令均从项目根目录开始。脚本只在新业务实例中创建 `studio_demo` 和 `studio_reader`，按固定主键补充数据；重复运行不会覆盖数据或重置已有用户密码。脚本结束后清除终端中的 root 密码环境变量。

脚本会把随机生成的业务账号密码和 AES-GCM 密钥保存到已被 Git 忽略的 `backend/application-local.properties`。root 密码不保存在该文件或源码中。`studio.datasource.encryption-key` 是 32 字节密钥的 Base64 表示，可用 `STUDIO_ENCRYPTION_KEY` 提供默认配置；本地配置中的同名属性优先。备份和迁移必须保存原密钥，否则已有数据源密码无法解密。

在两个 PowerShell 终端分别启动，后端终端执行：

```powershell
Set-Location backend
mvn.cmd spring-boot:run
```

前端终端执行：

```powershell
Set-Location frontend
# 首次运行或依赖变更后执行，成功后再启动
npm.cmd ci
npm.cmd run dev -- --port 5173 --strictPort
```

日志显示在各自终端，按 `Ctrl+C` 停止对应服务；如出现批处理终止提示，输入 `Y` 确认。数据库首次准备步骤见[项目启动说明](../README.md#启动)。

前端默认 `http://127.0.0.1:5173`，后端 `http://127.0.0.1:8080`。在数据源页面新增并测试上述业务连接；新建 MySQL 节点并选择连接，编写查询后运行。初始数据的已支付订单汇总为：上海 1095.00、北京 557.50、杭州 79.80。

可新增或编辑数据源、查看真实表与字段。MySQL 节点在新建时选择数据源，编辑器顶部可切换连接；已有超时配置保留；运行会先保存草稿，并携保存版本提交。版本不匹配时拒绝执行。

## 执行和结果约定

- 所有数据源统一支持读写；无执行能力开关或目标表授权列表。旧 `materializationEnabled`、`materializationTargets` 请求字段被忽略，历史数据库列和发布快照保留。
- 支持 SELECT/WITH、INSERT/REPLACE/UPDATE/DELETE，以及表、索引、视图的创建、修改、删除，表清空和重命名。整份脚本先解析校验，再逐条执行；字符串或注释内的分号不会拆分语句。上限为 200,000 字符、1,000 条语句。
- 每条语句自动提交，运行失败、超时或停止时，之前已提交的操作保留，后续语句不执行。表结构操作遵循 MySQL 隐式提交行为。暂不支持手写事务控制、数据库/账号管理、存储程序和文件/会话操作。
- 只允许操作数据源配置的业务库；保留系统库、元数据库及 root 限制。数据库账号的实际权限继续生效；权限不足会显示明确的权限错误。
- `${参数名}` 支持 SQL 值位置及单引号字符串；每条语句独立使用 JDBC 参数绑定，执行前检查所有参数。不支持动态表名/列名。
- 默认整次运行超时 30 秒，可选 1–300 秒；连接超时 5 秒。4 个执行线程、32 个排队位置；超额请求返回 429。
- 每条查询最多保存 1,000 行、单元格最多 64 KiB；整次运行的结果共用 5 MiB。截断只影响预览，后续 SQL 继续执行。二进制值以 `base64:` 显示，BIGINT/DECIMAL 用字符串保留精度。
- 结果区按语句切换，展示查询表格、写入影响行数、DDL 成功状态及提交情况。分页读取保存的结果，不重新执行 SQL。后续失败时仍可查看前面的成功结果。
- 取消会中止当前数据库语句并停止后续语句；正在提交的写入由执行线程确定终态。连接丢失或服务重启使提交结果无法确认时，记录 `FAILED / COMMIT_UNKNOWN`，对应语句为 `UNKNOWN`，需核实业务数据。
- 普通含写入脚本及包含此类节点的工作流不自动失败重试。纯查询和库存专用发布事务沿用原重试规则。手动重跑会从脚本开头重新执行。
- 旧单结果历史记录继续可读；代码、版本、参数和数据源绑定快照继续保存。库存专用落表仍要求单条 SELECT 和预建目标表，但不再要求数据源开关或授权列表。

已有演示环境仅升级账号权限时，在项目根目录执行 `pwsh -File scripts/grant-demo-readwrite.ps1`。脚本为现有 `studio_reader` 和 `studio_inventory_etl` 补齐各自业务库的读写及 DDL 权限，不初始化业务数据或更换密码。外部数据源按其自身账号权限执行。测试连接仅查询版本，不创建测试表。

## API

接口前缀 `/api/v1`，错误统一为 `{code,message}`。

| 方法与路径 | 内容 |
|---|---|
| GET /datasources?workspaceId= | 空间内数据源列表 |
| POST /datasources | 创建：workspaceId、name、host、port、database、username、password |
| GET /datasources/{id} | 公开配置，包含 passwordSet，不包含密码或密文 |
| PUT /datasources/{id} | 编辑；省略 password 保留已有密码，不支持跨空间移动 |
| POST /datasources/test | 测试未保存配置 |
| POST /datasources/{id}/test | 测试已保存配置；可提交待修改字段 |
| GET /datasources/{id}/tables | 业务库真实表 |
| GET /datasources/{id}/tables/{table}/columns | 指定表字段 |
| POST /runs | objectId、expectedVersion、mode；真实查询必须提供当前 expectedVersion |
| GET /runs?workspaceId=&summary=true&page=1&pageSize=12&status=&search= | 分页摘要、筛选后的 total、空间整体 stats；不带日志和结果 |
| GET /runs?workspaceId= | 兼容旧数组接口；真实查询结果仍通过独立结果接口读取 |
| GET /runs/{id} | 运行详情和 snapshot |
| GET /runs/{id}/results?page=1&pageSize=100&statementIndex=1 | 指定从 1 开始的语句序号；省略则选择首个查询，否则首条语句。返回原分页字段及语句类型、状态、提交状态、影响行数和 statements 摘要 |
| POST /runs/{id}/stop | 幂等停止 |

真实节点的配置示例：

```json
{"run":{"provider":"MYSQL","dataSourceId":"数据源ID","timeoutSeconds":30}}
```

只接受 `kind=NODE, nodeType=MySQL`。没有 `provider=MYSQL` 的旧对象仍然模拟；配置缺失或连接失败不会悄悄切换到模拟。

## 数据库升级与备份

Flyway 使用 Spring Boot BOM 指定的 12.4.0，禁用 clean 和自动基线。V1 保存原七表结构，V2 增加 `dw_datasource`、`dw_run_result`。旧的 Spring schema.sql 自动初始化已关闭。

空数据库直接从 V1 迁移。已有旧七表数据库首次升级时，先停止正在运行的后端，再在后端终端执行以下命令；确认备份成功后再执行迁移启动部分：

```powershell
Set-Location backend
powershell.exe -NoProfile -ExecutionPolicy Bypass -File ..\scripts\backup-metadata.ps1 -ContainerId 'your-mysql-container'

# 仅在备份成功后执行
$env:STUDIO_BASELINE_EXISTING = 'true'
try {
    mvn.cmd spring-boot:run
} finally {
    Remove-Item Env:STUDIO_BASELINE_EXISTING -ErrorAction SilentlyContinue
}
```

迁移成功后按 `Ctrl+C` 停止本次后端，命令退出时会清除迁移环境变量；之后使用普通启动命令。前端仍在另一个终端单独启动。

只有明确启用该开关、库内恰好是原七表且列名集合匹配时才允许建立 V1 基线；不符合时停止，不删除或重建。首次升级后不再需要开关。SQL 备份位于 `.runtime/backups/`；文件资源另保存在 `backend/storage/`。

Flyway 对 MySQL 9.7.2 会给出“高于已验证版本 9.4”的提示，本项目使用当前实例验证迁移和重复启动；该提示不等于迁移失败。

## 验证

`mvn test` 覆盖真实 SQL 脚本读写、逐条提交、DDL、多结果、失败停止、提交未知、自动重试边界，以及真实查询、权限、超时与数据库侧取消、排队、结果限制、精度、接口响应、密码隐藏、旧模拟功能以及空库和旧库迁移。查询测试需要上述本地业务库和配置；测试使用随机工作空间，迁移测试使用随机临时数据库，结束只清理自身资源。

前端执行 `npm run typecheck`、`npm test`、`npm run build`。浏览器验收覆盖数据源编辑和测试连接、目录字段、保存运行、结果、代码快照及刷新后的历史。
