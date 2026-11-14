# PM2.5 小时数据每日发布 API

环保监测站向分析人员发布每日 PM2.5 小时数据的服务。技术栈：**FastAPI + SQLite + 本地文件存储**，使用 **Docker Compose** 一键启动。

## 功能与业务规则

### 数据上传（按监测点 + 日期）

`POST /stations/{station_id}/dates/{data_date}/uploads`，multipart 表单：

| 字段 | 说明 |
|---|---|
| `file` | CSV 文件，表头 `hour,pm25`（兼容 `小时,浓度`、`pm2.5` 等别名） |
| `expected_version` | 当前版本号；首次上传传 `0`（默认），补报必须传当前版本号 |

**整批校验规则（任一不满足则整批拒绝，不发布任何记录，HTTP 422 并逐条给出原因）：**

- 必须恰好包含小时 `0～23` 共 24 条记录；
- 小时不重复、不缺失（响应中逐个列出缺失小时及重复行所在行号）；
- 浓度必须是非负有限数（拒绝负数、`NaN`、`Inf`、空值、非数值）；
- 列数必须恰好为 2，小时必须是 0..23 的整数。

### 站号变更后的归属登记（源站编号 -> 物理站）

监测网换号后，同一物理站在不同日期可能以不同源站编号发布数据。通过归属登记把
旧号、新号接到同一物理站下：

`POST /attributions`，JSON：

```json
{
  "physical_station_id": "PHYS-001",
  "source_station_id": "OLD-001",
  "start_date": "2026-11-01",
  "end_date": "2026-11-10"
}
```

- 日期区间**包含起止日**，`start_date` 不得晚于 `end_date`（否则 400）；
- **同一源站同一天只能归属一个物理站**：与已有登记（不同物理站）有任意一天重叠即返回
  `409 attribution_conflict`，并给出重叠区间，登记整体不生效；
- 并发登记由 SQLite `BEGIN IMMEDIATE` 写事务串行化，校验在事务内完成，只有一份生效；
  数据库触发器 `trg_attribution_no_source_split` 作为兜底约束；
- 完全相同的重复登记幂等返回已有记录（HTTP 200，`created=false`）；
- 同一物理站可以同时登记多个源站（换号交接期旧号、新号并存），查询时按天解析。

`GET /attributions?physical_station_id=&source_station_id=` 列出登记（可按物理站或源站过滤）。

### 按物理站读取逐日数据

`GET /physical-stations/{physical_station_id}/daily-series?start_date=...&end_date=...`

在日期范围内逐天返回：归属解析出的源站编号、当前发布版本、`file_id` 及 24 条读数。

- 某天归属到**唯一**源站且有已发布数据：`status = "ok"`，带 `source_station_id`、
  `version`、`file_id`、`records`（24 条 `{hour, pm25}`）；
- 某天有**多个**归属源站同时发布了数据：`status = "source_conflict"`，列出每个候选来源的
  源站编号、版本与 `file_id`，`records` 为 `null` —— **不会任取其中一份**；
- 归属存在但当天无已发布数据（或该物理站当天无归属）：`status = "missing"`；
- 响应含 `day_count`、`conflict_count`、`missing_count` 便于判读；日期区间非法返回 400；
- **快照一致性**：整个查询在同一个 SQLite 读事务（单一快照）中完成，归属解析与逐日读数
  来自同一时刻的数据库状态。登记或补报与查询并发时，一次查询看到的归属和读数要么全属于
  提交前、要么全属于提交后，不会出现"新归属 + 旧数据"的拼接。

### 版本与补报

- 每个 `(监测点, 日期)` 从版本 1 开始单调递增；
- 补报携带当前版本号，校验通过后**整批替换**为新版本，旧版本数据与原始 CSV 均保留；
- 版本号不匹配返回 `409 version_conflict`，并回带当前版本号；
- 并发补报通过 SQLite `BEGIN IMMEDIATE` 写事务串行化，**保证只有一份生效**。

### 查询与文件留存

| 接口 | 用途 |
|---|---|
| `GET /stations/{station_id}/dates/{data_date}/records` | 当前版本的 24 条小时记录 + 文件标识 `file_id` |
| `GET /stations/{station_id}/dates/{data_date}/versions` | 全部历史版本（含每版 `file_id`、文件路径、校验和） |
| `GET /stations/{station_id}/dates/{data_date}/versions/{version}/file` | 下载指定版本留存的原始 CSV（字节一致） |
| `POST /attributions` | 登记源站编号在日期区间内归属某个物理站 |
| `GET /attributions` | 列出归属登记（按物理站 / 源站过滤） |
| `GET /physical-stations/{physical_station_id}/daily-series` | 按物理站 + 日期范围读取逐日已发布数据 |
| `GET /health` | 健康检查 |

### 失败安全保证

- 原始 CSV 先写入临时文件，再 `os.replace` 原子落到**不可变路径**
  （`files/<station>/<date>/v<version>_<file_id>.csv`），旧文件永不被覆盖；
- 文件写入成功后才开启数据库事务，版本元数据与 24 条记录在同一事务中提交；
- 任何文件或数据库写入失败都会回滚，**上一版本仍完整可查**（测试覆盖）。

## 目录结构

```
app/
  config.py       # 数据目录配置（环境变量 PM25_DATA_DIR）
  validation.py   # CSV 解析与整批校验（错误行/缺失小时/原因）
  db.py           # SQLite 模式、乐观版本控制、事务与文件原子落盘
  main.py         # FastAPI 路由与错误处理
tests/            # 50 个 pytest 用例（校验/API/并发/故障恢复/归属与快照）
Dockerfile
docker-compose.yml
requirements.txt
```

## 启动方式

```bash
docker compose up --build
# 服务监听 http://localhost:8000，交互式文档 http://localhost:8000/docs
```

SQLite 数据库、留存 CSV、临时文件统一存放在命名卷 `pm25-data`（容器内 `/data`）。

## 本地开发与测试

```bash
pip install -r requirements.txt
pip install pytest httpx
PM25_DATA_DIR=./data python -m uvicorn app.main:app --reload
python -m pytest tests/ -q
```

## 使用示例

```bash
# 首次上传
curl -X POST http://localhost:8000/stations/ST-001/dates/2026-11-14/uploads \
  -F "expected_version=0" -F "file=@2026-11-14.csv"

# 查询当前版本
curl http://localhost:8000/stations/ST-001/dates/2026-11-14/records

# 补报（携带当前版本号 1）
curl -X POST http://localhost:8000/stations/ST-001/dates/2026-11-14/uploads \
  -F "expected_version=1" -F "file=@2026-11-14-fixed.csv"

# 下载历史版本原始 CSV
curl -OJ http://localhost:8000/stations/ST-001/dates/2026-11-14/versions/1/file

# 登记旧号归属物理站 (2026-11-01 ~ 2026-11-10)
curl -X POST http://localhost:8000/attributions \
  -H 'Content-Type: application/json' \
  -d '{"physical_station_id":"PHYS-001","source_station_id":"ST-OLD","start_date":"2026-11-01","end_date":"2026-11-10"}'

# 按物理站读取拼接后的逐日数据
curl 'http://localhost:8000/physical-stations/PHYS-001/daily-series?start_date=2026-11-01&end_date=2026-11-12'
```

## 校验失败响应示例

```json
{
  "error": "validation_failed",
  "message": "整批数据未通过校验，没有任何记录被发布",
  "reasons": [
    {"reason": "missing hour", "hour": 23},
    {"reason": "duplicate hour", "line": 25, "hour": 0, "first_line": 2},
    {"reason": "pm25 concentration must be non-negative", "line": 8, "value": -1.2}
  ]
}
```
