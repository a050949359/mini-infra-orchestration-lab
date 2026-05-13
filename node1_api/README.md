# Node1 API

Flask + Gunicorn，監聽 `0.0.0.0:5000`。

---

## Health

### `GET /healthz`

```json
{ "status": "ok", "redis": "ok" }
```

---

## Jobs

### `POST /api/v1/jobs`

建立 Job 並推送至 Redis Stream。

**Request body**

```json
{
  "type": "stress_test",
  "priority": 3,
  "payload": {
    "user_id": 42,
    "action": "generate_report",
    "data": "some-data"
  },
  "run_id": "uuid-optional"
}
```

| 欄位 | 型別 | 必填 | 說明 |
|---|---|---|---|
| `type` | string | ✓ | 非空字串 |
| `priority` | int | ✓ | 整數 |
| `payload.user_id` | int | ✓ | |
| `payload.action` | string | ✓ | 非空字串；含 `force_fail` 會觸發 worker 模擬失敗 |
| `payload.data` | string | ✓ | 非空字串 |
| `run_id` | string | | loadtest run UUID，可 null |

**Response `202`**

```json
{
  "job_id": "...",
  "status": "queued",
  "created_at": "2026-05-13T00:00:00+00:00",
  "stream": "jobs:stream",
  "stream_entry_id": "..."
}
```

**Response `503`** — Redis 寫入失敗，job 狀態為 `publish_failed`。

---

### `GET /api/v1/jobs/<job_id>`

取得單一 Job 狀態。

**Response `200`**

```json
{
  "job_id": "...",
  "status": "done",
  "run_id": "...",
  "type": "stress_test",
  "priority": 3,
  "payload": { "user_id": 42, "action": "generate_report", "data": "..." },
  "created_at": "...",
  "updated_at": "..."
}
```

Job status 流程：`queued` → `processing` → `done` / `failed` / `dead`

---

### `POST /api/v1/jobs/<job_id>/status`

直接覆寫 Job 狀態（admin / 測試用）。

**Request body**

```json
{ "status": "done" }
```

允許值：`queued` `processing` `done` `failed` `publish_failed` `dead`

---

## Stats

### `GET /api/v1/stats`

全域 worker 統計（來自 Redis Hash `worker:stats`）＋ queue 深度。

**Response `200`**

```json
{
  "global": {
    "processed": 1000,
    "failed": 20,
    "failed:force_fail": 5,
    "dead": 2
  },
  "queue_pending": 3
}
```

---

### `GET /api/v1/jobs/stats`

各 loadtest run 的 job 處理摘要（來自 SQLite）。

**Query params**

| 參數 | 說明 |
|---|---|
| `run_id` | 指定單一 run；不帶則回傳所有 runs |

**Response `200`**

```json
{
  "runs": [
    {
      "run_id": "...",
      "total": 500,
      "in_flight": 0,
      "is_complete": true,
      "by_status": { "done": 490, "failed": 10 }
    }
  ]
}
```

`in_flight` = `queued + processing`；`is_complete` = `in_flight == 0 && total > 0`

---

## SNMP

### `GET /api/v1/snmp`

讀取 SNMP 監控資料（來自 Redis DB1，由 `snmp_collector.py` 寫入）。

Nodes：`node1` `node2`  
Metrics：`cpu_load1` `mem_total_kb` `mem_avail_kb`

**Query params**

| 參數 | 說明 |
|---|---|
| `latest` | 存在即回傳每個 node 最新一筆快照 |
| (無) | 回傳近 2 小時時序資料 |

**Response `200` — latest**

```json
{
  "mode": "latest",
  "data": {
    "node1": { "ts": "...", "cpu_load1": 0.5, "mem_total_kb": 8000000, "mem_avail_kb": 4200000 },
    "node2": { "ts": "...", "cpu_load1": 0.3, "mem_total_kb": 8000000, "mem_avail_kb": 5100000 }
  }
}
```

**Response `200` — history**

```json
{
  "mode": "history",
  "data": {
    "node1": [
      { "ts": "...", "cpu_load1": 0.5, "mem_total_kb": 8000000, "mem_avail_kb": 4200000 }
    ],
    "node2": []
  }
}
```

---

## Loadtest

### `POST /api/v1/loadtest/runs`

啟動 k6 壓測（背景執行），回傳 `run_id`。

**Request body**

```json
{
  "script": "api_stress.js",
  "api_url": "http://localhost:5000",
  "vus": 50,
  "duration": "60s"
}
```

| 欄位 | 說明 | 預設 |
|---|---|---|
| `script` | `LOADTEST_SCRIPT_DIR` 下的檔名 | `api_stress.js` |
| `api_url` | 壓測目標 | `http://localhost:5000` |
| `vus` | Virtual Users | k6 script 預設 |
| `duration` | 執行時長 | k6 script 預設 |

**Response `202`**

```json
{ "run_id": "...", "status": "running", "started_at": "..." }
```

---

### `GET /api/v1/loadtest/runs/<run_id>`

查詢壓測結果。

**Response `202`** — 執行中

```json
{ "run_id": "...", "status": "running", "started_at": "..." }
```

**Response `200`** — 完成

```json
{
  "run_id": "...",
  "status": "done",
  "script": "api_stress.js",
  "api_url": "http://localhost:5000",
  "vus": 50,
  "duration": "60s",
  "started_at": "...",
  "finished_at": "...",
  "exit_code": 0,
  "k6_output": "..."
}
```

`status` 為 `done` 或 `failed`（exit_code != 0）。
