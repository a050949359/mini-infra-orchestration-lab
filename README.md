# mini-infra-orchestration-lab

簡單使用 Ansible 部署環境與程式至兩台免費 Oracle Cloud 主機

- **node1**：Flask API，使用 Redis Stream 接收任務並 enqueue
- **node2**：Go worker，多 goroutine 消化 queue 工作，狀態寫回 node1 Redis Stream
- **容錯**：node2 → node1 status 寫入失敗時暫存至 node2 local Redis，定期重試
- **壓測 API**：Loadtest API（port 5001）外部控制 k6 參數，支援 vus / duration / script 三個欄位，非同步回傳 run_id 供輪詢結果
- **監控**：SNMP 收集兩台主機 CPU / 記憶體，存 Redis 供壓測報告使用
- **TLS**：node1 兩支服務（API port 5000、Loadtest port 5001）均以自簽憑證跑 HTTPS；setup.yaml 自動產生憑證，cron 每 90 天重簽並重啟服務

## 控制與遠端安裝

#### Ubuntu 24.04 控制端初始化

```bash
chmod +x ./ansible/setup_ansible_server.sh
./ansible/setup_ansible_server.sh
```

腳本會依序執行：

1. 確認 Ubuntu 24.04
2. 確認 python3.12
3. 建立/啟用 venv `~/ansible-env`
4. 在 venv 內安裝 `ansible-core`
5. 安裝 Go
6. 建立 `/etc/ansible/` 並放入以下檔案：
   - `/etc/ansible/setup.yaml`
   - `/etc/ansible/ping.yaml`
   - `/etc/ansible/hosts.ini`
   - `/etc/ansible/group_vars/all.yml`
   - `/etc/ansible/ansible.cfg`
7. 對所有主機執行連線測試（`/etc/ansible/ping.yaml`）

#### 遠端安裝

```bash
# 遠端環境安裝
source ~/ansible-env/bin/activate
ansible-playbook /etc/ansible/setup.yaml

# node2 golang 編譯
./node2_worker/build.sh

# 遠端程式部署
ansible-playbook /etc/ansible/deploy.yaml
```

## YAML 功能簡介

- `ansible/ping.yaml`: `setup_ansible_server.sh` 最末尾會做的主機連線測試（`ansible.builtin.ping`）
- `ansible/setup.yaml`: 基礎環境安裝與初始化（Redis、SNMP、Python venv 等）
- `ansible/deploy.yaml`: 應用部署（node1 Flask API、node2 Go worker、systemd 服務）

## 手動設定 iptables

### node1

- 開放 PORT `5000` `6379` 給內網

```bash
sudo iptables -I INPUT -p tcp -s <remote ip> --dport 5001 -j ACCEPT
sudo iptables -I INPUT -p tcp -s NODE2_PRIVATE_IP/32 --dport 5000 -j ACCEPT
sudo iptables -I INPUT -p tcp -s NODE2_PRIVATE_IP/32 --dport 6379 -j ACCEPT
```

### node2

- 開放 SNMP（UDP 161）給 node1，讓壓測期間可收集 node2 狀態

```bash
sudo iptables -I INPUT -p udp -s NODE1_PRIVATE_IP/32 --dport 161 -j ACCEPT
```

### 雲端還需要額外設定雲端的防火牆

## SNMP 設定

`setup.yaml` 會在兩台 node 安裝並啟動 `snmpd`，但預設只允許 localhost 查詢。
需手動調整各節點的 `/etc/snmp/snmpd.conf`，讓 node1 可跨節點收集 node2 的資料。

### node2 — 允許 node1 查詢

```bash
sudo vim /etc/snmp/snmpd.conf
```

編輯以下
```
agentaddress <node2-private-ip>:161

# 此行 node1 也需要加
view   systemonly  included   .1.3.6.1.4.1.2021

rocommunity  public NODE_SUBNET/24 -V systemonly
rocommunity6:  public NODE_SUBNET/24 -V systemonly
```

儲存後重啟：

```bash
sudo systemctl restart snmpd
```

### 驗證

在 node1 上執行，確認可正常回傳數值：

```bash
# CPU 1分鐘 load avg
snmpget -v2c -c public <node2-private-ip> 1.3.6.1.4.1.2021.10.1.3.1

# 總記憶體 (KB)
snmpget -v2c -c public <node2-private-ip> 1.3.6.1.4.1.2021.4.5.0

# 可用記憶體 (KB)
snmpget -v2c -c public <node2-private-ip> 1.3.6.1.4.1.2021.4.6.0
```

### 壓測期間收集的 OID

| 欄位 | OID |
|------|-----|
| `cpu_load1` | `1.3.6.1.4.1.2021.10.1.3.1` |
| `mem_total_kb` | `1.3.6.1.4.1.2021.4.5.0` |
| `mem_avail_kb` | `1.3.6.1.4.1.2021.4.6.0` |

### 更換 community string

預設使用 `public`，可透過 `SNMP_COMMUNITY` 環境變數覆蓋（寫入 `loadtest_env_file`）：
snmpd.conf 也需要同步修改

```
SNMP_COMMUNITY=mystring
```

## 壓測

Loadtest API 運行於 node1 port `5001`，透過 HTTP 啟動 k6 壓測並在結束後回傳結果與 SNMP 資料。

### 啟動壓測

```bash
curl -sk -X POST https://<node1-ip>:5001/api/v1/loadtest/runs \
  -H 'Content-Type: application/json' \
  -d '{
    "vus": 20,
    "duration": "30s",
    "api_url": "https://localhost:5000"
  }' | jq .
# → {"run_id": "...", "status": "running", "started_at": "..."}
```

**可用參數：**

| 參數 | 說明 | 預設 |
|------|------|------|
| `vus` | 固定併發數，覆蓋腳本 stages | 依腳本 |
| `duration` | 測試時長（如 `"30s"`, `"2m"`），覆蓋腳本 stages | 依腳本 |
| `api_url` | 壓測目標 URL | `https://localhost:5000` |
| `script` | loadtest 目錄下的腳本檔名 | `api_stress.js` |

### 查詢結果

測試進行中回 `202 running`，結束後一次回傳完整結果：

```bash
curl -sk https://<node1-ip>:5001/api/v1/loadtest/runs/<run_id> | jq .
```

結束後的 response：

```json
{
  "status": "done",
  "exit_code": 0,
  "k6_output": "...",
  "snmp": {
    "node1": [{"ts": "...", "cpu_load1": 0.5, "mem_total_kb": 2048000, "mem_avail_kb": 1200000}],
    "node2": [...]
  },
  "started_at": "...",
  "finished_at": "..."
}
```

### k6 腳本說明（`api_stress.js`）

- Stages：20 → 50 → 100 VU，峰值維持 60s，共約 200s
- 每個 VU：POST `/api/v1/jobs` → 等 200ms → GET `/api/v1/jobs/<id>`
- 10% 機率送壞 payload（驗證 API 400 路徑）
- action 含 `force_fail` 時 worker 模擬失敗（約 20% 機率）

## systemd 服務名稱

- node1 API: `mini-orch-api`
- node1 Status Consumer: `mini-orch-status-consumer`
- node1 SNMP Collector: `mini-orch-snmp-collector`（寫入 Redis DB 1）
- node1 Loadtest API: `mini-orch-loadtest`（port `5001`）
- node2 worker: `mini-orch-worker`

