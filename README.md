# mini-infra-orchestration-lab
簡單使用 ansible 部署環境與程式至兩台免費 oracle cloud 主機
node1 flask api 使用 redis stream enqueue 
node2 goroutine 消化 queu 工作 

## 控制與遠端安裝

### Ubuntu 24.04 控制端初始化

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

### 遠端環境安裝

```bash
source ~/ansible-env/bin/activate
ansible-playbook /etc/ansible/setup.yaml
```

### node2 golang 編譯
```bash
./node2_worker/build.sh
```

### 遠端環境部署
```bash
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
sudo iptables -I INPUT -p tcp -s 10.0.0.48/32 --dport 5000 -j ACCEPT
sudo iptables -I INPUT -p tcp -s 10.0.0.48/32 --dport 6379 -j ACCEPT
```

### node2

- 開放 SNMP（UDP 161）給 node1，讓壓測期間可收集 node2 狀態

```bash
sudo iptables -I INPUT -p udp -s 10.0.0.143/32 --dport 161 -j ACCEPT
```

## SNMP 設定

`setup.yaml` 會在兩台 node 安裝並啟動 `snmpd`，但預設只允許 localhost 查詢。
需手動調整各節點的 `/etc/snmp/snmpd.conf`，讓 node1 可跨節點收集 node2 的資料。

### node2 — 允許 node1 查詢

```bash
sudo nano /etc/snmp/snmpd.conf
```

找到（或新增）`rocommunity` 行，加入 node1 的 private IP：

```
rocommunity public localhost
rocommunity public <node1-private-ip>
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

```
SNMP_COMMUNITY=mystring
```

## systemd 服務名稱

- node1 API: `mini-orch-api`
- node1 Status Consumer: `mini-orch-status-consumer`
- node1 Loadtest API: `mini-orch-loadtest`（port `5001`）
- node2 worker: `mini-orch-worker`

