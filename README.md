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

## 手動設定 iptables（node1）

- 開放 PORT `5000` `6379` 給內網

```bash
sudo iptables -I INPUT -p tcp -s 10.0.0.48/32 --dport 5000 -j ACCEPT
sudo iptables -I INPUT -p tcp -s 10.0.0.48/32 --dport 6379 -j ACCEPT
```
