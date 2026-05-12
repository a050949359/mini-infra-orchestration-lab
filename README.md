# mini-infra-orchestration-lab
簡單使用 ansible 部署環境與程式至兩台免費 oracle cloud 主機

## Ubuntu 24.04 控制端初始化

```bash
chmod +x ./ansible/setup_ansible_server.sh
./ansible/setup_ansible_server.sh
```

腳本會依序執行：

1. 確認 Ubuntu 24.04
2. 確認 python3.12
3. 建立/啟用 venv `~/ansible-env`
4. 在 venv 內安裝 `ansible-core`
5. 建立 `/etc/ansible/` 並放入以下檔案：
   - `/etc/ansible/setup.yaml`
   - `/etc/ansible/ping.yaml`
   - `/etc/ansible/hosts.ini`
   - `/etc/ansible/ansible.cfg`
6. 對所有主機執行連線測試（`/etc/ansible/ping.yaml`）

手動執行 playbook：

```bash
source ~/ansible-env/bin/activate
ansible-playbook /etc/ansible/setup.yaml
ansible-playbook /etc/ansible/ping.yaml
```
