#!/usr/bin/env bash
# Ansible server-side environment setup for Ubuntu 24.04
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ── 常數設定 ─────────────────────────────────────────
VENV_NAME="ansible-env"
VENV_PATH="$HOME/${VENV_NAME}"
ANSIBLE_HOME="$HOME/ansible"
HOSTS_INI_SRC="${SCRIPT_DIR}/hosts.ini"
PLAYBOOK_SRC="${SCRIPT_DIR}/main.yaml"

log()   { printf '\e[32m[setup]\e[0m %s\n' "$1"; }
warn()  { printf '\e[33m[warn]\e[0m  %s\n' "$1"; }
abort() { printf '\e[31m[error]\e[0m %s\n' "$1"; exit 1; }

SSH_KEY="$HOME/.ssh/oracle_id_rsa"
PLACEHOLDER_IPS=("192.168.56.11" "192.168.56.12")

# ── 0a. 確認 SSH 私鑰存在 ─────────────────────────────
check_ssh_key() {
  [[ -f "${SSH_KEY}" ]] || abort "SSH key not found: ${SSH_KEY}\n       請先把 Oracle Cloud 私鑰放到該路徑。"
  chmod 600 "${SSH_KEY}"
  log "SSH key: ${SSH_KEY}"
}

# ── 0b. 確認 hosts.ini 已填入真實 IP ────────────────
check_hosts_ini() {
  [[ -f "${HOSTS_INI_SRC}" ]] || abort "hosts.ini not found: ${HOSTS_INI_SRC}"

  for ip in "${PLACEHOLDER_IPS[@]}"; do
    if grep -q "ansible_host=${ip}" "${HOSTS_INI_SRC}"; then
      abort "hosts.ini 仍含預設佔位 IP ${ip}，請先填入真實 ansible_host。"
    fi
  done
  log "hosts.ini ansible_host 已設定"
}

# ── 1. 確認 Ubuntu 24.04 ──────────────────────────────
check_os() {
  [[ -f /etc/os-release ]] || abort "Cannot detect OS."
  # shellcheck disable=SC1091
  source /etc/os-release
  [[ "${ID:-}" == "ubuntu" && "${VERSION_ID:-}" == "24.04" ]] \
    || abort "Requires Ubuntu 24.04. Current: ${PRETTY_NAME:-unknown}"
  log "OS: ${PRETTY_NAME}"
}

# ── 2. 確認 Python 3.12 ───────────────────────────────
check_python() {
  local py
  py="$(command -v python3.12 2>/dev/null || true)"
  [[ -n "$py" ]] || abort "python3.12 not found. Install with: sudo apt install python3.12"

  local ver
  ver="$("$py" -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
  [[ "$ver" == "3.12" ]] || abort "python3.12 version check failed: got $ver"
  log "Python: $("$py" --version)"
}

# ── 3. 建立 / 啟用 venv ───────────────────────────────
setup_venv() {
  if [[ -d "${VENV_PATH}" ]]; then
    log "venv exists: ${VENV_PATH}"
  else
    log "Create venv: ${VENV_PATH}"
    python3.12 -m venv "${VENV_PATH}"
  fi

  # 啟用 venv
  # shellcheck disable=SC1091
  source "${VENV_PATH}/bin/activate"
  log "venv activated: ${VIRTUAL_ENV}"
}

# ── 4. 安裝 Ansible（只裝必要套件）──────────────────
install_ansible() {
  local pip="${VENV_PATH}/bin/pip"

  if "${VENV_PATH}/bin/ansible-playbook" --version &>/dev/null; then
    log "ansible-playbook already installed: $("${VENV_PATH}/bin/ansible-playbook" --version | head -1)"
    return
  fi

  log "Install ansible-core into venv"
  "$pip" install --upgrade pip --quiet
  "$pip" install ansible-core --quiet
  log "Installed: $("${VENV_PATH}/bin/ansible-playbook" --version | head -1)"
}

# ── 5. 建立 ~/ansible 目錄並放置檔案 ─────────────────
setup_ansible_dir() {
  mkdir -p "${ANSIBLE_HOME}"
  log "Ansible home: ${ANSIBLE_HOME}"

  # main.yaml
  if [[ -f "${PLAYBOOK_SRC}" ]]; then
    cp -n "${PLAYBOOK_SRC}" "${ANSIBLE_HOME}/main.yaml" 2>/dev/null \
      && log "Placed: ${ANSIBLE_HOME}/main.yaml" \
      || log "Skip (exists): ${ANSIBLE_HOME}/main.yaml"
  else
    warn "main.yaml not found in ${SCRIPT_DIR}, skipping."
  fi

  # hosts.ini → ~/ansible/hosts.ini
  if [[ -f "${HOSTS_INI_SRC}" ]]; then
    cp -n "${HOSTS_INI_SRC}" "${ANSIBLE_HOME}/hosts.ini" 2>/dev/null \
      && log "Placed: ${ANSIBLE_HOME}/hosts.ini" \
      || log "Skip (exists): ${ANSIBLE_HOME}/hosts.ini"
  else
    warn "hosts.ini not found in ${SCRIPT_DIR}, skipping."
  fi

  # ansible.cfg
  if [[ ! -f "${ANSIBLE_HOME}/ansible.cfg" ]]; then
    cat > "${ANSIBLE_HOME}/ansible.cfg" <<'EOF'
[defaults]
inventory          = ~/ansible/hosts.ini
host_key_checking  = False
retry_files_enabled = False
interpreter_python = auto_silent
stdout_callback    = yaml

[privilege_escalation]
become          = True
become_method   = sudo
become_ask_pass = False
EOF
    log "Created: ${ANSIBLE_HOME}/ansible.cfg"
  else
    log "Skip (exists): ${ANSIBLE_HOME}/ansible.cfg"
  fi
}

# ── 6. 連線測試（ansible ping）────────────────────────
run_ping_test() {
  log "Run connectivity test (ansible all -m ping)"
  cd "${ANSIBLE_HOME}"
  "${VENV_PATH}/bin/ansible" all \
    -i hosts.ini \
    -m ansible.builtin.ping \
    --one-line \
    2>&1 || warn "Some hosts failed ping — check hosts.ini and SSH keys."
}

# ── 主流程 ───────────────────────────────────────────
check_ssh_key
check_hosts_ini
check_os
check_python
setup_venv
install_ansible
setup_ansible_dir
run_ping_test

log "Setup complete. To run playbook manually:"
log "  source ${VENV_PATH}/bin/activate"
log "  cd ${ANSIBLE_HOME} && ansible-playbook main.yaml"
