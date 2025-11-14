#!/usr/bin/env bash

set -euo pipefail

REPO_URL="https://github.com/0x3654/gisp.git"
TARGET_DIR="${GISP_TARGET_DIR:-gisp}"
SERVICES=("postgres_registry" "api" "import" "semantic" "openwebui")

info() { printf "==> %s\n" "$*"; }

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    printf "❌ Command '%s' is required but not found. Aborting.\n" "$1" >&2
    exit 1
  fi
}

copy_if_missing() {
  local src="$1" dst="$2"
  if [[ -f "$dst" ]]; then
    info "Skipping $dst (already exists)"
  else
    cp "$src" "$dst"
  fi
}

info "Checking prerequisites (apt, sudo)"
require_command sudo
if ! command -v apt >/dev/null 2>&1 && ! command -v apt-get >/dev/null 2>&1; then
  printf "❌ This bootstrap script is intended for Debian/Ubuntu with apt.\n" >&2
  exit 1
fi

needs_install=false
for cmd in docker "docker compose" git; do
  if ! command -v ${cmd%% *} >/dev/null 2>&1; then
    needs_install=true
    break
  fi
done

if "$needs_install"; then
  info "Installing Docker, Compose plugin and Git (sudo password may be required)"
  sudo apt update
  sudo apt install -y docker.io docker-compose-plugin git
  sudo usermod -aG docker "$USER" || true
else
  info "Docker, Compose and Git already installed. Skipping apt install."
fi

if [[ -d "$TARGET_DIR/.git" ]]; then
  info "Directory '$TARGET_DIR' already exists. Reusing existing clone."
else
  info "Cloning repository into $TARGET_DIR"
  git clone "$REPO_URL" "$TARGET_DIR"
fi

cd "$TARGET_DIR"

info "Preparing configuration files"
copy_if_missing ".env.example" ".env"
copy_if_missing "services/semantic/synonyms.example.json" "services/semantic/synonyms.json"
mkdir -p services/openwebui/data
copy_if_missing "services/openwebui/webui.db.example" "services/openwebui/data/webui.db"

info "Restoring starter dump via docker compose (profile starter)"
sudo docker compose run --rm --profile starter starter-dump

info "Starting services: ${SERVICES[*]}"
sudo docker compose up -d --build "${SERVICES[@]}"

cat <<'EOF'
🎉 GISP stack is up and running.
- Open http://localhost:3000 for OpenWebUI.
- If 'docker' still requires sudo, log out and back in to refresh group membership.
EOF
