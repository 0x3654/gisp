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

# Auto-select optimal semantic image based on host architecture
ARCH=$(uname -m)
info "Detected architecture: $ARCH"
# ONNX is used by default in compose.yaml for both architectures
if [[ "$ARCH" == "aarch64" ]]; then
  info "Using ARM64 image with ONNX Runtime backend (optimized for Apple Silicon/ARM)"
else
  info "Using AMD64 image with ONNX Runtime backend (optimized for Intel/AMD)"
fi

info "Restoring starter dump via docker compose (profile starter)"
sudo env COMPOSE_PROFILES=starter COMPOSE_INTERACTIVE_NO_CLI=1 docker compose run -T --rm starter-dump </dev/null

info "Cleaning up starter image to free space"
mapfile -t starter_images < <(sudo docker images --filter "label=org.opencontainers.image.title=gisp-starter" --format '{{.ID}} {{.Repository}}:{{.Tag}}')

if ((${#starter_images[@]} == 0)); then
  info "No gisp-starter images found. Skipping cleanup."
else
  starter_image_ids=()
  starter_image_refs=()
  for entry in "${starter_images[@]}"; do
    starter_image_ids+=("${entry%% *}")
    starter_image_refs+=("${entry#* }")
  done
  info "Removing starter image(s): ${starter_image_refs[*]}"
  if ! sudo docker rmi "${starter_image_ids[@]}" >/dev/null 2>&1; then
    printf "⚠️ Failed to remove gisp-starter images (they might be in use).\n" >&2
  fi
fi

info "Starting services: ${SERVICES[*]}"
sudo docker compose up -d --build "${SERVICES[@]}"

cat <<'EOF'
🎉 GISP stack is up and running.
- Open http://localhost:3333 for OpenWebUI.
login: admin@gisp.ru pass: 123
EOF
