#!/usr/bin/env bash

set -euo pipefail

REPO_URL="https://github.com/0x3654/gisp.git"
TARGET_DIR="${GISP_TARGET_DIR:-gisp}"

info() { printf "==> %s\n" "$*"; }
warn() { printf "⚠️  %s\n" "$*" >&2; }
die()  { printf "❌ %s\n" "$*" >&2; exit 1; }

require_command() {
  command -v "$1" >/dev/null 2>&1 || die "Command '$1' is required but not found."
}

copy_if_missing() {
  local src="$1" dst="$2"
  if [[ -f "$dst" ]]; then
    info "Skipping $dst (already exists)"
  else
    cp "$src" "$dst"
    info "Created $dst"
  fi
}

# ── Prerequisites ──────────────────────────────────────────────────────────────
info "Checking prerequisites"
require_command sudo
command -v apt-get >/dev/null 2>&1 || die "This bootstrap script requires a Debian/Ubuntu system with apt."

needs_install=false
for cmd in docker git; do
  command -v "$cmd" >/dev/null 2>&1 || { needs_install=true; break; }
done
# Check compose plugin separately — 'docker' may exist without the plugin
docker compose version >/dev/null 2>&1 || needs_install=true

if $needs_install; then
  info "Installing Docker (Compose plugin) and Git — sudo password may be required"
  sudo apt-get update -qq
  sudo apt-get install -y docker.io docker-compose-plugin git
  sudo usermod -aG docker "$USER" || true
else
  info "Docker, Compose plugin and Git already installed — skipping."
fi

# ── Clone ──────────────────────────────────────────────────────────────────────
if [[ -d "$TARGET_DIR/.git" ]]; then
  info "Directory '$TARGET_DIR' already exists — reusing existing clone."
else
  info "Cloning repository (sparse, depth=1 — production files only)"
  git clone --depth 1 --filter=blob:none --sparse "$REPO_URL" "$TARGET_DIR"
  # services/init  — postgres init SQL (schemas, extensions); required for first-run DB init
  # services/semantic — synonyms config volume-mounted into the semantic container
  # services/openwebui — OpenWebUI persistent data
  git -C "$TARGET_DIR" sparse-checkout set \
    compose.yaml \
    .env.example \
    services/init \
    services/semantic \
    services/openwebui
fi

cd "$TARGET_DIR"

# ── Configuration ──────────────────────────────────────────────────────────────
info "Preparing configuration files"
if [[ ! -f .env ]]; then
  perl -pe "s/\{\{[^|}]+\|\s*default\('([^']*)'\)\s*\}\}/\$1/g" .env.example > .env
  info "Created .env from .env.example — review and adjust secrets before production use"
fi
copy_if_missing "services/semantic/synonyms.example.json" "services/semantic/synonyms.json"
mkdir -p services/openwebui/data
copy_if_missing "services/openwebui/webui.db.example" "services/openwebui/data/webui.db"

# ── Architecture ───────────────────────────────────────────────────────────────
ARCH=$(uname -m)
info "Detected architecture: $ARCH — both AMD64 and ARM64 use the ONNX Runtime image"

# ── Starter DB dump ────────────────────────────────────────────────────────────
info "Restoring starter dump (profile: starter)"
sudo env COMPOSE_PROFILES=starter COMPOSE_INTERACTIVE_NO_CLI=1 \
  docker compose run -T --rm starter-dump </dev/null

info "Cleaning up starter image to free disk space"
mapfile -t starter_images < <(
  sudo docker images \
    --filter "label=org.opencontainers.image.title=gisp-starter" \
    --format '{{.ID}} {{.Repository}}:{{.Tag}}'
)
if ((${#starter_images[@]} == 0)); then
  info "No gisp-starter images found — skipping cleanup."
else
  ids=()
  refs=()
  for entry in "${starter_images[@]}"; do
    ids+=("${entry%% *}")
    refs+=("${entry#* }")
  done
  info "Removing starter image(s): ${refs[*]}"
  sudo docker rmi "${ids[@]}" >/dev/null 2>&1 \
    || warn "Failed to remove gisp-starter images (may be in use)."
fi

# ── Start services ─────────────────────────────────────────────────────────────
info "Pulling and starting services"
sudo docker compose pull
sudo docker compose up -d

cat <<'EOF'

🎉 GISP stack is up and running.
   OpenWebUI  →  http://localhost:3333
   login:  admin@gisp.ru
   pass:   123
EOF
