#!/usr/bin/env bash
set -euo pipefail

REPO="${GITHUB_REPOSITORY:-geekravi92/dhancred-trading-app}"
TAG="${RELEASE_TAG:-latest}"
ASSET_NAME="${ASSET_NAME:-dhancred-trading-app-linux-amd64.tar.gz}"
REMOTE_DIR="${REMOTE_DIR:-/home/ubuntu/dhancred-trading-app}"
SERVICE_NAME="${SERVICE_NAME:-dhancred-trading-app}"

if ! command -v curl >/dev/null 2>&1; then
  echo "Missing curl" >&2
  exit 1
fi

if ! command -v python3 >/dev/null 2>&1; then
  echo "Missing python3" >&2
  exit 1
fi

tmp_dir="$(mktemp -d)"
cleanup() {
  rm -rf "$tmp_dir"
}
trap cleanup EXIT

release_url="https://api.github.com/repos/$REPO/releases/tags/$TAG"
asset_url="$(
  curl --fail --silent --show-error --location "$release_url" |
    python3 -c '
import json
import sys

asset_name = sys.argv[1]
release = json.load(sys.stdin)
for asset in release.get("assets", []):
    if asset.get("name") == asset_name:
        print(asset["browser_download_url"])
        break
else:
    raise SystemExit(f"asset not found: {asset_name}")
  ' "$ASSET_NAME"
)"

curl --fail --show-error --location "$asset_url" --output "$tmp_dir/package.tar.gz"

mkdir -p "$REMOTE_DIR"
tar -xzf "$tmp_dir/package.tar.gz" -C "$REMOTE_DIR" --strip-components=1
mkdir -p "$REMOTE_DIR/data" "$REMOTE_DIR/runtime/secrets"
chmod +x "$REMOTE_DIR/dhancred-trading-app"

if command -v systemctl >/dev/null 2>&1 &&
  systemctl list-unit-files "$SERVICE_NAME.service" >/dev/null 2>&1; then
  sudo systemctl restart "$SERVICE_NAME"
  systemctl --no-pager --full status "$SERVICE_NAME" | sed -n '1,14p'
else
  echo "Installed release into $REMOTE_DIR"
  echo "Service $SERVICE_NAME.service not found; start manually if needed."
fi
