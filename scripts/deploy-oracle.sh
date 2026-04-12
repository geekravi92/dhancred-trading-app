#!/usr/bin/env bash
set -euo pipefail

SSH_TARGET="${SSH_TARGET:-ubuntu@141.148.222.161}"
SSH_KEY="${SSH_KEY:-$HOME/.ssh/oci_trading_a1}"
REMOTE_DIR="${REMOTE_DIR:-/home/ubuntu/dhancred-trading-app}"
PACKAGE_TAR="${PACKAGE_TAR:-target/deploy/dhancred-trading-app-linux-amd64.tar.gz}"

if [[ ! -f "$PACKAGE_TAR" ]]; then
  echo "Package not found: $PACKAGE_TAR" >&2
  echo "Run scripts/build-linux-amd64.sh first." >&2
  exit 1
fi

ssh -i "$SSH_KEY" "$SSH_TARGET" "mkdir -p '$REMOTE_DIR'"
scp -i "$SSH_KEY" "$PACKAGE_TAR" "$SSH_TARGET:$REMOTE_DIR/package.tar.gz"
ssh -i "$SSH_KEY" "$SSH_TARGET" "cd '$REMOTE_DIR' && tar -xzf package.tar.gz --strip-components=1 && rm package.tar.gz && mkdir -p data runtime/secrets"

cat <<EOF
Deployed to $SSH_TARGET:$REMOTE_DIR

Run directly on server:
  cd $REMOTE_DIR
  ./dhancred-trading-app

Install systemd service on server:
  sudo cp $REMOTE_DIR/deploy/systemd/dhancred-trading-app.service /etc/systemd/system/dhancred-trading-app.service
  sudo systemctl daemon-reload
  sudo systemctl enable --now dhancred-trading-app

Note: .env is not copied by this script. Keep secrets on the server only.
Use systemd for restart-on-failure. Do not run Docker/Podman on the server for this app.
EOF
