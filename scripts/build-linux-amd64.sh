#!/usr/bin/env bash
set -euo pipefail

PACKAGE_DIR="target/deploy/dhancred-trading-app"
PACKAGE_TAR="target/deploy/dhancred-trading-app-linux-amd64.tar.gz"
TARGET="x86_64-unknown-linux-gnu"
TARGET_BIN="target/$TARGET/release/dhancred-trading-app"

mkdir -p target/deploy

if ! rustup target list --installed | grep -qx "$TARGET"; then
  rustup target add "$TARGET"
fi

if [[ "$(uname -s)" == "Linux" && "$(uname -m)" == "x86_64" ]]; then
  cargo build --release --locked --target "$TARGET"
else
  if ! command -v zig >/dev/null 2>&1; then
    echo "Missing zig. Install once on Mac: brew install zig" >&2
    exit 1
  fi

  if ! cargo zigbuild --help >/dev/null 2>&1; then
    echo "Missing cargo-zigbuild. Install once on Mac: cargo install cargo-zigbuild" >&2
    exit 1
  fi

  cargo zigbuild --release --locked --target "$TARGET"
fi

rm -rf "$PACKAGE_DIR"
mkdir -p "$PACKAGE_DIR/config/instruments/delta" "$PACKAGE_DIR/config/instruments/fyers" "$PACKAGE_DIR/deploy/systemd"

cp "$TARGET_BIN" "$PACKAGE_DIR/dhancred-trading-app"
cp config/feeder.toml "$PACKAGE_DIR/config/feeder.toml"
cp config/strategy.toml "$PACKAGE_DIR/config/strategy.toml"
cp -R config/brokers "$PACKAGE_DIR/config/brokers"
cp config/instruments/delta/base_instruments.csv "$PACKAGE_DIR/config/instruments/delta/base_instruments.csv"
cp config/instruments/fyers/base_instruments.csv "$PACKAGE_DIR/config/instruments/fyers/base_instruments.csv"
cp deploy/systemd/dhancred-trading-app.service "$PACKAGE_DIR/deploy/systemd/dhancred-trading-app.service"

chmod +x "$PACKAGE_DIR/dhancred-trading-app"
tar -C target/deploy -czf "$PACKAGE_TAR" dhancred-trading-app

echo "$PACKAGE_TAR"
