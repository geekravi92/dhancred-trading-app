use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use rusqlite::Connection;

use crate::strategy::StrategyError;

#[derive(Clone, Debug)]
pub struct StrategySqlite {
    path: PathBuf,
}

impl StrategySqlite {
    pub fn new(path: impl Into<PathBuf>) -> Result<Self, StrategyError> {
        let sqlite = Self { path: path.into() };
        sqlite.ensure_schema()?;
        Ok(sqlite)
    }

    pub fn open_connection(&self) -> Result<Connection, StrategyError> {
        open_connection(&self.path)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn ensure_schema(&self) -> Result<(), StrategyError> {
        let connection = self.open_connection()?;
        connection.execute_batch(
            "\
            PRAGMA journal_mode = WAL;
            PRAGMA synchronous = NORMAL;
            CREATE TABLE IF NOT EXISTS strategy_ssu (
                ssu_id INTEGER PRIMARY KEY,
                strategy_key TEXT NOT NULL,
                enabled INTEGER NOT NULL DEFAULT 1,
                trade_gap_secs INTEGER NOT NULL DEFAULT 0,
                max_overlap INTEGER NOT NULL DEFAULT 1,
                max_positions_per_day INTEGER NOT NULL DEFAULT 1,
                required_timeframes_json TEXT NOT NULL DEFAULT '[]',
                indicator_specs_json TEXT NOT NULL DEFAULT '[]',
                params_json TEXT NOT NULL DEFAULT '{}'
            );
            CREATE TABLE IF NOT EXISTS virtual_position (
                position_id TEXT PRIMARY KEY,
                ssu_id INTEGER NOT NULL,
                trigger_instrument TEXT NOT NULL,
                trade_instrument TEXT NOT NULL,
                side TEXT NOT NULL,
                status TEXT NOT NULL,
                entry_price REAL NOT NULL,
                entry_at INTEGER NOT NULL,
                exit_price REAL,
                exit_at INTEGER,
                exit_reason TEXT,
                pnl REAL
            );
            CREATE INDEX IF NOT EXISTS idx_virtual_position_ssu_status
                ON virtual_position (ssu_id, status, entry_at);
            ",
        )?;
        Ok(())
    }
}

fn open_connection(path: &Path) -> Result<Connection, StrategyError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let connection = Connection::open(path).map_err(|error| {
        StrategyError::Io(format!(
            "failed to open strategy sqlite db {}: {error}",
            path.display()
        ))
    })?;
    connection.busy_timeout(Duration::from_secs(5)).map_err(|error| {
        StrategyError::Io(format!("failed to set strategy sqlite busy timeout: {error}"))
    })?;
    Ok(connection)
}
