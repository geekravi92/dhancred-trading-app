use std::fs;
use std::path::Path;

use crate::feeder::FeedError;

pub fn write_access_token(path: impl AsRef<Path>, token: &str) -> Result<(), FeedError> {
    let token = token.trim();
    if token.is_empty() {
        return Err(FeedError::Config(
            "FYERS access token cannot be empty".to_string(),
        ));
    }
    if token.len() > 8_192 {
        return Err(FeedError::Config(
            "FYERS access token is too large".to_string(),
        ));
    }

    let path = path.as_ref();
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| FeedError::Config("invalid FYERS access token path".to_string()))?;
    let tmp_path = path.with_file_name(format!("{file_name}.tmp"));

    fs::write(&tmp_path, format!("{token}\n"))?;
    lock_down_file_permissions(&tmp_path)?;
    fs::rename(tmp_path, path)?;

    Ok(())
}

#[cfg(unix)]
fn lock_down_file_permissions(path: &Path) -> Result<(), FeedError> {
    use std::os::unix::fs::PermissionsExt;

    fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    Ok(())
}

#[cfg(not(unix))]
fn lock_down_file_permissions(_path: &Path) -> Result<(), FeedError> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;

    #[test]
    fn writes_trimmed_access_token() {
        let path = std::env::temp_dir().join(format!(
            "dhancred-fyers-token-test-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system time")
                .as_nanos()
        ));

        write_access_token(&path, "  token-value  ").expect("write token");
        assert_eq!(
            fs::read_to_string(path).expect("read token"),
            "token-value\n"
        );
    }
}
