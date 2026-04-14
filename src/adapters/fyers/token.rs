use std::env;
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

pub fn jwt_access_token_only(access_token: &str) -> Result<&str, FeedError> {
    let stripped = strip_bearer_prefix(access_token);
    let token = stripped
        .rsplit_once(':')
        .map(|(_, token)| token)
        .unwrap_or(stripped)
        .trim();
    if token.is_empty() {
        return Err(FeedError::Config(
            "FYERS access token cannot be empty".to_string(),
        ));
    }
    Ok(token)
}

pub fn history_authorization_header(
    access_token: &str,
    app_id_env: Option<&str>,
) -> Result<String, FeedError> {
    let stripped = strip_bearer_prefix(access_token);
    let app_id = if let Some(app_id_env) = app_id_env {
        env::var(app_id_env)
            .map_err(|_| FeedError::Config(format!("missing environment variable {app_id_env}")))?
    } else if let Ok(app_id) = env::var("FYERS_APP_ID") {
        app_id
    } else if let Some((app_id, _)) = stripped.rsplit_once(':') {
        app_id.to_string()
    } else {
        return Err(FeedError::Config(
            "missing FYERS app id for historical authorization".to_string(),
        ));
    };
    let jwt = jwt_access_token_only(stripped)?;
    Ok(format!("{}:{}", app_id.trim(), jwt))
}

fn strip_bearer_prefix(value: &str) -> &str {
    value
        .trim()
        .strip_prefix("Bearer ")
        .or_else(|| value.trim().strip_prefix("bearer "))
        .unwrap_or_else(|| value.trim())
        .trim()
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

    #[test]
    fn history_authorization_prefers_env_app_id_over_prefixed_token() {
        let key = "FYERS_TEST_APP_ID_FOR_HISTORY";
        unsafe {
            std::env::set_var(key, "HT4X8EWUF0-100");
        }
        let header =
            history_authorization_header("OLDAPP-100:jwt-token", Some(key)).expect("header");
        assert_eq!(header, "HT4X8EWUF0-100:jwt-token");
        unsafe {
            std::env::remove_var(key);
        }
    }
}
