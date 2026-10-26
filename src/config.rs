use serde::{Deserialize, Serialize};
use std::{
    net::SocketAddr,
    num::NonZeroUsize,
    path::{Path, PathBuf},
};

use crate::errors::ConfigError;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ServerConfig {
    pub endpoint: SocketAddr,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct FetcherConfig {
    pub concurrent_downloads: NonZeroUsize,
    pub save_directory: PathBuf,
    pub fetch_interval: u64,
    #[serde(default)]
    pub verify_crc: bool,
    #[serde(default)]
    pub allowed_hosts: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PatchConfig {
    pub host: String,
    pub port: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DebugConfig {
    pub level: Option<String>,
    pub file_logging: Option<bool>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DBConfig {
    pub path: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AppConfig {
    pub server: ServerConfig,
    pub fetcher: FetcherConfig,
    pub patch: PatchConfig,
    pub database: DBConfig,
    pub debug: Option<DebugConfig>,
}

impl AppConfig {
    pub fn load() -> Result<Self, ConfigError> {
        Self::load_from(Path::new("config.toml"))
    }

    fn load_from(path: &Path) -> Result<Self, ConfigError> {
        if let Ok(content) = std::fs::read_to_string(path) {
            let config: Self = toml::from_str(&content).map_err(ConfigError::ParseError)?;
            return config.with_absolute_save_directory(path);
        }

        let default_config = AppConfig::default();
        let toml_string =
            toml::to_string_pretty(&default_config).map_err(ConfigError::SerializeError)?;
        std::fs::write(path, &toml_string).map_err(ConfigError::PathError)?;

        default_config.with_absolute_save_directory(path)
    }

    fn with_absolute_save_directory(mut self, config_path: &Path) -> Result<Self, ConfigError> {
        let config_path = std::path::absolute(config_path).map_err(ConfigError::PathError)?;
        let config_dir = config_path.parent().unwrap_or(&config_path);
        self.fetcher.save_directory = config_dir.join(&self.fetcher.save_directory);
        Ok(self)
    }
}

impl Default for AppConfig {
    fn default() -> Self {
        AppConfig {
            server: ServerConfig {
                endpoint: SocketAddr::from(([127, 0, 0, 1], 12369)),
            },
            patch: PatchConfig {
                host: "patch.us.wizard101.com".to_string(),
                port: "12500".to_string(),
            },
            fetcher: FetcherConfig {
                concurrent_downloads: unsafe { NonZeroUsize::new_unchecked(2) },
                fetch_interval: 60 * 60 * 8,
                save_directory: PathBuf::from("data"),
                verify_crc: false,
                allowed_hosts: Vec::new(),
            },
            database: DBConfig {
                path: "aurorium.db".to_string(),
            },
            debug: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::AppConfig;

    const RELATIVE_CONFIG: &str = r#"
[server]
endpoint = "127.0.0.1:12369"
[fetcher]
concurrent_downloads = 2
save_directory = "data"
fetch_interval = 60
[patch]
host = "patch.example"
port = "12500"
[database]
path = "aurorium.db"
"#;

    #[test]
    fn relative_save_directory_resolves_against_config_dir() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("config.toml");
        std::fs::write(&path, RELATIVE_CONFIG).expect("write config");

        let config = AppConfig::load_from(&path).expect("load");

        assert_eq!(config.fetcher.save_directory, dir.path().join("data"));
    }

    #[test]
    fn absolute_save_directory_is_kept() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("config.toml");
        let absolute = dir.path().join("elsewhere");
        let content = RELATIVE_CONFIG.replace(
            "\"data\"",
            &format!("{:?}", absolute.to_str().expect("utf-8 path")),
        );
        std::fs::write(&path, content).expect("write config");

        let config = AppConfig::load_from(&path).expect("load");

        assert_eq!(config.fetcher.save_directory, absolute);
    }
}
