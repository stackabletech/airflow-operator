//! Renders the config files (`webserver_config.py`, env vars, logging, …) assembled into the
//! rolegroup `ConfigMap`.

pub mod env_vars;
pub mod product_logging;
pub mod webserver_config;

/// The names of the Airflow config files assembled into the rolegroup `ConfigMap`.
///
/// This is the single source of truth for the on-disk file names; nothing else should hard-code
/// them (the Vector agent config is the exception).
#[derive(Clone, Copy, Debug, strum::Display)]
pub enum ConfigFileName {
    #[strum(serialize = "webserver_config.py")]
    WebserverConfig,
    #[strum(serialize = "log_config.py")]
    LogConfig,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_names_match_the_airflow_on_disk_names() {
        assert_eq!(
            ConfigFileName::WebserverConfig.to_string(),
            "webserver_config.py"
        );
        assert_eq!(ConfigFileName::LogConfig.to_string(), "log_config.py");
    }
}
