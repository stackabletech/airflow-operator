//! Renders the logging config files (`log_config.py` and the Vector agent config)
//! assembled into the rolegroup `ConfigMap`.

use std::{cmp, fmt::Write};

use stackable_operator::{
    commons::product_image_selection::ResolvedProductImage,
    product_logging::spec::{AutomaticContainerLogConfig, LogLevel},
    v2::product_logging::framework::ValidatedContainerLogConfigChoice,
};

/// The rotating log file the generated `log_config.py` writes to (consumed by the Vector agent).
const LOG_FILE: &str = "airflow.py.json";

/// The logger whose configured level drives the `task` handler.
///
/// Airflow keeps the task logs the web UI displays on a dedicated `task` handler, which has no
/// appender of its own in [`AutomaticContainerLogConfig`]. Its level is therefore taken from the
/// level configured for this logger.
const TASK_LOGGER: &str = "airflow.task";

/// The Vector agent configuration (`vector.yaml`).
const VECTOR_CONFIG: &str = include_str!("vector.yaml");

/// Returns the Vector agent config (`vector.yaml`) content.
pub fn vector_config_file_content() -> String {
    VECTOR_CONFIG.to_owned()
}

/// Renders `log_config.py` for the product container.
///
/// Returns `None` when the product container does not use the operator's automatic logging
/// configuration (i.e. a custom log ConfigMap is referenced instead), in which case no
/// `log_config.py` should be added to the rolegroup `ConfigMap`.
pub fn create_airflow_config(
    product_container: &ValidatedContainerLogConfigChoice,
    log_dir: &str,
    resolved_product_image: &ResolvedProductImage,
) -> Option<String> {
    let ValidatedContainerLogConfigChoice::Automatic(log_config) = product_container else {
        return None;
    };

    let config = if resolved_product_image.product_version.starts_with("2.")
        || resolved_product_image.product_version.starts_with("3.0.")
    {
        create_airflow_stdlib_config(log_config, log_dir, resolved_product_image)
    } else {
        create_airflow_structlog_config(log_config, log_dir)
    };

    Some(config)
}

fn create_airflow_stdlib_config(
    log_config: &AutomaticContainerLogConfig,
    log_dir: &str,
    resolved_product_image: &ResolvedProductImage,
) -> String {
    let loggers_config = log_config
        .loggers
        .iter()
        // The task logger is rendered explicitly below, because its level also drives the
        // `task` handler and must not simply be assigned to the logger.
        .filter(|(name, _)| {
            name.as_str() != AutomaticContainerLogConfig::ROOT_LOGGER
                && name.as_str() != TASK_LOGGER
        })
        .fold(String::new(), |mut output, (name, config)| {
            let _ = writeln!(
                output,
                "
LOGGING_CONFIG['loggers'].setdefault('{name}', {{ 'propagate': True }})
LOGGING_CONFIG['loggers']['{name}']['level'] = {level}
",
                level = config.level.to_python_expression()
            );
            output
        });

    let remote_task_log = if resolved_product_image.product_version.starts_with("2.") {
        ""
    } else {
        "
# This will cause the relevant RemoteLogIO handler to be initialized
REMOTE_TASK_LOG = airflow_local_settings.REMOTE_TASK_LOG
log = logging.getLogger(__name__)
log.info('Custom logging remote task log %s', REMOTE_TASK_LOG)
"
    };

    format!(
        "\
import logging
import os
from copy import deepcopy
from airflow.config_templates import airflow_local_settings

os.makedirs('{log_dir}', exist_ok=True)

LOGGING_CONFIG = deepcopy(airflow_local_settings.DEFAULT_LOGGING_CONFIG)
{remote_task_log}

LOGGING_CONFIG.setdefault('loggers', {{}})
for logger_name, logger_config in LOGGING_CONFIG['loggers'].items():
    logger_config['level'] = logging.NOTSET
    # Do not change the setting of the airflow.task logger because
    # otherwise DAGs cannot be loaded anymore.
    if logger_name != 'airflow.task':
        logger_config['propagate'] = True
    # The default behavior of airflow is to enforce log level 'INFO' on tasks. (https://airflow.apache.org/docs/apache-airflow/stable/configurations-ref.html#logging-level)
    # Records are filtered by the task handler below rather than here, so this level is
    # only lowered, never raised. A logger drops records before any handler sees them, so
    # a level above INFO would starve the console and file handlers as well.
    if 'handlers' in logger_config and 'task' in logger_config['handlers']:
        logger_config['level'] = {task_logger_level}

LOGGING_CONFIG.setdefault('formatters', {{}})
LOGGING_CONFIG['formatters']['json'] = {{
    '()': 'airflow.utils.log.json_formatter.JSONFormatter',
    'json_fields': ['asctime', 'levelname', 'message', 'name']
}}

LOGGING_CONFIG.setdefault('handlers', {{}})
LOGGING_CONFIG['handlers'].setdefault('console', {{}})
LOGGING_CONFIG['handlers']['console']['level'] = {console_log_level}
LOGGING_CONFIG['handlers']['file'] = {{
    'class': 'logging.handlers.RotatingFileHandler',
    'level': {file_log_level},
    'formatter': 'json',
    'filename': '{log_dir}/{LOG_FILE}',
    'maxBytes': 1048576,
    'backupCount': 1,
}}
LOGGING_CONFIG['handlers'].setdefault('task', {{}})
LOGGING_CONFIG['handlers']['task']['level'] = {task_log_level}

LOGGING_CONFIG['root'] = {{
    'level': {root_log_level},
    'filters': ['mask_secrets'],
    'handlers': ['console', 'file'],
}}
{loggers_config}",
        root_log_level = log_config.root_log_level().to_python_expression(),
        console_log_level = log_config
            .console
            .as_ref()
            .and_then(|console| console.level)
            .unwrap_or_default()
            .to_python_expression(),
        file_log_level = log_config
            .file
            .as_ref()
            .and_then(|file| file.level)
            .unwrap_or_default()
            .to_python_expression(),
        task_log_level = task_log_level(log_config).to_python_expression(),
        task_logger_level = task_logger_level(log_config).to_python_expression(),
    )
}

/// The log level for the `task` handler, which is what the Airflow web UI displays.
///
/// Taken from the level configured for the [`TASK_LOGGER`], and defaults to `INFO`, which is
/// also Airflow's own default, when that logger is not configured.
fn task_log_level(log_config: &AutomaticContainerLogConfig) -> LogLevel {
    log_config
        .loggers
        .get(TASK_LOGGER)
        .map(|logger| logger.level)
        .unwrap_or(LogLevel::INFO)
}

/// The level to set on the [`TASK_LOGGER`] itself.
///
/// Airflow pins it to `INFO`. A logger discards records before any handler can filter them, so
/// asking for a level below `INFO` has to open the logger up as well, otherwise the handler
/// would never see the records. Asking for a level above `INFO` must *not* raise the logger,
/// because the console and file handlers still want those records, so the logger is left at
/// `INFO` and the `task` handler filters on its own.
fn task_logger_level(log_config: &AutomaticContainerLogConfig) -> LogLevel {
    cmp::min(LogLevel::INFO, task_log_level(log_config))
}

fn create_airflow_structlog_config(
    log_config: &AutomaticContainerLogConfig,
    log_dir: &str,
) -> String {
    let loggers_config = log_config
        .loggers
        .iter()
        // The task logger is rendered explicitly below, because its level also drives the
        // `task` handler and must not simply be assigned to the logger.
        .filter(|(name, _)| {
            name.as_str() != AutomaticContainerLogConfig::ROOT_LOGGER
                && name.as_str() != TASK_LOGGER
        })
        .fold(String::new(), |mut output, (name, config)| {
            let _ = writeln!(
                output,
                "
LOGGING_CONFIG['loggers'].setdefault('{name}', {{ 'propagate': True }})
LOGGING_CONFIG['loggers']['{name}']['level'] = {level}
",
                level = config.level.to_python_expression()
            );
            output
        });

    format!(
        "\
import logging
import os
from airflow.config_templates import airflow_local_settings
from airflow.configuration import conf

os.makedirs('{log_dir}', exist_ok=True)

LOGGING_CONFIG = {{
    'filters': {{
        'mask_secrets_core': {{
            '()': 'airflow._shared.secrets_masker._secrets_masker',
        }}
    }},
    'formatters': {{
        'airflow': {{
            'format': '%(asctime)s logLevel=%(levelname)s logger=%(name)s - %(message)s',
            'class': 'airflow.utils.log.timezone_aware.TimezoneAware',
        }},
        'json': {{
            '()': 'airflow.utils.log.json_formatter.JSONFormatter',
            'json_fields': ['asctime', 'levelname', 'message', 'name']
        }}
    }},
    'handlers': {{
        'default': {{
            'level': {console_log_level}
        }},
        'file': {{
            'class': 'logging.handlers.RotatingFileHandler',
            'level': {file_log_level},
            'formatter': 'json',
            'filename': '{log_dir}/{LOG_FILE}',
            'maxBytes': 1048576,
            'backupCount': 1
        }},
        'task': {{
            'class': 'airflow.utils.log.file_task_handler.FileTaskHandler',
            'level': {task_log_level},
            'formatter': 'airflow',
            # `serve_logs` on the workers serves task logs from this directory, so it must be
            # the folder the Task SDK writes task logs to, not the Vector agent log directory.
            'base_log_folder': os.path.expanduser(conf.get('logging', 'BASE_LOG_FOLDER')),
            'filters': ['mask_secrets_core']
        }}
    }},
    'loggers': {{
        'airflow.task': {{
            'handlers': ['task'],
            'level': {task_logger_level},
            'propagate': True,
            'filters': ['mask_secrets_core']
        }}
    }},
    'root': {{
        'handlers': ['default', 'file'],
        'level': {root_log_level},
        'propagate': True
    }}
}}
{loggers_config}
REMOTE_TASK_LOG = airflow_local_settings.REMOTE_TASK_LOG
",
        console_log_level = log_config
            .console
            .as_ref()
            .and_then(|console| console.level)
            .unwrap_or_default()
            .to_python_expression(),
        file_log_level = log_config
            .file
            .as_ref()
            .and_then(|file| file.level)
            .unwrap_or_default()
            .to_python_expression(),
        root_log_level = log_config.root_log_level().to_python_expression(),
        task_log_level = task_log_level(log_config).to_python_expression(),
        task_logger_level = task_logger_level(log_config).to_python_expression(),
    )
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use stackable_operator::product_logging::spec::{LogLevel, LoggerConfig};

    use super::*;

    fn resolved_image(product_version: &str) -> ResolvedProductImage {
        ResolvedProductImage {
            product_version: product_version.to_string(),
            app_version_label_value: product_version.parse().expect("valid label value"),
            image: format!("oci.example.org/sdp/airflow:{product_version}-stackable0.0.0-dev"),
            image_pull_policy: "IfNotPresent".to_string(),
            pull_secrets: None,
        }
    }

    /// The Vector agent tails `{log_dir}/airflow.py.json` (see the `files_py` source in
    /// `vector.yaml`), so every generated log config must create the log directory and write
    /// the rotating JSON log file there.
    #[test]
    fn test_vector_log_file() {
        let log_config = AutomaticContainerLogConfig::default();

        for content in [
            create_airflow_stdlib_config(
                &log_config,
                "/stackable/log/airflow",
                &resolved_image("3.0.6"),
            ),
            create_airflow_structlog_config(&log_config, "/stackable/log/airflow"),
        ] {
            assert!(content.contains("os.makedirs('/stackable/log/airflow', exist_ok=True)"));
            assert!(content.contains("'filename': '/stackable/log/airflow/airflow.py.json'"));
        }
    }

    /// Only the last version line before the stdlib/structlog switch gets the stdlib config;
    /// all later (including future) versions must get the structlog one.
    #[test]
    fn test_logging_variant_selection() {
        // The stdlib config copies Airflow's default logging config, the structlog one
        // defines its own `mask_secrets_core` filter.
        let log_config =
            ValidatedContainerLogConfigChoice::Automatic(AutomaticContainerLogConfig::default());
        let stdlib_content = create_airflow_config(
            &log_config,
            "/stackable/log/airflow",
            &resolved_image("3.0.6"),
        )
        .expect("automatic log config produces content");
        let structlog_content = create_airflow_config(
            &log_config,
            "/stackable/log/airflow",
            &resolved_image("3.1.6"),
        )
        .expect("automatic log config produces content");
        assert!(stdlib_content.contains("deepcopy(airflow_local_settings.DEFAULT_LOGGING_CONFIG)"));
        assert!(structlog_content.contains("mask_secrets_core"));
    }

    #[test]
    fn test_structlog_task_log_folder() {
        let log_config = AutomaticContainerLogConfig::default();

        let content = create_airflow_structlog_config(&log_config, "/stackable/log/airflow");

        // `serve_logs` on the workers serves task logs from the `task` handler's
        // `base_log_folder`, so it must point to the folder the Task SDK writes task logs to
        // (`[logging] base_log_folder`), not to the Vector agent log directory.
        assert!(content.contains(
            "'base_log_folder': os.path.expanduser(conf.get('logging', 'BASE_LOG_FOLDER'))"
        ));
        assert!(!content.contains("'base_log_folder': '/stackable/log/airflow'"));
        // The generated config must import `conf` itself.
        assert!(content.contains("from airflow.configuration import conf"));
    }

    #[test]
    fn test_vector_config_file_content() {
        let content = vector_config_file_content();
        assert!(!content.is_empty());
        // Airflow logs JSON to `airflow.py.json`, so the Python-JSON source must be present.
        assert!(content.contains("files_py"));
        assert!(content.contains("*.py.json"));
        // The config is env-var-parameterized (resolved at runtime by the Vector container), not
        // baked, so the role-group identity must appear as placeholders.
        assert!(content.contains("${ROLE_NAME}"));
        assert!(content.contains("${VECTOR_AGGREGATOR_ADDRESS}"));
    }

    fn log_config_with_task_logger(level: Option<LogLevel>) -> AutomaticContainerLogConfig {
        let mut loggers = BTreeMap::new();

        if let Some(level) = level {
            loggers.insert(TASK_LOGGER.to_string(), LoggerConfig { level });
        }

        AutomaticContainerLogConfig {
            loggers,
            console: None,
            file: None,
        }
    }

    fn stdlib_config(log_config: &AutomaticContainerLogConfig) -> String {
        create_airflow_stdlib_config(log_config, "/stackable/log", &resolved_image("2.10.0"))
    }

    /// The requested level paired with the `task` handler level and the `airflow.task` logger
    /// level that it produces, for every level a user can configure.
    ///
    /// The two are deliberately listed together, because the relationship between them is not
    /// symmetric and that is the part which is easy to get wrong:
    ///
    /// - At or above `INFO` the handler follows the request and the logger stays at `INFO`. Only
    ///   the UI goes quiet; the console and file handlers keep receiving the records it drops.
    /// - Below `INFO` the two are coupled. A logger discards records before any of its handlers
    ///   can filter them, so the logger has to be opened up too, which means the extra records
    ///   reach every destination and not just the UI. Lowering the UI on its own is therefore
    ///   not possible, whichever way this is implemented.
    const TASK_LEVELS: [(LogLevel, &str, &str); 7] = [
        // requested        task handler        airflow.task logger
        (LogLevel::TRACE, "logging.DEBUG", "logging.DEBUG"),
        (LogLevel::DEBUG, "logging.DEBUG", "logging.DEBUG"),
        (LogLevel::INFO, "logging.INFO", "logging.INFO"),
        (LogLevel::WARN, "logging.WARNING", "logging.INFO"),
        (LogLevel::ERROR, "logging.ERROR", "logging.INFO"),
        (LogLevel::FATAL, "logging.CRITICAL", "logging.INFO"),
        (LogLevel::NONE, "logging.CRITICAL + 1", "logging.INFO"),
    ];

    // Spells out the handler/logger pair for every configurable level in one place, so the
    // asymmetry documented on TASK_LEVELS is visible instead of being spread across cases.
    #[test]
    fn test_stdlib_config_task_handler_and_logger_levels() {
        for (requested, handler_level, logger_level) in TASK_LEVELS {
            let content = stdlib_config(&log_config_with_task_logger(Some(requested)));

            assert!(
                content.contains(&format!(
                    "LOGGING_CONFIG['handlers']['task']['level'] = {handler_level}"
                )),
                "{requested} must put {handler_level} on the task handler"
            );
            assert!(
                content.contains(&format!("logger_config['level'] = {logger_level}")),
                "{requested} must put {logger_level} on the airflow.task logger"
            );
        }
    }

    #[test]
    fn test_structlog_config_task_handler_and_logger_levels() {
        for (requested, handler_level, logger_level) in TASK_LEVELS {
            let content = create_airflow_structlog_config(
                &log_config_with_task_logger(Some(requested)),
                "/stackable/log",
            );

            assert_eq!(
                level_in_block(&content, "'task': {"),
                handler_level,
                "{requested} must put {handler_level} on the task handler"
            );
            assert_eq!(
                level_in_block(&content, "'airflow.task': {"),
                logger_level,
                "{requested} must put {logger_level} on the airflow.task logger"
            );
        }
    }

    // Lowering the task level cannot quieten or open up the UI alone: the logger moves with it,
    // so the console and file handlers see the extra records as well. This is the coupling the
    // docs and the changelog describe, pinned here so it cannot change unnoticed.
    #[test]
    fn test_task_level_below_info_is_coupled_to_the_logger() {
        for requested in [LogLevel::TRACE, LogLevel::DEBUG] {
            let log_config = log_config_with_task_logger(Some(requested));

            assert_eq!(task_log_level(&log_config), requested);
            assert_eq!(
                task_logger_level(&log_config),
                requested,
                "{requested} is below INFO, so the logger must be opened up to match the handler"
            );
        }
    }

    // At or above INFO the handler and the logger diverge, which is what lets the UI be
    // quietened without starving the console and file handlers.
    #[test]
    fn test_task_level_above_info_leaves_the_logger_at_info() {
        for requested in [
            LogLevel::WARN,
            LogLevel::ERROR,
            LogLevel::FATAL,
            LogLevel::NONE,
        ] {
            let log_config = log_config_with_task_logger(Some(requested));

            assert_eq!(task_log_level(&log_config), requested);
            assert_eq!(
                task_logger_level(&log_config),
                LogLevel::INFO,
                "{requested} must not raise the logger, or other handlers lose records"
            );
        }
    }

    // Nothing configured must land on Airflow's own default rather than on whatever the
    // clamping happens to produce.
    #[test]
    fn test_stdlib_config_defaults_task_handler_to_info() {
        let content = stdlib_config(&log_config_with_task_logger(None));

        assert!(content.contains("LOGGING_CONFIG['handlers']['task']['level'] = logging.INFO"));
        assert!(content.contains("logger_config['level'] = logging.INFO"));
    }

    // The airflow.task entry drives the handler, so the generic loggers block must not also
    // emit it. If it did, it would assign the requested level straight to the logger and undo
    // the clamping above.
    #[test]
    fn test_stdlib_config_does_not_emit_the_task_logger_generically() {
        let content = stdlib_config(&log_config_with_task_logger(Some(LogLevel::ERROR)));

        assert!(!content.contains("LOGGING_CONFIG['loggers']['airflow.task']"));
    }

    #[test]
    fn test_structlog_config_does_not_emit_the_task_logger_generically() {
        let content = create_airflow_structlog_config(
            &log_config_with_task_logger(Some(LogLevel::ERROR)),
            "/stackable/log",
        );

        assert!(!content.contains("LOGGING_CONFIG['loggers']['airflow.task']"));
    }

    /// Returns the level assigned inside the block starting at `block`, e.g. the `task`
    /// handler or the `airflow.task` logger, so the two cannot be confused for each other.
    fn level_in_block(content: &str, block: &str) -> String {
        let start = content
            .find(block)
            .unwrap_or_else(|| panic!("the {block} block must be present"));
        let rest = &content[start..];
        let level_at = rest.find("'level': ").expect("the block must set a level");

        rest[level_at + "'level': ".len()..]
            .split([',', '\n'])
            .next()
            .expect("the level must be terminated")
            .trim()
            .to_string()
    }

    #[test]
    fn test_structlog_config_defaults_task_handler_to_info() {
        let content =
            create_airflow_structlog_config(&log_config_with_task_logger(None), "/stackable/log");

        assert_eq!(level_in_block(&content, "'task': {"), "logging.INFO");
        assert_eq!(
            level_in_block(&content, "'airflow.task': {"),
            "logging.INFO"
        );
    }
}
