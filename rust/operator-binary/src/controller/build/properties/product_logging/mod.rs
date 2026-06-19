//! Renders the logging config files (`log_config.py` and the Vector agent config)
//! assembled into the rolegroup `ConfigMap`.

use std::fmt::Write;

use stackable_operator::{
    commons::product_image_selection::ResolvedProductImage,
    product_logging::spec::AutomaticContainerLogConfig,
    v2::product_logging::framework::ValidatedContainerLogConfigChoice,
};

/// The rotating log file the generated `log_config.py` writes to (consumed by the Vector agent).
const LOG_FILE: &str = "airflow.py.json";

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
        .filter(|(name, _)| name.as_str() != AutomaticContainerLogConfig::ROOT_LOGGER)
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
    # TODO: Make task handler log level configurable through CRDs with default 'INFO'.
    # e.g. LOGGING_CONFIG['handlers']['task']['level'] = {{task_log_level}}
    if 'handlers' in logger_config and 'task' in logger_config['handlers']:
        logger_config['level'] = logging.INFO

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
    )
}

fn create_airflow_structlog_config(
    log_config: &AutomaticContainerLogConfig,
    log_dir: &str,
) -> String {
    let loggers_config = log_config
        .loggers
        .iter()
        .filter(|(name, _)| name.as_str() != AutomaticContainerLogConfig::ROOT_LOGGER)
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
            'formatter': 'airflow',
            'base_log_folder': '{log_dir}',
            'filters': ['mask_secrets_core']
        }}
    }},
    'loggers': {{
        'airflow.task': {{
            'handlers': ['task'],
            'level': logging.INFO,
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
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The vendored `vector.yaml` keeps only the sources Airflow produces (Python JSON, stdout,
    /// stderr) and drops the ones it does not (log4j/log4j2/airlift/opa/tracing). Guards against
    /// accidental drift.
    #[test]
    fn test_vector_config_file_content() {
        let content = vector_config_file_content();
        assert!(!content.is_empty());
        // Airflow logs JSON to `airflow.py.json`, so the Python-JSON source must be present.
        assert!(content.contains("files_py"));
        assert!(content.contains("*.py.json"));
        // Sources Airflow does not emit must have been trimmed out.
        for dropped in [
            "files_log4j",
            "files_log4j2",
            "files_airlift",
            "files_opa_json",
            "files_tracing_rs",
        ] {
            assert!(
                !content.contains(dropped),
                "vendored vector.yaml should not contain the dropped source {dropped}"
            );
        }
        // The config is env-var-parameterized (resolved at runtime by the Vector container), not
        // baked, so the role-group identity must appear as placeholders.
        assert!(content.contains("${ROLE_NAME}"));
        assert!(content.contains("${VECTOR_AGGREGATOR_ADDRESS}"));
    }
}
