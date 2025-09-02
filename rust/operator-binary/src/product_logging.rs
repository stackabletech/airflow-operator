use std::fmt::{Display, Write};

use snafu::Snafu;
use stackable_operator::{
    builder::configmap::ConfigMapBuilder,
    kube::Resource,
    product_logging::{
        self,
        spec::{
            AutomaticContainerLogConfig, ContainerLogConfig, ContainerLogConfigChoice, Logging,
        },
    },
    role_utils::RoleGroupRef,
};

use crate::crd::STACKABLE_LOG_DIR;

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to retrieve the ConfigMap [{cm_name}]"))]
    ConfigMapNotFound {
        source: stackable_operator::client::Error,
        cm_name: String,
    },
    #[snafu(display("failed to retrieve the entry [{entry}] for ConfigMap [{cm_name}]"))]
    MissingConfigMapEntry {
        entry: &'static str,
        cm_name: String,
    },
    #[snafu(display("vectorAggregatorConfigMapName must be set"))]
    MissingVectorAggregatorAddress,
}

type Result<T, E = Error> = std::result::Result<T, E>;

const LOG_CONFIG_FILE: &str = "log_config.py";
const LOG_FILE: &str = "airflow.py.json";

/// Extend the ConfigMap with logging and Vector configurations
pub fn extend_config_map_with_log_config<C, K>(
    rolegroup: &RoleGroupRef<K>,
    logging: &Logging<C>,
    main_container: &C,
    vector_container: &C,
    cm_builder: &mut ConfigMapBuilder,
) -> Result<()>
where
    C: Clone + Ord + Display,
    K: Resource,
{
    if let Some(ContainerLogConfig {
        choice: Some(ContainerLogConfigChoice::Automatic(log_config)),
    }) = logging.containers.get(main_container)
    {
        let log_dir = format!("{STACKABLE_LOG_DIR}/{main_container}");
        cm_builder.add_data(LOG_CONFIG_FILE, create_airflow_config(log_config, &log_dir));
    }

    let vector_log_config = if let Some(ContainerLogConfig {
        choice: Some(ContainerLogConfigChoice::Automatic(log_config)),
    }) = logging.containers.get(vector_container)
    {
        Some(log_config)
    } else {
        None
    };

    if logging.enable_vector_agent {
        cm_builder.add_data(
            product_logging::framework::VECTOR_CONFIG_FILE,
            product_logging::framework::create_vector_config(rolegroup, vector_log_config),
        );
    }

    Ok(())
}

fn create_airflow_config(log_config: &AutomaticContainerLogConfig, log_dir: &str) -> String {
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
from copy import deepcopy
from airflow.config_templates.airflow_local_settings import DEFAULT_LOGGING_CONFIG

os.makedirs('{log_dir}', exist_ok=True)

LOGGING_CONFIG = deepcopy(DEFAULT_LOGGING_CONFIG)

REMOTE_TASK_LOG = None

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
