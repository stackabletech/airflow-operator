use snafu::{OptionExt, ResultExt, Snafu};
use stackable_airflow_crd::{
    airflowdb::{self, AirflowDB},
    AirflowCluster, Container, STACKABLE_LOG_DIR,
};
use stackable_operator::{
    builder::ConfigMapBuilder,
    client::Client,
    k8s_openapi::api::core::v1::ConfigMap,
    product_logging::{
        self,
        spec::{
            AutomaticContainerLogConfig, ContainerLogConfig, ContainerLogConfigChoice, Logging,
        },
    },
    role_utils::RoleGroupRef,
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to retrieve the ConfigMap [{cm_name}]"))]
    ConfigMapNotFound {
        source: stackable_operator::error::Error,
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

const VECTOR_AGGREGATOR_CM_ENTRY: &str = "ADDRESS";
const LOG_CONFIG_FILE: &str = "log_config.py";
const LOG_FILE: &str = "airflow.py.json";

/// Return the address of the Vector aggregator if the corresponding ConfigMap name is given in the
/// cluster spec
pub async fn resolve_vector_aggregator_address(
    vector_aggregator_config_map_name: &str,
    namespace: &str,
    client: &Client,
) -> Result<String> {
    let vector_aggregator_address = client
        .get::<ConfigMap>(vector_aggregator_config_map_name, namespace)
        .await
        .context(ConfigMapNotFoundSnafu {
            cm_name: vector_aggregator_config_map_name.to_string(),
        })?
        .data
        .and_then(|mut data| data.remove(VECTOR_AGGREGATOR_CM_ENTRY))
        .context(MissingConfigMapEntrySnafu {
            entry: VECTOR_AGGREGATOR_CM_ENTRY,
            cm_name: vector_aggregator_config_map_name.to_string(),
        })?;
    Ok(vector_aggregator_address)
}

/// Extend the role group ConfigMap with logging and Vector configurations
pub fn extend_role_group_config_map(
    rolegroup: &RoleGroupRef<AirflowCluster>,
    vector_aggregator_address: Option<&str>,
    logging: &Logging<Container>,
    cm_builder: &mut ConfigMapBuilder,
) -> Result<()> {
    if let Some(ContainerLogConfig {
        choice: Some(ContainerLogConfigChoice::Automatic(log_config)),
    }) = logging.containers.get(&Container::Airflow)
    {
        let log_dir = format!(
            "{STACKABLE_LOG_DIR}/{container}",
            container = Container::Airflow
        );
        cm_builder.add_data(LOG_CONFIG_FILE, create_airflow_config(log_config, &log_dir));
    }

    let vector_log_config = if let Some(ContainerLogConfig {
        choice: Some(ContainerLogConfigChoice::Automatic(log_config)),
    }) = logging.containers.get(&Container::Vector)
    {
        Some(log_config)
    } else {
        None
    };

    if logging.enable_vector_agent {
        cm_builder.add_data(
            product_logging::framework::VECTOR_CONFIG_FILE,
            product_logging::framework::create_vector_config(
                rolegroup,
                vector_aggregator_address.context(MissingVectorAggregatorAddressSnafu)?,
                vector_log_config,
            ),
        );
    }

    Ok(())
}

/// Extend the ConfigMap with logging and Vector configurations
pub fn extend_init_db_config_map(
    rolegroup: &RoleGroupRef<AirflowDB>,
    vector_aggregator_address: Option<&str>,
    logging: &Logging<airflowdb::Container>,
    cm_builder: &mut ConfigMapBuilder,
) -> Result<()> {
    if let Some(ContainerLogConfig {
        choice: Some(ContainerLogConfigChoice::Automatic(log_config)),
    }) = logging.containers.get(&airflowdb::Container::AirflowInitDb)
    {
        let log_dir = format!(
            "{STACKABLE_LOG_DIR}/{container}",
            container = airflowdb::Container::AirflowInitDb
        );
        cm_builder.add_data(LOG_CONFIG_FILE, create_airflow_config(log_config, &log_dir));
    }

    let vector_log_config = if let Some(ContainerLogConfig {
        choice: Some(ContainerLogConfigChoice::Automatic(log_config)),
    }) = logging.containers.get(&airflowdb::Container::Vector)
    {
        Some(log_config)
    } else {
        None
    };

    if logging.enable_vector_agent {
        cm_builder.add_data(
            product_logging::framework::VECTOR_CONFIG_FILE,
            product_logging::framework::create_vector_config(
                rolegroup,
                vector_aggregator_address.context(MissingVectorAggregatorAddressSnafu)?,
                vector_log_config,
            ),
        );
    }

    Ok(())
}

fn create_airflow_config(log_config: &AutomaticContainerLogConfig, log_dir: &str) -> String {
    let loggers_config = log_config
        .loggers
        .iter()
        .filter(|(name, _)| name.as_str() != AutomaticContainerLogConfig::ROOT_LOGGER)
        .map(|(name, config)| {
            format!(
                "
LOGGING_CONFIG['loggers'].setdefault('{name}', {{ 'propagate': True }})
LOGGING_CONFIG['loggers']['{name}']['level'] = {level}
",
                level = config.level.to_python_expression()
            )
        })
        .collect::<String>();

    format!(
        "\
import logging
import os
from copy import deepcopy
from airflow.config_templates.airflow_local_settings import DEFAULT_LOGGING_CONFIG

os.makedirs('{log_dir}', exist_ok=True)

LOGGING_CONFIG = deepcopy(DEFAULT_LOGGING_CONFIG)

LOGGING_CONFIG.setdefault('loggers', {{}})
for logger_name, logger_config in LOGGING_CONFIG['loggers'].items():
    logger_config['level'] = logging.NOTSET
    # Do not change the setting of the airflow.task logger because
    # otherwise DAGs cannot be loaded anymore.
    if logger_name != 'airflow.task':
        logger_config['propagate'] == True

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
