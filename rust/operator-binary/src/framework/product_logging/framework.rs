use std::fmt::Display;

use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::product_logging::spec::{
    AutomaticContainerLogConfig, ConfigMapLogConfig, ContainerLogConfig, ContainerLogConfigChoice,
    CustomContainerLogConfig, Logging,
};
use strum::{EnumDiscriminants, IntoStaticStr};

use crate::framework::types::kubernetes::ConfigMapName;

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
pub enum Error {
    #[snafu(display("failed to get container log configuration for container {container}"))]
    GetContainerLogConfiguration { container: String },

    #[snafu(display("failed to parse ConfigMap name for custom log configuration"))]
    ParseConfigMapName {
        source: crate::framework::macros::attributed_string_type::Error,
    },
}

#[derive(Clone, Debug)]
pub enum ValidatedContainerLogConfigChoice {
    Automatic(AutomaticContainerLogConfig),
    Custom(ConfigMapName),
}

impl ValidatedContainerLogConfigChoice {
    /// Converts back to the raw upstream type for use at API boundaries
    /// (e.g. calling `product_logging::framework::vector_container`).
    pub fn to_raw_container_log_config(&self) -> ContainerLogConfig {
        match self {
            Self::Automatic(auto) => ContainerLogConfig {
                choice: Some(ContainerLogConfigChoice::Automatic(auto.clone())),
            },
            Self::Custom(name) => ContainerLogConfig {
                choice: Some(ContainerLogConfigChoice::Custom(CustomContainerLogConfig {
                    custom: ConfigMapLogConfig {
                        config_map: name.to_string(),
                    },
                })),
            },
        }
    }
}

#[derive(Clone, Debug)]
pub struct VectorContainerLogConfig {
    pub log_config: ValidatedContainerLogConfigChoice,
}

pub fn validate_logging_configuration_for_container<T>(
    logging: &Logging<T>,
    container: T,
) -> Result<ValidatedContainerLogConfigChoice, Error>
where
    T: Clone + Display + Ord,
{
    use std::str::FromStr;

    let config = logging
        .containers
        .get(&container)
        .and_then(|c| c.choice.as_ref())
        .context(GetContainerLogConfigurationSnafu {
            container: container.to_string(),
        })?;

    match config {
        ContainerLogConfigChoice::Automatic(automatic) => Ok(
            ValidatedContainerLogConfigChoice::Automatic(automatic.clone()),
        ),
        ContainerLogConfigChoice::Custom(custom) => {
            let config_map_name = ConfigMapName::from_str(&custom.custom.config_map)
                .context(ParseConfigMapNameSnafu)?;
            Ok(ValidatedContainerLogConfigChoice::Custom(config_map_name))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use stackable_operator::product_logging::spec::{
        AutomaticContainerLogConfig, ContainerLogConfig, ContainerLogConfigChoice, Logging,
    };

    use super::*;
    use crate::crd::Container;

    fn logging_with_automatic_config() -> Logging<Container> {
        let mut containers = BTreeMap::new();
        containers.insert(
            Container::Airflow,
            ContainerLogConfig {
                choice: Some(ContainerLogConfigChoice::Automatic(
                    AutomaticContainerLogConfig::default(),
                )),
            },
        );
        Logging {
            enable_vector_agent: false,
            containers,
        }
    }

    #[test]
    fn test_validate_automatic_log_config() {
        let logging = logging_with_automatic_config();
        let result = validate_logging_configuration_for_container(&logging, Container::Airflow);
        assert!(result.is_ok());
        assert!(matches!(
            result.unwrap(),
            ValidatedContainerLogConfigChoice::Automatic(_)
        ));
    }

    #[test]
    fn test_validate_missing_container_config() {
        let logging = logging_with_automatic_config();
        let result = validate_logging_configuration_for_container(&logging, Container::Vector);
        assert!(result.is_err());
    }
}
