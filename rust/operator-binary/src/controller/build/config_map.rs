//! Builds the rolegroup [`ConfigMap`]: the rendered `webserver_config.py` plus the
//! logging/vector configuration.

use std::collections::BTreeMap;

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    builder::{configmap::ConfigMapBuilder, meta::ObjectMetaBuilder},
    k8s_openapi::api::core::v1::ConfigMap,
    product_logging::spec::Logging,
    role_utils::RoleGroupRef,
};

use crate::{
    airflow_controller::AIRFLOW_CONTROLLER_NAME,
    config::webserver_config,
    controller::validate::ValidatedAirflowCluster,
    crd::{AIRFLOW_CONFIG_FILENAME, Container, build_recommended_labels, v1alpha2},
    product_logging::extend_config_map_with_log_config,
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to build webserver config for {rolegroup}"))]
    BuildWebserverConfig {
        source: webserver_config::Error,
        rolegroup: RoleGroupRef<v1alpha2::AirflowCluster>,
    },

    #[snafu(display("object is missing metadata to build owner reference"))]
    ObjectMissingMetadataForOwnerRef {
        source: stackable_operator::builder::meta::Error,
    },

    #[snafu(display("failed to build object meta"))]
    ObjectMeta {
        source: stackable_operator::builder::meta::Error,
    },

    #[snafu(display("failed to add the logging configuration to the ConfigMap [{cm_name}]"))]
    InvalidLoggingConfig {
        source: crate::product_logging::Error,
        cm_name: String,
    },

    #[snafu(display("failed to build ConfigMap for {rolegroup}"))]
    BuildConfigMap {
        source: stackable_operator::builder::configmap::Error,
        rolegroup: RoleGroupRef<v1alpha2::AirflowCluster>,
    },
}

/// The rolegroup [`ConfigMap`] configures the rolegroup based on the configuration given by the administrator
#[allow(clippy::too_many_arguments)]
pub fn build_rolegroup_config_map(
    airflow: &v1alpha2::AirflowCluster,
    validated_cluster: &ValidatedAirflowCluster,
    rolegroup: &RoleGroupRef<v1alpha2::AirflowCluster>,
    config_file_overrides: &BTreeMap<String, String>,
    logging: &Logging<Container>,
    container: &Container,
) -> Result<ConfigMap, Error> {
    let config_file = webserver_config::build(
        &validated_cluster.authentication_config,
        &validated_cluster.authorization_config,
        &validated_cluster.image.product_version,
        config_file_overrides,
    )
    .with_context(|_| BuildWebserverConfigSnafu {
        rolegroup: rolegroup.clone(),
    })?;

    let mut cm_builder = ConfigMapBuilder::new();

    cm_builder
        .metadata(
            ObjectMetaBuilder::new()
                .name_and_namespace(airflow)
                .name(rolegroup.object_name())
                .ownerreference_from_resource(airflow, None, Some(true))
                .context(ObjectMissingMetadataForOwnerRefSnafu)?
                .with_recommended_labels(&build_recommended_labels(
                    airflow,
                    AIRFLOW_CONTROLLER_NAME,
                    &validated_cluster.image.app_version_label_value,
                    &rolegroup.role,
                    &rolegroup.role_group,
                ))
                .context(ObjectMetaSnafu)?
                .build(),
        )
        .add_data(AIRFLOW_CONFIG_FILENAME, config_file);

    extend_config_map_with_log_config(
        rolegroup,
        logging,
        container,
        &Container::Vector,
        &mut cm_builder,
        &validated_cluster.image,
    );

    cm_builder.build().with_context(|_| BuildConfigMapSnafu {
        rolegroup: rolegroup.clone(),
    })
}
