//! Ensures that `Pod`s are configured and running for each [`AirflowCluster`]

mod misc;

use super::types::{BuiltClusterResource, FetchedAdditionalData};
use super::AIRFLOW_CONTROLLER_NAME;
use crate::airflow_controller::build::misc::{
    build_role_service, build_rolegroup_config_map, build_rolegroup_service,
    build_server_rolegroup_statefulset, role_port,
use crate::airflow_controller::DOCKER_IMAGE_BASE_NAME;
use crate::common::config::{self, PYTHON_IMPORTS};
use crate::common::controller_commons::{
    CONFIG_VOLUME_NAME, LOG_CONFIG_VOLUME_NAME, LOG_VOLUME_NAME,
};
use crate::airflow_controller::DOCKER_IMAGE_BASE_NAME;

use crate::common::product_logging::{
    extend_config_map_with_log_config, resolve_vector_aggregator_address,
};
use crate::common::util::env_var_from_secret;
use crate::common::{controller_commons, rbac};

use snafu::{OptionExt, ResultExt, Snafu};
use stackable_airflow_crd::{
    airflowdb::{AirflowDB, AirflowDBStatusCondition},
    build_recommended_labels, AirflowCluster, AirflowConfig, AirflowConfigFragment,
    AirflowConfigOptions, AirflowRole, Container, AIRFLOW_CONFIG_FILENAME, APP_NAME, CONFIG_PATH,
    LOG_CONFIG_DIR, OPERATOR_NAME, STACKABLE_LOG_DIR,
};
use stackable_operator::{
    builder::{
        ConfigMapBuilder, ContainerBuilder, ObjectMetaBuilder, PodBuilder,
        PodSecurityContextBuilder,
    },
    cluster_resources::ClusterResources,
    commons::{
        authentication::{AuthenticationClass, AuthenticationClassProvider},
        product_image_selection::ResolvedProductImage,
    },
    k8s_openapi::{
        api::{
            apps::v1::{StatefulSet, StatefulSetSpec},
            core::v1::{
                ConfigMap, EnvVar, Probe, Service, ServicePort, ServiceSpec, TCPSocketAction,
            },
        },
        apimachinery::pkg::{apis::meta::v1::LabelSelector, util::intstr::IntOrString},
    },
    kube::{
        runtime::{controller::Action, reflector::ObjectRef},
        Resource, ResourceExt,
    },
    labels::{role_group_selector_labels, role_selector_labels},
    logging::controller::ReconcilerError,
    product_config::{
        flask_app_config_writer, flask_app_config_writer::FlaskAppConfigWriterError,
        types::PropertyNameKind, ProductConfigManager,
    },
    product_config_utils::{transform_all_roles_to_config, validate_all_roles_and_groups_config},
    product_logging::{self, spec::Logging},
    role_utils::RoleGroupRef,
};
use std::{
    collections::{BTreeMap, HashMap},
    str::FromStr,
    sync::Arc,
    time::Duration,
};
use strum::{EnumDiscriminants, IntoEnumIterator, IntoStaticStr};


#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("object has no namespace"))]
    ObjectHasNoNamespace,
    #[snafu(display("object defines no airflow config role"))]
    NoAirflowRole,

    #[snafu(display("invalid product config"))]
    InvalidProductConfig {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("object is missing metadata to build owner reference"))]
    ObjectMissingMetadataForOwnerRef {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("Failed to transform configs"))]
    ProductConfigTransform {
        source: stackable_operator::product_config_utils::ConfigError,
    },
    #[snafu(display("failed to apply Airflow DB"))]
    CreateAirflowDBObject {
        source: stackable_airflow_crd::airflowdb::Error,
    },
    #[snafu(display("failed to retrieve Airflow DB"))]
    AirflowDBRetrieval {
        source: stackable_operator::error::Error,
    },

    #[snafu(display("failed to retrieve AuthenticationClass {authentication_class}"))]
    AuthenticationClassRetrieval {
        source: stackable_operator::error::Error,
        authentication_class: ObjectRef<AuthenticationClass>,
    },
    #[snafu(display(
        "Airflow doesn't support the AuthenticationClass provider
    {authentication_class_provider} from AuthenticationClass {authentication_class}"
    ))]
    AuthenticationClassProviderNotSupported {
        authentication_class_provider: String,
        authentication_class: ObjectRef<AuthenticationClass>,
    },
    #[snafu(display("failed to build config file for {rolegroup}"))]
    BuildRoleGroupConfigFile {
        source: FlaskAppConfigWriterError,
        rolegroup: RoleGroupRef<AirflowCluster>,
    },
    #[snafu(display("failed to build ConfigMap for {rolegroup}"))]
    BuildRoleGroupConfig {
        source: stackable_operator::error::Error,
        rolegroup: RoleGroupRef<AirflowCluster>,
    },
    #[snafu(display("Airflow db {airflow_db} initialization failed, not starting airflow"))]
    AirflowDBFailed { airflow_db: ObjectRef<AirflowDB> },
    #[snafu(display("failed to resolve and merge config for role and role group"))]
    FailedToResolveConfig {
        source: stackable_airflow_crd::Error,
    },
    #[snafu(display("could not parse Airflow role [{role}]"))]
    UnidentifiedAirflowRole {
        source: strum::ParseError,
        role: String,
    },
    #[snafu(display("failed to build"))]
    BuildingFailure { source: misc::Error },
    #[snafu(display("invalid container name"))]
    InvalidContainerName {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to resolve the Vector aggregator address"))]
    ResolveVectorAggregatorAddress {
        source: crate::common::product_logging::Error,
    },
    #[snafu(display("failed to add the logging configuration to the ConfigMap [{cm_name}]"))]
    InvalidLoggingConfig {
        source: crate::common::product_logging::Error,
        cm_name: String,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub fn build_cluster_resources(
    airflow: Arc<AirflowCluster>,
    additional_data: FetchedAdditionalData,
    product_config: &ProductConfigManager,
) -> Result<Vec<BuiltClusterResource>> {
    let mut built_cluster_resources: Vec<BuiltClusterResource> = Vec::new();

    let resolved_product_image: ResolvedProductImage =
        airflow.spec.image.resolve(DOCKER_IMAGE_BASE_NAME);
    // ensure admin user has been set up on the airflow database
    let airflow_db = AirflowDB::for_airflow(&airflow, &resolved_product_image)
        .context(CreateAirflowDBObjectSnafu)?;

    built_cluster_resources.push(BuiltClusterResource::PatchAirflowDB(airflow_db));

    let airflow_db = additional_data.airflow_db;

    tracing::debug!("{}", format!("Checking status: {:#?}", airflow_db.status));

    if let Some(ref status) = airflow_db.status {
        match status.condition {
            AirflowDBStatusCondition::Pending | AirflowDBStatusCondition::Initializing => {
                tracing::debug!(
                    "Waiting for AirflowDB initialization to complete, not starting Airflow yet"
                );
                return Ok(built_cluster_resources);
            }
            AirflowDBStatusCondition::Failed => {
                return AirflowDBFailedSnafu {
                    airflow_db: ObjectRef::from_obj(&airflow_db),
                }
                .fail();
            }
            AirflowDBStatusCondition::Ready => (), // Continue starting Airflow
        }
    } else {
        tracing::debug!("Waiting for AirflowDBStatus to be reported, not starting Airflow yet");
        return Ok(built_cluster_resources);
    }

    let mut roles = HashMap::new();

    for role in AirflowRole::iter() {
        if let Some(resolved_role) = airflow.get_role(role.clone()).clone() {
            roles.insert(
                role.to_string(),
                (
                    vec![
                        PropertyNameKind::Env,
                        PropertyNameKind::File(AIRFLOW_CONFIG_FILENAME.into()),
                    ],
                    resolved_role,
                ),
            );
        }
    }

    let role_config = transform_all_roles_to_config::<AirflowConfigFragment>(&airflow, roles);
    let validated_role_config = validate_all_roles_and_groups_config(
        &resolved_product_image.product_version,
        &role_config.context(ProductConfigTransformSnafu)?,
        &product_config,
        false,
        false,
    )
    .context(InvalidProductConfigSnafu)?;

    let (rbac_sa, rbac_rolebinding) = rbac::build_rbac_resources(airflow.as_ref(), "airflow");
    built_cluster_resources.push(BuiltClusterResource::PatchRBAC);

    let vector_aggregator_address = additional_data.aggregator_address;

    let authentication_class = additional_data.authentication_class;

    for (role_name, role_config) in validated_role_config.iter() {
        // some roles will only run "internally" and do not need to be created as services
        if let Some(resolved_port) = role_port(role_name) {
            let role_service =
                build_role_service(&airflow, &resolved_product_image, role_name, resolved_port)
                    .context(BuildingFailureSnafu)?;
            built_cluster_resources.push(BuiltClusterResource::RoleService(role_service));
        }

        for (rolegroup_name, rolegroup_config) in role_config.iter() {
            let rolegroup = RoleGroupRef {
                cluster: ObjectRef::from_obj(&*airflow),
                role: role_name.into(),
                role_group: rolegroup_name.into(),
            };

            let airflow_role =
                AirflowRole::from_str(role_name).context(UnidentifiedAirflowRoleSnafu {
                    role: role_name.to_string(),
                })?;

            let config = airflow
                .merged_config(&airflow_role, &rolegroup)
                .context(FailedToResolveConfigSnafu)?;

            let rg_service = build_rolegroup_service(&airflow, &resolved_product_image, &rolegroup)
                .context(BuildingFailureSnafu)?;

            built_cluster_resources.push(BuiltClusterResource::RolegroupService(
                rg_service,
                rolegroup.clone(),
            ));

            let rg_configmap = build_rolegroup_config_map(
                &airflow,
                &resolved_product_image,
                &rolegroup,
                rolegroup_config,
                authentication_class.as_ref(),
                &config.logging,
                vector_aggregator_address.as_deref(),
            )
            .context(BuildingFailureSnafu)?;

            built_cluster_resources.push(BuiltClusterResource::RolegroupConfigMap(
                rg_configmap,
                rolegroup.clone(),
            ));

            let rg_statefulset = build_server_rolegroup_statefulset(
                &airflow,
                &resolved_product_image,
                &rolegroup,
                rolegroup_config,
                authentication_class.as_ref(),
                &rbac_sa.name_unchecked(),
                &config,
            )
            .context(BuildingFailureSnafu)?;

            built_cluster_resources.push(BuiltClusterResource::RolegroupStatefulSet(
                rg_statefulset,
                rolegroup.clone(),
            ));
        }
    }

    built_cluster_resources.push(BuiltClusterResource::DeleteOrphaned);

    Ok(built_cluster_resources)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::super::types::{BuiltClusterResource, FetchedAdditionalData};

    use super::build_cluster_resources;
    //use assert_json_diff::{assert_json_matches_no_panic, CompareMode, Config};
    use stackable_airflow_crd::airflowdb::AirflowDB;
    use stackable_airflow_crd::AirflowCluster;
    use stackable_operator::product_config::ProductConfigManager;

    #[test]
    fn test_build_step_just_runs() {
        let cluster_cr = std::fs::File::open("test/smoke/cluster.yaml").unwrap();
        let cluster_deserializer = serde_yaml::Deserializer::from_reader(&cluster_cr);
        let druid_cluster: AirflowCluster =
            serde_yaml::with::singleton_map_recursive::deserialize(cluster_deserializer).unwrap();

        let product_config_manager =
            ProductConfigManager::from_yaml_file("test/smoke/properties.yaml").unwrap();

        let db_cr = std::fs::File::open("test/smoke/db.yaml").unwrap();
        let db_deserializer = serde_yaml::Deserializer::from_reader(&db_cr);
        let airflow_db: AirflowDB =
            serde_yaml::with::singleton_map_recursive::deserialize(db_deserializer).unwrap();

        let result = build_cluster_resources(
            Arc::new(druid_cluster),
            FetchedAdditionalData {
                airflow_db,
                aggregator_address: Some("yei".to_string()),
                authentication_class: None,
            },
            &product_config_manager,
        );

        assert!(result.is_ok(), "we want an ok, instead we got {:?}", result);
    }
}
