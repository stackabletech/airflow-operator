//! Ensures that `Pod`s are configured and running for each [`AirflowCluster`]

mod misc;

use super::types::{BuiltClusterResource, FetchedAdditionalData};
use super::AIRFLOW_CONTROLLER_NAME;
use crate::airflow_controller::build::misc::{
    build_role_service, build_rolegroup_config_map, build_rolegroup_service,
    build_server_rolegroup_statefulset, role_port,
};
use crate::airflow_controller::DOCKER_IMAGE_BASE_NAME;

use crate::common::rbac;

use snafu::{ResultExt, Snafu};
use stackable_airflow_crd::{
    airflowdb::{AirflowDB, AirflowDBStatusCondition},
    AirflowCluster, AirflowConfigFragment, AirflowRole, AIRFLOW_CONFIG_FILENAME,
};
use stackable_operator::commons::product_image_selection::ResolvedProductImage;
use stackable_operator::{
    kube::{runtime::reflector::ObjectRef, ResourceExt},
    product_config::{types::PropertyNameKind, ProductConfigManager},
    product_config_utils::{transform_all_roles_to_config, validate_all_roles_and_groups_config},
    role_utils::RoleGroupRef,
};
use std::{collections::HashMap, str::FromStr, sync::Arc};
use strum::{EnumDiscriminants, IntoEnumIterator, IntoStaticStr};

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("invalid product config"))]
    InvalidProductConfig {
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

    if airflow_db.is_none() {
        tracing::debug!(
            "{}",
            format!("AirflowDB does not exist yet. Skipping over all remaining build steps.")
        );
        return Ok(built_cluster_resources);
    }
    let airflow_db = airflow_db.expect("AirflowDB can't be None at this point.");

    tracing::debug!(
        "{}",
        format!("Checking AirflowDB status: {:#?}", airflow_db.status)
    );

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
        product_config,
        false,
        false,
    )
    .context(InvalidProductConfigSnafu)?;

    let (rbac_sa, _) = rbac::build_rbac_resources(airflow.as_ref(), "airflow");
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

    use crate::airflow_controller::types::BuiltClusterResource;

    use super::super::types::FetchedAdditionalData;

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
        let airflow_db: Option<AirflowDB> =
            Some(serde_yaml::with::singleton_map_recursive::deserialize(db_deserializer).unwrap());

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

    #[test]
    fn test_initial_create_airflow_db() {
        let cluster_cr = std::fs::File::open("test/smoke/cluster.yaml").unwrap();
        let cluster_deserializer = serde_yaml::Deserializer::from_reader(&cluster_cr);
        let druid_cluster: AirflowCluster =
            serde_yaml::with::singleton_map_recursive::deserialize(cluster_deserializer).unwrap();

        let product_config_manager =
            ProductConfigManager::from_yaml_file("test/smoke/properties.yaml").unwrap();

        let result = build_cluster_resources(
            Arc::new(druid_cluster),
            FetchedAdditionalData {
                airflow_db: None,
                aggregator_address: None,
                authentication_class: None,
            },
            &product_config_manager,
        )
        .expect("should produce result");

        assert_eq!(
            result.len(),
            1,
            "we want to have a single resource entry, instead we got {:?}",
            result
        );

        if let BuiltClusterResource::PatchAirflowDB(_) = result[0] {
            // if let ... else is not a thing yet :( https://github.com/rust-lang/rust/pull/93628/
        } else {
            panic!("expected PatchAirflowDB entry, found {:?}", result[0]);
        }
    }
}
