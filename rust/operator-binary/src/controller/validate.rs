use std::{
    collections::{BTreeMap, HashMap},
    str::FromStr,
};

use product_config::{ProductConfigManager, types::PropertyNameKind};
use snafu::{ResultExt, Snafu};
use stackable_operator::{
    commons::product_image_selection::{self, ResolvedProductImage},
    product_config_utils::{transform_all_roles_to_config, validate_all_roles_and_groups_config},
    role_utils::RoleGroupRef,
};
use strum::IntoEnumIterator;

use crate::crd::{AIRFLOW_CONFIG_FILENAME, AirflowConfig, AirflowExecutor, AirflowRole, v1alpha2};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to resolve product image"))]
    ResolveProductImage {
        source: product_image_selection::Error,
    },

    #[snafu(display("invalid product config"))]
    InvalidProductConfig {
        source: stackable_operator::product_config_utils::Error,
    },

    #[snafu(display("failed to generate product config"))]
    GenerateProductConfig {
        source: stackable_operator::product_config_utils::Error,
    },

    #[snafu(display("failed to resolve and merge config for role and role group"))]
    FailedToResolveConfig { source: crate::crd::Error },

    #[snafu(display("could not parse Airflow role [{role}]"))]
    UnidentifiedAirflowRole {
        source: strum::ParseError,
        role: String,
    },
}

/// Per-role configuration extracted during validation.
#[derive(Clone, Debug)]
pub struct ValidatedRoleConfig {
    pub pdb: Option<stackable_operator::commons::pdb::PdbConfig>,
    pub listener_class: Option<String>,
    pub group_listener_name: Option<String>,
}

/// Per-rolegroup configuration: the merged CRD config plus the product-config properties.
#[derive(Clone, Debug)]
pub struct ValidatedRoleGroupConfig {
    pub merged_config: AirflowConfig,
    pub product_config_properties: HashMap<PropertyNameKind, BTreeMap<String, String>>,
}

/// The validated cluster: proves that product-config validation and config merging
/// succeeded for every role and role group before any resources are created.
#[derive(Clone, Debug)]
pub struct ValidatedAirflowCluster {
    pub image: ResolvedProductImage,
    pub role_groups: BTreeMap<AirflowRole, BTreeMap<String, ValidatedRoleGroupConfig>>,
    pub role_configs: BTreeMap<AirflowRole, ValidatedRoleConfig>,
    pub executor: AirflowExecutor,
}

pub fn validate_cluster(
    airflow: &v1alpha2::AirflowCluster,
    image_base_name: &str,
    image_repository: &str,
    pkg_version: &str,
    product_config_manager: &ProductConfigManager,
) -> Result<ValidatedAirflowCluster, Error> {
    let resolved_product_image = airflow
        .spec
        .image
        .resolve(image_base_name, image_repository, pkg_version)
        .context(ResolveProductImageSnafu)?;

    let mut roles = HashMap::new();

    // if the kubernetes executor is specified there will be no worker role as the pods
    // are provisioned by airflow as defined by the task (default: one pod per task)
    for role in AirflowRole::iter() {
        if let Some(resolved_role) = airflow.get_role(&role) {
            roles.insert(
                role.to_string(),
                (
                    vec![
                        PropertyNameKind::Env,
                        PropertyNameKind::File(AIRFLOW_CONFIG_FILENAME.into()),
                    ],
                    resolved_role.clone(),
                ),
            );
        }
    }

    let role_config = transform_all_roles_to_config(airflow, &roles);
    let validated_role_config = validate_all_roles_and_groups_config(
        &resolved_product_image.product_version,
        &role_config.context(GenerateProductConfigSnafu)?,
        product_config_manager,
        false,
        false,
    )
    .context(InvalidProductConfigSnafu)?;

    let mut role_groups = BTreeMap::new();
    let mut role_configs = BTreeMap::new();

    for (role_name, rolegroup_configs) in validated_role_config.iter() {
        let airflow_role =
            AirflowRole::from_str(role_name).context(UnidentifiedAirflowRoleSnafu {
                role: role_name.to_string(),
            })?;

        role_configs.insert(
            airflow_role.clone(),
            ValidatedRoleConfig {
                pdb: airflow
                    .role_config(&airflow_role)
                    .map(|rc| rc.pod_disruption_budget),
                listener_class: airflow_role
                    .listener_class_name(airflow)
                    .map(|s| s.to_string()),
                group_listener_name: airflow.group_listener_name(&airflow_role),
            },
        );

        let mut group_configs = BTreeMap::new();
        for (rolegroup_name, rolegroup_config) in rolegroup_configs.iter() {
            let rolegroup_ref = RoleGroupRef {
                cluster: stackable_operator::kube::runtime::reflector::ObjectRef::from_obj(airflow),
                role: role_name.into(),
                role_group: rolegroup_name.into(),
            };

            let merged_config = airflow
                .merged_config(&airflow_role, &rolegroup_ref)
                .context(FailedToResolveConfigSnafu)?;

            group_configs.insert(
                rolegroup_name.clone(),
                ValidatedRoleGroupConfig {
                    merged_config,
                    product_config_properties: rolegroup_config.clone(),
                },
            );
        }

        role_groups.insert(airflow_role, group_configs);
    }

    Ok(ValidatedAirflowCluster {
        image: resolved_product_image,
        role_groups,
        role_configs,
        executor: airflow.spec.executor.clone(),
    })
}
