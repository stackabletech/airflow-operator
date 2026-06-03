use std::collections::BTreeMap;

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    commons::product_image_selection::{self, ResolvedProductImage},
    role_utils::RoleGroupRef,
};
use strum::IntoEnumIterator;

use super::dereference::DereferencedObjects;
use crate::{
    airflow_controller::CONTAINER_IMAGE_BASE_NAME,
    crd::{
        AirflowConfig, AirflowExecutor, AirflowRole, MergedOverrides,
        authentication::AirflowClientAuthenticationDetailsResolved,
        authorization::AirflowAuthorizationResolved, v1alpha2,
    },
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to resolve product image"))]
    ResolveProductImage {
        source: product_image_selection::Error,
    },

    #[snafu(display("failed to resolve and merge config for role and role group"))]
    FailedToResolveConfig { source: crate::crd::Error },
}

/// Per-role configuration extracted during validation.
#[derive(Clone, Debug)]
pub struct ValidatedRoleConfig {
    pub pdb: Option<stackable_operator::commons::pdb::PdbConfig>,
    pub listener_class: Option<String>,
    pub group_listener_name: Option<String>,
}

/// Per-rolegroup configuration: the merged CRD config plus overrides.
#[derive(Clone, Debug)]
pub struct ValidatedRoleGroupConfig {
    pub merged_config: AirflowConfig,
    pub overrides: MergedOverrides,
}

/// The validated cluster: proves that config merging succeeded for every role and
/// role group before any resources are created. It also carries the dereferenced
/// external references, so every downstream build step reads them from here.
#[derive(Clone, Debug)]
pub struct ValidatedAirflowCluster {
    pub image: ResolvedProductImage,
    pub role_groups: BTreeMap<AirflowRole, BTreeMap<String, ValidatedRoleGroupConfig>>,
    pub role_configs: BTreeMap<AirflowRole, ValidatedRoleConfig>,
    pub executor: AirflowExecutor,
    pub authentication_config: AirflowClientAuthenticationDetailsResolved,
    pub authorization_config: AirflowAuthorizationResolved,
}

pub fn validate_cluster(
    airflow: &v1alpha2::AirflowCluster,
    image_repository: &str,
    dereferenced: DereferencedObjects,
) -> Result<ValidatedAirflowCluster, Error> {
    let resolved_product_image = airflow
        .spec
        .image
        .resolve(
            CONTAINER_IMAGE_BASE_NAME,
            image_repository,
            crate::built_info::PKG_VERSION,
        )
        .context(ResolveProductImageSnafu)?;

    let mut role_groups = BTreeMap::new();
    let mut role_configs = BTreeMap::new();

    // if the kubernetes executor is specified there will be no worker role as the pods
    // are provisioned by airflow as defined by the task (default: one pod per task)
    for role in AirflowRole::iter() {
        let Some(resolved_role) = airflow.get_role(&role) else {
            continue;
        };

        role_configs.insert(
            role.clone(),
            ValidatedRoleConfig {
                pdb: airflow
                    .role_config(&role)
                    .map(|rc| rc.pod_disruption_budget),
                listener_class: role.listener_class_name(airflow).map(|s| s.to_string()),
                group_listener_name: airflow.group_listener_name(&role),
            },
        );

        let mut group_configs = BTreeMap::new();
        for rolegroup_name in resolved_role.role_groups.keys() {
            let rolegroup_ref = RoleGroupRef {
                cluster: stackable_operator::kube::runtime::reflector::ObjectRef::from_obj(airflow),
                role: role.to_string(),
                role_group: rolegroup_name.into(),
            };

            let merged_config = airflow
                .merged_config(&role, &rolegroup_ref)
                .context(FailedToResolveConfigSnafu)?;

            let overrides = airflow
                .merged_overrides(&role, rolegroup_name)
                .context(FailedToResolveConfigSnafu)?;

            group_configs.insert(
                rolegroup_name.clone(),
                ValidatedRoleGroupConfig {
                    merged_config,
                    overrides,
                },
            );
        }

        role_groups.insert(role, group_configs);
    }

    let DereferencedObjects {
        authentication_config,
        authorization_config,
    } = dereferenced;

    Ok(ValidatedAirflowCluster {
        image: resolved_product_image,
        role_groups,
        role_configs,
        executor: airflow.spec.executor.clone(),
        authentication_config,
        authorization_config,
    })
}
