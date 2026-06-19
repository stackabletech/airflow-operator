//! Shared pod-building helpers used by more than one resource builder (the rolegroup StatefulSet
//! and the Kubernetes-executor pod template): authentication volumes, git-sync resources and the
//! Vector log-collection sidecar.

use std::{collections::BTreeSet, str::FromStr};

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    builder::pod::{PodBuilder, container::ContainerBuilder},
    commons::product_image_selection::ResolvedProductImage,
    crd::{authentication::ldap, git_sync},
    k8s_openapi::api::core::v1::Container as K8sContainer,
    v2::{
        builder::pod::container::EnvVarSet,
        product_logging::framework::{VectorContainerLogConfig, vector_container},
        role_group_utils::ResourceNames,
        types::kubernetes::ContainerName,
    },
};

use crate::{
    controller_commons::{CONFIG_VOLUME_NAME, LOG_VOLUME_NAME},
    crd::authentication::{
        AirflowAuthenticationClassResolved, AirflowClientAuthenticationDetailsResolved,
    },
};

#[derive(Snafu, Debug)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    #[snafu(display("failed to add a volume to the Pod"))]
    AddVolume {
        source: stackable_operator::builder::pod::Error,
    },

    #[snafu(display("failed to add a volume mount to the container"))]
    AddVolumeMount {
        source: stackable_operator::builder::pod::container::Error,
    },

    #[snafu(display("failed to add LDAP authentication volumes and volume mounts"))]
    LdapAuthentication { source: ldap::v1alpha1::Error },

    #[snafu(display("failed to add TLS authentication volumes and volume mounts"))]
    TlsAuthentication {
        source: stackable_operator::commons::tls_verification::TlsClientDetailsError,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub(crate) fn add_authentication_volumes_and_volume_mounts(
    authentication_config: &AirflowClientAuthenticationDetailsResolved,
    cb: &mut ContainerBuilder,
    pb: &mut PodBuilder,
) -> Result<()> {
    // Different authentication entries can reference the same secret
    // class or TLS certificate. It must be ensured that the volumes
    // and volume mounts are only added once in such a case.

    let mut ldap_authentication_providers = BTreeSet::new();
    let mut tls_client_credentials = BTreeSet::new();

    for auth_class_resolved in &authentication_config.authentication_classes_resolved {
        match auth_class_resolved {
            AirflowAuthenticationClassResolved::Ldap { provider } => {
                ldap_authentication_providers.insert(provider);
            }
            AirflowAuthenticationClassResolved::Oidc { provider, .. } => {
                tls_client_credentials.insert(&provider.tls);
            }
        }
    }

    for provider in ldap_authentication_providers {
        provider
            .add_volumes_and_mounts(pb, vec![cb])
            .context(LdapAuthenticationSnafu)?;
    }

    for tls in tls_client_credentials {
        tls.add_volumes_and_mounts(pb, vec![cb])
            .context(TlsAuthenticationSnafu)?;
    }
    Ok(())
}

pub(crate) fn add_git_sync_resources(
    pb: &mut PodBuilder,
    cb: &mut ContainerBuilder,
    git_sync_resources: &git_sync::v1alpha2::GitSyncResources,
    add_sidecar_containers: bool,
    add_init_containers: bool,
) -> Result<()> {
    if add_sidecar_containers {
        for container in git_sync_resources.git_sync_containers.iter().cloned() {
            pb.add_container(container);
        }
    }
    if add_init_containers {
        for container in git_sync_resources.git_sync_init_containers.iter().cloned() {
            pb.add_init_container(container);
        }
    }
    pb.add_volumes(git_sync_resources.git_content_volumes.to_owned())
        .context(AddVolumeSnafu)?;
    pb.add_volumes(git_sync_resources.git_ssh_volumes.to_owned())
        .context(AddVolumeSnafu)?;
    pb.add_volumes(git_sync_resources.git_ca_cert_volumes.to_owned())
        .context(AddVolumeSnafu)?;
    cb.add_volume_mounts(git_sync_resources.git_content_volume_mounts.to_owned())
        .context(AddVolumeMountSnafu)?;

    Ok(())
}

stackable_operator::constant!(VECTOR_CONTAINER_NAME: ContainerName = "vector");

/// Builds the Vector log-collection sidecar container from the up-front-validated logging config.
pub(crate) fn build_logging_container(
    resolved_product_image: &ResolvedProductImage,
    vector_log_config: &VectorContainerLogConfig,
    resource_names: &ResourceNames,
) -> K8sContainer {
    vector_container(
        &VECTOR_CONTAINER_NAME,
        resolved_product_image,
        vector_log_config,
        resource_names,
        &CONFIG_VOLUME_NAME,
        &LOG_VOLUME_NAME,
        EnvVarSet::new(),
    )
}
