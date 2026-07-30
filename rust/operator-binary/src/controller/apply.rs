//! The apply step in the AirflowCluster controller.

use std::marker::PhantomData;

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    client::Client,
    cluster_resources::{ClusterResource, ClusterResourceApplyStrategy, ClusterResources},
    commons::random_secret_creation,
    deep_merger::ObjectOverrides,
    v2::cluster_resources::cluster_resources_new,
};
use strum::{EnumDiscriminants, IntoStaticStr};

use crate::{
    controller::{
        Applied, KubernetesResources, Prepared, ValidatedCluster, controller_name, operator_name,
        product_name,
    },
    crd::internal_secret::{
        FERNET_KEY_SECRET_KEY, INTERNAL_SECRET_SECRET_KEY, JWT_SECRET_SECRET_KEY,
    },
};

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
pub enum Error {
    #[snafu(display("failed to apply Kubernetes resource"))]
    ApplyResource {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to delete orphaned resources"))]
    DeleteOrphanedResources {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to create internal secret"))]
    InternalSecret {
        source: random_secret_creation::Error,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// Applier for the Kubernetes resource specifications produced by this controller.
///
/// The implementation is not tied to this controller and could theoretically be moved to
/// stackable_operator if [`KubernetesResources`] would contain all possible resource types.
pub struct Applier<'a> {
    client: &'a Client,
    cluster_resources: ClusterResources<'a>,
}

impl<'a> Applier<'a> {
    pub fn new(
        client: &'a Client,
        cluster: &ValidatedCluster,
        apply_strategy: ClusterResourceApplyStrategy,
        object_overrides: &'a ObjectOverrides,
    ) -> Applier<'a> {
        let cluster_resources = cluster_resources_new(
            &product_name(),
            &operator_name(),
            &controller_name(),
            &cluster.name,
            &cluster.namespace,
            &cluster.uid,
            apply_strategy,
            object_overrides,
        );

        Applier {
            client,
            cluster_resources,
        }
    }

    /// Applies the given Kubernetes resources and marks them as applied.
    pub async fn apply(
        mut self,
        resources: KubernetesResources<Prepared>,
    ) -> Result<KubernetesResources<Applied>> {
        // Apply order is: StatefulSets last (a changed mounted ConfigMap/Secret
        // must exist first, else Pods restart -- commons-operator#111). The ServiceAccount comes
        // first because the Pods reference it at creation time.
        let service_accounts = self.add_resources(resources.service_accounts).await?;
        let role_bindings = self.add_resources(resources.role_bindings).await?;
        let services = self.add_resources(resources.services).await?;
        let listeners = self.add_resources(resources.listeners).await?;
        let config_maps = self.add_resources(resources.config_maps).await?;
        let pod_disruption_budgets = self.add_resources(resources.pod_disruption_budgets).await?;
        let stateful_sets = self.add_resources(resources.stateful_sets).await?;

        self.cluster_resources
            .delete_orphaned_resources(self.client)
            .await
            .context(DeleteOrphanedResourcesSnafu)?;

        Ok(KubernetesResources {
            stateful_sets,
            services,
            listeners,
            config_maps,
            pod_disruption_budgets,
            service_accounts,
            role_bindings,
            status: PhantomData,
        })
    }

    async fn add_resources<T: ClusterResource + Sync>(
        &mut self,
        resources: Vec<T>,
    ) -> Result<Vec<T>> {
        let mut applied_resources = vec![];

        for resource in resources {
            let applied_resource = self
                .cluster_resources
                .add(self.client, resource)
                .await
                .context(ApplyResourceSnafu)?;
            applied_resources.push(applied_resource);
        }

        Ok(applied_resources)
    }
}

/// Ensures the three shared random Secrets (internal / JWT / Fernet) exist, creating any that are
/// missing. These are read-or-create client operations, so they cannot be part of the client-free
/// `build()` step; they are also deliberately not tracked in [`ClusterResources`], so they survive
/// orphan deletion and an existing Secret is never overwritten.
pub async fn ensure_random_secrets(client: &Client, cluster: &ValidatedCluster) -> Result<()> {
    random_secret_creation::create_random_secret_if_not_exists(
        cluster.internal_secret_name().as_ref(),
        INTERNAL_SECRET_SECRET_KEY,
        256,
        cluster,
        client,
    )
    .await
    .context(InternalSecretSnafu)?;

    random_secret_creation::create_random_secret_if_not_exists(
        cluster.jwt_secret_name().as_ref(),
        JWT_SECRET_SECRET_KEY,
        256,
        cluster,
        client,
    )
    .await
    .context(InternalSecretSnafu)?;

    // https://airflow.apache.org/docs/apache-airflow/stable/security/secrets/fernet.html#security-fernet
    // does not document how long the fernet key should be, but recommends using
    // python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
    // which returns 32 bytes.
    random_secret_creation::create_random_secret_if_not_exists(
        cluster.fernet_key_name().as_ref(),
        FERNET_KEY_SECRET_KEY,
        32,
        cluster,
        client,
    )
    .await
    .context(InternalSecretSnafu)?;

    Ok(())
}
