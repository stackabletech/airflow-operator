use std::marker::PhantomData;

use snafu::{ResultExt, Snafu};
use stackable_operator::cluster_resources::{ClusterResource, ClusterResources};
use strum::{EnumDiscriminants, IntoStaticStr};

use super::{Applied, KubernetesResources, Prepared};

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
pub enum Error {
    #[snafu(display("failed to apply resource"))]
    ApplyResource {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to delete orphaned resources"))]
    DeleteOrphanedResources {
        source: stackable_operator::cluster_resources::Error,
    },
}

pub(crate) struct Applier<'a> {
    client: &'a stackable_operator::client::Client,
    cluster_resources: ClusterResources<'a>,
}

impl<'a> Applier<'a> {
    pub(crate) fn new(
        client: &'a stackable_operator::client::Client,
        cluster_resources: ClusterResources<'a>,
    ) -> Self {
        Applier {
            client,
            cluster_resources,
        }
    }

    pub(crate) async fn apply(
        mut self,
        resources: KubernetesResources<Prepared>,
    ) -> std::result::Result<KubernetesResources<Applied>, Error> {
        let config_maps = self.add_resources(resources.config_maps).await?;
        let service_accounts = self.add_resources(resources.service_accounts).await?;
        let services = self.add_resources(resources.services).await?;
        let role_bindings = self.add_resources(resources.role_bindings).await?;
        let listeners = self.add_resources(resources.listeners).await?;
        let stateful_sets = self.add_resources(resources.stateful_sets).await?;
        let pod_disruption_budgets = self.add_resources(resources.pod_disruption_budgets).await?;

        self.cluster_resources
            .delete_orphaned_resources(self.client)
            .await
            .context(DeleteOrphanedResourcesSnafu)?;

        Ok(KubernetesResources {
            stateful_sets,
            config_maps,
            services,
            service_accounts,
            role_bindings,
            pod_disruption_budgets,
            listeners,
            _status: PhantomData,
        })
    }

    async fn add_resources<T: ClusterResource + Sync>(
        &mut self,
        resources: Vec<T>,
    ) -> std::result::Result<Vec<T>, Error> {
        let mut applied = vec![];
        for resource in resources {
            let applied_resource = self
                .cluster_resources
                .add(self.client, resource)
                .await
                .context(ApplyResourceSnafu)?;
            applied.push(applied_resource);
        }
        Ok(applied)
    }
}
