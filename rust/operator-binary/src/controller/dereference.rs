use snafu::{ResultExt, Snafu};
use stackable_operator::v2::controller_utils::get_namespace;

use crate::{
    controller::build::openlineage::ResolvedOpenLineageConfig,
    crd::{
        authentication::AirflowClientAuthenticationDetailsResolved,
        authorization::AirflowAuthorizationResolved, v1alpha2,
    },
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to apply authentication configuration"))]
    AuthenticationConfig {
        source: crate::crd::authentication::Error,
    },

    #[snafu(display("invalid authorization config"))]
    AuthorizationConfig {
        source: stackable_operator::commons::opa::Error,
    },

    #[snafu(display("failed to resolve namespace"))]
    ResolveNamespace {
        source: stackable_operator::v2::controller_utils::Error,
    },

    #[snafu(display("failed to resolve OpenLineage configuration"))]
    OpenLineageConfig {
        source: crate::controller::build::openlineage::Error,
    },
}

/// External references resolved during the dereference step.
pub struct DereferencedObjects {
    pub authentication_config: AirflowClientAuthenticationDetailsResolved,
    pub authorization_config: AirflowAuthorizationResolved,
    /// The resolved OpenLineage configuration (`spec.clusterConfig.openLineage`), when configured.
    pub resolved_open_lineage_config: Option<ResolvedOpenLineageConfig>,
}

pub async fn dereference(
    client: &stackable_operator::client::Client,
    airflow: &v1alpha2::AirflowCluster,
) -> Result<DereferencedObjects, Error> {
    let authentication_config = AirflowClientAuthenticationDetailsResolved::from(
        &airflow.spec.cluster_config.authentication,
        client,
    )
    .await
    .context(AuthenticationConfigSnafu)?;

    let authorization_config = AirflowAuthorizationResolved::from_authorization_config(
        client,
        airflow,
        &airflow.spec.cluster_config.authorization,
    )
    .await
    .context(AuthorizationConfigSnafu)?;

    let resolved_open_lineage_config =
        if let Some(open_lineage) = &airflow.spec.cluster_config.open_lineage {
            let namespace = get_namespace(airflow)
                .context(ResolveNamespaceSnafu)?
                .to_string();
            Some(
                ResolvedOpenLineageConfig::from_config(open_lineage, client, &namespace)
                    .await
                    .context(OpenLineageConfigSnafu)?,
            )
        } else {
            None
        };

    Ok(DereferencedObjects {
        authentication_config,
        authorization_config,
        resolved_open_lineage_config,
    })
}
