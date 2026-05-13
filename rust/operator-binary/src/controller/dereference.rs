use snafu::{ResultExt, Snafu};

use crate::crd::{
    authentication::AirflowClientAuthenticationDetailsResolved,
    authorization::AirflowAuthorizationResolved, v1alpha2,
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
}

/// External references resolved during the dereference step.
pub struct DereferencedObjects {
    pub authentication_config: AirflowClientAuthenticationDetailsResolved,
    pub authorization_config: AirflowAuthorizationResolved,
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

    Ok(DereferencedObjects {
        authentication_config,
        authorization_config,
    })
}
