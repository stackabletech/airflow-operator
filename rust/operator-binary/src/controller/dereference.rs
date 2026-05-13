use snafu::{ResultExt, Snafu};
use stackable_operator::commons::random_secret_creation;

use crate::crd::{
    authentication::AirflowClientAuthenticationDetailsResolved,
    authorization::AirflowAuthorizationResolved,
    internal_secret::{FERNET_KEY_SECRET_KEY, INTERNAL_SECRET_SECRET_KEY, JWT_SECRET_SECRET_KEY},
    v1alpha2,
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

    #[snafu(display("failed to create internal secret"))]
    InternalSecret {
        source: random_secret_creation::Error,
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

    random_secret_creation::create_random_secret_if_not_exists(
        &airflow.shared_internal_secret_secret_name(),
        INTERNAL_SECRET_SECRET_KEY,
        256,
        airflow,
        client,
    )
    .await
    .context(InternalSecretSnafu)?;

    random_secret_creation::create_random_secret_if_not_exists(
        &airflow.shared_jwt_secret_secret_name(),
        JWT_SECRET_SECRET_KEY,
        256,
        airflow,
        client,
    )
    .await
    .context(InternalSecretSnafu)?;

    // https://airflow.apache.org/docs/apache-airflow/stable/security/secrets/fernet.html#security-fernet
    // does not document how long the fernet key should be, but recommends using
    // python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
    // which returns `jUm21LuA76YZmrIa9u4eXRg0h0P24MDC9IDOmDvJbfw=`, which has 44 characters, which makes 32 bytes.
    random_secret_creation::create_random_secret_if_not_exists(
        &airflow.shared_fernet_key_secret_name(),
        FERNET_KEY_SECRET_KEY,
        32,
        airflow,
        client,
    )
    .await
    .context(InternalSecretSnafu)?;

    Ok(DereferencedObjects {
        authentication_config,
        authorization_config,
    })
}
