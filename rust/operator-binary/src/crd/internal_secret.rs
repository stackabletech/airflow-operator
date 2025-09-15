use std::collections::BTreeMap;

use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    builder::meta::ObjectMetaBuilder, client::Client, k8s_openapi::api::core::v1::Secret,
    kube::ResourceExt, logging::controller::ReconcilerError,
};
use strum::{EnumDiscriminants, IntoStaticStr};

use crate::{airflow_controller::AIRFLOW_CONTROLLER_NAME, crd::v1alpha1};

// Used for env-vars: AIRFLOW__WEBSERVER__SECRET_KEY, AIRFLOW__API__SECRET_KEY
// N.B. AIRFLOW__WEBSERVER__SECRET_KEY is deprecated as of 3.0.2.
// Secret key used to run the api server. It should be as random as possible.
// It should be consistent across instances of the webserver. The webserver key
// is also used to authorize requests to Celery workers when logs are retrieved.
pub const ENV_INTERNAL_SECRET: &str = "INTERNAL_SECRET";
// Used for env-var: AIRFLOW__API_AUTH__JWT_SECRET
// Secret key used to encode and decode JWTs to authenticate to public and
// private APIs. It should be as random as possible, but consistent across
// instances of API services.
pub const ENV_JWT_SECRET: &str = "JWT_SECRET";

type Result<T, E = Error> = std::result::Result<T, E>;

impl ReconcilerError for Error {
    fn category(&self) -> &'static str {
        ErrorDiscriminants::from(self).into()
    }
}

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
pub enum Error {
    #[snafu(display("object defines no namespace"))]
    ObjectHasNoNamespace,

    #[snafu(display("object is missing metadata to build owner reference"))]
    ObjectMissingMetadataForOwnerRef {
        source: stackable_operator::builder::meta::Error,
    },

    #[snafu(display("failed to retrieve secret for internal communications"))]
    FailedToRetrieveInternalSecret {
        source: stackable_operator::client::Error,
    },

    #[snafu(display("failed to apply internal secret"))]
    ApplyInternalSecret {
        source: stackable_operator::client::Error,
    },
}

pub async fn create_random_secret(
    secret_name: &str,
    secret_key: &str,
    secret_byte_size: usize,
    airflow: &v1alpha1::AirflowCluster,
    client: &Client,
) -> Result<()> {
    let mut internal_secret = BTreeMap::new();
    internal_secret.insert(secret_key.to_string(), get_random_base64(secret_byte_size));

    let secret = Secret {
        immutable: Some(true),
        metadata: ObjectMetaBuilder::new()
            .name(secret_name)
            .namespace_opt(airflow.namespace())
            .ownerreference_from_resource(airflow, None, Some(true))
            .context(ObjectMissingMetadataForOwnerRefSnafu)?
            .build(),
        string_data: Some(internal_secret),
        ..Secret::default()
    };

    if client
        .get_opt::<Secret>(
            &secret.name_any(),
            secret
                .namespace()
                .as_deref()
                .context(ObjectHasNoNamespaceSnafu)?,
        )
        .await
        .context(FailedToRetrieveInternalSecretSnafu)?
        .is_none()
    {
        client
            .apply_patch(AIRFLOW_CONTROLLER_NAME, &secret, &secret)
            .await
            .context(ApplyInternalSecretSnafu)?;
    }

    Ok(())
}

fn get_random_base64(byte_size: usize) -> String {
    let mut buf: Vec<u8> = vec![0; byte_size];
    openssl::rand::rand_bytes(&mut buf).unwrap();
    openssl::base64::encode_block(&buf)
}
