use snafu::{OptionExt, ResultExt, Snafu};
use stackable_airflow_crd::airflowdb::AirflowDB;
use stackable_operator::{
    client::Client,
    k8s_openapi::api::{batch::v1::Job, core::v1::Secret},
    kube::{runtime::reflector::ObjectRef, ResourceExt},
};

use std::sync::Arc;
use strum::{EnumDiscriminants, IntoStaticStr};

use crate::common::product_logging::resolve_vector_aggregator_address;

use super::types::FetchedAdditionalData;

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("object has no namespace"))]
    ObjectHasNoNamespace,

    #[snafu(display("failed to resolve the Vector aggregator address"))]
    ResolveVectorAggregatorAddress {
        source: crate::common::product_logging::Error,
    },
    #[snafu(display("Failed to check whether the secret ({}) exists", secret))]
    SecretCheck {
        source: stackable_operator::error::Error,
        secret: ObjectRef<Secret>,
    },
    #[snafu(display("database state is 'initializing' but failed to find job {}", init_job))]
    GetInitializationJob {
        source: stackable_operator::error::Error,
        init_job: ObjectRef<Job>,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub async fn fetch_additional_data(
    airflow_db: &Arc<AirflowDB>,
    client: &Client,
) -> Result<FetchedAdditionalData> {
    let initial_secret = client
        .get_opt::<Secret>(
            &airflow_db.spec.credentials_secret,
            airflow_db
                .namespace()
                .as_deref()
                .context(ObjectHasNoNamespaceSnafu)?,
        )
        .await
        .with_context(|_| {
            let mut secret_ref = ObjectRef::<Secret>::new(&airflow_db.spec.credentials_secret);
            if let Some(ns) = airflow_db.namespace() {
                secret_ref = secret_ref.within(&ns);
            }
            SecretCheckSnafu { secret: secret_ref }
        })?;
    let vector_aggregator_address = resolve_vector_aggregator_address(
        client,
        airflow_db.as_ref(),
        airflow_db.spec.vector_aggregator_config_map_name.as_deref(),
    )
    .await
    .context(ResolveVectorAggregatorAddressSnafu)?;

    let ns = airflow_db
        .namespace()
        .unwrap_or_else(|| "default".to_string());
    let job_name = airflow_db.job_name();
    let job = client
        .get_opt::<Job>(&job_name, &ns)
        .await
        .context(GetInitializationJobSnafu {
            init_job: ObjectRef::<Job>::new(&job_name).within(&ns),
        })?;

    Ok(FetchedAdditionalData {
        job,
        vector_aggregator_address,
        initial_secret,
    })
}
