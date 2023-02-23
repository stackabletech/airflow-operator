use snafu::Snafu;
use stackable_airflow_crd::airflowdb::AirflowDB;
use stackable_operator::client::Client;

use std::sync::Arc;
use strum::{EnumDiscriminants, IntoStaticStr};

use super::types::FetchedAdditionalData;

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("placeholder"))]
    Placeholder,
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub async fn fetch_additional_data(
    airflow_db: &Arc<AirflowDB>,
    client: &Client,
) -> Result<FetchedAdditionalData> {
    Ok(FetchedAdditionalData::default())
}

/*
   initial_secret

   client
                   .get_opt::<Secret>(
                       &airflow_db.spec.credentials_secret,
                       airflow_db
                           .namespace()
                           .as_deref()
                           .context(ObjectHasNoNamespaceSnafu)?,
                   )
                   .await
                   .with_context(|_| {
                       let mut secret_ref =
                           ObjectRef::<Secret>::new(&airflow_db.spec.credentials_secret);
                       if let Some(ns) = airflow_db.namespace() {
                           secret_ref = secret_ref.within(&ns);
                       }
                       SecretCheckSnafu { secret: secret_ref }
                   })?;
*/

/*
   vector_aggregator_address

   resolve_vector_aggregator_address(
                       client,
                       airflow_db.as_ref(),
                       airflow_db.spec.vector_aggregator_config_map_name.as_deref(),
                   )
                   .await
                   .context(ResolveVectorAggregatorAddressSnafu)?;
*/

/*
                  job

                                      client
                       .get::<Job>(&job_name, &ns)
                       .await
                       .context(GetInitializationJobSnafu {
                           init_job: ObjectRef::<Job>::new(&job_name).within(&ns),
                       })?;

*/
