use snafu::Snafu;
use stackable_airflow_crd::AirflowCluster;
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
    airflow_db: &Arc<AirflowCluster>,
    client: &Client,
) -> Result<FetchedAdditionalData> {
    Err(Error::Placeholder)
}

/* airflow_db
client
        .get::<AirflowDB>(
            &airflow.name_unchecked(),
            airflow
                .namespace()
                .as_deref()
                .context(ObjectHasNoNamespaceSnafu)?,
        )
        .await
        .context(AirflowDBRetrievalSnafu)?;
 */

/* aggregator_address

resolve_vector_aggregator_address(
        client,
        airflow.as_ref(),
        airflow.spec.vector_aggregator_config_map_name.as_deref(),
    )
    .await
    .context(ResolveVectorAggregatorAddressSnafu)?;
 */

/*
authentication_class = match &airflow.spec.authentication_config {
        Some(authentication_config) => match &authentication_config.authentication_class {
            Some(authentication_class) => Some(
                AuthenticationClass::resolve(client, authentication_class)
                    .await
                    .context(AuthenticationClassRetrievalSnafu {
                        authentication_class: ObjectRef::<AuthenticationClass>::new(
                            authentication_class,
                        ),
                    })?,
            ),
            None => None,
        },
        None => None,
 */
