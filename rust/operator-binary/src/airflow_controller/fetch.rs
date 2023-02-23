use snafu::{OptionExt, ResultExt, Snafu};
use stackable_airflow_crd::{airflowdb::AirflowDB, AirflowCluster};
use stackable_operator::{
    client::Client,
    commons::authentication::AuthenticationClass,
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
    #[snafu(display("failed to retrieve Airflow DB"))]
    AirflowDBRetrieval {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to retrieve AuthenticationClass {authentication_class}"))]
    AuthenticationClassRetrieval {
        source: stackable_operator::error::Error,
        authentication_class: ObjectRef<AuthenticationClass>,
    },
    #[snafu(display("object has no namespace"))]
    ObjectHasNoNamespace,
    #[snafu(display("failed to resolve the Vector aggregator address"))]
    ResolveVectorAggregatorAddress {
        source: crate::common::product_logging::Error,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub async fn fetch_additional_data(
    airflow: &Arc<AirflowCluster>,
    client: &Client,
) -> Result<FetchedAdditionalData> {
    let airflow_db = client
        .get::<AirflowDB>(
            &airflow.name_unchecked(),
            airflow
                .namespace()
                .as_deref()
                .context(ObjectHasNoNamespaceSnafu)?,
        )
        .await
        .context(AirflowDBRetrievalSnafu)?;

    let aggregator_address = resolve_vector_aggregator_address(
        client,
        airflow.as_ref(),
        airflow.spec.vector_aggregator_config_map_name.as_deref(),
    )
    .await
    .context(ResolveVectorAggregatorAddressSnafu)?;

    let authentication_class = match &airflow.spec.authentication_config {
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
    };

    Ok(FetchedAdditionalData {
        airflow_db,
        authentication_class,
        aggregator_address,
    })
}
