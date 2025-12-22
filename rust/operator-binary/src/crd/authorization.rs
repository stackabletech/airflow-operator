use stackable_operator::{client::Client, commons::opa::OpaApiVersion, shared::time::Duration};

use crate::crd::{AirflowAuthorization, AirflowOpaConfig, v1alpha2};

pub struct AirflowAuthorizationResolved {
    pub opa: Option<OpaConfigResolved>,
}

impl AirflowAuthorizationResolved {
    pub async fn from_authorization_config(
        client: &Client,
        airflow: &v1alpha2::AirflowCluster,
        authorization: &Option<AirflowAuthorization>,
    ) -> Result<Self, stackable_operator::commons::opa::Error> {
        let opa = if let Some(AirflowAuthorization {
            opa: Some(opa_config),
        }) = authorization
        {
            Some(OpaConfigResolved::from_opa_config(client, airflow, opa_config).await?)
        } else {
            None
        };
        Ok(AirflowAuthorizationResolved { opa })
    }
}

pub struct OpaConfigResolved {
    pub connection_string: String,
    pub cache_entry_time_to_live: Duration,
    pub cache_max_entries: u32,
}

impl OpaConfigResolved {
    pub async fn from_opa_config(
        client: &Client,
        airflow: &v1alpha2::AirflowCluster,
        airflow_opa_config: &AirflowOpaConfig,
    ) -> Result<Self, stackable_operator::commons::opa::Error> {
        let connection_string = airflow_opa_config
            .opa
            .full_document_url_from_config_map(client, airflow, None, OpaApiVersion::V1)
            .await?;
        Ok(OpaConfigResolved {
            connection_string,
            cache_entry_time_to_live: airflow_opa_config.cache.entry_time_to_live,
            cache_max_entries: airflow_opa_config.cache.max_entries,
        })
    }
}
