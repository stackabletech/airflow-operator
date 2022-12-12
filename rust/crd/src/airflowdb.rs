use crate::{build_recommended_labels, AirflowCluster};

use serde::{Deserialize, Serialize};
use snafu::Snafu;
use stackable_operator::{
    builder::ObjectMetaBuilder,
    commons::product_image_selection::{ProductImage, ResolvedProductImage},
    k8s_openapi::{apimachinery::pkg::apis::meta::v1::Time, chrono::Utc},
    kube::{CustomResource, ResourceExt},
    schemars::{self, JsonSchema},
};

pub const AIRFLOW_DB_CONTROLLER_NAME: &str = "airflow-db";

#[derive(Snafu, Debug)]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("object is missing metadata to build owner reference"))]
    ObjectMissingMetadataForOwnerRef {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to retrieve airflow version"))]
    NoAirflowVersion,
}

type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Clone, CustomResource, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[kube(
    group = "airflow.stackable.tech",
    version = "v1alpha1",
    kind = "AirflowDB",
    plural = "airflowdbs",
    status = "AirflowDBStatus",
    namespaced,
    crates(
        kube_core = "stackable_operator::kube::core",
        k8s_openapi = "stackable_operator::k8s_openapi",
        schemars = "stackable_operator::schemars"
    )
)]
#[serde(rename_all = "camelCase")]
pub struct AirflowDBSpec {
    /// The Airflow image to use
    pub image: ProductImage,
    pub credentials_secret: String,
}

impl AirflowDB {
    /// Returns an AirflowDB resource with the same name, namespace and Airflow version as the cluster.
    pub fn for_airflow(
        airflow: &AirflowCluster,
        resolved_product_image: &ResolvedProductImage,
    ) -> Result<Self> {
        Ok(Self {
            // The db is deliberately not owned by the cluster so it doesn't get deleted when the
            // cluster gets deleted.  The schema etc. still exists in the database and can be reused
            // when the cluster is created again.
            metadata: ObjectMetaBuilder::new()
                .name_and_namespace(airflow)
                .with_recommended_labels(build_recommended_labels(
                    airflow,
                    AIRFLOW_DB_CONTROLLER_NAME,
                    &resolved_product_image.product_version,
                    "db-initializer",
                    "global",
                ))
                .build(),
            spec: AirflowDBSpec {
                image: airflow.spec.image.clone(),
                credentials_secret: airflow.spec.credentials_secret.clone(),
            },
            status: None,
        })
    }

    pub fn job_name(&self) -> String {
        self.name_unchecked()
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct AirflowDBStatus {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<Time>,
    pub condition: AirflowDBStatusCondition,
}

impl AirflowDBStatus {
    pub fn new() -> Self {
        Self {
            started_at: Some(Time(Utc::now())),
            condition: AirflowDBStatusCondition::Pending,
        }
    }

    pub fn initializing(&self) -> Self {
        let mut new = self.clone();
        new.condition = AirflowDBStatusCondition::Initializing;
        new
    }

    pub fn ready(&self) -> Self {
        let mut new = self.clone();
        new.condition = AirflowDBStatusCondition::Ready;
        new
    }

    pub fn failed(&self) -> Self {
        let mut new = self.clone();
        new.condition = AirflowDBStatusCondition::Failed;
        new
    }
}

impl Default for AirflowDBStatus {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, JsonSchema, PartialEq, Serialize)]
pub enum AirflowDBStatusCondition {
    Pending,
    Initializing,
    Ready,
    Failed,
}
