use stackable_airflow_crd::airflowdb::AirflowDBStatus;
use stackable_operator::k8s_openapi::api::{
    batch::v1::Job,
    core::v1::{ConfigMap, Secret},
};

#[derive(Debug, Default)]
pub struct FetchedAdditionalData {
    pub initial_secret: Option<Secret>,
    pub vector_aggregator_address: Option<String>,
    pub job: Job,
}

#[derive(Debug)]
pub enum BuiltClusterResource {
    PatchRBAC,
    PatchConfigMap(ConfigMap),
    PatchJob(Job, AirflowDBStatus),
    PatchJobStatus(AirflowDBStatus),
}
