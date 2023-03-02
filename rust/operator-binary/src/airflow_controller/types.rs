use stackable_airflow_crd::{airflowdb::AirflowDB, AirflowCluster};
use stackable_operator::{
    commons::authentication::AuthenticationClass,
    k8s_openapi::api::{
        apps::v1::StatefulSet,
        core::v1::{ConfigMap, Service},
    },
    role_utils::RoleGroupRef,
};

#[derive(Debug)]
pub struct FetchedAdditionalData {
    pub airflow_db: Option<AirflowDB>,
    pub aggregator_address: Option<String>,
    pub authentication_class: Option<AuthenticationClass>,
}

#[derive(Debug)]
pub enum BuiltClusterResource {
    PatchAirflowDB(AirflowDB),
    PatchRBAC,
    RoleService(Service),
    RolegroupService(Service, RoleGroupRef<AirflowCluster>),
    RolegroupConfigMap(ConfigMap, RoleGroupRef<AirflowCluster>),
    RolegroupStatefulSet(StatefulSet, RoleGroupRef<AirflowCluster>),
    DeleteOrphaned,
}
