use stackable_airflow_crd::airflowdb::AirflowDB;
use stackable_airflow_crd::AirflowCluster;
use stackable_operator::crd::CustomResourceExt;

fn main() {
    built::write_built_file().expect("Failed to acquire build-time information");

    AirflowCluster::write_yaml_schema("../../deploy/crd/airflowcluster.crd.yaml").unwrap();
    AirflowDB::write_yaml_schema("../../deploy/crd/airflowdb.crd.yaml").unwrap();
}
