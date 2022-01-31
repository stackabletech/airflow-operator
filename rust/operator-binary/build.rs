use stackable_airflow_crd::commands::Init;
use stackable_airflow_crd::AirflowCluster;
use stackable_operator::crd::CustomResourceExt;

fn main() {
    built::write_built_file().expect("Failed to acquire build-time information");

    AirflowCluster::write_yaml_schema("../../deploy/crd/airflowcluster.crd.yaml").unwrap();
    Init::write_yaml_schema("../../deploy/crd/init.crd.yaml").unwrap();
}
