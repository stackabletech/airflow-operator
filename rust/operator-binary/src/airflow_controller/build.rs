use snafu::Snafu;
use stackable_airflow_crd::AirflowCluster;
use stackable_operator::product_config::ProductConfigManager;

use std::sync::Arc;
use strum::{EnumDiscriminants, IntoStaticStr};

use super::types::{BuiltClusterResource, FetchedAdditionalData};

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("placegholder"))]
    Placeholder,
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub fn build_cluster_resources(
    druid: Arc<AirflowCluster>,
    additional_data: FetchedAdditionalData,
    product_config: &ProductConfigManager,
) -> Result<Vec<BuiltClusterResource>> {
    Ok(vec![])
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::super::types::{BuiltClusterResource, FetchedAdditionalData};

    use super::build_cluster_resources;
    //use assert_json_diff::{assert_json_matches_no_panic, CompareMode, Config};
    use stackable_airflow_crd::AirflowCluster;
    use stackable_operator::product_config::ProductConfigManager;

    #[test]
    fn test_build_step_just_runs() {
        let cluster_cr = std::fs::File::open("test/smoke/cluster.yaml").unwrap();
        let deserializer = serde_yaml::Deserializer::from_reader(&cluster_cr);
        let druid_cluster: AirflowCluster =
            serde_yaml::with::singleton_map_recursive::deserialize(deserializer).unwrap();
        let product_config_manager =
            ProductConfigManager::from_yaml_file("test/smoke/properties.yaml").unwrap();

        let result = build_cluster_resources(
            Arc::new(druid_cluster),
            FetchedAdditionalData {},
            &product_config_manager,
        );

        assert!(result.is_ok(), "we want an ok, instead we got {:?}", result);
    }
}

