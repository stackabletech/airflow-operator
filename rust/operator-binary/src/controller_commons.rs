use std::str::FromStr;

use stackable_operator::{
    builder::pod::volume::VolumeBuilder,
    k8s_openapi::api::core::v1::{ConfigMapVolumeSource, EmptyDirVolumeSource, Volume},
    product_logging,
    v2::{
        product_logging::framework::ValidatedContainerLogConfigChoice,
        types::kubernetes::VolumeName,
    },
};

use crate::crd::MAX_LOG_FILES_SIZE;

stackable_operator::constant!(pub CONFIG_VOLUME_NAME: VolumeName = "config");
stackable_operator::constant!(pub LOG_CONFIG_VOLUME_NAME: VolumeName = "log-config");
stackable_operator::constant!(pub LOG_VOLUME_NAME: VolumeName = "log");

pub fn create_volumes(
    config_map_name: &str,
    product_log_config: &ValidatedContainerLogConfigChoice,
) -> Vec<Volume> {
    let mut volumes = Vec::new();

    volumes.push(
        VolumeBuilder::new(&*CONFIG_VOLUME_NAME)
            .with_config_map(config_map_name)
            .build(),
    );
    volumes.push(Volume {
        name: LOG_VOLUME_NAME.to_string(),
        empty_dir: Some(EmptyDirVolumeSource {
            medium: None,
            size_limit: Some(product_logging::framework::calculate_log_volume_size_limit(
                &[MAX_LOG_FILES_SIZE],
            )),
        }),
        ..Volume::default()
    });

    // A custom log config is mounted from its own ConfigMap; an automatic one is rendered into the
    // rolegroup ConfigMap.
    let log_config_config_map = match product_log_config {
        ValidatedContainerLogConfigChoice::Custom(custom_config_map) => {
            custom_config_map.to_string()
        }
        ValidatedContainerLogConfigChoice::Automatic(_) => config_map_name.to_string(),
    };
    volumes.push(Volume {
        name: LOG_CONFIG_VOLUME_NAME.to_string(),
        config_map: Some(ConfigMapVolumeSource {
            name: log_config_config_map,
            ..ConfigMapVolumeSource::default()
        }),
        ..Volume::default()
    });

    volumes
}
