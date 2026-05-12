use stackable_operator::{
    builder::pod::volume::VolumeBuilder,
    k8s_openapi::api::core::v1::{ConfigMapVolumeSource, EmptyDirVolumeSource, Volume},
    product_logging,
};

use crate::{
    crd::MAX_LOG_FILES_SIZE,
    framework::product_logging::framework::ValidatedContainerLogConfigChoice,
};

pub const CONFIG_VOLUME_NAME: &str = "config";
pub const LOG_CONFIG_VOLUME_NAME: &str = "log-config";
pub const LOG_VOLUME_NAME: &str = "log";

// REVIEW: parameter changed from Option<&ContainerLogConfig> to &ValidatedContainerLogConfigChoice.
// Pattern matching is simpler now because we match on a flat enum instead of nested Option<ContainerLogConfig>.
pub fn create_volumes(
    config_map_name: &str,
    log_config: &ValidatedContainerLogConfigChoice,
) -> Vec<Volume> {
    let mut volumes = Vec::new();

    volumes.push(
        VolumeBuilder::new(CONFIG_VOLUME_NAME)
            .with_config_map(config_map_name)
            .build(),
    );
    volumes.push(Volume {
        name: LOG_VOLUME_NAME.into(),
        empty_dir: Some(EmptyDirVolumeSource {
            medium: None,
            size_limit: Some(product_logging::framework::calculate_log_volume_size_limit(
                &[MAX_LOG_FILES_SIZE],
            )),
        }),
        ..Volume::default()
    });

    if let ValidatedContainerLogConfigChoice::Custom(custom_config_map) = log_config {
        volumes.push(Volume {
            name: LOG_CONFIG_VOLUME_NAME.into(),
            config_map: Some(ConfigMapVolumeSource {
                name: custom_config_map.as_ref().into(),
                ..ConfigMapVolumeSource::default()
            }),
            ..Volume::default()
        });
    } else {
        volumes.push(Volume {
            name: LOG_CONFIG_VOLUME_NAME.into(),
            config_map: Some(ConfigMapVolumeSource {
                name: config_map_name.into(),
                ..ConfigMapVolumeSource::default()
            }),
            ..Volume::default()
        });
    }

    volumes
}
