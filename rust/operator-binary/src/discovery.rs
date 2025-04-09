use std::collections::BTreeMap;

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    builder::{configmap::ConfigMapBuilder, meta::ObjectMetaBuilder},
    commons::product_image_selection::ResolvedProductImage,
    k8s_openapi::api::core::v1::ConfigMap,
    kube::runtime::reflector::ObjectRef,
};

use crate::{
    airflow_controller::AIRFLOW_CONTROLLER_NAME,
    crd::{
        AirflowRole, HTTP_PORT_NAME, METRICS_PORT_NAME, build_recommended_labels, utils::PodRef,
        v1alpha1,
    },
};

type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("object {airflow} is missing metadata to build owner reference"))]
    ObjectMissingMetadataForOwnerRef {
        source: stackable_operator::builder::meta::Error,
        airflow: ObjectRef<v1alpha1::AirflowCluster>,
    },

    #[snafu(display("failed to build ConfigMap"))]
    BuildConfigMap {
        source: stackable_operator::builder::configmap::Error,
    },

    #[snafu(display("failed to build object meta data"))]
    ObjectMeta {
        source: stackable_operator::builder::meta::Error,
    },
}

/// Creates a discovery config map containing the webserver endpoint for clients.
pub fn build_discovery_configmap(
    airflow: &v1alpha1::AirflowCluster,
    resolved_product_image: &ResolvedProductImage,
    role_podrefs: &BTreeMap<String, Vec<PodRef>>,
) -> Result<ConfigMap> {
    let mut cm = ConfigMapBuilder::new();

    let cmm = cm.metadata(
        ObjectMetaBuilder::new()
            .name_and_namespace(airflow)
            .ownerreference_from_resource(airflow, None, Some(true))
            .with_context(|_| ObjectMissingMetadataForOwnerRefSnafu {
                airflow: ObjectRef::from_obj(airflow),
            })?
            .with_recommended_labels(build_recommended_labels(
                airflow,
                AIRFLOW_CONTROLLER_NAME,
                &resolved_product_image.app_version_label,
                &AirflowRole::Webserver.to_string(),
                "discovery",
            ))
            .context(ObjectMetaSnafu)?
            .build(),
    );

    for role_podref in role_podrefs {
        for podref in role_podref.1 {
            if let PodRef {
                fqdn_override: Some(fqdn_override),
                ports,
                pod_name,
                ..
            } = podref
            {
                if let Some(ui_port) = ports.get(HTTP_PORT_NAME) {
                    cmm.add_data(
                        format!("{pod_name}.{HTTP_PORT_NAME}"),
                        format!("{fqdn_override}:{ui_port}"),
                    );
                }
                if let Some(metrics_port) = ports.get(METRICS_PORT_NAME) {
                    cmm.add_data(
                        format!("{pod_name}.{METRICS_PORT_NAME}"),
                        format!("{fqdn_override}:{metrics_port}"),
                    );
                }
            }
        }
    }

    cm.build().context(BuildConfigMapSnafu)
}
