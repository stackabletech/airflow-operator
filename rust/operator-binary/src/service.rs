use std::collections::BTreeMap;

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    builder::meta::ObjectMetaBuilder,
    k8s_openapi::api::core::v1::{Service, ServicePort, ServiceSpec},
    kvp::{Annotations, Labels, ObjectLabels},
    role_utils::RoleGroupRef,
};

use crate::crd::{HTTP_PORT, HTTP_PORT_NAME, METRICS_PORT, METRICS_PORT_NAME, v1alpha1};

pub const METRICS_SERVICE_SUFFIX: &str = "metrics";
pub const HEADLESS_SERVICE_SUFFIX: &str = "headless";

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("object is missing metadata to build owner reference"))]
    ObjectMissingMetadataForOwnerRef {
        source: stackable_operator::builder::meta::Error,
    },

    #[snafu(display("failed to build Metadata"))]
    MetadataBuild {
        source: stackable_operator::builder::meta::Error,
    },

    #[snafu(display("failed to build Labels"))]
    LabelBuild {
        source: stackable_operator::kvp::LabelError,
    },
}

/// The rolegroup headless [`Service`] is a service that allows direct access to the instances of a certain rolegroup
/// This is mostly useful for internal communication between peers, or for clients that perform client-side load balancing.
pub fn build_rolegroup_headless_service(
    airflow: &v1alpha1::AirflowCluster,
    rolegroup_ref: &RoleGroupRef<v1alpha1::AirflowCluster>,
    object_labels: ObjectLabels<v1alpha1::AirflowCluster>,
    selector: BTreeMap<String, String>,
) -> Result<Service, Error> {
    let ports = headless_service_ports();

    let metadata = ObjectMetaBuilder::new()
        .name_and_namespace(airflow)
        .name(rolegroup_headless_service_name(
            &rolegroup_ref.object_name(),
        ))
        .ownerreference_from_resource(airflow, None, Some(true))
        .context(ObjectMissingMetadataForOwnerRefSnafu)?
        .with_recommended_labels(object_labels)
        .context(MetadataBuildSnafu)?
        .build();

    let service_spec = ServiceSpec {
        // Internal communication does not need to be exposed
        type_: Some("ClusterIP".to_string()),
        cluster_ip: Some("None".to_string()),
        ports: Some(ports),
        selector: Some(selector),
        publish_not_ready_addresses: Some(true),
        ..ServiceSpec::default()
    };

    Ok(Service {
        metadata,
        spec: Some(service_spec),
        status: None,
    })
}

/// The rolegroup metrics [`Service`] is a service that exposes metrics and a prometheus scraping label.
pub fn build_rolegroup_metrics_service(
    airflow: &v1alpha1::AirflowCluster,
    rolegroup_ref: &RoleGroupRef<v1alpha1::AirflowCluster>,
    object_labels: ObjectLabels<v1alpha1::AirflowCluster>,
    selector: BTreeMap<String, String>,
) -> Result<Service, Error> {
    let ports = metrics_service_ports();

    let metadata = ObjectMetaBuilder::new()
        .name_and_namespace(airflow)
        .name(rolegroup_metrics_service_name(&rolegroup_ref.object_name()))
        .ownerreference_from_resource(airflow, None, Some(true))
        .context(ObjectMissingMetadataForOwnerRefSnafu)?
        .with_recommended_labels(object_labels)
        .context(MetadataBuildSnafu)?
        .with_labels(prometheus_labels())
        .with_annotations(prometheus_annotations())
        .build();

    let service_spec = ServiceSpec {
        // Internal communication does not need to be exposed
        type_: Some("ClusterIP".to_string()),
        cluster_ip: Some("None".to_string()),
        ports: Some(ports),
        selector: Some(selector),
        publish_not_ready_addresses: Some(true),
        ..ServiceSpec::default()
    };

    Ok(Service {
        metadata,
        spec: Some(service_spec),
        status: None,
    })
}

pub fn stateful_set_service_name(
    rolegroup_ref: &RoleGroupRef<v1alpha1::AirflowCluster>,
) -> Option<String> {
    Some(rolegroup_headless_service_name(
        &rolegroup_ref.object_name(),
    ))
}

/// Returns the metrics rolegroup service name `<cluster>-<role>-<rolegroup>-<METRICS_SERVICE_SUFFIX>`.
// TODO: Replace by operator.rs functions
fn rolegroup_metrics_service_name(role_group_ref_object_name: &str) -> String {
    format!("{role_group_ref_object_name}-{METRICS_SERVICE_SUFFIX}")
}

/// Returns the headless rolegroup service name `<cluster>-<role>-<rolegroup>-<HEADLESS_SERVICE_SUFFIX>`.
// TODO: Replace by operator.rs functions
fn rolegroup_headless_service_name(role_group_ref_object_name: &str) -> String {
    format!("{role_group_ref_object_name}-{HEADLESS_SERVICE_SUFFIX}")
}

fn headless_service_ports() -> Vec<ServicePort> {
    vec![ServicePort {
        name: Some(HTTP_PORT_NAME.to_string()),
        port: HTTP_PORT.into(),
        protocol: Some("TCP".to_string()),
        ..ServicePort::default()
    }]
}

fn metrics_service_ports() -> Vec<ServicePort> {
    vec![ServicePort {
        name: Some(METRICS_PORT_NAME.to_string()),
        port: METRICS_PORT.into(),
        protocol: Some("TCP".to_string()),
        ..ServicePort::default()
    }]
}

/// Common labels for Prometheus
fn prometheus_labels() -> Labels {
    Labels::try_from([("prometheus.io/scrape", "true")]).expect("should be a valid label")
}

/// Common annotations for Prometheus
///
/// These annotations can be used in a ServiceMonitor.
///
/// see also <https://github.com/prometheus-community/helm-charts/blob/prometheus-27.32.0/charts/prometheus/values.yaml#L983-L1036>
fn prometheus_annotations() -> Annotations {
    Annotations::try_from([
        ("prometheus.io/path".to_owned(), "/metrics".to_owned()),
        ("prometheus.io/port".to_owned(), HTTP_PORT.to_string()),
        ("prometheus.io/scheme".to_owned(), "http".to_owned()),
        ("prometheus.io/scrape".to_owned(), "true".to_owned()),
    ])
    .expect("should be valid annotations")
}
